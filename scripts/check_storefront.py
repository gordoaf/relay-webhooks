"""Exercise the storefront against real local Relay and receiver processes.

Run with the repository's installed development environment. Optional browser
screenshots require Playwright and a browser; no real credentials/data are used.
"""

import argparse
import json
import os
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path
from urllib.error import URLError
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(check, label, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, URLError):
            pass
        time.sleep(0.15)
    raise AssertionError(f"Timed out waiting for {label}")


def request(base, path, data=None, token=None):
    headers = {"X-Storefront-Request": "1"}
    if token:
        headers["Authorization"] = "Bearer " + token
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = Request(base + path, None if data is None else json.dumps(data).encode(), headers)
    with build_opener(ProxyHandler({})).open(req, timeout=3) as response:
        return json.load(response)


def browser_check(base, screenshot, executable):
    from playwright.sync_api import sync_playwright

    screenshot = Path(screenshot).resolve()
    screenshot.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        options = {"headless": True}
        if executable:
            options["executable_path"] = executable
        browser = playwright.chromium.launch(**options)
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1050}, device_scale_factor=1)
            failures = []
            page.on("pageerror", lambda error: failures.append(str(error)))
            page.goto(base)
            page.locator("#warehouse-status").filter(has_text="Online").wait_for()
            page.locator("#customer").fill("Jordan Lee")
            page.locator("#quantity").fill("3")
            page.locator("#place-order").click()
            page.locator("#notice").filter(has_text="saved").wait_for()
            page.wait_for_function("""() => {
                const rows = [...document.querySelectorAll('#orders tr')];
                return rows.length === 3 && rows.every(row =>
                  row.textContent.includes('Fulfilled') && row.textContent.includes('Recorded'));
            }""")
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            page.screenshot(path=str(screenshot), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            mobile = screenshot.with_name(screenshot.stem + "-mobile" + screenshot.suffix)
            page.screenshot(path=str(mobile), full_page=True)
            assert not failures, failures
            return {"desktop": str(screenshot), "mobile": str(mobile)}
        finally:
            browser.close()


def check(screenshot=None, executable=None):
    relay_port, storefront_port = free_port(), free_port()
    while storefront_port == relay_port:
        storefront_port = free_port()
    relay_url, storefront_url = (
        f"http://127.0.0.1:{relay_port}",
        f"http://127.0.0.1:{storefront_port}",
    )
    token = secrets.token_urlsafe(32)
    processes = []
    with tempfile.TemporaryDirectory(prefix="relay-storefront-check-") as directory:
        folder = Path(directory)
        env = dict(os.environ)
        # Avoid inheriting operational restrictions from an unrelated installation.
        for key in list(env):
            if key.startswith("RELAY_"):
                del env[key]
        env.update(
            {
                "RELAY_ADMIN_TOKEN": token,
                "RELAY_DB_PATH": str(folder / "relay.db"),
                "RELAY_ALLOW_PRIVATE_NETWORKS": "true",
            }
        )
        log = (folder / "servers.log").open("w+", encoding="utf-8")

        def launch(args):
            child = subprocess.Popen(
                [sys.executable, *args], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
            )
            processes.append(child)
            return child

        try:
            launch(["-m", "relay.cli", "serve", "--port", str(relay_port)])
            wait_for(lambda: request(relay_url, "/healthz"), "Relay startup")
            setup = subprocess.run(
                [
                    sys.executable,
                    "examples/storefront/app.py",
                    "setup",
                    "--db",
                    str(folder / "storefront.db"),
                    "--relay-url",
                    relay_url,
                    "--port",
                    str(storefront_port),
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            assert setup.returncode == 0, setup.stderr.replace(token, "[redacted]")
            launch(
                [
                    "examples/storefront/app.py",
                    "serve",
                    "--db",
                    str(folder / "storefront.db"),
                    "--port",
                    str(storefront_port),
                ]
            )
            state = wait_for(lambda: request(storefront_url, "/api/state"), "storefront startup")
            project = state["project_id"]
            endpoints = request(relay_url, f"/api/projects/{project}/endpoints", token=token)[
                "endpoints"
            ]
            assert {tuple(item["subscriptions"]) for item in endpoints} == {
                ("order.created",),
                ("invoice.paid",),
            }

            def order_state(order_id):
                rows = request(storefront_url, "/api/state")["orders"]
                return next(row for row in rows if row["id"] == order_id)

            first = request(
                storefront_url, "/api/orders", {"customer": "Alex Morgan", "quantity": 1}
            )["order_id"]
            wait_for(
                lambda: all(order_state(first)[name] for name in ("warehouse", "accounting")),
                "both subscribed receivers",
            )
            request(storefront_url, "/api/services/warehouse", {"online": False})
            second = request(
                storefront_url, "/api/orders", {"customer": "Sam Rivera", "quantity": 2}
            )["order_id"]
            wait_for(
                lambda: order_state(second)["accounting"], "accounting during warehouse outage"
            )
            assert not order_state(second)["warehouse"]
            deliveries_path = f"/api/projects/{project}/deliveries"

            def retrying():
                return [
                    item
                    for item in request(relay_url, deliveries_path, token=token)["items"]
                    if item["status"] == "retrying"
                ]

            wait_for(retrying, "real receiver HTTP 503 retry")
            request(storefront_url, "/api/services/warehouse", {"online": True})
            wait_for(lambda: order_state(second)["warehouse"], "warehouse recovery")
            delivered = wait_for(
                lambda: (
                    items
                    if len(items := request(relay_url, deliveries_path, token=token)["items"]) == 4
                    and all(item["status"] == "delivered" for item in items)
                    else None
                ),
                "all four deliveries",
            )
            replayed = delivered[0]
            request(relay_url, f"{deliveries_path}/{replayed['id']}/replay", {}, token)
            wait_for(
                lambda: (
                    request(
                        relay_url,
                        f"{deliveries_path}/{replayed['id']}",
                        token=token,
                    )["status"]
                    == "delivered"
                ),
                "manual replay",
            )
            with closing(sqlite3.connect(folder / "storefront.db")) as db:
                assert db.execute("SELECT count(*) FROM receipts").fetchone()[0] == 4
                assert db.execute("SELECT count(*) FROM fulfillment").fetchone()[0] == 4
            pictures = browser_check(storefront_url, screenshot, executable) if screenshot else None
            return {
                "result": "passed",
                "checks": [
                    "two real event subscriptions",
                    "persistent outbox submission",
                    "signed receivers",
                    "independent receiver outage",
                    "retry and recovery",
                    "replay deduplication",
                ],
                "screenshots": pictures,
            }
        finally:
            for child in reversed(processes):
                if os.name == "nt":
                    # Windows venv launchers can spawn a second Python process.
                    # Terminate only this recorded child and its descendants.
                    subprocess.run(
                        ["taskkill", "/PID", str(child.pid), "/T", "/F"],
                        capture_output=True,
                        check=False,
                    )
                else:
                    child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screenshot")
    parser.add_argument("--browser-executable")
    args = parser.parse_args()
    print(json.dumps(check(args.screenshot, args.browser_executable), indent=2))
