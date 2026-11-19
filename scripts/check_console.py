"""Exercise the live console and browser simulation in an isolated Chromium session.

Install the optional ui dependencies, then: python scripts/check_console.py
Use --browser-path to test with an existing Chrome/Edge executable.
"""

import argparse
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import uvicorn
from playwright.sync_api import expect, sync_playwright
from starlette.staticfiles import StaticFiles

from relay.app import create_app
from relay.config import Settings

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def server(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    instance = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
    thread = threading.Thread(target=instance.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 15
        while not instance.started:
            if not thread.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError("Test server did not start")
            time.sleep(0.05)
        yield f"http://127.0.0.1:{port}"
    finally:
        instance.should_exit = True
        thread.join(timeout=15)
        sock.close()


def navigate(page, view):
    page.locator(f'.nav-item[data-view="{view}"]').click()
    expect(page.locator(f"#view-{view}")).to_be_visible()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser-path")
    args = parser.parse_args()
    subprocess.run([sys.executable, "scripts/build_demo.py"], cwd=ROOT, check=True)
    errors = []
    with (
        tempfile.TemporaryDirectory(prefix="relay-console-") as directory,
        sync_playwright() as engine,
    ):
        token = secrets.token_urlsafe(32)
        app = create_app(
            Settings(
                db_path=str(Path(directory) / "console.db"),
                admin_token=token,
                worker_enabled=False,
                maintenance_enabled=False,
                allow_private_networks=True,
                retention_days=30,
            )
        )
        browser = engine.chromium.launch(headless=True, executable_path=args.browser_path)
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        page = context.new_page()
        page.on("pageerror", lambda error: errors.append(str(error)))
        try:
            with server(app) as url:
                page.goto(url)
                page.locator("#admin-token").fill(token)
                page.locator("#login-form button[type=submit]").click()
                expect(page.locator("#login-dialog")).not_to_be_visible()
                page.locator("#new-project").click()
                page.locator("#project-name").fill("Browser shop")
                page.locator("#form-submit").click()
                expect(page.locator("#secret-value")).to_contain_text("relay_")
                page.locator("#close-secret").click()
                navigate(page, "endpoints")
                page.locator("#add-endpoint").click()
                page.locator("#endpoint-url").fill("http://127.0.0.1:9999/warehouse")
                page.locator("#endpoint-description").fill("Warehouse")
                page.locator("#endpoint-subscriptions").fill("order.*")
                page.locator("#form-submit").click()
                expect(page.locator("#secret-value")).to_contain_text("whsec_")
                page.locator("#close-secret").click()
                expect(page.locator("#endpoint-list")).to_contain_text("order.*")
                page.locator("[data-edit]").click()
                page.locator("#endpoint-rate").fill("5")
                page.locator("#form-submit").click()
                expect(page.locator("#endpoint-list")).to_contain_text("5 requests")
                page.locator("[data-rotate]").click()
                page.locator("#rotation-overlap").fill("60")
                page.locator("#form-submit").click()
                expect(page.locator("#secret-value")).to_contain_text("whsec_")
                page.locator("#close-secret").click()
                navigate(page, "deliveries")
                page.locator("#send-event").click()
                page.locator("#form-submit").click()
                expect(page.locator("#stat-total")).to_have_text("1")
                store = app.state.store
                with store._db(True) as db:
                    db.execute("UPDATE deliveries SET status='failed',next_attempt_at=NULL")
                navigate(page, "recovery")
                page.locator("#new-recovery").click()
                page.locator("#form-submit").click()
                expect(page.locator("#form-title")).to_have_text("Review recovery")
                expect(page.locator("#form-fields")).to_contain_text("1 selected")
                page.locator("#form-submit").click()
                expect(page.locator("#recovery-list")).to_contain_text("1 selected")
                page.locator("[data-cancel-recovery]").click()
                expect(page.locator("#recovery-list")).to_contain_text("1 cancelled")
                navigate(page, "operations")
                expect(page.locator("#storage-details")).to_contain_text("30 days")
                page.locator("#preview-cleanup").click()
                expect(page.locator("#form-title")).to_have_text("Review storage cleanup")
                page.locator("#form-submit").click()
                expect(page.locator("#cleanup-result")).to_contain_text("Removed")
                page.screenshot(path=str(ROOT / "dist" / "console-desktop.png"), full_page=True)
                page.set_viewport_size({"width": 390, "height": 844})
                page.screenshot(path=str(ROOT / "dist" / "console-mobile.png"), full_page=True)
                assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), (
                    "Mobile overflow"
                )
                page.set_viewport_size({"width": 1440, "height": 1000})
                navigate(page, "quickstart")
                page.locator("#rotate-project-key").click()
                page.locator("#form-submit").click()
                expect(page.locator("#secret-value")).to_contain_text("relay_")
                page.locator("#close-secret").click()
            with server(StaticFiles(directory=ROOT / "dist", html=True)) as url:
                page.goto(url)
                expect(page.locator("#stat-total")).to_have_text("8")
                navigate(page, "operations")
                expect(page.locator("#storage-details")).to_contain_text("30 days")
                navigate(page, "endpoints")
                page.locator("[data-edit]").click()
                page.locator("#endpoint-subscriptions").fill("invoice.*")
                page.locator("#form-submit").click()
                expect(page.locator("#endpoint-list")).to_contain_text("invoice.*")
                navigate(page, "recovery")
                page.locator("#new-recovery").click()
                page.locator("#form-submit").click()
                expect(page.locator("#form-title")).to_have_text("Review recovery")
                page.locator("#form-submit").click()
                expect(page.locator("#recovery-list")).to_contain_text("1 requeued")
            assert not errors, errors
            print(
                "Live console and browser simulation workflows passed; desktop/mobile captures in dist."
            )
        finally:
            browser.close()


if __name__ == "__main__":
    main()
