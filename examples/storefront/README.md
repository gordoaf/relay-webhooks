# A real storefront integration

This local example creates a paid sample order and sends two events through a
running Relay instance. The warehouse subscribes to `order.created`; accounting
subscribes to `invoice.paid`. Each receiver verifies the raw request signature and
commits event deduplication with its business effect in SQLite. No real payments
are processed.

## Run it

From the repository root, install Relay into a virtual environment:

```sh
python -m venv .venv
```

Activate it with `.venv\Scripts\Activate.ps1` in PowerShell, or
`source .venv/bin/activate` on macOS/Linux, then run:

```sh
python -m pip install -e .
```

In the first terminal, set a fresh administrator token and allow the example's
loopback receivers. PowerShell:

```powershell
$env:RELAY_ADMIN_TOKEN = python -c "import secrets; print(secrets.token_urlsafe(32))"
$env:RELAY_ALLOW_PRIVATE_NETWORKS = "true"
$env:RELAY_DB_PATH = "data/storefront-relay.db"
Write-Host "Copy this token to the second terminal: $env:RELAY_ADMIN_TOKEN"
python -m relay.cli serve
```

macOS/Linux:

```sh
export RELAY_ADMIN_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
export RELAY_ALLOW_PRIVATE_NETWORKS=true
export RELAY_DB_PATH=data/storefront-relay.db
printf 'Copy this token to the second terminal: %s\n' "$RELAY_ADMIN_TOKEN"
python -m relay.cli serve
```

In a second terminal, activate the same virtual environment and set
`RELAY_ADMIN_TOKEN` to that same token. Run:

```sh
python examples/storefront/app.py setup
python examples/storefront/app.py serve
```

Open **http://127.0.0.1:9000**. The Relay console is at
http://127.0.0.1:8787; use the administrator token there. Setup creates a separate
project and two event subscriptions. It saves only the project key and receiver
signing secrets in `data/storefront.db`; it does not retain the administrator
token or send credentials to the browser. Stop both programs with Ctrl+C.

Private-network destinations are enabled **only for this local exercise**. Both
programs bind to loopback. Keep the database in your private user directory; on
Windows it inherits that directory's ACLs. Do not publish this example as an
internet-facing store.

## Try the whole delivery lifecycle

1. Place an order. Both destination columns become complete after Relay delivers
   their respective events. Each endpoint should have only its subscribed type
   in the Relay console.
2. Take the warehouse offline, then place another order. Accounting continues;
   warehouse deliveries receive HTTP 503 and retry.
3. Bring the warehouse online. Relay's next scheduled attempt completes it. The
   switch does not reset the retry timer, so allow the displayed retry delay.
4. Keep a receiver offline until its retry budget is exhausted, then restore it.
   Use Replay or bulk recovery in Relay to deliver failed events. Replaying a
   completed event retains its ID, so the example records no duplicate effect.
5. Stop Relay and place an order. The order and its two events remain in the
   storefront database. Restart Relay with the **same token and database**; the
   persistent outbox resumes automatically. Restarting the storefront preserves
   orders, availability switches, deduplication, and pending submissions too.

An order and its outbox records commit in one transaction. The outbox uses a
stable `<order_id>:<event_type>` idempotency key, including after a crash between
Relay accepting an event and the storefront saving the receipt. Transient
acceptance failures retry with bounded backoff; permanent 4xx errors are shown
as needing operator attention instead of repeatedly creating new events.
If retention has removed an already accepted event, Relay returns its original
ID with HTTP 410. The storefront saves that acceptance and displays
**Accepted · details expired**; it does not fabricate a warehouse fulfillment or
accounting record, and never replaces the idempotency key.

This is an integration example, not a production commerce system: it has no
payment processor, inventory reservation, accounts, or public checkout
authentication. There is one storefront process. Its dashboard displays the
latest 100 orders and 20 pending outbox entries; the underlying database retains
all example data. The two consumers share a process and database for easy local
setup, while their availability and subscriptions are independent.

## Configuration

`setup` accepts `--relay-url http://127.0.0.1:8787`, `--port 9000`, and
`--db data/storefront.db`. `serve` accepts the same database and port. Setup
requires a local HTTP Relay origin and refuses to overwrite an existing
configuration. To run a fresh exercise, choose a new storefront database and
Relay database. If setup fails partway through, an incomplete project may remain
in Relay; inspect it in the console before trying again.

If you rotate a project key or signing secret in Relay, update the example's
local credentials before the overlap expires. Set `RELAY_PROJECT_KEY` and/or
`RELAY_WAREHOUSE_SIGNING_SECRET` / `RELAY_ACCOUNTING_SIGNING_SECRET` to the new
values in a terminal, then run:

```sh
python examples/storefront/app.py credentials --resume-blocked
```

The command validates and saves credentials without printing them. The running
sender and receivers load the new values without a restart. `--resume-blocked`
also makes blocked outbox records eligible to retry, retaining every original
event payload and idempotency key. Remove the environment variables afterward.
Use `--db` if your storefront has a different database path. The example never
stores an administrator token or performs the rotation itself.

After fixing other configuration errors, click **Retry blocked submissions** in
the dashboard or run `python examples/storefront/app.py resume`. A 409 payload
conflict still needs investigation and will block again if unresolved; recovery
does not rewrite the conflicting payload, replace its key, or create a new order.
Only pending blocked submissions are resumed, never already accepted events.

Run the regression tests from the repository root with:

```sh
python -m pip install -e ".[dev]"
python -m pytest tests/test_client.py tests/test_storefront.py
```

For an automated integration exercise using real temporary Relay and storefront
processes, run `python scripts/check_storefront.py`. It checks subscription
routing, an independent receiver outage, recovery, and replay deduplication,
then stops its servers and removes its temporary databases. The optional
`--screenshot path.png` uses Playwright to exercise checkout and capture desktop
and mobile views; `--browser-executable` selects an installed Chromium browser.
