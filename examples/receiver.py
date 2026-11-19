"""Minimal signed receiver with durable deduplication.

Run: RELAY_SIGNING_SECRET=whsec_... python examples/receiver.py
This example deliberately binds only to loopback.
"""

import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from relay.delivery import verify_signature

secret = os.environ["RELAY_SIGNING_SECRET"]
db = sqlite3.connect("receiver.db")
db.execute("CREATE TABLE IF NOT EXISTS orders(event_id TEXT PRIMARY KEY, data TEXT NOT NULL)")
db.commit()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/webhook":
            self.send_error(404)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 270000:
                self.send_error(413)
                return
            self.connection.settimeout(10)
            body = self.rfile.read(size)
            if len(body) != size or not verify_signature(secret, body, dict(self.headers)):
                self.send_error(401)
                return
            event = json.loads(body)
            # Commit deduplication and the example business effect in the same transaction.
            with db:
                db.execute(
                    "INSERT OR IGNORE INTO orders(event_id,data) VALUES(?,?)",
                    (event["id"], json.dumps(event["data"])),
                )
            self.send_response(204)
            self.end_headers()
        except (ValueError, KeyError, OSError):
            self.send_error(400)


print("Receiver: http://127.0.0.1:9000/webhook")
HTTPServer(("127.0.0.1", 9000), Handler).serve_forever()
