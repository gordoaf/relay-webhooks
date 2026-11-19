"""Workers hold short leases; delivery results are fenced by a claim token."""

import logging
import json
import math
import random
import threading
import time
from .delivery import DeliveryResult, deliver, sign_headers

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        store,
        allow_private=False,
        poll_interval=0.5,
        max_attempts=6,
        retry_base=2.0,
        concurrency=4,
    ):
        if concurrency < 1 or max_attempts < 1:
            raise ValueError("Concurrency and max_attempts must be positive")
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("poll_interval must be positive and finite")
        if not math.isfinite(retry_base) or retry_base < 0:
            raise ValueError("retry_base must be nonnegative and finite")
        self.store, self.allow_private = store, allow_private
        self.poll_interval, self.max_attempts = poll_interval, max_attempts
        self.retry_base, self.concurrency = retry_base, concurrency
        self._stopping = threading.Event()
        self._lifecycle = threading.Lock()
        self._threads = []

    def run_once(self):
        released = self.store.advance_recoveries()
        job = self.store.claim_delivery(lease_seconds=30)
        if job is None:
            return bool(released)
        attempt = job["cycle_attempts"] + 1
        started = time.monotonic()
        try:
            body = json.dumps(
                {
                    "id": job["event_id"],
                    "type": job["event_type"],
                    "created_at": job["created_at"],
                    "data": json.loads(job["payload_json"]),
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            headers = sign_headers(
                job["signing_secret"],
                job["event_id"],
                body,
                previous_secret=job.get("previous_signing_secret"),
            )
            headers["webhook-type"] = job["event_type"]
            result = deliver(
                job["endpoint_url"], body, headers, timeout=10, allow_private=self.allow_private
            )
        except (ValueError, TypeError, UnicodeError):
            result = DeliveryResult(None, "Invalid delivery configuration", 0, "", permanent=True)
        except Exception:
            logger.exception("Unexpected delivery error for delivery=%s", job["delivery_id"])
            result = DeliveryResult(
                None, "Unexpected delivery error", (time.monotonic() - started) * 1000, ""
            )
        status = result.status_code
        retryable = status is None or status in (408, 429) or 500 <= status <= 599
        next_attempt_at = None
        if retryable and not result.permanent and attempt < self.max_attempts:
            base = min(3600, self.retry_base * 2 ** min(attempt - 1, 16))
            delay = min(3600, random.uniform(base * 0.75, base * 1.25))
            if result.retry_after_seconds is not None:
                delay = max(delay, result.retry_after_seconds)
            next_attempt_at = time.time() + delay
        accepted = self.store.complete_delivery(
            job["delivery_id"],
            job["lease_token"],
            status_code=status,
            error=result.error,
            duration_ms=result.duration_ms,
            response_excerpt=result.response_excerpt,
            next_attempt_at=next_attempt_at,
            max_attempts=self.max_attempts,
            timed_out=result.timed_out,
        )
        logger.info(
            "delivery=%s attempt=%s status=%s retry=%s lease_accepted=%s",
            job["delivery_id"],
            attempt,
            status,
            next_attempt_at is not None,
            accepted,
        )
        return True

    def _run(self):
        while not self._stopping.is_set():
            try:
                worked = self.run_once()
            except Exception:
                logger.exception("Worker cycle failed; unfinished leases will expire")
                worked = False
            if not worked:
                self._stopping.wait(self.poll_interval)

    def start(self):
        with self._lifecycle:
            if any(t.is_alive() for t in self._threads):
                return
            self._stopping.clear()
            self._threads = [
                threading.Thread(target=self._run, name=f"relay-worker-{i}", daemon=True)
                for i in range(self.concurrency)
            ]
            for thread in self._threads:
                thread.start()

    def stop(self):
        with self._lifecycle:
            self._stopping.set()
            for thread in self._threads:
                if thread is not threading.current_thread():
                    thread.join()
            self._threads = []
