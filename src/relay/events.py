"""One serialization contract for admission and signed delivery bodies."""

import json


class PayloadTooLarge(RuntimeError):
    """The normalized event envelope exceeds the configured delivery limit."""


def serialize_event(event_id, event_type, created_at, payload):
    return json.dumps(
        {"id": event_id, "type": event_type, "created_at": created_at, "data": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
