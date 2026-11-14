"""Bound JSON complexity before persistence so accepted tasks remain readable."""

import json
import math
from typing import Any

MAX_DEPTH = 32
MAX_NODES = 10_000
MAX_BYTES = 65_536


def validate_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("payload/result must be a JSON object")
    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > MAX_DEPTH or nodes > MAX_NODES:
            raise ValueError("JSON exceeds maximum depth (32) or node count (10000)")
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ValueError("JSON object keys must be strings")
            if len(item) + len(pending) > MAX_NODES:
                raise ValueError("JSON exceeds maximum node count (10000)")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            if len(item) + len(pending) > MAX_NODES:
                raise ValueError("JSON exceeds maximum node count (10000)")
            pending.extend((child, depth + 1) for child in item)
        elif item is None or type(item) in (str, bool, int):
            continue
        elif type(item) is float and math.isfinite(item):
            continue
        else:
            raise ValueError("Values must be JSON-safe and numbers must be finite")
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=True)
    except (ValueError, OverflowError) as error:
        raise ValueError("Value cannot be serialized as JSON") from error
    if len(encoded) > MAX_BYTES:
        raise ValueError("JSON exceeds maximum encoded size (65536 bytes)")
    return value
