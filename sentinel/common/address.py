"""Shared bind/advertise/registration hostname and IP validation."""

import re
from ipaddress import ip_address

_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z")


def validate_host(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 253:
        raise ValueError("host must be a hostname or IP address, not a URL")
    try:
        ip_address(value)
        return value
    except ValueError:
        pass
    if not all(_LABEL.fullmatch(label) for label in value.rstrip(".").split(".")):
        raise ValueError("host must be a hostname or IP address, not a URL")
    return value
