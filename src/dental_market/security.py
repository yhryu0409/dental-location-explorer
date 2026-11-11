"""Small defensive redaction helpers shared by logs, CLI, and run metadata."""

from __future__ import annotations

import re
from typing import Mapping, Sequence


_AUTHORIZATION = re.compile(
    r"(?im)(?<![\"'])\b(?:proxy-)?authorization\b[\"']?\s*[:=]\s*[^\r\n]+"
)
_JSON_AUTHORIZATION = re.compile(
    r'''(?i)(["'](?:authorization|proxy-authorization|x-api-key|api-key|apikey)["']\s*:\s*)["'][^"']*["']'''
)
_SECRET_NAME = (
    r"(?:[A-Za-z0-9]+[_-])*"
    r"(?:service[_-]?key|api[_-]?key|apikey|confm[_-]?key|"
    r"consumer[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|client[_-]?secret|consumer[_-]?secret|"
    r"token|secret|password)"
)
_NAMED_SECRET = re.compile(
    r"(?i)\b(" + _SECRET_NAME + r")\b\s*[=:]\s*([^&\s,;]+)"
)
_JSON_SECRET = re.compile(
    r'''(?i)(["']''' + _SECRET_NAME + r'''["']\s*:\s*)["'][^"']*["']'''
)
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_URL_ENCODED_SECRET = re.compile(
    r"(?i)(" + _SECRET_NAME + r")%3[dD]([^&\s]+)"
)
_SENSITIVE_FIELD = re.compile(
    r"(?i)^(?:authorization|proxy-authorization|x-api-key|api-key|apikey|"
    + _SECRET_NAME
    + r")$"
)


class SensitiveDataError(ValueError):
    """Raised when durable evidence contains credential-shaped data."""


def redact_sensitive(value: object, *, limit: int = 2000) -> str:
    """Redact common credential forms without reading environment secrets."""

    message = str(value)[:limit]
    message = _JSON_AUTHORIZATION.sub(
        lambda match: match.group(1) + '"[REDACTED]"', message
    )
    message = _AUTHORIZATION.sub("Authorization=[REDACTED]", message)
    message = _JSON_SECRET.sub(lambda match: match.group(1) + '"[REDACTED]"', message)
    message = _NAMED_SECRET.sub(
        lambda match: match.group(1) + "=[REDACTED]", message
    )
    message = _URL_ENCODED_SECRET.sub(
        lambda match: match.group(1) + "%3D[REDACTED]", message
    )
    message = _BEARER.sub("Bearer [REDACTED]", message)
    return message


def reject_sensitive_data(value: object, *, location: str = "value") -> None:
    """Fail closed before nested metadata can persist credential material."""

    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key)
            if (
                _SENSITIVE_FIELD.fullmatch(key_text)
                or redact_sensitive(key_text) != key_text
            ):
                raise SensitiveDataError(
                    "sensitive field is forbidden in durable %s" % location
                )
            reject_sensitive_data(nested, location=location)
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for nested in value:
            reject_sensitive_data(nested, location=location)
        return
    if isinstance(value, str) and redact_sensitive(value) != value:
        raise SensitiveDataError(
            "credential-shaped string is forbidden in durable %s" % location
        )
