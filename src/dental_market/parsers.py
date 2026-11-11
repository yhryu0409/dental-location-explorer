"""Versioned parsers that operate only on verified immutable raw bytes."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple


class ParserError(ValueError):
    """Raised when a raw response cannot satisfy its declared parser contract."""


PageParser = Callable[[bytes, str], Sequence[Mapping[str, Any]]]


class ParserRegistry:
    """Explicit parser identity/version registry used by collection replay."""

    def __init__(self) -> None:
        self._parsers: Dict[Tuple[str, str], PageParser] = {}

    def register(
        self, parser_id: str, parser_version: str, parser: PageParser
    ) -> None:
        key = self._key(parser_id, parser_version)
        if key in self._parsers:
            raise ParserError("parser is already registered: %s@%s" % key)
        self._parsers[key] = parser

    def parse(
        self,
        parser_id: str,
        parser_version: str,
        body: bytes,
        content_type: str,
    ) -> List[Dict[str, Any]]:
        key = self._key(parser_id, parser_version)
        parser = self._parsers.get(key)
        if parser is None:
            raise ParserError("parser is not registered: %s@%s" % key)
        try:
            parsed = parser(body, content_type)
        except ParserError:
            raise
        except Exception as exc:
            raise ParserError("raw page parser failed: %s@%s" % key) from exc
        records: List[Dict[str, Any]] = []
        for index, value in enumerate(parsed):
            if not isinstance(value, Mapping):
                raise ParserError(
                    "parser output %d is not an object: %s@%s"
                    % (index, key[0], key[1])
                )
            records.append(dict(value))
        return records

    @staticmethod
    def _key(parser_id: str, parser_version: str) -> Tuple[str, str]:
        if not isinstance(parser_id, str) or not parser_id.strip():
            raise ParserError("parser_id must be non-empty")
        if not isinstance(parser_version, str) or not parser_version.strip():
            raise ParserError("parser_version must be non-empty")
        return parser_id, parser_version


def parse_jsonl_records(
    body: bytes, content_type: str
) -> Sequence[Mapping[str, Any]]:
    """Parse UTF-8 newline-delimited JSON objects without normalization."""

    del content_type  # Parser identity is authoritative; metadata is audited.
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ParserError("JSONL response is not valid UTF-8") from exc
    records: List[Mapping[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ParserError("invalid JSONL at line %d" % line_number) from exc
        if not isinstance(value, dict):
            raise ParserError("JSONL line %d is not an object" % line_number)
        records.append(value)
    return records


DEFAULT_PARSER_REGISTRY = ParserRegistry()
DEFAULT_PARSER_REGISTRY.register("jsonl-records", "1", parse_jsonl_records)


__all__ = [
    "DEFAULT_PARSER_REGISTRY",
    "PageParser",
    "ParserError",
    "ParserRegistry",
    "parse_jsonl_records",
]
