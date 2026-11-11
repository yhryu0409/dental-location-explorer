import pytest

from dental_market.parsers import ParserError, ParserRegistry, parse_jsonl_records


def test_parser_identity_and_version_are_independent_from_raw_bytes():
    registry = ParserRegistry()
    registry.register("fixture", "1", parse_jsonl_records)
    registry.register("fixture", "2", lambda body, content_type: [])
    body = b'{"id":"A"}\n'

    assert registry.parse("fixture", "1", body, "application/x-ndjson") == [
        {"id": "A"}
    ]
    assert registry.parse("fixture", "2", body, "application/x-ndjson") == []


def test_parser_registry_fails_closed_for_unknown_or_non_object_output():
    registry = ParserRegistry()
    with pytest.raises(ParserError, match="not registered"):
        registry.parse("missing", "1", b"", "application/octet-stream")

    registry.register("bad", "1", lambda body, content_type: ["not-object"])
    with pytest.raises(ParserError, match="not an object"):
        registry.parse("bad", "1", b"", "application/json")
