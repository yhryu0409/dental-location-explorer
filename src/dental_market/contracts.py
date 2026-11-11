"""Shared, dependency-free data contracts for the v2 storage layer.

The contracts in this module deliberately contain no I/O.  Keeping schema
inference, hashing inputs, and result types pure makes both collection and
rebuild paths use exactly the same rules.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


class ContractViolation(ValueError):
    """Raised when data cannot satisfy a declared storage contract."""


class RawIntegrityError(RuntimeError):
    """Raised when an immutable raw object no longer matches its checksum."""


class ReleaseValidationError(RuntimeError):
    """Raised when a release candidate fails validation."""


def utc_now_iso() -> str:
    """Return a sortable UTC timestamp with second precision."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a value deterministically for checksums and manifests."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jsonl_bytes(records: Iterable[Mapping[str, Any]]) -> Tuple[bytes, int]:
    """Encode JSON objects as deterministic newline-delimited JSON."""

    lines: List[bytes] = []
    for record in records:
        if not isinstance(record, Mapping):
            raise ContractViolation("JSONL records must be mapping objects")
        lines.append(canonical_json_bytes(dict(record)))
    if not lines:
        return b"", 0
    return b"\n".join(lines) + b"\n", len(lines)


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, Mapping):
        return "object"
    return type(value).__name__


def infer_schema(records: Iterable[Mapping[str, Any]]) -> Dict[str, List[str]]:
    """Infer a shallow, stable field-to-observed-types schema.

    Raw drift detection intentionally stays shallow.  Deep parsing belongs in
    dataset-specific staging contracts, while this schema catches upstream
    additions, removals, and common scalar type changes without assumptions.
    """

    observed: Dict[str, set] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise ContractViolation("schema inference requires mapping objects")
        for key, value in record.items():
            if not isinstance(key, str):
                raise ContractViolation("schema field names must be strings")
            observed.setdefault(key, set()).add(_type_name(value))
    return {key: sorted(types) for key, types in sorted(observed.items())}


def schema_fingerprint(schema: Mapping[str, Sequence[str]]) -> str:
    normalized = {key: sorted(set(values)) for key, values in sorted(schema.items())}
    return sha256_bytes(canonical_json_bytes(normalized))


@dataclass(frozen=True)
class SchemaDrift:
    added_fields: Tuple[str, ...] = ()
    removed_fields: Tuple[str, ...] = ()
    type_changes: Mapping[str, Mapping[str, Tuple[str, ...]]] = field(
        default_factory=dict
    )
    baseline: bool = False

    @property
    def has_drift(self) -> bool:
        return bool(self.added_fields or self.removed_fields or self.type_changes)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "baseline": self.baseline,
            "has_drift": self.has_drift,
            "added_fields": list(self.added_fields),
            "removed_fields": list(self.removed_fields),
            "type_changes": {
                key: {
                    "before": list(value["before"]),
                    "after": list(value["after"]),
                }
                for key, value in sorted(self.type_changes.items())
            },
        }


def detect_schema_drift(
    previous: Optional[Mapping[str, Sequence[str]]],
    current: Mapping[str, Sequence[str]],
) -> SchemaDrift:
    """Compare two inferred schemas without silently accepting changes."""

    if previous is None:
        return SchemaDrift(baseline=True)

    previous_fields = set(previous)
    current_fields = set(current)
    changes: Dict[str, Mapping[str, Tuple[str, ...]]] = {}
    for key in sorted(previous_fields & current_fields):
        before = tuple(sorted(set(previous[key])))
        after = tuple(sorted(set(current[key])))
        if before != after:
            changes[key] = {"before": before, "after": after}
    return SchemaDrift(
        added_fields=tuple(sorted(current_fields - previous_fields)),
        removed_fields=tuple(sorted(previous_fields - current_fields)),
        type_changes=changes,
    )


@dataclass(frozen=True)
class RawObject:
    source_id: str
    dataset_id: str
    object_hash: str
    relative_path: str
    manifest_relative_path: str
    byte_size: int
    record_count: int
    schema: Mapping[str, Sequence[str]]
    schema_fingerprint: str
    schema_version: str
    run_id: str
    request_fingerprint: str
    retrieved_at: str
    created: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class LayerArtifact:
    layer: str
    artifact_id: str
    relative_path: str
    manifest_relative_path: str
    manifest_checksum: str
    checksum: str
    byte_size: int
    record_count: int
    created: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReleasePointer:
    release_id: str
    manifest_relative_path: str
    manifest_checksum: str
    published_at: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
