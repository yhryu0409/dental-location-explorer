"""Deterministic M1 transforms from source-shaped rows to core and mart rows."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


STAGING_SCHEMA_VERSION = "m1.facility_staging.v1"
CORE_SCHEMA_VERSION = "m1.facility_core.v1"
MART_SCHEMA_VERSION = "m1.region_supply.v1"
_FACILITY_NAMESPACE = uuid.UUID("a8045c89-1c45-4bb4-b9c0-780906318faf")
_SOURCE_KEY_FIELDS = ("source_id", "dataset_id", "source_record_id")
_REQUIRED_FIELDS = _SOURCE_KEY_FIELDS + (
    "facility_name",
    "status",
    "sigungu_code",
    "effective_at",
    "observed_at",
    "available_at",
)
_ALLOWED_STATUSES = {"open", "closed", "temporarily_closed"}


class TransformContractError(ValueError):
    """Raised when an input record cannot satisfy the M1 data contract."""


@dataclass(frozen=True)
class TransformResult:
    records: Tuple[Dict[str, Any], ...]
    duplicates_dropped: int


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def source_key(record: Mapping[str, Any]) -> Tuple[str, str, str]:
    try:
        key = tuple(str(record[field]).strip() for field in _SOURCE_KEY_FIELDS)
    except KeyError as exc:
        raise TransformContractError(f"missing source key field: {exc.args[0]}") from exc
    if any(not value for value in key):
        raise TransformContractError("source key fields must not be blank")
    return key  # type: ignore[return-value]


def _validate_record(record: Mapping[str, Any]) -> None:
    missing = [field for field in _REQUIRED_FIELDS if record.get(field) in (None, "")]
    if missing:
        raise TransformContractError(
            "missing required fields: " + ", ".join(sorted(missing))
        )
    status = str(record["status"])
    if status not in _ALLOWED_STATUSES:
        raise TransformContractError(f"unsupported status: {status}")
    sigungu_code = str(record["sigungu_code"])
    if len(sigungu_code) != 5 or not sigungu_code.isdigit():
        raise TransformContractError("sigungu_code must be a five-digit official code")


def build_facility_core(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    retrieved_at: str,
) -> TransformResult:
    """Validate, deduplicate, and create deterministic facility-core records."""

    deduplicated: Dict[Tuple[str, str, str], Mapping[str, Any]] = {}
    duplicates = 0
    for record in records:
        _validate_record(record)
        key = source_key(record)
        existing = deduplicated.get(key)
        if existing is not None:
            if canonical_json(existing) != canonical_json(record):
                raise TransformContractError(
                    "conflicting records share source key: " + ":".join(key)
                )
            duplicates += 1
            continue
        deduplicated[key] = record

    core_rows: List[Dict[str, Any]] = []
    for key in sorted(deduplicated):
        source = deduplicated[key]
        canonical_id = str(
            uuid.uuid5(_FACILITY_NAMESPACE, "|".join(key))
        )
        core_rows.append(
            {
                "facility_id": canonical_id,
                "source_id": key[0],
                "dataset_id": key[1],
                "source_record_id": key[2],
                "facility_name": str(source["facility_name"]),
                "status": str(source["status"]),
                "sigungu_code": str(source["sigungu_code"]),
                "address": source.get("address"),
                "longitude": source.get("longitude"),
                "latitude": source.get("latitude"),
                "effective_at": str(source["effective_at"]),
                "effective_to": source.get("effective_to"),
                "observed_at": str(source["observed_at"]),
                "available_at": str(source["available_at"]),
                "retrieved_at": str(source.get("retrieved_at") or retrieved_at),
                "source_updated_at": source.get("source_updated_at"),
                "payload_hash": str(source.get("payload_hash") or sha256_json(source)),
                "ingestion_run_id": str(
                    source.get("ingestion_run_id") or run_id
                ),
                "schema_version": CORE_SCHEMA_VERSION,
                "license_id": "synthetic_test_only"
                if source.get("synthetic") is True
                else source.get("license_id"),
                "quality_status": "pass",
            }
        )
    return TransformResult(tuple(core_rows), duplicates)


def build_facility_staging(
    records: Iterable[Mapping[str, Any]],
    *,
    run_id: str,
    retrieved_at: str,
) -> TransformResult:
    """Validate and add raw provenance without changing source field meanings."""

    deduplicated: Dict[Tuple[str, str, str], Mapping[str, Any]] = {}
    duplicates = 0
    for record in records:
        _validate_record(record)
        key = source_key(record)
        existing = deduplicated.get(key)
        if existing is not None:
            if canonical_json(existing) != canonical_json(record):
                raise TransformContractError(
                    "conflicting records share source key: " + ":".join(key)
                )
            duplicates += 1
            continue
        deduplicated[key] = record

    staging_rows: List[Dict[str, Any]] = []
    for key in sorted(deduplicated):
        source = dict(deduplicated[key])
        staging_rows.append(
            {
                **source,
                "payload_hash": sha256_json(source),
                "ingestion_run_id": run_id,
                "retrieved_at": retrieved_at,
                "schema_version": STAGING_SCHEMA_VERSION,
                "quality_status": "pass",
            }
        )
    return TransformResult(tuple(staging_rows), duplicates)


def build_region_supply_mart(
    core_records: Sequence[Mapping[str, Any]], *, as_of_date: str
) -> List[Dict[str, Any]]:
    """Create a deterministic, descriptive supply mart without a composite score."""

    try:
        date.fromisoformat(as_of_date)
    except ValueError as exc:
        raise TransformContractError("as_of_date must use YYYY-MM-DD") from exc

    counts: Dict[str, Dict[str, int]] = {}
    for record in core_records:
        code = str(record["sigungu_code"])
        bucket = counts.setdefault(
            code,
            {
                "total_facilities": 0,
                "open_facilities": 0,
                "closed_facilities": 0,
                "temporarily_closed_facilities": 0,
            },
        )
        bucket["total_facilities"] += 1
        status_key = f"{record['status']}_facilities"
        bucket[status_key] += 1

    return [
        {
            "market_key": f"sigungu:{code}",
            "sigungu_code": code,
            "as_of_date": as_of_date,
            "schema_version": MART_SCHEMA_VERSION,
            **counts[code],
        }
        for code in sorted(counts)
    ]


def count_duplicate_source_keys(records: Iterable[Mapping[str, Any]]) -> int:
    seen = set()
    duplicates = 0
    for record in records:
        key = source_key(record)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
    return duplicates
