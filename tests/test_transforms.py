import pytest

from dental_market.transforms import (
    TransformContractError,
    build_facility_core,
    build_facility_staging,
    build_region_supply_mart,
    count_duplicate_source_keys,
)


def _record(record_id: str = "A") -> dict:
    return {
        "source_id": "fixture",
        "dataset_id": "facilities",
        "source_record_id": record_id,
        "facility_name": f"Sample {record_id}",
        "status": "open",
        "sigungu_code": "26320",
        "effective_at": "2026-08-01",
        "observed_at": "2026-08-31T00:00:00+09:00",
        "available_at": "2026-08-31T00:00:00+09:00",
        "synthetic": True,
    }


def test_core_transform_is_deterministic_and_deduplicates() -> None:
    row = _record()
    first = build_facility_core(
        [row, dict(row)], run_id="run-1", retrieved_at="2026-09-01T00:00:00Z"
    )
    second = build_facility_core(
        [row], run_id="run-1", retrieved_at="2026-09-01T00:00:00Z"
    )

    assert first.records == second.records
    assert first.duplicates_dropped == 1
    assert count_duplicate_source_keys(first.records) == 0
    assert first.records[0]["quality_status"] == "pass"


def test_staging_provenance_survives_core_rebuild() -> None:
    staging = build_facility_staging(
        [_record()], run_id="source-run", retrieved_at="2026-09-01T01:00:00Z"
    )
    core = build_facility_core(
        staging.records,
        run_id="rebuild-run",
        retrieved_at="2026-09-02T01:00:00Z",
    )

    assert core.records[0]["ingestion_run_id"] == "source-run"
    assert core.records[0]["retrieved_at"] == "2026-09-01T01:00:00Z"
    assert core.records[0]["payload_hash"] == staging.records[0]["payload_hash"]


def test_conflicting_duplicate_source_keys_fail_closed() -> None:
    first = _record()
    conflicting = {**first, "facility_name": "Different"}

    with pytest.raises(TransformContractError, match="conflicting records"):
        build_facility_core(
            [first, conflicting],
            run_id="run-1",
            retrieved_at="2026-09-01T00:00:00Z",
        )


def test_region_mart_has_separate_supply_counts() -> None:
    rows = [_record("A"), {**_record("B"), "status": "closed"}]
    core = build_facility_core(
        rows, run_id="run-1", retrieved_at="2026-09-01T00:00:00Z"
    )

    mart = build_region_supply_mart(core.records, as_of_date="2026-09-01")

    assert mart == [
        {
            "market_key": "sigungu:26320",
            "sigungu_code": "26320",
            "as_of_date": "2026-09-01",
            "schema_version": "m1.region_supply.v1",
            "total_facilities": 2,
            "open_facilities": 1,
            "closed_facilities": 1,
            "temporarily_closed_facilities": 0,
        }
    ]


def test_bad_official_code_is_rejected() -> None:
    with pytest.raises(TransformContractError, match="five-digit"):
        build_facility_core(
            [{**_record(), "sigungu_code": "북구"}],
            run_id="run-1",
            retrieved_at="2026-09-01T00:00:00Z",
        )
