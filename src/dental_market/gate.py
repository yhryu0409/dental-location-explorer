"""Executable Gate 1 acceptance scenario using a fresh data root."""

from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .config import PROJECT_ROOT, Settings
from .connectors import ConnectorError, FixtureConnector, Request
from .pipeline import (
    DentalMarketPipeline,
    SAMPLE_DATASET_ID,
    SAMPLE_SOURCE_ID,
    load_jsonl_fixture,
    run_sample,
)


@dataclass(frozen=True)
class GateCheck:
    passed: bool
    evidence: Mapping[str, Any]


@dataclass(frozen=True)
class GateResult:
    gate: str
    passed: bool
    data_root: str
    checks: Mapping[str, GateCheck]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _new_gate_root(base: Optional[Path] = None) -> Path:
    parent = Path(base) if base is not None else PROJECT_ROOT / "var" / "m1-gate-runs"
    parent = parent.expanduser().resolve()
    parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    target = parent / ("gate-%s-%s" % (stamp, uuid.uuid4().hex[:8]))
    target.mkdir()
    return target


def run_gate(settings: Settings, *, base_root: Optional[Path] = None) -> GateResult:
    """Execute every Gate 1 criterion against a newly created runtime root."""

    gate_root = _new_gate_root(base_root)
    gate_settings = replace(
        settings,
        data_root=gate_root,
        rate_limit_per_second=1_000_000.0,
        max_retries=2,
        retry_base_seconds=0.001,
    )
    pipeline = DentalMarketPipeline(gate_settings)

    first = run_sample(
        gate_settings,
        as_of_date="2026-09-01",
        run_id="gate-first",
    )
    if first.raw_collection_hash is None or first.artifacts is None:
        raise AssertionError("first Gate 1 run did not create raw and derived artifacts")
    first_pointer = pipeline.release_manager.current()
    first_collection_manifest = pipeline.catalog.get_collection_manifest(
        first.run_id, SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    raw_verified = pipeline.collection_store.verify(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID, first.raw_collection_hash
    )

    second = run_sample(
        gate_settings,
        as_of_date="2026-09-01",
        run_id="gate-second",
    )
    second_pointer = pipeline.release_manager.current()
    second_collection_manifest = pipeline.catalog.get_collection_manifest(
        second.run_id, SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    second_collection = pipeline.collection_store.read(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID, second.raw_collection_hash
    )

    recovery_root = gate_root / "raw-only-recovery"
    shutil.copytree(gate_root / "raw", recovery_root / "raw")
    recovery_settings = replace(gate_settings, data_root=recovery_root)
    recovery_pipeline = DentalMarketPipeline(recovery_settings)
    recovery_catalog_was_empty = recovery_pipeline.catalog.count_raw_objects() == 0
    rebuilt = recovery_pipeline.rebuild_from_raw(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        first.raw_collection_hash,
        as_of_date="2026-09-01",
        run_id="gate-rebuild",
    )
    if rebuilt.artifacts is None:
        raise AssertionError("Gate 1 rebuild did not create derived artifacts")

    watermark_before_failure = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=first.scope_fingerprint,
    )
    failure_caught = False
    try:
        run_sample(
            gate_settings,
            as_of_date="2026-09-01",
            run_id="gate-api-failure",
            transient_failures={None: 10},
        )
    except ConnectorError:
        failure_caught = True
    failure_pointer = pipeline.release_manager.current()
    failed_run = pipeline.catalog.get_run("gate-api-failure")
    watermark_after_failure = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=first.scope_fingerprint,
    )

    schema_before_atomic_failure = pipeline.catalog.latest_schema(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    atomic_pointer_before = pipeline.release_manager.current()
    atomic_watermark_before = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=first.scope_fingerprint,
    )
    with sqlite3.connect(str(pipeline.catalog.path)) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_gate_atomic_publish
            BEFORE UPDATE OF status ON runs
            WHEN NEW.run_id = 'gate-metadata-failure'
             AND NEW.status = 'succeeded'
            BEGIN
                SELECT RAISE(ABORT, 'forced Gate metadata failure');
            END
            """
        )
    changed_rows = load_jsonl_fixture()
    changed_rows[0] = {
        **changed_rows[0],
        "facility_name": "M1 원자성 실패 검증 치과",
    }
    atomic_failure_caught = False
    try:
        pipeline.collect_and_publish(
            FixtureConnector(
                changed_rows,
                page_size=20,
                dataset=SAMPLE_DATASET_ID,
                source=SAMPLE_SOURCE_ID,
            ),
            Request(dataset=SAMPLE_DATASET_ID, page_size=20),
            as_of_date="2026-09-02",
            run_id="gate-metadata-failure",
        )
    except sqlite3.IntegrityError:
        atomic_failure_caught = True
    atomic_failed_run = pipeline.catalog.get_run("gate-metadata-failure")
    atomic_failed_manifest = pipeline.catalog.get_collection_manifest(
        "gate-metadata-failure", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    atomic_failed_release = pipeline.catalog.get_release(
        "20260902-gate-metadata-failure"
    )

    clean_sample = GateCheck(
        passed=(
            first.status == "published"
            and first.collected_records == 6
            and first.artifacts.core.record_count == 6
            and first.artifacts.mart.record_count > 0
            and raw_verified
            and first_pointer is not None
        ),
        evidence={
            "run_id": first.run_id,
            "collected_records": first.collected_records,
            "core_records": first.artifacts.core.record_count,
            "mart_records": first.artifacts.mart.record_count,
            "raw_checksum_verified": raw_verified,
            "release_id": first.current_release_id,
        },
    )
    idempotent = GateCheck(
        passed=(
            second.status == "no_change"
            and second.collected_records == 0
            and second.raw_duplicates_dropped == 0
            and second.raw_collection_hash is not None
            and second_collection_manifest is not None
            and second_collection_manifest["status"] == "no_change"
            and second_collection_manifest["page_count"] == 1
            and second_collection_manifest["record_count"] == 0
            and len(second_collection.pages) == 1
            and second_collection.pages[0].status_code == 304
            and first_pointer == second_pointer
        ),
        evidence={
            "second_status": second.status,
            "new_records": second.collected_records,
            "duplicates": second.raw_duplicates_dropped,
            "raw_no_change_collection_captured": second.raw_collection_hash
            is not None,
            "raw_collection_hash": second.raw_collection_hash,
            "collection_status": (
                second_collection_manifest["status"]
                if second_collection_manifest is not None
                else None
            ),
            "http_status": second_collection.pages[0].status_code,
            "release_unchanged": first_pointer == second_pointer,
        },
    )
    raw_rebuild = GateCheck(
        passed=(
            recovery_catalog_was_empty
            and rebuilt.artifacts.staging.checksum
            == first.artifacts.staging.checksum
            and rebuilt.artifacts.core.checksum == first.artifacts.core.checksum
            and rebuilt.artifacts.mart.checksum == first.artifacts.mart.checksum
        ),
        evidence={
            "raw_collection_hash": first.raw_collection_hash,
            "ops_catalog_copied": False,
            "recovery_catalog_started_empty": recovery_catalog_was_empty,
            "staging_checksum_match": rebuilt.artifacts.staging.checksum
            == first.artifacts.staging.checksum,
            "core_checksum_match": rebuilt.artifacts.core.checksum
            == first.artifacts.core.checksum,
            "mart_checksum_match": rebuilt.artifacts.mart.checksum
            == first.artifacts.mart.checksum,
        },
    )
    failure_protection = GateCheck(
        passed=(
            failure_caught
            and failure_pointer == first_pointer
            and failed_run is not None
            and failed_run["status"] == "failed"
            and watermark_after_failure == watermark_before_failure
        ),
        evidence={
            "api_failure_caught": failure_caught,
            "release_unchanged": failure_pointer == first_pointer,
            "failed_run_status": failed_run["status"] if failed_run else None,
            "watermark_unchanged": watermark_after_failure
            == watermark_before_failure,
        },
    )
    run_history = GateCheck(
        passed=(
            first_collection_manifest is not None
            and first_collection_manifest["status"] == "published"
            and first_collection_manifest["page_count"] == 3
            and first_collection_manifest["record_count"] == 6
            and len(
                pipeline.catalog.list_watermark_history(
                    SAMPLE_SOURCE_ID,
                    SAMPLE_DATASET_ID,
                    scope_fingerprint=first.scope_fingerprint,
                )
            )
            == 1
            and pipeline.catalog.get_run(first.run_id)["result_json"] is not None
        ),
        evidence={
            "collection_manifest_recorded": first_collection_manifest is not None,
            "page_count": (
                first_collection_manifest["page_count"]
                if first_collection_manifest is not None
                else None
            ),
            "watermark_history_count": len(
                pipeline.catalog.list_watermark_history(
                    SAMPLE_SOURCE_ID,
                    SAMPLE_DATASET_ID,
                    scope_fingerprint=first.scope_fingerprint,
                )
            ),
            "run_result_recorded": pipeline.catalog.get_run(first.run_id)[
                "result_json"
            ]
            is not None,
        },
    )
    atomic_publish = GateCheck(
        passed=(
            atomic_failure_caught
            and pipeline.release_manager.current() == atomic_pointer_before
            and pipeline.catalog.get_watermark(
                SAMPLE_SOURCE_ID,
                SAMPLE_DATASET_ID,
                scope_fingerprint=first.scope_fingerprint,
            )
            == atomic_watermark_before
            and pipeline.catalog.latest_schema(
                SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
            )
            == schema_before_atomic_failure
            and atomic_failed_run is not None
            and atomic_failed_run["status"] == "failed"
            and atomic_failed_manifest is not None
            and atomic_failed_manifest["status"] == "failed"
            and atomic_failed_manifest["release_id"] is None
            and atomic_failed_release is not None
            and atomic_failed_release["status"] == "failed"
        ),
        evidence={
            "late_metadata_failure_caught": atomic_failure_caught,
            "release_pointer_restored": pipeline.release_manager.current()
            == atomic_pointer_before,
            "watermark_rolled_back": pipeline.catalog.get_watermark(
                SAMPLE_SOURCE_ID,
                SAMPLE_DATASET_ID,
                scope_fingerprint=first.scope_fingerprint,
            )
            == atomic_watermark_before,
            "schema_baseline_rolled_back": pipeline.catalog.latest_schema(
                SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
            )
            == schema_before_atomic_failure,
            "run_status": (
                atomic_failed_run["status"]
                if atomic_failed_run is not None
                else None
            ),
            "release_status": (
                atomic_failed_release["status"]
                if atomic_failed_release is not None
                else None
            ),
        },
    )
    checks = {
        "clean_one_command_sample_to_mart": clean_sample,
        "idempotent_rerun_zero_duplicates": idempotent,
        "raw_only_rebuild": raw_rebuild,
        "api_failure_preserves_last_good": failure_protection,
        "collection_manifest_and_watermark_history": run_history,
        "atomic_publish_failure_rolls_back": atomic_publish,
    }
    return GateResult(
        gate="M1",
        passed=all(check.passed for check in checks.values()),
        data_root=str(gate_root),
        checks=checks,
    )


def print_gate_result(result: GateResult) -> None:
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
