"""Focused contracts for M1 operational run and history metadata."""

import json
import sqlite3
from pathlib import Path

import pytest

from dental_market.contracts import ContractViolation
from dental_market.metadata_store import OpsCatalog
from dental_market.security import SensitiveDataError


def _record_collection(catalog: OpsCatalog, **overrides):
    values = {
        "run_id": "run-collection",
        "source_id": "fixture",
        "dataset_id": "facilities",
        "collection_hash": "a" * 64,
        "manifest_relative_path": "raw/source=fixture/collection.manifest.json",
        "manifest_checksum": "b" * 64,
        "request_fingerprint": "c" * 64,
        "scope_fingerprint": "d" * 64,
        "page_count": 2,
        "record_count": 11,
        "duplicate_count": 1,
        "parser_id": "jsonl-records",
        "parser_version": "1",
        "starting_watermark": "old",
        "ending_watermark": "new",
        "release_id": "release-001",
        "status": "published",
        "recorded_at": "2026-09-01T01:02:03Z",
    }
    values.update(overrides)
    catalog.record_collection_manifest(**values)


def test_finish_run_persists_result_without_breaking_old_call_shape(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run(
        "run-result",
        "facility_collection",
        config_fingerprint="safe-fingerprint",
        started_at="2026-09-01T00:00:00Z",
    )
    catalog.finish_run(
        "run-result",
        True,
        completed_at="2026-09-01T00:01:00Z",
        result={"records": 7, "release_id": "release-001"},
    )

    stored = catalog.get_run("run-result")
    assert stored is not None
    assert stored["status"] == "succeeded"
    assert json.loads(stored["result_json"]) == {
        "records": 7,
        "release_id": "release-001",
    }

    catalog.start_run("run-legacy-call", "facility_collection")
    # Preserve the pre-v3 positional signature through completed_at.
    catalog.finish_run(
        "run-legacy-call",
        False,
        {"message": "expected failure"},
        "2026-09-01T00:02:00Z",
    )
    legacy = catalog.get_run("run-legacy-call")
    assert legacy is not None
    assert json.loads(legacy["error_json"]) == {"message": "expected failure"}
    assert legacy["result_json"] is None


def test_completed_run_history_cannot_be_restarted_or_rewritten(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run("immutable-run", "facility_collection")
    catalog.finish_run(
        "immutable-run",
        False,
        error={"message": "original failure"},
    )
    original = catalog.get_run("immutable-run")

    with pytest.raises(ContractViolation, match="history is immutable"):
        catalog.start_run("immutable-run", "facility_collection")
    with pytest.raises(ContractViolation, match="already been completed"):
        catalog.finish_run(
            "immutable-run", True, result={"status": "published"}
        )

    assert catalog.get_run("immutable-run") == original


def test_collection_manifest_is_queryable_idempotent_and_run_scoped(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run("run-collection", "facility_collection")
    _record_collection(catalog)
    # A retry may not retain its original wall-clock timestamp.
    _record_collection(catalog, recorded_at=None)

    stored = catalog.get_collection_manifest(
        "run-collection", "fixture", "facilities"
    )
    assert stored is not None
    assert stored["collection_hash"] == "a" * 64
    assert stored["page_count"] == 2
    assert stored["record_count"] == 11
    assert stored["duplicate_count"] == 1
    assert stored["starting_watermark"] == "old"
    assert stored["ending_watermark"] == "new"
    assert stored["parser_id"] == "jsonl-records"
    assert stored["parser_version"] == "1"
    assert stored["release_id"] == "release-001"
    assert stored["status"] == "published"
    assert catalog.list_collection_manifests(source_id="fixture") == [stored]

    with pytest.raises(ContractViolation, match="different collection manifest"):
        _record_collection(catalog, record_count=12)
    with pytest.raises(ContractViolation, match="unknown run_id"):
        _record_collection(catalog, run_id="not-started")
    with pytest.raises(ContractViolation, match="page_count"):
        _record_collection(catalog, page_count=-1)


def test_watermark_history_records_each_write_and_is_sql_append_only(tmp_path: Path):
    database = tmp_path / "ops.sqlite"
    catalog = OpsCatalog(database)
    scope = "e" * 64
    catalog.set_watermark(
        "fixture",
        "facilities",
        "watermark-1",
        run_id="run-1",
        updated_at="2026-09-01T00:00:00Z",
        scope_fingerprint=scope,
    )
    catalog.set_watermark(
        "fixture",
        "facilities",
        "watermark-2",
        run_id="run-2",
        updated_at="2026-09-01T01:00:00Z",
        scope_fingerprint=scope,
    )
    # Reasserting a value is still an auditable write attempt.
    catalog.set_watermark(
        "fixture",
        "facilities",
        "watermark-2",
        run_id="run-3",
        updated_at="2026-09-01T02:00:00Z",
        scope_fingerprint=scope,
    )

    history = catalog.list_watermark_history(
        "fixture", "facilities", scope_fingerprint=scope
    )
    assert [row["previous_watermark"] for row in history] == [
        None,
        "watermark-1",
        "watermark-2",
    ]
    assert [row["new_watermark"] for row in history] == [
        "watermark-1",
        "watermark-2",
        "watermark-2",
    ]
    assert [row["run_id"] for row in history] == ["run-1", "run-2", "run-3"]
    assert catalog.list_watermark_history(
        "fixture", "facilities", limit=2, scope_fingerprint=scope
    ) == history[-2:]

    with sqlite3.connect(str(database)) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE watermark_history SET new_watermark = 'changed'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM watermark_history")


def test_watermarks_are_independent_by_collection_scope(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    scope_a = "1" * 64
    scope_b = "2" * 64
    catalog.set_watermark(
        "fixture",
        "facilities",
        "region-a-v1",
        scope_fingerprint=scope_a,
    )
    catalog.set_watermark(
        "fixture",
        "facilities",
        "region-b-v1",
        scope_fingerprint=scope_b,
    )

    assert catalog.get_watermark(
        "fixture", "facilities", scope_fingerprint=scope_a
    ) == "region-a-v1"
    assert catalog.get_watermark(
        "fixture", "facilities", scope_fingerprint=scope_b
    ) == "region-b-v1"
    assert len(
        catalog.list_watermark_history(
            "fixture", "facilities", scope_fingerprint=scope_a
        )
    ) == 1
    assert len(
        catalog.list_watermark_history(
            "fixture", "facilities", scope_fingerprint=scope_b
        )
    ) == 1


def test_direct_watermark_rejects_credentials_before_sqlite_write(
    tmp_path: Path,
):
    database = tmp_path / "ops.sqlite"
    catalog = OpsCatalog(database)
    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        catalog.set_watermark(
            "fixture",
            "facilities",
            "client_secret=WATERLEAK",
            scope_fingerprint="3" * 64,
        )
    assert b"WATERLEAK" not in database.read_bytes()


def test_direct_run_and_release_inputs_reject_credentials_before_sqlite_write(
    tmp_path: Path,
):
    database = tmp_path / "ops.sqlite"
    catalog = OpsCatalog(database)

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        catalog.start_run("client_secret=RUNLEAK", "facility_collection")
    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        catalog.record_release_candidate(
            "safe-release", "api_key=ASOFLEAK", "safe-run"
        )

    assert b"RUNLEAK" not in database.read_bytes()
    assert b"ASOFLEAK" not in database.read_bytes()


def test_v2_catalog_migrates_result_column_and_current_watermark_baseline(
    tmp_path: Path,
):
    database = tmp_path / "legacy-v2.sqlite"
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (2, '2026-08-31T00:00:00Z');

            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                config_fingerprint TEXT,
                error_json TEXT
            );
            INSERT INTO runs(
                run_id, pipeline_name, status, started_at
            ) VALUES (
                'legacy-run', 'facility_collection', 'running',
                '2026-08-31T00:00:00Z'
            );

            CREATE TABLE watermarks (
                source_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                watermark_value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                run_id TEXT,
                PRIMARY KEY(source_id, dataset_id)
            );
            INSERT INTO watermarks VALUES (
                'fixture', 'facilities', 'legacy-watermark',
                '2026-08-31T01:00:00Z', 'legacy-run'
            );
            """
        )

    catalog = OpsCatalog(database)
    catalog.finish_run(
        "legacy-run",
        True,
        result={"migration": "preserved"},
        completed_at="2026-09-01T00:00:00Z",
    )
    run = catalog.get_run("legacy-run")
    assert run is not None
    assert json.loads(run["result_json"]) == {"migration": "preserved"}

    new_scope = "f" * 64
    assert catalog.get_watermark(
        "fixture", "facilities", scope_fingerprint=new_scope
    ) is None
    with sqlite3.connect(str(database)) as connection:
        legacy_history = connection.execute(
            "SELECT * FROM watermark_history WHERE source_id = 'fixture'"
        ).fetchall()
    assert len(legacy_history) == 1
    assert legacy_history[0][3] == "legacy-v3-unscoped"
    assert legacy_history[0][4] is None
    assert legacy_history[0][5] == "legacy-watermark"
    assert legacy_history[0][6] == "2026-08-31T01:00:00Z"

    # Opening the upgraded catalog again must not duplicate the migration baseline.
    OpsCatalog(database)
    with sqlite3.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM watermark_history"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 4


def test_v3_watermark_scope_migration_preserves_history_and_is_idempotent(
    tmp_path: Path,
):
    database = tmp_path / "legacy-v3.sqlite"
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (3, '2026-08-31T00:00:00Z');
            CREATE TABLE watermarks (
                source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
                watermark_value TEXT NOT NULL, updated_at TEXT NOT NULL,
                run_id TEXT, PRIMARY KEY(source_id, dataset_id)
            );
            INSERT INTO watermarks VALUES (
                'fixture', 'facilities', 'current-v3',
                '2026-08-31T02:00:00Z', 'run-2'
            );
            CREATE TABLE watermark_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
                previous_watermark TEXT, new_watermark TEXT NOT NULL,
                changed_at TEXT NOT NULL, run_id TEXT
            );
            INSERT INTO watermark_history VALUES (
                7, 'fixture', 'facilities', NULL, 'first-v3',
                '2026-08-31T01:00:00Z', 'run-1'
            );
            INSERT INTO watermark_history VALUES (
                9, 'fixture', 'facilities', 'first-v3', 'current-v3',
                '2026-08-31T02:00:00Z', 'run-2'
            );
            CREATE INDEX idx_watermark_history_lookup
                ON watermark_history(source_id, dataset_id, history_id);
            CREATE TRIGGER watermark_history_no_update
            BEFORE UPDATE ON watermark_history
            BEGIN SELECT RAISE(ABORT, 'watermark history is append-only'); END;
            CREATE TRIGGER watermark_history_no_delete
            BEFORE DELETE ON watermark_history
            BEGIN SELECT RAISE(ABORT, 'watermark history is append-only'); END;
            """
        )

    catalog = OpsCatalog(database)
    new_scope = "4" * 64
    assert catalog.get_watermark(
        "fixture", "facilities", scope_fingerprint=new_scope
    ) is None
    with sqlite3.connect(str(database)) as connection:
        rows = connection.execute(
            """
            SELECT history_id, scope_fingerprint, previous_watermark,
                   new_watermark, changed_at
            FROM watermark_history ORDER BY history_id
            """
        ).fetchall()
        assert rows == [
            (
                7,
                "legacy-v3-unscoped",
                None,
                "first-v3",
                "2026-08-31T01:00:00Z",
            ),
            (
                9,
                "legacy-v3-unscoped",
                "first-v3",
                "current-v3",
                "2026-08-31T02:00:00Z",
            ),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='table' AND name LIKE '%_v3'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 4

    OpsCatalog(database)
    with sqlite3.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM watermark_history"
        ).fetchone()[0] == 2


def test_partial_collection_scope_migration_quarantines_null_on_restart(
    tmp_path: Path,
):
    database = tmp_path / "partial-v4.sqlite"
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (3, '2026-08-31T00:00:00Z');
            CREATE TABLE collection_manifests (
                run_id TEXT NOT NULL, source_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL, collection_hash TEXT NOT NULL,
                manifest_relative_path TEXT NOT NULL,
                manifest_checksum TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                scope_fingerprint TEXT,
                page_count INTEGER NOT NULL, record_count INTEGER NOT NULL,
                duplicate_count INTEGER NOT NULL, starting_watermark TEXT,
                ending_watermark TEXT, parser_id TEXT NOT NULL,
                parser_version TEXT NOT NULL, release_id TEXT,
                status TEXT NOT NULL, recorded_at TEXT NOT NULL,
                PRIMARY KEY(run_id, source_id, dataset_id)
            );
            INSERT INTO collection_manifests VALUES (
                'legacy-run', 'fixture', 'facilities',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'legacy.manifest.json',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                NULL, 1, 1, 0, NULL, NULL, 'jsonl-records', '1', NULL,
                'published', '2026-08-31T00:00:00Z'
            );
            """
        )

    OpsCatalog(database)
    with sqlite3.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT scope_fingerprint FROM collection_manifests"
        ).fetchone()[0] == "legacy-v3-unscoped"


def test_v2_schema_backfill_ignores_failed_unpublished_observations(
    tmp_path: Path,
):
    database = tmp_path / "schema-poison-v2.sqlite"
    failed_schema = json.dumps({"poison": ["string"]})
    accepted_schema = json.dumps({"id": ["string"]})
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (2, '2026-08-31T00:00:00Z');
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY, pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL, started_at TEXT NOT NULL,
                completed_at TEXT, config_fingerprint TEXT, error_json TEXT
            );
            INSERT INTO runs VALUES (
                'failed-run', 'facility_collection', 'failed',
                '2026-08-31T00:00:00Z', NULL, NULL, NULL
            );
            INSERT INTO runs VALUES (
                'good-run', 'facility_collection', 'succeeded',
                '2026-08-31T01:00:00Z', NULL, NULL, NULL
            );
            CREATE TABLE schema_observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL, schema_json TEXT NOT NULL,
                drift_json TEXT NOT NULL, observed_at TEXT NOT NULL,
                run_id TEXT NOT NULL,
                UNIQUE(run_id, source_id, dataset_id, schema_fingerprint)
            );
            CREATE TABLE releases (
                release_id TEXT PRIMARY KEY, as_of_date TEXT NOT NULL,
                run_id TEXT NOT NULL, status TEXT NOT NULL,
                manifest_relative_path TEXT, manifest_checksum TEXT,
                created_at TEXT NOT NULL, published_at TEXT,
                validation_json TEXT, error_json TEXT
            );
            INSERT INTO releases(
                release_id, as_of_date, run_id, status, created_at
            ) VALUES (
                'good-release', '2026-08-31', 'good-run', 'published',
                '2026-08-31T01:00:00Z'
            );
            """
        )
        for run_id, schema in (
            ("failed-run", failed_schema),
            ("good-run", accepted_schema),
        ):
            connection.execute(
                """
                INSERT INTO schema_observations(
                    source_id, dataset_id, schema_fingerprint, schema_json,
                    drift_json, observed_at, run_id
                ) VALUES ('fixture', 'facilities', ?, ?, '{}', ?, ?)
                """,
                (run_id + "-fingerprint", schema, "2026-08-31T00:00:00Z", run_id),
            )

    catalog = OpsCatalog(database)
    assert catalog.latest_schema("fixture", "facilities") == {
        "id": ["string"]
    }


def test_v3_schema_acceptance_is_rebuilt_before_v4_is_recorded(
    tmp_path: Path,
):
    database = tmp_path / "schema-poison-v3.sqlite"
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (3, '2026-08-31T00:00:00Z');
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY, pipeline_name TEXT NOT NULL,
                status TEXT NOT NULL, started_at TEXT NOT NULL,
                completed_at TEXT, config_fingerprint TEXT, error_json TEXT,
                result_json TEXT
            );
            INSERT INTO runs VALUES (
                'failed-run', 'facility_collection', 'failed',
                '2026-08-31T00:00:00Z', NULL, NULL, NULL, NULL
            );
            CREATE TABLE schema_observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL, schema_json TEXT NOT NULL,
                drift_json TEXT NOT NULL, observed_at TEXT NOT NULL,
                run_id TEXT NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 0,
                UNIQUE(run_id, source_id, dataset_id, schema_fingerprint)
            );
            INSERT INTO schema_observations(
                source_id, dataset_id, schema_fingerprint, schema_json,
                drift_json, observed_at, run_id, accepted
            ) VALUES (
                'fixture', 'facilities',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                '{"poison":["string"]}', '{}',
                '2026-08-31T00:00:00Z', 'failed-run', 1
            );
            """
        )

    catalog = OpsCatalog(database)
    assert catalog.latest_schema("fixture", "facilities") is None
    with sqlite3.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT accepted FROM schema_observations"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 4

    OpsCatalog(database)
    with sqlite3.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT accepted FROM schema_observations"
        ).fetchone()[0] == 0


def test_future_catalog_schema_is_rejected_fail_closed(tmp_path: Path):
    database = tmp_path / "future.sqlite"
    with sqlite3.connect(str(database)) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (99, '2099-01-01T00:00:00Z');
            """
        )
    before = database.read_bytes()
    with pytest.raises(ContractViolation, match="newer schema version"):
        OpsCatalog(database)
    assert database.read_bytes() == before
    with sqlite3.connect(str(database)) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    assert tables == [("schema_migrations",)]
