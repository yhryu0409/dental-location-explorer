"""SQLite operational metadata catalog for reproducible ingestion runs."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .contracts import (
    ContractViolation,
    RawObject,
    ReleasePointer,
    SchemaDrift,
    canonical_json_bytes,
    detect_schema_drift,
    infer_schema,
    schema_fingerprint,
    sha256_bytes,
    utc_now_iso,
)
from .security import reject_sensitive_data


_BASE_SCHEMA_VERSION = 4
_SCHEMA_VERSION = 5
_LEGACY_UNSCOPED_SCOPE = "legacy-v3-unscoped"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_REFRESH_PLAN_KEYS = {
    "source_id",
    "dataset_id",
    "scope_fingerprint",
    "parser_id",
    "parser_version",
    "contract_fingerprint",
    "expected_schema_fingerprint",
    "required",
}
_REFRESH_COMMIT_KEYS = {
    "source_id",
    "dataset_id",
    "scope_fingerprint",
    "collection_manifest",
    "schema",
    "contract_fingerprint",
    "watermark",
    "source_as_of",
    "artifacts",
}
_PAGE_REFERENCE_KEYS = {
    "object_hash",
    "relative_path",
    "manifest_relative_path",
    "page_index",
    "request_fingerprint",
    "request_cursor_fingerprint",
    "status_code",
    "content_type",
    "response_headers",
    "retrieved_at",
    "source_as_of",
}
_PUBLISHED_COLLECTION_KEYS = {
    "run_id",
    "source_id",
    "dataset_id",
    "collection_hash",
    "manifest_relative_path",
    "manifest_checksum",
    "request_fingerprint",
    "scope_fingerprint",
    "page_count",
    "record_count",
    "duplicate_count",
    "starting_watermark",
    "ending_watermark",
    "parser_id",
    "parser_version",
    "release_id",
    "status",
}
_RESUME_POLICIES = {"stable_snapshot_page", "partition", "restart_full"}


def _normalize_scope_fingerprint(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(
        r"[0-9a-f]{64}", value
    ) is None:
        raise ContractViolation(
            "scope_fingerprint must be a lowercase SHA-256 digest"
        )
    return value


def _normalize_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ContractViolation(
            field_name + " must be a lowercase SHA-256 digest"
        )
    return value


def _normalize_optional_text(
    value: Any,
    field_name: str,
    *,
    location: str,
) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ContractViolation(field_name + " must be a non-blank string")
    reject_sensitive_data(value, location=location + " " + field_name)
    return value


def _watermark_fingerprint(value: Optional[str]) -> Optional[str]:
    return sha256_bytes(value.encode("utf-8")) if value is not None else None


def _require_exact_keys(
    value: Mapping[str, Any], expected: set, location: str
) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ContractViolation(location + " shape is invalid")
    return dict(value)


def _validate_durable_strings(
    *, location: str, allow_none: bool = False, **values: Optional[str]
) -> None:
    """Reject blank or credential-shaped strings at catalog write boundaries."""

    for field_name, value in values.items():
        if value is None and allow_none:
            continue
        if not isinstance(value, str) or not value.strip():
            raise ContractViolation(field_name + " must be a non-blank string")
        reject_sensitive_data(value, location=location + " " + field_name)


class OpsCatalog:
    """Small operational catalog; analytical rows never live in this database."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._reject_future_schema_read_only()
        self._migrate()

    def _reject_future_schema_read_only(self) -> None:
        """Probe an existing catalog without writing before any migration DDL."""

        if not self.path.is_file():
            return
        uri = "file:%s?mode=ro" % self.path.resolve().as_posix()
        with sqlite3.connect(uri, uri=True) as connection:
            has_migrations = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'schema_migrations'
                """
            ).fetchone()
            if has_migrations is None:
                return
            newest = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            if newest is not None and int(newest) > _SCHEMA_VERSION:
                raise ContractViolation(
                    "ops catalog was created by a newer schema version"
                )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.path), timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _migrate(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            pipeline_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('running', 'succeeded', 'failed')),
            started_at TEXT NOT NULL,
            completed_at TEXT,
            config_fingerprint TEXT,
            error_json TEXT,
            result_json TEXT
        );

        CREATE TABLE IF NOT EXISTS watermarks (
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            scope_fingerprint TEXT NOT NULL,
            watermark_value TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            run_id TEXT,
            PRIMARY KEY(source_id, dataset_id, scope_fingerprint)
        );

        CREATE TABLE IF NOT EXISTS watermark_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            scope_fingerprint TEXT NOT NULL,
            previous_watermark TEXT,
            new_watermark TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            run_id TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_watermark_history_lookup
            ON watermark_history(
                source_id, dataset_id, history_id
            );

        CREATE TRIGGER IF NOT EXISTS watermark_history_no_update
        BEFORE UPDATE ON watermark_history
        BEGIN
            SELECT RAISE(ABORT, 'watermark history is append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS watermark_history_no_delete
        BEFORE DELETE ON watermark_history
        BEGIN
            SELECT RAISE(ABORT, 'watermark history is append-only');
        END;

        CREATE TABLE IF NOT EXISTS raw_objects (
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            object_hash TEXT NOT NULL,
            relative_path TEXT NOT NULL UNIQUE,
            manifest_relative_path TEXT NOT NULL UNIQUE,
            byte_size INTEGER NOT NULL,
            record_count INTEGER NOT NULL,
            schema_fingerprint TEXT NOT NULL,
            schema_json TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            first_run_id TEXT NOT NULL,
            first_retrieved_at TEXT NOT NULL,
            PRIMARY KEY(source_id, dataset_id, object_hash)
        );

        CREATE TABLE IF NOT EXISTS raw_ingestions (
            ingestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            object_hash TEXT NOT NULL,
            retrieved_at TEXT NOT NULL,
            created_object INTEGER NOT NULL CHECK(created_object IN (0, 1)),
            UNIQUE(run_id, source_id, dataset_id, request_fingerprint),
            FOREIGN KEY(source_id, dataset_id, object_hash)
                REFERENCES raw_objects(source_id, dataset_id, object_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_raw_request
            ON raw_ingestions(source_id, dataset_id, request_fingerprint, retrieved_at);

        CREATE TABLE IF NOT EXISTS schema_observations (
            observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            schema_fingerprint TEXT NOT NULL,
            schema_json TEXT NOT NULL,
            drift_json TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            run_id TEXT NOT NULL,
            accepted INTEGER NOT NULL DEFAULT 0 CHECK(accepted IN (0, 1)),
            UNIQUE(run_id, source_id, dataset_id, schema_fingerprint)
        );

        CREATE INDEX IF NOT EXISTS idx_schema_latest
            ON schema_observations(source_id, dataset_id, observation_id DESC);

        CREATE TABLE IF NOT EXISTS collection_manifests (
            run_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            collection_hash TEXT NOT NULL,
            manifest_relative_path TEXT NOT NULL,
            manifest_checksum TEXT NOT NULL,
            request_fingerprint TEXT NOT NULL,
            scope_fingerprint TEXT NOT NULL,
            page_count INTEGER NOT NULL CHECK(page_count >= 0),
            record_count INTEGER NOT NULL CHECK(record_count >= 0),
            duplicate_count INTEGER NOT NULL CHECK(duplicate_count >= 0),
            starting_watermark TEXT,
            ending_watermark TEXT,
            parser_id TEXT NOT NULL,
            parser_version TEXT NOT NULL,
            release_id TEXT,
            status TEXT NOT NULL,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY(run_id, source_id, dataset_id),
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_collection_manifest_lookup
            ON collection_manifests(
                source_id, dataset_id, collection_hash, recorded_at
            );

        CREATE TABLE IF NOT EXISTS releases (
            release_id TEXT PRIMARY KEY,
            as_of_date TEXT NOT NULL,
            run_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('candidate', 'published', 'failed')),
            manifest_relative_path TEXT,
            manifest_checksum TEXT,
            created_at TEXT NOT NULL,
            published_at TEXT,
            validation_json TEXT,
            error_json TEXT
        );
        """
        with self._connect() as connection:
            connection.executescript(ddl)
            run_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "result_json" not in run_columns:
                connection.execute("ALTER TABLE runs ADD COLUMN result_json TEXT")
            self._migrate_watermark_scopes(connection)
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_watermark_history_scope
                ON watermark_history(
                    source_id, dataset_id, scope_fingerprint, history_id
                )
                """
            )
            collection_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(collection_manifests)"
                ).fetchall()
            }
            if "scope_fingerprint" not in collection_columns:
                connection.execute(
                    "ALTER TABLE collection_manifests ADD COLUMN scope_fingerprint TEXT"
                )
            connection.execute(
                "UPDATE collection_manifests SET scope_fingerprint = ? "
                "WHERE scope_fingerprint IS NULL",
                (_LEGACY_UNSCOPED_SCOPE,),
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_collection_manifest_scope
                ON collection_manifests(
                    source_id, dataset_id, scope_fingerprint, recorded_at
                )
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(schema_observations)"
                ).fetchall()
            }
            if "accepted" not in columns:
                connection.execute(
                    """
                    ALTER TABLE schema_observations
                    ADD COLUMN accepted INTEGER NOT NULL DEFAULT 0
                    CHECK(accepted IN (0, 1))
                    """
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_schema_accepted
                ON schema_observations(
                    source_id, dataset_id, accepted, observation_id DESC
                )
                """
            )
            version_three_applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (3,),
            ).fetchone()
            if version_three_applied is None:
                # An older catalog cannot reconstruct past transitions.  Preserve
                # its current value as the first auditable baseline exactly once.
                connection.execute(
                    """
                    INSERT INTO watermark_history(
                        source_id, dataset_id, scope_fingerprint, previous_watermark,
                        new_watermark, changed_at, run_id
                    )
                    SELECT
                        w.source_id, w.dataset_id, w.scope_fingerprint, NULL,
                        w.watermark_value, w.updated_at, w.run_id
                    FROM watermarks AS w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM watermark_history AS h
                        WHERE h.source_id = w.source_id
                          AND h.dataset_id = w.dataset_id
                          AND h.scope_fingerprint = w.scope_fingerprint
                    )
                    """
                )
                connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                    "VALUES (3, ?)",
                    (utc_now_iso(),),
                )
            version_four_applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (_BASE_SCHEMA_VERSION,),
            ).fetchone()
            if version_four_applied is None:
                # v3 catalogs could contain accepted observations written by
                # failed or never-published runs. Rebuild the baseline once
                # from durable run/release evidence before declaring v4.
                self._backfill_schema_acceptance(connection)
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (_BASE_SCHEMA_VERSION, utc_now_iso()),
                )

    def _ensure_refresh_schema(self) -> None:
        """Lazily install the M2-C schema without changing M1-only catalogs.

        Keeping v5 feature-gated preserves the v4 migration contract observed by
        existing M1 callers.  The first refresh API call performs the complete,
        idempotent upgrade under a writer lock.
        """

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            newest = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            if newest is not None and int(newest) > _SCHEMA_VERSION:
                raise ContractViolation(
                    "ops catalog was created by a newer schema version"
                )

            release_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(releases)"
                ).fetchall()
            }
            if "inventory_fingerprint" not in release_columns:
                connection.execute(
                    "ALTER TABLE releases ADD COLUMN inventory_fingerprint TEXT"
                )

            observation_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(schema_observations)"
                ).fetchall()
            }
            for column in (
                "parser_id",
                "parser_version",
                "contract_fingerprint",
            ):
                if column not in observation_columns:
                    connection.execute(
                        "ALTER TABLE schema_observations ADD COLUMN "
                        + column
                        + " TEXT"
                    )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_run_sources (
                    run_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    scope_fingerprint TEXT NOT NULL,
                    parser_id TEXT NOT NULL,
                    parser_version TEXT NOT NULL,
                    contract_fingerprint TEXT NOT NULL,
                    expected_schema_fingerprint TEXT NOT NULL,
                    required INTEGER NOT NULL CHECK(required IN (0, 1)),
                    planned_order INTEGER NOT NULL CHECK(planned_order >= 0),
                    starting_watermark TEXT,
                    status TEXT NOT NULL CHECK(
                        status IN (
                            'planned', 'collecting', 'collected',
                            'published', 'failed'
                        )
                    ),
                    PRIMARY KEY(
                        run_id, source_id, dataset_id, scope_fingerprint
                    ),
                    UNIQUE(run_id, source_id, dataset_id),
                    UNIQUE(run_id, planned_order),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    scope_fingerprint TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    request_plan_fingerprint TEXT NOT NULL,
                    parser_id TEXT NOT NULL,
                    parser_version TEXT NOT NULL,
                    contract_fingerprint TEXT NOT NULL,
                    resume_policy TEXT NOT NULL CHECK(
                        resume_policy IN (
                            'stable_snapshot_page', 'partition', 'restart_full'
                        )
                    ),
                    status TEXT NOT NULL CHECK(
                        status IN ('collecting', 'complete', 'abandoned', 'failed')
                    ),
                    starting_watermark TEXT,
                    ending_watermark TEXT,
                    collection_hash TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    FOREIGN KEY(
                        run_id, source_id, dataset_id, scope_fingerprint
                    ) REFERENCES refresh_run_sources(
                        run_id, source_id, dataset_id, scope_fingerprint
                    )
                )
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_refresh_active_attempt
                ON collection_attempts(
                    run_id, source_id, dataset_id, scope_fingerprint
                ) WHERE status = 'collecting'
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_refresh_attempt_lookup
                ON collection_attempts(
                    run_id, source_id, dataset_id, scope_fingerprint,
                    started_at, attempt_id
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_attempt_page_requests (
                    attempt_id TEXT NOT NULL,
                    page_index INTEGER NOT NULL CHECK(page_index >= 0),
                    request_resume_token TEXT,
                    request_fingerprint TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(attempt_id, page_index),
                    FOREIGN KEY(attempt_id)
                        REFERENCES collection_attempts(attempt_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_page_requests_no_update
                BEFORE UPDATE ON collection_attempt_page_requests
                BEGIN
                    SELECT RAISE(ABORT, 'collection page requests are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_page_requests_no_delete
                BEFORE DELETE ON collection_attempt_page_requests
                BEGIN
                    SELECT RAISE(ABORT, 'collection page requests are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_attempt_page_responses (
                    attempt_id TEXT NOT NULL,
                    page_index INTEGER NOT NULL CHECK(page_index >= 0),
                    request_fingerprint TEXT NOT NULL,
                    object_hash TEXT NOT NULL,
                    response_envelope_fingerprint TEXT NOT NULL,
                    source_as_of TEXT,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(attempt_id, page_index),
                    FOREIGN KEY(attempt_id)
                        REFERENCES collection_attempts(attempt_id)
                )
                """
            )
            response_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(collection_attempt_page_responses)"
                ).fetchall()
            }
            if "source_as_of" not in response_columns:
                connection.execute(
                    "ALTER TABLE collection_attempt_page_responses "
                    "ADD COLUMN source_as_of TEXT"
                )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_page_responses_no_update
                BEFORE UPDATE ON collection_attempt_page_responses
                BEGIN
                    SELECT RAISE(ABORT, 'collection page responses are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_page_responses_no_delete
                BEFORE DELETE ON collection_attempt_page_responses
                BEGIN
                    SELECT RAISE(ABORT, 'collection page responses are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_attempt_pages (
                    attempt_id TEXT NOT NULL,
                    page_index INTEGER NOT NULL CHECK(page_index >= 0),
                    page_reference_json TEXT NOT NULL,
                    request_resume_token TEXT,
                    next_resume_token TEXT,
                    page_watermark TEXT,
                    consistency_fingerprint TEXT,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(attempt_id, page_index),
                    FOREIGN KEY(attempt_id)
                        REFERENCES collection_attempts(attempt_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_pages_no_update
                BEFORE UPDATE ON collection_attempt_pages
                BEGIN
                    SELECT RAISE(ABORT, 'collection attempt pages are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS collection_attempt_pages_no_delete
                BEFORE DELETE ON collection_attempt_pages
                BEGIN
                    SELECT RAISE(ABORT, 'collection attempt pages are append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS release_source_inventory (
                    release_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    scope_fingerprint TEXT NOT NULL,
                    collection_hash TEXT NOT NULL,
                    parser_id TEXT NOT NULL,
                    parser_version TEXT NOT NULL,
                    contract_fingerprint TEXT NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    starting_watermark_fingerprint TEXT,
                    ending_watermark_fingerprint TEXT,
                    source_as_of TEXT,
                    artifacts_json TEXT NOT NULL,
                    origin_release_id TEXT,
                    entry_fingerprint TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY(
                        release_id, source_id, dataset_id, scope_fingerprint
                    ),
                    FOREIGN KEY(release_id) REFERENCES releases(release_id),
                    FOREIGN KEY(
                        run_id, source_id, dataset_id, scope_fingerprint
                    ) REFERENCES refresh_run_sources(
                        run_id, source_id, dataset_id, scope_fingerprint
                    )
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_release_source_inventory_run
                ON release_source_inventory(run_id, source_id, dataset_id)
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS release_source_inventory_no_update
                BEFORE UPDATE ON release_source_inventory
                BEGIN
                    SELECT RAISE(ABORT, 'release source inventory is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS release_source_inventory_no_delete
                BEFORE DELETE ON release_source_inventory
                BEGIN
                    SELECT RAISE(ABORT, 'release source inventory is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_schema_parser_contract
                ON schema_observations(
                    source_id, dataset_id, parser_id, parser_version,
                    contract_fingerprint, accepted, observation_id DESC
                )
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) "
                "VALUES (?, ?)",
                (_SCHEMA_VERSION, utc_now_iso()),
            )

    @staticmethod
    def _migrate_watermark_scopes(connection: sqlite3.Connection) -> None:
        """Upgrade the v3 single-scope watermark tables without losing history."""

        watermark_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(watermarks)").fetchall()
        }
        history_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(watermark_history)"
            ).fetchall()
        }
        legacy_tables = {
            str(row["name"])
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name IN (
                    'watermarks_v3', 'watermark_history_v3'
                )
                """
            ).fetchall()
        }
        scoped = (
            "scope_fingerprint" in watermark_columns
            and "scope_fingerprint" in history_columns
        )
        if scoped and not legacy_tables:
            return
        connection.execute("SAVEPOINT watermark_scope_v4")
        try:
            connection.execute("DROP TRIGGER IF EXISTS watermark_history_no_update")
            connection.execute("DROP TRIGGER IF EXISTS watermark_history_no_delete")
            connection.execute("DROP INDEX IF EXISTS idx_watermark_history_lookup")
            connection.execute("DROP INDEX IF EXISTS idx_watermark_history_scope")
            if not scoped:
                connection.execute(
                    "ALTER TABLE watermarks RENAME TO watermarks_v3"
                )
                connection.execute(
                    "ALTER TABLE watermark_history RENAME TO watermark_history_v3"
                )
                connection.execute(
                    """
                    CREATE TABLE watermarks (
                        source_id TEXT NOT NULL,
                        dataset_id TEXT NOT NULL,
                        scope_fingerprint TEXT NOT NULL,
                        watermark_value TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        run_id TEXT,
                        PRIMARY KEY(source_id, dataset_id, scope_fingerprint)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE watermark_history (
                        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_id TEXT NOT NULL,
                        dataset_id TEXT NOT NULL,
                        scope_fingerprint TEXT NOT NULL,
                        previous_watermark TEXT,
                        new_watermark TEXT NOT NULL,
                        changed_at TEXT NOT NULL,
                        run_id TEXT
                    )
                    """
                )
            elif connection.execute(
                "SELECT COUNT(*) FROM watermarks"
            ).fetchone()[0] or connection.execute(
                "SELECT COUNT(*) FROM watermark_history"
            ).fetchone()[0]:
                raise ContractViolation(
                    "partial watermark scope migration requires manual recovery"
                )

            connection.execute(
                """
                INSERT INTO watermarks(
                    source_id, dataset_id, scope_fingerprint,
                    watermark_value, updated_at, run_id
                )
                SELECT source_id, dataset_id, ?, watermark_value, updated_at, run_id
                FROM watermarks_v3
                """,
                (_LEGACY_UNSCOPED_SCOPE,),
            )
            connection.execute(
                """
                INSERT INTO watermark_history(
                    history_id, source_id, dataset_id, scope_fingerprint,
                    previous_watermark, new_watermark, changed_at, run_id
                )
                SELECT history_id, source_id, dataset_id, ?, previous_watermark,
                       new_watermark, changed_at, run_id
                FROM watermark_history_v3
                """,
                (_LEGACY_UNSCOPED_SCOPE,),
            )
            connection.execute("DROP TABLE watermarks_v3")
            connection.execute("DROP TABLE watermark_history_v3")
            connection.execute(
                """
                CREATE INDEX idx_watermark_history_lookup
                ON watermark_history(source_id, dataset_id, history_id)
                """
            )
            connection.execute(
                """
                CREATE INDEX idx_watermark_history_scope
                ON watermark_history(
                    source_id, dataset_id, scope_fingerprint, history_id
                )
                """
            )
            connection.execute(
                """
                CREATE TRIGGER watermark_history_no_update
                BEFORE UPDATE ON watermark_history
                BEGIN
                    SELECT RAISE(ABORT, 'watermark history is append-only');
                END
                """
            )
            connection.execute(
                """
                CREATE TRIGGER watermark_history_no_delete
                BEFORE DELETE ON watermark_history
                BEGIN
                    SELECT RAISE(ABORT, 'watermark history is append-only');
                END
                """
            )
            connection.execute("RELEASE SAVEPOINT watermark_scope_v4")
        except Exception:
            connection.execute("ROLLBACK TO SAVEPOINT watermark_scope_v4")
            connection.execute("RELEASE SAVEPOINT watermark_scope_v4")
            raise

    @staticmethod
    def _backfill_schema_acceptance(connection: sqlite3.Connection) -> None:
        """Rebuild accepted baselines safely when upgrading the M1 catalog."""

        accepted_schemas: Dict[tuple, Mapping[str, Sequence[str]]] = {}
        rows = connection.execute(
            """
            SELECT
                o.observation_id, o.source_id, o.dataset_id, o.schema_json,
                r.status AS run_status,
                EXISTS(
                    SELECT 1 FROM releases AS rel
                    WHERE rel.run_id = o.run_id AND rel.status = 'published'
                ) AS has_published_release
            FROM schema_observations AS o
            LEFT JOIN runs AS r ON r.run_id = o.run_id
            ORDER BY o.observation_id
            """
        ).fetchall()
        for row in rows:
            key = (str(row["source_id"]), str(row["dataset_id"]))
            schema = json.loads(row["schema_json"])
            drift = detect_schema_drift(accepted_schemas.get(key), schema)
            eligible = (
                row["run_status"] == "succeeded"
                and int(row["has_published_release"]) == 1
            )
            accepted = eligible and not drift.has_drift
            connection.execute(
                """
                UPDATE schema_observations
                SET accepted = ?, drift_json = ?
                WHERE observation_id = ?
                """,
                (
                    int(accepted),
                    canonical_json_bytes(drift.to_dict()).decode("utf-8"),
                    row["observation_id"],
                ),
            )
            if accepted:
                accepted_schemas[key] = schema

    def start_run(
        self,
        run_id: str,
        pipeline_name: str,
        config_fingerprint: Optional[str] = None,
        started_at: Optional[str] = None,
    ) -> None:
        reject_sensitive_data(run_id, location="run id")
        reject_sensitive_data(pipeline_name, location="pipeline name")
        if config_fingerprint is not None:
            reject_sensitive_data(
                config_fingerprint, location="run config fingerprint"
            )
        if started_at is not None:
            reject_sensitive_data(started_at, location="run start time")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ContractViolation("run_id must be a non-blank string")
        if not isinstance(pipeline_name, str) or not pipeline_name.strip():
            raise ContractViolation("pipeline_name must be a non-blank string")
        timestamp = started_at or utc_now_iso()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if existing is not None:
                raise ContractViolation(
                    "run_id already exists; run history is immutable"
                )
            connection.execute(
                """
                INSERT INTO runs(
                    run_id, pipeline_name, status, started_at, config_fingerprint
                ) VALUES (?, ?, 'running', ?, ?)
                """,
                (run_id, pipeline_name, timestamp, config_fingerprint),
            )

    def finish_run(
        self,
        run_id: str,
        succeeded: bool,
        error: Optional[Mapping[str, Any]] = None,
        completed_at: Optional[str] = None,
        result: Optional[Mapping[str, Any]] = None,
    ) -> None:
        reject_sensitive_data(run_id, location="run id")
        if completed_at is not None:
            reject_sensitive_data(completed_at, location="run completion time")
        if error is not None:
            reject_sensitive_data(error, location="run error")
        if result is not None:
            reject_sensitive_data(result, location="run result")
        status = "succeeded" if succeeded else "failed"
        error_json = (
            canonical_json_bytes(dict(error)).decode("utf-8")
            if error is not None
            else None
        )
        result_json = (
            canonical_json_bytes(dict(result)).decode("utf-8")
            if result is not None
            else None
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE runs
                SET status = ?, completed_at = ?, error_json = ?, result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (
                    status,
                    completed_at or utc_now_iso(),
                    error_json,
                    result_json,
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ContractViolation(
                    "run_id is unknown or has already been completed"
                )

    def fail_refresh_run(
        self,
        run_id: str,
        error: Mapping[str, Any],
        *,
        result: Optional[Mapping[str, Any]] = None,
        completed_at: Optional[str] = None,
    ) -> Dict[str, int]:
        """Fail an M2 refresh and close every still-active source attempt.

        Complete attempts remain immutable evidence.  No accepted schema,
        release inventory, collection publication, or watermark is advanced by
        this transaction.
        """

        self._ensure_refresh_schema()
        _validate_durable_strings(location="failed refresh", run_id=run_id)
        reject_sensitive_data(error, location="failed refresh error")
        if result is not None:
            reject_sensitive_data(result, location="failed refresh result")
        timestamp = _normalize_optional_text(
            completed_at,
            "completed_at",
            location="failed refresh",
        ) or utc_now_iso()
        error_json = canonical_json_bytes(dict(error)).decode("utf-8")
        result_json = (
            canonical_json_bytes(dict(result)).decode("utf-8")
            if result is not None
            else None
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "refresh run is unknown or has already been completed"
                )
            attempt_cursor = connection.execute(
                """
                UPDATE collection_attempts
                SET status = 'failed', completed_at = ?
                WHERE run_id = ? AND status = 'collecting'
                """,
                (timestamp, run_id),
            )
            source_cursor = connection.execute(
                """
                UPDATE refresh_run_sources SET status = 'failed'
                WHERE run_id = ?
                  AND status IN ('planned', 'collecting', 'collected')
                """,
                (run_id,),
            )
            run_cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'failed', completed_at = ?, error_json = ?,
                    result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (timestamp, error_json, result_json, run_id),
            )
            if run_cursor.rowcount != 1:
                raise ContractViolation("refresh run could not be failed")
        return {
            "failed_attempts": attempt_cursor.rowcount,
            "failed_sources": source_cursor.rowcount,
        }

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def plan_refresh_run(
        self,
        run_id: str,
        sources: Sequence[Mapping[str, Any]],
        *,
        config_fingerprint: str,
        pipeline_name: str = "m2_refresh",
        started_at: Optional[str] = None,
    ) -> Sequence[Dict[str, Any]]:
        """Create one immutable, exact source inventory for an M2 refresh run."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="refresh plan",
            run_id=run_id,
            pipeline_name=pipeline_name,
        )
        config_hash = _normalize_sha256(
            config_fingerprint, "config_fingerprint"
        )
        timestamp = _normalize_optional_text(
            started_at, "started_at", location="refresh plan"
        ) or utc_now_iso()
        if not isinstance(sources, Sequence) or isinstance(
            sources, (str, bytes, bytearray)
        ) or not sources:
            raise ContractViolation("refresh plan must contain at least one source")

        normalized: List[Dict[str, Any]] = []
        identities = set()
        dataset_identities = set()
        for planned_order, source_value in enumerate(sources):
            source = _require_exact_keys(
                source_value, _REFRESH_PLAN_KEYS, "refresh source plan"
            )
            reject_sensitive_data(source, location="refresh source plan")
            for field_name in (
                "source_id",
                "dataset_id",
                "parser_id",
                "parser_version",
            ):
                _validate_durable_strings(
                    location="refresh source plan",
                    **{field_name: source[field_name]},
                )
            source["scope_fingerprint"] = _normalize_scope_fingerprint(
                source["scope_fingerprint"]
            )
            source["contract_fingerprint"] = _normalize_sha256(
                source["contract_fingerprint"], "contract_fingerprint"
            )
            source["expected_schema_fingerprint"] = _normalize_sha256(
                source["expected_schema_fingerprint"],
                "expected_schema_fingerprint",
            )
            if not isinstance(source["required"], bool):
                raise ContractViolation("required must be a boolean")
            identity = (
                source["source_id"],
                source["dataset_id"],
                source["scope_fingerprint"],
            )
            dataset_identity = (source["source_id"], source["dataset_id"])
            if identity in identities:
                raise ContractViolation("refresh source plan contains a duplicate")
            if dataset_identity in dataset_identities:
                raise ContractViolation(
                    "one refresh run cannot plan multiple scopes for the same dataset"
                )
            identities.add(identity)
            dataset_identities.add(dataset_identity)
            source["planned_order"] = planned_order
            normalized.append(source)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone() is not None:
                raise ContractViolation(
                    "run_id already exists; refresh plans are immutable"
                )
            connection.execute(
                """
                INSERT INTO runs(
                    run_id, pipeline_name, status, started_at, config_fingerprint
                ) VALUES (?, ?, 'running', ?, ?)
                """,
                (run_id, pipeline_name, timestamp, config_hash),
            )
            for source in normalized:
                watermark_row = connection.execute(
                    """
                    SELECT watermark_value FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    (
                        source["source_id"],
                        source["dataset_id"],
                        source["scope_fingerprint"],
                    ),
                ).fetchone()
                starting_watermark = (
                    str(watermark_row["watermark_value"])
                    if watermark_row is not None
                    else None
                )
                connection.execute(
                    """
                    INSERT INTO refresh_run_sources(
                        run_id, source_id, dataset_id, scope_fingerprint,
                        parser_id, parser_version, contract_fingerprint,
                        expected_schema_fingerprint, required, planned_order,
                        starting_watermark, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned')
                    """,
                    (
                        run_id,
                        source["source_id"],
                        source["dataset_id"],
                        source["scope_fingerprint"],
                        source["parser_id"],
                        source["parser_version"],
                        source["contract_fingerprint"],
                        source["expected_schema_fingerprint"],
                        int(source["required"]),
                        source["planned_order"],
                        starting_watermark,
                    ),
                )
        return self.load_refresh_plan(run_id)

    def load_refresh_plan(self, run_id: str) -> Sequence[Dict[str, Any]]:
        """Return the immutable source plan in its declared order."""

        self._ensure_refresh_schema()
        _validate_durable_strings(location="refresh plan", run_id=run_id)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM refresh_run_sources
                WHERE run_id = ? ORDER BY planned_order ASC
                """,
                (run_id,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            value = dict(row)
            value["required"] = bool(value["required"])
            result.append(value)
        return result

    def start_collection_attempt(
        self,
        attempt_id: str,
        run_id: str,
        source_id: str,
        dataset_id: str,
        scope_fingerprint: str,
        *,
        request_fingerprint: str,
        request_plan_fingerprint: str,
        resume_policy: str,
        started_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Start a durable collection attempt bound to one planned source."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt",
            attempt_id=attempt_id,
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        request_hash = _normalize_sha256(
            request_fingerprint, "request_fingerprint"
        )
        request_plan_hash = _normalize_sha256(
            request_plan_fingerprint, "request_plan_fingerprint"
        )
        if resume_policy not in _RESUME_POLICIES:
            raise ContractViolation("resume_policy is invalid")
        if not isinstance(resume_policy, str):
            raise ContractViolation("resume_policy is invalid")
        reject_sensitive_data(resume_policy, location="collection resume policy")
        supplied_started_at = _normalize_optional_text(
            started_at, "started_at", location="collection attempt"
        )

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if existing is not None:
                expected = {
                    "run_id": run_id,
                    "source_id": source_id,
                    "dataset_id": dataset_id,
                    "scope_fingerprint": scope,
                    "request_fingerprint": request_hash,
                    "request_plan_fingerprint": request_plan_hash,
                    "resume_policy": resume_policy,
                }
                if any(existing[key] != value for key, value in expected.items()):
                    raise ContractViolation(
                        "attempt_id already exists with a different contract"
                    )
                if (
                    supplied_started_at is not None
                    and existing["started_at"] != supplied_started_at
                ):
                    raise ContractViolation(
                        "attempt_id already exists with a different start time"
                    )
                return self._collection_attempt_result(connection, existing)

            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "collection attempts require a running refresh run"
                )
            plan = connection.execute(
                """
                SELECT * FROM refresh_run_sources
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ?
                """,
                (run_id, source_id, dataset_id, scope),
            ).fetchone()
            if plan is None:
                raise ContractViolation(
                    "collection attempt is not in the exact refresh plan"
                )
            if plan["status"] not in {"planned", "collecting"}:
                raise ContractViolation(
                    "planned source is no longer available for collection"
                )
            active = connection.execute(
                """
                SELECT attempt_id FROM collection_attempts
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ? AND status = 'collecting'
                """,
                (run_id, source_id, dataset_id, scope),
            ).fetchone()
            if active is not None:
                raise ContractViolation(
                    "planned source already has an active collection attempt"
                )
            timestamp = supplied_started_at or utc_now_iso()
            connection.execute(
                """
                INSERT INTO collection_attempts(
                    attempt_id, run_id, source_id, dataset_id,
                    scope_fingerprint, request_fingerprint,
                    request_plan_fingerprint, parser_id, parser_version,
                    contract_fingerprint, resume_policy, status,
                    starting_watermark, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'collecting', ?, ?)
                """,
                (
                    attempt_id,
                    run_id,
                    source_id,
                    dataset_id,
                    scope,
                    request_hash,
                    request_plan_hash,
                    plan["parser_id"],
                    plan["parser_version"],
                    plan["contract_fingerprint"],
                    resume_policy,
                    plan["starting_watermark"],
                    timestamp,
                ),
            )
            connection.execute(
                """
                UPDATE refresh_run_sources SET status = 'collecting'
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ? AND status = 'planned'
                """,
                (run_id, source_id, dataset_id, scope),
            )
            created = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if created is None:
                raise ContractViolation("collection attempt could not be created")
            return self._collection_attempt_result(connection, created)

    @staticmethod
    def _collection_attempt_result(
        connection: sqlite3.Connection, attempt: sqlite3.Row
    ) -> Dict[str, Any]:
        result = dict(attempt)
        page_rows = connection.execute(
            """
            SELECT * FROM collection_attempt_pages
            WHERE attempt_id = ? ORDER BY page_index ASC
            """,
            (attempt["attempt_id"],),
        ).fetchall()
        pages: List[Dict[str, Any]] = []
        for page_row in page_rows:
            page = dict(page_row)
            page["page_reference"] = json.loads(
                page.pop("page_reference_json")
            )
            pages.append(page)
        result["pages"] = pages
        request_rows = connection.execute(
            """
            SELECT * FROM collection_attempt_page_requests
            WHERE attempt_id = ? ORDER BY page_index ASC
            """,
            (attempt["attempt_id"],),
        ).fetchall()
        result["page_requests"] = [dict(row) for row in request_rows]
        response_rows = connection.execute(
            """
            SELECT * FROM collection_attempt_page_responses
            WHERE attempt_id = ? ORDER BY page_index ASC
            """,
            (attempt["attempt_id"],),
        ).fetchall()
        result["page_responses"] = [dict(row) for row in response_rows]
        return result

    def load_collection_attempt(
        self, attempt_id: str
    ) -> Optional[Dict[str, Any]]:
        """Load an attempt and its ordered append-only page journal."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt", attempt_id=attempt_id
        )
        with self._connect() as connection:
            attempt = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None:
                return None
            return self._collection_attempt_result(connection, attempt)

    def load_active_collection_attempt(
        self,
        run_id: str,
        source_id: str,
        dataset_id: str,
        scope_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        """Discover the sole active attempt for a planned source identity."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt",
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        with self._connect() as connection:
            attempt = connection.execute(
                """
                SELECT * FROM collection_attempts
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ? AND status = 'collecting'
                """,
                (run_id, source_id, dataset_id, scope),
            ).fetchone()
            if attempt is None:
                return None
            return self._collection_attempt_result(connection, attempt)

    def load_latest_collection_attempt(
        self,
        run_id: str,
        source_id: str,
        dataset_id: str,
        scope_fingerprint: str,
        *,
        statuses: Sequence[str] = ("collecting", "complete"),
    ) -> Optional[Dict[str, Any]]:
        """Discover the latest resumable or already-completed source attempt."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt",
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        allowed_statuses = {"collecting", "complete", "abandoned", "failed"}
        if (
            not isinstance(statuses, Sequence)
            or isinstance(statuses, (str, bytes, bytearray))
            or not statuses
            or len(set(statuses)) != len(statuses)
            or any(status not in allowed_statuses for status in statuses)
        ):
            raise ContractViolation("collection attempt status filter is invalid")
        placeholders = ", ".join("?" for _ in statuses)
        with self._connect() as connection:
            attempt = connection.execute(
                """
                SELECT * FROM collection_attempts
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ? AND status IN ("""
                + placeholders
                + ") ORDER BY started_at DESC, attempt_id DESC LIMIT 1",
                (run_id, source_id, dataset_id, scope, *statuses),
            ).fetchone()
            if attempt is None:
                return None
            return self._collection_attempt_result(connection, attempt)

    def begin_collection_attempt_page_request(
        self,
        attempt_id: str,
        page_index: int,
        *,
        request_resume_token: Optional[str],
        request_fingerprint: str,
        recorded_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Durably bind the next logical request before any network I/O.

        The immutable intent closes the process-crash window between fetching
        a response, fsyncing its raw bytes, and appending the decoded page
        journal.  A request without a matching journal page is never resumed
        in the same attempt generation.
        """

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection page request", attempt_id=attempt_id
        )
        if (
            isinstance(page_index, bool)
            or not isinstance(page_index, int)
            or page_index < 0
        ):
            raise ContractViolation("page_index must be a non-negative integer")
        request_token = _normalize_optional_text(
            request_resume_token,
            "request_resume_token",
            location="collection page request",
        )
        request_hash = _normalize_sha256(
            request_fingerprint, "request_fingerprint"
        )
        timestamp = _normalize_optional_text(
            recorded_at,
            "recorded_at",
            location="collection page request",
        ) or utc_now_iso()

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                "SELECT status FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None or attempt["status"] != "collecting":
                raise ContractViolation(
                    "page requests require an active collection attempt"
                )
            existing = connection.execute(
                """
                SELECT * FROM collection_attempt_page_requests
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_resume_token"] != request_token
                    or existing["request_fingerprint"] != request_hash
                    or (
                        recorded_at is not None
                        and existing["recorded_at"] != timestamp
                    )
                ):
                    raise ContractViolation(
                        "page request index already has different immutable evidence"
                    )
                return dict(existing)

            request_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS request_count
                    FROM collection_attempt_page_requests
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["request_count"]
            )
            page_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS page_count FROM collection_attempt_pages
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["page_count"]
            )
            if page_index != request_count or page_index != page_count:
                raise ContractViolation(
                    "page requests must be recorded one at a time in contiguous order"
                )
            if page_index > 0:
                previous = connection.execute(
                    """
                    SELECT next_resume_token FROM collection_attempt_pages
                    WHERE attempt_id = ? AND page_index = ?
                    """,
                    (attempt_id, page_index - 1),
                ).fetchone()
                if previous is None or previous["next_resume_token"] != request_token:
                    raise ContractViolation(
                        "page request does not continue the prior journal token"
                    )
            connection.execute(
                """
                INSERT INTO collection_attempt_page_requests(
                    attempt_id, page_index, request_resume_token,
                    request_fingerprint, recorded_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    page_index,
                    request_token,
                    request_hash,
                    timestamp,
                ),
            )
            stored = connection.execute(
                """
                SELECT * FROM collection_attempt_page_requests
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if stored is None:
                raise ContractViolation("collection page request write failed")
            return dict(stored)

    def load_pending_collection_attempt_page_request(
        self, attempt_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return the sole request intent that has no durable page journal."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection page request", attempt_id=attempt_id
        )
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT request.*
                FROM collection_attempt_page_requests AS request
                LEFT JOIN collection_attempt_pages AS page
                  ON page.attempt_id = request.attempt_id
                 AND page.page_index = request.page_index
                WHERE request.attempt_id = ? AND page.attempt_id IS NULL
                ORDER BY request.page_index ASC
                """,
                (attempt_id,),
            ).fetchall()
        if len(rows) > 1:
            raise ContractViolation(
                "collection attempt has multiple unjournaled page requests"
            )
        return dict(rows[0]) if rows else None

    def record_collection_attempt_page_response(
        self,
        attempt_id: str,
        page_index: int,
        *,
        request_fingerprint: str,
        object_hash: str,
        response_envelope_fingerprint: str,
        source_as_of: Optional[str],
        recorded_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bind a returned response before its raw filesystem write begins."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection page response", attempt_id=attempt_id
        )
        if (
            isinstance(page_index, bool)
            or not isinstance(page_index, int)
            or page_index < 0
        ):
            raise ContractViolation("page_index must be a non-negative integer")
        request_hash = _normalize_sha256(
            request_fingerprint, "request_fingerprint"
        )
        body_hash = _normalize_sha256(object_hash, "object_hash")
        envelope_hash = _normalize_sha256(
            response_envelope_fingerprint,
            "response_envelope_fingerprint",
        )
        source_timestamp = _normalize_optional_text(
            source_as_of,
            "source_as_of",
            location="collection page response",
        )
        timestamp = _normalize_optional_text(
            recorded_at,
            "recorded_at",
            location="collection page response",
        ) or utc_now_iso()

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                "SELECT status FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None or attempt["status"] != "collecting":
                raise ContractViolation(
                    "page responses require an active collection attempt"
                )
            request_intent = connection.execute(
                """
                SELECT request_fingerprint
                FROM collection_attempt_page_requests
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if (
                request_intent is None
                or request_intent["request_fingerprint"] != request_hash
            ):
                raise ContractViolation(
                    "page response does not match its pre-fetch request intent"
                )
            existing = connection.execute(
                """
                SELECT * FROM collection_attempt_page_responses
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if existing is not None:
                if (
                    existing["request_fingerprint"] != request_hash
                    or existing["object_hash"] != body_hash
                    or existing["response_envelope_fingerprint"]
                    != envelope_hash
                    or existing["source_as_of"] != source_timestamp
                    or (
                        recorded_at is not None
                        and existing["recorded_at"] != timestamp
                    )
                ):
                    raise ContractViolation(
                        "page response index already has different immutable evidence"
                    )
                return dict(existing)

            response_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS response_count
                    FROM collection_attempt_page_responses
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["response_count"]
            )
            page_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS page_count FROM collection_attempt_pages
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["page_count"]
            )
            if page_index != response_count or page_index != page_count:
                raise ContractViolation(
                    "page responses must be recorded one at a time in contiguous order"
                )
            connection.execute(
                """
                INSERT INTO collection_attempt_page_responses(
                    attempt_id, page_index, request_fingerprint, object_hash,
                    response_envelope_fingerprint, source_as_of, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    page_index,
                    request_hash,
                    body_hash,
                    envelope_hash,
                    source_timestamp,
                    timestamp,
                ),
            )
            stored = connection.execute(
                """
                SELECT * FROM collection_attempt_page_responses
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if stored is None:
                raise ContractViolation("collection page response write failed")
            return dict(stored)

    def append_collection_attempt_page(
        self,
        attempt_id: str,
        page_reference: Mapping[str, Any],
        *,
        request_resume_token: Optional[str] = None,
        next_resume_token: Optional[str] = None,
        page_watermark: Optional[str] = None,
        consistency_fingerprint: Optional[str] = None,
        response_envelope_fingerprint: str,
        recorded_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Append exactly the next verified raw-page reference to an attempt."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection page journal", attempt_id=attempt_id
        )
        reference = _require_exact_keys(
            page_reference,
            _PAGE_REFERENCE_KEYS,
            "collection page reference",
        )
        reject_sensitive_data(reference, location="collection page reference")
        page_index = reference["page_index"]
        if (
            isinstance(page_index, bool)
            or not isinstance(page_index, int)
            or page_index < 0
        ):
            raise ContractViolation("page_index must be a non-negative integer")
        reference["object_hash"] = _normalize_sha256(
            reference["object_hash"], "object_hash"
        )
        reference["request_fingerprint"] = _normalize_sha256(
            reference["request_fingerprint"], "request_fingerprint"
        )
        if reference["request_cursor_fingerprint"] is not None:
            reference["request_cursor_fingerprint"] = _normalize_sha256(
                reference["request_cursor_fingerprint"],
                "request_cursor_fingerprint",
            )
        for field_name in (
            "relative_path",
            "manifest_relative_path",
            "content_type",
            "retrieved_at",
        ):
            _validate_durable_strings(
                location="collection page reference",
                **{field_name: reference[field_name]},
            )
        status_code = reference["status_code"]
        if (
            isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or status_code < 100
            or status_code > 599
        ):
            raise ContractViolation("status_code must be an HTTP status integer")
        headers = reference["response_headers"]
        if not isinstance(headers, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in headers.items()
        ):
            raise ContractViolation("response_headers must be a string mapping")
        reference["response_headers"] = dict(sorted(headers.items()))
        reference["source_as_of"] = _normalize_optional_text(
            reference["source_as_of"],
            "source_as_of",
            location="collection page reference",
        )

        request_token = _normalize_optional_text(
            request_resume_token,
            "request_resume_token",
            location="collection page journal",
        )
        next_token = _normalize_optional_text(
            next_resume_token,
            "next_resume_token",
            location="collection page journal",
        )
        page_watermark_value = _normalize_optional_text(
            page_watermark,
            "page_watermark",
            location="collection page journal",
        )
        consistency_hash = (
            _normalize_sha256(
                consistency_fingerprint, "consistency_fingerprint"
            )
            if consistency_fingerprint is not None
            else None
        )
        response_envelope_hash = _normalize_sha256(
            response_envelope_fingerprint,
            "response_envelope_fingerprint",
        )
        timestamp = _normalize_optional_text(
            recorded_at,
            "recorded_at",
            location="collection page journal",
        ) or utc_now_iso()
        cursor_fingerprint = reference["request_cursor_fingerprint"]
        if cursor_fingerprint != _watermark_fingerprint(request_token):
            raise ContractViolation(
                "page cursor fingerprint does not match its resume token"
            )
        reference_json = canonical_json_bytes(reference).decode("utf-8")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None or attempt["status"] != "collecting":
                raise ContractViolation(
                    "pages can be appended only to an active collection attempt"
                )
            request_intent = connection.execute(
                """
                SELECT request_resume_token, request_fingerprint
                FROM collection_attempt_page_requests
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if request_intent is None:
                raise ContractViolation(
                    "page journal requires a durable pre-fetch request intent"
                )
            if (
                request_intent["request_resume_token"] != request_token
                or request_intent["request_fingerprint"]
                != reference["request_fingerprint"]
            ):
                raise ContractViolation(
                    "page journal does not match its pre-fetch request intent"
                )
            response_intent = connection.execute(
                """
                SELECT request_fingerprint, object_hash,
                       response_envelope_fingerprint
                FROM collection_attempt_page_responses
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if response_intent is None or (
                response_intent["request_fingerprint"]
                != reference["request_fingerprint"]
                or response_intent["object_hash"] != reference["object_hash"]
                or response_intent["response_envelope_fingerprint"]
                != response_envelope_hash
            ):
                raise ContractViolation(
                    "page journal does not match its pre-raw response intent"
                )
            existing = connection.execute(
                """
                SELECT * FROM collection_attempt_pages
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            expected_row = {
                "page_reference_json": reference_json,
                "request_resume_token": request_token,
                "next_resume_token": next_token,
                "page_watermark": page_watermark_value,
                "consistency_fingerprint": consistency_hash,
            }
            if existing is not None:
                if any(
                    existing[key] != value
                    for key, value in expected_row.items()
                ):
                    raise ContractViolation(
                        "page index already contains different journal evidence"
                    )
                return self._page_journal_result(existing)

            page_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS page_count FROM collection_attempt_pages
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["page_count"]
            )
            if page_index != page_count:
                raise ContractViolation(
                    "collection pages must be appended in contiguous order"
                )
            if page_index > 0:
                previous = connection.execute(
                    """
                    SELECT next_resume_token, consistency_fingerprint
                    FROM collection_attempt_pages
                    WHERE attempt_id = ? AND page_index = ?
                    """,
                    (attempt_id, page_index - 1),
                ).fetchone()
                if previous is None or previous["next_resume_token"] != request_token:
                    raise ContractViolation(
                        "resume token does not continue the prior journal page"
                    )
                if consistency_hash != previous["consistency_fingerprint"]:
                    raise ContractViolation(
                        "collection snapshot consistency fingerprint changed"
                    )
            connection.execute(
                """
                INSERT INTO collection_attempt_pages(
                    attempt_id, page_index, page_reference_json,
                    request_resume_token, next_resume_token, page_watermark,
                    consistency_fingerprint, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    page_index,
                    reference_json,
                    request_token,
                    next_token,
                    page_watermark_value,
                    consistency_hash,
                    timestamp,
                ),
            )
            stored = connection.execute(
                """
                SELECT * FROM collection_attempt_pages
                WHERE attempt_id = ? AND page_index = ?
                """,
                (attempt_id, page_index),
            ).fetchone()
            if stored is None:
                raise ContractViolation("collection page journal write failed")
            return self._page_journal_result(stored)

    @staticmethod
    def _page_journal_result(row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        result["page_reference"] = json.loads(
            result.pop("page_reference_json")
        )
        return result

    def complete_collection_attempt(
        self,
        attempt_id: str,
        *,
        collection_hash: str,
        ending_watermark: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Seal an attempt after its ordered raw collection was verified."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt", attempt_id=attempt_id
        )
        collection_digest = _normalize_sha256(
            collection_hash, "collection_hash"
        )
        watermark = _normalize_optional_text(
            ending_watermark,
            "ending_watermark",
            location="collection attempt",
        )
        supplied_completed_at = _normalize_optional_text(
            completed_at,
            "completed_at",
            location="collection attempt",
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None:
                raise ContractViolation("collection attempt is unknown")
            if attempt["status"] == "complete":
                if (
                    attempt["collection_hash"] != collection_digest
                    or attempt["ending_watermark"] != watermark
                    or (
                        supplied_completed_at is not None
                        and attempt["completed_at"] != supplied_completed_at
                    )
                ):
                    raise ContractViolation(
                        "completed attempt has a different immutable contract"
                    )
                return self._collection_attempt_result(connection, attempt)
            if attempt["status"] != "collecting":
                raise ContractViolation("collection attempt cannot be completed")
            page_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS page_count FROM collection_attempt_pages
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["page_count"]
            )
            if page_count == 0:
                raise ContractViolation(
                    "collection attempt cannot complete without page evidence"
                )
            request_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS request_count
                    FROM collection_attempt_page_requests
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["request_count"]
            )
            response_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) AS response_count
                    FROM collection_attempt_page_responses
                    WHERE attempt_id = ?
                    """,
                    (attempt_id,),
                ).fetchone()["response_count"]
            )
            if request_count != page_count or response_count != page_count:
                raise ContractViolation(
                    "collection attempt has an unjournaled page request or response"
                )
            timestamp = supplied_completed_at or utc_now_iso()
            connection.execute(
                """
                UPDATE collection_attempts
                SET status = 'complete', ending_watermark = ?,
                    collection_hash = ?, completed_at = ?
                WHERE attempt_id = ? AND status = 'collecting'
                """,
                (watermark, collection_digest, timestamp, attempt_id),
            )
            connection.execute(
                """
                UPDATE refresh_run_sources SET status = 'collected'
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ? AND status = 'collecting'
                """,
                (
                    attempt["run_id"],
                    attempt["source_id"],
                    attempt["dataset_id"],
                    attempt["scope_fingerprint"],
                ),
            )
            completed = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if completed is None:
                raise ContractViolation("collection attempt completion failed")
            return self._collection_attempt_result(connection, completed)

    def abandon_collection_attempt(
        self,
        attempt_id: str,
        *,
        abandoned_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Seal a non-resumable attempt before restarting it from page zero."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="collection attempt", attempt_id=attempt_id
        )
        supplied_timestamp = _normalize_optional_text(
            abandoned_at,
            "abandoned_at",
            location="collection attempt",
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            attempt = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if attempt is None:
                raise ContractViolation("collection attempt is unknown")
            if attempt["status"] == "abandoned":
                if (
                    supplied_timestamp is not None
                    and attempt["completed_at"] != supplied_timestamp
                ):
                    raise ContractViolation(
                        "abandoned attempt has a different immutable timestamp"
                    )
                return self._collection_attempt_result(connection, attempt)
            if attempt["status"] != "collecting":
                raise ContractViolation("collection attempt cannot be abandoned")
            connection.execute(
                """
                UPDATE collection_attempts
                SET status = 'abandoned', completed_at = ?
                WHERE attempt_id = ? AND status = 'collecting'
                """,
                (supplied_timestamp or utc_now_iso(), attempt_id),
            )
            abandoned = connection.execute(
                "SELECT * FROM collection_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if abandoned is None:
                raise ContractViolation("collection attempt abandonment failed")
            return self._collection_attempt_result(connection, abandoned)

    def set_watermark(
        self,
        source_id: str,
        dataset_id: str,
        value: str,
        run_id: Optional[str] = None,
        updated_at: Optional[str] = None,
        *,
        scope_fingerprint: str,
    ) -> None:
        _validate_durable_strings(
            location="watermark",
            source_id=source_id,
            dataset_id=dataset_id,
        )
        _validate_durable_strings(
            location="watermark",
            allow_none=True,
            run_id=run_id,
        )
        if not isinstance(value, str) or not value.strip():
            raise ContractViolation("watermark value must be non-blank")
        reject_sensitive_data(value, location="watermark")
        if updated_at is not None:
            reject_sensitive_data(updated_at, location="watermark update time")
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        timestamp = updated_at or utc_now_iso()
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT watermark_value FROM watermarks
                WHERE source_id = ? AND dataset_id = ? AND scope_fingerprint = ?
                """,
                (source_id, dataset_id, scope),
            ).fetchone()
            previous = (
                str(existing["watermark_value"])
                if existing is not None
                else None
            )
            connection.execute(
                """
                INSERT INTO watermarks(
                    source_id, dataset_id, scope_fingerprint,
                    watermark_value, updated_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, dataset_id, scope_fingerprint) DO UPDATE SET
                    watermark_value = excluded.watermark_value,
                    updated_at = excluded.updated_at,
                    run_id = excluded.run_id
                """,
                (source_id, dataset_id, scope, value, timestamp, run_id),
            )
            connection.execute(
                """
                INSERT INTO watermark_history(
                    source_id, dataset_id, scope_fingerprint, previous_watermark,
                    new_watermark, changed_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    dataset_id,
                    scope,
                    previous,
                    value,
                    timestamp,
                    run_id,
                ),
            )

    def get_watermark(
        self,
        source_id: str,
        dataset_id: str,
        *,
        scope_fingerprint: str,
    ) -> Optional[str]:
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT watermark_value FROM watermarks
                WHERE source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ?
                """,
                (source_id, dataset_id, scope),
            ).fetchone()
        return str(row["watermark_value"]) if row is not None else None

    def list_watermark_history(
        self,
        source_id: str,
        dataset_id: str,
        limit: Optional[int] = None,
        *,
        scope_fingerprint: str,
    ) -> Sequence[Dict[str, Any]]:
        """Return oldest-to-newest append-only watermark write history."""

        if limit is not None and (isinstance(limit, bool) or limit <= 0):
            raise ContractViolation("watermark history limit must be positive")
        scope_clause = " AND scope_fingerprint = ?"
        parameters_list: List[Any] = [
            source_id,
            dataset_id,
            _normalize_scope_fingerprint(scope_fingerprint),
        ]
        sql = (
            "SELECT * FROM watermark_history "
            "WHERE source_id = ? AND dataset_id = ?"
            + scope_clause
            + " ORDER BY history_id ASC"
        )
        parameters: Sequence[Any] = tuple(parameters_list)
        if limit is not None:
            # Select the newest N transitions, then restore chronological order.
            sql = (
                "SELECT * FROM (SELECT * FROM watermark_history "
                "WHERE source_id = ? AND dataset_id = ?"
                + scope_clause
                + " ORDER BY history_id DESC LIMIT ?) ORDER BY history_id ASC"
            )
            parameters = tuple([*parameters_list, limit])
        with self._connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [dict(row) for row in rows]

    def record_collection_manifest(
        self,
        run_id: str,
        source_id: str,
        dataset_id: str,
        collection_hash: str,
        manifest_relative_path: str,
        manifest_checksum: str,
        request_fingerprint: str,
        page_count: int,
        record_count: int,
        duplicate_count: int,
        parser_id: str,
        parser_version: str,
        *,
        scope_fingerprint: str,
        starting_watermark: Optional[str] = None,
        ending_watermark: Optional[str] = None,
        release_id: Optional[str] = None,
        status: str = "collected",
        recorded_at: Optional[str] = None,
    ) -> None:
        """Record one immutable collection summary for a source/dataset run."""

        required_strings = {
            "run_id": run_id,
            "source_id": source_id,
            "dataset_id": dataset_id,
            "collection_hash": collection_hash,
            "manifest_relative_path": manifest_relative_path,
            "manifest_checksum": manifest_checksum,
            "request_fingerprint": request_fingerprint,
            "parser_id": parser_id,
            "parser_version": parser_version,
            "status": status,
        }
        scope = _normalize_scope_fingerprint(scope_fingerprint)
        for field_name, value in required_strings.items():
            if not isinstance(value, str) or not value.strip():
                raise ContractViolation(field_name + " must be a non-blank string")
        for field_name, value in (
            ("collection_hash", collection_hash),
            ("manifest_checksum", manifest_checksum),
            ("request_fingerprint", request_fingerprint),
        ):
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ContractViolation(
                    field_name + " must be a lowercase SHA-256 digest"
                )
        for field_name, value in (
            ("page_count", page_count),
            ("record_count", record_count),
            ("duplicate_count", duplicate_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(field_name + " must be a non-negative integer")

        reject_sensitive_data(
            {
                **required_strings,
                "starting_watermark_value": starting_watermark,
                "ending_watermark_value": ending_watermark,
                "release_id_value": release_id,
                "recorded_at_value": recorded_at,
            },
            location="collection manifest",
        )

        with self._connect() as connection:
            if connection.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone() is None:
                raise ContractViolation(
                    "cannot record a collection manifest for an unknown run_id"
                )
            stored = connection.execute(
                """
                SELECT * FROM collection_manifests
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                """,
                (run_id, source_id, dataset_id),
            ).fetchone()
            timestamp = (
                recorded_at
                if recorded_at is not None
                else (
                    str(stored["recorded_at"])
                    if stored is not None
                    else utc_now_iso()
                )
            )
            values = (
                run_id,
                source_id,
                dataset_id,
                collection_hash,
                manifest_relative_path,
                manifest_checksum,
                request_fingerprint,
                scope,
                page_count,
                record_count,
                duplicate_count,
                starting_watermark,
                ending_watermark,
                parser_id,
                parser_version,
                release_id,
                status,
                timestamp,
            )
            if stored is None:
                connection.execute(
                    """
                    INSERT INTO collection_manifests(
                        run_id, source_id, dataset_id, collection_hash,
                        manifest_relative_path, manifest_checksum,
                        request_fingerprint, scope_fingerprint,
                        page_count, record_count,
                        duplicate_count, starting_watermark, ending_watermark,
                        parser_id, parser_version, release_id, status, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                stored = connection.execute(
                    """
                    SELECT * FROM collection_manifests
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                    """,
                    (run_id, source_id, dataset_id),
                ).fetchone()
            expected = {
                "run_id": run_id,
                "source_id": source_id,
                "dataset_id": dataset_id,
                "collection_hash": collection_hash,
                "manifest_relative_path": manifest_relative_path,
                "manifest_checksum": manifest_checksum,
                "request_fingerprint": request_fingerprint,
                "scope_fingerprint": scope,
                "page_count": page_count,
                "record_count": record_count,
                "duplicate_count": duplicate_count,
                "starting_watermark": starting_watermark,
                "ending_watermark": ending_watermark,
                "parser_id": parser_id,
                "parser_version": parser_version,
                "release_id": release_id,
                "status": status,
                "recorded_at": timestamp,
            }
            if stored is None or any(stored[key] != value for key, value in expected.items()):
                raise ContractViolation(
                    "run already has a different collection manifest contract"
                )

    def get_collection_manifest(
        self, run_id: str, source_id: str, dataset_id: str
    ) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM collection_manifests
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                """,
                (run_id, source_id, dataset_id),
            ).fetchone()
        return dict(row) if row is not None else None

    def list_collection_manifests(
        self,
        run_id: Optional[str] = None,
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> Sequence[Dict[str, Any]]:
        """List collection summaries using optional exact-match filters."""

        clauses = []
        parameters = []
        for column, value in (
            ("run_id", run_id),
            ("source_id", source_id),
            ("dataset_id", dataset_id),
        ):
            if value is not None:
                clauses.append(column + " = ?")
                parameters.append(value)
        sql = "SELECT * FROM collection_manifests"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY recorded_at ASC, run_id ASC, source_id ASC, dataset_id ASC"
        with self._connect() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [dict(row) for row in rows]

    def record_raw_object(self, raw_object: RawObject) -> None:
        reject_sensitive_data(
            raw_object.to_dict(), location="raw object catalog metadata"
        )
        _validate_durable_strings(
            location="raw object catalog metadata",
            source_id=raw_object.source_id,
            dataset_id=raw_object.dataset_id,
            run_id=raw_object.run_id,
            request_fingerprint=raw_object.request_fingerprint,
            retrieved_at=raw_object.retrieved_at,
            schema_version=raw_object.schema_version,
        )
        for field_name, value in (
            ("object_hash", raw_object.object_hash),
            ("schema_fingerprint", raw_object.schema_fingerprint),
            ("request_fingerprint", raw_object.request_fingerprint),
        ):
            if re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ContractViolation(
                    field_name + " must be a lowercase SHA-256 digest"
                )
        schema_json = canonical_json_bytes(raw_object.schema).decode("utf-8")
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO raw_objects(
                    source_id, dataset_id, object_hash, relative_path,
                    manifest_relative_path, byte_size, record_count,
                    schema_fingerprint, schema_json, schema_version,
                    first_run_id, first_retrieved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw_object.source_id,
                    raw_object.dataset_id,
                    raw_object.object_hash,
                    raw_object.relative_path,
                    raw_object.manifest_relative_path,
                    raw_object.byte_size,
                    raw_object.record_count,
                    raw_object.schema_fingerprint,
                    schema_json,
                    raw_object.schema_version,
                    raw_object.run_id,
                    raw_object.retrieved_at,
                ),
            )
            stored = connection.execute(
                """
                SELECT * FROM raw_objects
                WHERE source_id = ? AND dataset_id = ? AND object_hash = ?
                """,
                (
                    raw_object.source_id,
                    raw_object.dataset_id,
                    raw_object.object_hash,
                ),
            ).fetchone()
            if stored is None or (
                stored["relative_path"] != raw_object.relative_path
                or stored["manifest_relative_path"]
                != raw_object.manifest_relative_path
                or stored["byte_size"] != raw_object.byte_size
                or stored["record_count"] != raw_object.record_count
                or stored["schema_fingerprint"] != raw_object.schema_fingerprint
            ):
                raise ContractViolation(
                    "raw object hash is already registered with different metadata"
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO raw_ingestions(
                    run_id, source_id, dataset_id, request_fingerprint,
                    object_hash, retrieved_at, created_object
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw_object.run_id,
                    raw_object.source_id,
                    raw_object.dataset_id,
                    raw_object.request_fingerprint,
                    raw_object.object_hash,
                    raw_object.retrieved_at,
                    int(raw_object.created),
                ),
            )
            ingestion = connection.execute(
                """
                SELECT object_hash, retrieved_at FROM raw_ingestions
                WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                  AND request_fingerprint = ?
                """,
                (
                    raw_object.run_id,
                    raw_object.source_id,
                    raw_object.dataset_id,
                    raw_object.request_fingerprint,
                ),
            ).fetchone()
            # A process can stop after the exact body is fsynced but before the
            # page journal is appended.  Re-fetching that same logical request
            # in the same run may legitimately have a later retrieval time.
            # Preserve the first ingestion evidence and accept the retry only
            # when its exact response bytes have the same content hash.
            if (
                ingestion is None
                or ingestion["object_hash"] != raw_object.object_hash
            ):
                raise ContractViolation(
                    "raw request fingerprint is already linked to a different response"
                )

    def find_raw_for_request(
        self, source_id: str, dataset_id: str, request_fingerprint: str
    ) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT o.*, i.request_fingerprint, i.retrieved_at, i.run_id
                FROM raw_ingestions AS i
                JOIN raw_objects AS o
                  ON o.source_id = i.source_id
                 AND o.dataset_id = i.dataset_id
                 AND o.object_hash = i.object_hash
                WHERE i.source_id = ?
                  AND i.dataset_id = ?
                  AND i.request_fingerprint = ?
                ORDER BY i.ingestion_id DESC
                LIMIT 1
                """,
                (source_id, dataset_id, request_fingerprint),
            ).fetchone()
        return dict(row) if row is not None else None

    def get_raw_object(
        self, source_id: str, dataset_id: str, object_hash: str
    ) -> Optional[Dict[str, Any]]:
        """Return one immutable raw-object record for offline rebuilds."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM raw_objects
                WHERE source_id = ? AND dataset_id = ? AND object_hash = ?
                """,
                (source_id, dataset_id, object_hash),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["schema"] = json.loads(result.pop("schema_json"))
        return result

    def count_raw_objects(self) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM raw_objects").fetchone()
        return int(row["count"])

    def count_raw_ingestions(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM raw_ingestions"
            ).fetchone()
        return int(row["count"])

    def latest_schema(
        self, source_id: str, dataset_id: str
    ) -> Optional[Dict[str, Sequence[str]]]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT schema_json FROM schema_observations
                WHERE source_id = ? AND dataset_id = ? AND accepted = 1
                ORDER BY observation_id DESC
                LIMIT 1
                """,
                (source_id, dataset_id),
            ).fetchone()
        return json.loads(row["schema_json"]) if row is not None else None

    def observe_schema(
        self,
        source_id: str,
        dataset_id: str,
        schema: Mapping[str, Sequence[str]],
        run_id: str,
        observed_at: Optional[str] = None,
        accept_drift: bool = False,
    ) -> SchemaDrift:
        _validate_durable_strings(
            location="schema observation",
            source_id=source_id,
            dataset_id=dataset_id,
            run_id=run_id,
        )
        _validate_durable_strings(
            location="schema observation",
            allow_none=True,
            observed_at=observed_at,
        )
        reject_sensitive_data(schema, location="schema observation")
        if accept_drift:
            raise ContractViolation(
                "schema acceptance is allowed only inside a published run commit"
            )
        previous = self.latest_schema(source_id, dataset_id)
        drift = detect_schema_drift(previous, schema)
        schema_json = canonical_json_bytes(schema).decode("utf-8")
        drift_json = canonical_json_bytes(drift.to_dict()).decode("utf-8")
        with self._connect() as connection:
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "schema observations require a running run_id"
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO schema_observations(
                    source_id, dataset_id, schema_fingerprint, schema_json,
                    drift_json, observed_at, run_id, accepted
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    dataset_id,
                    schema_fingerprint(schema),
                    schema_json,
                    drift_json,
                    observed_at or utc_now_iso(),
                    run_id,
                    0,
                ),
            )
        return drift

    @staticmethod
    def _normalize_refresh_source_commit(
        source_value: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if not isinstance(source_value, Mapping):
            raise ContractViolation("refresh source commit shape is invalid")
        optional_origin_release_id = source_value.get("origin_release_id")
        compatible_source = dict(source_value)
        compatible_source.pop("origin_release_id", None)
        source = _require_exact_keys(
            compatible_source, _REFRESH_COMMIT_KEYS, "refresh source commit"
        )
        reject_sensitive_data(source, location="refresh source commit")
        for field_name in ("source_id", "dataset_id"):
            _validate_durable_strings(
                location="refresh source commit",
                **{field_name: source[field_name]},
            )
        source["scope_fingerprint"] = _normalize_scope_fingerprint(
            source["scope_fingerprint"]
        )
        source["contract_fingerprint"] = _normalize_sha256(
            source["contract_fingerprint"], "contract_fingerprint"
        )
        source["watermark"] = _normalize_optional_text(
            source["watermark"],
            "watermark",
            location="refresh source commit",
        )
        source["source_as_of"] = _normalize_optional_text(
            source["source_as_of"],
            "source_as_of",
            location="refresh source commit",
        )
        source["origin_release_id"] = _normalize_optional_text(
            optional_origin_release_id,
            "origin_release_id",
            location="refresh source commit",
        )

        manifest = _require_exact_keys(
            source["collection_manifest"],
            _PUBLISHED_COLLECTION_KEYS,
            "published collection manifest",
        )
        reject_sensitive_data(manifest, location="published collection manifest")
        for field_name in (
            "run_id",
            "source_id",
            "dataset_id",
            "manifest_relative_path",
            "parser_id",
            "parser_version",
            "release_id",
            "status",
        ):
            _validate_durable_strings(
                location="published collection manifest",
                **{field_name: manifest[field_name]},
            )
        for field_name in (
            "collection_hash",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
        ):
            manifest[field_name] = _normalize_sha256(
                manifest[field_name], field_name
            )
        for field_name in ("starting_watermark", "ending_watermark"):
            if manifest[field_name] is not None:
                manifest[field_name] = _normalize_sha256(
                    manifest[field_name], field_name
                )
        for field_name in ("page_count", "record_count", "duplicate_count"):
            value = manifest[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(
                    field_name + " must be a non-negative integer"
                )
        if manifest["status"] != "published":
            raise ContractViolation(
                "refresh collection status must be exactly published"
            )
        if (
            manifest["source_id"] != source["source_id"]
            or manifest["dataset_id"] != source["dataset_id"]
            or manifest["scope_fingerprint"] != source["scope_fingerprint"]
        ):
            raise ContractViolation(
                "refresh collection manifest identity does not match its source"
            )
        if manifest["ending_watermark"] != _watermark_fingerprint(
            source["watermark"]
        ):
            raise ContractViolation(
                "refresh collection ending watermark does not match its commit"
            )

        schema = source["schema"]
        if not isinstance(schema, Mapping):
            raise ContractViolation("refresh source schema must be a mapping")
        schema_value: Dict[str, Sequence[str]] = {}
        for field_name, types in schema.items():
            if not isinstance(field_name, str) or not field_name.strip():
                raise ContractViolation("schema field names must be non-blank")
            if (
                not isinstance(types, Sequence)
                or isinstance(types, (str, bytes, bytearray))
                or not types
                or any(not isinstance(item, str) or not item for item in types)
            ):
                raise ContractViolation(
                    "schema field types must be non-empty string sequences"
                )
            schema_value[field_name] = tuple(types)
        reject_sensitive_data(schema_value, location="refresh source schema")
        schema_hash = schema_fingerprint(schema_value)

        artifacts = source["artifacts"]
        if not isinstance(artifacts, Mapping) or not artifacts:
            raise ContractViolation(
                "refresh source artifacts must be a non-empty checksum mapping"
            )
        normalized_artifacts: Dict[str, str] = {}
        for artifact_name, checksum in artifacts.items():
            if not isinstance(artifact_name, str) or not artifact_name.strip():
                raise ContractViolation("artifact names must be non-blank strings")
            reject_sensitive_data(
                artifact_name, location="refresh source artifact name"
            )
            normalized_artifacts[artifact_name] = _normalize_sha256(
                checksum, "artifact checksum"
            )
        normalized_artifacts = dict(sorted(normalized_artifacts.items()))

        source["collection_manifest"] = manifest
        source["schema"] = schema_value
        source["artifacts"] = normalized_artifacts
        inventory_core = {
            "source_id": source["source_id"],
            "dataset_id": source["dataset_id"],
            "scope_fingerprint": source["scope_fingerprint"],
            "collection_hash": manifest["collection_hash"],
            "parser_id": manifest["parser_id"],
            "parser_version": manifest["parser_version"],
            "contract_fingerprint": source["contract_fingerprint"],
            "schema_fingerprint": schema_hash,
            "starting_watermark_fingerprint": manifest["starting_watermark"],
            "ending_watermark_fingerprint": manifest["ending_watermark"],
            "source_as_of": source["source_as_of"],
            "artifacts": normalized_artifacts,
            "origin_release_id": source["origin_release_id"],
        }
        return source, inventory_core

    @staticmethod
    def _inventory_fingerprint_from_cores(
        inventory_cores: Sequence[Mapping[str, Any]],
    ) -> str:
        ordered = sorted(
            (dict(core) for core in inventory_cores),
            key=lambda core: (
                core["source_id"],
                core["dataset_id"],
                core["scope_fingerprint"],
            ),
        )
        return sha256_bytes(
            canonical_json_bytes(
                {
                    "contract": "m2-refresh-inventory-v1",
                    "sources": ordered,
                }
            )
        )

    @classmethod
    def refresh_inventory_fingerprint(
        cls, source_commits: Sequence[Mapping[str, Any]]
    ) -> str:
        """Compute the exact inventory digest embedded in release validation."""

        if not isinstance(source_commits, Sequence) or isinstance(
            source_commits, (str, bytes, bytearray)
        ) or not source_commits:
            raise ContractViolation(
                "refresh commit must contain at least one source"
            )
        cores: List[Dict[str, Any]] = []
        identities = set()
        for source_value in source_commits:
            _, core = cls._normalize_refresh_source_commit(source_value)
            identity = (
                core["source_id"],
                core["dataset_id"],
                core["scope_fingerprint"],
            )
            if identity in identities:
                raise ContractViolation(
                    "refresh commit contains a duplicate source identity"
                )
            identities.add(identity)
            cores.append(core)
        return cls._inventory_fingerprint_from_cores(cores)

    @staticmethod
    def _validated_inventory_fingerprint(
        validation: Mapping[str, Any]
    ) -> str:
        candidates: List[Any] = []
        if "inventory_fingerprint" in validation:
            candidates.append(validation["inventory_fingerprint"])
        for evidence in validation.values():
            if isinstance(evidence, Mapping) and "inventory_fingerprint" in evidence:
                candidates.append(evidence["inventory_fingerprint"])
        if not candidates:
            raise ContractViolation(
                "release validation is missing inventory fingerprint evidence"
            )
        normalized = {
            _normalize_sha256(value, "validation inventory_fingerprint")
            for value in candidates
        }
        if len(normalized) != 1:
            raise ContractViolation(
                "release validation contains conflicting inventory fingerprints"
            )
        return next(iter(normalized))

    def commit_refresh_run(
        self,
        *,
        run_id: str,
        release: ReleasePointer,
        validation: Mapping[str, Any],
        result: Mapping[str, Any],
        source_commits: Sequence[Mapping[str, Any]],
        inventory_fingerprint: str,
        completed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atomically publish the exact planned multi-source refresh inventory."""

        self._ensure_refresh_schema()
        _validate_durable_strings(location="refresh commit", run_id=run_id)
        reject_sensitive_data(release.to_dict(), location="release pointer")
        reject_sensitive_data(validation, location="release validation")
        reject_sensitive_data(result, location="refresh run result")
        expected_inventory_hash = _normalize_sha256(
            inventory_fingerprint, "inventory_fingerprint"
        )
        if (
            _normalize_sha256(
                release.manifest_checksum, "release manifest checksum"
            )
            != release.manifest_checksum
        ):
            raise ContractViolation("release manifest checksum is invalid")
        if self._validated_inventory_fingerprint(validation) != expected_inventory_hash:
            raise ContractViolation(
                "release validation inventory does not match the commit"
            )
        timestamp = _normalize_optional_text(
            completed_at,
            "completed_at",
            location="refresh commit",
        ) or utc_now_iso()
        if not isinstance(source_commits, Sequence) or isinstance(
            source_commits, (str, bytes, bytearray)
        ) or not source_commits:
            raise ContractViolation(
                "refresh commit must contain at least one source"
            )

        normalized_sources: List[Dict[str, Any]] = []
        inventory_cores: List[Dict[str, Any]] = []
        identities = set()
        for source_value in source_commits:
            source, core = self._normalize_refresh_source_commit(source_value)
            manifest = source["collection_manifest"]
            if manifest["run_id"] != run_id:
                raise ContractViolation(
                    "refresh collection manifest does not match its run"
                )
            if manifest["release_id"] != release.release_id:
                raise ContractViolation(
                    "refresh collection manifest does not match its release"
                )
            identity = (
                source["source_id"],
                source["dataset_id"],
                source["scope_fingerprint"],
            )
            if identity in identities:
                raise ContractViolation(
                    "refresh commit contains a duplicate source identity"
                )
            identities.add(identity)
            normalized_sources.append(source)
            inventory_cores.append(core)
        computed_inventory_hash = self._inventory_fingerprint_from_cores(
            inventory_cores
        )
        if computed_inventory_hash != expected_inventory_hash:
            raise ContractViolation(
                "refresh source inventory fingerprint does not match"
            )

        validation_json = canonical_json_bytes(dict(validation)).decode("utf-8")
        result_json = canonical_json_bytes(dict(result)).decode("utf-8")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "refresh run is unknown or has already been completed"
                )
            release_row = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?",
                (release.release_id,),
            ).fetchone()
            if (
                release_row is None
                or release_row["run_id"] != run_id
                or release_row["status"] != "candidate"
                or release_row["manifest_relative_path"]
                != release.manifest_relative_path
                or release_row["manifest_checksum"]
                != release.manifest_checksum
                or release_row["published_at"] != release.published_at
                or release_row["validation_json"] != validation_json
                or release_row["inventory_fingerprint"] is not None
            ):
                raise ContractViolation(
                    "bound release candidate does not match the refresh publish"
                )

            plan_rows = connection.execute(
                """
                SELECT * FROM refresh_run_sources
                WHERE run_id = ? ORDER BY planned_order ASC
                """,
                (run_id,),
            ).fetchall()
            planned_identities = {
                (
                    str(row["source_id"]),
                    str(row["dataset_id"]),
                    str(row["scope_fingerprint"]),
                )
                for row in plan_rows
            }
            if not plan_rows or planned_identities != identities:
                raise ContractViolation(
                    "refresh commit inventory is not the exact planned inventory"
                )
            plans = {
                (
                    str(row["source_id"]),
                    str(row["dataset_id"]),
                    str(row["scope_fingerprint"]),
                ): row
                for row in plan_rows
            }

            for source, inventory_core in zip(
                normalized_sources, inventory_cores
            ):
                manifest = source["collection_manifest"]
                identity = (
                    source["source_id"],
                    source["dataset_id"],
                    source["scope_fingerprint"],
                )
                plan = plans[identity]
                carried_from_prior = (
                    source["origin_release_id"] is not None
                    and plan["status"] == "planned"
                )
                if plan["status"] != "collected" and not carried_from_prior:
                    raise ContractViolation(
                        "every planned source must have a completed collection"
                    )
                if (
                    plan["parser_id"] != manifest["parser_id"]
                    or plan["parser_version"] != manifest["parser_version"]
                    or plan["contract_fingerprint"]
                    != source["contract_fingerprint"]
                    or plan["expected_schema_fingerprint"]
                    != inventory_core["schema_fingerprint"]
                ):
                    raise ContractViolation(
                        "refresh source does not match its planned parser/schema contract"
                    )

                current_watermark_row = connection.execute(
                    """
                    SELECT watermark_value FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    identity,
                ).fetchone()
                current_watermark = (
                    str(current_watermark_row["watermark_value"])
                    if current_watermark_row is not None
                    else None
                )
                if current_watermark != plan["starting_watermark"]:
                    raise ContractViolation(
                        "refresh watermark compare-and-swap precondition failed"
                    )
                if manifest["starting_watermark"] != _watermark_fingerprint(
                    plan["starting_watermark"]
                ):
                    raise ContractViolation(
                        "refresh collection does not start at its planned watermark"
                    )

                if carried_from_prior:
                    origin = connection.execute(
                        """
                        SELECT * FROM release_source_inventory
                        WHERE release_id = ? AND source_id = ? AND dataset_id = ?
                          AND scope_fingerprint = ?
                        """,
                        (
                            source["origin_release_id"],
                            source["source_id"],
                            source["dataset_id"],
                            source["scope_fingerprint"],
                        ),
                    ).fetchone()
                    if (
                        origin is None
                        or origin["collection_hash"] != manifest["collection_hash"]
                        or origin["parser_id"] != manifest["parser_id"]
                        or origin["parser_version"] != manifest["parser_version"]
                        or origin["contract_fingerprint"]
                        != source["contract_fingerprint"]
                        or origin["schema_fingerprint"]
                        != inventory_core["schema_fingerprint"]
                        or origin["source_as_of"] != source["source_as_of"]
                        or json.loads(str(origin["artifacts_json"]))
                        != source["artifacts"]
                    ):
                        raise ContractViolation(
                            "carried source does not match its origin release"
                        )
                    attempt = None
                else:
                    attempt = connection.execute(
                    """
                    SELECT * FROM collection_attempts
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND status = 'complete'
                      AND collection_hash = ?
                    ORDER BY completed_at DESC, attempt_id DESC LIMIT 1
                    """,
                    (
                        run_id,
                        source["source_id"],
                        source["dataset_id"],
                        source["scope_fingerprint"],
                        manifest["collection_hash"],
                    ),
                    ).fetchone()
                if not carried_from_prior and (
                    attempt is None
                    or attempt["request_fingerprint"]
                    != manifest["request_fingerprint"]
                    or attempt["parser_id"] != manifest["parser_id"]
                    or attempt["parser_version"] != manifest["parser_version"]
                    or attempt["contract_fingerprint"]
                    != source["contract_fingerprint"]
                    or attempt["starting_watermark"]
                    != plan["starting_watermark"]
                    or attempt["ending_watermark"] != source["watermark"]
                ):
                    raise ContractViolation(
                        "refresh collection is not bound to a completed attempt"
                    )
                page_count = manifest["page_count"] if carried_from_prior else int(
                    connection.execute(
                        """
                        SELECT COUNT(*) AS page_count
                        FROM collection_attempt_pages WHERE attempt_id = ?
                        """,
                        (attempt["attempt_id"],),
                    ).fetchone()["page_count"]
                )
                if page_count != manifest["page_count"]:
                    raise ContractViolation(
                        "refresh collection page count does not match its journal"
                    )

                previous_row = connection.execute(
                    """
                    SELECT schema_json FROM schema_observations
                    WHERE source_id = ? AND dataset_id = ? AND accepted = 1
                      AND parser_id = ? AND parser_version = ?
                      AND contract_fingerprint = ?
                    ORDER BY observation_id DESC LIMIT 1
                    """,
                    (
                        source["source_id"],
                        source["dataset_id"],
                        manifest["parser_id"],
                        manifest["parser_version"],
                        source["contract_fingerprint"],
                    ),
                ).fetchone()
                previous_schema = (
                    json.loads(previous_row["schema_json"])
                    if previous_row is not None
                    else None
                )
                drift = detect_schema_drift(previous_schema, source["schema"])
                if drift.has_drift:
                    raise ContractViolation(
                        "schema drift occurred without a parser/contract promotion"
                    )
                connection.execute(
                    """
                    INSERT INTO schema_observations(
                        source_id, dataset_id, schema_fingerprint, schema_json,
                        drift_json, observed_at, run_id, accepted,
                        parser_id, parser_version, contract_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                    ON CONFLICT(
                        run_id, source_id, dataset_id, schema_fingerprint
                    ) DO UPDATE SET
                        drift_json = excluded.drift_json,
                        observed_at = excluded.observed_at,
                        accepted = 1,
                        parser_id = excluded.parser_id,
                        parser_version = excluded.parser_version,
                        contract_fingerprint = excluded.contract_fingerprint
                    """,
                    (
                        source["source_id"],
                        source["dataset_id"],
                        inventory_core["schema_fingerprint"],
                        canonical_json_bytes(source["schema"]).decode("utf-8"),
                        canonical_json_bytes(drift.to_dict()).decode("utf-8"),
                        timestamp,
                        run_id,
                        manifest["parser_id"],
                        manifest["parser_version"],
                        source["contract_fingerprint"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO collection_manifests(
                        run_id, source_id, dataset_id, collection_hash,
                        manifest_relative_path, manifest_checksum,
                        request_fingerprint, scope_fingerprint,
                        page_count, record_count, duplicate_count,
                        starting_watermark, ending_watermark,
                        parser_id, parser_version, release_id, status, recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        source["source_id"],
                        source["dataset_id"],
                        manifest["collection_hash"],
                        manifest["manifest_relative_path"],
                        manifest["manifest_checksum"],
                        manifest["request_fingerprint"],
                        source["scope_fingerprint"],
                        manifest["page_count"],
                        manifest["record_count"],
                        manifest["duplicate_count"],
                        manifest["starting_watermark"],
                        manifest["ending_watermark"],
                        manifest["parser_id"],
                        manifest["parser_version"],
                        release.release_id,
                        "published",
                        timestamp,
                    ),
                )

                if source["watermark"] is not None and (
                    source["watermark"] != current_watermark
                ):
                    connection.execute(
                        """
                        INSERT INTO watermarks(
                            source_id, dataset_id, scope_fingerprint,
                            watermark_value, updated_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(
                            source_id, dataset_id, scope_fingerprint
                        ) DO UPDATE SET
                            watermark_value = excluded.watermark_value,
                            updated_at = excluded.updated_at,
                            run_id = excluded.run_id
                        """,
                        (
                            source["source_id"],
                            source["dataset_id"],
                            source["scope_fingerprint"],
                            source["watermark"],
                            timestamp,
                            run_id,
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO watermark_history(
                            source_id, dataset_id, scope_fingerprint,
                            previous_watermark, new_watermark, changed_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source["source_id"],
                            source["dataset_id"],
                            source["scope_fingerprint"],
                            current_watermark,
                            source["watermark"],
                            timestamp,
                            run_id,
                        ),
                    )

                entry_hash = sha256_bytes(
                    canonical_json_bytes(
                        {
                            "contract": "m2-refresh-inventory-entry-v1",
                            "source": inventory_core,
                        }
                    )
                )
                connection.execute(
                    """
                    INSERT INTO release_source_inventory(
                        release_id, run_id, source_id, dataset_id,
                        scope_fingerprint, collection_hash,
                        parser_id, parser_version, contract_fingerprint,
                        schema_fingerprint, starting_watermark_fingerprint,
                        ending_watermark_fingerprint, source_as_of,
                        artifacts_json, origin_release_id, entry_fingerprint,
                        recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        release.release_id,
                        run_id,
                        source["source_id"],
                        source["dataset_id"],
                        source["scope_fingerprint"],
                        manifest["collection_hash"],
                        manifest["parser_id"],
                        manifest["parser_version"],
                        source["contract_fingerprint"],
                        inventory_core["schema_fingerprint"],
                        manifest["starting_watermark"],
                        manifest["ending_watermark"],
                        source["source_as_of"],
                        canonical_json_bytes(source["artifacts"]).decode("utf-8"),
                        source["origin_release_id"],
                        entry_hash,
                        timestamp,
                    ),
                )
                expected_status = "planned" if carried_from_prior else "collected"
                status_cursor = connection.execute(
                    """
                    UPDATE refresh_run_sources SET status = 'published'
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND status = ?
                    """,
                    (
                        run_id,
                        source["source_id"],
                        source["dataset_id"],
                        source["scope_fingerprint"],
                        expected_status,
                    ),
                )
                if status_cursor.rowcount != 1:
                    raise ContractViolation(
                        "refresh source publication state changed during commit"
                    )

            release_cursor = connection.execute(
                """
                UPDATE releases
                SET status = 'published', inventory_fingerprint = ?,
                    manifest_relative_path = ?, manifest_checksum = ?,
                    validation_json = ?, published_at = ?, error_json = NULL
                WHERE release_id = ? AND run_id = ? AND status = 'candidate'
                  AND inventory_fingerprint IS NULL
                """,
                (
                    expected_inventory_hash,
                    release.manifest_relative_path,
                    release.manifest_checksum,
                    validation_json,
                    release.published_at,
                    release.release_id,
                    run_id,
                ),
            )
            if release_cursor.rowcount != 1:
                raise ContractViolation("refresh release could not be committed")
            run_cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'succeeded', completed_at = ?, error_json = NULL,
                    result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (timestamp, result_json, run_id),
            )
            if run_cursor.rowcount != 1:
                raise ContractViolation("refresh run could not be committed")
        return {
            "run_id": run_id,
            "release_id": release.release_id,
            "inventory_fingerprint": expected_inventory_hash,
            "source_count": len(normalized_sources),
        }

    def verify_refresh_commit(
        self,
        release_id: str,
        expected_inventory_fingerprint: Optional[str] = None,
    ) -> bool:
        """Verify exact catalog binding for a published multi-source refresh."""

        self._ensure_refresh_schema()
        _validate_durable_strings(
            location="refresh commit verification", release_id=release_id
        )
        expected_hash = (
            _normalize_sha256(
                expected_inventory_fingerprint,
                "expected_inventory_fingerprint",
            )
            if expected_inventory_fingerprint is not None
            else None
        )
        with self._connect() as connection:
            connection.execute("BEGIN")
            release = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?", (release_id,)
            ).fetchone()
            if release is None or release["status"] != "published":
                raise ContractViolation(
                    "refresh release is not durably published"
                )
            inventory_hash = _normalize_sha256(
                release["inventory_fingerprint"], "inventory_fingerprint"
            )
            if expected_hash is not None and inventory_hash != expected_hash:
                raise ContractViolation(
                    "published refresh inventory fingerprint is unexpected"
                )
            if release["validation_json"] is None:
                raise ContractViolation("published refresh validation is missing")
            validation = json.loads(str(release["validation_json"]))
            if (
                not isinstance(validation, Mapping)
                or self._validated_inventory_fingerprint(validation)
                != inventory_hash
            ):
                raise ContractViolation(
                    "published release validation does not bind its inventory"
                )
            run_id = str(release["run_id"])
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "succeeded":
                raise ContractViolation(
                    "published refresh is not bound to a succeeded run"
                )
            plan_rows = connection.execute(
                """
                SELECT * FROM refresh_run_sources
                WHERE run_id = ? ORDER BY planned_order ASC
                """,
                (run_id,),
            ).fetchall()
            inventory_rows = connection.execute(
                """
                SELECT * FROM release_source_inventory
                WHERE release_id = ? ORDER BY source_id, dataset_id,
                    scope_fingerprint
                """,
                (release_id,),
            ).fetchall()
            plan_identities = {
                (
                    str(row["source_id"]),
                    str(row["dataset_id"]),
                    str(row["scope_fingerprint"]),
                )
                for row in plan_rows
            }
            inventory_identities = {
                (
                    str(row["source_id"]),
                    str(row["dataset_id"]),
                    str(row["scope_fingerprint"]),
                )
                for row in inventory_rows
            }
            if (
                not plan_rows
                or len(inventory_rows) != len(plan_rows)
                or inventory_identities != plan_identities
            ):
                raise ContractViolation(
                    "published source inventory is not the exact refresh plan"
                )
            plans = {
                (
                    str(row["source_id"]),
                    str(row["dataset_id"]),
                    str(row["scope_fingerprint"]),
                ): row
                for row in plan_rows
            }

            inventory_cores: List[Dict[str, Any]] = []
            for inventory in inventory_rows:
                identity = (
                    str(inventory["source_id"]),
                    str(inventory["dataset_id"]),
                    str(inventory["scope_fingerprint"]),
                )
                plan = plans[identity]
                if (
                    inventory["run_id"] != run_id
                    or plan["status"] != "published"
                    or plan["parser_id"] != inventory["parser_id"]
                    or plan["parser_version"] != inventory["parser_version"]
                    or plan["contract_fingerprint"]
                    != inventory["contract_fingerprint"]
                    or plan["expected_schema_fingerprint"]
                    != inventory["schema_fingerprint"]
                    or _watermark_fingerprint(plan["starting_watermark"])
                    != inventory["starting_watermark_fingerprint"]
                ):
                    raise ContractViolation(
                        "published source inventory violates its plan"
                    )
                artifacts = json.loads(str(inventory["artifacts_json"]))
                if not isinstance(artifacts, Mapping) or not artifacts:
                    raise ContractViolation(
                        "published source artifact inventory is invalid"
                    )
                normalized_artifacts = {
                    str(name): _normalize_sha256(
                        checksum, "artifact checksum"
                    )
                    for name, checksum in artifacts.items()
                }
                inventory_core = {
                    "source_id": identity[0],
                    "dataset_id": identity[1],
                    "scope_fingerprint": identity[2],
                    "collection_hash": _normalize_sha256(
                        inventory["collection_hash"], "collection_hash"
                    ),
                    "parser_id": str(inventory["parser_id"]),
                    "parser_version": str(inventory["parser_version"]),
                    "contract_fingerprint": _normalize_sha256(
                        inventory["contract_fingerprint"],
                        "contract_fingerprint",
                    ),
                    "schema_fingerprint": _normalize_sha256(
                        inventory["schema_fingerprint"],
                        "schema_fingerprint",
                    ),
                    "starting_watermark_fingerprint": (
                        _normalize_sha256(
                            inventory["starting_watermark_fingerprint"],
                            "starting_watermark_fingerprint",
                        )
                        if inventory["starting_watermark_fingerprint"]
                        is not None
                        else None
                    ),
                    "ending_watermark_fingerprint": (
                        _normalize_sha256(
                            inventory["ending_watermark_fingerprint"],
                            "ending_watermark_fingerprint",
                        )
                        if inventory["ending_watermark_fingerprint"] is not None
                        else None
                    ),
                    "source_as_of": inventory["source_as_of"],
                    "artifacts": dict(sorted(normalized_artifacts.items())),
                    "origin_release_id": inventory["origin_release_id"],
                }
                expected_entry_hash = sha256_bytes(
                    canonical_json_bytes(
                        {
                            "contract": "m2-refresh-inventory-entry-v1",
                            "source": inventory_core,
                        }
                    )
                )
                if inventory["entry_fingerprint"] != expected_entry_hash:
                    raise ContractViolation(
                        "published source inventory entry fingerprint is invalid"
                    )

                collection = connection.execute(
                    """
                    SELECT * FROM collection_manifests
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                    """,
                    (run_id, identity[0], identity[1]),
                ).fetchone()
                if (
                    collection is None
                    or collection["release_id"] != release_id
                    or collection["status"] != "published"
                    or collection["scope_fingerprint"] != identity[2]
                    or collection["collection_hash"]
                    != inventory_core["collection_hash"]
                    or collection["parser_id"] != inventory_core["parser_id"]
                    or collection["parser_version"]
                    != inventory_core["parser_version"]
                    or collection["starting_watermark"]
                    != inventory_core["starting_watermark_fingerprint"]
                    or collection["ending_watermark"]
                    != inventory_core["ending_watermark_fingerprint"]
                ):
                    raise ContractViolation(
                        "published collection does not match source inventory"
                    )
                attempt = connection.execute(
                    """
                    SELECT * FROM collection_attempts
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND status = 'complete'
                      AND collection_hash = ?
                    ORDER BY completed_at DESC, attempt_id DESC LIMIT 1
                    """,
                    (
                        run_id,
                        identity[0],
                        identity[1],
                        identity[2],
                        inventory_core["collection_hash"],
                    ),
                ).fetchone()
                carried_inventory = (
                    inventory["origin_release_id"] is not None and attempt is None
                )
                if carried_inventory:
                    origin = connection.execute(
                        """
                        SELECT collection_hash, parser_id, parser_version,
                               contract_fingerprint, schema_fingerprint,
                               source_as_of, artifacts_json
                        FROM release_source_inventory
                        WHERE release_id = ? AND source_id = ? AND dataset_id = ?
                          AND scope_fingerprint = ?
                        """,
                        (
                            inventory["origin_release_id"],
                            identity[0],
                            identity[1],
                            identity[2],
                        ),
                    ).fetchone()
                    if (
                        origin is None
                        or origin["collection_hash"]
                        != inventory_core["collection_hash"]
                        or origin["parser_id"] != inventory_core["parser_id"]
                        or origin["parser_version"]
                        != inventory_core["parser_version"]
                        or origin["contract_fingerprint"]
                        != inventory_core["contract_fingerprint"]
                        or origin["schema_fingerprint"]
                        != inventory_core["schema_fingerprint"]
                        or origin["source_as_of"] != inventory_core["source_as_of"]
                        or json.loads(str(origin["artifacts_json"]))
                        != inventory_core["artifacts"]
                    ):
                        raise ContractViolation(
                            "published carried source origin binding is invalid"
                        )
                elif (
                    attempt is None
                    or attempt["request_fingerprint"]
                    != collection["request_fingerprint"]
                    or attempt["contract_fingerprint"]
                    != inventory_core["contract_fingerprint"]
                    or _watermark_fingerprint(attempt["ending_watermark"])
                    != inventory_core["ending_watermark_fingerprint"]
                ):
                    raise ContractViolation(
                        "published collection attempt binding is invalid"
                    )
                page_count = collection["page_count"] if carried_inventory else int(
                    connection.execute(
                        """
                        SELECT COUNT(*) AS page_count
                        FROM collection_attempt_pages WHERE attempt_id = ?
                        """,
                        (attempt["attempt_id"],),
                    ).fetchone()["page_count"]
                )
                if page_count != collection["page_count"]:
                    raise ContractViolation(
                        "published collection journal page count is invalid"
                    )
                schema_row = connection.execute(
                    """
                    SELECT schema_json FROM schema_observations
                    WHERE run_id = ? AND source_id = ? AND dataset_id = ?
                      AND schema_fingerprint = ? AND accepted = 1
                      AND parser_id = ? AND parser_version = ?
                      AND contract_fingerprint = ?
                    """,
                    (
                        run_id,
                        identity[0],
                        identity[1],
                        inventory_core["schema_fingerprint"],
                        inventory_core["parser_id"],
                        inventory_core["parser_version"],
                        inventory_core["contract_fingerprint"],
                    ),
                ).fetchone()
                if schema_row is None or schema_fingerprint(
                    json.loads(str(schema_row["schema_json"]))
                ) != inventory_core["schema_fingerprint"]:
                    raise ContractViolation(
                        "published schema promotion binding is invalid"
                    )
                inventory_cores.append(inventory_core)

            if (
                self._inventory_fingerprint_from_cores(inventory_cores)
                != inventory_hash
            ):
                raise ContractViolation(
                    "published release inventory fingerprint is invalid"
                )
        return True

    def commit_published_run(
        self,
        *,
        run_id: str,
        source_id: str,
        dataset_id: str,
        schema: Mapping[str, Sequence[str]],
        collection_manifest: Mapping[str, Any],
        release: ReleasePointer,
        validation: Mapping[str, Any],
        result: Mapping[str, Any],
        watermark: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> SchemaDrift:
        """Commit all publish metadata in one SQLite transaction.

        ``ReleaseManager`` calls this only after replacing ``current.json``;
        any exception rolls this transaction back and causes the manager to
        restore the previous pointer.  Consequently a run cannot be recorded
        as failed while exposing its release as current.
        """

        _validate_durable_strings(
            location="published run",
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        reject_sensitive_data(release.to_dict(), location="release pointer")
        reject_sensitive_data(validation, location="release validation")
        reject_sensitive_data(result, location="run result")
        reject_sensitive_data(schema, location="accepted schema")
        if watermark is not None and (
            not isinstance(watermark, str) or not watermark.strip()
        ):
            raise ContractViolation("watermark value must be non-blank")
        if watermark is not None:
            reject_sensitive_data(watermark, location="watermark")
        if completed_at is not None:
            reject_sensitive_data(completed_at, location="publish completion time")

        required_collection = {
            "run_id",
            "source_id",
            "dataset_id",
            "collection_hash",
            "manifest_relative_path",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
            "page_count",
            "record_count",
            "duplicate_count",
            "starting_watermark",
            "ending_watermark",
            "parser_id",
            "parser_version",
            "release_id",
            "status",
        }
        if set(collection_manifest) != required_collection:
            raise ContractViolation("published collection manifest shape is invalid")
        manifest = dict(collection_manifest)
        reject_sensitive_data(manifest, location="published collection manifest")
        if (
            manifest["run_id"] != run_id
            or manifest["source_id"] != source_id
            or manifest["dataset_id"] != dataset_id
            or manifest["release_id"] != release.release_id
        ):
            raise ContractViolation(
                "published collection manifest does not match the release run"
            )
        if manifest["status"] not in {"published", "published_rebuild"}:
            raise ContractViolation("published collection status is invalid")
        for field_name in (
            "run_id",
            "source_id",
            "dataset_id",
            "manifest_relative_path",
            "parser_id",
            "parser_version",
            "release_id",
            "status",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or not value.strip():
                raise ContractViolation(field_name + " must be a non-blank string")
        for field_name in (
            "collection_hash",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or re.fullmatch(
                r"[0-9a-f]{64}", value
            ) is None:
                raise ContractViolation(
                    field_name + " must be a lowercase SHA-256 digest"
                )
        for field_name in ("page_count", "record_count", "duplicate_count"):
            value = manifest[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(
                    field_name + " must be a non-negative integer"
                )
        watermark_scope = _normalize_scope_fingerprint(
            manifest["scope_fingerprint"]
        )
        expected_ending_watermark = (
            sha256_bytes(watermark.encode("utf-8"))
            if watermark is not None
            else None
        )
        if manifest["ending_watermark"] != expected_ending_watermark:
            raise ContractViolation(
                "published collection watermark does not match its scope commit"
            )

        timestamp = completed_at or utc_now_iso()
        schema_json = canonical_json_bytes(schema).decode("utf-8")
        schema_hash = schema_fingerprint(schema)
        validation_json = canonical_json_bytes(dict(validation)).decode("utf-8")
        result_json = canonical_json_bytes(dict(result)).decode("utf-8")
        with self._connect() as connection:
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "publish run is unknown or has already been completed"
                )
            if manifest["status"] == "published":
                current_watermark = connection.execute(
                    """
                    SELECT watermark_value FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    (source_id, dataset_id, watermark_scope),
                ).fetchone()
                expected_starting_watermark = (
                    sha256_bytes(
                        str(current_watermark["watermark_value"]).encode("utf-8")
                    )
                    if current_watermark is not None
                    else None
                )
                if manifest["starting_watermark"] != expected_starting_watermark:
                    raise ContractViolation(
                        "published collection does not start at the durable scope watermark"
                    )
            release_row = connection.execute(
                """
                SELECT run_id, status, manifest_relative_path,
                       manifest_checksum, published_at, validation_json
                FROM releases WHERE release_id = ?
                """,
                (release.release_id,),
            ).fetchone()
            if (
                release_row is None
                or release_row["run_id"] != run_id
                or release_row["status"] != "candidate"
                or release_row["manifest_relative_path"]
                != release.manifest_relative_path
                or release_row["manifest_checksum"]
                != release.manifest_checksum
                or release_row["published_at"] != release.published_at
                or release_row["validation_json"] != validation_json
            ):
                raise ContractViolation(
                    "bound release candidate does not match the running publish"
                )

            previous_row = connection.execute(
                """
                SELECT schema_json FROM schema_observations
                WHERE source_id = ? AND dataset_id = ? AND accepted = 1
                ORDER BY observation_id DESC LIMIT 1
                """,
                (source_id, dataset_id),
            ).fetchone()
            previous = (
                json.loads(previous_row["schema_json"])
                if previous_row is not None
                else None
            )
            drift = detect_schema_drift(previous, schema)
            if drift.has_drift:
                raise ContractViolation(
                    "schema changed between validation and publish commit"
                )
            connection.execute(
                """
                INSERT INTO schema_observations(
                    source_id, dataset_id, schema_fingerprint, schema_json,
                    drift_json, observed_at, run_id, accepted
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    source_id,
                    dataset_id,
                    schema_hash,
                    schema_json,
                    canonical_json_bytes(drift.to_dict()).decode("utf-8"),
                    timestamp,
                    run_id,
                ),
            )

            connection.execute(
                """
                INSERT INTO collection_manifests(
                    run_id, source_id, dataset_id, collection_hash,
                    manifest_relative_path, manifest_checksum,
                    request_fingerprint, scope_fingerprint,
                    page_count, record_count,
                    duplicate_count, starting_watermark, ending_watermark,
                    parser_id, parser_version, release_id, status, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest["run_id"],
                    manifest["source_id"],
                    manifest["dataset_id"],
                    manifest["collection_hash"],
                    manifest["manifest_relative_path"],
                    manifest["manifest_checksum"],
                    manifest["request_fingerprint"],
                    manifest["scope_fingerprint"],
                    manifest["page_count"],
                    manifest["record_count"],
                    manifest["duplicate_count"],
                    manifest["starting_watermark"],
                    manifest["ending_watermark"],
                    manifest["parser_id"],
                    manifest["parser_version"],
                    manifest["release_id"],
                    manifest["status"],
                    timestamp,
                ),
            )

            if watermark is not None:
                existing = connection.execute(
                    """
                    SELECT watermark_value FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    (source_id, dataset_id, watermark_scope),
                ).fetchone()
                previous_watermark = (
                    str(existing["watermark_value"])
                    if existing is not None
                    else None
                )
                if previous_watermark != watermark:
                    connection.execute(
                        """
                        INSERT INTO watermarks(
                            source_id, dataset_id, scope_fingerprint,
                            watermark_value, updated_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_id, dataset_id, scope_fingerprint) DO UPDATE SET
                            watermark_value = excluded.watermark_value,
                            updated_at = excluded.updated_at,
                            run_id = excluded.run_id
                        """,
                        (
                            source_id,
                            dataset_id,
                            watermark_scope,
                            watermark,
                            timestamp,
                            run_id,
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO watermark_history(
                            source_id, dataset_id, scope_fingerprint,
                            previous_watermark,
                            new_watermark, changed_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_id,
                            dataset_id,
                            watermark_scope,
                            previous_watermark,
                            watermark,
                            timestamp,
                            run_id,
                        ),
                    )

            release_cursor = connection.execute(
                """
                UPDATE releases
                SET status = 'published', manifest_relative_path = ?,
                    manifest_checksum = ?, validation_json = ?,
                    published_at = ?, error_json = NULL
                WHERE release_id = ? AND run_id = ? AND status = 'candidate'
                """,
                (
                    release.manifest_relative_path,
                    release.manifest_checksum,
                    validation_json,
                    release.published_at,
                    release.release_id,
                    run_id,
                ),
            )
            if release_cursor.rowcount != 1:
                raise ContractViolation("release candidate could not be committed")
            run_cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'succeeded', completed_at = ?, error_json = NULL,
                    result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (timestamp, result_json, run_id),
            )
            if run_cursor.rowcount != 1:
                raise ContractViolation("publish run could not be committed")
        return drift

    def commit_no_change_run(
        self,
        *,
        run_id: str,
        source_id: str,
        dataset_id: str,
        collection_manifest: Mapping[str, Any],
        result: Mapping[str, Any],
        watermark: Optional[str] = None,
        completed_at: Optional[str] = None,
    ) -> None:
        """Atomically commit a verified no-change collection and its resume point."""

        _validate_durable_strings(
            location="no-change run",
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        reject_sensitive_data(result, location="run result")
        if watermark is not None and (
            not isinstance(watermark, str) or not watermark.strip()
        ):
            raise ContractViolation("watermark value must be non-blank")
        if watermark is not None:
            reject_sensitive_data(watermark, location="watermark")
        if completed_at is not None:
            reject_sensitive_data(completed_at, location="no-change completion time")
        required_collection = {
            "run_id",
            "source_id",
            "dataset_id",
            "collection_hash",
            "manifest_relative_path",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
            "page_count",
            "record_count",
            "duplicate_count",
            "starting_watermark",
            "ending_watermark",
            "parser_id",
            "parser_version",
            "release_id",
            "status",
        }
        if set(collection_manifest) != required_collection:
            raise ContractViolation("no-change collection manifest shape is invalid")
        manifest = dict(collection_manifest)
        reject_sensitive_data(manifest, location="no-change collection manifest")
        if (
            manifest["run_id"] != run_id
            or manifest["source_id"] != source_id
            or manifest["dataset_id"] != dataset_id
            or manifest["release_id"] is not None
            or manifest["status"] != "no_change"
        ):
            raise ContractViolation(
                "no-change collection manifest does not match the run"
            )
        for field_name in (
            "run_id",
            "source_id",
            "dataset_id",
            "manifest_relative_path",
            "parser_id",
            "parser_version",
            "status",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or not value.strip():
                raise ContractViolation(field_name + " must be a non-blank string")
        for field_name in (
            "collection_hash",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or re.fullmatch(
                r"[0-9a-f]{64}", value
            ) is None:
                raise ContractViolation(
                    field_name + " must be a lowercase SHA-256 digest"
                )
        for field_name in ("page_count", "record_count", "duplicate_count"):
            value = manifest[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(
                    field_name + " must be a non-negative integer"
                )
        watermark_scope = _normalize_scope_fingerprint(
            manifest["scope_fingerprint"]
        )
        expected_ending_watermark = (
            sha256_bytes(watermark.encode("utf-8"))
            if watermark is not None
            else None
        )
        if manifest["ending_watermark"] != expected_ending_watermark:
            raise ContractViolation(
                "no-change collection watermark does not match its scope commit"
            )

        timestamp = completed_at or utc_now_iso()
        result_json = canonical_json_bytes(dict(result)).decode("utf-8")
        with self._connect() as connection:
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "no-change run is unknown or has already been completed"
                )
            current_watermark = connection.execute(
                """
                SELECT watermark_value FROM watermarks
                WHERE source_id = ? AND dataset_id = ?
                  AND scope_fingerprint = ?
                """,
                (source_id, dataset_id, watermark_scope),
            ).fetchone()
            expected_starting_watermark = (
                sha256_bytes(
                    str(current_watermark["watermark_value"]).encode("utf-8")
                )
                if current_watermark is not None
                else None
            )
            if manifest["starting_watermark"] != expected_starting_watermark:
                raise ContractViolation(
                    "no-change collection does not start at the durable scope watermark"
                )
            connection.execute(
                """
                INSERT INTO collection_manifests(
                    run_id, source_id, dataset_id, collection_hash,
                    manifest_relative_path, manifest_checksum,
                    request_fingerprint, scope_fingerprint,
                    page_count, record_count,
                    duplicate_count, starting_watermark, ending_watermark,
                    parser_id, parser_version, release_id, status, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest["run_id"],
                    manifest["source_id"],
                    manifest["dataset_id"],
                    manifest["collection_hash"],
                    manifest["manifest_relative_path"],
                    manifest["manifest_checksum"],
                    manifest["request_fingerprint"],
                    manifest["scope_fingerprint"],
                    manifest["page_count"],
                    manifest["record_count"],
                    manifest["duplicate_count"],
                    manifest["starting_watermark"],
                    manifest["ending_watermark"],
                    manifest["parser_id"],
                    manifest["parser_version"],
                    None,
                    "no_change",
                    timestamp,
                ),
            )
            if watermark is not None:
                existing = connection.execute(
                    """
                    SELECT watermark_value FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    (source_id, dataset_id, watermark_scope),
                ).fetchone()
                previous_watermark = (
                    str(existing["watermark_value"])
                    if existing is not None
                    else None
                )
                if previous_watermark != watermark:
                    connection.execute(
                        """
                        INSERT INTO watermarks(
                            source_id, dataset_id, scope_fingerprint,
                            watermark_value, updated_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_id, dataset_id, scope_fingerprint) DO UPDATE SET
                            watermark_value = excluded.watermark_value,
                            updated_at = excluded.updated_at,
                            run_id = excluded.run_id
                        """,
                        (
                            source_id,
                            dataset_id,
                            watermark_scope,
                            watermark,
                            timestamp,
                            run_id,
                        ),
                    )
                    connection.execute(
                        """
                        INSERT INTO watermark_history(
                            source_id, dataset_id, scope_fingerprint,
                            previous_watermark,
                            new_watermark, changed_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_id,
                            dataset_id,
                            watermark_scope,
                            previous_watermark,
                            watermark,
                            timestamp,
                            run_id,
                        ),
                    )
            cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'succeeded', completed_at = ?, error_json = NULL,
                    result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (timestamp, result_json, run_id),
            )
            if cursor.rowcount != 1:
                raise ContractViolation("no-change run could not be committed")

    def commit_rebuilt_run(
        self,
        *,
        run_id: str,
        source_id: str,
        dataset_id: str,
        collection_manifest: Mapping[str, Any],
        result: Mapping[str, Any],
        completed_at: Optional[str] = None,
    ) -> None:
        """Atomically record a non-published raw rebuild and finish its run."""

        _validate_durable_strings(
            location="rebuilt run",
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
        )
        reject_sensitive_data(result, location="rebuild run result")
        if completed_at is not None:
            reject_sensitive_data(completed_at, location="rebuild completion time")
        required_collection = {
            "run_id",
            "source_id",
            "dataset_id",
            "collection_hash",
            "manifest_relative_path",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
            "page_count",
            "record_count",
            "duplicate_count",
            "starting_watermark",
            "ending_watermark",
            "parser_id",
            "parser_version",
            "release_id",
            "status",
        }
        if set(collection_manifest) != required_collection:
            raise ContractViolation("rebuilt collection manifest shape is invalid")
        manifest = dict(collection_manifest)
        reject_sensitive_data(manifest, location="rebuilt collection manifest")
        if (
            manifest["run_id"] != run_id
            or manifest["source_id"] != source_id
            or manifest["dataset_id"] != dataset_id
            or manifest["release_id"] is not None
            or manifest["status"] != "rebuilt"
        ):
            raise ContractViolation(
                "rebuilt collection manifest does not match the run"
            )
        for field_name in (
            "run_id",
            "source_id",
            "dataset_id",
            "manifest_relative_path",
            "parser_id",
            "parser_version",
            "status",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or not value.strip():
                raise ContractViolation(field_name + " must be a non-blank string")
        for field_name in (
            "collection_hash",
            "manifest_checksum",
            "request_fingerprint",
            "scope_fingerprint",
        ):
            value = manifest[field_name]
            if not isinstance(value, str) or re.fullmatch(
                r"[0-9a-f]{64}", value
            ) is None:
                raise ContractViolation(
                    field_name + " must be a lowercase SHA-256 digest"
                )
        for field_name in ("page_count", "record_count", "duplicate_count"):
            value = manifest[field_name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ContractViolation(
                    field_name + " must be a non-negative integer"
                )

        timestamp = completed_at or utc_now_iso()
        result_json = canonical_json_bytes(dict(result)).decode("utf-8")
        with self._connect() as connection:
            run = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None or run["status"] != "running":
                raise ContractViolation(
                    "rebuild run is unknown or has already been completed"
                )
            connection.execute(
                """
                INSERT INTO collection_manifests(
                    run_id, source_id, dataset_id, collection_hash,
                    manifest_relative_path, manifest_checksum,
                    request_fingerprint, scope_fingerprint,
                    page_count, record_count, duplicate_count,
                    starting_watermark, ending_watermark,
                    parser_id, parser_version, release_id, status, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest["run_id"],
                    manifest["source_id"],
                    manifest["dataset_id"],
                    manifest["collection_hash"],
                    manifest["manifest_relative_path"],
                    manifest["manifest_checksum"],
                    manifest["request_fingerprint"],
                    manifest["scope_fingerprint"],
                    manifest["page_count"],
                    manifest["record_count"],
                    manifest["duplicate_count"],
                    manifest["starting_watermark"],
                    manifest["ending_watermark"],
                    manifest["parser_id"],
                    manifest["parser_version"],
                    None,
                    "rebuilt",
                    timestamp,
                ),
            )
            cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'succeeded', completed_at = ?, error_json = NULL,
                    result_json = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (timestamp, result_json, run_id),
            )
            if cursor.rowcount != 1:
                raise ContractViolation("rebuild run could not be committed")

    def observe_records_schema(
        self,
        source_id: str,
        dataset_id: str,
        records: Iterable[Mapping[str, Any]],
        run_id: str,
        observed_at: Optional[str] = None,
        accept_drift: bool = False,
    ) -> SchemaDrift:
        return self.observe_schema(
            source_id,
            dataset_id,
            infer_schema(records),
            run_id,
            observed_at,
            accept_drift,
        )

    def record_release_candidate(
        self, release_id: str, as_of_date: str, run_id: str
    ) -> None:
        reject_sensitive_data(release_id, location="release id")
        reject_sensitive_data(as_of_date, location="release as-of date")
        reject_sensitive_data(run_id, location="release run id")
        for field_name, value in (
            ("release_id", release_id),
            ("as_of_date", as_of_date),
            ("run_id", run_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ContractViolation(field_name + " must be a non-blank string")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?", (release_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["as_of_date"] != as_of_date
                    or existing["run_id"] != run_id
                ):
                    raise ContractViolation(
                        "release_id already exists with a different contract"
                    )
                return
            connection.execute(
                """
                INSERT INTO releases(
                    release_id, as_of_date, run_id, status, created_at
                ) VALUES (?, ?, ?, 'candidate', ?)
                """,
                (release_id, as_of_date, run_id, utc_now_iso()),
            )

    def bind_release_candidate(
        self,
        pointer: ReleasePointer,
        validation: Mapping[str, Any],
    ) -> None:
        """Bind a candidate to the exact pointer before callback execution."""

        reject_sensitive_data(pointer.to_dict(), location="release pointer")
        reject_sensitive_data(validation, location="release validation")
        if re.fullmatch(r"[0-9a-f]{64}", pointer.manifest_checksum) is None:
            raise ContractViolation(
                "release manifest checksum must be a lowercase SHA-256 digest"
            )
        validation_json = canonical_json_bytes(dict(validation)).decode("utf-8")
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?",
                (pointer.release_id,),
            ).fetchone()
            if existing is None or existing["status"] != "candidate":
                raise ContractViolation("release candidate cannot be bound")
            expected = {
                "manifest_relative_path": pointer.manifest_relative_path,
                "manifest_checksum": pointer.manifest_checksum,
                "published_at": pointer.published_at,
                "validation_json": validation_json,
            }
            current_values = {
                field_name: existing[field_name] for field_name in expected
            }
            if any(value is not None for value in current_values.values()):
                if current_values != expected:
                    raise ContractViolation(
                        "release candidate is already bound to another pointer"
                    )
                return
            connection.execute(
                """
                UPDATE releases
                SET manifest_relative_path = ?, manifest_checksum = ?,
                    published_at = ?, validation_json = ?
                WHERE release_id = ? AND status = 'candidate'
                """,
                (
                    pointer.manifest_relative_path,
                    pointer.manifest_checksum,
                    pointer.published_at,
                    validation_json,
                    pointer.release_id,
                ),
            )

    def mark_release_published(
        self,
        release_id: str,
        manifest_relative_path: str,
        manifest_checksum: str,
        validation: Mapping[str, Any],
        published_at: Optional[str] = None,
    ) -> None:
        reject_sensitive_data(release_id, location="release id")
        reject_sensitive_data(
            manifest_relative_path, location="release manifest path"
        )
        reject_sensitive_data(
            manifest_checksum, location="release manifest checksum"
        )
        if published_at is not None:
            reject_sensitive_data(published_at, location="release publish time")
        reject_sensitive_data(validation, location="release validation")
        if (
            not isinstance(manifest_relative_path, str)
            or not manifest_relative_path.strip()
        ):
            raise ContractViolation("release manifest path must be non-blank")
        if not isinstance(manifest_checksum, str) or re.fullmatch(
            r"[0-9a-f]{64}", manifest_checksum
        ) is None:
            raise ContractViolation(
                "release manifest checksum must be a lowercase SHA-256 digest"
            )
        validation_json = canonical_json_bytes(dict(validation)).decode("utf-8")
        effective_published_at = published_at or utc_now_iso()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?", (release_id,)
            ).fetchone()
            if existing is None or existing["status"] == "failed":
                raise ContractViolation("release is unknown or already failed")
            if existing["status"] == "published":
                immutable_contract = {
                    "manifest_relative_path": manifest_relative_path,
                    "manifest_checksum": manifest_checksum,
                    "validation_json": validation_json,
                    "published_at": effective_published_at,
                }
                if any(
                    existing[field_name] != expected
                    for field_name, expected in immutable_contract.items()
                ):
                    raise ContractViolation("a published release is immutable")
                return
            bound_contract = {
                "manifest_relative_path": manifest_relative_path,
                "manifest_checksum": manifest_checksum,
                "validation_json": validation_json,
                "published_at": effective_published_at,
            }
            existing_bound = {
                field_name: existing[field_name]
                for field_name in bound_contract
            }
            if any(value is not None for value in existing_bound.values()) and (
                existing_bound != bound_contract
            ):
                raise ContractViolation(
                    "release publication does not match its bound candidate"
                )
            cursor = connection.execute(
                """
                UPDATE releases
                SET status = 'published', manifest_relative_path = ?,
                    manifest_checksum = ?, validation_json = ?,
                    published_at = ?, error_json = NULL
                WHERE release_id = ? AND status = 'candidate'
                """,
                (
                    manifest_relative_path,
                    manifest_checksum,
                    validation_json,
                    effective_published_at,
                    release_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ContractViolation("release is unknown or already failed")

    def mark_release_failed(
        self, release_id: str, error: Mapping[str, Any]
    ) -> None:
        _validate_durable_strings(
            location="failed release", release_id=release_id
        )
        reject_sensitive_data(error, location="release error")
        error_json = canonical_json_bytes(dict(error)).decode("utf-8")
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE releases
                SET status = 'failed', error_json = ?
                WHERE release_id = ? AND status = 'candidate'
                """,
                (error_json, release_id),
            )
            if cursor.rowcount != 1:
                existing = connection.execute(
                    "SELECT status FROM releases WHERE release_id = ?", (release_id,)
                ).fetchone()
                if existing is None:
                    raise ContractViolation("cannot fail an unknown release")
                if existing["status"] == "published":
                    raise ContractViolation("a published release is immutable")

    def quarantine_invalid_publish_commit(
        self,
        release_id: str,
        run_id: str,
        error: Mapping[str, Any],
    ) -> None:
        """Atomically compensate a conclusively invalid callback publication.

        A callback may have completed ``commit_published_run`` before a final
        artifact verification proves that the release is invalid.  Merely
        failing the release and run would leave its accepted schema,
        collection publication, and scoped watermark visible.  Roll those
        effects back in the same transaction while retaining append-only
        watermark evidence of any reverse transition.
        """

        _validate_durable_strings(
            location="invalid publish compensation",
            release_id=release_id,
            run_id=run_id,
        )
        reject_sensitive_data(error, location="invalid publish error")
        error_json = canonical_json_bytes(dict(error)).decode("utf-8")
        result_json = canonical_json_bytes(
            {"status": "failed", "compensated_invalid_publish": True}
        ).decode("utf-8")
        timestamp = utc_now_iso()
        with self._connect() as connection:
            # Acquire the writer lock before inspecting side effects so a
            # concurrent catalog writer cannot change the watermark between
            # the provenance check and compensation.
            connection.execute("BEGIN IMMEDIATE")
            release_row = connection.execute(
                """
                SELECT status FROM releases
                WHERE release_id = ? AND run_id = ?
                """,
                (release_id, run_id),
            ).fetchone()
            run_row = connection.execute(
                "SELECT status FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if (
                release_row is None
                or release_row["status"] not in {"candidate", "published"}
                or (
                    run_row is not None
                    and run_row["status"]
                    not in {"running", "succeeded", "failed"}
                )
            ):
                raise ContractViolation(
                    "invalid publish compensation did not match a release run"
                )

            published_collections = connection.execute(
                """
                SELECT source_id, dataset_id, scope_fingerprint
                FROM collection_manifests
                WHERE run_id = ? AND release_id = ?
                  AND status IN ('published', 'published_rebuild')
                """,
                (run_id, release_id),
            ).fetchall()
            for collection in published_collections:
                source_id = str(collection["source_id"])
                dataset_id = str(collection["dataset_id"])
                scope_fingerprint = str(collection["scope_fingerprint"])
                current = connection.execute(
                    """
                    SELECT watermark_value, updated_at, run_id
                    FROM watermarks
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ?
                    """,
                    (source_id, dataset_id, scope_fingerprint),
                ).fetchone()
                # A no-change commit does not claim the watermark.  Likewise,
                # never overwrite a newer writer that has already advanced it.
                if current is None or current["run_id"] != run_id:
                    continue
                transition = connection.execute(
                    """
                    SELECT history_id, previous_watermark, new_watermark
                    FROM watermark_history
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND run_id = ?
                    ORDER BY history_id DESC LIMIT 1
                    """,
                    (source_id, dataset_id, scope_fingerprint, run_id),
                ).fetchone()
                if (
                    transition is None
                    or transition["new_watermark"]
                    != current["watermark_value"]
                ):
                    raise ContractViolation(
                        "invalid publish watermark provenance is inconsistent"
                    )

                previous_watermark = transition["previous_watermark"]
                if previous_watermark is None:
                    connection.execute(
                        """
                        DELETE FROM watermarks
                        WHERE source_id = ? AND dataset_id = ?
                          AND scope_fingerprint = ? AND run_id = ?
                          AND watermark_value = ?
                        """,
                        (
                            source_id,
                            dataset_id,
                            scope_fingerprint,
                            run_id,
                            current["watermark_value"],
                        ),
                    )
                    # The v4 history contract has a non-null new watermark,
                    # so the failed forward transition plus failed run is the
                    # durable audit evidence when restoring "no watermark".
                    continue

                previous_origin = connection.execute(
                    """
                    SELECT changed_at, run_id
                    FROM watermark_history
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND history_id < ?
                      AND new_watermark = ?
                    ORDER BY history_id DESC LIMIT 1
                    """,
                    (
                        source_id,
                        dataset_id,
                        scope_fingerprint,
                        transition["history_id"],
                        previous_watermark,
                    ),
                ).fetchone()
                restored_at = (
                    str(previous_origin["changed_at"])
                    if previous_origin is not None
                    else timestamp
                )
                restored_run_id = (
                    previous_origin["run_id"]
                    if previous_origin is not None
                    else None
                )
                watermark_cursor = connection.execute(
                    """
                    UPDATE watermarks
                    SET watermark_value = ?, updated_at = ?, run_id = ?
                    WHERE source_id = ? AND dataset_id = ?
                      AND scope_fingerprint = ? AND run_id = ?
                      AND watermark_value = ?
                    """,
                    (
                        previous_watermark,
                        restored_at,
                        restored_run_id,
                        source_id,
                        dataset_id,
                        scope_fingerprint,
                        run_id,
                        current["watermark_value"],
                    ),
                )
                if watermark_cursor.rowcount != 1:
                    raise ContractViolation(
                        "invalid publish watermark changed during compensation"
                    )
                connection.execute(
                    """
                    INSERT INTO watermark_history(
                        source_id, dataset_id, scope_fingerprint,
                        previous_watermark, new_watermark, changed_at, run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_id,
                        dataset_id,
                        scope_fingerprint,
                        current["watermark_value"],
                        previous_watermark,
                        timestamp,
                        run_id,
                    ),
                )

            connection.execute(
                """
                UPDATE schema_observations
                SET accepted = 0
                WHERE run_id = ? AND accepted = 1
                """,
                (run_id,),
            )
            connection.execute(
                """
                UPDATE collection_manifests
                SET status = 'failed', release_id = NULL
                WHERE run_id = ? AND release_id = ?
                  AND status IN ('published', 'published_rebuild')
                """,
                (run_id, release_id),
            )
            has_refresh_sources = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'refresh_run_sources'
                """
            ).fetchone()
            if has_refresh_sources is not None:
                connection.execute(
                    """
                    UPDATE refresh_run_sources SET status = 'failed'
                    WHERE run_id = ? AND status = 'published'
                    """,
                    (run_id,),
                )
            release_cursor = connection.execute(
                """
                UPDATE releases
                SET status = 'failed', error_json = ?
                WHERE release_id = ? AND run_id = ?
                  AND status IN ('candidate', 'published')
                """,
                (error_json, release_id, run_id),
            )
            if release_cursor.rowcount != 1:
                raise ContractViolation(
                    "invalid publish compensation did not match a release"
                )
            run_cursor = connection.execute(
                """
                UPDATE runs
                SET status = 'failed', completed_at = ?, error_json = ?,
                    result_json = ?
                WHERE run_id = ? AND status IN ('running', 'succeeded', 'failed')
                """,
                (timestamp, error_json, result_json, run_id),
            )
            if run_row is not None and run_cursor.rowcount != 1:
                raise ContractViolation(
                    "invalid publish compensation did not match a running run"
                )

    def get_release(self, release_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM releases WHERE release_id = ?", (release_id,)
            ).fetchone()
        return dict(row) if row is not None else None
