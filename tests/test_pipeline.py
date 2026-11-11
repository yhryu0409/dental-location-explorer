import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from dental_market.config import Settings
from dental_market.connectors import (
    BaseConnector,
    ConnectorError,
    FixtureConnector,
    Page,
    RawResponse,
    Request,
)
from dental_market.pipeline import (
    DentalMarketPipeline,
    PipelineError,
    SAMPLE_DATASET_ID,
    SAMPLE_SOURCE_ID,
    SchemaDriftBlocked,
    load_jsonl_fixture,
    run_sample,
)
from dental_market.contracts import ContractViolation, ReleaseValidationError
from dental_market.parsers import ParserError
from dental_market.parsers import ParserRegistry
from dental_market.raw_collection import RawPagePayload
from dental_market.security import SensitiveDataError


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        data_root=tmp_path / "runtime",
        rate_limit_per_second=1_000_000,
        max_retries=1,
        retry_base_seconds=0.001,
    )


def test_gate_scenarios_are_reproducible_and_last_good_is_safe(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)

    first = run_sample(
        settings, as_of_date="2026-09-01", run_id="test-first"
    )
    assert first.status == "published"
    assert first.collected_records == 6
    assert first.raw_collection_hash is not None
    assert len(first.raw_object_hashes) == 3
    assert first.artifacts is not None
    first_pointer = pipeline.release_manager.current()
    first_manifest = pipeline.catalog.get_collection_manifest(
        "test-first", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert first_manifest is not None
    assert first_manifest["status"] == "published"
    assert first_manifest["page_count"] == 3
    assert first_manifest["record_count"] == 6

    second = run_sample(
        settings, as_of_date="2026-09-01", run_id="test-second"
    )
    assert second.status == "no_change"
    assert second.collected_records == 0
    assert second.raw_duplicates_dropped == 0
    assert second.raw_collection_hash is not None
    assert len(second.raw_object_hashes) == 1
    assert pipeline.release_manager.current() == first_pointer
    second_manifest = pipeline.catalog.get_collection_manifest(
        "test-second", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert second_manifest is not None
    assert second_manifest["status"] == "no_change"
    assert second_manifest["record_count"] == 0

    recovery_settings = Settings(
        data_root=tmp_path / "raw-only-recovery",
        rate_limit_per_second=1_000_000,
        max_retries=1,
        retry_base_seconds=0.001,
    )
    shutil.copytree(
        settings.data_root / "raw", recovery_settings.data_root / "raw"
    )
    recovery_pipeline = DentalMarketPipeline(recovery_settings)
    assert recovery_pipeline.catalog.count_raw_objects() == 0
    rebuilt = recovery_pipeline.rebuild_from_raw(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        first.raw_collection_hash,
        as_of_date="2026-09-01",
        run_id="test-rebuild",
    )
    assert rebuilt.artifacts is not None
    assert rebuilt.artifacts.staging.checksum == first.artifacts.staging.checksum
    assert rebuilt.artifacts.core.checksum == first.artifacts.core.checksum
    assert rebuilt.artifacts.mart.checksum == first.artifacts.mart.checksum
    assert recovery_pipeline.catalog.count_raw_objects() == 0

    watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=first.scope_fingerprint,
    )
    with pytest.raises(ConnectorError):
        run_sample(
            settings,
            as_of_date="2026-09-01",
            run_id="test-api-failure",
            transient_failures={None: 10},
        )
    assert pipeline.release_manager.current() == first_pointer
    assert (
        pipeline.catalog.get_watermark(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            scope_fingerprint=first.scope_fingerprint,
        )
        == watermark
    )
    assert pipeline.catalog.get_run("test-api-failure")["status"] == "failed"


def test_schema_drift_is_stored_but_not_released(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    first = run_sample(
        settings, as_of_date="2026-09-01", run_id="schema-baseline"
    )
    first_pointer = pipeline.release_manager.current()
    rows = load_jsonl_fixture()
    rows[0] = {**rows[0], "unexpected_upstream_field": "changed"}
    connector = FixtureConnector(
        rows,
        page_size=2,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    with pytest.raises(SchemaDriftBlocked):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                page_size=2,
            ),
            as_of_date="2026-09-01",
            run_id="schema-drift",
        )

    with pytest.raises(SchemaDriftBlocked):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                page_size=2,
            ),
            as_of_date="2026-09-01",
            run_id="schema-drift-repeated",
        )

    drift_manifest = pipeline.catalog.get_collection_manifest(
        "schema-drift", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert drift_manifest is not None
    with pytest.raises(SchemaDriftBlocked):
        pipeline.rebuild_from_raw(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            drift_manifest["collection_hash"],
            as_of_date="2026-09-01",
            run_id="schema-drift-rebuild-bypass",
            publish=True,
        )

    drift_manifests = pipeline.catalog.list_collection_manifests(
        source_id=SAMPLE_SOURCE_ID, dataset_id=SAMPLE_DATASET_ID
    )
    statuses = [row["status"] for row in drift_manifests]
    assert statuses.count("published") == 1
    assert statuses.count("failed") == 2
    assert statuses.count("failed_rebuild") == 1
    assert pipeline.release_manager.current() == first_pointer
    assert pipeline.catalog.get_run("schema-drift")["status"] == "failed"
    assert pipeline.catalog.get_run("schema-drift-repeated")["status"] == "failed"
    assert first.status == "published"


def test_failed_run_catalog_redacts_connector_credentials(tmp_path: Path) -> None:
    class SecretFailConnector(BaseConnector):
        source = "secret_fixture"
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            raise ConnectorError(
                "Authorization: Bearer CATALOGSECRET serviceKey=querysecret",
                retryable=False,
            )

        def decode_page(
            self, request: Request, response: RawResponse
        ) -> Page:
            raise AssertionError("failed fetch must not be decoded")

    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    with pytest.raises(ConnectorError):
        pipeline.collect_and_publish(
            SecretFailConnector(),
            Request(dataset="facilities"),
            run_id="secret-failure",
        )

    stored = pipeline.catalog.get_run("secret-failure")["error_json"]
    assert "CATALOGSECRET" not in stored
    assert "querysecret" not in stored
    assert "[REDACTED]" in stored


@pytest.mark.parametrize(
    "credential_name", ["serviceKey", "confmKey", "consumer_key"]
)
def test_request_params_reject_credentials_before_run_or_raw_write(
    tmp_path: Path, credential_name: str,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    connector = FixtureConnector(
        load_jsonl_fixture(),
        page_size=1,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    with pytest.raises(SensitiveDataError, match="request params"):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={credential_name: "TOPSECRET"},
                page_size=1,
            ),
            run_id="credential-in-request",
        )

    assert pipeline.catalog.get_run("credential-in-request") is None
    assert pipeline.catalog.count_raw_objects() == 0


def test_non_success_http_response_is_retried_captured_and_never_published(
    tmp_path: Path,
) -> None:
    class Http500Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def __init__(self) -> None:
            self.fetches = 0

        def fetch_raw_page(self, request: Request) -> RawResponse:
            self.fetches += 1
            body = (
                json.dumps(
                    load_jsonl_fixture()[0],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            return RawResponse(body=body, status_code=500)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            raise AssertionError("non-success response must not be decoded")

    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    connector = Http500Connector()
    with pytest.raises(ConnectorError, match="HTTP status 500"):
        pipeline.collect_and_publish(
            connector,
            Request(dataset=SAMPLE_DATASET_ID),
            run_id="http-500-failed",
        )

    assert connector.fetches == settings.max_retries + 1
    assert pipeline.release_manager.current() is None
    assert pipeline.catalog.get_run("http-500-failed")["status"] == "failed"
    result = json.loads(
        pipeline.catalog.get_run("http-500-failed")["result_json"]
    )
    assert result["partial_raw_pages"] == 1
    assert pipeline.catalog.count_raw_objects() == 1
    assert pipeline.catalog.count_raw_ingestions() == 1


def test_metadata_catalog_and_pipeline_lock_cannot_escape_via_symlink(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    settings.data_root.mkdir(parents=True)
    metadata = settings.data_root / "metadata"
    metadata.mkdir()
    outside_catalog = tmp_path / "outside-catalog.sqlite"
    (metadata / "ops.sqlite").symlink_to(outside_catalog)
    with pytest.raises(ContractViolation, match="symlink|inside data_root"):
        DentalMarketPipeline(settings)
    assert not outside_catalog.exists()

    (metadata / "ops.sqlite").unlink()
    pipeline = DentalMarketPipeline(settings)
    outside_lock = tmp_path / "outside-pipeline.lock"
    (metadata / "pipeline.lock").symlink_to(outside_lock)
    with pytest.raises(ContractViolation, match="symlink|inside data_root"):
        pipeline.collect_and_publish(
            FixtureConnector(
                load_jsonl_fixture(),
                dataset=SAMPLE_DATASET_ID,
                source=SAMPLE_SOURCE_ID,
            ),
            Request(dataset=SAMPLE_DATASET_ID),
            run_id="symlink-lock-failed",
        )
    assert not outside_lock.exists()


def test_exact_raw_preserves_identical_duplicate_before_transform(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    rows = load_jsonl_fixture()
    connector = FixtureConnector(
        rows + [dict(rows[0])],
        page_size=20,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    result = pipeline.collect_and_publish(
        connector,
        Request(dataset=SAMPLE_DATASET_ID, page_size=20),
        as_of_date="2026-09-01",
        run_id="raw-duplicate",
    )

    assert result.status == "published"
    assert result.collected_records == 6
    assert result.raw_duplicates_dropped == 1
    assert result.transform_duplicates_dropped == 1
    collection = pipeline.collection_store.read(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID, result.raw_collection_hash
    )
    raw_body = pipeline.raw_store.read_bytes(
        collection.pages[0].object_hash, SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert len(raw_body.decode("utf-8").splitlines()) == 7


def test_pipeline_preserves_raw_before_record_parser_failure(
    tmp_path: Path,
) -> None:
    class MalformedRawConnector(BaseConnector):
        source = "malformed_raw"
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"{malformed-json")

        def decode_page(
            self, request: Request, response: RawResponse
        ) -> Page:
            return Page(watermark="malformed-v1")

    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    with pytest.raises(ParserError, match="invalid JSONL"):
        pipeline.collect_and_publish(
            MalformedRawConnector(),
            Request(dataset="facilities"),
            run_id="malformed-raw",
        )
    assert pipeline.catalog.get_run("malformed-raw")["status"] == "failed"
    manifest = pipeline.catalog.get_collection_manifest(
        "malformed-raw", "malformed_raw", "facilities"
    )
    assert manifest is not None
    assert manifest["status"] == "failed"
    collection = pipeline.collection_store.read(
        "malformed_raw", "facilities", manifest["collection_hash"]
    )
    assert pipeline.raw_store.read_bytes(
        collection.pages[0].object_hash, "malformed_raw", "facilities"
    ) == b"{malformed-json"


def test_successful_pages_are_fsynced_before_a_later_page_fails(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    with pytest.raises(ConnectorError):
        run_sample(
            settings,
            as_of_date="2026-09-01",
            run_id="partial-page-failure",
            page_size=2,
            transient_failures={"2": 10},
        )

    pipeline = DentalMarketPipeline(settings)
    run = pipeline.catalog.get_run("partial-page-failure")
    assert run["status"] == "failed"
    result = json.loads(run["result_json"])
    assert result["partial_raw_pages"] == 1
    assert len(result["partial_raw_object_hashes"]) == 1
    assert pipeline.catalog.count_raw_objects() == 1
    assert pipeline.catalog.count_raw_ingestions() == 1
    assert pipeline.catalog.list_collection_manifests(
        run_id="partial-page-failure"
    ) == []


def test_failed_release_does_not_create_an_accepted_schema_baseline(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    rows = load_jsonl_fixture()
    connector = FixtureConnector(
        rows,
        page_size=20,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    def reject_release(_candidate):
        return False

    with pytest.raises(ReleaseValidationError):
        pipeline.collect_and_publish(
            connector,
            Request(dataset=SAMPLE_DATASET_ID, page_size=20),
            as_of_date="2026-09-01",
            run_id="rejected-baseline",
            validators=(reject_release,),
        )

    assert pipeline.catalog.latest_schema(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    ) is None
    accepted = run_sample(
        settings,
        as_of_date="2026-09-01",
        run_id="accepted-baseline",
        page_size=20,
    )
    assert accepted.status == "published"
    assert accepted.schema_drift["baseline"] is True


def test_no_change_cannot_advance_beyond_the_durable_watermark(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="watermark-baseline"
    )
    assert baseline.status == "published"

    class EmptyNewWatermarkConnector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(
            self, request: Request, response: RawResponse
        ) -> Page:
            return Page(watermark="new-safe-watermark")

    pipeline = DentalMarketPipeline(settings)
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )
    with pytest.raises(PipelineError, match="durable scope watermark"):
        pipeline.collect_and_publish(
            EmptyNewWatermarkConnector(),
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
            ),
            run_id="watermark-no-change",
        )
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    assert len(
        pipeline.catalog.list_watermark_history(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            scope_fingerprint=baseline.scope_fingerprint,
        )
    ) == 1


@pytest.mark.parametrize(
    "request_params,parser_version",
    [
        ({"fixture_contract": "different-region"}, "1"),
        ({"fixture_contract": "m1.v1"}, "2"),
    ],
)
def test_304_cannot_reuse_last_good_from_another_scope_or_parser(
    tmp_path: Path, request_params, parser_version: str
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="scope-baseline"
    )
    pipeline = DentalMarketPipeline(settings)

    class OutOfScope304Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="scope-specific-watermark")

    connector = OutOfScope304Connector()
    connector.parser_version = parser_version
    with pytest.raises(PipelineError, match="durable scope watermark"):
        pipeline.collect_and_publish(
            connector,
            Request(dataset=SAMPLE_DATASET_ID, params=request_params),
            run_id="scope-mismatch-%s" % parser_version,
        )

    assert pipeline.release_manager.current() == baseline.release
    manifest = pipeline.catalog.get_collection_manifest(
        "scope-mismatch-%s" % parser_version,
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
    )
    assert manifest is None
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) is not None


def test_explicit_200_watermark_override_cannot_poison_scope_state(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="override-200-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )
    connector = FixtureConnector(
        load_jsonl_fixture(),
        page_size=20,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    with pytest.raises(PipelineError, match="watermark override"):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                watermark="future-untrusted",
                page_size=20,
            ),
            as_of_date="2026-09-02",
            run_id="override-200-rejected",
        )

    assert pipeline.catalog.get_run("override-200-rejected") is None
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    assert pipeline.release_manager.current() == baseline.release


def test_connector_cannot_mutate_bound_request_scope(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    caller_params = {"region": "busan", "nested": {"districts": ["buk"]}}
    request = Request(dataset="facilities", params=caller_params)

    class MutatingParamsConnector(BaseConnector):
        source = "scope_source"
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, page_request: Request) -> RawResponse:
            page_request.params["region"] = "ulsan"
            return RawResponse(body=b"{}\n")

        def decode_page(self, page_request: Request, response: RawResponse) -> Page:
            return Page(watermark="done")

    caller_params["region"] = "ulsan"
    caller_params["nested"]["districts"].append("nam")
    assert request.params["region"] == "busan"
    assert request.params["nested"]["districts"] == ("buk",)

    with pytest.raises(TypeError, match="immutable"):
        pipeline.collect_and_publish(
            MutatingParamsConnector(),
            request,
            as_of_date="2026-09-01",
            run_id="mutating-request-scope",
        )

    assert pipeline.catalog.get_run("mutating-request-scope")["status"] == "failed"
    assert pipeline.release_manager.current() is None


def test_connector_identity_change_cannot_relabel_collected_bytes(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)

    class MutatingIdentityConnector(BaseConnector):
        source = "original_source"
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            self.source = "other_source"
            return RawResponse(
                body=(
                    b'{"source_id":"other_source","dataset_id":"facilities",'
                    b'"source_record_id":"1"}\n'
                )
            )

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="done")

    connector = MutatingIdentityConnector()
    with pytest.raises(PipelineError, match="identity changed"):
        pipeline.collect_and_publish(
            connector,
            Request(dataset="facilities", params={"region": "busan"}),
            as_of_date="2026-09-01",
            run_id="mutating-connector-identity",
        )

    assert connector.source == "other_source"
    assert pipeline.catalog.get_run("mutating-connector-identity")["status"] == "failed"
    assert pipeline.release_manager.current() is None


def test_http_200_empty_snapshot_is_not_treated_as_no_change(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="empty-200-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_pointer = pipeline.release_manager.current()
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )

    class Empty200Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=200)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="unsafe-empty-snapshot")

    with pytest.raises(PipelineError, match="explicit single-page 304"):
        pipeline.collect_and_publish(
            Empty200Connector(),
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
            ),
            run_id="empty-200-failed",
        )
    assert baseline.release == previous_pointer
    assert pipeline.release_manager.current() == previous_pointer
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    manifest = pipeline.catalog.get_collection_manifest(
        "empty-200-failed", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert manifest is not None
    assert manifest["status"] == "failed"


def test_publish_rejects_credential_shaped_watermark_before_metadata_write(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    rows = load_jsonl_fixture()

    class SecretWatermarkConnector(FixtureConnector):
        def decode_page(self, request: Request, response: RawResponse) -> Page:
            page = super().decode_page(request, response)
            return Page(
                next_cursor=page.next_cursor,
                watermark="client_secret=PUBLISHWATERLEAK",
            )

    connector = SecretWatermarkConnector(
        rows,
        page_size=20,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )
    pipeline = DentalMarketPipeline(settings)
    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                page_size=20,
            ),
            run_id="secret-publish-watermark",
        )

    assert pipeline.release_manager.current() is None
    assert pipeline.catalog.get_run("secret-publish-watermark")["status"] == "failed"
    assert b"PUBLISHWATERLEAK" not in pipeline.catalog.path.read_bytes()


def test_no_change_rejects_credential_shaped_watermark_without_advancing(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="secret-no-change-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )

    class SecretNoChangeConnector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="refresh_token=NOCHANGEWATERLEAK")

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        pipeline.collect_and_publish(
            SecretNoChangeConnector(),
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
            ),
            run_id="secret-no-change-watermark",
        )

    assert pipeline.release_manager.current() == baseline.release
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    assert b"NOCHANGEWATERLEAK" not in pipeline.catalog.path.read_bytes()


def test_http_304_with_nonempty_body_is_preserved_but_never_published(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    rows = load_jsonl_fixture()
    body = (
        "\n".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True)
            for row in rows
        )
        + "\n"
    ).encode("utf-8")

    class Nonempty304Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(
                body=body,
                status_code=304,
                content_type="application/x-ndjson; charset=utf-8",
            )

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="must-not-commit")

    pipeline = DentalMarketPipeline(settings)
    with pytest.raises(
        PipelineError,
        match="304 must be an exact empty single-page response",
    ):
        pipeline.collect_and_publish(
            Nonempty304Connector(),
            Request(dataset=SAMPLE_DATASET_ID),
            run_id="nonempty-304-failed",
        )

    assert pipeline.release_manager.current() is None
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint="f" * 64,
    ) is None
    manifest = pipeline.catalog.get_collection_manifest(
        "nonempty-304-failed", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert manifest is None
    run = pipeline.catalog.get_run("nonempty-304-failed")
    assert run["status"] == "failed"
    result = json.loads(run["result_json"])
    assert result["partial_raw_pages"] == 1
    assert len(result["partial_raw_object_hashes"]) == 1
    assert pipeline.raw_store.read_bytes(
        result["partial_raw_object_hashes"][0],
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
    ) == body


def test_exact_304_never_runs_a_parser_that_could_synthesize_records(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    parser_called = False
    registry = ParserRegistry()

    def synthesizing_parser(body: bytes, content_type: str):
        nonlocal parser_called
        parser_called = True
        return [{"source_record_id": "fabricated"}]

    registry.register("synthesizing-parser", "1", synthesizing_parser)

    class Exact304Connector(BaseConnector):
        source = "synthetic_304"
        parser_id = "synthesizing-parser"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark="unchanged")

    pipeline = DentalMarketPipeline(settings, parser_registry=registry)
    with pytest.raises(PipelineError, match="durable scope watermark"):
        pipeline.collect_and_publish(
            Exact304Connector(),
            Request(dataset="facilities"),
            run_id="synthesizing-304-failed",
        )
    assert parser_called is False
    assert pipeline.release_manager.current() is None


def test_no_change_collection_cannot_be_rebuilt_as_a_snapshot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)
    fingerprint = "a" * 64
    collection = pipeline.collection_store.put(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        [
            RawPagePayload(
                body=b"",
                request_fingerprint=fingerprint,
                status_code=304,
            )
        ],
        run_id="stored-no-change",
        parser_id="jsonl-records",
        parser_version="1",
        request_fingerprint=fingerprint,
    )

    with pytest.raises(PipelineError, match="not a rebuildable snapshot"):
        pipeline.rebuild_from_raw(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            collection.collection_hash,
            as_of_date="2026-09-01",
            run_id="rebuild-no-change-failed",
        )
    assert pipeline.catalog.get_run("rebuild-no-change-failed")["status"] == "failed"


def test_late_no_change_metadata_failure_rolls_back_watermark_and_manifest(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="no-change-atomic-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_pointer = pipeline.release_manager.current()
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )

    with sqlite3.connect(str(pipeline.catalog.path)) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_atomic_no_change_run
            BEFORE UPDATE OF status ON runs
            WHEN NEW.run_id = 'no-change-atomic-failure'
             AND NEW.status = 'succeeded'
            BEGIN
                SELECT RAISE(ABORT, 'forced no-change metadata failure');
            END
            """
        )

    class Empty304Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark=previous_watermark)

    with pytest.raises(sqlite3.IntegrityError, match="forced no-change"):
        pipeline.collect_and_publish(
            Empty304Connector(),
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
            ),
            run_id="no-change-atomic-failure",
        )

    assert pipeline.release_manager.current() == previous_pointer
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    failed_manifest = pipeline.catalog.get_collection_manifest(
        "no-change-atomic-failure", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert failed_manifest is not None
    assert failed_manifest["status"] == "failed"
    assert pipeline.catalog.get_run("no-change-atomic-failure")["status"] == "failed"


def test_late_publish_metadata_failure_rolls_back_pointer_and_catalog(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="atomic-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_pointer = pipeline.release_manager.current()
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )
    previous_schema = pipeline.catalog.latest_schema(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert previous_pointer is not None

    # Fail the final UPDATE inside commit_published_run. Earlier schema,
    # watermark, collection, and release writes must roll back with it.
    with sqlite3.connect(str(pipeline.catalog.path)) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_atomic_publish_run
            BEFORE UPDATE OF status ON runs
            WHEN NEW.run_id = 'atomic-metadata-failure'
             AND NEW.status = 'succeeded'
            BEGIN
                SELECT RAISE(ABORT, 'forced final metadata failure');
            END
            """
        )

    rows = load_jsonl_fixture()
    rows[0] = {**rows[0], "facility_name": "원자성 검증 치과"}
    connector = FixtureConnector(
        rows,
        page_size=20,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )
    with pytest.raises(sqlite3.IntegrityError, match="forced final metadata"):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                page_size=20,
            ),
            as_of_date="2026-09-02",
            run_id="atomic-metadata-failure",
        )

    assert baseline.release == previous_pointer
    assert pipeline.release_manager.current() == previous_pointer
    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    assert pipeline.catalog.latest_schema(
        SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    ) == previous_schema
    failed_manifest = pipeline.catalog.get_collection_manifest(
        "atomic-metadata-failure", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert failed_manifest is not None
    assert failed_manifest["status"] == "failed"
    assert failed_manifest["release_id"] is None
    assert pipeline.catalog.get_run("atomic-metadata-failure")["status"] == "failed"
    assert pipeline.catalog.get_release(
        "20260902-atomic-metadata-failure"
    )["status"] == "failed"


def test_initial_cursor_is_rejected_before_it_can_publish_a_partial_snapshot(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="cursor-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous = pipeline.release_manager.current()
    connector = FixtureConnector(
        load_jsonl_fixture(),
        page_size=2,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
    )

    with pytest.raises(PipelineError, match="initial cursor"):
        pipeline.collect_and_publish(
            connector,
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                cursor="2",
                page_size=2,
            ),
            as_of_date="2026-09-02",
            run_id="cursor-partial-rejected",
        )

    assert pipeline.release_manager.current() == previous == baseline.release
    assert pipeline.catalog.get_run("cursor-partial-rejected") is None


def test_explicit_304_watermark_override_cannot_advance_scope_state(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="override-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    previous_watermark = pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    )

    class Override304Connector(BaseConnector):
        source = SAMPLE_SOURCE_ID
        parser_id = "jsonl-records"
        parser_version = "1"

        def fetch_raw_page(self, request: Request) -> RawResponse:
            return RawResponse(body=b"", status_code=304)

        def decode_page(self, request: Request, response: RawResponse) -> Page:
            return Page(watermark=request.watermark)

    with pytest.raises(PipelineError, match="watermark override"):
        pipeline.collect_and_publish(
            Override304Connector(),
            Request(
                dataset=SAMPLE_DATASET_ID,
                params={"fixture_contract": "m1.v1"},
                watermark="future-untrusted",
            ),
            run_id="override-304-rejected",
        )

    assert pipeline.catalog.get_watermark(
        SAMPLE_SOURCE_ID,
        SAMPLE_DATASET_ID,
        scope_fingerprint=baseline.scope_fingerprint,
    ) == previous_watermark
    assert pipeline.release_manager.current() == baseline.release


def test_pipeline_run_id_secret_is_rejected_before_run_history_write(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = DentalMarketPipeline(settings)

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        run_sample(
            settings,
            as_of_date="2026-09-01",
            run_id="client_secret=RUNLEAK",
        )

    assert b"RUNLEAK" not in pipeline.catalog.path.read_bytes()


def test_rebuild_hash_secret_is_rejected_before_starting_a_run(
    tmp_path: Path,
) -> None:
    pipeline = DentalMarketPipeline(_settings(tmp_path))

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        pipeline.rebuild_from_raw(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            "client_secret=HASHLEAK",
            as_of_date="2026-09-01",
            run_id="safe-rebuild-run",
        )

    assert pipeline.catalog.get_run("safe-rebuild-run") is None
    assert b"HASHLEAK" not in pipeline.catalog.path.read_bytes()


def test_every_rebuild_requires_an_explicit_as_of_date(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="rebuild-date-baseline"
    )
    pipeline = DentalMarketPipeline(settings)

    with pytest.raises(PipelineError, match="explicit as_of_date"):
        pipeline.rebuild_from_raw(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            baseline.raw_collection_hash,
            run_id="rebuild-date-rejected",
        )

    assert pipeline.catalog.get_run("rebuild-date-rejected") is None
    assert pipeline.release_manager.current() == baseline.release


def test_nonpublished_rebuild_manifest_and_run_commit_atomically(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    baseline = run_sample(
        settings, as_of_date="2026-09-01", run_id="rebuild-atomic-baseline"
    )
    pipeline = DentalMarketPipeline(settings)
    with sqlite3.connect(str(pipeline.catalog.path)) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_atomic_rebuild_run
            BEFORE UPDATE OF status ON runs
            WHEN NEW.run_id = 'rebuild-atomic-failure'
             AND NEW.status = 'succeeded'
            BEGIN
                SELECT RAISE(ABORT, 'forced rebuild metadata failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced rebuild"):
        pipeline.rebuild_from_raw(
            SAMPLE_SOURCE_ID,
            SAMPLE_DATASET_ID,
            baseline.raw_collection_hash,
            as_of_date="2026-09-01",
            run_id="rebuild-atomic-failure",
        )

    manifest = pipeline.catalog.get_collection_manifest(
        "rebuild-atomic-failure", SAMPLE_SOURCE_ID, SAMPLE_DATASET_ID
    )
    assert manifest is not None
    assert manifest["status"] == "failed_rebuild"
    assert pipeline.catalog.get_run("rebuild-atomic-failure")["status"] == "failed"
    assert pipeline.release_manager.current() == baseline.release
