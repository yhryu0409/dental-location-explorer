"""End-to-end M1 collection, rebuild, and guarded release pipeline."""

from __future__ import annotations

import json
import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from .config import PACKAGE_ROOT, Settings
from .connectors import (
    CapturedPage,
    Connector,
    ConnectorRunner,
    FixtureConnector,
    RateLimiter,
    Request,
    RetryPolicy,
)
from .contracts import (
    LayerArtifact,
    ReleasePointer,
    ReleaseValidationError,
    canonical_json_bytes,
    detect_schema_drift,
    infer_schema,
    sha256_bytes,
)
from .metadata_store import OpsCatalog
from .parsers import DEFAULT_PARSER_REGISTRY, ParserRegistry
from .raw_collection import (
    RawCollection,
    RawCollectionStore,
    RawPagePayload,
    RawPageReference,
)
from .release import ReleaseManager, ReleaseValidator
from .security import redact_sensitive, reject_sensitive_data
from .storage import (
    AtomicLayerWriter,
    ContentAddressedRawStore,
    _validate_component,
    _validate_managed_path,
)
from .transforms import (
    build_facility_core,
    build_facility_staging,
    build_region_supply_mart,
    count_duplicate_source_keys,
)


SAMPLE_SOURCE_ID = "m1_fixture"
SAMPLE_DATASET_ID = "dental_facility_sample"
SAMPLE_FIXTURE_PATH = PACKAGE_ROOT / "fixtures" / "facilities.jsonl"


class PipelineError(RuntimeError):
    """Base error for a failed, non-promoted pipeline run."""


class SchemaDriftBlocked(PipelineError):
    """Raised when an upstream schema changes without an approved contract."""


class PipelineBusyError(PipelineError):
    """Raised when another process owns the pipeline lock."""


@dataclass(frozen=True)
class PipelineArtifacts:
    staging: LayerArtifact
    core: LayerArtifact
    mart: LayerArtifact

    def to_dict(self) -> Dict[str, Any]:
        return {
            "staging": self.staging.to_dict(),
            "core": self.core.to_dict(),
            "mart": self.mart.to_dict(),
        }


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    status: str
    source_id: str
    dataset_id: str
    request_fingerprint: Optional[str]
    scope_fingerprint: str
    pages_fetched: int
    collected_records: int
    raw_duplicates_dropped: int
    transform_duplicates_dropped: int
    raw_collection_hash: Optional[str]
    raw_object_hashes: Tuple[str, ...]
    schema_drift: Optional[Mapping[str, Any]]
    artifacts: Optional[PipelineArtifacts]
    release: Optional[ReleasePointer]
    current_release_id: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        return payload


def _new_run_id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return "%s-%s-%s" % (prefix, timestamp, uuid.uuid4().hex[:10])


def _validate_pipeline_identity(run_id: str, as_of_date: str) -> None:
    """Reject unsafe durable identifiers before starting a run."""

    reject_sensitive_data(run_id, location="pipeline run id")
    reject_sensitive_data(as_of_date, location="pipeline as-of date")
    _validate_component(run_id, "run_id")
    try:
        parsed_as_of = date.fromisoformat(as_of_date)
    except (TypeError, ValueError) as exc:
        raise PipelineError("as_of_date must be an ISO calendar date") from exc
    if parsed_as_of.isoformat() != as_of_date:
        raise PipelineError("as_of_date must be an ISO calendar date")


def _request_fingerprint(source_id: str, request: Request) -> str:
    contract = {
        "source_id": source_id,
        "dataset_id": request.dataset,
        "params": dict(request.params),
        "cursor_fingerprint": _optional_fingerprint(request.cursor),
        "watermark_fingerprint": _optional_fingerprint(request.watermark),
        "page_size": request.page_size,
    }
    return sha256_bytes(canonical_json_bytes(contract))


def _collection_scope_fingerprint(
    source_id: str,
    dataset_id: str,
    params: Mapping[str, Any],
    parser_id: str,
    parser_version: str,
) -> str:
    """Identify semantic collection scope without cursor or resume state."""

    return sha256_bytes(
        canonical_json_bytes(
            {
                "contract": "collection-scope-v1",
                "source_id": source_id,
                "dataset_id": dataset_id,
                "params": dict(params),
                "parser_id": parser_id,
                "parser_version": parser_version,
            }
        )
    )


def _optional_fingerprint(value: Optional[str]) -> Optional[str]:
    return sha256_bytes(value.encode("utf-8")) if value is not None else None


def _page_request_fingerprint(
    collection_request_fingerprint: str,
    page_index: int,
    request_cursor: Optional[str],
) -> str:
    return sha256_bytes(
        canonical_json_bytes(
            {
                "collection_request_fingerprint": collection_request_fingerprint,
                "page_index": page_index,
                "request_cursor_fingerprint": _optional_fingerprint(
                    request_cursor
                ),
            }
        )
    )


def _deduplicate_raw_records(
    records: Iterable[Mapping[str, Any]], source_id: str, dataset_id: str
) -> Tuple[List[Dict[str, Any]], int]:
    payloads = [dict(record) for record in records]
    deduplicated: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    duplicates = 0
    for payload in payloads:
        if payload.get("source_id") != source_id:
            raise PipelineError("raw source_id does not match connector")
        if payload.get("dataset_id") != dataset_id:
            raise PipelineError("raw dataset_id does not match connector")
        if not str(payload.get("source_record_id", "")).strip():
            raise PipelineError("raw source_record_id is missing")
        key = (
            str(payload["source_id"]),
            str(payload["dataset_id"]),
            str(payload["source_record_id"]),
        )
        existing = deduplicated.get(key)
        if existing is not None:
            if canonical_json_bytes(existing) != canonical_json_bytes(payload):
                raise PipelineError(
                    "conflicting records share a parsed raw natural key"
                )
            duplicates += 1
            continue
        deduplicated[key] = payload
    unique = list(deduplicated.values())
    unique.sort(
        key=lambda row: (
            str(row["source_id"]),
            str(row["dataset_id"]),
            str(row["source_record_id"]),
        )
    )
    return unique, duplicates


@contextmanager
def _pipeline_lock(data_root: Path) -> Iterator[None]:
    """Hold a non-blocking process lock for one pipeline mutation."""

    lock_path = data_root / "metadata" / "pipeline.lock"
    _validate_managed_path(data_root, lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    fallback_marker: Optional[Path] = None
    try:
        try:
            import fcntl  # type: ignore

            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise PipelineBusyError("another pipeline run is active") from exc
        except ImportError:  # pragma: no cover - exercised on non-POSIX hosts
            fallback_marker = lock_path.with_suffix(".lock.active")
            try:
                descriptor = os.open(
                    str(fallback_marker), os.O_CREAT | os.O_EXCL | os.O_WRONLY
                )
            except FileExistsError as exc:
                raise PipelineBusyError("another pipeline run is active") from exc
            else:
                os.close(descriptor)
        yield
    finally:
        if fallback_marker is not None:
            try:
                fallback_marker.unlink()
            except FileNotFoundError:
                pass
        try:
            import fcntl  # type: ignore

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except (ImportError, OSError):
            pass
        handle.close()


class _IdentityBoundConnector:
    """Delegate connector calls while enforcing one immutable source contract."""

    def __init__(
        self,
        delegate: Connector,
        source_id: str,
        parser_id: str,
        parser_version: str,
    ) -> None:
        self._delegate = delegate
        self.source = source_id
        self.parser_id = parser_id
        self.parser_version = parser_version
        self._assert_identity()

    def _assert_identity(self) -> None:
        actual = (
            getattr(self._delegate, "source", None),
            getattr(self._delegate, "parser_id", None),
            getattr(self._delegate, "parser_version", None),
        )
        expected = (self.source, self.parser_id, self.parser_version)
        if actual != expected:
            raise PipelineError(
                "connector source or parser identity changed during collection"
            )

    def fetch_raw_page(self, request: Request):
        self._assert_identity()
        response = self._delegate.fetch_raw_page(request)
        self._assert_identity()
        return response

    def decode_page(self, request: Request, response):
        self._assert_identity()
        page = self._delegate.decode_page(request, response)
        self._assert_identity()
        return page


class DentalMarketPipeline:
    """M1 pipeline with immutable raw, deterministic rebuild, and last-good release."""

    def __init__(
        self,
        settings: Settings,
        parser_registry: Optional[ParserRegistry] = None,
    ) -> None:
        self.settings = settings
        self.settings.ensure_data_directories()
        catalog_path = settings.data_root / "metadata" / "ops.sqlite"
        _validate_managed_path(settings.data_root, catalog_path)
        self.catalog = OpsCatalog(catalog_path)
        self.raw_store = ContentAddressedRawStore(settings.data_root, self.catalog)
        self.collection_store = RawCollectionStore(
            settings.data_root, self.raw_store
        )
        self.parser_registry = parser_registry or DEFAULT_PARSER_REGISTRY
        self.layer_writer = AtomicLayerWriter(settings.data_root)
        self.release_manager = ReleaseManager(settings.data_root, self.catalog)

    def collect_and_publish(
        self,
        connector: Connector,
        request: Request,
        *,
        as_of_date: Optional[str] = None,
        run_id: Optional[str] = None,
        validators: Sequence[ReleaseValidator] = (),
    ) -> PipelineResult:
        effective_as_of = as_of_date or date.today().isoformat()
        effective_run_id = run_id or _new_run_id("collect")
        _validate_pipeline_identity(effective_run_id, effective_as_of)
        source_id = getattr(connector, "source", None)
        dataset_id = request.dataset
        parser_id = getattr(connector, "parser_id", None)
        parser_version = getattr(connector, "parser_version", None)
        reject_sensitive_data(source_id, location="connector source id")
        reject_sensitive_data(dataset_id, location="request dataset id")
        reject_sensitive_data(parser_id, location="connector parser id")
        reject_sensitive_data(parser_version, location="connector parser version")
        reject_sensitive_data(request.params, location="request params")
        _validate_component(source_id, "source_id")
        _validate_component(dataset_id, "dataset_id")
        _validate_component(parser_id, "parser_id")
        _validate_component(parser_version, "parser_version")
        if request.cursor is not None:
            raise PipelineError(
                "an initial cursor cannot publish a complete snapshot"
            )
        raw_collection: Optional[RawCollection] = None
        raw_records: List[Dict[str, Any]] = []
        raw_duplicates = 0
        collection_recorded = False
        streamed_pages: List[RawPageReference] = []
        collection_result = None

        with _pipeline_lock(self.settings.data_root):
            if parser_id in ("", "unavailable") or parser_version in ("", "0"):
                raise PipelineError(
                    "connector must declare a registered parser id and version"
                )
            bound_connector = _IdentityBoundConnector(
                connector, source_id, parser_id, parser_version
            )
            scope_fingerprint = _collection_scope_fingerprint(
                source_id,
                dataset_id,
                request.params,
                parser_id,
                parser_version,
            )
            stored_watermark = self.catalog.get_watermark(
                source_id,
                dataset_id,
                scope_fingerprint=scope_fingerprint,
            )
            if request.watermark is not None:
                reject_sensitive_data(
                    request.watermark, location="request watermark override"
                )
                if request.watermark != stored_watermark:
                    raise PipelineError(
                        "request watermark override does not match durable scope state"
                    )
            effective_request = Request(
                dataset=dataset_id,
                params=request.params,
                cursor=request.cursor,
                watermark=(
                    request.watermark
                    if request.watermark is not None
                    else stored_watermark
                ),
                page_size=request.page_size,
            )
            fingerprint = _request_fingerprint(
                source_id, effective_request
            )
            self.catalog.start_run(
                effective_run_id, "facility_collection", fingerprint
            )
            try:
                def persist_page(page: CapturedPage) -> None:
                    streamed_pages.append(
                        self.collection_store.put_page(
                            source_id,
                            dataset_id,
                            RawPagePayload(
                                body=page.raw_response.body,
                                request_fingerprint=_page_request_fingerprint(
                                    fingerprint,
                                    page.page_index,
                                    page.request_cursor,
                                ),
                                request_cursor=page.request_cursor,
                                status_code=page.raw_response.status_code,
                                content_type=page.raw_response.content_type,
                                response_headers=page.raw_response.response_headers,
                                retrieved_at=page.raw_response.retrieved_at,
                                source_as_of=page.raw_response.source_as_of,
                            ),
                            run_id=effective_run_id,
                            page_index=page.page_index,
                        )
                    )

                runner = ConnectorRunner(
                    bound_connector,
                    retry_policy=RetryPolicy(
                        max_attempts=self.settings.max_retries + 1,
                        base_delay_seconds=self.settings.retry_base_seconds,
                    ),
                    rate_limiter=RateLimiter(
                        self.settings.rate_limit_per_second
                    ),
                    page_observer=persist_page,
                    retain_raw_pages=False,
                )
                collection_result = runner.collect(effective_request)
                if len(streamed_pages) != collection_result.pages_fetched:
                    raise PipelineError(
                        "every successful API page must include exact raw response bytes"
                    )
                if any(page.status_code == 304 for page in streamed_pages):
                    exact_not_modified_page = (
                        len(streamed_pages) == 1
                        and streamed_pages[0].status_code == 304
                        and streamed_pages[0].request_cursor_fingerprint is None
                        and self.raw_store.read_bytes(
                            streamed_pages[0].object_hash,
                            source_id,
                            dataset_id,
                        )
                        == b""
                    )
                    if not exact_not_modified_page:
                        raise PipelineError(
                            "HTTP 304 must be an exact empty single-page response"
                        )
                    reject_sensitive_data(
                        collection_result.starting_watermark,
                        location="HTTP 304 starting watermark",
                    )
                    reject_sensitive_data(
                        collection_result.ending_watermark,
                        location="HTTP 304 ending watermark",
                    )
                    if (
                        stored_watermark is None
                        or collection_result.starting_watermark
                        != stored_watermark
                        or collection_result.ending_watermark
                        != stored_watermark
                    ):
                        raise PipelineError(
                            "HTTP 304 does not match the durable scope watermark"
                        )
                raw_collection = self.collection_store.finalize(
                    source_id,
                    dataset_id,
                    streamed_pages,
                    parser_id=parser_id,
                    parser_version=parser_version,
                    request_fingerprint=fingerprint,
                    scope_fingerprint=scope_fingerprint,
                )
                has_not_modified_response = any(
                    page.status_code == 304 for page in raw_collection.pages
                )
                if has_not_modified_response:
                    exact_not_modified_response = (
                        len(raw_collection.pages) == 1
                        and raw_collection.pages[0].status_code == 304
                        and raw_collection.pages[0].request_cursor_fingerprint
                        is None
                        and self.raw_store.read_bytes(
                            raw_collection.pages[0].object_hash,
                            source_id,
                            dataset_id,
                        )
                        == b""
                    )
                    if not exact_not_modified_response:
                        raise PipelineError(
                            "HTTP 304 must be an exact empty single-page response"
                        )
                if has_not_modified_response:
                    # A 304 carries no representation.  Do not permit a custom
                    # parser to synthesize records from its required empty body.
                    raw_records = []
                else:
                    if all(
                        self.raw_store.read_bytes(
                            page.object_hash,
                            source_id,
                            dataset_id,
                        )
                        == b""
                        for page in raw_collection.pages
                    ):
                        raise PipelineError(
                            "empty parsed response is not an explicit single-page 304"
                        )
                    raw_records = self._parse_raw_collection(raw_collection)
                captured_payloads, raw_duplicates = _deduplicate_raw_records(
                    raw_records, source_id, dataset_id
                )

                if not captured_payloads:
                    explicit_not_modified = (
                        len(raw_collection.pages) == 1
                        and raw_collection.pages[0].status_code == 304
                        and raw_collection.pages[0].request_cursor_fingerprint
                        is None
                        and self.raw_store.read_bytes(
                            raw_collection.pages[0].object_hash,
                            source_id,
                            dataset_id,
                        )
                        == b""
                    )
                    if not explicit_not_modified:
                        raise PipelineError(
                            "empty parsed response is not an explicit single-page 304"
                        )
                    current = self.release_manager.current()
                    if current is None:
                        raise PipelineError(
                            "source returned no records and no last-good release exists"
                        )
                    current_manifest = self.release_manager.current_manifest()
                    current_metadata = (
                        current_manifest.get("metadata", {})
                        if current_manifest is not None
                        else {}
                    )
                    if (
                        current_metadata.get("source_id") != source_id
                        or current_metadata.get("dataset_id") != dataset_id
                        or current_metadata.get("scope_fingerprint")
                        != scope_fingerprint
                    ):
                        raise PipelineError(
                            "empty response cannot reuse a release from another source contract"
                        )
                    result = PipelineResult(
                        run_id=effective_run_id,
                        status="no_change",
                        source_id=source_id,
                        dataset_id=dataset_id,
                        request_fingerprint=fingerprint,
                        scope_fingerprint=scope_fingerprint,
                        pages_fetched=collection_result.pages_fetched,
                        collected_records=0,
                        raw_duplicates_dropped=raw_duplicates,
                        transform_duplicates_dropped=0,
                        raw_collection_hash=raw_collection.collection_hash,
                        raw_object_hashes=tuple(
                            page.object_hash for page in raw_collection.pages
                        ),
                        schema_drift=None,
                        artifacts=None,
                        release=None,
                        current_release_id=current.release_id,
                    )
                    collection_manifest = self._collection_manifest_contract(
                        effective_run_id,
                        raw_collection,
                        fingerprint,
                        record_count=0,
                        duplicate_count=raw_duplicates,
                        starting_watermark=collection_result.starting_watermark,
                        ending_watermark=collection_result.ending_watermark,
                        status="no_change",
                    )
                    self.catalog.commit_no_change_run(
                        run_id=effective_run_id,
                        source_id=source_id,
                        dataset_id=dataset_id,
                        collection_manifest=collection_manifest,
                        result=result.to_dict(),
                        watermark=collection_result.ending_watermark,
                    )
                    collection_recorded = True
                    return result

                candidate_schema = infer_schema(raw_records)
                drift = detect_schema_drift(
                    self.catalog.latest_schema(
                        source_id, dataset_id
                    ),
                    candidate_schema,
                )
                if drift.has_drift:
                    self.catalog.observe_schema(
                        source_id,
                        dataset_id,
                        candidate_schema,
                        effective_run_id,
                    )
                    raise SchemaDriftBlocked(
                        "unapproved schema drift: "
                        + json.dumps(
                            drift.to_dict(), ensure_ascii=False, sort_keys=True
                        )
                    )

                provenance_run_id, provenance_retrieved_at = (
                    self._collection_provenance(raw_collection)
                )
                artifacts, transform_duplicates = self._materialize(
                    raw_records,
                    raw_collection_hash=raw_collection.collection_hash,
                    provenance_run_id=provenance_run_id,
                    retrieved_at=provenance_retrieved_at,
                    as_of_date=effective_as_of,
                    namespace="main",
                )
                release_id = "%s-%s" % (
                    effective_as_of.replace("-", ""),
                    effective_run_id,
                )
                built_in_validator = self._release_validator(artifacts)
                committed_result: Optional[PipelineResult] = None

                def commit_release(
                    pointer: ReleasePointer,
                    validation: Mapping[str, Any],
                ) -> None:
                    nonlocal committed_result, collection_recorded
                    candidate_result = PipelineResult(
                        run_id=effective_run_id,
                        status="published",
                        source_id=source_id,
                        dataset_id=dataset_id,
                        request_fingerprint=fingerprint,
                        scope_fingerprint=scope_fingerprint,
                        pages_fetched=collection_result.pages_fetched,
                        collected_records=len(captured_payloads),
                        raw_duplicates_dropped=raw_duplicates,
                        transform_duplicates_dropped=transform_duplicates,
                        raw_collection_hash=raw_collection.collection_hash,
                        raw_object_hashes=tuple(
                            page.object_hash for page in raw_collection.pages
                        ),
                        schema_drift=drift.to_dict(),
                        artifacts=artifacts,
                        release=pointer,
                        current_release_id=pointer.release_id,
                    )
                    collection_manifest = self._collection_manifest_contract(
                        effective_run_id,
                        raw_collection,
                        fingerprint,
                        record_count=len(captured_payloads),
                        duplicate_count=raw_duplicates,
                        starting_watermark=collection_result.starting_watermark,
                        ending_watermark=collection_result.ending_watermark,
                        release_id=pointer.release_id,
                        status="published",
                    )
                    self.catalog.commit_published_run(
                        run_id=effective_run_id,
                        source_id=source_id,
                        dataset_id=dataset_id,
                        schema=candidate_schema,
                        collection_manifest=collection_manifest,
                        release=pointer,
                        validation=validation,
                        result=candidate_result.to_dict(),
                        watermark=collection_result.ending_watermark,
                    )
                    committed_result = candidate_result
                    collection_recorded = True

                release = self.release_manager.publish(
                    release_id=release_id,
                    as_of_date=effective_as_of,
                    run_id=effective_run_id,
                    artifacts={
                        "facility_core": artifacts.core,
                        "region_supply_mart": artifacts.mart,
                    },
                    validators=(built_in_validator, *validators),
                    metadata={
                        "source_id": source_id,
                        "dataset_id": dataset_id,
                        "raw_collection_hash": raw_collection.collection_hash,
                        "raw_object_hashes": [
                            page.object_hash for page in raw_collection.pages
                        ],
                        "parser_id": raw_collection.parser_id,
                        "parser_version": raw_collection.parser_version,
                        "scope_fingerprint": scope_fingerprint,
                        "synthetic_sample": source_id == SAMPLE_SOURCE_ID,
                    },
                    commit_callback=commit_release,
                )
                if committed_result is None or committed_result.release != release:
                    raise PipelineError("release metadata commit did not complete")
                return committed_result
            except Exception as exc:
                if raw_collection is not None and not collection_recorded:
                    try:
                        self._record_collection_manifest(
                            effective_run_id,
                            raw_collection,
                            fingerprint,
                            record_count=len(raw_records),
                            duplicate_count=raw_duplicates,
                            starting_watermark=(
                                collection_result.starting_watermark
                                if collection_result is not None
                                else None
                            ),
                            ending_watermark=(
                                collection_result.ending_watermark
                                if collection_result is not None
                                else None
                            ),
                            status="failed",
                        )
                    except Exception:
                        pass
                run = self.catalog.get_run(effective_run_id)
                if run is not None and run["status"] == "running":
                    self.catalog.finish_run(
                        effective_run_id,
                        False,
                        {
                            "type": type(exc).__name__,
                            "message": redact_sensitive(exc),
                        },
                        result={
                            "status": "failed",
                            "raw_collection_hash": (
                                raw_collection.collection_hash
                                if raw_collection is not None
                                else None
                            ),
                            "partial_raw_pages": len(streamed_pages),
                            "partial_raw_object_hashes": [
                                page.object_hash for page in streamed_pages
                            ],
                        },
                    )
                raise

    def rebuild_from_raw(
        self,
        source_id: str,
        dataset_id: str,
        collection_hash: str,
        *,
        as_of_date: Optional[str] = None,
        run_id: Optional[str] = None,
        publish: bool = False,
    ) -> PipelineResult:
        if as_of_date is None:
            raise PipelineError(
                "a raw rebuild requires an explicit as_of_date"
            )
        effective_as_of = as_of_date
        effective_run_id = run_id or _new_run_id("rebuild")
        _validate_pipeline_identity(effective_run_id, effective_as_of)
        reject_sensitive_data(source_id, location="rebuild source id")
        reject_sensitive_data(dataset_id, location="rebuild dataset id")
        reject_sensitive_data(collection_hash, location="raw collection hash")
        _validate_component(source_id, "source_id")
        _validate_component(dataset_id, "dataset_id")
        if not isinstance(collection_hash, str) or re.fullmatch(
            r"[0-9a-f]{64}", collection_hash
        ) is None:
            raise PipelineError(
                "collection_hash must be a lowercase SHA-256 digest"
            )

        raw_collection: Optional[RawCollection] = None
        raw_records: List[Dict[str, Any]] = []
        collection_recorded = False
        with _pipeline_lock(self.settings.data_root):
            self.catalog.start_run(
                effective_run_id,
                "raw_rebuild",
                collection_hash,
            )
            try:
                raw_collection = self.collection_store.read(
                    source_id, dataset_id, collection_hash
                )
                if any(
                    page.status_code == 304 for page in raw_collection.pages
                ):
                    raise PipelineError(
                        "a no-change 304 collection is not a rebuildable snapshot"
                    )
                raw_records = self._parse_raw_collection(raw_collection)
                if not raw_records:
                    raise PipelineError(
                        "a raw rebuild requires a non-empty 2xx snapshot"
                    )
                unique_records, raw_duplicates = _deduplicate_raw_records(
                    raw_records, source_id, dataset_id
                )
                rebuild_drift: Optional[Mapping[str, Any]] = None
                if publish:
                    accepted_schema = self.catalog.latest_schema(
                        source_id, dataset_id
                    )
                    if accepted_schema is None:
                        raise SchemaDriftBlocked(
                            "cannot publish a raw rebuild without an accepted schema baseline"
                        )
                    candidate_schema = infer_schema(raw_records)
                    candidate_drift = detect_schema_drift(
                        accepted_schema, candidate_schema
                    )
                    if candidate_drift.has_drift:
                        raise SchemaDriftBlocked(
                            "raw rebuild schema differs from the accepted baseline: "
                            + json.dumps(
                                candidate_drift.to_dict(),
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                        )
                    rebuild_drift = candidate_drift.to_dict()
                provenance_run_id, provenance_retrieved_at = (
                    self._collection_provenance(raw_collection)
                )
                artifacts, transform_duplicates = self._materialize(
                    raw_records,
                    raw_collection_hash=collection_hash,
                    provenance_run_id=provenance_run_id,
                    retrieved_at=provenance_retrieved_at,
                    as_of_date=effective_as_of,
                    namespace="main",
                )
                if publish:
                    release_id = "%s-%s" % (
                        effective_as_of.replace("-", ""),
                        effective_run_id,
                    )
                    committed_result: Optional[PipelineResult] = None

                    def commit_rebuild_release(
                        pointer: ReleasePointer,
                        validation: Mapping[str, Any],
                    ) -> None:
                        nonlocal committed_result, collection_recorded
                        candidate_result = PipelineResult(
                            run_id=effective_run_id,
                            status="published_rebuild",
                            source_id=source_id,
                            dataset_id=dataset_id,
                            request_fingerprint=None,
                            scope_fingerprint=raw_collection.scope_fingerprint,
                            pages_fetched=len(raw_collection.pages),
                            collected_records=len(unique_records),
                            raw_duplicates_dropped=raw_duplicates,
                            transform_duplicates_dropped=transform_duplicates,
                            raw_collection_hash=collection_hash,
                            raw_object_hashes=tuple(
                                page.object_hash for page in raw_collection.pages
                            ),
                            schema_drift=rebuild_drift,
                            artifacts=artifacts,
                            release=pointer,
                            current_release_id=pointer.release_id,
                        )
                        collection_manifest = self._collection_manifest_contract(
                            effective_run_id,
                            raw_collection,
                            raw_collection.request_fingerprint,
                            record_count=len(unique_records),
                            duplicate_count=raw_duplicates,
                            release_id=pointer.release_id,
                            status="published_rebuild",
                        )
                        self.catalog.commit_published_run(
                            run_id=effective_run_id,
                            source_id=source_id,
                            dataset_id=dataset_id,
                            schema=candidate_schema,
                            collection_manifest=collection_manifest,
                            release=pointer,
                            validation=validation,
                            result=candidate_result.to_dict(),
                        )
                        committed_result = candidate_result
                        collection_recorded = True

                    release = self.release_manager.publish(
                        release_id,
                        effective_as_of,
                        effective_run_id,
                        {
                            "facility_core": artifacts.core,
                            "region_supply_mart": artifacts.mart,
                        },
                        validators=(self._release_validator(artifacts),),
                        metadata={
                            "source_id": source_id,
                            "dataset_id": dataset_id,
                            "raw_collection_hash": collection_hash,
                            "scope_fingerprint": raw_collection.scope_fingerprint,
                            "raw_object_hashes": [
                                page.object_hash for page in raw_collection.pages
                            ],
                            "rebuild": True,
                        },
                        commit_callback=commit_rebuild_release,
                    )
                    if committed_result is None or committed_result.release != release:
                        raise PipelineError("rebuild release metadata commit did not complete")
                    return committed_result

                current = self.release_manager.current()
                result = PipelineResult(
                    run_id=effective_run_id,
                    status="rebuilt",
                    source_id=source_id,
                    dataset_id=dataset_id,
                    request_fingerprint=None,
                    scope_fingerprint=raw_collection.scope_fingerprint,
                    pages_fetched=len(raw_collection.pages),
                    collected_records=len(unique_records),
                    raw_duplicates_dropped=raw_duplicates,
                    transform_duplicates_dropped=transform_duplicates,
                    raw_collection_hash=collection_hash,
                    raw_object_hashes=tuple(
                        page.object_hash for page in raw_collection.pages
                    ),
                    schema_drift=rebuild_drift,
                    artifacts=artifacts,
                    release=None,
                    current_release_id=(
                        current.release_id if current is not None else None
                    ),
                )
                collection_manifest = self._collection_manifest_contract(
                    effective_run_id,
                    raw_collection,
                    raw_collection.request_fingerprint,
                    record_count=len(unique_records),
                    duplicate_count=raw_duplicates,
                    status="rebuilt",
                )
                self.catalog.commit_rebuilt_run(
                    run_id=effective_run_id,
                    source_id=source_id,
                    dataset_id=dataset_id,
                    collection_manifest=collection_manifest,
                    result=result.to_dict(),
                )
                collection_recorded = True
                return result
            except Exception as exc:
                if raw_collection is not None and not collection_recorded:
                    try:
                        _, raw_duplicates = _deduplicate_raw_records(
                            raw_records, source_id, dataset_id
                        )
                        self._record_collection_manifest(
                            effective_run_id,
                            raw_collection,
                            raw_collection.request_fingerprint,
                            record_count=len(raw_records),
                            duplicate_count=raw_duplicates,
                            status="failed_rebuild",
                        )
                    except Exception:
                        pass
                run = self.catalog.get_run(effective_run_id)
                if run is not None and run["status"] == "running":
                    self.catalog.finish_run(
                        effective_run_id,
                        False,
                        {
                            "type": type(exc).__name__,
                            "message": redact_sensitive(exc),
                        },
                        result={
                            "status": "failed",
                            "raw_collection_hash": collection_hash,
                        },
                    )
                raise

    def _parse_raw_collection(
        self, collection: RawCollection
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for page in collection.pages:
            body = self.raw_store.read_bytes(
                page.object_hash,
                collection.source_id,
                collection.dataset_id,
            )
            records.extend(
                self.parser_registry.parse(
                    collection.parser_id,
                    collection.parser_version,
                    body,
                    page.content_type,
                )
            )
        return records

    @staticmethod
    def _collection_provenance(
        collection: RawCollection,
    ) -> Tuple[str, str]:
        return (
            "collection-" + collection.collection_hash[:24],
            max(page.retrieved_at for page in collection.pages),
        )

    def _record_collection_manifest(
        self,
        run_id: str,
        collection: RawCollection,
        request_fingerprint: str,
        *,
        record_count: int,
        duplicate_count: int,
        starting_watermark: Optional[str] = None,
        ending_watermark: Optional[str] = None,
        release_id: Optional[str] = None,
        status: str,
    ) -> None:
        contract = self._collection_manifest_contract(
            run_id,
            collection,
            request_fingerprint,
            record_count=record_count,
            duplicate_count=duplicate_count,
            starting_watermark=starting_watermark,
            ending_watermark=ending_watermark,
            release_id=release_id,
            status=status,
        )
        self.catalog.record_collection_manifest(**contract)

    def _collection_manifest_contract(
        self,
        run_id: str,
        collection: RawCollection,
        request_fingerprint: str,
        *,
        record_count: int,
        duplicate_count: int,
        starting_watermark: Optional[str] = None,
        ending_watermark: Optional[str] = None,
        release_id: Optional[str] = None,
        status: str,
    ) -> Dict[str, Any]:
        checksum_path = (
            self.settings.data_root / collection.manifest_checksum_path
        )
        manifest_checksum = checksum_path.read_text(encoding="ascii").split()[0]
        if request_fingerprint != collection.request_fingerprint:
            raise PipelineError(
                "collection-level request fingerprint does not match raw manifest"
            )
        return {
            "run_id": run_id,
            "source_id": collection.source_id,
            "dataset_id": collection.dataset_id,
            "collection_hash": collection.collection_hash,
            "manifest_relative_path": collection.relative_path,
            "manifest_checksum": manifest_checksum,
            "request_fingerprint": request_fingerprint,
            "scope_fingerprint": collection.scope_fingerprint,
            "page_count": len(collection.pages),
            "record_count": record_count,
            "duplicate_count": duplicate_count,
            "parser_id": collection.parser_id,
            "parser_version": collection.parser_version,
            "starting_watermark": _optional_fingerprint(starting_watermark),
            "ending_watermark": _optional_fingerprint(ending_watermark),
            "release_id": release_id,
            "status": status,
        }

    def _materialize(
        self,
        raw_records: Sequence[Mapping[str, Any]],
        *,
        raw_collection_hash: str,
        provenance_run_id: str,
        retrieved_at: str,
        as_of_date: str,
        namespace: str,
    ) -> tuple[PipelineArtifacts, int]:
        staging = build_facility_staging(
            raw_records,
            run_id=provenance_run_id,
            retrieved_at=retrieved_at,
        )
        base_id = "%s-%s-v1" % (namespace, raw_collection_hash[:16])
        staging_artifact = self.layer_writer.write_jsonl(
            "staging",
            "facility-%s" % base_id,
            staging.records,
            metadata={
                "raw_collection_hash": raw_collection_hash,
                "contract": "m1.facility_staging.v1",
            },
        )
        core = build_facility_core(
            staging.records,
            run_id=provenance_run_id,
            retrieved_at=retrieved_at,
        )
        core_artifact = self.layer_writer.write_jsonl(
            "core",
            "facility-%s" % base_id,
            core.records,
            metadata={
                "staging_checksum": staging_artifact.checksum,
                "contract": "m1.facility_core.v1",
            },
        )
        mart_records = build_region_supply_mart(
            core.records, as_of_date=as_of_date
        )
        mart_artifact = self.layer_writer.write_jsonl(
            "mart",
            "region-supply-%s-%s" % (base_id, as_of_date.replace("-", "")),
            mart_records,
            metadata={
                "core_checksum": core_artifact.checksum,
                "contract": "m1.region_supply.v1",
            },
        )
        return (
            PipelineArtifacts(
                staging=staging_artifact,
                core=core_artifact,
                mart=mart_artifact,
            ),
            staging.duplicates_dropped + core.duplicates_dropped,
        )

    def _release_validator(
        self, artifacts: PipelineArtifacts
    ) -> ReleaseValidator:
        def validate_m1_release(_: Mapping[str, Any]) -> Mapping[str, Any]:
            core_records = self.layer_writer.read_records(artifacts.core)
            mart_records = self.layer_writer.read_records(artifacts.mart)
            duplicate_count = count_duplicate_source_keys(core_records)
            if not core_records:
                raise ReleaseValidationError("core must not be empty")
            if duplicate_count:
                raise ReleaseValidationError("core contains duplicate source keys")
            if sum(int(row["total_facilities"]) for row in mart_records) != len(
                core_records
            ):
                raise ReleaseValidationError(
                    "mart facility totals do not reconcile to core"
                )
            return {
                "core_records": len(core_records),
                "mart_records": len(mart_records),
                "duplicate_source_keys": duplicate_count,
                "reconciled": True,
            }

        return validate_m1_release


def load_jsonl_fixture(path: Path = SAMPLE_FIXTURE_PATH) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise PipelineError(
                    "fixture line %d must be a JSON object" % line_number
                )
            records.append(value)
    if not records:
        raise PipelineError("fixture must contain at least one record")
    return records


def run_sample(
    settings: Settings,
    *,
    fixture_path: Path = SAMPLE_FIXTURE_PATH,
    as_of_date: Optional[str] = None,
    page_size: int = 2,
    run_id: Optional[str] = None,
    transient_failures: Optional[Mapping[Optional[str], int]] = None,
) -> PipelineResult:
    rows = load_jsonl_fixture(fixture_path)
    connector = FixtureConnector(
        rows,
        page_size=page_size,
        dataset=SAMPLE_DATASET_ID,
        source=SAMPLE_SOURCE_ID,
        transient_failures=transient_failures,
    )
    request = Request(
        dataset=SAMPLE_DATASET_ID,
        params={"fixture_contract": "m1.v1"},
        page_size=page_size,
    )
    return DentalMarketPipeline(settings).collect_and_publish(
        connector,
        request,
        as_of_date=as_of_date,
        run_id=run_id,
    )
