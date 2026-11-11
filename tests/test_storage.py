import hashlib
import json
import multiprocessing
import os
import sqlite3
import threading
from dataclasses import replace
from pathlib import Path

import pytest
import dental_market.release as release_module

from dental_market.contracts import (
    ContractViolation,
    RawObject,
    RawIntegrityError,
    ReleasePointer,
    ReleaseValidationError,
    detect_schema_drift,
    infer_schema,
)
from dental_market.metadata_store import OpsCatalog
from dental_market.release import ReleaseManager
from dental_market.security import SensitiveDataError
from dental_market.storage import AtomicLayerWriter, ContentAddressedRawStore


def _crash_during_release_publish(data_root: str, artifact_relative: str) -> None:
    root = Path(data_root)
    catalog = OpsCatalog(root / "ops.sqlite")
    releases = ReleaseManager(root, catalog)

    def terminate_after_pointer(_pointer, _validation):
        os._exit(17)

    releases.publish(
        "crash-release-second",
        "2026-09-02",
        "crash-run-second",
        {"mart": root / artifact_relative},
        commit_callback=terminate_after_pointer,
    )


def _crash_after_committing_corrupt_release(
    data_root: str, artifact_relative: str
) -> None:
    root = Path(data_root)
    catalog = OpsCatalog(root / "ops.sqlite")
    releases = ReleaseManager(root, catalog)

    def commit_corrupt_then_exit(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run("corrupt-crash-run", True)
        (root / artifact_relative).write_bytes(b"corrupt-after-commit\n")
        os._exit(18)

    releases.publish(
        "corrupt-crash-release",
        "2026-09-02",
        "corrupt-crash-run",
        {"mart": root / artifact_relative},
        commit_callback=commit_corrupt_then_exit,
    )


def _source_rows():
    return [
        {
            "source_record_id": "A-1",
            "name": "가족치과",
            "open": True,
            "dentists": 2,
        },
        {
            "source_record_id": "A-2",
            "name": "바다치과",
            "open": False,
            "dentists": 1,
        },
    ]


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _assert_secret_absent(root: Path, secret: bytes) -> None:
    for path in root.rglob("*"):
        if path.is_file():
            assert secret not in path.read_bytes()


def _commit_test_collection_publish(
    catalog: OpsCatalog,
    pointer: ReleasePointer,
    validation,
    *,
    run_id: str,
    scope_fingerprint: str,
    previous_watermark: str,
    new_watermark: str,
):
    schema = infer_schema([{"id": "A"}])
    return catalog.commit_published_run(
        run_id=run_id,
        source_id="fixture",
        dataset_id="facilities",
        schema=schema,
        collection_manifest={
            "run_id": run_id,
            "source_id": "fixture",
            "dataset_id": "facilities",
            "collection_hash": _fingerprint(run_id + ":collection"),
            "manifest_relative_path": "collections/%s/manifest.json" % run_id,
            "manifest_checksum": _fingerprint(run_id + ":manifest"),
            "request_fingerprint": _fingerprint(run_id + ":request"),
            "scope_fingerprint": scope_fingerprint,
            "page_count": 1,
            "record_count": 1,
            "duplicate_count": 0,
            "starting_watermark": _fingerprint(previous_watermark),
            "ending_watermark": _fingerprint(new_watermark),
            "parser_id": "test-parser",
            "parser_version": "1",
            "release_id": pointer.release_id,
            "status": "published",
        },
        release=pointer,
        validation=validation,
        result={"status": "published", "record_count": 1},
        watermark=new_watermark,
    )


def test_raw_is_content_addressed_immutable_and_catalog_deduplicates(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops" / "catalog.sqlite3")
    catalog.start_run("run-001", "sample")
    store = ContentAddressedRawStore(tmp_path, catalog)

    first = store.put_jsonl(
        "fixture",
        "facilities",
        _source_rows(),
        run_id="run-001",
        request_fingerprint=_fingerprint("request-v1"),
        retrieved_at="2026-09-01T00:00:00Z",
    )
    catalog.finish_run("run-001", True)
    catalog.start_run("run-002", "sample")
    second = store.put_jsonl(
        "fixture",
        "facilities",
        _source_rows(),
        run_id="run-002",
        request_fingerprint=_fingerprint("request-v1"),
        retrieved_at="2026-09-01T01:00:00Z",
    )

    assert first.created is True
    assert second.created is False
    assert first.object_hash == second.object_hash
    assert first.relative_path == second.relative_path
    assert store.read_records(second) == _source_rows()
    assert catalog.count_raw_objects() == 1
    assert catalog.count_raw_ingestions() == 2
    assert catalog.find_raw_for_request(
        "fixture", "facilities", _fingerprint("request-v1")
    )["object_hash"] == first.object_hash
    catalog_object = catalog.get_raw_object(
        "fixture", "facilities", first.object_hash
    )
    assert catalog_object is not None
    assert catalog_object["relative_path"] == first.relative_path
    assert catalog_object["schema"] == first.schema

    raw_path = tmp_path / first.relative_path
    raw_path.write_bytes(raw_path.read_bytes() + b"{}\n")
    with pytest.raises(RawIntegrityError):
        store.verify(first)
    with pytest.raises(RawIntegrityError):
        store.put_jsonl(
            "fixture",
            "facilities",
            _source_rows(),
            run_id="run-003",
            request_fingerprint=_fingerprint("request-v1"),
        )


def test_exact_source_bytes_are_compressed_and_manifest_is_protected(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    source_bytes = b'{"z":2,"a":1}\n'
    raw = store.put_bytes(
        "fixture",
        "download",
        source_bytes,
        run_id="raw-bytes",
        request_fingerprint=_fingerprint("request-bytes"),
        retrieved_at="2026-09-01T00:00:00Z",
        content_type="application/x-ndjson",
        original_filename="download.jsonl",
    )

    assert raw.relative_path.endswith(".payload.gz")
    assert store.read_bytes(raw) == source_bytes
    assert (tmp_path / raw.relative_path).read_bytes() != source_bytes
    manifest = store.read_manifest(raw)
    assert manifest["compression"] == "gzip"
    assert manifest["first_original_filename"] == "download.jsonl"
    assert manifest["first_content_type"] == "application/x-ndjson"

    manifest_path = tmp_path / raw.manifest_relative_path
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace(
            "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z"
        ),
        encoding="utf-8",
    )
    with pytest.raises(RawIntegrityError, match="manifest checksum"):
        store.verify(raw)


def test_raw_object_identity_cannot_be_spoofed_with_another_objects_paths(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    first = store.put_bytes(
        "fixture",
        "facilities",
        b"first-body",
        run_id="identity-first",
        request_fingerprint=_fingerprint("identity-first"),
    )
    second = store.put_bytes(
        "fixture",
        "facilities",
        b"second-body",
        run_id="identity-second",
        request_fingerprint=_fingerprint("identity-second"),
    )
    spoofed = replace(
        first,
        relative_path=second.relative_path,
        manifest_relative_path=second.manifest_relative_path,
    )

    with pytest.raises(RawIntegrityError, match="path does not match"):
        store.verify(spoofed)
    with pytest.raises(RawIntegrityError, match="path does not match"):
        store.read_bytes(spoofed)


def test_missing_raw_manifest_sidecar_is_recovered_only_for_exact_retry(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    fingerprint = _fingerprint("recoverable-request")
    raw = store.put_bytes(
        "fixture",
        "facilities",
        b"exact-body",
        run_id="recover-sidecar",
        request_fingerprint=fingerprint,
        retrieved_at="2026-09-01T00:00:00Z",
    )
    checksum_path = (tmp_path / raw.manifest_relative_path).with_suffix(
        ".sha256"
    )
    checksum_path.unlink()

    recovered = store.put_bytes(
        "fixture",
        "facilities",
        b"exact-body",
        run_id="recover-sidecar-after-restart",
        request_fingerprint=fingerprint,
        retrieved_at="2026-09-02T00:00:00Z",
    )
    assert recovered.created is False
    assert checksum_path.is_file()
    assert store.verify(recovered)


def test_missing_raw_manifest_sidecar_rejects_unknown_manifest_shape(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    raw = store.put_bytes(
        "fixture",
        "facilities",
        b"exact-body",
        run_id="sidecar-shape-first",
        request_fingerprint=_fingerprint("sidecar-shape"),
        retrieved_at="2026-09-01T00:00:00Z",
    )
    manifest_path = tmp_path / raw.manifest_relative_path
    checksum_path = manifest_path.with_suffix(".sha256")
    checksum_path.unlink()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["unexpected"] = "untrusted"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RawIntegrityError, match="cannot be safely recovered"):
        store.put_bytes(
            "fixture",
            "facilities",
            b"exact-body",
            run_id="sidecar-shape-restart",
            request_fingerprint=_fingerprint("sidecar-shape"),
            retrieved_at="2026-09-02T00:00:00Z",
        )


def test_existing_manifest_with_valid_sidecar_still_rejects_extra_fields(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    body = b"strict-manifest-body"
    raw = store.put_bytes(
        "fixture",
        "facilities",
        body,
        run_id="strict-manifest-first",
        request_fingerprint=_fingerprint("strict-manifest"),
    )
    manifest_path = tmp_path / raw.manifest_relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["unexpected"] = "benign-looking-but-unaudited"
    manifest_payload = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    manifest_path.write_bytes(manifest_payload)
    manifest_path.with_suffix(".sha256").write_text(
        "%s  %s\n"
        % (hashlib.sha256(manifest_payload).hexdigest(), manifest_path.name),
        encoding="ascii",
    )

    with pytest.raises(RawIntegrityError, match="cannot be safely recovered"):
        store.put_bytes(
            "fixture",
            "facilities",
            body,
            run_id="strict-manifest-second",
            request_fingerprint=_fingerprint("strict-manifest"),
        )


def test_existing_valid_gzip_from_another_interpreter_is_idempotent(
    tmp_path: Path,
):
    store = ContentAddressedRawStore(tmp_path)
    source_bytes = b'exact bytes across gzip implementations\n'
    raw = store.put_bytes(
        "fixture",
        "facilities",
        source_bytes,
        run_id="gzip-runtime-first",
        request_fingerprint=_fingerprint("gzip-runtime"),
        retrieved_at="2026-09-01T00:00:00Z",
    )
    object_path = tmp_path / raw.relative_path
    alternate_gzip = bytearray(object_path.read_bytes())
    assert alternate_gzip[9] == 255
    alternate_gzip[9] = 3
    object_path.write_bytes(bytes(alternate_gzip))

    manifest_path = tmp_path / raw.manifest_relative_path
    checksum_path = manifest_path.with_suffix(".sha256")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["stored_checksum"] = hashlib.sha256(alternate_gzip).hexdigest()
    manifest["byte_size"] = len(alternate_gzip)
    manifest_payload = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    manifest_path.write_bytes(manifest_payload)
    checksum_path.write_text(
        "%s  %s\n"
        % (hashlib.sha256(manifest_payload).hexdigest(), manifest_path.name),
        encoding="ascii",
    )

    repeated = store.put_bytes(
        "fixture",
        "facilities",
        source_bytes,
        run_id="gzip-runtime-second",
        request_fingerprint=_fingerprint("gzip-runtime"),
        retrieved_at="2026-09-02T00:00:00Z",
    )
    assert repeated.created is False
    assert repeated.object_hash == raw.object_hash
    assert store.read_bytes(repeated) == source_bytes
    assert store.verify(repeated)


def test_one_run_request_fingerprint_cannot_silently_point_to_two_bodies(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run("conflict-run", "sample")
    store = ContentAddressedRawStore(tmp_path, catalog)
    fingerprint = _fingerprint("same-logical-request")
    store.put_bytes(
        "fixture",
        "facilities",
        b"first",
        run_id="conflict-run",
        request_fingerprint=fingerprint,
        retrieved_at="2026-09-01T00:00:00Z",
    )
    with pytest.raises(ContractViolation, match="different response"):
        store.put_bytes(
            "fixture",
            "facilities",
            b"second",
            run_id="conflict-run",
            request_fingerprint=fingerprint,
            retrieved_at="2026-09-01T00:00:00Z",
        )


def test_one_run_exact_retry_preserves_first_retrieval_evidence(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run("exact-retry-run", "sample")
    store = ContentAddressedRawStore(tmp_path, catalog)
    fingerprint = _fingerprint("same-logical-request")

    first = store.put_bytes(
        "fixture",
        "facilities",
        b"same-body",
        run_id="exact-retry-run",
        request_fingerprint=fingerprint,
        retrieved_at="2026-09-01T00:00:00Z",
    )
    retried = store.put_bytes(
        "fixture",
        "facilities",
        b"same-body",
        run_id="exact-retry-run",
        request_fingerprint=fingerprint,
        retrieved_at="2026-09-01T00:01:00Z",
    )

    ingestion = catalog.find_raw_for_request(
        "fixture", "facilities", fingerprint
    )
    assert retried.object_hash == first.object_hash
    assert ingestion is not None
    assert ingestion["retrieved_at"] == "2026-09-01T00:00:00Z"
    assert catalog.count_raw_ingestions() == 1


def test_schema_observations_report_added_removed_and_type_changes(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite3")
    baseline_schema = infer_schema(
        [{"id": "A", "count": 1, "old_field": "yes"}]
    )
    candidate_schema = infer_schema(
        [{"id": "B", "count": "2", "new_field": True}]
    )
    baseline = detect_schema_drift(None, baseline_schema)
    drift = detect_schema_drift(baseline_schema, candidate_schema)

    assert baseline.baseline is True
    assert baseline.has_drift is False
    assert drift.has_drift is True
    assert drift.added_fields == ("new_field",)
    assert drift.removed_fields == ("old_field",)
    assert drift.type_changes["count"] == {
        "before": ("integer",),
        "after": ("string",),
    }

    catalog.start_run("run-001", "schema-observation")
    observed = catalog.observe_records_schema(
        "fixture",
        "facilities",
        [{"id": "A", "count": 1, "old_field": "yes"}],
        "run-001",
    )
    assert observed.baseline is True
    assert catalog.latest_schema("fixture", "facilities") is None
    with pytest.raises(ContractViolation, match="published run commit"):
        catalog.observe_schema(
            "fixture",
            "facilities",
            candidate_schema,
            "run-001",
            accept_drift=True,
        )
    with pytest.raises(ContractViolation, match="running run_id"):
        catalog.observe_schema(
            "fixture",
            "facilities",
            candidate_schema,
            "missing-run",
        )


def test_atomic_layer_artifacts_are_idempotent_and_never_overwritten(tmp_path: Path):
    writer = AtomicLayerWriter(tmp_path)
    first = writer.write_jsonl(
        "core", "facility-run-001", [{"facility_id": "one"}]
    )
    same = writer.write_jsonl(
        "core", "facility-run-001", [{"facility_id": "one"}]
    )

    assert first.created is True
    assert same.created is False
    assert first.checksum == same.checksum
    assert writer.read_records(first) == [{"facility_id": "one"}]
    with pytest.raises(ContractViolation):
        writer.write_jsonl(
            "core", "facility-run-001", [{"facility_id": "different"}]
        )
    with pytest.raises(ContractViolation):
        writer.write_jsonl("release", "invalid-layer", [])


def test_durable_raw_and_derived_metadata_reject_credential_shapes(
    tmp_path: Path,
):
    writer = AtomicLayerWriter(tmp_path)
    with pytest.raises(SensitiveDataError, match="sensitive field"):
        writer.write_jsonl(
            "core",
            "secret-metadata",
            [{"id": "A"}],
            metadata={"nested": {"client_secret": "LEAKME"}},
        )
    assert not (tmp_path / "core" / "secret-metadata").exists()

    store = ContentAddressedRawStore(tmp_path)
    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        store.put_bytes(
            "fixture",
            "facilities",
            b"body",
            run_id="secret-content-type",
            request_fingerprint=_fingerprint("secret-content-type"),
            content_type="text/plain; client_secret=LEAKME",
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("retrieved_at", "refresh_token=TIMELEAK"),
        ("schema_version", "client_secret=VERSIONLEAK"),
    ],
)
def test_direct_raw_provenance_rejects_credentials_before_any_write(
    tmp_path: Path, field: str, value: str
):
    store = ContentAddressedRawStore(tmp_path)
    kwargs = {
        "run_id": "secret-provenance",
        "request_fingerprint": _fingerprint("secret-provenance"),
        field: value,
    }
    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        store.put_bytes(
            "fixture",
            "facilities",
            b"body",
            **kwargs,
        )
    assert not (tmp_path / "raw").exists()


def test_direct_catalog_watermark_rejects_secret_identifiers(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        catalog.set_watermark(
            "client_secret=WMLEAK",
            "facilities",
            "safe-watermark",
            run_id="safe-run",
            scope_fingerprint="a" * 64,
        )

    assert catalog.get_watermark(
        "safe-source", "facilities", scope_fingerprint="a" * 64
    ) is None
    _assert_secret_absent(tmp_path, b"WMLEAK")


def test_direct_catalog_schema_rejects_secret_keys(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.start_run("safe-schema-run", "test")

    with pytest.raises(SensitiveDataError, match="sensitive field"):
        catalog.observe_schema(
            "safe-source",
            "facilities",
            {"note-client_secret=SCHEMALEAK": ["string"]},
            "safe-schema-run",
        )

    with sqlite3.connect(str(catalog.path)) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM schema_observations"
        ).fetchone()[0] == 0
    _assert_secret_absent(tmp_path, b"SCHEMALEAK")


def test_direct_catalog_raw_object_rejects_secret_fields(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    raw = RawObject(
        source_id="safe-source",
        dataset_id="facilities",
        object_hash="a" * 64,
        relative_path="raw/safe-source/facilities/aa/" + "a" * 64 + ".payload.gz",
        manifest_relative_path=(
            "raw/safe-source/facilities/aa/" + "a" * 64 + ".manifest.json"
        ),
        byte_size=1,
        record_count=1,
        schema={"id": ["string"]},
        schema_fingerprint="b" * 64,
        schema_version="1",
        run_id="client_secret=RAWLEAK",
        request_fingerprint="c" * 64,
        retrieved_at="2026-09-01T00:00:00Z",
        created=True,
    )

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        catalog.record_raw_object(raw)

    assert catalog.count_raw_objects() == 0
    assert catalog.count_raw_ingestions() == 0
    _assert_secret_absent(tmp_path, b"RAWLEAK")


def test_failed_candidate_cannot_replace_last_good_release(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops" / "catalog.sqlite3")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first_mart = writer.write_jsonl(
        "mart", "market-2026-09-01", [{"candidate_id": "A", "score": 1}]
    )
    first_pointer = releases.publish(
        "release-001",
        "2026-09-01",
        "run-001",
        {"market": first_mart},
    )

    second_mart = writer.write_jsonl(
        "mart", "market-2026-09-02", [{"candidate_id": "B", "score": 2}]
    )

    def reject_candidate(_candidate):
        raise ReleaseValidationError("forced quality-gate failure")

    with pytest.raises(ReleaseValidationError):
        releases.publish(
            "release-002",
            "2026-09-02",
            "run-002",
            {"market": second_mart},
            validators=(reject_candidate,),
        )

    current = releases.current()
    assert current is not None
    assert current.release_id == first_pointer.release_id
    assert releases.current_manifest()["release_id"] == "release-001"
    assert catalog.get_release("release-001")["status"] == "published"
    assert catalog.get_release("release-002")["status"] == "failed"
    assert not (tmp_path / "releases" / "release-002").exists()


def test_noncallback_publish_rejects_an_existing_unfinished_run(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl(
        "mart", "unfinished-default-publish", [{"id": "A"}]
    )
    catalog.start_run("unfinished-run", "test")

    with pytest.raises(ContractViolation, match="unfinished run"):
        releases.publish(
            "unfinished-release",
            "2026-09-01",
            "unfinished-run",
            {"mart": artifact},
        )

    assert releases.current() is None
    assert catalog.get_release("unfinished-release") is None
    assert catalog.get_run("unfinished-run")["status"] == "running"


def test_pointer_replace_failure_keeps_catalog_and_current_on_last_good(
    tmp_path: Path, monkeypatch
):
    import dental_market.release as release_module

    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first = writer.write_jsonl("mart", "pointer-first", [{"id": "A"}])
    first_pointer = releases.publish(
        "pointer-release-first",
        "2026-09-01",
        "pointer-run-first",
        {"mart": first},
    )
    second = writer.write_jsonl("mart", "pointer-second", [{"id": "B"}])
    real_replace = release_module.os.replace

    def fail_new_pointer(source, destination):
        if (
            Path(destination).name == "current.json"
            and Path(source).name.startswith(".current.tmp-")
        ):
            raise OSError("forced pointer replacement failure")
        return real_replace(source, destination)

    monkeypatch.setattr(release_module.os, "replace", fail_new_pointer)
    with pytest.raises(OSError, match="forced pointer"):
        releases.publish(
            "pointer-release-second",
            "2026-09-02",
            "pointer-run-second",
            {"mart": second},
        )

    assert releases.current() == first_pointer
    assert catalog.get_release("pointer-release-first")["status"] == "published"
    assert catalog.get_release("pointer-release-second")["status"] == "failed"


def test_restart_recovers_process_crash_between_pointer_and_metadata_commit(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first = writer.write_jsonl("mart", "crash-first", [{"id": "A"}])
    first_pointer = releases.publish(
        "crash-release-first",
        "2026-09-01",
        "crash-run-first",
        {"mart": first},
    )
    second = writer.write_jsonl("mart", "crash-second", [{"id": "B"}])
    catalog.start_run("crash-run-second", "publish-crash-test")

    process = multiprocessing.get_context("spawn").Process(
        target=_crash_during_release_publish,
        args=(str(tmp_path), second.relative_path),
    )
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 17
    assert json.loads(
        (tmp_path / "releases" / "current.json").read_text(encoding="utf-8")
    )["release_id"] == "crash-release-second"
    assert (tmp_path / "releases" / ".publish-intent.json").is_file()

    restarted_catalog = OpsCatalog(tmp_path / "ops.sqlite")
    restarted = ReleaseManager(tmp_path, restarted_catalog)
    assert restarted.current() == first_pointer
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()
    assert restarted_catalog.get_release("crash-release-second")["status"] == "failed"
    assert restarted_catalog.get_run("crash-run-second")["status"] == "failed"


def test_restart_quarantines_committed_release_with_corrupt_artifact(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "corrupt-crash-baseline", [{"id": "baseline"}]
    )
    baseline = releases.publish(
        "corrupt-crash-baseline-release",
        "2026-09-01",
        "corrupt-crash-baseline-run",
        {"mart": baseline_artifact},
    )
    corrupt_artifact = writer.write_jsonl(
        "mart", "corrupt-crash-candidate", [{"id": "candidate"}]
    )
    catalog.start_run("corrupt-crash-run", "test")

    process = multiprocessing.get_context("spawn").Process(
        target=_crash_after_committing_corrupt_release,
        args=(str(tmp_path), corrupt_artifact.relative_path),
    )
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 18
    assert json.loads(
        (tmp_path / "releases" / "current.json").read_text(encoding="utf-8")
    )["release_id"] == "corrupt-crash-release"
    assert (tmp_path / "releases" / ".publish-intent.json").is_file()
    assert catalog.get_release("corrupt-crash-release")["status"] == "published"
    assert catalog.get_run("corrupt-crash-run")["status"] == "succeeded"

    restarted_catalog = OpsCatalog(tmp_path / "ops.sqlite")
    restarted = ReleaseManager(tmp_path, restarted_catalog)
    assert restarted.current() == baseline
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()
    assert restarted_catalog.get_release("corrupt-crash-release")["status"] == "failed"
    assert restarted_catalog.get_run("corrupt-crash-run")["status"] == "failed"


def test_intent_cleanup_failure_after_commit_keeps_new_release_authoritative(
    tmp_path: Path, monkeypatch
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first = writer.write_jsonl("mart", "intent-clear-first", [{"id": "A"}])
    releases.publish(
        "intent-clear-release-first",
        "2026-09-01",
        "intent-clear-run-first",
        {"mart": first},
    )
    second = writer.write_jsonl("mart", "intent-clear-second", [{"id": "B"}])
    real_clear = releases._clear_publish_intent
    failures = 0

    def fail_clear_once(expected_intent_id=None):
        nonlocal failures
        if failures == 0:
            failures += 1
            raise OSError("forced intent cleanup failure")
        return real_clear(expected_intent_id)

    monkeypatch.setattr(releases, "_clear_publish_intent", fail_clear_once)
    pointer = releases.publish(
        "intent-clear-release-second",
        "2026-09-02",
        "intent-clear-run-second",
        {"mart": second},
    )

    assert pointer.release_id == "intent-clear-release-second"
    assert releases.current() == pointer
    assert catalog.get_release("intent-clear-release-second")["status"] == "published"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_catalogless_reader_preserves_ambiguous_committed_publish_intent(
    tmp_path: Path, monkeypatch
):
    catalog_path = tmp_path / "ops.sqlite"
    catalog = OpsCatalog(catalog_path)
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first_artifact = writer.write_jsonl(
        "mart", "readonly-intent-first", [{"id": "A"}]
    )
    first = releases.publish(
        "readonly-intent-release-first",
        "2026-09-01",
        "readonly-intent-run-first",
        {"mart": first_artifact},
    )
    second_artifact = writer.write_jsonl(
        "mart", "readonly-intent-second", [{"id": "B"}]
    )

    def leave_committed_intent(_expected_intent_id=None):
        raise OSError("forced committed intent residue")

    monkeypatch.setattr(releases, "_clear_publish_intent", leave_committed_intent)
    second = releases.publish(
        "readonly-intent-release-second",
        "2026-09-02",
        "readonly-intent-run-second",
        {"mart": second_artifact},
    )
    intent_path = tmp_path / "releases" / ".publish-intent.json"
    monkeypatch.setattr(
        ReleaseManager,
        "_pid_is_alive",
        staticmethod(lambda _pid: False),
    )

    readonly = ReleaseManager(tmp_path)
    assert readonly.current() == first
    assert intent_path.is_file()
    assert json.loads(
        (tmp_path / "releases" / "current.json").read_text(encoding="utf-8")
    )["release_id"] == second.release_id

    recovered = ReleaseManager(tmp_path, OpsCatalog(catalog_path))
    assert recovered.current() == second
    assert not intent_path.exists()


def test_commit_callback_must_finish_catalog_before_new_pointer_is_visible(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "callback-noop", [{"id": "A"}])

    with pytest.raises(ContractViolation, match="did not publish catalog"):
        releases.publish(
            "callback-noop-release",
            "2026-09-01",
            "callback-noop-run",
            {"mart": artifact},
            commit_callback=lambda _pointer, _validation: None,
        )

    assert releases.current() is None
    assert catalog.get_release("callback-noop-release")["status"] == "failed"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_callback_exact_commit_then_raise_keeps_published_state(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl(
        "mart", "callback-committed-exception", [{"id": "A"}]
    )
    run_id = "callback-committed-exception-run"
    release_id = "callback-committed-exception-release"
    scope = _fingerprint("callback-committed-exception-scope")
    catalog.set_watermark(
        "fixture",
        "facilities",
        "watermark-before",
        run_id="watermark-seed-run",
        updated_at="2026-09-01T00:00:00Z",
        scope_fingerprint=scope,
    )
    catalog.start_run(run_id, "callback-committed-exception")

    def commit_then_raise(pointer, validation):
        _commit_test_collection_publish(
            catalog,
            pointer,
            validation,
            run_id=run_id,
            scope_fingerprint=scope,
            previous_watermark="watermark-before",
            new_watermark="watermark-after",
        )
        raise RuntimeError("callback raised after exact commit")

    with pytest.raises(RuntimeError, match="after exact commit"):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {"mart": artifact},
            commit_callback=commit_then_raise,
        )

    pointer = releases.current()
    assert pointer is not None
    assert pointer.release_id == release_id
    assert catalog.get_release(release_id)["status"] == "published"
    assert catalog.get_run(run_id)["status"] == "succeeded"
    assert catalog.get_collection_manifest(
        run_id, "fixture", "facilities"
    )["status"] == "published"
    assert catalog.latest_schema("fixture", "facilities") == infer_schema(
        [{"id": "A"}]
    )
    assert catalog.get_watermark(
        "fixture", "facilities", scope_fingerprint=scope
    ) == "watermark-after"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_callback_artifact_mutation_compensates_full_published_run(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "callback-compensation-baseline", [{"id": "baseline"}]
    )
    baseline = releases.publish(
        "callback-compensation-baseline-release",
        "2026-09-01",
        "callback-compensation-baseline-run",
        {"mart": baseline_artifact},
    )
    artifact = writer.write_jsonl(
        "mart", "callback-compensation-invalid", [{"id": "A"}]
    )
    run_id = "callback-compensation-invalid-run"
    release_id = "callback-compensation-invalid-release"
    scope = _fingerprint("callback-compensation-invalid-scope")
    catalog.set_watermark(
        "fixture",
        "facilities",
        "watermark-before",
        run_id="watermark-seed-run",
        updated_at="2026-09-01T00:00:00Z",
        scope_fingerprint=scope,
    )
    catalog.start_run(run_id, "callback-compensation-invalid")

    def commit_then_mutate(pointer, validation):
        _commit_test_collection_publish(
            catalog,
            pointer,
            validation,
            run_id=run_id,
            scope_fingerprint=scope,
            previous_watermark="watermark-before",
            new_watermark="watermark-after",
        )
        (tmp_path / artifact.relative_path).write_bytes(
            b"tampered-after-catalog-commit\n"
        )

    with pytest.raises(ReleaseValidationError, match="checksum changed"):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {"mart": artifact},
            commit_callback=commit_then_mutate,
        )

    assert releases.current() == baseline
    assert catalog.get_release(release_id)["status"] == "failed"
    assert catalog.get_run(run_id)["status"] == "failed"
    collection = catalog.get_collection_manifest(
        run_id, "fixture", "facilities"
    )
    assert collection["status"] == "failed"
    assert collection["release_id"] is None
    assert catalog.latest_schema("fixture", "facilities") is None
    with sqlite3.connect(str(catalog.path)) as connection:
        accepted = connection.execute(
            "SELECT accepted FROM schema_observations WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        watermark_row = connection.execute(
            """
            SELECT watermark_value, run_id FROM watermarks
            WHERE source_id = 'fixture' AND dataset_id = 'facilities'
              AND scope_fingerprint = ?
            """,
            (scope,),
        ).fetchone()
    assert accepted == (0,)
    assert watermark_row == ("watermark-before", "watermark-seed-run")
    history = catalog.list_watermark_history(
        "fixture", "facilities", scope_fingerprint=scope
    )
    assert [
        (row["previous_watermark"], row["new_watermark"], row["run_id"])
        for row in history
    ] == [
        (None, "watermark-before", "watermark-seed-run"),
        ("watermark-before", "watermark-after", run_id),
        ("watermark-after", "watermark-before", run_id),
    ]
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


@pytest.mark.parametrize("target", ["pointer", "release_manifest"])
def test_callback_cannot_mutate_published_pointer_or_release_manifest(
    tmp_path: Path, target: str
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "callback-boundary-baseline", [{"id": "baseline"}]
    )
    baseline = releases.publish(
        "callback-boundary-baseline-release",
        "2026-09-01",
        "callback-boundary-baseline-run",
        {"mart": baseline_artifact},
    )
    candidate_artifact = writer.write_jsonl(
        "mart", "callback-boundary-candidate", [{"id": "candidate"}]
    )
    run_id = "callback-boundary-" + target + "-run"
    release_id = "callback-boundary-" + target + "-release"
    catalog.start_run(run_id, "test")

    def commit_then_mutate(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run(run_id, True)
        if target == "pointer":
            (tmp_path / "releases" / "current.json").write_text(
                json.dumps(
                    baseline.to_dict(), sort_keys=True, separators=(",", ":")
                )
                + "\n",
                encoding="utf-8",
            )
        else:
            (tmp_path / pointer.manifest_relative_path).write_bytes(b"{}\n")

    with pytest.raises(
        ReleaseValidationError, match="pointer changed|manifest changed"
    ):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {"mart": candidate_artifact},
            commit_callback=commit_then_mutate,
        )

    assert releases.current() == baseline
    assert catalog.get_release(release_id)["status"] == "failed"
    assert catalog.get_run(run_id)["status"] == "failed"


def test_compensation_failure_preserves_pointer_and_intent_for_restart(
    tmp_path: Path, monkeypatch
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "compensation-retry-baseline", [{"id": "baseline"}]
    )
    baseline = releases.publish(
        "compensation-retry-baseline-release",
        "2026-09-01",
        "compensation-retry-baseline-run",
        {"mart": baseline_artifact},
    )
    candidate = writer.write_jsonl(
        "mart", "compensation-retry-candidate", [{"id": "candidate"}]
    )
    run_id = "compensation-retry-run"
    release_id = "compensation-retry-release"
    catalog.start_run(run_id, "test")

    def commit_then_corrupt(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run(run_id, True)
        (tmp_path / candidate.relative_path).write_bytes(b"corrupt\n")

    real_quarantine = catalog.quarantine_invalid_publish_commit

    def fail_compensation(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked during compensation")

    monkeypatch.setattr(
        catalog, "quarantine_invalid_publish_commit", fail_compensation
    )
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {"mart": candidate},
            commit_callback=commit_then_corrupt,
        )

    assert json.loads(
        (tmp_path / "releases" / "current.json").read_text(encoding="utf-8")
    )["release_id"] == release_id
    assert (tmp_path / "releases" / ".publish-intent.json").is_file()
    assert catalog.get_release(release_id)["status"] == "published"
    assert catalog.get_run(run_id)["status"] == "succeeded"

    monkeypatch.setattr(
        catalog, "quarantine_invalid_publish_commit", real_quarantine
    )
    restarted = ReleaseManager(tmp_path, catalog)
    assert restarted.current() == baseline
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()
    assert catalog.get_release(release_id)["status"] == "failed"
    assert catalog.get_run(run_id)["status"] == "failed"


def test_callback_published_release_with_failed_run_is_fully_quarantined(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "failed-run-baseline", [{"id": "baseline"}]
    )
    baseline = releases.publish(
        "failed-run-baseline-release",
        "2026-09-01",
        "failed-run-baseline-run",
        {"mart": baseline_artifact},
    )
    candidate = writer.write_jsonl(
        "mart", "failed-run-candidate", [{"id": "candidate"}]
    )
    run_id = "failed-run-callback-run"
    release_id = "failed-run-callback-release"
    catalog.start_run(run_id, "test")

    def publish_release_but_fail_run(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run(run_id, False, {"message": "planned failure"})

    with pytest.raises(ContractViolation, match="did not publish catalog"):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {"mart": candidate},
            commit_callback=publish_release_but_fail_run,
        )

    assert releases.current() == baseline
    assert catalog.get_release(release_id)["status"] == "failed"
    assert catalog.get_run(run_id)["status"] == "failed"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_post_commit_verification_read_failure_never_rolls_pointer_back(
    tmp_path: Path, monkeypatch
):
    catalog_path = tmp_path / "ops.sqlite"
    catalog = OpsCatalog(catalog_path)
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    first_artifact = writer.write_jsonl(
        "mart", "post-commit-read-first", [{"id": "A"}]
    )
    releases.publish(
        "post-commit-read-release-first",
        "2026-09-01",
        "post-commit-read-run-first",
        {"mart": first_artifact},
    )
    second_artifact = writer.write_jsonl(
        "mart", "post-commit-read-second", [{"id": "B"}]
    )
    catalog.start_run("post-commit-read-run-second", "test")
    real_get_release = catalog.get_release
    def fail_verification_read(release_id):
        row = real_get_release(release_id)
        if (
            release_id == "post-commit-read-release-second"
            and row is not None
            and row["status"] == "published"
        ):
            raise sqlite3.OperationalError("forced post-commit read failure")
        return row

    def commit_release(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run("post-commit-read-run-second", True)

    monkeypatch.setattr(catalog, "get_release", fail_verification_read)
    with pytest.raises(sqlite3.OperationalError, match="post-commit read"):
        releases.publish(
            "post-commit-read-release-second",
            "2026-09-02",
            "post-commit-read-run-second",
            {"mart": second_artifact},
            commit_callback=commit_release,
        )

    assert real_get_release("post-commit-read-release-second")["status"] == "published"
    assert json.loads(
        (tmp_path / "releases" / "current.json").read_text(encoding="utf-8")
    )["release_id"] == "post-commit-read-release-second"
    assert (tmp_path / "releases" / ".publish-intent.json").is_file()

    restarted = ReleaseManager(tmp_path, OpsCatalog(catalog_path))
    assert restarted.current().release_id == "post-commit-read-release-second"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_missing_artifact_is_rejected_without_creating_a_pointer(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite3")
    releases = ReleaseManager(tmp_path, catalog)
    with pytest.raises(ReleaseValidationError):
        releases.publish(
            "release-missing",
            "2026-09-01",
            "run-001",
            {"market": tmp_path / "mart" / "missing.jsonl"},
        )
    assert releases.current() is None
    assert catalog.get_release("release-missing")["status"] == "failed"


def test_current_release_revalidates_published_artifact_bytes(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite3")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    mart = writer.write_jsonl(
        "mart", "market-verified", [{"candidate_id": "A"}]
    )
    releases.publish(
        "release-verified",
        "2026-09-01",
        "run-verified",
        {"market": mart},
    )
    assert releases.current() is not None

    (tmp_path / mart.relative_path).write_bytes(b"tampered\n")
    with pytest.raises(ReleaseValidationError, match="artifact checksum changed"):
        releases.current()


def test_release_failure_catalog_redacts_validator_credentials(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    mart = writer.write_jsonl("mart", "secret-failure", [{"id": "A"}])

    def leak_secret(_candidate):
        raise ReleaseValidationError(
            "Authorization: Digest response=TOPSECRET"
        )

    with pytest.raises(ReleaseValidationError):
        releases.publish(
            "release-secret",
            "2026-09-01",
            "run-secret",
            {"market": mart},
            validators=(leak_secret,),
        )
    stored = catalog.get_release("release-secret")["error_json"]
    assert "TOPSECRET" not in stored
    assert "[REDACTED]" in stored


@pytest.mark.parametrize(
    "metadata,validator_result",
    [
        ({"nested": {"HIRA_API_KEY": "TOPSECRET"}}, None),
        ({}, {"client_secret": "METASECRET"}),
    ],
)
def test_release_rejects_sensitive_success_evidence(
    tmp_path: Path, metadata, validator_result
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    mart = writer.write_jsonl("mart", "sensitive-evidence", [{"id": "A"}])

    def evidence_validator(_candidate):
        return validator_result

    with pytest.raises(ValueError, match="sensitive field"):
        releases.publish(
            "release-sensitive-evidence",
            "2026-09-01",
            "run-sensitive-evidence",
            {"market": mart},
            validators=(evidence_validator,),
            metadata=metadata,
        )
    assert releases.current() is None
    stored = catalog.get_release("release-sensitive-evidence")
    assert stored["status"] == "failed"
    assert "TOPSECRET" not in str(stored)
    assert "METASECRET" not in str(stored)


@pytest.mark.parametrize("target", ["metadata", "artifact"])
def test_release_validator_cannot_mutate_the_candidate_contract(
    tmp_path: Path, target: str
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    mart = writer.write_jsonl("mart", "immutable-validator", [{"id": "A"}])

    def mutating_validator(candidate):
        if target == "metadata":
            candidate["metadata"]["api_key"] = "VALIDATORLEAK"
        else:
            candidate["artifacts"]["market"]["relative_path"] = "forged.jsonl"

    with pytest.raises(TypeError):
        releases.publish(
            "release-immutable-validator-" + target,
            "2026-09-01",
            "run-immutable-validator-" + target,
            {"market": mart},
            validators=(mutating_validator,),
            metadata={"source_id": "fixture"},
        )
    assert releases.current() is None
    stored = catalog.get_release("release-immutable-validator-" + target)
    assert stored["status"] == "failed"
    assert "VALIDATORLEAK" not in str(stored)


def test_release_metadata_is_detached_from_caller_owned_nested_values(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    mart = writer.write_jsonl("mart", "detached-metadata", [{"id": "A"}])
    metadata = {"scope": {"region": "A"}}

    def mutate_caller_copy(_candidate):
        metadata["scope"]["region"] = "B"

    pointer = releases.publish(
        "release-detached-metadata",
        "2026-09-01",
        "run-detached-metadata",
        {"market": mart},
        validators=(mutate_caller_copy,),
        metadata=metadata,
    )
    manifest = json.loads(
        (tmp_path / pointer.manifest_relative_path).read_text(encoding="utf-8")
    )
    assert metadata["scope"]["region"] == "B"
    assert manifest["metadata"]["scope"]["region"] == "A"


def test_release_rejects_secret_as_of_date_before_catalog_write(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "secret-as-of", [{"id": "A"}])

    with pytest.raises(SensitiveDataError, match="credential-shaped"):
        releases.publish(
            "secret-as-of-release",
            "client_secret=ASOFLEAK",
            "secret-as-of-run",
            {"mart": artifact},
        )

    assert catalog.get_release("secret-as-of-release") is None
    assert b"ASOFLEAK" not in catalog.path.read_bytes()


def test_release_metadata_rejects_credential_pattern_in_key(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "secret-key", [{"id": "A"}])

    with pytest.raises(SensitiveDataError, match="sensitive field"):
        releases.publish(
            "secret-key-release",
            "2026-09-01",
            "secret-key-run",
            {"mart": artifact},
            metadata={"note-client_secret=KEYLEAK": "safe"},
        )

    assert releases.current() is None
    assert b"KEYLEAK" not in catalog.path.read_bytes()


def test_validator_artifact_mutation_is_caught_before_publication(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "validator-toctou", [{"id": "A"}])

    def mutate_artifact(candidate):
        relative = candidate["artifacts"]["mart"]["relative_path"]
        (tmp_path / relative).write_bytes(b"tampered-by-validator\n")
        return {"mutated": True}

    with pytest.raises(ReleaseValidationError, match="checksum changed"):
        releases.publish(
            "validator-toctou-release",
            "2026-09-01",
            "validator-toctou-run",
            {"mart": artifact},
            validators=(mutate_artifact,),
        )

    assert releases.current() is None
    assert catalog.get_release("validator-toctou-release")["status"] == "failed"


def test_derived_artifact_manifest_is_bound_to_the_artifact_contract(
    tmp_path: Path,
):
    writer = AtomicLayerWriter(tmp_path)
    artifact = writer.write_jsonl(
        "core",
        "strict-derived-manifest",
        [{"id": "A"}],
        metadata={"source": "fixture"},
    )
    manifest_path = tmp_path / artifact.manifest_relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["layer"] = "mart"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RawIntegrityError, match="manifest checksum"):
        writer.verify(artifact)
    with pytest.raises(ContractViolation, match="immutable contract"):
        writer.write_jsonl(
            "core",
            "strict-derived-manifest",
            [{"id": "A"}],
            metadata={"source": "fixture"},
        )


def test_published_release_catalog_row_is_immutable(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    catalog.record_release_candidate("immutable-release", "2026-09-01", "run-a")
    catalog.mark_release_published(
        "immutable-release",
        "releases/immutable-release/manifest.json",
        "a" * 64,
        {"checks": "passed"},
        "2026-09-01T00:00:00Z",
    )
    original = catalog.get_release("immutable-release")

    with pytest.raises(ContractViolation, match="immutable"):
        catalog.mark_release_published(
            "immutable-release",
            "releases/immutable-release/wrong.json",
            "b" * 64,
            {"checks": "passed"},
            "2026-09-01T00:00:00Z",
        )

    assert catalog.get_release("immutable-release") == original


def test_callback_cannot_publish_metadata_outside_bound_pointer(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "bound-callback", [{"id": "A"}])

    def publish_wrong_pointer(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            "releases/wrong/manifest.json",
            "b" * 64,
            validation,
            pointer.published_at,
        )

    with pytest.raises(ContractViolation, match="bound candidate"):
        releases.publish(
            "bound-callback-release",
            "2026-09-01",
            "bound-callback-run",
            {"mart": artifact},
            commit_callback=publish_wrong_pointer,
        )

    assert releases.current() is None
    assert catalog.get_release("bound-callback-release")["status"] == "failed"


def test_same_run_retries_exact_candidate_after_crash_before_intent(
    tmp_path: Path, monkeypatch
):
    class SimulatedBindCrash(BaseException):
        pass

    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl(
        "mart", "bound-retry-artifact", [{"id": "A"}]
    )
    run_id = "bound-retry-run"
    release_id = "bound-retry-release"
    catalog.start_run(run_id, "test")
    timestamps = iter(
        (
            "2026-09-01T00:00:00Z",
            "2026-09-01T00:00:01Z",
            "2026-09-01T00:00:02Z",
        )
    )
    monkeypatch.setattr(release_module, "utc_now_iso", lambda: next(timestamps))
    real_bind = catalog.bind_release_candidate

    def bind_then_crash(pointer, validation):
        real_bind(pointer, validation)
        raise SimulatedBindCrash("power loss after candidate binding")

    monkeypatch.setattr(catalog, "bind_release_candidate", bind_then_crash)
    with pytest.raises(SimulatedBindCrash):
        releases.publish(
            release_id,
            "2026-09-01",
            run_id,
            {"mart": artifact},
            commit_callback=lambda _pointer, _validation: None,
        )
    bound = catalog.get_release(release_id)
    assert bound is not None
    assert bound["status"] == "candidate"
    assert bound["published_at"] == "2026-09-01T00:00:00Z"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()

    monkeypatch.setattr(catalog, "bind_release_candidate", real_bind)

    def commit(pointer, validation):
        catalog.mark_release_published(
            pointer.release_id,
            pointer.manifest_relative_path,
            pointer.manifest_checksum,
            validation,
            pointer.published_at,
        )
        catalog.finish_run(run_id, True)

    pointer = ReleaseManager(tmp_path, catalog).publish(
        release_id,
        "2026-09-01",
        run_id,
        {"mart": artifact},
        commit_callback=commit,
    )

    assert pointer.published_at == "2026-09-01T00:00:00Z"
    assert releases.current() == pointer
    assert catalog.get_release(release_id)["status"] == "published"


def test_current_rejects_unknown_manifest_fields_even_with_updated_pointer_hash(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "strict-current", [{"id": "A"}])
    pointer = releases.publish(
        "strict-current-release",
        "2026-09-01",
        "strict-current-run",
        {"mart": artifact},
    )
    manifest_path = tmp_path / pointer.manifest_relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["unexpected"] = "unaudited"
    manifest_payload = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    manifest_path.write_bytes(manifest_payload)
    pointer_path = tmp_path / "releases" / "current.json"
    pointer_payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer_payload["manifest_checksum"] = hashlib.sha256(
        manifest_payload
    ).hexdigest()
    pointer_path.write_text(
        json.dumps(pointer_payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseValidationError, match="manifest fields"):
        releases.current()


def test_current_manifest_returns_the_exact_validated_byte_snapshot(
    tmp_path: Path, monkeypatch
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    artifact = writer.write_jsonl("mart", "snapshot-manifest", [{"id": "A"}])
    pointer = releases.publish(
        "snapshot-manifest-release",
        "2026-09-01",
        "snapshot-manifest-run",
        {"mart": artifact},
        metadata={"scope": "trusted"},
    )
    manifest_path = tmp_path / pointer.manifest_relative_path
    trusted_payload = manifest_path.read_bytes()
    forged = json.loads(trusted_payload.decode("utf-8"))
    forged["metadata"] = {"scope": "forged-after-read"}
    forged_payload = (
        json.dumps(
            forged,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    real_read_bytes = Path.read_bytes
    swapped = False

    def swap_after_snapshot(path):
        nonlocal swapped
        payload = real_read_bytes(path)
        if path == manifest_path and not swapped:
            swapped = True
            path.write_bytes(forged_payload)
        return payload

    monkeypatch.setattr(Path, "read_bytes", swap_after_snapshot)
    manifest = releases.current_manifest()

    assert swapped is True
    assert manifest["metadata"] == {"scope": "trusted"}
    with pytest.raises(ReleaseValidationError, match="checksum changed"):
        releases.current_manifest()


def test_publish_intent_install_is_exclusive_and_preserves_first_owner(
    tmp_path: Path,
):
    first_manager = ReleaseManager(tmp_path)
    second_manager = ReleaseManager(tmp_path)
    first_pointer = ReleasePointer(
        "intent-owner-first",
        "releases/intent-owner-first/manifest.json",
        "a" * 64,
        "2026-09-01T00:00:00Z",
    )
    second_pointer = ReleasePointer(
        "intent-owner-second",
        "releases/intent-owner-second/manifest.json",
        "b" * 64,
        "2026-09-01T00:00:01Z",
    )
    first_id = first_manager._write_publish_intent(
        first_pointer, None, requires_succeeded_run=False
    )

    with pytest.raises(ReleaseValidationError, match="another release"):
        second_manager._write_publish_intent(
            second_pointer, None, requires_succeeded_run=False
        )
    stored = first_manager._read_publish_intent()
    assert stored is not None
    assert stored["intent_id"] == first_id
    assert stored["release_id"] == first_pointer.release_id
    first_manager._clear_publish_intent(first_id)


def test_concurrent_publish_rollback_restores_the_serialized_previous_pointer(
    tmp_path: Path, monkeypatch
):
    import dental_market.release as release_module

    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    baseline_manager = ReleaseManager(tmp_path, catalog)
    baseline_artifact = writer.write_jsonl(
        "mart", "serialized-baseline", [{"id": "baseline"}]
    )
    baseline_manager.publish(
        "serialized-release-baseline",
        "2026-09-01",
        "serialized-run-baseline",
        {"mart": baseline_artifact},
    )
    first_artifact = writer.write_jsonl(
        "mart", "serialized-first", [{"id": "first"}]
    )
    second_artifact = writer.write_jsonl(
        "mart", "serialized-second", [{"id": "second"}]
    )
    first_manager = ReleaseManager(tmp_path, catalog)
    second_manager = ReleaseManager(tmp_path, catalog)
    catalog.start_run("serialized-run-second", "test")

    first_at_pointer_replace = threading.Event()
    allow_first_replace = threading.Event()
    second_entered_intent = threading.Event()
    first_results = []
    first_errors = []
    second_errors = []
    captured_previous = []
    real_replace = release_module.os.replace

    def pause_first_pointer(source, destination):
        source_path = Path(source)
        destination_path = Path(destination)
        if (
            destination_path.name == "current.json"
            and source_path.name.startswith(".current.tmp-")
        ):
            payload = json.loads(source_path.read_text(encoding="utf-8"))
            if payload["release_id"] == "serialized-release-first":
                first_at_pointer_replace.set()
                if not allow_first_replace.wait(timeout=5):
                    raise RuntimeError("timed out waiting to release first publish")
        return real_replace(source, destination)

    real_second_intent = second_manager._write_publish_intent

    def capture_second_previous(
        pointer, previous_pointer, *, requires_succeeded_run
    ):
        captured_previous.append(
            previous_pointer.release_id if previous_pointer is not None else None
        )
        second_entered_intent.set()
        return real_second_intent(
            pointer,
            previous_pointer,
            requires_succeeded_run=requires_succeeded_run,
        )

    def publish_first():
        try:
            first_results.append(
                first_manager.publish(
                    "serialized-release-first",
                    "2026-09-02",
                    "serialized-run-first",
                    {"mart": first_artifact},
                )
            )
        except Exception as exc:  # pragma: no cover - assertion reports it
            first_errors.append(exc)

    def fail_second_commit(_pointer, _validation):
        raise RuntimeError("forced second publish rollback")

    def publish_second():
        try:
            second_manager.publish(
                "serialized-release-second",
                "2026-09-03",
                "serialized-run-second",
                {"mart": second_artifact},
                commit_callback=fail_second_commit,
            )
        except Exception as exc:
            second_errors.append(exc)

    monkeypatch.setattr(release_module.os, "replace", pause_first_pointer)
    monkeypatch.setattr(
        second_manager, "_write_publish_intent", capture_second_previous
    )
    first_thread = threading.Thread(target=publish_first)
    second_thread = threading.Thread(target=publish_second)
    first_thread.start()
    assert first_at_pointer_replace.wait(timeout=5)
    second_thread.start()
    assert second_entered_intent.wait(timeout=0.2) is False

    allow_first_replace.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert first_errors == []
    assert len(first_results) == 1
    assert len(second_errors) == 1
    assert isinstance(second_errors[0], RuntimeError)
    assert captured_previous == ["serialized-release-first"]
    assert first_manager.current() == first_results[0]
    assert catalog.get_release("serialized-release-first")["status"] == "published"
    assert catalog.get_release("serialized-release-second")["status"] == "failed"
    assert catalog.get_run("serialized-run-second")["status"] == "failed"


def test_non_object_current_pointer_is_a_normalized_validation_error(
    tmp_path: Path,
):
    releases = ReleaseManager(tmp_path)
    pointer_path = tmp_path / "releases" / "current.json"
    pointer_path.parent.mkdir(parents=True)
    pointer_path.write_text("[]\n", encoding="utf-8")

    with pytest.raises(ReleaseValidationError, match="JSON object"):
        releases.current()


def test_managed_output_directories_cannot_escape_through_symlinks(
    tmp_path: Path,
):
    outside = tmp_path / "outside"
    outside.mkdir()

    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    (raw_root / "source=fixture").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ContractViolation, match="symlink|inside data_root"):
        ContentAddressedRawStore(tmp_path).put_bytes(
            "fixture",
            "facilities",
            b"body",
            run_id="symlink-raw",
            request_fingerprint=_fingerprint("symlink-raw"),
        )
    assert list(outside.iterdir()) == []

    (tmp_path / "staging").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ContractViolation, match="symlink|inside data_root"):
        AtomicLayerWriter(tmp_path).write_jsonl(
            "staging", "symlink-layer", [{"id": "A"}]
        )
    assert list(outside.iterdir()) == []

    (tmp_path / "releases").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ContractViolation, match="symlink|inside data_root"):
        ReleaseManager(tmp_path).publish(
            "symlink-release",
            "2026-09-01",
            "symlink-run",
            {"market": tmp_path / "real-artifact.jsonl"},
        )
    assert list(outside.iterdir()) == []
