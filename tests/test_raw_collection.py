import hashlib
import json
import shutil
from pathlib import Path

import pytest

from dental_market.contracts import ContractViolation, RawIntegrityError
from dental_market.raw_collection import RawCollectionStore, RawPagePayload
from dental_market.security import SensitiveDataError


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _pages():
    return [
        RawPagePayload(
            body=b'{"items":[{"id":"A"}],"next":"cursor-2"}\n',
            request_fingerprint=_fingerprint("facilities-page-1"),
            request_cursor=None,
            status_code=200,
            content_type="application/json; charset=utf-8",
            response_headers={
                "Content-Type": "application/json; charset=utf-8",
                "ETag": '"page-one"',
                "X-RateLimit-Remaining": "99",
            },
            retrieved_at="2026-09-01T00:00:00Z",
            source_as_of="2026-08-31",
        ),
        RawPagePayload(
            body=b'{"items":[{"id":"B"}],"next":null}\n',
            request_fingerprint=_fingerprint("facilities-page-2"),
            request_cursor="cursor-2",
            status_code=200,
            content_type="application/json; charset=utf-8",
            response_headers={"x-total-count": "2"},
            retrieved_at="2026-09-01T00:00:01Z",
            source_as_of="2026-08-31",
        ),
    ]


def _rewrite_checksum(manifest_path: Path) -> None:
    payload = manifest_path.read_bytes()
    checksum_path = manifest_path.with_suffix(".sha256")
    checksum_path.write_text(
        "%s  %s\n" % (hashlib.sha256(payload).hexdigest(), manifest_path.name),
        encoding="ascii",
    )


def test_collection_preserves_exact_page_bodies_and_survives_raw_tree_copy(
    tmp_path: Path,
):
    source_root = tmp_path / "source"
    store = RawCollectionStore(source_root)
    collection = store.put(
        "public_api",
        "facilities",
        _pages(),
        run_id="run-001",
        parser_id="facility-json",
        parser_version="1.0.0",
        request_fingerprint=_fingerprint("collection-request"),
    )

    assert [page.page_index for page in collection.pages] == [0, 1]
    assert [page.request_cursor_fingerprint for page in collection.pages] == [
        None,
        hashlib.sha256(b"cursor-2").hexdigest(),
    ]
    assert [page.request_fingerprint for page in collection.pages] == [
        _fingerprint("facilities-page-1"),
        _fingerprint("facilities-page-2"),
    ]
    assert "cursor-2" not in (source_root / collection.relative_path).read_text(
        "utf-8"
    )
    assert collection.relative_path == (
        "raw/source=public_api/dataset=facilities/collections/"
        + collection.collection_hash[:2]
        + "/"
        + collection.collection_hash
        + ".manifest.json"
    )
    assert collection.manifest_checksum_path.endswith(".manifest.sha256")
    assert store.raw_store.read_bytes(
        collection.pages[0].object_hash, "public_api", "facilities"
    ) == _pages()[0].body
    assert store.raw_store.read_bytes(
        collection.pages[1].object_hash, "public_api", "facilities"
    ) == _pages()[1].body
    assert store.verify("public_api", "facilities", collection.collection_hash)

    duplicate = store.put(
        "public_api",
        "facilities",
        _pages(),
        run_id="run-002",
        parser_id="facility-json",
        parser_version="1.0.0",
        request_fingerprint=_fingerprint("collection-request"),
    )
    assert duplicate == collection

    recovery_root = tmp_path / "recovery"
    shutil.copytree(source_root / "raw", recovery_root / "raw")
    recovered = RawCollectionStore(recovery_root).read(
        "public_api", "facilities", collection.collection_hash
    )
    assert recovered == collection


def test_partial_page_journal_is_revalidated_before_resume(tmp_path: Path):
    store = RawCollectionStore(tmp_path)
    references = tuple(
        store.put_page(
            "public_api",
            "facilities",
            page,
            run_id="resume-seed",
            page_index=index,
        )
        for index, page in enumerate(_pages())
    )

    restored = store.restore_pages(
        "public_api",
        "facilities",
        [page.to_dict() for page in references],
    )
    assert restored == references

    tampered = [page.to_dict() for page in references]
    tampered[1]["page_index"] = 9
    with pytest.raises(RawIntegrityError, match="canonical order"):
        store.restore_pages("public_api", "facilities", tampered)


def test_collection_hash_is_sha256_of_canonical_manifest_core(tmp_path: Path):
    store = RawCollectionStore(tmp_path)
    collection = store.put(
        "public_api",
        "facilities",
        _pages(),
        run_id="run-001",
        parser_id="facility-json",
        parser_version="1",
        request_fingerprint=_fingerprint("collection-request"),
    )
    manifest = json.loads((tmp_path / collection.relative_path).read_text("utf-8"))
    core = {
        key: value
        for key, value in manifest.items()
        if key not in {"collection_hash", "relative_path", "manifest_checksum_path"}
    }
    canonical = json.dumps(
        core,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    assert hashlib.sha256(canonical).hexdigest() == collection.collection_hash


@pytest.mark.parametrize(
    "header",
    [
        {"Authorization": "Bearer secret"},
        {"Set-Cookie": "session=secret"},
        {"X-API-Key": "secret"},
        {"X-Unknown-Metadata": "could-be-sensitive"},
    ],
)
def test_sensitive_or_non_allowlisted_headers_are_rejected(
    tmp_path: Path, header
):
    store = RawCollectionStore(tmp_path)
    page = RawPagePayload(
        body=b"{}",
        request_fingerprint=_fingerprint("request"),
        response_headers=header,
    )
    with pytest.raises(ContractViolation):
        store.put(
            "public_api",
            "facilities",
            [page],
            run_id="run-001",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("collection-request"),
        )


def test_non_success_page_is_preserved_but_cannot_form_a_collection(
    tmp_path: Path,
):
    store = RawCollectionStore(tmp_path)
    with pytest.raises(ContractViolation, match="all 2xx pages"):
        store.put(
            "public_api",
            "facilities",
            [
                RawPagePayload(
                    body=b'{"error":"upstream unavailable"}',
                    request_fingerprint=_fingerprint("error-request"),
                    status_code=500,
                    content_type="application/json",
                )
            ],
            run_id="error-run",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("error-collection"),
        )
    assert len(list(tmp_path.rglob("*.payload.gz"))) == 1
    assert not list(tmp_path.rglob("collections/*.manifest.json"))


def test_allowlisted_response_header_cannot_embed_credential_text(
    tmp_path: Path,
):
    store = RawCollectionStore(tmp_path)
    page = RawPagePayload(
        body=b"{}",
        request_fingerprint=_fingerprint("request"),
        response_headers={"x-request-id": "client_secret=LEAKME"},
    )
    with pytest.raises(SensitiveDataError, match="sensitive field|credential-shaped"):
        store.put(
            "public_api",
            "facilities",
            [page],
            run_id="run-001",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("collection-request"),
        )
    assert not list(tmp_path.rglob("*.manifest.json"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("content_type", "application/json; client_secret=LEAKME"),
        ("source_as_of", "access_token=ALSOLEAK"),
        ("retrieved_at", "refresh_token=TIMELEAK"),
        ("original_filename", "api_key=FILELEAK.json"),
    ],
)
def test_raw_page_envelope_values_cannot_embed_credentials(
    tmp_path: Path, field: str, value: str
):
    store = RawCollectionStore(tmp_path)
    values = {
        "body": b"{}",
        "request_fingerprint": _fingerprint("request"),
        field: value,
    }
    with pytest.raises(SensitiveDataError):
        store.put(
            "public_api",
            "facilities",
            [RawPagePayload(**values)],
            run_id="run-001",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("collection-request"),
        )
    assert not list(tmp_path.rglob("*.manifest.json"))


def test_path_traversal_and_malformed_inputs_are_rejected(tmp_path: Path):
    store = RawCollectionStore(tmp_path)
    with pytest.raises(ContractViolation):
        store.put(
            "../escape",
            "facilities",
            _pages(),
            run_id="run-001",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("collection-request"),
        )
    with pytest.raises(ContractViolation):
        store.read("public_api", "facilities", "../../escape")
    with pytest.raises(ContractViolation):
        store.put(
            "public_api",
            "facilities",
            [],
            run_id="run-001",
            parser_id="json",
            parser_version="1",
            request_fingerprint=_fingerprint("collection-request"),
        )


def test_manifest_checksum_hash_order_paths_and_raw_objects_are_verified(
    tmp_path: Path,
):
    store = RawCollectionStore(tmp_path)
    collection = store.put(
        "public_api",
        "facilities",
        _pages(),
        run_id="run-001",
        parser_id="facility-json",
        parser_version="1",
        request_fingerprint=_fingerprint("collection-request"),
    )
    manifest_path = tmp_path / collection.relative_path

    original = manifest_path.read_bytes()
    manifest_path.write_bytes(original + b" ")
    with pytest.raises(RawIntegrityError, match="manifest checksum"):
        store.verify("public_api", "facilities", collection.collection_hash)

    manifest_path.write_bytes(original)
    _rewrite_checksum(manifest_path)
    manifest = json.loads(original)
    manifest["pages"][0]["page_index"] = 1
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_checksum(manifest_path)
    with pytest.raises(RawIntegrityError, match="canonical order"):
        store.verify("public_api", "facilities", collection.collection_hash)

    manifest_path.write_bytes(original)
    _rewrite_checksum(manifest_path)
    first_payload = tmp_path / collection.pages[0].relative_path
    first_payload.write_bytes(first_payload.read_bytes() + b"tampered")
    with pytest.raises(RawIntegrityError):
        store.verify("public_api", "facilities", collection.collection_hash)


def test_recomputed_sidecar_cannot_hide_embedded_path_tampering(tmp_path: Path):
    store = RawCollectionStore(tmp_path)
    collection = store.put(
        "public_api",
        "facilities",
        [_pages()[0]],
        run_id="run-001",
        parser_id="facility-json",
        parser_version="1",
        request_fingerprint=_fingerprint("collection-request"),
    )
    manifest_path = tmp_path / collection.relative_path
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["pages"][0]["relative_path"] = "../outside.payload.gz"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    _rewrite_checksum(manifest_path)

    with pytest.raises(RawIntegrityError, match="object path"):
        store.read("public_api", "facilities", collection.collection_hash)
