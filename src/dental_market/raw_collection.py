"""Content-addressed manifests for ordered, exact API response pages.

Each response body is delegated to :class:`ContentAddressedRawStore`, while
this module records the ordered page-level retrieval context needed to replay
one logical API collection.  Collection manifests intentionally have no
dependency on the operational SQLite catalog, so copying the ``raw`` tree is
sufficient for verification and recovery.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

from .contracts import (
    ContractViolation,
    RawIntegrityError,
    canonical_json_bytes,
    sha256_bytes,
    utc_now_iso,
)
from .security import SensitiveDataError, reject_sensitive_data
from .storage import (
    ContentAddressedRawStore,
    _fsync_directory,
    _validate_managed_path,
)


_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COLLECTION_CONTRACT = "raw-page-collection-v3"
_COLLECTION_MANIFEST_FIELDS = frozenset(
    {
        "manifest_version",
        "storage_contract",
        "collection_hash",
        "source_id",
        "dataset_id",
        "parser_id",
        "parser_version",
        "request_fingerprint",
        "scope_fingerprint",
        "relative_path",
        "manifest_checksum_path",
        "page_count",
        "pages",
    }
)
_PAGE_MANIFEST_FIELDS = frozenset(
    {
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
)

# Response metadata is deny-by-default.  The entries below are useful for
# caching, pagination, provenance, and rate-limit diagnostics but cannot carry
# credentials or session state.
_ALLOWED_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-encoding",
        "content-language",
        "content-length",
        "content-range",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "retry-after",
        "vary",
        "x-correlation-id",
        "x-page",
        "x-page-count",
        "x-per-page",
        "x-rate-limit-limit",
        "x-rate-limit-remaining",
        "x-rate-limit-reset",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-request-id",
        "x-total-count",
    }
)
_SENSITIVE_RESPONSE_HEADERS = frozenset(
    {
        "api-key",
        "apikey",
        "authorization",
        "cookie",
        "proxy-authorization",
        "set-cookie",
        "x-access-token",
        "x-api-key",
        "x-auth-token",
    }
)


def _validate_component(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_COMPONENT.fullmatch(value):
        raise ContractViolation(
            "%s must contain only letters, numbers, dot, underscore, or hyphen"
            % field_name
        )
    return value


def _validate_nonblank(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractViolation("%s must be a non-blank string" % field_name)
    return value


def _validate_optional_string(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    return _validate_nonblank(value, field_name)


def _validate_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ContractViolation(
            "%s must be a lowercase SHA-256 digest" % field_name
        )
    return value


def _normalize_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    if not isinstance(headers, Mapping):
        raise ContractViolation("response_headers must be a mapping")
    normalized: Dict[str, str] = {}
    for raw_name, raw_value in headers.items():
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise ContractViolation("response header names must be non-blank strings")
        name = raw_name.strip().lower()
        if name in normalized:
            raise ContractViolation("duplicate response header after normalization: " + name)
        if name in _SENSITIVE_RESPONSE_HEADERS:
            raise ContractViolation("sensitive response header is forbidden: " + name)
        if name not in _ALLOWED_RESPONSE_HEADERS:
            raise ContractViolation("response header is not allowlisted: " + name)
        if not isinstance(raw_value, str):
            raise ContractViolation("response header values must be strings: " + name)
        value = raw_value.strip()
        if any(character in value for character in ("\r", "\n", "\x00")):
            raise ContractViolation("response header contains a control character: " + name)
        reject_sensitive_data(value, location="response header " + name)
        normalized[name] = value
    return dict(sorted(normalized.items()))


def _write_file_fully(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _install_immutable(path: Path, payload: bytes) -> bool:
    """Atomically install ``payload`` while refusing replacement."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if not path.is_file() or path.read_bytes() != payload:
            raise RawIntegrityError(
                "immutable collection artifact already exists with other bytes"
            )
        return False

    temporary = path.with_name(".%s.tmp-%s" % (path.name, uuid.uuid4().hex))
    try:
        _write_file_fully(temporary, payload)
        try:
            os.link(str(temporary), str(path))
            _fsync_directory(path.parent)
            return True
        except FileExistsError:
            if not path.is_file() or path.read_bytes() != payload:
                raise RawIntegrityError(
                    "collection artifact was concurrently installed with other bytes"
                )
            return False
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class RawPagePayload:
    """One exact response body plus safe retrieval metadata for ``put``."""

    body: bytes
    request_fingerprint: str
    request_cursor: Optional[str] = None
    status_code: int = 200
    content_type: str = "application/octet-stream"
    response_headers: Mapping[str, str] = field(default_factory=dict)
    retrieved_at: Optional[str] = None
    source_as_of: Optional[str] = None
    original_filename: Optional[str] = None


def raw_page_envelope_fingerprint(page: RawPagePayload) -> str:
    """Hash the publish-relevant response envelope, excluding retrieval time."""

    if not isinstance(page, RawPagePayload):
        raise ContractViolation("response envelope requires a RawPagePayload")
    if not isinstance(page.body, bytes):
        raise ContractViolation("page body must be exact bytes")
    if isinstance(page.status_code, bool) or not isinstance(page.status_code, int):
        raise ContractViolation("status_code must be an integer")
    if page.status_code < 100 or page.status_code > 599:
        raise ContractViolation("status_code must be between 100 and 599")
    content_type = _validate_nonblank(page.content_type, "content_type")
    headers = _normalize_headers(page.response_headers)
    if headers.get("content-type") not in (None, content_type):
        raise ContractViolation(
            "content_type does not match the allowlisted Content-Type header"
        )
    source_as_of = _validate_optional_string(page.source_as_of, "source_as_of")
    reject_sensitive_data(
        {
            "content_type_value": content_type,
            "source_as_of_value": source_as_of,
            "response_header_values": headers,
        },
        location="raw response envelope",
    )
    return sha256_bytes(
        canonical_json_bytes(
            {
                "contract": "raw-response-envelope-v1",
                "object_hash": sha256_bytes(page.body),
                "status_code": page.status_code,
                "content_type": content_type,
                "response_headers": headers,
                "source_as_of": source_as_of,
            }
        )
    )


@dataclass(frozen=True)
class RawPageReference:
    """A verified reference to one page body in an ordered collection."""

    object_hash: str
    relative_path: str
    manifest_relative_path: str
    page_index: int
    request_fingerprint: str
    request_cursor_fingerprint: Optional[str]
    status_code: int
    content_type: str
    response_headers: Mapping[str, str]
    retrieved_at: str
    source_as_of: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "object_hash": self.object_hash,
            "relative_path": self.relative_path,
            "manifest_relative_path": self.manifest_relative_path,
            "page_index": self.page_index,
            "request_fingerprint": self.request_fingerprint,
            "request_cursor_fingerprint": self.request_cursor_fingerprint,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "response_headers": dict(sorted(self.response_headers.items())),
            "retrieved_at": self.retrieved_at,
            "source_as_of": self.source_as_of,
        }


@dataclass(frozen=True)
class RawCollection:
    """Content-addressed manifest for a complete ordered page retrieval."""

    collection_hash: str
    source_id: str
    dataset_id: str
    parser_id: str
    parser_version: str
    request_fingerprint: str
    scope_fingerprint: str
    relative_path: str
    manifest_checksum_path: str
    pages: Tuple[RawPageReference, ...]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "collection_hash": self.collection_hash,
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "request_fingerprint": self.request_fingerprint,
            "scope_fingerprint": self.scope_fingerprint,
            "relative_path": self.relative_path,
            "manifest_checksum_path": self.manifest_checksum_path,
            "pages": [page.to_dict() for page in self.pages],
        }


class RawCollectionStore:
    """Store and verify ordered page collections without an ops catalog."""

    def __init__(
        self,
        data_root: Path,
        raw_store: Optional[ContentAddressedRawStore] = None,
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.raw_store = raw_store or ContentAddressedRawStore(self.data_root)
        if self.raw_store.data_root != self.data_root:
            raise ContractViolation("raw_store and collection store need the same data_root")

    def _validate_http_collection(
        self,
        pages: Tuple[RawPageReference, ...],
        source_id: str,
        dataset_id: str,
        *,
        persisted: bool,
    ) -> None:
        """Accept only complete 2xx snapshots or one exact empty 304."""

        if all(200 <= page.status_code < 300 for page in pages):
            return
        exact_not_modified = (
            len(pages) == 1
            and pages[0].status_code == 304
            and pages[0].request_cursor_fingerprint is None
            and self.raw_store.read_bytes(
                pages[0].object_hash, source_id, dataset_id
            )
            == b""
        )
        if exact_not_modified:
            return
        message = (
            "raw collection must be all 2xx pages or one exact empty first-page 304"
        )
        if persisted:
            raise RawIntegrityError(message)
        raise ContractViolation(message)

    def put(
        self,
        source_id: str,
        dataset_id: str,
        pages: Iterable[Union[RawPagePayload, Mapping[str, Any]]],
        *,
        run_id: str,
        parser_id: str,
        parser_version: str,
        request_fingerprint: str,
        scope_fingerprint: Optional[str] = None,
    ) -> RawCollection:
        """Persist page bodies, then atomically publish their ordered manifest."""

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        run_id = _validate_component(run_id, "run_id")
        materialized = [self._coerce_page(page) for page in pages]
        if not materialized:
            raise ContractViolation("a raw collection must contain at least one page")
        references = tuple(
            self.put_page(
                source_id,
                dataset_id,
                page,
                run_id=run_id,
                page_index=page_index,
            )
            for page_index, page in enumerate(materialized)
        )
        return self.finalize(
            source_id,
            dataset_id,
            references,
            parser_id=parser_id,
            parser_version=parser_version,
            request_fingerprint=request_fingerprint,
            scope_fingerprint=scope_fingerprint,
        )

    def put_page(
        self,
        source_id: str,
        dataset_id: str,
        page: Union[RawPagePayload, Mapping[str, Any]],
        *,
        run_id: str,
        page_index: int,
    ) -> RawPageReference:
        """Fsync one successful response immediately, before later pages run."""

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        run_id = _validate_component(run_id, "run_id")
        if isinstance(page_index, bool) or not isinstance(page_index, int):
            raise ContractViolation("page_index must be an integer")
        if page_index < 0:
            raise ContractViolation("page_index must be non-negative")
        page = self._coerce_page(page)
        headers = _normalize_headers(page.response_headers)
        reject_sensitive_data(
            {
                "content_type_value": page.content_type,
                "retrieved_at_value": page.retrieved_at,
                "source_as_of_value": page.source_as_of,
                "original_filename_value": page.original_filename,
            },
            location="raw page metadata",
        )
        content_type = _validate_nonblank(page.content_type, "content_type")
        if headers.get("content-type") not in (None, content_type):
            raise ContractViolation(
                "content_type does not match the allowlisted Content-Type header"
            )
        if not isinstance(page.body, bytes):
            raise ContractViolation("page body must be exact bytes")
        if isinstance(page.status_code, bool) or not isinstance(page.status_code, int):
            raise ContractViolation("status_code must be an integer")
        if page.status_code < 100 or page.status_code > 599:
            raise ContractViolation("status_code must be between 100 and 599")
        request_fingerprint = _validate_sha256(
            page.request_fingerprint, "request_fingerprint"
        )
        request_cursor = _validate_optional_string(
            page.request_cursor, "request_cursor"
        )
        request_cursor_fingerprint = (
            sha256_bytes(request_cursor.encode("utf-8"))
            if request_cursor is not None
            else None
        )
        retrieved_at = _validate_nonblank(
            page.retrieved_at or utc_now_iso(), "retrieved_at"
        )
        source_as_of = _validate_optional_string(page.source_as_of, "source_as_of")
        raw_object = self.raw_store.put_bytes(
            source_id,
            dataset_id,
            page.body,
            run_id=run_id,
            request_fingerprint=request_fingerprint,
            retrieved_at=retrieved_at,
            content_type=content_type,
            original_filename=page.original_filename,
        )
        return RawPageReference(
            object_hash=raw_object.object_hash,
            relative_path=raw_object.relative_path,
            manifest_relative_path=raw_object.manifest_relative_path,
            page_index=page_index,
            request_fingerprint=request_fingerprint,
            request_cursor_fingerprint=request_cursor_fingerprint,
            status_code=page.status_code,
            content_type=content_type,
            response_headers=headers,
            retrieved_at=retrieved_at,
            source_as_of=source_as_of,
        )

    def finalize(
        self,
        source_id: str,
        dataset_id: str,
        pages: Iterable[RawPageReference],
        *,
        parser_id: str,
        parser_version: str,
        request_fingerprint: str,
        scope_fingerprint: Optional[str] = None,
    ) -> RawCollection:
        """Publish a collection manifest only after pagination completes."""

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        parser_id = _validate_component(parser_id, "parser_id")
        parser_version = _validate_component(parser_version, "parser_version")
        request_fingerprint = _validate_sha256(
            request_fingerprint, "request_fingerprint"
        )
        scope_fingerprint = _validate_sha256(
            scope_fingerprint or request_fingerprint,
            "scope_fingerprint",
        )
        references = tuple(pages)
        if not references:
            raise ContractViolation("a raw collection must contain at least one page")
        references = tuple(
            self._page_from_manifest(
                page.to_dict(), page_index, source_id, dataset_id
            )
            for page_index, page in enumerate(references)
        )
        for page in references:
            self.raw_store.verify(page.object_hash, source_id, dataset_id)
        self._validate_http_collection(
            references, source_id, dataset_id, persisted=False
        )
        core = self._manifest_core(
            source_id,
            dataset_id,
            parser_id,
            parser_version,
            request_fingerprint,
            scope_fingerprint,
            references,
        )
        collection_hash = sha256_bytes(canonical_json_bytes(core))
        manifest_path, checksum_path = self._collection_paths(
            source_id, dataset_id, collection_hash
        )
        relative_path = manifest_path.relative_to(self.data_root).as_posix()
        checksum_relative = checksum_path.relative_to(self.data_root).as_posix()
        manifest = {
            **core,
            "collection_hash": collection_hash,
            "relative_path": relative_path,
            "manifest_checksum_path": checksum_relative,
        }
        manifest_payload = canonical_json_bytes(manifest) + b"\n"
        checksum_payload = self._checksum_payload(manifest_path.name, manifest_payload)
        _install_immutable(manifest_path, manifest_payload)
        _install_immutable(checksum_path, checksum_payload)
        return self.read(source_id, dataset_id, collection_hash)

    def read(
        self, source_id: str, dataset_id: str, collection_hash: str
    ) -> RawCollection:
        """Read and fully verify a collection from only its raw tree."""

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        if not isinstance(collection_hash, str) or not _SHA256.fullmatch(
            collection_hash
        ):
            raise ContractViolation(
                "collection_hash must be a lowercase SHA-256 digest"
            )
        manifest_path, checksum_path = self._collection_paths(
            source_id, dataset_id, collection_hash
        )
        if not manifest_path.is_file() or not checksum_path.is_file():
            raise RawIntegrityError("raw collection manifest or checksum is missing")
        self._verify_checksum(manifest_path, checksum_path)
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawIntegrityError("raw collection manifest is not valid JSON") from exc
        if not isinstance(manifest, dict):
            raise RawIntegrityError("raw collection manifest must be a JSON object")
        if frozenset(manifest) != _COLLECTION_MANIFEST_FIELDS:
            raise RawIntegrityError("raw collection manifest fields do not match contract")
        if manifest.get("manifest_version") != 3:
            raise RawIntegrityError("raw collection manifest version is unsupported")
        if manifest.get("storage_contract") != _COLLECTION_CONTRACT:
            raise RawIntegrityError("raw collection storage contract is unsupported")
        if manifest.get("source_id") != source_id:
            raise RawIntegrityError("raw collection source does not match its path")
        if manifest.get("dataset_id") != dataset_id:
            raise RawIntegrityError("raw collection dataset does not match its path")
        if manifest.get("collection_hash") != collection_hash:
            raise RawIntegrityError("raw collection hash does not match its path")
        expected_relative = manifest_path.relative_to(self.data_root).as_posix()
        expected_checksum = checksum_path.relative_to(self.data_root).as_posix()
        if manifest.get("relative_path") != expected_relative:
            raise RawIntegrityError("raw collection manifest path is invalid")
        if manifest.get("manifest_checksum_path") != expected_checksum:
            raise RawIntegrityError("raw collection checksum path is invalid")

        parser_id = self._manifest_component(manifest, "parser_id")
        parser_version = self._manifest_component(manifest, "parser_version")
        try:
            request_fingerprint = _validate_sha256(
                manifest.get("request_fingerprint"), "request_fingerprint"
            )
            scope_fingerprint = _validate_sha256(
                manifest.get("scope_fingerprint"), "scope_fingerprint"
            )
        except ContractViolation as exc:
            raise RawIntegrityError(
                "raw collection request fingerprint is invalid"
            ) from exc
        page_values = manifest.get("pages")
        if not isinstance(page_values, list) or not page_values:
            raise RawIntegrityError("raw collection pages must be a non-empty list")
        pages = tuple(
            self._page_from_manifest(value, page_index, source_id, dataset_id)
            for page_index, value in enumerate(page_values)
        )
        if manifest.get("page_count") != len(pages):
            raise RawIntegrityError("raw collection page count changed")

        core = self._manifest_core(
            source_id,
            dataset_id,
            parser_id,
            parser_version,
            request_fingerprint,
            scope_fingerprint,
            pages,
        )
        if sha256_bytes(canonical_json_bytes(core)) != collection_hash:
            raise RawIntegrityError("raw collection core hash does not match its path")

        for page in pages:
            # read_bytes performs gzip, stored checksum, uncompressed checksum,
            # byte-size, and content-address verification. Parser and response
            # metadata belong to this collection, never the exact-byte object.
            body = self.raw_store.read_bytes(
                page.object_hash, source_id, dataset_id
            )
            if sha256_bytes(body) != page.object_hash:
                raise RawIntegrityError("raw page body hash changed")
        self._validate_http_collection(
            pages, source_id, dataset_id, persisted=True
        )

        return RawCollection(
            collection_hash=collection_hash,
            source_id=source_id,
            dataset_id=dataset_id,
            parser_id=parser_id,
            parser_version=parser_version,
            request_fingerprint=request_fingerprint,
            scope_fingerprint=scope_fingerprint,
            relative_path=expected_relative,
            manifest_checksum_path=expected_checksum,
            pages=pages,
        )

    def verify(
        self, source_id: str, dataset_id: str, collection_hash: str
    ) -> bool:
        """Return true only after manifest and every referenced page verify."""

        self.read(source_id, dataset_id, collection_hash)
        return True

    def restore_pages(
        self,
        source_id: str,
        dataset_id: str,
        pages: Iterable[Mapping[str, Any]],
    ) -> Tuple[RawPageReference, ...]:
        """Rebuild and verify an ordered partial page chain from a journal.

        A resume journal is not authoritative storage.  Every field is checked
        against the canonical raw-object layout and every referenced object is
        checksum-verified before a caller may continue pagination from it.
        """

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        materialized = tuple(pages)
        restored = tuple(
            self._page_from_manifest(
                dict(value), page_index, source_id, dataset_id
            )
            for page_index, value in enumerate(materialized)
        )
        for page in restored:
            self.raw_store.verify(page.object_hash, source_id, dataset_id)
        return restored

    @staticmethod
    def _coerce_page(
        page: Union[RawPagePayload, Mapping[str, Any]]
    ) -> RawPagePayload:
        if isinstance(page, RawPagePayload):
            return page
        if not isinstance(page, Mapping):
            raise ContractViolation("pages must contain RawPagePayload or mappings")
        allowed = {
            "body",
            "request_fingerprint",
            "request_cursor",
            "status_code",
            "content_type",
            "response_headers",
            "retrieved_at",
            "source_as_of",
            "original_filename",
        }
        unknown = set(page) - allowed
        if unknown:
            raise ContractViolation(
                "unknown raw page fields: " + ", ".join(sorted(str(key) for key in unknown))
            )
        try:
            return RawPagePayload(**dict(page))
        except TypeError as exc:
            raise ContractViolation("raw page fields do not match contract") from exc

    @staticmethod
    def _manifest_core(
        source_id: str,
        dataset_id: str,
        parser_id: str,
        parser_version: str,
        request_fingerprint: str,
        scope_fingerprint: str,
        pages: Tuple[RawPageReference, ...],
    ) -> Dict[str, Any]:
        return {
            "manifest_version": 3,
            "storage_contract": _COLLECTION_CONTRACT,
            "source_id": source_id,
            "dataset_id": dataset_id,
            "parser_id": parser_id,
            "parser_version": parser_version,
            "request_fingerprint": request_fingerprint,
            "scope_fingerprint": scope_fingerprint,
            "page_count": len(pages),
            "pages": [page.to_dict() for page in pages],
        }

    def _collection_paths(
        self, source_id: str, dataset_id: str, collection_hash: str
    ) -> Tuple[Path, Path]:
        base = (
            self.data_root
            / "raw"
            / ("source=" + source_id)
            / ("dataset=" + dataset_id)
            / "collections"
            / collection_hash[:2]
        )
        manifest = base / (collection_hash + ".manifest.json")
        checksum = base / (collection_hash + ".manifest.sha256")
        for path in (manifest, checksum):
            _validate_managed_path(self.data_root, path)
        return manifest, checksum

    @staticmethod
    def _checksum_payload(filename: str, payload: bytes) -> bytes:
        return ("%s  %s\n" % (sha256_bytes(payload), filename)).encode("ascii")

    @classmethod
    def _verify_checksum(cls, manifest_path: Path, checksum_path: Path) -> None:
        expected = cls._checksum_payload(manifest_path.name, manifest_path.read_bytes())
        if checksum_path.read_bytes() != expected:
            raise RawIntegrityError("raw collection manifest checksum does not match")

    @staticmethod
    def _manifest_component(manifest: Mapping[str, Any], field_name: str) -> str:
        value = manifest.get(field_name)
        try:
            return _validate_component(value, field_name)
        except ContractViolation as exc:
            raise RawIntegrityError(
                "raw collection %s is invalid" % field_name
            ) from exc

    def _page_from_manifest(
        self,
        value: Any,
        expected_index: int,
        source_id: str,
        dataset_id: str,
    ) -> RawPageReference:
        if not isinstance(value, dict) or frozenset(value) != _PAGE_MANIFEST_FIELDS:
            raise RawIntegrityError("raw collection page fields do not match contract")
        if value.get("page_index") != expected_index:
            raise RawIntegrityError("raw collection pages are not in canonical order")
        object_hash = value.get("object_hash")
        if not isinstance(object_hash, str) or not _SHA256.fullmatch(object_hash):
            raise RawIntegrityError("raw page object hash is invalid")
        expected_base = (
            self.data_root
            / "raw"
            / ("source=" + source_id)
            / ("dataset=" + dataset_id)
            / "objects"
            / object_hash[:2]
        )
        expected_payload = (expected_base / (object_hash + ".payload.gz")).relative_to(
            self.data_root
        ).as_posix()
        expected_manifest = (
            expected_base / (object_hash + ".manifest.json")
        ).relative_to(self.data_root).as_posix()
        if value.get("relative_path") != expected_payload:
            raise RawIntegrityError("raw page object path is invalid")
        if value.get("manifest_relative_path") != expected_manifest:
            raise RawIntegrityError("raw page manifest path is invalid")
        status_code = value.get("status_code")
        if (
            isinstance(status_code, bool)
            or not isinstance(status_code, int)
            or status_code < 100
            or status_code > 599
        ):
            raise RawIntegrityError("raw page status code is invalid")
        try:
            headers = _normalize_headers(value.get("response_headers"))
            content_type = _validate_nonblank(value.get("content_type"), "content_type")
            request_fingerprint = _validate_sha256(
                value.get("request_fingerprint"), "request_fingerprint"
            )
            request_cursor_fingerprint = _validate_optional_string(
                value.get("request_cursor_fingerprint"),
                "request_cursor_fingerprint",
            )
            if request_cursor_fingerprint is not None:
                _validate_sha256(
                    request_cursor_fingerprint,
                    "request_cursor_fingerprint",
                )
            retrieved_at = _validate_nonblank(
                value.get("retrieved_at"), "retrieved_at"
            )
            source_as_of = _validate_optional_string(
                value.get("source_as_of"), "source_as_of"
            )
            reject_sensitive_data(
                {
                    "content_type_value": content_type,
                    "retrieved_at_value": retrieved_at,
                    "source_as_of_value": source_as_of,
                    "response_header_values": headers,
                },
                location="raw collection page metadata",
            )
        except (ContractViolation, SensitiveDataError) as exc:
            raise RawIntegrityError("raw collection page metadata is invalid") from exc
        if headers.get("content-type") not in (None, content_type):
            raise RawIntegrityError(
                "raw page Content-Type header does not match its metadata"
            )
        return RawPageReference(
            object_hash=object_hash,
            relative_path=expected_payload,
            manifest_relative_path=expected_manifest,
            page_index=expected_index,
            request_fingerprint=request_fingerprint,
            request_cursor_fingerprint=request_cursor_fingerprint,
            status_code=status_code,
            content_type=content_type,
            response_headers=headers,
            retrieved_at=retrieved_at,
            source_as_of=source_as_of,
        )


__all__ = [
    "RawCollection",
    "RawCollectionStore",
    "RawPagePayload",
    "RawPageReference",
    "raw_page_envelope_fingerprint",
]
