"""Immutable raw storage and atomic derived-layer artifacts."""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

from .contracts import (
    ContractViolation,
    LayerArtifact,
    RawIntegrityError,
    RawObject,
    canonical_json_bytes,
    infer_schema,
    jsonl_bytes,
    schema_fingerprint,
    sha256_bytes,
    sha256_file,
    utc_now_iso,
)
from .metadata_store import OpsCatalog
from .security import SensitiveDataError, reject_sensitive_data


_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DERIVED_LAYERS = frozenset({"staging", "core", "mart"})
_RAW_MANIFEST_FIELDS = frozenset(
    {
        "manifest_version",
        "storage_contract",
        "source_id",
        "dataset_id",
        "object_hash",
        "checksum_algorithm",
        "stored_checksum",
        "compression",
        "relative_path",
        "manifest_relative_path",
        "manifest_checksum_relative_path",
        "byte_size",
        "uncompressed_byte_size",
        "schema_version",
        "first_run_id",
        "first_retrieved_at",
        "first_original_filename",
        "first_content_type",
    }
)


def _validate_component(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not _SAFE_COMPONENT.fullmatch(value):
        raise ContractViolation(
            "%s must contain only letters, numbers, dot, underscore, or hyphen"
            % field_name
        )
    return value


def _validate_managed_path(data_root: Path, path: Path) -> Path:
    """Reject path escape and symlinked managed components before I/O."""

    root = Path(data_root).resolve()
    candidate = Path(path)
    try:
        candidate.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise ContractViolation("managed path must stay inside data_root") from exc
    current = candidate
    while current != root:
        if current.is_symlink():
            raise ContractViolation("managed path must not contain symlinks")
        parent = current.parent
        if parent == current:
            raise ContractViolation("managed path is not rooted in data_root")
        current = parent
    return candidate


def _write_file_fully(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    """Persist directory-entry changes where the platform supports it."""

    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:  # pragma: no cover - platform/filesystem dependent
        return
    try:
        os.fsync(descriptor)
    except OSError:  # pragma: no cover - platform/filesystem dependent
        pass
    finally:
        os.close(descriptor)


def _install_immutable(path: Path, payload: bytes) -> bool:
    """Atomically install bytes without ever replacing an existing object."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != payload:
            raise RawIntegrityError("immutable object already exists with other bytes")
        return False

    temporary = path.with_name(".%s.tmp-%s" % (path.name, uuid.uuid4().hex))
    try:
        _write_file_fully(temporary, payload)
        try:
            os.link(str(temporary), str(path))
            created = True
            _fsync_directory(path.parent)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise RawIntegrityError(
                    "immutable object was concurrently installed with other bytes"
                )
            created = False
        return created
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise RawIntegrityError(
                    "JSONL line %d is not an object: %s" % (line_number, path)
                )
            records.append(value)
    return records


def _read_jsonl_bytes(payload: bytes, description: str) -> List[Dict[str, Any]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RawIntegrityError("raw JSONL is not valid UTF-8: " + description) from exc
    records: List[Dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RawIntegrityError(
                "invalid JSONL at line %d: %s" % (line_number, description)
            ) from exc
        if not isinstance(value, dict):
            raise RawIntegrityError(
                "JSONL line %d is not an object: %s"
                % (line_number, description)
            )
        records.append(value)
    return records


def _deterministic_gzip(payload: bytes) -> bytes:
    """Use a stable gzip header across supported CPython/zlib versions."""

    compressed = bytearray(gzip.compress(payload, compresslevel=9, mtime=0))
    if len(compressed) < 10:  # pragma: no cover - gzip always has this header
        raise RawIntegrityError("gzip encoder returned an invalid payload")
    compressed[9] = 255  # OS=unknown; CPython versions otherwise differ here.
    return bytes(compressed)


class ContentAddressedRawStore:
    """Append-only, compressed source bytes keyed by unmodified payload SHA-256."""

    def __init__(
        self, data_root: Path, catalog: Optional[OpsCatalog] = None
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.catalog = catalog

    def put_jsonl(
        self,
        source_id: str,
        dataset_id: str,
        records: Iterable[Mapping[str, Any]],
        *,
        run_id: str,
        request_fingerprint: str,
        retrieved_at: Optional[str] = None,
        schema_version: str = "1",
    ) -> RawObject:
        materialized = [dict(record) for record in records]
        payload, record_count = jsonl_bytes(materialized)
        return self.put_bytes(
            source_id,
            dataset_id,
            payload,
            run_id=run_id,
            request_fingerprint=request_fingerprint,
            retrieved_at=retrieved_at,
            schema_version=schema_version,
            content_type="application/x-ndjson; charset=utf-8",
        )

    def put_bytes(
        self,
        source_id: str,
        dataset_id: str,
        payload: bytes,
        *,
        run_id: str,
        request_fingerprint: str,
        retrieved_at: Optional[str] = None,
        schema_version: str = "1",
        content_type: str = "application/octet-stream",
        original_filename: Optional[str] = None,
    ) -> RawObject:
        """Store exact source bytes using deterministic gzip compression."""

        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        _validate_component(run_id, "run_id")
        if not isinstance(payload, bytes):
            raise ContractViolation("raw payload must be bytes")
        if not re.fullmatch(r"[0-9a-f]{64}", request_fingerprint):
            raise ContractViolation(
                "request_fingerprint must be a lowercase SHA-256 digest"
            )
        if not isinstance(schema_version, str) or not schema_version.strip():
            raise ContractViolation("schema_version must be a non-blank string")
        reject_sensitive_data(schema_version, location="raw schema version")
        if not isinstance(content_type, str) or not content_type.strip():
            raise ContractViolation("content_type is required")
        reject_sensitive_data(content_type, location="raw content type")
        if retrieved_at is not None and (
            not isinstance(retrieved_at, str) or not retrieved_at.strip()
        ):
            raise ContractViolation("retrieved_at must be a non-blank string")
        if retrieved_at is not None:
            reject_sensitive_data(retrieved_at, location="raw retrieval time")
        if original_filename is not None and (
            not isinstance(original_filename, str)
            or Path(original_filename).name != original_filename
        ):
            raise ContractViolation("original_filename must not contain a path")
        if original_filename is not None:
            reject_sensitive_data(
                original_filename, location="raw original filename"
            )

        record_count = -1
        schema: Dict[str, List[str]] = {}
        schema_hash = schema_fingerprint(schema)
        ingestion_retrieved_at = retrieved_at or utc_now_iso()
        object_hash = sha256_bytes(payload)
        candidate_stored_payload = _deterministic_gzip(payload)
        base = (
            self.data_root
            / "raw"
            / ("source=" + source_id)
            / ("dataset=" + dataset_id)
            / "objects"
            / object_hash[:2]
        )
        object_path = base / (object_hash + ".payload.gz")
        manifest_path = base / (object_hash + ".manifest.json")
        manifest_checksum_path = base / (object_hash + ".manifest.sha256")
        for managed_path in (
            object_path,
            manifest_path,
            manifest_checksum_path,
        ):
            _validate_managed_path(self.data_root, managed_path)
        object_relative = object_path.relative_to(self.data_root).as_posix()
        manifest_relative = manifest_path.relative_to(self.data_root).as_posix()
        manifest_checksum_relative = manifest_checksum_path.relative_to(
            self.data_root
        ).as_posix()
        if object_path.exists():
            if not object_path.is_file():
                raise RawIntegrityError("raw object path is not a file")
            stored_payload = object_path.read_bytes()
            created = False
        else:
            try:
                created = _install_immutable(
                    object_path, candidate_stored_payload
                )
                stored_payload = (
                    candidate_stored_payload
                    if created
                    else object_path.read_bytes()
                )
            except RawIntegrityError:
                # A concurrent writer or another supported interpreter may
                # use different valid gzip bytes for the same exact body.
                if not object_path.is_file():
                    raise
                stored_payload = object_path.read_bytes()
                created = False
        try:
            existing_body = gzip.decompress(stored_payload)
        except (gzip.BadGzipFile, OSError, EOFError) as exc:
            raise RawIntegrityError("existing raw gzip payload is invalid") from exc
        if existing_body != payload or sha256_bytes(existing_body) != object_hash:
            raise RawIntegrityError(
                "content-addressed raw path contains different source bytes"
            )
        stored_checksum = sha256_bytes(stored_payload)
        static_manifest = {
            "manifest_version": 2,
            "storage_contract": "content-addressed-raw-gzip-v2",
            "source_id": source_id,
            "dataset_id": dataset_id,
            "object_hash": object_hash,
            "checksum_algorithm": "sha256",
            "stored_checksum": stored_checksum,
            "compression": "gzip",
            "relative_path": object_relative,
            "manifest_relative_path": manifest_relative,
            "manifest_checksum_relative_path": manifest_checksum_relative,
            "byte_size": len(stored_payload),
            "uncompressed_byte_size": len(payload),
            "schema_version": str(schema_version),
        }
        candidate_manifest = {
            **static_manifest,
            "first_run_id": run_id,
            "first_retrieved_at": ingestion_retrieved_at,
            "first_original_filename": original_filename,
            "first_content_type": content_type,
        }
        self._validate_manifest_values(
            candidate_manifest,
            static_manifest,
            require_exact_shape=True,
        )
        manifest_payload = canonical_json_bytes(candidate_manifest) + b"\n"
        if manifest_path.is_file():
            if not manifest_checksum_path.is_file():
                # A process may have stopped after installing the immutable
                # manifest but before installing its checksum.  A later run
                # necessarily has different first-run provenance, so recovery
                # validates the already-installed manifest's exact shape and
                # body-derived contract rather than requiring candidate bytes
                # to match the retry.
                stored_manifest_payload = manifest_path.read_bytes()
                try:
                    stored_manifest = json.loads(
                        stored_manifest_payload.decode("utf-8")
                    )
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise RawIntegrityError(
                        "raw manifest is incomplete and cannot be safely recovered"
                    ) from exc
                self._validate_manifest_values(
                    stored_manifest, static_manifest, require_exact_shape=True
                )
                _install_immutable(
                    manifest_checksum_path,
                    self._manifest_checksum_payload(
                        manifest_path.name, stored_manifest_payload
                    ),
                )
            self._verify_manifest_checksum(
                manifest_path, manifest_checksum_path
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self._validate_manifest_values(
                manifest, static_manifest, require_exact_shape=True
            )
        else:
            try:
                _install_immutable(manifest_path, manifest_payload)
                _install_immutable(
                    manifest_checksum_path,
                    self._manifest_checksum_payload(
                        manifest_path.name, manifest_payload
                    ),
                )
                manifest = candidate_manifest
            except RawIntegrityError:
                # A concurrent first writer may have installed the same raw
                # object with different first-ingestion metadata.
                self._verify_manifest_checksum(
                    manifest_path, manifest_checksum_path
                )
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                self._validate_manifest_values(
                    manifest, static_manifest, require_exact_shape=True
                )
        raw_object = RawObject(
            source_id=source_id,
            dataset_id=dataset_id,
            object_hash=object_hash,
            relative_path=object_relative,
            manifest_relative_path=manifest_relative,
            byte_size=len(stored_payload),
            record_count=record_count,
            schema=schema,
            schema_fingerprint=schema_hash,
            schema_version=str(schema_version),
            run_id=run_id,
            request_fingerprint=request_fingerprint,
            retrieved_at=ingestion_retrieved_at,
            created=created,
        )
        if self.catalog is not None:
            self.catalog.record_raw_object(raw_object)
        return raw_object

    @staticmethod
    def _manifest_checksum_payload(filename: str, payload: bytes) -> bytes:
        return ("%s  %s\n" % (sha256_bytes(payload), filename)).encode("ascii")

    @classmethod
    def _verify_manifest_checksum(
        cls, manifest_path: Path, checksum_path: Path
    ) -> None:
        if not manifest_path.is_file() or not checksum_path.is_file():
            raise RawIntegrityError("raw manifest or its checksum is missing")
        expected = cls._manifest_checksum_payload(
            manifest_path.name, manifest_path.read_bytes()
        )
        if checksum_path.read_bytes() != expected:
            raise RawIntegrityError("raw manifest checksum does not match")

    @staticmethod
    def _validate_manifest_values(
        manifest: Mapping[str, Any],
        expected: Mapping[str, Any],
        *,
        require_exact_shape: bool = False,
    ) -> None:
        if not isinstance(manifest, Mapping):
            raise RawIntegrityError("raw manifest must be a JSON object")
        if require_exact_shape:
            if set(manifest) != _RAW_MANIFEST_FIELDS:
                raise RawIntegrityError(
                    "raw manifest is incomplete and cannot be safely recovered"
                )
        for key, value in expected.items():
            if manifest.get(key) != value:
                raise RawIntegrityError(
                    "raw manifest field does not match object contract: " + key
                )
        for field in ("first_run_id", "first_retrieved_at"):
            if field not in manifest or not str(manifest[field]).strip():
                raise RawIntegrityError("raw manifest provenance is blank: " + field)
        if not isinstance(manifest.get("first_content_type"), str) or not str(
            manifest["first_content_type"]
        ).strip():
            raise RawIntegrityError("raw manifest first_content_type is invalid")
        first_filename = manifest.get("first_original_filename")
        if first_filename is not None and (
            not isinstance(first_filename, str)
            or Path(first_filename).name != first_filename
        ):
            raise RawIntegrityError("raw manifest first_original_filename is invalid")
        reject_sensitive_data(dict(manifest), location="raw manifest metadata")

    def _resolve_object(
        self,
        raw_or_hash: Union[RawObject, str],
        source_id: Optional[str],
        dataset_id: Optional[str],
    ) -> tuple:
        if isinstance(raw_or_hash, RawObject):
            if source_id is not None and source_id != raw_or_hash.source_id:
                raise RawIntegrityError("raw object source identity is inconsistent")
            if dataset_id is not None and dataset_id != raw_or_hash.dataset_id:
                raise RawIntegrityError("raw object dataset identity is inconsistent")
            object_path, manifest_path = self._resolve_object(
                raw_or_hash.object_hash,
                raw_or_hash.source_id,
                raw_or_hash.dataset_id,
            )
            if raw_or_hash.relative_path != object_path.relative_to(
                self.data_root
            ).as_posix():
                raise RawIntegrityError("raw object path does not match its identity")
            if raw_or_hash.manifest_relative_path != manifest_path.relative_to(
                self.data_root
            ).as_posix():
                raise RawIntegrityError(
                    "raw manifest path does not match its identity"
                )
            return object_path, manifest_path
        if source_id is None or dataset_id is None:
            raise ContractViolation(
                "source_id and dataset_id are required when resolving an object hash"
            )
        source_id = _validate_component(source_id, "source_id")
        dataset_id = _validate_component(dataset_id, "dataset_id")
        object_hash = str(raw_or_hash)
        if not re.fullmatch(r"[0-9a-f]{64}", object_hash):
            raise ContractViolation("object_hash must be a lowercase SHA-256 digest")
        base = (
            self.data_root
            / "raw"
            / ("source=" + source_id)
            / ("dataset=" + dataset_id)
            / "objects"
            / object_hash[:2]
        )
        object_path = base / (object_hash + ".payload.gz")
        manifest_path = base / (object_hash + ".manifest.json")
        _validate_managed_path(self.data_root, object_path)
        _validate_managed_path(self.data_root, manifest_path)
        return object_path, manifest_path

    def verify(
        self,
        raw_or_hash: Union[RawObject, str],
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> bool:
        object_path, manifest_path = self._resolve_object(
            raw_or_hash, source_id, dataset_id
        )
        if not object_path.is_file() or not manifest_path.is_file():
            raise RawIntegrityError("raw object or its manifest is missing")
        manifest_checksum_path = manifest_path.with_suffix(".sha256")
        _validate_managed_path(self.data_root, manifest_checksum_path)
        self._verify_manifest_checksum(manifest_path, manifest_checksum_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, Mapping):
            raise RawIntegrityError("raw manifest must be a JSON object")
        try:
            self._validate_manifest_values(
                manifest, {}, require_exact_shape=True
            )
        except SensitiveDataError as exc:
            raise RawIntegrityError("raw manifest contains sensitive metadata") from exc
        if (
            manifest.get("manifest_version") != 2
            or manifest.get("storage_contract")
            != "content-addressed-raw-gzip-v2"
            or manifest.get("checksum_algorithm") != "sha256"
            or manifest.get("compression") != "gzip"
            or not isinstance(manifest.get("schema_version"), str)
            or not manifest["schema_version"].strip()
        ):
            raise RawIntegrityError("raw manifest contract is invalid")
        expected_hash = manifest.get("object_hash")
        path_hash = object_path.name.removesuffix(".payload.gz")
        if expected_hash != path_hash:
            raise RawIntegrityError(
                "raw manifest checksum does not match its content-addressed path"
            )
        stored_hash = sha256_file(object_path)
        if stored_hash != manifest.get("stored_checksum"):
            raise RawIntegrityError("compressed raw checksum does not match its manifest")
        if manifest.get("relative_path") != object_path.relative_to(
            self.data_root
        ).as_posix():
            raise RawIntegrityError("raw manifest points to a different object path")
        if manifest.get("manifest_relative_path") != manifest_path.relative_to(
            self.data_root
        ).as_posix():
            raise RawIntegrityError("raw manifest points to a different manifest path")
        if manifest.get("manifest_checksum_relative_path") != (
            manifest_checksum_path.relative_to(self.data_root).as_posix()
        ):
            raise RawIntegrityError("raw manifest checksum path is invalid")
        if manifest.get("source_id") != object_path.parents[3].name.removeprefix(
            "source="
        ):
            raise RawIntegrityError("raw manifest source does not match its path")
        if manifest.get("dataset_id") != object_path.parents[2].name.removeprefix(
            "dataset="
        ):
            raise RawIntegrityError("raw manifest dataset does not match its path")
        if object_path.stat().st_size != manifest.get("byte_size"):
            raise RawIntegrityError("raw object byte size does not match its manifest")
        if manifest.get("manifest_relative_path") != manifest_path.relative_to(
            self.data_root
        ).as_posix():
            raise RawIntegrityError("raw manifest path is invalid")
        try:
            raw_payload = gzip.decompress(object_path.read_bytes())
        except (gzip.BadGzipFile, OSError, EOFError) as exc:
            raise RawIntegrityError("raw gzip payload cannot be decompressed") from exc
        if sha256_bytes(raw_payload) != path_hash:
            raise RawIntegrityError("raw source bytes do not match their content hash")
        if len(raw_payload) != manifest.get("uncompressed_byte_size"):
            raise RawIntegrityError("raw uncompressed byte size changed")
        if manifest.get("parser_id") == "jsonl-records-v1":
            records = _read_jsonl_bytes(raw_payload, object_path.as_posix())
            if len(records) != manifest.get("record_count"):
                raise RawIntegrityError("raw manifest record count changed")
            if infer_schema(records) != manifest.get("schema"):
                raise RawIntegrityError("raw manifest schema does not match source bytes")
        return True

    def read_manifest(
        self,
        raw_or_hash: Union[RawObject, str],
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        _, manifest_path = self._resolve_object(
            raw_or_hash, source_id, dataset_id
        )
        self.verify(raw_or_hash, source_id, dataset_id)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise RawIntegrityError("raw manifest must be a JSON object")
        return manifest

    def read_bytes(
        self,
        raw_or_hash: Union[RawObject, str],
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> bytes:
        object_path, _ = self._resolve_object(raw_or_hash, source_id, dataset_id)
        self.verify(raw_or_hash, source_id, dataset_id)
        return gzip.decompress(object_path.read_bytes())

    def read_records(
        self,
        raw_or_hash: Union[RawObject, str],
        source_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return _read_jsonl_bytes(
            self.read_bytes(raw_or_hash, source_id, dataset_id),
            str(raw_or_hash),
        )


class AtomicLayerWriter:
    """Build immutable staging/core/mart artifacts through atomic directories."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(data_root).resolve()

    def write_jsonl(
        self,
        layer: str,
        artifact_id: str,
        records: Iterable[Mapping[str, Any]],
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> LayerArtifact:
        if layer not in _DERIVED_LAYERS:
            raise ContractViolation("layer must be staging, core, or mart")
        artifact_id = _validate_component(artifact_id, "artifact_id")
        materialized = [dict(record) for record in records]
        payload, record_count = jsonl_bytes(materialized)
        checksum = sha256_bytes(payload)
        final_dir = self.data_root / layer / artifact_id
        _validate_managed_path(self.data_root, final_dir)
        data_relative = (final_dir / "data.jsonl").relative_to(
            self.data_root
        ).as_posix()
        manifest_relative = (final_dir / "manifest.json").relative_to(
            self.data_root
        ).as_posix()
        artifact_metadata = dict(metadata or {})
        reject_sensitive_data(
            artifact_metadata, location="derived artifact metadata"
        )
        manifest = {
            "manifest_version": 1,
            "storage_contract": "atomic-jsonl-artifact-v1",
            "layer": layer,
            "artifact_id": artifact_id,
            "relative_path": data_relative,
            "checksum": checksum,
            "checksum_algorithm": "sha256",
            "byte_size": len(payload),
            "record_count": record_count,
            "metadata": artifact_metadata,
        }
        manifest_payload = canonical_json_bytes(manifest) + b"\n"
        manifest_checksum = sha256_bytes(manifest_payload)

        if final_dir.exists():
            return self._existing_artifact(
                final_dir,
                layer,
                artifact_id,
                checksum,
                len(payload),
                record_count,
                manifest_payload,
                manifest_checksum,
            )

        final_dir.parent.mkdir(parents=True, exist_ok=True)
        temporary = final_dir.parent / (".%s.tmp-%s" % (artifact_id, uuid.uuid4().hex))
        _validate_managed_path(self.data_root, temporary)
        temporary.mkdir()
        try:
            _write_file_fully(temporary / "data.jsonl", payload)
            _write_file_fully(temporary / "manifest.json", manifest_payload)
            _fsync_directory(temporary)
            try:
                os.rename(str(temporary), str(final_dir))
                _fsync_directory(final_dir.parent)
            except OSError:
                if not final_dir.exists():
                    raise
                return self._existing_artifact(
                    final_dir,
                    layer,
                    artifact_id,
                    checksum,
                    len(payload),
                    record_count,
                    manifest_payload,
                    manifest_checksum,
                )
        finally:
            if temporary.exists():
                shutil.rmtree(str(temporary))

        return LayerArtifact(
            layer=layer,
            artifact_id=artifact_id,
            relative_path=data_relative,
            manifest_relative_path=manifest_relative,
            manifest_checksum=manifest_checksum,
            checksum=checksum,
            byte_size=len(payload),
            record_count=record_count,
            created=True,
        )

    def _existing_artifact(
        self,
        final_dir: Path,
        layer: str,
        artifact_id: str,
        checksum: str,
        byte_size: int,
        record_count: int,
        expected_manifest_payload: bytes,
        manifest_checksum: str,
    ) -> LayerArtifact:
        data_path = final_dir / "data.jsonl"
        manifest_path = final_dir / "manifest.json"
        if not data_path.is_file() or not manifest_path.is_file():
            raise RawIntegrityError("derived artifact is incomplete")
        if manifest_path.read_bytes() != expected_manifest_payload:
            raise ContractViolation(
                "artifact_id already exists with a different immutable contract"
            )
        if sha256_file(data_path) != checksum:
            raise ContractViolation("artifact_id already exists with different content")
        return LayerArtifact(
            layer=layer,
            artifact_id=artifact_id,
            relative_path=data_path.relative_to(self.data_root).as_posix(),
            manifest_relative_path=manifest_path.relative_to(
                self.data_root
            ).as_posix(),
            manifest_checksum=manifest_checksum,
            checksum=checksum,
            byte_size=byte_size,
            record_count=record_count,
            created=False,
        )

    def verify(self, artifact: LayerArtifact) -> bool:
        _validate_component(artifact.layer, "artifact layer")
        _validate_component(artifact.artifact_id, "artifact_id")
        expected_data_relative = (
            Path(artifact.layer) / artifact.artifact_id / "data.jsonl"
        ).as_posix()
        expected_manifest_relative = (
            Path(artifact.layer) / artifact.artifact_id / "manifest.json"
        ).as_posix()
        if (
            artifact.layer not in _DERIVED_LAYERS
            or artifact.relative_path != expected_data_relative
            or artifact.manifest_relative_path != expected_manifest_relative
        ):
            raise RawIntegrityError("derived artifact identity or path is invalid")
        data_path = self.data_root / artifact.relative_path
        manifest_path = self.data_root / artifact.manifest_relative_path
        _validate_managed_path(self.data_root, data_path)
        _validate_managed_path(self.data_root, manifest_path)
        if not data_path.is_file() or not manifest_path.is_file():
            raise RawIntegrityError("derived artifact or manifest is missing")
        manifest_payload = manifest_path.read_bytes()
        if sha256_bytes(manifest_payload) != artifact.manifest_checksum:
            raise RawIntegrityError("derived artifact manifest checksum changed")
        try:
            manifest = json.loads(manifest_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise RawIntegrityError("derived artifact manifest is invalid JSON") from exc
        if not isinstance(manifest, dict):
            raise RawIntegrityError("derived artifact manifest must be a JSON object")
        if set(manifest) != {
            "manifest_version",
            "storage_contract",
            "layer",
            "artifact_id",
            "relative_path",
            "checksum",
            "checksum_algorithm",
            "byte_size",
            "record_count",
            "metadata",
        }:
            raise RawIntegrityError("derived artifact manifest fields are invalid")
        if (
            manifest["manifest_version"] != 1
            or manifest["storage_contract"] != "atomic-jsonl-artifact-v1"
            or manifest["layer"] != artifact.layer
            or manifest["artifact_id"] != artifact.artifact_id
            or manifest["relative_path"] != artifact.relative_path
            or manifest["checksum"] != artifact.checksum
            or manifest["checksum_algorithm"] != "sha256"
            or manifest["byte_size"] != artifact.byte_size
            or manifest["record_count"] != artifact.record_count
            or not isinstance(manifest["metadata"], Mapping)
        ):
            raise RawIntegrityError("derived artifact manifest contract changed")
        reject_sensitive_data(
            manifest["metadata"], location="derived artifact metadata"
        )
        if sha256_file(data_path) != artifact.checksum:
            raise RawIntegrityError("artifact data checksum changed")
        if data_path.stat().st_size != artifact.byte_size:
            raise RawIntegrityError("artifact data byte size changed")
        return True

    def read_records(self, artifact: LayerArtifact) -> List[Dict[str, Any]]:
        self.verify(artifact)
        return _read_jsonl(self.data_root / artifact.relative_path)
