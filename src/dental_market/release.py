"""Validated, immutable releases with an atomic last-good pointer."""

from __future__ import annotations

import json
import os
import re
import shutil
import threading
import uuid
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Union

from .contracts import (
    ContractViolation,
    LayerArtifact,
    ReleasePointer,
    ReleaseValidationError,
    canonical_json_bytes,
    sha256_bytes,
    sha256_file,
    utc_now_iso,
)
from .metadata_store import OpsCatalog
from .security import SensitiveDataError, redact_sensitive, reject_sensitive_data
from .storage import (
    _fsync_directory,
    _validate_component,
    _validate_managed_path,
    _write_file_fully,
)


ArtifactReference = Union[LayerArtifact, Path, str]
ReleaseValidator = Callable[[Mapping[str, Any]], Optional[Mapping[str, Any]]]
ReleaseCommit = Callable[[ReleasePointer, Mapping[str, Any]], None]
ReleaseCommitVerifier = Callable[[ReleasePointer, Mapping[str, Any]], bool]


_PUBLISH_THREAD_LOCKS: Dict[str, threading.RLock] = {}
_PUBLISH_THREAD_LOCKS_GUARD = threading.Lock()


@contextmanager
def _release_publish_lock(releases_root: Path):
    """Serialize pointer snapshot, replacement, commit, and compensation."""

    lock_key = str(releases_root.resolve(strict=False))
    with _PUBLISH_THREAD_LOCKS_GUARD:
        thread_lock = _PUBLISH_THREAD_LOCKS.setdefault(
            lock_key, threading.RLock()
        )
    with thread_lock:
        releases_root.mkdir(parents=True, exist_ok=True)
        lock_path = releases_root / ".publish.lock"
        handle = lock_path.open("a+", encoding="utf-8")
        fcntl_module = None
        try:
            try:
                import fcntl as fcntl_module  # type: ignore

                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_EX)
            except ImportError:  # pragma: no cover - non-POSIX fallback
                fcntl_module = None
            yield
        finally:
            if fcntl_module is not None:
                try:
                    fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_UN)
                except OSError:
                    pass
            handle.close()


_RELEASE_MANIFEST_FIELDS = frozenset(
    {
        "manifest_version",
        "release_contract",
        "release_id",
        "as_of_date",
        "run_id",
        "artifacts",
        "metadata",
        "validation",
    }
)
_ARTIFACT_CONTRACT_FIELDS = frozenset(
    {
        "name",
        "relative_path",
        "checksum",
        "checksum_algorithm",
        "byte_size",
        "layer",
        "artifact_id",
        "artifact_manifest_relative_path",
        "artifact_manifest_checksum",
    }
)


def _freeze_validator_input(value: Any) -> Any:
    """Expose a recursively read-only candidate contract to validators."""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_validator_input(nested) for key, nested in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_validator_input(nested) for nested in value)
    return value


class ReleaseManager:
    """Publish only fully validated candidates and retain the last good release."""

    def __init__(
        self, data_root: Path, catalog: Optional[OpsCatalog] = None
    ) -> None:
        self.data_root = Path(data_root).resolve()
        self.releases_root = self.data_root / "releases"
        self.catalog = catalog
        self._reconcile_publish_intent()

    def _artifact_contract(
        self, name: str, artifact: ArtifactReference
    ) -> Dict[str, Any]:
        _validate_component(name, "artifact name")
        expected_checksum: Optional[str] = None
        layer: Optional[str] = None
        artifact_id: Optional[str] = None
        artifact_manifest_relative_path: Optional[str] = None
        artifact_manifest_checksum: Optional[str] = None
        if isinstance(artifact, LayerArtifact):
            path = self.data_root / artifact.relative_path
            expected_checksum = artifact.checksum
            layer = artifact.layer
            artifact_id = artifact.artifact_id
            artifact_manifest_relative_path = artifact.manifest_relative_path
            artifact_manifest_checksum = artifact.manifest_checksum
        else:
            path = Path(artifact)
            if not path.is_absolute():
                path = self.data_root / path
        _validate_managed_path(self.data_root, path)
        relative = path.relative_to(self.data_root).as_posix()
        if not path.is_file():
            raise ReleaseValidationError("release artifact is missing: " + relative)
        checksum = sha256_file(path)
        if expected_checksum is not None and checksum != expected_checksum:
            raise ReleaseValidationError(
                "release artifact checksum changed: " + relative
            )
        return {
            "name": name,
            "relative_path": relative,
            "checksum": checksum,
            "checksum_algorithm": "sha256",
            "byte_size": path.stat().st_size,
            "layer": layer,
            "artifact_id": artifact_id,
            "artifact_manifest_relative_path": artifact_manifest_relative_path,
            "artifact_manifest_checksum": artifact_manifest_checksum,
        }

    def _verify_artifact_contracts(
        self, artifacts: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Recheck artifact bytes immediately around the publish boundary."""

        if not artifacts:
            raise ReleaseValidationError("release has no artifact contracts")
        for name, contract in artifacts.items():
            if not isinstance(name, str):
                raise ReleaseValidationError("release artifact name is invalid")
            _validate_component(name, "artifact name")
            if not isinstance(contract, Mapping) or set(contract) != set(
                _ARTIFACT_CONTRACT_FIELDS
            ):
                raise ReleaseValidationError(
                    "release artifact contract shape is invalid: " + name
                )
            if contract.get("name") != name:
                raise ReleaseValidationError(
                    "release artifact name does not match contract: " + name
                )
            relative = contract.get("relative_path")
            if not isinstance(relative, str) or not relative.strip():
                raise ReleaseValidationError(
                    "release artifact path is invalid: " + name
                )
            checksum = contract.get("checksum")
            if (
                contract.get("checksum_algorithm") != "sha256"
                or not isinstance(checksum, str)
                or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
            ):
                raise ReleaseValidationError(
                    "release artifact checksum contract is invalid: " + name
                )
            byte_size = contract.get("byte_size")
            if (
                isinstance(byte_size, bool)
                or not isinstance(byte_size, int)
                or byte_size < 0
            ):
                raise ReleaseValidationError(
                    "release artifact size contract is invalid: " + name
                )
            for optional_name in ("layer", "artifact_id"):
                optional_value = contract.get(optional_name)
                if optional_value is not None:
                    if not isinstance(optional_value, str):
                        raise ReleaseValidationError(
                            "release artifact %s is invalid: %s"
                            % (optional_name, name)
                        )
                    _validate_component(optional_value, optional_name)
            manifest_relative = contract.get("artifact_manifest_relative_path")
            manifest_checksum = contract.get("artifact_manifest_checksum")
            if (manifest_relative is None) != (manifest_checksum is None):
                raise ReleaseValidationError(
                    "release artifact manifest contract is incomplete: " + name
                )
            if manifest_relative is not None:
                if (
                    not isinstance(manifest_relative, str)
                    or not isinstance(manifest_checksum, str)
                    or re.fullmatch(r"[0-9a-f]{64}", manifest_checksum) is None
                ):
                    raise ReleaseValidationError(
                        "release artifact manifest contract is invalid: " + name
                    )
                artifact_manifest_path = self.data_root / manifest_relative
                _validate_managed_path(self.data_root, artifact_manifest_path)
                if (
                    not artifact_manifest_path.is_file()
                    or sha256_file(artifact_manifest_path) != manifest_checksum
                ):
                    raise ReleaseValidationError(
                        "release artifact manifest checksum changed: " + name
                    )
            artifact_path = self.data_root / relative
            _validate_managed_path(self.data_root, artifact_path)
            if not artifact_path.is_file():
                raise ReleaseValidationError(
                    "release artifact is missing: " + name
                )
            if sha256_file(artifact_path) != checksum:
                raise ReleaseValidationError(
                    "release artifact checksum changed: " + name
                )
            if artifact_path.stat().st_size != byte_size:
                raise ReleaseValidationError(
                    "release artifact size changed: " + name
                )

    def _validated_manifest_for_pointer(
        self, pointer: ReleasePointer
    ) -> Dict[str, Any]:
        """Validate one release manifest buffer and every artifact it binds."""

        expected_manifest_relative = (
            Path("releases") / pointer.release_id / "manifest.json"
        ).as_posix()
        if pointer.manifest_relative_path != expected_manifest_relative:
            raise ReleaseValidationError(
                "current release manifest path does not match release ID"
            )
        manifest_path = self.data_root / pointer.manifest_relative_path
        _validate_managed_path(self.data_root, manifest_path)
        if not manifest_path.is_file():
            raise ReleaseValidationError("current release manifest is missing")
        manifest_payload = manifest_path.read_bytes()
        if sha256_bytes(manifest_payload) != pointer.manifest_checksum:
            raise ReleaseValidationError("current release manifest checksum changed")
        try:
            manifest = json.loads(manifest_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ReleaseValidationError(
                "current release manifest is invalid JSON"
            ) from exc
        if not isinstance(manifest, dict):
            raise ReleaseValidationError(
                "current release manifest must be a JSON object"
            )
        if set(manifest) != set(_RELEASE_MANIFEST_FIELDS):
            raise ReleaseValidationError(
                "current release manifest fields are invalid"
            )
        if (
            manifest["manifest_version"] != 1
            or manifest["release_contract"] != "last-good-pointer-v1"
        ):
            raise ReleaseValidationError(
                "current release manifest contract is unsupported"
            )
        if manifest["release_id"] != pointer.release_id:
            raise ReleaseValidationError("current release ID does not match manifest")
        as_of_date = manifest["as_of_date"]
        try:
            parsed_as_of = date.fromisoformat(as_of_date)
        except (TypeError, ValueError) as exc:
            raise ReleaseValidationError(
                "current release as-of date is invalid"
            ) from exc
        if parsed_as_of.isoformat() != as_of_date:
            raise ReleaseValidationError("current release as-of date is invalid")
        run_id = manifest["run_id"]
        try:
            _validate_component(run_id, "current release run_id")
        except ContractViolation as exc:
            raise ReleaseValidationError(
                "current release run ID is invalid"
            ) from exc
        if not isinstance(manifest["metadata"], Mapping) or not isinstance(
            manifest["validation"], Mapping
        ):
            raise ReleaseValidationError(
                "current release metadata or validation is invalid"
            )
        try:
            reject_sensitive_data(manifest, location="current release manifest")
        except SensitiveDataError as exc:
            raise ReleaseValidationError(
                "current release manifest contains forbidden sensitive data"
            ) from exc
        artifacts = manifest["artifacts"]
        if not isinstance(artifacts, Mapping) or not artifacts:
            raise ReleaseValidationError("current release has no artifact contracts")
        self._verify_artifact_contracts(artifacts)
        return manifest

    def _verify_installed_candidate(
        self,
        pointer: ReleasePointer,
        expected_manifest_payload: bytes,
        artifacts: Mapping[str, Mapping[str, Any]],
        *,
        require_current_pointer: bool = False,
    ) -> None:
        """Recheck exact installed manifest and artifacts around callbacks."""

        if require_current_pointer:
            self._verify_current_pointer(pointer)
        manifest_path = self.data_root / pointer.manifest_relative_path
        _validate_managed_path(self.data_root, manifest_path)
        if (
            not manifest_path.is_file()
            or manifest_path.read_bytes() != expected_manifest_payload
        ):
            raise ReleaseValidationError(
                "installed release manifest changed during publication"
            )
        self._verify_artifact_contracts(artifacts)

    def _verify_current_pointer(self, expected: ReleasePointer) -> None:
        pointer_path = self.releases_root / "current.json"
        _validate_managed_path(self.data_root, pointer_path)
        if not pointer_path.is_file():
            raise ReleaseValidationError("current release pointer is missing")
        try:
            payload = json.loads(pointer_path.read_bytes().decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ReleaseValidationError(
                "current release pointer is invalid"
            ) from exc
        actual = self._pointer_from_payload(
            payload, location="current release pointer"
        )
        if actual != expected:
            raise ReleaseValidationError(
                "current release pointer changed during publication"
            )

    def _catalog_commit_matches(
        self,
        pointer: ReleasePointer,
        run_id: str,
        validation: Mapping[str, Any],
    ) -> bool:
        assert self.catalog is not None
        release_row = self.catalog.get_release(pointer.release_id)
        run_row = self.catalog.get_run(run_id)
        expected_validation_json = canonical_json_bytes(validation).decode("utf-8")
        return bool(
            release_row is not None
            and release_row["status"] == "published"
            and release_row["run_id"] == run_id
            and release_row["manifest_relative_path"]
            == pointer.manifest_relative_path
            and release_row["manifest_checksum"] == pointer.manifest_checksum
            and release_row["published_at"] == pointer.published_at
            and release_row["validation_json"] == expected_validation_json
            and run_row is not None
            and run_row["status"] == "succeeded"
        )

    def _callback_commit_matches(
        self,
        pointer: ReleasePointer,
        run_id: str,
        validation: Mapping[str, Any],
        commit_verifier: Optional[ReleaseCommitVerifier],
    ) -> bool:
        """Require both the base release commit and an optional domain commit.

        The base check protects the release/run rows.  Composite pipelines may
        additionally verify their exact source inventory in the same catalog
        transaction by supplying ``commit_verifier``.
        """

        if not self._catalog_commit_matches(pointer, run_id, validation):
            return False
        if commit_verifier is None:
            return True
        verifier_validation = json.loads(
            canonical_json_bytes(validation).decode("utf-8")
        )
        return commit_verifier(pointer, verifier_validation) is True

    def _quarantine_invalid_callback_commit(
        self, release_id: str, run_id: str, message: str
    ) -> None:
        assert self.catalog is not None
        self.catalog.quarantine_invalid_publish_commit(
            release_id,
            run_id,
            {"type": "InvalidPublishCommit", "message": message},
        )

    def publish(
        self,
        release_id: str,
        as_of_date: str,
        run_id: str,
        artifacts: Mapping[str, ArtifactReference],
        validators: Sequence[ReleaseValidator] = (),
        metadata: Optional[Mapping[str, Any]] = None,
        commit_callback: Optional[ReleaseCommit] = None,
        commit_verifier: Optional[ReleaseCommitVerifier] = None,
    ) -> ReleasePointer:
        metadata_committed = False
        publish_lock_context = None
        reject_sensitive_data(release_id, location="release id")
        reject_sensitive_data(as_of_date, location="release as-of date")
        reject_sensitive_data(run_id, location="release run id")
        release_id = _validate_component(release_id, "release_id")
        _validate_component(run_id, "run_id")
        if not as_of_date:
            raise ContractViolation("as_of_date is required")
        try:
            parsed_as_of = date.fromisoformat(as_of_date)
        except (TypeError, ValueError) as exc:
            raise ContractViolation("as_of_date must be an ISO calendar date") from exc
        if parsed_as_of.isoformat() != as_of_date:
            raise ContractViolation("as_of_date must be an ISO calendar date")
        if not artifacts:
            raise ContractViolation("at least one release artifact is required")
        if commit_callback is not None and self.catalog is None:
            raise ContractViolation(
                "a release commit callback requires an operational catalog"
            )
        if commit_verifier is not None and commit_callback is None:
            raise ContractViolation(
                "a release commit verifier requires an atomic commit callback"
            )
        _validate_managed_path(self.data_root, self.releases_root)

        if self.catalog is not None:
            existing_run = self.catalog.get_run(run_id)
            if (
                commit_callback is None
                and existing_run is not None
                and existing_run["status"] != "succeeded"
            ):
                raise ContractViolation(
                    "an existing unfinished run requires an atomic release commit callback"
                )
            self.catalog.record_release_candidate(release_id, as_of_date, run_id)

        try:
            release_metadata = json.loads(
                canonical_json_bytes(dict(metadata or {})).decode("utf-8")
            )
            reject_sensitive_data(release_metadata, location="release metadata")
            artifact_contracts = {
                name: self._artifact_contract(name, reference)
                for name, reference in sorted(artifacts.items())
            }
            candidate: Dict[str, Any] = {
                "manifest_version": 1,
                "release_contract": "last-good-pointer-v1",
                "release_id": release_id,
                "as_of_date": as_of_date,
                "run_id": run_id,
                "artifacts": artifact_contracts,
                "metadata": release_metadata,
            }
            validation_results: Dict[str, Any] = {
                "artifact_checksums": "passed"
            }
            for index, validator in enumerate(validators):
                validator_name = getattr(
                    validator, "__name__", "validator_%d" % (index + 1)
                )
                result = validator(_freeze_validator_input(candidate))
                if result is False:  # type: ignore[comparison-overlap]
                    raise ReleaseValidationError(
                        "release validator failed: " + validator_name
                    )
                if result is None or result is True:  # type: ignore[comparison-overlap]
                    validation_results[validator_name] = "passed"
                elif isinstance(result, Mapping):
                    validation_evidence = json.loads(
                        canonical_json_bytes(dict(result)).decode("utf-8")
                    )
                    reject_sensitive_data(
                        validation_evidence,
                        location="release validation evidence",
                    )
                    validation_results[validator_name] = validation_evidence
                else:
                    raise ContractViolation(
                        "release validators must return None, True, False, or a mapping"
                    )
            candidate["validation"] = validation_results
            reject_sensitive_data(candidate, location="release candidate")
            self._verify_artifact_contracts(artifact_contracts)
            manifest_payload = canonical_json_bytes(candidate) + b"\n"
            manifest_checksum = sha256_bytes(manifest_payload)

            final_dir = self.releases_root / release_id
            manifest_path = final_dir / "manifest.json"
            _validate_managed_path(self.data_root, manifest_path)
            manifest_relative = manifest_path.relative_to(self.data_root).as_posix()
            self._install_release_directory(final_dir, manifest_payload)

            lock_context = _release_publish_lock(self.releases_root)
            lock_context.__enter__()
            publish_lock_context = lock_context
            published_at = utc_now_iso()
            if self.catalog is not None:
                # A process may stop after the candidate is atomically bound
                # but before the publish intent is written.  Reusing the exact
                # stored timestamp makes an identical same-run retry
                # idempotent; a different manifest or validation contract is
                # still rejected by bind_release_candidate below.
                existing_candidate = self.catalog.get_release(release_id)
                expected_validation_json = canonical_json_bytes(
                    validation_results
                ).decode("utf-8")
                if (
                    existing_candidate is not None
                    and existing_candidate["status"] == "candidate"
                    and existing_candidate["manifest_relative_path"]
                    == manifest_relative
                    and existing_candidate["manifest_checksum"]
                    == manifest_checksum
                    and existing_candidate["validation_json"]
                    == expected_validation_json
                    and isinstance(existing_candidate["published_at"], str)
                    and existing_candidate["published_at"].strip()
                ):
                    published_at = str(existing_candidate["published_at"])
            pointer = ReleasePointer(
                release_id=release_id,
                manifest_relative_path=manifest_relative,
                manifest_checksum=manifest_checksum,
                published_at=published_at,
            )
            if self.catalog is not None:
                self.catalog.bind_release_candidate(pointer, validation_results)
            self.releases_root.mkdir(parents=True, exist_ok=True)
            pointer_path = self.releases_root / "current.json"
            pointer_temporary = self.releases_root / (
                ".current.tmp-" + uuid.uuid4().hex
            )
            _validate_managed_path(self.data_root, pointer_path)
            _validate_managed_path(self.data_root, pointer_temporary)
            pointer_payload = canonical_json_bytes(pointer.to_dict()) + b"\n"
            previous_pointer = self.current(_publish_lock_held=True)
            previous_pointer_payload = (
                canonical_json_bytes(previous_pointer.to_dict()) + b"\n"
                if previous_pointer is not None
                else None
            )
            pointer_replaced = False
            intent_written = False
            publish_intent_id: Optional[str] = None
            try:
                _write_file_fully(pointer_temporary, pointer_payload)
                publish_intent_id = self._write_publish_intent(
                    pointer,
                    previous_pointer,
                    requires_succeeded_run=commit_callback is not None,
                )
                intent_written = True
                self._verify_installed_candidate(
                    pointer, manifest_payload, artifact_contracts
                )
                os.replace(str(pointer_temporary), str(pointer_path))
                pointer_replaced = True
                _fsync_directory(self.releases_root)
                if commit_callback is not None:
                    assert self.catalog is not None
                    callback_validation = json.loads(
                        canonical_json_bytes(validation_results).decode("utf-8")
                    )
                    try:
                        commit_callback(pointer, callback_validation)
                    except Exception as callback_exc:
                        # The callback may have committed before raising. Check
                        # durable state; ambiguous verification I/O preserves
                        # the new pointer and intent for restart reconciliation.
                        metadata_committed = True
                        try:
                            self._verify_installed_candidate(
                                pointer,
                                manifest_payload,
                                artifact_contracts,
                                require_current_pointer=True,
                            )
                        except (ContractViolation, ReleaseValidationError):
                            self._quarantine_invalid_callback_commit(
                                release_id,
                                run_id,
                                "callback changed a bound release artifact",
                            )
                            metadata_committed = False
                            raise callback_exc
                        try:
                            callback_committed = self._callback_commit_matches(
                                pointer,
                                run_id,
                                validation_results,
                                commit_verifier,
                            )
                        except Exception:
                            raise callback_exc
                        if callback_committed:
                            try:
                                self._clear_publish_intent(publish_intent_id)
                                intent_written = False
                            except (OSError, ReleaseValidationError):
                                pass
                            raise callback_exc
                        self._quarantine_invalid_callback_commit(
                            release_id,
                            run_id,
                            "callback failed before an exact catalog commit",
                        )
                        metadata_committed = False
                        raise callback_exc

                    # A callback that returned may already have committed its
                    # transaction. Treat verification I/O failures as
                    # ambiguous post-commit state so the pointer is not rolled
                    # back against a durable catalog commit.
                    metadata_committed = True
                    try:
                        self._verify_installed_candidate(
                            pointer,
                            manifest_payload,
                            artifact_contracts,
                            require_current_pointer=True,
                        )
                    except (ContractViolation, ReleaseValidationError):
                        self._quarantine_invalid_callback_commit(
                            release_id,
                            run_id,
                            "callback changed a bound release artifact",
                        )
                        metadata_committed = False
                        raise
                    if not self._callback_commit_matches(
                        pointer,
                        run_id,
                        validation_results,
                        commit_verifier,
                    ):
                        self._quarantine_invalid_callback_commit(
                            release_id,
                            run_id,
                            "callback metadata did not match the release pointer",
                        )
                        metadata_committed = False
                        raise ContractViolation(
                            "release commit callback did not publish catalog metadata matching the pointer and run"
                        )
                elif self.catalog is not None:
                    self.catalog.mark_release_published(
                        release_id,
                        manifest_relative,
                        manifest_checksum,
                        validation_results,
                        published_at,
                    )
                    metadata_committed = True
                    try:
                        self._verify_installed_candidate(
                            pointer,
                            manifest_payload,
                            artifact_contracts,
                            require_current_pointer=True,
                        )
                    except (ContractViolation, ReleaseValidationError):
                        self._quarantine_invalid_callback_commit(
                            release_id,
                            run_id,
                            "published release files changed before return",
                        )
                        metadata_committed = False
                        raise
                else:
                    self._verify_installed_candidate(
                        pointer,
                        manifest_payload,
                        artifact_contracts,
                        require_current_pointer=True,
                    )
                metadata_committed = True
                try:
                    self._clear_publish_intent(publish_intent_id)
                    intent_written = False
                except (OSError, ReleaseValidationError):
                    # The committed catalog and new pointer are authoritative.
                    # A later reader will retry cleanup through reconciliation.
                    pass
            except Exception:
                if pointer_replaced and not metadata_committed:
                    self._restore_pointer(pointer_path, previous_pointer_payload)
                if intent_written and not metadata_committed:
                    try:
                        self._clear_publish_intent(publish_intent_id)
                    except (OSError, ReleaseValidationError):
                        pass
                raise
            finally:
                try:
                    pointer_temporary.unlink()
                except FileNotFoundError:
                    pass
            return pointer
        except Exception as exc:
            if self.catalog is not None and not metadata_committed:
                try:
                    self.catalog.mark_release_failed(
                        release_id,
                        {
                            "type": type(exc).__name__,
                            "message": redact_sensitive(exc),
                        },
                    )
                except ContractViolation:
                    # Never mask the original validation/storage error.
                    pass
            raise
        finally:
            if publish_lock_context is not None:
                publish_lock_context.__exit__(None, None, None)

    @property
    def _intent_path(self) -> Path:
        return self.releases_root / ".publish-intent.json"

    @staticmethod
    def _pointer_from_payload(payload: Any, *, location: str) -> ReleasePointer:
        if not isinstance(payload, Mapping) or set(payload) != {
            "release_id",
            "manifest_relative_path",
            "manifest_checksum",
            "published_at",
        }:
            raise ReleaseValidationError(
                location + " must be a JSON object release pointer"
            )
        for field in (
            "release_id",
            "manifest_relative_path",
            "manifest_checksum",
            "published_at",
        ):
            if not isinstance(payload[field], str) or not payload[field].strip():
                raise ReleaseValidationError(location + " has a blank field")
        _validate_component(payload["release_id"], location + " release_id")
        if re.fullmatch(r"[0-9a-f]{64}", payload["manifest_checksum"]) is None:
            raise ReleaseValidationError(location + " checksum is invalid")
        return ReleasePointer(
            release_id=payload["release_id"],
            manifest_relative_path=payload["manifest_relative_path"],
            manifest_checksum=payload["manifest_checksum"],
            published_at=payload["published_at"],
        )

    def _write_publish_intent(
        self,
        pointer: ReleasePointer,
        previous_pointer: Optional[ReleasePointer],
        *,
        requires_succeeded_run: bool,
    ) -> str:
        """Durably record enough state to recover a process-level crash."""

        intent_path = self._intent_path
        _validate_managed_path(self.data_root, intent_path)
        self.releases_root.mkdir(parents=True, exist_ok=True)
        intent_id = uuid.uuid4().hex
        release_row = (
            self.catalog.get_release(pointer.release_id)
            if self.catalog is not None
            else None
        )
        payload = canonical_json_bytes(
            {
                "intent_contract": "release-publish-intent-v2",
                "intent_id": intent_id,
                "release_id": pointer.release_id,
                "run_id": (
                    release_row["run_id"]
                    if release_row is not None
                    else None
                ),
                "owner_pid": os.getpid(),
                "created_at": utc_now_iso(),
                "requires_succeeded_run": requires_succeeded_run,
                "new_pointer": pointer.to_dict(),
                "previous_pointer": (
                    previous_pointer.to_dict()
                    if previous_pointer is not None
                    else None
                ),
            }
        ) + b"\n"
        temporary = self.releases_root / (
            ".publish-intent.tmp-" + uuid.uuid4().hex
        )
        _validate_managed_path(self.data_root, temporary)
        try:
            _write_file_fully(temporary, payload)
            try:
                os.link(str(temporary), str(intent_path))
            except FileExistsError as exc:
                raise ReleaseValidationError(
                    "another release publish intent is still unresolved"
                ) from exc
            _fsync_directory(self.releases_root)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return intent_id

    def _read_publish_intent(self) -> Optional[Dict[str, Any]]:
        intent_path = self._intent_path
        _validate_managed_path(self.data_root, intent_path)
        if not intent_path.is_file():
            return None
        try:
            payload = json.loads(intent_path.read_text(encoding="utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ReleaseValidationError("release publish intent is invalid") from exc
        expected_fields = {
            "intent_contract",
            "intent_id",
            "release_id",
            "run_id",
            "owner_pid",
            "created_at",
            "requires_succeeded_run",
            "new_pointer",
            "previous_pointer",
        }
        if not isinstance(payload, dict) or set(payload) != expected_fields:
            raise ReleaseValidationError("release publish intent shape is invalid")
        if payload["intent_contract"] != "release-publish-intent-v2":
            raise ReleaseValidationError("release publish intent version is unsupported")
        if not isinstance(payload["intent_id"], str) or re.fullmatch(
            r"[0-9a-f]{32}", payload["intent_id"]
        ) is None:
            raise ReleaseValidationError("release publish intent ID is invalid")
        if (
            not isinstance(payload["owner_pid"], int)
            or isinstance(payload["owner_pid"], bool)
            or payload["owner_pid"] <= 0
        ):
            raise ReleaseValidationError("release publish intent owner is invalid")
        if not isinstance(payload["created_at"], str) or not payload[
            "created_at"
        ].strip():
            raise ReleaseValidationError("release publish intent time is invalid")
        if not isinstance(payload["requires_succeeded_run"], bool):
            raise ReleaseValidationError(
                "release publish intent run requirement is invalid"
            )
        new_pointer = self._pointer_from_payload(
            payload["new_pointer"], location="new publish intent pointer"
        )
        if payload["release_id"] != new_pointer.release_id:
            raise ReleaseValidationError("release publish intent ID is inconsistent")
        if payload["run_id"] is not None and (
            not isinstance(payload["run_id"], str)
            or not payload["run_id"].strip()
        ):
            raise ReleaseValidationError("release publish intent run is invalid")
        if payload["previous_pointer"] is not None:
            self._pointer_from_payload(
                payload["previous_pointer"],
                location="previous publish intent pointer",
            )
        return payload

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _clear_publish_intent(
        self, expected_intent_id: Optional[str] = None
    ) -> None:
        if expected_intent_id is None:
            raise ContractViolation("publish intent ownership is required")
        claim_path = self.releases_root / (
            ".publish-intent.clear-%s-%s"
            % (expected_intent_id, uuid.uuid4().hex)
        )
        _validate_managed_path(self.data_root, claim_path)
        try:
            os.rename(str(self._intent_path), str(claim_path))
        except FileNotFoundError:
            return
        try:
            try:
                claimed_payload = json.loads(
                    claim_path.read_text(encoding="utf-8")
                )
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ReleaseValidationError(
                    "claimed release publish intent is invalid"
                ) from exc
            if (
                not isinstance(claimed_payload, Mapping)
                or claimed_payload.get("intent_id") != expected_intent_id
            ):
                raise ReleaseValidationError(
                    "refusing to clear another release publish intent"
                )
        except Exception:
            try:
                os.link(str(claim_path), str(self._intent_path))
                _fsync_directory(self.releases_root)
            except FileExistsError:
                pass
            raise
        else:
            claim_path.unlink()
            _fsync_directory(self.releases_root)

    def _reconcile_publish_intent(
        self,
        *,
        publish_lock_held: bool = False,
    ) -> tuple[bool, Optional[ReleasePointer]]:
        """Recover an interrupted publish or hide an in-flight pointer."""

        if self.catalog is not None and not publish_lock_held:
            _validate_managed_path(self.data_root, self.releases_root)
            with _release_publish_lock(self.releases_root):
                return self._reconcile_publish_intent(
                    publish_lock_held=True
                )

        intent = self._read_publish_intent()
        if intent is None:
            return False, None
        previous = (
            self._pointer_from_payload(
                intent["previous_pointer"],
                location="previous publish intent pointer",
            )
            if intent["previous_pointer"] is not None
            else None
        )
        if self.catalog is None:
            # A read-only consumer cannot distinguish a crash before the
            # catalog transaction from a crash after it committed but before
            # intent cleanup.  Never mutate recovery state without that
            # evidence; hide the provisional pointer and leave reconciliation
            # to a catalog-aware process.
            return True, previous
        release_row = (
            self.catalog.get_release(intent["release_id"])
            if self.catalog is not None
            else None
        )
        run_row = (
            self.catalog.get_run(intent["run_id"])
            if self.catalog is not None and intent["run_id"] is not None
            else None
        )
        new_pointer = self._pointer_from_payload(
            intent["new_pointer"], location="new publish intent pointer"
        )
        release_matches_pointer = (
            release_row is not None
            and release_row["status"] == "published"
            and release_row["run_id"] == intent["run_id"]
            and release_row["manifest_relative_path"]
            == new_pointer.manifest_relative_path
            and release_row["manifest_checksum"]
            == new_pointer.manifest_checksum
            and release_row["published_at"] == new_pointer.published_at
        )
        run_commit_satisfied = (
            run_row is not None and run_row["status"] == "succeeded"
            if intent["requires_succeeded_run"]
            else run_row is None or run_row["status"] == "succeeded"
        )
        committed = release_matches_pointer and run_commit_satisfied
        if committed:
            try:
                pointer_path = self.releases_root / "current.json"
                _validate_managed_path(self.data_root, pointer_path)
                if not pointer_path.is_file():
                    raise ReleaseValidationError(
                        "committed publish pointer is missing"
                    )
                try:
                    pointer_payload = json.loads(
                        pointer_path.read_bytes().decode("utf-8")
                    )
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise ReleaseValidationError(
                        "committed publish pointer is invalid"
                    ) from exc
                installed_pointer = self._pointer_from_payload(
                    pointer_payload, location="committed publish pointer"
                )
                if installed_pointer != new_pointer:
                    raise ReleaseValidationError(
                        "committed publish pointer does not match intent"
                    )
                manifest = self._validated_manifest_for_pointer(new_pointer)
                if (
                    manifest["run_id"] != intent["run_id"]
                    or release_row["as_of_date"] != manifest["as_of_date"]
                    or release_row["validation_json"]
                    != canonical_json_bytes(manifest["validation"]).decode("utf-8")
                ):
                    raise ReleaseValidationError(
                        "committed publish catalog does not match manifest"
                    )
            except OSError:
                # Storage availability is ambiguous. Hide the provisional
                # pointer and leave the durable intent for a later retry.
                return True, previous
            except (ContractViolation, ReleaseValidationError) as exc:
                previous_payload = (
                    canonical_json_bytes(previous.to_dict()) + b"\n"
                    if previous is not None
                    else None
                )
                self.catalog.quarantine_invalid_publish_commit(
                    intent["release_id"],
                    intent["run_id"],
                    {
                        "type": "InvalidRecoveredPublish",
                        "message": redact_sensitive(exc),
                    },
                )
                self._restore_pointer(
                    self.releases_root / "current.json", previous_payload
                )
                self._clear_publish_intent(intent["intent_id"])
                return False, None
            try:
                self._clear_publish_intent(intent["intent_id"])
            except OSError:
                pass
            return False, None
        if self._pid_is_alive(intent["owner_pid"]):
            return True, previous

        previous_payload = (
            canonical_json_bytes(previous.to_dict()) + b"\n"
            if previous is not None
            else None
        )
        self._restore_pointer(
            self.releases_root / "current.json", previous_payload
        )
        if self.catalog is not None:
            if release_row is not None and release_row["status"] == "candidate":
                self.catalog.mark_release_failed(
                    intent["release_id"],
                    {
                        "type": "InterruptedPublish",
                        "message": "release publish was interrupted before metadata commit",
                    },
                )
            if run_row is not None and run_row["status"] == "running":
                self.catalog.finish_run(
                    intent["run_id"],
                    False,
                    {
                        "type": "InterruptedPublish",
                        "message": "release publish was interrupted before metadata commit",
                    },
                    result={"status": "failed", "recovered_publish_intent": True},
                )
        self._clear_publish_intent(intent["intent_id"])
        return False, None

    def _restore_pointer(
        self, pointer_path: Path, previous_payload: Optional[bytes]
    ) -> None:
        """Compensate a failed metadata commit after pointer replacement."""

        _validate_managed_path(self.data_root, pointer_path)
        if previous_payload is None:
            try:
                pointer_path.unlink()
            except FileNotFoundError:
                pass
            _fsync_directory(self.releases_root)
            return
        temporary = self.releases_root / (
            ".current.restore-" + uuid.uuid4().hex
        )
        _validate_managed_path(self.data_root, temporary)
        try:
            _write_file_fully(temporary, previous_payload)
            os.replace(str(temporary), str(pointer_path))
            _fsync_directory(self.releases_root)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _install_release_directory(
        self, final_dir: Path, manifest_payload: bytes
    ) -> None:
        _validate_managed_path(self.data_root, final_dir)
        if final_dir.exists():
            manifest_path = final_dir / "manifest.json"
            if not manifest_path.is_file() or manifest_path.read_bytes() != manifest_payload:
                raise ContractViolation(
                    "release_id already exists with a different immutable manifest"
                )
            return

        self.releases_root.mkdir(parents=True, exist_ok=True)
        temporary = self.releases_root / (
            ".%s.tmp-%s" % (final_dir.name, uuid.uuid4().hex)
        )
        _validate_managed_path(self.data_root, temporary)
        temporary.mkdir()
        try:
            _write_file_fully(temporary / "manifest.json", manifest_payload)
            _fsync_directory(temporary)
            try:
                os.rename(str(temporary), str(final_dir))
                _fsync_directory(self.releases_root)
            except OSError:
                if not final_dir.exists():
                    raise
                manifest_path = final_dir / "manifest.json"
                if (
                    not manifest_path.is_file()
                    or manifest_path.read_bytes() != manifest_payload
                ):
                    raise ContractViolation(
                        "release_id was concurrently published with another manifest"
                    )
        finally:
            if temporary.exists():
                shutil.rmtree(str(temporary))

    def current(
        self, *, _publish_lock_held: bool = False
    ) -> Optional[ReleasePointer]:
        snapshot = self._current_snapshot(
            _publish_lock_held=_publish_lock_held
        )
        return snapshot[0] if snapshot is not None else None

    def _current_snapshot(
        self, *, _publish_lock_held: bool = False
    ) -> Optional[tuple[ReleasePointer, Dict[str, Any]]]:
        """Load and validate one pointer/manifest byte snapshot."""

        publish_in_flight, previous_pointer = self._reconcile_publish_intent(
            publish_lock_held=_publish_lock_held
        )
        if publish_in_flight:
            if previous_pointer is None:
                return None
            pointer = previous_pointer
        else:
            pointer = None
        pointer_path = self.releases_root / "current.json"
        _validate_managed_path(self.data_root, pointer_path)
        if pointer is None:
            if not pointer_path.is_file():
                return None
            try:
                payload = json.loads(pointer_path.read_text(encoding="utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ReleaseValidationError(
                    "current release pointer is invalid"
                ) from exc
            pointer = self._pointer_from_payload(
                payload, location="current release pointer"
            )
        try:
            manifest = self._validated_manifest_for_pointer(pointer)
        except OSError as exc:
            raise ReleaseValidationError(
                "current release manifest cannot be read"
            ) from exc
        as_of_date = manifest["as_of_date"]
        run_id = manifest["run_id"]
        if self.catalog is not None:
            release_row = self.catalog.get_release(pointer.release_id)
            expected_validation_json = canonical_json_bytes(
                manifest["validation"]
            ).decode("utf-8")
            if (
                release_row is None
                or release_row["status"] != "published"
                or release_row["as_of_date"] != as_of_date
                or release_row["run_id"] != run_id
                or release_row["manifest_relative_path"]
                != pointer.manifest_relative_path
                or release_row["manifest_checksum"] != pointer.manifest_checksum
                or release_row["published_at"] != pointer.published_at
                or release_row["validation_json"] != expected_validation_json
            ):
                raise ReleaseValidationError(
                    "current release catalog metadata does not match pointer"
                )
            run_row = self.catalog.get_run(run_id)
            if run_row is not None and run_row["status"] != "succeeded":
                raise ReleaseValidationError(
                    "current release run is not successfully completed"
                )
        return pointer, manifest

    def current_manifest(self) -> Optional[Dict[str, Any]]:
        snapshot = self._current_snapshot()
        if snapshot is None:
            return None
        return snapshot[1]
