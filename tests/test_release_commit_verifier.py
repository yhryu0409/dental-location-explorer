from pathlib import Path

import pytest

from dental_market.contracts import ContractViolation
from dental_market.metadata_store import OpsCatalog
from dental_market.release import ReleaseManager
from dental_market.storage import AtomicLayerWriter


def _publish_catalog_commit(catalog, run_id, pointer, validation):
    catalog.mark_release_published(
        pointer.release_id,
        pointer.manifest_relative_path,
        pointer.manifest_checksum,
        validation,
        pointer.published_at,
    )
    catalog.finish_run(run_id, True, result={"status": "published"})


def test_commit_verifier_rejects_partial_domain_commit_and_restores_pointer(
    tmp_path: Path,
):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    baseline = releases.publish(
        "verifier-baseline-release",
        "2026-09-01",
        "verifier-baseline-run",
        {"mart": writer.write_jsonl("mart", "verifier-baseline", [{"id": "A"}])},
    )
    run_id = "verifier-rejected-run"
    release_id = "verifier-rejected-release"
    catalog.start_run(run_id, "verifier-test")

    def commit(pointer, validation):
        _publish_catalog_commit(catalog, run_id, pointer, validation)

    with pytest.raises(ContractViolation, match="did not publish catalog"):
        releases.publish(
            release_id,
            "2026-09-02",
            run_id,
            {
                "mart": writer.write_jsonl(
                    "mart", "verifier-rejected", [{"id": "B"}]
                )
            },
            commit_callback=commit,
            commit_verifier=lambda _pointer, _validation: False,
        )

    assert releases.current() == baseline
    assert catalog.get_release(release_id)["status"] == "failed"
    assert catalog.get_run(run_id)["status"] == "failed"
    assert not (tmp_path / "releases" / ".publish-intent.json").exists()


def test_commit_verifier_runs_after_exact_callback_commit(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)
    run_id = "verifier-accepted-run"
    release_id = "verifier-accepted-release"
    catalog.start_run(run_id, "verifier-test")
    observations = []

    def commit(pointer, validation):
        _publish_catalog_commit(catalog, run_id, pointer, validation)

    def verify(pointer, validation):
        observations.append((pointer.release_id, validation["artifact_checksums"]))
        return catalog.get_run(run_id)["status"] == "succeeded"

    pointer = releases.publish(
        release_id,
        "2026-09-02",
        run_id,
        {
            "mart": writer.write_jsonl(
                "mart", "verifier-accepted", [{"id": "B"}]
            )
        },
        commit_callback=commit,
        commit_verifier=verify,
    )

    assert releases.current() == pointer
    assert observations == [(release_id, "passed")]


def test_commit_verifier_requires_atomic_callback(tmp_path: Path):
    catalog = OpsCatalog(tmp_path / "ops.sqlite")
    writer = AtomicLayerWriter(tmp_path)
    releases = ReleaseManager(tmp_path, catalog)

    with pytest.raises(ContractViolation, match="requires an atomic commit callback"):
        releases.publish(
            "verifier-without-callback-release",
            "2026-09-01",
            "verifier-without-callback-run",
            {
                "mart": writer.write_jsonl(
                    "mart", "verifier-without-callback", [{"id": "A"}]
                )
            },
            commit_verifier=lambda _pointer, _validation: True,
        )

    assert catalog.get_release("verifier-without-callback-release") is None
    assert releases.current() is None
