"""Public provenance for the displayed score and the latest verified HIRA observation."""
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=2)
def dental_counts(path, expected_hash, record_count):
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
        raise ValueError("medical_source_hash_mismatch")
    facilities = doctors = records = missing_workforce = 0
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            records += 1
            if row["clCd"] not in ("41", "51"):
                continue
            facilities += 1
            counts = [row.get("dety" + key) for key in ("GdrCnt", "IntnCnt", "ResdntCnt", "SdrCnt")]
            if any(value is None or value == "" for value in counts):
                missing_workforce += 1
            else:
                doctors += sum(int(value) for value in counts)
    if records != record_count:
        raise ValueError("medical_source_count_mismatch")
    return {"facilities": facilities, "doctors": None if missing_workforce else doctors}


def latest_hira(directory):
    observations = []
    for manifest in (directory / "manifests").glob("*.json"):
        raw = manifest.read_bytes().rstrip(b"\n")
        receipt = json.loads(raw)
        if receipt.get("runner_kind") != "operational_hira_national_refresh" or not receipt.get("ok"):
            continue
        if hashlib.sha256(raw).hexdigest() != manifest.stem:
            raise ValueError("medical_receipt_hash_mismatch")
        if receipt["status"] == "verified_nationwide_model_input" and receipt["scope"] == "nationwide":
            observations.append((datetime.fromisoformat(receipt["observed_at"]), receipt))
    if not observations:
        return None
    when, receipt = max(observations, key=lambda pair: pair[0])
    path = Path(receipt["normalized_artifact_path"]).resolve()
    if not path.is_relative_to((directory / "hira").resolve()):
        raise ValueError("unexpected_medical_source")
    counts = dental_counts(path, receipt["normalized_artifact_sha256"], receipt["record_count"])
    return {**counts, "date": when.astimezone(timezone(timedelta(hours=9))).date().isoformat()}


def summary(model, directory):
    vintages = model.data["source_vintages"]
    result = {"population_month": vintages["population"],
              "baseline": {"date": vintages["healthcare"].removeprefix("API observed "),
                           "facilities": len(model.facilities),
                           "doctors": int(sum(row["doctors"] for row in model.facilities))}}
    try:
        result["latest"] = latest_hira(directory)
    except (OSError, EOFError, ValueError, KeyError, TypeError):
        result["latest"] = None
    return result
