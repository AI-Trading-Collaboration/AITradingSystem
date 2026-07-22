from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.historical_portable_source_archive import (
    DEFAULT_MANIFEST_PATH,
    DEFAULT_POLICY_PATH,
    HistoricalPortableSourceArchive,
    HistoricalPortableSourceArchiveError,
    load_historical_portable_source_archive_policy,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_TRADING2449_SIDECAR_PATH,
)


def test_tracked_archive_resolves_only_two_exact_historical_sources() -> None:
    sidecar = _read_json(DEFAULT_TRADING2449_SIDECAR_PATH)
    archive = HistoricalPortableSourceArchive(
        manifest_path=DEFAULT_MANIFEST_PATH,
        expected_sidecar_id=sidecar["sidecar_id"],
        expected_sidecar_sha256=_sha256(DEFAULT_TRADING2449_SIDECAR_PATH),
        frozen_source_records=sidecar["sources"],
    )
    sources = {record["binding_id"]: record for record in sidecar["sources"]}

    expected = {
        "source_100e2a05abff91305c13": (
            6866,
            "0f81afa123d5646548496951a912b04a00dd4535b77ef60de5760ed6f02bf476",
            "historical_path_must_match_when_present",
        ),
        "source_2c9c99f56d401726e639": (
            49482,
            "626ad4a44cde8cfd6d29442ad2514aed776689aae77a9f7c878e278969e4156e",
            "active_locator_superseded_by_window_migration",
        ),
    }
    for binding_id, (size, digest, disposition) in expected.items():
        resolution = archive.resolve(sources[binding_id])
        assert resolution is not None
        assert resolution.path.stat().st_size == size
        assert _sha256(resolution.path) == digest
        assert resolution.legacy_locator_disposition == disposition
    assert archive.resolve(sources["source_f3af401bd04447415bc1"]) is None
    assert archive.evidence()["source_binding_count"] == 2
    assert load_historical_portable_source_archive_policy()["safety"]["production_effect"] == "none"


def test_archive_missing_or_tampered_bytes_fail_closed(tmp_path: Path) -> None:
    bundle = _copy_bundle(tmp_path)
    sidecar = _read_json(bundle["sidecar"])
    manifest = _read_json(bundle["manifest"])
    first = manifest["sources"][0]
    source_path = tmp_path / Path(first["archive_locator"]["path"])
    frozen = _source(sidecar, first["binding_id"])

    source_path.unlink()
    archive = _archive(bundle, sidecar)
    with pytest.raises(HistoricalPortableSourceArchiveError) as missing:
        archive.resolve(frozen)
    assert missing.value.reason_code == "ARCHIVE_SOURCE_MISSING"

    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(b"tampered")
    archive = _archive(bundle, sidecar)
    with pytest.raises(HistoricalPortableSourceArchiveError) as tampered:
        archive.resolve(frozen)
    assert tampered.value.reason_code == "ARCHIVE_SOURCE_TAMPERED"


def test_archive_manifest_content_id_covers_provenance(tmp_path: Path) -> None:
    bundle = _copy_bundle(tmp_path)
    manifest = _read_json(bundle["manifest"])
    manifest["sources"][0]["provenance"]["source_commit"] = "0" * 40
    bundle["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    sidecar = _read_json(bundle["sidecar"])

    with pytest.raises(HistoricalPortableSourceArchiveError) as exc:
        _archive(bundle, sidecar)
    assert exc.value.reason_code == "ARCHIVE_CONTENT_ID_MISMATCH"


def test_archive_rejects_recomputed_traversal_and_disposition_drift(
    tmp_path: Path,
) -> None:
    for mutation, reason in (
        ("traversal", "ARCHIVE_LOCATOR_PATH_TRAVERSAL"),
        ("disposition", "ARCHIVE_MANIFEST_SCHEMA_INVALID"),
    ):
        root = tmp_path / mutation
        bundle = _copy_bundle(root)
        manifest = _read_json(bundle["manifest"])
        record = manifest["sources"][1]
        if mutation == "traversal":
            record["archive_locator"]["path"] = "../outside.yaml"
        else:
            record["legacy_locator_disposition"] = "unreviewed_supersession"
        manifest["archive_id"] = _archive_id(manifest)
        bundle["manifest"].write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        sidecar = _read_json(bundle["sidecar"])

        with pytest.raises(HistoricalPortableSourceArchiveError) as exc:
            _archive(bundle, sidecar)
        assert exc.value.reason_code == reason


@pytest.mark.parametrize(
    "locator",
    [
        "/absolute/outside.yaml",
        r"C:\absolute\outside.yaml",
        r"\\server\share\outside.yaml",
    ],
)
def test_archive_rejects_absolute_and_unc_locators(tmp_path: Path, locator: str) -> None:
    bundle = _copy_bundle(tmp_path)
    manifest = _read_json(bundle["manifest"])
    manifest["sources"][0]["archive_locator"]["path"] = locator
    manifest["archive_id"] = _archive_id(manifest)
    bundle["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    sidecar = _read_json(bundle["sidecar"])

    with pytest.raises(HistoricalPortableSourceArchiveError) as exc:
        _archive(bundle, sidecar)
    assert exc.value.reason_code == "ARCHIVE_LOCATOR_PATH_TRAVERSAL"


def test_archive_rejects_symlink_escape(tmp_path: Path) -> None:
    bundle = _copy_bundle(tmp_path)
    manifest = _read_json(bundle["manifest"])
    record = manifest["sources"][0]
    archive_path = tmp_path / Path(record["archive_locator"]["path"])
    outside = tmp_path.parent / f"{tmp_path.name}-outside-archive-source.yaml"
    outside.write_bytes(archive_path.read_bytes())
    archive_path.unlink()
    try:
        os.symlink(outside, archive_path)
    except OSError as exc:
        outside.unlink(missing_ok=True)
        pytest.skip(f"symlink unavailable on this platform: {exc}")
    try:
        sidecar = _read_json(bundle["sidecar"])
        with pytest.raises(HistoricalPortableSourceArchiveError) as caught:
            _archive(bundle, sidecar)
        assert caught.value.reason_code == "ARCHIVE_LOCATOR_OUTSIDE_PROJECT_ROOT"
    finally:
        archive_path.unlink(missing_ok=True)
        outside.unlink(missing_ok=True)


def test_archive_rejects_duplicate_binding_and_sidecar_drift(tmp_path: Path) -> None:
    duplicate_root = tmp_path / "duplicate"
    bundle = _copy_bundle(duplicate_root)
    manifest = _read_json(bundle["manifest"])
    manifest["sources"].append(dict(manifest["sources"][0]))
    manifest["archive_id"] = _archive_id(manifest)
    bundle["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    sidecar = _read_json(bundle["sidecar"])
    with pytest.raises(HistoricalPortableSourceArchiveError) as duplicate:
        _archive(bundle, sidecar)
    assert duplicate.value.reason_code == "ARCHIVE_MANIFEST_SCHEMA_INVALID"

    unknown_root = tmp_path / "unknown-binding"
    bundle = _copy_bundle(unknown_root)
    manifest = _read_json(bundle["manifest"])
    manifest["sources"][0]["binding_id"] = "source_not_in_frozen_sidecar"
    manifest["archive_id"] = _archive_id(manifest)
    bundle["manifest"].write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    sidecar = _read_json(bundle["sidecar"])
    with pytest.raises(HistoricalPortableSourceArchiveError) as unknown:
        _archive(bundle, sidecar)
    assert unknown.value.reason_code == "ARCHIVE_SOURCE_BINDING_MISMATCH"

    drift_root = tmp_path / "sidecar-drift"
    bundle = _copy_bundle(drift_root)
    sidecar = _read_json(bundle["sidecar"])
    with pytest.raises(HistoricalPortableSourceArchiveError) as drift:
        HistoricalPortableSourceArchive(
            manifest_path=bundle["manifest"],
            expected_sidecar_id=sidecar["sidecar_id"],
            expected_sidecar_sha256="0" * 64,
            frozen_source_records=sidecar["sources"],
            project_root=drift_root,
            policy_path=bundle["policy"],
        )
    assert drift.value.reason_code == "ARCHIVE_SIDECAR_BINDING_MISMATCH"


def test_archive_policy_drift_fails_closed(tmp_path: Path) -> None:
    bundle = _copy_bundle(tmp_path)
    text = bundle["policy"].read_text(encoding="utf-8")
    bundle["policy"].write_text(
        text.replace("explicit_opt_in_required: true", "explicit_opt_in_required: false"),
        encoding="utf-8",
    )
    sidecar = _read_json(bundle["sidecar"])
    with pytest.raises(HistoricalPortableSourceArchiveError) as exc:
        _archive(bundle, sidecar)
    assert exc.value.reason_code == "ARCHIVE_POLICY_SCHEMA_INVALID"


def _copy_bundle(root: Path) -> dict[str, Path]:
    policy = root / DEFAULT_POLICY_PATH.relative_to(PROJECT_ROOT)
    sidecar = root / DEFAULT_TRADING2449_SIDECAR_PATH.relative_to(PROJECT_ROOT)
    manifest = root / DEFAULT_MANIFEST_PATH.relative_to(PROJECT_ROOT)
    for source, target in (
        (DEFAULT_POLICY_PATH, policy),
        (DEFAULT_TRADING2449_SIDECAR_PATH, sidecar),
        (DEFAULT_MANIFEST_PATH, manifest),
    ):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    payload = _read_json(DEFAULT_MANIFEST_PATH)
    for record in payload["sources"]:
        relative = Path(record["archive_locator"]["path"])
        source = PROJECT_ROOT / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return {"policy": policy, "sidecar": sidecar, "manifest": manifest}


def _archive(bundle: dict[str, Path], sidecar: dict[str, Any]) -> HistoricalPortableSourceArchive:
    return HistoricalPortableSourceArchive(
        manifest_path=bundle["manifest"],
        expected_sidecar_id=sidecar["sidecar_id"],
        expected_sidecar_sha256=_sha256(bundle["sidecar"]),
        frozen_source_records=sidecar["sources"],
        project_root=bundle["manifest"].parents[3],
        policy_path=bundle["policy"],
    )


def _source(sidecar: dict[str, Any], binding_id: str) -> dict[str, Any]:
    return next(record for record in sidecar["sources"] if record["binding_id"] == binding_id)


def _archive_id(manifest: dict[str, Any]) -> str:
    unsigned = dict(manifest)
    unsigned.pop("archive_id", None)
    encoded = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "historical-source-archive_" + hashlib.sha256(encoded).hexdigest()[:20]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
