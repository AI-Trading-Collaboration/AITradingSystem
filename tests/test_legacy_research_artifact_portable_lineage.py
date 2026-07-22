from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system import research_restart
from ai_trading_system import research_restart_decision as r2
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_r1_evidence as r1
from ai_trading_system.historical_portable_source_archive import (
    DEFAULT_MANIFEST_PATH,
)
from ai_trading_system.historical_portable_source_archive import (
    DEFAULT_POLICY_PATH as DEFAULT_HISTORICAL_ARCHIVE_POLICY_PATH,
)
from ai_trading_system.historical_portable_source_archive import (
    SAFETY as HISTORICAL_ARCHIVE_SAFETY,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_POLICY_PATH,
    DEFAULT_TRADING2449_SIDECAR_PATH,
    PortableLineageBinding,
    PortableLineageError,
    PortableLineageResolver,
    build_portable_lineage_sidecar,
    build_research_restart_portable_lineage_sidecar,
    load_portable_lineage_policy,
)


def test_resolver_replays_missing_historical_source_by_exact_content(tmp_path: Path) -> None:
    subject = _write(tmp_path / "portable" / "subject.json", b'{"id":"legacy"}\n')
    source = _write(tmp_path / "portable" / "source.bin", b"immutable-source")
    historical_subject = tmp_path / "missing-legacy" / "subject.json"
    historical_source = tmp_path / "missing-legacy" / "source.bin"
    sidecar = tmp_path / "portable-lineage.json"
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[_binding("artifact", historical_subject, subject, "r0_preflight")],
        sources=[_binding("source", historical_source, source, "r0_preflight")],
        project_root=tmp_path,
    )

    resolver = PortableLineageResolver(
        sidecar_path=sidecar,
        subject_artifact_path=subject,
        consumer="r0_preflight",
        project_root=tmp_path,
    )

    assert resolver.resolve(historical_source) == source.resolve()
    evidence = resolver.evidence()
    assert evidence["status"] == "PASS"
    assert evidence["policy_id"] == "legacy_research_artifact_portable_lineage_v1"
    assert evidence["legacy_artifact"]["sha256"] == _sha256(subject)
    assert evidence["resolved_sources"][0]["sha256"] == _sha256(source)
    assert evidence["production_effect"] == "none"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("require_policy_hash_binding: true", "require_policy_hash_binding: false"),
        (
            "historical_portable_conflict_action: fail_closed",
            "historical_portable_conflict_action: prefer_portable",
        ),
        ("allow_missing_historical_path: true", "allow_missing_historical_path: false"),
        ("- r2_decision", "- unexpected_consumer"),
        ("- PORTABLE_SOURCE_TAMPERED", "- PORTABLE_SOURCE_CHANGED"),
        ("persistence: tracked_repository_input", "persistence: transient_output"),
    ],
)
def test_policy_rejects_any_v1_fail_closed_contract_drift(
    tmp_path: Path, old: str, new: str
) -> None:
    raw = DEFAULT_POLICY_PATH.read_text(encoding="utf-8")
    assert old in raw
    mutated = tmp_path / "mutated_policy.yaml"
    mutated.write_text(raw.replace(old, new, 1), encoding="utf-8")

    with pytest.raises(PortableLineageError) as exc:
        load_portable_lineage_policy(mutated)
    assert exc.value.reason_code == "POLICY_SCHEMA_INVALID"


@pytest.mark.parametrize("entry", ["- r2_decision", "- PORTABLE_SOURCE_TAMPERED"])
def test_policy_rejects_duplicate_contract_entries(tmp_path: Path, entry: str) -> None:
    raw = DEFAULT_POLICY_PATH.read_text(encoding="utf-8")
    assert entry in raw
    mutated = tmp_path / "duplicate_policy.yaml"
    mutated.write_text(raw.replace(entry, f"{entry}\n    {entry}", 1), encoding="utf-8")

    with pytest.raises(PortableLineageError) as exc:
        load_portable_lineage_policy(mutated)
    assert exc.value.reason_code == "POLICY_SCHEMA_INVALID"


@pytest.mark.parametrize("mode", ["missing", "malformed"])
def test_policy_load_errors_are_normalized(tmp_path: Path, mode: str) -> None:
    path = tmp_path / "policy.yaml"
    if mode == "malformed":
        path.write_text("resolution: [unterminated", encoding="utf-8")

    with pytest.raises(PortableLineageError) as exc:
        load_portable_lineage_policy(path)
    assert exc.value.reason_code == "POLICY_SCHEMA_INVALID"


def test_resolver_fails_closed_when_historical_and_portable_conflict(
    tmp_path: Path,
) -> None:
    subject = _write(tmp_path / "portable" / "subject.json", b"subject")
    source = _write(tmp_path / "portable" / "source.bin", b"portable")
    historical_source = _write(tmp_path / "legacy" / "source.bin", b"historical")
    sidecar = tmp_path / "portable-lineage.json"
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[
            _binding("artifact", tmp_path / "missing" / "subject.json", subject, "r0_preflight")
        ],
        sources=[_binding("source", historical_source, source, "r0_preflight")],
        project_root=tmp_path,
    )

    with pytest.raises(PortableLineageError, match="HISTORICAL_PORTABLE_CONFLICT") as exc:
        PortableLineageResolver(
            sidecar_path=sidecar,
            subject_artifact_path=subject,
            consumer="r0_preflight",
            project_root=tmp_path,
        )
    assert exc.value.reason_code == "HISTORICAL_PORTABLE_CONFLICT"


def test_resolver_fails_closed_on_portable_bytes_or_sidecar_tamper(tmp_path: Path) -> None:
    subject, source, sidecar, historical_source = _minimal_sidecar(tmp_path)
    source.unlink()
    with pytest.raises(PortableLineageError) as missing_exc:
        PortableLineageResolver(
            sidecar_path=sidecar,
            subject_artifact_path=subject,
            consumer="r0_preflight",
            project_root=tmp_path,
        )
    assert missing_exc.value.reason_code == "PORTABLE_SOURCE_MISSING"

    source.write_bytes(b"tampered")
    with pytest.raises(PortableLineageError) as source_exc:
        PortableLineageResolver(
            sidecar_path=sidecar,
            subject_artifact_path=subject,
            consumer="r0_preflight",
            project_root=tmp_path,
        )
    assert source_exc.value.reason_code == "PORTABLE_SOURCE_TAMPERED"

    source.write_bytes(b"source")
    payload = _read_json(sidecar)
    payload["sources"][0]["legacy_path"] = str(historical_source.with_name("changed.bin"))
    sidecar.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(PortableLineageError) as sidecar_exc:
        PortableLineageResolver(
            sidecar_path=sidecar,
            subject_artifact_path=subject,
            consumer="r0_preflight",
            project_root=tmp_path,
        )
    assert sidecar_exc.value.reason_code == "SIDECAR_CONTENT_ID_MISMATCH"


def test_resolver_fails_closed_on_rehashed_path_traversal(tmp_path: Path) -> None:
    subject, _source, sidecar, _historical_source = _minimal_sidecar(tmp_path)
    payload = _read_json(sidecar)
    payload["sources"][0]["locator"]["path"] = "../outside.bin"
    payload["sidecar_id"] = _sidecar_id(payload)
    sidecar.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(PortableLineageError) as exc:
        PortableLineageResolver(
            sidecar_path=sidecar,
            subject_artifact_path=subject,
            consumer="r0_preflight",
            project_root=tmp_path,
        )
    assert exc.value.reason_code == "LOCATOR_PATH_TRAVERSAL"


def test_r0_portable_adapter_is_explicit_and_preserves_legacy_payload(tmp_path: Path) -> None:
    source = _write(tmp_path / "portable" / "source.bin", b"r0-source")
    markdown = tmp_path / "portable" / "preflight.md"
    artifact = tmp_path / "portable" / "preflight.json"
    legacy_source = tmp_path / "absent-legacy" / "source.bin"
    legacy_markdown = tmp_path / "absent-legacy" / "preflight.md"
    payload: dict[str, Any] = {
        "schema_version": research_restart.SCHEMA_VERSION,
        "report_type": research_restart.REPORT_TYPE,
        "status": "PASS",
        "research_execution_unblocked": True,
        "checks": [{"check_id": "fixture", "passed": True}],
        "safety": dict(research_restart.SAFETY_BOUNDARY),
        "window_semantics": {"status": "PASS"},
        "input_fingerprints": {
            "source": {"path": str(legacy_source), "sha256": _sha256(source), "exists": True}
        },
        "artifact_paths": {"json": str(artifact), "markdown": str(legacy_markdown)},
    }
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown.write_text(
        research_restart.render_research_restart_preflight(payload), encoding="utf-8"
    )
    original_bytes = artifact.read_bytes()
    sidecar = tmp_path / "portable-lineage.json"
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[
            _binding(
                "artifact_r0",
                tmp_path / "absent-legacy" / "preflight.json",
                artifact,
                "r0_preflight",
            )
        ],
        sources=[
            _binding("source", legacy_source, source, "r0_preflight"),
            _binding("markdown", legacy_markdown, markdown, "r0_preflight"),
        ],
        project_root=tmp_path,
    )

    direct = research_restart.validate_research_restart_preflight(artifact_path=artifact)
    portable = research_restart.validate_research_restart_preflight(
        artifact_path=artifact,
        portable_lineage_sidecar_path=sidecar,
        portable_project_root=tmp_path,
    )

    assert direct["status"] == "FAIL"
    assert portable["status"] == "PASS"
    assert portable["portable_lineage_resolution"]["mode"] == "explicit_portable_lineage"
    assert artifact.read_bytes() == original_bytes


def test_r1_and_r2_adapters_require_explicit_sidecar_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wf_id = "wf-fixture"
    robustness_id = "robustness-fixture"
    wf_manifest = _write(tmp_path / "wf" / wf_id / "r1_wf_manifest.json", b'{"immutable":true}\n')
    robustness_manifest = _write(
        tmp_path / "robustness" / robustness_id / "r1_robustness_manifest.json",
        b'{"immutable":true}\n',
    )
    r2_manifest = _write(
        tmp_path / "r2" / "strategy_research_restart_r2_manifest.json",
        b'{"immutable":true}\n',
    )
    immutable_before = {
        path: path.read_bytes() for path in (wf_manifest, robustness_manifest, r2_manifest)
    }
    sidecar = tmp_path / "portable-lineage.json"
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[
            _binding("wf", tmp_path / "legacy" / "wf.json", wf_manifest, "r1_walk_forward"),
            _binding(
                "robustness",
                tmp_path / "legacy" / "robustness.json",
                robustness_manifest,
                "r1_robustness",
            ),
            _binding("r2", tmp_path / "legacy" / "r2.json", r2_manifest, "r2_decision"),
        ],
        sources=[],
        project_root=tmp_path,
    )
    monkeypatch.setattr(
        r1,
        "_validate_r1_walk_forward_evidence",
        lambda **_kwargs: {"status": "PASS", "production_effect": "none"},
    )
    monkeypatch.setattr(
        r1,
        "_validate_r1_robustness_evidence",
        lambda **_kwargs: {"status": "PASS", "production_effect": "none"},
    )
    monkeypatch.setattr(
        r2,
        "_validate_strategy_research_restart_decision",
        lambda **_kwargs: {"status": "PASS", "production_effect": "none"},
    )

    wf = r1.validate_r1_walk_forward_evidence(
        walk_forward_id=wf_id,
        output_dir=tmp_path / "wf",
        portable_lineage_sidecar_path=sidecar,
        portable_project_root=tmp_path,
    )
    robustness = r1.validate_r1_robustness_evidence(
        robustness_id=robustness_id,
        output_dir=tmp_path / "robustness",
        portable_lineage_sidecar_path=sidecar,
        portable_project_root=tmp_path,
    )
    decision = r2.validate_strategy_research_restart_decision(
        output_root=tmp_path / "r2",
        portable_lineage_sidecar_path=sidecar,
        portable_project_root=tmp_path,
    )

    assert wf["portable_lineage_resolution"]["consumer"] == "r1_walk_forward"
    assert robustness["portable_lineage_resolution"]["consumer"] == "r1_robustness"
    assert decision["portable_lineage_resolution"]["consumer"] == "r2_decision"
    assert all(path.read_bytes() == value for path, value in immutable_before.items())


@pytest.mark.parametrize(
    ("consumer", "adapter"),
    [
        ("r0_preflight", research_restart._portable_path),
        ("r1_walk_forward", r1._portable_path),
        ("r2_decision", r2._portable_path),
    ],
)
def test_portable_adapters_return_verified_archive_path(
    tmp_path: Path, consumer: str, adapter: Any
) -> None:
    fixture = _archive_overlay_fixture(tmp_path)
    resolver = PortableLineageResolver(
        sidecar_path=fixture["sidecar"],
        subject_artifact_path=fixture["subject"],
        consumer=consumer,
        project_root=tmp_path,
        historical_source_archive_manifest_path=fixture["archive_manifest"],
    )

    resolved = adapter(
        Path("config/research/historical-source.yaml"),
        resolver,
        expected_sha256=fixture["source_sha256"],
        expected_size=len(fixture["historical_bytes"]),
    )

    assert resolved == fixture["archive_source"].resolve()
    assert resolved.read_bytes() == fixture["historical_bytes"]
    assert resolved != fixture["active_source"].resolve()


@pytest.mark.parametrize(
    "validator",
    [
        lambda manifest: research_restart.validate_research_restart_preflight(
            artifact_path=Path("missing-r0.json"),
            historical_source_archive_manifest_path=manifest,
        ),
        lambda manifest: r1.validate_r1_walk_forward_evidence(
            walk_forward_id="missing-wf",
            historical_source_archive_manifest_path=manifest,
        ),
        lambda manifest: r1.validate_r1_robustness_evidence(
            robustness_id="missing-robustness",
            historical_source_archive_manifest_path=manifest,
        ),
        lambda manifest: r2.validate_strategy_research_restart_decision(
            historical_source_archive_manifest_path=manifest,
        ),
    ],
)
def test_archive_opt_in_without_sidecar_fails_closed(validator: Any) -> None:
    with pytest.raises(ValueError, match="requires portable lineage sidecar"):
        validator(DEFAULT_MANIFEST_PATH)


def test_recovered_r0_r1_r2_bundle_fails_closed_after_reviewed_source_drift(
    tmp_path: Path,
) -> None:
    r0_path = (
        PROJECT_ROOT
        / "outputs"
        / "research_ops"
        / "strategy_restart"
        / "strategy_research_restart_preflight.json"
    )
    wf_id = "r1-wf_6447beb5464bad37"
    robustness_id = "r1-robustness_8c93b0e2615d0ace"
    wf_root = PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue" / "walk_forward_r1"
    robustness_root = (
        PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue" / "robustness_r1"
    )
    r2_root = r0_path.parent / "r2_decision"
    robustness_manifest_sha = "876b9d81ed98b8e0f38b696f51e253c5ad15e29e75e631ce725231b1c95e8692"
    r2_manifest_sha = "0fc4ded3cbde2aac73d6d6f7dee50a0946a7a9d5a7632a1b78dac3f677968743"
    artifacts = {
        r0_path: "b7272a44f8f9eba845e47a103895de7ce207da0913d5518d08fdd5be2ad4faf9",
        wf_root
        / wf_id
        / "r1_wf_manifest.json": "50a60f7aca02cd4e11b4a6fd982c7faf2cf46a9933b5eaa57f3dc1f4f89888bc",
        robustness_root / robustness_id / "r1_robustness_manifest.json": robustness_manifest_sha,
        r2_root / "strategy_research_restart_r2_manifest.json": r2_manifest_sha,
    }
    missing = [path for path in artifacts if not path.is_file()]
    if missing:
        pytest.skip("immutable TRADING-2449 bundle is not installed in this clone")
    before = {path: (path.stat().st_size, _sha256(path)) for path in artifacts}
    assert {path: digest for path, (_size, digest) in before.items()} == artifacts

    sidecar = tmp_path / "trading2450_portable_lineage.rebuilt.json"
    built = build_research_restart_portable_lineage_sidecar(
        output_path=sidecar,
        r0_preflight_path=r0_path,
        walk_forward_dir=wf_root / wf_id,
        robustness_dir=robustness_root / robustness_id,
        r2_output_root=r2_root,
        historical_project_root=Path(r"D:\Work\AITradingSystem-eb0-candidate"),
    )
    assert DEFAULT_TRADING2449_SIDECAR_PATH.is_file()
    frozen = _read_json(DEFAULT_TRADING2449_SIDECAR_PATH)
    assert frozen["sidecar_id"] == "portable-lineage_dfa5dfc7208e5913fc75"
    assert frozen != built
    kwargs = {
        "portable_lineage_sidecar_path": DEFAULT_TRADING2449_SIDECAR_PATH,
        "portable_project_root": PROJECT_ROOT,
    }
    r0_validation = research_restart.validate_research_restart_preflight(
        artifact_path=r0_path, **kwargs
    )
    wf_validation = r1.validate_r1_walk_forward_evidence(
        walk_forward_id=wf_id,
        output_dir=wf_root,
        **kwargs,
    )
    robustness_validation = r1.validate_r1_robustness_evidence(
        robustness_id=robustness_id,
        output_dir=robustness_root,
        **kwargs,
    )
    r2_validation = r2.validate_strategy_research_restart_decision(output_root=r2_root, **kwargs)

    assert built["sidecar_id"].startswith("portable-lineage_")
    assert len(built["legacy_artifacts"]) == 4
    assert len(built["sources"]) == 108
    for validation in (
        r0_validation,
        wf_validation,
        robustness_validation,
        r2_validation,
    ):
        assert validation["status"] == "FAIL"
        resolution = validation["portable_lineage_resolution"]
        assert resolution["reason_code"] == "HISTORICAL_PORTABLE_CONFLICT"
        assert resolution["production_effect"] == "none"
    assert {path: (path.stat().st_size, _sha256(path)) for path in artifacts} == before

    archive_kwargs = {
        **kwargs,
        "historical_source_archive_manifest_path": DEFAULT_MANIFEST_PATH,
    }
    archived_validations = (
        research_restart.validate_research_restart_preflight(
            artifact_path=r0_path, **archive_kwargs
        ),
        r1.validate_r1_walk_forward_evidence(
            walk_forward_id=wf_id,
            output_dir=wf_root,
            **archive_kwargs,
        ),
        r1.validate_r1_robustness_evidence(
            robustness_id=robustness_id,
            output_dir=robustness_root,
            **archive_kwargs,
        ),
        r2.validate_strategy_research_restart_decision(output_root=r2_root, **archive_kwargs),
    )
    expected_next_blockers = (
        "source_f3af401bd04447415bc1",
        "source_2359a08b2c37809e744c",
        "source_2359a08b2c37809e744c",
        "source_2359a08b2c37809e744c",
    )
    for validation, binding_id in zip(archived_validations, expected_next_blockers, strict=True):
        assert validation["status"] == "FAIL"
        resolution = validation["portable_lineage_resolution"]
        assert resolution["reason_code"] == "PORTABLE_SOURCE_TAMPERED"
        assert binding_id in resolution["detail"]
        assert resolution["production_effect"] == "none"
    assert {path: (path.stat().st_size, _sha256(path)) for path in artifacts} == before


def _minimal_sidecar(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    subject = _write(tmp_path / "portable" / "subject.json", b"subject")
    source = _write(tmp_path / "portable" / "source.bin", b"source")
    historical_source = tmp_path / "legacy" / "source.bin"
    sidecar = tmp_path / "portable-lineage.json"
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[
            _binding("artifact", tmp_path / "legacy" / "subject.json", subject, "r0_preflight")
        ],
        sources=[_binding("source", historical_source, source, "r0_preflight")],
        project_root=tmp_path,
    )
    return subject, source, sidecar, historical_source


def _archive_overlay_fixture(tmp_path: Path) -> dict[str, Any]:
    historical_bytes = b"reviewed-historical-source\n"
    active_source = _write(
        tmp_path / "config" / "research" / "historical-source.yaml",
        historical_bytes,
    )
    subject = _write(tmp_path / "portable" / "subject.json", b'{"immutable":true}\n')
    sidecar = tmp_path / "inputs" / "research" / "portable-lineage.json"
    consumers = ("r0_preflight", "r1_walk_forward", "r2_decision")
    build_portable_lineage_sidecar(
        output_path=sidecar,
        legacy_artifacts=[
            PortableLineageBinding(
                binding_id="artifact_all_consumers",
                legacy_path=str(tmp_path / "missing-legacy" / "subject.json"),
                portable_path=subject,
                consumers=consumers,
            )
        ],
        sources=[
            PortableLineageBinding(
                binding_id="source_historical_overlay",
                legacy_path="config/research/historical-source.yaml",
                portable_path=active_source,
                consumers=consumers,
            )
        ],
        project_root=tmp_path,
    )
    frozen = _read_json(sidecar)
    source = frozen["sources"][0]
    active_source.write_bytes(b"current-active-source\n")
    archive_source = (
        tmp_path
        / "inputs"
        / "research"
        / "legacy_lineage"
        / "source_archive"
        / frozen["sidecar_id"]
        / source["sha256"]
        / "historical-source.yaml"
    )
    _write(archive_source, historical_bytes)
    manifest = {
        "schema_version": "historical_portable_source_archive_manifest.v1",
        "policy_binding": {
            "policy_id": "historical_portable_source_archive_v1",
            "policy_version": 1,
            "policy_sha256": _sha256(DEFAULT_HISTORICAL_ARCHIVE_POLICY_PATH),
        },
        "sidecar_binding": {
            "sidecar_id": frozen["sidecar_id"],
            "path": sidecar.relative_to(tmp_path).as_posix(),
            "sha256": _sha256(sidecar),
        },
        "sources": [
            {
                "binding_id": source["binding_id"],
                "legacy_path": source["legacy_path"],
                "frozen_locator": source["locator"],
                "sha256": source["sha256"],
                "size": source["size"],
                "archive_locator": {
                    "kind": "project_relative_content_archive",
                    "path": archive_source.relative_to(tmp_path).as_posix(),
                },
                "legacy_locator_disposition": ("active_locator_superseded_by_window_migration"),
                "provenance": {
                    "source_commit": "0" * 40,
                    "source_git_blob": "1" * 40,
                    "sidecar_freeze_commit": "2" * 40,
                    "last_pre_migration_commit": "3" * 40,
                    "active_window_migration_commit": "4" * 40,
                    "recovery_source": "trusted_git_object_and_exact_historical_worktree",
                },
            }
        ],
        "safety": dict(HISTORICAL_ARCHIVE_SAFETY),
    }
    manifest["archive_id"] = _historical_archive_id(manifest)
    archive_manifest = _write(
        tmp_path / "inputs" / "research" / "historical-source-archive.json",
        (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(),
    )
    return {
        "active_source": active_source,
        "archive_manifest": archive_manifest,
        "archive_source": archive_source,
        "historical_bytes": historical_bytes,
        "sidecar": sidecar,
        "source_sha256": source["sha256"],
        "subject": subject,
    }


def _binding(
    binding_id: str,
    legacy_path: Path,
    portable_path: Path,
    consumer: str,
) -> PortableLineageBinding:
    return PortableLineageBinding(
        binding_id=binding_id,
        legacy_path=str(legacy_path),
        portable_path=portable_path,
        consumers=(consumer,),
    )


def _write(path: Path, value: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _sidecar_id(payload: dict[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("sidecar_id", None)
    encoded = json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "portable-lineage_" + sha256(encoded).hexdigest()[:20]


def _historical_archive_id(payload: dict[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("archive_id", None)
    encoded = json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "historical-source-archive_" + sha256(encoded).hexdigest()[:20]


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
