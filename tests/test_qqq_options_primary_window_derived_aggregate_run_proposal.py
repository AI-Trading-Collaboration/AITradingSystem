from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_run_proposal as proposal_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAMES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)


def _raw_payloads(
    built: proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
) -> dict[str, bytes]:
    return {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
        "proposal.json": built.proposal.canonical_bytes,
        "run_scope.json": built.run_scope.canonical_bytes,
    }


@pytest.fixture
def sandbox_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[
    tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ]
]:
    built = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    root = tmp_path / "repository"
    package_root = root / (
        proposal_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT
    )
    package_root.mkdir(parents=True)
    for name, raw in _raw_payloads(built).items():
        (package_root / name).write_bytes(raw)

    def expected_builder(
        *, project_root: Path = PROJECT_ROOT
    ) -> proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage:
        del project_root
        return built

    monkeypatch.setattr(
        proposal_v1,
        "build_qc_qqq_options_primary_window_run_proposal_package",
        expected_builder,
    )
    yield root, package_root, built


def test_policy_loads_and_binds_exact_2512_authority() -> None:
    loaded = proposal_v1.load_qc_qqq_options_primary_window_run_proposal_policy(
        project_root=PROJECT_ROOT
    )
    policy = loaded.policy

    assert loaded.policy_file_sha256 == (
        "dc64eae45a3581089af1223c8bc6da005c0962d17906ad447cf72f8a9a5fbbaf"
    )
    assert loaded.policy_canonical_sha256 == (
        "4c80425fae656c573ca74d44e5d738bc78307619c0471f2c852446430fefdbc6"
    )
    assert policy.registration_base_repository_code_sha == (
        "83d4f9680c4f78c7c1414659d51738ba7f615a7a"
    )
    assert policy.target_project_id == 34808569
    assert policy.expected_session_count == 1202
    assert policy.upstream_collector.policy_file_sha256 == (
        "48511cc64cab07b091787e2b0cb23354424248da66e7dba8866cd9ce9a766a8f"
    )
    assert policy.upstream_collector.transport_map_sha256 == (
        "60c970b71d3c47337fb76452d1384f2463079ef5026239e875e78b8c37d3eab5"
    )


def test_package_build_is_deterministic_and_golden() -> None:
    first = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    second = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )

    assert first == second
    assert first.manifest.canonical_sha256 == (
        "a100984326f8015ebe55459e3e87d3a20902bb6693a793f0b91ea7cf1ad5d85d"
    )
    assert first.manifest.content_sha256 == (
        "b44de8a0854cde6004f71ac2ed86cc619ab6c12c81b07f5efe790dad74219d58"
    )
    assert first.run_scope.content_sha256 == (
        "80c11d7073dcc86f1297a34b3497fe705069619d6f1f51927ab9b673172db15e"
    )
    assert first.proposal.content_sha256 == (
        "f48732afc0d69656fbe5c62b1965296feccda30caa3279c80b9d1c20ce272240"
    )
    assert first.manifest.project_code_lf_sha256 == (
        "d7f96fbb14e03a1f248b0a14b3ebdaa1bbeeada2d15f87fb3277b98b9c6641a6"
    )
    assert first.manifest.project_code_lf_byte_count == 26074


def test_package_keeps_primary_window_and_cash_preservation() -> None:
    built = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    scope = built.run_scope
    manifest = built.manifest

    assert scope.requested_start.isoformat() == "2021-02-22"
    assert scope.requested_end.isoformat() == "2025-12-02"
    assert scope.evaluated_start == scope.requested_start
    assert scope.evaluated_end == scope.requested_end
    assert len(scope.session_ids) == 1202
    assert scope.session_ids[0].isoformat() == "2021-02-22"
    assert scope.session_ids[-1].isoformat() == "2025-12-02"
    assert manifest.authorization_status == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert manifest.decision == "OWNER_AUTHORIZATION_REQUIRED"
    assert manifest.owner_policy_value_count == 0
    assert manifest.executable_policy_authorized is False
    assert manifest.selection_authorized is False
    assert manifest.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert manifest.maximum_orders == manifest.maximum_fills == 0
    assert manifest.production_effect == manifest.broker_action == "none"


def test_project_code_is_exact_2512_renderer_and_has_no_order_or_export_path() -> None:
    built = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    text = built.project_code_bytes.decode("utf-8")

    assert text.startswith("from AlgorithmImports import *\n")
    assert "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1" in text
    assert "self.set_start_date(2021, 2, 22)" in text
    assert "self.set_end_date(2025, 12, 2)" in text
    for prohibited in (
        "self.market_order(",
        "self.limit_order(",
        "self.object_store",
        "self.debug(",
        "self.log(",
        "requests.",
        "urllib",
    ):
        assert prohibited not in text.lower()
    assert hashlib.sha256(built.project_code_bytes).hexdigest() == (
        built.manifest.project_code_lf_sha256
    )


def test_owner_request_is_complete_but_unsigned() -> None:
    built = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    text = built.owner_decision_request_bytes.decode("utf-8")

    assert "owner_decision:TRADING-2513:<YYYY-MM-DD>" in text
    assert "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>" in text
    assert "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>" in text
    assert "maximum_project_mutations:1" in text
    assert "maximum_cloud_backtests:1" in text
    assert "maximum_orders:0" in text
    assert "maximum_fills:0" in text
    assert "independent_reviewer:project_owner" in text
    assert "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST" in text
    assert "RAW_OPTIONS_DATA_DOWNLOAD" in text
    assert "owner_decision:TRADING-2513:2026-" not in text
    assert "DQ PASS" in text
    assert "不等于" in text


def test_repository_package_inventory_and_loader_are_exact() -> None:
    package_root = PROJECT_ROOT / (
        proposal_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT
    )
    assert tuple(sorted(path.name for path in package_root.iterdir())) == PACKAGE_NAMES

    loaded = proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    assert loaded.manifest.canonical_sha256 == (
        "a100984326f8015ebe55459e3e87d3a20902bb6693a793f0b91ea7cf1ad5d85d"
    )
    for artifact in loaded.manifest.artifacts:
        path = package_root / artifact.relative_path
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == artifact.sha256
        assert len(raw) == artifact.byte_count


@pytest.mark.parametrize(
    "name",
    ("main.py", "owner_decision_request.md", "proposal.json", "run_scope.json"),
)
def test_loader_rejects_each_tampered_artifact(
    sandbox_package: tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ],
    name: str,
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / name
    target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(
        proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError,
        match="PROPOSAL_PACKAGE_ADMISSION_FAILED",
    ):
        proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )


def test_loader_rejects_manifest_tamper(
    sandbox_package: tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ],
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / "package_manifest.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["session_count"] = 1201
    target.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError):
        proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )


@pytest.mark.parametrize("mutation", ("extra", "missing"))
def test_loader_rejects_nonexact_inventory(
    sandbox_package: tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ],
    mutation: str,
) -> None:
    root, package_root, _ = sandbox_package
    if mutation == "extra":
        (package_root / "extra.txt").write_text("unexpected\n", encoding="utf-8")
    else:
        (package_root / "main.py").unlink()

    with pytest.raises(
        proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError,
        match="package file inventory is not exact",
    ):
        proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )


def test_loader_rejects_noncanonical_and_duplicate_json(
    sandbox_package: tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ],
) -> None:
    root, package_root, built = sandbox_package
    target = package_root / "proposal.json"
    payload = json.loads(built.proposal.canonical_bytes)
    target.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    with pytest.raises(proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError):
        proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )

    target.write_bytes(built.proposal.canonical_bytes)
    manifest = package_root / "package_manifest.json"
    raw = manifest.read_bytes().replace(
        b'{\n  "artifacts":', b'{\n  "artifacts": [],\n  "artifacts":', 1
    )
    manifest.write_bytes(raw)
    with pytest.raises(
        proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError,
        match="duplicate JSON key",
    ):
        proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )


def test_loader_rejects_symlink_entry_when_supported(
    sandbox_package: tuple[
        Path,
        Path,
        proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage,
    ],
) -> None:
    root, package_root, _ = sandbox_package
    target = package_root / "main.py"
    original = root / "main.original.py"
    target.rename(original)
    try:
        os.symlink(original, target)
    except OSError:
        pytest.skip("symlink creation is unavailable in this Windows environment")
    try:
        with pytest.raises(proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError):
            proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
                project_root=root
            )
    finally:
        if target.is_symlink():
            target.unlink()
        if original.exists():
            original.rename(target)


def test_manifest_seal_and_from_json_fail_closed() -> None:
    built = proposal_v1.build_qc_qqq_options_primary_window_run_proposal_package(
        project_root=PROJECT_ROOT
    )
    restored = proposal_v1.QCQQQOptionsPrimaryWindowRunProposalPackageManifest.from_json_bytes(
        built.manifest.canonical_bytes
    )
    assert restored == built.manifest
    assert restored.canonical_sha256 == built.manifest.canonical_sha256

    payload = json.loads(restored.canonical_bytes)
    payload["content_sha256"] = "0" * 64
    tampered = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    with pytest.raises(
        proposal_v1.QCQQQOptionsPrimaryWindowRunProposalError,
        match="semantic content SHA-256 mismatch",
    ):
        proposal_v1.QCQQQOptionsPrimaryWindowRunProposalPackageManifest.from_json_bytes(
            tampered
        )
