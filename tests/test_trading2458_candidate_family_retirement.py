from __future__ import annotations

import json
import shutil
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system import dynamic_v3_trading2452_historical_evaluator as evaluator
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_v3_clean_selection_trading2452 import (
    DEFAULT_PACKAGE_ROOT,
    write_trading2452_package,
)
from ai_trading_system.trading2458_candidate_family_retirement import (
    ALLOWED_ACTIONS,
    DEFAULT_POLICY_PATH,
    EXPECTED_PACKAGE_ID,
    PROHIBITED_ACTIONS,
    Trading2458CandidateFamilyRetirementError,
    build_candidate_family_retirement_record,
    candidate_family_action_decision,
    render_candidate_family_retirement_markdown,
    require_candidate_family_action_allowed,
    validate_candidate_family_retirement_record,
)


def test_retirement_record_binds_exact_family_and_immutable_artifacts() -> None:
    before = _package_hashes(DEFAULT_PACKAGE_ROOT)
    first = build_candidate_family_retirement_record()
    second = build_candidate_family_retirement_record()

    assert first == second
    assert first["status"] == "PASS"
    assert first["lifecycle_state"] == "RETIRED"
    assert first["active_consumption_status"] == "BLOCKED_RETIRED_CANDIDATE_FAMILY"
    assert first["retired_scope"]["package_id"] == EXPECTED_PACKAGE_ID
    assert first["retired_scope"]["candidate_count"] == 300
    assert len(first["retired_scope"]["template_ids"]) == 4
    assert len(first["retired_scope"]["candidate_axes"]) == 7
    assert first["retired_scope"]["research_window"] == {
        "requested_start": "2021-02-22",
        "evaluated_start": "2021-02-22",
        "evaluated_end": "2025-12-31",
        "prospective_untouched_start": "2026-07-22",
    }
    assert first["immutable_package"]["historical_manifest_eligibility_superseded"] is True
    assert first["immutable_package"]["historical_bytes_modified"] is False
    assert _package_hashes(DEFAULT_PACKAGE_ROOT) == before


@pytest.mark.parametrize("action", ALLOWED_ACTIONS)
def test_historical_evidence_only_actions_remain_allowed(action: str) -> None:
    decision = require_candidate_family_action_allowed(action)

    assert decision["allowed"] is True
    assert decision["status"] == "ALLOWED_HISTORICAL_EVIDENCE_ONLY"
    assert decision["production_effect"] == "none"
    assert decision["broker_action"] == "none"


@pytest.mark.parametrize("action", PROHIBITED_ACTIONS)
def test_active_candidate_family_actions_fail_closed(action: str) -> None:
    decision = candidate_family_action_decision(action)

    assert decision["allowed"] is False
    assert decision["status"] == "BLOCKED_RETIRED_CANDIDATE_FAMILY"
    with pytest.raises(
        Trading2458CandidateFamilyRetirementError,
        match="BLOCKED_RETIRED_CANDIDATE_FAMILY",
    ):
        require_candidate_family_action_allowed(action)


def test_unknown_action_fails_closed() -> None:
    decision = candidate_family_action_decision("future_unreviewed_action")

    assert decision["allowed"] is False
    assert decision["status"] == "BLOCKED_UNKNOWN_CANDIDATE_FAMILY_ACTION"


def test_historical_evaluator_blocks_before_dq_workers_or_artifact_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        evaluator,
        "validate_trading2452_package",
        lambda **_kwargs: pytest.fail("package evaluator path must not run"),
    )
    monkeypatch.setattr(
        evaluator,
        "_run_data_quality_gate",
        lambda **_kwargs: pytest.fail("DQ path must not run for a retired family"),
    )
    monkeypatch.setattr(
        evaluator,
        "ProcessPoolExecutor",
        lambda **_kwargs: pytest.fail("workers must not start for a retired family"),
    )
    output_root = tmp_path / "outputs"

    result = evaluator.run_trading2452_historical_seen_evaluator(output_root=output_root)

    assert result["status"] == "BLOCKED_RETIRED_CANDIDATE_FAMILY"
    assert result["run_id"] is None
    assert result["run_dir"] is None
    assert result["data_quality_gate_executed"] is False
    assert result["workers_started"] is False
    assert result["artifacts_written"] is False
    assert not output_root.exists()


def test_package_writer_blocks_before_target_mutation(tmp_path: Path) -> None:
    output_root = tmp_path / "retired_package"

    with pytest.raises(
        Trading2458CandidateFamilyRetirementError,
        match="BLOCKED_RETIRED_CANDIDATE_FAMILY",
    ):
        write_trading2452_package(
            package_root=output_root,
            project_root=PROJECT_ROOT,
        )

    assert not output_root.exists()


def test_policy_tamper_fails_closed(tmp_path: Path) -> None:
    policy_path = tmp_path / "retirement.yaml"
    shutil.copy2(DEFAULT_POLICY_PATH, policy_path)
    original = policy_path.read_text(encoding="utf-8")
    policy_path.write_text(
        original.replace("OWNER_APPROVED_RETIRED", "ACTIVE"),
        encoding="utf-8",
    )

    with pytest.raises(
        Trading2458CandidateFamilyRetirementError,
        match="policy fingerprint mismatch",
    ):
        build_candidate_family_retirement_record(policy_path=policy_path)


def test_immutable_package_tamper_fails_closed(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    package_root = project_root / "inputs" / "research" / "trading2452_dynamic_v3_clean_selection"
    shutil.copytree(DEFAULT_PACKAGE_ROOT, package_root)
    universe_path = package_root / "candidate_universe.json"
    universe = json.loads(universe_path.read_text(encoding="utf-8"))
    universe["candidate_count"] = 299
    universe_path.write_text(
        json.dumps(universe, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        Trading2458CandidateFamilyRetirementError,
        match="artifact fingerprint mismatch",
    ):
        build_candidate_family_retirement_record(project_root=project_root)


def test_record_tamper_is_rejected_and_markdown_is_content_derived() -> None:
    record = build_candidate_family_retirement_record()
    markdown = render_candidate_family_retirement_markdown(record)
    tracked_report = (
        PROJECT_ROOT / "docs" / "research" / "trading2458_candidate_family_retirement.md"
    ).read_text(encoding="utf-8")
    tampered: dict[str, Any] = json.loads(json.dumps(record))
    tampered["lifecycle_state"] = "ACTIVE"

    assert tracked_report == markdown
    assert "# TRADING-2458 Candidate Family 正式退役记录" in markdown
    assert "`RETIRED`" in markdown
    assert "`2021-02-22` / `2021-02-22`" in markdown
    assert validate_candidate_family_retirement_record(record)["status"] == "PASS"
    validation = validate_candidate_family_retirement_record(tampered)
    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] == 2


def _package_hashes(package_root: Path) -> dict[str, str]:
    return {
        path.name: sha256(path.read_bytes()).hexdigest()
        for path in sorted(package_root.iterdir())
        if path.is_file()
    }
