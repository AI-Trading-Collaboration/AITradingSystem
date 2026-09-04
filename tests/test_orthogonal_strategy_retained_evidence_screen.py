from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest

from ai_trading_system import orthogonal_strategy_retained_evidence_screen as screen

SOURCE_PATHS = (
    Path("config/research/simple_baseline_strategy_registry.yaml"),
    Path("config/research/layer2_strategy_component_pool_v1.yaml"),
    Path("config/research/qqq_plus_growth_candidate_registry.yaml"),
    Path("config/research/evidence_first_research_portfolio_v1.yaml"),
    Path(
        "config/research/first_layer_composer_v2_temporal_influence_"
        "failure_fix_result_admission_v1.yaml"
    ),
)


def _copy_policy_tree(tmp_path: Path) -> Path:
    for relative in SOURCE_PATHS:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(relative, target)
    policy_target = tmp_path / screen.DEFAULT_POLICY_PATH
    policy_target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(screen.DEFAULT_POLICY_PATH, policy_target)
    return policy_target


def test_policy_and_screen_are_static_only() -> None:
    policy = screen.load_screen_policy()
    result = screen.run_retained_evidence_screen(policy)

    assert policy.payload["run_envelope"] == screen.ZERO_ACTION_COUNTERS
    assert result["status"] == "CONTINUE_EXISTING_FORWARD_AGING_NO_NEW_EMPIRICAL_RUN"
    assert result["selected_continuation_candidate"] == "equal_risk_qqq_sgov"
    assert result["selected_new_experiment_candidate"] is None
    assert result["automatic_promotion_allowed"] is False
    assert result["production_effect"] == result["broker_action"] == "none"
    assert len(result["result_sha256"]) == 64


def test_structural_orthogonality_never_claims_empirical_independence() -> None:
    result = screen.run_retained_evidence_screen()
    by_id = {row["candidate_id"]: row for row in result["candidates"]}

    assert by_id["equal_risk_qqq_sgov"]["structural_orthogonality"] == "PARTIAL"
    assert by_id["equal_risk_qqq_sgov"]["route"] == ("CONTINUE_EXISTING_FORWARD_AGING")
    assert by_id["dyn_tqqq_capped_trend"]["structural_orthogonality"] == ("OVERLAPPING")
    assert by_id["LEAPS"]["route"] == "EXCLUDE"
    assert {row["empirical_independence_claim"] for row in result["candidates"]} == {
        "NOT_ESTABLISHED"
    }


def test_historical_portfolio_snapshot_is_not_allowed_to_override_terminal_result() -> None:
    result = screen.run_retained_evidence_screen()

    assert result["source_consistency"] == {
        "historical_portfolio_snapshot_verdict": "UNRESOLVED",
        "terminal_composer_verdict": "INSUFFICIENT_HOLD",
        "warning": "PORTFOLIO_SNAPSHOT_PRECEDES_TERMINAL_EVIDENCE",
        "terminal_evidence_precedence_applied": True,
    }


def test_candidate_order_does_not_change_result_rows(tmp_path: Path) -> None:
    policy_path = _copy_policy_tree(tmp_path)
    text = policy_path.read_text(encoding="utf-8")
    start = text.index("candidates:\n") + len("candidates:\n")
    end = text.index("result_contract:\n")
    blocks = text[start:end].rstrip().split("\n  - candidate_id: ")
    normalized = [blocks[0], *("  - candidate_id: " + item for item in blocks[1:])]
    reversed_candidates = "\n".join(reversed(normalized)) + "\n"
    policy_path.write_text(text[:start] + reversed_candidates + text[end:], encoding="utf-8")

    policy = screen.load_screen_policy(screen.DEFAULT_POLICY_PATH, project_root=tmp_path)
    result = screen.run_retained_evidence_screen(policy, project_root=tmp_path)

    assert [row["candidate_id"] for row in result["candidates"]] == sorted(
        row["candidate_id"] for row in result["candidates"]
    )


def test_unknown_source_candidate_fails_closed(tmp_path: Path) -> None:
    policy_path = _copy_policy_tree(tmp_path)
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8").replace(
            "lookup_value: equal_risk_qqq_sgov",
            "lookup_value: missing_candidate",
            1,
        ),
        encoding="utf-8",
    )
    policy = screen.load_screen_policy(screen.DEFAULT_POLICY_PATH, project_root=tmp_path)

    with pytest.raises(
        screen.OrthogonalStrategyScreenError,
        match="ORTHOGONAL_SCREEN_SOURCE_ENTRY_INVALID",
    ):
        screen.run_retained_evidence_screen(policy, project_root=tmp_path)


def test_unsafe_source_fails_even_when_binding_hash_is_updated(tmp_path: Path) -> None:
    policy_path = _copy_policy_tree(tmp_path)
    source_path = tmp_path / Path("config/research/layer2_strategy_component_pool_v1.yaml")
    source_path.write_text(
        source_path.read_text(encoding="utf-8").replace(
            "  production_allowed: false", "  production_allowed: true", 1
        ),
        encoding="utf-8",
    )
    updated_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8").replace(
            "c531dd99c7e345b0ab7d2571c6090b6ba149bd63c7367e099362c6a64451a773",
            updated_hash,
        ),
        encoding="utf-8",
    )
    policy = screen.load_screen_policy(screen.DEFAULT_POLICY_PATH, project_root=tmp_path)

    with pytest.raises(
        screen.OrthogonalStrategyScreenError, match="ORTHOGONAL_SCREEN_UNSAFE_SOURCE"
    ):
        screen.run_retained_evidence_screen(policy, project_root=tmp_path)


def test_each_source_entry_is_exact_bound() -> None:
    result = screen.run_retained_evidence_screen()

    assert len(result["candidates"]) == 5
    assert all(len(row["source_entry_sha256"]) == 64 for row in result["candidates"])
    assert result["run_counters"]["market_data_reads"] == 0
    assert result["run_counters"]["prospective_outcome_reads"] == 0
    assert result["run_counters"]["backtests"] == 0
