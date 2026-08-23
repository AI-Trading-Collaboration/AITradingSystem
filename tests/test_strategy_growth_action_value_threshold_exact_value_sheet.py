from __future__ import annotations

import hashlib
import json
import shutil
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_threshold_exact_value_sheet import (
    StrategyGrowthActionValueThresholdExactValueSheet,
    StrategyGrowthActionValueThresholdExactValueSheetError,
    load_strategy_growth_action_value_threshold_exact_value_sheet,
)

ROOT = Path(__file__).resolve().parents[1]
SHEET_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v1.yaml"
)
SHEET_FILE_SHA256 = "82f75b55bb4a9576775d4e60a9a31bc01b24d3b5b8cf270c6aabbed9e9d17e7f"
SHEET_CANONICAL_SHA256 = "14286008f464230921400c1def4173f34a6e9231e77c434504a5abab78451dfb"
AXIS_ORDER = (
    "NON_BETA_ACTION_VALUE",
    "NET_OF_COST_RETURN",
    "ACTUAL_PATH_DRAWDOWN_REGRESSION",
    "FALSE_RISK_OFF_COST",
    "CANONICAL_DQ_PIT",
    "SAMPLE_AND_WINDOW_DEPENDENCE",
    "ACTUAL_PATH_TURNOVER",
    "LEVERAGE_BETA_ATTRIBUTION",
)
PERIOD_SLICES = (
    "PRIMARY_2021_PARTIAL",
    "RATE_HIKE_BEAR_2022",
    "RECOVERY_2023",
    "AI_RALLY_2024",
    "PRIMARY_2025_TO_END",
)


def _load():
    return load_strategy_growth_action_value_threshold_exact_value_sheet(
        sheet_path=SHEET_PATH,
        project_root=ROOT,
    )


def _fixture_root(tmp_path: Path) -> Path:
    loaded = _load()
    fixture = tmp_path / "project"
    pack = loaded.decision_pack.pack
    paths = {
        SHEET_PATH.as_posix(),
        loaded.sheet.decision_pack_binding.path,
        *(item.path for item in pack.binding_authorities),
        *(item.path for item in pack.threshold_source_inventory),
    }
    for relative in sorted(paths):
        source = ROOT / relative
        target = fixture / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return fixture


def _read_sheet(root: Path) -> dict[str, object]:
    payload = yaml.safe_load((root / SHEET_PATH).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_sheet(root: Path, payload: dict[str, object]) -> None:
    (root / SHEET_PATH).write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def test_default_sheet_freezes_exact_draft_identity_and_decision_pack_binding() -> None:
    loaded = _load()

    assert loaded.sheet_file_sha256 == SHEET_FILE_SHA256
    assert loaded.sheet_canonical_sha256 == SHEET_CANONICAL_SHA256
    assert loaded.decision_pack.pack_file_sha256 == (
        loaded.sheet.decision_pack_binding.file_sha256
    )
    assert loaded.decision_pack.pack_canonical_sha256 == (
        loaded.sheet.decision_pack_binding.canonical_sha256
    )
    assert hashlib.sha256((ROOT / SHEET_PATH).read_bytes()).hexdigest() == SHEET_FILE_SHA256


def test_owner_instruction_approves_sources_and_review_rule_but_not_values() -> None:
    sheet = _load().sheet

    assert sheet.sheet_status == "DRAFT_FOR_OWNER_REVIEW"
    assert sheet.owner_instruction.source_assignment_choice == (
        "APPROVE_RECOMMENDED_PER_AXIS"
    )
    assert sheet.owner_instruction.review_condition_choice == (
        "LOCK_V1_FOR_ONE_PRIMARY_WINDOW_EVALUATION_NEW_VERSION_FOR_CHANGE"
    )
    assert sheet.owner_instruction.exact_value_approval_state == "NOT_PROVIDED"
    assert sheet.owner_instruction.partial_approval_can_freeze is False
    assert sheet.decision_timing.new_dq_result_visible is False
    assert sheet.decision_timing.new_strategy_result_visible is False
    assert sheet.decision_timing.holdout_result_visible is False
    assert sheet.decision_timing.draft_values_visible is True
    assert sheet.decision_timing.exact_owner_approval_visible is False
    assert sheet.decision_timing.threshold_bundle_frozen is False


def test_sheet_covers_all_eight_axes_with_recommended_sources_and_pending_review() -> None:
    loaded = _load()
    sheet = loaded.sheet
    pack_axes = {
        item.axis_id.value: item for item in loaded.decision_pack.pack.axis_gap_matrix
    }

    assert tuple(item.axis_id for item in sheet.axis_values) == AXIS_ORDER
    assert sheet.owner_review_contract.required_review_order == AXIS_ORDER
    for item in sheet.axis_values:
        source = pack_axes[item.axis_id]
        assert item.threshold_id == source.threshold_id
        assert item.recommended_option_id == source.recommended_option_id
        assert item.direction == source.proposed_direction
        assert item.source_selection_state == "OWNER_APPROVED_RECOMMENDED_SOURCE"
        assert item.owner_review_state == "PENDING_OWNER_APPROVAL"
        assert all(field in item.model_fields_set for field in source.owner_value_fields)


def test_exact_numeric_and_composite_draft_values_are_complete() -> None:
    sheet = _load().sheet
    axes = {item.axis_id: item for item in sheet.axis_values}

    non_beta = axes["NON_BETA_ACTION_VALUE"]
    assert non_beta.minimum_non_beta_return_delta == Decimal("0.0100")
    assert non_beta.attribution_confidence_rule.block_length_sessions == 20
    assert non_beta.attribution_confidence_rule.resamples == 10000
    assert non_beta.attribution_confidence_rule.one_sided_confidence_level == Decimal(
        "0.90"
    )
    assert non_beta.attribution_confidence_rule.random_seed == 2542

    net = axes["NET_OF_COST_RETURN"]
    assert net.minimum_net_of_cost_return_delta == Decimal("0.0075")
    assert net.cost_reconciliation_tolerance == Decimal("0.0001")

    drawdown = axes["ACTUAL_PATH_DRAWDOWN_REGRESSION"]
    assert drawdown.maximum_actual_path_drawdown_regression == Decimal("0.0200")
    assert drawdown.mandatory_stress_slice_set == (
        "PRIMARY_WINDOW_FULL",
        *PERIOD_SLICES,
    )

    false_risk_off = axes["FALSE_RISK_OFF_COST"]
    assert false_risk_off.maximum_false_risk_off_cost_regression == Decimal("0.0025")
    event = false_risk_off.false_risk_off_event_definition
    assert event.horizon_exchange_sessions == 20
    assert event.qqq_minus_sgov_forward_return_min == Decimal("0.0300")
    assert event.qqq_forward_max_drawdown_floor == Decimal("-0.0500")
    assert event.adjacent_signal_cooldown_sessions == 20

    sample = axes["SAMPLE_AND_WINDOW_DEPENDENCE"]
    assert sample.minimum_independent_action_count == 30
    assert sample.minimum_independent_action_count_per_slice == 3
    assert sample.independence_gap_exchange_sessions == 20
    assert sample.maximum_single_regime_contribution_share == Decimal("0.50")
    assert sample.mandatory_window_slices == PERIOD_SLICES

    turnover = axes["ACTUAL_PATH_TURNOVER"]
    assert turnover.maximum_annualized_actual_path_turnover == Decimal("1.00")
    assert turnover.maximum_cost_drag_share == Decimal("0.25")

    leverage = axes["LEVERAGE_BETA_ATTRIBUTION"]
    assert leverage.maximum_realized_beta_increment == Decimal("0.0200")
    assert leverage.exposure_match_tolerance == Decimal("0.0100")
    assert leverage.leverage_etf_allowed is False
    assert leverage.options_position_allowed is False
    assert leverage.borrowed_leverage_allowed is False


def test_dq_draft_is_complete_but_cannot_replace_canonical_unknown_policy() -> None:
    sheet = _load().sheet
    axes = {item.axis_id: item for item in sheet.axis_values}
    dq = sheet.numeric_dq_policy_draft

    assert dq.status == "DRAFT_FOR_OWNER_REVIEW"
    assert dq.applicable_stage == "DATA_RESEARCH"
    assert dq.max_quote_age_seconds == 120
    assert dq.max_relative_spread == Decimal("0.20")
    assert dq.min_open_interest == 10
    assert dq.min_volume == 1
    assert dq.exact_source_date_required is True
    assert dq.unknown_can_pass is False
    dq_axis = axes["CANONICAL_DQ_PIT"]
    assert dq_axis.required_data_research_gate_status == "PASS"
    assert dq_axis.numeric_dq_threshold_policy_ref == dq.policy_ref
    assert sheet.terminal.dq_successor_authorized is False


def test_window_catalog_is_exact_primary_window_and_predeclared_slices() -> None:
    sheet = _load().sheet
    actual = tuple(
        (item.slice_id, item.start.isoformat(), item.end.isoformat(), item.role)
        for item in sheet.window_slice_catalog
    )

    assert actual == (
        ("PRIMARY_WINDOW_FULL", "2021-02-22", "2025-12-02", "PRIMARY_AGGREGATE"),
        ("PRIMARY_2021_PARTIAL", "2021-02-22", "2021-12-31", "CALENDAR_STABILITY"),
        ("RATE_HIKE_BEAR_2022", "2022-01-03", "2022-12-30", "MANDATORY_STRESS"),
        ("RECOVERY_2023", "2023-01-03", "2023-12-29", "CALENDAR_STABILITY"),
        ("AI_RALLY_2024", "2024-01-02", "2024-12-31", "CALENDAR_STABILITY"),
        ("PRIMARY_2025_TO_END", "2025-01-02", "2025-12-02", "CALENDAR_STABILITY"),
    )
    assert sheet.scope_binding.primary_window_start.isoformat() == "2021-02-22"
    assert sheet.scope_binding.primary_window_end.isoformat() == "2025-12-02"


def test_sheet_is_canonical_sealed_and_replayable() -> None:
    sheet = _load().sheet

    replay = StrategyGrowthActionValueThresholdExactValueSheet.from_json_bytes(
        sheet.canonical_bytes
    )
    assert replay == sheet
    assert replay.canonical_bytes == sheet.canonical_bytes
    assert replay.canonical_sha256 == SHEET_CANONICAL_SHA256


def test_every_empirical_external_and_trading_action_remains_closed() -> None:
    sheet = _load().sheet

    assert sheet.terminal.status == "BLOCKED_OWNER_REVIEW"
    assert sheet.terminal.threshold_bundle_frozen is False
    assert sheet.terminal.dq_successor_authorized is False
    assert sheet.terminal.empirical_successor_authorized is False
    assert sheet.owner_review_contract.partial_review_can_freeze is False
    assert sheet.owner_review_contract.all_axes_approval_required_for_freeze is True
    assert sheet.owner_review_contract.approval_must_precede_any_new_result is True
    assert sheet.safety.empirical_research_authorized is False
    assert sheet.safety.dq_run_authorized is False
    assert sheet.safety.cache_mutation_authorized is False
    assert sheet.safety.backtest_authorized is False
    assert sheet.safety.holdout_access_authorized is False
    assert sheet.safety.external_action_authorized is False
    assert sheet.safety.investment_conclusion_authorized is False
    assert sheet.safety.paper_allowed is False
    assert sheet.safety.live_allowed is False
    assert sheet.safety.broker_allowed is False
    assert sheet.safety.production_effect == "none"
    assert sheet.safety.broker_action == "none"


def test_decision_pack_file_drift_is_rejected(tmp_path: Path) -> None:
    root = _fixture_root(tmp_path)
    pack_path = (
        root
        / "config/research/strategy_growth_action_value_threshold_decision_pack_v1.yaml"
    )
    pack_path.write_text(pack_path.read_text(encoding="utf-8") + "\n# drift\n", encoding="utf-8")

    with pytest.raises(
        StrategyGrowthActionValueThresholdExactValueSheetError,
        match="decision-pack file SHA-256 mismatch",
    ):
        load_strategy_growth_action_value_threshold_exact_value_sheet(
            sheet_path=SHEET_PATH,
            project_root=root,
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["axis_values"].pop(),
            "exact value sheet must contain all eight axes",
        ),
        (
            lambda payload: payload["axis_values"][0].__setitem__(
                "recommended_option_id", "OWNER_ECONOMIC_MATERIALITY"
            ),
            "recommended_option_id",
        ),
        (
            lambda payload: payload["owner_instruction"].__setitem__(
                "exact_value_approval_state", "APPROVED"
            ),
            "exact_value_approval_state",
        ),
        (
            lambda payload: payload["terminal"].__setitem__(
                "threshold_bundle_frozen", True
            ),
            "threshold_bundle_frozen",
        ),
        (
            lambda payload: payload["scope_binding"].__setitem__(
                "primary_window_start", "2022-12-01"
            ),
            "primary window start drifted",
        ),
        (
            lambda payload: payload["numeric_dq_policy_draft"].__setitem__(
                "unknown_can_pass", True
            ),
            "unknown_can_pass",
        ),
    ],
)
def test_semantic_tamper_fails_closed(tmp_path: Path, mutate, match: str) -> None:
    root = _fixture_root(tmp_path)
    payload = _read_sheet(root)
    mutate(payload)
    _write_sheet(root, payload)

    with pytest.raises(StrategyGrowthActionValueThresholdExactValueSheetError, match=match):
        load_strategy_growth_action_value_threshold_exact_value_sheet(
            sheet_path=SHEET_PATH,
            project_root=root,
        )


def test_noncanonical_and_duplicate_json_are_rejected() -> None:
    sheet = _load().sheet
    noncanonical = json.dumps(json.loads(sheet.canonical_bytes)).encode("utf-8")
    duplicate = sheet.canonical_bytes.replace(
        b'{\n  "axis_values"',
        b'{\n  "sheet_status": "DRAFT_FOR_OWNER_REVIEW",\n  "axis_values"',
        1,
    )

    with pytest.raises(
        StrategyGrowthActionValueThresholdExactValueSheetError,
        match="not canonical JSON bytes",
    ):
        StrategyGrowthActionValueThresholdExactValueSheet.from_json_bytes(noncanonical)
    with pytest.raises(
        StrategyGrowthActionValueThresholdExactValueSheetError,
        match="duplicate JSON key: sheet_status",
    ):
        StrategyGrowthActionValueThresholdExactValueSheet.from_json_bytes(duplicate)
