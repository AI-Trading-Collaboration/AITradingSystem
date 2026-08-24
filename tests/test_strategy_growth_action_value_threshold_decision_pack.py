from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_preregistration import MandatoryAxis
from ai_trading_system.strategy_growth_action_value_threshold_decision_pack import (
    AuthorityDisposition,
    StrategyGrowthActionValueThresholdDecisionPack,
    StrategyGrowthActionValueThresholdDecisionPackError,
    ThresholdDirection,
    load_strategy_growth_action_value_threshold_decision_pack,
)

ROOT = Path(__file__).resolve().parents[1]
PACK_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_decision_pack_v1.yaml"
)
PACK_FILE_SHA256 = "67965b51b7d73f38ef0f71a2b78c85f5d3f1301283f184fcb4a23f90a13e2ca7"
PACK_CANONICAL_SHA256 = "e4a83e702470ce5a4f79deb6e8ebe9367916f2d86c7c6c8df912a9f40fb9e13a"
AUTHORITY_SET_SHA256 = "78e29b6b54916b110d23ab194202c8176ebadb9851b5a0b2f64e0017bec9d9bf"

AXIS_ORDER = tuple(MandatoryAxis)
SOURCE_DISPOSITIONS = {
    "ACTION_VALUE_SCORE_POLICY_V2": AuthorityDisposition.WRONG_SCOPE,
    "DEFENSIVE_LANE_ACTION_VALUE_POLICY": AuthorityDisposition.RETIRED_FAMILY,
    "FIRST_LAYER_THRESHOLD_POLICY_V2": AuthorityDisposition.RETIRED_FAMILY,
    "PROMOTION_GATE_THRESHOLDS": AuthorityDisposition.WRONG_SCOPE,
    "THRESHOLD_REGISTRY_V1": AuthorityDisposition.UNCALIBRATED_INVENTORY,
    "TRANSACTION_COST_MODEL_V1": AuthorityDisposition.PARTIAL_INPUT_ONLY,
    "QQQ_OPTIONS_DQ_PIT_IDENTITY_V1": AuthorityDisposition.PARTIAL_INPUT_ONLY,
    "QQQ_OPTIONS_STAGED_DQ_PIT_READINESS_V1": AuthorityDisposition.PARTIAL_INPUT_ONLY,
}


def _load():
    return load_strategy_growth_action_value_threshold_decision_pack(
        pack_path=PACK_PATH,
        project_root=ROOT,
    )


def _fixture_root(tmp_path: Path) -> Path:
    loaded = _load()
    fixture = tmp_path / "project"
    paths = {
        PACK_PATH.as_posix(),
        *(item.path for item in loaded.pack.binding_authorities),
        *(item.path for item in loaded.pack.threshold_source_inventory),
    }
    for relative in sorted(paths):
        source = ROOT / relative
        target = fixture / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    return fixture


def _read_pack(root: Path) -> dict[str, object]:
    payload = yaml.safe_load((root / PACK_PATH).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_pack(root: Path, payload: dict[str, object]) -> None:
    (root / PACK_PATH).write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def test_default_pack_freezes_exact_identity_and_authority_set() -> None:
    loaded = _load()

    assert loaded.pack_file_sha256 == PACK_FILE_SHA256
    assert loaded.pack_canonical_sha256 == PACK_CANONICAL_SHA256
    assert loaded.authority_set_sha256 == AUTHORITY_SET_SHA256
    assert len(loaded.authority_observations) == 12
    assert all(item.identity_verified for item in loaded.authority_observations)


def test_pack_binds_predecessor_scope_and_keeps_every_action_closed() -> None:
    pack = _load().pack

    assert pack.pack_status == "OWNER_DECISION_REQUIRED"
    assert pack.scope_binding.primary_window_start.isoformat() == "2021-02-22"
    assert pack.scope_binding.primary_window_end.isoformat() == "2025-12-02"
    assert pack.scope_binding.comparator_ids == (
        "equal_risk_qqq_sgov",
        "exposure_matched_no_signal",
    )
    assert pack.scope_binding.action_universe == ("QQQ", "SGOV")
    assert pack.scope_binding.uses_leverage_etf is False
    assert pack.scope_binding.uses_options is False
    assert pack.scope_binding.threshold_after_result_allowed is False
    assert pack.decision_timing.new_dq_result_visible is False
    assert pack.decision_timing.new_strategy_result_visible is False
    assert pack.decision_timing.threshold_value_selected is False
    assert pack.terminal.status == "BLOCKED_OWNER_INPUT"
    assert pack.terminal.threshold_bundle_frozen is False
    assert pack.terminal.dq_successor_authorized is False
    assert pack.terminal.empirical_successor_authorized is False
    assert pack.safety.empirical_research_authorized is False
    assert pack.safety.dq_run_authorized is False
    assert pack.safety.cache_mutation_authorized is False
    assert pack.safety.backtest_authorized is False
    assert pack.safety.holdout_access_authorized is False
    assert pack.safety.external_action_authorized is False
    assert pack.safety.paper_allowed is False
    assert pack.safety.live_allowed is False
    assert pack.safety.broker_allowed is False
    assert pack.safety.production_effect == "none"
    assert pack.safety.broker_action == "none"


def test_authority_inventory_has_exact_scope_dispositions_and_no_admissible_bundle() -> None:
    pack = _load().pack
    actual = {item.authority_id: item.disposition for item in pack.threshold_source_inventory}

    assert actual == SOURCE_DISPOSITIONS
    assert AuthorityDisposition.ADMISSIBLE not in actual.values()
    transaction_cost = next(
        item
        for item in pack.threshold_source_inventory
        if item.authority_id == "TRANSACTION_COST_MODEL_V1"
    )
    assert transaction_cost.reusable_roles == (
        "COST_INPUT_MODEL",
        "COST_RECONCILIATION_IDENTITY",
    )
    assert "MINIMUM_NET_RETURN_VALUE" in transaction_cost.prohibited_roles
    dq_sources = {
        item.authority_id: item
        for item in pack.threshold_source_inventory
        if MandatoryAxis.CANONICAL_DQ_PIT in item.applicable_axes
    }
    assert set(dq_sources) == {
        "QQQ_OPTIONS_DQ_PIT_IDENTITY_V1",
        "QQQ_OPTIONS_STAGED_DQ_PIT_READINESS_V1",
    }
    assert all(
        "NUMERIC_DQ_THRESHOLD_SOURCE" in item.prohibited_roles
        for item in dq_sources.values()
    )


def test_gap_matrix_covers_all_axes_once_without_selecting_values() -> None:
    pack = _load().pack
    axes = pack.axis_gap_matrix

    assert tuple(item.axis_id for item in axes) == AXIS_ORDER
    assert len({item.threshold_id for item in axes}) == len(AXIS_ORDER)
    assert all(item.owner_value_state == "NOT_PROVIDED" for item in axes)
    assert all(item.owner_value_fields for item in axes)
    assert all(item.recommended_option_id in item.calibration_option_ids for item in axes)
    assert {item.proposed_direction for item in axes} == {
        ThresholdDirection.MINIMUM,
        ThresholdDirection.MAXIMUM,
        ThresholdDirection.EXACT_CATEGORICAL,
        ThresholdDirection.COMPOSITE_ALL,
    }

    raw = _read_pack(ROOT)
    raw_axes = raw["axis_gap_matrix"]
    assert isinstance(raw_axes, list)
    for raw_axis in raw_axes:
        assert isinstance(raw_axis, dict)
        assert "selected_value" not in raw_axis
        assert "threshold_value" not in raw_axis
        assert "current_value" not in raw_axis
        assert raw_axis["owner_value_state"] == "NOT_PROVIDED"


def test_axis_rules_replay_exact_trading_2540_taxonomy() -> None:
    pack = _load().pack
    predecessor = yaml.safe_load(
        (ROOT / "config/research/strategy_growth_action_value_preregistration_v1.yaml").read_text(
            encoding="utf-8"
        )
    )
    predecessor_axes = {item["axis_id"]: item for item in predecessor["mandatory_axes"]}

    for axis in pack.axis_gap_matrix:
        expected = predecessor_axes[axis.axis_id.value]
        assert axis.pass_rule_code == expected["pass_rule_code"]
        assert axis.fail_rule_code == expected["fail_rule_code"]
        assert axis.insufficient_rule_code == expected["insufficient_rule_code"]
        assert axis.invalid_rule_code == expected["invalid_rule_code"]


def test_owner_question_set_is_minimal_complete_and_pre_result() -> None:
    pack = _load().pack

    assert tuple(item.question_id for item in pack.owner_questions) == (
        "SOURCE_ASSIGNMENT",
        "EXACT_VALUE_SHEET",
        "REVIEW_CONDITION",
    )
    assert pack.owner_questions[0].recommended_choice == "APPROVE_RECOMMENDED_PER_AXIS"
    assert pack.owner_questions[1].recommended_choice == "PROVIDE_COMPLETE_EXACT_VALUE_SHEET"
    assert pack.owner_questions[2].recommended_choice == (
        "LOCK_V1_FOR_ONE_PRIMARY_WINDOW_EVALUATION_NEW_VERSION_FOR_CHANGE"
    )
    assert pack.owner_response_contract.axis_value_token == "OWNER_VALUE_REQUIRED"
    assert pack.owner_response_contract.complete_axis_set_required is True
    assert pack.owner_response_contract.partial_response_allowed is False
    assert pack.owner_response_contract.decision_before_result_required is True


def test_pack_is_canonical_sealed_and_replayable() -> None:
    pack = _load().pack

    replay = StrategyGrowthActionValueThresholdDecisionPack.from_json_bytes(pack.canonical_bytes)
    assert replay == pack
    assert replay.canonical_bytes == pack.canonical_bytes
    assert replay.canonical_sha256 == PACK_CANONICAL_SHA256


def test_source_byte_drift_is_rejected(tmp_path: Path) -> None:
    root = _fixture_root(tmp_path)
    source = root / "config/research/transaction_cost_model.yaml"
    source.write_text(source.read_text(encoding="utf-8") + "\n# drift\n", encoding="utf-8")

    with pytest.raises(
        StrategyGrowthActionValueThresholdDecisionPackError,
        match="authority file SHA-256 mismatch: TRANSACTION_COST_MODEL_V1",
    ):
        load_strategy_growth_action_value_threshold_decision_pack(
            pack_path=PACK_PATH,
            project_root=root,
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["threshold_source_inventory"][0].__setitem__(
                "disposition", "ADMISSIBLE"
            ),
            "threshold source inventory or disposition drifted",
        ),
        (
            lambda payload: payload["axis_gap_matrix"].pop(),
            "axis gap matrix must contain all eight axes",
        ),
        (
            lambda payload: payload["axis_gap_matrix"][0].__setitem__(
                "recommended_option_id", "CANONICAL_STRICT_DQ_PIT"
            ),
            "recommended option missing",
        ),
        (
            lambda payload: payload["scope_binding"].__setitem__(
                "primary_window_start", "2022-12-01"
            ),
            "primary window start drifted",
        ),
    ],
)
def test_policy_semantic_tamper_fails_closed(tmp_path: Path, mutate, match: str) -> None:
    root = _fixture_root(tmp_path)
    payload = _read_pack(root)
    mutate(payload)
    _write_pack(root, payload)

    with pytest.raises(StrategyGrowthActionValueThresholdDecisionPackError, match=match):
        load_strategy_growth_action_value_threshold_decision_pack(
            pack_path=PACK_PATH,
            project_root=root,
        )


def test_numeric_value_injection_is_rejected() -> None:
    pack = _load().pack
    payload = json.loads(pack.canonical_bytes)
    payload["axis_gap_matrix"][0]["selected_value"] = 0.01
    tampered = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")

    with pytest.raises(
        StrategyGrowthActionValueThresholdDecisionPackError,
        match="selected_value",
    ):
        StrategyGrowthActionValueThresholdDecisionPack.from_json_bytes(tampered)


def test_noncanonical_and_duplicate_json_are_rejected() -> None:
    pack = _load().pack
    noncanonical = json.dumps(json.loads(pack.canonical_bytes)).encode("utf-8")
    duplicate = pack.canonical_bytes.replace(
        b'{\n  "axis_gap_matrix"',
        b'{\n  "pack_status": "OWNER_DECISION_REQUIRED",\n  "axis_gap_matrix"',
        1,
    )

    with pytest.raises(
        StrategyGrowthActionValueThresholdDecisionPackError,
        match="not canonical JSON bytes",
    ):
        StrategyGrowthActionValueThresholdDecisionPack.from_json_bytes(noncanonical)
    with pytest.raises(
        StrategyGrowthActionValueThresholdDecisionPackError,
        match="duplicate JSON key: pack_status",
    ):
        StrategyGrowthActionValueThresholdDecisionPack.from_json_bytes(duplicate)
