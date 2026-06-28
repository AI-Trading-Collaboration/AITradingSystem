from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

LOCKED_FEATURE_SET_PATH = Path("config/research/channel_specific_feature_set_v1_locked.yaml")
CHANNEL_CONFIG_PATH = Path("config/research/channel_specific_first_layer_v3.yaml")
DO_NOT_SELECTION_RULE_PATH = Path("config/research/do_not_de_risk_v3_selection_rule.yaml")
RISK_VETO_SELECTION_RULE_PATH = Path("config/research/risk_on_veto_v3_selection_rule.yaml")
CHANNEL_PIT_MATRIX_PATH = Path(
    "outputs/research_trends/channel_specific_v3/channel_pit_feature_matrix_v3.csv"
)
COMPOSER_PATH = Path(
    "outputs/research_trends/channel_specific_v3/channel_composer_v3_predictions.csv"
)
DRY_RUN_PATH = Path("outputs/research_trends/channel_specific_v3/policy_compiler_dry_run.csv")
SELECTION_RESULT_PATH = Path(
    "inputs/research_reviews/channel_specific_v3_selection_rule_result.yaml"
)
FINAL_MATRIX_PATH = Path(
    "inputs/research_reviews/channel_specific_first_layer_v3_final_matrix.yaml"
)
ACTUAL_PATH_MATRIX_PATH = Path(
    "inputs/research_reviews/channel_specific_v3_actual_path_matrix.yaml"
)


def test_channel_v3_uses_selected_families_only() -> None:
    locked = _load_yaml(LOCKED_FEATURE_SET_PATH)
    config = _load_yaml(CHANNEL_CONFIG_PATH)

    assert locked["do_not_de_risk"]["allowed_families"] == ["drawdown_recovery"]
    assert locked["risk_on_veto"]["allowed_families"] == [
        "volatility_compression",
        "rates_liquidity",
    ]
    assert config["channels"]["do_not_de_risk"]["allowed_families"] == ["drawdown_recovery"]
    assert config["channels"]["risk_on_veto"]["allowed_families"] == [
        "volatility_compression",
        "rates_liquidity",
    ]
    assert config["summary"]["can_emit_weights"] is False


def test_blocked_families_cannot_enter_channel_model() -> None:
    locked = _load_yaml(LOCKED_FEATURE_SET_PATH)
    columns = set(_csv_columns(CHANNEL_PIT_MATRIX_PATH))

    assert locked["diagnostic_only"] == ["trend_persistence", "relative_strength"]
    assert locked["blocked"] == ["breadth_participation", "event_risk"]
    assert "qqq_vs_sgov_momentum_60d" not in columns
    assert "qqq_vs_tqqq_consistency_20d" not in columns
    assert not any("breadth" in column or "event" in column for column in columns)


def test_do_not_de_risk_channel_cannot_emit_add_risk() -> None:
    rows = _csv_rows(COMPOSER_PATH)
    columns = set(rows[0])

    assert "add_risk_probability" not in columns
    assert "add_risk_signal" not in columns
    assert all(row["add_risk_allowed"] == "False" for row in rows)


def test_risk_on_veto_channel_cannot_emit_weights() -> None:
    composer_columns = set(_csv_columns(COMPOSER_PATH))
    dry_run_columns = set(_csv_columns(DRY_RUN_PATH))
    forbidden = {
        "target_weights",
        "portfolio_weights",
        "QQQ",
        "SGOV",
        "TQQQ",
        "trade_action",
    }

    assert not (forbidden & composer_columns)
    assert not ({"target_weights", "portfolio_weights", "trade_action"} & dry_run_columns)


def test_policy_compiler_dry_run_blocks_growth_when_veto_active() -> None:
    veto_rows = [row for row in _csv_rows(DRY_RUN_PATH) if row["risk_on_veto_active"] == "True"]

    assert veto_rows
    assert all(row["compiler_veto_active"] == "True" for row in veto_rows)
    assert all(row["blocked_growth_overlay"] == "True" for row in veto_rows)
    assert all(row["tqqq_allowed"] == "False" for row in veto_rows)


def test_channel_v3_selection_rule_requires_primary_window() -> None:
    do_not_rule = _load_yaml(DO_NOT_SELECTION_RULE_PATH)
    risk_rule = _load_yaml(RISK_VETO_SELECTION_RULE_PATH)
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert do_not_rule["do_not_de_risk_v3_selection_rule"]["required"][
        "research_window_id"
    ] == "EXACT_THREE_ASSET_VALIDATED_WINDOW"
    assert risk_rule["risk_on_veto_v3_selection_rule"]["required"]["research_window_id"] == (
        "EXACT_THREE_ASSET_VALIDATED_WINDOW"
    )
    assert final["research_window_id"] == "exact_three_asset_validated"
    assert final["summary"]["market_regime"] == "ai_after_chatgpt"


def test_channel_v3_cannot_enable_promotion() -> None:
    selection = _load_yaml(SELECTION_RESULT_PATH)
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert selection["summary"]["candidate_count"] == 0
    assert final["summary"]["candidate_count"] == 0
    for artifact in (selection, final):
        assert artifact["promotion_allowed"] is False
        assert artifact["paper_shadow_allowed"] is False
        assert artifact["production_allowed"] is False
        assert artifact["broker_action"] == "none"
        assert artifact["dynamic_promotion_status"] == "BLOCKED"


def test_channel_v3_artifacts_have_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    paths = [
        LOCKED_FEATURE_SET_PATH,
        CHANNEL_CONFIG_PATH,
        ACTUAL_PATH_MATRIX_PATH,
        SELECTION_RESULT_PATH,
        FINAL_MATRIX_PATH,
    ]

    for path in paths:
        artifact = _load_yaml(path)
        metadata = artifact["research_audit_metadata"]

        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "first_layer"
        assert metadata["modified_channel"] == "channel_specific_first_layer_v3"
        assert metadata["candidate_count"] == 0
        assert artifact["promotion_allowed"] is False
        assert artifact["broker_action"] == "none"


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw


def _csv_columns(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames
        return list(reader.fieldnames)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
