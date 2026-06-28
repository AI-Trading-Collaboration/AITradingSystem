from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

TRUE_BREADTH_CONTRACT = PROJECT_ROOT / "config" / "research" / "true_breadth_data_contract.yaml"
VENDOR_REGISTRY = PROJECT_ROOT / "config" / "data" / "paid_breadth_data_vendor_registry.yaml"
TRIAL_GATE_POLICY = PROJECT_ROOT / "config" / "research" / "paid_data_trial_gate_policy.yaml"
FINAL_MATRIX = PROJECT_ROOT / "inputs" / "research_reviews" / (
    "paid_data_due_diligence_final_matrix.yaml"
)
SCORING_MATRIX = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "paid_data_due_diligence"
    / "vendor_scoring_matrix.csv"
)


def test_due_diligence_does_not_enable_promotion() -> None:
    final_matrix = _load(FINAL_MATRIX)

    assert final_matrix["status"] == "NORGATE_TRIAL_RECOMMENDED"
    assert final_matrix["summary"]["first_layer_reopen_allowed"] is False
    assert final_matrix["promotion_allowed"] is False
    assert final_matrix["paper_shadow_allowed"] is False
    assert final_matrix["production_allowed"] is False
    assert final_matrix["broker_action"] == "none"
    assert final_matrix["summary"]["candidate_count"] == 0


def test_vendor_trial_requires_owner_approval() -> None:
    policy = _load(TRIAL_GATE_POLICY)
    final_matrix = _load(FINAL_MATRIX)

    assert policy["required_preconditions"]["owner_manual_approval_before_trial"] is True
    assert policy["required_preconditions"]["owner_manual_approval_before_purchase"] is True
    assert policy["safety_boundary"]["trial_allowed_without_owner_approval"] is False
    assert final_matrix["summary"]["owner_manual_approval_required_before_trial"] is True
    assert final_matrix["summary"]["purchase_allowed"] is False


def test_price_only_sources_cannot_be_marked_true_breadth() -> None:
    registry = _load(VENDOR_REGISTRY)
    price_only = [
        vendor for vendor in registry["vendors"] if vendor.get("price_only_source") is True
    ]

    assert price_only
    assert all(vendor["true_breadth_candidate"] is False for vendor in price_only)
    assert all(vendor["model_ready_breadth_allowed_before_trial"] is False for vendor in price_only)
    assert all(
        "price_cross_check_only" in vendor["allowed_use_before_trial"]
        for vendor in price_only
    )


def test_pit_warning_source_is_diagnostic_only() -> None:
    contract = _load(TRUE_BREADTH_CONTRACT)
    registry = _load(VENDOR_REGISTRY)
    fmp = _vendor(registry, "fmp_etf_holdings")

    assert contract["holdings_based_sources"]["known_at_semantics"][
        "missing_or_ambiguous_status"
    ] == "PIT_WARNING_DIAGNOSTIC_ONLY"
    assert fmp["capability_check"]["sanitized_result"] == "HTTP_402_PAYMENT_REQUIRED"
    assert fmp["allowed_use_before_trial"].startswith("diagnostic_only")
    assert fmp["model_ready_breadth_allowed_before_trial"] is False


def test_current_constituents_backfill_forbidden() -> None:
    contract = _load(TRUE_BREADTH_CONTRACT)

    assert contract["forbidden_methods"]["current_constituents_backfill"]["allowed"] is False
    assert "current_constituents_backfill" in contract["forbidden_methods"]


def test_true_breadth_contract_requires_delisted_and_historical_membership() -> None:
    contract = _load(TRUE_BREADTH_CONTRACT)
    requirements = contract["requirements"]

    assert requirements["historical_constituents"]["required"] is True
    assert requirements["daily_membership_query"]["required"] is True
    assert requirements["delisted_securities"]["required"] is True
    assert requirements["survivorship_bias_free"]["required"] is True
    assert contract["primary_window_coverage"]["start"] == "2021-02-22"
    assert contract["primary_window_coverage"]["required"] is True


def test_norgate_recommendation_does_not_auto_purchase() -> None:
    registry = _load(VENDOR_REGISTRY)
    policy = _load(TRIAL_GATE_POLICY)
    final_matrix = _load(FINAL_MATRIX)
    norgate = _vendor(registry, "norgate_us_stocks_platinum")
    rows = _read_scoring_rows(SCORING_MATRIX)
    norgate_score = next(row for row in rows if row["vendor"] == "Norgate US Stocks Platinum")

    assert norgate["current_due_diligence_status"] == (
        "NORGATE_TRIAL_RECOMMENDED_OWNER_APPROVAL_REQUIRED"
    )
    assert int(norgate_score["total_score"]) >= 80
    assert norgate_score["recommendation"] == "TRIAL_RECOMMENDED"
    assert policy["safety_boundary"]["auto_purchase_allowed"] is False
    assert norgate["auto_purchase_allowed"] is False
    assert final_matrix["summary"]["purchase_allowed"] is False


def _load(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    assert isinstance(payload, dict)
    return payload


def _vendor(registry: dict[str, Any], vendor_id: str) -> dict[str, Any]:
    return next(vendor for vendor in registry["vendors"] if vendor["vendor_id"] == vendor_id)


def _read_scoring_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))
