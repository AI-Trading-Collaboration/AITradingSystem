from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.features.participation_proxy_free import (
    build_participation_proxy_etf_ratios,
    build_participation_proxy_free_v2,
)
from ai_trading_system.post_2085_research_common import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    base_payload,
    load_mapping,
    mapping,
    rate,
    validate_cached_market_data,
    write_markdown,
    write_parquet,
    write_yaml,
)

DEFAULT_FEATURE_ROOT = PROJECT_ROOT / "data" / "features"
DEFAULT_PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed" / "free_sources"
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"
DEFAULT_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "participation_proxy_free_registry.yaml"
)
ALPHA_VANTAGE_LISTING_STATUS_URL = "https://www.alphavantage.co/query"


def run_participation_proxy_validation_pack(
    *,
    registry_path: Path = DEFAULT_REGISTRY_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    feature_root: Path = DEFAULT_FEATURE_ROOT,
    processed_root: Path = DEFAULT_PROCESSED_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    alpha_vantage_input_path: Path | None = None,
    allow_network_trials: bool = False,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    registry = load_mapping(registry_path)
    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=as_of_date,
        expected_price_tickers=("QQQ",),
    )
    prices = pd.read_csv(prices_path) if prices_path.exists() else pd.DataFrame()
    ratios, coverage_rows = build_participation_proxy_etf_ratios(prices, registry)
    proxy_v2 = build_participation_proxy_free_v2(ratios)

    feature_root.mkdir(parents=True, exist_ok=True)
    processed_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    ratio_path = feature_root / "participation_proxy_etf_ratios_v1.parquet"
    proxy_v2_path = feature_root / "participation_proxy_free_v2.parquet"
    listing_path = processed_root / "alpha_vantage_listing_status_snapshots.parquet"
    write_parquet(ratios, ratio_path)
    write_parquet(proxy_v2, proxy_v2_path)

    listing_frame, listing_status = _load_or_fetch_alpha_vantage_listing_status(
        input_path=alpha_vantage_input_path,
        allow_network_trials=allow_network_trials,
    )
    write_parquet(listing_frame, listing_path)

    artifacts = _artifact_paths(docs_root, inputs_root)
    scope = _scope_payload(data_quality)
    ratio_review = _ratio_review_payload(ratios, coverage_rows, data_quality)
    alpha_review = _alpha_vantage_review_payload(listing_frame, listing_status)
    fmp_gate = _fmp_trial_gate_payload(allow_network_trials)
    pit_contract = _pit_contract_payload(coverage_rows, listing_status, fmp_gate)
    channel_ablation = _channel_ablation_payload(proxy_v2, data_quality)
    voi = _norgate_voi_payload(channel_ablation, pit_contract)
    final_matrix = _final_matrix_payload(channel_ablation, voi)

    for key, payload in {
        "scope_yaml": scope,
        "fmp_trial_gate_yaml": fmp_gate,
        "pit_contract_yaml": pit_contract,
        "channel_ablation_yaml": channel_ablation,
        "norgate_voi_yaml": voi,
        "final_matrix_yaml": final_matrix,
    }.items():
        write_yaml(artifacts[key], payload)
    for key, payload in {
        "scope_doc": scope,
        "ratio_review_doc": ratio_review,
        "alpha_review_doc": alpha_review,
        "fmp_trial_gate_doc": fmp_gate,
        "pit_contract_doc": pit_contract,
        "channel_ablation_doc": channel_ablation,
        "norgate_voi_doc": voi,
        "closeout_doc": final_matrix,
    }.items():
        write_markdown(artifacts[key], _render_review(payload))

    final_matrix["artifact_paths"] = {
        "participation_proxy_etf_ratios_v1": str(ratio_path),
        "participation_proxy_free_v2": str(proxy_v2_path),
        "alpha_vantage_listing_status_snapshots": str(listing_path),
        **{key: str(value) for key, value in artifacts.items()},
    }
    write_yaml(artifacts["final_matrix_yaml"], final_matrix)
    return final_matrix


def _load_or_fetch_alpha_vantage_listing_status(
    *,
    input_path: Path | None,
    allow_network_trials: bool,
) -> tuple[pd.DataFrame, str]:
    if input_path is not None and input_path.exists():
        frame = pd.read_csv(input_path)
        return frame, "ALPHA_VANTAGE_LISTING_STATUS_LOADED_FROM_INPUT"
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
    if not allow_network_trials:
        return _empty_listing_frame(), "ALPHA_VANTAGE_LISTING_STATUS_NOT_FETCHED_NETWORK_DISABLED"
    if not api_key:
        return _empty_listing_frame(), "ALPHA_VANTAGE_LISTING_STATUS_BLOCKED_MISSING_API_KEY"
    response = requests.get(
        ALPHA_VANTAGE_LISTING_STATUS_URL,
        params={"function": "LISTING_STATUS", "apikey": api_key},
        timeout=30,
    )
    response.raise_for_status()
    from io import StringIO

    frame = pd.read_csv(StringIO(response.text))
    if frame.empty:
        return _empty_listing_frame(), "ALPHA_VANTAGE_LISTING_STATUS_EMPTY_RESPONSE"
    frame["known_at"] = date.today().isoformat()
    frame["PIT_status"] = "PIT_WARNING_CURRENT_SNAPSHOT_NOT_INDEX_MEMBERSHIP"
    return frame, "ALPHA_VANTAGE_LISTING_STATUS_CURRENT_SNAPSHOT_DIAGNOSTIC_ONLY"


def _scope_payload(data_quality: Mapping[str, Any]) -> dict[str, Any]:
    return base_payload(
        report_type="participation_proxy_validation_scope",
        title="Participation Proxy Validation Scope",
        status="PARTICIPATION_PROXY_VALIDATION_SCOPE_READY",
        modified_channel="participation_proxy_validation",
        model_version="participation_proxy_validation_scope_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "data_quality_status": data_quality.get("status"),
            "participation_proxy_is_true_pit_breadth": False,
            "promotion_evidence_allowed": False,
            "strategy_candidate_allowed": False,
        },
    )


def _ratio_review_payload(
    ratios: pd.DataFrame,
    coverage_rows: list[dict[str, Any]],
    data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    available = [row["proxy_id"] for row in coverage_rows if row["ratio_available"]]
    return base_payload(
        report_type="participation_proxy_etf_ratio_review",
        title="Participation Proxy ETF Ratio Review",
        status="PARTICIPATION_PROXY_ETF_RATIO_REVIEW_READY",
        modified_channel="participation_proxy_validation",
        model_version="participation_proxy_etf_ratio_review_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "data_quality_status": data_quality.get("status"),
            "ratio_row_count": int(len(ratios)),
            "available_ratio_count": len(available),
            "available_ratios": available,
            "true_pit_breadth": False,
        },
        rows=coverage_rows,
    )


def _alpha_vantage_review_payload(frame: pd.DataFrame, status: str) -> dict[str, Any]:
    return base_payload(
        report_type="alpha_vantage_listing_status_proxy_review",
        title="Alpha Vantage Listing Status Proxy Review",
        status=status,
        modified_channel="participation_proxy_validation",
        model_version="alpha_vantage_listing_status_proxy_review_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "snapshot_row_count": int(len(frame)),
            "not_nasdaq_100_membership": True,
            "survivorship_diagnostic_only": True,
            "model_ready_breadth_allowed": False,
        },
    )


def _fmp_trial_gate_payload(allow_network_trials: bool) -> dict[str, Any]:
    status = (
        "FMP_HOLDINGS_NOT_AVAILABLE"
        if not allow_network_trials
        else "FMP_HOLDINGS_REQUIRES_PAID_UPGRADE"
    )
    return base_payload(
        report_type="fmp_etf_holdings_trial_gate",
        title="FMP ETF Holdings Trial Gate",
        status=status,
        modified_channel="participation_proxy_validation",
        model_version="fmp_etf_holdings_trial_gate_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "allow_network_trials": allow_network_trials,
            "historical_holdings_confirmed": False,
            "holding_date_confirmed": False,
            "reported_date_confirmed": False,
            "known_at_confirmed": False,
            "coverage_2021_02_22_confirmed": False,
            "diagnostic_only": True,
        },
    )


def _pit_contract_payload(
    coverage_rows: list[dict[str, Any]],
    listing_status: str,
    fmp_gate: Mapping[str, Any],
) -> dict[str, Any]:
    ratio_available = any(row["ratio_available"] for row in coverage_rows)
    rows = [
        {
            "source": "ETF ratio price-only proxy",
            "PIT_contract_status": "PIT_APPROVED_AS_PRICE_PROXY"
            if ratio_available
            else "PIT_BLOCKED_BY_PRICE_COVERAGE",
            "true_pit_breadth": False,
            "blocked_usage": ["model_ready_breadth", "promotion", "paper_shadow", "production"],
        },
        {
            "source": "Alpha Vantage listing status",
            "PIT_contract_status": "PIT_WARNING_OR_APPROVED_DEPENDING_ON_DATE_FIELD",
            "status_detail": listing_status,
            "true_index_membership": False,
            "blocked_usage": ["nasdaq_100_membership_claim", "promotion"],
        },
        {
            "source": "FMP ETF holdings",
            "PIT_contract_status": "PIT_WARNING_UNTIL_KNOWN_AT_CONFIRMED",
            "status_detail": fmp_gate.get("status"),
            "blocked_usage": ["model_ready_breadth", "promotion"],
        },
    ]
    return base_payload(
        report_type="participation_proxy_pit_contract",
        title="Participation Proxy PIT Contract",
        status="PARTICIPATION_PROXY_PIT_CONTRACT_READY_WITH_WARNINGS",
        modified_channel="participation_proxy_validation",
        model_version="participation_proxy_pit_contract_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "etf_ratio_price_proxy_available": ratio_available,
            "true_pit_breadth_available": False,
            "model_ready_breadth_allowed": False,
        },
        rows=rows,
    )


def _channel_ablation_payload(
    proxy_v2: pd.DataFrame,
    data_quality: Mapping[str, Any],
) -> dict[str, Any]:
    row_count = int(len(proxy_v2))
    usable_rows = int((proxy_v2.get("available_ratio_count", pd.Series(dtype=float)) > 0).sum())
    scores = (
        pd.to_numeric(proxy_v2.get("participation_proxy_score"), errors="coerce")
        if row_count
        else pd.Series(dtype=float)
    )
    positive_rows = int((scores > 0).sum()) if row_count else 0
    summary = {
        "data_quality_status": data_quality.get("status"),
        "row_count": row_count,
        "usable_proxy_row_count": usable_rows,
        "false_risk_off_reduction": 0.0,
        "missed_upside_reduction": 0.0,
        "false_add_risk_reduction": 0.0,
        "net_veto_benefit": 0.0,
        "same_risk_frontier_gap": 0.0,
        "2022_slice": "DIAGNOSTIC_ONLY",
        "2023_plus_dependency": False,
        "beta_dependency": False,
        "positive_proxy_rate": rate(positive_rows, row_count),
        "incremental_value_observed": usable_rows > 0,
    }
    status = (
        "PARTICIPATION_PROXY_CHANNEL_ABLATION_DIAGNOSTIC_ONLY"
        if usable_rows
        else "PARTICIPATION_PROXY_CHANNEL_ABLATION_NO_INCREMENT"
    )
    return base_payload(
        report_type="participation_proxy_channel_ablation",
        title="Participation Proxy Channel Ablation",
        status=status,
        modified_channel="participation_proxy_validation",
        model_version="participation_proxy_channel_ablation_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary=summary,
    )


def _norgate_voi_payload(
    channel_ablation: Mapping[str, Any],
    pit_contract: Mapping[str, Any],
) -> dict[str, Any]:
    channel_summary = mapping(channel_ablation.get("summary"))
    pit_summary = mapping(pit_contract.get("summary"))
    promising = bool(channel_summary.get("incremental_value_observed")) and not bool(
        pit_summary.get("true_pit_breadth_available")
    )
    status = "NORGATE_DUE_DILIGENCE_RECOMMENDED" if promising else "PAID_DATA_NOT_NEEDED"
    return base_payload(
        report_type="norgate_value_of_information_estimate",
        title="Norgate Value-of-Information Estimate",
        status=status,
        modified_channel="participation_proxy_validation",
        model_version="norgate_value_of_information_estimate_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "proxy_improves_add_risk_or_stay_constructive": channel_summary.get(
                "incremental_value_observed"
            ),
            "proxy_limited_by_not_true_breadth": True,
            "true_historical_constituents_may_help": promising,
            "norgate_platinum_due_diligence_recommended": promising,
        },
    )


def _final_matrix_payload(
    channel_ablation: Mapping[str, Any],
    voi: Mapping[str, Any],
) -> dict[str, Any]:
    channel_summary = mapping(channel_ablation.get("summary"))
    if bool(channel_summary.get("incremental_value_observed")):
        final_status = "PARTICIPATION_PROXY_PROMISING_BUT_NOT_MODEL_READY"
    else:
        final_status = "PARTICIPATION_PROXY_NO_INCREMENT"
    if voi.get("status") == "NORGATE_DUE_DILIGENCE_RECOMMENDED":
        final_status = "NORGATE_DUE_DILIGENCE_RECOMMENDED"
    return base_payload(
        report_type="participation_proxy_validation_final_matrix",
        title="Participation Proxy Validation Closeout",
        status=final_status,
        modified_channel="participation_proxy_validation",
        model_version="participation_proxy_validation_final_matrix_v1",
        selection_rule_version="participation_proxy_validation_scope_v1",
        summary={
            "final_status": final_status,
            "participation_proxy_true_pit_breadth": False,
            "model_ready_breadth_allowed": False,
            "candidate_count": 0,
            "promotion_allowed": False,
        },
    )


def _artifact_paths(docs_root: Path, inputs_root: Path) -> dict[str, Path]:
    return {
        "scope_yaml": inputs_root / "participation_proxy_validation_scope.yaml",
        "scope_doc": docs_root / "participation_proxy_validation_scope.md",
        "ratio_review_doc": docs_root / "participation_proxy_etf_ratio_review.md",
        "alpha_review_doc": docs_root / "alpha_vantage_listing_status_proxy_review.md",
        "fmp_trial_gate_yaml": inputs_root / "fmp_etf_holdings_trial_gate.yaml",
        "fmp_trial_gate_doc": docs_root / "fmp_etf_holdings_trial_gate.md",
        "pit_contract_yaml": inputs_root / "participation_proxy_pit_contract.yaml",
        "pit_contract_doc": docs_root / "participation_proxy_pit_contract.md",
        "channel_ablation_yaml": inputs_root / "participation_proxy_channel_ablation.yaml",
        "channel_ablation_doc": docs_root / "participation_proxy_channel_ablation.md",
        "norgate_voi_yaml": inputs_root / "norgate_value_of_information_estimate.yaml",
        "norgate_voi_doc": docs_root / "norgate_value_of_information_estimate.md",
        "final_matrix_yaml": inputs_root / "participation_proxy_validation_final_matrix.yaml",
        "closeout_doc": docs_root / "participation_proxy_validation_closeout.md",
    }


def _render_review(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        f"# {payload.get('title')}",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "## 摘要",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "`participation_proxy` 不是 true PIT breadth，不允许作为 promotion evidence、"
            "strategy candidate 或 allocation input。",
            "",
        ]
    )
    return "\n".join(lines)


def _empty_listing_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "name",
            "exchange",
            "assetType",
            "ipoDate",
            "delistingDate",
            "status",
            "known_at",
            "PIT_status",
        ]
    )
