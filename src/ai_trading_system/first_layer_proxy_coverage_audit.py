from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    SAFETY_BOUNDARY,
    clean_for_yaml,
    load_mapping,
    mapping,
    max_price_date,
    records,
    strings,
    validate_cached_market_data,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_proxy_coverage_audit_policy.yaml"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_proxy_coverage_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"
DEFAULT_FREE_FEATURE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "free_feature_family_registry_v2.yaml"
)
DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "participation_proxy_free_registry.yaml"
)
DEFAULT_COVERAGE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "free_data_feature_coverage_matrix.yaml"
)
DEFAULT_PIT_CONTRACT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "participation_proxy_pit_contract.yaml"
)
DEFAULT_FMP_GATE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "fmp_etf_holdings_trial_gate.yaml"
)
DEFAULT_FEATURE_ROOT = PROJECT_ROOT / "data" / "features"

BLOCKED_STATE = {
    "primary_window_validated": False,
    "reopen_gate_allowed": False,
    "validation_ready": False,
    "candidate_count": 0,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def run_first_layer_proxy_coverage_audit_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    free_feature_registry_path: Path = DEFAULT_FREE_FEATURE_REGISTRY_PATH,
    participation_proxy_registry_path: Path = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    coverage_matrix_path: Path = DEFAULT_COVERAGE_MATRIX_PATH,
    pit_contract_path: Path = DEFAULT_PIT_CONTRACT_PATH,
    fmp_gate_path: Path = DEFAULT_FMP_GATE_PATH,
    feature_root: Path = DEFAULT_FEATURE_ROOT,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    free_registry = load_mapping(free_feature_registry_path)
    participation_registry = load_mapping(participation_proxy_registry_path)
    coverage_matrix = load_mapping(coverage_matrix_path, missing_ok=True)
    pit_contract = load_mapping(pit_contract_path, missing_ok=True)
    fmp_gate = load_mapping(fmp_gate_path, missing_ok=True)

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=as_of_date,
        expected_price_tickers=strings(policy.get("data_quality_expected_price_tickers")),
        expected_rate_series=(),
    )
    resolved_as_of = (
        _parse_date(data_quality.get("as_of")) or as_of_date or max_price_date(prices_path)
    )
    price_summary = _price_summary(prices_path)
    feature_summaries = _feature_summaries(feature_root)

    rows = [
        *_free_feature_rows(
            free_registry=free_registry,
            coverage_matrix=coverage_matrix,
            feature_summaries=feature_summaries,
            policy=policy,
        ),
        *_etf_ratio_rows(
            participation_registry=participation_registry,
            price_summary=price_summary,
            policy=policy,
        ),
        *_external_proxy_rows(
            policy=policy,
            pit_contract=pit_contract,
            fmp_gate=fmp_gate,
        ),
    ]
    summary = _summary(rows, data_quality, policy, resolved_as_of)
    payload = _payload(
        policy=policy,
        data_quality=data_quality,
        rows=rows,
        summary=summary,
        as_of_date=resolved_as_of,
    )

    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    json_path = output_root / "proxy_coverage_matrix.json"
    yaml_path = inputs_root / "first_layer_proxy_coverage_audit.yaml"
    doc_path = docs_root / "first_layer_proxy_coverage_audit.md"
    write_json(json_path, payload)
    write_yaml(yaml_path, payload)
    write_markdown(doc_path, _render_audit(payload))

    payload["artifact_paths"] = {
        "proxy_coverage_matrix": str(json_path),
        "first_layer_proxy_coverage_audit_yaml": str(yaml_path),
        "first_layer_proxy_coverage_audit_doc": str(doc_path),
    }
    write_yaml(yaml_path, payload)
    return clean_for_yaml(payload)


def _free_feature_rows(
    *,
    free_registry: Mapping[str, Any],
    coverage_matrix: Mapping[str, Any],
    feature_summaries: Mapping[str, Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    families = mapping(free_registry.get("families"))
    coverage_by_family = {
        str(row.get("feature_family")): row for row in records(coverage_matrix.get("rows"))
    }
    roles = mapping(policy.get("feature_family_expected_roles"))
    rows: list[dict[str, Any]] = []
    for family_id, raw_family in families.items():
        family = mapping(raw_family)
        coverage = mapping(coverage_by_family.get(str(family_id)))
        feature_summary = mapping(feature_summaries.get(str(family_id)))
        earliest = _coverage_date(coverage.get("earliest_available_date"))
        feature_start = str(feature_summary.get("history_start_date", ""))
        history_start = earliest or feature_start
        data_available = bool(feature_summary.get("row_count", 0)) or bool(
            history_start and history_start not in {"missing", "source_dependent"}
        )
        if not earliest and feature_start:
            history_start = feature_start
        rows.append(
            {
                "proxy_id": str(family_id),
                "proxy_group": "free_feature_family",
                "data_available": data_available,
                "history_start_date": history_start if data_available else "",
                "primary_window_coverage": str(coverage.get("primary_window_coverage", "unknown")),
                "PIT_safe_or_not": str(family.get("PIT_status", "unknown")),
                "survivorship_risk": _survivorship_risk(str(family.get("PIT_status", ""))),
                "expected_signal_role": str(roles.get(str(family_id), family.get("usage", ""))),
                "replacement_for_true_breadth": False,
                "replacement_blocker": _replacement_blocker(str(family.get("PIT_status", ""))),
                "source": str(family.get("source", "")),
                "allowed_usage": strings(family.get("allowed_usage")),
                "blocked_usage": strings(family.get("blocked_usage")),
            }
        )
    return rows


def _etf_ratio_rows(
    *,
    participation_registry: Mapping[str, Any],
    price_summary: Mapping[str, Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    proxy_specs = mapping(policy.get("etf_ratio_proxies"))
    configured = {
        str(row.get("proxy_id")): row for row in records(participation_registry.get("proxies"))
    }
    primary_start = str(policy.get("primary_window_start", "2021-02-22"))
    rows: list[dict[str, Any]] = []
    for proxy_id, raw_spec in proxy_specs.items():
        spec = mapping(raw_spec)
        required_tickers = strings(spec.get("required_tickers"))
        available_tickers = [
            ticker
            for ticker in required_tickers
            if mapping(price_summary.get(ticker)).get("available")
        ]
        missing_tickers = [ticker for ticker in required_tickers if ticker not in available_tickers]
        starts = [
            str(mapping(price_summary.get(ticker)).get("history_start_date"))
            for ticker in available_tickers
        ]
        ends = [
            str(mapping(price_summary.get(ticker)).get("history_end_date"))
            for ticker in available_tickers
        ]
        data_available = len(missing_tickers) == 0
        history_start = max(starts) if data_available and starts else ""
        primary_window_coverage = (
            "covered"
            if data_available and history_start <= primary_start and min(ends) >= primary_start
            else f"missing_components:{','.join(missing_tickers)}"
        )
        configured_row = mapping(configured.get(str(proxy_id)))
        rows.append(
            {
                "proxy_id": str(proxy_id),
                "proxy_group": "etf_ratio_price_proxy",
                "data_available": data_available,
                "history_start_date": history_start,
                "primary_window_coverage": primary_window_coverage,
                "PIT_safe_or_not": "PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH"
                if data_available
                else "PIT_BLOCKED_BY_PRICE_COVERAGE",
                "survivorship_risk": _ratio_survivorship_risk(configured_row),
                "expected_signal_role": str(spec.get("expected_signal_role", "")),
                "replacement_for_true_breadth": False,
                "replacement_blocker": "price_proxy_not_constituent_membership",
                "required_tickers": required_tickers,
                "available_tickers": available_tickers,
                "missing_tickers": missing_tickers,
                "registry_status": str(configured_row.get("status", "REGISTRY_ONLY")),
                "caveats": strings(configured_row.get("caveats")),
            }
        )
    return rows


def _external_proxy_rows(
    *,
    policy: Mapping[str, Any],
    pit_contract: Mapping[str, Any],
    fmp_gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    pit_rows = {str(row.get("source")): row for row in records(pit_contract.get("rows"))}
    rows: list[dict[str, Any]] = []
    for item in records(policy.get("external_proxy_rows")):
        proxy_id = str(item.get("proxy_id"))
        source_key = (
            "Alpha Vantage listing status"
            if proxy_id == "alpha_vantage_listing_status"
            else "FMP ETF holdings"
        )
        pit_row = mapping(pit_rows.get(source_key))
        status_detail = str(pit_row.get("status_detail", ""))
        if proxy_id == "fmp_etf_holdings_low_cost_gate":
            status_detail = str(fmp_gate.get("status", status_detail))
        rows.append(
            {
                "proxy_id": proxy_id,
                "proxy_group": "external_low_cost_proxy_gate",
                "data_available": False,
                "history_start_date": "",
                "primary_window_coverage": "missing_or_unverified",
                "PIT_safe_or_not": str(item.get("PIT_safe_or_not")),
                "survivorship_risk": str(item.get("survivorship_risk")),
                "expected_signal_role": str(item.get("expected_signal_role")),
                "replacement_for_true_breadth": False,
                "replacement_blocker": "historical_membership_and_known_at_not_confirmed",
                "status_detail": status_detail,
            }
        )
    return rows


def _feature_summaries(feature_root: Path) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for path in feature_root.glob("*_free_v1.parquet"):
        frame = pd.read_parquet(path)
        family_id = path.stem
        summaries[family_id] = _frame_date_summary(frame)
    participation_path = feature_root / "participation_proxy_free_v2.parquet"
    if participation_path.exists():
        summaries["participation_proxy_free_v1"] = _frame_date_summary(
            pd.read_parquet(participation_path)
        )
    return summaries


def _frame_date_summary(frame: pd.DataFrame) -> dict[str, Any]:
    dates = pd.to_datetime(frame.get("date"), errors="coerce").dropna()
    return {
        "row_count": int(len(frame)),
        "history_start_date": dates.min().date().isoformat() if not dates.empty else "",
        "history_end_date": dates.max().date().isoformat() if not dates.empty else "",
    }


def _price_summary(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path, usecols=["date", "ticker", "adj_close"])
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()].copy()
    summaries: dict[str, dict[str, Any]] = {}
    for ticker, group in frame.groupby("ticker"):
        dates = group["date"].dropna()
        summaries[str(ticker)] = {
            "available": not dates.empty,
            "history_start_date": dates.min().date().isoformat() if not dates.empty else "",
            "history_end_date": dates.max().date().isoformat() if not dates.empty else "",
            "row_count": int(len(group)),
        }
    return summaries


def _summary(
    rows: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
    policy: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    replacement_count = sum(1 for row in rows if row.get("replacement_for_true_breadth"))
    data_available_count = sum(1 for row in rows if row.get("data_available"))
    primary_covered_count = sum(
        1 for row in rows if str(row.get("primary_window_coverage")) == "covered"
    )
    return {
        "final_status": "FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_TRUE_BREADTH_NOT_REPLACED",
        "market_regime": MARKET_REGIME,
        "requested_start": mapping(policy.get("requested_window")).get("start"),
        "requested_end": mapping(policy.get("requested_window")).get("end", "latest"),
        "primary_window_start": policy.get("primary_window_start"),
        "as_of": as_of_date.isoformat(),
        "data_quality_status": data_quality.get("status"),
        "proxy_count": len(rows),
        "data_available_count": data_available_count,
        "primary_window_covered_count": primary_covered_count,
        "replacement_for_true_breadth_count": replacement_count,
        "true_breadth_replaced": replacement_count > 0,
        "next_required_tasks": ["TRADING-2272", "TRADING-2273"],
    }


def _payload(
    *,
    policy: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    as_of_date: date,
) -> dict[str, Any]:
    requested = mapping(policy.get("requested_window"))
    return {
        "schema_version": "first_layer_proxy_coverage_audit.v1",
        "report_type": "first_layer_proxy_coverage_audit",
        "title": "First-Layer Free / Low-Cost Proxy Coverage Audit",
        "status": "FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED",
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "requested_start": str(requested.get("start", DEFAULT_BACKTEST_START)),
        "requested_end": str(requested.get("end", "latest")),
        "primary_window_start": str(policy.get("primary_window_start", "")),
        "as_of": as_of_date.isoformat(),
        "data_quality_status": data_quality.get("status"),
        "data_quality": data_quality,
        "policy_id": str(policy.get("policy_id", "")),
        "summary": clean_for_yaml(dict(summary)),
        "rows": clean_for_yaml(list(rows)),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "first_layer_proxy_coverage_audit",
            "model_version": "first_layer_proxy_coverage_audit_v1",
            "threshold_policy": str(policy.get("policy_id", "")),
            "candidate_count": 0,
            "boundary_contract_version": "first_layer_proxy_coverage_audit_research_only_v1",
        },
        **SAFETY_BOUNDARY,
        **BLOCKED_STATE,
    }


def _render_audit(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# First-layer proxy coverage audit",
        "",
        f"- status: `{payload.get('status')}`",
        f"- market_regime: `{payload.get('market_regime')}`",
        (
            f"- requested_date_range: `{payload.get('requested_start')}` "
            f"to `{payload.get('requested_end')}`"
        ),
        f"- primary_window_start: `{payload.get('primary_window_start')}`",
        f"- data_quality_status: `{payload.get('data_quality_status')}`",
        "- safety: `replacement_for_true_breadth_count=0`, `promotion_allowed=false`, "
        "`paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`",
        "",
        "## 结论",
        "",
        "免费 / 低成本 proxy 只能保留为 diagnostic input。可用 ETF ratio 是 price proxy，"
        "不是 historical constituent breadth；listing status 和 holdings gate 仍缺 known-at / "
        "historical membership 证明，不能替代 true breadth。",
        "",
        "## Coverage rows",
        "",
        "|proxy_id|group|data_available|primary_window_coverage|PIT_safe_or_not|replacement_for_true_breadth|",
        "|---|---|---:|---|---|---:|",
    ]
    for row in records(payload.get("rows")):
        lines.append(
            f"|`{row.get('proxy_id')}`|{row.get('proxy_group')}|{row.get('data_available')}|"
            f"{row.get('primary_window_coverage')}|`{row.get('PIT_safe_or_not')}`|"
            f"{row.get('replacement_for_true_breadth')}|"
        )
    lines.extend(
        [
            "",
            "## Audit notes",
            "",
            f"- proxy_count: `{summary.get('proxy_count')}`",
            f"- data_available_count: `{summary.get('data_available_count')}`",
            f"- primary_window_covered_count: `{summary.get('primary_window_covered_count')}`",
            "- `replacement_for_true_breadth=false` 是本报告的核心结论；任何后续 challenger "
            "experiment 都必须继续把 proxy 与 true breadth 区分开。",
        ]
    )
    return "\n".join(lines) + "\n"


def _coverage_date(value: object) -> str:
    text = str(value or "")
    return "" if text in {"", "missing", "source_dependent"} else text


def _replacement_blocker(pit_status: str) -> str:
    if "NOT_TRUE_PIT_BREADTH" in pit_status:
        return "not_true_pit_breadth"
    if "WARNING" in pit_status:
        return "pit_warning_or_known_at_unconfirmed"
    return "market_or_price_proxy_not_constituent_membership"


def _ratio_survivorship_risk(row: Mapping[str, Any]) -> str:
    caveats = set(strings(row.get("caveats")))
    if "SURVIVORSHIP_SAFE_IF_ETF_PRICE_ONLY" in caveats:
        return "low_for_etf_price_series_but_not_constituent_breadth"
    return "not_true_breadth_or_missing_price_component"


def _survivorship_risk(pit_status: str) -> str:
    if "NOT_TRUE_PIT_BREADTH" in pit_status:
        return "not_constituent_breadth_price_proxy_only"
    if "WARNING" in pit_status:
        return "pit_warning_or_timestamp_risk"
    if "PIT_APPROVED" in pit_status:
        return "low_for_market_series_not_constituent_breadth"
    return "unknown"


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None
