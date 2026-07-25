from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.config import DataQualityConfig, load_data_quality
from ai_trading_system.contracts import DataQualityEvidence
from ai_trading_system.data.market_data import FmpPriceProvider, PriceRequest
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_bytes_atomic,
    write_json_atomic,
)
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)

SCHEMA_VERSION = "leveraged_exposure_instrument_evaluation.v2"
SOURCE_PACKAGE_SCHEMA_VERSION = "leveraged_exposure_instrument_panel.v1"
REPORT_TYPE = "leveraged_exposure_instrument_evaluation"
ELIGIBLE_STATUS = "QLD_ELIGIBLE_FOR_OWNER_ACTION_UNIVERSE_REVIEW"
NO_VALUE_STATUS = "QLD_NO_INCREMENTAL_IMPLEMENTATION_VALUE"
MIXED_STATUS = "QLD_MIXED_EVIDENCE_KEEP_RESEARCH_ONLY"
BLOCKED_STATUS = "BLOCKED_DATA_QUALITY_OR_COVERAGE"
_EPSILON = 1.0e-12


def build_instrument_panel_package(
    *,
    policy: Mapping[str, Any],
    existing_prices_path: Path,
    rates_path: Path,
    output_root: Path,
    api_key: str,
    price_provider: Any | None = None,
    downloaded_at: datetime | None = None,
    request_cache_dir: Path | None = None,
    quality_config: DataQualityConfig | None = None,
) -> dict[str, Any]:
    """Build an isolated five-instrument panel without mutating canonical caches."""

    context = _mapping(policy.get("research_context"))
    start = date.fromisoformat(str(context.get("requested_start")))
    end = date.fromisoformat(str(context.get("historical_seen_end")))
    required_tickers = tuple(
        str(item)
        for item in _mapping(policy.get("source_commitments")).get("required_expected_tickers", ())
    )
    if not required_tickers:
        raise ValueError("required_expected_tickers must not be empty")

    existing = pd.read_csv(existing_prices_path, low_memory=False)
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce")
    existing = existing.loc[
        existing["ticker"].isin(set(required_tickers) - {"QLD"})
        & existing["date"].between(pd.Timestamp(start), pd.Timestamp(end))
    ].copy()

    cache_dir = request_cache_dir or output_root / "external_request_cache"
    provider = price_provider or FmpPriceProvider(
        api_key=api_key,
        request_cache_dir=cache_dir,
    )
    qld = provider.download_prices(
        PriceRequest(tickers=["QLD"], start=start, end=end, interval="1d")
    )
    qld["date"] = pd.to_datetime(qld["date"], errors="coerce")
    qld = qld.loc[qld["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
    panel = pd.concat(
        [
            _canonical_price_columns(existing),
            _canonical_price_columns(qld),
        ],
        ignore_index=True,
    )
    panel = panel.sort_values(["date", "ticker"], kind="stable").reset_index(drop=True)

    rates = pd.read_csv(rates_path, low_memory=False)
    rates["date"] = pd.to_datetime(rates["date"], errors="coerce")
    rates = rates.loc[rates["date"].between(pd.Timestamp(start), pd.Timestamp(end))].sort_values(
        ["date", "series"], kind="stable"
    )
    expected_rates = tuple(sorted(str(item) for item in rates["series"].dropna().unique()))

    output_root.mkdir(parents=True, exist_ok=True)
    panel_path = output_root / "instrument_panel.csv"
    rates_snapshot_path = output_root / "rates_snapshot.csv"
    _write_dataframe(panel, panel_path)
    _write_dataframe(rates, rates_snapshot_path)

    quality_report = validate_data_cache(
        prices_path=panel_path,
        rates_path=rates_snapshot_path,
        expected_price_tickers=list(required_tickers),
        expected_rate_series=list(expected_rates),
        quality_config=quality_config or load_data_quality(),
        as_of=end,
    )
    quality_report_path = output_root / "data_quality_report.md"
    write_data_quality_report(quality_report, quality_report_path)
    quality_report_sha256 = sha256_path(quality_report_path)
    evidence = DataQualityEvidence(
        contract_id="isolated_leveraged_exposure_instrument_panel_validation",
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v2",
        status=quality_report.status,
        passed=quality_report.passed,
        checked_at=quality_report.checked_at,
        as_of=end,
        report_path=str(quality_report_path.resolve()),
        report_sha256=quality_report_sha256,
        error_count=quality_report.error_count,
        warning_count=quality_report.warning_count,
        checked_input_count=2,
        blocking_issues=tuple(
            sorted(
                {issue.code for issue in quality_report.issues if issue.severity.value == "ERROR"}
            )
        ),
    )

    downloaded = downloaded_at or datetime.now(UTC)
    source_package = {
        "schema_version": SOURCE_PACKAGE_SCHEMA_VERSION,
        "as_of": end.isoformat(),
        "requested_start": start.isoformat(),
        "historical_seen_end": end.isoformat(),
        "prospective_source_rows_observed": True,
        "prospective_values_used_in_evaluation": False,
        "scoped_data_quality_exception": dict(
            _mapping(policy.get("scoped_data_quality_exception"))
        ),
        "scoped_warning_resolution": dict(_mapping(policy.get("scoped_warning_resolution"))),
        "canonical_full_cache_status": str(
            _mapping(policy.get("scoped_data_quality_exception")).get("global_canonical_dq_status")
        ),
        "canonical_full_cache_pass_claimed": False,
        "panel": _file_record(panel_path, row_count=len(panel)),
        "rates": _file_record(rates_snapshot_path, row_count=len(rates)),
        "data_quality_report": _file_record(quality_report_path),
        "data_quality_evidence": evidence.to_dict(),
        "provider_records": [
            {
                "ticker": "QLD",
                "provider_id": "fmp_eod_daily_prices",
                "provider": "Financial Modeling Prep",
                "endpoint": provider.endpoint_summary(),
                "request_parameters": {
                    "symbol": "QLD",
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                    "interval": "1d",
                },
                "downloaded_at": downloaded.isoformat(),
                "row_count": int(len(qld)),
                "normalized_sha256": _dataframe_sha256(_canonical_price_columns(qld)),
            },
            {
                "ticker": ",".join(sorted(set(required_tickers) - {"QLD"})),
                "provider_id": "existing_canonical_cache_snapshot",
                "provider": "project canonical prices cache",
                "endpoint": str(existing_prices_path.resolve()),
                "request_parameters": {
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                    "filter_applied_before_evaluation": True,
                },
                "downloaded_at": downloaded.isoformat(),
                "row_count": int(len(existing)),
                "source_file_sha256": sha256_path(existing_prices_path),
                "normalized_sha256": _dataframe_sha256(_canonical_price_columns(existing)),
            },
        ],
        "external_request_cache_commitments": _cache_records(cache_dir),
    }
    package_path = output_root / "instrument_panel_package.json"
    write_json_atomic(package_path, source_package)
    return {
        **source_package,
        "package": _file_record(package_path),
    }


class LeveragedExposureInstrumentCalculator:
    plugin_id = "leveraged_exposure_instrument_calculator"
    version = "v2"

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]:
        return build_evaluation_payload(context.sources, as_of=context.as_of)


class LeveragedExposureInstrumentReport:
    plugin_id = "leveraged_exposure_instrument_report"
    version = "v2"

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]:
        if section_id != "instrument_evaluation_summary":
            raise ValueError(f"unknown instrument evaluation section: {section_id}")
        return _mapping(payload.get(section_id))

    def render_markdown(self, payload: Mapping[str, Any]) -> str:
        return render_evaluation_markdown(payload)


def leveraged_exposure_instrument_registry() -> PluginRegistry:
    return PluginRegistry(
        calculators=(LeveragedExposureInstrumentCalculator(),),
        reports=(LeveragedExposureInstrumentReport(),),
    )


def build_evaluation_payload(
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> dict[str, Any]:
    policy = _mapping(sources.get("universe_policy"))
    package = _mapping(sources.get("instrument_panel_package"))
    cost_policy = _mapping(sources.get("transaction_cost_policy"))
    data_quality_policy = _mapping(sources.get("data_quality_policy"))
    role_decision = _role_limited_implementation_decision(policy)
    errors = _source_contract_errors(
        policy,
        package,
        cost_policy,
        data_quality_policy,
        as_of=as_of,
    )
    evidence = _data_quality_evidence(package, errors)
    if evidence is not None and evidence.passed is False:
        errors.extend(evidence.blocking_issues or ("DATA_QUALITY_NOT_PASSED",))
    if errors:
        return _blocked_payload(errors, evidence, role_decision=role_decision)

    assert evidence is not None
    evidence.assert_ready()
    panel_record = _mapping(package.get("panel"))
    panel_path = Path(str(panel_record.get("path")))
    rates_record = _mapping(package.get("rates"))
    rates_path = Path(str(rates_record.get("path")))
    commitment_errors = _source_file_commitment_errors(package)
    if commitment_errors:
        return _blocked_payload(
            commitment_errors,
            evidence,
            role_decision=role_decision,
        )

    panel = pd.read_csv(panel_path, low_memory=False)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    rates = pd.read_csv(rates_path, low_memory=False)
    start = date.fromisoformat(str(_mapping(policy.get("research_context")).get("requested_start")))
    end = date.fromisoformat(
        str(_mapping(policy.get("research_context")).get("historical_seen_end"))
    )
    if panel["date"].notna().any() and panel["date"].max().date() > end:
        return _blocked_payload(
            ("PROSPECTIVE_VALUE_ENTERED_SOURCE_PANEL",),
            evidence,
            role_decision=role_decision,
        )
    if int(panel_record.get("row_count") or -1) != len(panel):
        return _blocked_payload(
            ("INSTRUMENT_PANEL_ROW_COUNT_MISMATCH",),
            evidence,
            role_decision=role_decision,
        )
    if int(rates_record.get("row_count") or -1) != len(rates):
        return _blocked_payload(
            ("RATES_SNAPSHOT_ROW_COUNT_MISMATCH",),
            evidence,
            role_decision=role_decision,
        )

    expected_rates = sorted(str(item) for item in rates["series"].dropna().unique())
    quality_config = DataQualityConfig.model_validate(data_quality_policy)
    fresh_quality = validate_data_cache(
        prices_path=panel_path,
        rates_path=rates_path,
        expected_price_tickers=list(
            _mapping(policy.get("source_commitments")).get("required_expected_tickers", ())
        ),
        expected_rate_series=expected_rates,
        quality_config=quality_config,
        as_of=end,
    )
    quality_errors = _fresh_data_quality_errors(
        fresh_quality,
        evidence=evidence,
        policy=policy,
    )
    if quality_errors:
        return _blocked_payload(
            quality_errors,
            evidence,
            role_decision=role_decision,
        )

    panel = panel.loc[panel["date"].between(pd.Timestamp(start), pd.Timestamp(end))].copy()
    required_tickers = tuple(
        str(item)
        for item in _mapping(policy.get("source_commitments")).get("required_expected_tickers", ())
    )
    prices = (
        panel.loc[panel["ticker"].isin(required_tickers), ["date", "ticker", "adj_close"]]
        .assign(adj_close=lambda frame: pd.to_numeric(frame["adj_close"], errors="coerce"))
        .pivot(index="date", columns="ticker", values="adj_close")
        .sort_index()
        .dropna(subset=list(required_tickers))
    )
    if len(prices) < 2 or tuple(sorted(prices.columns)) != tuple(sorted(required_tickers)):
        return _blocked_payload(
            ("COMMON_PRICE_PANEL_INCOMPLETE",),
            evidence,
            role_decision=role_decision,
        )
    total_cost_bps = _total_cost_bps(cost_policy)
    implementations = _implementation_weights(policy)
    cadences = tuple(str(item) for item in policy.get("rebalance_cadences", ()))
    metric_conventions = _mapping(policy.get("metric_conventions"))
    annualization = int(metric_conventions.get("annualization_sessions", 252))
    slice_frames = _evaluation_slices(prices)
    slice_results: dict[str, Any] = {}
    for slice_id, frame in slice_frames.items():
        slice_results[slice_id] = {
            cadence: {
                implementation_id: _simulate(
                    frame,
                    weights,
                    cadence=cadence,
                    total_cost_bps=total_cost_bps,
                    annualization=annualization,
                )
                for implementation_id, weights in implementations.items()
            }
            for cadence in cadences
        }

    full = _mapping(slice_results.get("full_primary"))
    pareto = _pareto_decision(
        full,
        cadences=cadences,
        decision_policy=_mapping(policy.get("decision_mapping")),
    )
    status = str(pareto["status"])
    tracking = _tracking_diagnostics(prices, annualization=annualization)
    benchmarks = {
        ticker: _simulate(
            prices[[ticker]],
            {ticker: 1.0},
            cadence="buy_and_hold",
            total_cost_bps=total_cost_bps,
            annualization=annualization,
        )
        for ticker in ("SPY", "QQQ")
    }
    evaluation = {
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "evaluated_range": {
            "start": prices.index.min().date().isoformat(),
            "end": prices.index.max().date().isoformat(),
            "common_price_sessions": int(len(prices)),
            "return_sessions": int(len(prices) - 1),
        },
        "cost_policy": {
            "policy_id": cost_policy.get("policy_id"),
            "status": cost_policy.get("status"),
            "total_cost_bps": total_cost_bps,
        },
        "benchmarks": benchmarks,
        "tracking_diagnostics": tracking,
        "slice_results": slice_results,
        "pareto_decision": pareto,
        "owner_role_decision": role_decision,
        "source_package_commitment": {
            "schema_version": package.get("schema_version"),
            "panel_sha256": panel_record.get("sha256"),
            "data_quality_evidence_id": evidence.evidence_id,
            "canonical_full_cache_status": package.get("canonical_full_cache_status"),
            "scoped_data_quality_exception_approval_id": _mapping(
                package.get("scoped_data_quality_exception")
            ).get("approval_id"),
        },
    }
    commitment = hashlib.sha256(canonical_json_bytes(evaluation)).hexdigest()
    summary = _summary(status, evaluation, role_decision=role_decision)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": "TRADING-2459",
        "status": status,
        "data_quality_evidence": evidence.to_dict(),
        "evaluation": evaluation,
        "evaluation_commitment_sha256": commitment,
        "instrument_evaluation_summary": summary,
        "strict_validation_errors": [],
        "validation_status": "PASS",
        "canonical_full_cache_status": package.get("canonical_full_cache_status"),
        "canonical_full_cache_pass_claimed": False,
        "scoped_data_quality_status": evidence.status,
        "manual_review_required": True,
        "research_only": True,
        "official_action_universe_changed": False,
        "official_primary_action_universe_changed": False,
        "role_limited_implementation_universe_changed": True,
        "qld_role_limited_2x_implementation_approved": True,
        "qld_automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "next_route": _next_route(status, role_decision=role_decision),
    }


def validate_evaluation_payload(
    payload: Mapping[str, Any],
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> tuple[str, ...]:
    rebuilt = build_evaluation_payload(sources, as_of=as_of)
    errors: list[str] = []
    if payload.get("evaluation_commitment_sha256") != rebuilt.get("evaluation_commitment_sha256"):
        errors.append("EVALUATION_COMMITMENT_MISMATCH")
    if payload.get("evaluation") != rebuilt.get("evaluation"):
        errors.append("EVALUATION_CONTENT_MISMATCH")
    if payload.get("status") != rebuilt.get("status"):
        errors.append("EVALUATION_STATUS_MISMATCH")
    if render_evaluation_markdown(payload) != render_evaluation_markdown(rebuilt):
        errors.append("EVALUATION_MARKDOWN_MISMATCH")
    return tuple(errors)


def instrument_evaluation_summary(
    payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    return _mapping(payload.get("instrument_evaluation_summary"))


def leveraged_exposure_instrument_view_model(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return dict(instrument_evaluation_summary(payload))


def render_evaluation_markdown(payload: Mapping[str, Any]) -> str:
    status = str(payload.get("status") or "")
    summary = _mapping(payload.get("instrument_evaluation_summary"))
    evidence = _mapping(payload.get("data_quality_evidence"))
    role_decision = _mapping(summary.get("owner_role_decision"))
    if status == BLOCKED_STATUS:
        blockers = ", ".join(str(item) for item in payload.get("strict_validation_errors", ()))
        return "\n".join(
            [
                "# QLD 增量工具价值评估",
                "",
                f"- 结论：`{status}`",
                f"- 数据质量：`{evidence.get('status')}`",
                f"- 阻塞项：{blockers or '未知'}",
                "",
                "评估已 fail closed；既有角色批准不能绕过数据质量、趋势、风险或暴露门。",
                "",
            ]
        )

    cadence_rows = summary.get("cadence_rows", ())
    lines = [
        "# QLD 增量工具价值评估",
        "",
        f"- 机械结论：`{status}`",
        f"- 数据质量：`{evidence.get('status')}`",
        f"- Canonical full-cache data quality：`{payload.get('canonical_full_cache_status')}`",
        "- 本报告采用 Owner 批准的 scoped 五资产 DQ；不声称 canonical full-cache PASS。",
        f"- 评估区间：`{summary.get('evaluated_start')}` 至 `{summary.get('evaluated_end')}`",
        f"- 共同交易日：{summary.get('common_price_sessions')}",
        "- SPY 角色：立即进入 reference / benchmark / regime-control，不进入当前权重动作空间。",
        "- QLD 角色：Owner 已批准为 role-limited 2x execution / implementation instrument。",
        f"- 角色决策：`{role_decision.get('approval_id')}`",
        "- 自动执行：未批准；本报告不生成或修改正式 target weights。",
        "",
        "## Full-primary cadence 判定",
        "",
        "| Cadence | QLD Pareto non-dominated | 严格优于两种 comparator 的 objective | 被谁支配 |",
        "|---|---:|---|---|",
    ]
    for row in cadence_rows if isinstance(cadence_rows, Sequence) else ():
        item = _mapping(row)
        lines.append(
            "| "
            f"{item.get('cadence')} | "
            f"{'是' if item.get('qld_non_dominated') else '否'} | "
            f"{', '.join(str(value) for value in item.get('strict_advantages', ())) or '-'} | "
            f"{', '.join(str(value) for value in item.get('dominated_by', ())) or '-'} |"
        )
    lines.extend(
        [
            "",
            "## 关键指标（full-primary）",
            "",
            "```json",
            json.dumps(summary.get("full_primary_metrics"), ensure_ascii=False, indent=2),
            "```",
            "",
            "## Tracking diagnostics",
            "",
            "```json",
            json.dumps(summary.get("tracking_diagnostics"), ensure_ascii=False, indent=2),
            "```",
            "",
            "## 解释边界",
            "",
            "- 这是 historical-seen instrument implementation diagnostic，"
            "不是新策略搜索，也不是 unbiased OOS。",
            "- 2026-07-22 之后的共享源行曾在审计终端中可见，但未进入本次计算。",
            "- 独立趋势模型必须先确认可信 Nasdaq-100 上升趋势，组合层必须先形成接近 2x 的"
            " QQQ-equivalent target，且风险门必须通过；QLD 不参与这些上游判断。",
            "- QLD 不得作为 trend signal、独立 strategy style、自由 candidate dimension，"
            "也不得按本次历史收益动态切换工具。",
            "- “接近 2x”的数值容差、执行 selector、forward shadow 验收和退出规则尚未治理；"
            "在这些政策完成前，automatic instrument selection、paper-shadow、production 和"
            " broker action 均保持关闭。",
            "",
        ]
    )
    return "\n".join(lines)


def _source_contract_errors(
    policy: Mapping[str, Any],
    package: Mapping[str, Any],
    cost_policy: Mapping[str, Any],
    data_quality_policy: Mapping[str, Any],
    *,
    as_of: date,
) -> list[str]:
    errors: list[str] = []
    if policy.get("schema_version") != "strategy_style_discovery_universe_policy.v1":
        errors.append("UNIVERSE_POLICY_SCHEMA_INVALID")
    if package.get("schema_version") != SOURCE_PACKAGE_SCHEMA_VERSION:
        errors.append("SOURCE_PACKAGE_SCHEMA_INVALID")
    if cost_policy.get("policy_id") != "transaction_cost_model_v1":
        errors.append("TRANSACTION_COST_POLICY_INVALID")
    try:
        DataQualityConfig.model_validate(data_quality_policy)
    except ValueError:
        errors.append("DATA_QUALITY_POLICY_INVALID")
    if str(package.get("as_of")) != as_of.isoformat():
        errors.append("SOURCE_PACKAGE_AS_OF_MISMATCH")
    context = _mapping(policy.get("research_context"))
    if package.get("requested_start") != context.get("requested_start"):
        errors.append("SOURCE_PACKAGE_START_MISMATCH")
    if package.get("historical_seen_end") != context.get("historical_seen_end"):
        errors.append("SOURCE_PACKAGE_HISTORICAL_END_MISMATCH")
    if _mapping(policy.get("safety")).get("official_action_universe_changed") is not False:
        errors.append("OFFICIAL_ACTION_UNIVERSE_BOUNDARY_INVALID")
    universes = _mapping(policy.get("universes"))
    role_policy = _mapping(policy.get("role_limited_2x_implementation_policy"))
    required_true = (
        "independent_trend_model_required",
        "trusted_nasdaq_uptrend_required",
        "portfolio_target_qqq_equivalent_exposure_near_2x_required",
        "risk_gate_pass_required",
        "upstream_target_exposure_must_preexist_instrument_selection",
        "forward_shadow_required_before_automated_use",
    )
    required_false = (
        "historical_return_ranking_switch_allowed",
        "qld_as_trend_signal_allowed",
        "qld_as_independent_strategy_style_allowed",
        "qld_as_free_candidate_dimension_allowed",
        "automatic_exposure_increase_allowed",
        "automatic_instrument_selection_allowed",
        "official_target_weight_mutation_allowed",
        "paper_shadow_change_allowed",
        "production_use_allowed",
        "broker_action_allowed",
        "numeric_near_2x_tolerance_defined",
        "executable_selector_policy_defined",
    )
    if (
        policy.get("status") != "OWNER_APPROVED_ROLE_LIMITED_2X_IMPLEMENTATION_INSTRUMENT"
        or universes.get("role_limited_2x_implementation_instrument") != ["QLD"]
        or universes.get("qld_signal_input_allowed") is not False
        or universes.get("qld_independent_style_allowed") is not False
        or universes.get("qld_free_candidate_dimension_allowed") is not False
        or universes.get("role_limited_implementation_universe_change_approved") is not True
        or role_policy.get("approval_id")
        != (
            "owner_decision:TRADING-2459:2026-07-25:"
            "approve_qld_role_limited_2x_implementation_instrument"
        )
        or role_policy.get("status") != "OWNER_APPROVED_ROLE_LIMITED_MANUAL_CONSIDERATION"
        or role_policy.get("instrument") != "QLD"
        or role_policy.get("instrument_role") != "execution_implementation_only"
        or any(role_policy.get(field) is not True for field in required_true)
        or any(role_policy.get(field) is not False for field in required_false)
    ):
        errors.append("ROLE_LIMITED_2X_IMPLEMENTATION_POLICY_INVALID")
    if package.get("prospective_values_used_in_evaluation") is not False:
        errors.append("PROSPECTIVE_VALUE_USE_NOT_FORBIDDEN")
    exception = _mapping(package.get("scoped_data_quality_exception"))
    if not str(exception.get("approval_id") or "").startswith("owner_decision:TRADING-2459:"):
        errors.append("SCOPED_DATA_QUALITY_EXCEPTION_NOT_APPROVED")
    if package.get("canonical_full_cache_pass_claimed") is not False:
        errors.append("CANONICAL_FULL_CACHE_PASS_MISREPRESENTED")
    if _mapping(package.get("scoped_warning_resolution")).get("status") != (
        "OWNER_REVIEWED_KNOWN_CORPORATE_ACTIONS"
    ):
        errors.append("SCOPED_DATA_QUALITY_WARNING_NOT_REVIEWED")
    return errors


def _source_file_commitment_errors(package: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for record_id in ("panel", "rates", "data_quality_report"):
        record = _mapping(package.get(record_id))
        path_text = str(record.get("path") or "")
        if not path_text:
            errors.append(f"{record_id.upper()}_PATH_MISSING")
            continue
        path = Path(path_text)
        if not path.is_file():
            errors.append(f"{record_id.upper()}_FILE_MISSING")
            continue
        if sha256_path(path) != str(record.get("sha256") or ""):
            errors.append(f"{record_id.upper()}_SHA256_MISMATCH")
        if path.stat().st_size != int(record.get("size_bytes") or -1):
            errors.append(f"{record_id.upper()}_SIZE_MISMATCH")

    evidence = _mapping(package.get("data_quality_evidence"))
    report_record = _mapping(package.get("data_quality_report"))
    evidence_report_path = str(evidence.get("report_path") or "")
    record_report_path = str(report_record.get("path") or "")
    if (
        not evidence_report_path
        or Path(evidence_report_path).resolve() != Path(record_report_path).resolve()
    ):
        errors.append("DATA_QUALITY_REPORT_PATH_MISMATCH")
    if evidence.get("report_sha256") != report_record.get("sha256"):
        errors.append("DATA_QUALITY_REPORT_SHA256_MISMATCH")

    commitments = package.get("external_request_cache_commitments")
    if not isinstance(commitments, Sequence) or isinstance(commitments, (str, bytes)):
        errors.append("EXTERNAL_REQUEST_CACHE_COMMITMENTS_MISSING")
    elif not commitments:
        errors.append("EXTERNAL_REQUEST_CACHE_COMMITMENTS_EMPTY")
    else:
        for index, raw in enumerate(commitments):
            record = _mapping(raw)
            path_text = str(record.get("path") or "")
            cache_path = Path(path_text) if path_text else None
            if cache_path is None or not cache_path.is_file():
                errors.append(f"EXTERNAL_REQUEST_CACHE_FILE_MISSING:{index}")
                continue
            if sha256_path(cache_path) != str(record.get("sha256") or ""):
                errors.append(f"EXTERNAL_REQUEST_CACHE_SHA256_MISMATCH:{index}")
            if cache_path.stat().st_size != int(record.get("size_bytes") or -1):
                errors.append(f"EXTERNAL_REQUEST_CACHE_SIZE_MISMATCH:{index}")
    return errors


def _fresh_data_quality_errors(
    report: Any,
    *,
    evidence: DataQualityEvidence,
    policy: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    if not report.passed:
        errors.extend(
            str(issue.code) for issue in report.issues if str(issue.severity.value) == "ERROR"
        )
        errors.append("SCOPED_DATA_QUALITY_REVALIDATION_FAILED")
    if (
        report.status != evidence.status
        or report.error_count != evidence.error_count
        or report.warning_count != evidence.warning_count
    ):
        errors.append("SCOPED_DATA_QUALITY_EVIDENCE_DRIFT")

    reviewed_warning_code = str(
        _mapping(policy.get("scoped_warning_resolution")).get("warning_code") or ""
    )
    warning_codes = {
        str(issue.code) for issue in report.issues if str(issue.severity.value) == "WARNING"
    }
    unexpected = warning_codes - ({reviewed_warning_code} if reviewed_warning_code else set())
    errors.extend(f"SCOPED_DATA_QUALITY_UNREVIEWED_WARNING:{code}" for code in sorted(unexpected))
    return sorted(set(errors))


def _data_quality_evidence(
    package: Mapping[str, Any],
    errors: list[str],
) -> DataQualityEvidence | None:
    raw = package.get("data_quality_evidence")
    if not isinstance(raw, Mapping):
        errors.append("DATA_QUALITY_EVIDENCE_MISSING")
        return None
    try:
        return DataQualityEvidence.from_dict(raw)
    except ValueError:
        errors.append("DATA_QUALITY_EVIDENCE_INVALID")
        return None


def _blocked_payload(
    errors: Sequence[str],
    evidence: DataQualityEvidence | None,
    *,
    role_decision: Mapping[str, Any],
) -> dict[str, Any]:
    role_approved = bool(role_decision.get("role_limited_2x_implementation_approved"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": "TRADING-2459",
        "status": BLOCKED_STATUS,
        "data_quality_evidence": None if evidence is None else evidence.to_dict(),
        "evaluation": {},
        "evaluation_commitment_sha256": None,
        "instrument_evaluation_summary": {
            "schema_version": "leveraged_exposure_instrument_evaluation_summary.v2",
            "status": BLOCKED_STATUS,
            "blockers": sorted(set(str(item) for item in errors)),
            "owner_role_decision": dict(role_decision),
        },
        "strict_validation_errors": sorted(set(str(item) for item in errors)),
        "validation_status": "BLOCKED",
        "canonical_full_cache_status": "FAIL",
        "canonical_full_cache_pass_claimed": False,
        "scoped_data_quality_status": (None if evidence is None else evidence.status),
        "manual_review_required": True,
        "research_only": True,
        "official_action_universe_changed": False,
        "official_primary_action_universe_changed": False,
        "role_limited_implementation_universe_changed": role_approved,
        "qld_role_limited_2x_implementation_approved": role_approved,
        "qld_automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "next_route": "repair_data_quality_or_coverage_then_rerun",
    }


def _canonical_price_columns(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("date", "ticker", "open", "high", "low", "close", "adj_close", "volume")
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"price frame missing columns: {','.join(missing)}")
    result = frame.loc[:, list(required)].copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return result


def _write_dataframe(frame: pd.DataFrame, path: Path) -> None:
    content = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    write_bytes_atomic(path, content)


def _dataframe_sha256(frame: pd.DataFrame) -> str:
    content = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _file_record(path: Path, *, row_count: int | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(path.resolve()),
        "sha256": sha256_path(path),
        "size_bytes": path.stat().st_size,
    }
    if row_count is not None:
        record["row_count"] = int(row_count)
    return record


def _cache_records(cache_dir: Path) -> list[dict[str, Any]]:
    if not cache_dir.exists():
        return []
    return [_file_record(path) for path in sorted(cache_dir.rglob("*")) if path.is_file()]


def _total_cost_bps(cost_policy: Mapping[str, Any]) -> float:
    components = _mapping(cost_policy.get("cost_components_bps"))
    total = sum(float(value) for value in components.values())
    if not math.isfinite(total) or total < 0:
        raise ValueError("transaction cost total must be finite and non-negative")
    return total


def _implementation_weights(policy: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for raw in policy.get("fixed_implementations", ()):
        item = _mapping(raw)
        implementation_id = str(item.get("implementation_id"))
        weights = {
            str(ticker): float(weight) for ticker, weight in _mapping(item.get("weights")).items()
        }
        total = sum(weights.values())
        if not math.isclose(total, 1.0, rel_tol=0.0, abs_tol=_EPSILON):
            raise ValueError(f"implementation weights do not sum to one: {implementation_id}")
        result[implementation_id] = {ticker: weight / total for ticker, weight in weights.items()}
    return result


def _evaluation_slices(prices: pd.DataFrame) -> dict[str, pd.DataFrame]:
    slices: dict[str, pd.DataFrame] = {
        "full_primary": prices,
        "pre_ai_cycle_comparison": pd.DataFrame(
            prices.loc[prices.index <= pd.Timestamp("2022-11-30"), :]
        ),
        "ai_cycle_comparison": pd.DataFrame(
            prices.loc[prices.index >= pd.Timestamp("2022-12-01"), :]
        ),
    }
    date_index = pd.DatetimeIndex(prices.index)
    for year in sorted(set(int(item) for item in date_index.year)):
        slices[f"calendar_year_{year}"] = pd.DataFrame(prices.loc[date_index.year == year, :])
    return {key: value for key, value in slices.items() if len(value) >= 2}


def _simulate(
    prices: pd.DataFrame,
    weights: Mapping[str, float],
    *,
    cadence: str,
    total_cost_bps: float,
    annualization: int,
) -> dict[str, Any]:
    returns = prices.loc[:, list(weights)].pct_change(fill_method=None).dropna(how="any")
    target = np.array([weights[ticker] for ticker in weights], dtype=float)
    current = target.copy()
    gross_nav = 1.0
    net_nav = 1.0 - total_cost_bps / 10_000.0
    turnover = 1.0
    nav_values = [1.0]
    dates = list(returns.index)
    for index, (_, row) in enumerate(returns.iterrows()):
        asset_returns = row.to_numpy(dtype=float)
        portfolio_multiplier = 1.0 + float(np.dot(current, asset_returns))
        if portfolio_multiplier <= 0 or not math.isfinite(portfolio_multiplier):
            raise ValueError("portfolio NAV became non-positive or non-finite")
        gross_nav *= portfolio_multiplier
        net_nav *= portfolio_multiplier
        drifted = current * (1.0 + asset_returns) / portfolio_multiplier
        if _rebalance_after(index, dates, cadence):
            traded = 0.5 * float(np.abs(target - drifted).sum())
            turnover += traded
            net_nav *= 1.0 - traded * total_cost_bps / 10_000.0
            current = target.copy()
        else:
            current = drifted
        nav_values.append(net_nav)
    nav = pd.Series(nav_values, index=[prices.index[0], *dates], dtype=float)
    daily_returns = nav.pct_change(fill_method=None).dropna()
    years = max((prices.index[-1] - prices.index[0]).days / 365.25, 1.0 / 365.25)
    total_return = float(nav.iloc[-1] - 1.0)
    cagr = float(nav.iloc[-1] ** (1.0 / years) - 1.0)
    annualized_volatility = float(daily_returns.std(ddof=1) * math.sqrt(annualization))
    mean = float(daily_returns.mean())
    standard_deviation = float(daily_returns.std(ddof=1))
    sharpe = (
        float(mean / standard_deviation * math.sqrt(annualization))
        if standard_deviation > 0
        else 0.0
    )
    drawdown = nav / nav.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else 0.0
    return {
        "terminal_value_net": float(nav.iloc[-1]),
        "terminal_value_gross": float(gross_nav),
        "total_return": total_return,
        "cagr": cagr,
        "annualized_volatility": annualized_volatility,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "calmar": calmar,
        "worst_1d_loss": _worst_compounded_return(daily_returns, 1),
        "worst_5d_loss": _worst_compounded_return(daily_returns, 5),
        "worst_20d_loss": _worst_compounded_return(daily_returns, 20),
        "external_turnover": float(turnover),
        "cost_drag": float(gross_nav - nav.iloc[-1]),
        "price_sessions": int(len(prices)),
        "return_sessions": int(len(returns)),
    }


def _rebalance_after(index: int, dates: list[pd.Timestamp], cadence: str) -> bool:
    if index >= len(dates) - 1 or cadence == "buy_and_hold":
        return False
    current = dates[index]
    following = dates[index + 1]
    if cadence == "daily":
        return True
    if cadence == "weekly":
        return current.isocalendar()[:2] != following.isocalendar()[:2]
    if cadence == "monthly":
        return (current.year, current.month) != (following.year, following.month)
    raise ValueError(f"unknown rebalance cadence: {cadence}")


def _worst_compounded_return(returns: pd.Series, sessions: int) -> float:
    return_values = returns.to_numpy(dtype=float)
    if len(returns) < sessions:
        return float(np.prod(1.0 + return_values) - 1.0)
    values = pd.Series(1.0 + return_values).rolling(sessions).apply(np.prod, raw=True) - 1.0
    return float(np.nanmin(values.to_numpy(dtype=float)))


def _tracking_diagnostics(
    prices: pd.DataFrame,
    *,
    annualization: int,
) -> dict[str, Any]:
    returns = prices[["QQQ", "QLD", "TQQQ"]].pct_change(fill_method=None).dropna()
    qqq = returns["QQQ"]
    diagnostics: dict[str, Any] = {}
    for ticker, multiplier in (("QLD", 2.0), ("TQQQ", 3.0)):
        residual = returns[ticker] - multiplier * qqq
        variance = float(qqq.var(ddof=1))
        diagnostics[ticker] = {
            "target_daily_multiplier": multiplier,
            "residual_bias_daily": float(residual.mean()),
            "residual_bias_annualized": float(residual.mean() * annualization),
            "residual_mae": float(residual.abs().mean()),
            "residual_rmse": float(math.sqrt(float((residual**2).mean()))),
            "correlation": float(returns[ticker].corr(qqq)),
            "realized_beta": (float(returns[ticker].cov(qqq) / variance) if variance > 0 else 0.0),
        }
    return diagnostics


def _pareto_decision(
    full_results: Mapping[str, Any],
    *,
    cadences: Sequence[str],
    decision_policy: Mapping[str, Any],
) -> dict[str, Any]:
    objectives = _mapping(decision_policy.get("pareto_objectives"))
    maximize = tuple(str(item) for item in objectives.get("maximize", ()))
    minimize = tuple(str(item) for item in objectives.get("minimize", ()))
    qld_id = "qld_100"
    comparator_ids = ("qqq_50_tqqq_50", "sgov_33_tqqq_67")
    rows: list[dict[str, Any]] = []
    for cadence in cadences:
        results = _mapping(full_results.get(cadence))
        qld = _mapping(results.get(qld_id))
        dominated_by = [
            comparator
            for comparator in comparator_ids
            if _dominates(
                _mapping(results.get(comparator)),
                qld,
                maximize=maximize,
                minimize=minimize,
            )
        ]
        strict_advantages = [
            objective
            for objective in (*maximize, *minimize)
            if _strictly_beats_both(
                qld,
                tuple(_mapping(results.get(item)) for item in comparator_ids),
                objective=objective,
                maximize=objective in maximize,
            )
        ]
        rows.append(
            {
                "cadence": cadence,
                "qld_non_dominated": not dominated_by,
                "dominated_by": dominated_by,
                "strict_advantages": strict_advantages,
                "eligible_cadence": not dominated_by and bool(strict_advantages),
            }
        )
    if any(row["eligible_cadence"] for row in rows):
        status = str(decision_policy.get("eligible_status") or ELIGIBLE_STATUS)
    elif rows and all(row["dominated_by"] for row in rows):
        status = str(decision_policy.get("no_value_status") or NO_VALUE_STATUS)
    else:
        status = str(decision_policy.get("mixed_status") or MIXED_STATUS)
    return {
        "status": status,
        "cadence_rows": rows,
        "maximize_objectives": list(maximize),
        "minimize_objectives": list(minimize),
        "floating_point_epsilon": _EPSILON,
    }


def _dominates(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    maximize: Sequence[str],
    minimize: Sequence[str],
) -> bool:
    no_worse = True
    strictly_better = False
    for objective in maximize:
        left_value = float(left[objective])
        right_value = float(right[objective])
        no_worse &= left_value >= right_value - _EPSILON
        strictly_better |= left_value > right_value + _EPSILON
    for objective in minimize:
        left_value = float(left[objective])
        right_value = float(right[objective])
        no_worse &= left_value <= right_value + _EPSILON
        strictly_better |= left_value < right_value - _EPSILON
    return bool(no_worse and strictly_better)


def _strictly_beats_both(
    candidate: Mapping[str, Any],
    comparators: Sequence[Mapping[str, Any]],
    *,
    objective: str,
    maximize: bool,
) -> bool:
    candidate_value = float(candidate[objective])
    if maximize:
        return all(
            candidate_value > float(comparator[objective]) + _EPSILON for comparator in comparators
        )
    return all(
        candidate_value < float(comparator[objective]) - _EPSILON for comparator in comparators
    )


def _role_limited_implementation_decision(
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    role = _mapping(policy.get("role_limited_2x_implementation_policy"))
    approval_id = str(role.get("approval_id") or "")
    role_approved = (
        approval_id
        == (
            "owner_decision:TRADING-2459:2026-07-25:"
            "approve_qld_role_limited_2x_implementation_instrument"
        )
        and role.get("status") == "OWNER_APPROVED_ROLE_LIMITED_MANUAL_CONSIDERATION"
    )
    return {
        "approval_id": approval_id,
        "status": role.get("status"),
        "instrument": role.get("instrument"),
        "instrument_role": role.get("instrument_role"),
        "role_limited_2x_implementation_approved": role_approved,
        "independent_trend_model_required": role.get("independent_trend_model_required"),
        "trusted_nasdaq_uptrend_required": role.get("trusted_nasdaq_uptrend_required"),
        "portfolio_target_qqq_equivalent_exposure_near_2x_required": role.get(
            "portfolio_target_qqq_equivalent_exposure_near_2x_required"
        ),
        "risk_gate_pass_required": role.get("risk_gate_pass_required"),
        "upstream_target_exposure_must_preexist_instrument_selection": role.get(
            "upstream_target_exposure_must_preexist_instrument_selection"
        ),
        "allowed_comparison_dimensions": list(role.get("allowed_comparison_dimensions", ())),
        "historical_return_ranking_switch_allowed": role.get(
            "historical_return_ranking_switch_allowed"
        ),
        "qld_as_trend_signal_allowed": role.get("qld_as_trend_signal_allowed"),
        "qld_as_independent_strategy_style_allowed": role.get(
            "qld_as_independent_strategy_style_allowed"
        ),
        "qld_as_free_candidate_dimension_allowed": role.get(
            "qld_as_free_candidate_dimension_allowed"
        ),
        "automatic_exposure_increase_allowed": role.get("automatic_exposure_increase_allowed"),
        "automatic_instrument_selection_allowed": role.get(
            "automatic_instrument_selection_allowed"
        ),
        "official_target_weight_mutation_allowed": role.get(
            "official_target_weight_mutation_allowed"
        ),
        "paper_shadow_change_allowed": role.get("paper_shadow_change_allowed"),
        "production_use_allowed": role.get("production_use_allowed"),
        "broker_action_allowed": role.get("broker_action_allowed"),
        "numeric_near_2x_tolerance_defined": role.get("numeric_near_2x_tolerance_defined"),
        "executable_selector_policy_defined": role.get("executable_selector_policy_defined"),
        "forward_shadow_required_before_automated_use": role.get(
            "forward_shadow_required_before_automated_use"
        ),
        "unresolved_policy_dependencies": list(role.get("unresolved_policy_dependencies", ())),
    }


def _summary(
    status: str,
    evaluation: Mapping[str, Any],
    *,
    role_decision: Mapping[str, Any],
) -> dict[str, Any]:
    evaluated = _mapping(evaluation.get("evaluated_range"))
    full = _mapping(_mapping(evaluation.get("slice_results")).get("full_primary"))
    compact_metrics = {
        cadence: {
            implementation: {
                metric: _mapping(values).get(metric)
                for metric in (
                    "total_return",
                    "cagr",
                    "annualized_volatility",
                    "max_drawdown",
                    "sharpe",
                    "worst_5d_loss",
                    "worst_20d_loss",
                    "external_turnover",
                    "cost_drag",
                )
            }
            for implementation, values in _mapping(results).items()
        }
        for cadence, results in full.items()
    }
    decision = _mapping(evaluation.get("pareto_decision"))
    return {
        "schema_version": "leveraged_exposure_instrument_evaluation_summary.v2",
        "status": status,
        "evaluated_start": evaluated.get("start"),
        "evaluated_end": evaluated.get("end"),
        "common_price_sessions": evaluated.get("common_price_sessions"),
        "cadence_rows": decision.get("cadence_rows"),
        "full_primary_metrics": compact_metrics,
        "tracking_diagnostics": evaluation.get("tracking_diagnostics"),
        "owner_decision_required": False,
        "owner_role_decision_recorded": True,
        "additional_owner_decision_required_for_automated_or_production_use": True,
        "owner_role_decision": dict(role_decision),
        "official_action_universe_changed": False,
        "official_primary_action_universe_changed": False,
        "role_limited_implementation_universe_changed": True,
        "canonical_full_cache_status": evaluation.get("source_package_commitment", {}).get(
            "canonical_full_cache_status"
        ),
    }


def _next_route(
    status: str,
    *,
    role_decision: Mapping[str, Any],
) -> str:
    role_approved = bool(role_decision.get("role_limited_2x_implementation_approved"))
    if status == ELIGIBLE_STATUS and role_approved:
        return "govern_forward_shadow_and_non_automatic_implementation_selector"
    if status == ELIGIBLE_STATUS:
        return "owner_reviews_qld_for_separate_action_universe_decision"
    if status == NO_VALUE_STATUS:
        return "owner_reopens_role_decision_after_no_value_rerun"
    return "keep_qld_research_only_and_define_any_followup_before_new_data"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "BLOCKED_STATUS",
    "ELIGIBLE_STATUS",
    "MIXED_STATUS",
    "NO_VALUE_STATUS",
    "LeveragedExposureInstrumentCalculator",
    "LeveragedExposureInstrumentReport",
    "build_evaluation_payload",
    "build_instrument_panel_package",
    "instrument_evaluation_summary",
    "leveraged_exposure_instrument_registry",
    "leveraged_exposure_instrument_view_model",
    "render_evaluation_markdown",
    "validate_evaluation_payload",
]
