from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import DataQualityConfig, load_data_quality
from ai_trading_system.contracts import (
    CapabilityQualityBinding,
    ConsumerDataCapabilityReceipt,
    DataQualityEvidence,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.data.quality_capability import (
    build_consumer_data_capability,
    load_reviewed_consumer_data_capability_policy,
    verify_consumer_data_capability_receipt,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_json_atomic,
)
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)

SCHEMA_VERSION = "decision_target_capability_audit_label_foundation.v1"
SOURCE_PACKAGE_SCHEMA_VERSION = "decision_target_market_panel.v1"
SUMMARY_SCHEMA_VERSION = "decision_target_capability_audit_label_foundation_summary.v1"
REPORT_TYPE = "decision_target_capability_audit_label_foundation"
READY_STATUS = "LABEL_FOUNDATION_READY"
BLOCKED_STATUS = "BLOCKED_DATA_QUALITY_OR_SOURCE"

# This only absorbs deterministic IEEE-754 subtraction noise; it is not an
# investment-facing acceptance threshold.
_FLOAT_IDENTITY_EPSILON = 1.0e-12
_REQUIRED_TICKERS = ("QQQ", "SPY", "SGOV")


def build_decision_target_source_package(
    *,
    policy: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    output_root: Path,
    as_of: date,
    expected_price_tickers: Sequence[str],
    expected_rate_series: Sequence[str],
    manifest_path: Path | None = None,
    backtest_manifest_path: Path | None = None,
    secondary_prices_path: Path | None = None,
    require_secondary_prices: bool = False,
    quality_config: DataQualityConfig | None = None,
    captured_at: datetime | None = None,
    capability_policy_path: Path | None = None,
    data_quality_policy_path: Path | None = None,
) -> dict[str, Any]:
    """Run canonical DQ first and only materialize a research panel after PASS."""

    if policy.get("schema_version") == "decision_target_capability_audit_policy.v2":
        if capability_policy_path is None or data_quality_policy_path is None:
            raise ValueError("v2 source package requires capability and data-quality policy paths")
        return _build_capability_decision_target_source_package(
            policy=policy,
            prices_path=prices_path,
            rates_path=rates_path,
            output_root=output_root,
            as_of=as_of,
            expected_price_tickers=expected_price_tickers,
            expected_rate_series=expected_rate_series,
            manifest_path=manifest_path,
            backtest_manifest_path=backtest_manifest_path,
            secondary_prices_path=secondary_prices_path,
            require_secondary_prices=require_secondary_prices,
            quality_config=quality_config,
            captured_at=captured_at,
            capability_policy_path=capability_policy_path,
            data_quality_policy_path=data_quality_policy_path,
        )

    config = quality_config or load_data_quality()
    start = date.fromisoformat(str(_mapping(policy.get("research_context")).get("requested_start")))
    output_root.mkdir(parents=True, exist_ok=True)
    canonical_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=[str(item) for item in expected_price_tickers],
        expected_rate_series=[str(item) for item in expected_rate_series],
        quality_config=config,
        as_of=as_of,
        manifest_path=manifest_path,
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=secondary_prices_path,
        require_secondary_prices=require_secondary_prices,
        requested_window=((start, as_of) if manifest_path is not None else None),
    )
    canonical_report_path = output_root / "canonical_data_quality_report.md"
    write_data_quality_report(canonical_report, canonical_report_path)
    canonical_evidence = _quality_evidence(
        canonical_report,
        report_path=canonical_report_path,
        as_of=as_of,
        contract_id="canonical_decision_target_source_validation",
        checked_input_count=(
            2
            + int(manifest_path is not None)
            + int(backtest_manifest_path is not None)
            + int(secondary_prices_path is not None)
        ),
    )
    observed_at = _aware_utc(captured_at or datetime.now(UTC))
    package: dict[str, Any] = {
        "schema_version": SOURCE_PACKAGE_SCHEMA_VERSION,
        "as_of": as_of.isoformat(),
        "requested_start": start.isoformat(),
        "canonical_strict_validation_required": True,
        "scoped_data_quality_exception_used": False,
        "panel_materialized": False,
        "canonical_prices": _file_record(prices_path),
        "canonical_rates": _file_record(rates_path),
        "canonical_manifest": (
            _file_record(manifest_path)
            if manifest_path is not None and manifest_path.is_file()
            else None
        ),
        "canonical_backtest_manifest": (
            _file_record(backtest_manifest_path)
            if backtest_manifest_path is not None and backtest_manifest_path.is_file()
            else None
        ),
        "canonical_secondary_prices": (
            _file_record(secondary_prices_path)
            if secondary_prices_path is not None and secondary_prices_path.is_file()
            else None
        ),
        "canonical_data_quality_report": _file_record(canonical_report_path),
        "canonical_data_quality_evidence": canonical_evidence.to_dict(),
        "data_quality_report": _file_record(canonical_report_path),
        "data_quality_evidence": canonical_evidence.to_dict(),
        "panel": None,
        "rates": None,
        "provider_records": _canonical_provider_records(
            prices_path=prices_path,
            rates_path=rates_path,
            secondary_prices_path=secondary_prices_path,
            manifest_path=manifest_path,
            start=start,
            as_of=as_of,
            captured_at=observed_at,
            expected_price_tickers=expected_price_tickers,
            expected_rate_series=expected_rate_series,
        ),
        "captured_at": observed_at.isoformat(),
        "safety": {
            "canonical_cache_mutated": False,
            "scoped_data_quality_exception_used": False,
            "prospective_values_used": False,
            "feature_selection_executed": False,
            "model_training_executed": False,
            "candidate_search_executed": False,
            "strategy_backtest_executed": False,
            "target_weights_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }

    if canonical_evidence.passed:
        prices = pd.read_csv(prices_path, low_memory=False)
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        panel = prices.loc[
            prices["ticker"].isin(_REQUIRED_TICKERS)
            & prices["date"].between(pd.Timestamp(start), pd.Timestamp(as_of))
        ].copy()
        panel = _canonical_price_columns(panel).sort_values(["date", "ticker"], kind="stable")
        rates = pd.read_csv(rates_path, low_memory=False)
        rates["date"] = pd.to_datetime(rates["date"], errors="coerce")
        rates = rates.loc[
            rates["date"].between(pd.Timestamp(start), pd.Timestamp(as_of))
        ].sort_values(["date", "series"], kind="stable")
        panel_path = output_root / "market_panel.csv"
        rates_snapshot_path = output_root / "rates_snapshot.csv"
        _write_dataframe(panel, panel_path)
        _write_dataframe(rates, rates_snapshot_path)
        panel_report = validate_data_cache(
            prices_path=panel_path,
            rates_path=rates_snapshot_path,
            expected_price_tickers=list(_REQUIRED_TICKERS),
            expected_rate_series=sorted(str(item) for item in rates["series"].dropna().unique()),
            quality_config=config,
            as_of=as_of,
        )
        panel_report_path = output_root / "panel_data_quality_report.md"
        write_data_quality_report(panel_report, panel_report_path)
        panel_evidence = _quality_evidence(
            panel_report,
            report_path=panel_report_path,
            as_of=as_of,
            contract_id="isolated_decision_target_panel_validation",
            checked_input_count=2,
        )
        package.update(
            {
                "panel_materialized": True,
                "panel": _file_record(panel_path, row_count=len(panel)),
                "rates": _file_record(rates_snapshot_path, row_count=len(rates)),
                "panel_data_quality_report": _file_record(panel_report_path),
                "panel_data_quality_evidence": panel_evidence.to_dict(),
                "data_quality_report": _file_record(panel_report_path),
                "data_quality_evidence": panel_evidence.to_dict(),
                "provider_records": [
                    *package["provider_records"],
                    *_provider_records(
                        panel,
                        rates,
                        prices_path=prices_path,
                        rates_path=rates_path,
                        start=start,
                        as_of=as_of,
                        captured_at=observed_at,
                    ),
                ],
            }
        )

    package_path = output_root / "market_panel_package.json"
    write_json_atomic(package_path, package)
    return {**package, "package": _file_record(package_path)}


def _build_capability_decision_target_source_package(
    *,
    policy: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    output_root: Path,
    as_of: date,
    expected_price_tickers: Sequence[str],
    expected_rate_series: Sequence[str],
    manifest_path: Path | None,
    backtest_manifest_path: Path | None,
    secondary_prices_path: Path | None,
    require_secondary_prices: bool,
    quality_config: DataQualityConfig | None,
    captured_at: datetime | None,
    capability_policy_path: Path,
    data_quality_policy_path: Path,
) -> dict[str, Any]:
    capability_policy = load_reviewed_consumer_data_capability_policy(capability_policy_path)
    policy_dq = _mapping(policy.get("data_quality"))
    expected_binding = (
        capability_policy.capability_id == policy_dq.get("capability_id")
        and capability_policy.capability_version == policy_dq.get("capability_version")
        and capability_policy.consumer_id == policy_dq.get("capability_consumer_id")
        and capability_policy.consumer_version == policy_dq.get("capability_consumer_version")
        and capability_policy.required_price_tickers
        == tuple(sorted(str(item) for item in policy_dq.get("required_tickers", ())))
        and capability_policy.required_rate_series
        == tuple(sorted(str(item) for item in policy_dq.get("required_rate_series", ())))
    )
    if not expected_binding:
        raise ValueError("label policy and capability policy binding mismatch")
    result = build_consumer_data_capability(
        capability_policy=capability_policy,
        capability_policy_path=capability_policy_path,
        data_quality_policy_path=data_quality_policy_path,
        prices_path=prices_path,
        rates_path=rates_path,
        output_root=output_root / "capability",
        as_of=as_of,
        full_expected_price_tickers=expected_price_tickers,
        full_expected_rate_series=expected_rate_series,
        manifest_path=manifest_path,
        backtest_manifest_path=backtest_manifest_path,
        secondary_prices_path=secondary_prices_path,
        require_secondary_prices=require_secondary_prices,
        quality_config=quality_config,
        generated_at=_aware_utc(captured_at or datetime.now(UTC)),
    )
    receipt = result.receipt
    scoped_evidence = _receipt_quality_evidence(
        receipt,
        receipt.scoped_quality,
        contract_id="decision_target_label_core_capability",
    )
    full_evidence = _receipt_quality_evidence(
        receipt,
        receipt.full_quality,
        contract_id="canonical_decision_target_source_validation",
    )
    start = date.fromisoformat(str(_mapping(policy.get("research_context")).get("requested_start")))
    observed_at = _aware_utc(captured_at or receipt.generated_at)
    provider_records = _canonical_provider_records(
        prices_path=prices_path,
        rates_path=rates_path,
        secondary_prices_path=secondary_prices_path,
        manifest_path=manifest_path,
        start=start,
        as_of=as_of,
        captured_at=observed_at,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series,
    )
    if receipt.capability_passed:
        panel = pd.read_csv(result.scoped_prices_path, low_memory=False)
        rates = pd.read_csv(result.scoped_rates_path, low_memory=False)
        provider_records.extend(
            _provider_records(
                panel,
                rates,
                prices_path=prices_path,
                rates_path=rates_path,
                start=start,
                as_of=as_of,
                captured_at=observed_at,
            )
        )
    package: dict[str, Any] = {
        "schema_version": SOURCE_PACKAGE_SCHEMA_VERSION,
        "source_package_contract_version": "2.0.0",
        "as_of": as_of.isoformat(),
        "requested_start": start.isoformat(),
        "canonical_strict_validation_required": True,
        "canonical_full_cache_pass_required": False,
        "canonical_full_status_disclosure_required": True,
        "scoped_data_quality_exception_used": False,
        "consumer_capability_receipt_used": True,
        "panel_materialized": receipt.capability_passed,
        "global_cache_pass_claimed": receipt.global_cache_pass_claimed,
        "canonical_prices": _file_record(prices_path),
        "canonical_rates": _file_record(rates_path),
        "canonical_manifest": (
            _file_record(manifest_path)
            if manifest_path is not None and manifest_path.is_file()
            else None
        ),
        "canonical_backtest_manifest": (
            _file_record(backtest_manifest_path)
            if backtest_manifest_path is not None and backtest_manifest_path.is_file()
            else None
        ),
        "canonical_secondary_prices": (
            _file_record(secondary_prices_path)
            if secondary_prices_path is not None and secondary_prices_path.is_file()
            else None
        ),
        "capability_policy": _file_record(capability_policy_path),
        "data_quality_policy": _file_record(data_quality_policy_path),
        "capability_receipt": _file_record(result.receipt_path),
        "canonical_data_quality_report": _file_record(result.full_report_path),
        "canonical_data_quality_evidence": full_evidence.to_dict(),
        "panel_data_quality_report": _file_record(result.scoped_report_path),
        "panel_data_quality_evidence": scoped_evidence.to_dict(),
        "data_quality_report": _file_record(result.scoped_report_path),
        "data_quality_evidence": scoped_evidence.to_dict(),
        "panel": (
            _file_record(
                result.scoped_prices_path,
                row_count=_csv_row_count(result.scoped_prices_path),
            )
            if receipt.capability_passed
            else None
        ),
        "rates": (
            _file_record(
                result.scoped_rates_path,
                row_count=_csv_row_count(result.scoped_rates_path),
            )
            if receipt.capability_passed
            else None
        ),
        "provider_records": provider_records,
        "captured_at": observed_at.isoformat(),
        "safety": {
            "canonical_cache_mutated": False,
            "global_cache_pass_claimed": receipt.global_cache_pass_claimed,
            "scoped_data_quality_exception_used": False,
            "consumer_capability_receipt_used": True,
            "cross_consumer_reuse_allowed": False,
            "daily_operation_authorized": False,
            "prospective_values_used": False,
            "feature_selection_executed": False,
            "model_training_executed": False,
            "candidate_search_executed": False,
            "strategy_backtest_executed": False,
            "target_weights_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    package_path = output_root / "market_panel_package.json"
    write_json_atomic(package_path, package)
    return {**package, "package": _file_record(package_path)}


class DecisionTargetLabelFoundationCalculator:
    plugin_id = "decision_target_label_foundation_calculator"
    version = "v1"

    def calculate(self, context: ExperimentExecutionContext) -> dict[str, Any]:
        return build_label_payload(context.sources, as_of=context.as_of)


class DecisionTargetLabelFoundationReport:
    plugin_id = "decision_target_label_foundation_report"
    version = "v1"

    def section(self, payload: Mapping[str, Any], section_id: str) -> Mapping[str, Any]:
        if section_id != "label_foundation_summary":
            raise ValueError(f"unknown label foundation section: {section_id}")
        return _mapping(payload.get(section_id))

    def render_markdown(self, payload: Mapping[str, Any]) -> str:
        return render_label_markdown(payload)


def decision_target_label_foundation_registry() -> PluginRegistry:
    return PluginRegistry(
        calculators=(DecisionTargetLabelFoundationCalculator(),),
        reports=(DecisionTargetLabelFoundationReport(),),
    )


def build_label_payload(
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> dict[str, Any]:
    policy = _mapping(sources.get("label_policy"))
    package = _mapping(sources.get("market_panel_package"))
    data_quality_policy = _mapping(sources.get("data_quality_policy"))
    errors = _source_contract_errors(
        policy,
        package,
        data_quality_policy,
        as_of=as_of,
    )
    evidence = _data_quality_evidence(package, as_of=as_of)
    canonical_evidence = _mapping(package.get("canonical_data_quality_evidence"))
    capability_mode = package.get("consumer_capability_receipt_used") is True
    if not capability_mode and canonical_evidence.get("passed") is not True:
        errors.extend(
            str(item)
            for item in canonical_evidence.get(
                "blocking_issues", ("CANONICAL_DATA_QUALITY_NOT_PASSED",)
            )
        )
    if not evidence.passed:
        errors.extend(evidence.blocking_issues or ("DATA_QUALITY_NOT_PASSED",))
    if errors:
        return _blocked_payload(errors, evidence, as_of=as_of)

    evidence.assert_ready()
    commitment_errors = _source_file_commitment_errors(package)
    if commitment_errors:
        return _blocked_payload(commitment_errors, evidence, as_of=as_of)

    panel_record = _mapping(package.get("panel"))
    rates_record = _mapping(package.get("rates"))
    panel_path = Path(str(panel_record.get("path")))
    rates_path = Path(str(rates_record.get("path")))
    panel = pd.read_csv(panel_path, low_memory=False)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    rates = pd.read_csv(rates_path, low_memory=False)
    panel_row_count = panel_record.get("row_count")
    rates_row_count = rates_record.get("row_count")
    if panel_row_count is None or len(panel) != int(panel_row_count):
        return _blocked_payload(("MARKET_PANEL_ROW_COUNT_MISMATCH",), evidence, as_of=as_of)
    if rates_row_count is None or len(rates) != int(rates_row_count):
        return _blocked_payload(("RATES_SNAPSHOT_ROW_COUNT_MISMATCH",), evidence, as_of=as_of)
    if panel["date"].notna().any() and panel["date"].max().date() > as_of:
        return _blocked_payload(("PROSPECTIVE_VALUE_ENTERED_SOURCE_PANEL",), evidence, as_of=as_of)

    if not capability_mode:
        config = DataQualityConfig.model_validate(data_quality_policy)
        fresh_quality = validate_data_cache(
            prices_path=panel_path,
            rates_path=rates_path,
            expected_price_tickers=list(_REQUIRED_TICKERS),
            expected_rate_series=sorted(str(item) for item in rates["series"].dropna().unique()),
            quality_config=config,
            as_of=as_of,
        )
        if not fresh_quality.passed:
            return _blocked_payload(
                tuple(
                    sorted(
                        {
                            issue.code
                            for issue in fresh_quality.issues
                            if issue.severity.value == "ERROR"
                        }
                    )
                )
                or ("FRESH_PANEL_DATA_QUALITY_NOT_PASSED",),
                evidence,
                as_of=as_of,
            )

    start = date.fromisoformat(str(_mapping(policy.get("research_context")).get("requested_start")))
    panel = panel.loc[
        panel["ticker"].isin(_REQUIRED_TICKERS)
        & panel["date"].between(pd.Timestamp(start), pd.Timestamp(as_of))
    ].copy()
    session_sets = {
        ticker: tuple(
            panel.loc[panel["ticker"] == ticker, "date"].dropna().sort_values().dt.date.tolist()
        )
        for ticker in _REQUIRED_TICKERS
    }
    if not session_sets["QQQ"] or any(
        session_sets[ticker] != session_sets["QQQ"] for ticker in _REQUIRED_TICKERS[1:]
    ):
        return _blocked_payload(
            ("REQUIRED_TICKER_SESSION_ALIGNMENT_MISMATCH",), evidence, as_of=as_of
        )

    prices = (
        panel[["date", "ticker", "adj_close"]]
        .assign(adj_close=lambda frame: pd.to_numeric(frame["adj_close"], errors="coerce"))
        .pivot(index="date", columns="ticker", values="adj_close")
        .sort_index()
    )
    horizons = _horizons(policy)
    if not horizons or len(prices) <= max(item[1] for item in horizons):
        return _blocked_payload(("INSUFFICIENT_COMMON_PRICE_SESSIONS",), evidence, as_of=as_of)

    label_rows: list[dict[str, Any]] = []
    for decision_index in range(len(prices)):
        for horizon_id, sessions in horizons:
            end_index = decision_index + sessions
            if end_index >= len(prices):
                continue
            decision_date = prices.index[decision_index].date()
            start_date = prices.index[decision_index + 1].date()
            end_date = prices.index[end_index].date()
            returns = {
                ticker: float(
                    prices.iloc[end_index][ticker] / prices.iloc[decision_index][ticker] - 1.0
                )
                for ticker in _REQUIRED_TICKERS
            }
            direct_primary = returns["QQQ"] - returns["SGOV"]
            spy_minus_sgov = returns["SPY"] - returns["SGOV"]
            qqq_minus_spy = returns["QQQ"] - returns["SPY"]
            identity_residual = direct_primary - (spy_minus_sgov + qqq_minus_spy)
            if abs(identity_residual) > _FLOAT_IDENTITY_EPSILON:
                return _blocked_payload(
                    ("EXCESS_RETURN_ACCOUNTING_IDENTITY_FAILED",),
                    evidence,
                    as_of=as_of,
                )
            path = prices.iloc[decision_index : end_index + 1]
            risks = {ticker: _path_risk_diagnostics(path[ticker]) for ticker in _REQUIRED_TICKERS}
            label_rows.append(
                {
                    "decision_date": decision_date.isoformat(),
                    "horizon_id": horizon_id,
                    "horizon_sessions": sessions,
                    "label_start_date": start_date.isoformat(),
                    "label_end_date": end_date.isoformat(),
                    "label_available_on_session": end_date.isoformat(),
                    "qqq_forward_total_return": returns["QQQ"],
                    "spy_forward_total_return": returns["SPY"],
                    "sgov_forward_total_return": returns["SGOV"],
                    "qqq_minus_sgov": direct_primary,
                    "spy_minus_sgov": spy_minus_sgov,
                    "qqq_minus_spy": qqq_minus_spy,
                    "qqq_future_max_drawdown": risks["QQQ"]["future_max_drawdown"],
                    "spy_future_max_drawdown": risks["SPY"]["future_max_drawdown"],
                    "sgov_future_max_drawdown": risks["SGOV"]["future_max_drawdown"],
                    "qqq_future_worst_1d_return": risks["QQQ"]["future_worst_1d_return"],
                    "spy_future_worst_1d_return": risks["SPY"]["future_worst_1d_return"],
                    "sgov_future_worst_1d_return": risks["SGOV"]["future_worst_1d_return"],
                    "forward_total_returns": returns,
                    "excess_return_targets": {
                        "QQQ_MINUS_SGOV": direct_primary,
                        "SPY_MINUS_SGOV": spy_minus_sgov,
                        "QQQ_MINUS_SPY": qqq_minus_spy,
                    },
                    "accounting_identity_residual": identity_residual,
                    "future_path_risk": risks,
                }
            )

    if not label_rows:
        return _blocked_payload(("NO_MATURE_LABEL_ROWS",), evidence, as_of=as_of)
    summary = _ready_summary(label_rows, prices=prices, horizons=horizons)
    data_quality_scope = _data_quality_scope_summary(package)
    evaluation = {
        "requested_range": {
            "start": start.isoformat(),
            "end": as_of.isoformat(),
        },
        "evaluated_range": {
            "start": prices.index.min().date().isoformat(),
            "end": prices.index.max().date().isoformat(),
            "common_price_sessions": int(len(prices)),
        },
        "target_roles": policy.get("decision_targets"),
        "return_convention": policy.get("return_convention"),
        "split_readiness": policy.get("split_readiness"),
        "label_rows": label_rows,
        "source_package_commitment": {
            "schema_version": package.get("schema_version"),
            "panel_sha256": panel_record.get("sha256"),
            "rates_sha256": rates_record.get("sha256"),
            "canonical_prices_sha256": _mapping(package.get("canonical_prices")).get("sha256"),
            "canonical_rates_sha256": _mapping(package.get("canonical_rates")).get("sha256"),
            "data_quality_evidence_id": evidence.evidence_id,
            "scoped_data_quality_exception_used": False,
            "capability_receipt_id": data_quality_scope.get("capability_receipt_id"),
            "full_canonical_status": data_quality_scope.get("full_canonical_status"),
            "global_cache_pass_claimed": data_quality_scope.get("global_cache_pass_claimed"),
        },
    }
    commitment = hashlib.sha256(canonical_json_bytes(evaluation)).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": "TRADING-2460",
        "status": READY_STATUS,
        "data_quality_evidence": evidence.to_dict(),
        "data_quality_scope": data_quality_scope,
        "evaluation": evaluation,
        "evaluation_commitment_sha256": commitment,
        "label_foundation_summary": summary,
        "strict_validation_errors": [],
        "validation_status": "PASS",
        "manual_review_required": True,
        "research_only": True,
        "historical_seen_only": True,
        "prospective_accessed": False,
        "feature_selection_executed": False,
        "model_training_executed": False,
        "candidate_search_executed": False,
        "strategy_backtest_executed": False,
        "strategy_logic_changed": False,
        "target_weights_generated": False,
        "target_weights_changed": False,
        "official_action_universe_changed": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "next_route": "review_label_coverage_before_capability_modeling",
    }


def validate_label_payload(
    payload: Mapping[str, Any],
    sources: Mapping[str, Any],
    *,
    as_of: date,
) -> tuple[str, ...]:
    """Rebuild all derived content; do not trust stored summary or commitment fields."""

    rebuilt = build_label_payload(sources, as_of=as_of)
    errors: list[str] = []
    comparisons = (
        ("evaluation_commitment_sha256", "LABEL_COMMITMENT_MISMATCH"),
        ("evaluation", "LABEL_CONTENT_MISMATCH"),
        ("label_foundation_summary", "LABEL_SUMMARY_MISMATCH"),
        ("status", "LABEL_STATUS_MISMATCH"),
        ("data_quality_evidence", "LABEL_DATA_QUALITY_EVIDENCE_MISMATCH"),
        ("data_quality_scope", "LABEL_DATA_QUALITY_SCOPE_MISMATCH"),
    )
    for field, code in comparisons:
        if payload.get(field) != rebuilt.get(field):
            errors.append(code)
    safety_fields = (
        "manual_review_required",
        "research_only",
        "historical_seen_only",
        "prospective_accessed",
        "feature_selection_executed",
        "model_training_executed",
        "candidate_search_executed",
        "strategy_backtest_executed",
        "strategy_logic_changed",
        "target_weights_generated",
        "target_weights_changed",
        "official_action_universe_changed",
        "paper_shadow_changed",
        "promotion_allowed",
        "production_effect",
        "broker_action",
        "next_route",
    )
    if any(payload.get(field) != rebuilt.get(field) for field in safety_fields):
        errors.append("LABEL_SAFETY_BOUNDARY_MISMATCH")
    if render_label_markdown(payload) != render_label_markdown(rebuilt):
        errors.append("LABEL_MARKDOWN_MISMATCH")
    return tuple(errors)


def label_foundation_summary(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("label_foundation_summary"))


def decision_target_label_view_model(payload: Mapping[str, Any]) -> dict[str, Any]:
    return dict(label_foundation_summary(payload))


def render_label_markdown(payload: Mapping[str, Any]) -> str:
    status = str(payload.get("status") or "")
    summary = _mapping(payload.get("label_foundation_summary"))
    evidence = _mapping(payload.get("data_quality_evidence"))
    scope = _mapping(payload.get("data_quality_scope"))
    if status == BLOCKED_STATUS:
        blockers = ", ".join(str(item) for item in payload.get("strict_validation_errors", ()))
        return "\n".join(
            [
                "# Decision Target Capability Audit：第一批 Label Foundation",
                "",
                f"- 结论：`{status}`",
                f"- Canonical data quality：`{evidence.get('status')}`",
                f"- 阻塞项：{blockers or '未知'}",
                "- Scoped DQ exception：未使用、也不允许复用 QLD 的五资产例外。",
                "",
                "标签生成已 fail closed。修复 canonical 数据质量并取得 strict PASS 后，"
                "才能形成真实 label dataset；当前状态不影响 clean fixture 对语义实现的验证。",
                "",
            ]
        )
    lines = [
        "# Decision Target Capability Audit：第一批 Label Foundation",
        "",
        f"- 结论：`{status}`",
        f"- 数据质量：`{evidence.get('status')}`",
        f"- DQ capability：`{scope.get('capability_id') or 'legacy_full_cache'}`",
        f"- Full canonical DQ：`{scope.get('full_canonical_status') or evidence.get('status')}`",
        f"- Global cache PASS claim：`{scope.get('global_cache_pass_claimed')}`",
        f"- 研究窗口：`{summary.get('evaluated_start')}` 至 `{summary.get('evaluated_end')}`",
        f"- 共同交易日：{summary.get('common_price_sessions')}",
        f"- 成熟标签行：{summary.get('label_row_count')}",
        "- Primary target：`QQQ_MINUS_SGOV`",
        "- Diagnostic controls：`SPY_MINUS_SGOV`、`QQQ_MINUS_SPY`",
        "",
        "## Horizon coverage",
        "",
        "| Horizon | Rows | Decision start | Decision end | Latest label end |",
        "|---|---:|---|---|---|",
    ]
    for row in summary.get("horizon_rows", ()):
        item = _mapping(row)
        lines.append(
            f"| {item.get('horizon_id')} | {item.get('row_count')} | "
            f"{item.get('decision_start')} | {item.get('decision_end')} | "
            f"{item.get('latest_label_end')} |"
        )
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- 本批次只定义标签，不训练模型、不选择特征、不搜索策略、不改变仓位。",
            "- 收益区间从 decision close 到第 h 个未来共同交易日 close；"
            "`label_available_on_session` 明确标签何时才可用于训练。",
            "- 后续 fold 必须 purge 与验证/测试区间重叠的训练标签，并执行 label maturity gate。",
            "- Embargo 的数值尚未治理，第二批不得自行加入隐含天数。",
            "- 左尾字段是未来路径诊断，不是本批次的策略通过阈值。",
            "",
        ]
    )
    return "\n".join(lines)


def _source_contract_errors(
    policy: Mapping[str, Any],
    package: Mapping[str, Any],
    data_quality_policy: Mapping[str, Any],
    *,
    as_of: date,
) -> list[str]:
    errors: list[str] = []
    policy_schema = policy.get("schema_version")
    if policy_schema not in {
        "decision_target_capability_audit_policy.v1",
        "decision_target_capability_audit_policy.v2",
    }:
        errors.append("LABEL_POLICY_SCHEMA_INVALID")
    if package.get("schema_version") != SOURCE_PACKAGE_SCHEMA_VERSION:
        errors.append("SOURCE_PACKAGE_SCHEMA_INVALID")
    try:
        DataQualityConfig.model_validate(data_quality_policy)
    except ValueError:
        errors.append("DATA_QUALITY_POLICY_INVALID")
    if package.get("as_of") != as_of.isoformat():
        errors.append("SOURCE_PACKAGE_AS_OF_MISMATCH")
    if package.get("requested_start") != _mapping(policy.get("research_context")).get(
        "requested_start"
    ):
        errors.append("SOURCE_PACKAGE_START_MISMATCH")
    if not package.get("provider_records"):
        errors.append("PROVIDER_RECORDS_MISSING")
    if package.get("canonical_strict_validation_required") is not True:
        errors.append("CANONICAL_STRICT_DATA_QUALITY_NOT_REQUIRED")
    if package.get("scoped_data_quality_exception_used") is not False:
        errors.append("SCOPED_DATA_QUALITY_EXCEPTION_FORBIDDEN")
    data_quality = _mapping(policy.get("data_quality"))
    if data_quality.get("scoped_exception_allowed") is not False:
        errors.append("LABEL_POLICY_SCOPED_EXCEPTION_BOUNDARY_INVALID")
    if policy_schema == "decision_target_capability_audit_policy.v1":
        if data_quality.get("canonical_full_cache_pass_required") is not True:
            errors.append("LABEL_POLICY_CANONICAL_DQ_BOUNDARY_INVALID")
        if package.get("consumer_capability_receipt_used") not in {None, False}:
            errors.append("UNAUTHORIZED_CAPABILITY_RECEIPT_MODE")
    else:
        expected_v2 = (
            data_quality.get("canonical_full_cache_validation_required") is True
            and data_quality.get("canonical_full_cache_pass_required") is False
            and data_quality.get("canonical_full_status_disclosure_required") is True
            and data_quality.get("consumer_capability_receipt_required") is True
            and data_quality.get("accepted_capability_statuses") == ["PASS"]
            and data_quality.get("cross_consumer_reuse_allowed") is False
            and data_quality.get("daily_operation_authorized") is False
            and package.get("consumer_capability_receipt_used") is True
            and package.get("canonical_full_status_disclosure_required") is True
            and package.get("global_cache_pass_claimed")
            == (_mapping(package.get("canonical_data_quality_evidence")).get("status") == "PASS")
        )
        if not expected_v2:
            errors.append("LABEL_POLICY_CAPABILITY_DQ_BOUNDARY_INVALID")
        errors.extend(_capability_receipt_errors(package, data_quality=data_quality))
    target_policy = _mapping(policy.get("decision_targets"))
    if _mapping(target_policy.get("primary")).get("target_id") != "QQQ_MINUS_SGOV":
        errors.append("PRIMARY_DECISION_TARGET_INVALID")
    diagnostic_ids = {
        str(_mapping(item).get("target_id")) for item in target_policy.get("diagnostics", ())
    }
    if diagnostic_ids != {"SPY_MINUS_SGOV", "QQQ_MINUS_SPY"}:
        errors.append("DIAGNOSTIC_DECISION_TARGETS_INVALID")
    if _horizons(policy) != (("1d", 1), ("5d", 5), ("10d", 10), ("20d", 20)):
        errors.append("LABEL_HORIZONS_INVALID")
    split = _mapping(policy.get("split_readiness"))
    if (
        split.get("embargo_policy_status") != "DEFERRED_TO_NEXT_BATCH"
        or split.get("embargo_sessions") is not None
        or split.get("ungoverned_numeric_embargo_allowed") is not False
    ):
        errors.append("UNGOVERNED_EMBARGO_POLICY_INVALID")
    safety = _mapping(policy.get("safety"))
    required_false = (
        "prospective_values_used",
        "feature_selection_executed",
        "model_training_executed",
        "candidate_search_executed",
        "strategy_logic_changed",
        "target_weights_changed",
        "official_action_universe_changed",
        "paper_shadow_changed",
        "promotion_allowed",
    )
    if (
        safety.get("research_only") is not True
        or safety.get("historical_seen_only") is not True
        or any(safety.get(field) is not False for field in required_false)
        or safety.get("production_effect") != "none"
        or safety.get("broker_action") != "none"
    ):
        errors.append("LABEL_POLICY_SAFETY_BOUNDARY_INVALID")
    return errors


def _source_file_commitment_errors(package: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    required_records = (
        "canonical_prices",
        "canonical_rates",
        "canonical_data_quality_report",
        "panel",
        "rates",
        "data_quality_report",
    )
    optional_bound_records = (
        "canonical_manifest",
        "canonical_backtest_manifest",
        "canonical_secondary_prices",
        "capability_policy",
        "data_quality_policy",
        "capability_receipt",
        "panel_data_quality_report",
    )
    for record_id in required_records:
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
    for record_id in optional_bound_records:
        value = package.get(record_id)
        if value is None:
            continue
        record = _mapping(value)
        path = Path(str(record.get("path") or ""))
        if not path.is_file():
            errors.append(f"{record_id.upper()}_FILE_MISSING")
            continue
        if sha256_path(path) != str(record.get("sha256") or ""):
            errors.append(f"{record_id.upper()}_SHA256_MISMATCH")
        if path.stat().st_size != int(record.get("size_bytes") or -1):
            errors.append(f"{record_id.upper()}_SIZE_MISMATCH")
    evidence = _mapping(package.get("data_quality_evidence"))
    report = _mapping(package.get("data_quality_report"))
    if evidence.get("report_sha256") != report.get("sha256"):
        errors.append("DATA_QUALITY_REPORT_SHA256_MISMATCH")
    if str(evidence.get("report_path") or "") != str(report.get("path") or ""):
        errors.append("DATA_QUALITY_REPORT_PATH_MISMATCH")
    if not package.get("provider_records"):
        errors.append("PROVIDER_RECORDS_MISSING")
    canonical_evidence = _mapping(package.get("canonical_data_quality_evidence"))
    canonical_report = _mapping(package.get("canonical_data_quality_report"))
    if canonical_evidence.get("report_sha256") != canonical_report.get("sha256"):
        errors.append("CANONICAL_DATA_QUALITY_REPORT_SHA256_MISMATCH")
    if str(canonical_evidence.get("report_path") or "") != str(canonical_report.get("path") or ""):
        errors.append("CANONICAL_DATA_QUALITY_REPORT_PATH_MISMATCH")
    return errors


def _capability_receipt_errors(
    package: Mapping[str, Any],
    *,
    data_quality: Mapping[str, Any],
) -> list[str]:
    receipt_record = _mapping(package.get("capability_receipt"))
    capability_policy_record = _mapping(package.get("capability_policy"))
    dq_policy_record = _mapping(package.get("data_quality_policy"))
    if not receipt_record or not capability_policy_record or not dq_policy_record:
        return ["CAPABILITY_RECEIPT_BINDING_MISSING"]
    try:
        receipt = verify_consumer_data_capability_receipt(
            Path(str(receipt_record.get("path") or "")),
            capability_policy_path=Path(str(capability_policy_record.get("path") or "")),
            data_quality_policy_path=Path(str(dq_policy_record.get("path") or "")),
        )
    except ValueError:
        return ["CAPABILITY_RECEIPT_VERIFICATION_FAILED"]
    expected = (
        receipt.capability_id == data_quality.get("capability_id")
        and receipt.capability_version == data_quality.get("capability_version")
        and receipt.consumer_id == data_quality.get("capability_consumer_id")
        and receipt.consumer_version == data_quality.get("capability_consumer_version")
        and receipt.required_price_tickers
        == tuple(sorted(str(item) for item in data_quality.get("required_tickers", ())))
        and receipt.required_rate_series
        == tuple(sorted(str(item) for item in data_quality.get("required_rate_series", ())))
        and receipt.cross_consumer_reuse_allowed is False
        and receipt.daily_operation_authorized is False
        and receipt.production_effect == "none"
        and receipt.broker_action == "none"
    )
    if not expected:
        return ["CAPABILITY_RECEIPT_CONSUMER_SCOPE_MISMATCH"]
    if not receipt.capability_passed:
        return ["CAPABILITY_RECEIPT_NOT_PASSED"]
    return []


def _data_quality_scope_summary(package: Mapping[str, Any]) -> dict[str, Any]:
    canonical = _mapping(package.get("canonical_data_quality_evidence"))
    if package.get("consumer_capability_receipt_used") is not True:
        return {
            "mode": "canonical_full_cache",
            "capability_id": None,
            "capability_version": None,
            "capability_receipt_id": None,
            "full_canonical_status": canonical.get("status"),
            "global_cache_pass_claimed": canonical.get("passed") is True,
            "cross_consumer_reuse_allowed": False,
            "daily_operation_authorized": False,
        }
    record = _mapping(package.get("capability_receipt"))
    try:
        receipt = ConsumerDataCapabilityReceipt.from_json_bytes(
            Path(str(record.get("path") or "")).read_bytes()
        )
    except (OSError, ValueError):
        return {
            "mode": "consumer_capability_receipt",
            "capability_id": None,
            "capability_version": None,
            "capability_receipt_id": None,
            "full_canonical_status": canonical.get("status"),
            "global_cache_pass_claimed": False,
            "cross_consumer_reuse_allowed": False,
            "daily_operation_authorized": False,
        }
    return {
        "mode": "consumer_capability_receipt",
        "capability_id": receipt.capability_id,
        "capability_version": receipt.capability_version,
        "capability_receipt_id": receipt.receipt_id,
        "consumer_id": receipt.consumer_id,
        "consumer_version": receipt.consumer_version,
        "full_canonical_status": receipt.full_quality.status,
        "scoped_status": receipt.scoped_quality.status,
        "global_cache_pass_claimed": receipt.global_cache_pass_claimed,
        "isolated_global_error_codes": list(receipt.isolated_global_error_codes),
        "unisolated_global_error_codes": list(receipt.unisolated_global_error_codes),
        "cross_consumer_reuse_allowed": receipt.cross_consumer_reuse_allowed,
        "daily_operation_authorized": receipt.daily_operation_authorized,
    }


def _ready_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    prices: pd.DataFrame,
    horizons: Sequence[tuple[str, int]],
) -> dict[str, Any]:
    horizon_rows: list[dict[str, Any]] = []
    for horizon_id, sessions in horizons:
        selected = [row for row in rows if row.get("horizon_id") == horizon_id]
        horizon_rows.append(
            {
                "horizon_id": horizon_id,
                "horizon_sessions": sessions,
                "row_count": len(selected),
                "decision_start": selected[0]["decision_date"],
                "decision_end": selected[-1]["decision_date"],
                "latest_label_end": selected[-1]["label_end_date"],
            }
        )
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "status": READY_STATUS,
        "evaluated_start": prices.index.min().date().isoformat(),
        "evaluated_end": prices.index.max().date().isoformat(),
        "common_price_sessions": int(len(prices)),
        "label_row_count": len(rows),
        "primary_target": "QQQ_MINUS_SGOV",
        "diagnostic_targets": ["SPY_MINUS_SGOV", "QQQ_MINUS_SPY"],
        "horizon_rows": horizon_rows,
        "purge_ready": True,
        "label_maturity_gate_defined": True,
        "embargo_numeric_value_defined": False,
    }


def _blocked_payload(
    errors: Sequence[str],
    evidence: DataQualityEvidence,
    *,
    as_of: date,
) -> dict[str, Any]:
    normalized = sorted(set(str(item) for item in errors if str(item)))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": "TRADING-2460",
        "status": BLOCKED_STATUS,
        "data_quality_evidence": evidence.to_dict(),
        "evaluation": None,
        "evaluation_commitment_sha256": None,
        "label_foundation_summary": {
            "schema_version": SUMMARY_SCHEMA_VERSION,
            "status": BLOCKED_STATUS,
            "as_of": as_of.isoformat(),
            "label_row_count": 0,
            "horizon_rows": [],
            "purge_ready": False,
            "embargo_numeric_value_defined": False,
        },
        "strict_validation_errors": normalized,
        "validation_status": "BLOCKED",
        "manual_review_required": True,
        "research_only": True,
        "historical_seen_only": True,
        "prospective_accessed": False,
        "feature_selection_executed": False,
        "model_training_executed": False,
        "candidate_search_executed": False,
        "strategy_backtest_executed": False,
        "strategy_logic_changed": False,
        "target_weights_generated": False,
        "target_weights_changed": False,
        "official_action_universe_changed": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "next_route": "resolve_canonical_data_quality_then_rebuild_source_package",
    }


def _data_quality_evidence(
    package: Mapping[str, Any],
    *,
    as_of: date,
) -> DataQualityEvidence:
    raw = package.get("data_quality_evidence")
    if isinstance(raw, Mapping):
        try:
            return DataQualityEvidence.from_dict(raw)
        except ValueError:
            pass
    return DataQualityEvidence(
        contract_id="decision_target_source_contract_failure",
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v2",
        status="FAIL",
        passed=False,
        checked_at=datetime.combine(as_of, time.min, tzinfo=UTC),
        as_of=as_of,
        report_path=None,
        report_sha256=None,
        error_count=1,
        warning_count=0,
        checked_input_count=0,
        blocking_issues=("DATA_QUALITY_EVIDENCE_INVALID_OR_MISSING",),
    )


def _quality_evidence(
    report: Any,
    *,
    report_path: Path,
    as_of: date,
    contract_id: str,
    checked_input_count: int,
) -> DataQualityEvidence:
    return DataQualityEvidence(
        contract_id=contract_id,
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v2",
        status=report.status,
        passed=report.passed,
        checked_at=report.checked_at,
        as_of=as_of,
        report_path=str(report_path.resolve()),
        report_sha256=sha256_path(report_path),
        error_count=report.error_count,
        warning_count=report.warning_count,
        checked_input_count=checked_input_count,
        blocking_issues=tuple(
            sorted({issue.code for issue in report.issues if issue.severity.value == "ERROR"})
        ),
    )


def _receipt_quality_evidence(
    receipt: ConsumerDataCapabilityReceipt,
    quality: CapabilityQualityBinding,
    *,
    contract_id: str,
) -> DataQualityEvidence:
    blocking = tuple(sorted({issue.code for issue in quality.issues if issue.severity == "ERROR"}))
    return DataQualityEvidence(
        contract_id=contract_id,
        policy_id="DATA_QUALITY_CACHE_GATE",
        policy_version="data_quality_cache_gate.v2",
        status=quality.status,
        passed=quality.status in {"PASS", "PASS_WITH_WARNINGS"},
        checked_at=receipt.generated_at,
        as_of=receipt.as_of,
        report_path=str(Path(quality.report.path)),
        report_sha256=quality.report.sha256,
        error_count=quality.error_count,
        warning_count=quality.warning_count,
        checked_input_count=2,
        blocking_issues=blocking,
    )


def _horizons(policy: Mapping[str, Any]) -> tuple[tuple[str, int], ...]:
    items = policy.get("horizons")
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
        return ()
    result: list[tuple[str, int]] = []
    for item in items:
        row = _mapping(item)
        sessions = row.get("sessions")
        if not isinstance(sessions, int) or isinstance(sessions, bool):
            return ()
        result.append((str(row.get("horizon_id") or ""), sessions))
    return tuple(result)


def _path_risk_diagnostics(series: pd.Series) -> dict[str, float]:
    values = pd.to_numeric(series, errors="coerce")
    drawdowns = values / values.cummax() - 1.0
    one_day = values.pct_change().iloc[1:]
    return {
        "future_max_drawdown": float(drawdowns.min()),
        "future_worst_1d_return": float(one_day.min()),
    }


def _provider_records(
    panel: pd.DataFrame,
    rates: pd.DataFrame,
    *,
    prices_path: Path,
    rates_path: Path,
    start: date,
    as_of: date,
    captured_at: datetime,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for ticker in _REQUIRED_TICKERS:
        rows = panel.loc[panel["ticker"] == ticker]
        providers = (
            sorted(str(item) for item in rows["source"].dropna().unique())
            if "source" in rows.columns
            else ["UNDECLARED_IN_CANONICAL_CACHE"]
        )
        records.append(
            {
                "instrument": ticker,
                "provider": providers,
                "endpoint": str(prices_path.resolve()),
                "request_parameters": {
                    "ticker": ticker,
                    "from": start.isoformat(),
                    "to": as_of.isoformat(),
                    "price_field": "adj_close",
                },
                "download_or_capture_timestamp": captured_at.isoformat(),
                "row_count": int(len(rows)),
                "normalized_sha256": _dataframe_sha256(rows),
            }
        )
    records.append(
        {
            "instrument": ",".join(sorted(str(item) for item in rates["series"].dropna().unique())),
            "provider": (
                sorted(str(item) for item in rates["source"].dropna().unique())
                if "source" in rates.columns
                else ["UNDECLARED_IN_CANONICAL_CACHE"]
            ),
            "endpoint": str(rates_path.resolve()),
            "request_parameters": {
                "from": start.isoformat(),
                "to": as_of.isoformat(),
            },
            "download_or_capture_timestamp": captured_at.isoformat(),
            "row_count": int(len(rates)),
            "normalized_sha256": _dataframe_sha256(rates),
        }
    )
    return records


def _canonical_provider_records(
    *,
    prices_path: Path,
    rates_path: Path,
    secondary_prices_path: Path | None,
    manifest_path: Path | None,
    start: date,
    as_of: date,
    captured_at: datetime,
    expected_price_tickers: Sequence[str],
    expected_rate_series: Sequence[str],
) -> list[dict[str, Any]]:
    roles = [
        ("canonical_prices", prices_path, [str(item) for item in expected_price_tickers]),
        ("canonical_rates", rates_path, [str(item) for item in expected_rate_series]),
    ]
    if secondary_prices_path is not None:
        roles.append(
            (
                "canonical_secondary_prices",
                secondary_prices_path,
                [str(item) for item in expected_price_tickers],
            )
        )
    records: list[dict[str, Any]] = []
    manifest = (
        pd.read_csv(manifest_path, low_memory=False)
        if manifest_path is not None and manifest_path.is_file()
        else pd.DataFrame()
    )
    for role, source_path, expected_values in roles:
        matched = pd.DataFrame()
        if not manifest.empty and "output_path" in manifest.columns:

            def matches_source_path(
                value: object,
                expected: Path = source_path,
            ) -> bool:
                return _same_resolved_path(value, expected)

            matched = manifest.loc[manifest["output_path"].map(matches_source_path)].copy()
            if not matched.empty and "downloaded_at" in matched.columns:
                latest = matched["downloaded_at"].astype(str).max()
                matched = matched.loc[matched["downloaded_at"].astype(str) == latest]
        if matched.empty:
            records.append(
                {
                    "source_role": role,
                    "source_id": "canonical_cache_file_binding",
                    "provider": "project canonical cache",
                    "endpoint": str(source_path.resolve()),
                    "request_parameters": {
                        "from": start.isoformat(),
                        "to": as_of.isoformat(),
                        "expected_values": expected_values,
                    },
                    "download_or_capture_timestamp": captured_at.isoformat(),
                    "row_count": _csv_row_count(source_path),
                    "checksum_sha256": sha256_path(source_path),
                    "manifest_binding_available": False,
                }
            )
            continue
        for row in matched.to_dict(orient="records"):
            request_parameters = row.get("request_parameters")
            try:
                parsed_parameters = (
                    json.loads(str(request_parameters))
                    if isinstance(request_parameters, str)
                    else request_parameters
                )
            except json.JSONDecodeError:
                parsed_parameters = {"raw_manifest_value": str(request_parameters)}
            records.append(
                {
                    "source_role": role,
                    "source_id": str(row.get("source_id") or ""),
                    "provider": str(row.get("provider") or ""),
                    "endpoint": str(row.get("endpoint") or ""),
                    "request_parameters": parsed_parameters,
                    "download_or_capture_timestamp": str(
                        row.get("downloaded_at") or captured_at.isoformat()
                    ),
                    "row_count": int(row.get("row_count") or 0),
                    "checksum_sha256": str(row.get("checksum_sha256") or ""),
                    "output_path": str(source_path.resolve()),
                    "manifest_binding_available": True,
                }
            )
    if manifest_path is not None and manifest_path.is_file():
        records.append(
            {
                "source_role": "canonical_download_manifest",
                "source_id": "download_manifest",
                "provider": "project canonical download audit manifest",
                "endpoint": str(manifest_path.resolve()),
                "request_parameters": {
                    "bound_source_roles": [item[0] for item in roles],
                    "as_of": as_of.isoformat(),
                },
                "download_or_capture_timestamp": captured_at.isoformat(),
                "row_count": _csv_row_count(manifest_path),
                "checksum_sha256": sha256_path(manifest_path),
                "manifest_binding_available": True,
            }
        )
    return records


def _same_resolved_path(value: object, expected: Path) -> bool:
    try:
        return Path(str(value)).resolve() == expected.resolve()
    except (OSError, RuntimeError, ValueError):
        return False


def _csv_row_count(path: Path) -> int:
    return int(len(pd.read_csv(path, usecols=[0], low_memory=False)))


def _canonical_price_columns(frame: pd.DataFrame) -> pd.DataFrame:
    columns = (
        "date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source",
    )
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result[list(columns)].copy()


def _file_record(path: Path, *, row_count: int | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(path.resolve()),
        "sha256": sha256_path(path),
        "size_bytes": path.stat().st_size,
    }
    if row_count is not None:
        record["row_count"] = int(row_count)
    return record


def _dataframe_sha256(frame: pd.DataFrame) -> str:
    normalized = frame.copy()
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized[column] = normalized[column].dt.strftime("%Y-%m-%d")
    content = normalized.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _write_dataframe(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = frame.copy()
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized[column] = normalized[column].dt.strftime("%Y-%m-%d")
    normalized.to_csv(path, index=False, lineterminator="\n")


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    return value.astimezone(UTC).replace(microsecond=0)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "build_decision_target_source_package",
    "build_label_payload",
    "decision_target_label_foundation_registry",
    "decision_target_label_view_model",
    "label_foundation_summary",
    "render_label_markdown",
    "validate_label_payload",
]
