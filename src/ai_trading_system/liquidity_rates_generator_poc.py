from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
    CandidateArtifactProvenance,
    CandidateBoundPredictionArtifact,
    CandidateBoundSignalRecord,
)
from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    CandidateSignalSpec,
    candidate_artifact_safety_fields,
    generator_operation_safety_fields,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    clean_for_yaml,
    load_adjusted_price_matrix,
    mapping,
    max_price_date,
    round_float,
    to_float,
    write_json,
    write_markdown,
)
from ai_trading_system.regenerated_candidate_generator_common import (
    clamp_score,
    rolling_return,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

TASK_ID = "TRADING-2312_LIQUIDITY_RATES_PRESSURE_GENERATOR_POC"
SOURCE_TASK_ID = "TRADING-2311_LIQUIDITY_RATES_PRESSURE_DATA_FEASIBILITY_AUDIT"
REPORT_TYPE = "liquidity_rates_pressure_generator_poc"
STATUS = "LIQUIDITY_RATES_PRESSURE_GENERATOR_POC_READY_VALIDATION_BLOCKED"
MODE = "generator_poc"
ARTIFACT_ROLE = "liquidity_rates_pressure_generator_poc"
CANDIDATE_FAMILY = "liquidity_rates_pressure"

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "liquidity_rates_pressure_generator_policy.yaml"
)
DEFAULT_FEASIBILITY_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "liquidity_rates_data_feasibility_audit"
)

DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("10d", "20d", "1m")
DEFAULT_CANDIDATES = (
    "duration_pressure_proxy_v1",
    "rates_pressure_exposure_cap_modifier_v1",
)
BLOCKED_CANDIDATES = ("liquidity_headwind_proxy_v1",)
REQUIRED_PRICE_SYMBOLS = ("QQQ", "SMH", "TLT", "SHY")
REQUIRED_RATE_SERIES = ("DGS10", "DGS2", "DTWEXBGS")
DECISION_TIMESTAMP_UTC_HOUR = 21

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "partial_rates_only_generator_poc": True,
    "full_liquidity_pressure_poc_ready": False,
    "liquidity_headwind_generator_implemented": False,
    "actual_path_validation_ready": False,
    "actual_path_validation_executed": False,
    "promotion_eligible": False,
    "permanently_inconclusive_override_allowed": False,
}


class LiquidityRatesGeneratorPOCError(ValueError):
    pass


def run_liquidity_rates_pressure_generator_poc(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    policy_path: Path = DEFAULT_POLICY_PATH,
    feasibility_dir: Path = DEFAULT_FEASIBILITY_ROOT,
    target_assets: str | Sequence[str] = DEFAULT_TARGET_ASSETS,
    horizons: str | Sequence[str] = DEFAULT_HORIZONS,
    candidates: str | Sequence[str] = DEFAULT_CANDIDATES,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    quality_as_of: str | date | None = None,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    request = _validated_request(
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        mode=mode,
    )
    policy = _load_policy(policy_path)
    feasibility = _validate_feasibility_source(feasibility_dir)
    required_symbols = _required_symbols(policy=policy, candidates=request["candidates"])
    resolved_start = _resolve_start_date(start_date, policy)
    resolved_end = _resolve_end_date(end_date=end_date, prices_path=prices_path)
    resolved_quality_as_of = _resolve_quality_as_of(
        quality_as_of=quality_as_of,
        prices_path=prices_path,
        resolved_end=resolved_end,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    quality_report, quality_report_path = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        required_symbols=required_symbols,
        quality_as_of=resolved_quality_as_of,
        output_dir=output_dir,
    )
    if not quality_report.passed:
        raise LiquidityRatesGeneratorPOCError(
            f"TRADING-2312 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )
    artifacts = build_liquidity_rates_pressure_generator_artifacts(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
        policy=policy,
        feasibility=feasibility,
        target_assets=request["target_assets"],
        horizons=request["horizons"],
        candidates=request["candidates"],
        start_date=resolved_start,
        end_date=resolved_end,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        generated_at=generated_at,
    )
    artifact_paths = write_liquidity_rates_pressure_generator_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return clean_for_yaml(summary)


def build_liquidity_rates_pressure_generator_artifacts(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    policy_path: Path,
    policy: Mapping[str, Any],
    feasibility: Mapping[str, Any],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    candidates: Sequence[str],
    start_date: date,
    end_date: date,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    required_symbols = _required_symbols(policy=policy, candidates=candidates)
    price_matrix = load_adjusted_price_matrix(prices_path, required_symbols)
    rates_matrix = _load_rates_matrix(rates_path, REQUIRED_RATE_SERIES)
    source_dates = _source_dates(
        price_matrix=price_matrix,
        rates_matrix=rates_matrix,
        required_symbols=required_symbols,
        required_rate_series=_required_rate_series(policy, candidates),
        start_date=start_date,
        end_date=end_date,
        lookback_days=_lookback_days(policy),
    )
    if not source_dates:
        raise LiquidityRatesGeneratorPOCError(
            "TRADING-2312 generated no source dates after lookback and rates availability"
        )
    source_hashes = {
        "prices": _sha256(prices_path),
        "rates": _sha256(rates_path),
        "policy": _sha256(policy_path),
    }
    if marketstack_prices_path is not None and marketstack_prices_path.exists():
        source_hashes["marketstack_prices"] = _sha256(marketstack_prices_path)
    common = _common_payload(
        policy=policy,
        feasibility=feasibility,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        source_dates=source_dates,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        generated_at=generated_at,
        required_symbols=required_symbols,
        required_rate_series=_required_rate_series(policy, candidates),
        source_hashes=source_hashes,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
    )
    candidate_bundles: dict[str, dict[str, Any]] = {}
    for candidate_id in candidates:
        candidate_bundles[candidate_id] = _build_candidate_bundle(
            candidate_id=candidate_id,
            policy=policy,
            common=common,
            price_matrix=price_matrix,
            rates_matrix=rates_matrix,
            source_dates=source_dates,
            target_assets=target_assets,
            horizons=horizons,
            prices_path=prices_path,
            rates_path=rates_path,
            policy_path=policy_path,
            generated_at=generated_at,
        )
    blocked_report = _blocked_candidate_report(policy=policy, common=common)
    top_validation = _top_level_validation(candidate_bundles)
    summary = _summary_payload(
        common=common,
        candidate_bundles=candidate_bundles,
        blocked_report=blocked_report,
        top_validation=top_validation,
    )
    return {
        "summary": summary,
        "policy_summary": _policy_summary(policy=policy, common=common),
        "safety_boundary": _safety_boundary(common=common),
        "top_validation": top_validation,
        "blocked_report": blocked_report,
        "candidate_bundles": candidate_bundles,
        "docs": _docs_payload(
            summary=summary,
            candidate_bundles=candidate_bundles,
            blocked_report=blocked_report,
        ),
    }


def write_liquidity_rates_pressure_generator_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    paths: dict[str, Any] = {
        "summary": output_dir / "liquidity_rates_pressure_generator_poc_summary.json",
        "policy_summary": output_dir / "liquidity_rates_generator_policy_summary.json",
        "top_validation": output_dir / "liquidity_rates_generator_validation_summary.json",
        "safety_boundary": output_dir / "liquidity_rates_generator_safety_boundary.json",
        "blocked_candidate_report": output_dir
        / "blocked_liquidity_rates_candidate_report.json",
        "report_doc": docs_root / "liquidity_rates_pressure_generator_poc.md",
        "candidates": {},
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["policy_summary"], artifacts["policy_summary"])
    write_json(paths["top_validation"], artifacts["top_validation"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])
    write_json(paths["blocked_candidate_report"], artifacts["blocked_report"])
    write_markdown(paths["report_doc"], artifacts["docs"]["report"])
    for candidate_id, bundle in mapping(artifacts.get("candidate_bundles")).items():
        candidate_dir = output_dir / str(candidate_id)
        candidate_paths = {
            "candidate_signal_spec": candidate_dir / "candidate_signal_spec.json",
            "candidate_signal_series": candidate_dir / "candidate_signal_series.csv",
            "candidate_prediction_artifact": candidate_dir
            / "candidate_prediction_artifact.json",
            "generation_summary": candidate_dir / "generation_summary.json",
            "validation_summary": candidate_dir / "validation_summary.json",
        }
        write_json(candidate_paths["candidate_signal_spec"], bundle["signal_spec"])
        _write_signal_series_csv(
            candidate_paths["candidate_signal_series"],
            bundle["signal_records"],
        )
        write_json(
            candidate_paths["candidate_prediction_artifact"],
            bundle["prediction_artifact"],
        )
        write_json(candidate_paths["generation_summary"], bundle["generation_summary"])
        write_json(candidate_paths["validation_summary"], bundle["validation_summary"])
        paths["candidates"][str(candidate_id)] = candidate_paths
    return _clean_paths(paths)


def _build_candidate_bundle(
    *,
    candidate_id: str,
    policy: Mapping[str, Any],
    common: Mapping[str, Any],
    price_matrix: pd.DataFrame,
    rates_matrix: pd.DataFrame,
    source_dates: Sequence[pd.Timestamp],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    prices_path: Path,
    rates_path: Path,
    policy_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    candidate_policy = _candidate_policy(policy, candidate_id)
    signal_spec = CandidateSignalSpec(
        candidate_id=candidate_id,
        candidate_family=CANDIDATE_FAMILY,
        generator_id=f"{candidate_id}_generator",
        generator_version=f"{REPORT_TYPE}.v1",
        signal_spec_version=SIGNAL_SPEC_VERSION,
        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
        target_asset=",".join(target_assets),
        supported_horizons=tuple(horizons),
        required_inputs=tuple(_required_inputs_for_candidate(candidate_policy)),
        output_signal_names=tuple(candidate_policy["output_signal_names"]),
        signal_direction_mapping={
            name: "positive means rates pressure / risk-off, negative means relief"
            for name in candidate_policy["output_signal_names"]
        },
        validity_rule=(
            "valid_from equals next-day decision timestamp; valid_until equals "
            "decision timestamp plus horizon calendar days"
        ),
        pit_policy="pit_approximation",
        **generator_operation_safety_fields(),
    ).to_dict()
    signal_records = _candidate_signal_records(
        candidate_id=candidate_id,
        policy=policy,
        candidate_policy=candidate_policy,
        common=common,
        price_matrix=price_matrix,
        rates_matrix=rates_matrix,
        source_dates=source_dates,
        target_assets=target_assets,
        horizons=horizons,
        prices_path=prices_path,
        rates_path=rates_path,
        policy_path=policy_path,
        generated_at=generated_at,
        signal_spec=signal_spec,
    )
    prediction_artifact = _prediction_artifact(
        candidate_id=candidate_id,
        common=common,
        signal_records=signal_records,
        signal_spec=signal_spec,
        prices_path=prices_path,
        rates_path=rates_path,
        policy_path=policy_path,
    )
    validation_summary = _candidate_validation_summary(
        candidate_id=candidate_id,
        signal_spec=signal_spec,
        signal_records=signal_records,
        prediction_artifact=prediction_artifact,
    )
    generation_summary = _candidate_generation_summary(
        candidate_id=candidate_id,
        common=common,
        signal_records=signal_records,
        validation_summary=validation_summary,
    )
    return {
        "signal_spec": signal_spec,
        "signal_records": signal_records,
        "prediction_artifact": prediction_artifact,
        "generation_summary": generation_summary,
        "validation_summary": validation_summary,
    }


def _candidate_signal_records(
    *,
    candidate_id: str,
    policy: Mapping[str, Any],
    candidate_policy: Mapping[str, Any],
    common: Mapping[str, Any],
    price_matrix: pd.DataFrame,
    rates_matrix: pd.DataFrame,
    source_dates: Sequence[pd.Timestamp],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    prices_path: Path,
    rates_path: Path,
    policy_path: Path,
    generated_at: datetime,
    signal_spec: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_row_index = 0
    for current_ts in source_dates:
        signal_values = _compute_candidate_signals(
            candidate_id=candidate_id,
            policy=policy,
            candidate_policy=candidate_policy,
            price_matrix=price_matrix,
            rates_matrix=rates_matrix,
            current_ts=current_ts,
        )
        for target_asset in target_assets:
            for horizon in horizons:
                horizon_days = _parse_horizon_days(horizon)
                as_of = datetime.combine(
                    current_ts.date(),
                    time(DECISION_TIMESTAMP_UTC_HOUR, 0),
                    tzinfo=UTC,
                )
                decision = as_of + timedelta(days=1)
                valid_until = decision + timedelta(days=horizon_days)
                for signal_name, signal_value in signal_values.items():
                    source_row_index += 1
                    direction = _signal_direction(signal_value, policy)
                    confidence = _signal_confidence(common, policy, current_ts)
                    record = CandidateBoundSignalRecord(
                        candidate_id=candidate_id,
                        candidate_family=CANDIDATE_FAMILY,
                        source_experiment_id=str(signal_spec["generator_id"]),
                        source_artifact_id=(
                            f"{prices_path.stem}+{rates_path.stem}+{policy_path.stem}"
                        ),
                        source_artifact_path=str(prices_path),
                        source_artifact_hash=str(common["source_hashes"]["prices"]),
                        signal_spec_version=SIGNAL_SPEC_VERSION,
                        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
                        generated_at=generated_at.isoformat(),
                        as_of_timestamp=as_of.isoformat(),
                        decision_timestamp=decision.isoformat(),
                        target_asset=target_asset,
                        horizon=horizon,
                        signal_name=signal_name,
                        signal_value=round_float(signal_value),
                        signal_direction=direction,
                        signal_confidence=confidence,
                        valid_from=decision.isoformat(),
                        valid_until=valid_until.isoformat(),
                        input_snapshot_hash=str(common["input_snapshot_hash"]),
                        feature_snapshot_hash=str(common["feature_snapshot_hash"]),
                        model_or_rule_version=f"{REPORT_TYPE}.rules.v1",
                        provenance=CandidateArtifactProvenance(
                            source_paths=[
                                str(prices_path),
                                str(rates_path),
                                str(policy_path),
                            ],
                            source_hashes=[
                                str(common["source_hashes"]["prices"]),
                                str(common["source_hashes"]["rates"]),
                                str(common["source_hashes"]["policy"]),
                            ],
                            regeneration_mode="deterministic_regeneration",
                            pit_policy="pit_approximation",
                            candidate_binding_method="native_candidate_id",
                            source_schema_status="candidate_bound",
                            promotion_eligible=False,
                        ),
                        **candidate_artifact_safety_fields(),
                        source_row_index=source_row_index,
                        source_date=current_ts.date().isoformat(),
                        source_trend_state=direction,
                        source_confidence=confidence,
                        source_prediction_flags={
                            "liquidity_rates_pressure_generator_poc": True,
                            "required_symbol_data_quality_passed": bool(
                                common["data_quality"]["passed"]
                            ),
                            "partial_rates_only_generator_poc": True,
                            "full_liquidity_pressure_poc_ready": False,
                            "actual_path_validation_ready": False,
                        },
                    )
                    payload = record.to_dict()
                    payload.update(
                        {
                            "market_regime": common["market_regime"],
                            "actual_requested_date_range": common[
                                "actual_requested_date_range"
                            ],
                            "data_quality_status": common["data_quality_status"],
                            "policy_id": common["policy_id"],
                            "policy_version": common["policy_version"],
                            "macro_known_at_status": (
                                "release_timestamp_not_cached_observation_date_only"
                            ),
                        }
                    )
                    rows.append(payload)
    return clean_for_yaml(rows)


def _compute_candidate_signals(
    *,
    candidate_id: str,
    policy: Mapping[str, Any],
    candidate_policy: Mapping[str, Any],
    price_matrix: pd.DataFrame,
    rates_matrix: pd.DataFrame,
    current_ts: pd.Timestamp,
) -> dict[str, float]:
    duration_price_pressure = _duration_price_pressure(price_matrix, current_ts, policy)
    dgs10_pressure = _yield_pressure(rates_matrix, "DGS10", current_ts, policy)
    dgs2_pressure = _yield_pressure(rates_matrix, "DGS2", current_ts, policy)
    if candidate_id == "duration_pressure_proxy_v1":
        weights = mapping(candidate_policy.get("component_weights"))
        duration_pressure = clamp_score(
            to_float(weights["tlt_vs_shy_pressure"]) * duration_price_pressure
            + to_float(weights["dgs10_pressure"]) * dgs10_pressure
            + to_float(weights["dgs2_pressure"]) * dgs2_pressure
        )
        return {
            "duration_pressure_score": duration_pressure,
            "tlt_vs_shy_pressure_score": duration_price_pressure,
            "yield_curve_pressure_score": clamp_score(
                (dgs10_pressure + dgs2_pressure) / 2.0
            ),
        }
    if candidate_id == "rates_pressure_exposure_cap_modifier_v1":
        weights = mapping(candidate_policy.get("component_weights"))
        rates_pressure = clamp_score(
            to_float(weights["duration_pressure"]) * duration_price_pressure
            + to_float(weights["ten_year_yield_pressure"]) * dgs10_pressure
            + to_float(weights["two_year_yield_pressure"]) * dgs2_pressure
        )
        return {
            "rates_pressure_exposure_cap_score": rates_pressure,
            "valuation_pressure_context_score": clamp_score(
                (duration_price_pressure + dgs10_pressure) / 2.0
            ),
            "exposure_cap_modifier_pressure_score": max(0.0, rates_pressure),
        }
    raise LiquidityRatesGeneratorPOCError(f"unsupported candidate: {candidate_id}")


def _prediction_artifact(
    *,
    candidate_id: str,
    common: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    signal_spec: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    policy_path: Path,
) -> dict[str, Any]:
    if not signal_records:
        raise LiquidityRatesGeneratorPOCError(
            f"{candidate_id} prediction artifact requires signal records"
        )
    latest = signal_records[-1]
    prediction_records = [
        {
            **dict(record),
            "prediction_record_role": "research_only_candidate_bound_signal",
            "actual_path_validation_executed": False,
            "promotion_blocker": "TRADING-2313_liquidity_rates_actual_path_validation_required",
        }
        for record in signal_records
    ]
    artifact = CandidateBoundPredictionArtifact(
        artifact_id=f"{candidate_id}_prediction_artifact",
        artifact_role=ARTIFACT_ROLE,
        candidate_id=candidate_id,
        candidate_family=CANDIDATE_FAMILY,
        source_experiment_id=str(signal_spec["generator_id"]),
        source_artifact_id=f"{prices_path.stem}+{rates_path.stem}+{policy_path.stem}",
        source_artifact_path=str(prices_path),
        source_artifact_hash=str(common["source_hashes"]["prices"]),
        signal_spec_version=SIGNAL_SPEC_VERSION,
        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
        generated_at=str(latest["generated_at"]),
        as_of_timestamp=str(latest["as_of_timestamp"]),
        decision_timestamp=str(latest["decision_timestamp"]),
        target_asset=str(latest["target_asset"]),
        horizon=str(latest["horizon"]),
        signal_name=str(latest["signal_name"]),
        signal_value=to_float(latest["signal_value"]),
        signal_direction=str(latest["signal_direction"]),
        signal_confidence=to_float(latest["signal_confidence"]),
        valid_from=str(latest["valid_from"]),
        valid_until=str(latest["valid_until"]),
        input_snapshot_hash=str(common["input_snapshot_hash"]),
        feature_snapshot_hash=str(common["feature_snapshot_hash"]),
        model_or_rule_version=f"{REPORT_TYPE}.rules.v1",
        provenance=CandidateArtifactProvenance(
            source_paths=[str(prices_path), str(rates_path), str(policy_path)],
            source_hashes=[
                str(common["source_hashes"]["prices"]),
                str(common["source_hashes"]["rates"]),
                str(common["source_hashes"]["policy"]),
            ],
            regeneration_mode="deterministic_regeneration",
            pit_policy="pit_approximation",
            candidate_binding_method="native_candidate_id",
            source_schema_status="candidate_bound",
            promotion_eligible=False,
        ),
        prediction_records=prediction_records,
        source_schema_status="candidate_bound",
        historical_executable_artifact=False,
        actual_path_validation_ready=False,
        **candidate_artifact_safety_fields(),
    ).to_dict()
    artifact.update(
        {
            "schema_version": PREDICTION_SCHEMA_VERSION,
            "record_count": len(prediction_records),
            "generation_mode": MODE,
            "candidate_binding_method": "native_candidate_id",
            "target_assets": list(common["target_assets"]),
            "horizons": list(common["horizons"]),
            "data_quality": dict(common["data_quality"]),
            "actual_path_validation_blocker": (
                "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION"
            ),
            **SAFETY_FIELDS,
        }
    )
    return clean_for_yaml(artifact)


def _candidate_validation_summary(
    *,
    candidate_id: str,
    signal_spec: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    prediction_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    validator = CandidateSignalBindingValidator()
    spec_validation = validator.validate_candidate_signal_spec(signal_spec)
    series_validation = validator.validate_candidate_bound_signal_series(signal_records)
    artifact_validation = validator.validate_candidate_bound_prediction_artifact(
        prediction_artifact
    )
    errors = (
        list(spec_validation.errors)
        + list(series_validation.errors)
        + list(artifact_validation.errors)
    )
    return {
        "schema_version": f"{REPORT_TYPE}.validation_summary.v1",
        "task_id": TASK_ID,
        "candidate_id": candidate_id,
        "status": "PASS" if not errors else "FAIL",
        "candidate_bound_validator_reused": True,
        "signal_spec_validation": spec_validation.to_dict(),
        "signal_series_validation": series_validation.to_dict(),
        "prediction_artifact_validation": artifact_validation.to_dict(),
        "checked_signal_record_count": len(signal_records),
        "checked_prediction_record_count": len(
            prediction_artifact.get("prediction_records", [])
        ),
        "errors": errors,
        **SAFETY_FIELDS,
    }


def _candidate_generation_summary(
    *,
    candidate_id: str,
    common: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.generation_summary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": STATUS,
        "candidate_id": candidate_id,
        "candidate_family": CANDIDATE_FAMILY,
        "artifact_role": ARTIFACT_ROLE,
        "market_regime": MARKET_REGIME,
        "actual_requested_date_range": common["actual_requested_date_range"],
        "generated_source_date_range": common["generated_source_date_range"],
        "signal_record_count": len(signal_records),
        "prediction_record_count": len(signal_records),
        "validation_status": validation_summary.get("status"),
        "data_quality": dict(common["data_quality"]),
        "policy_id": common["policy_id"],
        "policy_version": common["policy_version"],
        "candidate_signal_spec_artifact": "generated",
        "candidate_signal_series_artifact": "generated",
        "candidate_prediction_artifact": "generated",
        "blocked_full_scope": common["blocked_full_scope"],
        "actual_path_validation_blocker": (
            "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION"
        ),
        **SAFETY_FIELDS,
    }


def _summary_payload(
    *,
    common: Mapping[str, Any],
    candidate_bundles: Mapping[str, Mapping[str, Any]],
    blocked_report: Mapping[str, Any],
    top_validation: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_rows = [
        {
            "candidate_id": candidate_id,
            "signal_record_count": len(bundle["signal_records"]),
            "validation_status": bundle["validation_summary"]["status"],
            "actual_path_validation_ready": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        for candidate_id, bundle in candidate_bundles.items()
    ]
    return clean_for_yaml(
        {
            **dict(common),
            "summary": {
                "candidate_count": len(candidate_bundles),
                "generated_candidate_count": len(candidate_bundles),
                "blocked_candidate_count": len(blocked_report["blocked_candidates"]),
                "blocked_candidate_ids": list(blocked_report["blocked_candidates"]),
                "candidate_signal_series_generated": True,
                "candidate_prediction_artifact_generated": True,
                "validation_status": top_validation["status"],
                "data_quality_status": common["data_quality"]["status"],
                "partial_rates_only_generator_poc": True,
                "full_liquidity_pressure_poc_ready": False,
                "actual_path_validation_ready": False,
                "recommended_next_task": (
                    "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION"
                ),
            },
            "candidate_rows": candidate_rows,
            "top_validation": dict(top_validation),
            **SAFETY_FIELDS,
        }
    )


def _common_payload(
    *,
    policy: Mapping[str, Any],
    feasibility: Mapping[str, Any],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    candidates: Sequence[str],
    start_date: date,
    end_date: date,
    source_dates: Sequence[pd.Timestamp],
    quality_report: DataQualityReport,
    quality_report_path: Path,
    generated_at: datetime,
    required_symbols: Sequence[str],
    required_rate_series: Sequence[str],
    source_hashes: Mapping[str, str],
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    policy_path: Path,
) -> dict[str, Any]:
    data_quality = _data_quality_payload(quality_report, quality_report_path)
    generated_start = source_dates[0].date().isoformat()
    generated_end = source_dates[-1].date().isoformat()
    blocked_full_scope = list(feasibility.get("blocked_full_scope", []))
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "Liquidity / Rates Pressure Generator POC",
        "task_id": TASK_ID,
        "source_task_id": SOURCE_TASK_ID,
        "status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "requested_start_date": start_date.isoformat(),
        "requested_end_date": end_date.isoformat(),
        "actual_requested_date_range": f"{start_date.isoformat()}..{end_date.isoformat()}",
        "generated_source_start_date": generated_start,
        "generated_source_end_date": generated_end,
        "generated_source_date_range": f"{generated_start}..{generated_end}",
        "generated_source_date_count": len(source_dates),
        "candidate_family": CANDIDATE_FAMILY,
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "candidates": list(candidates),
        "blocked_candidates": list(BLOCKED_CANDIDATES),
        "required_price_symbols": list(required_symbols),
        "required_rate_series": list(required_rate_series),
        "policy_id": str(policy["policy_id"]),
        "policy_version": str(policy["version"]),
        "policy_status": str(policy["status"]),
        "policy_path": str(policy_path),
        "prices_path": str(prices_path),
        "rates_path": str(rates_path),
        "marketstack_prices_path": str(marketstack_prices_path or ""),
        "source_hashes": dict(source_hashes),
        "data_quality": data_quality,
        "data_quality_status": data_quality["status"],
        "data_quality_report_path": str(quality_report_path),
        "source_feasibility_status": feasibility["status"],
        "source_feasibility_data_quality_status": feasibility["data_quality_status"],
        "blocked_full_scope": blocked_full_scope,
        "input_snapshot_hash": _stable_hash(
            {
                "required_symbols": list(required_symbols),
                "required_rate_series": list(required_rate_series),
                "target_assets": list(target_assets),
                "horizons": list(horizons),
                "prices_checksum": source_hashes["prices"],
                "rates_checksum": source_hashes["rates"],
                "quality_as_of": data_quality["as_of"],
            }
        ),
        "feature_snapshot_hash": _stable_hash(
            {
                "policy_hash": source_hashes["policy"],
                "policy_id": policy["policy_id"],
                "policy_version": policy["version"],
                "signal_policy": mapping(policy.get("signal_policy")),
                "candidate_policy": mapping(policy.get("candidate_policy")),
                "source_gap_policy": mapping(policy.get("source_gap_policy")),
            }
        ),
    }


def _policy_summary(*, policy: Mapping[str, Any], common: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.policy_summary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": STATUS,
        "policy_id": policy["policy_id"],
        "policy_version": policy["version"],
        "policy_status": policy["status"],
        "owner": policy["owner"],
        "market_regime": common["market_regime"],
        "rationale": policy["rationale"],
        "intended_effect": policy["intended_effect"],
        "validation_evidence": policy["validation_evidence"],
        "review_condition": policy["review_condition"],
        "expiry_condition": policy["expiry_condition"],
        "signal_policy": mapping(policy.get("signal_policy")),
        "candidate_policy": mapping(policy.get("candidate_policy")),
        "source_gap_policy": mapping(policy.get("source_gap_policy")),
        **SAFETY_FIELDS,
    }


def _safety_boundary(*, common: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
        "task_id": TASK_ID,
        "report_type": REPORT_TYPE,
        "status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED",
        "data_quality_status": common["data_quality"]["status"],
        "data_quality_report_path": common["data_quality_report_path"],
        "required_symbol_data_quality_passed": common["data_quality"]["passed"],
        "does_not_generate_liquidity_headwind_candidate": True,
        "does_not_run_actual_path_validation": True,
        "does_not_run_scope_review": True,
        "does_not_allow_promotion": True,
        "does_not_allow_paper_shadow": True,
        "does_not_allow_production": True,
        "does_not_allow_broker_action": True,
        "next_required_task": "TRADING-2313_LIQUIDITY_RATES_ACTUAL_PATH_VALIDATION",
        **SAFETY_FIELDS,
    }


def _blocked_candidate_report(
    *, policy: Mapping[str, Any], common: Mapping[str, Any]
) -> dict[str, Any]:
    source_gap_policy = mapping(policy.get("source_gap_policy"))
    blocked = mapping(source_gap_policy.get("blocked_candidates"))
    rows = []
    for candidate_id in BLOCKED_CANDIDATES:
        item = mapping(blocked.get(candidate_id))
        rows.append(
            {
                "candidate_id": candidate_id,
                "blocker": "SOURCE_GAP_BLOCKED_BY_TRADING_2311",
                "missing_inputs": _strings(item.get("missing_inputs")),
                "behavioral_impact": item.get("behavioral_impact", ""),
                "risk": item.get("risk", ""),
                "validation_coverage": item.get("validation_coverage", ""),
                "exit_condition": item.get("exit_condition", ""),
                "candidate_signal_series_generated": False,
                "candidate_prediction_artifact_generated": False,
                "generator_implemented": False,
                **SAFETY_FIELDS,
            }
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.blocked_candidate_report.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "status": "LIQUIDITY_HEADWIND_SOURCE_GAP_BLOCKED",
            "data_quality_status": common["data_quality_status"],
            "source_feasibility_status": common["source_feasibility_status"],
            "blocked_candidates": list(BLOCKED_CANDIDATES),
            "rows": rows,
            **SAFETY_FIELDS,
        }
    )


def _top_level_validation(candidate_bundles: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    errors: list[str] = []
    for candidate_id, bundle in candidate_bundles.items():
        errors.extend(
            f"{candidate_id}: {error}"
            for error in _strings(bundle["validation_summary"].get("errors"))
        )
    return {
        "schema_version": f"{REPORT_TYPE}.top_level_validation_summary.v1",
        "task_id": TASK_ID,
        "status": "PASS" if not errors else "FAIL",
        "candidate_count": len(candidate_bundles),
        "candidate_bound_validator_reused": True,
        "errors": errors,
        **SAFETY_FIELDS,
    }


def _docs_payload(
    *,
    summary: Mapping[str, Any],
    candidate_bundles: Mapping[str, Mapping[str, Any]],
    blocked_report: Mapping[str, Any],
) -> dict[str, str]:
    candidate_rows = [
        (
            f"|`{candidate_id}`|`{bundle['generation_summary']['signal_record_count']}`|"
            f"`{bundle['validation_summary']['status']}`|`False`|"
        )
        for candidate_id, bundle in candidate_bundles.items()
    ]
    blocked_rows = [
        (
            f"|`{row['candidate_id']}`|`{','.join(row['missing_inputs'])}`|"
            f"`{row['blocker']}`|"
        )
        for row in blocked_report["rows"]
    ]
    report = "\n".join(
        [
            "# Liquidity / Rates Pressure Generator POC",
            "",
            "TRADING-2312 生成 research-only partial rates-only candidate-bound artifacts。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- generated_source_date_range: `{summary['generated_source_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_report_path: `{summary['data_quality_report_path']}`",
            f"- policy_version: `{summary['policy_id']}:{summary['policy_version']}`",
            "- partial_rates_only_generator_poc: `True`",
            "- full_liquidity_pressure_poc_ready: `False`",
            "- liquidity_headwind_generator_implemented: `False`",
            "- actual_path_validation_ready: `False`",
            "",
            "## Generated Candidates",
            "",
            "|candidate_id|signal_record_count|validation_status|actual_path_validation_ready|",
            "|---|---:|---|---|",
            *candidate_rows,
            "",
            "## Blocked Candidates",
            "",
            "|candidate_id|missing_inputs|blocker|",
            "|---|---|---|",
            *blocked_rows,
            "",
            "## Safety",
            "",
            "本 POC 只使用 TLT / SHY / DGS10 / DGS2 partial rates route；"
            "`liquidity_headwind_proxy_v1` 因 UUP / HYG / LQD source gap 不生成 "
            "candidate-bound artifacts。当前不得用于 actual-path validation、scope review、"
            "promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return {"report": report}


def _validated_request(
    *,
    target_assets: str | Sequence[str],
    horizons: str | Sequence[str],
    candidates: str | Sequence[str],
    mode: str,
) -> dict[str, Any]:
    if mode != MODE:
        raise LiquidityRatesGeneratorPOCError(
            f"liquidity / rates generator POC only supports {MODE}"
        )
    parsed_assets = _parse_list(target_assets, uppercase=True)
    parsed_horizons = _parse_list(horizons, uppercase=False)
    parsed_candidates = _parse_list(candidates, uppercase=False)
    if not parsed_assets:
        raise LiquidityRatesGeneratorPOCError("--target-assets is required")
    if not parsed_horizons:
        raise LiquidityRatesGeneratorPOCError("--horizons is required")
    if not parsed_candidates:
        raise LiquidityRatesGeneratorPOCError("--candidates is required")
    blocked = sorted(set(parsed_candidates) & set(BLOCKED_CANDIDATES))
    if blocked:
        raise LiquidityRatesGeneratorPOCError(
            "blocked by TRADING-2311 source gap; no generator workaround is allowed "
            f"for candidates: {blocked}"
        )
    unsupported = sorted(set(parsed_candidates) - set(DEFAULT_CANDIDATES))
    if unsupported:
        raise LiquidityRatesGeneratorPOCError(
            f"unsupported TRADING-2312 candidates: {unsupported}"
        )
    return {
        "target_assets": parsed_assets,
        "horizons": parsed_horizons,
        "candidates": parsed_candidates,
    }


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise LiquidityRatesGeneratorPOCError(f"policy file missing: {path}")
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise LiquidityRatesGeneratorPOCError("policy must be a mapping")
    policy = dict(raw)
    required = (
        "policy_id",
        "version",
        "status",
        "owner",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
        "data_quality",
        "source_gap_policy",
        "signal_policy",
        "candidate_policy",
        "safety",
    )
    missing = [key for key in required if key not in policy]
    if missing:
        raise LiquidityRatesGeneratorPOCError(
            f"policy missing required fields: {missing}"
        )
    for candidate_id in DEFAULT_CANDIDATES:
        _candidate_policy(policy, candidate_id)
    return policy


def _validate_feasibility_source(feasibility_dir: Path) -> dict[str, Any]:
    summary_path = feasibility_dir / "liquidity_rates_data_feasibility_summary.json"
    design_path = feasibility_dir / "liquidity_pressure_candidate_design_sketch.json"
    if not summary_path.exists():
        raise LiquidityRatesGeneratorPOCError(
            f"TRADING-2311 feasibility summary missing: {summary_path}"
        )
    if not design_path.exists():
        raise LiquidityRatesGeneratorPOCError(
            f"TRADING-2311 design sketch missing: {design_path}"
        )
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary = mapping(payload.get("summary"))
    design = json.loads(design_path.read_text(encoding="utf-8"))
    expected_status = "LIQUIDITY_RATES_FEASIBILITY_AUDIT_READY_PARTIAL_PROXY"
    if summary.get("status") != expected_status:
        raise LiquidityRatesGeneratorPOCError(
            "TRADING-2311 feasibility summary is not ready for partial rates POC"
        )
    if summary.get("partial_poc_possible") is not True:
        raise LiquidityRatesGeneratorPOCError("TRADING-2311 partial POC is not allowed")
    if summary.get("full_liquidity_pressure_poc_ready") is not False:
        raise LiquidityRatesGeneratorPOCError(
            "TRADING-2311 unexpectedly marks full liquidity pressure POC ready"
        )
    if summary.get("promotion_allowed") is not False:
        raise LiquidityRatesGeneratorPOCError(
            "TRADING-2311 source unexpectedly allows promotion"
        )
    expected_missing = {"UUP", "HYG", "LQD"}
    missing_prices = set(_strings(summary.get("missing_price_proxy_symbols")))
    if not expected_missing.issubset(missing_prices):
        raise LiquidityRatesGeneratorPOCError(
            "TRADING-2311 no longer records required liquidity headwind source gaps"
        )
    return {
        "status": str(summary["status"]),
        "data_quality_status": str(summary["data_quality_status"]),
        "actual_requested_date_range": str(summary["actual_requested_date_range"]),
        "blocked_full_scope": _strings(design.get("blocked_full_scope")),
        "recommended_partial_scope": _strings(design.get("recommended_partial_scope")),
        "missing_price_proxy_symbols": sorted(missing_prices),
        "missing_macro_series": _strings(summary.get("missing_macro_series")),
    }


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    required_symbols: Sequence[str],
    quality_as_of: date,
    output_dir: Path,
) -> tuple[DataQualityReport, Path]:
    universe = load_universe()
    secondary_path = (
        marketstack_prices_path
        if marketstack_prices_path is not None and marketstack_prices_path.exists()
        else None
    )
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=list(required_symbols),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=quality_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=secondary_path,
        require_secondary_prices=False,
    )
    report_path = default_quality_report_path(output_dir, quality_as_of)
    write_data_quality_report(report, report_path)
    return report, report_path


def _data_quality_payload(report: DataQualityReport, report_path: Path) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "report_path": str(report_path),
        "expected_price_tickers": list(report.expected_price_tickers),
        "expected_rate_series": list(report.expected_rate_series),
        "price_row_count": report.price_summary.rows,
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "warning_count": report.warning_count,
        "error_count": report.error_count,
    }


def _load_rates_matrix(path: Path, series_ids: Sequence[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    missing = {"date", "series", "value"} - set(frame.columns)
    if missing:
        raise LiquidityRatesGeneratorPOCError(
            f"rates cache missing columns: {sorted(missing)}"
        )
    frame = frame.loc[frame["series"].astype(str).isin(set(series_ids))].copy()
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    pivot = frame.pivot_table(index="date", columns="series", values="value", aggfunc="last")
    pivot = pivot.sort_index()
    for series_id in series_ids:
        if series_id not in pivot.columns:
            pivot[series_id] = pd.NA
    return pivot.reindex(columns=list(series_ids))


def _source_dates(
    *,
    price_matrix: pd.DataFrame,
    rates_matrix: pd.DataFrame,
    required_symbols: Sequence[str],
    required_rate_series: Sequence[str],
    start_date: date,
    end_date: date,
    lookback_days: int,
) -> list[pd.Timestamp]:
    clean_prices = price_matrix.loc[:, list(required_symbols)].dropna(how="any")
    clean_rates = rates_matrix.loc[:, list(required_rate_series)].dropna(how="any")
    common_index = clean_prices.index.intersection(clean_rates.index).sort_values()
    output: list[pd.Timestamp] = []
    for ts in common_index:
        if ts.date() < start_date or ts.date() > end_date:
            continue
        price_pos = price_matrix.index.get_loc(ts)
        rate_pos = rates_matrix.index.get_loc(ts)
        if (
            isinstance(price_pos, int)
            and isinstance(rate_pos, int)
            and price_pos >= lookback_days
            and rate_pos >= lookback_days
        ):
            output.append(ts)
    return output


def _duration_price_pressure(
    price_matrix: pd.DataFrame,
    current_ts: pd.Timestamp,
    policy: Mapping[str, Any],
) -> float:
    tlt_return = rolling_return(price_matrix, "TLT", current_ts, _lookback_days(policy))
    shy_return = rolling_return(price_matrix, "SHY", current_ts, _lookback_days(policy))
    if tlt_return is None or shy_return is None:
        return 0.0
    return clamp_score(
        (shy_return - tlt_return)
        / _policy_float(policy, "duration_return_score_scale")
    )


def _yield_pressure(
    rates_matrix: pd.DataFrame,
    series_id: str,
    current_ts: pd.Timestamp,
    policy: Mapping[str, Any],
) -> float:
    if series_id not in rates_matrix.columns or current_ts not in rates_matrix.index:
        return 0.0
    position = rates_matrix.index.get_loc(current_ts)
    if not isinstance(position, int) or position < _lookback_days(policy):
        return 0.0
    current = to_float(rates_matrix.at[current_ts, series_id], default=float("nan"))
    previous = to_float(
        rates_matrix[series_id].iloc[position - _lookback_days(policy)],
        default=float("nan"),
    )
    if pd.isna(current) or pd.isna(previous):
        return 0.0
    return clamp_score((current - previous) / _policy_float(policy, "yield_change_score_scale"))


def _signal_direction(value: float, policy: Mapping[str, Any]) -> str:
    neutral_band = _policy_float(policy, "neutral_band")
    if value > neutral_band:
        return "risk_off"
    if value < -neutral_band:
        return "risk_on"
    return "neutral"


def _signal_confidence(
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    current_ts: pd.Timestamp,
) -> float:
    first_source_date = date.fromisoformat(str(common["requested_start_date"]))
    if current_ts.date() <= first_source_date:
        return round_float(_policy_float(policy, "confidence_floor"))
    confidence = _policy_float(policy, "base_confidence")
    if to_float(common["data_quality"].get("warning_count")) > 0.0:
        confidence -= _policy_float(policy, "data_quality_warning_confidence_penalty")
    confidence -= _policy_float(policy, "macro_known_at_confidence_penalty")
    confidence = max(_policy_float(policy, "confidence_floor"), confidence)
    return round_float(min(1.0, confidence))


def _required_symbols(
    *,
    policy: Mapping[str, Any],
    candidates: Sequence[str],
) -> tuple[str, ...]:
    symbols = list(REQUIRED_PRICE_SYMBOLS)
    for candidate_id in candidates:
        candidate = _candidate_policy(policy, candidate_id)
        symbols.extend(_strings(candidate.get("required_price_symbols")))
    return tuple(dict.fromkeys(symbols))


def _required_rate_series(
    policy: Mapping[str, Any],
    candidates: Sequence[str],
) -> tuple[str, ...]:
    series = list(REQUIRED_RATE_SERIES)
    for candidate_id in candidates:
        series.extend(_strings(_candidate_policy(policy, candidate_id).get("required_rate_series")))
    return tuple(dict.fromkeys(series))


def _required_inputs_for_candidate(candidate_policy: Mapping[str, Any]) -> list[str]:
    return [
        *[f"price:{symbol}" for symbol in _strings(candidate_policy.get("required_price_symbols"))],
        *[f"rate:{series}" for series in _strings(candidate_policy.get("required_rate_series"))],
    ]


def _candidate_policy(policy: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    candidates = mapping(policy.get("candidate_policy"))
    candidate = candidates.get(candidate_id)
    if not isinstance(candidate, Mapping):
        raise LiquidityRatesGeneratorPOCError(
            f"policy missing candidate policy: {candidate_id}"
        )
    return dict(candidate)


def _lookback_days(policy: Mapping[str, Any]) -> int:
    return int(_policy_value(policy, "lookback_trading_days"))


def _policy_float(policy: Mapping[str, Any], key: str) -> float:
    return to_float(_policy_value(policy, key))


def _policy_value(policy: Mapping[str, Any], key: str) -> Any:
    signal_policy = mapping(policy.get("signal_policy"))
    item = mapping(signal_policy.get(key))
    if "value" not in item:
        raise LiquidityRatesGeneratorPOCError(f"policy signal value missing: {key}")
    return item["value"]


def _resolve_start_date(value: str | date | None, policy: Mapping[str, Any]) -> date:
    if value is not None and value != "":
        return _resolve_date(value)
    return _resolve_date(str(policy.get("default_start_date") or DEFAULT_BACKTEST_START))


def _resolve_end_date(*, end_date: str | date | None, prices_path: Path) -> date:
    if end_date is not None and end_date != "":
        return _resolve_date(end_date)
    return max_price_date(prices_path)


def _resolve_quality_as_of(
    *,
    quality_as_of: str | date | None,
    prices_path: Path,
    resolved_end: date,
) -> date:
    if quality_as_of is not None and quality_as_of != "":
        return _resolve_date(quality_as_of)
    return min(resolved_end, max_price_date(prices_path))


def _resolve_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise LiquidityRatesGeneratorPOCError(
            f"date must use YYYY-MM-DD: {value}"
        ) from exc


def _parse_horizon_days(value: str) -> int:
    text = str(value).strip().lower()
    if text.endswith("d"):
        return int(text[:-1])
    if text.endswith("m"):
        return int(text[:-1]) * 30
    raise LiquidityRatesGeneratorPOCError(f"unsupported horizon: {value}")


def _parse_list(value: str | Sequence[str], *, uppercase: bool) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = value.split(",")
    else:
        parts = [str(item) for item in value]
    cleaned = [part.strip() for part in parts if part.strip()]
    if uppercase:
        return tuple(part.upper() for part in cleaned)
    return tuple(cleaned)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Sequence):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(clean_for_yaml(dict(payload)), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_signal_series_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(clean_for_yaml(value), sort_keys=True)
    return value


def _clean_paths(paths: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in paths.items():
        if isinstance(value, Path):
            cleaned[key] = str(value)
        elif isinstance(value, Mapping):
            cleaned[key] = _clean_paths(value)
        else:
            cleaned[key] = value
    return cleaned


__all__ = [
    "BLOCKED_CANDIDATES",
    "DEFAULT_CANDIDATES",
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_FEASIBILITY_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "STATUS",
    "LiquidityRatesGeneratorPOCError",
    "build_liquidity_rates_pressure_generator_artifacts",
    "run_liquidity_rates_pressure_generator_poc",
]
