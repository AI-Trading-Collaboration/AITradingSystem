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
    candidate_bound_signal_series_contract_dict,
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

TASK_ID = "TRADING-2308_AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC"
REPORT_TYPE = "ai_semiconductor_leadership_generator_poc"
STATUS = "AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC_READY_VALIDATION_BLOCKED"
MODE = "generator_poc"
ARTIFACT_ROLE = "ai_semiconductor_leadership_generator_poc"
CANDIDATE_FAMILY = "ai_semiconductor_leadership"

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "ai_semiconductor_leadership_generator_poc"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "ai_semiconductor_leadership_generator_policy.yaml"
)
DEFAULT_FEASIBILITY_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "ai_semiconductor_leadership_feasibility_audit"
)

DEFAULT_TARGET_ASSETS = ("QQQ", "SMH")
DEFAULT_HORIZONS = ("5d", "10d", "20d")
DEFAULT_CANDIDATES = (
    "smh_relative_strength_leadership_v1",
    "ai_semiconductor_leadership_quality_v1",
    "ai_core_basket_leadership_v1",
)

DECISION_TIMESTAMP_UTC_HOUR = 21
FULL_UNIVERSE_BLOCKER = "full_universe_validation_blocked_by_ASX_missing_out_of_scope"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
    "actual_path_validation_ready": False,
    "actual_path_validation_executed": False,
    "promotion_eligible": False,
    "permanently_inconclusive_override_allowed": False,
}


class AISemiconductorLeadershipGeneratorPOCError(ValueError):
    pass


def run_ai_semiconductor_leadership_generator_poc(
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
    _validate_feasibility_source(feasibility_dir)
    required_symbols = _required_symbols(policy=policy, candidates=request["candidates"])
    resolved_start = _resolve_start_date(start_date, policy)
    resolved_end = _resolve_end_date(
        end_date=end_date,
        prices_path=prices_path,
        required_symbols=required_symbols,
    )
    resolved_quality_as_of = _resolve_quality_as_of(
        quality_as_of=quality_as_of,
        prices_path=prices_path,
        required_symbols=required_symbols,
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
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"TRADING-2308 data quality gate failed: {quality_report.status}; "
            f"report={quality_report_path}"
        )
    artifacts = build_ai_semiconductor_leadership_generator_artifacts(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
        policy=policy,
        target_assets=request["target_assets"],
        horizons=request["horizons"],
        candidates=request["candidates"],
        start_date=resolved_start,
        end_date=resolved_end,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        generated_at=generated_at,
    )
    artifact_paths = write_ai_semiconductor_leadership_generator_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def build_ai_semiconductor_leadership_generator_artifacts(
    *,
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    policy_path: Path,
    policy: Mapping[str, Any],
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
    source_dates = _source_dates(
        price_matrix=price_matrix,
        required_symbols=required_symbols,
        start_date=start_date,
        end_date=end_date,
        lookback_days=_lookback_days(policy),
    )
    if not source_dates:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "TRADING-2308 generated no source dates after lookback and quality gate"
        )
    source_hashes = {
        "prices": _sha256(prices_path),
        "policy": _sha256(policy_path),
        "rates": _sha256(rates_path),
    }
    if marketstack_prices_path is not None and marketstack_prices_path.exists():
        source_hashes["marketstack_prices"] = _sha256(marketstack_prices_path)
    common = _common_payload(
        policy=policy,
        target_assets=target_assets,
        horizons=horizons,
        candidates=candidates,
        start_date=start_date,
        end_date=end_date,
        quality_report=quality_report,
        quality_report_path=quality_report_path,
        generated_at=generated_at,
        required_symbols=required_symbols,
        source_hashes=source_hashes,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        policy_path=policy_path,
    )
    candidate_bundles: dict[str, dict[str, Any]] = {}
    for candidate_id in candidates:
        bundle = _build_candidate_bundle(
            candidate_id=candidate_id,
            policy=policy,
            common=common,
            price_matrix=price_matrix,
            source_dates=source_dates,
            target_assets=target_assets,
            horizons=horizons,
            prices_path=prices_path,
            policy_path=policy_path,
            generated_at=generated_at,
        )
        candidate_bundles[candidate_id] = bundle
    top_validation = _top_level_validation(candidate_bundles)
    summary = _summary_payload(
        common=common,
        candidate_bundles=candidate_bundles,
        top_validation=top_validation,
    )
    docs = _docs_payload(summary=summary, candidate_bundles=candidate_bundles)
    policy_summary = _policy_summary(policy=policy, common=common)
    safety_boundary = _safety_boundary(common=common)
    return {
        "summary": summary,
        "policy_summary": policy_summary,
        "safety_boundary": safety_boundary,
        "top_validation": top_validation,
        "candidate_bundles": candidate_bundles,
        "docs": docs,
    }


def write_ai_semiconductor_leadership_generator_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    paths: dict[str, Any] = {
        "summary": output_dir / "ai_semiconductor_leadership_generator_poc_summary.json",
        "policy_summary": output_dir / "ai_leadership_generator_policy_summary.json",
        "safety_boundary": output_dir / "ai_leadership_generator_safety_boundary.json",
        "top_validation": output_dir / "ai_leadership_generator_validation_summary.json",
        "report_doc": docs_root / "ai_semiconductor_leadership_generator_poc.md",
        "candidates": {},
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["policy_summary"], artifacts["policy_summary"])
    write_json(paths["safety_boundary"], artifacts["safety_boundary"])
    write_json(paths["top_validation"], artifacts["top_validation"])
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
    source_dates: Sequence[pd.Timestamp],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    prices_path: Path,
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
        required_inputs=tuple(_required_inputs_for_candidate(candidate_id, candidate_policy)),
        output_signal_names=tuple(candidate_policy["output_signal_names"]),
        signal_direction_mapping=_signal_direction_mapping(candidate_policy),
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
        source_dates=source_dates,
        target_assets=target_assets,
        horizons=horizons,
        prices_path=prices_path,
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
    source_dates: Sequence[pd.Timestamp],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    prices_path: Path,
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
                    record = CandidateBoundSignalRecord(
                        candidate_id=candidate_id,
                        candidate_family=CANDIDATE_FAMILY,
                        source_experiment_id=str(signal_spec["generator_id"]),
                        source_artifact_id=f"{prices_path.stem}+{policy_path.stem}",
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
                        signal_confidence=_signal_confidence(common, policy, current_ts),
                        valid_from=decision.isoformat(),
                        valid_until=valid_until.isoformat(),
                        input_snapshot_hash=str(common["input_snapshot_hash"]),
                        feature_snapshot_hash=str(common["feature_snapshot_hash"]),
                        model_or_rule_version=f"{REPORT_TYPE}.rules.v1",
                        provenance=CandidateArtifactProvenance(
                            source_paths=[str(prices_path), str(policy_path)],
                            source_hashes=[
                                str(common["source_hashes"]["prices"]),
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
                        source_confidence=_signal_confidence(common, policy, current_ts),
                        source_prediction_flags={
                            "ai_semiconductor_leadership_generator_poc": True,
                            "required_symbol_data_quality_passed": bool(
                                common["data_quality"]["passed"]
                            ),
                            "full_universe_readiness_claimed": False,
                            "actual_path_validation_ready": False,
                        },
                    )
                    rows.append(record.to_dict())
    return clean_for_yaml(rows)


def _compute_candidate_signals(
    *,
    candidate_id: str,
    policy: Mapping[str, Any],
    candidate_policy: Mapping[str, Any],
    price_matrix: pd.DataFrame,
    current_ts: pd.Timestamp,
) -> dict[str, float]:
    if candidate_id == "smh_relative_strength_leadership_v1":
        score = _relative_score(price_matrix, "SMH", "QQQ", current_ts, policy)
        return {
            "smh_vs_qqq_relative_strength_score": score,
            "smh_overweight_confirmation_score": max(0.0, score),
            "semiconductor_leadership_weakening_score": min(0.0, score),
        }
    if candidate_id == "ai_semiconductor_leadership_quality_v1":
        weights = mapping(candidate_policy.get("component_weights"))
        smh_vs_qqq = _relative_score(price_matrix, "SMH", "QQQ", current_ts, policy)
        nvda_vs_smh = _relative_score(price_matrix, "NVDA", "SMH", current_ts, policy)
        peer_scores = [
            _relative_score(price_matrix, ticker, "SMH", current_ts, policy)
            for ticker in ("AMD", "TSM", "AVGO", "ASML")
        ]
        peer_diffusion = round_float(sum(peer_scores) / len(peer_scores))
        quality = clamp_score(
            to_float(weights.get("smh_vs_qqq")) * smh_vs_qqq
            + to_float(weights.get("nvda_vs_smh")) * nvda_vs_smh
            + to_float(weights.get("peer_diffusion_vs_smh")) * peer_diffusion
        )
        return {
            "ai_semiconductor_leadership_quality_score": quality,
            "nvda_vs_smh_leadership_score": nvda_vs_smh,
            "peer_diffusion_vs_smh_score": peer_diffusion,
        }
    if candidate_id == "ai_core_basket_leadership_v1":
        basket_symbols = _strings(candidate_policy.get("basket_symbols"))
        benchmark_symbol = str(candidate_policy.get("benchmark_symbol") or "QQQ")
        basket_score = _basket_relative_score(
            price_matrix=price_matrix,
            basket_symbols=basket_symbols,
            benchmark_symbol=benchmark_symbol,
            current_ts=current_ts,
            policy=policy,
        )
        return {
            "ai_core_basket_vs_qqq_score": basket_score,
            "ai_core_basket_leadership_confirmation_score": max(0.0, basket_score),
            "ai_core_basket_weakening_score": min(0.0, basket_score),
        }
    raise AISemiconductorLeadershipGeneratorPOCError(f"unsupported candidate: {candidate_id}")


def _prediction_artifact(
    *,
    candidate_id: str,
    common: Mapping[str, Any],
    signal_records: Sequence[Mapping[str, Any]],
    signal_spec: Mapping[str, Any],
    prices_path: Path,
    policy_path: Path,
) -> dict[str, Any]:
    if not signal_records:
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"{candidate_id} prediction artifact requires signal records"
        )
    latest = signal_records[-1]
    prediction_records = [
        {
            **dict(record),
            "prediction_record_role": "research_only_candidate_bound_signal",
            "actual_path_validation_executed": False,
            "promotion_blocker": "TRADING-2309_actual_path_validation_required",
        }
        for record in signal_records
    ]
    artifact = CandidateBoundPredictionArtifact(
        artifact_id=f"{candidate_id}_prediction_artifact",
        artifact_role=ARTIFACT_ROLE,
        candidate_id=candidate_id,
        candidate_family=CANDIDATE_FAMILY,
        source_experiment_id=str(signal_spec["generator_id"]),
        source_artifact_id=f"{prices_path.stem}+{policy_path.stem}",
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
            source_paths=[str(prices_path), str(policy_path)],
            source_hashes=[
                str(common["source_hashes"]["prices"]),
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
                "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION"
            ),
            "full_universe_validation_blocker_out_of_scope": FULL_UNIVERSE_BLOCKER,
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
        "signal_record_count": len(signal_records),
        "prediction_record_count": len(signal_records),
        "validation_status": validation_summary.get("status"),
        "data_quality": dict(common["data_quality"]),
        "policy_id": common["policy_id"],
        "policy_version": common["policy_version"],
        "candidate_signal_spec_artifact": "generated",
        "candidate_signal_series_artifact": "generated",
        "candidate_prediction_artifact": "generated",
        "actual_path_validation_blocker": (
            "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION"
        ),
        **SAFETY_FIELDS,
    }


def _summary_payload(
    *,
    common: Mapping[str, Any],
    candidate_bundles: Mapping[str, Mapping[str, Any]],
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
    return {
        **dict(common),
        "summary": {
            "candidate_count": len(candidate_bundles),
            "generated_candidate_count": len(candidate_bundles),
            "candidate_signal_series_generated": True,
            "candidate_prediction_artifact_generated": True,
            "validation_status": top_validation["status"],
            "data_quality_status": common["data_quality"]["status"],
            "full_universe_readiness_claimed": False,
            "actual_path_validation_ready": False,
            "recommended_next_task": "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION",
        },
        "candidate_rows": candidate_rows,
        "top_validation": dict(top_validation),
        **SAFETY_FIELDS,
    }


def _common_payload(
    *,
    policy: Mapping[str, Any],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    candidates: Sequence[str],
    start_date: date,
    end_date: date,
    quality_report: DataQualityReport,
    quality_report_path: Path,
    generated_at: datetime,
    required_symbols: Sequence[str],
    source_hashes: Mapping[str, str],
    prices_path: Path,
    rates_path: Path,
    marketstack_prices_path: Path | None,
    policy_path: Path,
) -> dict[str, Any]:
    data_quality = _data_quality_payload(quality_report, quality_report_path)
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "AI / 半导体 Leadership Generator POC",
        "task_id": TASK_ID,
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
        "candidate_family": CANDIDATE_FAMILY,
        "target_assets": list(target_assets),
        "horizons": list(horizons),
        "candidates": list(candidates),
        "required_price_symbols": list(required_symbols),
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
        "full_universe_validation_blocker_out_of_scope": FULL_UNIVERSE_BLOCKER,
        "input_snapshot_hash": _stable_hash(
            {
                "required_symbols": list(required_symbols),
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
        "full_universe_validation_blocker_out_of_scope": FULL_UNIVERSE_BLOCKER,
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
        "full_universe_readiness_claimed": False,
        "does_not_use_weights_or_market_cap_concentration": True,
        "does_not_use_earnings_capex_or_event_outcomes": True,
        "does_not_run_actual_path_validation": True,
        "does_not_reopen_generic_risk_appetite_current_form": True,
        "next_required_task": "TRADING-2309_AI_LEADERSHIP_ACTUAL_PATH_VALIDATION",
        **SAFETY_FIELDS,
    }


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
) -> dict[str, str]:
    rows = [
        (
            f"|`{candidate_id}`|`{bundle['generation_summary']['signal_record_count']}`|"
            f"`{bundle['validation_summary']['status']}`|`False`|"
        )
        for candidate_id, bundle in candidate_bundles.items()
    ]
    report = "\n".join(
        [
            "# AI / 半导体 Leadership Generator POC",
            "",
            "TRADING-2308 生成 research-only candidate-bound price-proxy POC artifacts。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['market_regime']}`",
            f"- actual_requested_date_range: `{summary['actual_requested_date_range']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_quality_report_path: `{summary['data_quality_report_path']}`",
            f"- policy_version: `{summary['policy_id']}:{summary['policy_version']}`",
            "- full_universe_readiness_claimed: `False`",
            f"- full_universe_validation_blocker_out_of_scope: `{FULL_UNIVERSE_BLOCKER}`",
            "- actual_path_validation_ready: `False`",
            "",
            "## Candidates",
            "",
            "|candidate_id|signal_record_count|validation_status|actual_path_validation_ready|",
            "|---|---:|---|---|",
            *rows,
            "",
            "## Safety",
            "",
            "本 POC 只使用 adjusted close relative-strength price proxy；不使用 weights、"
            "market cap concentration、earnings、capex 或 event outcome。",
            "",
            "本报告不得用于 actual-path validation、promotion、paper-shadow、"
            "production 或 broker action。",
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
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"AI / 半导体 leadership generator POC 只支持 {MODE}"
        )
    parsed_assets = _parse_list(target_assets, uppercase=True)
    parsed_horizons = _parse_list(horizons, uppercase=False)
    parsed_candidates = _parse_list(candidates, uppercase=False)
    if not parsed_assets:
        raise AISemiconductorLeadershipGeneratorPOCError("--target-assets is required")
    if not parsed_horizons:
        raise AISemiconductorLeadershipGeneratorPOCError("--horizons is required")
    if not parsed_candidates:
        raise AISemiconductorLeadershipGeneratorPOCError("--candidates is required")
    unsupported = sorted(set(parsed_candidates) - set(DEFAULT_CANDIDATES))
    if unsupported:
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"unsupported TRADING-2308 candidates: {unsupported}"
        )
    return {
        "target_assets": parsed_assets,
        "horizons": parsed_horizons,
        "candidates": parsed_candidates,
    }


def _load_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AISemiconductorLeadershipGeneratorPOCError(f"policy file missing: {path}")
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise AISemiconductorLeadershipGeneratorPOCError("policy must be a mapping")
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
        "signal_policy",
        "candidate_policy",
        "safety",
    )
    missing = [key for key in required if key not in policy]
    if missing:
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"policy missing required fields: {missing}"
        )
    for candidate_id in DEFAULT_CANDIDATES:
        _candidate_policy(policy, candidate_id)
    return policy


def _validate_feasibility_source(feasibility_dir: Path) -> None:
    summary_path = feasibility_dir / "ai_semiconductor_leadership_feasibility_summary.json"
    if not summary_path.exists():
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"TRADING-2307 feasibility summary missing: {summary_path}"
        )
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    expected_status = (
        "AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY"
    )
    if payload.get("status") != expected_status:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "TRADING-2307 feasibility summary is not ready for price-proxy POC"
        )
    if payload.get("promotion_allowed") is not False:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "TRADING-2307 source unexpectedly allows promotion"
        )


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


def _source_dates(
    *,
    price_matrix: pd.DataFrame,
    required_symbols: Sequence[str],
    start_date: date,
    end_date: date,
    lookback_days: int,
) -> list[pd.Timestamp]:
    clean = price_matrix.loc[:, list(required_symbols)].dropna(how="any")
    dates = [
        ts
        for ts in clean.index
        if ts.date() >= start_date and ts.date() <= end_date
    ]
    output: list[pd.Timestamp] = []
    for ts in dates:
        position = price_matrix.index.get_loc(ts)
        if isinstance(position, int) and position >= lookback_days:
            output.append(ts)
    return output


def _relative_score(
    price_matrix: pd.DataFrame,
    leader: str,
    benchmark: str,
    current_ts: pd.Timestamp,
    policy: Mapping[str, Any],
) -> float:
    leader_return = rolling_return(price_matrix, leader, current_ts, _lookback_days(policy))
    benchmark_return = rolling_return(
        price_matrix,
        benchmark,
        current_ts,
        _lookback_days(policy),
    )
    if leader_return is None or benchmark_return is None:
        return 0.0
    return clamp_score((leader_return - benchmark_return) / _score_scale(policy))


def _basket_relative_score(
    *,
    price_matrix: pd.DataFrame,
    basket_symbols: Sequence[str],
    benchmark_symbol: str,
    current_ts: pd.Timestamp,
    policy: Mapping[str, Any],
) -> float:
    basket_returns = [
        rolling_return(price_matrix, symbol, current_ts, _lookback_days(policy))
        for symbol in basket_symbols
    ]
    benchmark_return = rolling_return(
        price_matrix,
        benchmark_symbol,
        current_ts,
        _lookback_days(policy),
    )
    if benchmark_return is None or any(item is None for item in basket_returns):
        return 0.0
    basket_return = sum(float(item) for item in basket_returns if item is not None) / len(
        basket_returns
    )
    return clamp_score((basket_return - benchmark_return) / _score_scale(policy))


def _signal_direction(value: float, policy: Mapping[str, Any]) -> str:
    neutral_band = _policy_float(policy, "neutral_band")
    if value > neutral_band:
        return "risk_on"
    if value < -neutral_band:
        return "risk_off"
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
    confidence = max(_policy_float(policy, "confidence_floor"), confidence)
    return round_float(min(1.0, confidence))


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


def _candidate_policy(policy: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
    raw = mapping(policy.get("candidate_policy")).get(candidate_id)
    if not isinstance(raw, Mapping):
        raise AISemiconductorLeadershipGeneratorPOCError(
            f"policy missing candidate policy: {candidate_id}"
        )
    return dict(raw)


def _required_symbols(
    *,
    policy: Mapping[str, Any],
    candidates: Sequence[str],
) -> tuple[str, ...]:
    symbols: list[str] = []
    for candidate_id in candidates:
        candidate_policy = _candidate_policy(policy, candidate_id)
        symbols.extend(_strings(candidate_policy.get("required_symbols")))
        symbols.extend(_strings(candidate_policy.get("basket_symbols")))
        benchmark_symbol = candidate_policy.get("benchmark_symbol")
        if benchmark_symbol:
            symbols.append(str(benchmark_symbol))
    configured = _strings(mapping(policy.get("data_quality")).get("required_price_symbols"))
    symbols.extend(configured)
    return tuple(dict.fromkeys(symbol.upper() for symbol in symbols if symbol))


def _required_inputs_for_candidate(
    candidate_id: str,
    candidate_policy: Mapping[str, Any],
) -> list[str]:
    if candidate_id == "ai_core_basket_leadership_v1":
        return [
            "adjusted_close_price_cache",
            "pre_registered_ai_core_basket_policy",
            *_strings(candidate_policy.get("basket_symbols")),
            str(candidate_policy.get("benchmark_symbol") or "QQQ"),
        ]
    return [
        "adjusted_close_price_cache",
        *_strings(candidate_policy.get("required_symbols")),
    ]


def _signal_direction_mapping(candidate_policy: Mapping[str, Any]) -> dict[str, str]:
    return {
        f"{signal_name}_positive": "risk_on"
        for signal_name in _strings(candidate_policy.get("output_signal_names"))
    } | {
        f"{signal_name}_negative": "risk_off"
        for signal_name in _strings(candidate_policy.get("output_signal_names"))
    } | {
        f"{signal_name}_near_zero": "neutral"
        for signal_name in _strings(candidate_policy.get("output_signal_names"))
    }


def _resolve_start_date(value: str | date | None, policy: Mapping[str, Any]) -> date:
    if isinstance(value, date):
        return value
    if value:
        return date.fromisoformat(str(value))
    return date.fromisoformat(str(policy.get("default_start_date") or DEFAULT_BACKTEST_START))


def _resolve_end_date(
    *,
    end_date: str | date | None,
    prices_path: Path,
    required_symbols: Sequence[str],
) -> date:
    if isinstance(end_date, date):
        return end_date
    if end_date and str(end_date).lower() != "latest":
        return date.fromisoformat(str(end_date))
    return _latest_common_price_date(prices_path, required_symbols)


def _resolve_quality_as_of(
    *,
    quality_as_of: str | date | None,
    prices_path: Path,
    required_symbols: Sequence[str],
    resolved_end: date,
) -> date:
    if isinstance(quality_as_of, date):
        return quality_as_of
    if quality_as_of and str(quality_as_of).lower() != "latest":
        return date.fromisoformat(str(quality_as_of))
    latest_common = _latest_common_price_date(prices_path, required_symbols)
    return min(latest_common, resolved_end)


def _latest_common_price_date(prices_path: Path, required_symbols: Sequence[str]) -> date:
    if not prices_path.exists():
        return max_price_date(prices_path)
    matrix = load_adjusted_price_matrix(prices_path, required_symbols)
    clean = matrix.dropna(how="any")
    if clean.empty:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "price cache has no common date for TRADING-2308 required symbols"
        )
    return pd.Timestamp(clean.index.max()).date()


def _lookback_days(policy: Mapping[str, Any]) -> int:
    value = mapping(mapping(policy.get("signal_policy")).get("lookback_trading_days")).get(
        "value"
    )
    return int(value)


def _score_scale(policy: Mapping[str, Any]) -> float:
    return _policy_float(policy, "relative_strength_score_scale")


def _policy_float(policy: Mapping[str, Any], key: str) -> float:
    value = mapping(mapping(policy.get("signal_policy")).get(key)).get("value")
    if value is None:
        raise AISemiconductorLeadershipGeneratorPOCError(f"policy missing {key}.value")
    parsed = to_float(value)
    if parsed <= 0.0 and key != "neutral_band":
        raise AISemiconductorLeadershipGeneratorPOCError(f"policy {key}.value must be > 0")
    return parsed


def _parse_horizon_days(horizon: str) -> int:
    text = horizon.strip().lower()
    if not text.endswith("d"):
        raise AISemiconductorLeadershipGeneratorPOCError(
            "horizon must use day suffix, e.g. 10d"
        )
    try:
        days = int(text[:-1])
    except ValueError as exc:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "horizon day count must be an integer"
        ) from exc
    if days <= 0:
        raise AISemiconductorLeadershipGeneratorPOCError(
            "horizon day count must be positive"
        )
    return days


def _parse_list(value: str | Sequence[str], *, uppercase: bool) -> list[str]:
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    else:
        raw = [str(item).strip() for item in value]
    parsed = [item.upper() if uppercase else item for item in raw if item]
    return parsed


def _strings(value: Any) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item).strip()]
    return []


def _write_signal_series_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(candidate_bound_signal_series_contract_dict()["required_columns"])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            output = dict(row)
            output["provenance"] = json.dumps(
                mapping(output.get("provenance")),
                ensure_ascii=False,
                sort_keys=True,
            )
            output["source_prediction_flags"] = json.dumps(
                mapping(output.get("source_prediction_flags")),
                ensure_ascii=False,
                sort_keys=True,
            )
            writer.writerow({field: output.get(field) for field in fieldnames})


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    manifest_path = prices_path.parent / "download_manifest.csv"
    return manifest_path if manifest_path.exists() else None


def _clean_paths(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _clean_paths(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_paths(item) for item in value]
    if isinstance(value, tuple):
        return [_clean_paths(item) for item in value]
    return clean_for_yaml(value)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(clean_for_yaml(dict(payload)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "DEFAULT_FEASIBILITY_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_POLICY_PATH",
    "MODE",
    "run_ai_semiconductor_leadership_generator_poc",
]
