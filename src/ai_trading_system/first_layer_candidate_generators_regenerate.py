from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_generator_runtime import (
    FirstLayerCandidateGeneratorRuntime,
    _write_signal_series_csv,
    generator_registry_payload,
    validate_candidate_generation_bundle,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    REGENERATED_CANDIDATE_GENERATOR_MODE,
    REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
    CandidateGenerationBundle,
    CandidateGeneratorContext,
    CandidateGeneratorError,
    candidate_artifact_safety_fields,
    generator_operation_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    clean_for_yaml,
    validate_cached_market_data,
    write_json,
)
from ai_trading_system.regenerated_candidate_generator_common import (
    parse_csv_list,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_candidate_generators_regenerated"
)
TASK_ID = "TRADING-2284_TREND_RISK_VOLATILITY_EXECUTABLE_CANDIDATE_GENERATORS"
STATUS = "REGENERATED_CANDIDATE_ARTIFACTS_READY_ACTUAL_PATH_VALIDATION_BLOCKED"
NEXT_TASK = "TRADING-2285_Regenerated_Candidate_Actual_Path_Validation"
DEFAULT_CANDIDATES = (
    "baseline_plus_trend_structure",
    "risk_appetite",
    "volatility_regime",
)


def run_first_layer_candidate_generators_regenerate(
    *,
    candidates: Sequence[str] | str,
    target_assets: Sequence[str] | str,
    start_date: date,
    end_date: date,
    horizons: Sequence[str] | str,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = REGENERATED_CANDIDATE_GENERATOR_MODE,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
) -> dict[str, Any]:
    if mode != REGENERATED_CANDIDATE_GENERATOR_MODE:
        raise CandidateGeneratorError(
            "first-layer candidate regeneration only supports "
            "regenerated_candidate_artifacts mode"
        )
    candidate_ids = _normalize_list(candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    if start_date > end_date:
        raise CandidateGeneratorError("start_date must be <= end_date")

    data_quality = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=None,
        expected_price_tickers=asset_ids,
        expected_rate_series=(),
    )
    runtime = FirstLayerCandidateGeneratorRuntime()
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    registry_payload = generator_registry_payload(
        runtime.registry,
        generated_at=generated_at,
        task_id=TASK_ID,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "generator_registry.json", registry_payload)

    candidate_results: list[dict[str, Any]] = []
    validation_summaries: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        generator = runtime.registry.get_generator(candidate_id)
        source_path = prices_path.resolve()
        generator_source_path = Path(inspect.getfile(generator.__class__)).resolve()
        source_hash = _sha256(source_path)
        generator_source_hash = _sha256(generator_source_path)
        context = CandidateGeneratorContext(
            candidate_id=candidate_id,
            candidate_family=generator.candidate_family,
            target_asset=",".join(asset_ids),
            start_date=start_date,
            end_date=end_date,
            horizon=",".join(horizon_ids),
            output_dir=output_dir / candidate_id,
            mode=mode,
            generated_at=generated_at,
            signal_spec_version=SIGNAL_SPEC_VERSION,
            prediction_schema_version=PREDICTION_SCHEMA_VERSION,
            input_snapshot_hash=_stable_hash(
                {
                    "snapshot": "input",
                    "candidate_id": candidate_id,
                    "target_assets": asset_ids,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "horizons": horizon_ids,
                    "mode": mode,
                    "source_hash": source_hash,
                }
            ),
            feature_snapshot_hash=_stable_hash(
                {
                    "snapshot": "feature",
                    "candidate_id": candidate_id,
                    "target_assets": asset_ids,
                    "horizons": horizon_ids,
                    "generator_version": generator.generator_version,
                    "generator_source_hash": generator_source_hash,
                }
            ),
            source_paths=(source_path, generator_source_path),
            source_hashes=(source_hash, generator_source_hash),
        )
        bundle = runtime.run(context)
        validation_summary = validate_candidate_generation_bundle(bundle, task_id=TASK_ID)
        validation_summaries.append(validation_summary)
        artifact_paths, generation_summary = _write_regenerated_candidate_bundle(
            bundle,
            validation_summary=validation_summary,
        )
        if validation_summary["status"] != "PASS":
            raise CandidateGeneratorError(
                f"{candidate_id} validation failed: "
                + "; ".join(str(error) for error in validation_summary["errors"])
            )
        candidate_results.append(
            {
                "candidate_id": candidate_id,
                "candidate_family": generator.candidate_family,
                "generator_version": generator.generator_version,
                "artifact_paths": {key: str(path) for key, path in artifact_paths.items()},
                "generation_summary": generation_summary,
                "validation_summary": validation_summary,
            }
        )

    top_validation = _top_level_validation_summary(
        candidate_results=candidate_results,
        validation_summaries=validation_summaries,
        generated_at=generated_at,
    )
    run_summary = _run_summary(
        candidate_ids=candidate_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        start_date=start_date,
        end_date=end_date,
        generated_at=generated_at,
        candidate_results=candidate_results,
        data_quality=data_quality,
        top_validation=top_validation,
    )
    write_json(output_dir / "validation_summary.json", top_validation)
    write_json(output_dir / "regeneration_run_summary.json", run_summary)
    return {
        "schema_version": "first_layer_candidate_generators_regeneration_result.v1",
        "report_type": "first_layer_candidate_generators_regeneration",
        "title": "Trend / Risk / Volatility Executable Candidate Generators",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "summary": run_summary,
        "candidate_results": candidate_results,
        "validation_summary": top_validation,
        "data_quality": data_quality,
        "artifact_paths": {
            "generator_registry_json": str(output_dir / "generator_registry.json"),
            "regeneration_run_summary_json": str(
                output_dir / "regeneration_run_summary.json"
            ),
            "validation_summary_json": str(output_dir / "validation_summary.json"),
        },
        "research_only": True,
        "dynamic_promotion_status": "BLOCKED",
        **candidate_artifact_safety_fields(),
        "actual_path_validation_ready": False,
        "historical_executable_artifact": True,
        **trading_2281_boundary_fields(),
    }


def _write_regenerated_candidate_bundle(
    bundle: CandidateGenerationBundle,
    *,
    validation_summary: Mapping[str, Any],
) -> tuple[dict[str, Path], dict[str, Any]]:
    output_dir = bundle.context.output_dir
    paths = {
        "candidate_signal_spec_json": output_dir / "candidate_signal_spec.json",
        "candidate_signal_series_csv": output_dir / "candidate_signal_series.csv",
        "candidate_prediction_artifact_json": output_dir
        / "candidate_prediction_artifact.json",
        "candidate_generation_summary_json": output_dir / "generation_summary.json",
        "candidate_validation_summary_json": output_dir / "validation_summary.json",
    }
    generation_summary = _candidate_generation_summary(
        bundle,
        paths=paths,
        validation_summary=validation_summary,
    )
    write_json(paths["candidate_signal_spec_json"], bundle.signal_spec.to_dict())
    _write_signal_series_csv(paths["candidate_signal_series_csv"], bundle.signal_payloads())
    write_json(paths["candidate_prediction_artifact_json"], bundle.prediction_artifact)
    write_json(paths["candidate_generation_summary_json"], generation_summary)
    write_json(paths["candidate_validation_summary_json"], dict(validation_summary))
    return paths, generation_summary


def _candidate_generation_summary(
    bundle: CandidateGenerationBundle,
    *,
    paths: Mapping[str, Path],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    records = bundle.signal_payloads()
    missing_inputs = sorted(
        {
            str(item)
            for record in records
            for item in _provenance_list(record, "missing_inputs")
        }
    )
    proxy_input_used = any(
        bool(_provenance(record).get("proxy_input_used")) for record in records
    )
    volatility_proxy_modes = sorted(
        {
            str(_provenance(record).get("volatility_proxy_mode"))
            for record in records
            if str(_provenance(record).get("volatility_proxy_mode") or "")
        }
    )
    summary = {
        "task_id": TASK_ID,
        "candidate_id": bundle.context.candidate_id,
        "candidate_family": bundle.context.candidate_family,
        "target_assets": parse_csv_list(bundle.context.target_asset),
        "requested_start_date": bundle.context.start_date.isoformat(),
        "requested_end_date": bundle.context.end_date.isoformat(),
        "horizons": parse_csv_list(bundle.context.horizon),
        "mode": bundle.context.mode,
        "artifact_role": REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
        "signal_record_count": len(records),
        "prediction_record_count": len(
            bundle.prediction_artifact.get("prediction_records", [])
        ),
        "validation_status": validation_summary.get("status"),
        "candidate_signal_spec_artifact": "generated",
        "candidate_signal_series_artifact": "generated",
        "candidate_prediction_artifact": "generated",
        "candidate_binding_validator_reused": True,
        "missing_inputs": missing_inputs,
        "proxy_input_used": proxy_input_used,
        "volatility_proxy_modes": volatility_proxy_modes,
        "historical_executable_artifact": True,
        "actual_path_validation_ready": False,
        "next_task": NEXT_TASK,
        **candidate_artifact_safety_fields(),
        "dynamic_promotion_status": "BLOCKED",
        **trading_2281_boundary_fields(),
    }
    return clean_for_yaml(
        {
            "schema_version": "regenerated_candidate_generation_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": bundle.context.generated_at.isoformat(),
            "summary": summary,
            "context": bundle.context.to_dict(),
            "signal_spec": bundle.signal_spec.to_dict(),
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "validation_summary": dict(validation_summary),
            **candidate_artifact_safety_fields(),
            "historical_executable_artifact": True,
            "actual_path_validation_ready": False,
            "dynamic_promotion_status": "BLOCKED",
        }
    )


def _top_level_validation_summary(
    *,
    candidate_results: Sequence[Mapping[str, Any]],
    validation_summaries: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    errors = [
        str(error)
        for validation in validation_summaries
        for error in validation.get("errors", [])
    ]
    return {
        "schema_version": "first_layer_candidate_generators_regeneration_validation.v1",
        "task_id": TASK_ID,
        "status": "PASS" if not errors else "FAIL",
        "generated_at": generated_at.isoformat(),
        "candidate_count": len(candidate_results),
        "candidate_ids": [str(item.get("candidate_id")) for item in candidate_results],
        "candidate_validation_statuses": {
            str(item.get("candidate_id")): item.get("validation_summary", {}).get("status")
            for item in candidate_results
        },
        "candidate_bound_validator_reused": True,
        "errors": errors,
        **generator_operation_safety_fields(),
        "actual_path_validation_ready": False,
        "permanently_inconclusive_override_allowed": False,
        **trading_2281_boundary_fields(),
    }


def _run_summary(
    *,
    candidate_ids: Sequence[str],
    target_assets: Sequence[str],
    horizons: Sequence[str],
    start_date: date,
    end_date: date,
    generated_at: datetime,
    candidate_results: Sequence[Mapping[str, Any]],
    data_quality: Mapping[str, Any],
    top_validation: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": "first_layer_candidate_generators_regeneration_run_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "candidate_count": len(candidate_ids),
            "candidate_ids": list(candidate_ids),
            "target_assets": list(target_assets),
            "horizons": list(horizons),
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "data_quality_status": data_quality.get("status"),
            "data_quality": dict(data_quality),
            "candidate_artifact_roles": {
                str(item.get("candidate_id")): REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE
                for item in candidate_results
            },
            "validation_status": top_validation.get("status"),
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "actual_path_validation_ready": False,
            "historical_executable_artifact": True,
            "permanently_inconclusive_override_allowed": False,
            "next_task": NEXT_TASK,
            **trading_2281_boundary_fields(),
        }
    )


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    parsed = tuple(str(item).strip() for item in value if str(item).strip())
    if not parsed:
        raise CandidateGeneratorError("input list must be non-empty")
    return parsed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(clean_for_yaml(dict(payload)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _provenance(record: Mapping[str, Any]) -> dict[str, Any]:
    raw = record.get("provenance")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _provenance_list(record: Mapping[str, Any], field: str) -> list[Any]:
    raw = _provenance(record).get(field)
    return list(raw) if isinstance(raw, list | tuple) else []
