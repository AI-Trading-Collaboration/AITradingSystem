from __future__ import annotations

import csv
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
    candidate_bound_signal_series_contract_dict,
)
from ai_trading_system.candidate_signal_binding_validator import CandidateSignalBindingValidator
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_generator_registry import (
    CandidateGeneratorRegistry,
    default_candidate_generator_registry,
)
from ai_trading_system.first_layer_candidate_signal_generator import (
    FRAMEWORK_SMOKE_ARTIFACT_ROLE,
    REGENERATED_CANDIDATE_GENERATOR_MODE,
    REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
    CandidateGenerationBundle,
    CandidateGeneratorContext,
    CandidateGeneratorError,
    CandidateGeneratorResult,
    candidate_artifact_safety_fields,
    framework_smoke_artifact_safety_fields,
    generator_operation_safety_fields,
    trading_2281_boundary_fields,
)
from ai_trading_system.post_2085_research_common import clean_for_yaml, mapping, write_json

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_candidate_generators"
)
TASK_ID = "TRADING-2283_FIRST_LAYER_EXECUTABLE_CANDIDATE_SIGNAL_GENERATOR_FRAMEWORK"
STATUS = "FIRST_LAYER_CANDIDATE_GENERATOR_FRAMEWORK_READY_PROMOTION_BLOCKED"


class FirstLayerCandidateGeneratorRuntime:
    def __init__(self, registry: CandidateGeneratorRegistry | None = None) -> None:
        self.registry = registry or default_candidate_generator_registry()

    def run(self, context: CandidateGeneratorContext) -> CandidateGenerationBundle:
        generator = self.registry.get_generator(context.candidate_id)
        signal_spec = generator.build_signal_spec(context)
        signal_records = generator.generate_signal_series(context, signal_spec)
        raw_prediction_artifact = generator.generate_prediction_artifact(
            context,
            signal_spec,
            signal_records,
        )
        prediction_artifact = (
            raw_prediction_artifact.to_dict()
            if hasattr(raw_prediction_artifact, "to_dict")
            else dict(raw_prediction_artifact)
        )
        return CandidateGenerationBundle(
            context=context,
            signal_spec=signal_spec,
            signal_records=signal_records,
            prediction_artifact=prediction_artifact,
        )


def run_candidate_generator(
    *,
    candidate_id: str,
    target_asset: str,
    start_date: date,
    end_date: date,
    horizon: str,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = FRAMEWORK_SMOKE_ARTIFACT_ROLE,
    registry: CandidateGeneratorRegistry | None = None,
) -> dict[str, Any]:
    runtime = FirstLayerCandidateGeneratorRuntime(registry=registry)
    generator = runtime.registry.get_generator(candidate_id)
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    source_path = Path(inspect.getfile(generator.__class__)).resolve()
    source_hash = _sha256(source_path)
    input_snapshot_hash = _stable_hash(
        {
            "snapshot": "input",
            "candidate_id": candidate_id,
            "target_asset": target_asset,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "horizon": horizon,
            "mode": mode,
            "source_hash": source_hash,
        }
    )
    feature_snapshot_hash = _stable_hash(
        {
            "snapshot": "feature",
            "candidate_id": candidate_id,
            "target_asset": target_asset,
            "horizon": horizon,
            "generator_version": generator.generator_version,
            "source_hash": source_hash,
        }
    )
    context = CandidateGeneratorContext(
        candidate_id=candidate_id,
        candidate_family=generator.candidate_family,
        target_asset=target_asset,
        start_date=start_date,
        end_date=end_date,
        horizon=horizon,
        output_dir=output_dir,
        mode=mode,
        generated_at=generated_at,
        signal_spec_version=SIGNAL_SPEC_VERSION,
        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
        input_snapshot_hash=input_snapshot_hash,
        feature_snapshot_hash=feature_snapshot_hash,
        source_paths=(source_path,),
        source_hashes=(source_hash,),
    )
    bundle = runtime.run(context)
    validation_summary = validate_candidate_generation_bundle(bundle)
    if validation_summary["status"] != "PASS":
        raise CandidateGeneratorError(
            "candidate generation validation failed: "
            + "; ".join(str(error) for error in validation_summary["errors"])
        )
    registry_payload = generator_registry_payload(runtime.registry, generated_at=generated_at)
    artifact_paths, generation_summary = write_candidate_generation_bundle(
        bundle,
        registry_payload=registry_payload,
        validation_summary=validation_summary,
    )
    result = CandidateGeneratorResult(
        status=STATUS,
        candidate_id=candidate_id,
        artifact_paths={key: str(path) for key, path in artifact_paths.items()},
        generation_summary=generation_summary,
        validation_summary=validation_summary,
    )
    return {
        "schema_version": "first_layer_candidate_generator_framework_result.v1",
        "report_type": "first_layer_candidate_generator_framework",
        "title": "First-Layer Executable Candidate Signal Generator Framework",
        "task_id": TASK_ID,
        "status": STATUS,
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "candidate_id": candidate_id,
        "candidate_family": generator.candidate_family,
        "target_asset": target_asset,
        "horizon": horizon,
        "summary": generation_summary["summary"],
        "generation_summary": generation_summary,
        "validation_summary": validation_summary,
        "generator_registry": registry_payload,
        "artifact_paths": result.artifact_paths,
        "research_only": True,
        **framework_smoke_artifact_safety_fields(),
        "dynamic_promotion_status": "BLOCKED",
        **trading_2281_boundary_fields(),
    }


def write_candidate_generation_bundle(
    bundle: CandidateGenerationBundle,
    *,
    registry_payload: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
) -> tuple[dict[str, Path], dict[str, Any]]:
    candidate_id = bundle.context.candidate_id
    output_dir = bundle.context.output_dir
    paths = {
        "generator_registry_json": output_dir / "generator_registry.json",
        "candidate_signal_spec_json": output_dir / f"{candidate_id}_signal_spec.json",
        "candidate_signal_series_csv": output_dir / f"{candidate_id}_signal_series.csv",
        "candidate_prediction_artifact_json": output_dir
        / f"{candidate_id}_prediction_artifact.json",
        "candidate_generation_summary_json": output_dir
        / f"{candidate_id}_generation_summary.json",
        "candidate_validation_summary_json": output_dir
        / f"{candidate_id}_validation_summary.json",
    }
    generation_summary = _generation_summary(bundle, paths, validation_summary)
    write_json(paths["generator_registry_json"], registry_payload)
    write_json(paths["candidate_signal_spec_json"], bundle.signal_spec.to_dict())
    _write_signal_series_csv(paths["candidate_signal_series_csv"], bundle.signal_payloads())
    write_json(paths["candidate_prediction_artifact_json"], bundle.prediction_artifact)
    write_json(paths["candidate_generation_summary_json"], generation_summary)
    write_json(paths["candidate_validation_summary_json"], validation_summary)
    return paths, generation_summary


def validate_candidate_generation_bundle(
    bundle: CandidateGenerationBundle,
    *,
    task_id: str = TASK_ID,
) -> dict[str, Any]:
    validator = CandidateSignalBindingValidator()
    spec_validation = validator.validate_candidate_signal_spec(bundle.signal_spec.to_dict())
    signal_payloads = bundle.signal_payloads()
    signal_validation = validator.validate_candidate_bound_signal_series(signal_payloads)
    prediction_validation = validator.validate_candidate_bound_prediction_artifact(
        bundle.prediction_artifact
    )
    errors = (
        list(spec_validation.errors)
        + list(signal_validation.errors)
        + list(prediction_validation.errors)
        + _bundle_integrity_errors(bundle, signal_payloads)
    )
    return {
        "schema_version": "first_layer_candidate_generator_validation_summary.v1",
        "task_id": task_id,
        "status": "PASS" if not errors else "FAIL",
        "candidate_id": bundle.context.candidate_id,
        "mode": bundle.context.mode,
        "signal_spec_validation": spec_validation.to_dict(),
        "signal_series_validation": signal_validation.to_dict(),
        "prediction_artifact_validation": prediction_validation.to_dict(),
        "checked_signal_record_count": len(signal_payloads),
        "checked_prediction_record_count": len(
            bundle.prediction_artifact.get("prediction_records", [])
        )
        if isinstance(bundle.prediction_artifact.get("prediction_records"), list)
        else 0,
        "candidate_bound_validator_reused": True,
        "candidate_bound_minimum_fields_satisfied": not errors,
        "errors": errors,
        **generator_operation_safety_fields(),
        "permanently_inconclusive_override_allowed": False,
        **trading_2281_boundary_fields(),
    }


def generator_registry_payload(
    registry: CandidateGeneratorRegistry,
    *,
    generated_at: datetime,
    task_id: str = TASK_ID,
) -> dict[str, Any]:
    generators = registry.list_generators()
    return {
        "schema_version": "first_layer_candidate_generator_registry.v1",
        "artifact_role": "generator_registry",
        "task_id": task_id,
        "generated_at": generated_at.isoformat(),
        "generator_count": len(generators),
        "generators": generators,
        **generator_operation_safety_fields(),
    }


def _bundle_integrity_errors(
    bundle: CandidateGenerationBundle,
    signal_payloads: Sequence[Mapping[str, Any]],
) -> list[str]:
    errors: list[str] = []
    context = bundle.context
    spec = bundle.signal_spec.to_dict()
    artifact = bundle.prediction_artifact
    if spec.get("candidate_id") != context.candidate_id:
        errors.append("bundle: signal spec candidate_id does not match context")
    if spec.get("target_asset") != context.target_asset:
        errors.append("bundle: signal spec target_asset does not match context")
    supported_horizons = set(_as_strings(spec.get("supported_horizons")))
    context_horizons = {item.strip() for item in context.horizon.split(",") if item.strip()}
    if not context_horizons.issubset(supported_horizons):
        errors.append("bundle: context horizon missing from supported_horizons")
    for index, row in enumerate(signal_payloads):
        scope = f"bundle.signal_series[{index}]"
        _require_equal(row, "candidate_id", context.candidate_id, scope, errors)
        if not _context_allows_value(context.target_asset, row.get("target_asset")):
            errors.append(f"{scope}: target_asset must be within context target_asset")
        if not _context_allows_value(context.horizon, row.get("horizon")):
            errors.append(f"{scope}: horizon must be within context horizon")
        _require_false(row, "promotion_eligible", scope, errors)
        _require_false(row, "promotion_allowed", scope, errors)
        _require_false(row, "paper_shadow_allowed", scope, errors)
        _require_false(row, "production_allowed", scope, errors)
        _require_equal(row, "broker_action", "none", scope, errors)
    prediction_records = artifact.get("prediction_records")
    if not isinstance(prediction_records, list):
        errors.append("bundle: prediction_records is not list")
        prediction_records = []
    if len(prediction_records) != len(signal_payloads):
        errors.append("bundle: prediction_records count does not match signal series rows")
    _require_equal(artifact, "candidate_id", context.candidate_id, "bundle.prediction", errors)
    if context.mode == REGENERATED_CANDIDATE_GENERATOR_MODE:
        _require_equal(
            artifact,
            "artifact_role",
            REGENERATED_EXECUTABLE_CANDIDATE_ARTIFACT_ROLE,
            "bundle.prediction",
            errors,
        )
        _require_true(
            artifact,
            "historical_executable_artifact",
            "bundle.prediction",
            errors,
        )
    else:
        _require_equal(
            artifact,
            "artifact_role",
            FRAMEWORK_SMOKE_ARTIFACT_ROLE,
            "bundle.prediction",
            errors,
        )
        _require_false(
            artifact,
            "historical_executable_artifact",
            "bundle.prediction",
            errors,
        )
    _require_false(artifact, "actual_path_validation_ready", "bundle.prediction", errors)
    _require_false(artifact, "promotion_eligible", "bundle.prediction", errors)
    _require_false(artifact, "promotion_allowed", "bundle.prediction", errors)
    _require_false(artifact, "paper_shadow_allowed", "bundle.prediction", errors)
    _require_false(artifact, "production_allowed", "bundle.prediction", errors)
    _require_equal(artifact, "broker_action", "none", "bundle.prediction", errors)
    if not str(artifact.get("source_artifact_hash") or "").strip():
        errors.append("bundle.prediction: missing source_artifact_hash")
    for index, record in enumerate(prediction_records):
        if isinstance(record, Mapping):
            _require_equal(
                record,
                "candidate_id",
                context.candidate_id,
                f"bundle.prediction_records[{index}]",
                errors,
            )
    return errors


def _generation_summary(
    bundle: CandidateGenerationBundle,
    paths: Mapping[str, Path],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    context = bundle.context
    summary = {
        "task_id": TASK_ID,
        "candidate_id": context.candidate_id,
        "candidate_family": context.candidate_family,
        "target_asset": context.target_asset,
        "requested_start_date": context.start_date.isoformat(),
        "requested_end_date": context.end_date.isoformat(),
        "horizon": context.horizon,
        "mode": context.mode,
        "signal_record_count": len(bundle.signal_records),
        "prediction_record_count": len(bundle.prediction_artifact.get("prediction_records", [])),
        "validation_status": validation_summary.get("status"),
        "candidate_signal_spec_artifact": "generated",
        "candidate_signal_series_artifact": "generated",
        "candidate_prediction_artifact": "generated",
        "framework_smoke_candidate_validation": validation_summary.get("status"),
        "candidate_binding_validator_reused": True,
        **generator_operation_safety_fields(),
        "permanently_inconclusive_override_allowed": False,
        **trading_2281_boundary_fields(),
    }
    return clean_for_yaml(
        {
            "schema_version": "first_layer_candidate_generation_summary.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "generated_at": context.generated_at.isoformat(),
            "artifact_role": FRAMEWORK_SMOKE_ARTIFACT_ROLE,
            **{
                key: value
                for key, value in framework_smoke_artifact_safety_fields().items()
                if key in {"historical_executable_artifact", "actual_path_validation_ready"}
            },
            "summary": summary,
            "context": context.to_dict(),
            "signal_spec": bundle.signal_spec.to_dict(),
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "validation_summary": dict(validation_summary),
            **candidate_artifact_safety_fields(),
            "dynamic_promotion_status": "BLOCKED",
        }
    )


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(clean_for_yaml(dict(payload)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _as_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return []


def _require_equal(
    payload: Mapping[str, Any],
    field: str,
    expected: Any,
    scope: str,
    errors: list[str],
) -> None:
    if payload.get(field) != expected:
        errors.append(f"{scope}: {field} must be {expected}")


def _require_false(
    payload: Mapping[str, Any],
    field: str,
    scope: str,
    errors: list[str],
) -> None:
    if payload.get(field) is not False:
        errors.append(f"{scope}: {field} must be false")


def _require_true(
    payload: Mapping[str, Any],
    field: str,
    scope: str,
    errors: list[str],
) -> None:
    if payload.get(field) is not True:
        errors.append(f"{scope}: {field} must be true")


def _context_allows_value(context_value: str, row_value: Any) -> bool:
    allowed = {item.strip() for item in str(context_value).split(",") if item.strip()}
    return row_value in allowed
