from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.candidate_signal_binding_schema import (
    PREDICTION_SCHEMA_VERSION,
    SCHEMA_VERSION,
    SIGNAL_SPEC_VERSION,
    CandidateArtifactProvenance,
    CandidateBoundPredictionArtifact,
    CandidateBoundSignalRecord,
    candidate_bound_prediction_artifact_contract_dict,
    candidate_bound_signal_series_contract_dict,
    candidate_signal_binding_schema_dict,
)
from ai_trading_system.candidate_signal_binding_validator import (
    CandidateSignalBindingValidator,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    mapping,
    records,
    write_json,
    write_markdown,
)

DEFAULT_SOURCE_PREDICTIONS_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "models"
    / "first_layer_composer_v2_predictions.csv"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "candidate_signal_binding_schema"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2282_CANDIDATE_SIGNAL_BINDING_SCHEMA_BASELINE_REWRAP_POC"
STATUS = "CANDIDATE_SIGNAL_BINDING_SCHEMA_POC_READY_PROMOTION_BLOCKED"
MODE = "schema_migration_poc"
TARGET_ASSET = "QQQ_SGOV_TQQQ"
SIGNAL_NAME = "first_layer_composer_v2_trend_state"
SOURCE_SCHEMA_STATUS = "source_evidence_only"
PIT_POLICY = "non_pit_source_evidence_only"
REGENERATION_MODE = "schema_migration_poc"
CANDIDATE_BINDING_METHOD = "rewrap_mapping"
SOURCE_REQUIRED_COLUMNS = (
    "date",
    "model_id",
    "trend_state",
    "confidence",
    "expected_horizon_days",
    "validity_days",
    "feature_snapshot_hash",
    "model_version",
    "known_at",
    "available_at",
    "decision_at",
    "do_not_de_risk_pred",
    "stay_constructive_pred",
    "add_risk_pred",
    "high_confidence_risk_on_pred",
)


def run_candidate_signal_binding_schema_poc(
    *,
    candidate_id: str = "baseline",
    source_predictions: Path = DEFAULT_SOURCE_PREDICTIONS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if candidate_id != "baseline":
        raise ValueError("candidate-signal-binding-schema-poc currently supports only baseline")
    if mode != MODE:
        raise ValueError("--mode currently supports only schema_migration_poc")
    if not source_predictions.exists():
        raise ValueError(f"source predictions not found: {source_predictions}")

    generated_at = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
    source_hash = _sha256(source_predictions)
    source_rows = _load_source_rows(source_predictions)
    _validate_source_columns(source_rows, source_predictions)
    signal_records = [
        _rewrap_source_row(
            row=row,
            row_index=index,
            candidate_id=candidate_id,
            source_predictions=source_predictions,
            source_hash=source_hash,
            generated_at=generated_at,
        )
        for index, row in enumerate(source_rows, start=1)
    ]
    signal_payloads = [record.to_dict() for record in signal_records]
    prediction_artifact = _prediction_artifact(
        signal_records=signal_records,
        source_predictions=source_predictions,
        source_hash=source_hash,
        generated_at=generated_at,
    )

    validator = CandidateSignalBindingValidator()
    signal_validation = validator.validate_candidate_bound_signal_series(signal_payloads)
    prediction_validation = validator.validate_candidate_bound_prediction_artifact(
        prediction_artifact
    )
    validation_summary = _validation_summary(signal_validation, prediction_validation)
    if validation_summary["status"] != "PASS":
        raise ValueError(
            "candidate signal binding validation failed: "
            + "; ".join(validation_summary["errors"])
        )

    paths = {
        "candidate_signal_binding_schema_json": output_dir
        / "candidate_signal_binding_schema.json",
        "candidate_bound_signal_series_contract_json": output_dir
        / "candidate_bound_signal_series_contract.json",
        "candidate_bound_prediction_artifact_contract_json": output_dir
        / "candidate_bound_prediction_artifact_contract.json",
        "baseline_rewrapped_candidate_signal_series_csv": output_dir
        / "baseline_rewrapped_candidate_signal_series.csv",
        "baseline_rewrapped_candidate_prediction_artifact_json": output_dir
        / "baseline_rewrapped_candidate_prediction_artifact.json",
        "baseline_rewrap_provenance_report_json": output_dir
        / "baseline_rewrap_provenance_report.json",
        "baseline_rewrap_validation_summary_json": output_dir
        / "baseline_rewrap_validation_summary.json",
        "candidate_signal_binding_schema_md": docs_root / "candidate_signal_binding_schema.md",
        "candidate_bound_artifact_contract_md": docs_root
        / "candidate_bound_artifact_contract.md",
        "baseline_frozen_composer_rewrap_poc_report_md": docs_root
        / "baseline_frozen_composer_rewrap_poc_report.md",
    }

    schema_payload = candidate_signal_binding_schema_dict()
    signal_contract = candidate_bound_signal_series_contract_dict()
    prediction_contract = candidate_bound_prediction_artifact_contract_dict()
    provenance_report = _provenance_report(
        candidate_id=candidate_id,
        source_predictions=source_predictions,
        source_hash=source_hash,
        source_rows=source_rows,
        paths=paths,
        validation_summary=validation_summary,
        generated_at=generated_at,
    )
    summary = _summary(
        candidate_id=candidate_id,
        source_predictions=source_predictions,
        source_hash=source_hash,
        source_rows=source_rows,
        signal_records=signal_records,
        validation_summary=validation_summary,
    )
    common = {
        "schema_version": "candidate_signal_binding_schema_poc.v1",
        "report_type": "candidate_signal_binding_schema_poc",
        "title": "Candidate Signal Binding Schema + Baseline Rewrap POC",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": generated_at,
        "mode": mode,
        "candidate_id": candidate_id,
        "candidate_family": "first_layer_proxy_candidate",
        "artifact_role": "schema_migration_poc",
        "source_schema_status": SOURCE_SCHEMA_STATUS,
        "source_predictions_path": str(source_predictions),
        "source_artifact_hash": source_hash,
        "summary": summary,
        "validation_summary": validation_summary,
        "research_only": True,
        "historical_executable_artifact": False,
        "actual_path_validation_ready": False,
        "promotion_eligible": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "permanently_inconclusive_override_allowed": False,
        "dynamic_promotion_status": "BLOCKED",
        "trading_2281_permanently_inconclusive_unchanged": True,
    }

    write_json(paths["candidate_signal_binding_schema_json"], schema_payload)
    write_json(paths["candidate_bound_signal_series_contract_json"], signal_contract)
    write_json(paths["candidate_bound_prediction_artifact_contract_json"], prediction_contract)
    _write_signal_series_csv(
        paths["baseline_rewrapped_candidate_signal_series_csv"],
        signal_payloads,
    )
    write_json(paths["baseline_rewrapped_candidate_prediction_artifact_json"], prediction_artifact)
    write_json(paths["baseline_rewrap_provenance_report_json"], provenance_report)
    write_json(paths["baseline_rewrap_validation_summary_json"], validation_summary)
    write_markdown(
        paths["candidate_signal_binding_schema_md"],
        _render_schema_doc(schema_payload),
    )
    write_markdown(
        paths["candidate_bound_artifact_contract_md"],
        _render_contract_doc(signal_contract, prediction_contract),
    )
    write_markdown(
        paths["baseline_frozen_composer_rewrap_poc_report_md"],
        _render_rewrap_report({**common, "provenance_report": provenance_report}, paths),
    )
    return clean_for_yaml(
        {
            **common,
            "schema": schema_payload,
            "signal_series_contract": signal_contract,
            "prediction_artifact_contract": prediction_contract,
            "provenance_report": provenance_report,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _load_source_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _validate_source_columns(rows: Sequence[Mapping[str, Any]], path: Path) -> None:
    if not rows:
        raise ValueError(f"source predictions are empty: {path}")
    missing = set(SOURCE_REQUIRED_COLUMNS) - set(rows[0])
    if missing:
        raise ValueError(f"source predictions missing columns: {sorted(missing)}")


def _rewrap_source_row(
    *,
    row: Mapping[str, str],
    row_index: int,
    candidate_id: str,
    source_predictions: Path,
    source_hash: str,
    generated_at: str,
) -> CandidateBoundSignalRecord:
    available_at = _datetime_iso(row["available_at"])
    decision_at = _datetime_iso(row["decision_at"])
    valid_from = decision_at
    valid_until = _valid_until(decision_at, row["validity_days"])
    confidence = round(float(row["confidence"]), 6)
    provenance = _provenance(source_predictions=source_predictions, source_hash=source_hash)
    prediction_flags = {
        "do_not_de_risk_pred": _bool(row["do_not_de_risk_pred"]),
        "stay_constructive_pred": _bool(row["stay_constructive_pred"]),
        "add_risk_pred": _bool(row["add_risk_pred"]),
        "high_confidence_risk_on_pred": _bool(row["high_confidence_risk_on_pred"]),
    }
    return CandidateBoundSignalRecord(
        candidate_id=candidate_id,
        candidate_family="first_layer_proxy_candidate",
        source_experiment_id=str(row["model_id"]),
        source_artifact_id=source_predictions.stem,
        source_artifact_path=str(source_predictions),
        source_artifact_hash=source_hash,
        signal_spec_version=SIGNAL_SPEC_VERSION,
        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
        generated_at=generated_at,
        as_of_timestamp=available_at,
        decision_timestamp=decision_at,
        target_asset=TARGET_ASSET,
        horizon=f"{int(float(row['expected_horizon_days']))}d",
        signal_name=SIGNAL_NAME,
        signal_value=confidence,
        signal_direction=_signal_direction(str(row["trend_state"])),
        signal_confidence=confidence,
        valid_from=valid_from,
        valid_until=valid_until,
        input_snapshot_hash=source_hash,
        feature_snapshot_hash=str(row["feature_snapshot_hash"]),
        model_or_rule_version=str(row["model_version"]),
        provenance=provenance,
        promotion_eligible=False,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        permanently_inconclusive_override_allowed=False,
        source_row_index=row_index,
        source_date=str(row["date"]),
        source_trend_state=str(row["trend_state"]),
        source_confidence=confidence,
        source_prediction_flags=prediction_flags,
    )


def _prediction_artifact(
    *,
    signal_records: Sequence[CandidateBoundSignalRecord],
    source_predictions: Path,
    source_hash: str,
    generated_at: str,
) -> dict[str, Any]:
    latest = signal_records[-1]
    prediction_records = [_prediction_record(record) for record in signal_records]
    artifact = CandidateBoundPredictionArtifact(
        artifact_id="baseline_rewrapped_candidate_prediction_artifact",
        artifact_role="schema_migration_poc",
        candidate_id=latest.candidate_id,
        candidate_family=latest.candidate_family,
        source_experiment_id=latest.source_experiment_id,
        source_artifact_id=source_predictions.stem,
        source_artifact_path=str(source_predictions),
        source_artifact_hash=source_hash,
        signal_spec_version=SIGNAL_SPEC_VERSION,
        prediction_schema_version=PREDICTION_SCHEMA_VERSION,
        generated_at=generated_at,
        as_of_timestamp=latest.as_of_timestamp,
        decision_timestamp=latest.decision_timestamp,
        target_asset=latest.target_asset,
        horizon=latest.horizon,
        signal_name=latest.signal_name,
        signal_value=latest.signal_value,
        signal_direction=latest.signal_direction,
        signal_confidence=latest.signal_confidence,
        valid_from=latest.valid_from,
        valid_until=latest.valid_until,
        input_snapshot_hash=source_hash,
        feature_snapshot_hash=latest.feature_snapshot_hash,
        model_or_rule_version=latest.model_or_rule_version,
        provenance=_provenance(source_predictions=source_predictions, source_hash=source_hash),
        prediction_records=prediction_records,
        source_schema_status=SOURCE_SCHEMA_STATUS,
        historical_executable_artifact=False,
        actual_path_validation_ready=False,
        promotion_eligible=False,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        permanently_inconclusive_override_allowed=False,
    )
    payload = artifact.to_dict()
    payload["schema_version"] = PREDICTION_SCHEMA_VERSION
    payload["record_count"] = len(prediction_records)
    payload["source_evidence_role"] = "source_evidence"
    payload["rewrap_classification"] = "schema_migration_poc_artifact"
    payload["trading_2281_permanently_inconclusive_unchanged"] = True
    return payload


def _prediction_record(record: CandidateBoundSignalRecord) -> dict[str, Any]:
    payload = record.to_dict()
    payload["prediction_fields"] = {
        "trend_state": record.source_trend_state,
        "confidence": record.source_confidence,
        **record.source_prediction_flags,
    }
    return payload


def _provenance(*, source_predictions: Path, source_hash: str) -> CandidateArtifactProvenance:
    return CandidateArtifactProvenance(
        source_paths=[str(source_predictions)],
        source_hashes=[source_hash],
        regeneration_mode=REGENERATION_MODE,
        pit_policy=PIT_POLICY,
        candidate_binding_method=CANDIDATE_BINDING_METHOD,
        source_schema_status=SOURCE_SCHEMA_STATUS,
        promotion_eligible=False,
    )


def _validation_summary(
    signal_validation: Any,
    prediction_validation: Any,
) -> dict[str, Any]:
    errors = list(signal_validation.errors) + list(prediction_validation.errors)
    return {
        "schema_version": "baseline_rewrap_validation_summary.v1",
        "status": "PASS" if not errors else "FAIL",
        "signal_series_validation": signal_validation.to_dict(),
        "prediction_artifact_validation": prediction_validation.to_dict(),
        "candidate_bound_minimum_fields_satisfied": not errors,
        "errors": errors,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "permanently_inconclusive_override_allowed": False,
    }


def _provenance_report(
    *,
    candidate_id: str,
    source_predictions: Path,
    source_hash: str,
    source_rows: Sequence[Mapping[str, str]],
    paths: Mapping[str, Path],
    validation_summary: Mapping[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": "baseline_rewrap_provenance_report.v1",
        "task_id": TASK_ID,
        "generated_at": generated_at,
        "candidate_id": candidate_id,
        "source_csv_path": str(source_predictions),
        "source_csv_hash": source_hash,
        "source_row_count": len(source_rows),
        "source_schema_status": SOURCE_SCHEMA_STATUS,
        "source_evidence_classification": "source_evidence",
        "rewrapped_artifact_classification": "schema_migration_poc_artifact",
        "historical_executable_artifact": False,
        "rewrap_mapping_rules": _mapping_rules(),
        "artifact_paths": {key: str(path) for key, path in paths.items()},
        "validation_summary": clean_for_yaml(dict(validation_summary)),
        "promotion_eligible": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "permanently_inconclusive_override_allowed": False,
        "trading_2281_permanently_inconclusive_unchanged": True,
    }


def _summary(
    *,
    candidate_id: str,
    source_predictions: Path,
    source_hash: str,
    source_rows: Sequence[Mapping[str, str]],
    signal_records: Sequence[CandidateBoundSignalRecord],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "candidate_id": candidate_id,
        "source_predictions_path": str(source_predictions),
        "source_artifact_hash": source_hash,
        "source_row_count": len(source_rows),
        "rewrapped_signal_record_count": len(signal_records),
        "rewrapped_prediction_record_count": len(signal_records),
        "schema_version": SCHEMA_VERSION,
        "signal_spec_version": SIGNAL_SPEC_VERSION,
        "prediction_schema_version": PREDICTION_SCHEMA_VERSION,
        "regeneration_mode": REGENERATION_MODE,
        "pit_policy": PIT_POLICY,
        "candidate_binding_method": CANDIDATE_BINDING_METHOD,
        "source_schema_status": SOURCE_SCHEMA_STATUS,
        "validation_status": validation_summary.get("status"),
        "promotion_eligible": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "permanently_inconclusive_override_allowed": False,
        "trading_2281_permanently_inconclusive_unchanged": True,
    }


def _write_signal_series_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "candidate_id",
        "candidate_family",
        "source_experiment_id",
        "source_artifact_id",
        "source_artifact_path",
        "source_artifact_hash",
        "signal_spec_version",
        "prediction_schema_version",
        "generated_at",
        "as_of_timestamp",
        "decision_timestamp",
        "target_asset",
        "horizon",
        "signal_name",
        "signal_value",
        "signal_direction",
        "signal_confidence",
        "valid_from",
        "valid_until",
        "input_snapshot_hash",
        "feature_snapshot_hash",
        "model_or_rule_version",
        "provenance",
        "promotion_eligible",
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "broker_action",
        "permanently_inconclusive_override_allowed",
        "source_row_index",
        "source_date",
        "source_trend_state",
        "source_confidence",
        "source_prediction_flags",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            output = dict(row)
            output["provenance"] = json.dumps(
                mapping(row.get("provenance")),
                ensure_ascii=False,
                sort_keys=True,
            )
            output["source_prediction_flags"] = json.dumps(
                mapping(row.get("source_prediction_flags")),
                ensure_ascii=False,
                sort_keys=True,
            )
            writer.writerow({field: output.get(field) for field in fieldnames})


def _mapping_rules() -> list[dict[str, str]]:
    return [
        {"target_field": "candidate_id", "source": "constant:baseline"},
        {"target_field": "candidate_family", "source": "constant:first_layer_proxy_candidate"},
        {"target_field": "source_experiment_id", "source": "model_id"},
        {"target_field": "source_artifact_hash", "source": "sha256(source CSV bytes)"},
        {"target_field": "as_of_timestamp", "source": "available_at"},
        {"target_field": "decision_timestamp", "source": "decision_at"},
        {"target_field": "horizon", "source": "expected_horizon_days + 'd'"},
        {"target_field": "signal_value", "source": "confidence"},
        {"target_field": "signal_direction", "source": "trend_state enum mapping"},
        {"target_field": "valid_from", "source": "decision_at"},
        {"target_field": "valid_until", "source": "decision_at + validity_days"},
        {"target_field": "feature_snapshot_hash", "source": "feature_snapshot_hash"},
        {"target_field": "model_or_rule_version", "source": "model_version"},
        {"target_field": "provenance.regeneration_mode", "source": "schema_migration_poc"},
        {"target_field": "provenance.pit_policy", "source": "non_pit_source_evidence_only"},
        {"target_field": "promotion_eligible", "source": "constant:false"},
    ]


def _render_schema_doc(schema_payload: Mapping[str, Any]) -> str:
    required_fields = ", ".join(f"`{field}`" for field in schema_payload["required_fields"])
    provenance = mapping(schema_payload.get("provenance"))
    provenance_fields = ", ".join(
        f"`{field}`" for field in provenance.get("required_fields", [])
    )
    return "\n".join(
        [
            "# Candidate Signal Binding Schema",
            "",
            "## 设计目标",
            "",
            (
                "本 schema 要求 first-layer candidate signal / prediction artifact 显式绑定 "
                "candidate、source artifact、schema version、PIT 时间字段和 provenance。"
            ),
            "",
            "## 必需字段",
            "",
            required_fields,
            "",
            "## Provenance 字段",
            "",
            provenance_fields,
            "",
            "## PIT 与 Candidate Binding",
            "",
            (
                "`as_of_timestamp`、`decision_timestamp`、`valid_from`、`valid_until` 和 "
                "`horizon` 必须存在；缺少这些字段的 artifact 不能进入 candidate-level "
                "actual-path validation。"
            ),
            "",
            "## Source Evidence 边界",
            "",
            (
                "`source_evidence` 只能证明上游文件存在；`schema_migration_poc_artifact` 只证明 "
                "字段可映射；两者都不能被反向声明为 historical executable artifact。"
            ),
            "",
            "## Promotion Gating",
            "",
            (
                "`schema_migration_poc` 必须 `promotion_eligible=false`；"
                "`non_pit_source_evidence_only` 必须 `paper_shadow_allowed=false`、"
                "`production_allowed=false`、`broker_action=none`。"
            ),
            "",
            "## Future Generator 对接",
            "",
            (
                "后续 executable generator 应 native 写出本 schema，而不是依赖事后 rewrap；"
                "generator 输出通过 validator 后才可进入 candidate-level actual-path validation。"
            ),
            "",
        ]
    )


def _render_contract_doc(
    signal_contract: Mapping[str, Any],
    prediction_contract: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Candidate-Bound Artifact Contract",
            "",
            "## Signal Series Artifact Contract",
            "",
            f"- format: `{signal_contract.get('file_format')}`",
            "- required columns:",
            _inline_list(signal_contract.get("required_columns")),
            "",
            "## Prediction Artifact Contract",
            "",
            f"- format: `{prediction_contract.get('file_format')}`",
            "- required top-level fields:",
            _inline_list(prediction_contract.get("required_top_level_fields")),
            "",
            "## Validation Rules",
            "",
            _bullet_list(signal_contract.get("validation_rules")),
            "",
            "## Schema Versioning",
            "",
            f"- signal series: {signal_contract.get('schema_versioning_policy')}",
            f"- prediction artifact: {prediction_contract.get('schema_versioning_policy')}",
            "",
            "## Backward Compatibility",
            "",
            f"- signal series: {signal_contract.get('backward_compatibility_policy')}",
            f"- prediction artifact: {prediction_contract.get('backward_compatibility_policy')}",
            "",
            "## Failure Examples",
            "",
            _bullet_list(signal_contract.get("failure_examples")),
            _bullet_list(prediction_contract.get("failure_examples")),
            "",
        ]
    )


def _render_rewrap_report(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    summary = mapping(payload.get("summary"))
    validation = mapping(payload.get("validation_summary"))
    provenance = mapping(payload.get("provenance_report"))
    lines = [
        "# Baseline Frozen Composer Rewrap POC Report",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        f"- source CSV: `{payload.get('source_predictions_path')}`",
        f"- source CSV hash: `{payload.get('source_artifact_hash')}`",
        f"- source schema status: `{payload.get('source_schema_status')}`",
        (
            f"- source rows=`{summary.get('source_row_count')}`; "
            f"rewrapped records=`{summary.get('rewrapped_signal_record_count')}`; "
            f"validation=`{validation.get('status')}`."
        ),
        "",
        "## Rewrap Mapping",
        "",
        "| target_field | source |",
        "|---|---|",
    ]
    for rule in records(provenance.get("rewrap_mapping_rules")):
        lines.append(f"|`{rule.get('target_field')}`|`{rule.get('source')}`|")
    lines.extend(
        [
            "",
            "## Generated Candidate-Bound POC Artifacts",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.extend(
        [
            "",
            "## Validation Summary",
            "",
            (
                "- candidate_bound_minimum_fields_satisfied: "
                f"`{validation.get('candidate_bound_minimum_fields_satisfied')}`"
            ),
            (
                "- signal error count: "
                f"`{mapping(validation.get('signal_series_validation')).get('error_count')}`"
            ),
            (
                "- prediction error count: "
                f"`{mapping(validation.get('prediction_artifact_validation')).get('error_count')}`"
            ),
            "",
            "## Safety Boundary",
            "",
            (
                "- rewrap artifact 不允许 promotion、paper-shadow、production 或 broker action；"
                "`promotion_allowed=false`、`paper_shadow_allowed=false`、"
                "`production_allowed=false`、`broker_action=none`。"
            ),
            (
                "- rewrap 不改变 TRADING-2281 的 permanently inconclusive 结论；"
                "它不是 historical executable candidate artifact，也不是 actual-path "
                "validation ready。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _inline_list(value: Any) -> str:
    values = [str(item) for item in value] if isinstance(value, list) else []
    return ", ".join(f"`{item}`" for item in values)


def _bullet_list(value: Any) -> str:
    values = [str(item) for item in value] if isinstance(value, list) else []
    return "\n".join(f"- {item}" for item in values)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _datetime_iso(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.isoformat()


def _valid_until(decision_at: str, validity_days: str) -> str:
    parsed = datetime.fromisoformat(decision_at)
    return (parsed + timedelta(days=int(float(validity_days)))).isoformat()


def _bool(value: str) -> bool:
    return value.strip().lower() == "true"


def _signal_direction(trend_state: str) -> str:
    mapping_by_state = {
        "risk_on": "risk_on",
        "constructive": "trend_confirming",
        "neutral": "neutral",
        "defensive": "risk_off",
        "risk_off": "risk_off",
    }
    return mapping_by_state.get(trend_state, "neutral")
