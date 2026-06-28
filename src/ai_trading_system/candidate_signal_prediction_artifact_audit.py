from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_candidate_actual_path_validation import (
    MISSING_SIGNAL_STATUS,
    run_first_layer_candidate_actual_path_validation_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import DEFAULT_CHALLENGER_MATRIX_PATH
from ai_trading_system.first_layer_proxy_challenger_experiments import (
    run_first_layer_proxy_challenger_experiments_pack,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    load_mapping,
    mapping,
    records,
    strings,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "candidate_signal_prediction_artifact_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
BASELINE_COMPOSER_PREDICTION_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "models"
    / "first_layer_composer_v2_predictions.csv"
)

STATUS = "CANDIDATE_SIGNAL_PREDICTION_ARTIFACT_AUDIT_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2281_CANDIDATE_SIGNAL_PREDICTION_ARTIFACT_BACKFILL"

ARTIFACT_TYPES = (
    "experiment_definition",
    "candidate_signal_spec",
    "candidate_signal_series",
    "candidate_prediction_artifact",
    "candidate_actual_path_backtest",
    "candidate_risk_attribution",
    "registry_reference",
)


def run_candidate_signal_prediction_artifact_audit_pack(
    *,
    challenger_matrix_path: Path = DEFAULT_CHALLENGER_MATRIX_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
) -> dict[str, Any]:
    source_root = output_root / "_regenerated_sources"
    source_docs_root = source_root / "docs"
    candidate_validation = run_first_layer_candidate_actual_path_validation_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2280_candidate_actual_path_validation",
        docs_root=source_docs_root / "trading_2280_candidate_actual_path_validation",
    )
    proxy_challengers = run_first_layer_proxy_challenger_experiments_pack(
        output_root=source_root / "trading_2273_proxy_challengers",
        docs_root=source_docs_root / "trading_2273_proxy_challengers",
        inputs_root=source_root / "inputs" / "research_reviews",
    )
    report_registry = load_mapping(report_registry_path)

    experiment_rows = {
        str(row.get("experiment_id")): row for row in records(proxy_challengers.get("experiments"))
    }
    inconclusive_rows = [
        row
        for row in records(candidate_validation.get("candidate_rows"))
        if row.get("updated_state") == "INCONCLUSIVE"
        and row.get("validation_status") == MISSING_SIGNAL_STATUS
    ]
    artifact_rows = [
        artifact_row
        for row in inconclusive_rows
        for artifact_row in _artifact_rows_for_candidate(
            candidate_row=row,
            experiment_row=mapping(experiment_rows.get(str(row.get("candidate_id")))),
            candidate_validation=candidate_validation,
            proxy_challengers=proxy_challengers,
            report_registry=report_registry,
        )
    ]
    candidate_rows = [
        _candidate_provenance_row(
            candidate_row=row,
            experiment_row=mapping(experiment_rows.get(str(row.get("candidate_id")))),
            artifact_rows=[
                artifact
                for artifact in artifact_rows
                if artifact.get("candidate_id") == row.get("candidate_id")
            ],
        )
        for row in inconclusive_rows
    ]
    recovery_plan_rows = [_recovery_plan_row(row) for row in candidate_rows]
    summary = _summary(candidate_rows=candidate_rows, artifact_rows=artifact_rows)
    common = {
        "schema_version": "candidate_signal_prediction_artifact_audit.v1",
        "report_type": "candidate_signal_prediction_artifact_audit",
        "title": "Candidate Signal / Prediction Artifact Backfill and Provenance Audit",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": candidate_validation.get("market_regime"),
        "anchor_event": candidate_validation.get("anchor_event"),
        "anchor_date": candidate_validation.get("anchor_date"),
        "requested_start": candidate_validation.get("requested_start"),
        "actual_signal_start": candidate_validation.get("actual_signal_start"),
        "data_quality_status": candidate_validation.get("data_quality_status"),
        "source_generation": {
            "candidate_validation_source": "regenerated_from_trading_2280_code_path",
            "proxy_challenger_source": "regenerated_from_trading_2273_code_path",
            "report_registry_source": str(report_registry_path),
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "challenger_matrix": str(challenger_matrix_path),
            "report_registry": str(report_registry_path),
            "regenerated_2280_artifacts": clean_for_yaml(
                dict(mapping(candidate_validation.get("artifact_paths")))
            ),
            "regenerated_2273_artifacts": clean_for_yaml(
                dict(mapping(proxy_challengers.get("artifact_paths")))
            ),
        },
        "summary": summary,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = {
        **common,
        "candidate_rows": candidate_rows,
        "artifact_rows": artifact_rows,
        "recovery_plan_rows": recovery_plan_rows,
        "backfill_actions_executed": [],
    }
    paths = {
        "candidate_signal_prediction_artifact_gap_report": docs_root
        / "candidate_signal_prediction_artifact_gap_report.md",
        "candidate_artifact_provenance_matrix_md": docs_root
        / "candidate_artifact_provenance_matrix.md",
        "inconclusive_candidate_recovery_plan_md": docs_root
        / "inconclusive_candidate_recovery_plan.md",
        "candidate_artifact_provenance_matrix_json": output_root
        / "candidate_artifact_provenance_matrix.json",
        "candidate_artifact_gap_matrix_json": output_root / "candidate_artifact_gap_matrix.json",
        "inconclusive_candidate_recovery_plan_json": output_root
        / "inconclusive_candidate_recovery_plan.json",
    }
    write_json(
        paths["candidate_artifact_provenance_matrix_json"],
        {**common, "candidate_rows": candidate_rows, "artifact_rows": artifact_rows},
    )
    write_json(
        paths["candidate_artifact_gap_matrix_json"],
        {**common, "artifact_rows": artifact_rows},
    )
    write_json(
        paths["inconclusive_candidate_recovery_plan_json"],
        {**common, "recovery_plan_rows": recovery_plan_rows},
    )
    write_markdown(
        paths["candidate_signal_prediction_artifact_gap_report"],
        _render_gap_report(payload, paths),
    )
    write_markdown(
        paths["candidate_artifact_provenance_matrix_md"],
        _render_provenance_matrix(payload, paths),
    )
    write_markdown(
        paths["inconclusive_candidate_recovery_plan_md"],
        _render_recovery_plan(payload, paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _artifact_rows_for_candidate(
    *,
    candidate_row: Mapping[str, Any],
    experiment_row: Mapping[str, Any],
    candidate_validation: Mapping[str, Any],
    proxy_challengers: Mapping[str, Any],
    report_registry: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidate_id = str(candidate_row.get("candidate_id"))
    rows = []
    for artifact_type in ARTIFACT_TYPES:
        rows.append(
            _artifact_row(
                candidate_id=candidate_id,
                artifact_type=artifact_type,
                experiment_row=experiment_row,
                candidate_validation=candidate_validation,
                proxy_challengers=proxy_challengers,
                report_registry=report_registry,
            )
        )
    return rows


def _artifact_row(
    *,
    candidate_id: str,
    artifact_type: str,
    experiment_row: Mapping[str, Any],
    candidate_validation: Mapping[str, Any],
    proxy_challengers: Mapping[str, Any],
    report_registry: Mapping[str, Any],
) -> dict[str, Any]:
    if artifact_type == "experiment_definition":
        return _present_experiment_definition_row(
            candidate_id=candidate_id,
            experiment_row=experiment_row,
            proxy_challengers=proxy_challengers,
        )
    if artifact_type in {"candidate_signal_series", "candidate_prediction_artifact"}:
        return _signal_or_prediction_row(
            candidate_id=candidate_id,
            artifact_type=artifact_type,
            experiment_row=experiment_row,
        )
    if artifact_type == "candidate_risk_attribution":
        return _risk_attribution_row(
            candidate_id=candidate_id,
            candidate_validation=candidate_validation,
        )
    if artifact_type == "registry_reference":
        return _registry_reference_row(
            candidate_id=candidate_id,
            report_registry=report_registry,
        )
    return _missing_never_generated_row(
        candidate_id=candidate_id,
        artifact_type=artifact_type,
        experiment_row=experiment_row,
    )


def _present_experiment_definition_row(
    *,
    candidate_id: str,
    experiment_row: Mapping[str, Any],
    proxy_challengers: Mapping[str, Any],
) -> dict[str, Any]:
    artifact_paths = mapping(proxy_challengers.get("artifact_paths"))
    evidence_paths = [
        str(artifact_paths.get("first_layer_proxy_challenger_experiments_yaml", "")),
        str(artifact_paths.get("first_layer_proxy_challenger_experiments_json", "")),
    ]
    return _base_artifact_row(
        candidate_id=candidate_id,
        artifact_type="experiment_definition",
        artifact_status="present_registered",
        gap_category="available_upstream_definition_only",
        evidence_paths=[path for path in evidence_paths if path],
        schema_status="experiment_definition_not_executable_signal_schema",
        backfill_possible=False,
        recovery_action="use_as_design_input_only_not_actual_path_signal",
        permanently_inconclusive=False,
        experiment_row=experiment_row,
    )


def _signal_or_prediction_row(
    *,
    candidate_id: str,
    artifact_type: str,
    experiment_row: Mapping[str, Any],
) -> dict[str, Any]:
    if candidate_id == "baseline" and BASELINE_COMPOSER_PREDICTION_PATH.exists():
        return _base_artifact_row(
            candidate_id=candidate_id,
            artifact_type=artifact_type,
            artifact_status="present_but_not_candidate_bound",
            gap_category="schema_incompatible",
            evidence_paths=[str(BASELINE_COMPOSER_PREDICTION_PATH)],
            schema_status=_baseline_prediction_schema_status(),
            backfill_possible=False,
            recovery_action=(
                "define_candidate_signal_binding_schema_then_wrap_frozen_composer_predictions"
            ),
            permanently_inconclusive=True,
            experiment_row=experiment_row,
        )
    return _missing_never_generated_row(
        candidate_id=candidate_id,
        artifact_type=artifact_type,
        experiment_row=experiment_row,
    )


def _risk_attribution_row(
    *,
    candidate_id: str,
    candidate_validation: Mapping[str, Any],
) -> dict[str, Any]:
    artifact_paths = mapping(candidate_validation.get("artifact_paths"))
    return _base_artifact_row(
        candidate_id=candidate_id,
        artifact_type="candidate_risk_attribution",
        artifact_status="gap_record_present_metrics_missing",
        gap_category="schema_incompatible",
        evidence_paths=[
            str(artifact_paths.get("candidate_risk_attribution_matrix", "")),
            str(artifact_paths.get("candidate_actual_path_matrix", "")),
        ],
        schema_status="2280_matrix_records_missing_candidate_signal_artifact_not_attribution_metrics",
        backfill_possible=False,
        recovery_action="generate_candidate_actual_path_backtest_before_risk_attribution",
        permanently_inconclusive=True,
        experiment_row={},
    )


def _registry_reference_row(
    *,
    candidate_id: str,
    report_registry: Mapping[str, Any],
) -> dict[str, Any]:
    references = _registry_references(candidate_id, report_registry)
    if references:
        return _base_artifact_row(
            candidate_id=candidate_id,
            artifact_type="registry_reference",
            artifact_status="present",
            gap_category="none",
            evidence_paths=references,
            schema_status="registry_contains_candidate_specific_reference",
            backfill_possible=False,
            recovery_action="none",
            permanently_inconclusive=False,
            experiment_row={},
        )
    return _base_artifact_row(
        candidate_id=candidate_id,
        artifact_type="registry_reference",
        artifact_status="missing",
        gap_category="registry_missing_reference",
        evidence_paths=[str(DEFAULT_REPORT_REGISTRY_PATH)],
        schema_status="report_registry_has_report_level_artifacts_not_candidate_signal_artifacts",
        backfill_possible=False,
        recovery_action="register_candidate_artifacts_after_executable_signal_generation",
        permanently_inconclusive=True,
        experiment_row={},
    )


def _missing_never_generated_row(
    *,
    candidate_id: str,
    artifact_type: str,
    experiment_row: Mapping[str, Any],
) -> dict[str, Any]:
    return _base_artifact_row(
        candidate_id=candidate_id,
        artifact_type=artifact_type,
        artifact_status="missing",
        gap_category="never_generated",
        evidence_paths=[],
        schema_status="missing_candidate_level_executable_artifact",
        backfill_possible=False,
        recovery_action=_recovery_action_for_artifact(
            artifact_type=artifact_type,
            experiment_row=experiment_row,
        ),
        permanently_inconclusive=True,
        experiment_row=experiment_row,
    )


def _base_artifact_row(
    *,
    candidate_id: str,
    artifact_type: str,
    artifact_status: str,
    gap_category: str,
    evidence_paths: Sequence[str],
    schema_status: str,
    backfill_possible: bool,
    recovery_action: str,
    permanently_inconclusive: bool,
    experiment_row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "artifact_type": artifact_type,
        "required_for_candidate_actual_path_validation": artifact_type != "experiment_definition",
        "artifact_status": artifact_status,
        "gap_category": gap_category,
        "evidence_paths": [path for path in evidence_paths if path],
        "required_proxy_ids": strings(experiment_row.get("required_proxy_ids")),
        "target_objective_terms": strings(experiment_row.get("target_objective_terms")),
        "expected_signal_role": str(experiment_row.get("expected_signal_role") or ""),
        "schema_status": schema_status,
        "generated_but_unregistered": gap_category == "generated_but_unregistered",
        "registry_missing_reference": gap_category == "registry_missing_reference",
        "path_drift_detected": gap_category == "path_drift",
        "schema_incompatible": gap_category == "schema_incompatible",
        "ignored_outputs_cleaned": gap_category == "ignored_outputs_cleaned",
        "never_generated": gap_category == "never_generated",
        "backfill_possible": backfill_possible,
        "backfill_executed": False,
        "recovery_action": recovery_action,
        "permanently_inconclusive": permanently_inconclusive,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _baseline_prediction_schema_status() -> str:
    columns = _csv_columns(BASELINE_COMPOSER_PREDICTION_PATH)
    required_candidate_binding_columns = {"candidate_id", "candidate_signal_series"}
    if required_candidate_binding_columns.issubset(columns):
        return "candidate_binding_columns_present"
    return "source_model_predictions_exist_but_lack_candidate_id_and_candidate_signal_series_schema"


def _csv_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        return set(next(reader, []))


def _registry_references(candidate_id: str, report_registry: Mapping[str, Any]) -> list[str]:
    references = []
    for report in records(report_registry.get("reports")):
        report_id = str(report.get("report_id") or "")
        if not report_id.startswith("first_layer_"):
            continue
        for artifact_glob in strings(report.get("artifact_globs")):
            if candidate_id in artifact_glob:
                references.append(f"{report_id}::{artifact_glob}")
    return references


def _recovery_action_for_artifact(
    *,
    artifact_type: str,
    experiment_row: Mapping[str, Any],
) -> str:
    role = str(experiment_row.get("expected_signal_role") or "candidate_signal")
    if artifact_type == "candidate_signal_spec":
        return f"define_executable_signal_spec_for_{role}"
    if artifact_type == "candidate_signal_series":
        return "generate_candidate_signal_series_after_signal_spec_exists"
    if artifact_type == "candidate_prediction_artifact":
        return "generate_candidate_prediction_artifact_after_signal_series_exists"
    if artifact_type == "candidate_actual_path_backtest":
        return "run_candidate_actual_path_backtest_after_prediction_artifact_exists"
    if artifact_type == "candidate_risk_attribution":
        return "derive_risk_attribution_after_actual_path_backtest_exists"
    return "register_candidate_artifact_after_generation"


def _candidate_provenance_row(
    *,
    candidate_row: Mapping[str, Any],
    experiment_row: Mapping[str, Any],
    artifact_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    required_rows = [
        row
        for row in artifact_rows
        if row.get("required_for_candidate_actual_path_validation") is True
    ]
    missing_types = [
        str(row.get("artifact_type"))
        for row in required_rows
        if row.get("gap_category") not in {"none"}
    ]
    gap_counts = Counter(str(row.get("gap_category")) for row in required_rows)
    backfill_possible = any(bool(row.get("backfill_possible")) for row in required_rows)
    permanently_inconclusive = not backfill_possible and bool(missing_types)
    return {
        "candidate_id": candidate_row.get("candidate_id"),
        "previous_state": candidate_row.get("previous_state"),
        "updated_state": candidate_row.get("updated_state"),
        "validation_status": candidate_row.get("validation_status"),
        "expected_signal_role": experiment_row.get("expected_signal_role"),
        "required_proxy_ids": strings(experiment_row.get("required_proxy_ids")),
        "target_objective_terms": strings(experiment_row.get("target_objective_terms")),
        "missing_artifact_types": missing_types,
        "gap_category_counts": dict(sorted(gap_counts.items())),
        "primary_missing_reason": _primary_missing_reason(gap_counts),
        "backfill_possible": backfill_possible,
        "backfilled_artifact_count": 0,
        "permanently_inconclusive": permanently_inconclusive,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _primary_missing_reason(gap_counts: Counter[str]) -> str:
    for reason in (
        "never_generated",
        "schema_incompatible",
        "registry_missing_reference",
        "ignored_outputs_cleaned",
        "path_drift",
        "generated_but_unregistered",
    ):
        if gap_counts.get(reason):
            return reason
    return "none"


def _recovery_plan_row(candidate_row: Mapping[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate_row.get("candidate_id"))
    next_action = _candidate_next_action(candidate_id)
    return {
        "candidate_id": candidate_id,
        "current_state": candidate_row.get("updated_state"),
        "primary_missing_reason": candidate_row.get("primary_missing_reason"),
        "missing_artifact_types": candidate_row.get("missing_artifact_types"),
        "backfill_possible": candidate_row.get("backfill_possible"),
        "backfill_executed": False,
        "permanently_inconclusive": candidate_row.get("permanently_inconclusive"),
        "recovery_action": next_action,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _candidate_next_action(candidate_id: str) -> str:
    actions = {
        "baseline": (
            "define candidate signal binding schema, then rewrap frozen composer predictions "
            "as candidate-bound signal/prediction artifacts"
        ),
        "baseline_plus_trend_structure": (
            "implement trend-structure executable signal generator before actual-path backtest"
        ),
        "volatility_regime": (
            "implement volatility-regime executable signal generator from volatility proxy inputs"
        ),
        "risk_appetite": (
            "implement risk-appetite executable signal generator from rates and semiconductor "
            "proxy inputs"
        ),
    }
    return actions.get(candidate_id, "define executable signal generator before backfill")


def _summary(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    artifact_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    gap_counts = Counter(str(row.get("gap_category")) for row in artifact_rows)
    return {
        "task_id": TASK_ID,
        "inconclusive_candidate_count": len(candidate_rows),
        "artifact_row_count": len(artifact_rows),
        "artifact_types_checked": list(ARTIFACT_TYPES),
        "candidate_signal_prediction_artifacts_complete_count": 0,
        "backfill_possible_candidate_count": sum(
            1 for row in candidate_rows if row.get("backfill_possible") is True
        ),
        "backfilled_artifact_count": 0,
        "permanently_inconclusive_count": sum(
            1 for row in candidate_rows if row.get("permanently_inconclusive") is True
        ),
        "gap_category_counts": dict(sorted(gap_counts.items())),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_gap_report(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# Candidate Signal / Prediction Artifact Gap Report",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        (
            f"- inconclusive_candidate_count=`{summary.get('inconclusive_candidate_count')}`; "
            f"artifact_row_count=`{summary.get('artifact_row_count')}`; "
            f"backfilled_artifact_count=`{summary.get('backfilled_artifact_count')}`."
        ),
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "## Candidate Gap Summary",
        "",
        (
            "| candidate | primary_missing_reason | missing_artifact_types | "
            "backfill_possible | permanently_inconclusive |"
        ),
        "|---|---|---|---:|---:|",
    ]
    for row in records(payload.get("candidate_rows")):
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('primary_missing_reason')}`|"
            f"`{', '.join(strings(row.get('missing_artifact_types')))}`|"
            f"`{row.get('backfill_possible')}`|`{row.get('permanently_inconclusive')}`|"
        )
    lines.extend(
        [
            "",
            "## 结论",
            "",
            (
                "- 4 个 candidates 都只有 offline experiment definition；缺少可执行 "
                "candidate signal spec / signal series / prediction artifact，因此当前不能执行 "
                "candidate-level actual-path backtest。"
            ),
            (
                "- `baseline` 有 frozen composer prediction source，但该 CSV 缺少 "
                "`candidate_id` 和 candidate signal binding schema，只能作为 source evidence，"
                "不能直接视为 candidate-bound artifact。"
            ),
            (
                "- 本批没有可直接 backfill 的完整 candidate-level artifact；所有 4 个 candidates "
                "在当前证据链下保持 permanently inconclusive。"
            ),
            "",
            "## 产物",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _render_provenance_matrix(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    lines = [
        "# Candidate Artifact Provenance Matrix",
        "",
        "| candidate | artifact_type | status | gap_category | schema_status | evidence_paths |",
        "|---|---|---|---|---|---|",
    ]
    for row in records(payload.get("artifact_rows")):
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('artifact_type')}`|"
            f"`{row.get('artifact_status')}`|`{row.get('gap_category')}`|"
            f"`{row.get('schema_status')}`|"
            f"`{'; '.join(strings(row.get('evidence_paths')))}`|"
        )
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _render_recovery_plan(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    lines = [
        "# Inconclusive Candidate Recovery Plan",
        "",
        "| candidate | backfill_possible | permanently_inconclusive | recovery_action |",
        "|---|---:|---:|---|",
    ]
    for row in records(payload.get("recovery_plan_rows")):
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('backfill_possible')}`|"
            f"`{row.get('permanently_inconclusive')}`|{row.get('recovery_action')}|"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            (
                "Recovery plan 只定义下一步证据链修复，不允许 promotion、paper-shadow、"
                "production 或 broker action。"
            ),
            "",
            "## 产物",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)
