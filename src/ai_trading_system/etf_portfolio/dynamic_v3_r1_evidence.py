from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from math import isfinite
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_parameter_research as legacy
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    _false_signal_diagnostics,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    build_dynamic_v3_real_evaluation_report,
    load_dynamic_v3_real_evaluation_policy_config,
    precompute_dynamic_v3_fixed_robustness_reports,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_POLICY_PATH as DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    PortableLineageError,
    PortableLineageResolver,
    portable_lineage_failure_evidence,
)
from ai_trading_system.platform.artifacts.writer import (
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.research_restart import (
    DEFAULT_RESTART_OUTPUT_ROOT,
    DEFAULT_RESTART_POLICY_PATH,
    ResearchRestartError,
    load_restart_policy,
    validate_research_restart_preflight,
)

DEFAULT_R0_PREFLIGHT_PATH = DEFAULT_RESTART_OUTPUT_ROOT / "strategy_research_restart_preflight.json"
DEFAULT_R1_WALK_FORWARD_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue" / "walk_forward_r1"
)
DEFAULT_R1_ROBUSTNESS_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue" / "robustness_r1"
)

WF_SCHEMA_VERSION = "etf_dynamic_v3_walk_forward_r1.v1"
WF_REPORT_TYPE = "etf_dynamic_v3_walk_forward_r1_report"
WF_VALIDATION_TYPE = "etf_dynamic_v3_walk_forward_r1_validation"
ROBUSTNESS_SCHEMA_VERSION = "etf_dynamic_v3_robustness_r1.v1"
ROBUSTNESS_REPORT_TYPE = "etf_dynamic_v3_robustness_r1_report"
ROBUSTNESS_VALIDATION_TYPE = "etf_dynamic_v3_robustness_r1_validation"

SAFETY: dict[str, Any] = {
    "research_only": True,
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "promotion_gate_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
    "shadow_enrollment_allowed": False,
    "automatic_candidate_generation_allowed": False,
    "manual_review_required": True,
}


class DynamicV3R1EvidenceError(ValueError):
    """Raised when R1 evidence cannot be built or validated safely."""


@dataclass(frozen=True)
class R1RuntimeContext:
    prices: pd.DataFrame
    etf_config: Any
    real_policy: Any
    v3_rescue_policy: Any
    dynamic_robustness_policy: Any
    dynamic_policy: Any
    failure_policy: Any
    data_quality_status: str
    data_quality_report_path: str
    prices_path: Path


_FOLD_WORKER_CONTEXT: dict[str, Any] = {}


def run_r1_walk_forward_evidence(
    *,
    source_sweep_dir: Path,
    top_n: int = 20,
    restart_preflight_path: Path = DEFAULT_R0_PREFLIGHT_PATH,
    restart_policy_path: Path = DEFAULT_RESTART_POLICY_PATH,
    prices_path: Path,
    output_dir: Path = DEFAULT_R1_WALK_FORWARD_DIR,
    workers: int = 4,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if top_n <= 0:
        raise DynamicV3R1EvidenceError("top_n must be positive")
    if workers <= 0:
        raise DynamicV3R1EvidenceError("workers must be positive")
    generated = generated_at or datetime.now(UTC)
    preflight = _validated_preflight(
        restart_preflight_path=restart_preflight_path,
        source_sweep_dir=source_sweep_dir,
        prices_path=prices_path,
    )
    policy = load_restart_policy(restart_policy_path)
    walk_policy = _mapping(_mapping(policy.get("r1_evidence")).get("walk_forward"))
    source = _load_source_sweep(source_sweep_dir)
    config = source["config"]
    if config.execution.evaluator != legacy.EVALUATOR_REAL_DYNAMIC_V3_RESCUE:
        raise DynamicV3R1EvidenceError("R1 walk-forward requires real evaluator source sweep")
    selected_results = _selected_source_results(source, top_n=top_n)
    if not selected_results:
        raise DynamicV3R1EvidenceError("source leaderboard has no eligible real candidates")
    runtime = _load_runtime_context(
        prices_path=prices_path,
        preflight=preflight,
    )
    trading_dates = _trading_dates(runtime.prices)
    windows = _effective_walk_forward_windows(
        config=config,
        trading_dates=trading_dates,
        policy=walk_policy,
    )
    wf_id = (
        "r1-wf_"
        + _stable_id(
            source["source_sweep_id"],
            top_n,
            _file_sha256(restart_preflight_path),
            _file_sha256(restart_policy_path),
            generated.isoformat(),
        )[:16]
    )
    wf_dir = _unique_dir(output_dir / wf_id)
    wf_dir.mkdir(parents=True, exist_ok=False)
    evaluation_root = wf_dir / "fold_evaluations"
    evaluation_root.mkdir()
    jobs: list[tuple[Any, ...]] = []
    for window in windows:
        for phase in ("train", "test"):
            phase_start = date.fromisoformat(str(window[f"effective_{phase}_start"]))
            phase_end = date.fromisoformat(str(window[f"effective_{phase}_end"]))
            jobs.extend(
                (result, window, phase, phase_start, phase_end) for result in selected_results
            )
    worker_count = min(workers, len(jobs))
    if worker_count == 1:
        evaluated = _evaluate_fold_jobs_locally(
            jobs=jobs,
            runtime=runtime,
            config=config,
            walk_policy=walk_policy,
            trading_dates=trading_dates,
            generated=generated,
        )
        evaluation_index = _write_fold_evaluations(
            evaluated=evaluated,
            evaluation_root=evaluation_root,
        )
    else:
        normalized_config_path = Path(str(source["source_artifacts"]["normalized_config_path"]))
        with ProcessPoolExecutor(
            max_workers=worker_count,
            initializer=_initialize_fold_worker,
            initargs=(
                prices_path,
                preflight,
                normalized_config_path,
                walk_policy,
                trading_dates,
                generated,
            ),
        ) as executor:
            evaluation_index = _write_fold_evaluations(
                evaluated=executor.map(_evaluate_fold_process_job, jobs, chunksize=1),
                evaluation_root=evaluation_root,
            )
    evaluation_index.sort(
        key=lambda item: (int(item["window_index"]), str(item["phase"]), str(item["candidate_id"]))
    )
    report = _build_walk_forward_report(
        wf_id=wf_id,
        source=source,
        selected_results=selected_results,
        windows=windows,
        evaluation_index=evaluation_index,
        preflight=preflight,
        generated=generated,
    )
    index_path = wf_dir / "fold_evaluations_index.json"
    report_path = wf_dir / "r1_walk_forward_report.json"
    markdown_path = wf_dir / "r1_walk_forward_report.md"
    _write_json(index_path, {"evaluations": evaluation_index})
    _write_json(report_path, report)
    write_markdown_atomic(markdown_path, render_r1_walk_forward_report(report))
    manifest = {
        "schema_version": WF_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_walk_forward_r1_manifest",
        "walk_forward_id": wf_id,
        "status": report["status"],
        "evidence_completeness": report["evidence_completeness"],
        "source_sweep_id": source["source_sweep_id"],
        "top_n": top_n,
        "window_count": len(windows),
        "candidate_count": len(selected_results),
        "evaluation_count": len(evaluation_index),
        "restart_preflight_path": str(restart_preflight_path),
        "restart_preflight_sha256": _file_sha256(restart_preflight_path),
        "restart_policy_path": str(restart_policy_path),
        "restart_policy_sha256": _file_sha256(restart_policy_path),
        "source_artifacts": source["source_artifacts"],
        "source_checksums": source["source_checksums"],
        "prices_path": str(prices_path),
        "prices_sha256": _file_sha256(prices_path),
        "generated_at": generated.isoformat(),
        "output_artifact_checksums": {
            "fold_evaluations_index.json": _file_sha256(index_path),
            "r1_walk_forward_report.json": _file_sha256(report_path),
            "r1_walk_forward_report.md": _file_sha256(markdown_path),
        },
        "safety": dict(SAFETY),
    }
    manifest_path = wf_dir / "r1_wf_manifest.json"
    _write_json(manifest_path, manifest)
    return {
        "walk_forward_id": wf_id,
        "walk_forward_dir": wf_dir,
        "manifest_path": manifest_path,
        "report_path": report_path,
        "report": report,
    }


def validate_r1_walk_forward_evidence(
    *,
    walk_forward_id: str,
    output_dir: Path = DEFAULT_R1_WALK_FORWARD_DIR,
    portable_lineage_sidecar_path: Path | None = None,
    portable_project_root: Path = PROJECT_ROOT,
    portable_lineage_policy_path: Path = DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
) -> dict[str, Any]:
    resolver: PortableLineageResolver | None = None
    try:
        if portable_lineage_sidecar_path is not None:
            resolver = PortableLineageResolver(
                sidecar_path=portable_lineage_sidecar_path,
                subject_artifact_path=output_dir / walk_forward_id / "r1_wf_manifest.json",
                consumer="r1_walk_forward",
                project_root=portable_project_root,
                policy_path=portable_lineage_policy_path,
            )
        result = _validate_r1_walk_forward_evidence(
            walk_forward_id=walk_forward_id,
            output_dir=output_dir,
            resolver=resolver,
        )
    except PortableLineageError as exc:
        assert portable_lineage_sidecar_path is not None
        return _portable_r1_validation_failure(
            report_type=WF_VALIDATION_TYPE,
            identity_key="walk_forward_id",
            identity_value=walk_forward_id,
            consumer="r1_walk_forward",
            sidecar_path=portable_lineage_sidecar_path,
            error=exc,
        )
    if resolver is not None:
        result["portable_lineage_resolution"] = resolver.evidence()
    return result


def _validate_r1_walk_forward_evidence(
    *,
    walk_forward_id: str,
    output_dir: Path,
    resolver: PortableLineageResolver | None,
) -> dict[str, Any]:
    wf_dir = output_dir / walk_forward_id
    manifest = _load_json(wf_dir / "r1_wf_manifest.json")
    index_payload = _load_json(wf_dir / "fold_evaluations_index.json")
    report = _load_json(wf_dir / "r1_walk_forward_report.json")
    checks = [
        _check("manifest_id_matches_directory", manifest.get("walk_forward_id") == walk_forward_id),
        _check("manifest_schema", manifest.get("schema_version") == WF_SCHEMA_VERSION),
        _check("report_type", report.get("report_type") == WF_REPORT_TYPE),
        _check("safety_boundary", report.get("safety") == SAFETY),
        _check(
            "restart_preflight_validation",
            _safe_preflight_validation(
                _portable_path(Path(str(manifest.get("restart_preflight_path", ""))), resolver),
                resolver=resolver,
            ),
        ),
        _check(
            "source_checksums_fresh",
            _source_checksums_fresh(manifest, resolver=resolver),
        ),
        _check(
            "prices_checksum_fresh",
            _path_checksum_fresh(manifest, "prices", resolver=resolver),
        ),
    ]
    output_checksums = _mapping(manifest.get("output_artifact_checksums"))
    for filename in (
        "fold_evaluations_index.json",
        "r1_walk_forward_report.json",
        "r1_walk_forward_report.md",
    ):
        checks.append(
            _check(
                f"output_checksum:{filename}",
                output_checksums.get(filename) == _file_sha256(wf_dir / filename),
            )
        )
    evaluations = _records(index_payload.get("evaluations"))
    expected_count = (
        int(manifest.get("candidate_count", 0)) * int(manifest.get("window_count", 0)) * 2
    )
    checks.append(_check("evaluation_count_complete", len(evaluations) == expected_count))
    source_artifacts = _mapping(manifest.get("source_artifacts"))
    source_sweep_dir = _portable_path(
        Path(str(source_artifacts.get("sweep_manifest_path", ""))), resolver
    ).parent
    source = _load_source_sweep(source_sweep_dir)
    walk_policy = _mapping(
        _mapping(
            load_restart_policy(
                _portable_path(Path(str(manifest.get("restart_policy_path", ""))), resolver)
            ).get("r1_evidence")
        ).get("walk_forward")
    )
    runtime = _load_runtime_context(
        prices_path=_portable_path(Path(str(manifest.get("prices_path", ""))), resolver),
        preflight=_load_json(
            _portable_path(Path(str(manifest.get("restart_preflight_path", ""))), resolver)
        ),
    )
    trading_dates = _trading_dates(runtime.prices)
    for item in evaluations:
        path = _portable_path(
            Path(str(item.get("evaluation_path", ""))),
            resolver,
            expected_sha256=str(item.get("evaluation_sha256", "")),
        )
        payload = _load_json_optional(path)
        recomputed_summary = _summarize_fold_payload(
            payload=payload,
            result={"candidate_id": item.get("candidate_id")},
            config=source["config"],
            policy=walk_policy,
            trading_dates=trading_dates,
            phase=str(item.get("phase", "")),
            window_index=int(item.get("window_index", -1)),
        )
        checks.extend(
            [
                _check(
                    f"evaluation_checksum:{item.get('candidate_id')}:{item.get('window_index')}:{item.get('phase')}",
                    path.is_file() and item.get("evaluation_sha256") == _file_sha256(path),
                ),
                _check(
                    f"evaluation_identity:{item.get('candidate_id')}:{item.get('window_index')}:{item.get('phase')}",
                    _fold_payload_identity_matches(item, payload),
                ),
                _check(
                    f"evaluation_summary_recomputed:{item.get('candidate_id')}:{item.get('window_index')}:{item.get('phase')}",
                    _json_equivalent(_mapping(item.get("summary")), recomputed_summary),
                ),
            ]
        )
    checks.extend(
        [
            _check(
                "report_evaluation_count",
                int(report.get("evaluation_count", -1)) == len(evaluations),
            ),
            _check(
                "selection_contamination_disclosed",
                report.get("source_selection_contamination") is True,
            ),
            _check(
                "legacy_window_role_disclosed",
                report.get("source_window_role") == "legacy_comparison",
            ),
            _check(
                "markdown_matches_report",
                (wf_dir / "r1_walk_forward_report.md").read_text(encoding="utf-8")
                == render_r1_walk_forward_report(report),
            ),
        ]
    )
    passed = all(item["passed"] for item in checks)
    return {
        "schema_version": "etf_dynamic_v3_walk_forward_r1_validation.v1",
        "report_type": WF_VALIDATION_TYPE,
        "walk_forward_id": walk_forward_id,
        "status": "PASS" if passed else "FAIL",
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
        "broker_action": "none",
    }


def run_r1_robustness_evidence(
    *,
    source_sweep_dir: Path,
    candidate_id: str,
    restart_preflight_path: Path = DEFAULT_R0_PREFLIGHT_PATH,
    restart_policy_path: Path = DEFAULT_RESTART_POLICY_PATH,
    prices_path: Path,
    output_dir: Path = DEFAULT_R1_ROBUSTNESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    preflight = _validated_preflight(
        restart_preflight_path=restart_preflight_path,
        source_sweep_dir=source_sweep_dir,
        prices_path=prices_path,
    )
    policy = load_restart_policy(restart_policy_path)
    robustness_policy = _mapping(_mapping(policy.get("r1_evidence")).get("robustness"))
    source = _load_source_sweep(source_sweep_dir)
    result_by_id = {str(row.get("candidate_id")): row for row in source["results"]}
    source_result = result_by_id.get(candidate_id)
    if source_result is None:
        raise DynamicV3R1EvidenceError(f"candidate not found in source sweep: {candidate_id}")
    source_real_path = Path(str(source_result.get("real_evaluation_artifact_path", "")))
    source_real_payload = _load_json(source_real_path)
    robustness_id = (
        "r1-robustness_"
        + _stable_id(
            source["source_sweep_id"],
            candidate_id,
            _file_sha256(restart_preflight_path),
            _file_sha256(restart_policy_path),
            generated.isoformat(),
        )[:16]
    )
    robustness_dir = _unique_dir(output_dir / robustness_id)
    robustness_dir.mkdir(parents=True, exist_ok=False)
    runtime = _load_runtime_context(prices_path=prices_path, preflight=preflight)
    sensitivity = legacy._real_sensitivity_rows(
        source_result,
        source["results"],
        source["config"],
        source_sweep_id=source["source_sweep_id"],
        sweep_dir=source_sweep_dir,
    )
    derived_paths: list[dict[str, Any]] = []
    missing_rows = [
        row
        for row in sensitivity
        if row.get("neighbor_evaluation_status") != "AVAILABLE_REAL_NEIGHBOR_EVALUATION"
    ]
    if missing_rows and robustness_policy.get("allow_lineage_locked_derived_neighbor") is True:
        fixed = precompute_dynamic_v3_fixed_robustness_reports(
            prices=runtime.prices,
            etf_config=runtime.etf_config,
            policy=runtime.real_policy,
            dynamic_robustness_policy=runtime.dynamic_robustness_policy,
            dynamic_policy=runtime.dynamic_policy,
            failure_policy=runtime.failure_policy,
            start=runtime.real_policy.market_regime.default_backtest_start,
            end=source["config"].data.end,
            data_quality_status=runtime.data_quality_status,
            data_quality_report=runtime.data_quality_report_path,
            prices_path=runtime.prices_path,
        )
        derived_root = robustness_dir / "derived_neighbor_evaluations"
        derived_root.mkdir()
        for row in missing_rows:
            target_parameters = dict(_mapping(source_result.get("parameters")))
            target_parameters[str(row["parameter"])] = row["neighbor_value"]
            derived_id = (
                "derived-neighbor_"
                + _stable_id(source["source_sweep_id"], candidate_id, target_parameters)[:16]
            )
            derived_result = {
                "candidate_id": derived_id,
                "parameters": target_parameters,
                "status": "completed",
                "evaluator_mode": legacy.EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
            }
            payload = _evaluate_candidate_payload(
                result=derived_result,
                start=runtime.real_policy.market_regime.default_backtest_start,
                end=source["config"].data.end,
                runtime=runtime,
                fixed_reports=fixed["reports"],
                generated=generated,
            )
            path = derived_root / f"{derived_id}.json"
            _write_json(path, payload)
            metrics = legacy._metrics_from_real_evaluation_payload(payload, source["config"])
            gate, gate_reasons = legacy.gate_candidate(metrics, source["config"])
            score, _breakdown = legacy.score_candidate(metrics, source["config"], gate)
            row.update(
                {
                    "neighbor_candidate_id": derived_id,
                    "neighbor_evaluation_status": "AVAILABLE_DERIVED_REAL_NEIGHBOR_EVALUATION",
                    "sensitivity_evidence_source": "lineage_locked_derived_real_evaluation",
                    "metrics_source": "fold_local_real_evaluation_artifact",
                    "neighbor_real_evaluation_artifact_path": str(path),
                    "neighbor_real_evaluation_artifact_exists": True,
                    "neighbor_real_evaluation_artifact_sha256": _file_sha256(path),
                    "neighbor_real_evaluation_report_id": payload.get(
                        "dynamic_v3_real_evaluation_report_id"
                    ),
                    "neighbor_real_evaluation_path_owned": True,
                    "neighbor_source_identity_status": "PASS_DERIVED_LINEAGE_LOCKED",
                    "neighbor_gate": gate,
                    "neighbor_reasons": ";".join(gate_reasons),
                    "neighbor_score": score,
                    "score_delta": (
                        ""
                        if score is None or source_result.get("score") is None
                        else round(float(score) - float(source_result["score"]), 6)
                    ),
                    "constraint_hit_rate": metrics.get("constraint_hit_rate"),
                    "turnover": metrics.get("turnover"),
                    "drawdown_degradation_pp": metrics.get("drawdown_degradation_pp"),
                    "derived_parameters": target_parameters,
                }
            )
            derived_paths.append(
                {
                    "candidate_id": derived_id,
                    "path": str(path),
                    "sha256": _file_sha256(path),
                    "report_id": payload.get("dynamic_v3_real_evaluation_report_id"),
                }
            )
    comparator = _dedicated_robustness_comparators(
        payload=source_real_payload,
        runtime=runtime,
        policy=robustness_policy,
    )
    available_neighbors = sum(
        1
        for row in sensitivity
        if row.get("neighbor_evaluation_status")
        in {
            "AVAILABLE_REAL_NEIGHBOR_EVALUATION",
            "AVAILABLE_DERIVED_REAL_NEIGHBOR_EVALUATION",
        }
    )
    neighbor_complete = available_neighbors == len(sensitivity) and bool(sensitivity)
    stress_complete = comparator["stress_summary"]["complete"] is True
    regime_complete = comparator["regime_summary"]["complete"] is True
    negative_count = comparator["summary"]["negative_comparator_count"]
    evidence_complete = neighbor_complete and stress_complete and regime_complete
    if not evidence_complete:
        status = "REVIEW_REQUIRED_INCOMPLETE_EVIDENCE"
    elif negative_count:
        status = "FAIL_RESEARCH_EVIDENCE"
    else:
        status = "PASS_RESEARCH_ONLY"
    report = {
        "schema_version": ROBUSTNESS_SCHEMA_VERSION,
        "report_type": ROBUSTNESS_REPORT_TYPE,
        "robustness_id": robustness_id,
        "status": status,
        "evidence_complete": evidence_complete,
        "evidence_completeness": (
            "REAL_NEIGHBOR_STRESS_REGIME_COMPLETE" if evidence_complete else "REAL_EVIDENCE_PARTIAL"
        ),
        "source_sweep_id": source["source_sweep_id"],
        "candidate_id": candidate_id,
        "source_window_id": "legacy_research_window_2022_12",
        "source_window_role": "legacy_comparison",
        "real_neighbor_count": available_neighbors,
        "required_neighbor_count": len(sensitivity),
        "derived_neighbor_count": len(derived_paths),
        "neighbor_complete": neighbor_complete,
        "sensitivity": sensitivity,
        "dedicated_stress": comparator["stress"],
        "stress_summary": comparator["stress_summary"],
        "per_regime_comparator": comparator["regimes"],
        "regime_summary": comparator["regime_summary"],
        "negative_comparator_count": negative_count,
        "source_selection_contamination": True,
        "interpretation": (
            "legacy-window robustness diagnostic only; not primary validated window evidence"
        ),
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }
    sensitivity_path = robustness_dir / "r1_sensitivity.json"
    comparator_path = robustness_dir / "r1_dedicated_comparators.json"
    report_path = robustness_dir / "r1_robustness_report.json"
    markdown_path = robustness_dir / "r1_robustness_report.md"
    _write_json(sensitivity_path, {"rows": sensitivity})
    _write_json(comparator_path, comparator)
    _write_json(report_path, report)
    write_markdown_atomic(markdown_path, render_r1_robustness_report(report))
    manifest = {
        "schema_version": ROBUSTNESS_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_robustness_r1_manifest",
        "robustness_id": robustness_id,
        "status": status,
        "source_sweep_id": source["source_sweep_id"],
        "candidate_id": candidate_id,
        "source_real_evaluation_path": str(source_real_path),
        "source_real_evaluation_sha256": _file_sha256(source_real_path),
        "restart_preflight_path": str(restart_preflight_path),
        "restart_preflight_sha256": _file_sha256(restart_preflight_path),
        "restart_policy_path": str(restart_policy_path),
        "restart_policy_sha256": _file_sha256(restart_policy_path),
        "source_artifacts": source["source_artifacts"],
        "source_checksums": source["source_checksums"],
        "prices_path": str(prices_path),
        "prices_sha256": _file_sha256(prices_path),
        "derived_neighbors": derived_paths,
        "output_artifact_checksums": {
            "r1_sensitivity.json": _file_sha256(sensitivity_path),
            "r1_dedicated_comparators.json": _file_sha256(comparator_path),
            "r1_robustness_report.json": _file_sha256(report_path),
            "r1_robustness_report.md": _file_sha256(markdown_path),
        },
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
    }
    manifest_path = robustness_dir / "r1_robustness_manifest.json"
    _write_json(manifest_path, manifest)
    return {
        "robustness_id": robustness_id,
        "robustness_dir": robustness_dir,
        "manifest_path": manifest_path,
        "report_path": report_path,
        "report": report,
    }


def validate_r1_robustness_evidence(
    *,
    robustness_id: str,
    output_dir: Path = DEFAULT_R1_ROBUSTNESS_DIR,
    portable_lineage_sidecar_path: Path | None = None,
    portable_project_root: Path = PROJECT_ROOT,
    portable_lineage_policy_path: Path = DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
) -> dict[str, Any]:
    resolver: PortableLineageResolver | None = None
    try:
        if portable_lineage_sidecar_path is not None:
            resolver = PortableLineageResolver(
                sidecar_path=portable_lineage_sidecar_path,
                subject_artifact_path=output_dir / robustness_id / "r1_robustness_manifest.json",
                consumer="r1_robustness",
                project_root=portable_project_root,
                policy_path=portable_lineage_policy_path,
            )
        result = _validate_r1_robustness_evidence(
            robustness_id=robustness_id,
            output_dir=output_dir,
            resolver=resolver,
        )
    except PortableLineageError as exc:
        assert portable_lineage_sidecar_path is not None
        return _portable_r1_validation_failure(
            report_type=ROBUSTNESS_VALIDATION_TYPE,
            identity_key="robustness_id",
            identity_value=robustness_id,
            consumer="r1_robustness",
            sidecar_path=portable_lineage_sidecar_path,
            error=exc,
        )
    if resolver is not None:
        result["portable_lineage_resolution"] = resolver.evidence()
    return result


def _validate_r1_robustness_evidence(
    *,
    robustness_id: str,
    output_dir: Path,
    resolver: PortableLineageResolver | None,
) -> dict[str, Any]:
    root = output_dir / robustness_id
    manifest = _load_json(root / "r1_robustness_manifest.json")
    report = _load_json(root / "r1_robustness_report.json")
    checks = [
        _check("manifest_id_matches_directory", manifest.get("robustness_id") == robustness_id),
        _check("manifest_schema", manifest.get("schema_version") == ROBUSTNESS_SCHEMA_VERSION),
        _check("report_type", report.get("report_type") == ROBUSTNESS_REPORT_TYPE),
        _check("safety_boundary", report.get("safety") == SAFETY),
        _check(
            "restart_preflight_validation",
            _safe_preflight_validation(
                _portable_path(Path(str(manifest.get("restart_preflight_path", ""))), resolver),
                resolver=resolver,
            ),
        ),
        _check(
            "source_checksums_fresh",
            _source_checksums_fresh(manifest, resolver=resolver),
        ),
        _check(
            "prices_checksum_fresh",
            _path_checksum_fresh(manifest, "prices", resolver=resolver),
        ),
        _check(
            "source_real_evaluation_fresh",
            _path_checksum_fresh(manifest, "source_real_evaluation", resolver=resolver),
        ),
        _check(
            "legacy_window_role_disclosed", report.get("source_window_role") == "legacy_comparison"
        ),
        _check(
            "selection_contamination_disclosed",
            report.get("source_selection_contamination") is True,
        ),
    ]
    for item in _records(manifest.get("derived_neighbors")):
        path = _portable_path(
            Path(str(item.get("path", ""))),
            resolver,
            expected_sha256=str(item.get("sha256", "")),
        )
        payload = _load_json_optional(path)
        checks.extend(
            [
                _check(
                    f"derived_neighbor_checksum:{item.get('candidate_id')}",
                    path.is_file() and item.get("sha256") == _file_sha256(path),
                ),
                _check(
                    f"derived_neighbor_identity:{item.get('candidate_id')}",
                    payload.get("dynamic_v3_real_evaluation_report_id") == item.get("report_id")
                    and payload.get("evaluator_mode") in {None, "real_dynamic_v3_rescue"},
                ),
            ]
        )
    output_checksums = _mapping(manifest.get("output_artifact_checksums"))
    for filename in (
        "r1_sensitivity.json",
        "r1_dedicated_comparators.json",
        "r1_robustness_report.json",
        "r1_robustness_report.md",
    ):
        checks.append(
            _check(
                f"output_checksum:{filename}",
                output_checksums.get(filename) == _file_sha256(root / filename),
            )
        )
    comparator_payload = _load_json(root / "r1_dedicated_comparators.json")
    runtime = _load_runtime_context(
        prices_path=_portable_path(Path(str(manifest.get("prices_path", ""))), resolver),
        preflight=_load_json(
            _portable_path(Path(str(manifest.get("restart_preflight_path", ""))), resolver)
        ),
    )
    robustness_policy = _mapping(
        _mapping(
            load_restart_policy(
                _portable_path(Path(str(manifest.get("restart_policy_path", ""))), resolver)
            ).get("r1_evidence")
        ).get("robustness")
    )
    recomputed_comparator = _dedicated_robustness_comparators(
        payload=_load_json(
            _portable_path(Path(str(manifest.get("source_real_evaluation_path", ""))), resolver)
        ),
        runtime=runtime,
        policy=robustness_policy,
    )
    checks.extend(
        [
            _check(
                "dedicated_comparator_recomputed",
                comparator_payload == recomputed_comparator,
            ),
            _check(
                "report_stress_matches_recomputed",
                report.get("dedicated_stress") == recomputed_comparator["stress"]
                and report.get("stress_summary") == recomputed_comparator["stress_summary"],
            ),
            _check(
                "report_regimes_match_recomputed",
                report.get("per_regime_comparator") == recomputed_comparator["regimes"]
                and report.get("regime_summary") == recomputed_comparator["regime_summary"],
            ),
        ]
    )
    checks.append(
        _check(
            "markdown_matches_report",
            (root / "r1_robustness_report.md").read_text(encoding="utf-8")
            == render_r1_robustness_report(report),
        )
    )
    evidence_complete = (
        report.get("neighbor_complete") is True
        and _mapping(report.get("stress_summary")).get("complete") is True
        and _mapping(report.get("regime_summary")).get("complete") is True
    )
    checks.append(
        _check(
            "evidence_complete_derived_from_components",
            report.get("evidence_complete") is evidence_complete,
        )
    )
    passed = all(item["passed"] for item in checks)
    return {
        "schema_version": "etf_dynamic_v3_robustness_r1_validation.v1",
        "report_type": ROBUSTNESS_VALIDATION_TYPE,
        "robustness_id": robustness_id,
        "status": "PASS" if passed else "FAIL",
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
        "broker_action": "none",
    }


def render_r1_walk_forward_report(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("oos_summary"))
    lines = [
        "# Dynamic v3 R1 Walk-forward OOS Evidence",
        "",
        f"- status: `{payload.get('status')}`",
        f"- evidence_completeness: `{payload.get('evidence_completeness')}`",
        f"- source_window_role: `{payload.get('source_window_role')}`",
        f"- fold_local_evaluator: `{payload.get('fold_local_evaluator')}`",
        f"- source_selection_contamination: `{payload.get('source_selection_contamination')}`",
        f"- evaluation_count: `{payload.get('evaluation_count')}`",
        f"- negative_test_count: `{summary.get('negative_test_count')}`",
        "",
        "## Interpretation",
        "",
        "- 本报告使用逐 fold 真实 evaluator，不复用 full-period metrics。",
        "- source candidate universe 来自 full-period leaderboard，且与 locked holdout 重叠，",
        "  因此结果最高只能作为 legacy-window diagnostic，不能声称 unbiased OOS PASS。",
        "- production_effect=none；不自动 promotion、paper-shadow、weights 或 broker action。",
        "",
    ]
    return "\n".join(lines)


def render_r1_robustness_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Dynamic v3 R1 Robustness Evidence",
        "",
        f"- status: `{payload.get('status')}`",
        f"- evidence_complete: `{payload.get('evidence_complete')}`",
        f"- source_window_role: `{payload.get('source_window_role')}`",
        f"- real_neighbor_count: `{payload.get('real_neighbor_count')}` / "
        f"`{payload.get('required_neighbor_count')}`",
        f"- derived_neighbor_count: `{payload.get('derived_neighbor_count')}`",
        f"- negative_comparator_count: `{payload.get('negative_comparator_count')}`",
        "",
        "## Interpretation",
        "",
        "- stress buckets 来自 real daily path 的专用日期集合，不复用 aggregate proxy。",
        "- per-regime comparator 比较 dynamic/static return、drawdown、cost、false-risk-off。",
        "- 本结果只属于 2022-12-01 legacy comparison，不是 2021 primary validated evidence。",
        "- production_effect=none；不自动 promotion、paper-shadow、weights 或 broker action。",
        "",
    ]
    return "\n".join(lines)


def _evaluate_fold_job(
    job: tuple[Any, ...],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    (
        result,
        window,
        phase,
        start,
        end,
        runtime,
        config,
        fixed_reports,
        walk_policy,
        trading_dates,
        generated,
    ) = job
    payload = _evaluate_candidate_payload(
        result=result,
        start=start,
        end=end,
        runtime=runtime,
        fixed_reports=fixed_reports,
        generated=generated,
    )
    payload = {
        **payload,
        "r1_fold_context": {
            "source_candidate_id": result.get("candidate_id"),
            "source_candidate_parameters": result.get("parameters", {}),
            "window_index": window["window_index"],
            "phase": phase,
            "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
            "purge_trading_days": walk_policy.get("purge_trading_days"),
            "embargo_trading_days": walk_policy.get("embargo_trading_days"),
            "fold_local_evaluator": True,
            "source_selection_contamination": True,
        },
        "evaluator_mode": legacy.EVALUATOR_REAL_DYNAMIC_V3_RESCUE,
        "evaluator_version": legacy.EVALUATOR_VERSIONS[legacy.EVALUATOR_REAL_DYNAMIC_V3_RESCUE],
        "production_effect": "none",
        "broker_action": "none",
    }
    summary = _summarize_fold_payload(
        payload=payload,
        result=result,
        config=config,
        policy=walk_policy,
        trading_dates=trading_dates,
        phase=phase,
        window_index=int(window["window_index"]),
    )
    return result, payload, summary


def _initialize_fold_worker(
    prices_path: Path,
    preflight: Mapping[str, Any],
    normalized_config_path: Path,
    walk_policy: Mapping[str, Any],
    trading_dates: Sequence[date],
    generated: datetime,
) -> None:
    global _FOLD_WORKER_CONTEXT
    _FOLD_WORKER_CONTEXT = {
        "runtime": _load_runtime_context(prices_path=prices_path, preflight=preflight),
        "config": legacy.load_parameter_sweep_config(normalized_config_path),
        "walk_policy": dict(walk_policy),
        "trading_dates": list(trading_dates),
        "generated": generated,
        "fixed_reports": {},
    }


def _evaluate_fold_process_job(
    job: tuple[Any, ...],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not _FOLD_WORKER_CONTEXT:
        raise DynamicV3R1EvidenceError("R1 fold worker was not initialized")
    result, window, phase, start, end = job
    runtime = _FOLD_WORKER_CONTEXT["runtime"]
    cache = _FOLD_WORKER_CONTEXT["fixed_reports"]
    cache_key = (start, end)
    if cache_key not in cache:
        cache[cache_key] = _precompute_fixed_reports(runtime=runtime, start=start, end=end)
    return _evaluate_fold_job(
        (
            result,
            window,
            phase,
            start,
            end,
            runtime,
            _FOLD_WORKER_CONTEXT["config"],
            cache[cache_key],
            _FOLD_WORKER_CONTEXT["walk_policy"],
            _FOLD_WORKER_CONTEXT["trading_dates"],
            _FOLD_WORKER_CONTEXT["generated"],
        )
    )


def _evaluate_fold_jobs_locally(
    *,
    jobs: Sequence[tuple[Any, ...]],
    runtime: R1RuntimeContext,
    config: Any,
    walk_policy: Mapping[str, Any],
    trading_dates: Sequence[date],
    generated: datetime,
) -> Any:
    fixed_cache: dict[tuple[date, date], Mapping[str, Mapping[str, Any]]] = {}
    for result, window, phase, start, end in jobs:
        cache_key = (start, end)
        if cache_key not in fixed_cache:
            fixed_cache[cache_key] = _precompute_fixed_reports(
                runtime=runtime,
                start=start,
                end=end,
            )
        yield _evaluate_fold_job(
            (
                result,
                window,
                phase,
                start,
                end,
                runtime,
                config,
                fixed_cache[cache_key],
                walk_policy,
                trading_dates,
                generated,
            )
        )


def _precompute_fixed_reports(
    *, runtime: R1RuntimeContext, start: date, end: date
) -> Mapping[str, Mapping[str, Any]]:
    fixed = precompute_dynamic_v3_fixed_robustness_reports(
        prices=runtime.prices,
        etf_config=runtime.etf_config,
        policy=runtime.real_policy,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
        dynamic_policy=runtime.dynamic_policy,
        failure_policy=runtime.failure_policy,
        start=start,
        end=end,
        data_quality_status=runtime.data_quality_status,
        data_quality_report=runtime.data_quality_report_path,
        prices_path=runtime.prices_path,
    )
    return cast(Mapping[str, Mapping[str, Any]], fixed["reports"])


def _write_fold_evaluations(*, evaluated: Any, evaluation_root: Path) -> list[dict[str, Any]]:
    evaluation_index = []
    for result, payload, summary in evaluated:
        context = _mapping(payload.get("r1_fold_context"))
        candidate_id = str(result["candidate_id"])
        candidate_dir = evaluation_root / candidate_id
        candidate_dir.mkdir(exist_ok=True)
        filename = f"window_{context['window_index']}_{context['phase']}.json"
        path = candidate_dir / filename
        _write_json(path, payload)
        evaluation_index.append(
            {
                "window_index": context["window_index"],
                "phase": context["phase"],
                "candidate_id": candidate_id,
                "selected_parameters": result.get("parameters", {}),
                "evaluation_path": str(path),
                "evaluation_sha256": _file_sha256(path),
                "summary": summary,
            }
        )
    return evaluation_index


def _evaluate_candidate_payload(
    *,
    result: Mapping[str, Any],
    start: date,
    end: date,
    runtime: R1RuntimeContext,
    fixed_reports: Mapping[str, Mapping[str, Any]],
    generated: datetime,
) -> dict[str, Any]:
    real_policy = legacy._real_policy_for_sweep_candidate(
        runtime.real_policy, _mapping(result.get("parameters"))
    )
    v3_policy = legacy._real_rescue_policy_for_sweep_candidate(
        runtime.v3_rescue_policy, _mapping(result.get("parameters"))
    )
    return build_dynamic_v3_real_evaluation_report(
        prices=runtime.prices,
        etf_config=runtime.etf_config,
        policy=real_policy,
        v3_rescue_policy=v3_policy,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
        dynamic_policy=runtime.dynamic_policy,
        failure_policy=runtime.failure_policy,
        start=start,
        end=end,
        data_quality_status=runtime.data_quality_status,
        data_quality_report=runtime.data_quality_report_path,
        prices_path=runtime.prices_path,
        generated_at=generated,
        precomputed_robustness_reports=fixed_reports,
    )


def _summarize_fold_payload(
    *,
    payload: Mapping[str, Any],
    result: Mapping[str, Any],
    config: Any,
    policy: Mapping[str, Any],
    trading_dates: Sequence[date],
    phase: str,
    window_index: int,
) -> dict[str, Any]:
    paths = _mapping(payload.get("comparison_daily_paths"))
    dynamic_rows = _records(paths.get("dynamic_candidate"))
    static_rows = _records(paths.get("static_base_candidate"))
    qqq_rows = _records(paths.get("QQQ_buy_and_hold"))
    chronology = _chronology_summary(dynamic_rows, trading_dates)
    false_signal = _false_signal_summary(
        dynamic_rows=dynamic_rows,
        static_rows=static_rows,
        benchmark_rows=qqq_rows,
    )
    metrics = legacy._metrics_from_real_evaluation_payload(payload, config)
    gate, gate_reasons = legacy.gate_candidate(metrics, config)
    score, score_breakdown = legacy.score_candidate(metrics, config, gate)
    row_floor = int(
        policy.get("minimum_train_rows" if phase == "train" else "minimum_test_rows", 1)
    )
    cost_complete = bool(dynamic_rows) and all(
        "gross_return" in row and "transaction_cost" in row and "strategy_return" in row
        for row in dynamic_rows
    )
    required_fields_complete = (
        len(dynamic_rows) >= row_floor
        and len(dynamic_rows) == len(static_rows) == len(qqq_rows)
        and chronology["status"] == "PASS"
        and cost_complete
        and false_signal["status"] == "PASS"
        and str(metrics.get("data_quality")) in {"PASS", "PASS_WITH_WARNINGS"}
        and str(metrics.get("lookahead_status")) == "PASS"
    )
    evidence_status = "COMPLETE" if required_fields_complete else "INCOMPLETE"
    return {
        "window_index": window_index,
        "phase": phase,
        "candidate_id": result.get("candidate_id"),
        "requested_range": payload.get("requested_range", {}),
        "actual_range": {
            "start": dynamic_rows[0].get("signal_date") if dynamic_rows else None,
            "end": dynamic_rows[-1].get("signal_date") if dynamic_rows else None,
        },
        "row_count": len(dynamic_rows),
        "required_row_floor": row_floor,
        "evidence_status": evidence_status,
        "gate": gate,
        "gate_reasons": gate_reasons,
        "selection_score": score,
        "score_breakdown": score_breakdown,
        "metrics": metrics,
        "chronology": chronology,
        "transaction_cost": {
            "fields_complete": cost_complete,
            "total_transaction_cost": round(
                sum(float(row.get("transaction_cost") or 0.0) for row in dynamic_rows), 10
            ),
            "gross_return_sum": round(
                sum(float(row.get("gross_return") or 0.0) for row in dynamic_rows), 10
            ),
            "net_return_sum": round(
                sum(float(row.get("strategy_return") or 0.0) for row in dynamic_rows), 10
            ),
        },
        "false_signal": false_signal,
        "fold_local_evaluator": True,
        "source_selection_contamination": True,
        "production_effect": "none",
    }


def _build_walk_forward_report(
    *,
    wf_id: str,
    source: Mapping[str, Any],
    selected_results: Sequence[Mapping[str, Any]],
    windows: Sequence[Mapping[str, Any]],
    evaluation_index: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    generated: datetime,
) -> dict[str, Any]:
    summaries = [_mapping(item.get("summary")) for item in evaluation_index]
    complete_count = sum(item.get("evidence_status") == "COMPLETE" for item in summaries)
    test_rows = [item for item in summaries if item.get("phase") == "test"]
    negative_tests = [item for item in test_rows if item.get("gate") == legacy.GATE_REJECT]
    all_complete = complete_count == len(summaries) and bool(summaries)
    holdout = source["config"].out_of_sample
    holdout_overlap = any(
        _ranges_overlap(
            date.fromisoformat(str(window["effective_train_start"])),
            date.fromisoformat(str(window["effective_test_end"])),
            holdout.holdout_start,
            holdout.holdout_end,
        )
        for window in windows
    )
    if not all_complete:
        status = "INCOMPLETE"
        completeness = "FOLD_LOCAL_PARTIAL"
    elif negative_tests:
        status = "FAIL_RESEARCH_EVIDENCE"
        completeness = "FOLD_LOCAL_COMPLETE_WITH_SELECTION_CONTAMINATION"
    else:
        status = "REVIEW_REQUIRED_SELECTION_CONTAMINATION"
        completeness = "FOLD_LOCAL_COMPLETE_WITH_SELECTION_CONTAMINATION"
    candidate_summaries = []
    for result in selected_results:
        candidate_id = str(result.get("candidate_id"))
        candidate_tests = [item for item in test_rows if item.get("candidate_id") == candidate_id]
        candidate_summaries.append(
            {
                "candidate_id": candidate_id,
                "test_count": len(candidate_tests),
                "complete_test_count": sum(
                    item.get("evidence_status") == "COMPLETE" for item in candidate_tests
                ),
                "negative_test_count": sum(
                    item.get("gate") == legacy.GATE_REJECT for item in candidate_tests
                ),
                "review_required_test_count": sum(
                    item.get("gate") == legacy.GATE_REVIEW_REQUIRED for item in candidate_tests
                ),
            }
        )
    return {
        "schema_version": WF_SCHEMA_VERSION,
        "report_type": WF_REPORT_TYPE,
        "walk_forward_id": wf_id,
        "status": status,
        "evidence_completeness": completeness,
        "fold_local_evaluator": True,
        "source_sweep_id": source["source_sweep_id"],
        "source_window_id": "legacy_research_window_2022_12",
        "source_window_role": "legacy_comparison",
        "primary_validated_window_id": "exact_three_asset_validated",
        "primary_validated_window_start": "2021-02-22",
        "source_candidate_selection_method": "full_period_source_leaderboard_top_n",
        "source_selection_contamination": True,
        "locked_holdout_overlap": holdout_overlap,
        "locked_holdout": {
            "start": holdout.holdout_start.isoformat(),
            "end": holdout.holdout_end.isoformat(),
        },
        "candidate_count": len(selected_results),
        "window_count": len(windows),
        "evaluation_count": len(evaluation_index),
        "complete_evaluation_count": complete_count,
        "windows": list(windows),
        "candidate_summaries": candidate_summaries,
        "oos_summary": {
            "test_count": len(test_rows),
            "negative_test_count": len(negative_tests),
            "review_required_test_count": sum(
                item.get("gate") == legacy.GATE_REVIEW_REQUIRED for item in test_rows
            ),
            "unbiased_oos_claim_allowed": False,
            "primary_window_conclusion_allowed": False,
        },
        "r0_preflight_status": preflight.get("status"),
        "interpretation": (
            "fold-local legacy-window diagnostic; source selection and locked holdout overlap "
            "prevent an unbiased OOS or primary-window claim"
        ),
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _dedicated_robustness_comparators(
    *,
    payload: Mapping[str, Any],
    runtime: R1RuntimeContext,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    paths = _mapping(payload.get("comparison_daily_paths"))
    dynamic = _records(paths.get("dynamic_candidate"))
    static = _records(paths.get("static_base_candidate"))
    benchmark = _records(paths.get("QQQ_buy_and_hold"))
    aligned = _aligned_paths(dynamic, static, benchmark)
    if not aligned:
        raise DynamicV3R1EvidenceError("source real evaluation daily paths are not aligned")
    dynamic_by_date, static_by_date, benchmark_by_date = aligned
    ordered_dates = sorted(dynamic_by_date)
    static_returns = [float(static_by_date[day]["strategy_return"]) for day in ordered_dates]
    static_equity = 1.0
    static_peak = 1.0
    static_drawdown_by_date: dict[date, float] = {}
    for day, value in zip(ordered_dates, static_returns, strict=True):
        static_equity *= 1.0 + value
        static_peak = max(static_peak, static_equity)
        static_drawdown_by_date[day] = static_equity / static_peak - 1.0
    high_drawdown_threshold = float(policy.get("high_drawdown_threshold", -0.10))
    high_drawdown_dates = [
        day for day in ordered_dates if static_drawdown_by_date[day] <= high_drawdown_threshold
    ]
    recovery_lookback = int(policy.get("fast_recovery_lookback_trading_days", 20))
    recovery_floor = float(policy.get("fast_recovery_min_rebound", 0.05))
    fast_recovery_dates: list[date] = []
    for index, day in enumerate(ordered_dates):
        if index + 1 < recovery_lookback:
            continue
        window = static_returns[index + 1 - recovery_lookback : index + 1]
        if _compounded_return(window) >= recovery_floor:
            fast_recovery_dates.append(day)
    stress = [
        _comparator_for_dates(
            comparator_id="high_drawdown",
            dates=high_drawdown_dates,
            dynamic_by_date=dynamic_by_date,
            static_by_date=static_by_date,
            benchmark_by_date=benchmark_by_date,
            runtime=runtime,
            policy=policy,
            method=(f"static_path_drawdown_lte_{high_drawdown_threshold:.4f}"),
        ),
        _comparator_for_dates(
            comparator_id="fast_recovery",
            dates=fast_recovery_dates,
            dynamic_by_date=dynamic_by_date,
            static_by_date=static_by_date,
            benchmark_by_date=benchmark_by_date,
            runtime=runtime,
            policy=policy,
            method=(f"static_{recovery_lookback}d_compounded_return_gte_{recovery_floor:.4f}"),
        ),
    ]
    regimes = []
    regime_dates: dict[str, list[date]] = {}
    for day in ordered_dates:
        regime_dates.setdefault(
            str(dynamic_by_date[day].get("selected_regime", "UNKNOWN")), []
        ).append(day)
    for regime, dates in sorted(regime_dates.items()):
        regimes.append(
            _comparator_for_dates(
                comparator_id=regime,
                dates=dates,
                dynamic_by_date=dynamic_by_date,
                static_by_date=static_by_date,
                benchmark_by_date=benchmark_by_date,
                runtime=runtime,
                policy=policy,
                method="dynamic_path_selected_regime",
            )
        )
    stress_complete = all(item["evidence_complete"] for item in stress)
    regime_complete = bool(regimes) and all(item["evidence_complete"] for item in regimes)
    all_items = [*stress, *regimes]
    negative_count = sum(item["gate"] == "FAIL" for item in all_items)
    return {
        "stress": stress,
        "stress_summary": {
            "complete": stress_complete,
            "bucket_count": len(stress),
            "negative_count": sum(item["gate"] == "FAIL" for item in stress),
        },
        "regimes": regimes,
        "regime_summary": {
            "complete": regime_complete,
            "regime_count": len(regimes),
            "negative_count": sum(item["gate"] == "FAIL" for item in regimes),
        },
        "summary": {
            "negative_comparator_count": negative_count,
            "comparator_count": len(all_items),
            "dedicated_path_evidence": True,
        },
    }


def _comparator_for_dates(
    *,
    comparator_id: str,
    dates: Sequence[date],
    dynamic_by_date: Mapping[date, Mapping[str, Any]],
    static_by_date: Mapping[date, Mapping[str, Any]],
    benchmark_by_date: Mapping[date, Mapping[str, Any]],
    runtime: R1RuntimeContext,
    policy: Mapping[str, Any],
    method: str,
) -> dict[str, Any]:
    ordered = sorted(day for day in dates if day in dynamic_by_date and day in static_by_date)
    dynamic_rows = [dict(dynamic_by_date[day]) for day in ordered]
    static_rows = [dict(static_by_date[day]) for day in ordered]
    benchmark_rows = [dict(benchmark_by_date[day]) for day in ordered]
    dynamic_returns = [float(row["strategy_return"]) for row in dynamic_rows]
    static_returns = [float(row["strategy_return"]) for row in static_rows]
    dynamic_total = _compounded_return(dynamic_returns)
    static_total = _compounded_return(static_returns)
    dynamic_drawdown = legacy._max_drawdown_from_returns(dynamic_returns)
    static_drawdown = legacy._max_drawdown_from_returns(static_returns)
    false_signal = _false_signal_summary(
        dynamic_rows=dynamic_rows,
        static_rows=static_rows,
        benchmark_rows=benchmark_rows,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
    )
    minimum_rows = int(
        policy.get(
            (
                "minimum_regime_rows"
                if method == "dynamic_path_selected_regime"
                else "minimum_stress_rows"
            ),
            1,
        )
    )
    evidence_complete = len(ordered) >= minimum_rows and false_signal["status"] == "PASS"
    return_gap = dynamic_total - static_total
    drawdown_degradation = abs(dynamic_drawdown) - abs(static_drawdown)
    if not evidence_complete:
        gate = "REVIEW_REQUIRED"
    elif return_gap < float(
        policy.get("min_dynamic_vs_static_return_gap", -0.01)
    ) or drawdown_degradation > float(policy.get("max_drawdown_degradation_pp", 0.02)):
        gate = "FAIL"
    else:
        gate = "PASS"
    return {
        "comparator_id": comparator_id,
        "method": method,
        "row_count": len(ordered),
        "required_row_floor": minimum_rows,
        "first_date": ordered[0].isoformat() if ordered else None,
        "last_date": ordered[-1].isoformat() if ordered else None,
        "dynamic_total_return": round(dynamic_total, 10),
        "static_total_return": round(static_total, 10),
        "dynamic_vs_static_return_gap": round(return_gap, 10),
        "dynamic_max_drawdown": round(dynamic_drawdown, 10),
        "static_max_drawdown": round(static_drawdown, 10),
        "drawdown_degradation_pp": round(drawdown_degradation, 10),
        "dynamic_turnover": round(
            sum(float(row.get("turnover") or 0.0) for row in dynamic_rows), 10
        ),
        "dynamic_transaction_cost": round(
            sum(float(row.get("transaction_cost") or 0.0) for row in dynamic_rows), 10
        ),
        "false_risk_off_count": false_signal.get("false_risk_off_count"),
        "false_risk_on_count": false_signal.get("false_risk_on_count"),
        "evidence_complete": evidence_complete,
        "gate": gate,
        "production_effect": "none",
    }


def _false_signal_summary(
    *,
    dynamic_rows: Sequence[Mapping[str, Any]],
    static_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
    dynamic_robustness_policy: Any | None = None,
) -> dict[str, Any]:
    if not dynamic_rows or not static_rows or not benchmark_rows:
        return {"status": "FAIL", "reason": "missing_aligned_daily_paths"}
    try:
        static_weights = json.loads(str(static_rows[0].get("target_weights_json", "{}")))
        diagnostics = _false_signal_diagnostics(
            dynamic_daily=pd.DataFrame(dynamic_rows),
            static_base_daily=pd.DataFrame(static_rows),
            benchmark_daily=pd.DataFrame(benchmark_rows),
            static_base_weights=static_weights,
            policy=dynamic_robustness_policy or load_dynamic_robustness_policy_config(),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {"status": "FAIL", "reason": str(exc)}
    return {
        "status": "PASS",
        "method": "dynamic_robustness_false_signal_policy_v1",
        "forward_horizon_days": diagnostics.get("forward_horizon_days"),
        "false_risk_off_count": _mapping(diagnostics.get("false_risk_off")).get("event_count"),
        "false_risk_on_count": _mapping(diagnostics.get("false_risk_on")).get("event_count"),
    }


def _chronology_summary(
    rows: Sequence[Mapping[str, Any]], trading_dates: Sequence[date]
) -> dict[str, Any]:
    positions = {day: index for index, day in enumerate(trading_dates)}
    invalid = 0
    execution_lags: Counter[int] = Counter()
    outcome_lags: Counter[int] = Counter()
    for row in rows:
        signal = _date(row.get("signal_date"))
        execution = _date(row.get("execution_date"))
        outcome = _date(row.get("return_date"))
        if (
            signal is None
            or execution is None
            or outcome is None
            or signal not in positions
            or execution not in positions
            or outcome not in positions
            or not (positions[signal] < positions[execution] <= positions[outcome])
        ):
            invalid += 1
            continue
        execution_lags[positions[execution] - positions[signal]] += 1
        outcome_lags[positions[outcome] - positions[execution]] += 1
    return {
        "status": "PASS" if rows and invalid == 0 else "FAIL",
        "row_count": len(rows),
        "invalid_row_count": invalid,
        "execution_lag_trading_days": dict(sorted(execution_lags.items())),
        "outcome_lag_trading_days": dict(sorted(outcome_lags.items())),
    }


def _effective_walk_forward_windows(
    *, config: Any, trading_dates: Sequence[date], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    purge = int(policy.get("purge_trading_days", 0))
    embargo = int(policy.get("embargo_trading_days", 0))
    windows = []
    for index, raw in enumerate(legacy.walk_forward_windows(config), start=1):
        train_start = date.fromisoformat(str(raw["train_start"]))
        train_end = _shift_trading_day(
            date.fromisoformat(str(raw["train_end"])), trading_dates, -purge, side="backward"
        )
        test_start = _shift_trading_day(
            date.fromisoformat(str(raw["test_start"])), trading_dates, embargo, side="forward"
        )
        test_end = _shift_trading_day(
            date.fromisoformat(str(raw["test_end"])), trading_dates, 0, side="backward"
        )
        train_start_effective = _shift_trading_day(train_start, trading_dates, 0, side="forward")
        if train_end >= test_start or train_start_effective >= train_end or test_start >= test_end:
            raise DynamicV3R1EvidenceError(f"invalid purged/embargo window {index}")
        windows.append(
            {
                "window_index": index,
                **raw,
                "effective_train_start": train_start_effective.isoformat(),
                "effective_train_end": train_end.isoformat(),
                "effective_test_start": test_start.isoformat(),
                "effective_test_end": test_end.isoformat(),
                "purge_trading_days": purge,
                "embargo_trading_days": embargo,
            }
        )
    return windows


def _shift_trading_day(
    target: date,
    trading_dates: Sequence[date],
    offset: int,
    *,
    side: str,
) -> date:
    if side == "forward":
        base = next((index for index, day in enumerate(trading_dates) if day >= target), None)
    else:
        base = next(
            (
                index
                for index in range(len(trading_dates) - 1, -1, -1)
                if trading_dates[index] <= target
            ),
            None,
        )
    if base is None:
        raise DynamicV3R1EvidenceError(f"trading date coverage missing around {target}")
    index = base + offset
    if index < 0 or index >= len(trading_dates):
        raise DynamicV3R1EvidenceError(f"trading date offset outside coverage: {target} {offset}")
    return trading_dates[index]


def _load_runtime_context(*, prices_path: Path, preflight: Mapping[str, Any]) -> R1RuntimeContext:
    etf_config = load_etf_config_bundle()
    prices, quality = load_standard_prices(
        prices_path,
        etf_config.assets,
        etf_config.strategy,
    )
    if not quality.passed:
        raise DynamicV3R1EvidenceError(f"ETF standard price validation failed: {quality.status}")
    dq = _mapping(preflight.get("data_quality_gate"))
    return R1RuntimeContext(
        prices=prices,
        etf_config=etf_config,
        real_policy=load_dynamic_v3_real_evaluation_policy_config(
            DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH
        ),
        v3_rescue_policy=load_dynamic_v3_rescue_policy_config(),
        dynamic_robustness_policy=load_dynamic_robustness_policy_config(),
        dynamic_policy=load_dynamic_allocation_policy_config(),
        failure_policy=load_dynamic_failure_diagnostics_policy_config(),
        data_quality_status=str(dq.get("status", "UNKNOWN")),
        data_quality_report_path=str(_mapping(preflight.get("artifact_paths")).get("markdown", "")),
        prices_path=prices_path,
    )


def _validated_preflight(
    *, restart_preflight_path: Path, source_sweep_dir: Path, prices_path: Path
) -> dict[str, Any]:
    validation = validate_research_restart_preflight(artifact_path=restart_preflight_path)
    if validation["status"] != "PASS":
        raise ResearchRestartError("R0 preflight validation failed before R1")
    payload = _load_json(restart_preflight_path)
    if payload.get("research_execution_unblocked") is not True:
        raise ResearchRestartError("R0 preflight does not unblock research execution")
    if Path(str(payload.get("source_sweep_dir", ""))).resolve() != source_sweep_dir.resolve():
        raise ResearchRestartError("R1 source sweep differs from R0 preflight")
    price_record = _mapping(_mapping(payload.get("input_fingerprints")).get("prices"))
    if Path(
        str(price_record.get("path", ""))
    ).resolve() != prices_path.resolve() or price_record.get("sha256") != _file_sha256(prices_path):
        raise ResearchRestartError("R1 prices differ from R0 PIT snapshot")
    return payload


def _load_source_sweep(source_sweep_dir: Path) -> dict[str, Any]:
    paths = {
        "sweep_manifest_path": source_sweep_dir / "sweep_manifest.json",
        "normalized_config_path": source_sweep_dir / "sweep_config.normalized.yaml",
        "leaderboard_path": source_sweep_dir / "leaderboard.json",
        "candidate_results_path": source_sweep_dir / "candidate_results.jsonl",
    }
    for path in paths.values():
        if not path.is_file():
            raise DynamicV3R1EvidenceError(f"source sweep artifact missing: {path}")
    manifest = _load_json(paths["sweep_manifest_path"])
    source_sweep_id = str(manifest.get("sweep_id", ""))
    if source_sweep_dir.name != source_sweep_id:
        raise DynamicV3R1EvidenceError("source sweep directory does not match manifest id")
    return {
        "source_sweep_id": source_sweep_id,
        "manifest": manifest,
        "config": legacy.load_parameter_sweep_config(paths["normalized_config_path"]),
        "leaderboard": _load_json(paths["leaderboard_path"]),
        "results": legacy._read_candidate_results(source_sweep_dir),
        "source_artifacts": {name: str(path) for name, path in paths.items()},
        "source_checksums": {
            name.removesuffix("_path") + "_sha256": _file_sha256(path)
            for name, path in paths.items()
        },
    }


def _selected_source_results(source: Mapping[str, Any], *, top_n: int) -> list[dict[str, Any]]:
    ids = [
        str(item.get("candidate_id", ""))
        for item in _records(_mapping(source.get("leaderboard")).get("top_eligible_candidates"))[
            :top_n
        ]
    ]
    by_id = {
        str(item.get("candidate_id", "")): item
        for item in _records(source.get("results"))
        if item.get("status") == "completed"
    }
    missing = [candidate_id for candidate_id in ids if candidate_id not in by_id]
    if missing:
        raise DynamicV3R1EvidenceError(
            f"selected source candidates missing completed results: {', '.join(missing)}"
        )
    return [by_id[candidate_id] for candidate_id in ids]


def _trading_dates(prices: pd.DataFrame) -> list[date]:
    values: Any
    if "date" in prices.columns:
        values = prices["date"]
    elif isinstance(prices.index, pd.MultiIndex):
        values = prices.index.get_level_values(0)
    else:
        values = prices.index
    result = sorted({pd.Timestamp(value).date() for value in values})
    if not result:
        raise DynamicV3R1EvidenceError("standard price frame has no trading dates")
    return result


def _aligned_paths(
    dynamic: Sequence[Mapping[str, Any]],
    static: Sequence[Mapping[str, Any]],
    benchmark: Sequence[Mapping[str, Any]],
) -> (
    tuple[dict[date, dict[str, Any]], dict[date, dict[str, Any]], dict[date, dict[str, Any]]] | None
):
    frames = []
    for rows in (dynamic, static, benchmark):
        mapped: dict[date, dict[str, Any]] = {}
        for row in rows:
            day = _date(row.get("signal_date"))
            if day is None or day in mapped:
                return None
            mapped[day] = dict(row)
        frames.append(mapped)
    if not frames[0] or set(frames[0]) != set(frames[1]) or set(frames[0]) != set(frames[2]):
        return None
    return frames[0], frames[1], frames[2]


def _fold_payload_identity_matches(item: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    context = _mapping(payload.get("r1_fold_context"))
    return (
        context.get("source_candidate_id") == item.get("candidate_id")
        and int(context.get("window_index", -1)) == int(item.get("window_index", -2))
        and context.get("phase") == item.get("phase")
        and context.get("fold_local_evaluator") is True
        and context.get("source_selection_contamination") is True
        and payload.get("production_effect") == "none"
        and payload.get("broker_action") == "none"
    )


def _source_checksums_fresh(
    manifest: Mapping[str, Any], *, resolver: PortableLineageResolver | None = None
) -> bool:
    artifacts = _mapping(manifest.get("source_artifacts"))
    checksums = _mapping(manifest.get("source_checksums"))
    if not artifacts:
        return False
    for name, raw_path in artifacts.items():
        checksum_name = name.removesuffix("_path") + "_sha256"
        path = _portable_path(
            Path(str(raw_path)),
            resolver,
            expected_sha256=str(checksums.get(checksum_name, "")),
        )
        if not path.is_file() or checksums.get(checksum_name) != _file_sha256(path):
            return False
    return True


def _path_checksum_fresh(
    manifest: Mapping[str, Any],
    prefix: str,
    *,
    resolver: PortableLineageResolver | None = None,
) -> bool:
    path = _portable_path(
        Path(str(manifest.get(f"{prefix}_path", ""))),
        resolver,
        expected_sha256=str(manifest.get(f"{prefix}_sha256", "")),
    )
    return path.is_file() and manifest.get(f"{prefix}_sha256") == _file_sha256(path)


def _safe_preflight_validation(
    path: Path, *, resolver: PortableLineageResolver | None = None
) -> bool:
    try:
        if resolver is None:
            validation = validate_research_restart_preflight(artifact_path=path)
        else:
            validation = validate_research_restart_preflight(
                artifact_path=path,
                portable_lineage_sidecar_path=resolver.sidecar_path,
                portable_project_root=resolver.project_root,
                portable_lineage_policy_path=resolver.policy_path,
            )
        return bool(validation["status"] == "PASS")
    except (OSError, ValueError, json.JSONDecodeError):
        return False


def _portable_path(
    path: Path,
    resolver: PortableLineageResolver | None,
    *,
    expected_sha256: str | None = None,
    expected_size: int | None = None,
) -> Path:
    if resolver is None:
        return path
    return resolver.resolve(
        path,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
    )


def _portable_r1_validation_failure(
    *,
    report_type: str,
    identity_key: str,
    identity_value: str,
    consumer: str,
    sidecar_path: Path,
    error: PortableLineageError,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        identity_key: identity_value,
        "status": "FAIL",
        "checks": [
            {
                "check_id": "portable_lineage_resolution",
                "passed": False,
                "reason_code": error.reason_code,
            }
        ],
        "failed_check_count": 1,
        "portable_lineage_resolution": portable_lineage_failure_evidence(
            error=error,
            consumer=consumer,
            sidecar_path=sidecar_path,
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _ranges_overlap(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return max(a_start, b_start) <= min(a_end, b_end)


def _compounded_return(values: Sequence[float]) -> float:
    result = 1.0
    for value in values:
        if not isfinite(value) or value <= -1:
            return float("nan")
        result *= 1.0 + value
    return result - 1.0


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _stable_id(*values: Any) -> str:
    encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{index}")
        if not candidate.exists():
            return candidate
    raise DynamicV3R1EvidenceError(f"cannot allocate unique artifact directory: {path}")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload, default=str)


def _load_json(path: Path) -> dict[str, Any]:
    payload = _load_json_optional(path)
    if not payload:
        raise DynamicV3R1EvidenceError(f"required JSON missing or invalid: {path}")
    return payload


def _load_json_optional(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _check(check_id: str, passed: bool) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed)}


def _json_equivalent(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, default=str) == json.dumps(
        right, sort_keys=True, default=str
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_mapping(item) for item in value]
