from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.dynamic_v3_clean_selection_trading2452 import (
    DEFAULT_PACKAGE_ROOT,
    DynamicV3Trading2452Error,
    validate_trading2452_package,
)
from ai_trading_system.dynamic_v3_clean_selection_trading2452 import (
    SAFETY as PACKAGE_SAFETY,
)
from ai_trading_system.etf_portfolio import dynamic_robustness
from ai_trading_system.etf_portfolio import dynamic_v3_parameter_research as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_r1_evidence as r1
from ai_trading_system.etf_portfolio import dynamic_v3_real_evaluation as real_evaluation
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.platform.artifacts.writer import (
    canonical_json_bytes,
    write_bytes_atomic,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "dynamic_v3_trading2452_historical_seen_evaluator.v1"
VALIDATION_SCHEMA_VERSION = "dynamic_v3_trading2452_historical_seen_validation.v1"
PRIMARY_WINDOW_START = date(2021, 2, 22)
DEFAULT_WORKERS = 24
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_v3_rescue" / "trading2452_historical_seen"
)

OUTPUT_FILENAMES = (
    "data_quality_gate.json",
    "effective_windows.json",
    "train_evaluations.jsonl",
    "train_selections.jsonl",
    "test_evaluations.jsonl",
    "recent_known_diagnostics.jsonl",
    "evaluator_runtime_telemetry.json",
    "historical_seen_report.json",
    "historical_seen_report.md",
)

SAFETY: dict[str, Any] = {
    **PACKAGE_SAFETY,
    "prospective_holdout_accessed": False,
    "paper_shadow_changed": False,
    "production_weights_changed": False,
    "broker_action_executed": False,
}


class DynamicV3Trading2452EvaluatorError(ValueError):
    """Raised when the owner-authorized historical-seen evaluator must stop."""


_WORKER_CONTEXT: dict[str, Any] = {}


def run_trading2452_historical_seen_evaluator(
    *,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    workers: int = DEFAULT_WORKERS,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if workers <= 0:
        raise DynamicV3Trading2452EvaluatorError("workers must be positive")
    generated = generated_at or datetime.now(UTC)
    package_validation = validate_trading2452_package(package_root=package_root)
    if package_validation.get("status") != "PASS":
        raise DynamicV3Trading2452EvaluatorError(
            "TRADING-2452 package validation must PASS before evaluator"
        )
    package = _load_package(package_root)
    package_id = str(package["package_manifest.json"].get("package_id", ""))
    selection = _load_yaml(package_root / "selection_rule.yaml")
    windows_catalog = _load_yaml(package_root / "window_catalog.yaml")
    _assert_execution_boundary(selection=selection, windows=windows_catalog)
    candidate_universe = _records(package["candidate_universe.json"].get("candidates"))
    if len(candidate_universe) != 300:
        raise DynamicV3Trading2452EvaluatorError("frozen candidate universe must contain 300 rows")

    replay = _mapping(windows_catalog.get("historical_protocol_replay"))
    recent = _mapping(windows_catalog.get("recent_known_diagnostic"))
    holdout = _mapping(windows_catalog.get("prospective_holdout"))
    dq_as_of = date.fromisoformat(str(recent.get("end")))
    run_id = (
        "trading2452-historical-seen_"
        + generated.strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + _stable_hash(package_id, generated.isoformat())[:12]
    )
    run_dir = _unique_dir(output_root / run_id)
    run_dir.mkdir(parents=True, exist_ok=False)
    quality_report_path = run_dir / "data_quality_gate.md"
    quality_report = _run_data_quality_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=dq_as_of,
        output_path=quality_report_path,
    )
    dq_payload = _data_quality_payload(
        report=quality_report,
        report_path=quality_report_path,
        prices_path=prices_path,
        rates_path=rates_path,
    )
    write_json_atomic(run_dir / "data_quality_gate.json", dq_payload)
    if not dq_payload["passed"]:
        blocked = _blocked_manifest(
            run_id=run_dir.name,
            package_id=package_id,
            package_root=package_root,
            generated=generated,
            dq_payload=dq_payload,
            run_dir=run_dir,
        )
        write_json_atomic(run_dir / "evaluator_manifest.json", blocked)
        return {
            "status": "BLOCKED_DATA_QUALITY",
            "run_id": run_dir.name,
            "run_dir": run_dir,
            "manifest": blocked,
            "production_effect": "none",
            "broker_action": "none",
        }

    source_config_path = _project_path(
        _mapping(selection.get("candidate_universe")).get("source_config")
    )
    execution_policy = _mapping(selection.get("execution"))
    walk_policy = {
        **_mapping(execution_policy.get("evidence_row_floors")),
        "purge_trading_days": execution_policy.get("purge_trading_days"),
        "embargo_trading_days": execution_policy.get("embargo_trading_days"),
    }
    runtime = r1._load_runtime_context(
        prices_path=prices_path,
        preflight={
            "data_quality_gate": {"status": quality_report.status},
            "artifact_paths": {"markdown": str(quality_report_path)},
        },
    )
    trading_dates = r1._trading_dates(runtime.prices)
    effective_windows = _effective_windows(
        replay=replay,
        trading_dates=trading_dates,
    )
    write_json_atomic(
        run_dir / "effective_windows.json",
        {
            "schema_version": SCHEMA_VERSION,
            "trading_calendar": "XNYS",
            "windows": effective_windows,
            "prospective_holdout_start": holdout.get("start"),
            "prospective_holdout_accessed": False,
        },
    )
    fixed_cache_root = run_dir / "fixed_report_cache"
    fixed_cache_root.mkdir(parents=True, exist_ok=False)
    candidate_cache_root = run_dir / "candidate_report_cache"
    candidate_cache_root.mkdir(parents=True, exist_ok=False)
    runtime_binding = _runtime_binding(
        runtime=runtime,
        prices_path=prices_path,
        rates_path=rates_path,
        quality_report_path=quality_report_path,
        source_config_path=source_config_path,
    )

    worker_args = (
        prices_path,
        {"status": quality_report.status, "report_path": str(quality_report_path)},
        source_config_path,
        walk_policy,
        generated,
        fixed_cache_root,
        candidate_cache_root,
        runtime_binding,
    )
    train_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    phase_telemetry: list[dict[str, Any]] = []
    fixed_cache_artifacts: list[dict[str, Any]] = []
    candidate_cache_artifacts: list[dict[str, Any]] = []
    top_n = int(_mapping(selection.get("selection")).get("top_n", 0))
    executor: ProcessPoolExecutor | None = None
    if workers == 1:
        _initialize_worker(*worker_args)
    else:
        executor = ProcessPoolExecutor(
            max_workers=workers,
            initializer=_initialize_worker,
            initargs=worker_args,
        )
    try:
        for window in effective_windows:
            window_index = int(window["window_index"])
            fold_train, train_timing, train_fixed_cache, train_candidate_cache = _run_phase_jobs(
                candidates=candidate_universe,
                window_index=window_index,
                phase="train",
                start=date.fromisoformat(str(window["effective_train_start"])),
                end=date.fromisoformat(str(window["effective_train_end"])),
                package_id=package_id,
                workers=workers,
                executor=executor,
                runtime=runtime,
                runtime_binding=runtime_binding,
                candidate_cache_root=candidate_cache_root,
            )
            train_rows.extend(fold_train)
            selection_started = perf_counter()
            fold_selected = select_train_only_top_n(fold_train, top_n=top_n)
            train_timing["selection_seconds"] = perf_counter() - selection_started
            phase_telemetry.append(train_timing)
            fixed_cache_artifacts.extend(train_fixed_cache)
            candidate_cache_artifacts.extend(train_candidate_cache)
            for rank, row in enumerate(fold_selected, start=1):
                selected_rows.append(
                    {
                        "window_index": window["window_index"],
                        "candidate_id": row["candidate_id"],
                        "train_rank": rank,
                        "train_selection_score": row.get("selection_score"),
                        "train_gate": row.get("gate"),
                        "parameters": row.get("parameters", {}),
                        "selection_evidence_phase": "train_only",
                        "test_metric_selection_allowed": False,
                    }
                )
            test_candidates = [
                {
                    "candidate_id": row["candidate_id"],
                    "parameters": row.get("parameters", {}),
                }
                for row in fold_selected
            ]
            fold_test, test_timing, test_fixed_cache, test_candidate_cache = _run_phase_jobs(
                candidates=test_candidates,
                window_index=window_index,
                phase="test",
                start=date.fromisoformat(str(window["effective_test_start"])),
                end=date.fromisoformat(str(window["effective_test_end"])),
                package_id=package_id,
                workers=workers,
                executor=executor,
                runtime=runtime,
                runtime_binding=runtime_binding,
                candidate_cache_root=candidate_cache_root,
            )
            test_rows.extend(fold_test)
            phase_telemetry.append(test_timing)
            fixed_cache_artifacts.extend(test_fixed_cache)
            candidate_cache_artifacts.extend(test_candidate_cache)

        selected_by_id = {
            str(row["candidate_id"]): {
                "candidate_id": row["candidate_id"],
                "parameters": row.get("parameters", {}),
            }
            for row in selected_rows
        }
        (
            recent_rows,
            recent_timing,
            recent_fixed_cache,
            recent_candidate_cache,
        ) = _run_phase_jobs(
            candidates=list(selected_by_id.values()),
            window_index=0,
            phase="recent_known_diagnostic",
            start=date.fromisoformat(str(recent.get("start"))),
            end=date.fromisoformat(str(recent.get("end"))),
            package_id=package_id,
            workers=workers,
            executor=executor,
            runtime=runtime,
            runtime_binding=runtime_binding,
            candidate_cache_root=candidate_cache_root,
        )
        phase_telemetry.append(recent_timing)
        fixed_cache_artifacts.extend(recent_fixed_cache)
        candidate_cache_artifacts.extend(recent_candidate_cache)
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)
    for row in recent_rows:
        row["included_in_main_fold_ranking"] = False
        row["candidate_selection_from_recent_metric"] = False

    telemetry = _runtime_telemetry(
        phases=phase_telemetry,
        fixed_cache_artifacts=fixed_cache_artifacts,
        candidate_cache_artifacts=candidate_cache_artifacts,
        runtime_binding=runtime_binding,
        workers=workers,
    )
    write_json_atomic(run_dir / "evaluator_runtime_telemetry.json", telemetry)

    _write_jsonl(run_dir / "train_evaluations.jsonl", train_rows)
    _write_jsonl(run_dir / "train_selections.jsonl", selected_rows)
    _write_jsonl(run_dir / "test_evaluations.jsonl", test_rows)
    _write_jsonl(run_dir / "recent_known_diagnostics.jsonl", recent_rows)
    report = _historical_seen_report(
        run_id=run_dir.name,
        package_id=package_id,
        effective_windows=effective_windows,
        train_rows=train_rows,
        selected_rows=selected_rows,
        test_rows=test_rows,
        recent_rows=recent_rows,
        dq_payload=dq_payload,
        generated=generated,
    )
    write_json_atomic(run_dir / "historical_seen_report.json", report)
    write_markdown_atomic(
        run_dir / "historical_seen_report.md",
        _render_historical_seen_report(report),
    )
    output_checksums = {filename: _file_sha256(run_dir / filename) for filename in OUTPUT_FILENAMES}
    output_checksums["data_quality_gate.md"] = _file_sha256(quality_report_path)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "dynamic_v3_trading2452_historical_seen_manifest",
        "run_id": run_dir.name,
        "status": report["status"],
        "package_id": package_id,
        "package_root": str(package_root),
        "package_manifest_sha256": _file_sha256(package_root / "package_manifest.json"),
        "source_config_path": str(source_config_path),
        "source_config_sha256": _file_sha256(source_config_path),
        "prices_path": str(prices_path),
        "prices_sha256": _file_sha256(prices_path),
        "rates_path": str(rates_path),
        "rates_sha256": _file_sha256(rates_path),
        "runtime_source_commitments": _runtime_source_commitments(),
        "runtime_binding": runtime_binding,
        "runtime_binding_hash": runtime_binding["binding_hash"],
        "fixed_report_cache_artifacts": fixed_cache_artifacts,
        "candidate_report_cache_commitments": candidate_cache_artifacts,
        "runtime_telemetry": telemetry["summary"],
        "data_quality_status": quality_report.status,
        "data_quality_as_of": quality_report.as_of.isoformat(),
        "candidate_count": len(candidate_universe),
        "historical_fold_count": len(effective_windows),
        "train_evaluation_count": len(train_rows),
        "selected_count": len(selected_rows),
        "test_evaluation_count": len(test_rows),
        "recent_diagnostic_count": len(recent_rows),
        "result_inputs_consumed": [],
        "candidate_expansion_performed": False,
        "parameter_search_performed": False,
        "prospective_holdout_start": holdout.get("start"),
        "prospective_holdout_accessed": False,
        "output_artifact_checksums": output_checksums,
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }
    write_json_atomic(run_dir / "evaluator_manifest.json", manifest)
    validation = validate_trading2452_historical_seen_artifact(
        run_id=run_dir.name,
        output_root=output_root,
        package_root=package_root,
    )
    write_json_atomic(run_dir / "evaluator_validation.json", validation)
    if validation.get("status") != "PASS":
        raise DynamicV3Trading2452EvaluatorError(
            "historical evaluator artifact validation failed: "
            f"{validation.get('failed_check_count')}"
        )
    return {
        "status": report["status"],
        "run_id": run_dir.name,
        "run_dir": run_dir,
        "manifest": manifest,
        "report": report,
        "validation": validation,
        "production_effect": "none",
        "broker_action": "none",
    }


def select_train_only_top_n(
    train_rows: Sequence[Mapping[str, Any]], *, top_n: int
) -> list[dict[str, Any]]:
    if top_n <= 0:
        raise DynamicV3Trading2452EvaluatorError("top_n must be positive")
    eligible = [
        dict(row)
        for row in train_rows
        if row.get("evidence_status") == "COMPLETE"
        and row.get("gate") != legacy.GATE_REJECT
        and row.get("selection_score") is not None
    ]
    eligible.sort(
        key=lambda row: (
            -float(row.get("selection_score", 0.0)),
            str(row.get("candidate_id", "")),
        )
    )
    return eligible[:top_n]


def validate_trading2452_historical_seen_artifact(
    *,
    run_id: str,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
) -> dict[str, Any]:
    run_dir = output_root / run_id
    checks: list[dict[str, Any]] = []
    try:
        manifest = _load_json(run_dir / "evaluator_manifest.json")
        package_validation = validate_trading2452_package(package_root=package_root)
        checks.extend(
            [
                _check("manifest_run_id", manifest.get("run_id") == run_id),
                _check("package_validation_pass", package_validation.get("status") == "PASS"),
                _check(
                    "package_id_matches",
                    manifest.get("package_id") == package_validation.get("package_id"),
                ),
                _check("manifest_safety", manifest.get("safety") == SAFETY),
                _check(
                    "no_expansion_or_search",
                    manifest.get("candidate_expansion_performed") is False
                    and manifest.get("parameter_search_performed") is False
                    and manifest.get("result_inputs_consumed") == [],
                ),
                _check(
                    "prospective_not_accessed",
                    manifest.get("prospective_holdout_start") == "2026-07-22"
                    and manifest.get("prospective_holdout_accessed") is False,
                ),
                _check(
                    "runtime_dq_pass",
                    manifest.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
                ),
                _check(
                    "runtime_source_commitments_fresh",
                    _runtime_source_commitments_fresh(
                        _mapping(manifest.get("runtime_source_commitments"))
                    ),
                ),
                _check(
                    "runtime_binding_fresh",
                    _runtime_binding_fresh(
                        binding=_mapping(manifest.get("runtime_binding")),
                        manifest=manifest,
                    ),
                ),
            ]
        )
        checksums = _mapping(manifest.get("output_artifact_checksums"))
        expected_names = {*OUTPUT_FILENAMES, "data_quality_gate.md"}
        checks.append(_check("output_inventory_complete", set(checksums) == expected_names))
        for filename in expected_names:
            path = run_dir / filename
            checks.append(
                _check(
                    f"output_checksum:{filename}",
                    path.is_file() and checksums.get(filename) == _file_sha256(path),
                )
            )
        windows = _records(_load_json(run_dir / "effective_windows.json").get("windows"))
        train_rows = _read_jsonl(run_dir / "train_evaluations.jsonl")
        selected_rows = _read_jsonl(run_dir / "train_selections.jsonl")
        test_rows = _read_jsonl(run_dir / "test_evaluations.jsonl")
        recent_rows = _read_jsonl(run_dir / "recent_known_diagnostics.jsonl")
        telemetry = _load_json(run_dir / "evaluator_runtime_telemetry.json")
        telemetry_phases = _records(telemetry.get("phases"))
        fixed_cache_artifacts = _records(manifest.get("fixed_report_cache_artifacts"))
        candidate_cache_artifacts = _records(manifest.get("candidate_report_cache_commitments"))
        expected_phase_sequence = [
            item for index in range(1, 7) for item in ((index, "train"), (index, "test"))
        ] + [(0, "recent_known_diagnostic")]
        observed_phase_sequence = [
            (int(item.get("window_index", -1)), str(item.get("phase", "")))
            for item in telemetry_phases
        ]
        checks.extend(
            [
                _check("six_frozen_folds", len(windows) == 6),
                _check("all_300_train_evaluated_per_fold", len(train_rows) == 1800),
                _check(
                    "manifest_counts_match",
                    manifest.get("train_evaluation_count") == len(train_rows)
                    and manifest.get("selected_count") == len(selected_rows)
                    and manifest.get("test_evaluation_count") == len(test_rows)
                    and manifest.get("recent_diagnostic_count") == len(recent_rows),
                ),
                _check(
                    "phase_barrier_sequence",
                    observed_phase_sequence == expected_phase_sequence,
                ),
                _check(
                    "fixed_reports_once_per_nonempty_phase",
                    len(fixed_cache_artifacts)
                    == 3 * sum(int(item.get("candidate_count", 0)) > 0 for item in telemetry_phases)
                    and all(
                        int(item.get("fixed_report_computations", -1))
                        == (3 if int(item.get("candidate_count", 0)) > 0 else 0)
                        and item.get("prospective_holdout_accessed") is False
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "runtime_telemetry_binding",
                    telemetry.get("runtime_binding_hash") == manifest.get("runtime_binding_hash")
                    and telemetry.get("prospective_holdout_accessed") is False
                    and _mapping(telemetry.get("summary"))
                    == _mapping(manifest.get("runtime_telemetry"))
                    and all(
                        int(item.get("workers", 0))
                        == int(_mapping(telemetry.get("summary")).get("workers", -1))
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "candidate_cache_accounting",
                    sum(
                        int(item.get("candidate_report_cache_hits", 0))
                        + int(item.get("candidate_report_cache_misses", 0))
                        for item in telemetry_phases
                    )
                    == sum(
                        int(item.get("candidate_count", 0))
                        * int(item.get("candidate_policy_count", 0))
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "candidate_global_unique_computations",
                    len(candidate_cache_artifacts)
                    == sum(
                        int(item.get("expected_global_unique_candidate_reports", 0))
                        for item in telemetry_phases
                    )
                    and all(
                        int(item.get("candidate_report_computations", -1))
                        == int(item.get("expected_global_unique_candidate_reports", -2))
                        == int(item.get("candidate_report_cache_misses", -3))
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "candidate_artifact_load_accounting",
                    sum(
                        int(item.get("candidate_report_artifact_loads", 0))
                        + int(item.get("candidate_report_memory_hits", 0))
                        for item in telemetry_phases
                    )
                    == sum(
                        int(item.get("candidate_count", 0))
                        * int(item.get("candidate_policy_count", 0))
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "candidate_phase_cleanup_complete",
                    all(
                        _mapping(item.get("candidate_cache_cleanup")).get("status") == "PASS"
                        and _mapping(item.get("candidate_cache_cleanup")).get(
                            "validated_artifact_count"
                        )
                        == item.get("expected_global_unique_candidate_reports")
                        and _mapping(item.get("candidate_cache_cleanup")).get(
                            "deleted_artifact_count"
                        )
                        == item.get("expected_global_unique_candidate_reports")
                        and _mapping(item.get("candidate_cache_cleanup")).get("directory_empty")
                        is True
                        for item in telemetry_phases
                    ),
                ),
                _check(
                    "fixed_cache_artifacts_valid",
                    _fixed_cache_artifacts_valid(
                        run_dir=run_dir,
                        records=fixed_cache_artifacts,
                        runtime_binding_hash=str(manifest.get("runtime_binding_hash", "")),
                    ),
                ),
                _check(
                    "candidate_cache_commitments_valid",
                    _candidate_cache_commitments_valid(
                        run_dir=run_dir,
                        records=candidate_cache_artifacts,
                        runtime_binding_hash=str(manifest.get("runtime_binding_hash", "")),
                    )
                    and _mapping(telemetry.get("summary")).get("candidate_cache_commitment_sha256")
                    == _candidate_commitment_digest(candidate_cache_artifacts),
                ),
            ]
        )
        selected_by_window = _group_by_window(selected_rows)
        train_by_window = _group_by_window(train_rows)
        test_by_window = _group_by_window(test_rows)
        selection_exact = True
        test_bound = True
        for index in range(1, 7):
            expected = select_train_only_top_n(train_by_window.get(index, []), top_n=20)
            expected_ids = [str(row.get("candidate_id")) for row in expected]
            observed_ids = [
                str(row.get("candidate_id")) for row in selected_by_window.get(index, [])
            ]
            if expected_ids != observed_ids:
                selection_exact = False
            if {str(row.get("candidate_id")) for row in test_by_window.get(index, [])} != set(
                expected_ids
            ):
                test_bound = False
        checks.extend(
            [
                _check("selection_recomputed_from_train_only", selection_exact),
                _check("test_evaluates_only_train_selected_candidates", test_bound),
                _check(
                    "recent_diagnostic_does_not_rank_or_select",
                    all(
                        row.get("included_in_main_fold_ranking") is False
                        and row.get("candidate_selection_from_recent_metric") is False
                        for row in recent_rows
                    ),
                ),
                _check(
                    "all_evaluation_ranges_precede_prospective",
                    all(
                        str(row.get("requested_range", {}).get("end", "")) < "2026-07-22"
                        for row in [*train_rows, *test_rows, *recent_rows]
                    ),
                ),
            ]
        )
    except (
        DynamicV3Trading2452Error,
        DynamicV3Trading2452EvaluatorError,
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
    ) as exc:
        checks.append(_check("artifact_validation", False, str(exc)))
    status = "PASS" if checks and all(item["passed"] for item in checks) else "FAIL"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "report_type": "dynamic_v3_trading2452_historical_seen_validation",
        "run_id": run_id,
        "status": status,
        "failed_check_count": sum(not item["passed"] for item in checks),
        "checks": checks,
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _run_phase_jobs(
    *,
    candidates: Sequence[Mapping[str, Any]],
    window_index: int,
    phase: str,
    start: date,
    end: date,
    package_id: str,
    workers: int,
    executor: ProcessPoolExecutor | None,
    runtime: r1.R1RuntimeContext,
    runtime_binding: Mapping[str, Any],
    candidate_cache_root: Path,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    if phase not in {"train", "test", "recent_known_diagnostic"}:
        raise DynamicV3Trading2452EvaluatorError(f"unsupported evaluator phase: {phase}")
    if end >= date(2026, 7, 22):
        raise DynamicV3Trading2452EvaluatorError("prospective holdout access is forbidden")
    phase_started = perf_counter()
    if not candidates:
        candidate_commitments, cleanup = _cleanup_candidate_cache_artifacts(
            cache_root=candidate_cache_root,
            records=[],
            runtime_binding_hash=str(runtime_binding.get("binding_hash", "")),
        )
        return (
            [],
            {
                "window_index": window_index,
                "phase": phase,
                "workers": workers,
                "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
                "candidate_count": 0,
                "candidate_policy_count": 0,
                "fixed_report_computations": 0,
                "fixed_report_cache_misses": 0,
                "fixed_report_cache_hits": 0,
                "fixed_report_artifact_validations": 0,
                "expected_global_unique_candidate_reports": 0,
                "candidate_report_computations": 0,
                "candidate_report_cache_hits": 0,
                "candidate_report_cache_misses": 0,
                "candidate_report_cache_hit_rate": 0.0,
                "candidate_report_artifact_loads": 0,
                "candidate_report_memory_hits": 0,
                "candidate_cache_cleanup": cleanup,
                "affinity_anchor_label": None,
                "affinity_batch_sizes": [],
                "fixed_precompute_seconds": 0.0,
                "candidate_precompute_seconds": 0.0,
                "candidate_cache_cleanup_seconds": 0.0,
                "candidate_evaluation_seconds": 0.0,
                "phase_seconds": perf_counter() - phase_started,
                "prospective_holdout_accessed": False,
                "production_effect": "none",
            },
            [],
            candidate_commitments,
        )
    fixed_plan = _fixed_policy_hashes(runtime)
    fixed_jobs = [
        (
            phase,
            window_index,
            start,
            end,
            str(runtime_binding.get("binding_hash", "")),
            label,
            policy_hash,
        )
        for label, policy_hash in fixed_plan.items()
    ]
    fixed_started = perf_counter()
    if executor is None:
        fixed_records = [_precompute_fixed_policy_process_job(job) for job in fixed_jobs]
    else:
        fixed_records = list(
            executor.map(_precompute_fixed_policy_process_job, fixed_jobs, chunksize=1)
        )
    fixed_seconds = perf_counter() - fixed_started
    plans = [
        (
            input_index,
            dict(candidate),
            _candidate_policy_hashes(runtime=runtime, candidate=candidate),
        )
        for input_index, candidate in enumerate(candidates)
    ]
    candidate_precompute_plan = _global_candidate_precompute_plan(plans)
    candidate_jobs = [
        (
            representative,
            label,
            policy_hash,
            phase,
            window_index,
            start,
            end,
            str(runtime_binding.get("binding_hash", "")),
        )
        for label, policy_hash, representative in candidate_precompute_plan
    ]
    candidate_precompute_started = perf_counter()
    if executor is None:
        candidate_records = [
            _precompute_candidate_policy_process_job(job) for job in candidate_jobs
        ]
    else:
        candidate_records = list(
            executor.map(
                _precompute_candidate_policy_process_job,
                candidate_jobs,
                chunksize=1,
            )
        )
    candidate_precompute_seconds = perf_counter() - candidate_precompute_started
    candidate_record_index = {
        (str(record["candidate_id"]), str(record["dynamic_allocation_policy_hash"])): record
        for record in candidate_records
    }
    expected_candidate_keys = {
        (label, policy_hash) for label, policy_hash, _ in candidate_precompute_plan
    }
    if set(candidate_record_index) != expected_candidate_keys:
        raise DynamicV3Trading2452EvaluatorError(
            "candidate cache precompute did not produce the exact global inventory"
        )
    batches, affinity = _balanced_affinity_batches(plans=plans, workers=workers)
    batch_jobs = [
        (
            batch,
            window_index,
            phase,
            start,
            end,
            package_id,
            fixed_records,
            _candidate_records_for_batch(batch, candidate_record_index),
            str(runtime_binding.get("binding_hash", "")),
        )
        for batch in batches
    ]
    evaluation_started = perf_counter()
    if executor is None:
        batch_results = [_evaluate_affinity_batch(job) for job in batch_jobs]
    else:
        batch_results = list(executor.map(_evaluate_affinity_batch, batch_jobs, chunksize=1))
    evaluation_seconds = perf_counter() - evaluation_started
    indexed_rows = [item for batch in batch_results for item in batch["rows"]]
    indexed_rows.sort(key=lambda item: int(item["input_index"]))
    rows = [dict(item["row"]) for item in indexed_rows]
    batch_telemetry = [_mapping(batch.get("telemetry")) for batch in batch_results]
    candidate_artifact_loads = sum(
        int(item.get("candidate_report_artifact_loads", 0)) for item in batch_telemetry
    )
    candidate_memory_hits = sum(
        int(item.get("candidate_report_memory_hits", 0)) for item in batch_telemetry
    )
    candidate_requests = sum(len(plan[2]) for plan in plans)
    observed_indices = [int(item["input_index"]) for item in indexed_rows]
    if observed_indices != list(range(len(plans))):
        raise DynamicV3Trading2452EvaluatorError(
            "candidate cache cleanup blocked by incomplete candidate evaluation"
        )
    if candidate_artifact_loads + candidate_memory_hits != candidate_requests:
        raise DynamicV3Trading2452EvaluatorError(
            "candidate cache cleanup blocked by incomplete report consumption"
        )
    cleanup_started = perf_counter()
    candidate_commitments, cleanup = _cleanup_candidate_cache_artifacts(
        cache_root=candidate_cache_root,
        records=candidate_records,
        runtime_binding_hash=str(runtime_binding.get("binding_hash", "")),
    )
    cleanup_seconds = perf_counter() - cleanup_started
    candidate_computations = len(candidate_records)
    candidate_hits = candidate_requests - candidate_computations
    telemetry = {
        "window_index": window_index,
        "phase": phase,
        "workers": workers,
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "candidate_count": len(candidates),
        "candidate_policy_count": len(plans[0][2]) if plans else 0,
        "fixed_report_computations": len(fixed_records),
        "fixed_report_cache_misses": len(fixed_records),
        "fixed_report_cache_hits": len(fixed_records) * len(candidates),
        "fixed_report_artifact_validations": len(fixed_records) * len(batches),
        "expected_global_unique_candidate_reports": len(candidate_precompute_plan),
        "candidate_report_computations": candidate_computations,
        "candidate_report_cache_hits": candidate_hits,
        "candidate_report_cache_misses": candidate_computations,
        "candidate_report_cache_hit_rate": (
            candidate_hits / candidate_requests if candidate_requests else 0.0
        ),
        "candidate_report_artifact_loads": candidate_artifact_loads,
        "candidate_report_memory_hits": candidate_memory_hits,
        "candidate_cache_cleanup": cleanup,
        "affinity_anchor_label": affinity.get("anchor_label"),
        "affinity_batch_sizes": affinity.get("batch_sizes", []),
        "fixed_precompute_seconds": fixed_seconds,
        "candidate_precompute_seconds": candidate_precompute_seconds,
        "candidate_cache_cleanup_seconds": cleanup_seconds,
        "candidate_evaluation_seconds": evaluation_seconds,
        "phase_seconds": perf_counter() - phase_started,
        "prospective_holdout_accessed": False,
        "production_effect": "none",
    }
    return rows, telemetry, fixed_records, candidate_commitments


def _initialize_worker(
    prices_path: Path,
    data_quality: Mapping[str, Any],
    source_config_path: Path,
    walk_policy: Mapping[str, Any],
    generated: datetime,
    fixed_cache_root: Path,
    candidate_cache_root: Path,
    runtime_binding: Mapping[str, Any],
) -> None:
    global _WORKER_CONTEXT
    if not _runtime_binding_payload_valid(runtime_binding):
        raise DynamicV3Trading2452EvaluatorError("invalid evaluator runtime binding")
    runtime = r1._load_runtime_context(
        prices_path=prices_path,
        preflight={
            "data_quality_gate": {"status": data_quality.get("status")},
            "artifact_paths": {"markdown": data_quality.get("report_path")},
        },
    )
    if not _worker_runtime_matches_binding(
        runtime=runtime,
        prices_path=prices_path,
        source_config_path=source_config_path,
        runtime_binding=runtime_binding,
    ):
        raise DynamicV3Trading2452EvaluatorError("worker runtime does not match binding")
    _WORKER_CONTEXT = {
        "runtime": runtime,
        "config": legacy.load_parameter_sweep_config(source_config_path),
        "walk_policy": dict(walk_policy),
        "trading_dates": [],
        "generated": generated,
        "fixed_cache_root": fixed_cache_root.resolve(strict=True),
        "candidate_cache_root": candidate_cache_root.resolve(strict=True),
        "runtime_binding": dict(runtime_binding),
    }
    _WORKER_CONTEXT["trading_dates"] = r1._trading_dates(_WORKER_CONTEXT["runtime"].prices)


def _precompute_fixed_policy_process_job(job: tuple[Any, ...]) -> dict[str, Any]:
    if not _WORKER_CONTEXT:
        raise DynamicV3Trading2452EvaluatorError("evaluator worker was not initialized")
    phase, window_index, start, end, binding_hash, label, expected_policy_hash = job
    runtime = _WORKER_CONTEXT["runtime"]
    if binding_hash != _WORKER_CONTEXT["runtime_binding"].get("binding_hash"):
        raise DynamicV3Trading2452EvaluatorError("fixed report runtime binding mismatch")
    policy = _fixed_policies(runtime).get(str(label))
    if policy is None or _policy_hash(policy) != expected_policy_hash:
        raise DynamicV3Trading2452EvaluatorError("fixed report policy binding mismatch")
    started = perf_counter()
    report = dynamic_robustness.build_dynamic_robustness_report(
        prices=runtime.prices,
        etf_config=runtime.etf_config,
        policy=runtime.dynamic_robustness_policy,
        dynamic_policy=policy,
        candidate_id=str(label),
        start=start,
        end=end,
        data_quality_status=runtime.data_quality_status,
        data_quality_report=runtime.data_quality_report_path,
        prices_path=runtime.prices_path,
    )
    validated = real_evaluation._validated_precomputed_robustness_report(
        label=str(label),
        report=report,
        allocation_policy=policy,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
        requested_start=start,
        requested_end=end,
        data_quality_status=runtime.data_quality_status,
    )
    report_sha256 = sha256(canonical_json_bytes(validated)).hexdigest()
    cache_root = Path(_WORKER_CONTEXT["fixed_cache_root"])
    filename = (
        f"w{int(window_index):02d}_{phase}_{start.isoformat()}_{end.isoformat()}_"
        f"{_stable_hash(label)[:12]}.json"
    )
    path = cache_root / filename
    if path.exists():
        raise DynamicV3Trading2452EvaluatorError(f"fixed cache artifact already exists: {path}")
    envelope = {
        "schema_version": "dynamic_v3_trading2452_fixed_report_cache.v1",
        "phase": phase,
        "window_index": int(window_index),
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "runtime_binding_hash": binding_hash,
        "candidate_id": label,
        "dynamic_allocation_policy_hash": expected_policy_hash,
        "report_sha256": report_sha256,
        "report": validated,
        "prospective_holdout_accessed": False,
        "production_effect": "none",
    }
    write_json_atomic(path, envelope)
    return {
        "relative_path": filename,
        "file_sha256": _file_sha256(path),
        "report_sha256": report_sha256,
        "phase": phase,
        "window_index": int(window_index),
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "runtime_binding_hash": binding_hash,
        "candidate_id": label,
        "dynamic_allocation_policy_hash": expected_policy_hash,
        "compute_seconds": perf_counter() - started,
    }


def _precompute_candidate_policy_process_job(job: tuple[Any, ...]) -> dict[str, Any]:
    if not _WORKER_CONTEXT:
        raise DynamicV3Trading2452EvaluatorError("evaluator worker was not initialized")
    (
        representative,
        label,
        expected_policy_hash,
        phase,
        window_index,
        start,
        end,
        binding_hash,
    ) = job
    if binding_hash != _WORKER_CONTEXT["runtime_binding"].get("binding_hash"):
        raise DynamicV3Trading2452EvaluatorError("candidate report runtime binding mismatch")
    runtime = _WORKER_CONTEXT["runtime"]
    policy = _candidate_materialized_policies(
        runtime=runtime,
        candidate=representative,
    ).get(str(label))
    if policy is None or _policy_hash(policy) != expected_policy_hash:
        raise DynamicV3Trading2452EvaluatorError("candidate report policy binding mismatch")
    started = perf_counter()
    report = dynamic_robustness.build_dynamic_robustness_report(
        prices=runtime.prices,
        etf_config=runtime.etf_config,
        policy=runtime.dynamic_robustness_policy,
        dynamic_policy=policy,
        candidate_id=str(label),
        start=start,
        end=end,
        data_quality_status=runtime.data_quality_status,
        data_quality_report=runtime.data_quality_report_path,
        prices_path=runtime.prices_path,
    )
    validated = real_evaluation._validated_precomputed_robustness_report(
        label=str(label),
        report=report,
        allocation_policy=policy,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
        requested_start=start,
        requested_end=end,
        data_quality_status=runtime.data_quality_status,
    )
    report_sha256 = sha256(canonical_json_bytes(validated)).hexdigest()
    cache_root = Path(_WORKER_CONTEXT["candidate_cache_root"])
    filename = _candidate_cache_filename(
        label=str(label),
        policy_hash=expected_policy_hash,
        phase=str(phase),
        window_index=int(window_index),
        start=start.isoformat(),
        end=end.isoformat(),
        binding_hash=str(binding_hash),
    )
    path = cache_root / filename
    if path.exists():
        raise DynamicV3Trading2452EvaluatorError(f"candidate cache artifact already exists: {path}")
    envelope = {
        "schema_version": "dynamic_v3_trading2452_candidate_report_cache.v1",
        "cache_kind": "candidate_robustness_report",
        "phase": phase,
        "window_index": int(window_index),
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "runtime_binding_hash": binding_hash,
        "candidate_id": label,
        "dynamic_allocation_policy_hash": expected_policy_hash,
        "report_sha256": report_sha256,
        "report": validated,
        "prospective_holdout_accessed": False,
        "production_effect": "none",
    }
    write_json_atomic(path, envelope)
    return {
        "cache_kind": "candidate_robustness_report",
        "transient": True,
        "content_address": filename,
        "relative_path": filename,
        "file_sha256": _file_sha256(path),
        "report_sha256": report_sha256,
        "phase": phase,
        "window_index": int(window_index),
        "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
        "runtime_binding_hash": binding_hash,
        "candidate_id": label,
        "dynamic_allocation_policy_hash": expected_policy_hash,
        "compute_seconds": perf_counter() - started,
    }


def _evaluate_affinity_batch(job: tuple[Any, ...]) -> dict[str, Any]:
    if not _WORKER_CONTEXT:
        raise DynamicV3Trading2452EvaluatorError("evaluator worker was not initialized")
    (
        batch,
        window_index,
        phase,
        start,
        end,
        package_id,
        fixed_records,
        candidate_records,
        binding_hash,
    ) = job
    if binding_hash != _WORKER_CONTEXT["runtime_binding"].get("binding_hash"):
        raise DynamicV3Trading2452EvaluatorError("candidate batch runtime binding mismatch")
    runtime = _WORKER_CONTEXT["runtime"]
    fixed_reports = _load_fixed_reports(
        records=fixed_records,
        runtime=runtime,
        phase=phase,
        window_index=int(window_index),
        start=start,
        end=end,
        binding_hash=binding_hash,
    )
    candidate_record_index = {
        (str(record["candidate_id"]), str(record["dynamic_allocation_policy_hash"])): record
        for record in candidate_records
    }
    expected_batch_keys = {
        (label, policy_hash)
        for _, _, policy_hashes in batch
        for label, policy_hash in policy_hashes.items()
    }
    if set(candidate_record_index) != expected_batch_keys:
        raise DynamicV3Trading2452EvaluatorError("candidate batch cache inventory mismatch")
    candidate_cache: dict[tuple[str, str], dict[str, Any]] = {}
    artifact_loads = 0
    memory_hits = 0
    rows: list[dict[str, Any]] = []
    started = perf_counter()
    for input_index, candidate, expected_hashes in batch:
        materialized = _candidate_materialized_policies(runtime=runtime, candidate=candidate)
        candidate_reports: dict[str, dict[str, Any]] = {}
        if set(materialized) != set(expected_hashes):
            raise DynamicV3Trading2452EvaluatorError("candidate policy label binding mismatch")
        for label, policy in materialized.items():
            policy_hash = _policy_hash(policy)
            if policy_hash != expected_hashes.get(label):
                raise DynamicV3Trading2452EvaluatorError("candidate policy hash binding mismatch")
            cache_key = (label, policy_hash)
            if cache_key in candidate_cache:
                candidate_reports[label] = candidate_cache[cache_key]
                memory_hits += 1
                continue
            record = candidate_record_index.get(cache_key)
            if record is None:
                raise DynamicV3Trading2452EvaluatorError("candidate cache artifact is missing")
            validated = _load_candidate_report(
                record=record,
                runtime=runtime,
                policy=policy,
                label=label,
                phase=phase,
                window_index=int(window_index),
                start=start,
                end=end,
                binding_hash=binding_hash,
            )
            candidate_cache[cache_key] = validated
            candidate_reports[label] = validated
            artifact_loads += 1
        payload = r1._evaluate_candidate_payload(
            result=candidate,
            start=start,
            end=end,
            runtime=runtime,
            fixed_reports={**fixed_reports, **candidate_reports},
            generated=_WORKER_CONTEXT["generated"],
        )
        summary_phase = "test" if phase == "recent_known_diagnostic" else phase
        summary = r1._summarize_fold_payload(
            payload=payload,
            result=candidate,
            config=_WORKER_CONTEXT["config"],
            policy=_WORKER_CONTEXT["walk_policy"],
            trading_dates=_WORKER_CONTEXT["trading_dates"],
            phase=summary_phase,
            window_index=int(window_index),
        )
        summary.update(
            {
                "phase": phase,
                "parameters": _mapping(candidate.get("parameters")),
                "requested_range": {"start": start.isoformat(), "end": end.isoformat()},
                "package_id": package_id,
                "fold_local_evaluator": True,
                "preregistered_candidate_universe": True,
                "source_selection_contamination": False,
                "prior_market_outcome_visibility": "KNOWN",
                "historical_replay_investigator_blind": False,
                "unbiased_oos_claim_allowed": False,
                "prospective_holdout_accessed": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
        rows.append({"input_index": int(input_index), "row": summary})
    return {
        "rows": rows,
        "telemetry": {
            "candidate_count": len(batch),
            "candidate_report_artifact_loads": artifact_loads,
            "candidate_report_memory_hits": memory_hits,
            "evaluation_seconds": perf_counter() - started,
        },
    }


def _fixed_policies(runtime: r1.R1RuntimeContext) -> dict[str, Any]:
    return real_evaluation._fixed_dynamic_v3_comparison_policies(
        dynamic_policy=runtime.dynamic_policy,
        failure_policy=runtime.failure_policy,
        real_policy=runtime.real_policy,
    )


def _fixed_policy_hashes(runtime: r1.R1RuntimeContext) -> dict[str, str]:
    return {label: _policy_hash(policy) for label, policy in _fixed_policies(runtime).items()}


def _candidate_materialized_policies(
    *, runtime: r1.R1RuntimeContext, candidate: Mapping[str, Any]
) -> dict[str, Any]:
    parameters = _mapping(candidate.get("parameters"))
    candidate_real_policy = legacy._real_policy_for_sweep_candidate(runtime.real_policy, parameters)
    candidate_v3_policy = legacy._real_rescue_policy_for_sweep_candidate(
        runtime.v3_rescue_policy, parameters
    )
    materialized = real_evaluation._materialized_policy_set(
        dynamic_policy=runtime.dynamic_policy,
        failure_policy=runtime.failure_policy,
        v3_rescue_policy=candidate_v3_policy,
        real_policy=candidate_real_policy,
    )
    policies = _mapping(materialized.get("policies"))
    fixed_labels = set(_fixed_policies(runtime))
    candidate_policies = {
        str(label): policy for label, policy in policies.items() if label not in fixed_labels
    }
    if not candidate_policies:
        raise DynamicV3Trading2452EvaluatorError("candidate policy materialization is empty")
    return candidate_policies


def _candidate_policy_hashes(
    *, runtime: r1.R1RuntimeContext, candidate: Mapping[str, Any]
) -> dict[str, str]:
    return {
        label: _policy_hash(policy)
        for label, policy in _candidate_materialized_policies(
            runtime=runtime, candidate=candidate
        ).items()
    }


def _global_candidate_precompute_plan(
    plans: Sequence[tuple[int, dict[str, Any], dict[str, str]]],
) -> list[tuple[str, str, dict[str, Any]]]:
    representatives: dict[tuple[str, str], dict[str, Any]] = {}
    for _, candidate, policy_hashes in plans:
        for label, policy_hash in policy_hashes.items():
            representatives.setdefault((label, policy_hash), candidate)
    return [
        (label, policy_hash, representatives[(label, policy_hash)])
        for label, policy_hash in sorted(representatives)
    ]


def _candidate_cache_filename(
    *,
    label: str,
    policy_hash: str,
    phase: str,
    window_index: int,
    start: str,
    end: str,
    binding_hash: str,
) -> str:
    content_key = {
        "label": label,
        "policy_hash": policy_hash,
        "phase": phase,
        "window_index": window_index,
        "start": start,
        "end": end,
        "runtime_binding_hash": binding_hash,
    }
    return f"candidate_{_stable_hash(content_key)}.json"


def _candidate_records_for_batch(
    batch: Sequence[tuple[int, dict[str, Any], dict[str, str]]],
    record_index: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    required_keys = sorted(
        {
            (label, policy_hash)
            for _, _, policy_hashes in batch
            for label, policy_hash in policy_hashes.items()
        }
    )
    missing = [key for key in required_keys if key not in record_index]
    if missing:
        raise DynamicV3Trading2452EvaluatorError(
            f"candidate cache precompute inventory is incomplete: {len(missing)}"
        )
    return [dict(record_index[key]) for key in required_keys]


def _balanced_affinity_batches(
    *,
    plans: Sequence[tuple[int, dict[str, Any], dict[str, str]]],
    workers: int,
) -> tuple[
    list[list[tuple[int, dict[str, Any], dict[str, str]]]],
    dict[str, Any],
]:
    if workers <= 0:
        raise DynamicV3Trading2452EvaluatorError("workers must be positive")
    if not plans:
        return [], {"anchor_label": None, "batch_sizes": []}
    labels = set(plans[0][2])
    if not labels or any(set(plan[2]) != labels for plan in plans):
        raise DynamicV3Trading2452EvaluatorError("candidate affinity policy labels mismatch")
    anchor_label = sorted(
        labels,
        key=lambda label: (
            -len({plan[2][label] for plan in plans}),
            label,
        ),
    )[0]
    groups: dict[str, list[tuple[int, dict[str, Any], dict[str, str]]]] = {}
    for plan in plans:
        groups.setdefault(plan[2][anchor_label], []).append(plan)
    batch_count = min(workers, len(groups))
    batches: list[list[tuple[int, dict[str, Any], dict[str, str]]]] = [
        [] for _ in range(batch_count)
    ]
    for _, group in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        batch_index = min(range(batch_count), key=lambda index: (len(batches[index]), index))
        batches[batch_index].extend(group)
    for batch in batches:
        batch.sort(key=lambda item: item[0])
    return batches, {
        "anchor_label": anchor_label,
        "batch_sizes": [len(batch) for batch in batches],
    }


def _load_fixed_reports(
    *,
    records: Sequence[Mapping[str, Any]],
    runtime: r1.R1RuntimeContext,
    phase: str,
    window_index: int,
    start: date,
    end: date,
    binding_hash: str,
) -> dict[str, dict[str, Any]]:
    expected_policies = _fixed_policies(runtime)
    if {str(record.get("candidate_id")) for record in records} != set(expected_policies):
        raise DynamicV3Trading2452EvaluatorError("fixed cache inventory mismatch")
    cache_root = Path(_WORKER_CONTEXT["fixed_cache_root"]).resolve(strict=True)
    reports: dict[str, dict[str, Any]] = {}
    for raw_record in records:
        record = dict(raw_record)
        relative_path = Path(str(record.get("relative_path", "")))
        if relative_path.is_absolute() or len(relative_path.parts) != 1:
            raise DynamicV3Trading2452EvaluatorError("unsafe fixed cache relative path")
        unresolved_path = cache_root / relative_path
        if unresolved_path.is_symlink():
            raise DynamicV3Trading2452EvaluatorError("fixed cache symlinks are forbidden")
        path = unresolved_path.resolve(strict=True)
        if cache_root not in path.parents:
            raise DynamicV3Trading2452EvaluatorError("fixed cache path escapes cache root")
        if record.get("file_sha256") != _file_sha256(path):
            raise DynamicV3Trading2452EvaluatorError("fixed cache file checksum mismatch")
        envelope = _load_json(path)
        label = str(record.get("candidate_id", ""))
        policy = expected_policies.get(label)
        expected_policy_hash = _policy_hash(policy) if policy is not None else ""
        expected_range = {"start": start.isoformat(), "end": end.isoformat()}
        exact_bindings = (
            envelope.get("phase") == phase
            and record.get("phase") == phase
            and int(envelope.get("window_index", -1)) == window_index
            and int(record.get("window_index", -1)) == window_index
            and envelope.get("requested_range") == expected_range
            and record.get("requested_range") == expected_range
            and envelope.get("runtime_binding_hash") == binding_hash
            and record.get("runtime_binding_hash") == binding_hash
            and envelope.get("candidate_id") == label
            and envelope.get("dynamic_allocation_policy_hash") == expected_policy_hash
            and record.get("dynamic_allocation_policy_hash") == expected_policy_hash
            and envelope.get("prospective_holdout_accessed") is False
        )
        if not exact_bindings:
            raise DynamicV3Trading2452EvaluatorError("fixed cache binding mismatch")
        report = _mapping(envelope.get("report"))
        report_sha256 = sha256(canonical_json_bytes(report)).hexdigest()
        if (
            envelope.get("report_sha256") != report_sha256
            or record.get("report_sha256") != report_sha256
        ):
            raise DynamicV3Trading2452EvaluatorError("fixed cache report checksum mismatch")
        reports[label] = real_evaluation._validated_precomputed_robustness_report(
            label=label,
            report=report,
            allocation_policy=policy,
            dynamic_robustness_policy=runtime.dynamic_robustness_policy,
            requested_start=start,
            requested_end=end,
            data_quality_status=runtime.data_quality_status,
        )
    return reports


def _load_candidate_report(
    *,
    record: Mapping[str, Any],
    runtime: r1.R1RuntimeContext,
    policy: Any,
    label: str,
    phase: str,
    window_index: int,
    start: date,
    end: date,
    binding_hash: str,
) -> dict[str, Any]:
    relative_path = Path(str(record.get("relative_path", "")))
    if relative_path.is_absolute() or len(relative_path.parts) != 1:
        raise DynamicV3Trading2452EvaluatorError("unsafe candidate cache relative path")
    expected_filename = _candidate_cache_filename(
        label=label,
        policy_hash=_policy_hash(policy),
        phase=phase,
        window_index=window_index,
        start=start.isoformat(),
        end=end.isoformat(),
        binding_hash=binding_hash,
    )
    if relative_path.name != expected_filename:
        raise DynamicV3Trading2452EvaluatorError("candidate cache content address mismatch")
    cache_root = Path(_WORKER_CONTEXT["candidate_cache_root"]).resolve(strict=True)
    unresolved_path = cache_root / relative_path
    if unresolved_path.is_symlink():
        raise DynamicV3Trading2452EvaluatorError("candidate cache symlinks are forbidden")
    path = unresolved_path.resolve(strict=True)
    if cache_root not in path.parents:
        raise DynamicV3Trading2452EvaluatorError("candidate cache path escapes cache root")
    if record.get("file_sha256") != _file_sha256(path):
        raise DynamicV3Trading2452EvaluatorError("candidate cache file checksum mismatch")
    envelope = _load_json(path)
    policy_hash = _policy_hash(policy)
    expected_range = {"start": start.isoformat(), "end": end.isoformat()}
    exact_bindings = (
        envelope.get("cache_kind") == "candidate_robustness_report"
        and envelope.get("phase") == phase
        and record.get("phase") == phase
        and int(envelope.get("window_index", -1)) == window_index
        and int(record.get("window_index", -1)) == window_index
        and envelope.get("requested_range") == expected_range
        and record.get("requested_range") == expected_range
        and envelope.get("runtime_binding_hash") == binding_hash
        and record.get("runtime_binding_hash") == binding_hash
        and envelope.get("candidate_id") == label
        and record.get("candidate_id") == label
        and envelope.get("dynamic_allocation_policy_hash") == policy_hash
        and record.get("dynamic_allocation_policy_hash") == policy_hash
        and envelope.get("prospective_holdout_accessed") is False
    )
    if not exact_bindings:
        raise DynamicV3Trading2452EvaluatorError("candidate cache binding mismatch")
    report = _mapping(envelope.get("report"))
    report_sha256 = sha256(canonical_json_bytes(report)).hexdigest()
    if (
        envelope.get("report_sha256") != report_sha256
        or record.get("report_sha256") != report_sha256
    ):
        raise DynamicV3Trading2452EvaluatorError("candidate cache report checksum mismatch")
    return real_evaluation._validated_precomputed_robustness_report(
        label=label,
        report=report,
        allocation_policy=policy,
        dynamic_robustness_policy=runtime.dynamic_robustness_policy,
        requested_start=start,
        requested_end=end,
        data_quality_status=runtime.data_quality_status,
    )


def _runtime_telemetry(
    *,
    phases: Sequence[Mapping[str, Any]],
    fixed_cache_artifacts: Sequence[Mapping[str, Any]],
    candidate_cache_artifacts: Sequence[Mapping[str, Any]],
    runtime_binding: Mapping[str, Any],
    workers: int,
) -> dict[str, Any]:
    phase_rows = [dict(item) for item in phases]
    summary = {
        "phase_count": len(phase_rows),
        "fixed_report_computations": sum(
            int(item.get("fixed_report_computations", 0)) for item in phase_rows
        ),
        "fixed_report_cache_misses": sum(
            int(item.get("fixed_report_cache_misses", 0)) for item in phase_rows
        ),
        "fixed_report_cache_hits": sum(
            int(item.get("fixed_report_cache_hits", 0)) for item in phase_rows
        ),
        "fixed_report_artifact_validations": sum(
            int(item.get("fixed_report_artifact_validations", 0)) for item in phase_rows
        ),
        "candidate_report_cache_hits": sum(
            int(item.get("candidate_report_cache_hits", 0)) for item in phase_rows
        ),
        "candidate_report_cache_misses": sum(
            int(item.get("candidate_report_cache_misses", 0)) for item in phase_rows
        ),
        "candidate_report_computations": sum(
            int(item.get("candidate_report_computations", 0)) for item in phase_rows
        ),
        "candidate_report_artifact_loads": sum(
            int(item.get("candidate_report_artifact_loads", 0)) for item in phase_rows
        ),
        "candidate_report_memory_hits": sum(
            int(item.get("candidate_report_memory_hits", 0)) for item in phase_rows
        ),
        "expected_global_unique_candidate_reports": sum(
            int(item.get("expected_global_unique_candidate_reports", 0)) for item in phase_rows
        ),
        "fixed_cache_artifact_count": len(fixed_cache_artifacts),
        "candidate_cache_commitment_count": len(candidate_cache_artifacts),
        "candidate_cache_commitment_sha256": _candidate_commitment_digest(
            candidate_cache_artifacts
        ),
        "candidate_cache_cleanup_deleted_count": sum(
            int(_mapping(item.get("candidate_cache_cleanup")).get("deleted_artifact_count", 0))
            for item in phase_rows
        ),
        "candidate_cache_cleanup_released_bytes": sum(
            int(_mapping(item.get("candidate_cache_cleanup")).get("released_bytes", 0))
            for item in phase_rows
        ),
        "candidate_cache_directory_empty": all(
            _mapping(item.get("candidate_cache_cleanup")).get("directory_empty") is True
            for item in phase_rows
        ),
        "workers": workers,
    }
    candidate_total = (
        summary["candidate_report_cache_hits"] + summary["candidate_report_cache_misses"]
    )
    summary["candidate_report_cache_hit_rate"] = (
        summary["candidate_report_cache_hits"] / candidate_total if candidate_total else 0.0
    )
    return {
        "schema_version": "dynamic_v3_trading2452_evaluator_runtime_telemetry.v1",
        "runtime_binding_hash": runtime_binding.get("binding_hash"),
        "summary": summary,
        "phases": phase_rows,
        "prospective_holdout_accessed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_binding(
    *,
    runtime: r1.R1RuntimeContext,
    prices_path: Path,
    rates_path: Path,
    quality_report_path: Path,
    source_config_path: Path,
) -> dict[str, Any]:
    commitments = _runtime_source_commitments()
    payload = {
        "prices": {"path": str(prices_path.resolve()), "sha256": _file_sha256(prices_path)},
        "rates": {"path": str(rates_path.resolve()), "sha256": _file_sha256(rates_path)},
        "source_config": {
            "path": str(source_config_path.resolve()),
            "sha256": _file_sha256(source_config_path),
        },
        "data_quality": {
            "status": runtime.data_quality_status,
            "report_path": str(quality_report_path.resolve()),
            "report_sha256": _file_sha256(quality_report_path),
        },
        "configuration_hashes": {
            "etf_config": _config_hash(runtime.etf_config),
            "real_policy": _config_hash(runtime.real_policy),
            "v3_rescue_policy": _config_hash(runtime.v3_rescue_policy),
            "dynamic_robustness_policy": _config_hash(runtime.dynamic_robustness_policy),
            "dynamic_policy": _config_hash(runtime.dynamic_policy),
            "failure_policy": _config_hash(runtime.failure_policy),
        },
        "source_hashes": {name: item["sha256"] for name, item in commitments.items()},
    }
    return {**payload, "binding_hash": _stable_hash(payload)}


def _runtime_binding_payload_valid(binding: Mapping[str, Any]) -> bool:
    payload = dict(binding)
    binding_hash = payload.pop("binding_hash", None)
    return bool(binding_hash) and binding_hash == _stable_hash(payload)


def _runtime_binding_fresh(*, binding: Mapping[str, Any], manifest: Mapping[str, Any]) -> bool:
    if not _runtime_binding_payload_valid(binding):
        return False
    prices = _mapping(binding.get("prices"))
    rates = _mapping(binding.get("rates"))
    source_config = _mapping(binding.get("source_config"))
    data_quality = _mapping(binding.get("data_quality"))
    data_quality_path = Path(str(data_quality.get("report_path", "")))
    source_hashes = _mapping(binding.get("source_hashes"))
    config_hashes = _mapping(binding.get("configuration_hashes"))
    commitments = _mapping(manifest.get("runtime_source_commitments"))
    expected_source_hashes = {
        name: _mapping(item).get("sha256") for name, item in commitments.items()
    }
    expected_config_names = {
        "etf_config",
        "real_policy",
        "v3_rescue_policy",
        "dynamic_robustness_policy",
        "dynamic_policy",
        "failure_policy",
    }
    return (
        binding.get("binding_hash") == manifest.get("runtime_binding_hash")
        and prices.get("path") == str(Path(str(manifest.get("prices_path", ""))).resolve())
        and prices.get("sha256") == manifest.get("prices_sha256")
        and rates.get("path") == str(Path(str(manifest.get("rates_path", ""))).resolve())
        and rates.get("sha256") == manifest.get("rates_sha256")
        and source_config.get("path")
        == str(Path(str(manifest.get("source_config_path", ""))).resolve())
        and source_config.get("sha256") == manifest.get("source_config_sha256")
        and data_quality.get("status") == manifest.get("data_quality_status")
        and data_quality_path.is_file()
        and data_quality.get("report_sha256") == _file_sha256(data_quality_path)
        and data_quality.get("report_sha256")
        == _mapping(manifest.get("output_artifact_checksums")).get("data_quality_gate.md")
        and source_hashes == expected_source_hashes
        and set(config_hashes) == expected_config_names
        and all(isinstance(value, str) and len(value) == 64 for value in config_hashes.values())
    )


def _worker_runtime_matches_binding(
    *,
    runtime: r1.R1RuntimeContext,
    prices_path: Path,
    source_config_path: Path,
    runtime_binding: Mapping[str, Any],
) -> bool:
    prices = _mapping(runtime_binding.get("prices"))
    source_config = _mapping(runtime_binding.get("source_config"))
    config_hashes = _mapping(runtime_binding.get("configuration_hashes"))
    worker_config_hashes = {
        "etf_config": _config_hash(runtime.etf_config),
        "real_policy": _config_hash(runtime.real_policy),
        "v3_rescue_policy": _config_hash(runtime.v3_rescue_policy),
        "dynamic_robustness_policy": _config_hash(runtime.dynamic_robustness_policy),
        "dynamic_policy": _config_hash(runtime.dynamic_policy),
        "failure_policy": _config_hash(runtime.failure_policy),
    }
    current_source_hashes = {
        name: item["sha256"] for name, item in _runtime_source_commitments().items()
    }
    return (
        prices.get("path") == str(prices_path.resolve())
        and prices.get("sha256") == _file_sha256(prices_path)
        and source_config.get("path") == str(source_config_path.resolve())
        and source_config.get("sha256") == _file_sha256(source_config_path)
        and config_hashes == worker_config_hashes
        and _mapping(runtime_binding.get("source_hashes")) == current_source_hashes
        and runtime.data_quality_status
        == _mapping(runtime_binding.get("data_quality")).get("status")
    )


def _config_hash(config: Any) -> str:
    if not hasattr(config, "model_dump"):
        raise DynamicV3Trading2452EvaluatorError("runtime configuration is not hashable")
    return _stable_hash(config.model_dump(mode="json"))


def _policy_hash(policy: Any) -> str:
    if policy is None or not hasattr(policy, "model_dump"):
        raise DynamicV3Trading2452EvaluatorError("allocation policy is not hashable")
    return dynamic_robustness._stable_hash(policy.model_dump(mode="json"))


def _effective_windows(
    *, replay: Mapping[str, Any], trading_dates: Sequence[date]
) -> list[dict[str, Any]]:
    purge = int(replay.get("purge_trading_days", 0))
    embargo = int(replay.get("embargo_trading_days", 0))
    windows = []
    for raw in _records(replay.get("folds")):
        train_start = r1._shift_trading_day(
            date.fromisoformat(str(raw["train_start"])),
            trading_dates,
            0,
            side="forward",
        )
        train_end = r1._shift_trading_day(
            date.fromisoformat(str(raw["train_end"])),
            trading_dates,
            -purge,
            side="backward",
        )
        test_start = r1._shift_trading_day(
            date.fromisoformat(str(raw["test_start"])),
            trading_dates,
            embargo,
            side="forward",
        )
        test_end = r1._shift_trading_day(
            date.fromisoformat(str(raw["test_end"])),
            trading_dates,
            0,
            side="backward",
        )
        if train_start >= train_end or train_end >= test_start or test_start >= test_end:
            raise DynamicV3Trading2452EvaluatorError(
                f"invalid effective fold {raw.get('window_index')}"
            )
        windows.append(
            {
                **raw,
                "effective_train_start": train_start.isoformat(),
                "effective_train_end": train_end.isoformat(),
                "effective_test_start": test_start.isoformat(),
                "effective_test_end": test_end.isoformat(),
                "purge_trading_days": purge,
                "embargo_trading_days": embargo,
                "trading_calendar": "XNYS",
            }
        )
    return windows


def _run_data_quality_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    output_path: Path,
) -> Any:
    universe = load_universe()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=prices_path.resolve() == DEFAULT_ETF_PRICE_PATH.resolve(),
    )
    write_data_quality_report(report, output_path)
    return report


def _data_quality_payload(
    *,
    report: Any,
    report_path: Path,
    prices_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
    primary_window_alignment_passed = (
        report.price_consistency_start_date == PRIMARY_WINDOW_START
        and report.rate_consistency_start_date == PRIMARY_WINDOW_START
    )
    passed = bool(report.passed and primary_window_alignment_passed)
    return {
        "schema_version": "dynamic_v3_trading2452_runtime_dq.v1",
        "source": "aits_validate_data_same_code_path",
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "same_source_validation_status": report.status,
        "same_source_validation_passed": report.passed,
        "required_consistency_start_date": PRIMARY_WINDOW_START.isoformat(),
        "primary_window_alignment_passed": primary_window_alignment_passed,
        "as_of": report.as_of.isoformat(),
        "checked_at": report.checked_at.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "price_consistency_start_date": (
            None
            if report.price_consistency_start_date is None
            else report.price_consistency_start_date.isoformat()
        ),
        "rate_consistency_start_date": (
            None
            if report.rate_consistency_start_date is None
            else report.rate_consistency_start_date.isoformat()
        ),
        "prices_path": str(prices_path),
        "prices_sha256": _file_sha256(prices_path),
        "rates_path": str(rates_path),
        "rates_sha256": _file_sha256(rates_path),
        "report_path": str(report_path),
        "report_sha256": _file_sha256(report_path),
        "issues": [
            {
                **asdict(issue),
                "severity": issue.severity.value,
            }
            for issue in report.issues
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _assert_execution_boundary(*, selection: Mapping[str, Any], windows: Mapping[str, Any]) -> None:
    primary = _mapping(windows.get("primary_research_window"))
    replay = _mapping(windows.get("historical_protocol_replay"))
    recent = _mapping(windows.get("recent_known_diagnostic"))
    holdout = _mapping(windows.get("prospective_holdout"))
    safety = _mapping(selection.get("safety"))
    if (
        primary.get("start_date") != "2021-02-22"
        or primary.get("legacy_2022_boundary_consumption_allowed") is not False
        or replay.get("main_scoring_end") != "2025-12-31"
        or recent.get("included_in_main_fold_ranking") is not False
        or str(recent.get("end")) >= str(holdout.get("start"))
        or holdout.get("start") != "2026-07-22"
        or holdout.get("accessed_by_this_task") is not False
        or safety.get("candidate_expansion_allowed") is not False
        or safety.get("new_parameter_search_allowed") is not False
        or safety.get("prospective_holdout_access_allowed") is not False
    ):
        raise DynamicV3Trading2452EvaluatorError("TRADING-2452 execution boundary mismatch")


def _historical_seen_report(
    *,
    run_id: str,
    package_id: str,
    effective_windows: Sequence[Mapping[str, Any]],
    train_rows: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
    test_rows: Sequence[Mapping[str, Any]],
    recent_rows: Sequence[Mapping[str, Any]],
    dq_payload: Mapping[str, Any],
    generated: datetime,
) -> dict[str, Any]:
    selected_counts = Counter(int(row.get("window_index", 0)) for row in selected_rows)
    incomplete_folds = [index for index in range(1, 7) if selected_counts[index] < 20]
    incomplete_evaluations = sum(
        row.get("evidence_status") != "COMPLETE" for row in [*train_rows, *test_rows]
    )
    negative_tests = sum(row.get("gate") == legacy.GATE_REJECT for row in test_rows)
    if incomplete_folds:
        status = "INCOMPLETE_NO_ELIGIBLE_CANDIDATE"
    elif incomplete_evaluations:
        status = "INCOMPLETE_EVIDENCE"
    elif negative_tests:
        status = "REVIEW_REQUIRED_NEGATIVE_HISTORICAL_SEEN_TESTS"
    else:
        status = "REVIEW_REQUIRED_HISTORICAL_SEEN_ONLY"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "dynamic_v3_trading2452_historical_seen_report",
        "run_id": run_id,
        "package_id": package_id,
        "status": status,
        "primary_research_window": "2021-02-22..2025-12-31",
        "historical_fold_count": len(effective_windows),
        "candidate_count": 300,
        "top_n_per_fold": 20,
        "train_evaluation_count": len(train_rows),
        "selected_count": len(selected_rows),
        "test_evaluation_count": len(test_rows),
        "negative_test_count": negative_tests,
        "incomplete_evaluation_count": incomplete_evaluations,
        "incomplete_fold_indices": incomplete_folds,
        "recent_known_diagnostic": {
            "evaluation_count": len(recent_rows),
            "included_in_main_fold_ranking": False,
            "candidate_selection_from_recent_metric": False,
        },
        "data_quality": dict(dq_payload),
        "prior_market_outcome_visibility": "KNOWN",
        "historical_replay_investigator_blind": False,
        "unbiased_oos_claim_allowed": False,
        "prospective_holdout": {
            "start": "2026-07-22",
            "accessed": False,
        },
        "interpretation": (
            "本结果是 owner-authorized historical-seen protocol replay；只能用于研究复盘，"
            "不能声明为 investigator-blind 或 unbiased OOS evidence。"
        ),
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _render_historical_seen_report(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic v3 TRADING-2452 historical-seen 评估",
            "",
            f"- 状态：`{payload.get('status')}`",
            f"- 主研究窗口：`{payload.get('primary_research_window')}`",
            f"- historical folds：`{payload.get('historical_fold_count')}`",
            f"- frozen candidates：`{payload.get('candidate_count')}`",
            f"- 每 fold train-only top N：`{payload.get('top_n_per_fold')}`",
            f"- train evaluations：`{payload.get('train_evaluation_count')}`",
            f"- test evaluations：`{payload.get('test_evaluation_count')}`",
            f"- negative tests：`{payload.get('negative_test_count')}`",
            f"- data quality：`{_mapping(payload.get('data_quality')).get('status')}`",
            "- 2026 recent-known diagnostic 不进入主 fold ranking。",
            "- prospective holdout 从 2026-07-22 开始，本次未访问。",
            "- prior_market_outcome_visibility=KNOWN；unbiased_oos_claim_allowed=false。",
            "- production_effect=none；broker_action=none。",
            "",
            "## 解释边界",
            "",
            str(payload.get("interpretation", "")),
            "",
        ]
    )


def _blocked_manifest(
    *,
    run_id: str,
    package_id: str,
    package_root: Path,
    generated: datetime,
    dq_payload: Mapping[str, Any],
    run_dir: Path,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "dynamic_v3_trading2452_historical_seen_manifest",
        "run_id": run_id,
        "status": "BLOCKED_DATA_QUALITY",
        "package_id": package_id,
        "package_root": str(package_root),
        "data_quality": dict(dq_payload),
        "evaluator_executed": False,
        "blocked_reason": "RUNTIME_DATA_QUALITY_GATE_FAILS",
        "output_artifact_checksums": {
            "data_quality_gate.json": _file_sha256(run_dir / "data_quality_gate.json"),
            "data_quality_gate.md": _file_sha256(run_dir / "data_quality_gate.md"),
        },
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _load_package(package_root: Path) -> dict[str, dict[str, Any]]:
    return {
        filename: _load_json(package_root / filename)
        for filename in (
            "candidate_universe.json",
            "research_context.json",
            "preregistration.json",
            "campaign.json",
            "source_contract.json",
            "eligibility.json",
            "package_manifest.json",
        )
    }


def _runtime_source_commitments() -> dict[str, dict[str, Any]]:
    paths = {
        "trading2452_evaluator": Path(__file__),
        "trading2452_package": PROJECT_ROOT
        / "src"
        / "ai_trading_system"
        / "dynamic_v3_clean_selection_trading2452.py",
        "fold_local_evaluator_contract": Path(str(r1.__file__)),
        "dynamic_v3_parameter_evaluator": Path(str(legacy.__file__)),
        "dynamic_v3_real_evaluation": Path(str(real_evaluation.__file__)),
        "dynamic_robustness": Path(str(dynamic_robustness.__file__)),
    }
    return {
        name: {
            "path": str(path),
            "sha256": _file_sha256(path),
            "size": path.stat().st_size,
        }
        for name, path in paths.items()
    }


def _runtime_source_commitments_fresh(commitments: Mapping[str, Any]) -> bool:
    if set(commitments) != {
        "trading2452_evaluator",
        "trading2452_package",
        "fold_local_evaluator_contract",
        "dynamic_v3_parameter_evaluator",
        "dynamic_v3_real_evaluation",
        "dynamic_robustness",
    }:
        return False
    for raw in commitments.values():
        item = _mapping(raw)
        path = Path(str(item.get("path", "")))
        if (
            not path.is_file()
            or item.get("sha256") != _file_sha256(path)
            or item.get("size") != path.stat().st_size
        ):
            return False
    return True


def _fixed_cache_artifacts_valid(
    *, run_dir: Path, records: Sequence[Mapping[str, Any]], runtime_binding_hash: str
) -> bool:
    cache_root = (run_dir / "fixed_report_cache").resolve(strict=True)
    cache_children = list(cache_root.iterdir())
    if any(child.is_symlink() or not child.is_file() for child in cache_children):
        return False
    if {child.name for child in cache_children} != {
        str(record.get("relative_path", "")) for record in records
    }:
        return False
    identities: set[tuple[Any, ...]] = set()
    for raw_record in records:
        record = dict(raw_record)
        relative_path = Path(str(record.get("relative_path", "")))
        if relative_path.is_absolute() or len(relative_path.parts) != 1:
            return False
        unresolved_path = cache_root / relative_path
        if unresolved_path.is_symlink():
            return False
        path = unresolved_path.resolve(strict=True)
        if cache_root not in path.parents:
            return False
        if record.get("file_sha256") != _file_sha256(path):
            return False
        envelope = _load_json(path)
        report = _mapping(envelope.get("report"))
        report_sha256 = sha256(canonical_json_bytes(report)).hexdigest()
        identity = (
            record.get("window_index"),
            record.get("phase"),
            record.get("candidate_id"),
        )
        if identity in identities:
            return False
        identities.add(identity)
        if (
            envelope.get("window_index") != record.get("window_index")
            or envelope.get("phase") != record.get("phase")
            or envelope.get("requested_range") != record.get("requested_range")
            or envelope.get("runtime_binding_hash") != runtime_binding_hash
            or record.get("runtime_binding_hash") != runtime_binding_hash
            or envelope.get("candidate_id") != record.get("candidate_id")
            or envelope.get("dynamic_allocation_policy_hash")
            != record.get("dynamic_allocation_policy_hash")
            or envelope.get("report_sha256") != report_sha256
            or record.get("report_sha256") != report_sha256
            or envelope.get("prospective_holdout_accessed") is not False
        ):
            return False
    return len(identities) == len(records)


def _candidate_cache_files_valid(
    *, cache_root: Path, records: Sequence[Mapping[str, Any]], runtime_binding_hash: str
) -> bool:
    cache_root = cache_root.resolve(strict=True)
    cache_children = list(cache_root.iterdir())
    if any(child.is_symlink() or not child.is_file() for child in cache_children):
        return False
    if {child.name for child in cache_children} != {
        str(record.get("relative_path", "")) for record in records
    }:
        return False
    identities: set[tuple[Any, ...]] = set()
    for raw_record in records:
        record = dict(raw_record)
        relative_path = Path(str(record.get("relative_path", "")))
        if relative_path.is_absolute() or len(relative_path.parts) != 1:
            return False
        requested_range = _mapping(record.get("requested_range"))
        if relative_path.name != _candidate_cache_filename(
            label=str(record.get("candidate_id", "")),
            policy_hash=str(record.get("dynamic_allocation_policy_hash", "")),
            phase=str(record.get("phase", "")),
            window_index=int(record.get("window_index", -1)),
            start=str(requested_range.get("start", "")),
            end=str(requested_range.get("end", "")),
            binding_hash=runtime_binding_hash,
        ):
            return False
        unresolved_path = cache_root / relative_path
        if unresolved_path.is_symlink():
            return False
        path = unresolved_path.resolve(strict=True)
        if cache_root not in path.parents:
            return False
        if record.get("file_sha256") != _file_sha256(path):
            return False
        envelope = _load_json(path)
        report = _mapping(envelope.get("report"))
        report_sha256 = sha256(canonical_json_bytes(report)).hexdigest()
        identity = (
            record.get("window_index"),
            record.get("phase"),
            record.get("candidate_id"),
            record.get("dynamic_allocation_policy_hash"),
        )
        if identity in identities:
            return False
        identities.add(identity)
        if (
            envelope.get("cache_kind") != "candidate_robustness_report"
            or envelope.get("window_index") != record.get("window_index")
            or envelope.get("phase") != record.get("phase")
            or envelope.get("requested_range") != record.get("requested_range")
            or envelope.get("runtime_binding_hash") != runtime_binding_hash
            or record.get("runtime_binding_hash") != runtime_binding_hash
            or envelope.get("candidate_id") != record.get("candidate_id")
            or envelope.get("dynamic_allocation_policy_hash")
            != record.get("dynamic_allocation_policy_hash")
            or envelope.get("report_sha256") != report_sha256
            or record.get("report_sha256") != report_sha256
            or envelope.get("prospective_holdout_accessed") is not False
        ):
            return False
    return len(identities) == len(records)


def _cleanup_candidate_cache_artifacts(
    *,
    cache_root: Path,
    records: Sequence[Mapping[str, Any]],
    runtime_binding_hash: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    resolved_root = cache_root.resolve(strict=True)
    if not _candidate_cache_files_valid(
        cache_root=resolved_root,
        records=records,
        runtime_binding_hash=runtime_binding_hash,
    ):
        raise DynamicV3Trading2452EvaluatorError(
            "candidate cache cleanup blocked by invalid artifact inventory"
        )
    commitments: list[dict[str, Any]] = []
    released_bytes = 0
    paths: list[Path] = []
    for raw_record in records:
        record = dict(raw_record)
        path = resolved_root / str(record.get("relative_path", ""))
        released_bytes += path.stat().st_size
        paths.append(path)
        commitments.append(
            {
                **record,
                "file_size_bytes": path.stat().st_size,
                "validated_before_cleanup": True,
                "cleanup_status": "DELETED_AFTER_PHASE_CONSUMPTION",
                "retained_payload": False,
            }
        )
    for path in paths:
        path.unlink()
    directory_empty = not any(resolved_root.iterdir())
    if not directory_empty:
        raise DynamicV3Trading2452EvaluatorError(
            "candidate cache cleanup left unexpected artifacts"
        )
    return commitments, {
        "status": "PASS",
        "transient": True,
        "validated_artifact_count": len(records),
        "deleted_artifact_count": len(paths),
        "released_bytes": released_bytes,
        "directory_empty": True,
        "production_effect": "none",
    }


def _candidate_cache_commitments_valid(
    *, run_dir: Path, records: Sequence[Mapping[str, Any]], runtime_binding_hash: str
) -> bool:
    cache_root = (run_dir / "candidate_report_cache").resolve(strict=True)
    if any(cache_root.iterdir()):
        return False
    identities: set[tuple[Any, ...]] = set()
    for raw_record in records:
        record = dict(raw_record)
        relative_path = Path(str(record.get("relative_path", "")))
        requested_range = _mapping(record.get("requested_range"))
        identity = (
            record.get("window_index"),
            record.get("phase"),
            record.get("candidate_id"),
            record.get("dynamic_allocation_policy_hash"),
        )
        if identity in identities:
            return False
        identities.add(identity)
        expected_filename = _candidate_cache_filename(
            label=str(record.get("candidate_id", "")),
            policy_hash=str(record.get("dynamic_allocation_policy_hash", "")),
            phase=str(record.get("phase", "")),
            window_index=int(record.get("window_index", -1)),
            start=str(requested_range.get("start", "")),
            end=str(requested_range.get("end", "")),
            binding_hash=runtime_binding_hash,
        )
        if (
            relative_path.is_absolute()
            or len(relative_path.parts) != 1
            or relative_path.name != expected_filename
            or record.get("content_address") != expected_filename
            or record.get("cache_kind") != "candidate_robustness_report"
            or record.get("transient") is not True
            or record.get("runtime_binding_hash") != runtime_binding_hash
            or record.get("validated_before_cleanup") is not True
            or record.get("cleanup_status") != "DELETED_AFTER_PHASE_CONSUMPTION"
            or record.get("retained_payload") is not False
            or not _is_sha256(record.get("file_sha256"))
            or not _is_sha256(record.get("report_sha256"))
            or not _is_sha256(record.get("dynamic_allocation_policy_hash"))
            or int(record.get("file_size_bytes", 0)) <= 0
            or str(requested_range.get("end", "")) >= "2026-07-22"
        ):
            return False
    return len(identities) == len(records)


def _candidate_commitment_digest(records: Sequence[Mapping[str, Any]]) -> str:
    material = [
        {
            key: record.get(key)
            for key in (
                "cache_kind",
                "content_address",
                "file_sha256",
                "report_sha256",
                "phase",
                "window_index",
                "requested_range",
                "runtime_binding_hash",
                "candidate_id",
                "dynamic_allocation_policy_hash",
                "file_size_bytes",
                "validated_before_cleanup",
                "cleanup_status",
                "retained_payload",
            )
        }
        for record in sorted(records, key=lambda item: str(item.get("content_address", "")))
    ]
    return sha256(canonical_json_bytes(material)).hexdigest()


def _group_by_window(rows: Sequence[Mapping[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(int(row.get("window_index", 0)), []).append(dict(row))
    return result


def _project_path(value: object) -> Path:
    path = Path(str(value or ""))
    if path.is_absolute() or ".." in path.parts:
        raise DynamicV3Trading2452EvaluatorError(f"project-relative path required: {value}")
    resolved = (PROJECT_ROOT / path).resolve(strict=False)
    if PROJECT_ROOT.resolve(strict=False) not in (resolved, *resolved.parents):
        raise DynamicV3Trading2452EvaluatorError(f"path escapes project root: {value}")
    return resolved


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(2, 1000):
        candidate = path.with_name(f"{path.name}_{index}")
        if not candidate.exists():
            return candidate
    raise DynamicV3Trading2452EvaluatorError(f"unable to allocate output directory: {path}")


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise DynamicV3Trading2452EvaluatorError(f"mapping YAML required: {path}")
    return dict(payload)


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise DynamicV3Trading2452EvaluatorError(f"mapping JSON required: {path}")
    return dict(payload)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            if not isinstance(payload, Mapping):
                raise DynamicV3Trading2452EvaluatorError(f"JSONL object required: {path}")
            rows.append(dict(payload))
    return rows


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = b"".join(
        canonical_json_bytes(dict(row), indent=None, trailing_newline=True) for row in rows
    )
    write_bytes_atomic(path, payload)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _stable_hash(*values: object) -> str:
    return sha256(canonical_json_bytes({"values": values})).hexdigest()


def _check(check_id: str, passed: bool, details: str | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [] if details is None else [details],
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Run the frozen historical-seen evaluation from a spawn-safe module entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Run the owner-authorized TRADING-2452 frozen historical-seen evaluator. "
            "This command creates research artifacts and starts worker processes."
        )
    )
    parser.parse_args(argv)
    started = datetime.now(UTC)
    result = run_trading2452_historical_seen_evaluator()
    elapsed_seconds = (datetime.now(UTC) - started).total_seconds()
    summary = {
        "status": result.get("status"),
        "run_id": result.get("run_id"),
        "run_dir": str(result.get("run_dir")),
        "elapsed_seconds": elapsed_seconds,
        "artifact_validation_status": _mapping(result.get("validation")).get("status"),
        "production_effect": result.get("production_effect"),
        "broker_action": result.get("broker_action"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if _execution_succeeded(result) else 1


def _execution_succeeded(result: Mapping[str, Any]) -> bool:
    status = str(result.get("status", ""))
    return (
        bool(status)
        and not status.startswith("BLOCKED")
        and _mapping(result.get("validation")).get("status") == "PASS"
    )


__all__ = [
    "DEFAULT_OUTPUT_ROOT",
    "DynamicV3Trading2452EvaluatorError",
    "main",
    "run_trading2452_historical_seen_evaluator",
    "select_train_only_top_n",
    "validate_trading2452_historical_seen_artifact",
]


if __name__ == "__main__":
    raise SystemExit(main())
