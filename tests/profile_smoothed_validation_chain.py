from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any, TypeVar

from dynamic_v3_system_target_helpers import (
    TARGET_AS_OF,
    build_model_target_fixture,
    run_smoothed_recorded_owner_authority_fixture,
    write_market_cache,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    ArtifactValidationSessionTelemetry,
    artifact_validation_session,
)

_T = TypeVar("_T")
_DEFAULT_SOURCE_FULL = Path(
    "outputs/validation_runtime/full_20260726T041711Z/test_runtime_summary.json"
)
_DURATION_PROFILE = Path("inputs/architecture/arch_004g2_full_duration_profile.yaml")


def _file_record(path: Path) -> dict[str, Any]:
    payload = path.read_bytes()
    return {
        "path": path.as_posix(),
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _timed_stage(
    stage_seconds: dict[str, float],
    stage_id: str,
    function: Callable[[], _T],
) -> _T:
    started = perf_counter()
    try:
        return function()
    finally:
        stage_seconds[stage_id] = perf_counter() - started


def build_smoothed_validation_observation(
    *,
    output_path: Path,
    source_full_summary: Path,
) -> dict[str, Any]:
    source_summary = json.loads(source_full_summary.read_text(encoding="utf-8"))
    runtime_summary = source_summary.get("runtime_profile_summary") or {}
    if (
        source_summary.get("status") != "PASS"
        or runtime_summary.get("scheduler_applied") is not True
        or runtime_summary.get("scheduler_fallback") is not False
        or runtime_summary.get("telemetry_status") != "PASS"
        or runtime_summary.get("performance_evidence_status") != "PASS"
    ):
        raise ValueError("source Full must be a scheduler-applied, no-fallback PASS")

    stage_seconds: dict[str, float] = {}
    total_started = perf_counter()
    with TemporaryDirectory(prefix="aits_smoothed_validation_observation_") as temp_dir:
        work_root = Path(temp_dir)
        with artifact_validation_session(collect_telemetry=True) as telemetry:
            if not isinstance(telemetry, ArtifactValidationSessionTelemetry):
                raise RuntimeError("validation-session telemetry was not enabled")
            target = _timed_stage(
                stage_seconds,
                "model_target_fixture",
                lambda: build_model_target_fixture(work_root),
            )
            authority = _timed_stage(
                stage_seconds,
                "recorded_owner_authority_fixture",
                lambda: run_smoothed_recorded_owner_authority_fixture(work_root),
            )
            prices_path, rates_path = _timed_stage(
                stage_seconds,
                "market_cache_fixture",
                lambda: write_market_cache(work_root / "weekly_market_cache"),
            )
            generated_at = authority["authority_ready_at"] + timedelta(seconds=1)
            weekly = _timed_stage(
                stage_seconds,
                "weekly_producer",
                lambda: system_target.run_smoothed_forward_weekly_run(
                    week_ending=TARGET_AS_OF,
                    target_id=target["target_id"],
                    binding_id=authority["binding"]["binding_id"],
                    switch_plan_id=authority["switch_plan"]["switch_plan_id"],
                    owner_promotion_id=authority["recorded_owner_promotion"]["decision_id"],
                    model_target_dir=work_root / "model_target",
                    emission_dir=work_root / "smoothed_daily_emission",
                    due_dir=work_root / "smoothed_outcome_due",
                    update_dir=work_root / "smoothed_outcome_update",
                    classification_dir=work_root / "smoothed_forward_classification",
                    binding_dir=work_root / "smoothed_forward_binding",
                    progress_dir=work_root / "smoothed_forward_progress_weekly",
                    dashboard_dir=work_root / "smoothed_weekly_dashboard_weekly",
                    monitor_dir=work_root / "smoothed_event_monitor_weekly",
                    switch_plan_dir=work_root / "paper_shadow_primary_switch",
                    recheck_dir=work_root / "smoothed_switch_readiness_weekly",
                    owner_promotion_dir=work_root / "smoothed_owner_promotion",
                    renewal_dir=work_root / "smoothed_owner_renewal_weekly",
                    output_dir=work_root / "smoothed_forward_weekly_run",
                    price_cache_path=prices_path,
                    rates_cache_path=rates_path,
                    generated_at=generated_at,
                ),
            )
            check = _timed_stage(
                stage_seconds,
                "weekly_validator",
                lambda: system_target.validate_smoothed_forward_weekly_run_artifact(
                    weekly_run_id=weekly["weekly_run_id"],
                    output_dir=work_root / "smoothed_forward_weekly_run",
                ),
            )
            telemetry_payload = telemetry.to_dict()

    summary = weekly["weekly_run_summary"]
    if (
        check.get("status") != "PASS"
        or summary.get("due_windows") != 0
        or summary.get("updated_windows") != 0
        or summary.get("candidate_method") is not None
        or summary.get("weekly_recommendation") != "continue_observation"
        or summary.get("production_effect") != "none"
        or summary.get("broker_action_allowed") is not False
    ):
        raise RuntimeError("Smoothed Weekly diagnostic did not preserve the frozen path")

    validator_rows = telemetry_payload["validators"]
    top_validators = sorted(
        ({"validator_id": validator_id, **row} for validator_id, row in validator_rows.items()),
        key=lambda row: (
            -float(row["validator_seconds"]),
            -int(row["validator_execution_count"]),
            str(row["validator_id"]),
        ),
    )
    generated_at_utc = datetime.now(UTC)
    payload = {
        "schema_version": "smoothed_validation_chain_observation.v1",
        "status": "OBSERVED",
        "generated_at_utc": generated_at_utc.isoformat().replace("+00:00", "Z"),
        "task_id": "ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE",
        "scope": "smoothed_forward_weekly_no_due_windows",
        "source_full_summary": _file_record(source_full_summary),
        "duration_profile": _file_record(_DURATION_PROFILE),
        "source_full_runtime": {
            "collection_count": runtime_summary.get("collection_count"),
            "file_count": runtime_summary.get("file_count"),
            "worker_count": runtime_summary.get("worker_count"),
            "duration_matched_file_count": runtime_summary.get("duration_matched_file_count"),
            "scheduler_applied": runtime_summary.get("scheduler_applied"),
            "scheduler_fallback": runtime_summary.get("scheduler_fallback"),
            "tail_idle_max_seconds": runtime_summary.get("tail_idle_max_seconds"),
            "tail_idle_total_seconds": runtime_summary.get("tail_idle_total_seconds"),
        },
        "stage_seconds": stage_seconds,
        "diagnostic_wall_seconds": perf_counter() - total_started,
        "validation_session": telemetry_payload,
        "top_validators_by_execution_seconds": top_validators,
        "observed_weekly_path": {
            "due_windows": summary["due_windows"],
            "updated_windows": summary["updated_windows"],
            "candidate_method": summary["candidate_method"],
            "weekly_recommendation": summary["weekly_recommendation"],
            "validator_status": check["status"],
        },
        "stable_full_improvement_claimed": False,
        "optimization_authorized": False,
        "strategy_logic_changed": False,
        "cached_data_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    write_json_atomic(output_path, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="生成Smoothed真实producer/validator链的只读调用级诊断。"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--source-full-summary",
        type=Path,
        default=_DEFAULT_SOURCE_FULL,
    )
    args = parser.parse_args()
    payload = build_smoothed_validation_observation(
        output_path=args.output,
        source_full_summary=args.source_full_summary,
    )
    telemetry = payload["validation_session"]
    print(
        json.dumps(
            {
                "status": payload["status"],
                "output": args.output.as_posix(),
                "diagnostic_wall_seconds": payload["diagnostic_wall_seconds"],
                "validation_calls": telemetry["call_count"],
                "cache_hits": telemetry["cache_hit_count"],
                "cache_misses": telemetry["cache_miss_count"],
                "cache_bypasses": telemetry["cache_bypass_count"],
                "validator_executions": telemetry["validator_execution_count"],
                "production_effect": payload["production_effect"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
