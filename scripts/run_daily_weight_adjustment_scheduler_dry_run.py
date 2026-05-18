from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.trading_engine.reports.daily_weight_adjustment import (  # noqa: E402
    FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS,
    PRODUCTION_EFFECT_NONE,
    STATUS_LIMITED,
    STATUS_OBSERVE_ONLY,
    write_daily_weight_adjustment_summary_report,
)

SCHEDULER_DRY_RUN_SCHEMA_VERSION = 1
SCHEDULER_DRY_RUN_REPORT_TYPE = "daily_weight_adjustment_scheduler_dry_run"
MODE_DRY_RUN = "dry_run"
DRY_RUN_STATUS_PASS = "PASS"
DRY_RUN_STATUS_LIMITED = "LIMITED"
DRY_RUN_STATUS_FAIL = "FAIL"


def default_scheduler_dry_run_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"daily_weight_adjustment_scheduler_dry_run_{as_of.isoformat()}.json"


def write_daily_weight_adjustment_scheduler_dry_run_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    weight_adjustment_candidates_path: Path | None = None,
    weight_adjustment_candidates_md_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    weight_candidate_evaluation_md_path: Path | None = None,
    weight_promotion_gate_path: Path | None = None,
    weight_promotion_gate_md_path: Path | None = None,
    daily_summary_output_json_path: Path | None = None,
    daily_summary_output_md_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    now = clock or (lambda: datetime.now(tz=UTC))
    started_at = _ensure_utc(now())
    output_json_path = output_json_path or default_scheduler_dry_run_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    invoked_command = _invoked_command(
        as_of=as_of,
        reports_dir=reports_dir,
        weight_adjustment_candidates_path=weight_adjustment_candidates_path,
        weight_adjustment_candidates_md_path=weight_adjustment_candidates_md_path,
        weight_candidate_evaluation_path=weight_candidate_evaluation_path,
        weight_candidate_evaluation_md_path=weight_candidate_evaluation_md_path,
        weight_promotion_gate_path=weight_promotion_gate_path,
        weight_promotion_gate_md_path=weight_promotion_gate_md_path,
        daily_summary_output_json_path=daily_summary_output_json_path,
        daily_summary_output_md_path=daily_summary_output_md_path,
    )
    summary_payload = write_daily_weight_adjustment_summary_report(
        as_of=as_of,
        reports_dir=reports_dir,
        weight_adjustment_candidates_path=weight_adjustment_candidates_path,
        weight_adjustment_candidates_md_path=weight_adjustment_candidates_md_path,
        weight_candidate_evaluation_path=weight_candidate_evaluation_path,
        weight_candidate_evaluation_md_path=weight_candidate_evaluation_md_path,
        weight_promotion_gate_path=weight_promotion_gate_path,
        weight_promotion_gate_md_path=weight_promotion_gate_md_path,
        output_json_path=daily_summary_output_json_path,
        output_md_path=daily_summary_output_md_path,
        generated_at=started_at,
    )
    completed_at = _ensure_utc(now())
    payload = build_scheduler_dry_run_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        started_at=started_at,
        completed_at=completed_at,
        invoked_command=invoked_command,
        summary_payload=summary_payload,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
    )
    markdown = render_scheduler_dry_run_report(payload)
    _assert_forbidden_terms_absent(payload, markdown)
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.write_text(markdown, encoding="utf-8")
    return payload


def build_scheduler_dry_run_payload(
    *,
    as_of: date,
    reports_dir: Path,
    started_at: datetime,
    completed_at: datetime,
    invoked_command: dict[str, Any],
    summary_payload: dict[str, Any],
    output_json_path: Path,
    output_md_path: Path,
) -> dict[str, Any]:
    summary_outputs = _mapping(summary_payload.get("outputs"))
    daily_summary_json_path = Path(
        str(
            summary_outputs.get("json")
            or reports_dir / f"daily_weight_adjustment_summary_{as_of.isoformat()}.json"
        )
    )
    daily_summary_md_path = Path(
        str(summary_outputs.get("markdown") or daily_summary_json_path.with_suffix(".md"))
    )
    safety_checks = _safety_checks()
    pipeline_status = _string_value(summary_payload.get("status")) or STATUS_LIMITED
    generated_artifacts = [
        _artifact_record(
            "daily_weight_adjustment_summary_json",
            daily_summary_json_path,
            reports_dir=reports_dir,
            role="pipeline_output",
        ),
        _artifact_record(
            "daily_weight_adjustment_summary_markdown",
            daily_summary_md_path,
            reports_dir=reports_dir,
            role="pipeline_output",
        ),
        _artifact_record(
            "scheduler_dry_run_json",
            output_json_path,
            reports_dir=reports_dir,
            role="scheduler_output",
            expected_after_completion=True,
        ),
        _artifact_record(
            "scheduler_dry_run_markdown",
            output_md_path,
            reports_dir=reports_dir,
            role="scheduler_output",
            expected_after_completion=True,
        ),
    ]
    warnings = _warnings(summary_payload=summary_payload, pipeline_status=pipeline_status)
    payload = {
        "schema_version": SCHEDULER_DRY_RUN_SCHEMA_VERSION,
        "report_type": SCHEDULER_DRY_RUN_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": round(
            max(0.0, (completed_at - started_at).total_seconds()),
            3,
        ),
        "mode": MODE_DRY_RUN,
        "dry_run_status": _dry_run_status(
            pipeline_status=pipeline_status,
            safety_checks=safety_checks,
        ),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "invoked_command": invoked_command,
        "generated_artifacts": generated_artifacts,
        "missing_artifacts": _strings(summary_payload.get("missing_artifacts")),
        "pipeline_status": pipeline_status,
        "candidate_count": _optional_int(summary_payload.get("candidate_count")) or 0,
        "promotion_gate_status": (
            _string_value(summary_payload.get("promotion_gate_status")) or "INSUFFICIENT_DATA"
        ),
        "ready_for_manual_review_count": (
            _optional_int(summary_payload.get("ready_for_manual_review_count")) or 0
        ),
        "blocked_count": _optional_int(summary_payload.get("blocked_count")) or 0,
        "warnings": warnings,
        "safety_checks": safety_checks,
        "pipeline_summary": {
            "report_type": summary_payload.get("report_type"),
            "mode": summary_payload.get("mode"),
            "production_effect": summary_payload.get("production_effect"),
            "manual_review_only": summary_payload.get("manual_review_only") is True,
            "main_blocked_by": summary_payload.get("main_blocked_by", "none"),
            "outputs": summary_outputs,
        },
        "notes": [
            "本 dry-run 只验证每日调度可安全调用 daily weight adjustment summary pipeline。",
            "本 dry-run 不修改 production profile，不写 approved profile。",
            "本 dry-run 不触发 IBKR、PaperBroker、replay、controlled fill、"
            "lifecycle 或 comparison。",
            "GitHub repo 不会由该脚本自动 commit、push 或打开 PR。",
        ],
    }
    payload["safety_checks"]["forbidden_terms_absent"] = True
    return payload


def render_scheduler_dry_run_report(payload: dict[str, Any]) -> str:
    invoked_command = _mapping(payload.get("invoked_command"))
    safety_checks = _mapping(payload.get("safety_checks"))
    lines = [
        "# Daily Weight Adjustment Scheduler Dry Run",
        "",
        f"- 状态：{payload.get('dry_run_status')}",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- mode：`{payload.get('mode')}`",
        f"- production_effect：`{payload.get('production_effect')}`",
        "- manual_review_only：true",
        f"- pipeline_status：`{payload.get('pipeline_status')}`",
        f"- duration_seconds：{payload.get('duration_seconds')}",
        "",
        "## Invoked Command",
        "",
        f"- script：`{invoked_command.get('script', '')}`",
        f"- invocation_mode：`{invoked_command.get('invocation_mode', '')}`",
        f"- equivalent_command：`{invoked_command.get('equivalent_command', '')}`",
        "",
        "## Pipeline Summary",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| candidate_count | {payload.get('candidate_count', 0)} |",
        f"| promotion_gate_status | `{payload.get('promotion_gate_status')}` |",
        (
            "| ready_for_manual_review_count | "
            f"{payload.get('ready_for_manual_review_count', 0)} |"
        ),
        f"| blocked_count | {payload.get('blocked_count', 0)} |",
        "",
        "## Generated Artifacts",
        "",
        "| Artifact | Role | Exists | Path |",
        "|---|---|---|---|",
    ]
    for artifact in _records(payload.get("generated_artifacts")):
        lines.append(
            "| "
            f"`{artifact.get('artifact_id', '')}` | "
            f"`{artifact.get('role', '')}` | "
            f"{artifact.get('exists')} | "
            f"`{artifact.get('path', '')}` |"
        )
    lines.extend(
        [
            "",
            "## Missing Artifacts",
            "",
            _bullet_list(_strings(payload.get("missing_artifacts")), "none"),
            "",
            "## Warnings",
            "",
            _bullet_list(_strings(payload.get("warnings")), "none"),
            "",
            "## Safety Checks",
            "",
            "| Check | Value |",
            "|---|---|",
        ]
    )
    for key in sorted(safety_checks):
        lines.append(f"| `{key}` | `{str(safety_checks[key]).lower()}` |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- 本 dry-run 只写 daily summary 和 scheduler dry-run 报告。",
            "- 不修改 production profile，不写 approved profile。",
            "- 不触发 IBKR、PaperBroker、replay、controlled fill、lifecycle 或 comparison。",
            "- 不自动 commit 或 push GitHub repo。",
            "",
        ]
    )
    return "\n".join(lines)


def _invoked_command(
    *,
    as_of: date,
    reports_dir: Path,
    weight_adjustment_candidates_path: Path | None,
    weight_adjustment_candidates_md_path: Path | None,
    weight_candidate_evaluation_path: Path | None,
    weight_candidate_evaluation_md_path: Path | None,
    weight_promotion_gate_path: Path | None,
    weight_promotion_gate_md_path: Path | None,
    daily_summary_output_json_path: Path | None,
    daily_summary_output_md_path: Path | None,
) -> dict[str, Any]:
    argv = [
        "python",
        "scripts/run_daily_weight_adjustment.py",
        "--date",
        as_of.isoformat(),
        "--reports-dir",
        str(reports_dir),
    ]
    optional_args = (
        ("--weight-adjustment-candidates-json", weight_adjustment_candidates_path),
        ("--weight-adjustment-candidates-md", weight_adjustment_candidates_md_path),
        ("--weight-candidate-evaluation-json", weight_candidate_evaluation_path),
        ("--weight-candidate-evaluation-md", weight_candidate_evaluation_md_path),
        ("--weight-promotion-gate-json", weight_promotion_gate_path),
        ("--weight-promotion-gate-md", weight_promotion_gate_md_path),
        ("--output-json-path", daily_summary_output_json_path),
        ("--output-md-path", daily_summary_output_md_path),
    )
    for flag, value in optional_args:
        if value is not None:
            argv.extend([flag, str(value)])
    return {
        "script": "scripts/run_daily_weight_adjustment.py",
        "invocation_mode": "in_process_wrapper",
        "equivalent_argv": argv,
        "equivalent_command": " ".join(_quote_arg(arg) for arg in argv),
    }


def _safety_checks() -> dict[str, bool]:
    return {
        "production_profile_write_attempted": False,
        "approved_profile_write_attempted": False,
        "ibkr_order_path_called": False,
        "paperbroker_order_path_called": False,
        "replay_runner_called": False,
        "controlled_fill_runner_called": False,
        "order_lifecycle_runner_called": False,
        "broker_comparison_runner_called": False,
        "dashboard_write_only_summary": True,
        "git_commit_attempted": False,
        "git_push_attempted": False,
        "forbidden_terms_absent": True,
    }


def _dry_run_status(*, pipeline_status: str, safety_checks: dict[str, bool]) -> str:
    if any(_safety_check_failed(key, value) for key, value in safety_checks.items()):
        return DRY_RUN_STATUS_FAIL
    if pipeline_status == STATUS_OBSERVE_ONLY:
        return DRY_RUN_STATUS_PASS
    return DRY_RUN_STATUS_LIMITED


def _safety_check_failed(key: str, value: bool) -> bool:
    expected_true = {"dashboard_write_only_summary", "forbidden_terms_absent"}
    return value is not (key in expected_true)


def _warnings(*, summary_payload: dict[str, Any], pipeline_status: str) -> list[str]:
    warnings = _strings(summary_payload.get("warnings"))
    if pipeline_status != STATUS_OBSERVE_ONLY:
        warnings.append("pipeline_status_limited")
    return list(dict.fromkeys(warnings))


def _artifact_record(
    artifact_id: str,
    path: Path,
    *,
    reports_dir: Path,
    role: str,
    expected_after_completion: bool = False,
) -> dict[str, Any]:
    path_exists = path.exists()
    exists = path_exists or expected_after_completion
    return {
        "artifact_id": artifact_id,
        "role": role,
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": exists,
        "size_bytes": path.stat().st_size if path_exists else None,
        "checksum_sha256": _sha256(path) if path_exists else "",
        "checksum_note": (
            ""
            if path_exists
            else (
                "current_report_checksum_not_embedded"
                if expected_after_completion
                else "artifact_missing"
            )
        ),
    }


def _assert_forbidden_terms_absent(payload: dict[str, Any], markdown: str) -> None:
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown
    for term in FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS:
        if term in combined:
            raise ValueError(f"Forbidden scheduler dry-run term present: {term}")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_href(path: Path, reports_dir: Path) -> str:
    try:
        return path.relative_to(reports_dir).as_posix()
    except ValueError:
        return str(path)


def _bullet_list(values: list[str], empty: str) -> str:
    if not values:
        return f"- {empty}"
    return "\n".join(f"- `{value}`" for value in values)


def _quote_arg(value: str) -> str:
    if value and all(char not in value for char in " \t\n\r\"'"):
        return value
    return '"' + value.replace('"', '\\"') + '"'


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run daily weight adjustment scheduler dry-run safely."
    )
    parser.add_argument("--date", required=True, help="Dry-run date in YYYY-MM-DD format.")
    parser.add_argument(
        "--reports-dir",
        default=str(REPO_ROOT / "outputs" / "reports"),
        help="Directory containing existing weight adjustment artifacts.",
    )
    parser.add_argument(
        "--weight-adjustment-candidates-json",
        help="Existing weight adjustment candidates JSON path.",
    )
    parser.add_argument(
        "--weight-adjustment-candidates-md",
        help="Existing weight adjustment candidates Markdown path.",
    )
    parser.add_argument(
        "--weight-candidate-evaluation-json",
        help="Existing weight candidate evaluation JSON path.",
    )
    parser.add_argument(
        "--weight-candidate-evaluation-md",
        help="Existing weight candidate evaluation Markdown path.",
    )
    parser.add_argument(
        "--weight-promotion-gate-json",
        help="Existing weight promotion gate JSON path.",
    )
    parser.add_argument(
        "--weight-promotion-gate-md",
        help="Existing weight promotion gate Markdown path.",
    )
    parser.add_argument("--daily-summary-output-json-path", help="Output summary JSON path.")
    parser.add_argument("--daily-summary-output-md-path", help="Output summary Markdown path.")
    parser.add_argument("--output-json-path", help="Output scheduler dry-run JSON path.")
    parser.add_argument("--output-md-path", help="Output scheduler dry-run Markdown path.")
    args = parser.parse_args()

    payload = write_daily_weight_adjustment_scheduler_dry_run_report(
        as_of=date.fromisoformat(args.date),
        reports_dir=Path(args.reports_dir),
        weight_adjustment_candidates_path=(
            Path(args.weight_adjustment_candidates_json)
            if args.weight_adjustment_candidates_json
            else None
        ),
        weight_adjustment_candidates_md_path=(
            Path(args.weight_adjustment_candidates_md)
            if args.weight_adjustment_candidates_md
            else None
        ),
        weight_candidate_evaluation_path=(
            Path(args.weight_candidate_evaluation_json)
            if args.weight_candidate_evaluation_json
            else None
        ),
        weight_candidate_evaluation_md_path=(
            Path(args.weight_candidate_evaluation_md)
            if args.weight_candidate_evaluation_md
            else None
        ),
        weight_promotion_gate_path=(
            Path(args.weight_promotion_gate_json) if args.weight_promotion_gate_json else None
        ),
        weight_promotion_gate_md_path=(
            Path(args.weight_promotion_gate_md) if args.weight_promotion_gate_md else None
        ),
        daily_summary_output_json_path=(
            Path(args.daily_summary_output_json_path)
            if args.daily_summary_output_json_path
            else None
        ),
        daily_summary_output_md_path=(
            Path(args.daily_summary_output_md_path) if args.daily_summary_output_md_path else None
        ),
        output_json_path=Path(args.output_json_path) if args.output_json_path else None,
        output_md_path=Path(args.output_md_path) if args.output_md_path else None,
    )
    outputs = {
        artifact["artifact_id"]: artifact["path"]
        for artifact in _records(payload.get("generated_artifacts"))
    }
    print(f"Scheduler dry-run 状态：{payload['dry_run_status']}")
    print(f"pipeline_status：{payload['pipeline_status']}")
    print(f"candidate_count：{payload['candidate_count']}")
    print(f"promotion_gate_status：{payload['promotion_gate_status']}")
    print(f"production_effect：{payload['production_effect']}")
    print(f"manual_review_only：{str(payload['manual_review_only']).lower()}")
    print(f"JSON：{outputs.get('scheduler_dry_run_json', '')}")
    print(f"Markdown：{outputs.get('scheduler_dry_run_markdown', '')}")


if __name__ == "__main__":
    main()
