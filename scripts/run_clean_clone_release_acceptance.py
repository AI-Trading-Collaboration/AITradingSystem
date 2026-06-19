from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
REPORT_TYPE = "clean_clone_release_acceptance"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "CLEAN_CLONE_ACCEPTANCE_PASS"
BLOCKED_DIRTY_STATUS = "CLEAN_CLONE_ACCEPTANCE_BLOCKED_UNCOMMITTED_CHANGES"
FAIL_STATUS = "CLEAN_CLONE_ACCEPTANCE_FAIL"
DEFAULT_WORK_DIR = Path("run/ccra")
RUN_ID_PREFIX = "ccra"
GIT_LONGPATHS_CONFIG = "core.longpaths=true"


def main() -> int:
    args = _parse_args()
    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    source_root = args.source_root.resolve()
    output_dir = args.output_dir.resolve()
    work_dir = args.work_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"{RUN_ID_PREFIX}_{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"
    run_root = work_dir / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    checkout_root = run_root / "checkout"
    sample_root = run_root / "sample_project"

    dirty_files = _git_status_porcelain(source_root)
    checkout_mode = "clean_clone"
    steps: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []

    if dirty_files and not args.allow_dirty_snapshot:
        blocking_issues.append(
            _issue(
                "working_tree_dirty",
                "当前工作树存在未提交变更，不能作为 clean clone release acceptance 证据。",
                "commit_or_stash_current_closeout_changes_then_rerun_clean_clone_acceptance",
                {"dirty_file_count": len(dirty_files), "dirty_files": dirty_files[:200]},
            )
        )
    else:
        if dirty_files:
            checkout_mode = "working_tree_snapshot"
            _copy_git_visible_files(source_root, checkout_root)
            blocking_issues.append(
                _issue(
                    "dirty_snapshot_is_not_release_clone",
                    (
                        "本次使用 working-tree snapshot 运行 smoke，只能证明当前文件快照可跑，"
                        "不能证明提交后的 clean clone。"
                    ),
                    "commit_closeout_changes_then_rerun_without_allow_dirty_snapshot",
                    {"dirty_file_count": len(dirty_files), "dirty_files": dirty_files[:200]},
                )
            )
        else:
            steps.append(
                _run_step(
                    "git_clone",
                    _git_clone_command(source_root, checkout_root),
                    cwd=run_root,
                    timeout_seconds=args.command_timeout_seconds,
                )
            )

        if checkout_root.exists():
            _write_sample_project(sample_root, as_of)
            steps.extend(
                _run_acceptance_steps(
                    checkout_root=checkout_root,
                    sample_root=sample_root,
                    as_of=as_of,
                    run_artifact_reproduce=not args.skip_artifact_reproduce,
                    timeout_seconds=args.command_timeout_seconds,
                )
            )

    failed_steps = [step for step in steps if step["exit_code"] != 0]
    if failed_steps:
        blocking_issues.append(
            _issue(
                "acceptance_step_failed",
                "Clean-clone release acceptance step failed.",
                "inspect_failed_step_stdout_stderr_and_fix_before_platform_freeze",
                {"failed_step_ids": [step["step_id"] for step in failed_steps]},
            )
        )

    if blocking_issues:
        status = BLOCKED_DIRTY_STATUS if dirty_files else FAIL_STATUS
    else:
        status = PASS_STATUS

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "release_acceptance_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_engineering_closeout",
        "run_id": run_id,
        "checkout_mode": checkout_mode,
        "source_root": str(source_root),
        "checkout_root": str(checkout_root),
        "sample_project_root": str(sample_root),
        "summary": {
            "step_count": len(steps),
            "failed_step_count": len(failed_steps),
            "blocking_issue_count": len(blocking_issues),
            "dirty_file_count": len(dirty_files),
            "artifact_reproduce_requested": not args.skip_artifact_reproduce,
            "clean_clone_verified": status == PASS_STATUS and checkout_mode == "clean_clone",
        },
        "steps": steps,
        "blocking_issues": blocking_issues,
        "reader_brief": {
            "summary": (
                f"Clean clone release acceptance status is {status}; "
                f"steps={len(steps)}, failed={len(failed_steps)}."
            ),
            "key_result": status,
            "blocking_issues": [issue["issue_id"] for issue in blocking_issues],
            "warnings": [],
            "safety_boundary": (
                "read_only_release_acceptance; production_effect=none; no broker/order, "
                "official target weights, paper-shadow activation, or production mutation."
            ),
            "next_action": (
                "platform_freeze_release_candidate_review"
                if status == PASS_STATUS
                else "commit_closeout_changes_and_rerun_clean_clone_acceptance"
            ),
        },
        "methodology": {
            "mode": "clean_clone_release_acceptance",
            "requires_clean_git_worktree_for_pass": True,
            "dirty_snapshot_never_counts_as_clean_clone_pass": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_logic": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }

    json_path = output_dir / f"clean_clone_release_acceptance_{as_of.isoformat()}.json"
    md_path = output_dir / f"clean_clone_release_acceptance_{as_of.isoformat()}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(payload), encoding="utf-8")

    print(f"Clean clone release acceptance：{status}")
    print(f"JSON：{json_path}")
    print(f"Markdown：{md_path}")
    print(
        f"steps={len(steps)}；failed={len(failed_steps)}；"
        f"blocking={len(blocking_issues)}；production_effect=none"
    )
    if status != PASS_STATUS and not args.allow_blocked_exit_zero:
        return 1
    return 0


def _run_acceptance_steps(
    *,
    checkout_root: Path,
    sample_root: Path,
    as_of: date,
    run_artifact_reproduce: bool,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    venv_dir = checkout_root / ".acceptance_venv"
    python_exe = _venv_python(venv_dir)
    sample_reports = sample_root / "outputs" / "reports"
    sample_registry = sample_root / "config" / "report_registry.yaml"
    sample_waivers = sample_root / "config" / "report_index_visibility_waivers.yaml"
    steps = [
        _run_step(
            "create_venv",
            [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "install_editable_no_deps",
            [str(python_exe), "-m", "pip", "install", "-e", ".", "--no-deps"],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "aits_help",
            [str(python_exe), "-m", "ai_trading_system.cli", "--help"],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_engineering_surface_inventory",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "reports",
                "engineering-surface-inventory",
                "--as-of",
                as_of.isoformat(),
                "--project-root",
                str(sample_root),
                "--registry-path",
                str(sample_registry),
                "--reports-dir",
                str(sample_reports),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_engineering_surface_inventory_validation",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "reports",
                "validate-engineering-surface-inventory",
                "--source-json-path",
                str(sample_reports / f"engineering_surface_inventory_{as_of.isoformat()}.json"),
                "--reports-dir",
                str(sample_reports),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_report_index",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "reports",
                "index",
                "--as-of",
                as_of.isoformat(),
                "--project-root",
                str(sample_root),
                "--registry-path",
                str(sample_registry),
                "--waiver-path",
                str(sample_waivers),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_report_latest",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "reports",
                "latest",
                "--report-id",
                "candidate_v2_research_gate",
                "--as-of",
                as_of.isoformat(),
                "--project-root",
                str(sample_root),
                "--registry-path",
                str(sample_registry),
                "--waiver-path",
                str(sample_waivers),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_system_status",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "system",
                "status",
                "--as-of",
                as_of.isoformat(),
                "--project-root",
                str(sample_root),
                "--registry-path",
                str(sample_registry),
                "--waiver-path",
                str(sample_waivers),
                "--reports-dir",
                str(sample_reports),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
        _run_step(
            "sample_system_doctor",
            [
                str(python_exe),
                "-m",
                "ai_trading_system.cli",
                "system",
                "doctor",
                "--source-json-path",
                str(sample_reports / f"canonical_system_status_{as_of.isoformat()}.json"),
                "--reports-dir",
                str(sample_reports),
            ],
            cwd=checkout_root,
            timeout_seconds=timeout_seconds,
        ),
    ]
    if run_artifact_reproduce:
        steps.append(
            _run_step(
                "artifact_reproduce_validation_tier",
                [
                    str(python_exe),
                    "scripts/run_validation_tier.py",
                    "artifact-reproduce",
                    "--write-runtime-artifact",
                ],
                cwd=checkout_root,
                timeout_seconds=timeout_seconds,
            )
        )
    return steps


def _write_sample_project(sample_root: Path, as_of: date) -> None:
    (sample_root / "src" / "ai_trading_system" / "cli_commands").mkdir(parents=True)
    (sample_root / "config").mkdir(parents=True)
    (sample_root / "docs" / "operations").mkdir(parents=True)
    (sample_root / "outputs" / "reports").mkdir(parents=True)
    (sample_root / "src" / "ai_trading_system" / "cli.py").write_text(
        "import typer\n"
        "app = typer.Typer()\n"
        "app.add_typer(typer.Typer(), name='system')\n"
        "app.add_typer(typer.Typer(), name='reports')\n",
        encoding="utf-8",
    )
    (sample_root / "src" / "ai_trading_system" / "cli_commands" / "reports.py").write_text(
        "import typer\n"
        "reports_app = typer.Typer()\n"
        "@reports_app.command('sample-research-gate')\n"
        "def sample_research_gate() -> None:\n"
        "    pass\n",
        encoding="utf-8",
    )
    (sample_root / "config" / "report_index_visibility_waivers.yaml").write_text(
        "schema_version: 1\n"
        "policy_id: clean_clone_sample_waivers\n"
        "policy_metadata:\n"
        "  owner: system\n"
        "  status: active_empty\n"
        "  rationale: clean clone sample uses no waivers\n"
        "  intended_effect: keep sample warnings explicit\n"
        "  validation_evidence: clean clone release acceptance\n"
        "  review_condition: before platform freeze\n"
        "waivers: []\n",
        encoding="utf-8",
    )
    (sample_root / "config" / "report_registry.yaml").write_text(
        _sample_registry_yaml(),
        encoding="utf-8",
    )
    (sample_root / "docs" / "artifact_catalog.md").write_text(
        "|Artifact|Generator|Inputs|Schema|Consumers|Production|Notes|\n"
        "|---|---|---|---|---|---|---|\n"
        "|`outputs/reports/engineering_surface_inventory_YYYY-MM-DD.json`|"
        "`aits reports engineering-surface-inventory`|sample project metadata|"
        "schema_version=1|clean clone acceptance|否|read-only|\n"
        "|`outputs/reports/candidate_v2_research_gate_YYYY-MM-DD.json`|"
        "sample fixture|sample candidate|schema_version=1|system status|否|research-only sample|\n",
        encoding="utf-8",
    )
    (sample_root / "docs" / "system_flow.md").write_text("# Sample System Flow\n", encoding="utf-8")
    (sample_root / "docs" / "task_register.md").write_text("# Sample Tasks\n", encoding="utf-8")
    (sample_root / "docs" / "operations" / "operations_runbook.md").write_text(
        "# Sample Operations Runbook\n",
        encoding="utf-8",
    )
    research_gate = {
        "schema_version": 1,
        "report_type": "candidate_v2_research_gate",
        "as_of": as_of.isoformat(),
        "status": "V2_RETURN_TO_HYPOTHESIS_BACKLOG",
        "production_effect": PRODUCTION_EFFECT,
        "summary": {
            "candidate_id": "clean_clone_sample_candidate",
            "source_research_gate_decision": "V2_RETURN_TO_HYPOTHESIS_BACKLOG",
        },
        "reader_brief": {
            "summary": "Clean-clone sample research gate returns to backlog.",
            "key_result": "V2_RETURN_TO_HYPOTHESIS_BACKLOG",
            "blocking_issues": [],
            "warnings": [],
            "safety_boundary": "sample_research_only; production_effect=none",
            "next_action": "use_clean_clone_acceptance_only_as_engineering_smoke",
        },
    }
    sample_reports = sample_root / "outputs" / "reports"
    gate_json = sample_reports / f"candidate_v2_research_gate_{as_of.isoformat()}.json"
    gate_md = sample_reports / f"candidate_v2_research_gate_{as_of.isoformat()}.md"
    gate_json.write_text(json.dumps(research_gate, ensure_ascii=False, indent=2), encoding="utf-8")
    gate_md.write_text(
        "# Sample Candidate V2 Research Gate\n\n"
        "- status: V2_RETURN_TO_HYPOTHESIS_BACKLOG\n"
        "- production_effect: none\n",
        encoding="utf-8",
    )


def _sample_registry_yaml() -> str:
    return """schema_version: 1
policy_version: clean_clone_sample_report_registry_v1
policy_metadata:
  owner: system
  status: sample
  rationale: Minimal clean-clone acceptance registry.
  intended_effect: Exercise canonical reports/status commands without local runtime cache.
  validation_evidence: scripts/run_clean_clone_release_acceptance.py
  review_condition: before platform freeze.
defaults:
  production_effect: none
  missing_status: MISSING
  stale_status: STALE
reports:
  - report_id: engineering_surface_inventory
    title: Engineering Surface Inventory
    group: governance
    cadence: daily
    audience: operator
    owner: system
    command: aits reports engineering-surface-inventory
    artifact_globs:
      - outputs/reports/engineering_surface_inventory_*.json
      - outputs/reports/engineering_surface_inventory_*.md
    freshness_sla_days: 1
    freshness_rationale: Clean-clone acceptance must generate this source artifact.
    owner_action: rerun_clean_clone_acceptance
    include_in_reader_brief: true
    include_in_daily_task_dashboard: false
    required_for_daily_reading: false
  - report_id: engineering_surface_inventory_validation
    title: Engineering Surface Inventory Validation
    group: governance
    cadence: daily
    audience: operator
    owner: system
    command: aits reports validate-engineering-surface-inventory
    artifact_globs:
      - outputs/reports/engineering_surface_inventory_validation_*.json
      - outputs/reports/engineering_surface_inventory_validation_*.md
    freshness_sla_days: 1
    freshness_rationale: Clean-clone acceptance must validate the inventory.
    owner_action: rerun_clean_clone_acceptance
    include_in_reader_brief: true
    include_in_daily_task_dashboard: false
    required_for_daily_reading: false
  - report_id: candidate_v2_research_gate
    title: Clean Clone Sample Research Gate
    group: research
    cadence: daily
    audience: reviewer
    owner: system
    command: sample fixture
    artifact_globs:
      - outputs/reports/candidate_v2_research_gate_*.json
      - outputs/reports/candidate_v2_research_gate_*.md
    freshness_sla_days: 1
    freshness_rationale: Sample minimal research gate used only by clean-clone release acceptance.
    owner_action: use_as_sample_only
    include_in_reader_brief: true
    include_in_daily_task_dashboard: false
    required_for_daily_reading: false
"""


def _git_status_porcelain(source_root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=source_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return [f"git_status_failed:{result.stderr.strip()}"]
    return [line for line in result.stdout.splitlines() if line.strip()]


def _git_clone_command(source_root: Path, checkout_root: Path) -> list[str]:
    return [
        "git",
        "-c",
        GIT_LONGPATHS_CONFIG,
        "clone",
        "--local",
        "--no-hardlinks",
        str(source_root),
        str(checkout_root),
    ]


def _copy_git_visible_files(source_root: Path, checkout_root: Path) -> None:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=source_root,
        text=True,
        capture_output=True,
        check=True,
    )
    for relative in result.stdout.splitlines():
        source = source_root / relative
        target = checkout_root / relative
        if not source.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


def _run_step(
    step_id: str,
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            check=False,
        )
        exit_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr = f"{stderr}\nTimed out after {timeout_seconds} seconds.".strip()
    elapsed = round(time.monotonic() - started, 3)
    return {
        "step_id": step_id,
        "command": command,
        "cwd": str(cwd),
        "exit_code": exit_code,
        "status": "PASS" if exit_code == 0 else "FAIL",
        "elapsed_seconds": elapsed,
        "stdout_tail": _tail(stdout),
        "stderr_tail": _tail(stderr),
        "production_effect": PRODUCTION_EFFECT,
    }


def _tail(text: str, *, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _issue(
    issue_id: str,
    message: str,
    recommended_action: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "severity": "BLOCKING",
        "message": message,
        "recommended_action": recommended_action,
        "details": details,
    }


def _venv_python(venv_dir: Path) -> Path:
    if sys.platform.startswith("win"):
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Clean Clone Release Acceptance {payload['as_of']}",
        "",
        "## Summary",
        "",
        f"- status: {payload['release_acceptance_status']}",
        f"- checkout_mode: {payload['checkout_mode']}",
        f"- production_effect: {payload['production_effect']}",
        f"- step_count: {payload['summary']['step_count']}",
        f"- failed_step_count: {payload['summary']['failed_step_count']}",
        f"- blocking_issue_count: {payload['summary']['blocking_issue_count']}",
        f"- clean_clone_verified: {payload['summary']['clean_clone_verified']}",
        "",
        "## Blocking Issues",
        "",
        "|issue_id|message|recommended_action|",
        "|---|---|---|",
    ]
    issues = payload.get("blocking_issues", [])
    if issues:
        for issue in issues:
            lines.append(
                f"|{_cell(issue['issue_id'])}|{_cell(issue['message'])}|"
                f"{_cell(issue['recommended_action'])}|"
            )
    else:
        lines.append("|NONE|No blocking issues.|platform_freeze_release_candidate_review|")
    lines.extend(
        ["", "## Steps", "", "|step_id|status|exit_code|elapsed_seconds|", "|---|---|---|---|"]
    )
    for step in payload.get("steps", []):
        lines.append(
            f"|{_cell(step['step_id'])}|{_cell(step['status'])}|"
            f"{_cell(step['exit_code'])}|{_cell(step['elapsed_seconds'])}|"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "This acceptance runner is read-only for project state. It does not refresh data, "
            "modify strategy logic, generate official target weights, touch broker/order systems, "
            "activate paper shadow, or mutate production state.",
            "",
        ]
    )
    return "\n".join(lines)


def _cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run clean-clone release acceptance for engineering closeout."
    )
    parser.add_argument("--as-of", help="Acceptance date in YYYY-MM-DD format.")
    parser.add_argument("--source-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/reports"))
    parser.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    parser.add_argument("--command-timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--allow-dirty-snapshot",
        action="store_true",
        help=(
            "Run a working-tree snapshot smoke when the source worktree is dirty. "
            "This never counts as clean-clone PASS."
        ),
    )
    parser.add_argument(
        "--allow-blocked-exit-zero",
        action="store_true",
        help=(
            "Return exit code 0 even when status is blocked; useful for generating audit "
            "artifacts."
        ),
    )
    parser.add_argument(
        "--skip-artifact-reproduce",
        action="store_true",
        help="Skip artifact-reproduce validation tier inside the clone/snapshot.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
