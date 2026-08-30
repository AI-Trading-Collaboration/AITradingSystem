from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.workflow_health import (
    DEFAULT_POLICY_PATH,
    build_workflow_health_payloads,
    render_workflow_health_markdown,
    validate_workflow_health_payloads,
)

AS_OF = date(2026, 8, 31)
GENERATED_AT = datetime(2026, 8, 31, 12, tzinfo=UTC)


def test_workflow_health_builds_metrics_and_review_only_candidates(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, permissive=True)
    _write_validation_summary(
        tmp_path,
        run_id="full_20260830T010000Z",
        tier="full",
        status="FAIL",
        elapsed=600,
        commit="a" * 40,
        tail="FAILED tests/test_authority.py::test_current_hash - AssertionError",
    )
    _write_validation_summary(
        tmp_path,
        run_id="full_20260830T020000Z",
        tier="full",
        status="PASS",
        elapsed=400,
        commit="a" * 40,
    )
    _write_validation_summary(
        tmp_path,
        run_id="full_20260830T030000Z",
        tier="full",
        status="FAIL",
        elapsed=200,
        commit="a" * 40,
        tail="FAILED tests/test_authority.py::test_current_hash_again - AssertionError",
    )
    _write_transaction(
        tmp_path,
        transaction_id="task-a-20260830-v1",
        task_id="TASK-A",
        generators=["architecture-manifests"],
        phases=["ACQUIRED", "GENERATED_REBUILD_PRE", "FAILED"],
    )
    _write_transaction(
        tmp_path,
        transaction_id="task-a-registration-20260830-v2",
        task_id="TASK-A",
        generators=["canonical-task-source"],
        phases=["ACQUIRED", "TASK_SOURCE_PRE_WRITE", "FAILED"],
    )
    _write_transaction(
        tmp_path,
        transaction_id="task-b-20260830-v1",
        task_id="TASK-B",
        generators=["architecture-manifests"],
        phases=["ACQUIRED", "CANDIDATE_COMMIT_PRE", "RELEASED"],
    )
    commit_records = [
        {"commit": "1" * 40, "paths": ["docs/system_flow.md", "registry/a.yaml"]},
        {"commit": "2" * 40, "paths": ["docs/task_register.md"]},
        {"commit": "3" * 40, "paths": ["src/ai_trading_system/example.py"]},
    ]

    report, candidates = build_workflow_health_payloads(
        as_of=AS_OF,
        project_root=tmp_path,
        policy_path=policy_path,
        generated_at=GENERATED_AT,
        git_commit_records=commit_records,
    )
    validation = validate_workflow_health_payloads(report, candidates)
    markdown = render_workflow_health_markdown(report)
    rule_ids = {item["rule_id"] for item in candidates["candidates"]}

    assert report["schema_version"] == "workflow_health_report.v1"
    assert report["metrics"]["validation"]["summary_count"] == 3
    assert report["metrics"]["validation"]["failed_full_runtime_ratio"] == 0.666667
    assert report["metrics"]["validation"]["duplicate_validation_group_count"] == 1
    assert report["metrics"]["publication"]["transaction_count"] == 3
    assert report["metrics"]["publication"]["administrative_stop_count"] == 1
    assert report["metrics"]["publication"]["non_admin_failed_terminal_count"] == 1
    assert report["metrics"]["git"]["authority_only_commit_count"] == 2
    assert {
        "failed_full_runtime",
        "early_transaction_churn",
        "authority_only_amplification",
        "duplicate_validation_dispatch",
    }.issubset(rule_ids)
    assert validation["validation_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert "研发流程健康周报" in markdown
    assert all(item["automatic_execution_allowed"] is False for item in candidates["candidates"])


def test_workflow_candidate_fingerprint_is_stable_across_report_dates(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, permissive=True)
    _write_validation_summary(
        tmp_path,
        run_id="full_20260830T010000Z",
        tier="full",
        status="FAIL",
        elapsed=600,
        commit="b" * 40,
    )
    records = [{"commit": "1" * 40, "paths": ["docs/system_flow.md"]}]

    _, first = build_workflow_health_payloads(
        as_of=AS_OF,
        project_root=tmp_path,
        policy_path=policy_path,
        generated_at=GENERATED_AT,
        git_commit_records=records,
    )
    _, second = build_workflow_health_payloads(
        as_of=date(2026, 9, 1),
        project_root=tmp_path,
        policy_path=policy_path,
        generated_at=GENERATED_AT,
        git_commit_records=records,
    )

    first_by_rule = {item["rule_id"]: item["candidate_id"] for item in first["candidates"]}
    second_by_rule = {item["rule_id"]: item["candidate_id"] for item in second["candidates"]}
    assert first_by_rule["failed_full_runtime"] == second_by_rule["failed_full_runtime"]


def test_workflow_health_validation_blocks_executable_candidate(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, permissive=True)
    _write_validation_summary(
        tmp_path,
        run_id="full_20260830T010000Z",
        tier="full",
        status="FAIL",
        elapsed=600,
        commit="c" * 40,
    )
    report, candidates = build_workflow_health_payloads(
        as_of=AS_OF,
        project_root=tmp_path,
        policy_path=policy_path,
        generated_at=GENERATED_AT,
        git_commit_records=[{"commit": "1" * 40, "paths": ["docs/system_flow.md"]}],
    )
    candidates["candidates"][0]["automatic_execution_allowed"] = True

    validation = validate_workflow_health_payloads(report, candidates)

    assert validation["validation_status"] == "FAIL"
    assert {item["issue_id"] for item in validation["blocking_issues"]} == {"candidate_safety"}


def test_workflow_health_discloses_in_window_malformed_summary(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, permissive=False)
    malformed = (
        tmp_path
        / "outputs"
        / "validation_runtime"
        / "full_20260830T010000Z"
        / "test_runtime_summary.json"
    )
    malformed.parent.mkdir(parents=True)
    malformed.write_text("{not-json", encoding="utf-8")

    report, candidates = build_workflow_health_payloads(
        as_of=AS_OF,
        project_root=tmp_path,
        policy_path=policy_path,
        generated_at=GENERATED_AT,
        git_commit_records=[],
    )
    validation = validate_workflow_health_payloads(report, candidates)

    assert report["status"] == "WORKFLOW_HEALTH_LIMITED"
    assert report["telemetry_gaps"][0]["source"] == "validation_runtime"
    assert validation["validation_status"] == "PASS_WITH_WARNINGS"


def test_workflow_health_cli_writes_and_revalidates_bundle(tmp_path: Path) -> None:
    policy_path = _write_policy(tmp_path, permissive=False)
    _init_git_repository(tmp_path)
    _write_validation_summary(
        tmp_path,
        run_id="fast-unit_20260830T010000Z",
        tier="fast-unit",
        status="PASS",
        elapsed=12,
        commit=_git_head(tmp_path),
    )
    reports_dir = tmp_path / "outputs" / "reports"
    runner = CliRunner()

    run = runner.invoke(
        app,
        [
            "reports",
            "workflow-health",
            "--as-of",
            AS_OF.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--policy-path",
            str(policy_path),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    validate = runner.invoke(
        app,
        [
            "reports",
            "validate-workflow-health",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert run.exit_code == 0, run.output
    assert validate.exit_code == 0, validate.output
    expected = {
        f"workflow_health_{AS_OF.isoformat()}.json",
        f"workflow_health_{AS_OF.isoformat()}.md",
        f"workflow_optimization_candidates_{AS_OF.isoformat()}.json",
        f"workflow_health_validation_{AS_OF.isoformat()}.json",
        f"workflow_health_validation_{AS_OF.isoformat()}.md",
    }
    assert expected.issubset({path.name for path in reports_dir.iterdir()})
    payload = json.loads(
        (reports_dir / f"workflow_health_{AS_OF.isoformat()}.json").read_text(encoding="utf-8")
    )
    assert payload["production_effect"] == "none"
    assert payload["market_data_read"] is False


def _write_policy(tmp_path: Path, *, permissive: bool) -> Path:
    payload = yaml.safe_load(DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))
    if permissive:
        rules = payload["candidate_rules"]
        rules["failed_full_runtime"]["minimum_failed_runs"] = 1
        rules["failed_full_runtime"]["minimum_failed_runtime_ratio"] = 0.1
        rules["early_transaction_churn"]["minimum_failed_terminals"] = 1
        rules["early_transaction_churn"]["minimum_failed_ratio"] = 0.1
        rules["authority_only_amplification"]["minimum_commit_count"] = 1
        rules["authority_only_amplification"]["minimum_authority_only_ratio"] = 0.1
        rules["task_retry_churn"]["minimum_transaction_count"] = 1
        rules["task_retry_churn"]["minimum_failed_ratio"] = 0.1
        rules["duplicate_validation_dispatch"]["minimum_group_size"] = 2
        rules["validation_failure_cluster"]["minimum_failed_summaries"] = 2
        rules["validation_failure_cluster"]["minimum_cluster_count"] = 2
    policy_path = tmp_path / "config" / "architecture" / "workflow_health_policy.yaml"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return policy_path


def _write_validation_summary(
    root: Path,
    *,
    run_id: str,
    tier: str,
    status: str,
    elapsed: float,
    commit: str,
    tail: str = "",
) -> None:
    path = root / "outputs" / "validation_runtime" / run_id / "test_runtime_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.strptime(run_id.rsplit("_", 1)[-1], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    payload = {
        "schema_version": 1,
        "resolved_tier": tier,
        "status": status,
        "elapsed_seconds": elapsed,
        "git_commit": commit,
        "ended_at_utc": timestamp.isoformat(),
        "pytest_output_tail": tail.splitlines(),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_transaction(
    root: Path,
    *,
    transaction_id: str,
    task_id: str,
    generators: list[str],
    phases: list[str],
) -> None:
    directory = (
        root
        / "outputs"
        / "architecture"
        / "arch_005_integration_publication_fence"
        / "transactions"
        / transaction_id
    )
    event_dir = directory / "events"
    event_dir.mkdir(parents=True)
    created = datetime(2026, 8, 30, 3, tzinfo=UTC)
    transaction = {
        "transaction_id": transaction_id,
        "task_id": task_id,
        "created_at": created.isoformat(),
        "generator_ids": generators,
    }
    (directory / "transaction.json").write_text(json.dumps(transaction), encoding="utf-8")
    for index, phase in enumerate(phases, start=1):
        event = {
            "sequence": index,
            "phase": phase,
            "occurred_at": created.replace(minute=index).isoformat(),
            "terminal": phase in {"FAILED", "RELEASED"},
        }
        (event_dir / f"{index:04d}_{phase.lower()}.json").write_text(
            json.dumps(event), encoding="utf-8"
        )


def _init_git_repository(path: Path) -> None:
    subprocess.run(["git", "init", "-b", "main", str(path)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "Test"], check=True)
    marker = path / "README.md"
    marker.write_text("test\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-m", "test"], check=True, capture_output=True
    )


def _git_head(path: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()
