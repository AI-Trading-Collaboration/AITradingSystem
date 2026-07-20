from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from ai_trading_system.platform.architecture.parallel_control import ParallelControlError
from ai_trading_system.platform.architecture.supervised_automation import (
    SupervisedAutomationController,
    audit_supervised_orphans,
    cleanup_clean_supervised_worktrees,
    load_supervised_automation_policy,
    validate_supervised_run,
)
from scripts.architecture_arch005_supervised_automation import _run_result_payload

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_SOURCE = PROJECT_ROOT / "config/architecture/arch_005_supervised_automation_policy.yaml"
PILOT_SOURCE = PROJECT_ROOT / "inputs/architecture/arch_005_s4a_supervised_pilot.yaml"


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=root, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "S4A Test")
    _git(root, "config", "user.email", "s4a@example.invalid")
    (root / "README.md").write_text("fixture\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "-m", "fixture")
    return root


def _files(
    tmp_path: Path,
    *,
    engineering_code: str = "print('engineering-pass')",
    research_code: str = "print('research-pass')",
    engineering_timeout: int = 20,
    research_timeout: int = 20,
) -> tuple[Path, Path]:
    policy = yaml.safe_load(POLICY_SOURCE.read_text(encoding="utf-8"))
    commands = {row["command_id"]: row for row in policy["commands"]}
    commands["engineering-focused-validation"]["argv"] = [
        "{python}",
        "-c",
        engineering_code,
    ]
    commands["research-evidence-validation"]["argv"] = [
        "{python}",
        "-c",
        research_code,
    ]
    commands["engineering-focused-validation"]["timeout_seconds"] = engineering_timeout
    commands["research-evidence-validation"]["timeout_seconds"] = research_timeout
    policy_path = tmp_path / "policy.yaml"
    pilot_path = tmp_path / "pilot.yaml"
    policy_path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    pilot_path.write_text(PILOT_SOURCE.read_text(encoding="utf-8"), encoding="utf-8")
    return policy_path, pilot_path


def _controller(
    tmp_path: Path,
    *,
    engineering_code: str = "print('engineering-pass')",
    research_code: str = "print('research-pass')",
    engineering_timeout: int = 20,
    research_timeout: int = 20,
) -> tuple[SupervisedAutomationController, Path, Path, Path]:
    repo = _repo(tmp_path)
    policy, pilot = _files(
        tmp_path,
        engineering_code=engineering_code,
        research_code=research_code,
        engineering_timeout=engineering_timeout,
        research_timeout=research_timeout,
    )
    controller = SupervisedAutomationController(
        project_root=repo,
        runtime_root=tmp_path / "runtime",
        policy_path=policy,
        pilot_path=pilot,
    )
    return controller, repo, policy, pilot


def _run(controller: SupervisedAutomationController) -> Path:
    return controller.run(started_at=datetime.now(UTC))


def _worker_payloads(report_path: Path) -> dict[str, dict[str, object]]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        row["change_id"]: json.loads(
            (report_path.parent / row["artifact_path"]).read_text(encoding="utf-8")
        )
        for row in report["workers"]
    }


def test_reviewed_policy_and_plan_keep_human_gate_closed(tmp_path: Path) -> None:
    controller, _, policy_path, _ = _controller(tmp_path)
    policy = load_supervised_automation_policy(policy_path)

    plan = controller.plan()

    assert policy.status == "REVIEWED_SUPERVISED_BASELINE"
    assert policy.max_workers == 2
    assert policy.source_of_truth == "LEGACY_MARKDOWN_ONLY"
    assert plan["status"] == "PASS"
    assert plan["worker_count"] == 2
    assert plan["dispatch_allowed_by_this_artifact"] is False


def test_two_real_worktrees_execute_and_queue_human_integration(tmp_path: Path) -> None:
    controller, repo, policy, pilot = _controller(tmp_path)

    report_path = _run(controller)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    payloads = _worker_payloads(report_path)
    candidate = json.loads(
        (report_path.parent / report["integration_candidate_path"]).read_text(encoding="utf-8")
    )
    validation = validate_supervised_run(
        report_path, project_root=repo, policy_path=policy, pilot_path=pilot
    )
    orphan = audit_supervised_orphans(report_path, project_root=repo, policy_path=policy)

    assert report["status"] == "PASS"
    assert {row["status"] for row in report["workers"]} == {"PASS"}
    assert len({row["worktree_path"] for row in report["workers"]}) == 2
    assert len({row["branch"] for row in report["workers"]}) == 2
    assert all(payload["final_head"] == report["base_commit"] for payload in payloads.values())
    assert candidate["status"] == "AWAITING_HUMAN_COORDINATOR_APPROVAL"
    assert candidate["merge_allowed"] is False
    assert validation["status"] == "PASS"
    assert orphan["status"] == "PASS"


def test_one_worker_failure_does_not_cancel_independent_worker(tmp_path: Path) -> None:
    controller, _, _, _ = _controller(
        tmp_path,
        engineering_code="import sys; print('expected-failure'); sys.exit(7)",
        research_code="print('independent-pass')",
    )

    report_path = _run(controller)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    payloads = _worker_payloads(report_path)
    candidate = json.loads(
        (report_path.parent / report["integration_candidate_path"]).read_text(encoding="utf-8")
    )

    assert report["status"] == "FAIL"
    assert payloads["arch-005-s4a-engineering-worker"]["status"] == "FAIL"
    assert payloads["arch-005-s4a-engineering-worker"]["exit_code"] == 7
    assert payloads["arch-005-s4a-research-worker"]["status"] == "PASS"
    assert candidate["status"] == "BLOCKED_WORKER_OR_LEASE_FAILURE"
    assert report["lease_replay"]["active_leases"] == []


def test_timeout_is_fail_closed_and_other_worker_completes(tmp_path: Path) -> None:
    controller, _, _, _ = _controller(
        tmp_path,
        engineering_code="import time; time.sleep(10)",
        research_code="print('research-pass')",
        engineering_timeout=1,
    )

    report_path = _run(controller)
    payloads = _worker_payloads(report_path)

    timed_out = payloads["arch-005-s4a-engineering-worker"]
    assert timed_out["status"] == "FAIL"
    assert timed_out["timed_out"] is True
    assert "COMMAND_TIMEOUT" in timed_out["reason_codes"]
    assert payloads["arch-005-s4a-research-worker"]["status"] == "PASS"


def test_unexpected_or_secret_like_worker_path_fails(tmp_path: Path) -> None:
    controller, _, _, _ = _controller(
        tmp_path,
        engineering_code="from pathlib import Path; Path('.env').write_text('x')",
    )

    report_path = _run(controller)
    payload = _worker_payloads(report_path)["arch-005-s4a-engineering-worker"]

    assert payload["status"] == "FAIL"
    assert payload["unexpected_changed_paths"] == [".env"]
    assert "SECRET_LIKE_PATH_DETECTED" in payload["reason_codes"]


def test_tampered_log_fails_static_validation(tmp_path: Path) -> None:
    controller, repo, policy, pilot = _controller(tmp_path)
    report_path = _run(controller)
    payload = next(iter(_worker_payloads(report_path).values()))
    log_path = report_path.parent / payload["stdout_path"]
    log_path.write_text("tampered\n", encoding="utf-8")

    validation = validate_supervised_run(
        report_path, project_root=repo, policy_path=policy, pilot_path=pilot
    )

    assert validation["status"] == "FAIL"
    assert "worker_artifacts" in {
        row["check_id"] for row in validation["checks"] if row["passed"] is False
    }


def test_cleanup_requires_approval_and_refuses_any_dirty_worktree(tmp_path: Path) -> None:
    controller, repo, policy, _ = _controller(tmp_path)
    report_path = _run(controller)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    dirty_worktree = Path(report["workers"][0]["worktree_path"])
    (dirty_worktree / "unreviewed.txt").write_text("keep\n", encoding="utf-8")

    with pytest.raises(ParallelControlError, match="CLEANUP_APPROVAL_REQUIRED"):
        cleanup_clean_supervised_worktrees(
            report_path,
            project_root=repo,
            policy_path=policy,
            coordinator_approved=False,
        )
    with pytest.raises(ParallelControlError, match="CLEANUP_DIRTY"):
        cleanup_clean_supervised_worktrees(
            report_path,
            project_root=repo,
            policy_path=policy,
            coordinator_approved=True,
        )
    assert dirty_worktree.exists()


def test_cleanup_removes_only_clean_worktrees_and_retains_branches(tmp_path: Path) -> None:
    controller, repo, policy, _ = _controller(tmp_path)
    report_path = _run(controller)
    report = json.loads(report_path.read_text(encoding="utf-8"))

    result = cleanup_clean_supervised_worktrees(
        report_path,
        project_root=repo,
        policy_path=policy,
        coordinator_approved=True,
    )

    assert result["status"] == "PASS"
    assert result["force_used"] is False
    assert result["branches_deleted"] == []
    assert all(not Path(row["worktree_path"]).exists() for row in report["workers"])
    assert all(
        _git(repo, "show-ref", "--verify", f"refs/heads/{row['branch']}")
        for row in report["workers"]
    )


def test_policy_rejects_worktree_and_command_escape(tmp_path: Path) -> None:
    policy = yaml.safe_load(POLICY_SOURCE.read_text(encoding="utf-8"))
    policy["workspace"]["worktree_root"] = "../../outside"
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    with pytest.raises(ParallelControlError, match="WORKTREE_ROOT"):
        load_supervised_automation_policy(path)

    policy = yaml.safe_load(POLICY_SOURCE.read_text(encoding="utf-8"))
    policy["commands"][0]["argv"] = ["sh", "-c", "unsafe"]
    path.write_text(yaml.safe_dump(policy, sort_keys=False), encoding="utf-8")
    with pytest.raises(ParallelControlError, match="COMMAND_ARGV"):
        load_supervised_automation_policy(path)


def test_cli_run_payload_propagates_inner_report_failure(tmp_path: Path) -> None:
    report_path = tmp_path / "run" / "supervised_run_report.json"
    report_path.parent.mkdir()
    report_path.write_text(
        json.dumps({"status": "FAIL", "report_id": "failed-report"}),
        encoding="utf-8",
    )

    payload = _run_result_payload(report_path, project_root=tmp_path)

    assert payload["status"] == "FAIL"
    assert payload["report_id"] == "failed-report"
    assert payload["merge_allowed"] is False
