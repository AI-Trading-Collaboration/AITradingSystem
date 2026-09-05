from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands import workflow_health_reports as commands
from ai_trading_system.reports import workflow_health as health
from ai_trading_system.reports import workflow_improvement as improvement

TASK_ID = "DEVX-013_CONTINUOUS_IMPROVEMENT_EXECUTION_AND_OUTCOME_LOOP_V1"
CANDIDATE_ID = "workflow-opt-e9ff5d6b5ed565a901f4"
ACCEPTANCE = {
    "minimum_after_samples": 2,
    "minimum_relative_reduction": 0.2,
    "minimum_absolute_reduction_seconds": 30,
}


def _plan() -> dict[str, Any]:
    return {
        "schema_version": "workflow_improvement_plan.v1",
        "plan_version": "test@1",
        "owner": "owner",
        "owner_decision_ref": "owner:test",
        "review_condition": "after observations",
        "automatic_execution_allowed": False,
        "entries": [
            {
                "candidate_id": CANDIDATE_ID,
                "rule_id": "failed_full_runtime",
                "scope": "repository",
                "task_id": TASK_ID,
                "root_cause_id": "late_authority",
                "owner_decision_ref": "owner:test",
                "next_action": "前置同类失败",
                "review_after": "2026-10-05",
                "outcome_evidence": None,
            }
        ],
    }


def _observation() -> dict[str, Any]:
    def sample(code: str, artifact: str, seconds: float, timestamp: str) -> dict[str, Any]:
        return {
            "code_sha": code * 40,
            "artifact_sha256": artifact * 64,
            "execution_identity_sha256": hashlib.sha256(f"{code}:{timestamp}".encode()).hexdigest(),
            "elapsed_seconds": seconds,
            "workload_sha256": "d" * 64,
            "environment_sha256": "e" * 64,
            "ended_at_utc": timestamp,
            "validation_status": "PASS",
        }

    return {
        "schema_version": "workflow_improvement_observation.v1",
        "task_id": TASK_ID,
        "candidate_id": CANDIDATE_ID,
        "before": sample("a", "1", 200, "2026-09-01T00:00:00Z"),
        "after": [
            sample("b", "2", 140, "2026-09-02T00:00:00Z"),
            sample("b", "3", 150, "2026-09-03T00:00:00Z"),
        ],
    }


def test_done_task_does_not_claim_savings_or_disappear_when_symptom_is_absent() -> None:
    result = improvement.build_improvement_tracking(
        plan=_plan(),
        candidates=[],
        tasks={TASK_ID: {"task_id": TASK_ID, "status": "DONE"}},
        observations={},
        acceptance=ACCEPTANCE,
        main_item_limit=1,
    )
    row = result["rows"][0]
    assert row["engineering"]["status"] == "DONE"
    assert row["outcome"]["status"] == "OBSERVING"
    assert row["observed_this_window"] is False
    assert result["automatic_execution_allowed"] is False
    assert not improvement.validate_improvement_tracking(result)
    row["outcome"]["status"] = "BENEFIT_SUPPORTED"
    assert "IMPROVEMENT_PAYLOAD_DRIFT" in improvement.validate_improvement_tracking(result)
    result["payload_sha256"] = improvement._digest(result)
    assert "IMPROVEMENT_OUTCOME_REPLAY" in improvement.validate_improvement_tracking(result)


def test_unregistered_task_and_wrong_candidate_identity_are_rejected() -> None:
    with pytest.raises(ValueError, match="canonical registry"):
        improvement.build_improvement_tracking(
            plan=_plan(),
            candidates=[],
            tasks={},
            observations={},
            acceptance=ACCEPTANCE,
            main_item_limit=1,
        )
    plan = _plan()
    plan["entries"][0]["scope"] = "different-root"
    with pytest.raises(ValueError, match="identity"):
        improvement.build_improvement_tracking(
            plan=plan,
            candidates=[],
            tasks={},
            observations={},
            acceptance=ACCEPTANCE,
            main_item_limit=1,
        )


def test_outcome_uses_slower_after_and_both_reviewed_thresholds() -> None:
    observation = _observation()
    result = improvement.evaluate_outcome(observation, ACCEPTANCE)
    assert result["status"] == "BENEFIT_SUPPORTED"
    assert result["saved_seconds"] == 50
    observation["after"][1]["elapsed_seconds"] = 190
    assert improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "INSUFFICIENT_BENEFIT"
    observation["after"][1]["elapsed_seconds"] = 210
    assert improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "REGRESSION"


@pytest.mark.parametrize(
    "mutation", ["workload", "environment", "code", "duplicate", "time", "one_sample"]
)
def test_incomparable_or_reused_evidence_never_claims_benefit(mutation: str) -> None:
    observation = _observation()
    if mutation == "workload":
        observation["after"][0]["workload_sha256"] = "f" * 64
    elif mutation == "environment":
        observation["after"][0]["environment_sha256"] = "f" * 64
    elif mutation == "code":
        observation["after"][0]["code_sha"] = "c" * 40
    elif mutation == "duplicate":
        observation["after"][1] = copy.deepcopy(observation["after"][0])
    elif mutation == "time":
        observation["after"][0]["ended_at_utc"] = "2026-08-31T00:00:00Z"
    else:
        observation["after"].pop()
    assert (
        improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "INSUFFICIENT_EVIDENCE"
    )


@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "100"])
def test_invalid_measurement_is_rejected(value: object) -> None:
    observation = _observation()
    observation["before"]["elapsed_seconds"] = value
    with pytest.raises(ValueError):
        improvement.evaluate_outcome(observation, ACCEPTANCE)


def test_correctness_failure_overrides_faster_runtime() -> None:
    observation = _observation()
    observation["after"][0]["validation_status"] = "FAIL"
    assert improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "REGRESSION"


def test_other_implementation_cannot_be_credited_to_the_linked_task() -> None:
    plan = _plan()
    plan["entries"][0]["reviewed_implementation_sha"] = "f" * 40
    with pytest.raises(ValueError, match="reviewed implementation"):
        improvement.build_improvement_tracking(
            plan=plan,
            candidates=[],
            tasks={TASK_ID: {"task_id": TASK_ID, "status": "DONE"}},
            observations={CANDIDATE_ID: _observation()},
            acceptance=ACCEPTANCE,
            main_item_limit=1,
        )


def test_complete_week_handles_iso_year_boundary() -> None:
    start, end = improvement.complete_week_window(date(2027, 1, 1))
    assert start == datetime(2026, 12, 21, tzinfo=UTC)
    assert end == datetime(2026, 12, 28, tzinfo=UTC)
    assert improvement.complete_week_window(date(2026, 12, 28)) == (start, end)


def test_measurement_must_match_retained_executed_artifact(tmp_path: Path) -> None:
    observation = _observation()
    for index, sample in enumerate([observation["before"], *observation["after"]]):
        source = {
            "git_commit": sample["code_sha"],
            "status": "PASS",
            "exit_code": 0,
            "print_only": False,
            "elapsed_seconds": sample["elapsed_seconds"],
            "ended_at_utc": sample["ended_at_utc"],
            "command": [
                "python",
                "-m",
                "pytest",
                "-n",
                "16",
                "--dist",
                "loadfile",
                "tests/test_example.py",
            ],
            "input_checksums": {"fixture.json": "1" * 64},
            "environment_summary": {"python_version": "3.14.4"},
            "started_at_utc": (
                datetime.fromisoformat(sample["ended_at_utc"].replace("Z", "+00:00"))
                - timedelta(seconds=sample["elapsed_seconds"])
            ).isoformat(),
            "validation_provenance": {"task_id": TASK_ID},
            "validation_provenance_status": "PASS",
        }
        path = tmp_path / f"run{index}.json"
        raw = json.dumps(source).encode()
        path.write_bytes(raw)
        sample["artifact_path"] = path.name
        sample["artifact_sha256"] = hashlib.sha256(raw).hexdigest()
        sample["workload_sha256"] = improvement._digest(
            {"command": source["command"], "input_checksums": source["input_checksums"]}
        )
        sample["environment_sha256"] = improvement._digest(source["environment_summary"])
        sample["execution_identity_sha256"] = improvement._execution_identity(source)
    improvement._verify_measurement_artifacts(tmp_path, observation)
    # Different bytes/paths for one real run cannot supply independent after samples.
    original_after = copy.deepcopy(observation["after"])
    repeated = copy.deepcopy(observation["after"][0])
    repeated_path = tmp_path / "reformatted_run.json"
    repeated_raw = json.dumps(
        json.loads((tmp_path / repeated["artifact_path"]).read_bytes()), indent=2
    ).encode()
    repeated_path.write_bytes(repeated_raw)
    repeated["artifact_path"] = repeated_path.name
    repeated["artifact_sha256"] = hashlib.sha256(repeated_raw).hexdigest()
    observation["after"][1] = repeated
    improvement._verify_measurement_artifacts(tmp_path, observation)
    assert (
        improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "INSUFFICIENT_EVIDENCE"
    )
    observation["after"] = original_after
    # An actual failed execution is retained as a regression, without claiming benefit.
    sample = observation["after"][0]
    failed_path = tmp_path / sample["artifact_path"]
    source = json.loads(failed_path.read_bytes())
    source.update(status="FAIL", exit_code=1)
    failed_raw = json.dumps(source).encode()
    failed_path.write_bytes(failed_raw)
    sample.update(validation_status="FAIL", artifact_sha256=hashlib.sha256(failed_raw).hexdigest())
    improvement._verify_measurement_artifacts(tmp_path, observation)
    assert improvement.evaluate_outcome(observation, ACCEPTANCE)["status"] == "REGRESSION"
    observation["after"][0]["elapsed_seconds"] = 10
    with pytest.raises(ValueError, match="not backed"):
        improvement._verify_measurement_artifacts(tmp_path, observation)


def _v2_policy(tmp_path: Path) -> Path:
    policy = yaml.safe_load(health.DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))
    policy["continuous_improvement"]["plan_path"] = "plan.yaml"
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(policy), encoding="utf-8")
    plan = _plan()
    plan["entries"] = []
    (tmp_path / "plan.yaml").write_text(yaml.safe_dump(plan), encoding="utf-8")
    return path


def test_v2_report_integrates_explicit_empty_token_root_and_rejects_tamper(tmp_path: Path) -> None:
    policy = _v2_policy(tmp_path)
    report, candidates = health.build_workflow_health_payloads(
        as_of=date(2026, 9, 6),
        generated_at=datetime(2026, 9, 6, tzinfo=UTC),
        project_root=tmp_path,
        policy_path=policy,
        git_commit_records=[],
        token_log_roots=[],
    )
    assert report["schema_version"] == "workflow_health_report.v2"
    assert report["window"]["start_inclusive_utc"] == "2026-08-24T00:00:00+00:00"
    assert report["window"]["end_exclusive_utc"] == "2026-08-31T00:00:00+00:00"
    assert report["metrics"]["token_usage"]["status"] == "UNAVAILABLE"
    assert (
        health.validate_workflow_health_payloads(report, candidates)["validation_status"]
        == "PASS_WITH_WARNINGS"
    )
    assert "改进执行与收益观察" in health.render_workflow_health_markdown(report)
    report["metrics"]["token_usage"]["totals"]["input_tokens"] = 1
    assert (
        health.validate_workflow_health_payloads(report, candidates)["validation_status"] == "FAIL"
    )


def test_new_policy_does_not_reuse_an_older_policy_bundle(tmp_path: Path) -> None:
    # Exercise the actual discovery/validation path, not just a policy equality helper.
    legacy = yaml.safe_load(health.DEFAULT_POLICY_PATH.read_text(encoding="utf-8"))
    legacy.pop("continuous_improvement")
    legacy["policy_version"] = "DEVX-012@1.1.0"
    policy = tmp_path / "policy.yaml"
    policy.write_text(yaml.safe_dump(legacy), encoding="utf-8")
    report, candidates = health.build_workflow_health_payloads(
        as_of=date(2026, 8, 31), project_root=tmp_path, policy_path=policy, git_commit_records=[]
    )
    output = tmp_path / "outputs/reports"
    output.mkdir(parents=True)
    report_date = date(2026, 8, 31)
    validation = health.validate_workflow_health_payloads(report, candidates)
    health.write_workflow_health_json(
        report, health.default_workflow_health_json_path(output, report_date)
    )
    health.write_workflow_health_markdown(
        report, health.default_workflow_health_markdown_path(output, report_date)
    )
    health.write_workflow_candidates_json(
        candidates, health.default_workflow_candidates_json_path(output, report_date)
    )
    health.write_workflow_health_validation_json(
        validation, health.default_workflow_health_validation_json_path(output, report_date)
    )
    health.write_workflow_health_validation_markdown(
        validation, health.default_workflow_health_validation_markdown_path(output, report_date)
    )
    assert (
        health.latest_current_week_validated_bundle(reports_dir=output, as_of=date(2026, 8, 31))
        is not None
    )
    assert (
        health.latest_current_week_validated_bundle(
            reports_dir=output, as_of=date(2026, 8, 31), expected_policy_sha256="f" * 64
        )
        is None
    )
    # Renaming a valid historical bundle cannot make its old observation current.
    for source in list(output.iterdir()):
        target = output / source.name.replace("2026-08-31", "2026-09-07")
        target.write_bytes(source.read_bytes())
    assert (
        health.latest_current_week_validated_bundle(reports_dir=output, as_of=date(2026, 9, 7))
        is None
    )


def test_v2_schema_cannot_be_downgraded_to_bypass_integrity(tmp_path: Path) -> None:
    policy = _v2_policy(tmp_path)
    report, candidates = health.build_workflow_health_payloads(
        as_of=date(2026, 9, 6),
        project_root=tmp_path,
        policy_path=policy,
        git_commit_records=[],
        token_log_roots=[],
    )
    report["schema_version"] = "workflow_health_report.v1"
    assert (
        health.validate_workflow_health_payloads(report, candidates)["validation_status"] == "FAIL"
    )


def test_v2_existing_cycle_generates_then_reuses_with_extended_governed_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policy = _v2_policy(tmp_path)
    paths_seen: list[Path] = []

    def identity(
        *, project_root: Path, governed_paths: tuple[Path, ...]
    ) -> tuple[dict[str, Any], tuple[str, ...]]:
        assert project_root == tmp_path
        paths_seen.extend(governed_paths)
        return {"head": "a" * 40}, ()

    monkeypatch.setattr(commands, "resolve_workflow_health_checkout_identity", identity)
    reports = tmp_path / "outputs/reports"
    args = [
        "reports",
        "ensure-workflow-health",
        "--as-of",
        "2026-09-06",
        "--project-root",
        str(tmp_path),
        "--policy-path",
        str(policy),
        "--reports-dir",
        str(reports),
        "--receipt-dir",
        str(tmp_path / "receipts"),
        "--token-log-root",
        str(tmp_path / "missing-logs"),
    ]
    first = CliRunner().invoke(app, args)
    assert first.exit_code == 0, first.output
    report_path = reports / "workflow_health_2026-09-06.json"
    original = report_path.read_bytes()
    assert json.loads(original)["schema_version"] == "workflow_health_report.v2"
    second = CliRunner().invoke(app, args)
    assert second.exit_code == 0, second.output
    assert "ALREADY_CURRENT" in second.output
    assert report_path.read_bytes() == original
    assert tmp_path / "plan.yaml" in paths_seen
    assert tmp_path / "src/ai_trading_system/reports/workflow_token_usage.py" in paths_seen
    assert tmp_path / "src/ai_trading_system/reports/workflow_improvement.py" in paths_seen
    changed_source = CliRunner().invoke(app, [*args[:-1], str(tmp_path / "different-logs")])
    assert changed_source.exit_code == 1
    assert "ALREADY_CURRENT" not in changed_source.output
    assert report_path.read_bytes() == original
