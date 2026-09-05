"""Read-only links from workflow candidates to canonical work and measured outcomes."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_registry,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA = "workflow_improvement_tracking.v1"
PLAN_SCHEMA = "workflow_improvement_plan.v1"
_SHA = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")


def complete_week_window(as_of: date) -> tuple[datetime, datetime]:
    """ISO weeks contain seven days; exclude the current, unfinished UTC week."""
    end = datetime.combine(as_of - timedelta(days=as_of.weekday()), time.min, tzinfo=UTC)
    return end - timedelta(days=7), end


def load_improvement_tracking(
    *,
    project_root: Path,
    policy: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    path = _contained_file(project_root, str(policy["plan_path"]))
    plan = safe_load_yaml_path(path)
    if not isinstance(plan, dict):
        raise ValueError("improvement plan must be a mapping")
    entries = _validate_plan(plan)
    # Reuse the existing canonical validator, including event-chain and index checks.
    # This is a weekly read, not an independent task-state database.
    registry = validate_canonical_registry(project_root=project_root) if entries else None
    tasks: dict[str, dict[str, Any]] = {}
    observations: dict[str, dict[str, Any]] = {}
    for entry in entries:
        task_id = str(entry["task_id"])
        assert registry is not None
        fragment = registry.fragment(task_id)
        cells = fragment["projection"]["legacy_first_eight_cells"]
        events = fragment["events"]
        tasks[task_id] = {
            "task_id": task_id,
            "status": cells[3],
            "next_owner": cells[4],
            "next_step": cells[5],
            "last_event_id": fragment["last_event_id"],
            "fragment_checksum": fragment["fragment_checksum"],
            "last_update": events[-1].get("occurred_at"),
        }
        reference = entry.get("outcome_evidence")
        if reference is not None:
            if not isinstance(reference, Mapping):
                raise ValueError("outcome_evidence must be null or an exact path/hash binding")
            evidence_path = _contained_file(project_root, str(reference.get("path", "")))
            raw = evidence_path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != reference.get("sha256"):
                raise ValueError("outcome evidence checksum mismatch")
            observation = json.loads(raw)
            if not isinstance(observation, dict):
                raise ValueError("outcome observation must be a mapping")
            if (
                observation.get("task_id") != task_id
                or observation.get("candidate_id") != entry["candidate_id"]
            ):
                raise ValueError("outcome observation task/candidate binding mismatch")
            _verify_measurement_artifacts(project_root, observation)
            observations[str(entry["candidate_id"])] = observation
    result = build_improvement_tracking(
        plan=plan,
        candidates=candidates,
        tasks=tasks,
        observations=observations,
        acceptance=policy["outcome_acceptance"],
        main_item_limit=policy["maintenance_main_item_limit"],
    )
    result["source_bindings"] = {
        "plan_path": path.relative_to(project_root.resolve()).as_posix(),
        "plan_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "registry_index_checksum": None if registry is None else registry.index["index_checksum"],
    }
    result["payload_sha256"] = _digest(result)
    return result


def build_improvement_tracking(
    *,
    plan: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    observations: Mapping[str, Mapping[str, Any]],
    acceptance: Mapping[str, Any],
    main_item_limit: int,
) -> dict[str, Any]:
    entries = _validate_plan(plan)
    if type(main_item_limit) is not int or main_item_limit < 1:
        raise ValueError("maintenance item limit must be a positive integer")
    current = {str(row["candidate_id"]): row for row in candidates}
    rows = []
    for entry in entries:
        candidate_id, task_id = str(entry["candidate_id"]), str(entry["task_id"])
        task = tasks.get(task_id)
        if not task or task.get("task_id") != task_id:
            raise ValueError(f"improvement task is not resolved from canonical registry: {task_id}")
        observation = observations.get(candidate_id)
        implementation_sha = entry.get("reviewed_implementation_sha")
        if observation is not None:
            if (
                not isinstance(implementation_sha, str)
                or _COMMIT.fullmatch(implementation_sha) is None
            ):
                raise ValueError("outcome requires a reviewed implementation commit in the plan")
            if (
                observation.get("task_id") != task_id
                or observation.get("candidate_id") != candidate_id
            ):
                raise ValueError("outcome snapshot task/candidate mismatch")
            if any(
                sample.get("code_sha") != implementation_sha
                for sample in observation.get("after", [])
            ):
                raise ValueError("outcome after commit differs from the reviewed implementation")
        outcome = evaluate_outcome(observation, acceptance)
        rows.append(
            {
                "candidate_id": candidate_id,
                "root_cause_id": entry["root_cause_id"],
                "task_id": task_id,
                "owner_decision_ref": entry["owner_decision_ref"],
                "engineering": dict(task),
                "observed_this_window": candidate_id in current,
                "next_action": entry["next_action"],
                "review_after": entry["review_after"],
                "outcome": outcome,
                "observation_snapshot": None if observation is None else dict(observation),
                "acceptance_snapshot": dict(acceptance),
                "reviewed_implementation_sha": implementation_sha,
            }
        )
    linked = {row["candidate_id"] for row in rows}
    # Several symptoms may be attached to one task; capacity counts work, not symptoms.
    active_tasks = sorted(
        {
            row["task_id"]
            for row in rows
            if row["engineering"]["status"] in {"IN_PROGRESS", "VALIDATING"}
        }
    )
    result = {
        "schema_version": SCHEMA,
        "plan_version": plan["plan_version"],
        "rows": rows,
        "unlinked_candidate_ids": sorted(set(current) - linked),
        "active_task_ids": active_tasks,
        "main_item_limit": main_item_limit,
        "capacity_status": (
            "WITHIN_LIMIT" if len(active_tasks) <= main_item_limit else "REVIEW_CAPACITY"
        ),
        "automatic_execution_allowed": False,
        "task_register_mutation_allowed": False,
        "causal_claim_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    result["payload_sha256"] = _digest(result)
    return result


def evaluate_outcome(
    observation: Mapping[str, Any] | None, acceptance: Mapping[str, Any]
) -> dict[str, Any]:
    """Assess explicitly comparable measurements without turning task DONE into savings."""
    if observation is None:
        return {
            "status": "OBSERVING",
            "reason": "尚无绑定的 before/after 实验；工程完成不代表效率收益。",
        }
    if observation.get("schema_version") != "workflow_improvement_observation.v1":
        raise ValueError("unsupported workflow outcome observation schema")
    before, after = observation.get("before"), observation.get("after")
    if not isinstance(before, Mapping) or not isinstance(after, list):
        raise ValueError("outcome requires one before and a list of after measurements")
    minimum = acceptance.get("minimum_after_samples")
    if type(minimum) is not int or minimum < 2:
        raise ValueError("minimum_after_samples must preserve two-sample acceptance")
    relative = _finite_number(acceptance.get("minimum_relative_reduction"))
    absolute = _finite_number(acceptance.get("minimum_absolute_reduction_seconds"))
    if relative > 1:
        raise ValueError("relative reduction must be at most one")
    samples = [before, *after]
    for sample in samples:
        if not isinstance(sample, Mapping):
            raise ValueError("measurement must be a mapping")
        _finite_number(sample.get("elapsed_seconds"))
        for key, pattern in (
            ("code_sha", _COMMIT),
            ("artifact_sha256", _SHA),
            ("execution_identity_sha256", _SHA),
            ("workload_sha256", _SHA),
            ("environment_sha256", _SHA),
        ):
            if not isinstance(sample.get(key), str) or pattern.fullmatch(sample[key]) is None:
                raise ValueError(f"measurement {key} must bind an exact identity")
        _timestamp(sample.get("ended_at_utc"))
    if any(sample.get("validation_status") != "PASS" for sample in samples):
        return {"status": "REGRESSION", "reason": "实验存在未通过正确性验证的测量。"}
    if len(after) < minimum:
        return {"status": "INSUFFICIENT_EVIDENCE", "reason": "after 测量数量不足。"}
    if any(
        len({sample[key] for sample in samples}) != len(samples)
        for key in ("artifact_sha256", "execution_identity_sha256")
    ):
        return {"status": "INSUFFICIENT_EVIDENCE", "reason": "重复实验 artifact 不能充当独立测量。"}
    if any(
        sample["workload_sha256"] != before["workload_sha256"]
        or sample["environment_sha256"] != before["environment_sha256"]
        for sample in after
    ):
        return {
            "status": "INSUFFICIENT_EVIDENCE",
            "reason": "工作负载或环境不同，不能宣称效率收益。",
        }
    if (
        len({sample["code_sha"] for sample in after}) != 1
        or after[0]["code_sha"] == before["code_sha"]
    ):
        return {
            "status": "INSUFFICIENT_EVIDENCE",
            "reason": "after 必须绑定同一新实现，不能混合候选。",
        }
    if any(
        _timestamp(sample["ended_at_utc"]) <= _timestamp(before["ended_at_utc"]) for sample in after
    ):
        return {"status": "INSUFFICIENT_EVIDENCE", "reason": "实验顺序不符合 before/after。"}
    baseline = _finite_number(before["elapsed_seconds"])
    slowest = max(_finite_number(sample["elapsed_seconds"]) for sample in after)
    saved = baseline - slowest
    ratio = saved / baseline if baseline else None
    status = "INSUFFICIENT_BENEFIT"
    if saved < 0:
        status = "REGRESSION"
    elif ratio is not None and ratio >= relative and saved >= absolute:
        status = "BENEFIT_SUPPORTED"
    return {
        "status": status,
        "before_seconds": baseline,
        "slowest_after_seconds": slowest,
        "saved_seconds": saved,
        "relative_reduction": ratio,
        "after_sample_count": len(after),
        "measurement_scope": "EXPLICIT_COMPARABLE_EXPERIMENT_ONLY",
        "after_code_sha": after[0]["code_sha"],
        "reason": "按同工作负载/环境的较慢 after 评估；不能外推为整个项目或账单节省。",
    }


def validate_improvement_tracking(payload: Mapping[str, Any]) -> list[str]:
    errors = []
    if payload.get("schema_version") != SCHEMA:
        errors.append("IMPROVEMENT_SCHEMA")
    if payload.get("payload_sha256") != _digest(payload):
        errors.append("IMPROVEMENT_PAYLOAD_DRIFT")
    if any(
        payload.get(key) is not False
        for key in (
            "automatic_execution_allowed",
            "task_register_mutation_allowed",
            "causal_claim_allowed",
        )
    ):
        errors.append("IMPROVEMENT_EXECUTION_BOUNDARY")
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        errors.append("IMPROVEMENT_PRODUCTION_BOUNDARY")
    rows = payload.get("rows")
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        errors.append("IMPROVEMENT_ROWS")
    else:
        ids = [str(row.get("candidate_id")) for row in rows]
        if len(ids) != len(set(ids)):
            errors.append("IMPROVEMENT_DUPLICATE_LINK")
        for row in rows:
            engineering = row.get("engineering")
            if not isinstance(engineering, Mapping) or row.get("task_id") != engineering.get(
                "task_id"
            ):
                errors.append("IMPROVEMENT_TASK_BINDING")
            try:
                observation = row.get("observation_snapshot")
                if row.get("outcome") != evaluate_outcome(observation, row["acceptance_snapshot"]):
                    errors.append("IMPROVEMENT_OUTCOME_REPLAY")
                if observation is not None:
                    implementation_sha = row.get("reviewed_implementation_sha")
                    if (
                        not isinstance(implementation_sha, str)
                        or _COMMIT.fullmatch(implementation_sha) is None
                    ):
                        errors.append("IMPROVEMENT_COMMIT_BINDING")
                    if (
                        observation.get("task_id") != row["task_id"]
                        or observation.get("candidate_id") != row["candidate_id"]
                    ):
                        errors.append("IMPROVEMENT_OBSERVATION_BINDING")
                    if any(
                        sample.get("code_sha") != implementation_sha
                        for sample in observation.get("after", [])
                    ):
                        errors.append("IMPROVEMENT_COMMIT_BINDING")
            except (KeyError, TypeError, ValueError, AttributeError):
                errors.append("IMPROVEMENT_OUTCOME_REPLAY")
    return sorted(set(errors))


def _validate_plan(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("automatic_execution_allowed") is not False
    ):
        raise ValueError("improvement plan must use the reviewed read-only schema")
    for key in ("plan_version", "owner", "owner_decision_ref", "review_condition"):
        if not isinstance(plan.get(key), str) or not plan[key].strip():
            raise ValueError(f"improvement plan missing {key}")
    raw = plan.get("entries")
    if not isinstance(raw, list):
        raise ValueError("improvement plan entries must be a list")
    rows = []
    ids = set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("improvement entry must be a mapping")
        for key in (
            "candidate_id",
            "rule_id",
            "scope",
            "root_cause_id",
            "task_id",
            "owner_decision_ref",
            "next_action",
            "review_after",
        ):
            if not isinstance(item.get(key), str) or not item[key].strip():
                raise ValueError(f"improvement entry missing {key}")
        identity = {"rule_id": item["rule_id"], "scope": item["scope"]}
        expected = "workflow-opt-" + _digest(identity)[:20]
        if item["candidate_id"] != expected or expected in ids:
            raise ValueError("candidate identity mismatch or duplicate improvement entry")
        date.fromisoformat(item["review_after"])
        ids.add(expected)
        rows.append(item)
    return rows


def _contained_file(root: Path, relative: str) -> Path:
    candidate = Path(relative)
    if not relative or candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("improvement evidence path must be repository-relative")
    path = root / candidate
    resolved = path.resolve()
    resolved.relative_to(root.resolve())
    if not resolved.is_file() or any(parent.is_symlink() for parent in (path, *path.parents)):
        raise ValueError("improvement input must be a regular contained file")
    return resolved


def _verify_measurement_artifacts(root: Path, observation: Mapping[str, Any]) -> None:
    """Do not accept user-entered durations or merely well-formed checksum strings."""
    before, after = observation.get("before"), observation.get("after")
    if not isinstance(before, Mapping) or not isinstance(after, list):
        raise ValueError("observation measurements are incomplete")
    for index, sample in enumerate([before, *after]):
        if not isinstance(sample, Mapping):
            raise ValueError("measurement must be an object")
        path = _contained_file(root, str(sample.get("artifact_path", "")))
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != sample.get("artifact_sha256"):
            raise ValueError("measurement artifact bytes have changed")
        source = json.loads(raw)
        if not isinstance(source, Mapping):
            raise ValueError("measurement artifact must contain a validation summary")
        for field, source_field in (
            ("elapsed_seconds", "elapsed_seconds"),
            ("code_sha", "git_commit"),
            ("validation_status", "status"),
            ("ended_at_utc", "ended_at_utc"),
        ):
            if sample.get(field) != source.get(source_field):
                raise ValueError(f"measurement {field} is not backed by its artifact")
        exit_code = source.get("exit_code")
        if (
            source.get("print_only") is not False
            or type(exit_code) is not int
            or source.get("status") not in {"PASS", "FAIL"}
            or (source.get("status") == "PASS") != (exit_code == 0)
        ):
            raise ValueError("outcome requires a consistent actually executed validation")
        if index:
            provenance = source.get("validation_provenance")
            if (
                not isinstance(provenance, Mapping)
                or provenance.get("task_id") != observation.get("task_id")
                or source.get("validation_provenance_status") != "PASS"
            ):
                raise ValueError("after validation provenance is not bound to the improvement task")
        command, inputs, environment = (
            source.get("command"),
            source.get("input_checksums"),
            source.get("environment_summary"),
        )
        if (
            not isinstance(command, list)
            or not command
            or not isinstance(inputs, dict)
            or not inputs
            or not isinstance(environment, dict)
            or not environment
        ):
            raise ValueError("measurement lacks a verifiable workload/environment identity")
        if any(
            not isinstance(value, str) or _SHA.fullmatch(value) is None for value in inputs.values()
        ):
            raise ValueError("unhashed inputs cannot establish a comparable workload")
        if sample.get("workload_sha256") != _digest(
            {"command": command, "input_checksums": inputs}
        ):
            raise ValueError("measurement workload identity differs from validation artifact")
        if sample.get("environment_sha256") != _digest(environment):
            raise ValueError("measurement environment identity differs from validation artifact")
        if sample.get("execution_identity_sha256") != _execution_identity(source):
            raise ValueError("measurement execution identity differs from validation artifact")


def _execution_identity(source: Mapping[str, Any]) -> str:
    """One executed run remains one sample across paths and JSON serializations."""
    started = _timestamp(source.get("started_at_utc"))
    ended = _timestamp(source.get("ended_at_utc"))
    if ended <= started:
        raise ValueError("measurement execution interval must be positive")
    return _digest(
        {
            "started_at_utc": started.isoformat(),
            "ended_at_utc": ended.isoformat(),
            "git_commit": source.get("git_commit"),
            "command": source.get("command"),
            "input_checksums": source.get("input_checksums"),
            "environment_summary": source.get("environment_summary"),
        }
    )


def _finite_number(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError("measurement must be a finite non-negative number")
    return float(value)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("measurement time must be an aware ISO timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("measurement timestamp timezone is required")
    return result.astimezone(UTC)


def _digest(value: Mapping[str, Any]) -> str:
    body = {key: item for key, item in value.items() if key != "payload_sha256"}
    return hashlib.sha256(
        json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()
