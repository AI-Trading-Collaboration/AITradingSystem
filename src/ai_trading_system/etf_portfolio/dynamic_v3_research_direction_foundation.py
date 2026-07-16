from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_micro_search_foundation as micro
from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "research_direction_foundation_v1.yaml"
)
DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR = micro.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR
DEFAULT_NEXT_RESEARCH_DIRECTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "next_research_direction"
)
DEFAULT_OWNER_RESEARCH_ROADMAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_research_roadmap"

DIRECTION_INPUT_SCHEMA = "next_research_direction_input_snapshot.v2"
ROADMAP_INPUT_SCHEMA = "owner_research_roadmap_input_snapshot.v2"

DIRECTION_VIEWS = (
    "next_research_direction_manifest.json",
    "next_research_direction_decision.json",
    "next_task_plan.json",
    "next_research_direction_report.md",
    "reader_brief_section.md",
)
ROADMAP_VIEWS = (
    "owner_research_roadmap_manifest.json",
    "owner_roadmap_summary.json",
    "owner_roadmap_checklist.md",
    "owner_research_roadmap_report.md",
    "reader_brief_section.md",
)
DIRECTION_FILES = (*DIRECTION_VIEWS, "next_research_direction_input_snapshot.json")
ROADMAP_FILES = (*ROADMAP_VIEWS, "owner_research_roadmap_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_texts = foundation._texts
_text = foundation._text
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_artifact_dir = foundation._artifact_dir
_read_json = foundation._read_json
_write_latest_pointer = foundation._write_latest_pointer
_validation_payload = foundation._validation_payload


class DynamicV3ResearchDirectionFoundationError(ValueError):
    """Raised when TRADING-324～325 planning evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3ResearchDirectionFoundationError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _aware_time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3ResearchDirectionFoundationError(f"{field} must be an ISO datetime") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_research_direction_foundation_policy.v1",
        "research direction policy schema mismatch",
    )
    metadata = _mapping(payload.get("policy_metadata"))
    for field in (
        "policy_version",
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
    ):
        _require(bool(_text(metadata.get(field))), f"policy metadata missing: {field}")
    _require(metadata.get("status") in {"pilot_baseline", "reviewed", "active"}, "policy status")

    direction = _mapping(payload.get("direction"))
    mapping = _mapping(direction.get("shift_to_decision"))
    required_shifts = {
        "CONTINUE_MICRO_SEARCH",
        "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS",
        "SHIFT_TO_CANDIDATE_QUALITY_FILTER",
        "REVIEW_GATE_POLICY",
        "DEFER",
        "DEFER_AND_BUILD_DATED_EVIDENCE",
    }
    _require(set(mapping) == required_shifts, "shift-to-decision policy incomplete")
    _require(
        direction.get("unknown_shift_behavior") == "FAIL_CLOSED",
        "unknown shift must fail closed",
    )
    _require(
        mapping.get(direction.get("insufficient_source_shift"))
        == direction.get("insufficient_decision"),
        "insufficient evidence decision mapping mismatch",
    )
    _require(direction.get("insufficient_failure_source") == "INCONCLUSIVE", "failure policy")
    _require(direction.get("insufficient_confidence") == "LOW", "confidence policy")
    _require(direction.get("task_status") == "PROPOSED_OWNER_REVIEW", "task status policy")
    _require(
        direction.get("historical_downstream_evidence_role")
        == "HISTORICAL_CONTEXT_ONLY_NOT_CURRENT_ATTRIBUTION_PROOF",
        "historical downstream evidence role",
    )
    _require(
        set(_texts(direction.get("historical_downstream_task_ranges")))
        == {"TRADING-326_to_335", "TRADING-336_to_345"},
        "historical downstream task ranges",
    )

    task_plan = _mapping(payload.get("task_plan"))
    _require(set(task_plan) == required_shifts, "task plan policy incomplete")
    for shift in required_shifts:
        tasks = _records(task_plan.get(shift))
        _require(bool(tasks), f"task plan missing for {shift}")
        for task in tasks:
            _require(bool(_text(task.get("task_id"))), "task_id missing")
            _require(bool(_text(task.get("title"))), "task title missing")
            _require(bool(_text(task.get("acceptance"))), "task acceptance missing")

    roadmap = _mapping(payload.get("roadmap"))
    _require(roadmap.get("task_state_mutation_allowed") is False, "task mutation policy")
    _require(
        roadmap.get("automatic_implementation_allowed") is False,
        "automatic implementation policy",
    )
    _require(
        roadmap.get("roadmap_decision_status") == "OWNER_REVIEW_REQUIRED",
        "roadmap decision status",
    )

    safety = _mapping(payload.get("safety"))
    for key, expected in st.EXPERIMENT_FACTORY_SAFETY.items():
        _require(safety.get(key) == expected, f"safety policy mismatch: {key}")
    _require(safety.get("owner_review_required") is True, "owner review required")
    return payload


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _validate_binding(binding: Mapping[str, Any], *, kind: str, names: Sequence[str]) -> None:
    _require(binding.get("kind") == kind, f"artifact binding kind mismatch: {kind}")
    source_dir = _binding_root(binding).resolve()
    _require(source_dir.name == _binding_id(binding), f"{kind} binding id/path mismatch")
    files = _mapping(binding.get("files"))
    _require(set(files) == set(names), f"{kind} binding file set mismatch")
    for name in names:
        bound_path = Path(_text(_mapping(files.get(name)).get("path"))).resolve()
        _require(
            bound_path == (source_dir / name).resolve(),
            f"{kind} binding path mismatch: {name}",
        )
    foundation._validate_artifact_binding(binding, kind=kind)


def _binding_root(binding: Mapping[str, Any]) -> Path:
    return Path(_text(binding.get("source_dir")))


def _binding_id(binding: Mapping[str, Any]) -> str:
    return _text(binding.get("artifact_id"))


def _chronology(generated: datetime, *payloads: Mapping[str, Any]) -> None:
    for payload in payloads:
        _require(
            _aware_time(payload.get("generated_at"), "source.generated_at") <= generated,
            "source artifact is newer than generated_at",
        )


def _validated_attribution(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=micro.validate_signal_vs_parameter_attribution_artifact,
        validator_key="attribution_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "attribution source validation failed")
    payload = micro.signal_vs_parameter_attribution_report_payload(
        attribution_id=artifact_id,
        output_dir=root,
    )
    _require(_text(payload.get("attribution_id")) == artifact_id, "attribution id mismatch")
    return payload


def _direction_decision(
    attribution: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    shift = _mapping(attribution.get("recommended_research_shift"))
    failure = _mapping(attribution.get("failure_source_attribution"))
    source_shift = _text(shift.get("recommended_shift"))
    rules = _mapping(policy.get("direction"))
    shift_to_decision = _mapping(rules.get("shift_to_decision"))
    _require(source_shift in shift_to_decision, f"unmapped recommended shift: {source_shift}")
    decision = _text(shift_to_decision.get(source_shift))
    evidence_status = _text(failure.get("evidence_status"))
    _require(
        evidence_status in {"SUFFICIENT", "INSUFFICIENT_DATA"},
        f"unsupported source evidence status: {evidence_status}",
    )
    insufficient_shift = _text(rules.get("insufficient_source_shift"))
    if evidence_status == "INSUFFICIENT_DATA":
        _require(
            source_shift == insufficient_shift,
            "insufficient evidence must defer and build dated evidence",
        )
        _require(
            failure.get("failure_source") == rules.get("insufficient_failure_source"),
            "insufficient failure source mismatch",
        )
        _require(
            failure.get("confidence") == rules.get("insufficient_confidence"),
            "insufficient confidence mismatch",
        )
        _require(decision == rules.get("insufficient_decision"), "insufficient decision mismatch")
    else:
        _require(
            source_shift != insufficient_shift,
            "dated-evidence defer shift requires insufficient evidence",
        )
    historical_ranges = _texts(rules.get("historical_downstream_task_ranges"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "direction_id": "",
        "source_attribution_id": attribution.get("attribution_id"),
        "source_evidence_status": evidence_status,
        "source_failure_source": failure.get("failure_source"),
        "source_recommended_shift": source_shift,
        "decision": decision,
        "confidence": failure.get("confidence"),
        "reason": _texts(failure.get("evidence")),
        "continue_parameter_search": decision == "CONTINUE_MICRO_SEARCH_V5",
        "dated_evidence_required": source_shift == rules.get("insufficient_source_shift"),
        "historical_downstream_task_ranges": historical_ranges,
        "historical_downstream_evidence_role": rules.get("historical_downstream_evidence_role"),
        "research_direction_change_authorized": False,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _task_plan(decision: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    source_shift = _text(decision.get("source_recommended_shift"))
    task_rows = _records(_mapping(policy.get("task_plan")).get(source_shift))
    task_status = _mapping(policy.get("direction")).get("task_status")
    tasks = []
    for row in task_rows:
        tasks.append(
            {
                "task_id": row.get("task_id"),
                "title": row.get("title"),
                "status": task_status,
                "acceptance": row.get("acceptance"),
                "implemented": False,
                "auto_register": False,
                "task_state_mutation_allowed": False,
                "owner_review_required": True,
                "policy_version": _policy_version(policy),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_recommended_shift": source_shift,
        "tasks": tasks,
        "automatic_implementation_allowed": False,
        "task_state_mutation_allowed": False,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _render_direction_reader(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Next Research Direction",
            "",
            f"- source_evidence_status: {decision.get('source_evidence_status')}",
            f"- source_recommended_shift: {decision.get('source_recommended_shift')}",
            f"- decision: {decision.get('decision')}",
            f"- confidence: {decision.get('confidence')}",
            f"- dated_evidence_required: {decision.get('dated_evidence_required')}",
            "- research_direction_change_authorized: false",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def _render_direction_report(
    manifest: Mapping[str, Any], decision: Mapping[str, Any], task_plan: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Next Research Direction {manifest.get('direction_id')}",
            "",
            f"- source_attribution_id：{decision.get('source_attribution_id')}",
            f"- source_evidence_status：{decision.get('source_evidence_status')}",
            f"- source_failure_source：{decision.get('source_failure_source')}",
            f"- source_recommended_shift：{decision.get('source_recommended_shift')}",
            f"- decision：{decision.get('decision')}",
            f"- confidence：{decision.get('confidence')}",
            f"- dated_evidence_required：{decision.get('dated_evidence_required')}",
            f"- historical_downstream_evidence_role："
            f"{decision.get('historical_downstream_evidence_role')}",
            "",
            "## Proposed Owner-Review Tasks",
            *[
                f"- {row.get('task_id')}: {row.get('status')} / {row.get('acceptance')}"
                for row in _records(task_plan.get("tasks"))
            ],
            "",
        ]
    )


def _direction_material(
    *,
    root: Path,
    direction_id: str,
    attribution: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    decision = _direction_decision(attribution, policy)
    decision["direction_id"] = direction_id
    task_plan = _task_plan(decision, policy)
    insufficient = decision.get("source_evidence_status") == "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_next_research_direction_manifest",
        "direction_id": direction_id,
        "attribution_id": attribution.get("attribution_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if insufficient else "PASS",
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "research_direction_policy_version": _policy_version(policy),
        "source_evidence_status": decision.get("source_evidence_status"),
        "next_research_direction_manifest_path": str(root / DIRECTION_VIEWS[0]),
        "next_research_direction_decision_path": str(root / DIRECTION_VIEWS[1]),
        "next_task_plan_path": str(root / DIRECTION_VIEWS[2]),
        "next_research_direction_report_path": str(root / DIRECTION_VIEWS[3]),
        "reader_brief_section_path": str(root / DIRECTION_VIEWS[4]),
        "next_research_direction_input_snapshot_path": str(
            root / "next_research_direction_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = _render_direction_report(manifest, decision, task_plan)
    reader = _render_direction_reader(decision)
    views = {
        DIRECTION_VIEWS[0]: foundation._json_bytes(manifest),
        DIRECTION_VIEWS[1]: foundation._json_bytes(decision),
        DIRECTION_VIEWS[2]: foundation._json_bytes(task_plan),
        DIRECTION_VIEWS[3]: foundation._text_file_bytes(report),
        DIRECTION_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def run_next_research_direction(
    *,
    attribution_id: str,
    attribution_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    attribution = _validated_attribution(attribution_id, attribution_dir)
    policy = _policy(policy_path)
    _chronology(generated, attribution)
    direction_id = _stable_id("next-research-direction", attribution_id, generated.isoformat())
    root = _unique_dir(output_dir / direction_id)
    manifest, views = _direction_material(
        root=root,
        direction_id=root.name,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": DIRECTION_INPUT_SCHEMA,
        "direction_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "attribution_source": _binding(
            kind="signal_vs_parameter_attribution",
            artifact_id=attribution_id,
            root=attribution_dir / attribution_id,
            names=micro.ATTRIBUTION_FILES,
        ),
        "source_recommended_shift": _mapping(attribution.get("recommended_research_shift")).get(
            "recommended_shift"
        ),
        "view_hashes": foundation._view_hashes(root, DIRECTION_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "next_research_direction_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_next_research_direction", root.name, root / DIRECTION_VIEWS[0])
    return {
        "direction_id": root.name,
        "direction_dir": root,
        "manifest": manifest,
        "next_research_direction_decision": _read_json(root / DIRECTION_VIEWS[1]),
        "next_task_plan": _read_json(root / DIRECTION_VIEWS[2]),
        "reader_brief_section": (root / DIRECTION_VIEWS[4]).read_text(encoding="utf-8"),
    }


def next_research_direction_report_payload(
    *,
    direction_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=direction_id,
        latest_pointer="latest_next_research_direction",
        latest=latest,
        output_dir=output_dir,
        required_name=DIRECTION_VIEWS[0],
    )
    return {
        **_read_json(root / DIRECTION_VIEWS[0]),
        "next_research_direction_decision": _read_json(root / DIRECTION_VIEWS[1]),
        "next_task_plan": _read_json(root / DIRECTION_VIEWS[2]),
        "reader_brief_section": (root / DIRECTION_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "next_research_direction_input_snapshot.json"),
        "direction_dir": str(root),
    }


def _rebuild_direction(root: Path, direction_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "next_research_direction_input_snapshot.json")
    _require(snapshot.get("schema_version") == DIRECTION_INPUT_SCHEMA, "direction schema")
    _require(
        foundation._payload_experiment_safe(snapshot),
        "direction snapshot safety fields invalid",
    )
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    source = _mapping(snapshot.get("attribution_source"))
    _validate_binding(
        source,
        kind="signal_vs_parameter_attribution",
        names=micro.ATTRIBUTION_FILES,
    )
    attribution = _validated_attribution(_binding_id(source), _binding_root(source).parent)
    _require(
        _text(_mapping(attribution.get("recommended_research_shift")).get("recommended_shift"))
        == _text(snapshot.get("source_recommended_shift")),
        "direction source shift mismatch",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, attribution)
    _, expected = _direction_material(
        root=root,
        direction_id=direction_id,
        attribution=attribution,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_next_research_direction_artifact(
    *,
    direction_id: str,
    output_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
) -> dict[str, Any]:
    root = output_dir / direction_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="next_research_direction_input_snapshot.json",
        schema=DIRECTION_INPUT_SCHEMA,
        id_key="direction_id",
        artifact_id=direction_id,
        view_names=DIRECTION_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_next_research_direction_validation", direction_id, checks
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_next_research_direction_validation",
        artifact_id=direction_id,
        checks=checks,
        rebuild=lambda: _rebuild_direction(root, direction_id),
    )


def _validated_direction(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_next_research_direction_artifact,
        validator_key="direction_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "direction source validation failed")
    payload = next_research_direction_report_payload(direction_id=artifact_id, output_dir=root)
    _require(_text(payload.get("direction_id")) == artifact_id, "direction id mismatch")
    return payload


def _roadmap_summary(
    direction: Mapping[str, Any], policy: Mapping[str, Any], roadmap_id: str
) -> dict[str, Any]:
    decision = _mapping(direction.get("next_research_direction_decision"))
    rules = _mapping(policy.get("roadmap"))
    insufficient = decision.get("dated_evidence_required") is True
    _require(
        decision.get("research_direction_change_authorized") is False,
        "direction unexpectedly authorized",
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "roadmap_id": roadmap_id,
        "source_direction_id": direction.get("direction_id"),
        "source_attribution_id": decision.get("source_attribution_id"),
        "current_phase": (
            rules.get("insufficient_current_phase")
            if insufficient
            else "post_batch_search_diagnosis"
        ),
        "parameter_search_status": (
            rules.get("insufficient_parameter_search_status") if insufficient else "OWNER_REVIEW"
        ),
        "best_current_observation_candidate": (
            rules.get("insufficient_best_candidate") if insufficient else "OWNER_REVIEW_REQUIRED"
        ),
        "next_research_direction": decision.get("decision"),
        "source_evidence_status": decision.get("source_evidence_status"),
        "dated_evidence_required": decision.get("dated_evidence_required"),
        "recommended_owner_action": (
            rules.get("insufficient_owner_action")
            if insufficient
            else "review_validated_direction_before_any_implementation"
        ),
        "roadmap_decision_status": rules.get("roadmap_decision_status"),
        "historical_downstream_task_ranges": decision.get("historical_downstream_task_ranges"),
        "historical_downstream_evidence_role": decision.get("historical_downstream_evidence_role"),
        "task_state_mutation_allowed": False,
        "automatic_implementation_allowed": False,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _render_roadmap_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Research Roadmap",
            "",
            f"- current_phase: {summary.get('current_phase')}",
            f"- source_evidence_status: {summary.get('source_evidence_status')}",
            f"- parameter_search_status: {summary.get('parameter_search_status')}",
            f"- next_research_direction: {summary.get('next_research_direction')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            "- roadmap_decision_status: OWNER_REVIEW_REQUIRED",
            "- safety: no official target / no broker / no production",
            "",
        ]
    )


def _render_roadmap_checklist(summary: Mapping[str, Any], direction: Mapping[str, Any]) -> str:
    decision = _mapping(direction.get("next_research_direction_decision"))
    if summary.get("dated_evidence_required") is True:
        actions = [
            "- register or update the dated evidence requirement before implementation",
            "- build PIT dated signal events and exact candidate-method weight observations",
            "- rerun CX1 through CX3 validators before changing the research direction",
            "- treat TRADING-326～345 as historical context, not current attribution proof",
        ]
    else:
        actions = [
            "- review the validated direction and proposed task plan before implementation",
            "- confirm historical downstream task status before reusing or reopening research",
            "- keep any official gate-policy change under separately registered owner review",
        ]
    return "\n".join(
        [
            f"# Owner Research Roadmap Checklist {summary.get('roadmap_id')}",
            "",
            f"- source_direction_id: {summary.get('source_direction_id')}",
            f"- source_attribution_id: {summary.get('source_attribution_id')}",
            f"- source_evidence_status: {summary.get('source_evidence_status')}",
            f"- next_research_direction: {summary.get('next_research_direction')}",
            f"- decision_confidence: {decision.get('confidence')}",
            *actions,
            "- require explicit owner review before task registration or implementation",
            "- confirm broker_action_allowed=false and production_effect=none",
            "",
        ]
    )


def _render_roadmap_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], checklist: str
) -> str:
    return "\n".join(
        [
            f"# Owner Research Roadmap {manifest.get('roadmap_id')}",
            "",
            f"- source_direction_id：{summary.get('source_direction_id')}",
            f"- source_attribution_id：{summary.get('source_attribution_id')}",
            f"- current_phase：{summary.get('current_phase')}",
            f"- source_evidence_status：{summary.get('source_evidence_status')}",
            f"- parameter_search_status：{summary.get('parameter_search_status')}",
            f"- best_current_observation_candidate："
            f"{summary.get('best_current_observation_candidate')}",
            f"- next_research_direction：{summary.get('next_research_direction')}",
            f"- recommended_owner_action：{summary.get('recommended_owner_action')}",
            f"- historical_downstream_evidence_role："
            f"{summary.get('historical_downstream_evidence_role')}",
            f"- roadmap_decision_status：{summary.get('roadmap_decision_status')}",
            "",
            "## Checklist",
            checklist,
            "",
        ]
    )


def _roadmap_material(
    *,
    root: Path,
    roadmap_id: str,
    direction: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    summary = _roadmap_summary(direction, policy, roadmap_id)
    checklist = _render_roadmap_checklist(summary, direction)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_research_roadmap_manifest",
        "roadmap_id": roadmap_id,
        "direction_id": direction.get("direction_id"),
        "attribution_id": summary.get("source_attribution_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if summary.get("source_evidence_status") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "market_regime": direction.get("market_regime", "ai_after_chatgpt"),
        "research_direction_policy_version": _policy_version(policy),
        "owner_research_roadmap_manifest_path": str(root / ROADMAP_VIEWS[0]),
        "owner_roadmap_summary_path": str(root / ROADMAP_VIEWS[1]),
        "owner_roadmap_checklist_path": str(root / ROADMAP_VIEWS[2]),
        "owner_research_roadmap_report_path": str(root / ROADMAP_VIEWS[3]),
        "reader_brief_section_path": str(root / ROADMAP_VIEWS[4]),
        "owner_research_roadmap_input_snapshot_path": str(
            root / "owner_research_roadmap_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = _render_roadmap_report(manifest, summary, checklist)
    reader = _render_roadmap_reader(summary)
    views = {
        ROADMAP_VIEWS[0]: foundation._json_bytes(manifest),
        ROADMAP_VIEWS[1]: foundation._json_bytes(summary),
        ROADMAP_VIEWS[2]: foundation._text_file_bytes(checklist),
        ROADMAP_VIEWS[3]: foundation._text_file_bytes(report),
        ROADMAP_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def update_owner_research_roadmap(
    *,
    direction_id: str,
    direction_dir: Path = DEFAULT_NEXT_RESEARCH_DIRECTION_DIR,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    direction = _validated_direction(direction_id, direction_dir)
    policy = _policy(policy_path)
    _chronology(generated, direction)
    roadmap_id = _stable_id("owner-research-roadmap", direction_id, generated.isoformat())
    root = _unique_dir(output_dir / roadmap_id)
    manifest, views = _roadmap_material(
        root=root,
        roadmap_id=root.name,
        direction=direction,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": ROADMAP_INPUT_SCHEMA,
        "roadmap_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "direction_source": _binding(
            kind="next_research_direction",
            artifact_id=direction_id,
            root=direction_dir / direction_id,
            names=DIRECTION_FILES,
        ),
        "source_attribution_id": manifest.get("attribution_id"),
        "view_hashes": foundation._view_hashes(root, ROADMAP_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "owner_research_roadmap_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_owner_research_roadmap", root.name, root / ROADMAP_VIEWS[0])
    return {
        "roadmap_id": root.name,
        "roadmap_dir": root,
        "manifest": manifest,
        "owner_roadmap_summary": _read_json(root / ROADMAP_VIEWS[1]),
        "owner_roadmap_checklist": (root / ROADMAP_VIEWS[2]).read_text(encoding="utf-8"),
        "reader_brief_section": (root / ROADMAP_VIEWS[4]).read_text(encoding="utf-8"),
    }


def owner_research_roadmap_report_payload(
    *,
    roadmap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=roadmap_id,
        latest_pointer="latest_owner_research_roadmap",
        latest=latest,
        output_dir=output_dir,
        required_name=ROADMAP_VIEWS[0],
    )
    return {
        **_read_json(root / ROADMAP_VIEWS[0]),
        "owner_roadmap_summary": _read_json(root / ROADMAP_VIEWS[1]),
        "owner_roadmap_checklist": (root / ROADMAP_VIEWS[2]).read_text(encoding="utf-8"),
        "reader_brief_section": (root / ROADMAP_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "owner_research_roadmap_input_snapshot.json"),
        "roadmap_dir": str(root),
    }


def _rebuild_roadmap(root: Path, roadmap_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "owner_research_roadmap_input_snapshot.json")
    _require(snapshot.get("schema_version") == ROADMAP_INPUT_SCHEMA, "roadmap schema")
    _require(
        foundation._payload_experiment_safe(snapshot),
        "roadmap snapshot safety fields invalid",
    )
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    source = _mapping(snapshot.get("direction_source"))
    _validate_binding(source, kind="next_research_direction", names=DIRECTION_FILES)
    direction = _validated_direction(_binding_id(source), _binding_root(source).parent)
    _require(
        _text(
            _mapping(direction.get("next_research_direction_decision")).get("source_attribution_id")
        )
        == _text(snapshot.get("source_attribution_id")),
        "roadmap attribution lineage mismatch",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, direction)
    _, expected = _roadmap_material(
        root=root,
        roadmap_id=roadmap_id,
        direction=direction,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_owner_research_roadmap_artifact(
    *,
    roadmap_id: str,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_ROADMAP_DIR,
) -> dict[str, Any]:
    root = output_dir / roadmap_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="owner_research_roadmap_input_snapshot.json",
        schema=ROADMAP_INPUT_SCHEMA,
        id_key="roadmap_id",
        artifact_id=roadmap_id,
        view_names=ROADMAP_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_owner_research_roadmap_validation", roadmap_id, checks
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_owner_research_roadmap_validation",
        artifact_id=roadmap_id,
        checks=checks,
        rebuild=lambda: _rebuild_roadmap(root, roadmap_id),
    )


__all__ = [
    "DEFAULT_NEXT_RESEARCH_DIRECTION_DIR",
    "DEFAULT_OWNER_RESEARCH_ROADMAP_DIR",
    "DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH",
    "DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR",
    "DIRECTION_FILES",
    "ROADMAP_FILES",
    "next_research_direction_report_payload",
    "owner_research_roadmap_report_payload",
    "run_next_research_direction",
    "update_owner_research_roadmap",
    "validate_next_research_direction_artifact",
    "validate_owner_research_roadmap_artifact",
]
