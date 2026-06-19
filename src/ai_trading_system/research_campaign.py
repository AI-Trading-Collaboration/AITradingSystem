from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_CAMPAIGN_ROOT = PROJECT_ROOT / "data" / "research_campaigns"
DEFAULT_CAMPAIGN_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_campaigns"
DEFAULT_MODULE_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "module_capability_registry.yaml"
)
DEFAULT_GATE_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "research_gate_policies.yaml"
DEFAULT_WINDOW_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "research_window_policy.yaml"
DEFAULT_MIGRATION_PATH = PROJECT_ROOT / "config" / "research" / "campaign_migrations.yaml"
DEFAULT_COMPATIBILITY_PATH = (
    PROJECT_ROOT / "config" / "research" / "task_specific_runner_compatibility.yaml"
)

WORKFLOW_STAGES = [
    "DRAFT",
    "SCOPE_READY",
    "INPUT_PRECHECK",
    "MINI_DIAGNOSTIC",
    "ATTRIBUTION",
    "TARGETED_EVIDENCE",
    "FULL_DIAGNOSTIC",
    "INTERACTION",
    "GATE_READY",
    "OWNER_REVIEW",
    "ARCHIVED",
]

DECISION_OUTCOMES = [
    "NOT_EVALUATED",
    "PASS",
    "MIXED",
    "NEEDS_MORE_EVIDENCE",
    "PROMISING",
    "NARROW_ROLE",
    "RETURN_TO_DESIGN",
    "WEAK",
    "REJECTED",
    "BLOCKED",
    "OWNER_OVERRIDE_REQUIRED",
]

EVIDENCE_CATEGORIES = [
    "INPUT_VALIDITY",
    "SIGNAL_COVERAGE",
    "SIGNAL_DIRECTION",
    "TRIGGER_BEHAVIOR",
    "PORTFOLIO_EFFECT",
    "DRAWDOWN_PROTECTION",
    "REENTRY_BEHAVIOR",
    "TURNOVER_COST",
    "BENCHMARK_RELATIVE",
    "WINDOW_STABILITY",
    "INTERACTION_EFFECT",
    "SAFETY",
]

CONTROL_ONLY_DATA_QUALITY_STATUS = "NOT_REQUIRED_CONTROL_PLANE_ONLY"
RESEARCH_ONLY_SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "official_target_weights": False,
    "paper_shadow_allowed": False,
    "broker_effect": "none",
    "order_effect": "none",
    "production_effect": "none",
}
FORBIDDEN_WEIGHT_OUTPUTS = {
    "target_weight",
    "official_target_weight",
    "official_target_weights",
    "executed_weight",
    "hypothetical_weight",
    "broker_order",
    "order_ticket",
}
FORBIDDEN_EVALUATION_MUTATIONS = {
    "modified_feature",
    "modified_signal",
    "modified_target_path",
    "strategy_parameter_update",
    "allocator_rerun",
}
RESTRICTED_MORE_EVIDENCE_OUTCOMES = [
    "NARROW_ROLE",
    "RETURN_TO_DESIGN",
    "WEAK",
    "OWNER_OVERRIDE_REQUIRED",
]


class ResearchCampaignError(ValueError):
    pass


class StopRuleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str


class HypothesisSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    statement: str
    expected_gain: list[str] = Field(default_factory=list)
    expected_failure_modes: list[str] = Field(default_factory=list)


class ModuleGraphSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    baseline: str
    modules: list[str] = Field(default_factory=list)
    allowed_mechanisms: list[str] = Field(default_factory=list)
    forbidden_mechanisms: list[str] = Field(default_factory=list)
    allowed_interaction_order: int = 2

    @field_validator("allowed_interaction_order")
    @classmethod
    def _interaction_order_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("allowed_interaction_order must be >= 1")
        return value


class WindowPolicySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    development_catalog: str
    diagnostic_catalog: str
    holdout_catalog: str
    holdout_access: str = "FINAL_GATE_ONLY"
    full_catalog: str | None = None

    @field_validator("holdout_access")
    @classmethod
    def _holdout_access_known(cls, value: str) -> str:
        allowed = {"FINAL_GATE_ONLY", "OWNER_AUTHORIZATION_REQUIRED", "FORBIDDEN"}
        if value not in allowed:
            raise ValueError(f"holdout_access must be one of {sorted(allowed)}")
        return value


class EvidenceBudgetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_mini_rounds: int = 1
    max_targeted_rounds: int = 2
    max_window_expansions: int = 1
    max_redesign_rounds: int = 1
    max_needs_more_evidence_occurrences: int = 2
    time_budget: str | None = None
    compute_budget: str | None = None

    @model_validator(mode="after")
    def _non_negative(self) -> EvidenceBudgetSpec:
        for field_name in (
            "max_mini_rounds",
            "max_targeted_rounds",
            "max_window_expansions",
            "max_redesign_rounds",
            "max_needs_more_evidence_occurrences",
        ):
            if getattr(self, field_name) < 0:
                raise ValueError(f"{field_name} must be >= 0")
        return self


class SafetySpec(BaseModel):
    model_config = ConfigDict(extra="allow")

    research_only: bool = True
    manual_review_only: bool = True
    official_target_weights: bool = False
    paper_shadow_allowed: bool = False
    broker_effect: str = "none"
    order_effect: str = "none"
    production_effect: str = "none"


class CampaignSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str
    campaign_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    program_id: str
    title: str
    market_regime: str = "ai_after_chatgpt"
    requested_date_range: str | None = None
    hypothesis: HypothesisSpec
    module_graph: ModuleGraphSpec
    window_policy: WindowPolicySpec
    scorecard_policy: str
    evidence_budget: EvidenceBudgetSpec = Field(default_factory=EvidenceBudgetSpec)
    stop_rules: list[StopRuleSpec] = Field(default_factory=list)
    safety: SafetySpec = Field(default_factory=SafetySpec)
    owner_authorized_holdout: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModuleCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    module_id: str
    module_type: str
    input_contract: list[str] = Field(default_factory=list)
    output_contract: list[str] = Field(default_factory=list)
    allowed_layers: list[str] = Field(default_factory=list)
    allowed_mechanisms: list[str] = Field(default_factory=list)
    required_features: list[str] = Field(default_factory=list)
    required_signals: list[str] = Field(default_factory=list)
    supported_window_types: list[str] = Field(default_factory=list)
    compatible_modules: list[str] = Field(default_factory=list)
    incompatible_modules: list[str] = Field(default_factory=list)
    forbidden_outputs: list[str] = Field(default_factory=list)
    version: str
    approved_for_campaign_modules: bool = True
    requires_gate_approval_for_interaction: bool = False
    interaction_alias: str | None = None


class EvidenceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    campaign_id: str
    run_id: str
    stage: str
    category: str
    metric_name: str
    value: Any = None
    baseline_value: Any = None
    delta: float | None = None
    direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] = "unknown"
    window_id: str | None = None
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"] = "INFO"
    confidence: Literal["high", "medium", "low", "unknown"] = "unknown"
    source_artifact_id: str
    reason_codes: list[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: _now_iso())


class BudgetUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mini_rounds: int = 0
    targeted_rounds: int = 0
    window_expansions: int = 0
    redesign_rounds: int = 0
    needs_more_evidence_occurrences: int = 0
    holdout_accesses: int = 0
    owner_overrides: int = 0


class CampaignState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    campaign_id: str
    program_id: str
    title: str
    current_stage: str = "DRAFT"
    current_outcome: str = "NOT_EVALUATED"
    reason_codes: list[str] = Field(default_factory=list)
    evidence_budget_used: BudgetUsed = Field(default_factory=BudgetUsed)
    source_artifacts: list[str] = Field(default_factory=list)
    data_quality_status: str = CONTROL_ONLY_DATA_QUALITY_STATUS
    safety_boundary: dict[str, Any] = Field(
        default_factory=lambda: dict(RESEARCH_ONLY_SAFETY_BOUNDARY)
    )
    migrated_from_legacy: bool = False
    legacy_status: str | None = None
    created_at: str = Field(default_factory=lambda: _now_iso())
    updated_at: str = Field(default_factory=lambda: _now_iso())
    stage_history: list[dict[str, Any]] = Field(default_factory=list)


def load_campaign_spec(path: Path) -> CampaignSpec:
    try:
        raw = _read_yaml(path)
        return CampaignSpec.model_validate(raw)
    except ValidationError as exc:
        raise ResearchCampaignError(f"Campaign spec schema invalid: {exc}") from exc


def build_campaign_validation_payload(
    *,
    spec: CampaignSpec,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
) -> dict[str, Any]:
    module_registry = load_module_registry(module_registry_path)
    gate_policy = _read_yaml(gate_policy_path)
    window_policy = _read_yaml(window_policy_path)
    issues: list[dict[str, Any]] = []

    issues.extend(_validate_safety(spec))
    issues.extend(_validate_module_graph(spec, module_registry))
    issues.extend(_validate_window_policy(spec, window_policy))
    issues.extend(_validate_gate_policy(spec, gate_policy))
    issues.extend(_validate_budget(spec))

    status = "PASS" if not [issue for issue in issues if issue["severity"] == "error"] else "FAIL"
    warning_count = len([issue for issue in issues if issue["severity"] == "warning"])
    if status == "PASS" and warning_count:
        status = "PASS_WITH_WARNINGS"
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_validation",
        "campaign_id": spec.campaign_id,
        "program_id": spec.program_id,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "validation_status": status,
        "status": status,
        "issues": issues,
        "summary": {
            "issue_count": len(issues),
            "error_count": len([issue for issue in issues if issue["severity"] == "error"]),
            "warning_count": warning_count,
            "module_count": len(spec.module_graph.modules),
            "stop_rule_count": len(spec.stop_rules),
        },
        "data_quality_status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "safety_boundary": dict(RESEARCH_ONLY_SAFETY_BOUNDARY),
    }


def initialize_campaign(
    *,
    spec_path: Path,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
    migration_path: Path = DEFAULT_MIGRATION_PATH,
    force: bool = False,
) -> dict[str, Any]:
    spec = load_campaign_spec(spec_path)
    validation = build_campaign_validation_payload(
        spec=spec,
        module_registry_path=module_registry_path,
        gate_policy_path=gate_policy_path,
        window_policy_path=window_policy_path,
    )
    if validation["validation_status"] == "FAIL":
        raise ResearchCampaignError(
            f"Campaign validation failed: {validation['summary']['error_count']} errors"
        )

    directory = campaign_directory(spec.campaign_id, campaign_root)
    if directory.exists() and not force:
        raise ResearchCampaignError(f"Campaign already exists: {directory}")
    directory.mkdir(parents=True, exist_ok=True)

    migration = load_campaign_migrations(migration_path).get(spec.campaign_id)
    state = _initial_state_from_spec(spec, migration)
    _append_transition(
        directory,
        {
            "transition_id": f"{spec.campaign_id}-init",
            "campaign_id": spec.campaign_id,
            "from_stage": None,
            "to_stage": state.current_stage,
            "outcome": state.current_outcome,
            "reason_codes": state.reason_codes,
            "created_at": _now_iso(),
            "source": "campaign_init",
        },
    )
    _write_json(directory / "spec.json", spec.model_dump(mode="json"))
    _write_json(directory / "validation.json", validation)
    write_campaign_state(state, directory)

    evidence_records: list[EvidenceRecord] = []
    if migration:
        for raw_record in migration.get("evidence_records", []):
            record = EvidenceRecord.model_validate(
                {
                    **raw_record,
                    "campaign_id": spec.campaign_id,
                    "created_at": raw_record.get("created_at") or _now_iso(),
                }
            )
            evidence_records.append(record)
    write_evidence_records(evidence_records, directory, replace=True)
    manifest = build_reproducibility_manifest(
        spec_path=spec_path,
        spec=spec,
        config_paths=[
            module_registry_path,
            gate_policy_path,
            window_policy_path,
            migration_path,
        ],
        output_paths=[
            directory / "spec.json",
            directory / "validation.json",
            directory / "state.json",
            directory / "evidence.jsonl",
        ],
        transition_source="campaign_init",
    )
    _write_json(directory / "reproducibility_manifest.json", manifest)

    return {
        "campaign_id": spec.campaign_id,
        "campaign_dir": str(directory),
        "validation_status": validation["validation_status"],
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "migrated_from_legacy": state.migrated_from_legacy,
        "evidence_record_count": len(evidence_records),
        "reproducibility_manifest": str(directory / "reproducibility_manifest.json"),
        "safety_boundary": state.safety_boundary,
    }


def campaign_plan(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_payload = evaluate_gate(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_policy_path=gate_policy_path,
    )
    budget_payload = evaluate_evidence_budget(spec, state)
    actions = build_next_action_plan(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_payload=gate_payload,
    )
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_plan",
        "campaign_id": campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "reason_codes": state.reason_codes,
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_limits": spec.evidence_budget.model_dump(mode="json"),
        "budget_status": budget_payload["budget_status"],
        "stop_rule_proximity": budget_payload["stop_rule_proximity"],
        "allowed_next_actions": actions["allowed_next_actions"],
        "blocked_actions": actions["blocked_actions"],
        "required_owner_actions": actions["required_owner_actions"],
        "next_recommended_stage": actions["next_recommended_stage"],
        "gate_preview": gate_payload,
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
    }


def run_campaign_stage(
    *,
    campaign_id: str,
    requested_stage: str = "next",
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Path = DEFAULT_WINDOW_POLICY_PATH,
) -> dict[str, Any]:
    directory = campaign_directory(campaign_id, campaign_root)
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    target_stage = resolve_requested_stage(state.current_stage, requested_stage)
    transition = validate_stage_transition(state.current_stage, target_stage)
    if not transition["allowed"]:
        raise ResearchCampaignError(transition["reason"])

    run_id = f"{campaign_id}-{target_stage.lower()}-{_compact_time()}"
    outcome = "PASS"
    reason_codes: list[str] = []
    generated_evidence: list[EvidenceRecord] = []
    result_payload: dict[str, Any]

    if target_stage == "SCOPE_READY":
        validation = build_campaign_validation_payload(
            spec=spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
        outcome = "PASS" if validation["validation_status"] != "FAIL" else "BLOCKED"
        reason_codes = [
            issue["issue_id"]
            for issue in validation["issues"]
            if issue["severity"] == "error"
        ]
        result_payload = validation
    elif target_stage == "INPUT_PRECHECK":
        validation = build_campaign_validation_payload(
            spec=spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
        outcome = "PASS" if validation["validation_status"] != "FAIL" else "BLOCKED"
        reason_codes = [
            issue["issue_id"]
            for issue in validation["issues"]
            if issue["severity"] == "error"
        ]
        generated_evidence.append(
            _control_evidence(
                campaign_id=campaign_id,
                run_id=run_id,
                stage=target_stage,
                category="INPUT_VALIDITY",
                metric_name="campaign_precheck_validation",
                value=validation["validation_status"],
                status="PASS" if outcome == "PASS" else "BLOCKED",
                source_artifact_id="campaign_validation",
                reason_codes=reason_codes,
            )
        )
        result_payload = validation
    elif target_stage in {"MINI_DIAGNOSTIC", "ATTRIBUTION", "TARGETED_EVIDENCE", "FULL_DIAGNOSTIC"}:
        imported_stage_evidence = [record for record in evidence if record.stage == target_stage]
        if imported_stage_evidence:
            outcome = _outcome_from_imported_stage_evidence(imported_stage_evidence, state)
            reason_codes = sorted(
                {
                    code
                    for record in imported_stage_evidence
                    for code in record.reason_codes
                }
                | set(state.reason_codes)
            )
            result_payload = {
                "status": outcome,
                "source": "imported_audited_evidence",
                "evidence_record_count": len(imported_stage_evidence),
                "source_artifact_ids": sorted(
                    {record.source_artifact_id for record in imported_stage_evidence}
                ),
                "production_effect": "none",
            }
        else:
            outcome = "BLOCKED"
            reason_codes = ["STAGE_ADAPTER_NOT_CONFIGURED"]
            generated_evidence.append(
                _control_evidence(
                    campaign_id=campaign_id,
                    run_id=run_id,
                    stage=target_stage,
                    category="SAFETY",
                    metric_name="stage_execution_adapter",
                    value="not_configured",
                    status="BLOCKED",
                    source_artifact_id="campaign_stage_runner",
                    reason_codes=reason_codes,
                )
            )
            result_payload = {
                "status": "BLOCKED",
                "blocking_reason": "STAGE_ADAPTER_NOT_CONFIGURED",
                "intended_solution": (
                    "configure an explicit stage adapter or import audited evidence"
                ),
                "production_effect": "none",
            }
    elif target_stage == "INTERACTION":
        matrix = plan_experiment_matrix(spec=spec, module_registry_path=module_registry_path)
        generated_evidence.append(
            _control_evidence(
                campaign_id=campaign_id,
                run_id=run_id,
                stage=target_stage,
                category="INTERACTION_EFFECT",
                metric_name="experiment_matrix_count",
                value=len(matrix["experiment_matrix"]),
                status="PASS",
                source_artifact_id="experiment_design_planner",
                reason_codes=[],
            )
        )
        result_payload = matrix
    elif target_stage == "GATE_READY":
        result_payload = evaluate_gate(
            spec=spec,
            state=state,
            evidence=evidence,
            gate_policy_path=gate_policy_path,
        )
        outcome = str(result_payload["decision_outcome"])
        reason_codes = list(result_payload["reason_codes"])
    elif target_stage == "OWNER_REVIEW":
        result_payload = build_owner_packet(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            output_root=DEFAULT_CAMPAIGN_OUTPUT_ROOT,
            gate_policy_path=gate_policy_path,
        )
        outcome = "PASS"
    else:
        raise ResearchCampaignError(f"Stage cannot be run directly: {target_stage}")

    if target_stage == "MINI_DIAGNOSTIC":
        state.evidence_budget_used.mini_rounds += 1
    if target_stage == "TARGETED_EVIDENCE":
        state.evidence_budget_used.targeted_rounds += 1
    if outcome == "NEEDS_MORE_EVIDENCE":
        state.evidence_budget_used.needs_more_evidence_occurrences += 1

    state.current_stage = target_stage
    state.current_outcome = outcome
    state.reason_codes = reason_codes
    state.updated_at = _now_iso()
    state.stage_history.append(
        {
            "run_id": run_id,
            "stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
        }
    )
    write_campaign_state(state, directory)
    append_evidence_records(generated_evidence, directory)
    _append_transition(
        directory,
        {
            "transition_id": run_id,
            "campaign_id": campaign_id,
            "from_stage": transition["from_stage"],
            "to_stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "source": "campaign_stage_runner",
        },
    )
    _append_jsonl(
        directory / "runs.jsonl",
        {
            "run_id": run_id,
            "campaign_id": campaign_id,
            "stage": target_stage,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "created_at": state.updated_at,
            "result": result_payload,
            "production_effect": "none",
        },
    )
    return {
        "run_id": run_id,
        "campaign_id": campaign_id,
        "stage": target_stage,
        "outcome": outcome,
        "reason_codes": reason_codes,
        "generated_evidence_count": len(generated_evidence),
        "result": result_payload,
        "safety_boundary": state.safety_boundary,
    }


def build_status_payload(
    *,
    campaign_id: str,
    detailed: bool = False,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    compatibility_path: Path = DEFAULT_COMPATIBILITY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    plan = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "research_campaign_status",
        "campaign_id": campaign_id,
        "title": spec.title,
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "reason_codes": state.reason_codes,
        "evidence_record_count": len(evidence),
        "allowed_next_actions": plan["allowed_next_actions"],
        "blocked_actions": plan["blocked_actions"],
        "required_owner_actions": plan["required_owner_actions"],
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }
    if detailed:
        payload["evidence_budget_used"] = state.evidence_budget_used.model_dump(mode="json")
        payload["evidence_budget_limits"] = spec.evidence_budget.model_dump(mode="json")
        payload["stage_history"] = state.stage_history
        payload["source_artifacts"] = state.source_artifacts
        payload["task_specific_runner_compatibility"] = load_compatibility_policy(
            compatibility_path
        )
    return payload


def diagnose_campaign(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_policy = _scorecard_policy(spec.scorecard_policy, _read_yaml(gate_policy_path))
    required_categories = set(gate_policy.get("required_evidence_categories", []))
    by_category: dict[str, list[dict[str, Any]]] = {}
    for record in evidence:
        by_category.setdefault(record.category, []).append(record.model_dump(mode="json"))

    positive = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "positive" and record.status in {"PASS", "INFO", "WARNING"}
    ]
    negative = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "negative" or record.status in {"FAIL", "BLOCKED"}
    ]
    missing = sorted(required_categories - set(by_category))
    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_diagnosis",
        "campaign_id": campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "evidence_matrix": by_category,
        "positive_evidence": positive,
        "negative_evidence": negative,
        "missing_required_evidence_categories": missing,
        "best_evidence": positive[:5],
        "worst_evidence": negative[:5],
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
    }


def evaluate_gate(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    evidence: list[EvidenceRecord],
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    gate_policy = _scorecard_policy(spec.scorecard_policy, _read_yaml(gate_policy_path))
    required_categories = set(gate_policy.get("required_evidence_categories", []))
    present_categories = {record.category for record in evidence}
    missing_categories = sorted(required_categories - present_categories)
    blocking_records = [
        record
        for record in evidence
        if record.status in {"FAIL", "BLOCKED"} or record.direction == "negative"
    ]
    reason_codes = sorted(
        {
            code
            for record in evidence
            for code in record.reason_codes
        }
        | set(state.reason_codes)
    )

    if missing_categories:
        decision = str(gate_policy.get("missing_evidence_outcome", "NEEDS_MORE_EVIDENCE"))
        reason_codes.extend(f"MISSING_{category}" for category in missing_categories)
    elif blocking_records:
        decision = _decision_from_reason_codes(
            reason_codes,
            gate_policy.get("blocking_reason_outcome_map", {}),
            default=str(gate_policy.get("blocking_evidence_outcome", "WEAK")),
        )
    elif any(record.direction == "mixed" or record.status == "MIXED" for record in evidence):
        decision = "MIXED"
    else:
        decision = str(gate_policy.get("pass_outcome", "PROMISING"))

    budget = evaluate_evidence_budget(spec, state)
    if (
        decision == "NEEDS_MORE_EVIDENCE"
        and budget["limits_exceeded"]["needs_more_evidence_occurrences"]
    ):
        decision = str(gate_policy.get("over_budget_outcome", "OWNER_OVERRIDE_REQUIRED"))
        reason_codes.append("EVIDENCE_BUDGET_EXHAUSTED")

    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_gate",
        "campaign_id": spec.campaign_id,
        "scorecard_policy": spec.scorecard_policy,
        "decision_outcome": decision,
        "reason_codes": sorted(set(reason_codes)),
        "required_evidence_categories": sorted(required_categories),
        "missing_required_evidence_categories": missing_categories,
        "blocking_evidence_ids": [record.evidence_id for record in blocking_records],
        "budget_status": budget["budget_status"],
        "allowed_decision_outcomes": (
            RESTRICTED_MORE_EVIDENCE_OUTCOMES
            if budget["limits_exceeded"]["needs_more_evidence_occurrences"]
            else DECISION_OUTCOMES
        ),
        "data_quality_status": state.data_quality_status,
        "safety_boundary": state.safety_boundary,
        "production_effect": "none",
    }


def evaluate_evidence_budget(spec: CampaignSpec, state: CampaignState) -> dict[str, Any]:
    used = state.evidence_budget_used
    limits = spec.evidence_budget
    exceeded = {
        "mini_rounds": used.mini_rounds >= limits.max_mini_rounds,
        "targeted_rounds": used.targeted_rounds >= limits.max_targeted_rounds,
        "window_expansions": used.window_expansions >= limits.max_window_expansions,
        "redesign_rounds": used.redesign_rounds >= limits.max_redesign_rounds,
        "needs_more_evidence_occurrences": (
            used.needs_more_evidence_occurrences >= limits.max_needs_more_evidence_occurrences
        ),
    }
    hard_exceeded = {
        key: value
        for key, value in exceeded.items()
        if value
        and key
        in {
            "targeted_rounds",
            "window_expansions",
            "redesign_rounds",
            "needs_more_evidence_occurrences",
        }
    }
    proximity = {
        "mini_rounds_remaining": max(0, limits.max_mini_rounds - used.mini_rounds),
        "targeted_rounds_remaining": max(0, limits.max_targeted_rounds - used.targeted_rounds),
        "window_expansions_remaining": max(
            0,
            limits.max_window_expansions - used.window_expansions,
        ),
        "redesign_rounds_remaining": max(0, limits.max_redesign_rounds - used.redesign_rounds),
        "needs_more_evidence_remaining": max(
            0,
            limits.max_needs_more_evidence_occurrences
            - used.needs_more_evidence_occurrences,
        ),
    }
    return {
        "budget_status": "EXHAUSTED" if hard_exceeded else "AVAILABLE",
        "limits_exceeded": exceeded,
        "stop_rule_proximity": proximity,
        "restricted_outcomes_when_exhausted": RESTRICTED_MORE_EVIDENCE_OUTCOMES,
    }


def build_next_action_plan(
    *,
    spec: CampaignSpec,
    state: CampaignState,
    evidence: list[EvidenceRecord],
    gate_payload: dict[str, Any],
) -> dict[str, Any]:
    budget = evaluate_evidence_budget(spec, state)
    allowed: list[str] = []
    blocked: list[str] = []
    owner_required: list[str] = []
    reason_codes = set(state.reason_codes) | set(gate_payload.get("reason_codes", []))

    if (
        state.current_stage == "TARGETED_EVIDENCE"
        and "SLOW_DRAWDOWN_SINGLE_WINDOW_ONLY" in reason_codes
        and budget["stop_rule_proximity"]["targeted_rounds_remaining"] > 0
    ):
        allowed.append("COMPLETE_FINAL_REPEATABILITY_ROUND")
    if "FAST_RISK_NOT_SUPPORTED" in reason_codes:
        allowed.append("NARROW_ROLE")
    if "REENTRY_LAG_SIGNAL_DRIVEN" in reason_codes:
        allowed.append("RETURN_TO_DESIGN")
    if gate_payload["decision_outcome"] in {"PROMISING", "PASS"}:
        allowed.append("PREPARE_OWNER_PACKET")
    if not allowed and gate_payload["decision_outcome"] == "NEEDS_MORE_EVIDENCE":
        allowed.append("COLLECT_DEFINED_EVIDENCE_WITHIN_BUDGET")

    if budget["limits_exceeded"]["targeted_rounds"]:
        blocked.append("RUN_ADDITIONAL_TARGETED_EVIDENCE_WITHOUT_OWNER_OVERRIDE")
    if budget["limits_exceeded"]["needs_more_evidence_occurrences"]:
        blocked.append("EMIT_OPEN_ENDED_NEEDS_MORE_EVIDENCE")
        owner_required.append("OWNER_OVERRIDE_REQUIRED_FOR_MORE_EVIDENCE")
    if not (state.current_stage == "GATE_READY" and spec.owner_authorized_holdout):
        blocked.append("ACCESS_UNTOUCHED_HOLDOUT")
        owner_required.append("FINAL_GATE_OWNER_AUTHORIZATION_REQUIRED_FOR_HOLDOUT")
    if "MISSING_INTERACTION_EFFECT" in reason_codes:
        blocked.append("PROMOTE_WITHOUT_INTERACTION_EVIDENCE")

    next_stage = _recommended_next_stage(state.current_stage, allowed, blocked)
    return {
        "allowed_next_actions": sorted(set(allowed)),
        "blocked_actions": sorted(set(blocked)),
        "required_owner_actions": sorted(set(owner_required)),
        "next_recommended_stage": next_stage,
    }


def plan_experiment_matrix(
    *,
    spec: CampaignSpec,
    module_registry_path: Path = DEFAULT_MODULE_REGISTRY_PATH,
) -> dict[str, Any]:
    registry = load_module_registry(module_registry_path)
    approved_modules: list[str] = []
    blocked_modules: list[dict[str, str]] = []
    for module_id in spec.module_graph.modules:
        capability = registry[module_id]
        if capability.requires_gate_approval_for_interaction:
            blocked_modules.append(
                {
                    "module_id": module_id,
                    "reason": "REQUIRES_GATE_APPROVAL_FOR_INTERACTION",
                }
            )
            continue
        approved_modules.append(module_id)

    max_order = min(spec.module_graph.allowed_interaction_order, len(approved_modules))
    matrix: list[dict[str, Any]] = [
        {
            "experiment_id": spec.module_graph.baseline,
            "modules": [],
            "role": "baseline",
        }
    ]
    for order in range(1, max_order + 1):
        for combo in combinations(approved_modules, order):
            aliases = [_module_alias(registry[module_id], module_id) for module_id in combo]
            matrix.append(
                {
                    "experiment_id": f"{spec.module_graph.baseline}+{'+'.join(aliases)}",
                    "modules": list(combo),
                    "role": "main_effect" if order == 1 else f"{order}_way_interaction",
                }
            )

    return {
        "schema_version": "1.0",
        "report_type": "research_campaign_experiment_matrix",
        "campaign_id": spec.campaign_id,
        "baseline": spec.module_graph.baseline,
        "allowed_interaction_order": spec.module_graph.allowed_interaction_order,
        "experiment_matrix": matrix,
        "blocked_modules": blocked_modules,
        "effect_formulas": {
            "main_effect": "U(B0 + A) - U(B0)",
            "pair_interaction": "U(B0 + A + B) - U(B0 + A) - U(B0 + B) + U(B0)",
            "utility_definition": "portfolio utility from configured scorecard policy",
        },
        "production_effect": "none",
    }


def classify_interaction_effect(
    *,
    value: float,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> str:
    policy = _read_yaml(gate_policy_path).get("interaction_effect_policy", {})
    thresholds = policy.get("classification_thresholds", {})
    positive_min = float(thresholds.get("positive_synergy_min", 0.02))
    negative_max = float(thresholds.get("negative_interference_max", -0.02))
    redundant_abs_max = float(thresholds.get("redundant_abs_max", 0.005))
    additive_abs_max = float(thresholds.get("mostly_additive_abs_max", 0.02))
    if value >= positive_min:
        return "POSITIVE_SYNERGY"
    if value <= negative_max:
        return "NEGATIVE_INTERFERENCE"
    if abs(value) <= redundant_abs_max:
        return "REDUNDANT"
    if abs(value) <= additive_abs_max:
        return "MOSTLY_ADDITIVE"
    return "INCONCLUSIVE"


def build_owner_packet(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    output_root: Path = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    gate_policy_path: Path = DEFAULT_GATE_POLICY_PATH,
) -> dict[str, Any]:
    spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
    gate_payload = evaluate_gate(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_policy_path=gate_policy_path,
    )
    plan = build_next_action_plan(
        spec=spec,
        state=state,
        evidence=evidence,
        gate_payload=gate_payload,
    )
    positive = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "positive" and record.status in {"PASS", "INFO", "WARNING"}
    ]
    negative = [
        record.model_dump(mode="json")
        for record in evidence
        if record.direction == "negative" or record.status in {"FAIL", "BLOCKED"}
    ]
    output_dir = output_root / campaign_id
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "1.0",
        "report_type": "research_campaign_owner_packet",
        "campaign_id": campaign_id,
        "decision": gate_payload["decision_outcome"],
        "current_stage": state.current_stage,
        "hypothesis": spec.hypothesis.model_dump(mode="json"),
        "what_was_tested": {
            "modules": spec.module_graph.modules,
            "baseline": spec.module_graph.baseline,
            "window_policy": spec.window_policy.model_dump(mode="json"),
        },
        "positive_evidence": positive,
        "negative_evidence": negative,
        "evidence_budget_used": state.evidence_budget_used.model_dump(mode="json"),
        "evidence_budget_limits": spec.evidence_budget.model_dump(mode="json"),
        "remaining_uncertainty": gate_payload["missing_required_evidence_categories"],
        "allowed_next_actions": plan["allowed_next_actions"],
        "blocked_actions": plan["blocked_actions"],
        "required_owner_actions": plan["required_owner_actions"],
        "stop_rules_approaching": evaluate_evidence_budget(spec, state)["stop_rule_proximity"],
        "safety_boundary": state.safety_boundary,
        "source_artifacts": state.source_artifacts,
        "owner_decision_appended": False,
        "data_quality_status": state.data_quality_status,
        "production_effect": "none",
    }
    json_path = output_dir / f"owner_packet_{campaign_id}.json"
    md_path = output_dir / f"owner_packet_{campaign_id}.md"
    _write_json(json_path, payload)
    md_path.write_text(render_owner_packet_markdown(payload), encoding="utf-8")
    return {
        **payload,
        "json_path": str(json_path),
        "markdown_path": str(md_path),
    }


def render_owner_packet_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Research Campaign Owner Packet: {payload['campaign_id']}",
        "",
        "## Reader Brief",
        "",
        f"- Decision: {payload['decision']}",
        f"- Current Stage: {payload['current_stage']}",
        f"- Data Quality: {payload['data_quality_status']}",
        "- Safety Boundary: "
        f"research_only={payload['safety_boundary']['research_only']}; "
        f"official_target_weights={payload['safety_boundary']['official_target_weights']}; "
        f"production_effect={payload['safety_boundary']['production_effect']}",
        "- Owner Decision Appended: false",
        "",
        "## Hypothesis",
        "",
        payload["hypothesis"]["statement"],
        "",
        "## What Was Tested",
        "",
        f"- Baseline: {payload['what_was_tested']['baseline']}",
        f"- Modules: {', '.join(payload['what_was_tested']['modules'])}",
        "",
        "## Positive Evidence",
        "",
    ]
    lines.extend(_evidence_lines(payload["positive_evidence"]))
    lines.extend(["", "## Negative Evidence", ""])
    lines.extend(_evidence_lines(payload["negative_evidence"]))
    lines.extend(
        [
            "",
            "## Evidence Budget Used",
            "",
            json.dumps(payload["evidence_budget_used"], ensure_ascii=False, sort_keys=True),
            "",
            "## Remaining Uncertainty",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["remaining_uncertainty"] or ["none"])
    lines.extend(["", "## Allowed Next Actions", ""])
    lines.extend(f"- {item}" for item in payload["allowed_next_actions"] or ["none"])
    lines.extend(["", "## Blocked Actions", ""])
    lines.extend(f"- {item}" for item in payload["blocked_actions"] or ["none"])
    lines.extend(["", "## Stop Rules Approaching", ""])
    for key, value in payload["stop_rules_approaching"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Source Artifacts", ""])
    lines.extend(f"- {item}" for item in payload["source_artifacts"] or ["none"])
    return "\n".join(lines) + "\n"


def archive_campaign(
    *,
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
    reason: str = "manual_archive",
) -> dict[str, Any]:
    directory = campaign_directory(campaign_id, campaign_root)
    spec, state, _ = load_campaign_bundle(campaign_id, campaign_root)
    state.current_stage = "ARCHIVED"
    state.current_outcome = (
        state.current_outcome
        if state.current_outcome != "NOT_EVALUATED"
        else "MIXED"
    )
    state.updated_at = _now_iso()
    state.stage_history.append(
        {
            "run_id": f"{campaign_id}-archive-{_compact_time()}",
            "stage": "ARCHIVED",
            "outcome": state.current_outcome,
            "reason_codes": [reason],
            "created_at": state.updated_at,
        }
    )
    write_campaign_state(state, directory)
    _append_transition(
        directory,
        {
            "transition_id": f"{campaign_id}-archive-{_compact_time()}",
            "campaign_id": campaign_id,
            "from_stage": None,
            "to_stage": "ARCHIVED",
            "outcome": state.current_outcome,
            "reason_codes": [reason],
            "created_at": state.updated_at,
            "source": "campaign_archive",
        },
    )
    return {
        "campaign_id": spec.campaign_id,
        "current_stage": state.current_stage,
        "current_outcome": state.current_outcome,
        "archive_reason": reason,
        "production_effect": "none",
    }


def build_reproducibility_manifest(
    *,
    spec_path: Path,
    spec: CampaignSpec,
    config_paths: list[Path],
    output_paths: list[Path],
    transition_source: str,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "manifest_type": "research_campaign_reproducibility",
        "campaign_id": spec.campaign_id,
        "generated_at": _now_iso(),
        "git_commit": _git_commit(),
        "git_dirty": _git_dirty(),
        "market_regime": spec.market_regime,
        "requested_date_range": spec.requested_date_range,
        "spec": {
            "path": str(spec_path),
            "checksum": _checksum(spec_path),
        },
        "configs": [_path_record(path) for path in config_paths],
        "inputs": [],
        "windows": spec.window_policy.model_dump(mode="json"),
        "seed": None,
        "outputs": [_path_record(path) for path in output_paths],
        "transition_source": transition_source,
        "data_quality_status": CONTROL_ONLY_DATA_QUALITY_STATUS,
        "production_effect": "none",
    }


def validate_stage_transition(current_stage: str, next_stage: str) -> dict[str, Any]:
    if current_stage not in WORKFLOW_STAGES:
        return _transition(False, current_stage, next_stage, "UNKNOWN_CURRENT_STAGE")
    if next_stage not in WORKFLOW_STAGES:
        return _transition(False, current_stage, next_stage, "UNKNOWN_NEXT_STAGE")
    current_index = WORKFLOW_STAGES.index(current_stage)
    next_index = WORKFLOW_STAGES.index(next_stage)
    if next_stage == current_stage:
        return _transition(True, current_stage, next_stage, "same_stage_rerun")
    if next_stage == "ARCHIVED" and current_stage in {"OWNER_REVIEW", "GATE_READY"}:
        return _transition(True, current_stage, next_stage, "archive_after_review_or_gate")
    if next_index - current_index == 1:
        return _transition(True, current_stage, next_stage, "standard_next_stage")
    return _transition(False, current_stage, next_stage, "ILLEGAL_STAGE_SKIP")


def resolve_requested_stage(current_stage: str, requested_stage: str) -> str:
    normalized = requested_stage.upper().replace("-", "_")
    aliases = {
        "NEXT": _next_stage(current_stage),
        "SCOPE": "SCOPE_READY",
        "VALIDATE": "SCOPE_READY",
        "PRECHECK": "INPUT_PRECHECK",
        "INPUT": "INPUT_PRECHECK",
        "MINI": "MINI_DIAGNOSTIC",
        "TARGETED": "TARGETED_EVIDENCE",
        "FULL": "FULL_DIAGNOSTIC",
        "GATE": "GATE_READY",
        "OWNER": "OWNER_REVIEW",
        "PACKET": "OWNER_REVIEW",
    }
    return aliases.get(normalized, normalized)


def load_campaign_bundle(
    campaign_id: str,
    campaign_root: Path = DEFAULT_CAMPAIGN_ROOT,
) -> tuple[CampaignSpec, CampaignState, list[EvidenceRecord]]:
    directory = campaign_directory(campaign_id, campaign_root)
    if not directory.exists():
        raise ResearchCampaignError(f"Campaign not found: {campaign_id}")
    spec = CampaignSpec.model_validate(_read_json(directory / "spec.json"))
    state = CampaignState.model_validate(_read_json(directory / "state.json"))
    evidence = read_evidence_records(directory)
    return spec, state, evidence


def load_module_registry(path: Path = DEFAULT_MODULE_REGISTRY_PATH) -> dict[str, ModuleCapability]:
    raw = _read_yaml(path)
    modules = raw.get("modules", {})
    registry: dict[str, ModuleCapability] = {}
    for module_id, payload in modules.items():
        registry[module_id] = ModuleCapability.model_validate({"module_id": module_id, **payload})
    return registry


def load_campaign_migrations(path: Path = DEFAULT_MIGRATION_PATH) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    return dict(_read_yaml(path).get("migrations", {}))


def load_compatibility_policy(path: Path = DEFAULT_COMPATIBILITY_PATH) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "COMPATIBILITY_POLICY_MISSING",
            "deprecated_task_specific_runner_count": 0,
        }
    payload = _read_yaml(path)
    runners = payload.get("deprecated_task_specific_runners", [])
    return {
        "status": payload.get("status", "COMPATIBILITY_POLICY_READY"),
        "deprecated_task_specific_runner_count": len(runners),
        "deprecated_task_specific_runners": runners,
    }


def write_campaign_state(state: CampaignState, directory: Path) -> None:
    _write_json(directory / "state.json", state.model_dump(mode="json"))


def read_evidence_records(directory: Path) -> list[EvidenceRecord]:
    path = directory / "evidence.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            records.append(EvidenceRecord.model_validate(json.loads(line)))
    return records


def write_evidence_records(
    records: list[EvidenceRecord],
    directory: Path,
    *,
    replace: bool = False,
) -> None:
    path = directory / "evidence.jsonl"
    if replace:
        path.write_text("", encoding="utf-8")
    append_evidence_records(records, directory)


def append_evidence_records(records: list[EvidenceRecord], directory: Path) -> None:
    for record in records:
        _append_jsonl(directory / "evidence.jsonl", record.model_dump(mode="json"))


def campaign_directory(campaign_id: str, campaign_root: Path = DEFAULT_CAMPAIGN_ROOT) -> Path:
    if "/" in campaign_id or "\\" in campaign_id or ".." in campaign_id:
        raise ResearchCampaignError(f"Unsafe campaign_id: {campaign_id}")
    return campaign_root / campaign_id


def _initial_state_from_spec(spec: CampaignSpec, migration: dict[str, Any] | None) -> CampaignState:
    if not migration:
        return CampaignState(
            campaign_id=spec.campaign_id,
            program_id=spec.program_id,
            title=spec.title,
            source_artifacts=[],
        )
    budget = BudgetUsed.model_validate(migration.get("evidence_budget_used", {}))
    return CampaignState(
        campaign_id=spec.campaign_id,
        program_id=spec.program_id,
        title=spec.title,
        current_stage=str(migration.get("stage", "DRAFT")),
        current_outcome=str(migration.get("outcome", "NOT_EVALUATED")),
        reason_codes=list(migration.get("reason_codes", [])),
        evidence_budget_used=budget,
        source_artifacts=list(migration.get("source_artifacts", [])),
        migrated_from_legacy=True,
        legacy_status=migration.get("legacy_status"),
        stage_history=list(migration.get("stage_history", [])),
    )


def _validate_safety(spec: CampaignSpec) -> list[dict[str, Any]]:
    safety = spec.safety
    issues: list[dict[str, Any]] = []
    checks = {
        "research_only": safety.research_only is True,
        "manual_review_only": safety.manual_review_only is True,
        "official_target_weights": safety.official_target_weights is False,
        "paper_shadow_allowed": safety.paper_shadow_allowed is False,
        "broker_effect": safety.broker_effect == "none",
        "order_effect": safety.order_effect == "none",
        "production_effect": safety.production_effect == "none",
    }
    for field_name, passed in checks.items():
        if not passed:
            issues.append(
                _issue(
                    "error",
                    f"safety_{field_name}_invalid",
                    f"safety.{field_name} violates research-only boundary",
                )
            )
    return issues


def _validate_module_graph(
    spec: CampaignSpec,
    registry: dict[str, ModuleCapability],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    all_module_ids = [spec.module_graph.baseline, *spec.module_graph.modules]
    for module_id in all_module_ids:
        if module_id not in registry:
            issues.append(_issue("error", "unknown_module", f"Unknown module: {module_id}"))
    if issues:
        return issues

    selected = [registry[module_id] for module_id in spec.module_graph.modules]
    for capability in selected:
        if not capability.approved_for_campaign_modules:
            issues.append(
                _issue(
                    "error",
                    "module_not_approved_for_campaign",
                    f"{capability.module_id} cannot be used as a campaign module",
                )
            )
        if capability.module_type in {"P0_MIXED_ALLOCATOR", "MIXED_ALLOCATOR"}:
            issues.append(
                _issue(
                    "error",
                    "p0_mixed_allocator_not_single_module",
                    f"{capability.module_id} cannot masquerade as a single module",
                )
            )
        output_set = set(capability.output_contract)
        if capability.module_type == "SIGNAL" and output_set & FORBIDDEN_WEIGHT_OUTPUTS:
            issues.append(
                _issue(
                    "error",
                    "signal_module_outputs_weight",
                    f"{capability.module_id} output_contract contains weight output",
                )
            )
        if capability.module_type == "EVALUATOR" and output_set & FORBIDDEN_EVALUATION_MUTATIONS:
            issues.append(
                _issue(
                    "error",
                    "evaluation_module_mutates_strategy",
                    f"{capability.module_id} output_contract contains strategy mutation output",
                )
            )
        mechanism_overlap = set(capability.allowed_mechanisms) - set(
            spec.module_graph.allowed_mechanisms
        )
        if mechanism_overlap:
            issues.append(
                _issue(
                    "error",
                    "module_uses_unallowed_mechanism",
                    f"{capability.module_id} mechanisms not allowed by campaign: "
                    f"{sorted(mechanism_overlap)}",
                )
            )
        forbidden_overlap = set(capability.allowed_mechanisms) & set(
            spec.module_graph.forbidden_mechanisms
        )
        if forbidden_overlap:
            issues.append(
                _issue(
                    "error",
                    "module_uses_forbidden_mechanism",
                    f"{capability.module_id} mechanisms forbidden by campaign: "
                    f"{sorted(forbidden_overlap)}",
                )
            )
    selected_ids = set(spec.module_graph.modules)
    for capability in selected:
        incompatible = selected_ids & set(capability.incompatible_modules)
        if incompatible:
            issues.append(
                _issue(
                    "error",
                    "incompatible_module_combination",
                    f"{capability.module_id} incompatible with {sorted(incompatible)}",
                )
            )
    return issues


def _validate_window_policy(spec: CampaignSpec, policy: dict[str, Any]) -> list[dict[str, Any]]:
    catalogs = policy.get("catalogs", {})
    issues: list[dict[str, Any]] = []
    for field_name in ("development_catalog", "diagnostic_catalog", "holdout_catalog"):
        catalog_id = getattr(spec.window_policy, field_name)
        if catalog_id not in catalogs:
            issues.append(
                _issue(
                    "error",
                    "unknown_window_catalog",
                    f"Unknown {field_name}: {catalog_id}",
                )
            )
    if issues:
        return issues
    holdout = catalogs[spec.window_policy.holdout_catalog]
    if not holdout.get("holdout", False):
        issues.append(
            _issue(
                "error",
                "holdout_catalog_not_marked_holdout",
                f"{spec.window_policy.holdout_catalog} must be holdout=true",
            )
        )
    if (
        spec.window_policy.holdout_access != "FINAL_GATE_ONLY"
        and not spec.owner_authorized_holdout
    ):
        issues.append(
            _issue(
                "error",
                "holdout_access_requires_owner_authorization",
                "Holdout access beyond FINAL_GATE_ONLY requires owner_authorized_holdout=true",
            )
        )
    if spec.owner_authorized_holdout and spec.window_policy.holdout_access == "FORBIDDEN":
        issues.append(
            _issue("error", "holdout_forbidden", "Campaign forbids holdout access")
        )
    return issues


def _validate_gate_policy(spec: CampaignSpec, gate_policy: dict[str, Any]) -> list[dict[str, Any]]:
    scorecards = gate_policy.get("scorecards", {})
    if spec.scorecard_policy not in scorecards:
        return [
            _issue(
                "error",
                "unknown_scorecard_policy",
                f"Unknown scorecard policy: {spec.scorecard_policy}",
            )
        ]
    scorecard = scorecards[spec.scorecard_policy]
    metadata = scorecard.get("policy_metadata", {})
    missing = [
        key
        for key in (
            "owner",
            "status",
            "rationale",
            "intended_effect",
            "validation_evidence",
            "review_condition",
        )
        if not metadata.get(key)
    ]
    if missing:
        return [
            _issue(
                "error",
                "scorecard_policy_metadata_incomplete",
                f"Scorecard metadata missing: {missing}",
            )
        ]
    return []


def _validate_budget(spec: CampaignSpec) -> list[dict[str, Any]]:
    budget = spec.evidence_budget
    if budget.max_needs_more_evidence_occurrences == 0:
        return [
            _issue(
                "warning",
                "needs_more_evidence_budget_zero",
                "Campaign cannot emit NEEDS_MORE_EVIDENCE without immediate restricted decision",
            )
        ]
    return []


def _scorecard_policy(scorecard_policy: str, raw_policy: dict[str, Any]) -> dict[str, Any]:
    scorecards = raw_policy.get("scorecards", {})
    if scorecard_policy not in scorecards:
        raise ResearchCampaignError(f"Unknown scorecard policy: {scorecard_policy}")
    return dict(scorecards[scorecard_policy])


def _decision_from_reason_codes(
    reason_codes: list[str],
    mapping: dict[str, str],
    *,
    default: str,
) -> str:
    for reason_code in reason_codes:
        if reason_code in mapping:
            return str(mapping[reason_code])
    return default


def _control_evidence(
    *,
    campaign_id: str,
    run_id: str,
    stage: str,
    category: str,
    metric_name: str,
    value: Any,
    status: Literal["PASS", "FAIL", "MIXED", "BLOCKED", "WARNING", "INFO"],
    source_artifact_id: str,
    reason_codes: list[str],
) -> EvidenceRecord:
    return EvidenceRecord(
        evidence_id=f"{run_id}-{metric_name}",
        campaign_id=campaign_id,
        run_id=run_id,
        stage=stage,
        category=category,
        metric_name=metric_name,
        value=value,
        status=status,
        confidence="high" if status == "PASS" else "medium",
        source_artifact_id=source_artifact_id,
        reason_codes=reason_codes,
        direction="neutral" if status == "PASS" else "negative",
    )


def _recommended_next_stage(current_stage: str, allowed: list[str], blocked: list[str]) -> str:
    if "PREPARE_OWNER_PACKET" in allowed:
        return "OWNER_REVIEW"
    if "COMPLETE_FINAL_REPEATABILITY_ROUND" in allowed:
        return "TARGETED_EVIDENCE"
    if "RETURN_TO_DESIGN" in allowed or "NARROW_ROLE" in allowed:
        return "OWNER_REVIEW"
    if "EMIT_OPEN_ENDED_NEEDS_MORE_EVIDENCE" in blocked:
        return "OWNER_REVIEW"
    return _next_stage(current_stage)


def _outcome_from_imported_stage_evidence(
    records: list[EvidenceRecord],
    state: CampaignState,
) -> str:
    if state.current_stage == records[0].stage and state.current_outcome != "NOT_EVALUATED":
        return state.current_outcome
    if any(record.status == "BLOCKED" for record in records):
        return "BLOCKED"
    if any(record.status == "FAIL" or record.direction == "negative" for record in records):
        return "MIXED"
    if any(record.status == "MIXED" or record.direction == "mixed" for record in records):
        return "MIXED"
    return "PASS"


def _next_stage(current_stage: str) -> str:
    if current_stage not in WORKFLOW_STAGES:
        raise ResearchCampaignError(f"Unknown current stage: {current_stage}")
    index = WORKFLOW_STAGES.index(current_stage)
    if index >= len(WORKFLOW_STAGES) - 1:
        return current_stage
    return WORKFLOW_STAGES[index + 1]


def _transition(allowed: bool, current_stage: str, next_stage: str, reason: str) -> dict[str, Any]:
    return {
        "allowed": allowed,
        "from_stage": current_stage,
        "to_stage": next_stage,
        "reason": reason,
    }


def _module_alias(capability: ModuleCapability, module_id: str) -> str:
    return capability.interaction_alias or module_id


def _evidence_lines(records: list[dict[str, Any]]) -> list[str]:
    if not records:
        return ["- none"]
    return [
        f"- {record['evidence_id']}: {record['category']} / {record['metric_name']} "
        f"= {record.get('value')} ({record['status']})"
        for record in records
    ]


def _append_transition(directory: Path, payload: dict[str, Any]) -> None:
    _append_jsonl(directory / "transitions.jsonl", payload)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ResearchCampaignError(f"Cannot read JSON: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ResearchCampaignError(f"Invalid JSON: {path}") from exc


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ResearchCampaignError(f"Cannot read YAML: {path}") from exc
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ResearchCampaignError(f"YAML root must be a mapping: {path}")
    return raw


def _issue(severity: str, issue_id: str, message: str) -> dict[str, Any]:
    return {"severity": severity, "issue_id": issue_id, "message": message}


def _path_record(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "checksum": _checksum(path) if path.exists() and path.is_file() else None,
    }


def _checksum(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def _git_dirty() -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return bool(result.stdout.strip())


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact_time() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
