from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import yaml

from ai_trading_system.yaml_loader import load_strict_yaml_text


class EvidenceFirstPortfolioError(ValueError):
    pass


_FROZEN_BASE_FILE_SHA256 = "2df617dc247e509cb94799dda10e4c75ed1e8f1fc069a47466267125e39b8a05"
_RESULT_ADMISSION_PATH = Path(
    "config/research/frozen_signal_value_confirmation_result_admission_v1.yaml"
)
_RETAIN_STOP_ACTION = "OPEN_OWNER_REVIEW_FOR_CONDITIONAL_OPTIONS_PAIRED_COMPARISON"
_RETAIN_NEXT_EXPERIMENT = "OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON"
_RETAIN_NEXT_EXPERIMENT_ZH = (
    "信号价值已得到“保留”结论；下一合法动作仅为人工复核是否启动同信号、同资本的 "
    "QQQ 与期权实现 paired comparison，当前不自动创建或执行该实验。"
)


class EvidenceState(StrEnum):
    READY = "READY"
    UNRESOLVED = "UNRESOLVED"
    RETAIN = "RETAIN"
    REJECT = "REJECT"
    INSUFFICIENT = "INSUFFICIENT"
    NOT_RUN = "NOT_RUN"
    NOT_ESTABLISHED = "NOT_ESTABLISHED"
    NOT_ELIGIBLE = "NOT_ELIGIBLE"


class P0AdmissionClass(StrEnum):
    EMPIRICAL_EVIDENCE = "EMPIRICAL_EVIDENCE"
    DIRECT_EXPERIMENT_ENABLER = "DIRECT_EXPERIMENT_ENABLER"
    MANDATORY_CORRECTNESS = "MANDATORY_CORRECTNESS"


def _required(value: object, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_REQUIRED:{field}")
    return text


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_MAPPING_REQUIRED:{field}")
    return value


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise EvidenceFirstPortfolioError(
            f"EVIDENCE_FIRST_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _strings(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_LIST_REQUIRED:{field}")
    result = tuple(_required(item, field) for item in value)
    if not result or len(result) != len(set(result)):
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_LIST_INVALID:{field}")
    return result


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_BOOLEAN_REQUIRED:{field}")
    return value


@dataclass(frozen=True)
class EvidenceLadderItem:
    evidence_id: str
    label_zh: str
    state: EvidenceState
    explanation_zh: str

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> EvidenceLadderItem:
        _exact_keys(
            payload,
            {"evidence_id", "label_zh", "state", "explanation_zh"},
            "evidence_ladder.item",
        )
        return cls(
            evidence_id=_required(payload["evidence_id"], "evidence_id"),
            label_zh=_required(payload["label_zh"], "label_zh"),
            state=EvidenceState(_required(payload["state"], "state")),
            explanation_zh=_required(payload["explanation_zh"], "explanation_zh"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "evidence_id": self.evidence_id,
            "label_zh": self.label_zh,
            "state": self.state.value,
            "explanation_zh": self.explanation_zh,
        }


@dataclass(frozen=True)
class EvidenceFirstResearchPortfolio:
    schema_version: ClassVar[str] = "evidence_first_research_portfolio.v1"
    EXPECTED_LADDER_IDS: ClassVar[tuple[str, ...]] = (
        "ENGINEERING_REPRODUCIBILITY",
        "PRIMARY_WINDOW_DQ_PIT",
        "EXACT_SIGNAL_PACKAGE",
        "SIGNAL_VALUE",
        "IMPLEMENTATION_VALUE",
        "ROBUSTNESS",
        "PRODUCTION",
    )
    EXPECTED_LADDER_STATE_PREFIX: ClassVar[tuple[EvidenceState, ...]] = (
        EvidenceState.READY,
        EvidenceState.READY,
        EvidenceState.READY,
    )
    EXPECTED_LADDER_STATE_SUFFIX: ClassVar[tuple[EvidenceState, ...]] = (
        EvidenceState.NOT_RUN,
        EvidenceState.NOT_ESTABLISHED,
        EvidenceState.NOT_ELIGIBLE,
    )
    VERDICT_NEXT_EXPERIMENT: ClassVar[Mapping[EvidenceState, str]] = {
        EvidenceState.UNRESOLVED: "FROZEN_SIGNAL_VALUE_CONFIRMATION",
        EvidenceState.RETAIN: "OWNER_REVIEW_CONDITIONAL_OPTIONS_PAIRED_COMPARISON",
        EvidenceState.REJECT: "OPTIONS_IMPLEMENTATION_P0_CLOSED",
        EvidenceState.INSUFFICIENT: "EXPLICIT_PROSPECTIVE_EVIDENCE_ONLY",
    }
    REQUIRED_P0_FIELDS: ClassVar[tuple[str, ...]] = (
        "research_question_id",
        "decision_enabled",
        "evidence_type",
        "blocked_experiment",
        "stop_condition",
        "successor_condition",
    )
    L0_SECTIONS: ClassVar[tuple[str, ...]] = (
        "RESEARCH_QUESTION",
        "CURRENT_VERDICT",
        "EVIDENCE_LADDER",
        "NEXT_EXPERIMENT",
        "STOP_CONDITION",
    )

    policy_id: str
    policy_version: str
    status: str
    owner_decision: str
    research_goal_zh: str
    question_id: str
    question_zh: str
    current_verdict: EvidenceState
    next_experiment_id: str
    allowed_verdicts: tuple[str, ...]
    historical_window_role: str
    primary_research_start: str
    evidence_ladder: tuple[EvidenceLadderItem, ...]
    allowed_p0_classes: tuple[P0AdmissionClass, ...]
    default_denied_class: str
    required_p0_fields: tuple[str, ...]
    no_automatic_successor: bool
    phase_ready_conditions: tuple[str, ...]
    next_p0_when_ready: P0AdmissionClass
    drift_status: str
    reopen_allowed_triggers: tuple[str, ...]
    reopen_forbidden_reasons: tuple[str, ...]
    l0_sections: tuple[str, ...]
    l1_entry_label_zh: str
    audit_entry_label_zh: str
    l0_forbidden_payloads: tuple[str, ...]
    next_experiment_zh: str
    stop_condition_zh: str
    prohibited_inference_zh: str
    empirical_run_authorized: bool
    quantconnect_action_authorized: bool
    external_provider_action_authorized: bool
    investment_conclusion_generated: bool
    production_effect: str
    broker_action: str

    def __post_init__(self) -> None:
        if self.policy_id != "evidence_first_research_portfolio_v1":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_POLICY_ID_INVALID")
        if self.policy_version != "1.0.0" or self.status != "REVIEWED_OWNER_SELECTED":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_POLICY_STATE_INVALID")
        if self.question_id != "SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_PRIMARY_QUESTION_INVALID")
        if self.current_verdict not in self.VERDICT_NEXT_EXPERIMENT:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_CURRENT_VERDICT_INVALID")
        if self.next_experiment_id != self.VERDICT_NEXT_EXPERIMENT[self.current_verdict]:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_NEXT_EXPERIMENT_INVALID")
        if self.allowed_verdicts != ("RETAIN", "REJECT", "INSUFFICIENT"):
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_VERDICTS_INVALID")
        if self.historical_window_role != "REUSED_DEVELOPMENT_CONFIRMATION":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_WINDOW_ROLE_INVALID")
        if self.primary_research_start != "2021-02-22":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESEARCH_START_INVALID")
        if tuple(item.evidence_id for item in self.evidence_ladder) != self.EXPECTED_LADDER_IDS:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_LADDER_ORDER_INVALID")
        expected_ladder_states = (
            *self.EXPECTED_LADDER_STATE_PREFIX,
            self.current_verdict,
            *self.EXPECTED_LADDER_STATE_SUFFIX,
        )
        if tuple(item.state for item in self.evidence_ladder) != expected_ladder_states:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_LADDER_STATE_INVALID")
        if self.allowed_p0_classes != tuple(P0AdmissionClass):
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_P0_CLASSES_INVALID")
        if self.default_denied_class != "PUBLISHING_OR_CONVENIENCE":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_DEFAULT_DENIAL_INVALID")
        if self.required_p0_fields != self.REQUIRED_P0_FIELDS or not self.no_automatic_successor:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_P0_FIELDS_INVALID")
        if self.next_p0_when_ready is not P0AdmissionClass.EMPIRICAL_EVIDENCE:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_PHASE_SWITCH_INVALID")
        if self.drift_status != "RESEARCH_PRIORITY_DRIFT":
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_DRIFT_STATUS_INVALID")
        if self.l0_sections != self.L0_SECTIONS:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_READER_L0_INVALID")
        if not {"task_id", "contract_id", "sha256", "receipt", "manifest", "full_ledger"} <= set(
            self.l0_forbidden_payloads
        ):
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_READER_AUDIT_BOUNDARY_INVALID")
        if (
            any(
                (
                    self.empirical_run_authorized,
                    self.quantconnect_action_authorized,
                    self.external_provider_action_authorized,
                    self.investment_conclusion_generated,
                )
            )
            or self.production_effect != "none"
            or self.broker_action != "none"
        ):
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_SAFETY_INVALID")

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "status": self.status,
            "owner_decision": self.owner_decision,
            "research_goal_zh": self.research_goal_zh,
            "primary_evidence_question": {
                "question_id": self.question_id,
                "question_zh": self.question_zh,
                "current_verdict": self.current_verdict.value,
                "next_experiment_id": self.next_experiment_id,
                "allowed_verdicts": list(self.allowed_verdicts),
                "historical_window_role": self.historical_window_role,
                "primary_research_start": self.primary_research_start,
            },
            "evidence_ladder": [item.to_dict() for item in self.evidence_ladder],
            "p0_admission": {
                "allowed_classes": [item.value for item in self.allowed_p0_classes],
                "default_denied_class": self.default_denied_class,
                "required_fields": list(self.required_p0_fields),
                "no_automatic_successor": self.no_automatic_successor,
            },
            "phase_switch": {
                "ready_conditions": list(self.phase_ready_conditions),
                "next_p0_when_ready": self.next_p0_when_ready.value,
                "drift_status": self.drift_status,
            },
            "reopen_policy": {
                "allowed_triggers": list(self.reopen_allowed_triggers),
                "forbidden_reasons": list(self.reopen_forbidden_reasons),
            },
            "reader_entry": {
                "l0_sections": list(self.l0_sections),
                "l1_entry_label_zh": self.l1_entry_label_zh,
                "audit_entry_label_zh": self.audit_entry_label_zh,
                "l0_forbidden_payloads": list(self.l0_forbidden_payloads),
                "next_experiment_zh": self.next_experiment_zh,
                "stop_condition_zh": self.stop_condition_zh,
                "prohibited_inference_zh": self.prohibited_inference_zh,
            },
            "safety": {
                "empirical_run_authorized": self.empirical_run_authorized,
                "quantconnect_action_authorized": self.quantconnect_action_authorized,
                "external_provider_action_authorized": self.external_provider_action_authorized,
                "investment_conclusion_generated": self.investment_conclusion_generated,
                "production_effect": self.production_effect,
                "broker_action": self.broker_action,
            },
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> EvidenceFirstResearchPortfolio:
        _exact_keys(
            payload,
            {
                "schema_version",
                "policy_id",
                "policy_version",
                "status",
                "owner_decision",
                "research_goal_zh",
                "primary_evidence_question",
                "evidence_ladder",
                "p0_admission",
                "phase_switch",
                "reopen_policy",
                "reader_entry",
                "safety",
            },
            "root",
        )
        if payload["schema_version"] != cls.schema_version:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_SCHEMA_INVALID")
        question = _mapping(payload["primary_evidence_question"], "primary_evidence_question")
        admission = _mapping(payload["p0_admission"], "p0_admission")
        phase = _mapping(payload["phase_switch"], "phase_switch")
        reopen = _mapping(payload["reopen_policy"], "reopen_policy")
        reader = _mapping(payload["reader_entry"], "reader_entry")
        safety = _mapping(payload["safety"], "safety")
        ladder_payload = payload["evidence_ladder"]
        if not isinstance(ladder_payload, list):
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_LIST_REQUIRED:evidence_ladder")
        return cls(
            policy_id=_required(payload["policy_id"], "policy_id"),
            policy_version=_required(payload["policy_version"], "policy_version"),
            status=_required(payload["status"], "status"),
            owner_decision=_required(payload["owner_decision"], "owner_decision"),
            research_goal_zh=_required(payload["research_goal_zh"], "research_goal_zh"),
            question_id=_required(question["question_id"], "question_id"),
            question_zh=_required(question["question_zh"], "question_zh"),
            current_verdict=EvidenceState(
                _required(question["current_verdict"], "current_verdict")
            ),
            next_experiment_id=_required(question["next_experiment_id"], "next_experiment_id"),
            allowed_verdicts=_strings(question["allowed_verdicts"], "allowed_verdicts"),
            historical_window_role=_required(
                question["historical_window_role"], "historical_window_role"
            ),
            primary_research_start=_required(
                question["primary_research_start"], "primary_research_start"
            ),
            evidence_ladder=tuple(
                EvidenceLadderItem.from_dict(_mapping(item, "evidence_ladder.item"))
                for item in ladder_payload
            ),
            allowed_p0_classes=tuple(
                P0AdmissionClass(item)
                for item in _strings(admission["allowed_classes"], "allowed_classes")
            ),
            default_denied_class=_required(
                admission["default_denied_class"], "default_denied_class"
            ),
            required_p0_fields=_strings(admission["required_fields"], "required_fields"),
            no_automatic_successor=_bool(
                admission["no_automatic_successor"], "no_automatic_successor"
            ),
            phase_ready_conditions=_strings(phase["ready_conditions"], "ready_conditions"),
            next_p0_when_ready=P0AdmissionClass(
                _required(phase["next_p0_when_ready"], "next_p0_when_ready")
            ),
            drift_status=_required(phase["drift_status"], "drift_status"),
            reopen_allowed_triggers=_strings(reopen["allowed_triggers"], "allowed_triggers"),
            reopen_forbidden_reasons=_strings(reopen["forbidden_reasons"], "forbidden_reasons"),
            l0_sections=_strings(reader["l0_sections"], "l0_sections"),
            l1_entry_label_zh=_required(reader["l1_entry_label_zh"], "l1_entry_label_zh"),
            audit_entry_label_zh=_required(reader["audit_entry_label_zh"], "audit_entry_label_zh"),
            l0_forbidden_payloads=_strings(
                reader["l0_forbidden_payloads"], "l0_forbidden_payloads"
            ),
            next_experiment_zh=_required(reader["next_experiment_zh"], "next_experiment_zh"),
            stop_condition_zh=_required(reader["stop_condition_zh"], "stop_condition_zh"),
            prohibited_inference_zh=_required(
                reader["prohibited_inference_zh"], "prohibited_inference_zh"
            ),
            empirical_run_authorized=_bool(
                safety["empirical_run_authorized"], "empirical_run_authorized"
            ),
            quantconnect_action_authorized=_bool(
                safety["quantconnect_action_authorized"], "quantconnect_action_authorized"
            ),
            external_provider_action_authorized=_bool(
                safety["external_provider_action_authorized"], "external_provider_action_authorized"
            ),
            investment_conclusion_generated=_bool(
                safety["investment_conclusion_generated"], "investment_conclusion_generated"
            ),
            production_effect=_required(safety["production_effect"], "production_effect"),
            broker_action=_required(safety["broker_action"], "broker_action"),
        )

    @classmethod
    def from_yaml_bytes(cls, raw: bytes) -> EvidenceFirstResearchPortfolio:
        payload = yaml.safe_load(raw.decode("utf-8"))
        return cls.from_dict(_mapping(payload, "root"))


def load_evidence_first_research_portfolio(
    *, repository_root: Path
) -> EvidenceFirstResearchPortfolio:
    return EvidenceFirstResearchPortfolio.from_yaml_bytes(
        (repository_root / "config/research/evidence_first_research_portfolio_v1.yaml").read_bytes()
    )


def load_projected_evidence_first_research_portfolio(
    *, repository_root: Path
) -> EvidenceFirstResearchPortfolio:
    """Project a validated terminal result without mutating the frozen portfolio bytes."""

    base_path = repository_root / "config/research/evidence_first_research_portfolio_v1.yaml"
    base_raw = base_path.read_bytes()
    base = EvidenceFirstResearchPortfolio.from_yaml_bytes(base_raw)
    admission_path = repository_root / _RESULT_ADMISSION_PATH
    if not admission_path.is_file():
        return base
    if hashlib.sha256(base_raw).hexdigest() != _FROZEN_BASE_FILE_SHA256:
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_FROZEN_BASE_IDENTITY_DRIFT")

    admission = _mapping(
        load_strict_yaml_text(
            admission_path.read_text(encoding="utf-8"), label=str(_RESULT_ADMISSION_PATH)
        ),
        "result_admission",
    )
    _validate_result_admission_header(admission)
    for field in ("authorization_binding", "manifest_binding"):
        _validate_file_binding(
            repository_root=repository_root,
            binding=_mapping(admission.get(field), field),
            field=field,
        )
    evidence = admission.get("evidence_bindings")
    if not isinstance(evidence, list) or not evidence:
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_EVIDENCE_BINDINGS_INVALID")
    bindings: dict[str, Mapping[str, Any]] = {}
    for item in evidence:
        binding = _mapping(item, "evidence_binding")
        role = _required(binding.get("role"), "evidence_binding.role")
        if role in bindings:
            raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_EVIDENCE_ROLE_DUPLICATE")
        _validate_file_binding(
            repository_root=repository_root,
            binding=binding,
            field=f"evidence_binding:{role}",
        )
        bindings[role] = binding
    if set(bindings) != {
        "RUN_ATTEMPT_CONSUMPTION",
        "MANIFEST_REPLAY",
        "CANONICAL_DQ",
        "INDEPENDENT_REPLAY",
        "AGGREGATE_RESULT",
    }:
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_EVIDENCE_ROLES_INVALID")

    aggregate_path = _repository_file(
        repository_root,
        _required(bindings["AGGREGATE_RESULT"].get("path"), "aggregate_result.path"),
        "aggregate_result.path",
    )
    aggregate = _mapping(
        json.loads(aggregate_path.read_text(encoding="utf-8")), "aggregate_result"
    )
    _validate_retain_aggregate(admission=admission, aggregate=aggregate, base=base)
    return _project_retain(base=base, aggregate=aggregate)


def _validate_result_admission_header(admission: Mapping[str, Any]) -> None:
    expected = {
        "schema_version": "frozen_signal_value_confirmation_result_admission.v1",
        "admission_id": "frozen_signal_value_confirmation_result_admission_v1",
        "admission_version": "1.0.0",
        "status": "TECHNICALLY_VALIDATED_AGGREGATE_RETAIN_ADMITTED",
        "task_id": "TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1",
        "scope": "BOUNDED_DATA_RESEARCH_CONFIRMATION_RESULT",
        "authorization_state": "EXACT_PREAUTHORIZED",
        "technical_validation_state": "PASS",
    }
    if any(admission.get(key) != value for key, value in expected.items()):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_ADMISSION_HEADER_INVALID")


def _validate_file_binding(
    *, repository_root: Path, binding: Mapping[str, Any], field: str
) -> None:
    path = _repository_file(
        repository_root, _required(binding.get("path"), f"{field}.path"), f"{field}.path"
    )
    expected_sha256 = _required(binding.get("file_sha256"), f"{field}.file_sha256")
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256:
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_RESULT_BINDING_DRIFT:{field}")


def _repository_file(repository_root: Path, relative: str, field: str) -> Path:
    root = repository_root.resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise EvidenceFirstPortfolioError(f"EVIDENCE_FIRST_RESULT_FILE_INVALID:{field}")
    return path


def _validate_retain_aggregate(
    *,
    admission: Mapping[str, Any],
    aggregate: Mapping[str, Any],
    base: EvidenceFirstResearchPortfolio,
) -> None:
    result = _mapping(admission.get("result"), "result_admission.result")
    run_accounting = _mapping(admission.get("run_accounting"), "result_admission.run_accounting")
    dq_pit = _mapping(admission.get("dq_pit"), "result_admission.dq_pit")
    interpretation = _mapping(
        admission.get("interpretation_boundary"), "result_admission.interpretation_boundary"
    )
    safety = _mapping(admission.get("safety"), "result_admission.safety")
    expected_ones = {
        "manifest_replays",
        "canonical_dq_runs",
        "local_signal_value_confirmations",
        "independent_replays",
    }
    expected_zeros = {
        "data_downloads",
        "cache_mutations",
        "quantconnect_actions",
        "option_backtests",
        "external_provider_actions",
        "orders",
        "fills",
        "positions",
    }
    if any(run_accounting.get(key) != 1 for key in expected_ones) or any(
        run_accounting.get(key) != 0 for key in expected_zeros
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_RUN_ACCOUNTING_INVALID")
    if (
        dq_pit.get("status") != "PASS"
        or dq_pit.get("error_count") != 0
        or dq_pit.get("warning_count") != 0
        or dq_pit.get("requested_start") != base.primary_research_start
        or dq_pit.get("evaluated_start") != base.primary_research_start
        or dq_pit.get("requested_end") != "2025-12-02"
        or dq_pit.get("evaluated_end") != "2025-12-02"
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_DQ_INVALID")
    if (
        result.get("verdict") != "RETAIN"
        or result.get("stop_action") != _RETAIN_STOP_ACTION
        or result.get("historical_window_role") != base.historical_window_role
        or result.get("signal_session_count") != 1202
        or result.get("return_interval_count") != 1201
        or result.get("independent_replay_status") != "PASS"
        or float(result.get("net_total_return_difference_percentage_points", 0.0)) <= 0.0
        or float(result.get("max_drawdown_magnitude_delta_percentage_points", 1.0)) > 0.0
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_RETAIN_SEMANTICS_INVALID")
    if (
        interpretation.get("signal_value_retained_for_conditional_implementation_review") is not True
        or interpretation.get("proves_options_implementation_value") is not False
        or interpretation.get("proves_robustness_or_pristine_oos") is not False
        or interpretation.get("investment_conclusion_generated") is not False
        or interpretation.get("automatic_successor_created") is not False
        or interpretation.get("next_legal_action")
        != "OWNER_REVIEW_FOR_CONDITIONAL_OPTIONS_PAIRED_COMPARISON_ONLY"
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_INTERPRETATION_INVALID")
    required_false = {
        "raw_market_payload_exported",
        "raw_option_payload_exported",
        "further_confirmation_authorized",
        "quantconnect_authorized",
        "option_backtest_authorized",
        "provider_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    }
    if (
        safety.get("aggregate_result_only") is not True
        or any(safety.get(key) is not False for key in required_false)
        or safety.get("production_effect") != "none"
        or safety.get("broker_action") != "none"
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_SAFETY_INVALID")
    aggregate_counters = _mapping(aggregate.get("actual_counters"), "aggregate.actual_counters")
    aggregate_dq = _mapping(aggregate.get("canonical_dq"), "aggregate.canonical_dq")
    aggregate_replay = _mapping(aggregate.get("independent_replay"), "aggregate.independent_replay")
    aggregate_estimand = _mapping(aggregate.get("primary_estimand"), "aggregate.primary_estimand")
    aggregate_guard = _mapping(aggregate.get("falsification_guard"), "aggregate.falsification_guard")
    if (
        aggregate.get("schema_version") != "frozen_signal_value_confirmation_result.v1"
        or aggregate.get("status") != "TERMINAL"
        or aggregate.get("task_id") != "TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1"
        or aggregate.get("verdict") != result.get("verdict")
        or aggregate.get("stop_action") != result.get("stop_action")
        or aggregate.get("historical_window_role") != result.get("historical_window_role")
        or aggregate.get("signal_session_count") != result.get("signal_session_count")
        or aggregate.get("return_interval_count") != result.get("return_interval_count")
        or aggregate_dq.get("status") != "PASS"
        or aggregate_replay.get("status") != "PASS"
        or aggregate_estimand.get("net_total_return_difference_percentage_points")
        != result.get("net_total_return_difference_percentage_points")
        or aggregate_guard.get("max_drawdown_magnitude_delta_percentage_points")
        != result.get("max_drawdown_magnitude_delta_percentage_points")
        or aggregate_counters != run_accounting
    ):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_AGGREGATE_MISMATCH")


def _project_retain(
    *, base: EvidenceFirstResearchPortfolio, aggregate: Mapping[str, Any]
) -> EvidenceFirstResearchPortfolio:
    payload = base.to_dict()
    question = _mapping(payload["primary_evidence_question"], "projection.question")
    question["current_verdict"] = EvidenceState.RETAIN.value
    question["next_experiment_id"] = _RETAIN_NEXT_EXPERIMENT
    ladder = payload["evidence_ladder"]
    if not isinstance(ladder, list):
        raise EvidenceFirstPortfolioError("EVIDENCE_FIRST_RESULT_PROJECTION_LADDER_INVALID")
    signal_value = _mapping(ladder[3], "projection.signal_value")
    signal_value["state"] = EvidenceState.RETAIN.value
    estimand = _mapping(aggregate.get("primary_estimand"), "projection.primary_estimand")
    signal_value["explanation_zh"] = (
        "唯一 bounded confirmation 已按预注册同资本 benchmark 完成；净总收益差为 "
        f"+{estimand['net_total_return_difference_percentage_points']} 个百分点，"
        "预注册风险守门未恶化，冻结 reducer 输出 RETAIN。"
    )
    reader = _mapping(payload["reader_entry"], "projection.reader_entry")
    reader["next_experiment_zh"] = _RETAIN_NEXT_EXPERIMENT_ZH
    return EvidenceFirstResearchPortfolio.from_dict(payload)
