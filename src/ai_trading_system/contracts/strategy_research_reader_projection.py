from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar

import yaml

from ai_trading_system.contracts.strategy_research_cited_query import (
    CitedQueryQuestionId,
)

_STABLE_ID = re.compile(r"^[a-z][a-z0-9_]*$")


class ReaderProjectionContractError(ValueError):
    pass


class ReaderProjectionLayer(StrEnum):
    READER_DEFAULT = "READER_DEFAULT"
    RESEARCH_DRILLDOWN = "RESEARCH_DRILLDOWN"
    AUDIT_STRATUM = "AUDIT_STRATUM"


class ReaderSectionId(StrEnum):
    TRUST_STRIP = "TRUST_STRIP"
    WHY_CONTEXT = "WHY_CONTEXT"
    CANONICAL_QUESTIONS = "CANONICAL_QUESTIONS"
    CHANGE_SUMMARY = "CHANGE_SUMMARY"
    CONCLUSION_BOUNDARY = "CONCLUSION_BOUNDARY"
    ACCEPTANCE_AXES = "ACCEPTANCE_AXES"
    FLOW_POSITION = "FLOW_POSITION"
    RESEARCH_DRILLDOWN = "RESEARCH_DRILLDOWN"
    AUDIT_DESTINATIONS = "AUDIT_DESTINATIONS"


class ReaderPageQuestionId(StrEnum):
    CURRENT_RESEARCH_MAINLINE = "CURRENT_RESEARCH_MAINLINE"
    LARGEST_CURRENT_BLOCKER = "LARGEST_CURRENT_BLOCKER"
    ENGINEERING_VS_RESEARCH_EVIDENCE = "ENGINEERING_VS_RESEARCH_EVIDENCE"
    PROHIBITED_INFERENCES = "PROHIBITED_INFERENCES"
    NEXT_OWNER_AND_ACTION = "NEXT_OWNER_AND_ACTION"
    INVESTMENT_ORDER_ENGINE_AUTHORITY = "INVESTMENT_ORDER_ENGINE_AUTHORITY"


class ReaderCausalNodeKind(StrEnum):
    PROBLEM = "PROBLEM"
    CONSTRAINT = "CONSTRAINT"
    CHOICE = "CHOICE"
    EVIDENCE = "EVIDENCE"
    RESULT = "RESULT"
    NEXT_STEP = "NEXT_STEP"


class ReaderCausalEdgeKind(StrEnum):
    BOUNDED_BY = "BOUNDED_BY"
    JUSTIFIES = "JUSTIFIES"
    REQUIRES_EVIDENCE = "REQUIRES_EVIDENCE"
    SUPPORTS = "SUPPORTS"
    LIMITS = "LIMITS"
    TRIGGERS = "TRIGGERS"


def canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _required(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ReaderProjectionContractError(f"READER_PROJECTION_REQUIRED:{field}")
    return normalized


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise ReaderProjectionContractError(
            f"READER_PROJECTION_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReaderProjectionContractError(f"READER_PROJECTION_MAPPING_REQUIRED:{field}")
    return value


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise ReaderProjectionContractError(f"READER_PROJECTION_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _string_tuple(
    value: object,
    field: str,
    *,
    empty_allowed: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ReaderProjectionContractError(f"READER_PROJECTION_LIST_REQUIRED:{field}")
    result = tuple(_required(str(item), field) for item in value)
    if (not result and not empty_allowed) or len(result) != len(set(result)):
        raise ReaderProjectionContractError(f"READER_PROJECTION_LIST_INVALID:{field}")
    return result


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ReaderProjectionContractError(f"READER_PROJECTION_BOOLEAN_REQUIRED:{field}")
    return value


@dataclass(frozen=True)
class ReaderSectionSlot:
    section_id: ReaderSectionId
    layer: ReaderProjectionLayer
    default_visible: bool
    reader_purpose_zh: str
    always_visible_fields: tuple[str, ...]

    def __post_init__(self) -> None:
        _required(self.reader_purpose_zh, f"section.{self.section_id}.reader_purpose_zh")
        if len(self.always_visible_fields) != len(set(self.always_visible_fields)):
            raise ReaderProjectionContractError(
                f"READER_PROJECTION_SECTION_FIELD_DUPLICATE:{self.section_id}"
            )
        if any(not _STABLE_ID.fullmatch(item) for item in self.always_visible_fields):
            raise ReaderProjectionContractError(
                f"READER_PROJECTION_SECTION_FIELD_INVALID:{self.section_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "section_id": self.section_id.value,
            "layer": self.layer.value,
            "default_visible": self.default_visible,
            "reader_purpose_zh": self.reader_purpose_zh,
            "always_visible_fields": list(self.always_visible_fields),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderSectionSlot:
        _exact_keys(
            payload,
            {
                "section_id",
                "layer",
                "default_visible",
                "reader_purpose_zh",
                "always_visible_fields",
            },
            "section",
        )
        return cls(
            section_id=ReaderSectionId(str(payload["section_id"])),
            layer=ReaderProjectionLayer(str(payload["layer"])),
            default_visible=_bool(payload["default_visible"], "section.default_visible"),
            reader_purpose_zh=str(payload["reader_purpose_zh"]),
            always_visible_fields=_string_tuple(
                payload["always_visible_fields"],
                "section.always_visible_fields",
                empty_allowed=True,
            ),
        )


@dataclass(frozen=True)
class ReaderQuestionMapping:
    page_question_id: ReaderPageQuestionId
    cited_question_ids: tuple[CitedQueryQuestionId, ...]

    def __post_init__(self) -> None:
        if not self.cited_question_ids or len(self.cited_question_ids) != len(
            set(self.cited_question_ids)
        ):
            raise ReaderProjectionContractError(
                f"READER_PROJECTION_QUESTION_MAPPING_INVALID:{self.page_question_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "page_question_id": self.page_question_id.value,
            "cited_question_ids": [item.value for item in self.cited_question_ids],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderQuestionMapping:
        _exact_keys(payload, {"page_question_id", "cited_question_ids"}, "question")
        return cls(
            page_question_id=ReaderPageQuestionId(str(payload["page_question_id"])),
            cited_question_ids=tuple(
                CitedQueryQuestionId(item)
                for item in _string_tuple(
                    payload["cited_question_ids"], "question.cited_question_ids"
                )
            ),
        )


@dataclass(frozen=True)
class ReaderCausalEdgeSpec:
    source_node: ReaderCausalNodeKind
    relation: ReaderCausalEdgeKind
    target_node: ReaderCausalNodeKind

    def __post_init__(self) -> None:
        if self.source_node is self.target_node:
            raise ReaderProjectionContractError("READER_PROJECTION_CAUSAL_SELF_EDGE")

    def to_dict(self) -> dict[str, str]:
        return {
            "source_node": self.source_node.value,
            "relation": self.relation.value,
            "target_node": self.target_node.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderCausalEdgeSpec:
        _exact_keys(payload, {"source_node", "relation", "target_node"}, "causal_edge")
        return cls(
            source_node=ReaderCausalNodeKind(str(payload["source_node"])),
            relation=ReaderCausalEdgeKind(str(payload["relation"])),
            target_node=ReaderCausalNodeKind(str(payload["target_node"])),
        )


@dataclass(frozen=True)
class ReaderTermInteractionContract:
    first_occurrence_scope: str
    first_occurrence_focusable: bool
    repeated_occurrence_additional_tab_stop: bool
    hover_focus_tap_same_definition: bool
    accessible_description_required: bool
    escape_closes: bool
    focus_context_restored: bool
    title_only_allowed: bool
    nested_interactive_control_allowed: bool
    short_definition_route: str
    long_definition_route: str
    raw_identifier_route: str
    unavailable_interaction_fallback: str

    def __post_init__(self) -> None:
        expected = {
            "first_occurrence_scope": "PER_READER_SECTION",
            "first_occurrence_focusable": True,
            "repeated_occurrence_additional_tab_stop": False,
            "hover_focus_tap_same_definition": True,
            "accessible_description_required": True,
            "escape_closes": True,
            "focus_context_restored": True,
            "title_only_allowed": False,
            "nested_interactive_control_allowed": False,
            "short_definition_route": "INLINE_TOOLTIP",
            "long_definition_route": "GLOSSARY_OR_RESEARCH_DRILLDOWN",
            "raw_identifier_route": "AUDIT_STRATUM",
            "unavailable_interaction_fallback": "VISIBLE_INLINE_OR_GLOSSARY",
        }
        if self.to_dict() != expected:
            raise ReaderProjectionContractError("READER_PROJECTION_TERM_INTERACTION_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "first_occurrence_scope": self.first_occurrence_scope,
            "first_occurrence_focusable": self.first_occurrence_focusable,
            "repeated_occurrence_additional_tab_stop": self.repeated_occurrence_additional_tab_stop,
            "hover_focus_tap_same_definition": self.hover_focus_tap_same_definition,
            "accessible_description_required": self.accessible_description_required,
            "escape_closes": self.escape_closes,
            "focus_context_restored": self.focus_context_restored,
            "title_only_allowed": self.title_only_allowed,
            "nested_interactive_control_allowed": self.nested_interactive_control_allowed,
            "short_definition_route": self.short_definition_route,
            "long_definition_route": self.long_definition_route,
            "raw_identifier_route": self.raw_identifier_route,
            "unavailable_interaction_fallback": self.unavailable_interaction_fallback,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderTermInteractionContract:
        expected = {
            "first_occurrence_scope",
            "first_occurrence_focusable",
            "repeated_occurrence_additional_tab_stop",
            "hover_focus_tap_same_definition",
            "accessible_description_required",
            "escape_closes",
            "focus_context_restored",
            "title_only_allowed",
            "nested_interactive_control_allowed",
            "short_definition_route",
            "long_definition_route",
            "raw_identifier_route",
            "unavailable_interaction_fallback",
        }
        _exact_keys(payload, expected, "term_interaction")
        return cls(
            first_occurrence_scope=str(payload["first_occurrence_scope"]),
            first_occurrence_focusable=_bool(
                payload["first_occurrence_focusable"],
                "term_interaction.first_occurrence_focusable",
            ),
            repeated_occurrence_additional_tab_stop=_bool(
                payload["repeated_occurrence_additional_tab_stop"],
                "term_interaction.repeated_occurrence_additional_tab_stop",
            ),
            hover_focus_tap_same_definition=_bool(
                payload["hover_focus_tap_same_definition"],
                "term_interaction.hover_focus_tap_same_definition",
            ),
            accessible_description_required=_bool(
                payload["accessible_description_required"],
                "term_interaction.accessible_description_required",
            ),
            escape_closes=_bool(payload["escape_closes"], "term_interaction.escape_closes"),
            focus_context_restored=_bool(
                payload["focus_context_restored"],
                "term_interaction.focus_context_restored",
            ),
            title_only_allowed=_bool(
                payload["title_only_allowed"], "term_interaction.title_only_allowed"
            ),
            nested_interactive_control_allowed=_bool(
                payload["nested_interactive_control_allowed"],
                "term_interaction.nested_interactive_control_allowed",
            ),
            short_definition_route=str(payload["short_definition_route"]),
            long_definition_route=str(payload["long_definition_route"]),
            raw_identifier_route=str(payload["raw_identifier_route"]),
            unavailable_interaction_fallback=str(
                payload["unavailable_interaction_fallback"]
            ),
        )


@dataclass(frozen=True)
class ReaderAttentionBudget:
    first_screen_required_fields: tuple[str, ...]
    max_reader_decisions_per_l0_card: int
    max_primary_disclosures_per_l1_card: int
    max_reader_disclosure_depth: int
    nested_disclosures_allowed: bool
    glossary_position: str
    audit_payload_default_visible: bool

    EXPECTED_FIRST_SCREEN_FIELDS: ClassVar[tuple[str, ...]] = (
        "trust_strip",
        "current_problem",
        "why_chain_summary",
        "conclusion_boundary",
        "largest_blocker",
        "prohibited_inference",
        "next_legal_action",
    )

    def __post_init__(self) -> None:
        if self.first_screen_required_fields != self.EXPECTED_FIRST_SCREEN_FIELDS:
            raise ReaderProjectionContractError("READER_PROJECTION_FIRST_SCREEN_BUDGET_INVALID")
        if (
            self.max_reader_decisions_per_l0_card != 1
            or self.max_primary_disclosures_per_l1_card != 1
            or self.max_reader_disclosure_depth != 1
            or self.nested_disclosures_allowed
            or self.glossary_position != "AFTER_READER_MAINLINE"
            or self.audit_payload_default_visible
        ):
            raise ReaderProjectionContractError("READER_PROJECTION_DISCLOSURE_BUDGET_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "first_screen_required_fields": list(self.first_screen_required_fields),
            "max_reader_decisions_per_l0_card": self.max_reader_decisions_per_l0_card,
            "max_primary_disclosures_per_l1_card": self.max_primary_disclosures_per_l1_card,
            "max_reader_disclosure_depth": self.max_reader_disclosure_depth,
            "nested_disclosures_allowed": self.nested_disclosures_allowed,
            "glossary_position": self.glossary_position,
            "audit_payload_default_visible": self.audit_payload_default_visible,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderAttentionBudget:
        expected = {
            "first_screen_required_fields",
            "max_reader_decisions_per_l0_card",
            "max_primary_disclosures_per_l1_card",
            "max_reader_disclosure_depth",
            "nested_disclosures_allowed",
            "glossary_position",
            "audit_payload_default_visible",
        }
        _exact_keys(payload, expected, "attention_budget")
        return cls(
            first_screen_required_fields=_string_tuple(
                payload["first_screen_required_fields"],
                "attention_budget.first_screen_required_fields",
            ),
            max_reader_decisions_per_l0_card=int(
                str(payload["max_reader_decisions_per_l0_card"])
            ),
            max_primary_disclosures_per_l1_card=int(
                str(payload["max_primary_disclosures_per_l1_card"])
            ),
            max_reader_disclosure_depth=int(str(payload["max_reader_disclosure_depth"])),
            nested_disclosures_allowed=_bool(
                payload["nested_disclosures_allowed"],
                "attention_budget.nested_disclosures_allowed",
            ),
            glossary_position=str(payload["glossary_position"]),
            audit_payload_default_visible=_bool(
                payload["audit_payload_default_visible"],
                "attention_budget.audit_payload_default_visible",
            ),
        )


@dataclass(frozen=True)
class ReaderSourceBindingContract:
    required: bool
    renderer_inference_allowed: bool
    missing_causal_fact_status: str
    node_required_fields: tuple[str, ...]
    edge_required_fields: tuple[str, ...]
    exact_identity_fields: tuple[str, ...]
    canonical_serialization: str

    def __post_init__(self) -> None:
        required_source_fields = ("source_ref_id", "source_locator", "source_sha256")
        if (
            not self.required
            or self.renderer_inference_allowed
            or self.missing_causal_fact_status != "INSUFFICIENT"
            or self.node_required_fields != required_source_fields
            or self.edge_required_fields != required_source_fields
            or self.exact_identity_fields
            != ("source_commit", "contract_sha256", "html_sha256", "manifest_sha256")
            or self.canonical_serialization != "UTF8_SORTED_KEY_JSON_V1"
        ):
            raise ReaderProjectionContractError("READER_PROJECTION_SOURCE_BINDING_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "required": self.required,
            "renderer_inference_allowed": self.renderer_inference_allowed,
            "missing_causal_fact_status": self.missing_causal_fact_status,
            "node_required_fields": list(self.node_required_fields),
            "edge_required_fields": list(self.edge_required_fields),
            "exact_identity_fields": list(self.exact_identity_fields),
            "canonical_serialization": self.canonical_serialization,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderSourceBindingContract:
        expected = {
            "required",
            "renderer_inference_allowed",
            "missing_causal_fact_status",
            "node_required_fields",
            "edge_required_fields",
            "exact_identity_fields",
            "canonical_serialization",
        }
        _exact_keys(payload, expected, "source_binding")
        return cls(
            required=_bool(payload["required"], "source_binding.required"),
            renderer_inference_allowed=_bool(
                payload["renderer_inference_allowed"],
                "source_binding.renderer_inference_allowed",
            ),
            missing_causal_fact_status=str(payload["missing_causal_fact_status"]),
            node_required_fields=_string_tuple(
                payload["node_required_fields"], "source_binding.node_required_fields"
            ),
            edge_required_fields=_string_tuple(
                payload["edge_required_fields"], "source_binding.edge_required_fields"
            ),
            exact_identity_fields=_string_tuple(
                payload["exact_identity_fields"], "source_binding.exact_identity_fields"
            ),
            canonical_serialization=str(payload["canonical_serialization"]),
        )


@dataclass(frozen=True)
class ReaderProjectionInterfaces:
    state_projection: str
    date_change_projection: str
    accessibility_validation: str
    remediation_handoff: str

    def __post_init__(self) -> None:
        expected = {
            "state_projection": "reader_state_projection.v1",
            "date_change_projection": "reader_date_change_projection.v1",
            "accessibility_validation": "reader_accessibility_validation.v1",
            "remediation_handoff": "reader_projection_remediation_handoff.v1",
        }
        if self.to_dict() != expected:
            raise ReaderProjectionContractError("READER_PROJECTION_INTERFACE_INVALID")

    def to_dict(self) -> dict[str, str]:
        return {
            "state_projection": self.state_projection,
            "date_change_projection": self.date_change_projection,
            "accessibility_validation": self.accessibility_validation,
            "remediation_handoff": self.remediation_handoff,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderProjectionInterfaces:
        expected = {
            "state_projection",
            "date_change_projection",
            "accessibility_validation",
            "remediation_handoff",
        }
        _exact_keys(payload, expected, "interfaces")
        return cls(**{key: str(payload[key]) for key in expected})


@dataclass(frozen=True)
class ReaderProjectionSafety:
    primary_research_start: str
    human_acceptance_auto_upgrade_allowed: bool
    strategy_conclusion_generated: bool
    order_authorized: bool
    real_engine_authorized: bool
    production_effect: str
    broker_action: str

    def __post_init__(self) -> None:
        expected: dict[str, object] = {
            "primary_research_start": "2021-02-22",
            "human_acceptance_auto_upgrade_allowed": False,
            "strategy_conclusion_generated": False,
            "order_authorized": False,
            "real_engine_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        if self.to_dict() != expected:
            raise ReaderProjectionContractError("READER_PROJECTION_SAFETY_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "primary_research_start": self.primary_research_start,
            "human_acceptance_auto_upgrade_allowed": self.human_acceptance_auto_upgrade_allowed,
            "strategy_conclusion_generated": self.strategy_conclusion_generated,
            "order_authorized": self.order_authorized,
            "real_engine_authorized": self.real_engine_authorized,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderProjectionSafety:
        expected = {
            "primary_research_start",
            "human_acceptance_auto_upgrade_allowed",
            "strategy_conclusion_generated",
            "order_authorized",
            "real_engine_authorized",
            "production_effect",
            "broker_action",
        }
        _exact_keys(payload, expected, "safety")
        return cls(
            primary_research_start=str(payload["primary_research_start"]),
            human_acceptance_auto_upgrade_allowed=_bool(
                payload["human_acceptance_auto_upgrade_allowed"],
                "safety.human_acceptance_auto_upgrade_allowed",
            ),
            strategy_conclusion_generated=_bool(
                payload["strategy_conclusion_generated"],
                "safety.strategy_conclusion_generated",
            ),
            order_authorized=_bool(payload["order_authorized"], "safety.order_authorized"),
            real_engine_authorized=_bool(
                payload["real_engine_authorized"], "safety.real_engine_authorized"
            ),
            production_effect=str(payload["production_effect"]),
            broker_action=str(payload["broker_action"]),
        )


@dataclass(frozen=True)
class StrategyResearchReaderProjectionContract:
    schema_version: ClassVar[str] = "atlas_reader_projection_contract.v1"

    contract_id: str
    contract_version: str
    status: str
    owner: str
    section_slots: tuple[ReaderSectionSlot, ...]
    question_mappings: tuple[ReaderQuestionMapping, ...]
    causal_nodes: tuple[ReaderCausalNodeKind, ...]
    causal_edges: tuple[ReaderCausalEdgeSpec, ...]
    term_interaction: ReaderTermInteractionContract
    attention_budget: ReaderAttentionBudget
    source_binding: ReaderSourceBindingContract
    interfaces: ReaderProjectionInterfaces
    safety: ReaderProjectionSafety

    EXPECTED_SECTION_ORDER: ClassVar[tuple[ReaderSectionId, ...]] = tuple(ReaderSectionId)
    EXPECTED_CAUSAL_NODES: ClassVar[tuple[ReaderCausalNodeKind, ...]] = tuple(
        ReaderCausalNodeKind
    )
    EXPECTED_CAUSAL_EDGES: ClassVar[
        tuple[tuple[ReaderCausalNodeKind, ReaderCausalEdgeKind, ReaderCausalNodeKind], ...]
    ] = (
        (
            ReaderCausalNodeKind.PROBLEM,
            ReaderCausalEdgeKind.BOUNDED_BY,
            ReaderCausalNodeKind.CONSTRAINT,
        ),
        (
            ReaderCausalNodeKind.CONSTRAINT,
            ReaderCausalEdgeKind.JUSTIFIES,
            ReaderCausalNodeKind.CHOICE,
        ),
        (
            ReaderCausalNodeKind.CHOICE,
            ReaderCausalEdgeKind.REQUIRES_EVIDENCE,
            ReaderCausalNodeKind.EVIDENCE,
        ),
        (ReaderCausalNodeKind.EVIDENCE, ReaderCausalEdgeKind.SUPPORTS, ReaderCausalNodeKind.RESULT),
        (ReaderCausalNodeKind.RESULT, ReaderCausalEdgeKind.LIMITS, ReaderCausalNodeKind.NEXT_STEP),
        (
            ReaderCausalNodeKind.RESULT,
            ReaderCausalEdgeKind.TRIGGERS,
            ReaderCausalNodeKind.NEXT_STEP,
        ),
    )

    def __post_init__(self) -> None:
        if not _STABLE_ID.fullmatch(self.contract_id):
            raise ReaderProjectionContractError("READER_PROJECTION_CONTRACT_ID_INVALID")
        for field in ("contract_version", "owner"):
            _required(str(getattr(self, field)), field)
        if self.status != "REVIEWED_SERIAL_CONTRACT":
            raise ReaderProjectionContractError("READER_PROJECTION_STATUS_INVALID")
        if tuple(item.section_id for item in self.section_slots) != self.EXPECTED_SECTION_ORDER:
            raise ReaderProjectionContractError("READER_PROJECTION_SECTION_ORDER_INVALID")
        expected_layers = (
            *(ReaderProjectionLayer.READER_DEFAULT for _ in range(7)),
            ReaderProjectionLayer.RESEARCH_DRILLDOWN,
            ReaderProjectionLayer.AUDIT_STRATUM,
        )
        if tuple(item.layer for item in self.section_slots) != expected_layers:
            raise ReaderProjectionContractError("READER_PROJECTION_SECTION_LAYER_INVALID")
        if tuple(item.default_visible for item in self.section_slots) != (
            *(True for _ in range(7)),
            False,
            False,
        ):
            raise ReaderProjectionContractError("READER_PROJECTION_SECTION_VISIBILITY_INVALID")
        page_questions = tuple(item.page_question_id for item in self.question_mappings)
        if page_questions != tuple(ReaderPageQuestionId):
            raise ReaderProjectionContractError("READER_PROJECTION_PAGE_QUESTION_SET_INVALID")
        cited_questions = {
            cited for mapping in self.question_mappings for cited in mapping.cited_question_ids
        }
        if cited_questions != set(CitedQueryQuestionId):
            raise ReaderProjectionContractError("READER_PROJECTION_CITED_QUESTION_SET_INVALID")
        if self.causal_nodes != self.EXPECTED_CAUSAL_NODES:
            raise ReaderProjectionContractError("READER_PROJECTION_CAUSAL_NODE_ORDER_INVALID")
        actual_edges = tuple(
            (item.source_node, item.relation, item.target_node) for item in self.causal_edges
        )
        if actual_edges != self.EXPECTED_CAUSAL_EDGES:
            raise ReaderProjectionContractError("READER_PROJECTION_CAUSAL_EDGE_SET_INVALID")
        visible_fields = {
            field
            for section in self.section_slots
            if section.default_visible
            for field in section.always_visible_fields
        }
        required_visible = {
            "current_problem",
            "largest_blocker",
            "conclusion_boundary",
            "critical_risk",
            "prohibited_inference",
            "next_legal_action",
            "strategy_conclusion_pass_count",
            "production_effect",
            "broker_action",
        }
        if not required_visible <= visible_fields:
            raise ReaderProjectionContractError("READER_PROJECTION_ALWAYS_VISIBLE_FIELD_MISSING")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "status": self.status,
            "owner": self.owner,
            "section_slots": [item.to_dict() for item in self.section_slots],
            "question_mappings": [item.to_dict() for item in self.question_mappings],
            "causal_nodes": [item.value for item in self.causal_nodes],
            "causal_edges": [item.to_dict() for item in self.causal_edges],
            "term_interaction": self.term_interaction.to_dict(),
            "attention_budget": self.attention_budget.to_dict(),
            "source_binding": self.source_binding.to_dict(),
            "interfaces": self.interfaces.to_dict(),
            "safety": self.safety.to_dict(),
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, object]
    ) -> StrategyResearchReaderProjectionContract:
        expected = {
            "schema_version",
            "contract_id",
            "contract_version",
            "status",
            "owner",
            "section_slots",
            "question_mappings",
            "causal_nodes",
            "causal_edges",
            "term_interaction",
            "attention_budget",
            "source_binding",
            "interfaces",
            "safety",
        }
        _exact_keys(payload, expected, "contract")
        if payload["schema_version"] != cls.schema_version:
            raise ReaderProjectionContractError("READER_PROJECTION_SCHEMA_INVALID")
        return cls(
            contract_id=str(payload["contract_id"]),
            contract_version=str(payload["contract_version"]),
            status=str(payload["status"]),
            owner=str(payload["owner"]),
            section_slots=tuple(
                ReaderSectionSlot.from_dict(item)
                for item in _mapping_tuple(payload["section_slots"], "contract.section_slots")
            ),
            question_mappings=tuple(
                ReaderQuestionMapping.from_dict(item)
                for item in _mapping_tuple(
                    payload["question_mappings"], "contract.question_mappings"
                )
            ),
            causal_nodes=tuple(
                ReaderCausalNodeKind(item)
                for item in _string_tuple(payload["causal_nodes"], "contract.causal_nodes")
            ),
            causal_edges=tuple(
                ReaderCausalEdgeSpec.from_dict(item)
                for item in _mapping_tuple(payload["causal_edges"], "contract.causal_edges")
            ),
            term_interaction=ReaderTermInteractionContract.from_dict(
                _mapping(payload["term_interaction"], "contract.term_interaction")
            ),
            attention_budget=ReaderAttentionBudget.from_dict(
                _mapping(payload["attention_budget"], "contract.attention_budget")
            ),
            source_binding=ReaderSourceBindingContract.from_dict(
                _mapping(payload["source_binding"], "contract.source_binding")
            ),
            interfaces=ReaderProjectionInterfaces.from_dict(
                _mapping(payload["interfaces"], "contract.interfaces")
            ),
            safety=ReaderProjectionSafety.from_dict(
                _mapping(payload["safety"], "contract.safety")
            ),
        )

    @classmethod
    def from_yaml_bytes(
        cls, payload: bytes
    ) -> StrategyResearchReaderProjectionContract:
        try:
            decoded = yaml.safe_load(payload.decode("utf-8"))
        except (UnicodeDecodeError, yaml.YAMLError) as exc:
            raise ReaderProjectionContractError("READER_PROJECTION_YAML_INVALID") from exc
        return cls.from_dict(_mapping(decoded, "contract"))


__all__ = [
    "ReaderAttentionBudget",
    "ReaderCausalEdgeKind",
    "ReaderCausalEdgeSpec",
    "ReaderCausalNodeKind",
    "ReaderPageQuestionId",
    "ReaderProjectionContractError",
    "ReaderProjectionInterfaces",
    "ReaderProjectionLayer",
    "ReaderProjectionSafety",
    "ReaderQuestionMapping",
    "ReaderSectionId",
    "ReaderSectionSlot",
    "ReaderSourceBindingContract",
    "ReaderTermInteractionContract",
    "StrategyResearchReaderProjectionContract",
    "canonical_json_bytes",
]
