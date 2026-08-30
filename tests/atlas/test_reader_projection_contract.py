from __future__ import annotations

import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from ai_trading_system.contracts.strategy_research_cited_query import (
    CitedQueryQuestionId,
)
from ai_trading_system.contracts.strategy_research_reader_projection import (
    ReaderCausalEdgeKind,
    ReaderCausalNodeKind,
    ReaderPageQuestionId,
    ReaderProjectionContractError,
    ReaderProjectionLayer,
    ReaderSectionId,
    StrategyResearchReaderProjectionContract,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = PROJECT_ROOT / "config/atlas/reader_projection_contract.yaml"


def _payload() -> dict[str, object]:
    decoded = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(decoded, dict)
    return decoded


def _contract() -> StrategyResearchReaderProjectionContract:
    return StrategyResearchReaderProjectionContract.from_yaml_bytes(CONTRACT_PATH.read_bytes())


def test_reader_projection_contract_roundtrips_deterministically() -> None:
    contract = _contract()

    assert contract.contract_id == "atlas_reader_evidence_first_entry_v2"
    assert contract.status == "REVIEWED_SERIAL_CONTRACT"
    assert (
        StrategyResearchReaderProjectionContract.from_dict(contract.to_dict()).canonical_bytes
        == contract.canonical_bytes
    )
    assert contract.content_sha256 == hashlib.sha256(contract.canonical_bytes).hexdigest()


def test_reader_projection_contract_freezes_why_first_section_order() -> None:
    contract = _contract()

    assert tuple(item.section_id for item in contract.section_slots) == tuple(ReaderSectionId)
    assert tuple(item.layer for item in contract.section_slots) == (
        ReaderProjectionLayer.READER_DEFAULT,
        ReaderProjectionLayer.READER_DEFAULT,
        *(ReaderProjectionLayer.RESEARCH_DRILLDOWN for _ in range(6)),
        ReaderProjectionLayer.AUDIT_STRATUM,
    )
    assert all(item.default_visible for item in contract.section_slots[:2])
    assert all(not item.default_visible for item in contract.section_slots[2:])
    assert contract.attention_budget.first_screen_required_fields == (
        "freshness",
        "evidence_date",
        "primary_evidence_question",
        "current_verdict",
        "evidence_ladder",
        "next_experiment",
        "stop_condition",
        "prohibited_inference",
        "production_effect",
        "broker_action",
    )
    assert contract.attention_budget.glossary_position == "AFTER_READER_MAINLINE"


def test_reader_projection_contract_maps_six_questions_to_five_cited_questions() -> None:
    contract = _contract()

    assert tuple(item.page_question_id for item in contract.question_mappings) == tuple(
        ReaderPageQuestionId
    )
    assert {
        cited for mapping in contract.question_mappings for cited in mapping.cited_question_ids
    } == set(CitedQueryQuestionId)


def test_reader_projection_contract_requires_source_bound_causal_chain() -> None:
    contract = _contract()

    assert contract.causal_nodes == tuple(ReaderCausalNodeKind)
    assert tuple(item.relation for item in contract.causal_edges) == tuple(ReaderCausalEdgeKind)
    assert contract.source_binding.required is True
    assert contract.source_binding.renderer_inference_allowed is False
    assert contract.source_binding.missing_causal_fact_status == "INSUFFICIENT"
    assert contract.source_binding.node_required_fields == (
        "source_ref_id",
        "source_locator",
        "source_sha256",
    )
    assert contract.source_binding.edge_required_fields == (
        "source_ref_id",
        "source_locator",
        "source_sha256",
    )


def test_reader_projection_contract_requires_accessible_inline_term_interaction() -> None:
    interaction = _contract().term_interaction

    assert interaction.first_occurrence_scope == "PER_READER_SECTION"
    assert interaction.first_occurrence_focusable is True
    assert interaction.repeated_occurrence_additional_tab_stop is False
    assert interaction.hover_focus_tap_same_definition is True
    assert interaction.accessible_description_required is True
    assert interaction.escape_closes is True
    assert interaction.focus_context_restored is True
    assert interaction.title_only_allowed is False
    assert interaction.nested_interactive_control_allowed is False
    assert interaction.raw_identifier_route == "AUDIT_STRATUM"
    assert interaction.unavailable_interaction_fallback == "VISIBLE_INLINE_OR_GLOSSARY"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda payload: payload["section_slots"].reverse(),
            "READER_PROJECTION_SECTION_ORDER_INVALID",
        ),
        (
            lambda payload: payload["causal_nodes"].remove("PROBLEM"),
            "READER_PROJECTION_CAUSAL_NODE_ORDER_INVALID",
        ),
        (
            lambda payload: payload["causal_edges"].pop(),
            "READER_PROJECTION_CAUSAL_EDGE_SET_INVALID",
        ),
        (
            lambda payload: payload["source_binding"].__setitem__(
                "renderer_inference_allowed", True
            ),
            "READER_PROJECTION_SOURCE_BINDING_INVALID",
        ),
        (
            lambda payload: payload["term_interaction"].__setitem__("title_only_allowed", True),
            "READER_PROJECTION_TERM_INTERACTION_INVALID",
        ),
        (
            lambda payload: payload["term_interaction"].__setitem__(
                "first_occurrence_focusable", False
            ),
            "READER_PROJECTION_TERM_INTERACTION_INVALID",
        ),
        (
            lambda payload: payload["attention_budget"].__setitem__(
                "nested_disclosures_allowed", True
            ),
            "READER_PROJECTION_DISCLOSURE_BUDGET_INVALID",
        ),
        (
            lambda payload: payload["attention_budget"].__setitem__(
                "audit_payload_default_visible", True
            ),
            "READER_PROJECTION_DISCLOSURE_BUDGET_INVALID",
        ),
    ],
)
def test_reader_projection_contract_fails_closed_on_semantic_drift(
    mutate: object,
    reason: str,
) -> None:
    payload = copy.deepcopy(_payload())
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(ReaderProjectionContractError, match=reason):
        StrategyResearchReaderProjectionContract.from_dict(payload)


def test_reader_projection_contract_never_upgrades_human_or_trading_authority() -> None:
    safety = _contract().safety

    assert safety.primary_research_start == "2021-02-22"
    assert safety.human_acceptance_auto_upgrade_allowed is False
    assert safety.strategy_conclusion_generated is False
    assert safety.order_authorized is False
    assert safety.real_engine_authorized is False
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"
