# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness_pipeline as upstream,
)
from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR = upstream.DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR
DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR = upstream.DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR
DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR = (
    upstream.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR
)
DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR = upstream.DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR
DEFAULT_FLIP_ROTATION_REDUCTION_DIR = upstream.DEFAULT_FLIP_ROTATION_REDUCTION_DIR
DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR = upstream.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR
DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR = upstream.DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR
DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR = (
    upstream.DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR
)
DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR = (
    upstream.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR
)
DEFAULT_FILTERED_NEXT_DECISION_DIR = upstream.DEFAULT_FILTERED_NEXT_DECISION_DIR
DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "formal_research_method_contract"
)
DEFAULT_CANDIDATE_DECISION_LEDGER_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_decision_ledger"
)

TOP_FILTERED_CANDIDATE = upstream.TOP_FILTERED_CANDIDATE
FORMAL_RESEARCH_PROMOTION_STATES = (
    "REJECTED",
    "NEEDS_MORE_EVIDENCE",
    "PROMISING",
    "PAPER_SHADOW_ELIGIBLE",
    "FORMAL_RESEARCH_READY",
)
FORMAL_RESEARCH_CONTRACT_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "research_screening_only": True,
    "manual_review_only": True,
    "formal_research_contract_only": True,
    "not_formal_research_method": True,
    "automatic_candidate_promotion": False,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "production_effect": "none",
}
CANDIDATE_DECISION_LEDGER_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "candidate_decision_ledger_only": True,
    "append_only_ledger": True,
    "automatic_candidate_promotion": False,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "production_effect": "none",
}

FORMAL_INPUT_SCHEMA = "formal_research_method_contract_input_snapshot.v2"
LEDGER_INPUT_SCHEMA = "candidate_decision_ledger_input_snapshot.v2"
FORMAL_VIEWS = (
    "formal_research_method_contract_manifest.json",
    "formal_research_method_contract.json",
    "formal_research_method_decision.json",
    "formal_research_method_contract_report.md",
    "reader_brief_section.md",
)
FORMAL_FILES = (*FORMAL_VIEWS, "formal_research_method_contract_input_snapshot.json")
LEDGER_VIEWS = (
    "candidate_decision_ledger_manifest.json",
    "candidate_decision_record.json",
    "candidate_decision_ledger_snapshot.jsonl",
    "candidate_decision_ledger_report.md",
    "reader_brief_section.md",
)
LEDGER_FILES = (*LEDGER_VIEWS, "candidate_decision_ledger_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text


class DynamicV3ResearchContractLedgerError(ValueError):
    """Raised when contract or ledger lineage is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3ResearchContractLedgerError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _aware_time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DynamicV3ResearchContractLedgerError(f"invalid {field}") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _source_specs(
    *,
    evidence_id: str | None,
    spec_id: str | None,
    stress_backfill_id: str | None,
    mismatch_reduction_id: str | None,
    flip_reduction_id: str | None,
    ab_review_id: str | None,
    confirmation_id: str | None,
    readiness_id: str | None,
    owner_review_id: str | None,
    next_decision_id: str | None,
    evidence_dir: Path,
    spec_dir: Path,
    stress_backfill_dir: Path,
    mismatch_reduction_dir: Path,
    flip_reduction_dir: Path,
    ab_review_dir: Path,
    confirmation_dir: Path,
    readiness_dir: Path,
    owner_review_dir: Path,
    next_decision_dir: Path,
) -> list[dict[str, Any]]:
    return [
        _spec("evidence", evidence_id, evidence_dir, upstream.EVIDENCE_FILES,
              upstream.validate_filtered_candidate_evidence_artifact,
              upstream.filtered_candidate_evidence_report_payload, "evidence_id"),
        _spec("spec", spec_id, spec_dir, upstream.SPEC_FILES,
              upstream.validate_median_regime_filter_spec_artifact,
              upstream.median_regime_filter_spec_report_payload, "spec_id"),
        _spec("stress_backfill", stress_backfill_id, stress_backfill_dir, upstream.STRESS_FILES,
              upstream.validate_filtered_candidate_stress_backfill_artifact,
              upstream.filtered_candidate_stress_backfill_report_payload, "stress_backfill_id"),
        _spec("mismatch_reduction", mismatch_reduction_id, mismatch_reduction_dir,
              upstream.MISMATCH_FILES, upstream.validate_drawdown_mismatch_reduction_artifact,
              upstream.drawdown_mismatch_reduction_report_payload, "reduction_id"),
        _spec("flip_reduction", flip_reduction_id, flip_reduction_dir, upstream.FLIP_FILES,
              upstream.validate_flip_rotation_reduction_artifact,
              upstream.flip_rotation_reduction_report_payload, "flip_reduction_id"),
        _spec("ab_review", ab_review_id, ab_review_dir, upstream.AB_FILES,
              upstream.validate_filtered_candidate_ab_review_artifact,
              upstream.filtered_candidate_ab_review_report_payload, "ab_review_id"),
        _spec("confirmation", confirmation_id, confirmation_dir, upstream.CONFIRMATION_FILES,
              upstream.validate_signal_gate_confirmation_artifact,
              upstream.signal_gate_confirmation_report_payload, "confirmation_id"),
        _spec("readiness", readiness_id, readiness_dir, upstream.READINESS_FILES,
              upstream.validate_filtered_formalization_readiness_artifact,
              upstream.filtered_formalization_readiness_report_payload, "readiness_id"),
        _spec("owner_review", owner_review_id, owner_review_dir, upstream.OWNER_FILES,
              upstream.validate_owner_filtered_candidate_review_artifact,
              upstream.owner_filtered_candidate_review_report_payload, "owner_review_id"),
        _spec("next_decision", next_decision_id, next_decision_dir, upstream.DECISION_FILES,
              upstream.validate_filtered_next_decision_artifact,
              upstream.filtered_next_decision_report_payload, "decision_id"),
    ]


def _spec(
    kind: str,
    artifact_id: str | None,
    root: Path,
    files: Sequence[str],
    validator: Callable[..., dict[str, Any]],
    reader: Callable[..., dict[str, Any]],
    id_key: str,
) -> dict[str, Any]:
    _require(bool(artifact_id), f"explicit {kind} artifact id is required")
    return {
        "kind": kind,
        "artifact_id": _text(artifact_id),
        "root": Path(root),
        "files": tuple(files),
        "validator": validator,
        "reader": reader,
        "id_key": id_key,
    }


def _validated_source(spec: Mapping[str, Any]) -> dict[str, Any]:
    artifact_id = _text(spec.get("artifact_id"))
    root = Path(spec["root"])
    validator = spec["validator"]
    id_key = _text(spec.get("id_key"))
    validation = cached_artifact_validation(
        validator=validator,
        validator_key=id_key,
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", f"{spec.get('kind')} source validation failed")
    reader = spec["reader"]
    payload = reader(**{id_key: artifact_id, "output_dir": root})
    _require(_text(payload.get(id_key)) == artifact_id, f"{spec.get('kind')} id mismatch")
    return payload


def _load_sources(specs: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_text(spec.get("kind")): _validated_source(spec) for spec in specs}


def _validate_lineage(
    candidate: str, sources: Mapping[str, Mapping[str, Any]], generated: datetime
) -> dict[str, Any]:
    for kind, payload in sources.items():
        _require(payload.get("candidate") == candidate, f"{kind} candidate lineage mismatch")
        _require(
            _aware_time(payload.get("generated_at"), f"{kind}.generated_at") <= generated,
            f"{kind} generated after contract",
        )
    context: dict[str, Any] = {"candidate": candidate}
    for field in ("market_regime", "date_start", "date_end"):
        values = {_text(payload.get(field)) for payload in sources.values() if _text(payload.get(field))}
        _require(len(values) <= 1, f"source {field} lineage mismatch")
        context[field] = next(iter(values), None)
    policy_sources = {
        json.dumps(
            _mapping(_mapping(payload.get("input_snapshot")).get("policy_source")),
            sort_keys=True,
        )
        for payload in sources.values()
        if _mapping(_mapping(payload.get("input_snapshot")).get("policy_source"))
    }
    _require(len(policy_sources) == 1, "source policy binding lineage mismatch")
    context["policy_source"] = json.loads(next(iter(policy_sources)))
    decision = sources["next_decision"]
    owner = sources["owner_review"]
    readiness = sources["readiness"]
    confirmation = sources["confirmation"]
    ab = sources["ab_review"]
    _require(decision.get("owner_review_id") == owner.get("owner_review_id"), "decision owner lineage")
    _require(owner.get("readiness_id") == readiness.get("readiness_id"), "owner readiness lineage")
    _require(
        readiness.get("confirmation_id") == confirmation.get("confirmation_id"),
        "readiness confirmation lineage",
    )
    _require(readiness.get("ab_review_id") == ab.get("ab_review_id"), "readiness A/B lineage")
    return context


def _source_bindings(specs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        _text(spec.get("kind")): upstream._binding(
            kind=_text(spec.get("kind")),
            artifact_id=_text(spec.get("artifact_id")),
            root=Path(spec["root"]) / _text(spec.get("artifact_id")),
            names=spec["files"],
        )
        for spec in specs
    }


def _source_artifacts(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        kind: {
            "artifact_id": payload.get({
                "evidence": "evidence_id", "spec": "spec_id",
                "stress_backfill": "stress_backfill_id", "mismatch_reduction": "reduction_id",
                "flip_reduction": "flip_reduction_id", "ab_review": "ab_review_id",
                "confirmation": "confirmation_id", "readiness": "readiness_id",
                "owner_review": "owner_review_id", "next_decision": "decision_id",
            }[kind]),
            "candidate": payload.get("candidate"),
            "generated_at": payload.get("generated_at"),
            "evidence_status": payload.get("evidence_status"),
        }
        for kind, payload in sources.items()
    }


def _objective_gates(sources: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    evidence = _mapping(sources["evidence"].get("filtered_candidate_evidence_summary"))
    stress = _mapping(sources["stress_backfill"].get("filtered_candidate_stress_summary"))
    mismatch = _mapping(sources["mismatch_reduction"].get("mismatch_reduction_summary"))
    flip = _mapping(sources["flip_reduction"].get("flip_rotation_reduction_summary"))
    ab = _mapping(sources["ab_review"].get("ab_summary"))
    confirmation = _mapping(sources["confirmation"].get("signal_gate_confirmation_targets"))
    readiness = _mapping(sources["readiness"].get("formalization_readiness_decision"))
    owner = _mapping(sources["owner_review"].get("owner_filtered_candidate_summary"))
    next_decision = _mapping(sources["next_decision"].get("filtered_next_decision"))
    completed = confirmation.get("completed_observation_count")
    owner_approval = (
        owner.get("owner_approval_observed") is True
        and owner.get("owner_decision_status") == "APPROVED"
    )
    flip_status = flip.get("flip_reduction_status")
    rotation_status = flip.get("rotation_reduction_status")
    rows = [
        (
            "evidence_status",
            evidence.get("evidence_status"),
            evidence.get("evidence_status") == "SUFFICIENT_FOR_RESEARCH_REVIEW",
        ),
        (
            "stress_result",
            stress.get("stress_robustness_status"),
            stress.get("stress_robustness_status") == "STRONG",
        ),
        (
            "drawdown_mismatch_reduction",
            mismatch.get("drawdown_mismatch_reduction_status"),
            mismatch.get("drawdown_mismatch_reduction_status") == "IMPROVED",
        ),
        (
            "flip_rotation_reduction",
            f"flip={flip_status}; rotation={rotation_status}",
            flip_status == "IMPROVED" and rotation_status == "IMPROVED",
        ),
        ("ab_review", ab.get("overall_ab_status"), ab.get("overall_ab_status") == "PROMISING"),
        (
            "confirmation_completed_observations",
            completed,
            isinstance(completed, int) and not isinstance(completed, bool) and completed > 0,
        ),
        (
            "formalization_readiness",
            readiness.get("decision"),
            readiness.get("can_implement_research_only_method") is True,
        ),
        ("owner_approval", "APPROVED" if owner_approval else "NOT_OBSERVED", owner_approval),
        (
            "next_decision",
            next_decision.get("decision"),
            next_decision.get("decision") == "FORMALIZE_RESEARCH_METHOD",
        ),
    ]
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "gate_id": gate_id,
            "observed_status": observed,
            "passed": passed,
            "observed_evidence_only": True,
            **FORMAL_RESEARCH_CONTRACT_SAFETY,
        }
        for gate_id, observed, passed in rows
    ]


def _formal_material(
    *, root: Path, contract_id: str, candidate: str,
    sources: Mapping[str, Mapping[str, Any]], lineage: Mapping[str, Any], generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    gates = _objective_gates(sources)
    blocker_by_gate = {
        "evidence_status": "validated_dated_filtered_outcomes_missing",
        "stress_result": "validated_stress_evidence_missing",
        "drawdown_mismatch_reduction": "drawdown_mismatch_evidence_missing",
        "flip_rotation_reduction": "flip_rotation_evidence_missing",
        "ab_review": "validated_ab_evidence_missing",
        "confirmation_completed_observations": "completed_confirmation_observations_missing",
        "formalization_readiness": "formalization_readiness_missing",
        "owner_approval": "explicit_owner_approval_missing",
        "next_decision": "formalization_decision_missing",
    }
    blockers = [
        blocker_by_gate[_text(row.get("gate_id"))]
        for row in gates
        if row.get("passed") is not True
    ]
    ready = not blockers
    paper_shadow = {
        "status": "ELIGIBLE_FOR_PROTOCOL_DESIGN" if ready else "NOT_ELIGIBLE",
        "eligible": ready,
        "blocking_reasons": blockers,
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "contract_id": contract_id,
        "candidate": candidate,
        "formal_research_method_status": "READY_FOR_MANUAL_FORMALIZATION_REVIEW" if ready else "NOT_READY",
        "promotion_state": "FORMAL_RESEARCH_READY" if ready else "NEEDS_MORE_EVIDENCE",
        "blocking_reasons": blockers,
        "paper_shadow_eligibility": (
            "ELIGIBLE_FOR_PROTOCOL_DESIGN" if ready else "NOT_ELIGIBLE"
        ),
        "safety_boundary_status": "PASS",
        "next_required_action": (
            "manual_formalization_review" if ready else "collect_missing_research_evidence"
        ),
        "owner_approval_observed": ready,
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    contract = {
        "schema_version": st.SCHEMA_VERSION,
        "contract_id": contract_id,
        "candidate": candidate,
        "promotion_states": list(FORMAL_RESEARCH_PROMOTION_STATES),
        "objective_gates": gates,
        "failure_conditions": blockers,
        "paper_shadow_eligibility": paper_shadow,
        "source_artifacts": _source_artifacts(sources),
        "lineage": dict(lineage),
        "safety_boundary_status": "PASS",
        "method_boundary": {
            "formal_research_ready_is_not_production_approval": True,
            "registered_targets_are_not_observed_confirmation": True,
            "system_recommendation_is_not_owner_approval": True,
            "official_target_weights_allowed": False,
            "broker_or_order_flow_allowed": False,
            "manual_review_only": True,
            "positive_state_requires_all_observed_gates": True,
        },
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_formal_research_method_contract_manifest",
        "contract_id": contract_id,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "status": "PASS" if ready else "PASS_WITH_WARNINGS",
        **{key: lineage.get(key) for key in ("market_regime", "date_start", "date_end")},
        "formal_research_method_contract_manifest_path": str(root / FORMAL_VIEWS[0]),
        "formal_research_method_contract_path": str(root / FORMAL_VIEWS[1]),
        "formal_research_method_decision_path": str(root / FORMAL_VIEWS[2]),
        "formal_research_method_contract_report_path": str(root / FORMAL_VIEWS[3]),
        "reader_brief_section_path": str(root / FORMAL_VIEWS[4]),
        "formal_research_method_contract_input_snapshot_path": str(
            root / "formal_research_method_contract_input_snapshot.json"
        ),
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    report = render_formal_research_method_contract_report(manifest, contract, decision)
    reader = render_formal_research_method_contract_reader_brief(decision)
    views = {
        FORMAL_VIEWS[0]: foundation._json_bytes(manifest),
        FORMAL_VIEWS[1]: foundation._json_bytes(contract),
        FORMAL_VIEWS[2]: foundation._json_bytes(decision),
        FORMAL_VIEWS[3]: foundation._text_file_bytes(report),
        FORMAL_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def build_formal_research_method_contract(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    evidence_id: str | None = None,
    spec_id: str | None = None,
    stress_backfill_id: str | None = None,
    mismatch_reduction_id: str | None = None,
    flip_reduction_id: str | None = None,
    ab_review_id: str | None = None,
    confirmation_id: str | None = None,
    readiness_id: str | None = None,
    owner_review_id: str | None = None,
    next_decision_id: str | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    spec_dir: Path = DEFAULT_MEDIAN_REGIME_FILTER_SPEC_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    readiness_dir: Path = DEFAULT_FILTERED_FORMALIZATION_READINESS_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    next_decision_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    specs = _source_specs(
        evidence_id=evidence_id, spec_id=spec_id, stress_backfill_id=stress_backfill_id,
        mismatch_reduction_id=mismatch_reduction_id, flip_reduction_id=flip_reduction_id,
        ab_review_id=ab_review_id, confirmation_id=confirmation_id, readiness_id=readiness_id,
        owner_review_id=owner_review_id, next_decision_id=next_decision_id,
        evidence_dir=evidence_dir, spec_dir=spec_dir, stress_backfill_dir=stress_backfill_dir,
        mismatch_reduction_dir=mismatch_reduction_dir, flip_reduction_dir=flip_reduction_dir,
        ab_review_dir=ab_review_dir, confirmation_dir=confirmation_dir,
        readiness_dir=readiness_dir, owner_review_dir=owner_review_dir,
        next_decision_dir=next_decision_dir,
    )
    sources = _load_sources(specs)
    lineage = _validate_lineage(candidate, sources, generated)
    contract_id = foundation._stable_id(
        "formal-research-method-contract", candidate,
        *[_text(spec.get("artifact_id")) for spec in specs], generated.isoformat(),
    )
    root = foundation._unique_dir(output_dir / contract_id)
    _, views = _formal_material(
        root=root, contract_id=root.name, candidate=candidate,
        sources=sources, lineage=lineage, generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    upstream._write_views(root, views)
    first_snapshot = _mapping(sources["evidence"].get("input_snapshot"))
    snapshot = {
        "schema_version": FORMAL_INPUT_SCHEMA,
        "contract_id": root.name,
        "candidate": candidate,
        "generated_at": generated.isoformat(),
        "lineage": lineage,
        "policy_source": _mapping(first_snapshot.get("policy_source")),
        "sources": _source_bindings(specs),
        "view_hashes": foundation._view_hashes(root, FORMAL_VIEWS),
        **FORMAL_RESEARCH_CONTRACT_SAFETY,
    }
    foundation._write_snapshot(root / "formal_research_method_contract_input_snapshot.json", snapshot)
    foundation._write_latest_pointer(
        "latest_formal_research_method_contract", root.name, root / FORMAL_VIEWS[0]
    )
    payload = formal_research_method_contract_report_payload(
        contract_id=root.name, output_dir=output_dir
    )
    return {
        **payload,
        "contract_dir": root,
        "manifest": foundation._read_json(root / FORMAL_VIEWS[0]),
    }


def formal_research_method_contract_report_payload(
    *, contract_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=contract_id, latest_pointer="latest_formal_research_method_contract",
        latest=latest, output_dir=output_dir, required_name=FORMAL_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / FORMAL_VIEWS[0]),
        "formal_research_method_contract": foundation._read_json(root / FORMAL_VIEWS[1]),
        "formal_research_method_decision": foundation._read_json(root / FORMAL_VIEWS[2]),
        "reader_brief_section": (root / FORMAL_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(
            root / "formal_research_method_contract_input_snapshot.json"
        ),
        "contract_dir": str(root),
    }
    validation = st._read_optional_json(root / "formal_research_method_contract_validation.json")
    if validation:
        payload["formal_research_method_contract_validation"] = validation
    return payload


def _rebuild_formal(root: Path, contract_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(root / "formal_research_method_contract_input_snapshot.json")
    foundation._validate_file_binding(_mapping(snapshot.get("policy_source")))
    source_bindings = _mapping(snapshot.get("sources"))
    specs: list[dict[str, Any]] = []
    source_template = _source_specs(
        evidence_id=upstream._binding_id(_mapping(source_bindings.get("evidence"))),
        spec_id=upstream._binding_id(_mapping(source_bindings.get("spec"))),
        stress_backfill_id=upstream._binding_id(_mapping(source_bindings.get("stress_backfill"))),
        mismatch_reduction_id=upstream._binding_id(_mapping(source_bindings.get("mismatch_reduction"))),
        flip_reduction_id=upstream._binding_id(_mapping(source_bindings.get("flip_reduction"))),
        ab_review_id=upstream._binding_id(_mapping(source_bindings.get("ab_review"))),
        confirmation_id=upstream._binding_id(_mapping(source_bindings.get("confirmation"))),
        readiness_id=upstream._binding_id(_mapping(source_bindings.get("readiness"))),
        owner_review_id=upstream._binding_id(_mapping(source_bindings.get("owner_review"))),
        next_decision_id=upstream._binding_id(_mapping(source_bindings.get("next_decision"))),
        evidence_dir=upstream._binding_root(_mapping(source_bindings.get("evidence"))).parent,
        spec_dir=upstream._binding_root(_mapping(source_bindings.get("spec"))).parent,
        stress_backfill_dir=upstream._binding_root(_mapping(source_bindings.get("stress_backfill"))).parent,
        mismatch_reduction_dir=upstream._binding_root(_mapping(source_bindings.get("mismatch_reduction"))).parent,
        flip_reduction_dir=upstream._binding_root(_mapping(source_bindings.get("flip_reduction"))).parent,
        ab_review_dir=upstream._binding_root(_mapping(source_bindings.get("ab_review"))).parent,
        confirmation_dir=upstream._binding_root(_mapping(source_bindings.get("confirmation"))).parent,
        readiness_dir=upstream._binding_root(_mapping(source_bindings.get("readiness"))).parent,
        owner_review_dir=upstream._binding_root(_mapping(source_bindings.get("owner_review"))).parent,
        next_decision_dir=upstream._binding_root(_mapping(source_bindings.get("next_decision"))).parent,
    )
    for spec in source_template:
        binding = _mapping(source_bindings.get(_text(spec.get("kind"))))
        upstream._validate_binding(binding, kind=_text(spec.get("kind")), names=spec["files"])
        specs.append(spec)
    sources = _load_sources(specs)
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    candidate = _text(snapshot.get("candidate"))
    lineage = _validate_lineage(candidate, sources, generated)
    _require(lineage == _mapping(snapshot.get("lineage")), "frozen lineage drift")
    _, expected = _formal_material(
        root=root, contract_id=contract_id, candidate=candidate,
        sources=sources, lineage=lineage, generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_formal_research_method_contract_artifact(
    *, contract_id: str, output_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    validation = upstream._validate_content_stage(
        artifact_id=contract_id, output_dir=output_dir,
        snapshot_name="formal_research_method_contract_input_snapshot.json",
        schema=FORMAL_INPUT_SCHEMA, id_key="contract_id", view_names=FORMAL_VIEWS,
        report_type="etf_dynamic_v3_formal_research_method_contract_validation",
        rebuild=_rebuild_formal,
    )
    if write_output:
        root = output_dir / contract_id
        st._write_json(root / "formal_research_method_contract_validation.json", validation)
        st._write_text(
            root / "formal_research_method_contract_validation.md",
            render_formal_research_method_contract_validation_report(validation),
        )
    return validation


def _record_hash(record: Mapping[str, Any]) -> str:
    canonical = {key: value for key, value in record.items() if key != "record_hash"}
    payload = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate_ledger_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    seen: set[str] = set()
    previous = "GENESIS"
    for index, row in enumerate(rows, start=1):
        record_id = _text(row.get("record_id"))
        _require(bool(record_id) and record_id not in seen, "ledger duplicate record_id")
        _require(row.get("ledger_sequence") == index, "ledger sequence is not monotonic")
        _require(row.get("previous_record_hash") == previous, "ledger hash-chain prefix drift")
        digest = _record_hash(row)
        _require(row.get("record_hash") == digest, "ledger record hash mismatch")
        seen.add(record_id)
        previous = digest


def _ledger_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return foundation._jsonl_bytes(rows)


def _ledger_material(
    *, root: Path, ledger_run_id: str, record: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
    generated: datetime, ledger_path: Path,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_decision_ledger_manifest",
        "ledger_run_id": ledger_run_id,
        "record_id": record.get("record_id"),
        "candidate": record.get("candidate"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "ledger_path": str(ledger_path),
        "record_count": len(rows),
        "candidate_decision_ledger_manifest_path": str(root / LEDGER_VIEWS[0]),
        "candidate_decision_record_path": str(root / LEDGER_VIEWS[1]),
        "candidate_decision_ledger_snapshot_path": str(root / LEDGER_VIEWS[2]),
        "candidate_decision_ledger_report_path": str(root / LEDGER_VIEWS[3]),
        "reader_brief_section_path": str(root / LEDGER_VIEWS[4]),
        "candidate_decision_ledger_input_snapshot_path": str(
            root / "candidate_decision_ledger_input_snapshot.json"
        ),
        **CANDIDATE_DECISION_LEDGER_SAFETY,
    }
    report = render_candidate_decision_ledger_report(manifest, record, rows)
    reader = render_candidate_decision_ledger_reader_brief(record)
    return manifest, {
        LEDGER_VIEWS[0]: foundation._json_bytes(manifest),
        LEDGER_VIEWS[1]: foundation._json_bytes(record),
        LEDGER_VIEWS[2]: _ledger_bytes(rows),
        LEDGER_VIEWS[3]: foundation._text_file_bytes(report),
        LEDGER_VIEWS[4]: foundation._text_file_bytes(reader),
    }


def _contract_source_id(contract: Mapping[str, Any], kind: str) -> str:
    return _text(_mapping(_mapping(contract.get("source_artifacts")).get(kind)).get("artifact_id"))


@with_artifact_validation_session
def record_candidate_decision_ledger(
    *,
    candidate: str = TOP_FILTERED_CANDIDATE,
    evidence_id: str | None = None,
    stress_backfill_id: str | None = None,
    mismatch_reduction_id: str | None = None,
    flip_reduction_id: str | None = None,
    ab_review_id: str | None = None,
    confirmation_id: str | None = None,
    owner_review_id: str | None = None,
    next_decision_id: str | None = None,
    contract_id: str | None = None,
    protocol_id: str | None = None,
    evidence_dir: Path = DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    stress_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    mismatch_reduction_dir: Path = DEFAULT_DRAWDOWN_MISMATCH_REDUCTION_DIR,
    flip_reduction_dir: Path = DEFAULT_FLIP_ROTATION_REDUCTION_DIR,
    ab_review_dir: Path = DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    confirmation_dir: Path = DEFAULT_SIGNAL_GATE_CONFIRMATION_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    next_decision_dir: Path = DEFAULT_FILTERED_NEXT_DECISION_DIR,
    contract_dir: Path = DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    protocol_dir: Path | None = None,
    output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    del evidence_dir, stress_backfill_dir, mismatch_reduction_dir, flip_reduction_dir
    del ab_review_dir, confirmation_dir, owner_review_dir, next_decision_dir, protocol_dir
    generated = _generated_time(generated_at)
    _require(bool(contract_id), "explicit contract_id is required")
    contract_validation = cached_artifact_validation(
        validator=validate_formal_research_method_contract_artifact,
        validator_key="contract_id", artifact_id=_text(contract_id), root=contract_dir,
    )
    _require(contract_validation.get("status") == "PASS", "formal contract validation failed")
    contract_payload = formal_research_method_contract_report_payload(
        contract_id=contract_id, output_dir=contract_dir
    )
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    decision = _mapping(contract_payload.get("formal_research_method_decision"))
    _require(contract_payload.get("candidate") == candidate, "ledger candidate lineage mismatch")
    _require(
        _aware_time(contract_payload.get("generated_at"), "contract.generated_at") <= generated,
        "contract generated after ledger record",
    )
    supplied = {
        "evidence": evidence_id, "stress_backfill": stress_backfill_id,
        "mismatch_reduction": mismatch_reduction_id, "flip_reduction": flip_reduction_id,
        "ab_review": ab_review_id, "confirmation": confirmation_id,
        "owner_review": owner_review_id, "next_decision": next_decision_id,
    }
    for kind, artifact_id in supplied.items():
        if artifact_id is not None:
            _require(artifact_id == _contract_source_id(contract, kind), f"ledger {kind} lineage")
    gates = {str(row.get("gate_id")): row for row in _records(contract.get("objective_gates"))}
    record_id = foundation._stable_id(
        "candidate-decision-ledger", candidate, contract_id, generated.isoformat()
    )
    output_root = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = (output_dir / "candidate_decision_ledger.jsonl").resolve()
    _require(ledger_path.parent == output_root, "canonical ledger escaped output root")
    lock_path = output_dir / ".candidate_decision_ledger.lock"
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise DynamicV3ResearchContractLedgerError("candidate ledger is locked") from exc
    os.close(descriptor)
    staging: Path | None = None
    try:
        before = ledger_path.read_bytes() if ledger_path.exists() else b""
        existing = st._read_jsonl(ledger_path)
        _require(_ledger_bytes(existing) == before, "ledger contains partial or non-canonical bytes")
        _validate_ledger_rows(existing)
        _require(all(row.get("record_id") != record_id for row in existing), "duplicate ledger record")
        completed = _mapping(gates.get("confirmation_completed_observations")).get(
            "observed_status"
        )
        flip_rotation = _text(
            _mapping(gates.get("flip_rotation_reduction")).get("observed_status")
        )
        flip_result = flip_rotation.split(";", 1)[0].removeprefix("flip=")
        rotation_result = (
            flip_rotation.split(";", 1)[1].strip().removeprefix("rotation=")
            if ";" in flip_rotation
            else None
        )
        record: dict[str, Any] = {
            "schema_version": st.SCHEMA_VERSION,
            "record_id": record_id,
            "candidate": candidate,
            "generated_at": generated.isoformat(),
            "ledger_sequence": len(existing) + 1,
            "previous_record_hash": existing[-1]["record_hash"] if existing else "GENESIS",
            "evidence_status": _mapping(gates.get("evidence_status")).get("observed_status"),
            "stress_result": _mapping(gates.get("stress_result")).get("observed_status"),
            "mismatch_result": _mapping(gates.get("drawdown_mismatch_reduction")).get("observed_status"),
            "flip_result": flip_result,
            "rotation_result": rotation_result,
            "ab_result": _mapping(gates.get("ab_review")).get("observed_status"),
            "confirmation_count": completed,
            "completed_confirmation_observations": completed,
            "owner_action": None,
            "owner_decision_status": "NOT_OBSERVED",
            "system_recommended_action": "COLLECT_VALIDATED_DATED_FILTERED_OUTCOMES",
            "final_decision": "COLLECT_DATED_EVIDENCE",
            "next_required_action": decision.get("next_required_action"),
            "formal_research_method_status": decision.get("formal_research_method_status"),
            "source_artifacts": {"contract_id": contract_id},
            "eb5_protocol_status": (
                "UNVALIDATED_EB5_PROTOCOL_IGNORED" if protocol_id else "INSUFFICIENT_EB5_PROTOCOL"
            ),
            "requested_protocol_id": protocol_id,
            "ledger_path": str(ledger_path),
            **CANDIDATE_DECISION_LEDGER_SAFETY,
        }
        record["record_hash"] = _record_hash(record)
        rows = [*existing, record]
        _validate_ledger_rows(rows)
        root = output_dir / record_id
        _require(not root.exists(), f"ledger artifact already exists: {root}")
        staging = output_dir / f".{record_id}.{uuid.uuid4().hex}.staging"
        staging.mkdir(parents=False, exist_ok=False)
        _, views = _ledger_material(
            root=root, ledger_run_id=record_id, record=record, rows=rows,
            generated=generated, ledger_path=ledger_path,
        )
        upstream._write_views(staging, views)
        contract_binding = upstream._binding(
            kind="formal_research_method_contract", artifact_id=_text(contract_id),
            root=contract_dir / _text(contract_id), names=FORMAL_FILES,
        )
        snapshot = {
            "schema_version": LEDGER_INPUT_SCHEMA,
            "ledger_run_id": record_id,
            "record_id": record_id,
            "candidate": candidate,
            "generated_at": generated.isoformat(),
            "contract_source": contract_binding,
            "ledger_prefix_sha256": hashlib.sha256(before).hexdigest(),
            "ledger_after_sha256": hashlib.sha256(_ledger_bytes(rows)).hexdigest(),
            "ledger_record_count": len(rows),
            "view_hashes": foundation._view_hashes(staging, LEDGER_VIEWS),
            **CANDIDATE_DECISION_LEDGER_SAFETY,
        }
        foundation._write_snapshot(staging / "candidate_decision_ledger_input_snapshot.json", snapshot)
        write_bytes_atomic(ledger_path, _ledger_bytes(rows))
        try:
            staging.replace(root)
        except OSError:
            write_bytes_atomic(ledger_path, before)
            raise
        staging = None
    finally:
        if staging is not None and staging.exists():
            for child in staging.iterdir():
                child.unlink()
            staging.rmdir()
        lock_path.unlink(missing_ok=True)
    foundation._write_latest_pointer(
        "latest_candidate_decision_ledger", record_id, root / LEDGER_VIEWS[0]
    )
    validation = validate_candidate_decision_ledger_artifact(
        ledger_run_id=record_id, output_dir=output_dir, write_output=True
    )
    payload = candidate_decision_ledger_report_payload(
        ledger_run_id=record_id, output_dir=output_dir
    )
    return {
        **payload,
        "ledger_dir": root,
        "manifest": foundation._read_json(root / LEDGER_VIEWS[0]),
        "ledger_rows": payload["candidate_decision_ledger_snapshot"],
        "candidate_decision_ledger_validation": validation,
    }


def candidate_decision_ledger_report_payload(
    *, ledger_run_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=ledger_run_id, latest_pointer="latest_candidate_decision_ledger",
        latest=latest, output_dir=output_dir, required_name=LEDGER_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / LEDGER_VIEWS[0]),
        "candidate_decision_record": foundation._read_json(root / LEDGER_VIEWS[1]),
        "candidate_decision_ledger_snapshot": st._read_jsonl(root / LEDGER_VIEWS[2]),
        "reader_brief_section": (root / LEDGER_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(root / "candidate_decision_ledger_input_snapshot.json"),
        "ledger_dir": str(root),
    }
    validation = st._read_optional_json(root / "candidate_decision_ledger_validation.json")
    if validation:
        payload["candidate_decision_ledger_validation"] = validation
    return payload


def _rebuild_ledger(root: Path, ledger_run_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(root / "candidate_decision_ledger_input_snapshot.json")
    source = _mapping(snapshot.get("contract_source"))
    upstream._validate_binding(source, kind="formal_research_method_contract", names=FORMAL_FILES)
    contract_id = upstream._binding_id(source)
    contract_dir = upstream._binding_root(source).parent
    validation = cached_artifact_validation(
        validator=validate_formal_research_method_contract_artifact,
        validator_key="contract_id", artifact_id=contract_id, root=contract_dir,
    )
    _require(validation.get("status") == "PASS", "ledger contract source validation failed")
    record = foundation._read_json(root / LEDGER_VIEWS[1])
    rows = st._read_jsonl(root / LEDGER_VIEWS[2])
    _validate_ledger_rows(rows)
    count = int(snapshot.get("ledger_record_count", -1))
    _require(count == len(rows), "ledger snapshot count drift")
    _require(rows and rows[-1] == record, "ledger record is not snapshot tail")
    ledger_path = (root.parent / "candidate_decision_ledger.jsonl").resolve()
    _require(ledger_path.parent == root.parent.resolve(), "canonical ledger escaped output root")
    canonical_bytes = ledger_path.read_bytes()
    canonical_rows = st._read_jsonl(ledger_path)
    _require(_ledger_bytes(canonical_rows) == canonical_bytes, "canonical ledger partial bytes")
    _validate_ledger_rows(canonical_rows)
    _require(canonical_rows[:count] == rows, "canonical ledger prefix mutated")
    _require(
        hashlib.sha256(_ledger_bytes(rows)).hexdigest() == snapshot.get("ledger_after_sha256"),
        "ledger after hash drift",
    )
    _require(
        hashlib.sha256(_ledger_bytes(rows[:-1])).hexdigest()
        == snapshot.get("ledger_prefix_sha256"),
        "ledger prefix hash drift",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _, expected = _ledger_material(
        root=root, ledger_run_id=ledger_run_id, record=record, rows=rows,
        generated=generated, ledger_path=ledger_path,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_candidate_decision_ledger_artifact(
    *, ledger_run_id: str, output_dir: Path = DEFAULT_CANDIDATE_DECISION_LEDGER_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    validation = upstream._validate_content_stage(
        artifact_id=ledger_run_id, output_dir=output_dir,
        snapshot_name="candidate_decision_ledger_input_snapshot.json",
        schema=LEDGER_INPUT_SCHEMA, id_key="ledger_run_id", view_names=LEDGER_VIEWS,
        report_type="etf_dynamic_v3_candidate_decision_ledger_validation",
        rebuild=_rebuild_ledger,
    )
    if write_output:
        root = output_dir / ledger_run_id
        st._write_json(root / "candidate_decision_ledger_validation.json", validation)
        st._write_text(
            root / "candidate_decision_ledger_validation.md",
            render_candidate_decision_ledger_validation_report(validation),
        )
    return validation


def render_formal_research_method_contract_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Formal Research Method Contract", "",
        f"- formal_research_method_status: {decision.get('formal_research_method_status')}",
        f"- promotion_state: {decision.get('promotion_state')}",
        f"- blocking_issues: {','.join(str(v) for v in _records_or_values(decision.get('blocking_reasons')))}",
        "- safety: research/manual review only / production_effect=none", "",
    ])


def _records_or_values(value: Any) -> list[Any]:
    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else []


def render_formal_research_method_contract_report(
    manifest: Mapping[str, Any], contract: Mapping[str, Any], decision: Mapping[str, Any]
) -> str:
    return "\n".join([
        f"# Formal Research Method Contract {manifest.get('contract_id')}", "",
        f"- candidate：{manifest.get('candidate')}",
        f"- status：{decision.get('formal_research_method_status')}",
        f"- promotion_state：{decision.get('promotion_state')}",
        f"- objective_gate_count：{len(_records(contract.get('objective_gates')))}",
        "- registered target不是observed confirmation；system recommendation不是owner approval。",
        "- safety：manual research only / no official weights / no broker / no production", "",
    ])


def render_formal_research_method_contract_validation_report(validation: Mapping[str, Any]) -> str:
    return _validation_markdown("Formal Research Method Contract", validation)


def render_candidate_decision_ledger_reader_brief(record: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Candidate Decision Ledger", "",
        f"- candidate_decision_ledger_status: {record.get('final_decision')}",
        f"- next_required_action: {record.get('next_required_action')}",
        f"- eb5_protocol_status: {record.get('eb5_protocol_status')}",
        "- safety: append-only research ledger / production_effect=none", "",
    ])


def render_candidate_decision_ledger_report(
    manifest: Mapping[str, Any], record: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join([
        f"# Candidate Decision Ledger {manifest.get('ledger_run_id')}", "",
        f"- sequence：{record.get('ledger_sequence')}",
        f"- record_count：{len(rows)}",
        f"- final_decision：{record.get('final_decision')}",
        f"- next_required_action：{record.get('next_required_action')}",
        "- EB5 protocol未canonical时不能覆盖contract结论。",
        "- safety：manual research only / no official weights / no broker / no production", "",
    ])


def render_candidate_decision_ledger_validation_report(validation: Mapping[str, Any]) -> str:
    return _validation_markdown("Candidate Decision Ledger", validation)


def _validation_markdown(title: str, validation: Mapping[str, Any]) -> str:
    rows = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join([
        f"# {title} Validation {validation.get('artifact_id')}", "",
        f"- status: {validation.get('status')}",
        f"- failed_check_count: {validation.get('failed_check_count')}",
        "- production_effect: none", "", "## Checks", *rows, "",
    ])
