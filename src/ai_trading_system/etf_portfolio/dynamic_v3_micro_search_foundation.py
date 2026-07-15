from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.data.quality import render_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "micro_search_foundation_v1.yaml"
)
DEFAULT_GATE_CALIBRATION_REVIEW_DIR = diagnosis.DEFAULT_GATE_CALIBRATION_REVIEW_DIR
DEFAULT_SCORECARD_ATTRIBUTION_DIR = diagnosis.DEFAULT_SCORECARD_ATTRIBUTION_DIR
DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR = diagnosis.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR
DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR = diagnosis.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR
DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR = legacy.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR
DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR = legacy.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR
DEFAULT_GATE_CALIBRATED_REVIEW_DIR = legacy.DEFAULT_GATE_CALIBRATED_REVIEW_DIR
DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR = (
    legacy.DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR
)

DESIGN_INPUT_SCHEMA = "micro_search_v4_design_input_snapshot.v2"
BACKFILL_INPUT_SCHEMA = "micro_search_v4_backfill_input_snapshot.v2"
GATE_REVIEW_INPUT_SCHEMA = "gate_calibrated_review_input_snapshot.v2"
ATTRIBUTION_INPUT_SCHEMA = "signal_vs_parameter_attribution_input_snapshot.v2"

DESIGN_VIEWS = (
    "micro_search_v4_design_manifest.json",
    "v4_design_rationale.json",
    "v4_variant_specs.jsonl",
    "micro_search_v4_design_report.md",
)
BACKFILL_VIEWS = (
    "micro_search_v4_backfill_manifest.json",
    "v4_backfill_progress.json",
    "validate_data_quality_report.md",
    "v4_variant_performance.jsonl",
    "v4_variant_regime_metrics.jsonl",
    "v4_variant_stability_metrics.jsonl",
    "v4_variant_signal_metrics.jsonl",
    "micro_search_v4_backfill_report.md",
)
GATE_REVIEW_VIEWS = (
    "gate_calibrated_review_manifest.json",
    "official_gate_results.jsonl",
    "diagnostic_gate_results.jsonl",
    "gate_calibrated_summary.json",
    "gate_calibrated_review_report.md",
)
ATTRIBUTION_VIEWS = (
    "signal_vs_parameter_manifest.json",
    "failure_source_attribution.json",
    "recommended_research_shift.json",
    "signal_vs_parameter_attribution_report.md",
    "reader_brief_section.md",
)
DESIGN_FILES = (*DESIGN_VIEWS, "micro_search_v4_design_input_snapshot.json")
BACKFILL_FILES = (*BACKFILL_VIEWS, "micro_search_v4_backfill_input_snapshot.json")
GATE_REVIEW_FILES = (*GATE_REVIEW_VIEWS, "gate_calibrated_review_input_snapshot.json")
ATTRIBUTION_FILES = (
    *ATTRIBUTION_VIEWS,
    "signal_vs_parameter_attribution_input_snapshot.json",
)
PAPER_BACKFILL_FILES = (
    "paper_shadow_backfill_input_snapshot.json",
    "paper_shadow_backfill_manifest.json",
    "backfill_rebalance_calendar.json",
    "backfill_method_states.jsonl",
    "backfill_trade_ledger.jsonl",
    "backfill_data_quality.json",
    "paper_shadow_backfill_report.md",
    "validate_data_quality_report.md",
)

_mapping = foundation._mapping
_records = foundation._records
_texts = foundation._texts
_text = foundation._text
_float = foundation._float
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_artifact_dir = foundation._artifact_dir
_read_json = foundation._read_json
_read_jsonl = foundation._read_jsonl
_write_json = foundation._write_json
_write_jsonl = foundation._write_jsonl
_write_text = foundation._write_text
_write_latest_pointer = foundation._write_latest_pointer
_validation_payload = foundation._validation_payload


class DynamicV3MicroSearchFoundationError(ValueError):
    """Raised when TRADING-320～323 evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3MicroSearchFoundationError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _aware_time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3MicroSearchFoundationError(f"{field} must be ISO datetime") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _integer(value: Any, field: str) -> int:
    _require(not isinstance(value, bool), f"{field} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise DynamicV3MicroSearchFoundationError(f"{field} must be an integer") from exc
    return result


def _number(value: Any, field: str) -> float:
    _require(not isinstance(value, bool), f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DynamicV3MicroSearchFoundationError(f"{field} must be numeric") from exc
    return result


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_micro_search_foundation_policy.v1",
        "micro search policy schema mismatch",
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

    design = _mapping(payload.get("design"))
    minimum = _integer(design.get("minimum_variant_count"), "minimum_variant_count")
    maximum = _integer(design.get("maximum_variant_count"), "maximum_variant_count")
    _require(1 <= minimum <= maximum <= 40, "variant count policy invalid")
    required_ids = _texts(design.get("required_variant_ids"))
    required_families = _texts(design.get("required_families"))
    _require(len(required_ids) == len(set(required_ids)) >= 12, "required variants invalid")
    _require(
        len(required_families) == len(set(required_families)) >= 6,
        "required families invalid",
    )
    _require(
        _text(design.get("insufficient_evidence_status"))
        == "DESIGN_READY_WITH_EVIDENCE_LIMITATIONS",
        "insufficient design status invalid",
    )

    gate = _mapping(payload.get("gate_review"))
    official = _number(gate.get("official_research_score_min"), "official score")
    relaxation = _number(gate.get("diagnostic_relaxation"), "diagnostic relaxation")
    _require(0.0 < official <= 1.0, "official research score invalid")
    _require(0.0 < relaxation < official, "diagnostic relaxation invalid")
    _require(
        abs(official - legacy.BATCH2_PROMOTE_SCORE) < 1e-12,
        "reviewed official score differs from scorecard implementation",
    )
    _require(
        abs(relaxation - legacy.GATE_DIAGNOSTIC_RELAXATION) < 1e-12,
        "reviewed diagnostic relaxation differs from implementation",
    )
    _require(gate.get("official_gate_change_allowed") is False, "official gate change forbidden")
    _require(bool(_texts(gate.get("high_risk_failed_gates"))), "high-risk gates required")

    attribution = _mapping(payload.get("failure_attribution"))
    _require(
        attribution.get("missing_dated_signal_status") == "INSUFFICIENT_DATA",
        "missing signal status invalid",
    )
    _require(
        attribution.get("missing_dated_consensus_status") == "INSUFFICIENT_DATA",
        "missing consensus status invalid",
    )
    _require(
        attribution.get("insufficient_failure_source") == "INCONCLUSIVE",
        "insufficient failure source invalid",
    )
    _require(attribution.get("insufficient_confidence") == "LOW", "confidence policy invalid")

    safety = _mapping(payload.get("safety"))
    for field in (
        "research_screening_only",
        "experiment_only",
        "not_formal_research_method",
        "not_official_target_weights",
        "paper_shadow_only",
        "owner_review_required",
    ):
        _require(safety.get(field) is True, f"safety must enable {field}")
    for field in (
        "paper_shadow_primary_changed",
        "broker_action_allowed",
        "broker_action_taken",
        "order_ticket_generated",
        "auto_apply",
    ):
        _require(safety.get(field) is False, f"safety must disable {field}")
    _require(safety.get("production_effect") == "none", "production effect must be none")
    return payload


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _validate_binding(binding: Mapping[str, Any], *, kind: str) -> None:
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


def _validated_gate(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis.validate_gate_calibration_review_artifact,
        validator_key="gate_calibration_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "gate calibration source validation failed")
    return diagnosis.gate_calibration_review_report_payload(
        gate_calibration_id=artifact_id, output_dir=root
    )


def _validated_attribution(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis.validate_scorecard_attribution_artifact,
        validator_key="scorecard_attribution_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "scorecard attribution source validation failed")
    return diagnosis.scorecard_attribution_report_payload(
        scorecard_attribution_id=artifact_id, output_dir=root
    )


def _validated_signal(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis.validate_signal_instability_diagnosis_artifact,
        validator_key="signal_diagnosis_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "signal diagnosis source validation failed")
    return diagnosis.signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=artifact_id, output_dir=root
    )


def _validated_consensus(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis.validate_consensus_quality_review_artifact,
        validator_key="consensus_review_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "consensus review source validation failed")
    return diagnosis.consensus_quality_review_report_payload(
        consensus_review_id=artifact_id, output_dir=root
    )


def _validate_design_lineage(
    gate: Mapping[str, Any],
    attribution: Mapping[str, Any],
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
) -> None:
    attribution_id = _text(attribution.get("scorecard_attribution_id"))
    scorecard_id = _text(attribution.get("scorecard_id"))
    _require(
        scorecard_id == _text(gate.get("source_scorecard_id")),
        "gate/scorecard lineage mismatch",
    )
    _require(
        attribution_id == _text(signal.get("scorecard_attribution_id")),
        "signal/attribution lineage mismatch",
    )
    _require(
        attribution_id == _text(consensus.get("scorecard_attribution_id")),
        "consensus/attribution lineage mismatch",
    )
    _require(
        _text(signal.get("signal_diagnosis_id")) == _text(consensus.get("signal_diagnosis_id")),
        "consensus/signal lineage mismatch",
    )
    _require(
        _text(attribution.get("v3_backfill_id")) == _text(signal.get("v3_backfill_id"))
        == _text(consensus.get("v3_backfill_id")),
        "targeted backfill lineage mismatch",
    )


def _design_material(
    *,
    root: Path,
    design_id: str,
    gate: Mapping[str, Any],
    attribution: Mapping[str, Any],
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    _validate_design_lineage(gate, attribution, signal, consensus)
    rationale = legacy._micro_search_v4_design_rationale(gate, attribution, signal, consensus)
    signal_status = _text(signal.get("evidence_status"))
    consensus_status = _text(consensus.get("evidence_status"))
    evidence_limited = "INSUFFICIENT_DATA" in {signal_status, consensus_status}
    rationale.update(
        {
            "evidence_status": "INSUFFICIENT_DATA" if evidence_limited else "SUFFICIENT",
            "design_status": (
                _text(_mapping(policy.get("design")).get("insufficient_evidence_status"))
                if evidence_limited
                else "DESIGN_READY"
            ),
            "dominant_signal_issue": (
                None if evidence_limited else rationale.get("dominant_signal_issue")
            ),
            "consensus_failure_reason": (
                "insufficient_dated_consensus_evidence"
                if evidence_limited
                else rationale.get("consensus_failure_reason")
            ),
            "evidence_limitations": (
                [
                    "validated dated signal ledger is unavailable",
                    "validated dated candidate weight path is unavailable",
                    "variants are reviewed pilot hypotheses, not observed fixes",
                ]
                if evidence_limited
                else []
            ),
            "policy_version": _policy_version(policy),
        }
    )
    variants = legacy._micro_search_v4_variant_specs(rationale)
    role = _text(_mapping(policy.get("design")).get("variant_evidence_role"))
    for row in variants:
        row["evidence_role"] = role
        row["source_evidence_status"] = rationale["evidence_status"]
        row["policy_version"] = _policy_version(policy)
    rules = _mapping(policy.get("design"))
    minimum = _integer(rules.get("minimum_variant_count"), "minimum_variant_count")
    maximum = _integer(rules.get("maximum_variant_count"), "maximum_variant_count")
    required_ids = set(_texts(rules.get("required_variant_ids")))
    variant_ids = {_text(row.get("variant_id")) for row in variants}
    families = {family for row in variants for family in _texts(row.get("families"))}
    _require(minimum <= len(variants) <= maximum, "variant count outside reviewed policy")
    _require(required_ids.issubset(variant_ids), "required design variants missing")
    _require(
        set(_texts(rules.get("required_families"))).issubset(families),
        "required design families missing",
    )
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_micro_search_v4_design_manifest",
        "v4_design_id": design_id,
        "gate_calibration_id": gate.get("gate_calibration_id"),
        "scorecard_attribution_id": attribution.get("scorecard_attribution_id"),
        "signal_diagnosis_id": signal.get("signal_diagnosis_id"),
        "consensus_review_id": consensus.get("consensus_review_id"),
        "source_scorecard_id": attribution.get("scorecard_id"),
        "v3_matrix_id": attribution.get("v3_matrix_id"),
        "v3_backfill_id": attribution.get("v3_backfill_id"),
        "source_backfill_id": attribution.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if evidence_limited else "PASS",
        "evidence_status": rationale["evidence_status"],
        "market_regime": attribution.get("market_regime", "ai_after_chatgpt"),
        "variant_count": len(variants),
        "micro_search_policy_version": _policy_version(policy),
        "micro_search_v4_design_manifest_path": str(root / DESIGN_VIEWS[0]),
        "v4_design_rationale_path": str(root / DESIGN_VIEWS[1]),
        "v4_variant_specs_path": str(root / DESIGN_VIEWS[2]),
        "micro_search_v4_design_report_path": str(root / DESIGN_VIEWS[3]),
        "micro_search_v4_design_input_snapshot_path": str(
            root / "micro_search_v4_design_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_micro_search_v4_design_report(manifest, rationale, variants)
    views = {
        DESIGN_VIEWS[0]: foundation._json_bytes(manifest),
        DESIGN_VIEWS[1]: foundation._json_bytes(rationale),
        DESIGN_VIEWS[2]: foundation._jsonl_bytes(variants),
        DESIGN_VIEWS[3]: foundation._text_file_bytes(report),
    }
    return manifest, views


@with_artifact_validation_session
def run_micro_search_v4_design(
    *,
    gate_calibration_id: str,
    scorecard_attribution_id: str,
    signal_diagnosis_id: str,
    consensus_review_id: str,
    gate_calibration_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    attribution_dir: Path = DEFAULT_SCORECARD_ATTRIBUTION_DIR,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    gate = _validated_gate(gate_calibration_id, gate_calibration_dir)
    attribution = _validated_attribution(scorecard_attribution_id, attribution_dir)
    signal = _validated_signal(signal_diagnosis_id, signal_dir)
    consensus = _validated_consensus(consensus_review_id, consensus_dir)
    policy = _policy(policy_path)
    _chronology(generated, gate, attribution, signal, consensus)
    design_id = _stable_id(
        "micro-search-v4-design",
        gate_calibration_id,
        scorecard_attribution_id,
        signal_diagnosis_id,
        consensus_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / design_id)
    manifest, views = _design_material(
        root=root,
        design_id=root.name,
        gate=gate,
        attribution=attribution,
        signal=signal,
        consensus=consensus,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": DESIGN_INPUT_SCHEMA,
        "v4_design_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "gate_source": _binding(
            kind="gate_calibration_review",
            artifact_id=gate_calibration_id,
            root=gate_calibration_dir / gate_calibration_id,
            names=diagnosis.GATE_FILES,
        ),
        "attribution_source": _binding(
            kind="scorecard_attribution",
            artifact_id=scorecard_attribution_id,
            root=attribution_dir / scorecard_attribution_id,
            names=diagnosis.ATTRIBUTION_FILES,
        ),
        "signal_source": _binding(
            kind="signal_instability_diagnosis",
            artifact_id=signal_diagnosis_id,
            root=signal_dir / signal_diagnosis_id,
            names=diagnosis.SIGNAL_FILES,
        ),
        "consensus_source": _binding(
            kind="consensus_quality_review",
            artifact_id=consensus_review_id,
            root=consensus_dir / consensus_review_id,
            names=diagnosis.CONSENSUS_FILES,
        ),
        "source_scorecard_id": manifest["source_scorecard_id"],
        "v3_matrix_id": manifest["v3_matrix_id"],
        "v3_backfill_id": manifest["v3_backfill_id"],
        "view_hashes": foundation._view_hashes(root, DESIGN_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "micro_search_v4_design_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_micro_search_v4_design", root.name, root / DESIGN_VIEWS[0])
    return {
        "v4_design_id": root.name,
        "v4_design_dir": root,
        "manifest": manifest,
        "v4_design_rationale": _read_json(root / DESIGN_VIEWS[1]),
        "v4_variant_specs": _read_jsonl(root / DESIGN_VIEWS[2]),
    }


def micro_search_v4_design_report_payload(
    *,
    v4_design_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v4_design_id,
        latest_pointer="latest_micro_search_v4_design",
        latest=latest,
        output_dir=output_dir,
        required_name=DESIGN_VIEWS[0],
    )
    return {
        **_read_json(root / DESIGN_VIEWS[0]),
        "v4_design_rationale": _read_json(root / DESIGN_VIEWS[1]),
        "v4_variant_specs": _read_jsonl(root / DESIGN_VIEWS[2]),
        "input_snapshot": _read_json(root / "micro_search_v4_design_input_snapshot.json"),
        "v4_design_dir": str(root),
    }


def _rebuild_design(root: Path, design_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "micro_search_v4_design_input_snapshot.json")
    _require(snapshot.get("schema_version") == DESIGN_INPUT_SCHEMA, "design snapshot schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    bindings = (
        ("gate_source", "gate_calibration_review"),
        ("attribution_source", "scorecard_attribution"),
        ("signal_source", "signal_instability_diagnosis"),
        ("consensus_source", "consensus_quality_review"),
    )
    for field, kind in bindings:
        _validate_binding(_mapping(snapshot.get(field)), kind=kind)
    gate_binding = _mapping(snapshot.get("gate_source"))
    attribution_binding = _mapping(snapshot.get("attribution_source"))
    signal_binding = _mapping(snapshot.get("signal_source"))
    consensus_binding = _mapping(snapshot.get("consensus_source"))
    gate = _validated_gate(_binding_id(gate_binding), _binding_root(gate_binding).parent)
    attribution = _validated_attribution(
        _binding_id(attribution_binding), _binding_root(attribution_binding).parent
    )
    signal = _validated_signal(_binding_id(signal_binding), _binding_root(signal_binding).parent)
    consensus = _validated_consensus(
        _binding_id(consensus_binding), _binding_root(consensus_binding).parent
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, gate, attribution, signal, consensus)
    _, expected = _design_material(
        root=root,
        design_id=design_id,
        gate=gate,
        attribution=attribution,
        signal=signal,
        consensus=consensus,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_micro_search_v4_design_artifact(
    *,
    v4_design_id: str,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
) -> dict[str, Any]:
    root = output_dir / v4_design_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="micro_search_v4_design_input_snapshot.json",
        schema=DESIGN_INPUT_SCHEMA,
        id_key="v4_design_id",
        artifact_id=v4_design_id,
        view_names=DESIGN_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_micro_search_v4_design_validation", v4_design_id, checks
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_micro_search_v4_design_validation",
        artifact_id=v4_design_id,
        checks=checks,
        rebuild=lambda: _rebuild_design(root, v4_design_id),
    )


def _validated_design(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_micro_search_v4_design_artifact,
        validator_key="v4_design_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "micro search design validation failed")
    return micro_search_v4_design_report_payload(v4_design_id=artifact_id, output_dir=root)


def _validated_paper_backfill(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=st.validate_paper_shadow_backfill_artifact,
        validator_key="backfill_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "paper backfill source validation failed")
    return st.paper_shadow_backfill_report_payload(backfill_id=artifact_id, output_dir=root)


def _coerce_date(value: Any, fallback: date) -> date:
    return legacy._coerce_date(value, fallback)


def _calculate_backfill(
    *,
    design: Mapping[str, Any],
    backfill: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    generated: datetime,
    quality_checked_at: datetime | None = None,
) -> dict[str, Any]:
    baseline_states = _records(backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(backfill)
    start = max(
        _coerce_date(backfill.get("date_start"), st.AI_AFTER_CHATGPT_START),
        st.AI_AFTER_CHATGPT_START,
    )
    requested_end = _coerce_date(backfill.get("date_end"), generated.date())
    _require(requested_end >= start, "requested backfill range is empty")
    symbols = st._symbols_from_state_paths(baseline_states)
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = legacy._latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    _require(end >= start, "actual backfill range is empty")
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    quality_as_of = max(end, generated.date())
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    _require(quality.passed, f"data quality gate failed: {quality.status}")
    if quality_checked_at is not None:
        quality = replace(quality, checked_at=quality_checked_at)
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variants = _records(design.get("v4_variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for variant in variants:
        try:
            variant_states.extend(
                st._run_variant_weight_path(
                    variant=variant,
                    baseline_states=baseline_states,
                    returns=returns,
                    labels=labels,
                    config=config,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failed.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    signal = legacy._v4_variant_signal_metrics(variant_states, stability, regime)
    return {
        "start": start,
        "end": end,
        "requested_end": requested_end,
        "latest_valid_as_of": latest_valid_as_of,
        "used_latest_valid_as_of": end < requested_end,
        "quality_as_of": quality_as_of,
        "quality": quality,
        "symbols": symbols,
        "variants": variants,
        "failed": failed,
        "performance": performance,
        "regime": regime,
        "stability": stability,
        "signal": signal,
    }


def _backfill_material(
    *,
    root: Path,
    backfill_id: str,
    design: Mapping[str, Any],
    source_backfill: Mapping[str, Any],
    calculated: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    quality = calculated["quality"]
    variants = calculated["variants"]
    failed = calculated["failed"]
    performance = calculated["performance"]
    regime = calculated["regime"]
    stability = calculated["stability"]
    signal = calculated["signal"]
    quality_path = root / "validate_data_quality_report.md"
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "v4_backfill_id": backfill_id,
        "variants_total": len(variants),
        "variants_completed": len({_text(row.get("variant_id")) for row in performance}),
        "variants_failed": len(failed),
        "failed_variants": failed,
        "date_start": calculated["start"].isoformat(),
        "date_end": calculated["end"].isoformat(),
        "requested_date_end": calculated["requested_end"].isoformat(),
        "latest_valid_as_of": calculated["latest_valid_as_of"].isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": calculated["quality_as_of"].isoformat(),
        "validate_data_quality_report_path": str(quality_path),
        "used_latest_valid_as_of": calculated["used_latest_valid_as_of"],
        "calculation_cache_role": "HISTORICAL_WINDOW_INPUT",
        "data_quality_cache_role": "CURRENT_QUALITY_EVIDENCE",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    status = (
        "PASS"
        if not failed and performance
        else "PASS_WITH_WARNINGS"
        if performance
        else "FAIL"
    )
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_micro_search_v4_backfill_manifest",
        "v4_backfill_id": backfill_id,
        "v4_design_id": design.get("v4_design_id"),
        "source_backfill_id": source_backfill.get("backfill_id"),
        "source_scorecard_id": design.get("source_scorecard_id"),
        "v3_matrix_id": design.get("v3_matrix_id"),
        "v3_backfill_id": design.get("v3_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": status,
        "market_regime": source_backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": calculated["start"].isoformat(),
        "date_end": calculated["end"].isoformat(),
        "requested_start_date": source_backfill.get(
            "requested_start_date", calculated["start"].isoformat()
        ),
        "requested_end_date": calculated["requested_end"].isoformat(),
        "latest_valid_as_of": calculated["latest_valid_as_of"].isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": calculated["quality_as_of"].isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_path),
        "used_latest_valid_as_of": calculated["used_latest_valid_as_of"],
        "calculation_cache_role": progress["calculation_cache_role"],
        "data_quality_cache_role": progress["data_quality_cache_role"],
        "variants_total": len(variants),
        "variants_completed": progress["variants_completed"],
        "variants_failed": len(failed),
        "micro_search_policy_version": _policy_version(policy),
        "micro_search_v4_backfill_manifest_path": str(root / BACKFILL_VIEWS[0]),
        "v4_backfill_progress_path": str(root / BACKFILL_VIEWS[1]),
        "v4_variant_performance_path": str(root / BACKFILL_VIEWS[3]),
        "v4_variant_regime_metrics_path": str(root / BACKFILL_VIEWS[4]),
        "v4_variant_stability_metrics_path": str(root / BACKFILL_VIEWS[5]),
        "v4_variant_signal_metrics_path": str(root / BACKFILL_VIEWS[6]),
        "micro_search_v4_backfill_report_path": str(root / BACKFILL_VIEWS[7]),
        "micro_search_v4_backfill_input_snapshot_path": str(
            root / "micro_search_v4_backfill_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_micro_search_v4_backfill_report(manifest, progress)
    views = {
        BACKFILL_VIEWS[0]: foundation._json_bytes(manifest),
        BACKFILL_VIEWS[1]: foundation._json_bytes(progress),
        BACKFILL_VIEWS[2]: render_data_quality_report(quality).encode("utf-8"),
        BACKFILL_VIEWS[3]: foundation._jsonl_bytes(performance),
        BACKFILL_VIEWS[4]: foundation._jsonl_bytes(regime),
        BACKFILL_VIEWS[5]: foundation._jsonl_bytes(stability),
        BACKFILL_VIEWS[6]: foundation._jsonl_bytes(signal),
        BACKFILL_VIEWS[7]: foundation._text_file_bytes(report),
    }
    return manifest, views


@with_artifact_validation_session
def run_micro_search_v4_backfill(
    *,
    v4_design_id: str,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    design = _validated_design(v4_design_id, v4_design_dir)
    source_backfill_id = _text(design.get("source_backfill_id"))
    _require(bool(source_backfill_id), "design source_backfill_id missing")
    backfill = _validated_paper_backfill(source_backfill_id, baseline_backfill_dir)
    policy = _policy(policy_path)
    _chronology(generated, design, backfill)
    config = st._load_backfill_config_from_manifest(backfill)
    source = _mapping(config.get("source"))
    prices_path = price_cache_path or st._resolve_project_path(
        source.get("price_cache_path"), st.DEFAULT_PRICE_CACHE_PATH
    )
    calculated = _calculate_backfill(
        design=design,
        backfill=backfill,
        prices_path=prices_path,
        rates_path=rates_cache_path,
        generated=generated,
    )
    backfill_id = _stable_id(
        "micro-search-v4-backfill",
        v4_design_id,
        calculated["end"].isoformat(),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backfill_id)
    manifest, views = _backfill_material(
        root=root,
        backfill_id=root.name,
        design=design,
        source_backfill=backfill,
        calculated=calculated,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": BACKFILL_INPUT_SCHEMA,
        "v4_backfill_id": root.name,
        "generated_at": generated.isoformat(),
        "quality_checked_at": calculated["quality"].checked_at.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "design_source": _binding(
            kind="micro_search_v4_design",
            artifact_id=v4_design_id,
            root=v4_design_dir / v4_design_id,
            names=DESIGN_FILES,
        ),
        "paper_backfill_source": _binding(
            kind="paper_shadow_backfill",
            artifact_id=source_backfill_id,
            root=baseline_backfill_dir / source_backfill_id,
            names=PAPER_BACKFILL_FILES,
        ),
        "calculation_price_source": foundation._file_binding(prices_path),
        "data_quality_price_source": foundation._file_binding(prices_path),
        "data_quality_rates_source": foundation._file_binding(rates_cache_path),
        "calculation_window": {
            "date_start": calculated["start"].isoformat(),
            "date_end": calculated["end"].isoformat(),
        },
        "data_quality_as_of": calculated["quality_as_of"].isoformat(),
        "expected_symbols": list(calculated["symbols"]),
        "source_scorecard_id": design.get("source_scorecard_id"),
        "view_hashes": foundation._view_hashes(root, BACKFILL_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "micro_search_v4_backfill_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_micro_search_v4_backfill", root.name, root / BACKFILL_VIEWS[0])
    return {
        "v4_backfill_id": root.name,
        "v4_backfill_dir": root,
        "manifest": manifest,
        "v4_backfill_progress": _read_json(root / BACKFILL_VIEWS[1]),
        "v4_variant_performance": _read_jsonl(root / BACKFILL_VIEWS[3]),
        "v4_variant_regime_metrics": _read_jsonl(root / BACKFILL_VIEWS[4]),
        "v4_variant_stability_metrics": _read_jsonl(root / BACKFILL_VIEWS[5]),
        "v4_variant_signal_metrics": _read_jsonl(root / BACKFILL_VIEWS[6]),
    }


def micro_search_v4_backfill_report_payload(
    *,
    v4_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v4_backfill_id,
        latest_pointer="latest_micro_search_v4_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name=BACKFILL_VIEWS[0],
    )
    return {
        **_read_json(root / BACKFILL_VIEWS[0]),
        "v4_backfill_progress": _read_json(root / BACKFILL_VIEWS[1]),
        "v4_variant_performance": _read_jsonl(root / BACKFILL_VIEWS[3]),
        "v4_variant_regime_metrics": _read_jsonl(root / BACKFILL_VIEWS[4]),
        "v4_variant_stability_metrics": _read_jsonl(root / BACKFILL_VIEWS[5]),
        "v4_variant_signal_metrics": _read_jsonl(root / BACKFILL_VIEWS[6]),
        "input_snapshot": _read_json(root / "micro_search_v4_backfill_input_snapshot.json"),
        "v4_backfill_dir": str(root),
    }


def _rebuild_backfill(root: Path, backfill_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "micro_search_v4_backfill_input_snapshot.json")
    _require(snapshot.get("schema_version") == BACKFILL_INPUT_SCHEMA, "backfill snapshot schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    design_source = _mapping(snapshot.get("design_source"))
    paper_source = _mapping(snapshot.get("paper_backfill_source"))
    _validate_binding(design_source, kind="micro_search_v4_design")
    _validate_binding(paper_source, kind="paper_shadow_backfill")
    design = _validated_design(_binding_id(design_source), _binding_root(design_source).parent)
    backfill = _validated_paper_backfill(
        _binding_id(paper_source), _binding_root(paper_source).parent
    )
    price_source = _mapping(snapshot.get("calculation_price_source"))
    quality_price_source = _mapping(snapshot.get("data_quality_price_source"))
    rates_source = _mapping(snapshot.get("data_quality_rates_source"))
    for source in (price_source, quality_price_source, rates_source):
        foundation._validate_file_binding(source)
    _require(
        price_source == quality_price_source,
        "calculation/data-quality cache binding mismatch",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, design, backfill)
    calculated = _calculate_backfill(
        design=design,
        backfill=backfill,
        prices_path=Path(_text(price_source.get("path"))),
        rates_path=Path(_text(rates_source.get("path"))),
        generated=generated,
        quality_checked_at=_aware_time(snapshot.get("quality_checked_at"), "quality_checked_at"),
    )
    _require(
        list(calculated["symbols"]) == _texts(snapshot.get("expected_symbols")),
        "expected symbol set drift",
    )
    _, expected = _backfill_material(
        root=root,
        backfill_id=backfill_id,
        design=design,
        source_backfill=backfill,
        calculated=calculated,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_micro_search_v4_backfill_artifact(
    *,
    v4_backfill_id: str,
    output_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / v4_backfill_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="micro_search_v4_backfill_input_snapshot.json",
        schema=BACKFILL_INPUT_SCHEMA,
        id_key="v4_backfill_id",
        artifact_id=v4_backfill_id,
        view_names=BACKFILL_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_micro_search_v4_backfill_validation", v4_backfill_id, checks
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_micro_search_v4_backfill_validation",
        artifact_id=v4_backfill_id,
        checks=checks,
        rebuild=lambda: _rebuild_backfill(root, v4_backfill_id),
    )


def _validated_v4_backfill(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_micro_search_v4_backfill_artifact,
        validator_key="v4_backfill_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "micro search backfill validation failed")
    return micro_search_v4_backfill_report_payload(v4_backfill_id=artifact_id, output_dir=root)


def _v4_scorecard_rows(
    backfill: Mapping[str, Any], design: Mapping[str, Any]
) -> list[dict[str, Any]]:
    payload = {
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_performance_metrics": backfill.get("v4_variant_performance"),
        "variant_stability_metrics": backfill.get("v4_variant_stability_metrics"),
        "variant_churn_metrics": backfill.get("v4_variant_signal_metrics"),
        "variant_lag_metrics": legacy._variant_lag_metrics(
            backfill.get("v4_variant_regime_metrics", [])
        ),
        "variant_regime_metrics": backfill.get("v4_variant_regime_metrics"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return legacy._scorecard_rows(payload, _records(design.get("v4_variant_specs")))


def _gate_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    threshold: float,
    diagnostic: bool,
    high_risk_gates: set[str],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        failed = legacy._failed_gates(row)
        promoted = _float(row.get("overall_score")) >= threshold and not (
            high_risk_gates & set(failed)
        )
        result.append(
            {
                "variant_id": row.get("variant_id"),
                "overall_score": row.get("overall_score"),
                "score_threshold": threshold,
                "gate_track": (
                    "diagnostic_calibrated_gate" if diagnostic else "official_research_gate"
                ),
                "promoted": promoted,
                "candidate_status": (
                    "DIAGNOSTIC_ONLY_PROMOTED"
                    if diagnostic and promoted
                    else "PROMOTED"
                    if promoted
                    else "REJECTED"
                ),
                "failed_gates": failed,
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return result


def _gate_review_material(
    *,
    root: Path,
    gate_review_id: str,
    backfill: Mapping[str, Any],
    design: Mapping[str, Any],
    gate: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    _require(
        _text(backfill.get("v4_design_id")) == _text(design.get("v4_design_id")),
        "backfill/design lineage mismatch",
    )
    _require(
        _text(design.get("source_scorecard_id")) == _text(gate.get("source_scorecard_id")),
        "gate calibration/design scorecard mismatch",
    )
    rules = _mapping(policy.get("gate_review"))
    official_threshold = _number(rules.get("official_research_score_min"), "official score")
    relaxation = _number(rules.get("diagnostic_relaxation"), "diagnostic relaxation")
    high_risk = set(_texts(rules.get("high_risk_failed_gates")))
    scorecard = _v4_scorecard_rows(backfill, design)
    official = _gate_rows(
        scorecard,
        threshold=official_threshold,
        diagnostic=False,
        high_risk_gates=high_risk,
    )
    diagnostic_rows = _gate_rows(
        scorecard,
        threshold=official_threshold - relaxation,
        diagnostic=True,
        high_risk_gates=high_risk,
    )
    official_promoted = {_text(row.get("variant_id")) for row in official if row.get("promoted")}
    diagnostic_promoted = {
        _text(row.get("variant_id")) for row in diagnostic_rows if row.get("promoted")
    }
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "official_gate_promoted_count": len(official_promoted),
        "diagnostic_gate_promoted_count": len(diagnostic_promoted),
        "diagnostic_only_candidates": sorted(diagnostic_promoted - official_promoted),
        "official_research_score_min": official_threshold,
        "diagnostic_score_min": official_threshold - relaxation,
        "diagnostic_candidate_role": rules.get("diagnostic_candidate_role"),
        "gate_policy_change_recommended": False,
        "official_gate_changed": False,
        "recommended_next_action": "signal_vs_parameter_attribution",
        "source_gate_calibrated_assessment": _mapping(
            gate.get("gate_strictness_diagnosis")
        ).get("calibrated_assessment"),
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_gate_calibrated_review_manifest",
        "gate_review_id": gate_review_id,
        "v4_backfill_id": backfill.get("v4_backfill_id"),
        "v4_design_id": design.get("v4_design_id"),
        "gate_calibration_id": gate.get("gate_calibration_id"),
        "source_scorecard_id": design.get("source_scorecard_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if scorecard else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "micro_search_policy_version": _policy_version(policy),
        "gate_calibrated_review_manifest_path": str(root / GATE_REVIEW_VIEWS[0]),
        "official_gate_results_path": str(root / GATE_REVIEW_VIEWS[1]),
        "diagnostic_gate_results_path": str(root / GATE_REVIEW_VIEWS[2]),
        "gate_calibrated_summary_path": str(root / GATE_REVIEW_VIEWS[3]),
        "gate_calibrated_review_report_path": str(root / GATE_REVIEW_VIEWS[4]),
        "gate_calibrated_review_input_snapshot_path": str(
            root / "gate_calibrated_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = legacy.render_gate_calibrated_review_report(manifest, summary)
    views = {
        GATE_REVIEW_VIEWS[0]: foundation._json_bytes(manifest),
        GATE_REVIEW_VIEWS[1]: foundation._jsonl_bytes(official),
        GATE_REVIEW_VIEWS[2]: foundation._jsonl_bytes(diagnostic_rows),
        GATE_REVIEW_VIEWS[3]: foundation._json_bytes(summary),
        GATE_REVIEW_VIEWS[4]: foundation._text_file_bytes(report),
    }
    return manifest, views


@with_artifact_validation_session
def run_gate_calibrated_review(
    *,
    v4_backfill_id: str,
    gate_calibration_id: str,
    v4_backfill_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    gate_calibration_dir: Path = DEFAULT_GATE_CALIBRATION_REVIEW_DIR,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    backfill = _validated_v4_backfill(v4_backfill_id, v4_backfill_dir)
    design_id = _text(backfill.get("v4_design_id"))
    design = _validated_design(design_id, v4_design_dir)
    gate = _validated_gate(gate_calibration_id, gate_calibration_dir)
    policy = _policy(policy_path)
    _chronology(generated, backfill, design, gate)
    gate_review_id = _stable_id(
        "gate-calibrated-review", v4_backfill_id, gate_calibration_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / gate_review_id)
    manifest, views = _gate_review_material(
        root=root,
        gate_review_id=root.name,
        backfill=backfill,
        design=design,
        gate=gate,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": GATE_REVIEW_INPUT_SCHEMA,
        "gate_review_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "backfill_source": _binding(
            kind="micro_search_v4_backfill",
            artifact_id=v4_backfill_id,
            root=v4_backfill_dir / v4_backfill_id,
            names=BACKFILL_FILES,
        ),
        "design_source": _binding(
            kind="micro_search_v4_design",
            artifact_id=design_id,
            root=v4_design_dir / design_id,
            names=DESIGN_FILES,
        ),
        "gate_calibration_source": _binding(
            kind="gate_calibration_review",
            artifact_id=gate_calibration_id,
            root=gate_calibration_dir / gate_calibration_id,
            names=diagnosis.GATE_FILES,
        ),
        "source_scorecard_id": manifest["source_scorecard_id"],
        "view_hashes": foundation._view_hashes(root, GATE_REVIEW_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "gate_calibrated_review_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_gate_calibrated_review", root.name, root / GATE_REVIEW_VIEWS[0])
    return {
        "gate_review_id": root.name,
        "gate_review_dir": root,
        "manifest": manifest,
        "official_gate_results": _read_jsonl(root / GATE_REVIEW_VIEWS[1]),
        "diagnostic_gate_results": _read_jsonl(root / GATE_REVIEW_VIEWS[2]),
        "gate_calibrated_summary": _read_json(root / GATE_REVIEW_VIEWS[3]),
    }


def gate_calibrated_review_report_payload(
    *,
    gate_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=gate_review_id,
        latest_pointer="latest_gate_calibrated_review",
        latest=latest,
        output_dir=output_dir,
        required_name=GATE_REVIEW_VIEWS[0],
    )
    return {
        **_read_json(root / GATE_REVIEW_VIEWS[0]),
        "official_gate_results": _read_jsonl(root / GATE_REVIEW_VIEWS[1]),
        "diagnostic_gate_results": _read_jsonl(root / GATE_REVIEW_VIEWS[2]),
        "gate_calibrated_summary": _read_json(root / GATE_REVIEW_VIEWS[3]),
        "input_snapshot": _read_json(root / "gate_calibrated_review_input_snapshot.json"),
        "gate_review_dir": str(root),
    }


def _rebuild_gate_review(root: Path, gate_review_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "gate_calibrated_review_input_snapshot.json")
    _require(snapshot.get("schema_version") == GATE_REVIEW_INPUT_SCHEMA, "gate snapshot schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    design_source = _mapping(snapshot.get("design_source"))
    gate_source = _mapping(snapshot.get("gate_calibration_source"))
    _validate_binding(backfill_source, kind="micro_search_v4_backfill")
    _validate_binding(design_source, kind="micro_search_v4_design")
    _validate_binding(gate_source, kind="gate_calibration_review")
    backfill = _validated_v4_backfill(
        _binding_id(backfill_source), _binding_root(backfill_source).parent
    )
    design = _validated_design(_binding_id(design_source), _binding_root(design_source).parent)
    gate = _validated_gate(_binding_id(gate_source), _binding_root(gate_source).parent)
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, backfill, design, gate)
    _, expected = _gate_review_material(
        root=root,
        gate_review_id=gate_review_id,
        backfill=backfill,
        design=design,
        gate=gate,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_gate_calibrated_review_artifact(
    *,
    gate_review_id: str,
    output_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / gate_review_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="gate_calibrated_review_input_snapshot.json",
        schema=GATE_REVIEW_INPUT_SCHEMA,
        id_key="gate_review_id",
        artifact_id=gate_review_id,
        view_names=GATE_REVIEW_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_gate_calibrated_review_validation", gate_review_id, checks
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_gate_calibrated_review_validation",
        artifact_id=gate_review_id,
        checks=checks,
        rebuild=lambda: _rebuild_gate_review(root, gate_review_id),
    )


def _validated_gate_review(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_gate_calibrated_review_artifact,
        validator_key="gate_review_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "gate review source validation failed")
    return gate_calibrated_review_report_payload(gate_review_id=artifact_id, output_dir=root)


def _failure_and_shift(
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
    gate_review: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    rules = _mapping(policy.get("failure_attribution"))
    signal_status = _text(signal.get("evidence_status"))
    consensus_status = _text(consensus.get("evidence_status"))
    if signal_status == rules.get("missing_dated_signal_status") or consensus_status == rules.get(
        "missing_dated_consensus_status"
    ):
        gate_summary = _mapping(gate_review.get("gate_calibrated_summary"))
        failure = {
            "schema_version": st.SCHEMA_VERSION,
            "failure_source": rules.get("insufficient_failure_source"),
            "confidence": rules.get("insufficient_confidence"),
            "evidence_status": "INSUFFICIENT_DATA",
            "evidence": [
                f"signal_evidence_status={signal_status}",
                f"consensus_evidence_status={consensus_status}",
                f"official_gate_promoted_count={gate_summary.get('official_gate_promoted_count')}",
                f"diagnostic_gate_promoted_count={gate_summary.get('diagnostic_gate_promoted_count')}",
            ],
            "parameter_search_still_promising": None,
            "signal_level_fix_required": None,
            "market_regime_failure_claimed": False,
            "policy_version": _policy_version(policy),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        shift = {
            "schema_version": st.SCHEMA_VERSION,
            "recommended_shift": rules.get("insufficient_recommended_shift"),
            "reason": [
                "failure_source is not identifiable without dated signal and consensus evidence",
                "gate sensitivity cannot identify an unobserved signal failure source",
            ],
            "next_task_family": rules.get("insufficient_next_task_family"),
            "policy_version": _policy_version(policy),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        return failure, shift
    failure = legacy._signal_vs_parameter_failure_source(signal, consensus, gate_review)
    failure["evidence_status"] = "SUFFICIENT"
    failure["policy_version"] = _policy_version(policy)
    shift = legacy._recommended_research_shift(failure, consensus)
    shift["policy_version"] = _policy_version(policy)
    return failure, shift


def _attribution_material(
    *,
    root: Path,
    attribution_id: str,
    signal: Mapping[str, Any],
    consensus: Mapping[str, Any],
    gate_review: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    _require(
        _text(signal.get("signal_diagnosis_id")) == _text(consensus.get("signal_diagnosis_id")),
        "signal/consensus lineage mismatch",
    )
    _require(
        _text(signal.get("scorecard_id")) == _text(gate_review.get("source_scorecard_id")),
        "signal/gate scorecard lineage mismatch",
    )
    failure, shift = _failure_and_shift(signal, consensus, gate_review, policy)
    insufficient = failure.get("evidence_status") == "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_vs_parameter_manifest",
        "attribution_id": attribution_id,
        "signal_diagnosis_id": signal.get("signal_diagnosis_id"),
        "consensus_review_id": consensus.get("consensus_review_id"),
        "gate_review_id": gate_review.get("gate_review_id"),
        "source_scorecard_id": signal.get("scorecard_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if insufficient else "PASS",
        "evidence_status": failure.get("evidence_status"),
        "market_regime": signal.get("market_regime", "ai_after_chatgpt"),
        "micro_search_policy_version": _policy_version(policy),
        "signal_vs_parameter_manifest_path": str(root / ATTRIBUTION_VIEWS[0]),
        "failure_source_attribution_path": str(root / ATTRIBUTION_VIEWS[1]),
        "recommended_research_shift_path": str(root / ATTRIBUTION_VIEWS[2]),
        "signal_vs_parameter_attribution_report_path": str(root / ATTRIBUTION_VIEWS[3]),
        "reader_brief_section_path": str(root / ATTRIBUTION_VIEWS[4]),
        "signal_vs_parameter_attribution_input_snapshot_path": str(
            root / "signal_vs_parameter_attribution_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = legacy.render_signal_vs_parameter_reader_brief(failure, shift)
    report = legacy.render_signal_vs_parameter_attribution_report(manifest, failure, shift)
    views = {
        ATTRIBUTION_VIEWS[0]: foundation._json_bytes(manifest),
        ATTRIBUTION_VIEWS[1]: foundation._json_bytes(failure),
        ATTRIBUTION_VIEWS[2]: foundation._json_bytes(shift),
        ATTRIBUTION_VIEWS[3]: foundation._text_file_bytes(report),
        ATTRIBUTION_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def run_signal_vs_parameter_attribution(
    *,
    signal_diagnosis_id: str,
    consensus_review_id: str,
    gate_review_id: str,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    gate_review_dir: Path = DEFAULT_GATE_CALIBRATED_REVIEW_DIR,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    signal = _validated_signal(signal_diagnosis_id, signal_dir)
    consensus = _validated_consensus(consensus_review_id, consensus_dir)
    gate_review = _validated_gate_review(gate_review_id, gate_review_dir)
    policy = _policy(policy_path)
    _chronology(generated, signal, consensus, gate_review)
    attribution_id = _stable_id(
        "signal-vs-parameter-attribution",
        signal_diagnosis_id,
        consensus_review_id,
        gate_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / attribution_id)
    manifest, views = _attribution_material(
        root=root,
        attribution_id=root.name,
        signal=signal,
        consensus=consensus,
        gate_review=gate_review,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": ATTRIBUTION_INPUT_SCHEMA,
        "attribution_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "signal_source": _binding(
            kind="signal_instability_diagnosis",
            artifact_id=signal_diagnosis_id,
            root=signal_dir / signal_diagnosis_id,
            names=diagnosis.SIGNAL_FILES,
        ),
        "consensus_source": _binding(
            kind="consensus_quality_review",
            artifact_id=consensus_review_id,
            root=consensus_dir / consensus_review_id,
            names=diagnosis.CONSENSUS_FILES,
        ),
        "gate_review_source": _binding(
            kind="gate_calibrated_review",
            artifact_id=gate_review_id,
            root=gate_review_dir / gate_review_id,
            names=GATE_REVIEW_FILES,
        ),
        "source_scorecard_id": manifest["source_scorecard_id"],
        "view_hashes": foundation._view_hashes(root, ATTRIBUTION_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "signal_vs_parameter_attribution_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_signal_vs_parameter_attribution", root.name, root / ATTRIBUTION_VIEWS[0]
    )
    return {
        "signal_vs_parameter_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "failure_source_attribution": _read_json(root / ATTRIBUTION_VIEWS[1]),
        "recommended_research_shift": _read_json(root / ATTRIBUTION_VIEWS[2]),
        "reader_brief_section": (root / ATTRIBUTION_VIEWS[4]).read_text(encoding="utf-8"),
    }


def signal_vs_parameter_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_signal_vs_parameter_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name=ATTRIBUTION_VIEWS[0],
    )
    return {
        **_read_json(root / ATTRIBUTION_VIEWS[0]),
        "failure_source_attribution": _read_json(root / ATTRIBUTION_VIEWS[1]),
        "recommended_research_shift": _read_json(root / ATTRIBUTION_VIEWS[2]),
        "reader_brief_section": (root / ATTRIBUTION_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(
            root / "signal_vs_parameter_attribution_input_snapshot.json"
        ),
        "attribution_dir": str(root),
    }


def _rebuild_attribution(root: Path, attribution_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_vs_parameter_attribution_input_snapshot.json")
    _require(snapshot.get("schema_version") == ATTRIBUTION_INPUT_SCHEMA, "attribution schema")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    signal_source = _mapping(snapshot.get("signal_source"))
    consensus_source = _mapping(snapshot.get("consensus_source"))
    gate_source = _mapping(snapshot.get("gate_review_source"))
    _validate_binding(signal_source, kind="signal_instability_diagnosis")
    _validate_binding(consensus_source, kind="consensus_quality_review")
    _validate_binding(gate_source, kind="gate_calibrated_review")
    signal = _validated_signal(_binding_id(signal_source), _binding_root(signal_source).parent)
    consensus = _validated_consensus(
        _binding_id(consensus_source), _binding_root(consensus_source).parent
    )
    gate_review = _validated_gate_review(
        _binding_id(gate_source), _binding_root(gate_source).parent
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, signal, consensus, gate_review)
    _, expected = _attribution_material(
        root=root,
        attribution_id=attribution_id,
        signal=signal,
        consensus=consensus,
        gate_review=gate_review,
        policy=policy,
        generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_signal_vs_parameter_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root,
        snapshot_name="signal_vs_parameter_attribution_input_snapshot.json",
        schema=ATTRIBUTION_INPUT_SCHEMA,
        id_key="attribution_id",
        artifact_id=attribution_id,
        view_names=ATTRIBUTION_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_signal_vs_parameter_attribution_validation",
            attribution_id,
            checks,
        )
    return diagnosis._validate_content(
        report_type="etf_dynamic_v3_signal_vs_parameter_attribution_validation",
        artifact_id=attribution_id,
        checks=checks,
        rebuild=lambda: _rebuild_attribution(root, attribution_id),
    )


__all__ = [
    "ATTRIBUTION_FILES",
    "BACKFILL_FILES",
    "DEFAULT_GATE_CALIBRATED_REVIEW_DIR",
    "DEFAULT_MICRO_SEARCH_FOUNDATION_POLICY_PATH",
    "DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR",
    "DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR",
    "DEFAULT_SIGNAL_VS_PARAMETER_ATTRIBUTION_DIR",
    "DESIGN_FILES",
    "GATE_REVIEW_FILES",
    "gate_calibrated_review_report_payload",
    "micro_search_v4_backfill_report_payload",
    "micro_search_v4_design_report_payload",
    "run_gate_calibrated_review",
    "run_micro_search_v4_backfill",
    "run_micro_search_v4_design",
    "run_signal_vs_parameter_attribution",
    "signal_vs_parameter_attribution_report_payload",
    "validate_gate_calibrated_review_artifact",
    "validate_micro_search_v4_backfill_artifact",
    "validate_micro_search_v4_design_artifact",
    "validate_signal_vs_parameter_attribution_artifact",
]
