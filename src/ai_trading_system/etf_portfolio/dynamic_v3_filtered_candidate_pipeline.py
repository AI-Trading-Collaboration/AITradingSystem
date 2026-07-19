from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as diagnosis_foundation,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_filter_foundation as signal_filter,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR = (
    signal_filter.DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR
)
DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR = signal_filter.DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR
DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_backfill"
)
DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_vs_original_comparison"
)
DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_gate_experiment"
DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "filtered_candidate_promotion_review"
)
DEFAULT_OWNER_SIGNAL_ROADMAP_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "owner_signal_roadmap"

BACKFILL_INPUT_SCHEMA = "filtered_candidate_backfill_input_snapshot.v2"
COMPARISON_INPUT_SCHEMA = "filtered_vs_original_comparison_input_snapshot.v2"
GATE_INPUT_SCHEMA = "signal_gate_experiment_input_snapshot.v2"
REVIEW_INPUT_SCHEMA = "filtered_candidate_promotion_review_input_snapshot.v2"
ROADMAP_INPUT_SCHEMA = "owner_signal_roadmap_input_snapshot.v2"

BACKFILL_VIEWS = (
    "filtered_candidate_backfill_manifest.json",
    "filtered_variant_specs.jsonl",
    "filtered_variant_performance.jsonl",
    "filtered_variant_signal_metrics.jsonl",
    "filtered_candidate_backfill_report.md",
)
COMPARISON_VIEWS = (
    "filtered_vs_original_manifest.json",
    "filtered_comparison_matrix.jsonl",
    "filtered_improvement_summary.json",
    "filtered_vs_original_comparison_report.md",
)
GATE_VIEWS = (
    "signal_gate_experiment_manifest.json",
    "signal_gate_experiment_results.jsonl",
    "signal_gate_experiment_summary.json",
    "signal_gate_experiment_report.md",
    "reader_brief_section.md",
)
REVIEW_VIEWS = (
    "filtered_promotion_manifest.json",
    "filtered_promotion_decision.json",
    "filtered_candidate_specs.json",
    "filtered_candidate_promotion_review_report.md",
    "reader_brief_section.md",
)
ROADMAP_VIEWS = (
    "owner_signal_roadmap_manifest.json",
    "owner_signal_roadmap_summary.json",
    "owner_signal_checklist.md",
    "owner_signal_roadmap_report.md",
    "reader_brief_section.md",
)

BACKFILL_FILES = (*BACKFILL_VIEWS, "filtered_candidate_backfill_input_snapshot.json")
COMPARISON_FILES = (*COMPARISON_VIEWS, "filtered_vs_original_comparison_input_snapshot.json")
GATE_FILES = (*GATE_VIEWS, "signal_gate_experiment_input_snapshot.json")
REVIEW_FILES = (*REVIEW_VIEWS, "filtered_candidate_promotion_review_input_snapshot.json")
ROADMAP_FILES = (*ROADMAP_VIEWS, "owner_signal_roadmap_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_texts = foundation._texts
_text = foundation._text
_float = foundation._float
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_write_latest_pointer = foundation._write_latest_pointer
_read_json = foundation._read_json
_read_jsonl = foundation._read_jsonl
_artifact_dir = foundation._artifact_dir
_validation_payload = foundation._validation_payload
_payload_experiment_safe = foundation._payload_experiment_safe


class DynamicV3FilteredCandidatePipelineError(ValueError):
    """Raised when TRADING-331～335 evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3FilteredCandidatePipelineError(message)


def _optional_input_snapshot(root: Path, name: str) -> dict[str, Any]:
    """Expose v2 lineage without breaking pre-EB2 report artifacts.

    EB2 writers and validators always require the versioned snapshot contract. The
    public report readers pre-date those snapshots, so already-materialized artifacts
    can legitimately contain only the legacy views. Keeping the field absent for those
    artifacts preserves the old read contract; newly written artifacts still expose
    their complete lineage.
    """

    path = root / name
    return {"input_snapshot": _read_json(path)} if path.is_file() else {}


def _generated_time(value: datetime | None) -> datetime:
    try:
        return signal_filter._generated_time(value)
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _aware_time(value: Any, field: str) -> datetime:
    try:
        return signal_filter._aware_time(value, field)
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _chronology(generated: datetime, *payloads: Mapping[str, Any]) -> None:
    try:
        signal_filter._chronology(generated, *payloads)
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return signal_filter._binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _binding_id(binding: Mapping[str, Any]) -> str:
    return signal_filter._binding_id(binding)


def _binding_root(binding: Mapping[str, Any]) -> Path:
    return signal_filter._binding_root(binding)


def _validate_binding(binding: Mapping[str, Any], *, kind: str, names: Sequence[str]) -> None:
    try:
        signal_filter._validate_binding(binding, kind=kind, names=names)
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _policy_source(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return signal_filter._source_policy_binding(payload)
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _policy(binding: Mapping[str, Any]) -> dict[str, Any]:
    foundation._validate_file_binding(binding)
    try:
        return signal_filter._policy(Path(_text(binding.get("path"))))
    except ValueError as exc:
        raise DynamicV3FilteredCandidatePipelineError(str(exc)) from exc


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    signal_filter._write_views(root, views)


def _validated_filter(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=signal_filter.validate_candidate_quality_filter_design_artifact,
        validator_key="filter_design_id",
        artifact_id=artifact_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "filter design source validation failed")
    payload = signal_filter.candidate_quality_filter_design_report_payload(
        filter_design_id=artifact_id,
        output_dir=output_dir,
    )
    _require(_text(payload.get("filter_design_id")) == artifact_id, "filter design id mismatch")
    return payload


def _validated_ledger(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=signal_filter.validate_candidate_signal_ledger_artifact,
        validator_key="ledger_id",
        artifact_id=artifact_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "candidate ledger source validation failed")
    payload = signal_filter.candidate_signal_ledger_report_payload(
        ledger_id=artifact_id,
        output_dir=output_dir,
    )
    _require(_text(payload.get("ledger_id")) == artifact_id, "candidate ledger id mismatch")
    return payload


def _validated_filter_and_ledger(
    *, filter_design_id: str, filter_design_dir: Path, ledger_dir: Path
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    design = _validated_filter(filter_design_id, filter_design_dir)
    ledger_id = _text(design.get("source_ledger_id"))
    _require(bool(ledger_id), "filter design ledger lineage missing")
    ledger = _validated_ledger(ledger_id, ledger_dir)
    design_policy = _policy_source(design)
    ledger_policy = _policy_source(ledger)
    _require(design_policy == ledger_policy, "filter/ledger policy lineage mismatch")
    return design, ledger, design_policy, _policy(design_policy)


def _validated_local(
    *,
    artifact_id: str,
    output_dir: Path,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    reader: Callable[..., dict[str, Any]],
    reader_key: str,
    label: str,
) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validator,
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", f"{label} source validation failed")
    payload = reader(**{reader_key: artifact_id, "output_dir": output_dir})
    _require(_text(payload.get(reader_key)) == artifact_id, f"{label} id mismatch")
    return payload


def run_filtered_candidate_backfill(
    *,
    filter_design_id: str,
    filter_design_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = _run_backfill(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )
    return _run_payload(
        payload,
        directory_key="filtered_backfill_dir",
        manifest_name=BACKFILL_VIEWS[0],
    )


def filtered_candidate_backfill_report_payload(
    *,
    filtered_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> dict[str, Any]:
    return _backfill_report(
        filtered_backfill_id=filtered_backfill_id,
        latest=latest,
        output_dir=output_dir,
    )


def validate_filtered_candidate_backfill_artifact(
    *,
    filtered_backfill_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
) -> dict[str, Any]:
    return _validate_backfill(filtered_backfill_id=filtered_backfill_id, output_dir=output_dir)


def run_filtered_vs_original_comparison(
    *,
    filtered_backfill_id: str,
    filtered_backfill_dir: Path = DEFAULT_FILTERED_CANDIDATE_BACKFILL_DIR,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = _run_comparison(
        filtered_backfill_id=filtered_backfill_id,
        filtered_backfill_dir=filtered_backfill_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )
    return _run_payload(
        payload,
        directory_key="comparison_dir",
        manifest_name=COMPARISON_VIEWS[0],
    )


def filtered_vs_original_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> dict[str, Any]:
    return _comparison_report(comparison_id=comparison_id, latest=latest, output_dir=output_dir)


def validate_filtered_vs_original_comparison_artifact(
    *,
    comparison_id: str,
    output_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
) -> dict[str, Any]:
    return _validate_comparison(comparison_id=comparison_id, output_dir=output_dir)


def run_signal_gate_experiment(
    *,
    filter_design_id: str,
    filter_design_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = _run_gate(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )
    return _run_payload(
        payload,
        directory_key="signal_gate_experiment_dir",
        manifest_name=GATE_VIEWS[0],
    )


def signal_gate_experiment_report_payload(
    *,
    signal_gate_experiment_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> dict[str, Any]:
    return _gate_report(
        signal_gate_experiment_id=signal_gate_experiment_id,
        latest=latest,
        output_dir=output_dir,
    )


def validate_signal_gate_experiment_artifact(
    *,
    signal_gate_experiment_id: str,
    output_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
) -> dict[str, Any]:
    return _validate_gate(
        signal_gate_experiment_id=signal_gate_experiment_id,
        output_dir=output_dir,
    )


def run_filtered_candidate_promotion_review(
    *,
    comparison_id: str,
    signal_gate_experiment_id: str,
    comparison_dir: Path = DEFAULT_FILTERED_VS_ORIGINAL_COMPARISON_DIR,
    experiment_dir: Path = DEFAULT_SIGNAL_GATE_EXPERIMENT_DIR,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = _run_review(
        comparison_id=comparison_id,
        signal_gate_experiment_id=signal_gate_experiment_id,
        comparison_dir=comparison_dir,
        experiment_dir=experiment_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )
    return _run_payload(
        payload,
        directory_key="filtered_review_dir",
        manifest_name=REVIEW_VIEWS[0],
    )


def filtered_candidate_promotion_review_report_payload(
    *,
    filtered_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    return _review_report(
        filtered_review_id=filtered_review_id,
        latest=latest,
        output_dir=output_dir,
    )


def validate_filtered_candidate_promotion_review_artifact(
    *,
    filtered_review_id: str,
    output_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    return _validate_review(filtered_review_id=filtered_review_id, output_dir=output_dir)


def build_owner_signal_roadmap(
    *,
    filtered_review_id: str,
    review_dir: Path = DEFAULT_FILTERED_CANDIDATE_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = _run_roadmap(
        filtered_review_id=filtered_review_id,
        review_dir=review_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )
    return _run_payload(
        payload,
        directory_key="owner_signal_roadmap_dir",
        manifest_name=ROADMAP_VIEWS[0],
    )


def owner_signal_roadmap_report_payload(
    *,
    owner_signal_roadmap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> dict[str, Any]:
    return _roadmap_report(
        owner_signal_roadmap_id=owner_signal_roadmap_id,
        latest=latest,
        output_dir=output_dir,
    )


def validate_owner_signal_roadmap_artifact(
    *,
    owner_signal_roadmap_id: str,
    output_dir: Path = DEFAULT_OWNER_SIGNAL_ROADMAP_DIR,
) -> dict[str, Any]:
    return _validate_roadmap(
        owner_signal_roadmap_id=owner_signal_roadmap_id,
        output_dir=output_dir,
    )


def _run_payload(
    payload: Mapping[str, Any], *, directory_key: str, manifest_name: str
) -> dict[str, Any]:
    result = dict(payload)
    result["manifest"] = _read_json(Path(_text(payload.get(directory_key))) / manifest_name)
    return result


def _filter_rows(design: Mapping[str, Any]) -> list[dict[str, Any]]:
    proposed = _mapping(design.get("proposed_quality_filters"))
    if proposed.get("evidence_status") != "SUFFICIENT":
        return []
    return _records(proposed.get("filters"))


def _ledger_events(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = _mapping(ledger.get("candidate_signal_summary"))
    if summary.get("evidence_status") != "SUFFICIENT":
        return []
    return _records(ledger.get("signal_events"))


def _dated_methods(ledger: Mapping[str, Any]) -> list[str]:
    return sorted({_text(row.get("method")) for row in _ledger_events(ledger) if row.get("method")})


def _screening_specs(
    design: Mapping[str, Any], ledger: Mapping[str, Any]
) -> list[dict[str, Any]]:
    filters = _filter_rows(design)
    methods = _dated_methods(ledger)
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant_id": _stable_id("filtered-screening-spec", method, row.get("filter_id")),
            "base_method": method,
            "applied_filters": [_text(row.get("filter_id"))],
            "filter_descriptions": [_text(row.get("intended_effect"))],
            "candidate_status": "SCREENING_SPEC_ONLY",
            "outcome_backfill_available": False,
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for method in methods
        for row in filters
        if row.get("filter_id")
    ]


def _event_matches_filter(
    event: Mapping[str, Any], filter_id: str, policy: Mapping[str, Any]
) -> bool | None:
    modes = set(_texts(event.get("failure_modes")))
    rules = _mapping(policy.get("signal_quality_policy"))
    if filter_id == "high_dispersion_hold_filter":
        value = event.get("candidate_dispersion")
        return (
            None
            if value is None
            else _float(value) >= _float(rules.get("candidate_dispersion_threshold"))
        )
    if filter_id == "signal_persistence_3d_filter":
        value = event.get("persistence_days")
        return None if value is None else _float(value) < _float(rules.get("persistence_days"))
    if filter_id == "regime_mismatch_filter":
        return "regime_mismatch" in modes
    if filter_id == "low_confidence_reduce_tilt_filter":
        return bool(modes & {"candidate_disagreement_high", "consensus_dispersion_high"})
    if filter_id == "top_candidate_stability_filter":
        return bool(modes & {"unstable_top_candidate", "direction_flip_high"})
    return None


def _screening_metrics(
    specs: Sequence[Mapping[str, Any]], ledger: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    events = _ledger_events(ledger)
    rows: list[dict[str, Any]] = []
    for spec in specs:
        method_events = [row for row in events if row.get("method") == spec.get("base_method")]
        filter_id = _text(_texts(spec.get("applied_filters"))[0])
        outcomes = [_event_matches_filter(row, filter_id, policy) for row in method_events]
        evaluable = [value for value in outcomes if value is not None]
        triggered = sum(1 for value in evaluable if value)
        harmful_triggered = sum(
            1
            for row, value in zip(method_events, outcomes, strict=True)
            if value is True and row.get("event_quality") == "HARMFUL"
        )
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "variant_id": spec.get("variant_id"),
                "base_method": spec.get("base_method"),
                "observed_event_count": len(method_events),
                "evaluable_event_count": len(evaluable),
                "triggered_event_count": triggered,
                "harmful_triggered_event_count": harmful_triggered,
                "direction_flip_delta_vs_base": None,
                "signal_churn_delta_vs_base": None,
                "harmful_event_delta_vs_base": None,
                "regime_mismatch_delta_vs_base": None,
                "filter_effect_status": (
                    "OBSERVED_SCREENING_ONLY" if evaluable else "INSUFFICIENT_FILTER_FIELDS"
                ),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _performance_placeholders(specs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant_id": row.get("variant_id"),
            "base_method": row.get("base_method"),
            "return_delta_vs_base": None,
            "drawdown_delta_vs_base": None,
            "regime_score_delta_vs_base": None,
            "signal_churn_delta_vs_base": None,
            "outcome_sample_count": 0,
            "outcome_evidence_status": "INSUFFICIENT_DATA",
            "metric_source": "NO_VALIDATED_FILTERED_OUTCOME_COHORT",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for row in specs
    ]


def _backfill_evidence_status(
    design: Mapping[str, Any], ledger: Mapping[str, Any], specs: Sequence[Mapping[str, Any]]
) -> str:
    if not _filter_rows(design) or not _ledger_events(ledger):
        return "INSUFFICIENT_DATA"
    return "INSUFFICIENT_FILTERED_OUTCOME_DATA" if specs else "INSUFFICIENT_DATA"


def _backfill_material(
    *,
    root: Path,
    artifact_id: str,
    design: Mapping[str, Any],
    ledger: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    specs = _screening_specs(design, ledger)
    performance = _performance_placeholders(specs)
    signal_metrics = _screening_metrics(specs, ledger, policy)
    evidence_status = _backfill_evidence_status(design, ledger, specs)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_backfill_manifest",
        "filtered_backfill_id": artifact_id,
        "filter_design_id": design.get("filter_design_id"),
        "source_ledger_id": ledger.get("ledger_id"),
        "source_backfill_id": ledger.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS",
        "evidence_status": evidence_status,
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "policy_version": ledger.get("policy_version"),
        "filtered_candidate_backfill_manifest_path": str(root / BACKFILL_VIEWS[0]),
        "filtered_variant_specs_path": str(root / BACKFILL_VIEWS[1]),
        "filtered_variant_performance_path": str(root / BACKFILL_VIEWS[2]),
        "filtered_variant_signal_metrics_path": str(root / BACKFILL_VIEWS[3]),
        "filtered_candidate_backfill_report_path": str(root / BACKFILL_VIEWS[4]),
        "filtered_candidate_backfill_input_snapshot_path": str(
            root / "filtered_candidate_backfill_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        BACKFILL_VIEWS[0]: foundation._json_bytes(manifest),
        BACKFILL_VIEWS[1]: foundation._jsonl_bytes(specs),
        BACKFILL_VIEWS[2]: foundation._jsonl_bytes(performance),
        BACKFILL_VIEWS[3]: foundation._jsonl_bytes(signal_metrics),
        BACKFILL_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_candidate_backfill_report(manifest, specs, performance, signal_metrics)
        ),
    }
    return manifest, views


def render_filtered_candidate_backfill_report(
    manifest: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    performance: Sequence[Mapping[str, Any]],
    signal_metrics: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Filtered Candidate Backfill {manifest.get('filtered_backfill_id')}",
            "",
            f"- evidence_status：{manifest.get('evidence_status')}",
            f"- filter_design_id：{manifest.get('filter_design_id')}",
            f"- source_ledger_id：{manifest.get('source_ledger_id')}",
            f"- market_regime：{manifest.get('market_regime')}",
            f"- date_range：{manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- candidate_spec_count：{len(specs)}",
            f"- performance_row_count：{len(performance)}",
            f"- signal_metric_row_count：{len(signal_metrics)}",
            "- 结论边界：没有validated filtered outcome cohort时，所有表现delta保持null。",
            "- safety：research screening only / no official weights / no broker / no production",
            "",
        ]
    )


@with_artifact_validation_session
def _run_backfill(
    *,
    filter_design_id: str,
    filter_design_dir: Path,
    ledger_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    design, ledger, policy_source, policy = _validated_filter_and_ledger(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
    )
    _chronology(generated, design, ledger)
    artifact_id = _stable_id("filtered-candidate-backfill", filter_design_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _backfill_material(
        root=root,
        artifact_id=root.name,
        design=design,
        ledger=ledger,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": BACKFILL_INPUT_SCHEMA,
        "filtered_backfill_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "filter_design_source": _binding(
            kind="candidate_quality_filter_design",
            artifact_id=filter_design_id,
            root=filter_design_dir / filter_design_id,
            names=signal_filter.FILTER_FILES,
        ),
        "ledger_source": _binding(
            kind="candidate_signal_ledger",
            artifact_id=_text(ledger.get("ledger_id")),
            root=ledger_dir / _text(ledger.get("ledger_id")),
            names=signal_filter.LEDGER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, BACKFILL_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "filtered_candidate_backfill_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_filtered_candidate_backfill", root.name, root / BACKFILL_VIEWS[0])
    return _backfill_report(filtered_backfill_id=root.name, latest=False, output_dir=output_dir)


def _backfill_report(
    *, filtered_backfill_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filtered_backfill_id,
        latest_pointer="latest_filtered_candidate_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name=BACKFILL_VIEWS[0],
    )
    return {
        **_read_json(root / BACKFILL_VIEWS[0]),
        "filtered_variant_specs": _read_jsonl(root / BACKFILL_VIEWS[1]),
        "filtered_variant_performance": _read_jsonl(root / BACKFILL_VIEWS[2]),
        "filtered_variant_signal_metrics": _read_jsonl(root / BACKFILL_VIEWS[3]),
        **_optional_input_snapshot(root, "filtered_candidate_backfill_input_snapshot.json"),
        "filtered_backfill_dir": str(root),
    }


def _rebuild_backfill(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_candidate_backfill_input_snapshot.json")
    _require(snapshot.get("schema_version") == BACKFILL_INPUT_SCHEMA, "backfill snapshot schema")
    _require(_payload_experiment_safe(snapshot), "backfill snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    policy = _policy(policy_source)
    design_source = _mapping(snapshot.get("filter_design_source"))
    ledger_source = _mapping(snapshot.get("ledger_source"))
    _validate_binding(
        design_source,
        kind="candidate_quality_filter_design",
        names=signal_filter.FILTER_FILES,
    )
    _validate_binding(
        ledger_source,
        kind="candidate_signal_ledger",
        names=signal_filter.LEDGER_FILES,
    )
    design = _validated_filter(_binding_id(design_source), _binding_root(design_source).parent)
    ledger = _validated_ledger(_binding_id(ledger_source), _binding_root(ledger_source).parent)
    _require(
        _text(design.get("source_ledger_id")) == _binding_id(ledger_source),
        "backfill ledger lineage",
    )
    _require(_policy_source(design) == policy_source, "backfill design policy lineage")
    _require(_policy_source(ledger) == policy_source, "backfill ledger policy lineage")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, design, ledger)
    _, expected = _backfill_material(
        root=root,
        artifact_id=artifact_id,
        design=design,
        ledger=ledger,
        policy=policy,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_backfill(*, filtered_backfill_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / filtered_backfill_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="filtered_candidate_backfill_input_snapshot.json",
        schema=BACKFILL_INPUT_SCHEMA,
        id_key="filtered_backfill_id",
        artifact_id=filtered_backfill_id,
        view_names=BACKFILL_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_filtered_candidate_backfill_validation",
            filtered_backfill_id,
            checks,
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_filtered_candidate_backfill_validation",
        artifact_id=filtered_backfill_id,
        checks=checks,
        rebuild=lambda: _rebuild_backfill(root, filtered_backfill_id),
    )


def _validated_backfill(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_local(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=validate_filtered_candidate_backfill_artifact,
        validator_key="filtered_backfill_id",
        reader=filtered_candidate_backfill_report_payload,
        reader_key="filtered_backfill_id",
        label="filtered backfill",
    )


def _comparison_rows(backfill: Mapping[str, Any]) -> list[dict[str, Any]]:
    performance = {
        _text(row.get("variant_id")): row
        for row in _records(backfill.get("filtered_variant_performance"))
        if row.get("outcome_evidence_status") == "AVAILABLE"
    }
    metrics = {
        _text(row.get("variant_id")): row
        for row in _records(backfill.get("filtered_variant_signal_metrics"))
    }
    rows: list[dict[str, Any]] = []
    for spec in _records(backfill.get("filtered_variant_specs")):
        variant_id = _text(spec.get("variant_id"))
        perf = _mapping(performance.get(variant_id))
        metric = _mapping(metrics.get(variant_id))
        required = (
            perf.get("return_delta_vs_base"),
            perf.get("drawdown_delta_vs_base"),
            metric.get("signal_churn_delta_vs_base"),
        )
        if any(value is None for value in required):
            continue
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "variant_id": variant_id,
                "base_method": spec.get("base_method"),
                "return_delta_vs_base": required[0],
                "drawdown_delta_vs_base": required[1],
                "signal_churn_delta_vs_base": required[2],
                "comparison_status": "EVIDENCE_AVAILABLE_REQUIRES_REVIEW",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _comparison_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "evidence_status": "INSUFFICIENT_DATA",
            "best_filtered_variant": None,
            "best_base_method": None,
            "filtered_win_count": None,
            "tested_variant_count": 0,
            "recommendation": "INSUFFICIENT_DATA",
            "confidence": None,
            "requires_forward_confirmation": True,
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    return {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": "SUFFICIENT_FOR_REVIEW_ONLY",
        "best_filtered_variant": None,
        "best_base_method": None,
        "filtered_win_count": None,
        "tested_variant_count": len(rows),
        "recommendation": "CONTINUE_TESTING",
        "confidence": "LOW",
        "requires_forward_confirmation": True,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _comparison_material(
    *, root: Path, artifact_id: str, backfill: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    rows = _comparison_rows(backfill)
    summary = _comparison_summary(rows)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_vs_original_comparison_manifest",
        "comparison_id": artifact_id,
        "filtered_backfill_id": backfill.get("filtered_backfill_id"),
        "filter_design_id": backfill.get("filter_design_id"),
        "source_ledger_id": backfill.get("source_ledger_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if not rows else "PASS",
        "evidence_status": summary.get("evidence_status"),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "policy_version": backfill.get("policy_version"),
        "filtered_vs_original_manifest_path": str(root / COMPARISON_VIEWS[0]),
        "filtered_comparison_matrix_path": str(root / COMPARISON_VIEWS[1]),
        "filtered_improvement_summary_path": str(root / COMPARISON_VIEWS[2]),
        "filtered_vs_original_comparison_report_path": str(root / COMPARISON_VIEWS[3]),
        "filtered_vs_original_comparison_input_snapshot_path": str(
            root / "filtered_vs_original_comparison_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        COMPARISON_VIEWS[0]: foundation._json_bytes(manifest),
        COMPARISON_VIEWS[1]: foundation._jsonl_bytes(rows),
        COMPARISON_VIEWS[2]: foundation._json_bytes(summary),
        COMPARISON_VIEWS[3]: foundation._text_file_bytes(
            render_filtered_vs_original_comparison_report(manifest, summary, rows)
        ),
    }
    return manifest, views


def render_filtered_vs_original_comparison_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join(
        [
            f"# Filtered vs Original Comparison {manifest.get('comparison_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- comparable_variant_count：{len(rows)}",
            f"- best_filtered_variant：{summary.get('best_filtered_variant')}",
            f"- recommendation：{summary.get('recommendation')}",
            "- 结论边界：缺少同cohort filtered outcome时不计算score、不选择winner。",
            "- safety：research screening only / no official weights / no broker / no production",
            "",
        ]
    )


@with_artifact_validation_session
def _run_comparison(
    *,
    filtered_backfill_id: str,
    filtered_backfill_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    backfill = _validated_backfill(filtered_backfill_id, filtered_backfill_dir)
    _chronology(generated, backfill)
    artifact_id = _stable_id(
        "filtered-vs-original-comparison", filtered_backfill_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    _, views = _comparison_material(
        root=root, artifact_id=root.name, backfill=backfill, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": COMPARISON_INPUT_SCHEMA,
        "comparison_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(backfill.get("input_snapshot")).get("policy_source")),
        "backfill_source": _binding(
            kind="filtered_candidate_backfill",
            artifact_id=filtered_backfill_id,
            root=filtered_backfill_dir / filtered_backfill_id,
            names=BACKFILL_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, COMPARISON_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "filtered_vs_original_comparison_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_filtered_vs_original_comparison", root.name, root / COMPARISON_VIEWS[0]
    )
    return _comparison_report(comparison_id=root.name, latest=False, output_dir=output_dir)


def _comparison_report(
    *, comparison_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=comparison_id,
        latest_pointer="latest_filtered_vs_original_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name=COMPARISON_VIEWS[0],
    )
    return {
        **_read_json(root / COMPARISON_VIEWS[0]),
        "filtered_comparison_matrix": _read_jsonl(root / COMPARISON_VIEWS[1]),
        "filtered_improvement_summary": _read_json(root / COMPARISON_VIEWS[2]),
        **_optional_input_snapshot(root, "filtered_vs_original_comparison_input_snapshot.json"),
        "comparison_dir": str(root),
    }


def _rebuild_comparison(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_vs_original_comparison_input_snapshot.json")
    _require(
        snapshot.get("schema_version") == COMPARISON_INPUT_SCHEMA,
        "comparison snapshot schema",
    )
    _require(_payload_experiment_safe(snapshot), "comparison snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    _policy(policy_source)
    source = _mapping(snapshot.get("backfill_source"))
    _validate_binding(source, kind="filtered_candidate_backfill", names=BACKFILL_FILES)
    backfill = _validated_backfill(_binding_id(source), _binding_root(source).parent)
    _require(
        _mapping(_mapping(backfill.get("input_snapshot")).get("policy_source")) == policy_source,
        "comparison policy lineage",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, backfill)
    _, expected = _comparison_material(
        root=root, artifact_id=artifact_id, backfill=backfill, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_comparison(*, comparison_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / comparison_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="filtered_vs_original_comparison_input_snapshot.json",
        schema=COMPARISON_INPUT_SCHEMA,
        id_key="comparison_id",
        artifact_id=comparison_id,
        view_names=COMPARISON_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_filtered_vs_original_comparison_validation", comparison_id, checks
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_filtered_vs_original_comparison_validation",
        artifact_id=comparison_id,
        checks=checks,
        rebuild=lambda: _rebuild_comparison(root, comparison_id),
    )


def _validated_comparison(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_local(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=validate_filtered_vs_original_comparison_artifact,
        validator_key="comparison_id",
        reader=filtered_vs_original_comparison_report_payload,
        reader_key="comparison_id",
        label="filtered comparison",
    )


def _gate_results(
    design: Mapping[str, Any], ledger: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    filters = _filter_rows(design)
    events = _ledger_events(ledger)
    if not filters or not events:
        return []
    harmful_total = sum(1 for row in events if row.get("event_quality") == "HARMFUL")
    nonharmful_total = len(events) - harmful_total
    rows: list[dict[str, Any]] = []
    for filter_row in filters:
        filter_id = _text(filter_row.get("filter_id"))
        outcomes = [_event_matches_filter(row, filter_id, policy) for row in events]
        evaluable = [value for value in outcomes if value is not None]
        if not evaluable:
            continue
        triggered = sum(1 for value in evaluable if value)
        harmful_triggered = sum(
            1
            for row, value in zip(events, outcomes, strict=True)
            if value is True and row.get("event_quality") == "HARMFUL"
        )
        nonharmful_triggered = triggered - harmful_triggered
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "gate_id": _stable_id("signal-gate", filter_id),
                "filter_id": filter_id,
                "gate_type": filter_id.removesuffix("_filter"),
                "observed_event_count": len(events),
                "evaluable_event_count": len(evaluable),
                "blocked_event_count": triggered,
                "harmful_event_reduction_rate": (
                    round(harmful_triggered / harmful_total, 6) if harmful_total else None
                ),
                "false_block_rate": (
                    round(nonharmful_triggered / nonharmful_total, 6)
                    if nonharmful_total
                    else None
                ),
                "turnover_reduction_rate": None,
                "gate_result_status": "OBSERVED_SCREENING_ONLY",
                "formalization_ready": False,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def _gate_summary(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not results:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "evidence_status": "INSUFFICIENT_DATA",
            "tested_gate_count": 0,
            "promising_gate_count": None,
            "recommended_next_action": "BUILD_VALIDATED_DATED_SIGNAL_LEDGER",
            "formalization_ready": False,
            "confidence": None,
            "official_gate_changed": False,
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    return {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": "SCREENING_EVIDENCE_ONLY",
        "tested_gate_count": len(results),
        "promising_gate_count": None,
        "recommended_next_action": "BUILD_FILTERED_OUTCOME_COHORT",
        "formalization_ready": False,
        "confidence": "LOW",
        "official_gate_changed": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _gate_material(
    *,
    root: Path,
    artifact_id: str,
    design: Mapping[str, Any],
    ledger: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    results = _gate_results(design, ledger, policy)
    summary = _gate_summary(results)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_gate_experiment_manifest",
        "signal_gate_experiment_id": artifact_id,
        "filter_design_id": design.get("filter_design_id"),
        "source_ledger_id": ledger.get("ledger_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if not results else "PASS",
        "evidence_status": summary.get("evidence_status"),
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "policy_version": ledger.get("policy_version"),
        "signal_gate_experiment_manifest_path": str(root / GATE_VIEWS[0]),
        "signal_gate_experiment_results_path": str(root / GATE_VIEWS[1]),
        "signal_gate_experiment_summary_path": str(root / GATE_VIEWS[2]),
        "signal_gate_experiment_report_path": str(root / GATE_VIEWS[3]),
        "reader_brief_section_path": str(root / GATE_VIEWS[4]),
        "signal_gate_experiment_input_snapshot_path": str(
            root / "signal_gate_experiment_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        GATE_VIEWS[0]: foundation._json_bytes(manifest),
        GATE_VIEWS[1]: foundation._jsonl_bytes(results),
        GATE_VIEWS[2]: foundation._json_bytes(summary),
        GATE_VIEWS[3]: foundation._text_file_bytes(
            render_signal_gate_experiment_report(manifest, summary, results)
        ),
        GATE_VIEWS[4]: foundation._text_file_bytes(
            render_signal_gate_experiment_reader_brief(summary)
        ),
    }
    return manifest, views


def render_signal_gate_experiment_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> str:
    return "\n".join(
        [
            f"# Signal Gate Experiment {manifest.get('signal_gate_experiment_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- tested_gate_count：{len(rows)}",
            f"- recommended_next_action：{summary.get('recommended_next_action')}",
            f"- formalization_ready：{summary.get('formalization_ready')}",
            "- 结论边界：rate只使用非空真实分母；缺dated events不物化gate result。",
            "- safety：research screening only / official gate unchanged / "
            "no broker / no production",
            "",
        ]
    )


def render_signal_gate_experiment_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Gate Experiment",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- tested_gate_count: {summary.get('tested_gate_count')}",
            f"- recommended_next_action: {summary.get('recommended_next_action')}",
            "- safety: research screening only / official gate unchanged / no production",
            "",
        ]
    )


@with_artifact_validation_session
def _run_gate(
    *,
    filter_design_id: str,
    filter_design_dir: Path,
    ledger_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    design, ledger, policy_source, policy = _validated_filter_and_ledger(
        filter_design_id=filter_design_id,
        filter_design_dir=filter_design_dir,
        ledger_dir=ledger_dir,
    )
    _chronology(generated, design, ledger)
    artifact_id = _stable_id("signal-gate-experiment", filter_design_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _gate_material(
        root=root,
        artifact_id=root.name,
        design=design,
        ledger=ledger,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": GATE_INPUT_SCHEMA,
        "signal_gate_experiment_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "filter_design_source": _binding(
            kind="candidate_quality_filter_design",
            artifact_id=filter_design_id,
            root=filter_design_dir / filter_design_id,
            names=signal_filter.FILTER_FILES,
        ),
        "ledger_source": _binding(
            kind="candidate_signal_ledger",
            artifact_id=_text(ledger.get("ledger_id")),
            root=ledger_dir / _text(ledger.get("ledger_id")),
            names=signal_filter.LEDGER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, GATE_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "signal_gate_experiment_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_signal_gate_experiment", root.name, root / GATE_VIEWS[0])
    return _gate_report(signal_gate_experiment_id=root.name, latest=False, output_dir=output_dir)


def _gate_report(
    *, signal_gate_experiment_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=signal_gate_experiment_id,
        latest_pointer="latest_signal_gate_experiment",
        latest=latest,
        output_dir=output_dir,
        required_name=GATE_VIEWS[0],
    )
    return {
        **_read_json(root / GATE_VIEWS[0]),
        "signal_gate_experiment_results": _read_jsonl(root / GATE_VIEWS[1]),
        "signal_gate_experiment_summary": _read_json(root / GATE_VIEWS[2]),
        "reader_brief_section": (root / GATE_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_input_snapshot(root, "signal_gate_experiment_input_snapshot.json"),
        "signal_gate_experiment_dir": str(root),
    }


def _rebuild_gate(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_gate_experiment_input_snapshot.json")
    _require(snapshot.get("schema_version") == GATE_INPUT_SCHEMA, "gate snapshot schema")
    _require(_payload_experiment_safe(snapshot), "gate snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    policy = _policy(policy_source)
    design_source = _mapping(snapshot.get("filter_design_source"))
    ledger_source = _mapping(snapshot.get("ledger_source"))
    _validate_binding(
        design_source,
        kind="candidate_quality_filter_design",
        names=signal_filter.FILTER_FILES,
    )
    _validate_binding(
        ledger_source,
        kind="candidate_signal_ledger",
        names=signal_filter.LEDGER_FILES,
    )
    design = _validated_filter(_binding_id(design_source), _binding_root(design_source).parent)
    ledger = _validated_ledger(_binding_id(ledger_source), _binding_root(ledger_source).parent)
    _require(
        _text(design.get("source_ledger_id")) == _binding_id(ledger_source),
        "gate ledger lineage",
    )
    _require(_policy_source(design) == policy_source, "gate design policy lineage")
    _require(_policy_source(ledger) == policy_source, "gate ledger policy lineage")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, design, ledger)
    _, expected = _gate_material(
        root=root,
        artifact_id=artifact_id,
        design=design,
        ledger=ledger,
        policy=policy,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_gate(*, signal_gate_experiment_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / signal_gate_experiment_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="signal_gate_experiment_input_snapshot.json",
        schema=GATE_INPUT_SCHEMA,
        id_key="signal_gate_experiment_id",
        artifact_id=signal_gate_experiment_id,
        view_names=GATE_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_signal_gate_experiment_validation",
            signal_gate_experiment_id,
            checks,
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_signal_gate_experiment_validation",
        artifact_id=signal_gate_experiment_id,
        checks=checks,
        rebuild=lambda: _rebuild_gate(root, signal_gate_experiment_id),
    )


def _validated_gate(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_local(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=validate_signal_gate_experiment_artifact,
        validator_key="signal_gate_experiment_id",
        reader=signal_gate_experiment_report_payload,
        reader_key="signal_gate_experiment_id",
        label="signal gate experiment",
    )


def _review_decision(
    comparison: Mapping[str, Any], gate: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    comparison_summary = _mapping(comparison.get("filtered_improvement_summary"))
    gate_summary = _mapping(gate.get("signal_gate_experiment_summary"))
    sufficient = (
        comparison_summary.get("evidence_status") == "SUFFICIENT_FOR_REVIEW_ONLY"
        and gate_summary.get("evidence_status") == "SCREENING_EVIDENCE_ONLY"
    )
    if not sufficient:
        decision_name = "INSUFFICIENT_DATA"
        confidence = None
        action = "BUILD_VALIDATED_DATED_SIGNAL_AND_FILTERED_OUTCOME_EVIDENCE"
    else:
        decision_name = "CONTINUE_TESTING"
        confidence = "LOW"
        action = "CONTINUE_FORWARD_CONFIRMATION"
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": "SUFFICIENT_FOR_REVIEW_ONLY" if sufficient else "INSUFFICIENT_DATA",
        "decision": decision_name,
        "best_filtered_variant": None,
        "comparison_recommendation": comparison_summary.get("recommendation"),
        "gate_recommendation": gate_summary.get("recommended_next_action"),
        "confidence": confidence,
        "requires_forward_confirmation": sufficient,
        "recommended_next_action": action,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    specs = {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": decision.get("evidence_status"),
        "candidate_variant": None,
        "keep_testing_plan": (
            [action] if decision_name in {"INSUFFICIENT_DATA", "CONTINUE_TESTING"} else []
        ),
        "formal_implementation_plan": [],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return decision, specs


def _review_material(
    *,
    root: Path,
    artifact_id: str,
    comparison: Mapping[str, Any],
    gate: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    decision, specs = _review_decision(comparison, gate)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_promotion_review_manifest",
        "filtered_review_id": artifact_id,
        "comparison_id": comparison.get("comparison_id"),
        "signal_gate_experiment_id": gate.get("signal_gate_experiment_id"),
        "filtered_backfill_id": comparison.get("filtered_backfill_id"),
        "filter_design_id": comparison.get("filter_design_id"),
        "source_ledger_id": comparison.get("source_ledger_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if decision.get("decision") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "evidence_status": decision.get("evidence_status"),
        "market_regime": comparison.get("market_regime", "ai_after_chatgpt"),
        "date_start": comparison.get("date_start"),
        "date_end": comparison.get("date_end"),
        "data_quality_status": comparison.get("data_quality_status"),
        "policy_version": comparison.get("policy_version"),
        "filtered_promotion_manifest_path": str(root / REVIEW_VIEWS[0]),
        "filtered_promotion_decision_path": str(root / REVIEW_VIEWS[1]),
        "filtered_candidate_specs_path": str(root / REVIEW_VIEWS[2]),
        "filtered_candidate_promotion_review_report_path": str(root / REVIEW_VIEWS[3]),
        "reader_brief_section_path": str(root / REVIEW_VIEWS[4]),
        "filtered_candidate_promotion_review_input_snapshot_path": str(
            root / "filtered_candidate_promotion_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        REVIEW_VIEWS[0]: foundation._json_bytes(manifest),
        REVIEW_VIEWS[1]: foundation._json_bytes(decision),
        REVIEW_VIEWS[2]: foundation._json_bytes(specs),
        REVIEW_VIEWS[3]: foundation._text_file_bytes(
            render_filtered_promotion_review_report(manifest, decision, specs)
        ),
        REVIEW_VIEWS[4]: foundation._text_file_bytes(
            render_filtered_promotion_review_reader_brief(decision)
        ),
    }
    return manifest, views


def render_filtered_promotion_review_report(
    manifest: Mapping[str, Any], decision: Mapping[str, Any], specs: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Filtered Candidate Promotion Review {manifest.get('filtered_review_id')}",
            "",
            f"- evidence_status：{decision.get('evidence_status')}",
            f"- decision：{decision.get('decision')}",
            f"- candidate_variant：{specs.get('candidate_variant')}",
            f"- recommended_next_action：{decision.get('recommended_next_action')}",
            "- safety：owner review required / no official weights / no broker / no production",
            "",
        ]
    )


def render_filtered_promotion_review_reader_brief(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Filtered Candidate Promotion Review",
            "",
            f"- evidence_status: {decision.get('evidence_status')}",
            f"- decision: {decision.get('decision')}",
            f"- recommended_next_action: {decision.get('recommended_next_action')}",
            "- safety: owner review required / no official weights / no production",
            "",
        ]
    )


@with_artifact_validation_session
def _run_review(
    *,
    comparison_id: str,
    signal_gate_experiment_id: str,
    comparison_dir: Path,
    experiment_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    comparison = _validated_comparison(comparison_id, comparison_dir)
    gate = _validated_gate(signal_gate_experiment_id, experiment_dir)
    for field in ("filter_design_id", "source_ledger_id", "policy_version"):
        _require(comparison.get(field) == gate.get(field), f"review {field} lineage mismatch")
    _chronology(generated, comparison, gate)
    artifact_id = _stable_id(
        "filtered-candidate-promotion-review",
        comparison_id,
        signal_gate_experiment_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    _, views = _review_material(
        root=root,
        artifact_id=root.name,
        comparison=comparison,
        gate=gate,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    comparison_policy = _mapping(_mapping(comparison.get("input_snapshot")).get("policy_source"))
    gate_policy = _mapping(_mapping(gate.get("input_snapshot")).get("policy_source"))
    _require(comparison_policy == gate_policy, "review policy binding mismatch")
    snapshot = {
        "schema_version": REVIEW_INPUT_SCHEMA,
        "filtered_review_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": comparison_policy,
        "comparison_source": _binding(
            kind="filtered_vs_original_comparison",
            artifact_id=comparison_id,
            root=comparison_dir / comparison_id,
            names=COMPARISON_FILES,
        ),
        "gate_source": _binding(
            kind="signal_gate_experiment",
            artifact_id=signal_gate_experiment_id,
            root=experiment_dir / signal_gate_experiment_id,
            names=GATE_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, REVIEW_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "filtered_candidate_promotion_review_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_filtered_candidate_promotion_review", root.name, root / REVIEW_VIEWS[0]
    )
    return _review_report(filtered_review_id=root.name, latest=False, output_dir=output_dir)


def _review_report(
    *, filtered_review_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filtered_review_id,
        latest_pointer="latest_filtered_candidate_promotion_review",
        latest=latest,
        output_dir=output_dir,
        required_name=REVIEW_VIEWS[0],
    )
    return {
        **_read_json(root / REVIEW_VIEWS[0]),
        "filtered_promotion_decision": _read_json(root / REVIEW_VIEWS[1]),
        "filtered_candidate_specs": _read_json(root / REVIEW_VIEWS[2]),
        "reader_brief_section": (root / REVIEW_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_input_snapshot(
            root, "filtered_candidate_promotion_review_input_snapshot.json"
        ),
        "filtered_review_dir": str(root),
    }


def _rebuild_review(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "filtered_candidate_promotion_review_input_snapshot.json")
    _require(snapshot.get("schema_version") == REVIEW_INPUT_SCHEMA, "review snapshot schema")
    _require(_payload_experiment_safe(snapshot), "review snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    _policy(policy_source)
    comparison_source = _mapping(snapshot.get("comparison_source"))
    gate_source = _mapping(snapshot.get("gate_source"))
    _validate_binding(
        comparison_source,
        kind="filtered_vs_original_comparison",
        names=COMPARISON_FILES,
    )
    _validate_binding(gate_source, kind="signal_gate_experiment", names=GATE_FILES)
    comparison = _validated_comparison(
        _binding_id(comparison_source), _binding_root(comparison_source).parent
    )
    gate = _validated_gate(_binding_id(gate_source), _binding_root(gate_source).parent)
    for field in ("filter_design_id", "source_ledger_id", "policy_version"):
        _require(comparison.get(field) == gate.get(field), f"review {field} lineage mismatch")
    _require(
        _mapping(_mapping(comparison.get("input_snapshot")).get("policy_source"))
        == policy_source,
        "review comparison policy lineage",
    )
    _require(
        _mapping(_mapping(gate.get("input_snapshot")).get("policy_source")) == policy_source,
        "review gate policy lineage",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, comparison, gate)
    _, expected = _review_material(
        root=root,
        artifact_id=artifact_id,
        comparison=comparison,
        gate=gate,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_review(*, filtered_review_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / filtered_review_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="filtered_candidate_promotion_review_input_snapshot.json",
        schema=REVIEW_INPUT_SCHEMA,
        id_key="filtered_review_id",
        artifact_id=filtered_review_id,
        view_names=REVIEW_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_filtered_candidate_promotion_review_validation",
            filtered_review_id,
            checks,
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_filtered_candidate_promotion_review_validation",
        artifact_id=filtered_review_id,
        checks=checks,
        rebuild=lambda: _rebuild_review(root, filtered_review_id),
    )


def _validated_review(artifact_id: str, output_dir: Path) -> dict[str, Any]:
    return _validated_local(
        artifact_id=artifact_id,
        output_dir=output_dir,
        validator=validate_filtered_candidate_promotion_review_artifact,
        validator_key="filtered_review_id",
        reader=filtered_candidate_promotion_review_report_payload,
        reader_key="filtered_review_id",
        label="promotion review",
    )


def _roadmap_summary(review: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(review.get("filtered_promotion_decision"))
    insufficient = decision.get("evidence_status") == "INSUFFICIENT_DATA"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": "INSUFFICIENT_DATA" if insufficient else "SUFFICIENT_FOR_REVIEW_ONLY",
        "current_phase": "signal_quality_filter_research",
        "filtered_candidate_status": decision.get("decision"),
        "recommended_owner_action": (
            "BUILD_VALIDATED_DATED_SIGNAL_AND_FILTERED_OUTCOME_EVIDENCE"
            if insufficient
            else "CONTINUE_FORWARD_CONFIRMATION"
        ),
        "next_task_family": "dated_signal_evidence" if insufficient else "signal_feature_diagnosis",
        "requires_forward_confirmation": not insufficient,
        "candidate_available": False,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _owner_checklist(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Owner Signal Research Checklist",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- candidate_available: {summary.get('candidate_available')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            "- official_target_change: forbidden",
            "- broker_action: forbidden",
            "- production_effect: none",
            "",
        ]
    )


def _roadmap_material(
    *, root: Path, artifact_id: str, review: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    summary = _roadmap_summary(review)
    checklist = _owner_checklist(summary)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_signal_roadmap_manifest",
        "owner_signal_roadmap_id": artifact_id,
        "filtered_review_id": review.get("filtered_review_id"),
        "comparison_id": review.get("comparison_id"),
        "signal_gate_experiment_id": review.get("signal_gate_experiment_id"),
        "filter_design_id": review.get("filter_design_id"),
        "source_ledger_id": review.get("source_ledger_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if summary.get("evidence_status") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "evidence_status": summary.get("evidence_status"),
        "market_regime": review.get("market_regime", "ai_after_chatgpt"),
        "date_start": review.get("date_start"),
        "date_end": review.get("date_end"),
        "data_quality_status": review.get("data_quality_status"),
        "policy_version": review.get("policy_version"),
        "owner_signal_roadmap_manifest_path": str(root / ROADMAP_VIEWS[0]),
        "owner_signal_roadmap_summary_path": str(root / ROADMAP_VIEWS[1]),
        "owner_signal_checklist_path": str(root / ROADMAP_VIEWS[2]),
        "owner_signal_roadmap_report_path": str(root / ROADMAP_VIEWS[3]),
        "reader_brief_section_path": str(root / ROADMAP_VIEWS[4]),
        "owner_signal_roadmap_input_snapshot_path": str(
            root / "owner_signal_roadmap_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        ROADMAP_VIEWS[0]: foundation._json_bytes(manifest),
        ROADMAP_VIEWS[1]: foundation._json_bytes(summary),
        ROADMAP_VIEWS[2]: foundation._text_file_bytes(checklist),
        ROADMAP_VIEWS[3]: foundation._text_file_bytes(
            render_owner_signal_roadmap_report(manifest, summary, checklist)
        ),
        ROADMAP_VIEWS[4]: foundation._text_file_bytes(
            render_owner_signal_roadmap_reader_brief(summary)
        ),
    }
    return manifest, views


def render_owner_signal_roadmap_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], checklist: str
) -> str:
    _ = checklist
    return "\n".join(
        [
            f"# Owner Signal Roadmap {manifest.get('owner_signal_roadmap_id')}",
            "",
            f"- evidence_status：{summary.get('evidence_status')}",
            f"- filtered_candidate_status：{summary.get('filtered_candidate_status')}",
            f"- candidate_available：{summary.get('candidate_available')}",
            f"- recommended_owner_action：{summary.get('recommended_owner_action')}",
            "- safety：no official weights / no broker / no production",
            "",
        ]
    )


def render_owner_signal_roadmap_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Owner Signal Roadmap",
            "",
            f"- evidence_status: {summary.get('evidence_status')}",
            f"- candidate_available: {summary.get('candidate_available')}",
            f"- recommended_owner_action: {summary.get('recommended_owner_action')}",
            "- safety: owner decision only / no official weights / no production",
            "",
        ]
    )


@with_artifact_validation_session
def _run_roadmap(
    *,
    filtered_review_id: str,
    review_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    review = _validated_review(filtered_review_id, review_dir)
    _chronology(generated, review)
    artifact_id = _stable_id("owner-signal-roadmap", filtered_review_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    _, views = _roadmap_material(
        root=root, artifact_id=root.name, review=review, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    policy_source = _mapping(_mapping(review.get("input_snapshot")).get("policy_source"))
    snapshot = {
        "schema_version": ROADMAP_INPUT_SCHEMA,
        "owner_signal_roadmap_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "review_source": _binding(
            kind="filtered_candidate_promotion_review",
            artifact_id=filtered_review_id,
            root=review_dir / filtered_review_id,
            names=REVIEW_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, ROADMAP_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "owner_signal_roadmap_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_owner_signal_roadmap", root.name, root / ROADMAP_VIEWS[0])
    return _roadmap_report(owner_signal_roadmap_id=root.name, latest=False, output_dir=output_dir)


def _roadmap_report(
    *, owner_signal_roadmap_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_signal_roadmap_id,
        latest_pointer="latest_owner_signal_roadmap",
        latest=latest,
        output_dir=output_dir,
        required_name=ROADMAP_VIEWS[0],
    )
    return {
        **_read_json(root / ROADMAP_VIEWS[0]),
        "owner_signal_roadmap_summary": _read_json(root / ROADMAP_VIEWS[1]),
        "owner_signal_checklist": (root / ROADMAP_VIEWS[2]).read_text(encoding="utf-8"),
        "reader_brief_section": (root / ROADMAP_VIEWS[4]).read_text(encoding="utf-8"),
        **_optional_input_snapshot(root, "owner_signal_roadmap_input_snapshot.json"),
        "owner_signal_roadmap_dir": str(root),
    }


def _rebuild_roadmap(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "owner_signal_roadmap_input_snapshot.json")
    _require(snapshot.get("schema_version") == ROADMAP_INPUT_SCHEMA, "roadmap snapshot schema")
    _require(_payload_experiment_safe(snapshot), "roadmap snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    _policy(policy_source)
    review_source = _mapping(snapshot.get("review_source"))
    _validate_binding(
        review_source,
        kind="filtered_candidate_promotion_review",
        names=REVIEW_FILES,
    )
    review = _validated_review(_binding_id(review_source), _binding_root(review_source).parent)
    _require(
        _mapping(_mapping(review.get("input_snapshot")).get("policy_source")) == policy_source,
        "roadmap policy lineage",
    )
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, review)
    _, expected = _roadmap_material(
        root=root, artifact_id=artifact_id, review=review, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_roadmap(*, owner_signal_roadmap_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / owner_signal_roadmap_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="owner_signal_roadmap_input_snapshot.json",
        schema=ROADMAP_INPUT_SCHEMA,
        id_key="owner_signal_roadmap_id",
        artifact_id=owner_signal_roadmap_id,
        view_names=ROADMAP_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_owner_signal_roadmap_validation",
            owner_signal_roadmap_id,
            checks,
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_owner_signal_roadmap_validation",
        artifact_id=owner_signal_roadmap_id,
        checks=checks,
        rebuild=lambda: _rebuild_roadmap(root, owner_signal_roadmap_id),
    )
