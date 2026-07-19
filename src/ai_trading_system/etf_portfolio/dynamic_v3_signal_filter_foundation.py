from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import (
    dynamic_v3_micro_search_foundation as micro_search_foundation,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_diagnosis_foundation as diagnosis_foundation,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR = (
    diagnosis_foundation.DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR
)
DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR = diagnosis_foundation.DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR
DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR = micro_search_foundation.DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR
DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR = micro_search_foundation.DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR
DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "signal_feature_failure_taxonomy_v1.yaml"
)
DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_failure_taxonomy"
)
DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_signal_ledger"
)
DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_churn_root_cause"
)
DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "regime_mismatch_attribution"
)
DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_quality_filter_design"
)

TAXONOMY_INPUT_SCHEMA = "signal_failure_taxonomy_input_snapshot.v2"
LEDGER_INPUT_SCHEMA = "candidate_signal_ledger_input_snapshot.v2"
CHURN_INPUT_SCHEMA = "signal_churn_root_cause_input_snapshot.v2"
MISMATCH_INPUT_SCHEMA = "regime_mismatch_attribution_input_snapshot.v2"
FILTER_INPUT_SCHEMA = "candidate_quality_filter_design_input_snapshot.v2"

TAXONOMY_VIEWS = (
    "signal_failure_taxonomy_manifest.json",
    "normalized_signal_failure_taxonomy.yaml",
    "signal_failure_mode_catalog.json",
    "signal_failure_taxonomy_report.md",
)
LEDGER_VIEWS = (
    "candidate_signal_ledger_manifest.json",
    "signal_events.jsonl",
    "candidate_signal_summary.json",
    "candidate_signal_ledger_report.md",
    "reader_brief_section.md",
)
CHURN_VIEWS = (
    "signal_churn_root_cause_manifest.json",
    "churn_root_cause_summary.json",
    "churn_event_clusters.jsonl",
    "churn_mitigation_candidates.json",
    "signal_churn_root_cause_report.md",
)
MISMATCH_VIEWS = (
    "regime_mismatch_manifest.json",
    "regime_mismatch_events.jsonl",
    "regime_mismatch_summary.json",
    "regime_mismatch_report.md",
)
FILTER_VIEWS = (
    "candidate_quality_filter_manifest.json",
    "proposed_quality_filters.json",
    "filter_design_config.yaml",
    "candidate_quality_filter_design_report.md",
    "reader_brief_section.md",
)
TAXONOMY_FILES = (*TAXONOMY_VIEWS, "signal_failure_taxonomy_input_snapshot.json")
LEDGER_FILES = (*LEDGER_VIEWS, "candidate_signal_ledger_input_snapshot.json")
CHURN_FILES = (*CHURN_VIEWS, "signal_churn_root_cause_input_snapshot.json")
MISMATCH_FILES = (*MISMATCH_VIEWS, "regime_mismatch_attribution_input_snapshot.json")
FILTER_FILES = (*FILTER_VIEWS, "candidate_quality_filter_design_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_texts = foundation._texts
_text = foundation._text
_float = foundation._float
_coerce_date = foundation._coerce_date
_stable_id = foundation._stable_id
_unique_dir = foundation._unique_dir
_write_json = foundation._write_json
_write_jsonl = foundation._write_jsonl
_write_text = foundation._write_text
_read_json = foundation._read_json
_read_jsonl = foundation._read_jsonl
_read_optional_json = foundation._read_optional_json
_write_latest_pointer = foundation._write_latest_pointer
_artifact_dir = foundation._artifact_dir
_required_file_checks = foundation._required_file_checks
_validation_payload = foundation._validation_payload
_payload_safe = foundation._payload_safe
_payload_experiment_safe = foundation._payload_experiment_safe


class DynamicV3SignalFilterFoundationError(ValueError):
    """Raised when TRADING-326～335 evidence is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SignalFilterFoundationError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _aware_time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value))
    except ValueError as exc:
        raise DynamicV3SignalFilterFoundationError(f"{field} must be an ISO datetime") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _chronology(generated: datetime, *payloads: Mapping[str, Any]) -> None:
    for payload in payloads:
        _require(
            _aware_time(payload.get("generated_at"), "source.generated_at") <= generated,
            "source artifact is newer than generated_at",
        )


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_signal_filter_foundation_policy.v1",
        "signal filter policy schema mismatch",
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
    rules = _mapping(payload.get("signal_quality_policy"))
    _require(_float(rules.get("candidate_dispersion_threshold")) > 0, "dispersion policy")
    _require(int(_float(rules.get("persistence_days"))) > 0, "persistence policy")
    _require(int(_float(rules.get("high_flip_count"))) > 0, "flip policy")
    harmful_share = _float(rules.get("harmful_event_share"), -1)
    _require(0 <= harmful_share <= 1, "harmful share policy")
    _require(bool(_texts(rules.get("candidate_methods"))), "candidate method policy")
    _require(
        rules.get("missing_dated_signal_status") == "INSUFFICIENT_DATA",
        "missing evidence must remain insufficient",
    )
    _require(
        rules.get("automatic_filter_implementation_allowed") is False,
        "automatic filter implementation must be disabled",
    )
    _assert_signal_failure_taxonomy_safety(_mapping(payload.get("safety")))
    _require(len(_mapping(payload.get("failure_modes"))) >= 10, "failure taxonomy incomplete")
    return payload


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _policy_rules(policy: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(policy.get("signal_quality_policy"))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _binding_root(binding: Mapping[str, Any]) -> Path:
    return Path(_text(binding.get("source_dir")))


def _binding_id(binding: Mapping[str, Any]) -> str:
    return _text(binding.get("artifact_id"))


def _validate_binding(binding: Mapping[str, Any], *, kind: str, names: Sequence[str]) -> None:
    _require(binding.get("kind") == kind, f"artifact binding kind mismatch: {kind}")
    source_dir = _binding_root(binding).resolve()
    _require(source_dir.name == _binding_id(binding), f"{kind} binding id/path mismatch")
    files = _mapping(binding.get("files"))
    _require(set(files) == set(names), f"{kind} binding file set mismatch")
    for name in names:
        bound_path = Path(_text(_mapping(files.get(name)).get("path"))).resolve()
        _require(
            bound_path == (source_dir / name).resolve(), f"{kind} binding path mismatch: {name}"
        )
    foundation._validate_artifact_binding(binding, kind=kind)


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def _yaml_bytes(payload: Mapping[str, Any]) -> bytes:
    return foundation._text_file_bytes(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=True)
    )


def run_signal_failure_taxonomy_validation(
    *,
    config_path: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_CONFIG_PATH,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return _run_taxonomy(config_path=config_path, output_dir=output_dir, generated_at=generated_at)


def signal_failure_taxonomy_report_payload(
    *,
    taxonomy_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> dict[str, Any]:
    return _taxonomy_report(taxonomy_id=taxonomy_id, latest=latest, output_dir=output_dir)


def validate_signal_failure_taxonomy_artifact(
    *,
    taxonomy_id: str,
    output_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
) -> dict[str, Any]:
    return _validate_taxonomy(taxonomy_id=taxonomy_id, output_dir=output_dir)


def build_candidate_signal_ledger(
    *,
    taxonomy_id: str,
    source_backfill_id: str,
    taxonomy_dir: Path = DEFAULT_SIGNAL_FAILURE_TAXONOMY_DIR,
    source_backfill_dir: Path = DEFAULT_MICRO_SEARCH_V4_BACKFILL_DIR,
    v4_design_dir: Path = DEFAULT_MICRO_SEARCH_V4_DESIGN_DIR,
    signal_dir: Path = DEFAULT_SIGNAL_INSTABILITY_DIAGNOSIS_DIR,
    consensus_dir: Path = DEFAULT_CONSENSUS_QUALITY_REVIEW_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return _run_ledger(
        taxonomy_id=taxonomy_id,
        source_backfill_id=source_backfill_id,
        taxonomy_dir=taxonomy_dir,
        source_backfill_dir=source_backfill_dir,
        v4_design_dir=v4_design_dir,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )


def candidate_signal_ledger_report_payload(
    *,
    ledger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> dict[str, Any]:
    return _ledger_report(ledger_id=ledger_id, latest=latest, output_dir=output_dir)


def validate_candidate_signal_ledger_artifact(
    *,
    ledger_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
) -> dict[str, Any]:
    return _validate_ledger(ledger_id=ledger_id, output_dir=output_dir)


def run_signal_churn_root_cause_review(
    *,
    ledger_id: str,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return _run_churn(
        ledger_id=ledger_id, ledger_dir=ledger_dir, output_dir=output_dir, generated_at=generated_at
    )


def signal_churn_root_cause_report_payload(
    *,
    root_cause_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> dict[str, Any]:
    return _churn_report(root_cause_id=root_cause_id, latest=latest, output_dir=output_dir)


def validate_signal_churn_root_cause_artifact(
    *,
    root_cause_id: str,
    output_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
) -> dict[str, Any]:
    return _validate_churn(root_cause_id=root_cause_id, output_dir=output_dir)


def run_regime_mismatch_attribution(
    *,
    ledger_id: str,
    ledger_dir: Path = DEFAULT_CANDIDATE_SIGNAL_LEDGER_DIR,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return _run_mismatch(
        ledger_id=ledger_id, ledger_dir=ledger_dir, output_dir=output_dir, generated_at=generated_at
    )


def regime_mismatch_attribution_report_payload(
    *,
    mismatch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    return _mismatch_report(mismatch_id=mismatch_id, latest=latest, output_dir=output_dir)


def validate_regime_mismatch_attribution_artifact(
    *,
    mismatch_id: str,
    output_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    return _validate_mismatch(mismatch_id=mismatch_id, output_dir=output_dir)


def run_candidate_quality_filter_design(
    *,
    root_cause_id: str,
    mismatch_id: str,
    root_cause_dir: Path = DEFAULT_SIGNAL_CHURN_ROOT_CAUSE_DIR,
    mismatch_dir: Path = DEFAULT_REGIME_MISMATCH_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    return _run_filter(
        root_cause_id=root_cause_id,
        mismatch_id=mismatch_id,
        root_cause_dir=root_cause_dir,
        mismatch_dir=mismatch_dir,
        output_dir=output_dir,
        generated_at=generated_at,
    )


def candidate_quality_filter_design_report_payload(
    *,
    filter_design_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> dict[str, Any]:
    return _filter_report(filter_design_id=filter_design_id, latest=latest, output_dir=output_dir)


def validate_candidate_quality_filter_design_artifact(
    *,
    filter_design_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_QUALITY_FILTER_DESIGN_DIR,
) -> dict[str, Any]:
    return _validate_filter(filter_design_id=filter_design_id, output_dir=output_dir)


def render_signal_failure_taxonomy_report(
    manifest: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> str:
    mode_lines = [
        f"- {row.get('mode')}: severity={row.get('severity_default')} "
        f"families={','.join(_texts(row.get('families')))}"
        for row in _records(catalog.get("failure_modes"))
    ]
    return "\n".join(
        [
            f"# Signal Feature Failure Taxonomy {manifest.get('taxonomy_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- failure_mode_count：{manifest.get('failure_mode_count')}",
            "- safety：research_only / screening_only / no broker / no production",
            "",
            "## Failure Modes",
            *mode_lines,
            "",
        ]
    )


def render_candidate_signal_ledger_reader_brief(summary: Mapping[str, Any]) -> str:
    dominant = summary.get("dominant_failure_mode", "unknown")
    unstable = summary.get("unstable_method_count", 0)
    return "\n".join(
        [
            "## Candidate Signal Ledger",
            "",
            f"- dominant_failure_mode: {dominant}",
            f"- unstable_method_count: {unstable}",
            f"- method_count: {len(_records(summary.get('methods')))}",
            "- safety: research screening only / no broker / no production",
            "",
        ]
    )


def render_candidate_signal_ledger_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        f"- {row.get('method')}: events={row.get('event_count')} "
        f"flips={row.get('direction_change_count')} status={row.get('signal_quality_status')} "
        f"dominant={row.get('dominant_failure_mode')}"
        for row in _records(summary.get("methods"))
    ]
    return "\n".join(
        [
            f"# Candidate Signal Ledger {manifest.get('ledger_id')}",
            "",
            f"- source_backfill_id：{manifest.get('source_backfill_id')}",
            f"- market_regime：{manifest.get('market_regime')}",
            f"- date_range：{manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            f"- event_count：{manifest.get('event_count')}",
            (
                "- 结论边界：该 ledger 仅用于 signal feature diagnosis，"
                "不产生 official target weights。"
            ),
            "",
            "## Method Summary",
            *lines,
            "",
        ]
    )


def render_signal_churn_root_cause_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    clusters: Sequence[Mapping[str, Any]],
    mitigations: Mapping[str, Any],
) -> str:
    cluster_lines = [
        f"- {row.get('cluster_id')}: cause={row.get('root_cause')} "
        f"events={row.get('event_count')} methods={','.join(_texts(row.get('methods')))}"
        for row in clusters
    ]
    mitigation_lines = [
        f"- {row.get('mitigation_id')}: {row.get('description')} "
        f"status={row.get('screening_status')}"
        for row in _records(mitigations.get("mitigations"))
    ]
    return "\n".join(
        [
            f"# Signal Churn Root Cause {manifest.get('root_cause_id')}",
            "",
            f"- dominant_root_cause：{summary.get('dominant_root_cause')}",
            f"- confidence：{summary.get('confidence')}",
            f"- affected_methods：{', '.join(_texts(summary.get('affected_methods')))}",
            "- safety：diagnostic only / no broker / no production",
            "",
            "## Event Clusters",
            *cluster_lines,
            "",
            "## Mitigation Candidates",
            *mitigation_lines,
            "",
        ]
    )


def render_regime_mismatch_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        f"- {key}: {value}"
        for key, value in sorted(_mapping(summary.get("by_mismatch_type")).items())
    ]
    return "\n".join(
        [
            f"# Regime Mismatch Attribution {manifest.get('mismatch_id')}",
            "",
            f"- mismatch_count：{summary.get('mismatch_count')}",
            f"- dominant_mismatch_type：{summary.get('dominant_mismatch_type')}",
            f"- affected_method_count：{summary.get('affected_method_count')}",
            "- safety：diagnostic only / no broker / no production",
            "",
            "## Mismatch Types",
            *lines,
            "",
        ]
    )


def render_candidate_quality_filter_reader_brief(filters: Mapping[str, Any]) -> str:
    names = [_text(row.get("filter_id")) for row in _records(filters.get("filters"))]
    return "\n".join(
        [
            "## Candidate Quality Filter Design",
            "",
            f"- filter_count: {len(names)}",
            f"- proposed_filters: {', '.join(names)}",
            "- safety: research screening only / no official weights / no broker",
            "",
        ]
    )


def render_candidate_quality_filter_design_report(
    manifest: Mapping[str, Any],
    filters: Mapping[str, Any],
) -> str:
    lines = [
        f"- {row.get('filter_id')}: trigger={row.get('trigger')} "
        f"action={row.get('action')} effect={row.get('intended_effect')}"
        for row in _records(filters.get("filters"))
    ]
    return "\n".join(
        [
            f"# Candidate Quality Filter Design {manifest.get('filter_design_id')}",
            "",
            f"- root_cause_id：{manifest.get('root_cause_id')}",
            f"- mismatch_id：{manifest.get('mismatch_id')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 设计状态：pilot research baseline；不可作为正式交易或 target weight 规则。",
            "",
            "## Proposed Filters",
            *lines,
            "",
        ]
    )


def _assert_signal_failure_taxonomy_safety(safety: Mapping[str, Any]) -> None:
    if not _signal_failure_taxonomy_safety_locked(safety):
        raise ValueError("signal failure taxonomy safety boundary is not locked")


def _signal_failure_taxonomy_safety_locked(safety: Mapping[str, Any]) -> bool:
    return (
        (safety.get("research_only") is True or safety.get("research_screening_only") is True)
        and safety.get("research_screening_only") is True
        and safety.get("experiment_only") is True
        and safety.get("not_official_target_weights") is True
        and safety.get("not_formal_research_method") is True
        and safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("auto_apply") is False
        and safety.get("production_effect") == st.PRODUCTION_EFFECT
    )


def _normalized_signal_failure_taxonomy(config: Mapping[str, Any]) -> dict[str, Any]:
    modes = _mapping(config.get("failure_modes"))
    families = _mapping(config.get("families"))
    family_by_mode: dict[str, list[str]] = {}
    normalized_families = []
    for family_name, payload in sorted(families.items()):
        mode_ids = _texts(_mapping(payload).get("modes"))
        normalized_families.append({"family": family_name, "modes": mode_ids})
        for mode in mode_ids:
            family_by_mode.setdefault(mode, []).append(family_name)
    normalized_modes = []
    for mode, payload in sorted(modes.items()):
        row = _mapping(payload)
        normalized_modes.append(
            {
                "mode": mode,
                "description": row.get("description", ""),
                "severity_default": row.get("severity_default", "REVIEW_REQUIRED"),
                "families": family_by_mode.get(mode, []),
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "taxonomy_id": config.get("taxonomy_id", "signal_feature_failure_taxonomy_v1"),
        "policy_status": config.get("policy_status", "pilot_research_baseline"),
        "owner": config.get("owner", "system"),
        "review_condition": config.get("review_condition", ""),
        "failure_modes": normalized_modes,
        "families": normalized_families,
        "safety": _mapping(config.get("safety")),
    }


def _signal_failure_mode_catalog(normalized: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "taxonomy_id": normalized.get("taxonomy_id"),
        "failure_modes": [
            {
                "mode": row.get("mode"),
                "description": row.get("description"),
                "severity_default": row.get("severity_default"),
                "families": _texts(row.get("families")),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for row in _records(normalized.get("failure_modes"))
        ],
        "families": _records(normalized.get("families")),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _churn_root_cause_summary(ledger: Mapping[str, Any]) -> dict[str, Any]:
    events = _records(ledger.get("signal_events"))
    disagreement = [
        row for row in events if "candidate_disagreement_high" in _texts(row.get("failure_modes"))
    ]
    sideways = [row for row in events if row.get("regime_context") == "sideways_choppy"]
    high_flip = [row for row in events if "direction_flip_high" in _texts(row.get("failure_modes"))]
    harmful = [row for row in events if row.get("event_quality") == "HARMFUL"]
    if len(disagreement) >= max(1, len(events) // 3):
        cause = "candidate_disagreement_high"
        confidence = "HIGH"
    elif len(sideways) >= max(1, len(events) // 3):
        cause = "sideways_noise"
        confidence = "MEDIUM"
    elif high_flip:
        cause = "top_candidate_rotation"
        confidence = "MEDIUM"
    else:
        cause = "mixed_signal_quality"
        confidence = "LOW"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "dominant_root_cause": cause,
        "confidence": confidence,
        "event_count": len(events),
        "harmful_event_count": len(harmful),
        "affected_methods": sorted({_text(row.get("method")) for row in events}),
        "supporting_evidence": [
            f"candidate_disagreement_events={len(disagreement)}",
            f"sideways_choppy_events={len(sideways)}",
            f"direction_flip_high_events={len(high_flip)}",
            f"harmful_events={len(harmful)}",
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _churn_event_clusters(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    events = _records(ledger.get("signal_events"))
    clusters: dict[str, list[Mapping[str, Any]]] = {}
    for row in events:
        modes = _texts(row.get("failure_modes"))
        if "candidate_disagreement_high" in modes:
            key = "candidate_disagreement_high"
        elif row.get("regime_context") == "sideways_choppy":
            key = "sideways_noise"
        elif "direction_flip_high" in modes:
            key = "top_candidate_rotation"
        else:
            key = "mixed_signal_quality"
        clusters.setdefault(key, []).append(row)
    return [
        {
            "schema_version": st.SCHEMA_VERSION,
            "cluster_id": _stable_id("churn-cluster", key, len(rows)),
            "root_cause": key,
            "event_count": len(rows),
            "methods": sorted({_text(row.get("method")) for row in rows}),
            "regime_contexts": sorted({_text(row.get("regime_context")) for row in rows}),
            "representative_event_ids": [_text(row.get("event_id")) for row in rows[:5]],
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for key, rows in sorted(clusters.items())
    ]


def _churn_mitigation_candidates(summary: Mapping[str, Any]) -> dict[str, Any]:
    cause = _text(summary.get("dominant_root_cause"))
    if cause == "candidate_disagreement_high":
        mitigations = [
            (
                "high_dispersion_hold_filter",
                "hold active tilt when candidate dispersion is above pilot threshold",
            ),
            ("low_confidence_reduce_tilt_filter", "reduce active tilt under weak consensus"),
        ]
    elif cause == "sideways_noise":
        mitigations = [
            (
                "signal_persistence_3d_filter",
                "require three-day signal persistence before acting on direction changes",
            ),
            ("top_candidate_stability_filter", "delay changes when top candidate rotates quickly"),
        ]
    else:
        mitigations = [
            ("regime_mismatch_filter", "block risk-increasing actions in drawdown regimes"),
            ("signal_persistence_3d_filter", "require short persistence before changing tilt"),
        ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "mitigations": [
            {
                "mitigation_id": mitigation_id,
                "description": description,
                "screening_status": "PROPOSED_RESEARCH_FILTER",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for mitigation_id, description in mitigations
        ],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _regime_mismatch_attribution_events(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    results = []
    for row in _records(ledger.get("signal_events")):
        modes = _texts(row.get("failure_modes"))
        regime = _text(row.get("regime_context"))
        direction = _text(row.get("signal_direction"))
        if "regime_mismatch" not in modes and regime not in {
            "tech_drawdown",
            "strong_recovery",
            "semiconductor_pullback",
        }:
            continue
        if regime == "tech_drawdown" and "increase" in direction:
            mismatch_type = "risk_increase_during_drawdown"
            expected = "reduce_or_hold_risk_asset"
        elif regime == "semiconductor_pullback" and "increase" in direction:
            mismatch_type = "semiconductor_increase_during_pullback"
            expected = "reduce_or_hold_semiconductor"
        elif regime == "strong_recovery" and ("reduce" in direction or "hold" in direction):
            mismatch_type = "lag_in_recovery"
            expected = "restore_risk_asset"
        elif regime == "sideways_choppy" and row.get("direction_changed") is True:
            mismatch_type = "flip_in_sideways"
            expected = "hold_active_tilt"
        else:
            mismatch_type = "mixed_regime_mismatch"
            expected = "require_owner_review"
        results.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "event_id": row.get("event_id"),
                "date": row.get("date"),
                "method": row.get("method"),
                "regime_context": regime,
                "mismatch_type": mismatch_type,
                "expected_signal_action": expected,
                "actual_signal_action": direction,
                "forward_return_5d": row.get("subsequent_5d_return"),
                "forward_return_20d": row.get("subsequent_20d_return"),
                "attribution_confidence": (
                    "HIGH" if row.get("event_quality") == "HARMFUL" else "MEDIUM"
                ),
                "failure_modes": modes,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return results


def _regime_mismatch_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for row in events:
        key = _text(row.get("mismatch_type"), "unknown")
        by_type[key] = by_type.get(key, 0) + 1
    return {
        "schema_version": st.SCHEMA_VERSION,
        "mismatch_count": len(events),
        "dominant_mismatch_type": max(by_type, key=by_type.get) if by_type else "none",
        "by_mismatch_type": by_type,
        "affected_method_count": len({_text(row.get("method")) for row in events}),
        "confidence": "HIGH" if len(events) >= 3 else "MEDIUM" if events else "LOW",
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


# ARCH-004G2.4-EB1 canonical materialization and validation path. The original
# TRADING-326～335 functions above are retained only as rendering primitives;
# public entrypoints delegate to this snapshot-backed path.
def _validated_taxonomy(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_signal_failure_taxonomy_artifact,
        validator_key="taxonomy_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "taxonomy source validation failed")
    payload = signal_failure_taxonomy_report_payload(taxonomy_id=artifact_id, output_dir=root)
    _require(_text(payload.get("taxonomy_id")) == artifact_id, "taxonomy id mismatch")
    return payload


def _validated_design(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=micro_search_foundation.validate_micro_search_v4_design_artifact,
        validator_key="v4_design_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "micro-search design validation failed")
    return micro_search_foundation.micro_search_v4_design_report_payload(
        v4_design_id=artifact_id,
        output_dir=root,
    )


def _validated_backfill(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=micro_search_foundation.validate_micro_search_v4_backfill_artifact,
        validator_key="v4_backfill_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "micro-search backfill validation failed")
    return micro_search_foundation.micro_search_v4_backfill_report_payload(
        v4_backfill_id=artifact_id,
        output_dir=root,
    )


def _validated_signal(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis_foundation.validate_signal_instability_diagnosis_artifact,
        validator_key="signal_diagnosis_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "signal diagnosis validation failed")
    return diagnosis_foundation.signal_instability_diagnosis_report_payload(
        signal_diagnosis_id=artifact_id,
        output_dir=root,
    )


def _validated_consensus(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=diagnosis_foundation.validate_consensus_quality_review_artifact,
        validator_key="consensus_review_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "consensus review validation failed")
    return diagnosis_foundation.consensus_quality_review_report_payload(
        consensus_review_id=artifact_id,
        output_dir=root,
    )


def _validated_ledger(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_candidate_signal_ledger_artifact,
        validator_key="ledger_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "candidate signal ledger validation failed")
    return candidate_signal_ledger_report_payload(ledger_id=artifact_id, output_dir=root)


def _validated_churn(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_signal_churn_root_cause_artifact,
        validator_key="root_cause_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "churn source validation failed")
    return signal_churn_root_cause_report_payload(root_cause_id=artifact_id, output_dir=root)


def _validated_mismatch(artifact_id: str, root: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_regime_mismatch_attribution_artifact,
        validator_key="mismatch_id",
        artifact_id=artifact_id,
        root=root,
    )
    _require(validation.get("status") == "PASS", "mismatch source validation failed")
    return regime_mismatch_attribution_report_payload(mismatch_id=artifact_id, output_dir=root)


def _signal_sources(
    *,
    source_backfill_id: str,
    source_backfill_dir: Path,
    v4_design_dir: Path,
    signal_dir: Path,
    consensus_dir: Path,
) -> dict[str, Any]:
    backfill = _validated_backfill(source_backfill_id, source_backfill_dir)
    design_id = _text(backfill.get("v4_design_id"))
    _require(bool(design_id), "backfill design lineage missing")
    design = _validated_design(design_id, v4_design_dir)
    _require(_text(design.get("v4_design_id")) == design_id, "design lineage mismatch")
    signal_id = _text(design.get("signal_diagnosis_id"))
    consensus_id = _text(design.get("consensus_review_id"))
    signal = _validated_signal(signal_id, signal_dir) if signal_id else {}
    consensus = _validated_consensus(consensus_id, consensus_dir) if consensus_id else {}
    if consensus:
        _require(
            _text(consensus.get("signal_diagnosis_id")) == signal_id,
            "consensus/signal lineage mismatch",
        )
    return {
        "source_backfill_type": "micro_search_v4_backfill",
        "source_backfill_id": source_backfill_id,
        "v4_design_id": design_id,
        "signal_diagnosis_id": signal_id or None,
        "consensus_review_id": consensus_id or None,
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "data_quality_status": backfill.get("data_quality_status"),
        "latest_valid_as_of": backfill.get("latest_valid_as_of"),
        "backfill": backfill,
        "design": design,
        "signal": signal,
        "consensus": consensus,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _dated_ledger_events(
    taxonomy: Mapping[str, Any], source: Mapping[str, Any]
) -> list[dict[str, Any]]:
    _ = taxonomy
    signal = _mapping(source.get("signal"))
    rows = _records(signal.get("signal_flip_events"))
    events: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        event_date = _text(row.get("date"))
        method = _text(row.get("method"))
        _require(bool(event_date and method), "dated signal event identity missing")
        modes = _texts(row.get("failure_modes")) or ["signal_churn"]
        events.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "event_id": _text(row.get("event_id"))
                or _stable_id("signal-event", method, event_date, index),
                "date": event_date,
                "method": method,
                "symbol_group": row.get("symbol_group"),
                "signal_direction": row.get("signal_direction"),
                "previous_signal_direction": row.get("previous_signal_direction"),
                "direction_changed": row.get("direction_changed"),
                "weight_delta": row.get("weight_delta"),
                "total_abs_weight_change": row.get("total_abs_weight_change"),
                "regime_context": row.get("regime_context"),
                "candidate_dispersion": row.get("candidate_dispersion"),
                "consensus_confidence": row.get("consensus_confidence"),
                "subsequent_5d_return": row.get("subsequent_5d_return"),
                "subsequent_20d_return": row.get("subsequent_20d_return"),
                "event_quality": row.get("event_quality"),
                "failure_modes": sorted(set(modes)),
                "event_source": "validated_dated_signal_ledger",
                "source_backfill_id": source.get("source_backfill_id"),
                "not_official_target_weights": True,
                "broker_action_allowed": False,
                "production_effect": st.PRODUCTION_EFFECT,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(events, key=lambda row: (_text(row.get("date")), _text(row.get("method"))))


def _dated_ledger_summary(
    events: Sequence[Mapping[str, Any]],
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    signal = _mapping(source.get("signal"))
    source_methods = {
        _text(row.get("method")): row
        for row in _records(signal.get("method_signal_stability"))
        if _text(row.get("method"))
    }
    methods = sorted(set(source_methods) | {_text(row.get("method")) for row in events})
    method_rows: list[dict[str, Any]] = []
    failure_counts: dict[str, int] = {}
    for event in events:
        for mode in _texts(event.get("failure_modes")):
            failure_counts[mode] = failure_counts.get(mode, 0) + 1
    for method in methods:
        rows = [row for row in events if row.get("method") == method]
        source_row = source_methods.get(method, {})
        if not rows:
            method_rows.append(
                {
                    "method": method,
                    "event_count": None,
                    "direction_change_count": None,
                    "harmful_event_count": None,
                    "harmful_event_share": None,
                    "high_dispersion_event_count": None,
                    "dominant_failure_mode": None,
                    "signal_quality_status": "INSUFFICIENT_DATA",
                    "aggregate_proxy_available": bool(source_row),
                    "dated_signal_ledger_available": False,
                    **st.EXPERIMENT_FACTORY_SAFETY,
                }
            )
            continue
        method_counts: dict[str, int] = {}
        for row in rows:
            for mode in _texts(row.get("failure_modes")):
                method_counts[mode] = method_counts.get(mode, 0) + 1
        harmful = sum(1 for row in rows if row.get("event_quality") == "HARMFUL")
        direction_changes = sum(1 for row in rows if row.get("direction_changed") is True)
        dispersion_threshold = _float(_policy_rules(policy).get("candidate_dispersion_threshold"))
        high_dispersion = sum(
            1
            for row in rows
            if row.get("candidate_dispersion") is not None
            and _float(row.get("candidate_dispersion")) >= dispersion_threshold
        )
        harmful_share = harmful / len(rows)
        status = "UNSTABLE" if harmful or direction_changes else "STABLE"
        method_rows.append(
            {
                "method": method,
                "event_count": len(rows),
                "direction_change_count": direction_changes,
                "harmful_event_count": harmful,
                "harmful_event_share": round(harmful_share, 6),
                "high_dispersion_event_count": high_dispersion,
                "dominant_failure_mode": (
                    max(method_counts, key=method_counts.get) if method_counts else None
                ),
                "signal_quality_status": status,
                "aggregate_proxy_available": bool(source_row),
                "dated_signal_ledger_available": True,
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    sufficient = bool(events)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "evidence_status": "SUFFICIENT" if sufficient else "INSUFFICIENT_DATA",
        "dated_signal_ledger_available": sufficient,
        "event_count": len(events),
        "method_count": len(methods),
        "unstable_method_count": (
            sum(1 for row in method_rows if row.get("signal_quality_status") == "UNSTABLE")
            if sufficient
            else None
        ),
        "dominant_failure_mode": (
            max(failure_counts, key=failure_counts.get) if failure_counts else None
        ),
        "failure_mode_counts": failure_counts,
        "recommended_next_action": (
            None if sufficient else _policy_rules(policy).get("missing_dated_signal_action")
        ),
        "methods": method_rows,
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _evidence_churn_summary(ledger: Mapping[str, Any]) -> dict[str, Any]:
    events = _records(ledger.get("signal_events"))
    if not events:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "dominant_root_cause": None,
            "confidence": None,
            "evidence_status": "INSUFFICIENT_DATA",
            "event_count": 0,
            "harmful_event_count": None,
            "affected_methods": [],
            "supporting_evidence": [],
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    summary = _churn_root_cause_summary(ledger)
    summary["evidence_status"] = "SUFFICIENT"
    return summary


def _evidence_mismatch_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "schema_version": st.SCHEMA_VERSION,
            "mismatch_count": 0,
            "dominant_mismatch_type": None,
            "by_mismatch_type": {},
            "affected_method_count": None,
            "confidence": None,
            "evidence_status": "INSUFFICIENT_DATA",
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    summary = _regime_mismatch_summary(events)
    summary["evidence_status"] = "SUFFICIENT"
    return summary


def _evidence_filters(
    root_cause: Mapping[str, Any],
    mismatch: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    root_summary = _mapping(root_cause.get("churn_root_cause_summary"))
    mismatch_summary = _mapping(mismatch.get("regime_mismatch_summary"))
    if (
        root_summary.get("evidence_status") != "SUFFICIENT"
        or mismatch_summary.get("evidence_status") != "SUFFICIENT"
    ):
        return {
            "schema_version": st.SCHEMA_VERSION,
            "root_cause_id": root_cause.get("root_cause_id"),
            "mismatch_id": mismatch.get("mismatch_id"),
            "evidence_status": "INSUFFICIENT_DATA",
            "dominant_root_cause": None,
            "dominant_mismatch_type": None,
            "filters": [],
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    rules = _policy_rules(policy)
    dispersion = rules.get("candidate_dispersion_threshold")
    persistence = rules.get("persistence_days")
    filters = [
        {
            "filter_id": "high_dispersion_hold_filter",
            "trigger": f"candidate_dispersion >= {dispersion}",
            "action": "hold_previous_target_or_reduce_active_tilt",
            "intended_effect": "reduce false active-tilt moves when candidates disagree",
            "target_failure_modes": [
                "candidate_disagreement_high",
                "consensus_dispersion_high",
            ],
            "complexity": "LOW",
        },
        {
            "filter_id": "signal_persistence_filter",
            "trigger": f"direction_changed and persistence_days < {persistence}",
            "action": "delay_signal_change",
            "intended_effect": "reduce churn in sideways or noisy periods",
            "target_failure_modes": [
                "signal_churn",
                "direction_flip_high",
                "overreact_to_noise",
            ],
            "complexity": "LOW",
        },
        {
            "filter_id": "regime_mismatch_filter",
            "trigger": "risk_increase_during_drawdown or lag_in_recovery",
            "action": "block_or_scale_conflicting_signal",
            "intended_effect": "align signal action with regime context",
            "target_failure_modes": ["regime_mismatch", "risk_asset_false_positive"],
            "complexity": "MEDIUM",
        },
        {
            "filter_id": "top_candidate_stability_filter",
            "trigger": "unstable_top_candidate or repeated direction flips",
            "action": "require stable top candidate before switching active tilt",
            "intended_effect": "reduce top-candidate rotation",
            "target_failure_modes": ["unstable_top_candidate", "direction_flip_high"],
            "complexity": "MEDIUM",
        },
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "root_cause_id": root_cause.get("root_cause_id"),
        "mismatch_id": mismatch.get("mismatch_id"),
        "evidence_status": "SUFFICIENT",
        "dominant_root_cause": root_summary.get("dominant_root_cause"),
        "dominant_mismatch_type": mismatch_summary.get("dominant_mismatch_type"),
        "filters": [{**row, **st.EXPERIMENT_FACTORY_SAFETY} for row in filters],
        "policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _evidence_filter_config(
    filters: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    rules = _policy_rules(policy)
    return {
        "schema_version": st.SCHEMA_VERSION,
        "filter_design_id": "",
        "evidence_status": filters.get("evidence_status"),
        "method": {
            "mode": "research_screening_only",
            "policy_status": _mapping(policy.get("policy_metadata")).get("status"),
            "owner": _mapping(policy.get("policy_metadata")).get("owner"),
            "automatic_filter_implementation_allowed": False,
        },
        "thresholds": {
            "candidate_dispersion": rules.get("candidate_dispersion_threshold"),
            "persistence_days": rules.get("persistence_days"),
            "high_flip_count": rules.get("high_flip_count"),
            "harmful_event_share": rules.get("harmful_event_share"),
        },
        "filters": _records(filters.get("filters")),
        "policy_version": _policy_version(policy),
        "safety": {**st.EXPERIMENT_FACTORY_SAFETY},
    }


def _taxonomy_material(
    *,
    root: Path,
    taxonomy_id: str,
    policy: Mapping[str, Any],
    policy_path: Path,
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    normalized = _normalized_signal_failure_taxonomy(policy)
    normalized["policy_version"] = _policy_version(policy)
    normalized["signal_quality_policy"] = _policy_rules(policy)
    catalog = _signal_failure_mode_catalog(normalized)
    catalog["policy_version"] = _policy_version(policy)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_failure_taxonomy_manifest",
        "taxonomy_id": taxonomy_id,
        "source_taxonomy_id": policy.get("taxonomy_id"),
        "config_path": str(policy_path),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "policy_version": _policy_version(policy),
        "failure_mode_count": len(_records(catalog.get("failure_modes"))),
        "family_count": len(_records(normalized.get("families"))),
        "signal_failure_taxonomy_manifest_path": str(root / TAXONOMY_VIEWS[0]),
        "normalized_signal_failure_taxonomy_path": str(root / TAXONOMY_VIEWS[1]),
        "signal_failure_mode_catalog_path": str(root / TAXONOMY_VIEWS[2]),
        "signal_failure_taxonomy_report_path": str(root / TAXONOMY_VIEWS[3]),
        "signal_failure_taxonomy_input_snapshot_path": str(
            root / "signal_failure_taxonomy_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        TAXONOMY_VIEWS[0]: foundation._json_bytes(manifest),
        TAXONOMY_VIEWS[1]: _yaml_bytes(normalized),
        TAXONOMY_VIEWS[2]: foundation._json_bytes(catalog),
        TAXONOMY_VIEWS[3]: foundation._text_file_bytes(
            render_signal_failure_taxonomy_report(manifest, catalog)
        ),
    }
    return manifest, views


def _ledger_material(
    *,
    root: Path,
    ledger_id: str,
    taxonomy: Mapping[str, Any],
    source: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    events = _dated_ledger_events(taxonomy, source)
    summary = _dated_ledger_summary(events, source, policy)
    insufficient = summary.get("evidence_status") == "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_signal_ledger_manifest",
        "ledger_id": ledger_id,
        "taxonomy_id": taxonomy.get("taxonomy_id"),
        "source_backfill_id": source.get("source_backfill_id"),
        "source_backfill_type": source.get("source_backfill_type"),
        "source_v4_design_id": source.get("v4_design_id"),
        "source_signal_diagnosis_id": source.get("signal_diagnosis_id"),
        "source_consensus_review_id": source.get("consensus_review_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS_WITH_WARNINGS" if insufficient else "PASS",
        "evidence_status": summary.get("evidence_status"),
        "market_regime": source.get("market_regime", "ai_after_chatgpt"),
        "date_start": source.get("date_start"),
        "date_end": source.get("date_end"),
        "data_quality_status": source.get("data_quality_status", "UNKNOWN"),
        "event_count": len(events),
        "policy_version": _policy_version(policy),
        "candidate_signal_ledger_manifest_path": str(root / LEDGER_VIEWS[0]),
        "signal_events_path": str(root / LEDGER_VIEWS[1]),
        "candidate_signal_summary_path": str(root / LEDGER_VIEWS[2]),
        "candidate_signal_ledger_report_path": str(root / LEDGER_VIEWS[3]),
        "reader_brief_section_path": str(root / LEDGER_VIEWS[4]),
        "candidate_signal_ledger_input_snapshot_path": str(
            root / "candidate_signal_ledger_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    reader = render_candidate_signal_ledger_reader_brief(summary)
    views = {
        LEDGER_VIEWS[0]: foundation._json_bytes(manifest),
        LEDGER_VIEWS[1]: foundation._jsonl_bytes(events),
        LEDGER_VIEWS[2]: foundation._json_bytes(summary),
        LEDGER_VIEWS[3]: foundation._text_file_bytes(
            render_candidate_signal_ledger_report(manifest, summary)
        ),
        LEDGER_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


def _churn_material(
    *, root: Path, artifact_id: str, ledger: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    summary = _evidence_churn_summary(ledger)
    summary["root_cause_id"] = artifact_id
    clusters = _churn_event_clusters(ledger) if _records(ledger.get("signal_events")) else []
    mitigations = (
        _churn_mitigation_candidates(summary)
        if summary.get("evidence_status") == "SUFFICIENT"
        else {
            "schema_version": st.SCHEMA_VERSION,
            "evidence_status": "INSUFFICIENT_DATA",
            "mitigations": [],
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
    )
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_churn_root_cause_manifest",
        "root_cause_id": artifact_id,
        "ledger_id": ledger.get("ledger_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if summary.get("evidence_status") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "evidence_status": summary.get("evidence_status"),
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "policy_version": ledger.get("policy_version"),
        "signal_churn_root_cause_manifest_path": str(root / CHURN_VIEWS[0]),
        "churn_root_cause_summary_path": str(root / CHURN_VIEWS[1]),
        "churn_event_clusters_path": str(root / CHURN_VIEWS[2]),
        "churn_mitigation_candidates_path": str(root / CHURN_VIEWS[3]),
        "signal_churn_root_cause_report_path": str(root / CHURN_VIEWS[4]),
        "signal_churn_root_cause_input_snapshot_path": str(
            root / "signal_churn_root_cause_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        CHURN_VIEWS[0]: foundation._json_bytes(manifest),
        CHURN_VIEWS[1]: foundation._json_bytes(summary),
        CHURN_VIEWS[2]: foundation._jsonl_bytes(clusters),
        CHURN_VIEWS[3]: foundation._json_bytes(mitigations),
        CHURN_VIEWS[4]: foundation._text_file_bytes(
            render_signal_churn_root_cause_report(manifest, summary, clusters, mitigations)
        ),
    }
    return manifest, views


def _mismatch_material(
    *, root: Path, artifact_id: str, ledger: Mapping[str, Any], generated: datetime
) -> tuple[dict[str, Any], dict[str, bytes]]:
    events = _regime_mismatch_attribution_events(ledger)
    summary = _evidence_mismatch_summary(events)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_mismatch_manifest",
        "mismatch_id": artifact_id,
        "ledger_id": ledger.get("ledger_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if summary.get("evidence_status") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "evidence_status": summary.get("evidence_status"),
        "market_regime": ledger.get("market_regime", "ai_after_chatgpt"),
        "date_start": ledger.get("date_start"),
        "date_end": ledger.get("date_end"),
        "data_quality_status": ledger.get("data_quality_status"),
        "policy_version": ledger.get("policy_version"),
        "regime_mismatch_manifest_path": str(root / MISMATCH_VIEWS[0]),
        "regime_mismatch_events_path": str(root / MISMATCH_VIEWS[1]),
        "regime_mismatch_summary_path": str(root / MISMATCH_VIEWS[2]),
        "regime_mismatch_report_path": str(root / MISMATCH_VIEWS[3]),
        "regime_mismatch_attribution_input_snapshot_path": str(
            root / "regime_mismatch_attribution_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        MISMATCH_VIEWS[0]: foundation._json_bytes(manifest),
        MISMATCH_VIEWS[1]: foundation._jsonl_bytes(events),
        MISMATCH_VIEWS[2]: foundation._json_bytes(summary),
        MISMATCH_VIEWS[3]: foundation._text_file_bytes(
            render_regime_mismatch_report(manifest, summary)
        ),
    }
    return manifest, views


def _filter_material(
    *,
    root: Path,
    artifact_id: str,
    root_cause: Mapping[str, Any],
    mismatch: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    filters = _evidence_filters(root_cause, mismatch, policy)
    config = _evidence_filter_config(filters, policy)
    config["filter_design_id"] = artifact_id
    reader = render_candidate_quality_filter_reader_brief(filters)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_quality_filter_manifest",
        "filter_design_id": artifact_id,
        "root_cause_id": root_cause.get("root_cause_id"),
        "mismatch_id": mismatch.get("mismatch_id"),
        "source_ledger_id": root_cause.get("ledger_id"),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS_WITH_WARNINGS"
            if filters.get("evidence_status") == "INSUFFICIENT_DATA"
            else "PASS"
        ),
        "evidence_status": filters.get("evidence_status"),
        "market_regime": root_cause.get("market_regime", "ai_after_chatgpt"),
        "data_quality_status": root_cause.get("data_quality_status"),
        "policy_version": _policy_version(policy),
        "candidate_quality_filter_manifest_path": str(root / FILTER_VIEWS[0]),
        "proposed_quality_filters_path": str(root / FILTER_VIEWS[1]),
        "filter_design_config_path": str(root / FILTER_VIEWS[2]),
        "candidate_quality_filter_design_report_path": str(root / FILTER_VIEWS[3]),
        "reader_brief_section_path": str(root / FILTER_VIEWS[4]),
        "candidate_quality_filter_design_input_snapshot_path": str(
            root / "candidate_quality_filter_design_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        FILTER_VIEWS[0]: foundation._json_bytes(manifest),
        FILTER_VIEWS[1]: foundation._json_bytes(filters),
        FILTER_VIEWS[2]: _yaml_bytes(config),
        FILTER_VIEWS[3]: foundation._text_file_bytes(
            render_candidate_quality_filter_design_report(manifest, filters)
        ),
        FILTER_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    return manifest, views


@with_artifact_validation_session
def _run_taxonomy(
    *, config_path: Path, output_dir: Path, generated_at: datetime | None
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    policy = _policy(config_path)
    requested_id = _text(policy.get("taxonomy_id"), "signal_feature_failure_taxonomy_v1")
    taxonomy_id = _stable_id("signal-failure-taxonomy", requested_id, generated.isoformat())
    root = _unique_dir(output_dir / taxonomy_id)
    manifest, views = _taxonomy_material(
        root=root,
        taxonomy_id=root.name,
        policy=policy,
        policy_path=config_path,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": TAXONOMY_INPUT_SCHEMA,
        "taxonomy_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(config_path),
        "view_hashes": foundation._view_hashes(root, TAXONOMY_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "signal_failure_taxonomy_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_signal_failure_taxonomy", root.name, root / TAXONOMY_VIEWS[0])
    return {
        "taxonomy_id": root.name,
        "taxonomy_dir": root,
        "manifest": manifest,
        "normalized_signal_failure_taxonomy": yaml.safe_load(
            (root / TAXONOMY_VIEWS[1]).read_text(encoding="utf-8")
        ),
        "signal_failure_mode_catalog": _read_json(root / TAXONOMY_VIEWS[2]),
    }


def _taxonomy_report(*, taxonomy_id: str | None, latest: bool, output_dir: Path) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=taxonomy_id,
        latest_pointer="latest_signal_failure_taxonomy",
        latest=latest,
        output_dir=output_dir,
        required_name=TAXONOMY_VIEWS[0],
    )
    return {
        **_read_json(root / TAXONOMY_VIEWS[0]),
        "normalized_signal_failure_taxonomy": yaml.safe_load(
            (root / TAXONOMY_VIEWS[1]).read_text(encoding="utf-8")
        ),
        "signal_failure_mode_catalog": _read_json(root / TAXONOMY_VIEWS[2]),
        "input_snapshot": _read_json(root / "signal_failure_taxonomy_input_snapshot.json"),
        "taxonomy_dir": str(root),
    }


def _rebuild_taxonomy(root: Path, taxonomy_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_failure_taxonomy_input_snapshot.json")
    _require(snapshot.get("schema_version") == TAXONOMY_INPUT_SCHEMA, "taxonomy schema")
    _require(_payload_experiment_safe(snapshot), "taxonomy snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy_path = Path(_text(policy_source.get("path")))
    policy = _policy(policy_path)
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _, expected = _taxonomy_material(
        root=root,
        taxonomy_id=taxonomy_id,
        policy=policy,
        policy_path=policy_path,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_taxonomy(*, taxonomy_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / taxonomy_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="signal_failure_taxonomy_input_snapshot.json",
        schema=TAXONOMY_INPUT_SCHEMA,
        id_key="taxonomy_id",
        artifact_id=taxonomy_id,
        view_names=TAXONOMY_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_signal_failure_taxonomy_validation", taxonomy_id, checks
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_signal_failure_taxonomy_validation",
        artifact_id=taxonomy_id,
        checks=checks,
        rebuild=lambda: _rebuild_taxonomy(root, taxonomy_id),
    )


@with_artifact_validation_session
def _run_ledger(
    *,
    taxonomy_id: str,
    source_backfill_id: str,
    taxonomy_dir: Path,
    source_backfill_dir: Path,
    v4_design_dir: Path,
    signal_dir: Path,
    consensus_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    taxonomy = _validated_taxonomy(taxonomy_id, taxonomy_dir)
    source = _signal_sources(
        source_backfill_id=source_backfill_id,
        source_backfill_dir=source_backfill_dir,
        v4_design_dir=v4_design_dir,
        signal_dir=signal_dir,
        consensus_dir=consensus_dir,
    )
    policy_source = _mapping(_mapping(taxonomy.get("input_snapshot")).get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    source_payloads = [source["backfill"], source["design"]]
    source_payloads.extend(
        payload for payload in (source.get("signal"), source.get("consensus")) if payload
    )
    _chronology(generated, taxonomy, *source_payloads)
    ledger_id = _stable_id(
        "candidate-signal-ledger", taxonomy_id, source_backfill_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / ledger_id)
    manifest, views = _ledger_material(
        root=root,
        ledger_id=root.name,
        taxonomy=taxonomy,
        source=source,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    signal_id = _text(source.get("signal_diagnosis_id"))
    consensus_id = _text(source.get("consensus_review_id"))
    design_id = _text(source.get("v4_design_id"))
    snapshot = {
        "schema_version": LEDGER_INPUT_SCHEMA,
        "ledger_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "taxonomy_source": _binding(
            kind="signal_failure_taxonomy",
            artifact_id=taxonomy_id,
            root=taxonomy_dir / taxonomy_id,
            names=TAXONOMY_FILES,
        ),
        "backfill_source": _binding(
            kind="micro_search_v4_backfill",
            artifact_id=source_backfill_id,
            root=source_backfill_dir / source_backfill_id,
            names=micro_search_foundation.BACKFILL_FILES,
        ),
        "design_source": _binding(
            kind="micro_search_v4_design",
            artifact_id=design_id,
            root=v4_design_dir / design_id,
            names=micro_search_foundation.DESIGN_FILES,
        ),
        "signal_source": (
            _binding(
                kind="signal_instability_diagnosis",
                artifact_id=signal_id,
                root=signal_dir / signal_id,
                names=diagnosis_foundation.SIGNAL_FILES,
            )
            if signal_id
            else None
        ),
        "consensus_source": (
            _binding(
                kind="consensus_quality_review",
                artifact_id=consensus_id,
                root=consensus_dir / consensus_id,
                names=diagnosis_foundation.CONSENSUS_FILES,
            )
            if consensus_id
            else None
        ),
        "view_hashes": foundation._view_hashes(root, LEDGER_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "candidate_signal_ledger_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_candidate_signal_ledger", root.name, root / LEDGER_VIEWS[0])
    return {
        "ledger_id": root.name,
        "ledger_dir": root,
        "manifest": manifest,
        "signal_events": _read_jsonl(root / LEDGER_VIEWS[1]),
        "candidate_signal_summary": _read_json(root / LEDGER_VIEWS[2]),
        "reader_brief_section": (root / LEDGER_VIEWS[4]).read_text(encoding="utf-8"),
    }


def _ledger_report(*, ledger_id: str | None, latest: bool, output_dir: Path) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ledger_id,
        latest_pointer="latest_candidate_signal_ledger",
        latest=latest,
        output_dir=output_dir,
        required_name=LEDGER_VIEWS[0],
    )
    return {
        **_read_json(root / LEDGER_VIEWS[0]),
        "signal_events": _read_jsonl(root / LEDGER_VIEWS[1]),
        "candidate_signal_summary": _read_json(root / LEDGER_VIEWS[2]),
        "reader_brief_section": (root / LEDGER_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "candidate_signal_ledger_input_snapshot.json"),
        "ledger_dir": str(root),
    }


def _rebuild_ledger(root: Path, ledger_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "candidate_signal_ledger_input_snapshot.json")
    _require(snapshot.get("schema_version") == LEDGER_INPUT_SCHEMA, "ledger schema")
    _require(_payload_experiment_safe(snapshot), "ledger snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    taxonomy_source = _mapping(snapshot.get("taxonomy_source"))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    design_source = _mapping(snapshot.get("design_source"))
    _validate_binding(taxonomy_source, kind="signal_failure_taxonomy", names=TAXONOMY_FILES)
    _validate_binding(
        backfill_source,
        kind="micro_search_v4_backfill",
        names=micro_search_foundation.BACKFILL_FILES,
    )
    _validate_binding(
        design_source,
        kind="micro_search_v4_design",
        names=micro_search_foundation.DESIGN_FILES,
    )
    taxonomy = _validated_taxonomy(
        _binding_id(taxonomy_source), _binding_root(taxonomy_source).parent
    )
    backfill = _validated_backfill(
        _binding_id(backfill_source), _binding_root(backfill_source).parent
    )
    design = _validated_design(_binding_id(design_source), _binding_root(design_source).parent)
    _require(_text(backfill.get("v4_design_id")) == _binding_id(design_source), "design lineage")
    signal_binding = snapshot.get("signal_source")
    consensus_binding = snapshot.get("consensus_source")
    signal: dict[str, Any] = {}
    consensus: dict[str, Any] = {}
    if signal_binding is not None:
        signal_source = _mapping(signal_binding)
        _validate_binding(
            signal_source,
            kind="signal_instability_diagnosis",
            names=diagnosis_foundation.SIGNAL_FILES,
        )
        signal = _validated_signal(_binding_id(signal_source), _binding_root(signal_source).parent)
    if consensus_binding is not None:
        consensus_source = _mapping(consensus_binding)
        _validate_binding(
            consensus_source,
            kind="consensus_quality_review",
            names=diagnosis_foundation.CONSENSUS_FILES,
        )
        consensus = _validated_consensus(
            _binding_id(consensus_source), _binding_root(consensus_source).parent
        )
    signal_id = _text(design.get("signal_diagnosis_id"))
    consensus_id = _text(design.get("consensus_review_id"))
    _require(
        signal_id == (_binding_id(_mapping(signal_binding)) if signal_binding else ""),
        "signal lineage",
    )
    _require(
        consensus_id == (_binding_id(_mapping(consensus_binding)) if consensus_binding else ""),
        "consensus lineage",
    )
    if consensus:
        _require(_text(consensus.get("signal_diagnosis_id")) == signal_id, "consensus lineage")
    taxonomy_policy_source = _mapping(_mapping(taxonomy.get("input_snapshot")).get("policy_source"))
    _require(taxonomy_policy_source == policy_source, "taxonomy policy binding mismatch")
    source = {
        "source_backfill_type": "micro_search_v4_backfill",
        "source_backfill_id": _binding_id(backfill_source),
        "v4_design_id": _binding_id(design_source),
        "signal_diagnosis_id": signal_id or None,
        "consensus_review_id": consensus_id or None,
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "data_quality_status": backfill.get("data_quality_status"),
        "latest_valid_as_of": backfill.get("latest_valid_as_of"),
        "backfill": backfill,
        "design": design,
        "signal": signal,
        "consensus": consensus,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    payloads = [taxonomy, backfill, design, *[x for x in (signal, consensus) if x]]
    _chronology(generated, *payloads)
    _, expected = _ledger_material(
        root=root,
        ledger_id=ledger_id,
        taxonomy=taxonomy,
        source=source,
        policy=policy,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_ledger(*, ledger_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / ledger_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="candidate_signal_ledger_input_snapshot.json",
        schema=LEDGER_INPUT_SCHEMA,
        id_key="ledger_id",
        artifact_id=ledger_id,
        view_names=LEDGER_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_candidate_signal_ledger_validation", ledger_id, checks
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_candidate_signal_ledger_validation",
        artifact_id=ledger_id,
        checks=checks,
        rebuild=lambda: _rebuild_ledger(root, ledger_id),
    )


def _source_policy_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
    source = _mapping(_mapping(payload.get("input_snapshot")).get("policy_source"))
    foundation._validate_file_binding(source)
    return source


@with_artifact_validation_session
def _run_churn(
    *, ledger_id: str, ledger_dir: Path, output_dir: Path, generated_at: datetime | None
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    ledger = _validated_ledger(ledger_id, ledger_dir)
    _chronology(generated, ledger)
    policy_source = _source_policy_binding(ledger)
    artifact_id = _stable_id("signal-churn-root-cause", ledger_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _churn_material(
        root=root, artifact_id=root.name, ledger=ledger, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": CHURN_INPUT_SCHEMA,
        "root_cause_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "ledger_source": _binding(
            kind="candidate_signal_ledger",
            artifact_id=ledger_id,
            root=ledger_dir / ledger_id,
            names=LEDGER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, CHURN_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "signal_churn_root_cause_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_signal_churn_root_cause", root.name, root / CHURN_VIEWS[0])
    return {
        "root_cause_id": root.name,
        "root_cause_dir": root,
        "manifest": manifest,
        "churn_root_cause_summary": _read_json(root / CHURN_VIEWS[1]),
        "churn_event_clusters": _read_jsonl(root / CHURN_VIEWS[2]),
        "churn_mitigation_candidates": _read_json(root / CHURN_VIEWS[3]),
    }


def _churn_report(*, root_cause_id: str | None, latest: bool, output_dir: Path) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=root_cause_id,
        latest_pointer="latest_signal_churn_root_cause",
        latest=latest,
        output_dir=output_dir,
        required_name=CHURN_VIEWS[0],
    )
    return {
        **_read_json(root / CHURN_VIEWS[0]),
        "churn_root_cause_summary": _read_json(root / CHURN_VIEWS[1]),
        "churn_event_clusters": _read_jsonl(root / CHURN_VIEWS[2]),
        "churn_mitigation_candidates": _read_json(root / CHURN_VIEWS[3]),
        "input_snapshot": _read_json(root / "signal_churn_root_cause_input_snapshot.json"),
        "root_cause_dir": str(root),
    }


def _rebuild_churn(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "signal_churn_root_cause_input_snapshot.json")
    _require(snapshot.get("schema_version") == CHURN_INPUT_SCHEMA, "churn schema")
    _require(_payload_experiment_safe(snapshot), "churn snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    _policy(Path(_text(policy_source.get("path"))))
    ledger_source = _mapping(snapshot.get("ledger_source"))
    _validate_binding(ledger_source, kind="candidate_signal_ledger", names=LEDGER_FILES)
    ledger = _validated_ledger(_binding_id(ledger_source), _binding_root(ledger_source).parent)
    _require(_source_policy_binding(ledger) == policy_source, "churn policy lineage mismatch")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, ledger)
    _, expected = _churn_material(
        root=root, artifact_id=artifact_id, ledger=ledger, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_churn(*, root_cause_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / root_cause_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="signal_churn_root_cause_input_snapshot.json",
        schema=CHURN_INPUT_SCHEMA,
        id_key="root_cause_id",
        artifact_id=root_cause_id,
        view_names=CHURN_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_signal_churn_root_cause_validation", root_cause_id, checks
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_signal_churn_root_cause_validation",
        artifact_id=root_cause_id,
        checks=checks,
        rebuild=lambda: _rebuild_churn(root, root_cause_id),
    )


@with_artifact_validation_session
def _run_mismatch(
    *, ledger_id: str, ledger_dir: Path, output_dir: Path, generated_at: datetime | None
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    ledger = _validated_ledger(ledger_id, ledger_dir)
    _chronology(generated, ledger)
    policy_source = _source_policy_binding(ledger)
    artifact_id = _stable_id("regime-mismatch-attribution", ledger_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _mismatch_material(
        root=root, artifact_id=root.name, ledger=ledger, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": MISMATCH_INPUT_SCHEMA,
        "mismatch_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "ledger_source": _binding(
            kind="candidate_signal_ledger",
            artifact_id=ledger_id,
            root=ledger_dir / ledger_id,
            names=LEDGER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, MISMATCH_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "regime_mismatch_attribution_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_regime_mismatch_attribution", root.name, root / MISMATCH_VIEWS[0])
    return {
        "mismatch_id": root.name,
        "mismatch_dir": root,
        "manifest": manifest,
        "regime_mismatch_events": _read_jsonl(root / MISMATCH_VIEWS[1]),
        "regime_mismatch_summary": _read_json(root / MISMATCH_VIEWS[2]),
    }


def _mismatch_report(*, mismatch_id: str | None, latest: bool, output_dir: Path) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=mismatch_id,
        latest_pointer="latest_regime_mismatch_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name=MISMATCH_VIEWS[0],
    )
    return {
        **_read_json(root / MISMATCH_VIEWS[0]),
        "regime_mismatch_events": _read_jsonl(root / MISMATCH_VIEWS[1]),
        "regime_mismatch_summary": _read_json(root / MISMATCH_VIEWS[2]),
        "input_snapshot": _read_json(root / "regime_mismatch_attribution_input_snapshot.json"),
        "mismatch_dir": str(root),
    }


def _rebuild_mismatch(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "regime_mismatch_attribution_input_snapshot.json")
    _require(snapshot.get("schema_version") == MISMATCH_INPUT_SCHEMA, "mismatch schema")
    _require(_payload_experiment_safe(snapshot), "mismatch snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    _policy(Path(_text(policy_source.get("path"))))
    ledger_source = _mapping(snapshot.get("ledger_source"))
    _validate_binding(ledger_source, kind="candidate_signal_ledger", names=LEDGER_FILES)
    ledger = _validated_ledger(_binding_id(ledger_source), _binding_root(ledger_source).parent)
    _require(_source_policy_binding(ledger) == policy_source, "mismatch policy lineage mismatch")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, ledger)
    _, expected = _mismatch_material(
        root=root, artifact_id=artifact_id, ledger=ledger, generated=generated
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_mismatch(*, mismatch_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / mismatch_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="regime_mismatch_attribution_input_snapshot.json",
        schema=MISMATCH_INPUT_SCHEMA,
        id_key="mismatch_id",
        artifact_id=mismatch_id,
        view_names=MISMATCH_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_regime_mismatch_attribution_validation", mismatch_id, checks
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_regime_mismatch_attribution_validation",
        artifact_id=mismatch_id,
        checks=checks,
        rebuild=lambda: _rebuild_mismatch(root, mismatch_id),
    )


@with_artifact_validation_session
def _run_filter(
    *,
    root_cause_id: str,
    mismatch_id: str,
    root_cause_dir: Path,
    mismatch_dir: Path,
    output_dir: Path,
    generated_at: datetime | None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    root_cause = _validated_churn(root_cause_id, root_cause_dir)
    mismatch = _validated_mismatch(mismatch_id, mismatch_dir)
    _require(
        _text(root_cause.get("ledger_id")) == _text(mismatch.get("ledger_id")),
        "filter source ledger lineage mismatch",
    )
    _chronology(generated, root_cause, mismatch)
    root_policy = _source_policy_binding(root_cause)
    mismatch_policy = _source_policy_binding(mismatch)
    _require(root_policy == mismatch_policy, "filter source policy lineage mismatch")
    policy = _policy(Path(_text(root_policy.get("path"))))
    artifact_id = _stable_id(
        "candidate-quality-filter-design",
        root_cause_id,
        mismatch_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, views = _filter_material(
        root=root,
        artifact_id=root.name,
        root_cause=root_cause,
        mismatch=mismatch,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    snapshot = {
        "schema_version": FILTER_INPUT_SCHEMA,
        "filter_design_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": root_policy,
        "root_cause_source": _binding(
            kind="signal_churn_root_cause",
            artifact_id=root_cause_id,
            root=root_cause_dir / root_cause_id,
            names=CHURN_FILES,
        ),
        "mismatch_source": _binding(
            kind="regime_mismatch_attribution",
            artifact_id=mismatch_id,
            root=mismatch_dir / mismatch_id,
            names=MISMATCH_FILES,
        ),
        "source_ledger_id": root_cause.get("ledger_id"),
        "view_hashes": foundation._view_hashes(root, FILTER_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "candidate_quality_filter_design_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_candidate_quality_filter_design", root.name, root / FILTER_VIEWS[0]
    )
    return {
        "filter_design_id": root.name,
        "filter_design_dir": root,
        "manifest": manifest,
        "proposed_quality_filters": _read_json(root / FILTER_VIEWS[1]),
        "filter_design_config": yaml.safe_load(
            (root / FILTER_VIEWS[2]).read_text(encoding="utf-8")
        ),
        "reader_brief_section": (root / FILTER_VIEWS[4]).read_text(encoding="utf-8"),
    }


def _filter_report(
    *, filter_design_id: str | None, latest: bool, output_dir: Path
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=filter_design_id,
        latest_pointer="latest_candidate_quality_filter_design",
        latest=latest,
        output_dir=output_dir,
        required_name=FILTER_VIEWS[0],
    )
    return {
        **_read_json(root / FILTER_VIEWS[0]),
        "proposed_quality_filters": _read_json(root / FILTER_VIEWS[1]),
        "filter_design_config": yaml.safe_load(
            (root / FILTER_VIEWS[2]).read_text(encoding="utf-8")
        ),
        "reader_brief_section": (root / FILTER_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "candidate_quality_filter_design_input_snapshot.json"),
        "filter_design_dir": str(root),
    }


def _rebuild_filter(root: Path, artifact_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "candidate_quality_filter_design_input_snapshot.json")
    _require(snapshot.get("schema_version") == FILTER_INPUT_SCHEMA, "filter schema")
    _require(_payload_experiment_safe(snapshot), "filter snapshot safety fields invalid")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    root_source = _mapping(snapshot.get("root_cause_source"))
    mismatch_source = _mapping(snapshot.get("mismatch_source"))
    _validate_binding(root_source, kind="signal_churn_root_cause", names=CHURN_FILES)
    _validate_binding(mismatch_source, kind="regime_mismatch_attribution", names=MISMATCH_FILES)
    root_cause = _validated_churn(_binding_id(root_source), _binding_root(root_source).parent)
    mismatch = _validated_mismatch(
        _binding_id(mismatch_source), _binding_root(mismatch_source).parent
    )
    ledger_id = _text(snapshot.get("source_ledger_id"))
    _require(_text(root_cause.get("ledger_id")) == ledger_id, "filter root ledger lineage")
    _require(_text(mismatch.get("ledger_id")) == ledger_id, "filter mismatch ledger lineage")
    _require(_source_policy_binding(root_cause) == policy_source, "filter root policy lineage")
    _require(_source_policy_binding(mismatch) == policy_source, "filter mismatch policy lineage")
    generated = _aware_time(snapshot.get("generated_at"), "snapshot.generated_at")
    _chronology(generated, root_cause, mismatch)
    _, expected = _filter_material(
        root=root,
        artifact_id=artifact_id,
        root_cause=root_cause,
        mismatch=mismatch,
        policy=policy,
        generated=generated,
    )
    return diagnosis_foundation._check_bytes(root, expected)


@with_artifact_validation_session
def _validate_filter(*, filter_design_id: str, output_dir: Path) -> dict[str, Any]:
    root = output_dir / filter_design_id
    checks, ok = diagnosis_foundation._snapshot_preflight(
        root=root,
        snapshot_name="candidate_quality_filter_design_input_snapshot.json",
        schema=FILTER_INPUT_SCHEMA,
        id_key="filter_design_id",
        artifact_id=filter_design_id,
        view_names=FILTER_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_candidate_quality_filter_design_validation",
            filter_design_id,
            checks,
        )
    return diagnosis_foundation._validate_content(
        report_type="etf_dynamic_v3_candidate_quality_filter_design_validation",
        artifact_id=filter_design_id,
        checks=checks,
        rebuild=lambda: _rebuild_filter(root, filter_design_id),
    )
