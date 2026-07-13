from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
    _operations_datetime,
    _operations_generated_at,
    _operations_source_bundle,
    _report_input_snapshot,
    _validate_operations_source_bundle,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    EVIDENCE_QUALITY_BY_SOURCE,
    PRESSURE_REGIMES,
    SOURCE_MODES,
    _policy_snapshot,
    _validate_policy_live,
    _write_views_atomic,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _source_binding as _pressure_source_binding,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _validate_binding_live as _validate_pressure_binding_live,
)

DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_hypothesis_deep_dive"
)
DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_label_review"
DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_failure_study"
DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_research_note"
DEFAULT_DEFENSIVE_OWNER_PACK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_owner_pack"
DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "defensive_research_synthesis_v1.yaml"
)

DEEP_DIVE_SNAPSHOT_SCHEMA_VERSION = "defensive_hypothesis_deep_dive_input_snapshot.v2"
LABEL_REVIEW_SNAPSHOT_SCHEMA_VERSION = "defensive_label_review_input_snapshot.v2"
FAILURE_STUDY_SNAPSHOT_SCHEMA_VERSION = "defensive_failure_study_input_snapshot.v2"
RESEARCH_NOTE_SNAPSHOT_SCHEMA_VERSION = "defensive_research_note_input_snapshot.v2"
OWNER_PACK_SNAPSHOT_SCHEMA_VERSION = "defensive_owner_pack_input_snapshot.v2"


class DynamicV3DefensiveResearchError(ValueError):
    """Raised when defensive research synthesis cannot be reproduced safely."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3DefensiveResearchError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return _operations_generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3DefensiveResearchError(str(exc)) from exc


def _datetime(value: Any, *, field: str) -> datetime:
    try:
        return _operations_datetime(value, field=field)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3DefensiveResearchError(str(exc)) from exc


def _finite(value: Any) -> bool:
    return (
        isinstance(value, int | float)
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def _optional_float(value: Any) -> float | None:
    return float(value) if _finite(value) else None


def _rounded_mean(values: Sequence[Any]) -> float | None:
    finite = [float(value) for value in values if _finite(value)]
    return round(sum(finite) / len(finite), 6) if finite else None


def _read_policy(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3DefensiveResearchError(f"defensive research policy invalid: {path}") from exc
    _require(isinstance(payload, Mapping), "defensive research policy must be a mapping")
    policy = dict(payload)
    _require(policy.get("schema_version") == 1, "defensive research policy schema invalid")
    metadata = _mapping(policy.get("policy_metadata"))
    for field in ("owner", "version", "status", "rationale", "review_condition"):
        _require(bool(_text(metadata.get(field))), f"defensive research policy {field} missing")
    severity = _mapping(policy.get("failure_severity"))
    high = _optional_float(severity.get("high_score_min"))
    medium = _optional_float(severity.get("medium_score_min"))
    _require(
        high is not None and medium is not None and high > medium >= 0,
        "failure severity policy invalid",
    )
    label = _mapping(policy.get("label_review"))
    _require(bool(_text(label.get("current_label"))), "current label policy missing")
    _require(bool(_text(label.get("recommended_label"))), "recommended label policy missing")
    floor = label.get("minimum_forward_distinct_events_for_acceptable_label")
    _require(
        isinstance(floor, int) and not isinstance(floor, bool) and floor > 0,
        "label evidence floor invalid",
    )
    candidates = _records(label.get("candidate_labels"))
    _require(bool(candidates), "candidate label policy missing")
    labels = [_text(row.get("label")) for row in candidates]
    _require(
        all(labels) and len(labels) == len(set(labels)), "candidate labels invalid or duplicate"
    )
    options = _records(policy.get("owner_options"))
    decisions = [_text(row.get("decision")) for row in options]
    _require(
        all(decisions) and len(decisions) == len(set(decisions)),
        "owner options invalid or duplicate",
    )
    reason_rules = _mapping(policy.get("failure_reason_rules"))
    risk_regimes = reason_rules.get("risk_increase_regimes")
    _require(
        isinstance(risk_regimes, list)
        and bool(risk_regimes)
        and all(bool(_text(value)) for value in risk_regimes)
        and bool(_text(reason_rules.get("positive_exposure_reason")))
        and bool(_text(reason_rules.get("positive_turnover_reason")))
        and bool(_text(reason_rules.get("otherwise_reason"))),
        "failure reason policy invalid",
    )
    ideas = _records(policy.get("mitigation_ideas"))
    idea_ids = [_text(row.get("idea_id")) for row in ideas]
    _require(
        all(idea_ids) and len(idea_ids) == len(set(idea_ids)),
        "mitigation ideas invalid",
    )
    safety = _mapping(policy.get("safety"))
    _require(
        safety.get("can_support_rule_approval") is False
        and safety.get("auto_apply") is False
        and safety.get("policy_change_allowed") is False
        and safety.get("config_change_allowed") is False
        and safety.get("broker_action_allowed") is False
        and safety.get("production_effect") == "none",
        "defensive research policy safety invalid",
    )
    return policy


def _policy_binding(path: Path) -> dict[str, Any]:
    binding = _policy_snapshot(path)
    policy = _read_policy(path)
    _require(_mapping(binding.get("payload")) == policy, "defensive research policy snapshot drift")
    return binding


_LOCAL_SOURCES: dict[str, tuple[str, str, Callable[..., dict[str, Any]]]] = {}


def _register_local_sources() -> None:
    if _LOCAL_SOURCES:
        return
    _LOCAL_SOURCES.update(
        {
            "defensive_deep_dive": (
                "deep_dive_manifest.json",
                "deep_dive_id",
                validate_defensive_hypothesis_deep_dive_artifact,
            ),
            "defensive_label_review": (
                "label_review_manifest.json",
                "label_review_id",
                validate_defensive_label_review_artifact,
            ),
            "defensive_failure_study": (
                "failure_study_manifest.json",
                "failure_study_id",
                validate_defensive_failure_study_artifact,
            ),
            "defensive_research_note": (
                "defensive_research_note_manifest.json",
                "note_id",
                validate_defensive_research_note_artifact,
            ),
        }
    )


def _local_source_binding(
    *,
    source_kind: str,
    source_dir: Path,
    generated: datetime,
    canonical_files: Sequence[str],
    json_views: Sequence[str] = (),
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    _register_local_sources()
    manifest_name, id_key, validator = _LOCAL_SOURCES[source_kind]
    manifest = _read_json(source_dir / manifest_name)
    artifact_id = _text(manifest.get(id_key))
    _require(artifact_id == source_dir.name, f"{source_kind} identity mismatch")
    source_generated = _datetime(manifest.get("generated_at"), field=f"{source_kind} generated_at")
    _require(source_generated <= generated, f"{source_kind} generated after cutoff")
    validation = validator(**{id_key: artifact_id, "output_dir": source_dir.parent})
    _require(validation.get("status") == "PASS", f"{source_kind} validation failed")
    return {
        "binding_type": "local_defensive_research",
        "source_kind": source_kind,
        "artifact_id": artifact_id,
        "generated_at": source_generated.isoformat(),
        "validation": validation,
        "bundle": _operations_source_bundle(
            source_dir=source_dir,
            canonical_files=canonical_files,
            json_views=json_views,
            jsonl_views=jsonl_views,
            text_views=text_views,
        ),
    }


def _validate_local_binding_live(binding: Mapping[str, Any], *, generated: datetime) -> list[str]:
    errors = _validate_operations_source_bundle(_mapping(binding.get("bundle")))
    try:
        _register_local_sources()
        source_kind = _text(binding.get("source_kind"))
        manifest_name, id_key, validator = _LOCAL_SOURCES[source_kind]
        source_dir = Path(_text(_mapping(binding.get("bundle")).get("source_dir")))
        manifest = _read_json(source_dir / manifest_name)
        artifact_id = _text(manifest.get(id_key))
        source_generated = _datetime(
            manifest.get("generated_at"), field=f"{source_kind} generated_at"
        )
        _require(source_generated <= generated, f"{source_kind} generated after cutoff")
        validation = validator(**{id_key: artifact_id, "output_dir": source_dir.parent})
        if validation != _mapping(binding.get("validation")):
            errors.append(f"source validation drift: {source_dir}")
        if artifact_id != binding.get("artifact_id") or artifact_id != source_dir.name:
            errors.append(f"source identity drift: {source_dir}")
        if source_generated.isoformat() != binding.get("generated_at"):
            errors.append(f"source generated_at drift: {source_dir}")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


def _validate_snapshot_live(
    snapshot: Mapping[str, Any], *, expected_schema: str, generated: datetime
) -> list[str]:
    errors: list[str] = []
    if snapshot.get("schema_version") != expected_schema:
        errors.append("snapshot schema invalid")
    for binding in _records(snapshot.get("source_bindings")):
        if binding.get("binding_type") == "local_defensive_research":
            errors.extend(_validate_local_binding_live(binding, generated=generated))
        else:
            try:
                errors.extend(_validate_pressure_binding_live(binding, generated=generated))
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
    for policy in _records(snapshot.get("policy_bindings")):
        errors.extend(_validate_policy_live(policy))
        try:
            if _read_policy(Path(_text(policy.get("path")))) != _mapping(policy.get("payload")):
                errors.append("defensive research policy payload drift")
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
    return errors


def _binding(snapshot: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
    matches = [
        row for row in _records(snapshot.get("source_bindings")) if row.get("source_kind") == kind
    ]
    _require(len(matches) == 1, f"snapshot source binding invalid: {kind}")
    return matches[0]


def _bundle_json(binding: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    return _mapping(_mapping(_mapping(binding.get("bundle")).get("json")).get(name))


def _bundle_jsonl(binding: Mapping[str, Any], name: str) -> list[Mapping[str, Any]]:
    return _records(_mapping(_mapping(binding.get("bundle")).get("jsonl")).get(name))


def _policy(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    policies = _records(snapshot.get("policy_bindings"))
    _require(len(policies) == 1, "defensive research policy binding invalid")
    return _mapping(policies[0].get("payload"))


def _write_views(output_dir: Path, views: Mapping[str, bytes]) -> None:
    _write_views_atomic(output_dir, views)


def _view_mismatches(output_dir: Path, views: Mapping[str, bytes]) -> list[str]:
    return [
        name for name, payload in views.items() if not _file_bytes_match(output_dir / name, payload)
    ]


def _text_bytes(value: str) -> bytes:
    return value.encode("utf-8")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _safety() -> dict[str, Any]:
    return {
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "policy_change_allowed": False,
        "auto_apply": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "owner_approval_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
        "can_support_rule_approval": False,
        "config_change_allowed": False,
    }


def _source_generated(binding: Mapping[str, Any]) -> datetime:
    return _datetime(
        binding.get("generated_at"), field=f"{binding.get('source_kind')} generated_at"
    )


def _assert_chronology(*bindings: Mapping[str, Any], generated: datetime) -> None:
    times = [_source_generated(binding) for binding in bindings]
    _require(all(value <= generated for value in times), "source generated after output cutoff")


def _source_policy_version(snapshot: Mapping[str, Any]) -> str:
    return _text(_mapping(_policy(snapshot).get("policy_metadata")).get("version"))


def _render_deep_dive_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    attribution: Mapping[str, Any],
    comparison_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Hypothesis Deep Dive",
            "",
            f"- deep_dive_id: `{manifest.get('deep_dive_id')}`",
            f"- pressure_backfill_id: `{manifest.get('pressure_backfill_id')}`",
            f"- comparison_id: `{manifest.get('comparison_id')}`",
            f"- defensive_pressure_status: `{comparison_summary.get('defensive_status')}`",
            f"- supporting_cases: {manifest.get('supporting_case_count')}",
            f"- contradicting_cases: {manifest.get('contradicting_case_count')}",
            f"- mixed_cases: {manifest.get('mixed_or_empty_case_count')}",
            f"- can_support_rule_approval: `{manifest.get('can_support_rule_approval')}`",
            "",
            "## Regime Effects",
            *[
                "- "
                f"{row.get('regime')}: samples={row.get('sample_count')}, "
                f"distinct_events={row.get('distinct_event_count')}, "
                f"avg_return_delta={row.get('avg_return_delta_vs_no_trade')}, "
                f"avg_drawdown_delta={row.get('avg_drawdown_delta_vs_no_trade')}, "
                f"effect_status=`{row.get('effect_status')}`"
                for row in _records(matrix.get("regimes"))
            ],
            "",
            "## Interpretation",
            "- simulation evidence is research-only and cannot approve a production rule.",
            "- positive drawdown_delta_vs_no_trade means a smaller drawdown than no_trade.",
            "- missing exposure fields remain null and are not interpreted as zero change.",
            f"- exposure_attribution_status: `{attribution.get('attribution_status')}`",
            "- no broker / no production / no auto apply。",
            "",
        ]
    )


def _render_label_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    labels: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Label Review",
            "",
            f"- label_review_id: `{manifest.get('label_review_id')}`",
            f"- current_label: `{matrix.get('current_label')}`",
            f"- label_status: `{matrix.get('label_status')}`",
            f"- recommended_label: `{matrix.get('recommended_label')}`",
            f"- forward_distinct_events: {matrix.get('forward_distinct_event_count')}",
            f"- required_forward_distinct_events: "
            f"{matrix.get('minimum_forward_distinct_events_required')}",
            f"- auto_rename: `{matrix.get('auto_rename')}`",
            f"- owner_approval_required: `{matrix.get('owner_approval_required')}`",
            f"- reason: {matrix.get('reason')}",
            "",
            "## Candidate Labels",
            *[
                f"- `{row.get('label')}`: {row.get('description')} {row.get('risk', '')}"
                for row in _records(labels.get("labels"))
            ],
            "",
        ]
    )


def _render_label_brief(matrix: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Defensive Label Review",
            "",
            f"- current_label: `{matrix.get('current_label')}`",
            f"- label_status: `{matrix.get('label_status')}`",
            f"- recommended_label: `{matrix.get('recommended_label')}`",
            f"- auto_rename: `{matrix.get('auto_rename')}`",
            "- production_effect: `none`",
            "",
        ]
    )


def _render_failure_report(
    manifest: Mapping[str, Any],
    pattern_summary: Mapping[str, Any],
    ideas: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Failure Study",
            "",
            f"- failure_study_id: `{manifest.get('failure_study_id')}`",
            f"- deep_dive_id: `{manifest.get('deep_dive_id')}`",
            f"- failure_case_count: {manifest.get('failure_case_count')}",
            "",
            "## Failure Patterns",
            *[
                f"- {row.get('pattern')}: count={row.get('count')}, "
                f"avg_loss_vs_no_trade={row.get('avg_loss_vs_no_trade')}, "
                f"mitigation={row.get('mitigation')}"
                for row in _records(pattern_summary.get("patterns"))
            ],
            "",
            "## Mitigation Ideas",
            *[
                f"- `{row.get('idea_id')}`: {row.get('description')} "
                f"auto_apply=`{row.get('auto_apply')}`"
                for row in _records(ideas.get("ideas"))
            ],
            "",
            "- conclusion: research-only; forward confirmation is required.",
            "",
        ]
    )


def _render_research_note(
    summary: Mapping[str, Any],
    matrix: Mapping[str, Any],
    label_matrix: Mapping[str, Any],
    failure_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Hypothesis Review",
            "",
            "## 当前假设",
            "",
            f"- {summary.get('hypothesis')}",
            f"- current_status: `{summary.get('current_status')}`",
            "",
            "## pressure regime 证据",
            *[
                f"- {row.get('regime')}: `{row.get('effect_status')}`, "
                f"samples={row.get('sample_count')}, "
                f"avg_return_delta={row.get('avg_return_delta_vs_no_trade')}, "
                f"avg_drawdown_delta={row.get('avg_drawdown_delta_vs_no_trade')}"
                for row in _records(matrix.get("regimes"))
            ],
            "",
            "## 失败模式",
            *[
                f"- {row.get('pattern')}: count={row.get('count')}, "
                f"mitigation={row.get('mitigation')}"
                for row in _records(failure_summary.get("patterns"))
            ],
            "",
            "## forward / PIT 证据边界",
            "",
            f"- forward_support: `{summary.get('forward_support')}`",
            f"- pit_replay_support: `{summary.get('pit_replay_support')}`",
            "- 未满足forward证据要求时不能进入rule approval。",
            "",
            "## label 解释",
            "",
            f"- label_status: `{label_matrix.get('label_status')}`",
            f"- recommended_label: `{label_matrix.get('recommended_label')}`",
            "",
            "## owner 当前应如何理解",
            "",
            f"- recommended_action: `{summary.get('recommended_action')}`",
            f"- can_support_rule_approval: `{summary.get('can_support_rule_approval')}`",
            "- no broker / no production / no auto apply。",
            "",
        ]
    )


def _render_research_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Defensive Hypothesis Review",
            "",
            f"- current_status: `{summary.get('current_status')}`",
            f"- simulation_support: `{summary.get('simulation_support')}`",
            f"- forward_support: `{summary.get('forward_support')}`",
            f"- label_status: `{summary.get('label_status')}`",
            f"- recommended_action: `{summary.get('recommended_action')}`",
            f"- can_support_rule_approval: `{summary.get('can_support_rule_approval')}`",
            "",
        ]
    )


def _render_owner_checklist() -> str:
    return "\n".join(
        [
            "# Owner Defensive Hypothesis Decision Checklist",
            "",
            "1. 是否接受该variant仍为RESEARCH_ONLY？",
            "2. 是否接受当前名称只能与unproven-defense warning并存？",
            "3. 是否希望在更多forward evidence后重审命名？",
            "4. 是否优先收集validated forward pressure samples？",
            "5. 是否确认本pack不修改任何config/policy？",
            "6. 是否确认不触发broker/production？",
            "",
        ]
    )


def _render_owner_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    options: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Owner Pack",
            "",
            f"- pack_id: `{manifest.get('pack_id')}`",
            f"- note_id: `{manifest.get('note_id')}`",
            f"- current_status: `{summary.get('current_status')}`",
            f"- recommended_action: `{summary.get('recommended_action')}`",
            f"- auto_apply: `{options.get('auto_apply')}`",
            f"- policy_change_allowed: `{options.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{options.get('broker_action_allowed')}`",
            "",
            "## Research Options",
            *[
                f"- `{row.get('decision')}`: recommended=`{row.get('recommended')}`"
                for row in _records(options.get("options"))
            ],
            "",
        ]
    )


def _classify_case(return_delta: float, drawdown_delta: float) -> tuple[str, str, str]:
    if return_delta >= 0 and drawdown_delta >= 0:
        return "supporting", "return_and_drawdown_non_negative", ""
    if return_delta < 0 and drawdown_delta < 0:
        return "contradicting", "", "worse_return_and_worse_drawdown"
    return "mixed", "tradeoff", "single_dimension_tradeoff"


def _case_rows(inventory: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    identities: set[tuple[str, str, int]] = set()
    for row in inventory:
        if row.get("defensive_validation_relevant") is not True:
            continue
        _require(row.get("outcome_status") == "AVAILABLE", "deep dive source outcome unavailable")
        source_event_id = _text(row.get("source_event_id"))
        as_of = _text(row.get("as_of"))
        window_days = row.get("window_days")
        _require(
            bool(source_event_id)
            and bool(as_of)
            and isinstance(window_days, int)
            and not isinstance(window_days, bool)
            and window_days > 0,
            "deep dive source identity invalid",
        )
        identity = (source_event_id, as_of, window_days)
        _require(identity not in identities, "duplicate defensive case identity")
        identities.add(identity)
        variants = _mapping(row.get("variant_results"))
        defensive = _mapping(variants.get("defensive_limited_adjustment"))
        no_trade = _mapping(variants.get("no_trade"))
        _require(bool(defensive) and bool(no_trade), "defensive/no-trade pair missing")
        for variant_name, variant in (("defensive", defensive), ("no_trade", no_trade)):
            for field in ("return", "max_drawdown"):
                _require(_finite(variant.get(field)), f"{variant_name} {field} invalid")
        return_delta = float(defensive["return"]) - float(no_trade["return"])
        drawdown_delta = float(defensive["max_drawdown"]) - float(no_trade["max_drawdown"])
        defensive_turnover = _optional_float(defensive.get("turnover"))
        no_trade_turnover = _optional_float(no_trade.get("turnover"))
        turnover_delta = (
            defensive_turnover - no_trade_turnover
            if defensive_turnover is not None and no_trade_turnover is not None
            else None
        )
        defensive_exposure = _optional_float(defensive.get("risk_asset_exposure"))
        no_trade_exposure = _optional_float(no_trade.get("risk_asset_exposure"))
        exposure_delta = (
            defensive_exposure - no_trade_exposure
            if defensive_exposure is not None and no_trade_exposure is not None
            else None
        )
        classification, support_type, failure_type = _classify_case(return_delta, drawdown_delta)
        regime_tags = [_text(value) for value in row.get("regime_tags", []) if _text(value)]
        regime = next((tag for tag in PRESSURE_REGIMES if tag in regime_tags), "unknown")
        source_mode = _text(row.get("source_mode"))
        _require(source_mode in SOURCE_MODES, "deep dive source mode invalid")
        rows.append(
            {
                "case_id": _stable_id(
                    "defensive-hypothesis-case",
                    row.get("pressure_outcome_id"),
                    source_mode,
                    source_event_id,
                    as_of,
                    window_days,
                    regime,
                ),
                "source_mode": source_mode,
                "source_event_id": source_event_id,
                "regime": regime,
                "as_of": as_of,
                "window_days": window_days,
                "defensive_return_delta_vs_no_trade": round(return_delta, 6),
                "defensive_drawdown_delta_vs_no_trade": round(drawdown_delta, 6),
                "turnover_delta": round(turnover_delta, 6) if turnover_delta is not None else None,
                "risk_asset_exposure_delta": (
                    round(exposure_delta, 6) if exposure_delta is not None else None
                ),
                "support_type": support_type,
                "failure_type": failure_type,
                "classification": classification,
                "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE[source_mode],
                "can_support_rule_approval": False,
            }
        )
    return rows


def _case_output(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "case_id",
        "source_mode",
        "source_event_id",
        "regime",
        "as_of",
        "window_days",
        "defensive_return_delta_vs_no_trade",
        "defensive_drawdown_delta_vs_no_trade",
        "turnover_delta",
        "risk_asset_exposure_delta",
        "support_type",
        "failure_type",
        "evidence_quality",
        "can_support_rule_approval",
    )
    return {key: row.get(key) for key in keys}


def _regime_matrix(cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes: list[dict[str, Any]] = []
    for regime in PRESSURE_REGIMES:
        rows = [row for row in cases if row.get("regime") == regime]
        counts = Counter(_text(row.get("classification")) for row in rows)
        if not rows:
            status = "INSUFFICIENT_DATA"
        elif counts["supporting"] and not counts["contradicting"] and not counts["mixed"]:
            status = "SUPPORTS"
        elif counts["contradicting"] and not counts["supporting"] and not counts["mixed"]:
            status = "CONTRADICTS"
        else:
            status = "MIXED"
        regimes.append(
            {
                "regime": regime,
                "sample_count": len(rows),
                "distinct_event_count": len(
                    {
                        _text(row.get("source_event_id"))
                        for row in rows
                        if row.get("source_event_id")
                    }
                ),
                "supporting_cases": counts["supporting"],
                "contradicting_cases": counts["contradicting"],
                "mixed_cases": counts["mixed"],
                "avg_return_delta_vs_no_trade": _rounded_mean(
                    [row.get("defensive_return_delta_vs_no_trade") for row in rows]
                ),
                "avg_drawdown_delta_vs_no_trade": _rounded_mean(
                    [row.get("defensive_drawdown_delta_vs_no_trade") for row in rows]
                ),
                "effect_status": status,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_effect_matrix",
        "regimes": regimes,
        "missing_metrics_are_null": True,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _exposure_attribution(cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    exposure = [row.get("risk_asset_exposure_delta") for row in cases]
    turnover = [row.get("turnover_delta") for row in cases]
    available_exposure = [float(value) for value in exposure if _finite(value)]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_exposure_change_attribution",
        "case_count": len(cases),
        "turnover_available_count": sum(1 for value in turnover if _finite(value)),
        "exposure_available_count": len(available_exposure),
        "exposure_missing_count": len(exposure) - len(available_exposure),
        "avg_turnover_delta": _rounded_mean(turnover),
        "avg_risk_asset_exposure_delta": _rounded_mean(exposure),
        "positive_risk_asset_exposure_delta_count": sum(
            1 for value in available_exposure if value > 0
        ),
        "attribution_status": (
            "AVAILABLE" if available_exposure else "INSUFFICIENT_SOURCE_EXPOSURE_FIELDS"
        ),
        "interpretation": (
            "Missing source exposure fields remain null and are excluded from averages; "
            "they are not evidence of zero exposure change."
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _deep_dive_views(
    snapshot: Mapping[str, Any], *, deep_dive_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    backfill = _binding(snapshot, "pressure_backfill")
    comparison = _binding(snapshot, "defensive_compare")
    _assert_chronology(
        backfill,
        comparison,
        generated=_datetime(snapshot.get("generated_at"), field="deep dive generated_at"),
    )
    backfill_manifest = _bundle_json(backfill, "pressure_backfill_manifest.json")
    comparison_manifest = _bundle_json(comparison, "defensive_pressure_compare_manifest.json")
    comparison_snapshot = _bundle_json(comparison, "defensive_pressure_compare_input_snapshot.json")
    comparison_summary = _bundle_json(comparison, "defensive_pressure_summary.json")
    _require(
        backfill_manifest.get("pressure_backfill_id") == backfill.get("artifact_id"),
        "backfill identity binding invalid",
    )
    _require(
        comparison_manifest.get("comparison_id") == comparison.get("artifact_id"),
        "comparison identity binding invalid",
    )
    compare_sources = _records(comparison_snapshot.get("source_bindings"))
    compare_backfills = [
        row for row in compare_sources if row.get("source_kind") == "pressure_backfill"
    ]
    _require(
        len(compare_backfills) == 1
        and compare_backfills[0].get("artifact_id") == backfill.get("artifact_id"),
        "comparison does not derive from selected pressure backfill",
    )
    cases = _case_rows(_bundle_jsonl(backfill, "pressure_outcome_inventory.jsonl"))
    supporting = [_case_output(row) for row in cases if row.get("classification") == "supporting"]
    contradicting = [
        _case_output(row) for row in cases if row.get("classification") == "contradicting"
    ]
    mixed = [_case_output(row) for row in cases if row.get("classification") == "mixed"]
    matrix = _regime_matrix(cases)
    attribution = _exposure_attribution(cases)
    source_counts = Counter(_text(row.get("source_mode")) for row in cases)
    distinct_counts = {
        mode: len(
            {
                _text(row.get("source_event_id"))
                for row in cases
                if row.get("source_mode") == mode and row.get("source_event_id")
            }
        )
        for mode in SOURCE_MODES
    }
    generated = _datetime(snapshot.get("generated_at"), field="deep dive generated_at")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_hypothesis_deep_dive_manifest",
        "deep_dive_id": deep_dive_id,
        "pressure_backfill_id": backfill.get("artifact_id"),
        "comparison_id": comparison.get("artifact_id"),
        "source_pressure_backfill_id": backfill_manifest.get("pressure_backfill_id"),
        "source_comparison_id": comparison_manifest.get("comparison_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if cases else "PASS_WITH_WARNINGS",
        "market_regime": "ai_after_chatgpt",
        "source_mode_counts": {mode: source_counts.get(mode, 0) for mode in SOURCE_MODES},
        "source_mode_distinct_event_counts": distinct_counts,
        "supporting_case_count": len(supporting),
        "contradicting_case_count": len(contradicting),
        "mixed_or_empty_case_count": len(mixed),
        "defensive_pressure_status": comparison_summary.get("defensive_status"),
        "research_policy_version": _source_policy_version(snapshot),
        "deep_dive_input_snapshot_path": str(output_dir / "deep_dive_input_snapshot.json"),
        "deep_dive_manifest_path": str(output_dir / "deep_dive_manifest.json"),
        "supporting_cases_path": str(output_dir / "supporting_cases.jsonl"),
        "contradicting_cases_path": str(output_dir / "contradicting_cases.jsonl"),
        "mixed_cases_path": str(output_dir / "mixed_cases.jsonl"),
        "regime_effect_matrix_path": str(output_dir / "regime_effect_matrix.json"),
        "exposure_change_attribution_path": str(output_dir / "exposure_change_attribution.json"),
        "defensive_hypothesis_deep_dive_report_path": str(
            output_dir / "defensive_hypothesis_deep_dive_report.md"
        ),
        **_safety(),
    }
    report = _render_deep_dive_report(manifest, matrix, attribution, comparison_summary)
    views = {
        "deep_dive_input_snapshot.json": _json_bytes(snapshot),
        "deep_dive_manifest.json": _json_bytes(manifest),
        "supporting_cases.jsonl": _jsonl_bytes(supporting),
        "contradicting_cases.jsonl": _jsonl_bytes(contradicting),
        "mixed_cases.jsonl": _jsonl_bytes(mixed),
        "regime_effect_matrix.json": _json_bytes(matrix),
        "exposure_change_attribution.json": _json_bytes(attribution),
        "defensive_hypothesis_deep_dive_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "supporting_cases": supporting,
        "contradicting_cases": contradicting,
        "mixed_cases": mixed,
        "regime_effect_matrix": matrix,
        "exposure_change_attribution": attribution,
    }


def run_defensive_hypothesis_deep_dive(
    *,
    pressure_backfill_id: str,
    comparison_id: str,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    policy_path: Path = DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    backfill = _pressure_source_binding(
        source_kind="pressure_backfill",
        source_dir=backfill_dir / pressure_backfill_id,
        generated=generated,
        canonical_files=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_outcome_inventory.jsonl",
            "pressure_source_summary.json",
            "pressure_backfill_report.md",
        ),
        json_views=(
            "pressure_outcome_backfill_input_snapshot.json",
            "pressure_backfill_manifest.json",
            "pressure_source_summary.json",
        ),
        jsonl_views=("pressure_outcome_inventory.jsonl",),
    )
    comparison = _pressure_source_binding(
        source_kind="defensive_compare",
        source_dir=comparison_dir / comparison_id,
        generated=generated,
        canonical_files=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "pressure_variant_metrics.jsonl",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
            "defensive_pressure_compare_report.md",
        ),
        json_views=(
            "defensive_pressure_compare_input_snapshot.json",
            "defensive_pressure_compare_manifest.json",
            "defensive_pairwise_comparison.json",
            "defensive_pressure_summary.json",
        ),
        jsonl_views=("pressure_variant_metrics.jsonl",),
    )
    deep_dive_id = _stable_id(
        "defensive-hypothesis-deep-dive",
        pressure_backfill_id,
        comparison_id,
        generated.isoformat(),
    )
    artifact_dir = _unique_dir(output_dir / deep_dive_id)
    snapshot = {
        "schema_version": DEEP_DIVE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_hypothesis_deep_dive_input_snapshot",
        "deep_dive_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [backfill, comparison],
        "policy_bindings": [_policy_binding(policy_path)],
        "lineage": {
            "pressure_backfill_id": pressure_backfill_id,
            "comparison_id": comparison_id,
            "comparison_must_bind_same_backfill": True,
        },
        "calculation_contract": {
            "supporting": "return_delta>=0 and drawdown_delta>=0",
            "contradicting": "return_delta<0 and drawdown_delta<0",
            "otherwise": "mixed",
            "missing_metrics_are_null": True,
            "sample_unit": "distinct source_event_id/as_of/window_days",
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _deep_dive_views(
        snapshot, deep_dive_id=artifact_dir.name, output_dir=artifact_dir
    )
    _write_views(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_hypothesis_deep_dive",
        artifact_dir.name,
        artifact_dir / "deep_dive_manifest.json",
    )
    return {
        "deep_dive_id": artifact_dir.name,
        "deep_dive_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_hypothesis_deep_dive_report_payload(
    *,
    deep_dive_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=deep_dive_id if not latest else None,
        pointer_name="latest_defensive_hypothesis_deep_dive",
    )
    return {
        **_read_json(artifact_dir / "deep_dive_manifest.json"),
        **_report_input_snapshot(artifact_dir / "deep_dive_input_snapshot.json"),
        "supporting_cases": _read_jsonl(artifact_dir / "supporting_cases.jsonl"),
        "contradicting_cases": _read_jsonl(artifact_dir / "contradicting_cases.jsonl"),
        "mixed_cases": _read_jsonl(artifact_dir / "mixed_cases.jsonl"),
        "regime_effect_matrix": _read_json(artifact_dir / "regime_effect_matrix.json"),
        "exposure_change_attribution": _read_json(
            artifact_dir / "exposure_change_attribution.json"
        ),
        "deep_dive_dir": str(artifact_dir),
    }


def _validate_recomputed_artifact(
    *,
    artifact_id: str,
    id_key: str,
    output_dir: Path,
    manifest_name: str,
    snapshot_name: str,
    snapshot_schema: str,
    view_builder: Callable[..., tuple[dict[str, bytes], dict[str, Any]]],
    report_type: str,
) -> dict[str, Any]:
    artifact_dir = output_dir / artifact_id
    manifest = _read_optional_json(artifact_dir / manifest_name) or {}
    snapshot = _read_optional_json(artifact_dir / snapshot_name) or {}
    checks = [
        _check("snapshot_exists", (artifact_dir / snapshot_name).is_file(), snapshot_name),
        _check("artifact_id_matches", manifest.get(id_key) == artifact_id, id_key),
        _check(
            "safety_research_only",
            manifest.get("can_support_rule_approval") is False
            and manifest.get("auto_apply") is False
            and manifest.get("policy_change_allowed") is False
            and manifest.get("broker_action_allowed") is False
            and manifest.get("production_effect") == "none",
            "research-only safety boundary",
        ),
    ]
    if snapshot:
        try:
            generated = _datetime(snapshot.get("generated_at"), field=f"{id_key} generated_at")
            live_errors = _validate_snapshot_live(
                snapshot, expected_schema=snapshot_schema, generated=generated
            )
            checks.append(
                _check("live_sources_and_policy", not live_errors, "; ".join(live_errors))
            )
            expected, _ = view_builder(snapshot, **{id_key: artifact_id}, output_dir=artifact_dir)
            mismatches = _view_mismatches(artifact_dir, expected)
            checks.append(
                _check("content_derived_views", not mismatches, f"mismatched files: {mismatches}")
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(_check("content_derived_views", False, str(exc)))
    else:
        checks.append(_check("content_derived_views", False, "versioned snapshot missing"))
    return _validation_payload(
        report_type=report_type,
        artifact_id_key=id_key,
        artifact_id=artifact_id,
        checks=checks,
    )


def validate_defensive_hypothesis_deep_dive_artifact(
    *,
    deep_dive_id: str,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=deep_dive_id,
        id_key="deep_dive_id",
        output_dir=output_dir,
        manifest_name="deep_dive_manifest.json",
        snapshot_name="deep_dive_input_snapshot.json",
        snapshot_schema=DEEP_DIVE_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_deep_dive_views,
        report_type="etf_dynamic_v3_defensive_hypothesis_deep_dive_validation",
    )


_DEEP_DIVE_CANONICAL_FILES = (
    "deep_dive_input_snapshot.json",
    "deep_dive_manifest.json",
    "supporting_cases.jsonl",
    "contradicting_cases.jsonl",
    "mixed_cases.jsonl",
    "regime_effect_matrix.json",
    "exposure_change_attribution.json",
    "defensive_hypothesis_deep_dive_report.md",
)


def _deep_dive_binding(
    *, deep_dive_id: str, deep_dive_dir: Path, generated: datetime
) -> dict[str, Any]:
    return _local_source_binding(
        source_kind="defensive_deep_dive",
        source_dir=deep_dive_dir / deep_dive_id,
        generated=generated,
        canonical_files=_DEEP_DIVE_CANONICAL_FILES,
        json_views=(
            "deep_dive_input_snapshot.json",
            "deep_dive_manifest.json",
            "regime_effect_matrix.json",
            "exposure_change_attribution.json",
        ),
        jsonl_views=("supporting_cases.jsonl", "contradicting_cases.jsonl", "mixed_cases.jsonl"),
    )


def _label_review_views(
    snapshot: Mapping[str, Any], *, label_review_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    deep = _binding(snapshot, "defensive_deep_dive")
    generated = _datetime(snapshot.get("generated_at"), field="label review generated_at")
    _assert_chronology(deep, generated=generated)
    deep_manifest = _bundle_json(deep, "deep_dive_manifest.json")
    matrix = _bundle_json(deep, "regime_effect_matrix.json")
    policy = _policy(snapshot)
    label_policy = _mapping(policy.get("label_review"))
    distinct_counts = _mapping(deep_manifest.get("source_mode_distinct_event_counts"))
    forward_count = distinct_counts.get("FORWARD_OUTCOME")
    _require(
        isinstance(forward_count, int)
        and not isinstance(forward_count, bool)
        and forward_count >= 0,
        "deep dive forward distinct event count invalid",
    )
    floor = label_policy.get("minimum_forward_distinct_events_for_acceptable_label")
    mixed_regime = any(
        row.get("effect_status") == "MIXED" for row in _records(matrix.get("regimes"))
    )
    contradictory = int(deep_manifest.get("contradicting_case_count", 0))
    mixed_cases = int(deep_manifest.get("mixed_or_empty_case_count", 0))
    acceptable = (
        forward_count >= floor and contradictory == 0 and mixed_cases == 0 and not mixed_regime
    )
    label_matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_label_decision_matrix",
        "current_label": label_policy.get("current_label"),
        "label_status": "ACCEPTABLE_WITH_WARNING" if acceptable else "POTENTIALLY_MISLEADING",
        "recommended_label": (
            label_policy.get("current_label")
            if acceptable
            else label_policy.get("recommended_label")
        ),
        "forward_distinct_event_count": forward_count,
        "minimum_forward_distinct_events_required": floor,
        "contradicting_case_count": contradictory,
        "mixed_case_count": mixed_cases,
        "reason": (
            "Validated forward evidence meets the naming floor, but owner review remains required."
            if acceptable
            else "Current evidence does not establish consistent forward defensive behavior."
        ),
        "warning": (
            "The current defensive label can imply unproven protection; reports remain "
            "research-only."
        ),
        "auto_rename": False,
        "owner_approval_required": True,
        "config_change_allowed": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    candidate_labels = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_labels",
        "recommended_label": label_matrix["recommended_label"],
        "labels": [dict(row) for row in _records(label_policy.get("candidate_labels"))],
        "research_policy_version": _source_policy_version(snapshot),
        "auto_rename": False,
        "production_effect": "none",
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_label_review_manifest",
        "label_review_id": label_review_id,
        "deep_dive_id": deep.get("artifact_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "research_policy_version": _source_policy_version(snapshot),
        "label_review_input_snapshot_path": str(output_dir / "label_review_input_snapshot.json"),
        "label_review_manifest_path": str(output_dir / "label_review_manifest.json"),
        "label_decision_matrix_path": str(output_dir / "label_decision_matrix.json"),
        "candidate_labels_path": str(output_dir / "candidate_labels.json"),
        "defensive_label_review_report_path": str(output_dir / "defensive_label_review_report.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **_safety(),
    }
    report = _render_label_report(manifest, label_matrix, candidate_labels)
    brief = _render_label_brief(label_matrix)
    views = {
        "label_review_input_snapshot.json": _json_bytes(snapshot),
        "label_review_manifest.json": _json_bytes(manifest),
        "label_decision_matrix.json": _json_bytes(label_matrix),
        "candidate_labels.json": _json_bytes(candidate_labels),
        "defensive_label_review_report.md": _text_bytes(report),
        "reader_brief_section.md": _text_bytes(brief),
    }
    return views, {
        "manifest": manifest,
        "label_decision_matrix": label_matrix,
        "candidate_labels": candidate_labels,
        "reader_brief_section": brief,
    }


def run_defensive_label_review(
    *,
    deep_dive_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
    policy_path: Path = DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    deep = _deep_dive_binding(
        deep_dive_id=deep_dive_id, deep_dive_dir=deep_dive_dir, generated=generated
    )
    label_review_id = _stable_id("defensive-label-review", deep_dive_id, generated.isoformat())
    artifact_dir = _unique_dir(output_dir / label_review_id)
    snapshot = {
        "schema_version": LABEL_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_label_review_input_snapshot",
        "label_review_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [deep],
        "policy_bindings": [_policy_binding(policy_path)],
        "lineage": {"deep_dive_id": deep_dive_id},
        "calculation_contract": {
            "acceptable_requires_forward_floor": True,
            "contradicting_or_mixed_evidence_is_potentially_misleading": True,
            "owner_approval_required": True,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _label_review_views(
        snapshot, label_review_id=artifact_dir.name, output_dir=artifact_dir
    )
    _write_views(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_label_review",
        artifact_dir.name,
        artifact_dir / "label_review_manifest.json",
    )
    return {
        "label_review_id": artifact_dir.name,
        "label_review_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_label_review_report_payload(
    *,
    label_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=label_review_id if not latest else None,
        pointer_name="latest_defensive_label_review",
    )
    return {
        **_read_json(artifact_dir / "label_review_manifest.json"),
        **_report_input_snapshot(artifact_dir / "label_review_input_snapshot.json"),
        "label_decision_matrix": _read_json(artifact_dir / "label_decision_matrix.json"),
        "candidate_labels": _read_json(artifact_dir / "candidate_labels.json"),
        "reader_brief_section": _read_text(artifact_dir / "reader_brief_section.md"),
        "label_review_dir": str(artifact_dir),
    }


def validate_defensive_label_review_artifact(
    *, label_review_id: str, output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=label_review_id,
        id_key="label_review_id",
        output_dir=output_dir,
        manifest_name="label_review_manifest.json",
        snapshot_name="label_review_input_snapshot.json",
        snapshot_schema=LABEL_REVIEW_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_label_review_views,
        report_type="etf_dynamic_v3_defensive_label_review_validation",
    )


def _failure_reason(row: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    rules = _mapping(policy.get("failure_reason_rules"))
    regimes = {_text(value) for value in rules.get("risk_increase_regimes", []) if _text(value)}
    exposure = row.get("risk_asset_exposure_delta")
    turnover = row.get("turnover_delta")
    if _finite(exposure) and float(exposure) > 0 and row.get("regime") in regimes:
        return _text(rules.get("positive_exposure_reason"))
    if _finite(turnover) and float(turnover) > 0:
        return _text(rules.get("positive_turnover_reason"))
    return _text(rules.get("otherwise_reason"))


def _rank_failures(
    contradicting: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    severity = _mapping(policy.get("failure_severity"))
    high = float(severity["high_score_min"])
    medium = float(severity["medium_score_min"])
    ranked: list[dict[str, Any]] = []
    for row in contradicting:
        return_delta = row.get("defensive_return_delta_vs_no_trade")
        drawdown_delta = row.get("defensive_drawdown_delta_vs_no_trade")
        _require(_finite(return_delta) and _finite(drawdown_delta), "failure delta invalid")
        score = abs(min(float(return_delta), 0.0)) + abs(min(float(drawdown_delta), 0.0))
        band = "HIGH" if score >= high else "MEDIUM" if score >= medium else "LOW"
        ranked.append(
            {
                "case_id": row.get("case_id"),
                "source_event_id": row.get("source_event_id"),
                "regime": row.get("regime"),
                "as_of": row.get("as_of"),
                "window_days": row.get("window_days"),
                "relative_return_vs_no_trade": return_delta,
                "drawdown_delta_vs_no_trade": drawdown_delta,
                "turnover": row.get("turnover_delta"),
                "risk_asset_exposure_delta": row.get("risk_asset_exposure_delta"),
                "failure_severity": band,
                "likely_failure_reason": _failure_reason(row, policy),
                "failure_score": round(score, 6),
            }
        )
    return sorted(ranked, key=lambda item: (-float(item["failure_score"]), _text(item["case_id"])))


def _failure_patterns(
    ranked: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    rules = _mapping(policy.get("failure_reason_rules"))
    ordered = [
        _text(rules.get("positive_exposure_reason")),
        _text(rules.get("positive_turnover_reason")),
        _text(rules.get("otherwise_reason")),
    ]
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in ranked:
        grouped[_text(row.get("likely_failure_reason"))].append(row)
    mitigation_by_reason = {
        ordered[0]: "review risk-increase guard with forward confirmation",
        ordered[1]: "review timing guard with forward confirmation",
        ordered[2]: "collect more forward evidence before causal interpretation",
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_failure_pattern_summary",
        "patterns": [
            {
                "pattern": reason,
                "count": len(grouped[reason]),
                "avg_loss_vs_no_trade": _rounded_mean(
                    [row.get("relative_return_vs_no_trade") for row in grouped[reason]]
                ),
                "mitigation": mitigation_by_reason[reason],
            }
            for reason in ordered
        ],
        "missing_metrics_are_null": True,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _mitigation_ideas(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_failure_mitigation_ideas",
        "ideas": [
            {**dict(row), "auto_apply": False} for row in _records(policy.get("mitigation_ideas"))
        ],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _failure_study_views(
    snapshot: Mapping[str, Any], *, failure_study_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    deep = _binding(snapshot, "defensive_deep_dive")
    generated = _datetime(snapshot.get("generated_at"), field="failure study generated_at")
    _assert_chronology(deep, generated=generated)
    policy = _policy(snapshot)
    ranked = _rank_failures(_bundle_jsonl(deep, "contradicting_cases.jsonl"), policy)
    patterns = _failure_patterns(ranked, policy)
    ideas = _mitigation_ideas(policy)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_failure_study_manifest",
        "failure_study_id": failure_study_id,
        "deep_dive_id": deep.get("artifact_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if ranked else "PASS_WITH_WARNINGS",
        "market_regime": "ai_after_chatgpt",
        "failure_case_count": len(ranked),
        "research_policy_version": _source_policy_version(snapshot),
        "failure_study_input_snapshot_path": str(output_dir / "failure_study_input_snapshot.json"),
        "failure_study_manifest_path": str(output_dir / "failure_study_manifest.json"),
        "failure_cases_ranked_path": str(output_dir / "failure_cases_ranked.jsonl"),
        "failure_pattern_summary_path": str(output_dir / "failure_pattern_summary.json"),
        "failure_mitigation_ideas_path": str(output_dir / "failure_mitigation_ideas.json"),
        "defensive_failure_study_report_path": str(
            output_dir / "defensive_failure_study_report.md"
        ),
        **_safety(),
    }
    report = _render_failure_report(manifest, patterns, ideas)
    views = {
        "failure_study_input_snapshot.json": _json_bytes(snapshot),
        "failure_study_manifest.json": _json_bytes(manifest),
        "failure_cases_ranked.jsonl": _jsonl_bytes(ranked),
        "failure_pattern_summary.json": _json_bytes(patterns),
        "failure_mitigation_ideas.json": _json_bytes(ideas),
        "defensive_failure_study_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "failure_cases_ranked": ranked,
        "failure_pattern_summary": patterns,
        "failure_mitigation_ideas": ideas,
    }


def run_defensive_failure_study(
    *,
    deep_dive_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
    policy_path: Path = DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    deep = _deep_dive_binding(
        deep_dive_id=deep_dive_id, deep_dive_dir=deep_dive_dir, generated=generated
    )
    failure_study_id = _stable_id("defensive-failure-study", deep_dive_id, generated.isoformat())
    artifact_dir = _unique_dir(output_dir / failure_study_id)
    snapshot = {
        "schema_version": FAILURE_STUDY_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_failure_study_input_snapshot",
        "failure_study_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [deep],
        "policy_bindings": [_policy_binding(policy_path)],
        "lineage": {"deep_dive_id": deep_dive_id},
        "calculation_contract": {
            "failure_score": "abs(min(return_delta,0))+abs(min(drawdown_delta,0))",
            "severity_and_reason_rules_from_policy": True,
            "missing_metrics_are_null": True,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _failure_study_views(
        snapshot, failure_study_id=artifact_dir.name, output_dir=artifact_dir
    )
    _write_views(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_failure_study",
        artifact_dir.name,
        artifact_dir / "failure_study_manifest.json",
    )
    return {
        "failure_study_id": artifact_dir.name,
        "failure_study_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_failure_study_report_payload(
    *,
    failure_study_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=failure_study_id if not latest else None,
        pointer_name="latest_defensive_failure_study",
    )
    return {
        **_read_json(artifact_dir / "failure_study_manifest.json"),
        **_report_input_snapshot(artifact_dir / "failure_study_input_snapshot.json"),
        "failure_cases_ranked": _read_jsonl(artifact_dir / "failure_cases_ranked.jsonl"),
        "failure_pattern_summary": _read_json(artifact_dir / "failure_pattern_summary.json"),
        "failure_mitigation_ideas": _read_json(artifact_dir / "failure_mitigation_ideas.json"),
        "failure_study_dir": str(artifact_dir),
    }


def validate_defensive_failure_study_artifact(
    *, failure_study_id: str, output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=failure_study_id,
        id_key="failure_study_id",
        output_dir=output_dir,
        manifest_name="failure_study_manifest.json",
        snapshot_name="failure_study_input_snapshot.json",
        snapshot_schema=FAILURE_STUDY_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_failure_study_views,
        report_type="etf_dynamic_v3_defensive_failure_study_validation",
    )


_LABEL_REVIEW_CANONICAL_FILES = (
    "label_review_input_snapshot.json",
    "label_review_manifest.json",
    "label_decision_matrix.json",
    "candidate_labels.json",
    "defensive_label_review_report.md",
    "reader_brief_section.md",
)
_FAILURE_STUDY_CANONICAL_FILES = (
    "failure_study_input_snapshot.json",
    "failure_study_manifest.json",
    "failure_cases_ranked.jsonl",
    "failure_pattern_summary.json",
    "failure_mitigation_ideas.json",
    "defensive_failure_study_report.md",
)


def _label_binding(
    *, label_review_id: str, label_review_dir: Path, generated: datetime
) -> dict[str, Any]:
    return _local_source_binding(
        source_kind="defensive_label_review",
        source_dir=label_review_dir / label_review_id,
        generated=generated,
        canonical_files=_LABEL_REVIEW_CANONICAL_FILES,
        json_views=(
            "label_review_input_snapshot.json",
            "label_review_manifest.json",
            "label_decision_matrix.json",
            "candidate_labels.json",
        ),
        text_views=("reader_brief_section.md",),
    )


def _failure_binding(
    *, failure_study_id: str, failure_study_dir: Path, generated: datetime
) -> dict[str, Any]:
    return _local_source_binding(
        source_kind="defensive_failure_study",
        source_dir=failure_study_dir / failure_study_id,
        generated=generated,
        canonical_files=_FAILURE_STUDY_CANONICAL_FILES,
        json_views=(
            "failure_study_input_snapshot.json",
            "failure_study_manifest.json",
            "failure_pattern_summary.json",
            "failure_mitigation_ideas.json",
        ),
        jsonl_views=("failure_cases_ranked.jsonl",),
    )


def _nested_source_id(source_snapshot: Mapping[str, Any], kind: str) -> str:
    matches = [
        row
        for row in _records(source_snapshot.get("source_bindings"))
        if row.get("source_kind") == kind
    ]
    _require(len(matches) == 1, f"nested lineage source invalid: {kind}")
    return _text(matches[0].get("artifact_id"))


def _hypothesis_summary(
    deep_manifest: Mapping[str, Any], label_matrix: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    distinct = _mapping(deep_manifest.get("source_mode_distinct_event_counts"))
    simulation_count = distinct.get("BACKTEST_SIMULATION")
    forward_count = distinct.get("FORWARD_OUTCOME")
    replay_count = distinct.get("HISTORICAL_REPLAY")
    _require(
        all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in (simulation_count, forward_count, replay_count)
        ),
        "deep dive distinct event counts invalid",
    )
    floor = _mapping(policy.get("label_review")).get(
        "minimum_forward_distinct_events_for_acceptable_label"
    )
    if forward_count == 0:
        forward_support = "NONE"
    elif forward_count < floor:
        forward_support = "INSUFFICIENT"
    else:
        forward_support = "EVIDENCE_PRESENT_OWNER_REVIEW_REQUIRED"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_hypothesis_summary",
        "hypothesis": (
            "defensive_limited_adjustment may help in selected pressure regimes, but "
            "the synthesis remains research-only."
        ),
        "current_status": "RESEARCH_ONLY",
        "simulation_support": "PARTIAL" if simulation_count > 0 else "NONE",
        "forward_support": forward_support,
        "pit_replay_support": "PIT_WARNING" if replay_count > 0 else "NONE",
        "simulation_distinct_event_count": simulation_count,
        "forward_distinct_event_count": forward_count,
        "historical_replay_distinct_event_count": replay_count,
        "label_status": label_matrix.get("label_status"),
        "recommended_action": "continue_tracking_and_consider_rename",
        "can_support_rule_approval": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _research_note_views(
    snapshot: Mapping[str, Any], *, note_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    deep = _binding(snapshot, "defensive_deep_dive")
    label = _binding(snapshot, "defensive_label_review")
    failure = _binding(snapshot, "defensive_failure_study")
    generated = _datetime(snapshot.get("generated_at"), field="research note generated_at")
    _assert_chronology(deep, label, failure, generated=generated)
    deep_time = _source_generated(deep)
    _require(
        deep_time <= _source_generated(label) and deep_time <= _source_generated(failure),
        "research note source chronology invalid",
    )
    deep_id = _text(deep.get("artifact_id"))
    label_manifest = _bundle_json(label, "label_review_manifest.json")
    failure_manifest = _bundle_json(failure, "failure_study_manifest.json")
    _require(
        label_manifest.get("deep_dive_id") == deep_id
        and failure_manifest.get("deep_dive_id") == deep_id,
        "research note cross-lineage source",
    )
    _require(
        _nested_source_id(
            _bundle_json(label, "label_review_input_snapshot.json"),
            "defensive_deep_dive",
        )
        == deep_id
        and _nested_source_id(
            _bundle_json(failure, "failure_study_input_snapshot.json"),
            "defensive_deep_dive",
        )
        == deep_id,
        "research note nested lineage mismatch",
    )
    deep_manifest = _bundle_json(deep, "deep_dive_manifest.json")
    regime_matrix = _bundle_json(deep, "regime_effect_matrix.json")
    label_matrix = _bundle_json(label, "label_decision_matrix.json")
    failure_summary = _bundle_json(failure, "failure_pattern_summary.json")
    summary = _hypothesis_summary(deep_manifest, label_matrix, _policy(snapshot))
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_research_note_manifest",
        "note_id": note_id,
        "deep_dive_id": deep_id,
        "label_review_id": label.get("artifact_id"),
        "failure_study_id": failure.get("artifact_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "research_policy_version": _source_policy_version(snapshot),
        "defensive_research_note_input_snapshot_path": str(
            output_dir / "defensive_research_note_input_snapshot.json"
        ),
        "defensive_research_note_manifest_path": str(
            output_dir / "defensive_research_note_manifest.json"
        ),
        "defensive_hypothesis_summary_path": str(output_dir / "defensive_hypothesis_summary.json"),
        "defensive_research_note_path": str(output_dir / "defensive_research_note.md"),
        "reader_brief_section_path": str(output_dir / "reader_brief_section.md"),
        **_safety(),
    }
    note = _render_research_note(summary, regime_matrix, label_matrix, failure_summary)
    brief = _render_research_brief(summary)
    views = {
        "defensive_research_note_input_snapshot.json": _json_bytes(snapshot),
        "defensive_research_note_manifest.json": _json_bytes(manifest),
        "defensive_hypothesis_summary.json": _json_bytes(summary),
        "defensive_research_note.md": _text_bytes(note),
        "reader_brief_section.md": _text_bytes(brief),
    }
    return views, {
        "manifest": manifest,
        "defensive_hypothesis_summary": summary,
        "reader_brief_section": brief,
    }


def run_defensive_research_note(
    *,
    deep_dive_id: str,
    label_review_id: str,
    failure_study_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    label_review_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
    failure_study_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
    policy_path: Path = DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    deep = _deep_dive_binding(
        deep_dive_id=deep_dive_id, deep_dive_dir=deep_dive_dir, generated=generated
    )
    label = _label_binding(
        label_review_id=label_review_id,
        label_review_dir=label_review_dir,
        generated=generated,
    )
    failure = _failure_binding(
        failure_study_id=failure_study_id,
        failure_study_dir=failure_study_dir,
        generated=generated,
    )
    note_id = _stable_id(
        "defensive-research-note",
        deep_dive_id,
        label_review_id,
        failure_study_id,
        generated.isoformat(),
    )
    artifact_dir = _unique_dir(output_dir / note_id)
    snapshot = {
        "schema_version": RESEARCH_NOTE_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_research_note_input_snapshot",
        "note_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [deep, label, failure],
        "policy_bindings": [_policy_binding(policy_path)],
        "lineage": {
            "deep_dive_id": deep_dive_id,
            "label_review_id": label_review_id,
            "failure_study_id": failure_study_id,
            "label_and_failure_must_bind_same_deep_dive": True,
        },
        "calculation_contract": {
            "event_counts_are_distinct_source_events": True,
            "simulation_and_historical_evidence_remain_research_only": True,
            "rule_approval_is_always_false": True,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _research_note_views(
        snapshot, note_id=artifact_dir.name, output_dir=artifact_dir
    )
    _write_views(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_research_note",
        artifact_dir.name,
        artifact_dir / "defensive_research_note_manifest.json",
    )
    return {
        "note_id": artifact_dir.name,
        "note_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_research_note_report_payload(
    *,
    note_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=note_id if not latest else None,
        pointer_name="latest_defensive_research_note",
    )
    return {
        **_read_json(artifact_dir / "defensive_research_note_manifest.json"),
        **_report_input_snapshot(artifact_dir / "defensive_research_note_input_snapshot.json"),
        "defensive_hypothesis_summary": _read_json(
            artifact_dir / "defensive_hypothesis_summary.json"
        ),
        "defensive_research_note": _read_text(artifact_dir / "defensive_research_note.md"),
        "reader_brief_section": _read_text(artifact_dir / "reader_brief_section.md"),
        "note_dir": str(artifact_dir),
    }


def validate_defensive_research_note_artifact(
    *, note_id: str, output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=note_id,
        id_key="note_id",
        output_dir=output_dir,
        manifest_name="defensive_research_note_manifest.json",
        snapshot_name="defensive_research_note_input_snapshot.json",
        snapshot_schema=RESEARCH_NOTE_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_research_note_views,
        report_type="etf_dynamic_v3_defensive_research_note_validation",
    )


_RESEARCH_NOTE_CANONICAL_FILES = (
    "defensive_research_note_input_snapshot.json",
    "defensive_research_note_manifest.json",
    "defensive_hypothesis_summary.json",
    "defensive_research_note.md",
    "reader_brief_section.md",
)


def _research_note_binding(*, note_id: str, note_dir: Path, generated: datetime) -> dict[str, Any]:
    return _local_source_binding(
        source_kind="defensive_research_note",
        source_dir=note_dir / note_id,
        generated=generated,
        canonical_files=_RESEARCH_NOTE_CANONICAL_FILES,
        json_views=(
            "defensive_research_note_input_snapshot.json",
            "defensive_research_note_manifest.json",
            "defensive_hypothesis_summary.json",
        ),
        text_views=("defensive_research_note.md", "reader_brief_section.md"),
    )


def _owner_options(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_decision_options",
        "options": [dict(row) for row in _records(policy.get("owner_options"))],
        "auto_apply": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _owner_pack_views(
    snapshot: Mapping[str, Any], *, pack_id: str, output_dir: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    note = _binding(snapshot, "defensive_research_note")
    generated = _datetime(snapshot.get("generated_at"), field="owner pack generated_at")
    _assert_chronology(note, generated=generated)
    note_manifest = _bundle_json(note, "defensive_research_note_manifest.json")
    note_snapshot = _bundle_json(note, "defensive_research_note_input_snapshot.json")
    summary = _bundle_json(note, "defensive_hypothesis_summary.json")
    _require(
        note_manifest.get("note_id") == note.get("artifact_id"), "owner pack note identity mismatch"
    )
    _require(
        note_snapshot.get("note_id") == note.get("artifact_id")
        and len(_records(note_snapshot.get("source_bindings"))) == 3,
        "owner pack inherited lineage invalid",
    )
    _require(
        summary.get("can_support_rule_approval") is False
        and summary.get("current_status") == "RESEARCH_ONLY",
        "owner pack source safety invalid",
    )
    options = _owner_options(_policy(snapshot))
    checklist = _render_owner_checklist()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_owner_pack_manifest",
        "pack_id": pack_id,
        "note_id": note.get("artifact_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "research_policy_version": _source_policy_version(snapshot),
        "owner_pack_input_snapshot_path": str(output_dir / "owner_pack_input_snapshot.json"),
        "defensive_owner_pack_manifest_path": str(
            output_dir / "defensive_owner_pack_manifest.json"
        ),
        "owner_decision_options_path": str(output_dir / "owner_decision_options.json"),
        "owner_decision_checklist_path": str(output_dir / "owner_decision_checklist.md"),
        "defensive_owner_pack_report_path": str(output_dir / "defensive_owner_pack_report.md"),
        **_safety(),
    }
    report = _render_owner_report(manifest, summary, options)
    views = {
        "owner_pack_input_snapshot.json": _json_bytes(snapshot),
        "defensive_owner_pack_manifest.json": _json_bytes(manifest),
        "owner_decision_options.json": _json_bytes(options),
        "owner_decision_checklist.md": _text_bytes(checklist),
        "defensive_owner_pack_report.md": _text_bytes(report),
    }
    return views, {
        "manifest": manifest,
        "owner_decision_options": options,
        "owner_decision_checklist": checklist,
    }


def run_defensive_owner_pack(
    *,
    note_id: str,
    note_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
    policy_path: Path = DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    note = _research_note_binding(note_id=note_id, note_dir=note_dir, generated=generated)
    pack_id = _stable_id("defensive-owner-pack", note_id, generated.isoformat())
    artifact_dir = _unique_dir(output_dir / pack_id)
    snapshot = {
        "schema_version": OWNER_PACK_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_owner_pack_input_snapshot",
        "pack_id": artifact_dir.name,
        "generated_at": generated.isoformat(),
        "source_bindings": [note],
        "policy_bindings": [_policy_binding(policy_path)],
        "lineage": {"note_id": note_id, "inherit_complete_note_lineage": True},
        "decision_boundary": {
            "options_are_research_proposals_only": True,
            "owner_record_or_execution_performed": False,
            "rule_approval_allowed": False,
        },
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    views, payload = _owner_pack_views(snapshot, pack_id=artifact_dir.name, output_dir=artifact_dir)
    _write_views(artifact_dir, views)
    _update_latest_pointer(
        "latest_defensive_owner_pack",
        artifact_dir.name,
        artifact_dir / "defensive_owner_pack_manifest.json",
    )
    return {
        "pack_id": artifact_dir.name,
        "pack_dir": artifact_dir,
        "input_snapshot": snapshot,
        **payload,
    }


def defensive_owner_pack_report_payload(
    *,
    pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
) -> dict[str, Any]:
    artifact_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=pack_id if not latest else None,
        pointer_name="latest_defensive_owner_pack",
    )
    return {
        **_read_json(artifact_dir / "defensive_owner_pack_manifest.json"),
        **_report_input_snapshot(artifact_dir / "owner_pack_input_snapshot.json"),
        "owner_decision_options": _read_json(artifact_dir / "owner_decision_options.json"),
        "owner_decision_checklist": _read_text(artifact_dir / "owner_decision_checklist.md"),
        "defensive_owner_pack_report": _read_text(artifact_dir / "defensive_owner_pack_report.md"),
        "pack_dir": str(artifact_dir),
    }


def validate_defensive_owner_pack_artifact(
    *, pack_id: str, output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR
) -> dict[str, Any]:
    return _validate_recomputed_artifact(
        artifact_id=pack_id,
        id_key="pack_id",
        output_dir=output_dir,
        manifest_name="defensive_owner_pack_manifest.json",
        snapshot_name="owner_pack_input_snapshot.json",
        snapshot_schema=OWNER_PACK_SNAPSHOT_SCHEMA_VERSION,
        view_builder=_owner_pack_views,
        report_type="etf_dynamic_v3_defensive_owner_pack_validation",
    )
