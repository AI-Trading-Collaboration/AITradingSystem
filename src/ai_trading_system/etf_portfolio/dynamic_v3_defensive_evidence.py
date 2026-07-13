from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    run_pressure_regime_tagging,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_RATES_CACHE_PATH,
    _artifact_dir_from_latest,
    _check,
    _date_from_any,
    _float,
    _int,
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
    _write_json,
    _write_jsonl,
    _write_text,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    EVIDENCE_QUALITY_BY_SOURCE,
    PRESSURE_REGIMES,
    SOURCE_MODES,
    run_defensive_pressure_compare,
    run_pressure_outcome_backfill,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH

DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "forward_pressure_capture_v1.yaml"
)
DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_hypothesis_deep_dive"
)
DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_label_review"
)
DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_failure_study"
)
DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_research_note"
)
DEFAULT_DEFENSIVE_OWNER_PACK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_owner_pack"
DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "forward_pressure_capture"
)
DEFAULT_PRESSURE_TRIGGER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_trigger"
DEFAULT_PRESSURE_CAPTURE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_capture"
DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_sample_ledger"
)
DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weekly_defensive_evidence"
)

AI_AFTER_CHATGPT_START = date(2022, 12, 1)
REQUIRED_FORWARD_PRESSURE_SAMPLES_DEFAULT = 5

# Diagnostic display bands only. They rank research failure cases and do not approve,
# reject, size, or route any investment action.
FAILURE_SEVERITY_HIGH_SCORE = 0.02
FAILURE_SEVERITY_MEDIUM_SCORE = 0.005


class DynamicV3DefensiveEvidenceError(ValueError):
    """Raised when defensive evidence artifacts fail closed."""


def run_defensive_hypothesis_deep_dive(
    *,
    pressure_backfill_id: str,
    comparison_id: str,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_backfill_dir = backfill_dir / pressure_backfill_id
    source_comparison_dir = comparison_dir / comparison_id
    backfill_manifest = _read_json(source_backfill_dir / "pressure_backfill_manifest.json")
    inventory = _read_jsonl(source_backfill_dir / "pressure_outcome_inventory.jsonl")
    comparison_manifest = _read_json(
        source_comparison_dir / "defensive_pressure_compare_manifest.json"
    )
    comparison_summary = _read_json(source_comparison_dir / "defensive_pressure_summary.json")
    cases = _defensive_case_rows(inventory)
    supporting = [
        _supporting_case_payload(row)
        for row in cases
        if row["classification"] == "supporting"
    ]
    contradicting = [
        _contradicting_case_payload(row)
        for row in cases
        if row["classification"] == "contradicting"
    ]
    matrix = _regime_effect_matrix(cases)
    attribution = _exposure_change_attribution(cases)
    source_counts = Counter(_text(row.get("source_mode")) for row in cases)
    deep_dive_id = _stable_id(
        "defensive-hypothesis-deep-dive",
        pressure_backfill_id,
        comparison_id,
        generated.isoformat(),
    )
    deep_dive_dir = _unique_dir(output_dir / deep_dive_id)
    deep_dive_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_hypothesis_deep_dive_manifest",
        "deep_dive_id": deep_dive_dir.name,
        "pressure_backfill_id": pressure_backfill_id,
        "comparison_id": comparison_id,
        "source_pressure_backfill_id": backfill_manifest.get("pressure_backfill_id"),
        "source_comparison_id": comparison_manifest.get("comparison_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if cases else "PASS_WITH_WARNINGS",
        "market_regime": "ai_after_chatgpt",
        "source_mode_counts": {mode: source_counts.get(mode, 0) for mode in SOURCE_MODES},
        "supporting_case_count": len(supporting),
        "contradicting_case_count": len(contradicting),
        "mixed_or_empty_case_count": sum(1 for row in cases if row["classification"] == "mixed"),
        "defensive_pressure_status": comparison_summary.get("defensive_status"),
        "can_support_rule_approval": False,
        "deep_dive_manifest_path": str(deep_dive_dir / "deep_dive_manifest.json"),
        "supporting_cases_path": str(deep_dive_dir / "supporting_cases.jsonl"),
        "contradicting_cases_path": str(deep_dive_dir / "contradicting_cases.jsonl"),
        "regime_effect_matrix_path": str(deep_dive_dir / "regime_effect_matrix.json"),
        "exposure_change_attribution_path": str(
            deep_dive_dir / "exposure_change_attribution.json"
        ),
        "defensive_hypothesis_deep_dive_report_path": str(
            deep_dive_dir / "defensive_hypothesis_deep_dive_report.md"
        ),
        **_artifact_safety(),
    }
    _write_json(deep_dive_dir / "deep_dive_manifest.json", manifest)
    _write_jsonl(deep_dive_dir / "supporting_cases.jsonl", supporting)
    _write_jsonl(deep_dive_dir / "contradicting_cases.jsonl", contradicting)
    _write_json(deep_dive_dir / "regime_effect_matrix.json", matrix)
    _write_json(deep_dive_dir / "exposure_change_attribution.json", attribution)
    _write_text(
        deep_dive_dir / "defensive_hypothesis_deep_dive_report.md",
        render_defensive_hypothesis_deep_dive_report(
            manifest,
            matrix,
            attribution,
            comparison_summary,
        ),
    )
    _update_latest_pointer(
        "latest_defensive_hypothesis_deep_dive",
        deep_dive_dir.name,
        deep_dive_dir / "deep_dive_manifest.json",
    )
    return {
        "deep_dive_id": deep_dive_dir.name,
        "deep_dive_dir": deep_dive_dir,
        "manifest": manifest,
        "supporting_cases": supporting,
        "contradicting_cases": contradicting,
        "regime_effect_matrix": matrix,
        "exposure_change_attribution": attribution,
    }


def defensive_hypothesis_deep_dive_report_payload(
    *,
    deep_dive_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> dict[str, Any]:
    deep_dive_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=deep_dive_id if not latest else None,
        pointer_name="latest_defensive_hypothesis_deep_dive",
    )
    return {
        **_read_json(deep_dive_dir / "deep_dive_manifest.json"),
        "supporting_cases": _read_jsonl(deep_dive_dir / "supporting_cases.jsonl"),
        "contradicting_cases": _read_jsonl(deep_dive_dir / "contradicting_cases.jsonl"),
        "regime_effect_matrix": _read_json(deep_dive_dir / "regime_effect_matrix.json"),
        "exposure_change_attribution": _read_json(
            deep_dive_dir / "exposure_change_attribution.json"
        ),
        "deep_dive_dir": str(deep_dive_dir),
    }


def validate_defensive_hypothesis_deep_dive_artifact(
    *,
    deep_dive_id: str,
    output_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
) -> dict[str, Any]:
    deep_dive_dir = output_dir / deep_dive_id
    manifest = _read_optional_json(deep_dive_dir / "deep_dive_manifest.json") or {}
    supporting = _read_jsonl(deep_dive_dir / "supporting_cases.jsonl")
    contradicting = _read_jsonl(deep_dive_dir / "contradicting_cases.jsonl")
    matrix = _read_optional_json(deep_dive_dir / "regime_effect_matrix.json") or {}
    checks = [
        _check("manifest_exists", (deep_dive_dir / "deep_dive_manifest.json").exists(), ""),
        _check("supporting_cases_exists", (deep_dive_dir / "supporting_cases.jsonl").exists(), ""),
        _check(
            "contradicting_cases_exists",
            (deep_dive_dir / "contradicting_cases.jsonl").exists(),
            "",
        ),
        _check("regime_matrix_exists", (deep_dive_dir / "regime_effect_matrix.json").exists(), ""),
        _check(
            "exposure_attribution_exists",
            (deep_dive_dir / "exposure_change_attribution.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (deep_dive_dir / "defensive_hypothesis_deep_dive_report.md").exists(),
            "",
        ),
        _check("deep_dive_id_matches", manifest.get("deep_dive_id") == deep_dive_id, ""),
        _check(
            "case_source_modes_valid",
            all(
                _text(row.get("source_mode")) in SOURCE_MODES
                for row in supporting + contradicting
            ),
            "known source modes",
        ),
        _check(
            "regime_matrix_complete",
            set(row.get("regime") for row in _records(matrix.get("regimes")))
            >= set(PRESSURE_REGIMES),
            "all pressure regimes present",
        ),
        _check(
            "simulation_not_rule_approval",
            manifest.get("can_support_rule_approval") is False
            and manifest.get("production_effect") == "none",
            "simulation research does not approve rule",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_hypothesis_deep_dive_validation",
        artifact_id_key="deep_dive_id",
        artifact_id=deep_dive_id,
        checks=checks,
    )


def run_defensive_label_review(
    *,
    deep_dive_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = deep_dive_dir / deep_dive_id
    deep_manifest = _read_json(source_dir / "deep_dive_manifest.json")
    matrix = _read_json(source_dir / "regime_effect_matrix.json")
    label_matrix = _label_decision_matrix(deep_manifest, matrix)
    candidate_labels = _candidate_labels(label_matrix)
    label_review_id = _stable_id("defensive-label-review", deep_dive_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / label_review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_label_review_manifest",
        "label_review_id": review_dir.name,
        "deep_dive_id": deep_dive_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "label_review_manifest_path": str(review_dir / "label_review_manifest.json"),
        "label_decision_matrix_path": str(review_dir / "label_decision_matrix.json"),
        "candidate_labels_path": str(review_dir / "candidate_labels.json"),
        "defensive_label_review_report_path": str(
            review_dir / "defensive_label_review_report.md"
        ),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    reader_brief = render_defensive_label_reader_brief(label_matrix)
    _write_json(review_dir / "label_review_manifest.json", manifest)
    _write_json(review_dir / "label_decision_matrix.json", label_matrix)
    _write_json(review_dir / "candidate_labels.json", candidate_labels)
    _write_text(
        review_dir / "defensive_label_review_report.md",
        render_defensive_label_review_report(manifest, label_matrix, candidate_labels),
    )
    _write_text(review_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_defensive_label_review",
        review_dir.name,
        review_dir / "label_review_manifest.json",
    )
    return {
        "label_review_id": review_dir.name,
        "label_review_dir": review_dir,
        "manifest": manifest,
        "label_decision_matrix": label_matrix,
        "candidate_labels": candidate_labels,
        "reader_brief_section": reader_brief,
    }


def defensive_label_review_report_payload(
    *,
    label_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=label_review_id if not latest else None,
        pointer_name="latest_defensive_label_review",
    )
    return {
        **_read_json(review_dir / "label_review_manifest.json"),
        "label_decision_matrix": _read_json(review_dir / "label_decision_matrix.json"),
        "candidate_labels": _read_json(review_dir / "candidate_labels.json"),
        "reader_brief_section": _read_text(review_dir / "reader_brief_section.md"),
        "label_review_dir": str(review_dir),
    }


def validate_defensive_label_review_artifact(
    *, label_review_id: str, output_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR
) -> dict[str, Any]:
    review_dir = output_dir / label_review_id
    manifest = _read_optional_json(review_dir / "label_review_manifest.json") or {}
    matrix = _read_optional_json(review_dir / "label_decision_matrix.json") or {}
    labels = _read_optional_json(review_dir / "candidate_labels.json") or {}
    checks = [
        _check("manifest_exists", (review_dir / "label_review_manifest.json").exists(), ""),
        _check("decision_matrix_exists", (review_dir / "label_decision_matrix.json").exists(), ""),
        _check("candidate_labels_exists", (review_dir / "candidate_labels.json").exists(), ""),
        _check("report_exists", (review_dir / "defensive_label_review_report.md").exists(), ""),
        _check("reader_brief_exists", (review_dir / "reader_brief_section.md").exists(), ""),
        _check("label_review_id_matches", manifest.get("label_review_id") == label_review_id, ""),
        _check("auto_rename_false", matrix.get("auto_rename") is False, ""),
        _check(
            "config_change_disallowed",
            matrix.get("config_change_allowed") is False
            and manifest.get("policy_change_allowed") is False,
            "review cannot change config",
        ),
        _check("candidate_labels_present", bool(_records(labels.get("labels"))), "labels"),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_label_review_validation",
        artifact_id_key="label_review_id",
        artifact_id=label_review_id,
        checks=checks,
    )


def run_defensive_failure_study(
    *,
    deep_dive_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = deep_dive_dir / deep_dive_id
    _read_json(source_dir / "deep_dive_manifest.json")
    contradicting = _read_jsonl(source_dir / "contradicting_cases.jsonl")
    ranked = _rank_failure_cases(contradicting)
    pattern_summary = _failure_pattern_summary(ranked)
    mitigation_ideas = _failure_mitigation_ideas(pattern_summary)
    failure_study_id = _stable_id("defensive-failure-study", deep_dive_id, generated.isoformat())
    study_dir = _unique_dir(output_dir / failure_study_id)
    study_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_failure_study_manifest",
        "failure_study_id": study_dir.name,
        "deep_dive_id": deep_dive_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if ranked else "PASS_WITH_WARNINGS",
        "market_regime": "ai_after_chatgpt",
        "failure_case_count": len(ranked),
        "failure_study_manifest_path": str(study_dir / "failure_study_manifest.json"),
        "failure_cases_ranked_path": str(study_dir / "failure_cases_ranked.jsonl"),
        "failure_pattern_summary_path": str(study_dir / "failure_pattern_summary.json"),
        "failure_mitigation_ideas_path": str(study_dir / "failure_mitigation_ideas.json"),
        "defensive_failure_study_report_path": str(
            study_dir / "defensive_failure_study_report.md"
        ),
        **_artifact_safety(),
    }
    _write_json(study_dir / "failure_study_manifest.json", manifest)
    _write_jsonl(study_dir / "failure_cases_ranked.jsonl", ranked)
    _write_json(study_dir / "failure_pattern_summary.json", pattern_summary)
    _write_json(study_dir / "failure_mitigation_ideas.json", mitigation_ideas)
    _write_text(
        study_dir / "defensive_failure_study_report.md",
        render_defensive_failure_study_report(manifest, pattern_summary, mitigation_ideas),
    )
    _update_latest_pointer(
        "latest_defensive_failure_study",
        study_dir.name,
        study_dir / "failure_study_manifest.json",
    )
    return {
        "failure_study_id": study_dir.name,
        "failure_study_dir": study_dir,
        "manifest": manifest,
        "failure_cases_ranked": ranked,
        "failure_pattern_summary": pattern_summary,
        "failure_mitigation_ideas": mitigation_ideas,
    }


def defensive_failure_study_report_payload(
    *,
    failure_study_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
) -> dict[str, Any]:
    study_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=failure_study_id if not latest else None,
        pointer_name="latest_defensive_failure_study",
    )
    return {
        **_read_json(study_dir / "failure_study_manifest.json"),
        "failure_cases_ranked": _read_jsonl(study_dir / "failure_cases_ranked.jsonl"),
        "failure_pattern_summary": _read_json(study_dir / "failure_pattern_summary.json"),
        "failure_mitigation_ideas": _read_json(study_dir / "failure_mitigation_ideas.json"),
        "failure_study_dir": str(study_dir),
    }


def validate_defensive_failure_study_artifact(
    *, failure_study_id: str, output_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR
) -> dict[str, Any]:
    study_dir = output_dir / failure_study_id
    manifest = _read_optional_json(study_dir / "failure_study_manifest.json") or {}
    ranked = _read_jsonl(study_dir / "failure_cases_ranked.jsonl")
    ideas = _read_optional_json(study_dir / "failure_mitigation_ideas.json") or {}
    checks = [
        _check("manifest_exists", (study_dir / "failure_study_manifest.json").exists(), ""),
        _check("ranked_cases_exists", (study_dir / "failure_cases_ranked.jsonl").exists(), ""),
        _check("pattern_summary_exists", (study_dir / "failure_pattern_summary.json").exists(), ""),
        _check(
            "mitigation_ideas_exists",
            (study_dir / "failure_mitigation_ideas.json").exists(),
            "",
        ),
        _check("report_exists", (study_dir / "defensive_failure_study_report.md").exists(), ""),
        _check(
            "failure_study_id_matches",
            manifest.get("failure_study_id") == failure_study_id,
            "",
        ),
        _check(
            "severity_values_valid",
            all(row.get("failure_severity") in {"HIGH", "MEDIUM", "LOW"} for row in ranked),
            "severity bands",
        ),
        _check(
            "mitigation_not_auto_apply",
            all(row.get("auto_apply") is False for row in _records(ideas.get("ideas"))),
            "mitigation ideas are research-only",
        ),
        _check(
            "safety_no_policy_change",
            manifest.get("policy_change_allowed") is False
            and manifest.get("production_effect") == "none",
            "no policy change",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_failure_study_validation",
        artifact_id_key="failure_study_id",
        artifact_id=failure_study_id,
        checks=checks,
    )


def run_defensive_research_note(
    *,
    deep_dive_id: str,
    label_review_id: str,
    failure_study_id: str,
    deep_dive_dir: Path = DEFAULT_DEFENSIVE_HYPOTHESIS_DEEP_DIVE_DIR,
    label_review_dir: Path = DEFAULT_DEFENSIVE_LABEL_REVIEW_DIR,
    failure_study_dir: Path = DEFAULT_DEFENSIVE_FAILURE_STUDY_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    deep_manifest = _read_json(deep_dive_dir / deep_dive_id / "deep_dive_manifest.json")
    regime_matrix = _read_json(deep_dive_dir / deep_dive_id / "regime_effect_matrix.json")
    label_matrix = _read_json(label_review_dir / label_review_id / "label_decision_matrix.json")
    failure_summary = _read_json(
        failure_study_dir / failure_study_id / "failure_pattern_summary.json"
    )
    summary = _defensive_hypothesis_summary(deep_manifest, label_matrix)
    note_id = _stable_id(
        "defensive-research-note",
        deep_dive_id,
        label_review_id,
        failure_study_id,
        generated.isoformat(),
    )
    note_dir = _unique_dir(output_dir / note_id)
    note_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_research_note_manifest",
        "note_id": note_dir.name,
        "deep_dive_id": deep_dive_id,
        "label_review_id": label_review_id,
        "failure_study_id": failure_study_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "defensive_research_note_manifest_path": str(
            note_dir / "defensive_research_note_manifest.json"
        ),
        "defensive_hypothesis_summary_path": str(
            note_dir / "defensive_hypothesis_summary.json"
        ),
        "defensive_research_note_path": str(note_dir / "defensive_research_note.md"),
        "reader_brief_section_path": str(note_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    reader_brief = render_defensive_research_reader_brief(summary)
    _write_json(note_dir / "defensive_research_note_manifest.json", manifest)
    _write_json(note_dir / "defensive_hypothesis_summary.json", summary)
    _write_text(
        note_dir / "defensive_research_note.md",
        render_defensive_research_note(summary, regime_matrix, label_matrix, failure_summary),
    )
    _write_text(note_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_defensive_research_note",
        note_dir.name,
        note_dir / "defensive_research_note_manifest.json",
    )
    return {
        "note_id": note_dir.name,
        "note_dir": note_dir,
        "manifest": manifest,
        "defensive_hypothesis_summary": summary,
        "reader_brief_section": reader_brief,
    }


def defensive_research_note_report_payload(
    *,
    note_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
) -> dict[str, Any]:
    note_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=note_id if not latest else None,
        pointer_name="latest_defensive_research_note",
    )
    return {
        **_read_json(note_dir / "defensive_research_note_manifest.json"),
        "defensive_hypothesis_summary": _read_json(
            note_dir / "defensive_hypothesis_summary.json"
        ),
        "defensive_research_note": _read_text(note_dir / "defensive_research_note.md"),
        "reader_brief_section": _read_text(note_dir / "reader_brief_section.md"),
        "note_dir": str(note_dir),
    }


def validate_defensive_research_note_artifact(
    *, note_id: str, output_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR
) -> dict[str, Any]:
    note_dir = output_dir / note_id
    manifest = _read_optional_json(note_dir / "defensive_research_note_manifest.json") or {}
    summary = _read_optional_json(note_dir / "defensive_hypothesis_summary.json") or {}
    note = _read_text(note_dir / "defensive_research_note.md")
    checks = [
        _check(
            "manifest_exists",
            (note_dir / "defensive_research_note_manifest.json").exists(),
            "",
        ),
        _check("summary_exists", (note_dir / "defensive_hypothesis_summary.json").exists(), ""),
        _check("note_exists", (note_dir / "defensive_research_note.md").exists(), ""),
        _check("reader_brief_exists", (note_dir / "reader_brief_section.md").exists(), ""),
        _check("note_id_matches", manifest.get("note_id") == note_id, ""),
        _check(
            "rule_approval_false",
            summary.get("can_support_rule_approval") is False,
            "research note cannot approve rule",
        ),
        _check(
            "required_sections_present",
            all(
                phrase in note
                for phrase in (
                    "当前假设",
                    "simulation 支持",
                    "forward / PIT",
                    "owner",
                )
            ),
            "core research sections",
        ),
        _check("safety_no_production", manifest.get("production_effect") == "none", ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_research_note_validation",
        artifact_id_key="note_id",
        artifact_id=note_id,
        checks=checks,
    )


def run_defensive_owner_pack(
    *,
    note_id: str,
    note_dir: Path = DEFAULT_DEFENSIVE_RESEARCH_NOTE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    summary = _read_json(note_dir / note_id / "defensive_hypothesis_summary.json")
    options = _owner_decision_options()
    checklist = render_owner_decision_checklist()
    pack_id = _stable_id("defensive-owner-pack", note_id, generated.isoformat())
    pack_dir = _unique_dir(output_dir / pack_id)
    pack_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_owner_pack_manifest",
        "pack_id": pack_dir.name,
        "note_id": note_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "defensive_owner_pack_manifest_path": str(
            pack_dir / "defensive_owner_pack_manifest.json"
        ),
        "owner_decision_options_path": str(pack_dir / "owner_decision_options.json"),
        "owner_decision_checklist_path": str(pack_dir / "owner_decision_checklist.md"),
        "defensive_owner_pack_report_path": str(pack_dir / "defensive_owner_pack_report.md"),
        **_artifact_safety(),
    }
    _write_json(pack_dir / "defensive_owner_pack_manifest.json", manifest)
    _write_json(pack_dir / "owner_decision_options.json", options)
    _write_text(pack_dir / "owner_decision_checklist.md", checklist)
    _write_text(
        pack_dir / "defensive_owner_pack_report.md",
        render_defensive_owner_pack_report(manifest, summary, options),
    )
    _update_latest_pointer(
        "latest_defensive_owner_pack",
        pack_dir.name,
        pack_dir / "defensive_owner_pack_manifest.json",
    )
    return {
        "pack_id": pack_dir.name,
        "pack_dir": pack_dir,
        "manifest": manifest,
        "owner_decision_options": options,
        "owner_decision_checklist": checklist,
    }


def defensive_owner_pack_report_payload(
    *,
    pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR,
) -> dict[str, Any]:
    pack_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=pack_id if not latest else None,
        pointer_name="latest_defensive_owner_pack",
    )
    return {
        **_read_json(pack_dir / "defensive_owner_pack_manifest.json"),
        "owner_decision_options": _read_json(pack_dir / "owner_decision_options.json"),
        "owner_decision_checklist": _read_text(pack_dir / "owner_decision_checklist.md"),
        "pack_dir": str(pack_dir),
    }


def validate_defensive_owner_pack_artifact(
    *, pack_id: str, output_dir: Path = DEFAULT_DEFENSIVE_OWNER_PACK_DIR
) -> dict[str, Any]:
    pack_dir = output_dir / pack_id
    manifest = _read_optional_json(pack_dir / "defensive_owner_pack_manifest.json") or {}
    options = _read_optional_json(pack_dir / "owner_decision_options.json") or {}
    checklist = _read_text(pack_dir / "owner_decision_checklist.md")
    checks = [
        _check("manifest_exists", (pack_dir / "defensive_owner_pack_manifest.json").exists(), ""),
        _check("options_exists", (pack_dir / "owner_decision_options.json").exists(), ""),
        _check("checklist_exists", (pack_dir / "owner_decision_checklist.md").exists(), ""),
        _check("report_exists", (pack_dir / "defensive_owner_pack_report.md").exists(), ""),
        _check("pack_id_matches", manifest.get("pack_id") == pack_id, ""),
        _check("auto_apply_false", options.get("auto_apply") is False, ""),
        _check("policy_change_false", options.get("policy_change_allowed") is False, ""),
        _check("broker_action_false", options.get("broker_action_allowed") is False, ""),
        _check("checklist_mentions_broker", "broker" in checklist, "broker boundary"),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_owner_pack_validation",
        artifact_id_key="pack_id",
        artifact_id=pack_id,
        checks=checks,
    )


def build_forward_pressure_capture_plan(
    *,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = _read_yaml_config(config_path)
    validation = _capture_config_checks(config)
    if not all(row["passed"] for row in validation):
        failed = ", ".join(row["check_id"] for row in validation if not row["passed"])
        raise DynamicV3DefensiveEvidenceError(f"forward pressure capture config failed: {failed}")
    daily_pack = _command_pack("daily", config)
    weekly_pack = _command_pack("weekly", config)
    event_plan = _event_trigger_plan(config)
    capture_plan_id = _stable_id(
        "forward-pressure-capture-plan",
        str(config_path),
        generated.isoformat(),
    )
    plan_dir = _unique_dir(output_dir / capture_plan_id)
    plan_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_pressure_capture_manifest",
        "capture_plan_id": plan_dir.name,
        "config_path": str(config_path),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "capture_plan_manifest_path": str(plan_dir / "capture_plan_manifest.json"),
        "daily_command_pack_path": str(plan_dir / "daily_command_pack.json"),
        "weekly_command_pack_path": str(plan_dir / "weekly_command_pack.json"),
        "event_driven_trigger_plan_path": str(plan_dir / "event_driven_trigger_plan.json"),
        "forward_pressure_capture_report_path": str(
            plan_dir / "forward_pressure_capture_report.md"
        ),
        **_artifact_safety(),
    }
    _write_json(plan_dir / "capture_plan_manifest.json", manifest)
    _write_json(plan_dir / "daily_command_pack.json", daily_pack)
    _write_json(plan_dir / "weekly_command_pack.json", weekly_pack)
    _write_json(plan_dir / "event_driven_trigger_plan.json", event_plan)
    _write_text(
        plan_dir / "forward_pressure_capture_report.md",
        render_forward_pressure_capture_report(manifest, daily_pack, weekly_pack, event_plan),
    )
    _update_latest_pointer(
        "latest_forward_pressure_capture",
        plan_dir.name,
        plan_dir / "capture_plan_manifest.json",
    )
    return {
        "capture_plan_id": plan_dir.name,
        "capture_plan_dir": plan_dir,
        "manifest": manifest,
        "daily_command_pack": daily_pack,
        "weekly_command_pack": weekly_pack,
        "event_driven_trigger_plan": event_plan,
    }


def forward_pressure_capture_report_payload(
    *,
    capture_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    plan_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=capture_plan_id if not latest else None,
        pointer_name="latest_forward_pressure_capture",
    )
    return {
        **_read_json(plan_dir / "capture_plan_manifest.json"),
        "daily_command_pack": _read_json(plan_dir / "daily_command_pack.json"),
        "weekly_command_pack": _read_json(plan_dir / "weekly_command_pack.json"),
        "event_driven_trigger_plan": _read_json(plan_dir / "event_driven_trigger_plan.json"),
        "capture_plan_dir": str(plan_dir),
    }


def validate_forward_pressure_capture_artifact(
    *,
    capture_plan_id: str,
    output_dir: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    plan_dir = output_dir / capture_plan_id
    manifest = _read_optional_json(plan_dir / "capture_plan_manifest.json") or {}
    daily_pack = _read_optional_json(plan_dir / "daily_command_pack.json") or {}
    weekly_pack = _read_optional_json(plan_dir / "weekly_command_pack.json") or {}
    event_plan = _read_optional_json(plan_dir / "event_driven_trigger_plan.json") or {}
    checks = [
        _check("manifest_exists", (plan_dir / "capture_plan_manifest.json").exists(), ""),
        _check("daily_pack_exists", (plan_dir / "daily_command_pack.json").exists(), ""),
        _check("weekly_pack_exists", (plan_dir / "weekly_command_pack.json").exists(), ""),
        _check("event_plan_exists", (plan_dir / "event_driven_trigger_plan.json").exists(), ""),
        _check("report_exists", (plan_dir / "forward_pressure_capture_report.md").exists(), ""),
        _check("capture_plan_id_matches", manifest.get("capture_plan_id") == capture_plan_id, ""),
        _check("daily_commands_present", bool(_command_items(daily_pack.get("commands"))), ""),
        _check("weekly_commands_present", bool(_command_items(weekly_pack.get("commands"))), ""),
        _check("event_triggers_present", bool(_mapping(event_plan.get("triggers"))), ""),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and manifest.get("production_effect") == "none",
            "no broker/no production",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_forward_pressure_capture_validation",
        artifact_id_key="capture_plan_id",
        artifact_id=capture_plan_id,
        checks=checks,
    )


def run_pressure_trigger_scan(
    *,
    as_of: date,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = _read_yaml_config(config_path)
    validation = _capture_config_checks(config)
    if not all(row["passed"] for row in validation):
        failed = ", ".join(row["check_id"] for row in validation if not row["passed"])
        raise DynamicV3DefensiveEvidenceError(f"pressure trigger config failed: {failed}")
    trigger_id = _stable_id("pressure-trigger", as_of.isoformat(), generated.isoformat())
    trigger_dir = _unique_dir(output_dir / trigger_id)
    trigger_dir.mkdir(parents=True, exist_ok=False)
    data_quality_status, data_quality_report_path = _quality_gate_for_pressure_trigger(
        as_of=as_of,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=trigger_dir / "validate_data_quality_report.md",
        enforce=enforce_data_quality_gate,
    )
    metrics = _trigger_metrics(as_of=as_of, prices_path=prices_path)
    metrics["trigger_status"] = _trigger_status(metrics, config)
    actions = _triggered_actions(metrics, config)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_trigger_manifest",
        "trigger_id": trigger_dir.name,
        "as_of": as_of.isoformat(),
        "config_path": str(config_path),
        "generated_at": generated.isoformat(),
        "status": (
            "PASS"
            if metrics["trigger_status"] != "INSUFFICIENT_DATA"
            else "INSUFFICIENT_DATA"
        ),
        "data_quality_status": data_quality_status,
        "data_quality_report_path": data_quality_report_path,
        "market_regime": "ai_after_chatgpt",
        "pressure_trigger_manifest_path": str(trigger_dir / "pressure_trigger_manifest.json"),
        "trigger_metrics_path": str(trigger_dir / "trigger_metrics.json"),
        "triggered_actions_path": str(trigger_dir / "triggered_actions.json"),
        "pressure_trigger_report_path": str(trigger_dir / "pressure_trigger_report.md"),
        **_artifact_safety(),
    }
    _write_json(trigger_dir / "pressure_trigger_manifest.json", manifest)
    _write_json(trigger_dir / "trigger_metrics.json", metrics)
    _write_json(trigger_dir / "triggered_actions.json", actions)
    _write_text(
        trigger_dir / "pressure_trigger_report.md",
        render_pressure_trigger_report(manifest, metrics, actions),
    )
    _update_latest_pointer(
        "latest_pressure_trigger",
        trigger_dir.name,
        trigger_dir / "pressure_trigger_manifest.json",
    )
    return {
        "trigger_id": trigger_dir.name,
        "trigger_dir": trigger_dir,
        "manifest": manifest,
        "trigger_metrics": metrics,
        "triggered_actions": actions,
    }


def pressure_trigger_report_payload(
    *,
    trigger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
) -> dict[str, Any]:
    trigger_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=trigger_id if not latest else None,
        pointer_name="latest_pressure_trigger",
    )
    return {
        **_read_json(trigger_dir / "pressure_trigger_manifest.json"),
        "trigger_metrics": _read_json(trigger_dir / "trigger_metrics.json"),
        "triggered_actions": _read_json(trigger_dir / "triggered_actions.json"),
        "trigger_dir": str(trigger_dir),
    }


def validate_pressure_trigger_artifact(
    *, trigger_id: str, output_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR
) -> dict[str, Any]:
    trigger_dir = output_dir / trigger_id
    manifest = _read_optional_json(trigger_dir / "pressure_trigger_manifest.json") or {}
    metrics = _read_optional_json(trigger_dir / "trigger_metrics.json") or {}
    actions = _read_optional_json(trigger_dir / "triggered_actions.json") or {}
    checks = [
        _check("manifest_exists", (trigger_dir / "pressure_trigger_manifest.json").exists(), ""),
        _check("metrics_exists", (trigger_dir / "trigger_metrics.json").exists(), ""),
        _check("actions_exists", (trigger_dir / "triggered_actions.json").exists(), ""),
        _check("report_exists", (trigger_dir / "pressure_trigger_report.md").exists(), ""),
        _check("trigger_id_matches", manifest.get("trigger_id") == trigger_id, ""),
        _check(
            "trigger_status_valid",
            metrics.get("trigger_status")
            in {"NO_TRIGGER", "PRESSURE_TRIGGERED", "INSUFFICIENT_DATA"},
            "trigger status",
        ),
        _check(
            "no_trigger_has_no_capture_required",
            metrics.get("trigger_status") != "NO_TRIGGER"
            or actions.get("event_driven_capture_required") is False,
            "no heavy flow when no trigger",
        ),
        _check("safety_no_broker", manifest.get("broker_action_allowed") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_trigger_validation",
        artifact_id_key="trigger_id",
        artifact_id=trigger_id,
        checks=checks,
    )


def run_pressure_capture_workflow(
    *,
    trigger_id: str,
    force: bool = False,
    trigger_dir: Path = DEFAULT_PRESSURE_TRIGGER_DIR,
    output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    pressure_backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = trigger_dir / trigger_id
    trigger_manifest = _read_json(source_dir / "pressure_trigger_manifest.json")
    trigger_metrics = _read_json(source_dir / "trigger_metrics.json")
    as_of = _date_from_any(trigger_manifest.get("as_of")) or generated.date()
    trigger_status = _text(trigger_metrics.get("trigger_status"), "INSUFFICIENT_DATA")
    steps: list[dict[str, Any]] = []
    artifacts: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_artifacts",
    }
    should_run = force or trigger_status == "PRESSURE_TRIGGERED"
    capture_status = "PASS" if should_run else "SKIPPED"
    if should_run:
        tag = run_pressure_regime_tagging(
            start=AI_AFTER_CHATGPT_START,
            end=as_of,
            output_dir=pressure_tag_dir,
            prices_path=prices_path,
            rates_path=rates_path,
            enforce_data_quality_gate=enforce_data_quality_gate,
            generated_at=generated,
        )
        artifacts["pressure_regime_tag_id"] = tag["tag_id"]
        steps.append(_capture_step("pressure-regime-tag", "PASS", tag["tag_id"]))
        backfill = run_pressure_outcome_backfill(
            start=AI_AFTER_CHATGPT_START,
            end=as_of,
            output_dir=pressure_backfill_dir,
            pressure_tag_dir=pressure_tag_dir,
            advisory_outcome_dir=advisory_outcome_dir,
            backfilled_outcome_dir=backfilled_outcome_dir,
            backtest_sim_outcome_dir=backtest_sim_outcome_dir,
            generated_at=generated,
        )
        artifacts["pressure_backfill_id"] = backfill["pressure_backfill_id"]
        steps.append(
            _capture_step(
                "pressure-outcome-backfill",
                _text(_mapping(backfill.get("manifest")).get("status"), "PASS"),
                backfill["pressure_backfill_id"],
            )
        )
        compare = run_defensive_pressure_compare(
            pressure_backfill_id=backfill["pressure_backfill_id"],
            backfill_dir=pressure_backfill_dir,
            output_dir=comparison_dir,
            generated_at=generated,
        )
        artifacts["comparison_id"] = compare["comparison_id"]
        steps.append(_capture_step("defensive-pressure-compare", "PASS", compare["comparison_id"]))
    else:
        artifacts["pressure_regime_tag_id"] = ""
        artifacts["pressure_backfill_id"] = ""
        artifacts["comparison_id"] = ""
        steps.extend(
            [
                _capture_step("pressure-regime-tag", "SKIPPED", "", reason="no_trigger"),
                _capture_step("pressure-outcome-backfill", "SKIPPED", "", reason="no_trigger"),
                _capture_step("defensive-pressure-compare", "SKIPPED", "", reason="no_trigger"),
            ]
        )
    capture_steps = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_steps",
        "trigger_id": trigger_id,
        "trigger_status": trigger_status,
        "manual_force": bool(force),
        "steps": steps,
        "broker_action_allowed": False,
        "policy_change_allowed": False,
        "production_effect": "none",
    }
    artifacts.update(
        {
            "trigger_id": trigger_id,
            "trigger_status": trigger_status,
            "manual_force": bool(force),
            "broker_action_allowed": False,
            "policy_change_allowed": False,
            "production_effect": "none",
        }
    )
    capture_id = _stable_id("pressure-capture", trigger_id, force, generated.isoformat())
    capture_dir = _unique_dir(output_dir / capture_id)
    capture_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_capture_manifest",
        "capture_id": capture_dir.name,
        "trigger_id": trigger_id,
        "generated_at": generated.isoformat(),
        "status": capture_status,
        "manual_force": bool(force),
        "market_regime": "ai_after_chatgpt",
        "pressure_capture_manifest_path": str(capture_dir / "pressure_capture_manifest.json"),
        "pressure_capture_steps_path": str(capture_dir / "pressure_capture_steps.json"),
        "pressure_capture_artifacts_path": str(capture_dir / "pressure_capture_artifacts.json"),
        "pressure_capture_report_path": str(capture_dir / "pressure_capture_report.md"),
        **_artifact_safety(),
    }
    _write_json(capture_dir / "pressure_capture_manifest.json", manifest)
    _write_json(capture_dir / "pressure_capture_steps.json", capture_steps)
    _write_json(capture_dir / "pressure_capture_artifacts.json", artifacts)
    _write_text(
        capture_dir / "pressure_capture_report.md",
        render_pressure_capture_report(manifest, capture_steps, artifacts),
    )
    _update_latest_pointer(
        "latest_pressure_capture",
        capture_dir.name,
        capture_dir / "pressure_capture_manifest.json",
    )
    return {
        "capture_id": capture_dir.name,
        "capture_dir": capture_dir,
        "manifest": manifest,
        "pressure_capture_steps": capture_steps,
        "pressure_capture_artifacts": artifacts,
    }


def pressure_capture_report_payload(
    *,
    capture_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR,
) -> dict[str, Any]:
    capture_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=capture_id if not latest else None,
        pointer_name="latest_pressure_capture",
    )
    return {
        **_read_json(capture_dir / "pressure_capture_manifest.json"),
        "pressure_capture_steps": _read_json(capture_dir / "pressure_capture_steps.json"),
        "pressure_capture_artifacts": _read_json(capture_dir / "pressure_capture_artifacts.json"),
        "capture_dir": str(capture_dir),
    }


def validate_pressure_capture_artifact(
    *, capture_id: str, output_dir: Path = DEFAULT_PRESSURE_CAPTURE_DIR
) -> dict[str, Any]:
    capture_dir = output_dir / capture_id
    manifest = _read_optional_json(capture_dir / "pressure_capture_manifest.json") or {}
    steps = _read_optional_json(capture_dir / "pressure_capture_steps.json") or {}
    checks = [
        _check("manifest_exists", (capture_dir / "pressure_capture_manifest.json").exists(), ""),
        _check("steps_exists", (capture_dir / "pressure_capture_steps.json").exists(), ""),
        _check("artifacts_exists", (capture_dir / "pressure_capture_artifacts.json").exists(), ""),
        _check("report_exists", (capture_dir / "pressure_capture_report.md").exists(), ""),
        _check("capture_id_matches", manifest.get("capture_id") == capture_id, ""),
        _check(
            "no_trigger_skip_or_force",
            steps.get("trigger_status") != "NO_TRIGGER"
            or manifest.get("status") == "SKIPPED"
            or steps.get("manual_force") is True,
            "NO_TRIGGER must skip unless force",
        ),
        _check("broker_action_false", steps.get("broker_action_allowed") is False, ""),
        _check("production_effect_none", steps.get("production_effect") == "none", ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_capture_validation",
        artifact_id_key="capture_id",
        artifact_id=capture_id,
        checks=checks,
    )


def update_pressure_sample_ledger(
    *,
    output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
    pressure_backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    config_path: Path = DEFAULT_FORWARD_PRESSURE_CAPTURE_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    inventory = _latest_pressure_inventory(pressure_backfill_dir)
    required_forward = _required_forward_samples(config_path)
    samples = [_pressure_sample_row(row) for row in inventory]
    summary = _pressure_sample_summary(samples, required_forward=required_forward)
    ledger_id = _stable_id("pressure-sample-ledger", generated.isoformat(), len(samples))
    ledger_dir = _unique_dir(output_dir / ledger_id)
    ledger_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_ledger_manifest",
        "ledger_id": ledger_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS" if samples else "PASS_WITH_WARNINGS",
        "market_regime": "ai_after_chatgpt",
        "pressure_sample_ledger_manifest_path": str(
            ledger_dir / "pressure_sample_ledger_manifest.json"
        ),
        "pressure_samples_path": str(ledger_dir / "pressure_samples.jsonl"),
        "pressure_sample_summary_path": str(ledger_dir / "pressure_sample_summary.json"),
        "pressure_sample_ledger_report_path": str(
            ledger_dir / "pressure_sample_ledger_report.md"
        ),
        **_artifact_safety(),
    }
    _write_json(ledger_dir / "pressure_sample_ledger_manifest.json", manifest)
    _write_jsonl(ledger_dir / "pressure_samples.jsonl", samples)
    _write_json(ledger_dir / "pressure_sample_summary.json", summary)
    _write_text(
        ledger_dir / "pressure_sample_ledger_report.md",
        render_pressure_sample_ledger_report(manifest, summary),
    )
    _update_latest_pointer(
        "latest_pressure_sample_ledger",
        ledger_dir.name,
        ledger_dir / "pressure_sample_ledger_manifest.json",
    )
    return {
        "ledger_id": ledger_dir.name,
        "ledger_dir": ledger_dir,
        "manifest": manifest,
        "pressure_samples": samples,
        "pressure_sample_summary": summary,
    }


def pressure_sample_ledger_report_payload(
    *,
    ledger_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
) -> dict[str, Any]:
    ledger_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=ledger_id if not latest else None,
        pointer_name="latest_pressure_sample_ledger",
    )
    return {
        **_read_json(ledger_dir / "pressure_sample_ledger_manifest.json"),
        "pressure_samples": _read_jsonl(ledger_dir / "pressure_samples.jsonl"),
        "pressure_sample_summary": _read_json(ledger_dir / "pressure_sample_summary.json"),
        "ledger_dir": str(ledger_dir),
    }


def validate_pressure_sample_ledger_artifact(
    *, ledger_id: str, output_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR
) -> dict[str, Any]:
    ledger_dir = output_dir / ledger_id
    manifest = _read_optional_json(ledger_dir / "pressure_sample_ledger_manifest.json") or {}
    samples = _read_jsonl(ledger_dir / "pressure_samples.jsonl")
    summary = _read_optional_json(ledger_dir / "pressure_sample_summary.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (ledger_dir / "pressure_sample_ledger_manifest.json").exists(),
            "",
        ),
        _check("samples_exists", (ledger_dir / "pressure_samples.jsonl").exists(), ""),
        _check("summary_exists", (ledger_dir / "pressure_sample_summary.json").exists(), ""),
        _check("report_exists", (ledger_dir / "pressure_sample_ledger_report.md").exists(), ""),
        _check("ledger_id_matches", manifest.get("ledger_id") == ledger_id, ""),
        _check(
            "source_modes_valid",
            all(_text(row.get("source_mode")) in SOURCE_MODES for row in samples),
            "source modes",
        ),
        _check(
            "simulation_not_approval_eligible",
            all(
                row.get("can_support_rule_approval") is False
                for row in samples
                if row.get("source_mode") == "BACKTEST_SIMULATION"
            ),
            "simulation not approval eligible",
        ),
        _check(
            "summary_counts_present",
            all(
                key in summary
                for key in (
                    "forward_samples",
                    "pit_replay_samples",
                    "simulation_samples",
                    "progress_to_requirement",
                )
            ),
            "summary source counts",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_sample_ledger_validation",
        artifact_id_key="ledger_id",
        artifact_id=ledger_id,
        checks=checks,
    )


def run_weekly_defensive_evidence_update(
    *,
    week_ending: date,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
    ledger_dir: Path = DEFAULT_PRESSURE_SAMPLE_LEDGER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    ledger_payload = _latest_ledger_payload(ledger_dir)
    samples = _records(ledger_payload.get("pressure_samples"))
    ledger_summary = _mapping(ledger_payload.get("pressure_sample_summary"))
    weekly_summary = _weekly_defensive_summary(
        week_ending=week_ending,
        samples=samples,
        ledger_summary=ledger_summary,
    )
    weekly_defensive_id = _stable_id(
        "weekly-defensive-evidence",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    weekly_dir = _unique_dir(output_dir / weekly_defensive_id)
    weekly_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_defensive_manifest",
        "weekly_defensive_id": weekly_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "weekly_defensive_manifest_path": str(weekly_dir / "weekly_defensive_manifest.json"),
        "weekly_defensive_summary_path": str(weekly_dir / "weekly_defensive_summary.json"),
        "weekly_defensive_report_path": str(weekly_dir / "weekly_defensive_report.md"),
        "reader_brief_section_path": str(weekly_dir / "reader_brief_section.md"),
        **_artifact_safety(),
    }
    reader_brief = render_weekly_defensive_reader_brief(weekly_summary)
    _write_json(weekly_dir / "weekly_defensive_manifest.json", manifest)
    _write_json(weekly_dir / "weekly_defensive_summary.json", weekly_summary)
    _write_text(
        weekly_dir / "weekly_defensive_report.md",
        render_weekly_defensive_report(manifest, weekly_summary),
    )
    _write_text(weekly_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_weekly_defensive_evidence",
        weekly_dir.name,
        weekly_dir / "weekly_defensive_manifest.json",
    )
    return {
        "weekly_defensive_id": weekly_dir.name,
        "weekly_defensive_dir": weekly_dir,
        "manifest": manifest,
        "weekly_defensive_summary": weekly_summary,
        "reader_brief_section": reader_brief,
    }


def weekly_defensive_evidence_report_payload(
    *,
    weekly_defensive_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> dict[str, Any]:
    weekly_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=weekly_defensive_id if not latest else None,
        pointer_name="latest_weekly_defensive_evidence",
    )
    return {
        **_read_json(weekly_dir / "weekly_defensive_manifest.json"),
        "weekly_defensive_summary": _read_json(weekly_dir / "weekly_defensive_summary.json"),
        "reader_brief_section": _read_text(weekly_dir / "reader_brief_section.md"),
        "weekly_defensive_dir": str(weekly_dir),
    }


def validate_weekly_defensive_evidence_artifact(
    *,
    weekly_defensive_id: str,
    output_dir: Path = DEFAULT_WEEKLY_DEFENSIVE_EVIDENCE_DIR,
) -> dict[str, Any]:
    weekly_dir = output_dir / weekly_defensive_id
    manifest = _read_optional_json(weekly_dir / "weekly_defensive_manifest.json") or {}
    summary = _read_optional_json(weekly_dir / "weekly_defensive_summary.json") or {}
    checks = [
        _check("manifest_exists", (weekly_dir / "weekly_defensive_manifest.json").exists(), ""),
        _check("summary_exists", (weekly_dir / "weekly_defensive_summary.json").exists(), ""),
        _check("report_exists", (weekly_dir / "weekly_defensive_report.md").exists(), ""),
        _check("reader_brief_exists", (weekly_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "weekly_defensive_id_matches",
            manifest.get("weekly_defensive_id") == weekly_defensive_id,
            "",
        ),
        _check(
            "rule_status_research_only",
            summary.get("defensive_rule_status") == "RESEARCH_ONLY",
            "",
        ),
        _check("policy_change_false", summary.get("policy_change_allowed") is False, ""),
        _check("safety_no_production", manifest.get("production_effect") == "none", ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_weekly_defensive_evidence_validation",
        artifact_id_key="weekly_defensive_id",
        artifact_id=weekly_defensive_id,
        checks=checks,
    )


def render_defensive_hypothesis_deep_dive_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    attribution: Mapping[str, Any],
    comparison_summary: Mapping[str, Any],
) -> str:
    rows = _records(matrix.get("regimes"))
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
            f"- can_support_rule_approval: `{manifest.get('can_support_rule_approval')}`",
            "",
            "## Regime Effects",
            *[
                "- "
                f"{row.get('regime')}: samples={row.get('sample_count')}, "
                f"avg_return_delta={row.get('avg_return_delta_vs_no_trade')}, "
                f"avg_drawdown_delta={row.get('avg_drawdown_delta_vs_no_trade')}, "
                f"effect_status=`{row.get('effect_status')}`"
                for row in rows
            ],
            "",
            "## Interpretation",
            "- simulation 中的优势只可解释为 research hypothesis，不是 production evidence。",
            "- drawdown_delta_vs_no_trade 为正值时表示相对 no_trade 回撤更小。",
            "- active exposure attribution 目前依赖 source artifact 是否披露 exposure 字段；"
            f"平均 risk_asset_exposure_delta={attribution.get('avg_risk_asset_exposure_delta')}。",
            "- no broker / no production / no auto apply。",
            "",
        ]
    )


def render_defensive_label_review_report(
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
            f"- auto_rename: `{matrix.get('auto_rename')}`",
            f"- owner_approval_required: `{matrix.get('owner_approval_required')}`",
            f"- config_change_allowed: `{matrix.get('config_change_allowed')}`",
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


def render_defensive_label_reader_brief(matrix: Mapping[str, Any]) -> str:
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


def render_defensive_failure_study_report(
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
                "- "
                f"{row.get('pattern')}: count={row.get('count')}, "
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
            "- conclusion: mitigation ideas are research-only and require forward confirmation.",
            "",
        ]
    )


def render_defensive_research_note(
    summary: Mapping[str, Any],
    matrix: Mapping[str, Any],
    label_matrix: Mapping[str, Any],
    failure_summary: Mapping[str, Any],
) -> str:
    regime_lines = [
        "- "
        f"{row.get('regime')}: `{row.get('effect_status')}`, "
        f"samples={row.get('sample_count')}, "
        f"avg_return_delta={row.get('avg_return_delta_vs_no_trade')}, "
        f"avg_drawdown_delta={row.get('avg_drawdown_delta_vs_no_trade')}"
        for row in _records(matrix.get("regimes"))
    ]
    failure_lines = [
        f"- {row.get('pattern')}: count={row.get('count')}, mitigation={row.get('mitigation')}"
        for row in _records(failure_summary.get("patterns"))
    ]
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Hypothesis Review",
            "",
            "## 当前假设",
            "",
            f"- {summary.get('hypothesis')}",
            f"- current_status: `{summary.get('current_status')}`",
            "",
            "## simulation 支持什么",
            "",
            *regime_lines,
            "",
            "## simulation 反驳什么",
            "",
            *failure_lines,
            "",
            "## forward / PIT 证据为什么不足",
            "",
            f"- forward_support: `{summary.get('forward_support')}`",
            f"- pit_replay_support: `{summary.get('pit_replay_support')}`",
            "- 当前不满足 forward pressure sample requirement，不能进入 rule approval。",
            "",
            "## defensive label 是否准确",
            "",
            f"- label_status: `{label_matrix.get('label_status')}`",
            f"- recommended_label: `{label_matrix.get('recommended_label')}`",
            "",
            "## 失败案例说明",
            "",
            "- 失败窗口用于识别何时不该启用该 variant，不用于自动修改规则。",
            "",
            "## 后续需要收集哪些 forward pressure samples",
            "",
            "- QQQ / SMH drawdown pressure trigger 后的 FORWARD_OUTCOME 样本。",
            "- 每周 pressure sample ledger 与 weekly defensive evidence update。",
            "",
            "## owner 当前应如何理解",
            "",
            f"- recommended_action: `{summary.get('recommended_action')}`",
            f"- can_support_rule_approval: `{summary.get('can_support_rule_approval')}`",
            "- no broker / no production / no auto apply。",
            "",
        ]
    )


def render_defensive_research_reader_brief(summary: Mapping[str, Any]) -> str:
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


def render_owner_decision_checklist() -> str:
    return "\n".join(
        [
            "# Owner Defensive Hypothesis Decision Checklist",
            "",
            "1. 是否接受 defensive_limited_adjustment 仍为 RESEARCH_ONLY？",
            "2. 是否接受继续使用当前名称但加 warning？",
            "3. 是否希望未来改名？",
            "4. 是否优先收集 forward pressure samples？",
            "5. 是否确认不修改 position_advisory_v1.yaml？",
            "6. 是否确认不触发 broker / production？",
            "",
        ]
    )


def render_defensive_owner_pack_report(
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
            "## Recommended Decisions",
            *[
                f"- `{row.get('decision')}`: recommended=`{row.get('recommended')}`"
                for row in _records(options.get("options"))
            ],
            "",
        ]
    )


def render_forward_pressure_capture_report(
    manifest: Mapping[str, Any],
    daily_pack: Mapping[str, Any],
    weekly_pack: Mapping[str, Any],
    event_plan: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Forward Pressure Capture Plan",
            "",
            f"- capture_plan_id: `{manifest.get('capture_plan_id')}`",
            f"- config_path: `{manifest.get('config_path')}`",
            f"- daily_enabled: `{daily_pack.get('enabled')}`",
            f"- weekly_enabled: `{weekly_pack.get('enabled')}`",
            f"- event_driven_enabled: `{event_plan.get('enabled')}`",
            f"- broker_action_allowed: `{event_plan.get('broker_action_allowed')}`",
            f"- production_effect: `{event_plan.get('production_effect')}`",
            "",
            "## Daily Commands",
            *[f"- `{cmd}`" for cmd in _command_items(daily_pack.get("commands"))],
            "",
            "## Weekly Commands",
            *[f"- `{cmd}`" for cmd in _command_items(weekly_pack.get("commands"))],
            "",
            "## Event-driven Commands",
            *[f"- `{cmd}`" for cmd in _command_items(event_plan.get("commands"))],
            "",
        ]
    )


def render_pressure_trigger_report(
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
    actions: Mapping[str, Any],
) -> str:
    metric_values = _mapping(metrics.get("metrics"))
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Trigger",
            "",
            f"- trigger_id: `{manifest.get('trigger_id')}`",
            f"- as_of: `{metrics.get('as_of')}`",
            f"- trigger_status: `{metrics.get('trigger_status')}`",
            f"- qqq_1d_return: {metric_values.get('qqq_1d_return')}",
            f"- qqq_1w_return: {metric_values.get('qqq_1w_return')}",
            f"- smh_1d_return: {metric_values.get('smh_1d_return')}",
            f"- smh_1w_return: {metric_values.get('smh_1w_return')}",
            f"- qqq_drawdown: {metric_values.get('qqq_drawdown')}",
            f"- smh_drawdown: {metric_values.get('smh_drawdown')}",
            f"- event_driven_capture_required: `{actions.get('event_driven_capture_required')}`",
            "- recommended_actions: "
            + ", ".join(_text(item) for item in _records(actions.get("recommended_actions"))),
            "- broker_action_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_pressure_capture_report(
    manifest: Mapping[str, Any],
    steps: Mapping[str, Any],
    artifacts: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Event-driven Pressure Capture",
            "",
            f"- capture_id: `{manifest.get('capture_id')}`",
            f"- trigger_id: `{manifest.get('trigger_id')}`",
            f"- status: `{manifest.get('status')}`",
            f"- trigger_status: `{steps.get('trigger_status')}`",
            f"- manual_force: `{steps.get('manual_force')}`",
            f"- policy_change_allowed: `{steps.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{steps.get('broker_action_allowed')}`",
            f"- production_effect: `{steps.get('production_effect')}`",
            "",
            "## Steps",
            *[
                f"- {row.get('step')}: `{row.get('status')}` artifact_id=`{row.get('artifact_id')}`"
                for row in _records(steps.get("steps"))
            ],
            "",
            f"- pressure_regime_tag_id: `{artifacts.get('pressure_regime_tag_id')}`",
            f"- pressure_backfill_id: `{artifacts.get('pressure_backfill_id')}`",
            f"- comparison_id: `{artifacts.get('comparison_id')}`",
            "",
        ]
    )


def render_pressure_sample_ledger_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Sample Ledger",
            "",
            f"- ledger_id: `{manifest.get('ledger_id')}`",
            f"- total_samples: {summary.get('total_samples')}",
            f"- forward_samples: {summary.get('forward_samples')}",
            f"- pit_replay_samples: {summary.get('pit_replay_samples')}",
            f"- simulation_samples: {summary.get('simulation_samples')}",
            f"- defensive_validation_relevant: {summary.get('defensive_validation_relevant')}",
            f"- rule_approval_eligible_samples: {summary.get('rule_approval_eligible_samples')}",
            "- required_forward_pressure_samples: "
            f"{summary.get('required_forward_pressure_samples')}",
            f"- progress_to_requirement: {summary.get('progress_to_requirement')}",
            "- simulation samples remain `can_support_rule_approval=false`.",
            "",
        ]
    )


def render_weekly_defensive_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Weekly Defensive Evidence",
            "",
            f"- weekly_defensive_id: `{manifest.get('weekly_defensive_id')}`",
            f"- week_ending: `{summary.get('week_ending')}`",
            f"- new_forward_pressure_samples: {summary.get('new_forward_pressure_samples')}",
            f"- new_pit_pressure_samples: {summary.get('new_pit_pressure_samples')}",
            f"- new_simulation_pressure_samples: {summary.get('new_simulation_pressure_samples')}",
            f"- total_forward_pressure_samples: {summary.get('total_forward_pressure_samples')}",
            "- required_forward_pressure_samples: "
            f"{summary.get('required_forward_pressure_samples')}",
            f"- defensive_rule_status: `{summary.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{summary.get('weekly_recommendation')}`",
            f"- owner_action_required: `{summary.get('owner_action_required')}`",
            f"- policy_change_allowed: `{summary.get('policy_change_allowed')}`",
            "- next_action: continue collecting forward pressure samples.",
            "",
        ]
    )


def render_weekly_defensive_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Weekly Defensive Evidence",
            "",
            f"- new_forward_pressure_samples: {summary.get('new_forward_pressure_samples')}",
            f"- total_forward_pressure_samples: {summary.get('total_forward_pressure_samples')}",
            f"- defensive_rule_status: `{summary.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{summary.get('weekly_recommendation')}`",
            "- next_action: `continue_pressure_sample_collection`",
            "",
        ]
    )


def _defensive_case_rows(inventory: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in inventory:
        if row.get("defensive_validation_relevant") is not True:
            continue
        variants = _mapping(row.get("variant_results"))
        defensive = _mapping(variants.get("defensive_limited_adjustment"))
        no_trade = _mapping(variants.get("no_trade"))
        if not defensive or not no_trade:
            continue
        regime = _primary_regime(row)
        return_delta = _float(defensive.get("return")) - _float(no_trade.get("return"))
        drawdown_delta = _float(defensive.get("max_drawdown")) - _float(
            no_trade.get("max_drawdown")
        )
        turnover_delta = _float(defensive.get("turnover")) - _float(no_trade.get("turnover"))
        risk_asset_delta = _risk_asset_exposure(defensive) - _risk_asset_exposure(no_trade)
        classification, support_type, failure_type = _case_classification(
            return_delta,
            drawdown_delta,
        )
        rows.append(
            {
                "case_id": _stable_id(
                    "defensive-hypothesis-case",
                    row.get("pressure_outcome_id"),
                    row.get("source_mode"),
                    row.get("as_of"),
                    row.get("window_days"),
                    regime,
                ),
                "source_mode": _text(row.get("source_mode")),
                "regime": regime,
                "as_of": _text(row.get("as_of")),
                "window_days": _int(row.get("window_days")),
                "defensive_return_delta_vs_no_trade": round(return_delta, 6),
                "defensive_drawdown_delta_vs_no_trade": round(drawdown_delta, 6),
                "turnover_delta": round(turnover_delta, 6),
                "risk_asset_exposure_delta": round(risk_asset_delta, 6),
                "support_type": support_type,
                "failure_type": failure_type,
                "classification": classification,
                "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE.get(
                    _text(row.get("source_mode")),
                    "UNKNOWN",
                ),
                "can_support_rule_approval": False,
            }
        )
    return rows


def _supporting_case_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "case_id": row.get("case_id"),
        "source_mode": row.get("source_mode"),
        "regime": row.get("regime"),
        "as_of": row.get("as_of"),
        "window_days": row.get("window_days"),
        "defensive_return_delta_vs_no_trade": row.get("defensive_return_delta_vs_no_trade"),
        "defensive_drawdown_delta_vs_no_trade": row.get("defensive_drawdown_delta_vs_no_trade"),
        "turnover_delta": row.get("turnover_delta"),
        "risk_asset_exposure_delta": row.get("risk_asset_exposure_delta"),
        "support_type": row.get("support_type"),
        "evidence_quality": row.get("evidence_quality"),
        "can_support_rule_approval": False,
    }


def _contradicting_case_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "case_id": row.get("case_id"),
        "source_mode": row.get("source_mode"),
        "regime": row.get("regime"),
        "as_of": row.get("as_of"),
        "window_days": row.get("window_days"),
        "defensive_return_delta_vs_no_trade": row.get("defensive_return_delta_vs_no_trade"),
        "defensive_drawdown_delta_vs_no_trade": row.get("defensive_drawdown_delta_vs_no_trade"),
        "failure_type": row.get("failure_type"),
        "turnover_delta": row.get("turnover_delta"),
        "risk_asset_exposure_delta": row.get("risk_asset_exposure_delta"),
        "evidence_quality": row.get("evidence_quality"),
        "can_support_rule_approval": False,
    }


def _case_classification(return_delta: float, drawdown_delta: float) -> tuple[str, str, str]:
    if return_delta >= 0 and drawdown_delta >= 0:
        return "supporting", "both", ""
    if drawdown_delta >= 0 and return_delta < 0:
        return "supporting", "drawdown_improvement", ""
    if return_delta >= 0 and drawdown_delta < 0:
        return "supporting", "return_improvement", ""
    return "contradicting", "", "worse_return_and_worse_drawdown"


def _regime_effect_matrix(cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes = []
    for regime in PRESSURE_REGIMES:
        rows = [row for row in cases if row.get("regime") == regime]
        supporting = [row for row in rows if row.get("classification") == "supporting"]
        contradicting = [row for row in rows if row.get("classification") == "contradicting"]
        avg_return = _avg(
            [_float(row.get("defensive_return_delta_vs_no_trade")) for row in rows]
        )
        avg_drawdown = _avg(
            [_float(row.get("defensive_drawdown_delta_vs_no_trade")) for row in rows]
        )
        regimes.append(
            {
                "regime": regime,
                "sample_count": len(rows),
                "supporting_cases": len(supporting),
                "contradicting_cases": len(contradicting),
                "avg_return_delta_vs_no_trade": round(avg_return, 6),
                "avg_drawdown_delta_vs_no_trade": round(avg_drawdown, 6),
                "effect_status": _effect_status(len(rows), len(supporting), len(contradicting)),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_regime_effect_matrix",
        "regimes": regimes,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _effect_status(sample_count: int, supporting: int, contradicting: int) -> str:
    if sample_count <= 0:
        return "INSUFFICIENT_DATA"
    if supporting > 0 and contradicting <= 0:
        return "SUPPORTS"
    if contradicting > 0 and supporting <= 0:
        return "CONTRADICTS"
    return "MIXED"


def _exposure_change_attribution(cases: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    exposure_values = [_float(row.get("risk_asset_exposure_delta")) for row in cases]
    turnover_values = [_float(row.get("turnover_delta")) for row in cases]
    positive_exposure = sum(1 for value in exposure_values if value > 0)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_exposure_change_attribution",
        "case_count": len(cases),
        "avg_turnover_delta": round(_avg(turnover_values), 6),
        "avg_risk_asset_exposure_delta": round(_avg(exposure_values), 6),
        "positive_risk_asset_exposure_delta_count": positive_exposure,
        "attribution_status": "SOURCE_EXPOSURE_FIELDS_LIMITED",
        "interpretation": (
            "Risk exposure attribution uses source artifact exposure fields when present; "
            "missing fields are treated as 0 and must not be read as proven de-risking."
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _label_decision_matrix(
    deep_manifest: Mapping[str, Any], matrix: Mapping[str, Any]
) -> dict[str, Any]:
    source_counts = _mapping(deep_manifest.get("source_mode_counts"))
    forward_count = _int(source_counts.get("FORWARD_OUTCOME"))
    contradicting = _int(deep_manifest.get("contradicting_case_count"))
    mixed = any(row.get("effect_status") == "MIXED" for row in _records(matrix.get("regimes")))
    label_status = (
        "POTENTIALLY_MISLEADING"
        if forward_count <= 0 or contradicting > 0 or mixed
        else "ACCEPTABLE_WITH_WARNING"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_label_decision_matrix",
        "current_label": "defensive_limited_adjustment",
        "label_status": label_status,
        "recommended_label": "risk_aware_limited_adjustment",
        "reason": (
            "Simulation does not prove consistent forward drawdown reduction in pressure regimes."
        ),
        "warning": (
            "The current defensive label can imply unproven protection; reports must state "
            "RESEARCH_ONLY and simulation-only evidence."
        ),
        "auto_rename": False,
        "owner_approval_required": True,
        "config_change_allowed": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _candidate_labels(matrix: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_labels",
        "recommended_label": matrix.get("recommended_label"),
        "labels": [
            {
                "label": "defensive_limited_adjustment",
                "description": (
                    "Current label; only partially supported by simulation, not supported by "
                    "forward evidence."
                ),
                "risk": "May imply unproven drawdown protection.",
            },
            {
                "label": "risk_aware_limited_adjustment",
                "description": (
                    "More conservative naming; emphasizes research status rather than proven "
                    "defense."
                ),
                "risk": "Still requires owner review before any config rename.",
            },
            {
                "label": "defensive_hypothesis_limited_adjustment",
                "description": "Keeps the defensive hypothesis explicit without claiming proof.",
                "risk": "Longer name, but clearer audit boundary.",
            },
            {
                "label": "active_limited_adjustment",
                "description": (
                    "Treats the behavior as active risk tilt until forward evidence matures."
                ),
                "risk": "May understate any eventual defensive benefit.",
            },
        ],
        "auto_rename": False,
        "production_effect": "none",
    }


def _rank_failure_cases(contradicting: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    for row in contradicting:
        return_loss = min(_float(row.get("defensive_return_delta_vs_no_trade")), 0.0)
        drawdown_loss = min(_float(row.get("defensive_drawdown_delta_vs_no_trade")), 0.0)
        score = abs(return_loss) + abs(drawdown_loss)
        ranked.append(
            {
                "case_id": row.get("case_id"),
                "regime": row.get("regime"),
                "as_of": row.get("as_of"),
                "window_days": row.get("window_days"),
                "relative_return_vs_no_trade": row.get(
                    "defensive_return_delta_vs_no_trade"
                ),
                "drawdown_delta_vs_no_trade": row.get(
                    "defensive_drawdown_delta_vs_no_trade"
                ),
                "turnover": row.get("turnover_delta"),
                "risk_asset_exposure_delta": row.get("risk_asset_exposure_delta"),
                "failure_severity": _failure_severity(score),
                "likely_failure_reason": _likely_failure_reason(row),
                "failure_score": round(score, 6),
            }
        )
    return sorted(ranked, key=lambda item: _float(item.get("failure_score")), reverse=True)


def _failure_severity(score: float) -> str:
    if score >= FAILURE_SEVERITY_HIGH_SCORE:
        return "HIGH"
    if score >= FAILURE_SEVERITY_MEDIUM_SCORE:
        return "MEDIUM"
    return "LOW"


def _likely_failure_reason(row: Mapping[str, Any]) -> str:
    if _float(row.get("risk_asset_exposure_delta")) > 0 and _text(row.get("regime")) in {
        "tech_drawdown",
        "risk_off",
    }:
        return "increased_risk_exposure_during_drawdown"
    if _float(row.get("turnover_delta")) > 0:
        return "late_de_risking"
    return "unknown"


def _failure_pattern_summary(ranked: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_pattern: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in ranked:
        by_pattern[_text(row.get("likely_failure_reason"), "unknown")].append(row)
    patterns = []
    for pattern in (
        "increased_risk_exposure_during_drawdown",
        "late_de_risking",
        "over_smoothing",
        "unknown",
    ):
        rows = by_pattern.get(pattern, [])
        patterns.append(
            {
                "pattern": pattern,
                "count": len(rows),
                "avg_loss_vs_no_trade": round(
                    _avg([_float(row.get("relative_return_vs_no_trade")) for row in rows]),
                    6,
                ),
                "mitigation": _mitigation_for_pattern(pattern),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_failure_pattern_summary",
        "patterns": patterns,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _mitigation_for_pattern(pattern: str) -> str:
    if pattern == "increased_risk_exposure_during_drawdown":
        return "require stronger risk-off confirmation before increasing exposure"
    if pattern == "late_de_risking":
        return "review timing guard before any de-risking relaxation"
    if pattern == "over_smoothing":
        return "compare shorter and longer adjustment windows in research only"
    return "collect more forward pressure samples before interpreting failure mechanism"


def _failure_mitigation_ideas(summary: Mapping[str, Any]) -> dict[str, Any]:
    del summary
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_failure_mitigation_ideas",
        "ideas": [
            {
                "idea_id": "block_risk_increase_in_tech_drawdown",
                "description": (
                    "Do not allow defensive_limited_adjustment to increase risk exposure "
                    "during tech_drawdown regimes."
                ),
                "requires_forward_confirmation": True,
                "auto_apply": False,
            },
            {
                "idea_id": "require_forward_pressure_confirmation",
                "description": (
                    "Keep mitigation ideas in research until FORWARD_OUTCOME pressure samples "
                    "confirm drawdown improvement."
                ),
                "requires_forward_confirmation": True,
                "auto_apply": False,
            },
        ],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _defensive_hypothesis_summary(
    deep_manifest: Mapping[str, Any], label_matrix: Mapping[str, Any]
) -> dict[str, Any]:
    source_counts = _mapping(deep_manifest.get("source_mode_counts"))
    supporting = _int(deep_manifest.get("supporting_case_count"))
    contradicting = _int(deep_manifest.get("contradicting_case_count"))
    simulation_support = "NONE"
    if _int(source_counts.get("BACKTEST_SIMULATION")) > 0:
        simulation_support = "PARTIAL" if supporting and contradicting else "PARTIAL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_hypothesis_summary",
        "hypothesis": (
            "defensive_limited_adjustment may help in selected pressure regimes, but current "
            "evidence is simulation-only."
        ),
        "current_status": "RESEARCH_ONLY",
        "simulation_support": simulation_support,
        "forward_support": "NONE" if _int(source_counts.get("FORWARD_OUTCOME")) <= 0 else "PARTIAL",
        "pit_replay_support": (
            "NONE" if _int(source_counts.get("HISTORICAL_REPLAY")) <= 0 else "PIT_WARNING"
        ),
        "label_status": label_matrix.get("label_status"),
        "recommended_action": "continue_tracking_and_consider_rename",
        "can_support_rule_approval": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _owner_decision_options() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_decision_options",
        "options": [
            {
                "decision": "continue_tracking",
                "description": (
                    "Keep defensive_limited_adjustment as research-only and continue "
                    "collecting forward pressure samples."
                ),
                "recommended": True,
            },
            {
                "decision": "rename_later",
                "description": "Consider renaming after more evidence or owner review.",
                "recommended": False,
            },
            {
                "decision": "keep_label_with_warning",
                "description": (
                    "Keep label but show explicit warning that defensive behavior is not proven."
                ),
                "recommended": True,
            },
            {
                "decision": "request_more_forward_pressure_samples",
                "description": "Prioritize event-driven pressure capture.",
                "recommended": True,
            },
        ],
        "auto_apply": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _command_pack(name: str, config: Mapping[str, Any]) -> dict[str, Any]:
    section = _mapping(config.get(name))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": f"etf_dynamic_v3_{name}_command_pack",
        "cadence": name,
        "enabled": section.get("enabled") is True,
        "commands": _command_items(section.get("commands")),
        "broker_action_allowed": False,
        "policy_change_allowed": False,
        "production_effect": "none",
    }


def _event_trigger_plan(config: Mapping[str, Any]) -> dict[str, Any]:
    section = _mapping(config.get("event_driven"))
    safety = _mapping(config.get("safety"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_event_driven_trigger_plan",
        "enabled": section.get("enabled") is True,
        "triggers": dict(_mapping(section.get("triggers"))),
        "commands": _command_items(section.get("commands")),
        "broker_action_allowed": safety.get("broker_action_allowed") is True and False,
        "policy_change_allowed": False,
        "auto_apply_policy": safety.get("auto_apply_policy") is True and False,
        "production_effect": _text(safety.get("production_effect"), "none"),
    }


def _command_items(value: Any) -> list[str]:
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        return []
    commands: list[str] = []
    for item in value:
        if isinstance(item, Mapping):
            text = _text(item.get("command") or item.get("name") or item.get("id"))
        else:
            text = _text(item)
        if text:
            commands.append(text)
    return commands


def _capture_config_checks(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily = _mapping(config.get("daily"))
    weekly = _mapping(config.get("weekly"))
    event = _mapping(config.get("event_driven"))
    triggers = _mapping(event.get("triggers"))
    safety = _mapping(config.get("safety"))
    return [
        _check("schema_version_supported", _int(config.get("schema_version")) == 1, "schema=1"),
        _check(
            "daily_commands_present",
            bool(_command_items(daily.get("commands"))),
            "daily commands",
        ),
        _check(
            "weekly_commands_present",
            bool(_command_items(weekly.get("commands"))),
            "weekly commands",
        ),
        _check(
            "event_commands_present",
            bool(_command_items(event.get("commands"))),
            "event-driven commands",
        ),
        _check(
            "trigger_thresholds_present",
            all(
                key in triggers
                for key in (
                    "qqq_1w_drawdown_pct",
                    "smh_1w_drawdown_pct",
                    "qqq_1d_drawdown_pct",
                    "smh_1d_drawdown_pct",
                )
            ),
            "trigger thresholds",
        ),
        _check(
            "safety_no_broker",
            safety.get("broker_action_allowed") is False
            and _text(safety.get("production_effect")) == "none"
            and safety.get("auto_apply_policy") is False,
            "no broker/no production/no auto apply",
        ),
        _check(
            "policy_metadata_present",
            bool(_mapping(config.get("policy_metadata")).get("owner"))
            and bool(_mapping(config.get("policy_metadata")).get("version")),
            "policy metadata",
        ),
    ]


def _quality_gate_for_pressure_trigger(
    *,
    as_of: date,
    generated: datetime,
    prices_path: Path,
    rates_path: Path,
    report_path: Path,
    enforce: bool,
) -> tuple[str, str]:
    if not enforce:
        return "SKIPPED_EXPLICIT_TEST_FIXTURE", ""
    from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
        _quality_gate_for_cached_data,
    )

    return _quality_gate_for_cached_data(
        as_of=as_of,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=report_path,
        enforce=True,
    )


def _trigger_metrics(*, as_of: date, prices_path: Path) -> dict[str, Any]:
    qqq = _price_series(prices_path, "QQQ", as_of)
    smh = _price_series(prices_path, "SMH", as_of)
    insufficient = len(qqq) < 6 or len(smh) < 6
    values = {
        "qqq_1d_return": _period_return(qqq, 1),
        "qqq_1w_return": _period_return(qqq, 5),
        "smh_1d_return": _period_return(smh, 1),
        "smh_1w_return": _period_return(smh, 5),
        "qqq_drawdown": _period_drawdown(qqq, 5),
        "smh_drawdown": _period_drawdown(smh, 5),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_trigger_metrics",
        "as_of": as_of.isoformat(),
        "metrics": {key: round(value, 6) for key, value in values.items()},
        "trigger_status": "INSUFFICIENT_DATA" if insufficient else "NO_TRIGGER",
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _trigger_status(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> str:
    if metrics.get("trigger_status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    values = _mapping(metrics.get("metrics"))
    thresholds = _mapping(_mapping(config.get("event_driven")).get("triggers"))
    triggered = (
        _float(values.get("qqq_drawdown")) <= _float(thresholds.get("qqq_1w_drawdown_pct"))
        or _float(values.get("smh_drawdown")) <= _float(thresholds.get("smh_1w_drawdown_pct"))
        or min(_float(values.get("qqq_1d_return")), 0.0)
        <= _float(thresholds.get("qqq_1d_drawdown_pct"))
        or min(_float(values.get("smh_1d_return")), 0.0)
        <= _float(thresholds.get("smh_1d_drawdown_pct"))
    )
    return "PRESSURE_TRIGGERED" if triggered else "NO_TRIGGER"


def _triggered_actions(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    status = _text(metrics.get("trigger_status"))
    event = _mapping(config.get("event_driven"))
    if status == "PRESSURE_TRIGGERED":
        commands = _command_items(event.get("commands"))
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_triggered_actions",
            "triggered": True,
            "triggers": _trigger_reasons(metrics, config),
            "recommended_actions": commands,
            "event_driven_capture_required": True,
            "broker_action_allowed": False,
            "production_effect": "none",
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_triggered_actions",
        "triggered": False,
        "triggers": [],
        "recommended_actions": ["continue_daily_monitoring"],
        "event_driven_capture_required": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _trigger_reasons(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> list[str]:
    values = _mapping(metrics.get("metrics"))
    thresholds = _mapping(_mapping(config.get("event_driven")).get("triggers"))
    checks = [
        ("qqq_1w_drawdown_pct", "qqq_drawdown"),
        ("smh_1w_drawdown_pct", "smh_drawdown"),
        ("qqq_1d_drawdown_pct", "qqq_1d_return"),
        ("smh_1d_drawdown_pct", "smh_1d_return"),
    ]
    reasons = []
    for threshold_key, metric_key in checks:
        metric_value = min(_float(values.get(metric_key)), 0.0)
        if metric_value <= _float(thresholds.get(threshold_key)):
            reasons.append(threshold_key)
    return reasons


def _price_series(prices_path: Path, symbol: str, as_of: date) -> list[tuple[date, float]]:
    if not prices_path.exists():
        return []
    frame = pd.read_csv(prices_path)
    symbol_col = "symbol" if "symbol" in frame.columns else "ticker"
    price_col = "adj_close" if "adj_close" in frame.columns else "close"
    if (
        "date" not in frame.columns
        or symbol_col not in frame.columns
        or price_col not in frame.columns
    ):
        return []
    frame = frame.loc[frame[symbol_col].astype(str) == symbol].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["_price"] = pd.to_numeric(frame[price_col], errors="coerce")
    frame = frame.dropna(subset=["_date", "_price"])
    rows = []
    for row in frame.loc[frame["_date"] <= as_of].sort_values("_date").to_dict("records"):
        price = _float(row.get("_price"))
        if price > 0:
            rows.append((row["_date"], price))
    return rows


def _period_return(series: Sequence[tuple[date, float]], periods: int) -> float:
    if len(series) <= periods:
        return 0.0
    start = series[-periods - 1][1]
    end = series[-1][1]
    if start <= 0:
        return 0.0
    return end / start - 1.0


def _period_drawdown(series: Sequence[tuple[date, float]], periods: int) -> float:
    if len(series) <= periods:
        return 0.0
    rows = series[-periods - 1 :]
    peak = max(price for _, price in rows)
    if peak <= 0:
        return 0.0
    return rows[-1][1] / peak - 1.0


def _capture_step(step: str, status: str, artifact_id: str, **extra: Any) -> dict[str, Any]:
    return {
        "step": step,
        "status": status,
        "artifact_id": artifact_id,
        "broker_action_allowed": False,
        "production_effect": "none",
        **extra,
    }


def _latest_pressure_inventory(output_dir: Path) -> list[dict[str, Any]]:
    try:
        backfill_dir = _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name="latest_pressure_outcome_backfill",
        )
    except Exception:  # noqa: BLE001
        return []
    return _read_jsonl(backfill_dir / "pressure_outcome_inventory.jsonl")


def _pressure_sample_row(row: Mapping[str, Any]) -> dict[str, Any]:
    source_mode = _text(row.get("source_mode"))
    eligible = (
        source_mode == "FORWARD_OUTCOME"
        and row.get("defensive_validation_relevant") is True
        and row.get("can_support_production") is True
    )
    return {
        "sample_id": _stable_id(
            "pressure-sample",
            row.get("pressure_outcome_id"),
            source_mode,
            row.get("as_of"),
            row.get("window_days"),
        ),
        "source_mode": source_mode,
        "as_of": row.get("as_of"),
        "regime_tags": _texts(row.get("regime_tags")),
        "defensive_validation_relevant": row.get("defensive_validation_relevant") is True,
        "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE.get(source_mode, "UNKNOWN"),
        "can_support_rule_approval": eligible,
        "source_artifact_id": row.get("source_artifact_id"),
        "source_event_id": row.get("source_event_id"),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_sample_summary(
    samples: Sequence[Mapping[str, Any]], *, required_forward: int
) -> dict[str, Any]:
    source_counts = Counter(_text(row.get("source_mode")) for row in samples)
    forward = source_counts.get("FORWARD_OUTCOME", 0)
    eligible = sum(1 for row in samples if row.get("can_support_rule_approval") is True)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_sample_summary",
        "total_samples": len(samples),
        "forward_samples": forward,
        "pit_replay_samples": source_counts.get("HISTORICAL_REPLAY", 0),
        "simulation_samples": source_counts.get("BACKTEST_SIMULATION", 0),
        "defensive_validation_relevant": sum(
            1 for row in samples if row.get("defensive_validation_relevant") is True
        ),
        "rule_approval_eligible_samples": eligible,
        "required_forward_pressure_samples": required_forward,
        "progress_to_requirement": round(
            min(forward / required_forward, 1.0) if required_forward > 0 else 0.0,
            6,
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _required_forward_samples(config_path: Path) -> int:
    config = _read_yaml_config(config_path)
    return _int(
        _mapping(config.get("validation")).get(
            "required_forward_pressure_samples",
            REQUIRED_FORWARD_PRESSURE_SAMPLES_DEFAULT,
        ),
        REQUIRED_FORWARD_PRESSURE_SAMPLES_DEFAULT,
    )


def _latest_ledger_payload(output_dir: Path) -> dict[str, Any]:
    try:
        ledger_dir = _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name="latest_pressure_sample_ledger",
        )
    except Exception:  # noqa: BLE001
        return {"pressure_samples": [], "pressure_sample_summary": {}}
    return {
        "pressure_samples": _read_jsonl(ledger_dir / "pressure_samples.jsonl"),
        "pressure_sample_summary": _read_json(ledger_dir / "pressure_sample_summary.json"),
    }


def _weekly_defensive_summary(
    *,
    week_ending: date,
    samples: Sequence[Mapping[str, Any]],
    ledger_summary: Mapping[str, Any],
) -> dict[str, Any]:
    start = week_ending - timedelta(days=6)
    weekly_samples = [
        row
        for row in samples
        if (sample_date := _date_from_any(row.get("as_of"))) is not None
        and start <= sample_date <= week_ending
    ]
    counts = Counter(_text(row.get("source_mode")) for row in weekly_samples)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_defensive_summary",
        "week_ending": week_ending.isoformat(),
        "new_forward_pressure_samples": counts.get("FORWARD_OUTCOME", 0),
        "new_pit_pressure_samples": counts.get("HISTORICAL_REPLAY", 0),
        "new_simulation_pressure_samples": counts.get("BACKTEST_SIMULATION", 0),
        "total_forward_pressure_samples": _int(ledger_summary.get("forward_samples")),
        "required_forward_pressure_samples": _int(
            ledger_summary.get(
                "required_forward_pressure_samples",
                REQUIRED_FORWARD_PRESSURE_SAMPLES_DEFAULT,
            ),
            REQUIRED_FORWARD_PRESSURE_SAMPLES_DEFAULT,
        ),
        "defensive_rule_status": "RESEARCH_ONLY",
        "weekly_recommendation": "continue_tracking",
        "owner_action_required": False,
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _primary_regime(row: Mapping[str, Any]) -> str:
    tags = _texts(row.get("regime_tags"))
    for regime in PRESSURE_REGIMES:
        if regime in tags:
            return regime
    return tags[0] if tags else "unknown"


def _risk_asset_exposure(value: Mapping[str, Any]) -> float:
    return _float(
        value.get("risk_asset_exposure"),
        _float(value.get("risk_exposure"), _float(value.get("equity_exposure"))),
    )


def _texts(value: Any) -> list[str]:
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        return []
    return [_text(item) for item in value if _text(item)]


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if value == value]
    return sum(clean) / len(clean) if clean else 0.0


def _read_yaml_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _artifact_safety() -> dict[str, Any]:
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
    }
