from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    DEFAULT_PRESSURE_REGIME_TAG_DIR,
    PRESSURE_TAGS,
    PRESSURE_VALIDATION_TAGS,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILLED_OUTCOME_DIR,
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
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)

DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_tag_diagnosis"
DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "pressure_outcome_backfill"
)
DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_pressure_compare"
)
DEFAULT_DEFENSIVE_RULE_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "defensive_rule_review"
DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "weekly_ops_decision_update"
)

SOURCE_MODES = ("FORWARD_OUTCOME", "HISTORICAL_REPLAY", "BACKTEST_SIMULATION")
EVIDENCE_QUALITY_BY_SOURCE = {
    "FORWARD_OUTCOME": "FORWARD",
    "HISTORICAL_REPLAY": "PIT_WARNING",
    "BACKTEST_SIMULATION": "SIMULATION_NOT_PIT",
}
COMPARISON_VARIANTS = (
    "no_trade",
    "defensive_limited_adjustment",
    "limited_adjustment",
    "consensus_target",
)
PRESSURE_REGIMES = ("tech_drawdown", "risk_off", "semiconductor_pullback")

# Diagnostic-only bands: they do not mutate tagging policy or define trading thresholds.
NEAR_MISS_ABS_DISTANCE = 0.005
NEAR_MISS_VOL_DISTANCE = 0.02


class DynamicV3PressureValidationError(ValueError):
    """Raised when pressure-regime validation artifacts fail closed."""


def run_pressure_tag_diagnosis(
    *,
    tag_id: str,
    output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = pressure_tag_dir / tag_id
    manifest = _read_json(source_dir / "pressure_regime_manifest.json")
    window_tags = _read_jsonl(source_dir / "regime_window_tags.jsonl")
    outcome_tags = _read_jsonl(source_dir / "outcome_regime_tags.jsonl")
    pressure_summary = _read_json(source_dir / "pressure_regime_summary.json")
    config_path = _resolve_project_path(Path(_text(manifest.get("config_path"))))
    config = _read_yaml(config_path)
    thresholds = dict(_mapping(config.get("thresholds")))
    distribution, near_misses = _threshold_distribution_and_near_misses(
        window_tags=window_tags,
        thresholds=thresholds,
        config_path=config_path,
    )
    mapping_diagnostics = _outcome_mapping_diagnostics(
        outcome_tags=outcome_tags,
        pressure_summary=pressure_summary,
        backfilled_outcome_dir=backfilled_outcome_dir,
        backtest_sim_outcome_dir=backtest_sim_outcome_dir,
    )
    diagnosis_summary = _pressure_tag_diagnosis_summary(
        pressure_summary=pressure_summary,
        distribution=distribution,
        mapping_diagnostics=mapping_diagnostics,
        near_misses=near_misses,
    )
    diagnosis_id = _stable_id("pressure-tag-diagnosis", tag_id, generated.isoformat())
    diagnosis_dir = _unique_dir(output_dir / diagnosis_id)
    diagnosis_dir.mkdir(parents=True, exist_ok=False)
    manifest_out = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_tag_diagnosis_manifest",
        "diagnosis_id": diagnosis_dir.name,
        "tag_id": tag_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "source_pressure_tag_manifest_path": str(source_dir / "pressure_regime_manifest.json"),
        "config_path": str(config_path),
        "pressure_tag_diagnosis_manifest_path": str(
            diagnosis_dir / "pressure_tag_diagnosis_manifest.json"
        ),
        "threshold_hit_distribution_path": str(
            diagnosis_dir / "threshold_hit_distribution.json"
        ),
        "near_miss_windows_path": str(diagnosis_dir / "near_miss_windows.jsonl"),
        "outcome_mapping_diagnostics_path": str(
            diagnosis_dir / "outcome_mapping_diagnostics.json"
        ),
        "pressure_tag_diagnosis_report_path": str(
            diagnosis_dir / "pressure_tag_diagnosis_report.md"
        ),
        "diagnosis_summary": diagnosis_summary,
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(diagnosis_dir / "pressure_tag_diagnosis_manifest.json", manifest_out)
    _write_json(diagnosis_dir / "threshold_hit_distribution.json", distribution)
    _write_jsonl(diagnosis_dir / "near_miss_windows.jsonl", near_misses)
    _write_json(diagnosis_dir / "outcome_mapping_diagnostics.json", mapping_diagnostics)
    _write_text(
        diagnosis_dir / "pressure_tag_diagnosis_report.md",
        render_pressure_tag_diagnosis_report(
            manifest_out,
            distribution,
            mapping_diagnostics,
            diagnosis_summary,
        ),
    )
    _update_latest_pointer(
        "latest_pressure_tag_diagnosis",
        diagnosis_dir.name,
        diagnosis_dir / "pressure_tag_diagnosis_manifest.json",
    )
    return {
        "diagnosis_id": diagnosis_dir.name,
        "diagnosis_dir": diagnosis_dir,
        "manifest": manifest_out,
        "threshold_hit_distribution": distribution,
        "near_miss_windows": near_misses,
        "outcome_mapping_diagnostics": mapping_diagnostics,
        "diagnosis_summary": diagnosis_summary,
    }


def pressure_tag_diagnosis_report_payload(
    *,
    diagnosis_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR,
) -> dict[str, Any]:
    diagnosis_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=diagnosis_id if not latest else None,
        pointer_name="latest_pressure_tag_diagnosis",
    )
    return {
        **_read_json(diagnosis_dir / "pressure_tag_diagnosis_manifest.json"),
        "threshold_hit_distribution": _read_json(
            diagnosis_dir / "threshold_hit_distribution.json"
        ),
        "near_miss_windows": _read_jsonl(diagnosis_dir / "near_miss_windows.jsonl"),
        "outcome_mapping_diagnostics": _read_json(
            diagnosis_dir / "outcome_mapping_diagnostics.json"
        ),
        "diagnosis_dir": str(diagnosis_dir),
    }


def validate_pressure_tag_diagnosis_artifact(
    *, diagnosis_id: str, output_dir: Path = DEFAULT_PRESSURE_TAG_DIAGNOSIS_DIR
) -> dict[str, Any]:
    diagnosis_dir = output_dir / diagnosis_id
    manifest = _read_optional_json(diagnosis_dir / "pressure_tag_diagnosis_manifest.json") or {}
    distribution = _read_optional_json(diagnosis_dir / "threshold_hit_distribution.json") or {}
    mapping = _read_optional_json(diagnosis_dir / "outcome_mapping_diagnostics.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (diagnosis_dir / "pressure_tag_diagnosis_manifest.json").exists(),
            "",
        ),
        _check(
            "threshold_distribution_exists",
            (diagnosis_dir / "threshold_hit_distribution.json").exists(),
            "",
        ),
        _check(
            "near_miss_windows_exists",
            (diagnosis_dir / "near_miss_windows.jsonl").exists(),
            "",
        ),
        _check(
            "mapping_diagnostics_exists",
            (diagnosis_dir / "outcome_mapping_diagnostics.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (diagnosis_dir / "pressure_tag_diagnosis_report.md").exists(),
            "",
        ),
        _check("diagnosis_id_matches", manifest.get("diagnosis_id") == diagnosis_id, ""),
        _check(
            "hit_counts_complete",
            set(_mapping(distribution.get("hit_counts"))) >= set(PRESSURE_TAGS),
            "all pressure tag buckets present",
        ),
        _check(
            "mapping_counts_present",
            "pressure_relevant_outcomes" in mapping
            and "backtest_simulation_outcomes_available" in mapping,
            "mapping diagnostics counts",
        ),
        _check(
            "safety_no_auto_change",
            manifest.get("production_effect") == "none"
            and manifest.get("policy_change_allowed") is False,
            "diagnosis does not mutate policy",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_tag_diagnosis_validation",
        artifact_id_key="diagnosis_id",
        artifact_id=diagnosis_id,
        checks=checks,
    )


def run_pressure_outcome_backfill(
    *,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfilled_outcome_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    backtest_sim_outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    pressure_payload = _latest_pressure_payload(pressure_tag_dir)
    pressure_window_tags = _records(pressure_payload.get("regime_window_tags"))
    outcome_regime_tags = _records(pressure_payload.get("outcome_regime_tags"))
    inventory: list[dict[str, Any]] = []
    inventory.extend(
        _forward_pressure_inventory(
            outcome_regime_tags=outcome_regime_tags,
            advisory_outcome_dir=advisory_outcome_dir,
            start=start,
            end=end,
        )
    )
    inventory.extend(
        _historical_replay_pressure_inventory(
            pressure_window_tags=pressure_window_tags,
            backfilled_outcome_dir=backfilled_outcome_dir,
            start=start,
            end=end,
        )
    )
    inventory.extend(
        _backtest_simulation_pressure_inventory(
            backtest_sim_outcome_dir=backtest_sim_outcome_dir,
            start=start,
            end=end,
        )
    )
    source_summary = _pressure_source_summary(inventory)
    pressure_backfill_id = _stable_id(
        "pressure-outcome-backfill",
        start.isoformat(),
        end.isoformat(),
        generated.isoformat(),
    )
    backfill_dir = _unique_dir(output_dir / pressure_backfill_id)
    backfill_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_backfill_manifest",
        "pressure_backfill_id": backfill_dir.name,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "status": "PASS" if inventory else "PASS_WITH_WARNINGS",
        "source_pressure_tag_id": _text(pressure_payload.get("tag_id")),
        "pressure_backfill_manifest_path": str(backfill_dir / "pressure_backfill_manifest.json"),
        "pressure_outcome_inventory_path": str(backfill_dir / "pressure_outcome_inventory.jsonl"),
        "pressure_source_summary_path": str(backfill_dir / "pressure_source_summary.json"),
        "pressure_backfill_report_path": str(backfill_dir / "pressure_backfill_report.md"),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(backfill_dir / "pressure_backfill_manifest.json", manifest)
    _write_jsonl(backfill_dir / "pressure_outcome_inventory.jsonl", inventory)
    _write_json(backfill_dir / "pressure_source_summary.json", source_summary)
    _write_text(
        backfill_dir / "pressure_backfill_report.md",
        render_pressure_backfill_report(manifest, source_summary),
    )
    _update_latest_pointer(
        "latest_pressure_outcome_backfill",
        backfill_dir.name,
        backfill_dir / "pressure_backfill_manifest.json",
    )
    return {
        "pressure_backfill_id": backfill_dir.name,
        "pressure_backfill_dir": backfill_dir,
        "manifest": manifest,
        "pressure_outcome_inventory": inventory,
        "pressure_source_summary": source_summary,
    }


def pressure_outcome_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
) -> dict[str, Any]:
    backfill_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=backfill_id if not latest else None,
        pointer_name="latest_pressure_outcome_backfill",
    )
    return {
        **_read_json(backfill_dir / "pressure_backfill_manifest.json"),
        "pressure_outcome_inventory": _read_jsonl(
            backfill_dir / "pressure_outcome_inventory.jsonl"
        ),
        "pressure_source_summary": _read_json(backfill_dir / "pressure_source_summary.json"),
        "pressure_backfill_dir": str(backfill_dir),
    }


def validate_pressure_outcome_backfill_artifact(
    *, backfill_id: str, output_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR
) -> dict[str, Any]:
    backfill_dir = output_dir / backfill_id
    manifest = _read_optional_json(backfill_dir / "pressure_backfill_manifest.json") or {}
    inventory = _read_jsonl(backfill_dir / "pressure_outcome_inventory.jsonl")
    summary = _read_optional_json(backfill_dir / "pressure_source_summary.json") or {}
    checks = [
        _check("manifest_exists", (backfill_dir / "pressure_backfill_manifest.json").exists(), ""),
        _check(
            "inventory_exists",
            (backfill_dir / "pressure_outcome_inventory.jsonl").exists(),
            "",
        ),
        _check("summary_exists", (backfill_dir / "pressure_source_summary.json").exists(), ""),
        _check("report_exists", (backfill_dir / "pressure_backfill_report.md").exists(), ""),
        _check(
            "backfill_id_matches",
            manifest.get("pressure_backfill_id") == backfill_id,
            "",
        ),
        _check(
            "source_modes_valid",
            all(_text(row.get("source_mode")) in SOURCE_MODES for row in inventory),
            "known source modes",
        ),
        _check(
            "simulation_not_pit",
            all(
                row.get("evidence_quality") == "SIMULATION_NOT_PIT"
                and row.get("can_support_production") is False
                for row in inventory
                if row.get("source_mode") == "BACKTEST_SIMULATION"
            ),
            "simulation evidence is research-only",
        ),
        _check(
            "summary_source_modes_complete",
            set(_mapping(summary.get("by_source_mode"))) >= set(SOURCE_MODES),
            "source mode buckets",
        ),
        _check(
            "safety_no_approval",
            manifest.get("production_effect") == "none"
            and manifest.get("policy_change_allowed") is False,
            "no approval from backfill",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_pressure_outcome_backfill_validation",
        artifact_id_key="pressure_backfill_id",
        artifact_id=backfill_id,
        checks=checks,
    )


def run_defensive_pressure_compare(
    *,
    pressure_backfill_id: str,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = backfill_dir / pressure_backfill_id
    backfill_manifest = _read_json(source_dir / "pressure_backfill_manifest.json")
    inventory = _read_jsonl(source_dir / "pressure_outcome_inventory.jsonl")
    metrics = _pressure_variant_metrics(inventory)
    pairwise = _defensive_pairwise_comparison(inventory)
    summary = _defensive_pressure_summary(metrics, pairwise)
    comparison_id = _stable_id(
        "defensive-pressure-compare",
        pressure_backfill_id,
        generated.isoformat(),
    )
    comparison_dir = _unique_dir(output_dir / comparison_id)
    comparison_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pressure_compare_manifest",
        "comparison_id": comparison_dir.name,
        "pressure_backfill_id": pressure_backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "source_pressure_backfill_id": backfill_manifest.get("pressure_backfill_id"),
        "defensive_pressure_compare_manifest_path": str(
            comparison_dir / "defensive_pressure_compare_manifest.json"
        ),
        "pressure_variant_metrics_path": str(comparison_dir / "pressure_variant_metrics.jsonl"),
        "defensive_pairwise_comparison_path": str(
            comparison_dir / "defensive_pairwise_comparison.json"
        ),
        "defensive_pressure_summary_path": str(
            comparison_dir / "defensive_pressure_summary.json"
        ),
        "defensive_pressure_compare_report_path": str(
            comparison_dir / "defensive_pressure_compare_report.md"
        ),
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(comparison_dir / "defensive_pressure_compare_manifest.json", manifest)
    _write_jsonl(comparison_dir / "pressure_variant_metrics.jsonl", metrics)
    _write_json(comparison_dir / "defensive_pairwise_comparison.json", pairwise)
    _write_json(comparison_dir / "defensive_pressure_summary.json", summary)
    _write_text(
        comparison_dir / "defensive_pressure_compare_report.md",
        render_defensive_pressure_compare_report(manifest, summary, metrics, pairwise),
    )
    _update_latest_pointer(
        "latest_defensive_pressure_compare",
        comparison_dir.name,
        comparison_dir / "defensive_pressure_compare_manifest.json",
    )
    return {
        "comparison_id": comparison_dir.name,
        "comparison_dir": comparison_dir,
        "manifest": manifest,
        "pressure_variant_metrics": metrics,
        "defensive_pairwise_comparison": pairwise,
        "defensive_pressure_summary": summary,
    }


def defensive_pressure_compare_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
) -> dict[str, Any]:
    comparison_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=comparison_id if not latest else None,
        pointer_name="latest_defensive_pressure_compare",
    )
    return {
        **_read_json(comparison_dir / "defensive_pressure_compare_manifest.json"),
        "pressure_variant_metrics": _read_jsonl(comparison_dir / "pressure_variant_metrics.jsonl"),
        "defensive_pairwise_comparison": _read_json(
            comparison_dir / "defensive_pairwise_comparison.json"
        ),
        "defensive_pressure_summary": _read_json(
            comparison_dir / "defensive_pressure_summary.json"
        ),
        "comparison_dir": str(comparison_dir),
    }


def validate_defensive_pressure_compare_artifact(
    *, comparison_id: str, output_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR
) -> dict[str, Any]:
    comparison_dir = output_dir / comparison_id
    manifest = (
        _read_optional_json(comparison_dir / "defensive_pressure_compare_manifest.json") or {}
    )
    metrics = _read_jsonl(comparison_dir / "pressure_variant_metrics.jsonl")
    summary = _read_optional_json(comparison_dir / "defensive_pressure_summary.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (comparison_dir / "defensive_pressure_compare_manifest.json").exists(),
            "",
        ),
        _check(
            "metrics_exists",
            (comparison_dir / "pressure_variant_metrics.jsonl").exists(),
            "",
        ),
        _check(
            "pairwise_exists",
            (comparison_dir / "defensive_pairwise_comparison.json").exists(),
            "",
        ),
        _check(
            "summary_exists",
            (comparison_dir / "defensive_pressure_summary.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (comparison_dir / "defensive_pressure_compare_report.md").exists(),
            "",
        ),
        _check("comparison_id_matches", manifest.get("comparison_id") == comparison_id, ""),
        _check(
            "variant_metrics_valid",
            all(
                _text(row.get("variant")) in COMPARISON_VARIANTS
                and _text(row.get("status"))
                in {"PASS", "PASS_WITH_WARNINGS", "INSUFFICIENT_DATA"}
                for row in metrics
            ),
            "comparison variants and statuses",
        ),
        _check(
            "rule_approval_guarded",
            summary.get("can_support_rule_approval") is False
            or _mapping(summary.get("source_mode_breakdown")).get("FORWARD_OUTCOME")
            == "PROVEN_DEFENSIVE",
            "rule approval requires forward evidence",
        ),
        _check(
            "safety_no_broker",
            manifest.get("broker_action_allowed") is False
            and manifest.get("production_effect") == "none",
            "no broker/no production",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_pressure_compare_validation",
        artifact_id_key="comparison_id",
        artifact_id=comparison_id,
        checks=checks,
    )


def run_defensive_rule_review(
    *,
    comparison_id: str,
    comparison_dir: Path = DEFAULT_DEFENSIVE_PRESSURE_COMPARE_DIR,
    output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = comparison_dir / comparison_id
    comparison_manifest = _read_json(source_dir / "defensive_pressure_compare_manifest.json")
    defensive_summary = _read_json(source_dir / "defensive_pressure_summary.json")
    decision_matrix = _defensive_rule_decision_matrix(defensive_summary)
    checklist = render_defensive_rule_owner_checklist(decision_matrix, defensive_summary)
    reader_brief = render_defensive_rule_reader_brief(decision_matrix)
    review_id = _stable_id("defensive-rule-review", comparison_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_rule_review_manifest",
        "review_id": review_dir.name,
        "comparison_id": comparison_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "source_comparison_id": comparison_manifest.get("comparison_id"),
        "defensive_rule_review_manifest_path": str(
            review_dir / "defensive_rule_review_manifest.json"
        ),
        "defensive_rule_decision_matrix_path": str(
            review_dir / "defensive_rule_decision_matrix.json"
        ),
        "defensive_rule_owner_checklist_path": str(
            review_dir / "defensive_rule_owner_checklist.md"
        ),
        "defensive_rule_review_report_path": str(
            review_dir / "defensive_rule_review_report.md"
        ),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "rule_approval_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    _write_json(review_dir / "defensive_rule_review_manifest.json", manifest)
    _write_json(review_dir / "defensive_rule_decision_matrix.json", decision_matrix)
    _write_text(review_dir / "defensive_rule_owner_checklist.md", checklist)
    _write_text(
        review_dir / "defensive_rule_review_report.md",
        render_defensive_rule_review_report(manifest, decision_matrix, defensive_summary),
    )
    _write_text(review_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_defensive_rule_review",
        review_dir.name,
        review_dir / "defensive_rule_review_manifest.json",
    )
    return {
        "review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "defensive_rule_decision_matrix": decision_matrix,
        "defensive_rule_owner_checklist": checklist,
        "reader_brief_section": reader_brief,
    }


def defensive_rule_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_defensive_rule_review",
    )
    return {
        **_read_json(review_dir / "defensive_rule_review_manifest.json"),
        "defensive_rule_decision_matrix": _read_json(
            review_dir / "defensive_rule_decision_matrix.json"
        ),
        "defensive_rule_owner_checklist": _read_text(
            review_dir / "defensive_rule_owner_checklist.md"
        ),
        "reader_brief_section": _read_text(review_dir / "reader_brief_section.md"),
        "review_dir": str(review_dir),
    }


def validate_defensive_rule_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "defensive_rule_review_manifest.json") or {}
    matrix = _read_optional_json(review_dir / "defensive_rule_decision_matrix.json") or {}
    checklist = _read_text(review_dir / "defensive_rule_owner_checklist.md")
    checks = [
        _check(
            "manifest_exists",
            (review_dir / "defensive_rule_review_manifest.json").exists(),
            "",
        ),
        _check(
            "decision_matrix_exists",
            (review_dir / "defensive_rule_decision_matrix.json").exists(),
            "",
        ),
        _check(
            "owner_checklist_exists",
            (review_dir / "defensive_rule_owner_checklist.md").exists(),
            "",
        ),
        _check("report_exists", (review_dir / "defensive_rule_review_report.md").exists(), ""),
        _check("reader_brief_exists", (review_dir / "reader_brief_section.md").exists(), ""),
        _check("review_id_matches", manifest.get("review_id") == review_id, ""),
        _check("rule_approval_false", matrix.get("rule_approval_allowed") is False, ""),
        _check("auto_apply_false", matrix.get("auto_apply") is False, ""),
        _check(
            "owner_checklist_complete",
            "active_limited_adjustment" in checklist
            and "no broker" in checklist
            and "forward pressure samples" in checklist,
            "required owner checklist prompts",
        ),
        _check(
            "safety_no_policy_change",
            manifest.get("policy_change_allowed") is False
            and manifest.get("production_effect") == "none",
            "no policy change",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_defensive_rule_review_validation",
        artifact_id_key="review_id",
        artifact_id=review_id,
        checks=checks,
    )


def run_weekly_ops_decision_update(
    *,
    weekly_cycle_id: str,
    pressure_backfill_id: str,
    defensive_review_id: str,
    weekly_cycle_dir: Path = DEFAULT_CONFIRMATION_CYCLE_WEEKLY_DIR,
    backfill_dir: Path = DEFAULT_PRESSURE_OUTCOME_BACKFILL_DIR,
    defensive_review_dir: Path = DEFAULT_DEFENSIVE_RULE_REVIEW_DIR,
    pressure_tag_dir: Path = DEFAULT_PRESSURE_REGIME_TAG_DIR,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    weekly_summary = _read_json(weekly_cycle_dir / weekly_cycle_id / "weekly_cycle_summary.json")
    backfill_summary = _read_json(
        backfill_dir / pressure_backfill_id / "pressure_source_summary.json"
    )
    review_matrix = _read_json(
        defensive_review_dir / defensive_review_id / "defensive_rule_decision_matrix.json"
    )
    before_count = _latest_defensive_relevant_outcomes(pressure_tag_dir)
    after_count = _int(backfill_summary.get("defensive_validation_relevant_count"))
    decision_matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_updated_weekly_decision_matrix",
        "weekly_cycle_id": weekly_cycle_id,
        "pressure_backfill_id": pressure_backfill_id,
        "defensive_review_id": defensive_review_id,
        "defensive_validation_relevant_outcomes_before": before_count,
        "defensive_validation_relevant_outcomes_after": after_count,
        "defensive_rule_status": _text(review_matrix.get("recommended_status"), "RESEARCH_ONLY"),
        "rule_approval_allowed": False,
        "weekly_recommendation": "continue_tracking",
        "owner_action_required": _text(review_matrix.get("recommended_status"))
        in {"OWNER_REVIEW_REQUIRED", "RENAME_RECOMMENDED", "DISABLE_RECOMMENDED"},
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    next_actions = _weekly_ops_next_actions(decision_matrix, backfill_summary, review_matrix)
    decision_update_id = _stable_id(
        "weekly-ops-decision-update",
        weekly_cycle_id,
        pressure_backfill_id,
        defensive_review_id,
        generated.isoformat(),
    )
    update_dir = _unique_dir(output_dir / decision_update_id)
    update_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_ops_decision_update_manifest",
        "decision_update_id": update_dir.name,
        "weekly_cycle_id": weekly_cycle_id,
        "pressure_backfill_id": pressure_backfill_id,
        "defensive_review_id": defensive_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "weekly_ops_decision_update_manifest_path": str(
            update_dir / "weekly_ops_decision_update_manifest.json"
        ),
        "updated_weekly_decision_matrix_path": str(
            update_dir / "updated_weekly_decision_matrix.json"
        ),
        "weekly_ops_next_actions_path": str(update_dir / "weekly_ops_next_actions.json"),
        "weekly_ops_decision_update_report_path": str(
            update_dir / "weekly_ops_decision_update_report.md"
        ),
        "reader_brief_section_path": str(update_dir / "reader_brief_section.md"),
        "source_weekly_summary": weekly_summary,
        "market_regime": "ai_after_chatgpt",
        **_artifact_safety(),
    }
    reader_brief = render_weekly_ops_decision_reader_brief(decision_matrix, next_actions)
    _write_json(update_dir / "weekly_ops_decision_update_manifest.json", manifest)
    _write_json(update_dir / "updated_weekly_decision_matrix.json", decision_matrix)
    _write_json(update_dir / "weekly_ops_next_actions.json", next_actions)
    _write_text(
        update_dir / "weekly_ops_decision_update_report.md",
        render_weekly_ops_decision_update_report(manifest, decision_matrix, next_actions),
    )
    _write_text(update_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_weekly_ops_decision_update",
        update_dir.name,
        update_dir / "weekly_ops_decision_update_manifest.json",
    )
    return {
        "decision_update_id": update_dir.name,
        "decision_update_dir": update_dir,
        "manifest": manifest,
        "updated_weekly_decision_matrix": decision_matrix,
        "weekly_ops_next_actions": next_actions,
        "reader_brief_section": reader_brief,
    }


def weekly_ops_decision_update_report_payload(
    *,
    decision_update_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    update_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=decision_update_id if not latest else None,
        pointer_name="latest_weekly_ops_decision_update",
    )
    return {
        **_read_json(update_dir / "weekly_ops_decision_update_manifest.json"),
        "updated_weekly_decision_matrix": _read_json(
            update_dir / "updated_weekly_decision_matrix.json"
        ),
        "weekly_ops_next_actions": _read_json(update_dir / "weekly_ops_next_actions.json"),
        "reader_brief_section": _read_text(update_dir / "reader_brief_section.md"),
        "decision_update_dir": str(update_dir),
    }


def validate_weekly_ops_decision_update_artifact(
    *,
    decision_update_id: str,
    output_dir: Path = DEFAULT_WEEKLY_OPS_DECISION_UPDATE_DIR,
) -> dict[str, Any]:
    update_dir = output_dir / decision_update_id
    manifest = _read_optional_json(update_dir / "weekly_ops_decision_update_manifest.json") or {}
    matrix = _read_optional_json(update_dir / "updated_weekly_decision_matrix.json") or {}
    actions = _read_optional_json(update_dir / "weekly_ops_next_actions.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (update_dir / "weekly_ops_decision_update_manifest.json").exists(),
            "",
        ),
        _check(
            "decision_matrix_exists",
            (update_dir / "updated_weekly_decision_matrix.json").exists(),
            "",
        ),
        _check("next_actions_exists", (update_dir / "weekly_ops_next_actions.json").exists(), ""),
        _check(
            "report_exists",
            (update_dir / "weekly_ops_decision_update_report.md").exists(),
            "",
        ),
        _check("reader_brief_exists", (update_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "decision_update_id_matches",
            manifest.get("decision_update_id") == decision_update_id,
            "",
        ),
        _check("policy_change_disallowed", matrix.get("policy_change_allowed") is False, ""),
        _check("broker_action_disallowed", matrix.get("broker_action_allowed") is False, ""),
        _check("rule_approval_disallowed", matrix.get("rule_approval_allowed") is False, ""),
        _check(
            "next_actions_present",
            bool(_records(actions.get("next_actions"))),
            "weekly next actions",
        ),
        _check(
            "safety_no_production",
            manifest.get("production_effect") == "none"
            and manifest.get("broker_action_allowed") is False,
            "no production/no broker",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_weekly_ops_decision_update_validation",
        artifact_id_key="decision_update_id",
        artifact_id=decision_update_id,
        checks=checks,
    )


def render_pressure_tag_diagnosis_report(
    manifest: Mapping[str, Any],
    distribution: Mapping[str, Any],
    mapping: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    hit_counts = _mapping(distribution.get("hit_counts"))
    near_counts = _mapping(distribution.get("near_miss_counts"))
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Tag Diagnosis",
            "",
            f"- diagnosis_id: `{manifest.get('diagnosis_id')}`",
            f"- source_tag_id: `{manifest.get('tag_id')}`",
            f"- primary_reason: `{summary.get('primary_reason')}`",
            f"- pressure_relevant_outcomes: {mapping.get('pressure_relevant_outcomes')}",
            f"- forward_outcomes_scanned: {mapping.get('forward_outcomes_scanned')}",
            "- backtest_simulation_outcomes_scanned: "
            f"{mapping.get('backtest_simulation_outcomes_scanned')}",
            "- backtest_simulation_outcomes_available: "
            f"{mapping.get('backtest_simulation_outcomes_available')}",
            f"- tech_drawdown_hits: {hit_counts.get('tech_drawdown')}",
            f"- risk_off_hits: {hit_counts.get('risk_off')}",
            f"- semiconductor_pullback_hits: {hit_counts.get('semiconductor_pullback')}",
            f"- near_miss_windows: {sum(_int(v) for v in near_counts.values())}",
            "- threshold_adjustment_recommended: "
            f"`{summary.get('threshold_adjustment_recommended')}`",
            "- tagging_logic_adjustment_recommended: "
            f"`{summary.get('tagging_logic_adjustment_recommended')}`",
            "- conclusion: forward outcome windows did not overlap tagged pressure windows; "
            "simulation pressure evidence must be backfilled as research-only, "
            "not forward evidence.",
            "- broker_action_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_pressure_backfill_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    by_source = _mapping(summary.get("by_source_mode"))
    by_regime = _mapping(summary.get("by_regime"))
    return "\n".join(
        [
            "# Dynamic Rescue Pressure Outcome Backfill",
            "",
            f"- pressure_backfill_id: `{manifest.get('pressure_backfill_id')}`",
            f"- date_range: `{manifest.get('start')}` to `{manifest.get('end')}`",
            f"- total_pressure_outcomes: {summary.get('total_pressure_outcomes')}",
            f"- FORWARD_OUTCOME: {by_source.get('FORWARD_OUTCOME')}",
            f"- HISTORICAL_REPLAY: {by_source.get('HISTORICAL_REPLAY')}",
            f"- BACKTEST_SIMULATION: {by_source.get('BACKTEST_SIMULATION')}",
            f"- tech_drawdown: {by_regime.get('tech_drawdown')}",
            f"- risk_off: {by_regime.get('risk_off')}",
            f"- semiconductor_pullback: {by_regime.get('semiconductor_pullback')}",
            "- defensive_validation_relevant_count: "
            f"{summary.get('defensive_validation_relevant_count')}",
            "- research_evidence: `HISTORICAL_REPLAY`, `BACKTEST_SIMULATION`",
            "- forward_confirmation_evidence: `FORWARD_OUTCOME` only",
            "- defensive_rule_approval_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_defensive_pressure_compare_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    pairwise: Mapping[str, Any],
) -> str:
    source_modes = _mapping(summary.get("source_mode_breakdown"))
    defensive_rows = [
        row
        for row in metrics
        if row.get("variant") == "defensive_limited_adjustment"
        and row.get("regime") in PRESSURE_REGIMES
    ]
    lines = [
        "# Dynamic Rescue Defensive Pressure Compare",
        "",
        f"- comparison_id: `{manifest.get('comparison_id')}`",
        f"- pressure_backfill_id: `{manifest.get('pressure_backfill_id')}`",
        f"- defensive_status: `{summary.get('defensive_status')}`",
        f"- can_support_rule_approval: `{summary.get('can_support_rule_approval')}`",
        f"- FORWARD_OUTCOME: `{source_modes.get('FORWARD_OUTCOME')}`",
        f"- HISTORICAL_REPLAY: `{source_modes.get('HISTORICAL_REPLAY')}`",
        f"- BACKTEST_SIMULATION: `{source_modes.get('BACKTEST_SIMULATION')}`",
        "",
        "## Defensive Limited Adjustment",
    ]
    for row in defensive_rows:
        lines.append(
            "- "
            f"{row.get('source_mode')} / {row.get('regime')}: "
            f"sample_count={row.get('sample_count')}, "
            f"avg_relative_to_no_trade={row.get('avg_relative_to_no_trade')}, "
            f"drawdown_delta_vs_no_trade={row.get('drawdown_delta_vs_no_trade')}, "
            f"status=`{row.get('status')}`"
        )
    lines.extend(
        [
            "",
            "## Pairwise",
            f"- comparison_count: {len(_records(pairwise.get('comparisons')))}",
            "- conclusion: simulation pressure samples can inform research, but forward "
            "pressure evidence is insufficient for approval.",
            "- policy_change_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )
    return "\n".join(lines)


def render_defensive_rule_review_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Defensive Rule Review",
            "",
            f"- review_id: `{manifest.get('review_id')}`",
            f"- rule_name: `{matrix.get('rule_name')}`",
            f"- current_status: `{matrix.get('current_status')}`",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- auto_apply: `{matrix.get('auto_apply')}`",
            f"- owner_approval_required: `{matrix.get('owner_approval_required')}`",
            f"- reason: {matrix.get('reason')}",
            f"- source_mode_breakdown: `{defensive_summary.get('source_mode_breakdown')}`",
            "- conclusion: defensive_limited_adjustment remains research-only until "
            "forward pressure samples prove drawdown improvement.",
            "- broker_action_allowed: `false`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_defensive_rule_owner_checklist(
    matrix: Mapping[str, Any], defensive_summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Defensive Rule Owner Checklist",
            "",
            "- 是否继续使用 `defensive_limited_adjustment` 这个名称？",
            "- 是否将其改名为 `active_limited_adjustment` 或 `risk_aware_limited_adjustment`？",
            "- 是否继续作为 observe-only variant？",
            "- 是否需要 forward pressure samples 后再评估？",
            "- 是否禁止其进入默认执行规则？",
            "- 是否继续 no broker / no production？",
            "",
            "## 当前判断",
            "",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- defensive_status: `{defensive_summary.get('defensive_status')}`",
            "- local drawdown_delta convention: positive means less drawdown than no_trade.",
            "",
        ]
    )


def render_defensive_rule_reader_brief(matrix: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Defensive Rule Status",
            "",
            f"- rule_name: `{matrix.get('rule_name')}`",
            f"- recommended_status: `{matrix.get('recommended_status')}`",
            f"- rule_approval_allowed: `{matrix.get('rule_approval_allowed')}`",
            f"- auto_apply: `{matrix.get('auto_apply')}`",
            "- production_effect: `none`",
            "",
        ]
    )


def render_weekly_ops_decision_update_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    actions: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Rescue Weekly Operations Decision Update",
            "",
            f"- decision_update_id: `{manifest.get('decision_update_id')}`",
            f"- weekly_cycle_id: `{matrix.get('weekly_cycle_id')}`",
            f"- pressure_backfill_id: `{matrix.get('pressure_backfill_id')}`",
            f"- defensive_review_id: `{matrix.get('defensive_review_id')}`",
            "- defensive_validation_relevant_outcomes_before: "
            f"{matrix.get('defensive_validation_relevant_outcomes_before')}",
            "- defensive_validation_relevant_outcomes_after: "
            f"{matrix.get('defensive_validation_relevant_outcomes_after')}",
            f"- defensive_rule_status: `{matrix.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{matrix.get('weekly_recommendation')}`",
            f"- owner_action_required: `{matrix.get('owner_action_required')}`",
            f"- policy_change_allowed: `{matrix.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{matrix.get('broker_action_allowed')}`",
            "",
            "## Next Actions",
            *[
                f"- {row.get('action')}: `{row.get('priority')}` - {row.get('reason')}"
                for row in _records(actions.get("next_actions"))
            ],
            "",
        ]
    )


def render_weekly_ops_decision_reader_brief(
    matrix: Mapping[str, Any], actions: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Weekly Operations Decision",
            "",
            "- pressure_sample_status: "
            f"`{matrix.get('defensive_validation_relevant_outcomes_after')}` "
            "relevant pressure outcomes",
            f"- defensive_rule_status: `{matrix.get('defensive_rule_status')}`",
            f"- weekly_recommendation: `{matrix.get('weekly_recommendation')}`",
            f"- policy_change_allowed: `{matrix.get('policy_change_allowed')}`",
            f"- broker_action_allowed: `{matrix.get('broker_action_allowed')}`",
            "- next_actions: "
            + ", ".join(_text(row.get("action")) for row in _records(actions.get("next_actions"))),
            "",
        ]
    )


def _threshold_distribution_and_near_misses(
    *,
    window_tags: Sequence[Mapping[str, Any]],
    thresholds: Mapping[str, Any],
    config_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    hit_counts = Counter(
        tag for row in window_tags for tag in _records_to_texts(row.get("regime_tags"))
    )
    vol_values = sorted(
        _float(_mapping(row.get("metrics")).get("realized_volatility"))
        for row in window_tags
    )
    vol_threshold = _percentile(
        vol_values,
        _float(thresholds.get("risk_off_volatility_percentile")),
    )
    near_misses: list[dict[str, Any]] = []
    for row in window_tags:
        metrics = _mapping(row.get("metrics"))
        tags = set(_records_to_texts(row.get("regime_tags")))
        _append_near_miss(
            near_misses,
            row=row,
            metric_name="qqq_drawdown",
            candidate_tag="tech_drawdown",
            actual=_float(metrics.get("qqq_drawdown")),
            threshold=_float(thresholds.get("tech_drawdown_pct")),
            already_hit="tech_drawdown" in tags,
            distance_band=NEAR_MISS_ABS_DISTANCE,
        )
        _append_near_miss(
            near_misses,
            row=row,
            metric_name="smh_drawdown",
            candidate_tag="semiconductor_pullback",
            actual=_float(metrics.get("smh_drawdown")),
            threshold=_float(thresholds.get("semiconductor_pullback_pct")),
            already_hit="semiconductor_pullback" in tags,
            distance_band=NEAR_MISS_ABS_DISTANCE,
        )
        qqq_drawdown = _float(metrics.get("qqq_drawdown"))
        vol = _float(metrics.get("realized_volatility"))
        if (
            "risk_off" not in tags
            and qqq_drawdown <= _float(thresholds.get("tech_drawdown_pct"))
            and 0 <= vol_threshold - vol <= NEAR_MISS_VOL_DISTANCE
        ):
            near_misses.append(
                _near_miss_row(
                    row=row,
                    candidate_tag="risk_off",
                    actual_metric=round(vol, 6),
                    threshold=round(vol_threshold, 6),
                    distance=round(vol_threshold - vol, 6),
                )
            )
    near_counts = Counter(_text(row.get("candidate_tag")) for row in near_misses)
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_threshold_hit_distribution",
            "config_path": str(config_path),
            "thresholds": {
                "tech_drawdown_pct": _float(thresholds.get("tech_drawdown_pct")),
                "semiconductor_pullback_pct": _float(
                    thresholds.get("semiconductor_pullback_pct")
                ),
                "risk_off_volatility_percentile": _float(
                    thresholds.get("risk_off_volatility_percentile")
                ),
                "risk_off_realized_volatility_threshold": round(vol_threshold, 6),
            },
            "hit_counts": {tag: hit_counts.get(tag, 0) for tag in PRESSURE_TAGS},
            "near_miss_counts": {tag: near_counts.get(tag, 0) for tag in PRESSURE_REGIMES},
            "production_effect": "none",
            "broker_action_allowed": False,
        },
        near_misses,
    )


def _append_near_miss(
    rows: list[dict[str, Any]],
    *,
    row: Mapping[str, Any],
    metric_name: str,
    candidate_tag: str,
    actual: float,
    threshold: float,
    already_hit: bool,
    distance_band: float,
) -> None:
    del metric_name
    distance = actual - threshold
    if already_hit or not (0 < distance <= distance_band):
        return
    rows.append(
        _near_miss_row(
            row=row,
            candidate_tag=candidate_tag,
            actual_metric=round(actual, 6),
            threshold=round(threshold, 6),
            distance=round(distance, 6),
        )
    )


def _near_miss_row(
    *,
    row: Mapping[str, Any],
    candidate_tag: str,
    actual_metric: float,
    threshold: float,
    distance: float,
) -> dict[str, Any]:
    return {
        "window_id": _text(row.get("window_id")),
        "start_date": _text(row.get("start_date")),
        "end_date": _text(row.get("end_date")),
        "candidate_tag": candidate_tag,
        "actual_metric": actual_metric,
        "threshold": threshold,
        "distance_to_threshold": distance,
        "near_miss": True,
        "suggested_action": "review_threshold_not_auto_change",
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _outcome_mapping_diagnostics(
    *,
    outcome_tags: Sequence[Mapping[str, Any]],
    pressure_summary: Mapping[str, Any],
    backfilled_outcome_dir: Path,
    backtest_sim_outcome_dir: Path,
) -> dict[str, Any]:
    forward_scanned = len(outcome_tags)
    with_tags = sum(1 for row in outcome_tags if _records_to_texts(row.get("regime_tags")))
    missing_tags = forward_scanned - with_tags
    backfilled_available = _latest_jsonl_count(
        backfilled_outcome_dir,
        "latest_backfilled_outcome",
        "replay_outcome_windows.jsonl",
    )
    sim_available, sim_pressure_available = _latest_backtest_sim_counts(backtest_sim_outcome_dir)
    mapping_failures = []
    if _int(pressure_summary.get("pressure_window_count")) > 0 and _int(
        pressure_summary.get("pressure_tagged_outcomes")
    ) == 0:
        mapping_failures.append(
            {
                "reason": "outcome_window_not_mapped_to_regime_window",
                "count": forward_scanned,
            }
        )
    if missing_tags:
        mapping_failures.append(
            {"reason": "outcomes_missing_regime_tags", "count": missing_tags}
        )
    if sim_pressure_available:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_backtest_simulation",
                "count": sim_pressure_available,
            }
        )
    if backfilled_available:
        mapping_failures.append(
            {
                "reason": "pressure_tagger_does_not_scan_historical_replay_backfill",
                "count": backfilled_available,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_mapping_diagnostics",
        "forward_outcomes_scanned": forward_scanned,
        "historical_replay_outcomes_scanned": 0,
        "backtest_simulation_outcomes_scanned": 0,
        "historical_replay_outcomes_available": backfilled_available,
        "backtest_simulation_outcomes_available": sim_available,
        "backtest_simulation_pressure_outcomes_available": sim_pressure_available,
        "outcomes_with_regime_tags": with_tags,
        "outcomes_missing_regime_tags": missing_tags,
        "pressure_relevant_outcomes": _int(
            pressure_summary.get("defensive_validation_relevant_outcomes")
        ),
        "mapping_failures": mapping_failures,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_tag_diagnosis_summary(
    *,
    pressure_summary: Mapping[str, Any],
    distribution: Mapping[str, Any],
    mapping_diagnostics: Mapping[str, Any],
    near_misses: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pressure_window_count = _int(pressure_summary.get("pressure_window_count"))
    relevant = _int(mapping_diagnostics.get("pressure_relevant_outcomes"))
    sim_pressure = _int(mapping_diagnostics.get("backtest_simulation_pressure_outcomes_available"))
    if pressure_window_count <= 0:
        primary = "thresholds_or_price_proxy_produced_no_pressure_windows"
    elif relevant <= 0 and sim_pressure > 0:
        primary = "forward_outcome_mapping_gap_and_simulation_not_scanned"
    elif relevant <= 0:
        primary = "forward_outcome_window_not_mapped_to_pressure_window"
    else:
        primary = "pressure_outcome_mapping_available"
    hit_counts = _mapping(distribution.get("hit_counts"))
    threshold_maybe_strict = (
        sum(_int(hit_counts.get(tag)) for tag in PRESSURE_REGIMES) == 0 and bool(near_misses)
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_tag_diagnosis_summary",
        "primary_reason": primary,
        "threshold_too_strict": threshold_maybe_strict,
        "outcome_mapping_problem": relevant <= 0 and pressure_window_count > 0,
        "simulation_not_scanned": sim_pressure > 0,
        "near_miss_window_count": len(near_misses),
        "threshold_adjustment_recommended": (
            "review_only_not_auto_change" if threshold_maybe_strict else "not_primary"
        ),
        "tagging_logic_adjustment_recommended": (
            "include_research_only_simulation_inventory"
            if sim_pressure > 0
            else "review_forward_window_mapping"
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _forward_pressure_inventory(
    *,
    outcome_regime_tags: Sequence[Mapping[str, Any]],
    advisory_outcome_dir: Path,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    outcome_rows_by_key = _forward_outcome_rows_by_key(advisory_outcome_dir)
    rows = []
    for tag_row in outcome_regime_tags:
        as_of = _date_from_any(tag_row.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            continue
        tags = _records_to_texts(tag_row.get("regime_tags"))
        if not (set(tags) & PRESSURE_VALIDATION_TAGS):
            continue
        key = (
            _text(tag_row.get("outcome_id")),
            _text(tag_row.get("daily_advisory_id")),
            _int(tag_row.get("window_days")),
        )
        source_window = outcome_rows_by_key.get(key, {})
        rows.append(
            _pressure_inventory_row(
                source_mode="FORWARD_OUTCOME",
                source_artifact_id=_text(tag_row.get("outcome_id")),
                source_event_id=_text(tag_row.get("daily_advisory_id")),
                as_of=_text(tag_row.get("as_of")),
                window_days=_int(tag_row.get("window_days")),
                regime_tags=tags,
                variant_results=_forward_variant_results(source_window),
                outcome_status=_text(source_window.get("outcome_status"), "UNKNOWN"),
            )
        )
    return rows


def _historical_replay_pressure_inventory(
    *,
    pressure_window_tags: Sequence[Mapping[str, Any]],
    backfilled_outcome_dir: Path,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    source_dir = _optional_latest_dir(backfilled_outcome_dir, "latest_backfilled_outcome")
    if source_dir is None:
        return []
    manifest = _read_optional_json(source_dir / "backfill_manifest.json") or {}
    pressure_by_end = _pressure_tags_by_end(pressure_window_tags)
    grouped: dict[tuple[str, str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(source_dir / "replay_outcome_windows.jsonl"):
        as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
        if as_of is None or not (start <= as_of <= end):
            continue
        grouped[
            (
                _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
                _text(row.get("as_of") or row.get("start_date")),
                _int(row.get("window_days")),
            )
        ].append(row)
    inventory = []
    for (event_id, as_of_text, window_days), rows in sorted(grouped.items()):
        first = rows[0]
        tags = _records_to_texts(first.get("regime_tags"))
        if not tags:
            tags = pressure_by_end.get((_text(first.get("end_date")), window_days), [])
        if not (set(tags) & PRESSURE_VALIDATION_TAGS):
            continue
        inventory.append(
            _pressure_inventory_row(
                source_mode="HISTORICAL_REPLAY",
                source_artifact_id=_text(manifest.get("backfill_id"), source_dir.name),
                source_event_id=event_id,
                as_of=as_of_text,
                window_days=window_days,
                regime_tags=tags,
                variant_results=_variant_results_from_rows(rows),
                outcome_status=_text(first.get("outcome_status"), "UNKNOWN"),
            )
        )
    return inventory


def _backtest_simulation_pressure_inventory(
    *, backtest_sim_outcome_dir: Path, start: date, end: date
) -> list[dict[str, Any]]:
    source_dir = _optional_latest_dir(backtest_sim_outcome_dir, "latest_backtest_sim_outcome")
    if source_dir is None:
        return []
    manifest = _read_json(source_dir / "sim_outcome_manifest.json")
    grouped: dict[tuple[str, str, int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in _read_jsonl(source_dir / "simulated_outcome_windows.jsonl"):
        as_of = _date_from_any(row.get("as_of") or row.get("start_date"))
        if as_of is None or not (start <= as_of <= end):
            continue
        regime = _text(row.get("regime_label"), "unknown")
        if regime not in PRESSURE_VALIDATION_TAGS:
            continue
        grouped[
            (
                _text(row.get("sim_event_id")),
                _text(row.get("as_of") or row.get("start_date")),
                _int(row.get("window_days")),
                regime,
            )
        ].append(row)
    inventory = []
    for (event_id, as_of_text, window_days, regime), rows in sorted(grouped.items()):
        first = rows[0]
        inventory.append(
            _pressure_inventory_row(
                source_mode="BACKTEST_SIMULATION",
                source_artifact_id=_text(manifest.get("sim_outcome_id"), source_dir.name),
                source_event_id=event_id,
                as_of=as_of_text,
                window_days=window_days,
                regime_tags=[regime],
                variant_results=_variant_results_from_rows(rows),
                outcome_status=_text(first.get("outcome_status"), "UNKNOWN"),
            )
        )
    return inventory


def _pressure_inventory_row(
    *,
    source_mode: str,
    source_artifact_id: str,
    source_event_id: str,
    as_of: str,
    window_days: int,
    regime_tags: Sequence[str],
    variant_results: Mapping[str, Any],
    outcome_status: str,
) -> dict[str, Any]:
    pressure = bool(set(regime_tags) & PRESSURE_VALIDATION_TAGS)
    defensive_relevant = pressure and window_days in {5, 10, 20}
    can_support_production = (
        source_mode == "FORWARD_OUTCOME" and outcome_status == "AVAILABLE" and pressure
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "pressure_outcome_id": _stable_id(
            "pressure-outcome",
            source_mode,
            source_artifact_id,
            source_event_id,
            as_of,
            window_days,
            ",".join(sorted(regime_tags)),
        ),
        "source_mode": source_mode,
        "source_artifact_id": source_artifact_id,
        "source_event_id": source_event_id,
        "as_of": as_of,
        "window_days": window_days,
        "regime_tags": list(regime_tags),
        "pressure_regime": pressure,
        "defensive_validation_relevant": defensive_relevant,
        "outcome_status": outcome_status,
        "variant_results": dict(variant_results),
        "evidence_quality": EVIDENCE_QUALITY_BY_SOURCE[source_mode],
        "can_support_production": can_support_production,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_source_summary(inventory: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_source = Counter(_text(row.get("source_mode")) for row in inventory)
    by_regime: Counter[str] = Counter()
    for row in inventory:
        for tag in _records_to_texts(row.get("regime_tags")):
            by_regime[tag] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pressure_source_summary",
        "total_pressure_outcomes": len(inventory),
        "by_source_mode": {mode: by_source.get(mode, 0) for mode in SOURCE_MODES},
        "by_regime": {tag: by_regime.get(tag, 0) for tag in PRESSURE_TAGS},
        "defensive_validation_relevant_count": sum(
            1 for row in inventory if row.get("defensive_validation_relevant") is True
        ),
        "can_support_production_count": sum(
            1 for row in inventory if row.get("can_support_production") is True
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _pressure_variant_metrics(inventory: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for source_mode in SOURCE_MODES:
        for regime in PRESSURE_REGIMES:
            regime_rows = [
                row
                for row in inventory
                if row.get("source_mode") == source_mode
                and regime in _records_to_texts(row.get("regime_tags"))
                and row.get("defensive_validation_relevant") is True
            ]
            for variant in COMPARISON_VARIANTS:
                variant_samples = [
                    _variant_sample_metrics(row, variant)
                    for row in regime_rows
                    if variant in _mapping(row.get("variant_results"))
                ]
                result.append(
                    _pressure_variant_metric_row(
                        source_mode=source_mode,
                        regime=regime,
                        variant=variant,
                        samples=variant_samples,
                    )
                )
    return result


def _pressure_variant_metric_row(
    *,
    source_mode: str,
    regime: str,
    variant: str,
    samples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    sample_count = len(samples)
    status = (
        "INSUFFICIENT_DATA"
        if sample_count <= 0
        else "PASS"
        if sample_count >= 5
        else "PASS_WITH_WARNINGS"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "source_mode": source_mode,
        "regime": regime,
        "variant": variant,
        "sample_count": sample_count,
        "avg_return": round(_avg([_float(row.get("return")) for row in samples]), 6),
        "avg_relative_to_no_trade": round(
            _avg([_float(row.get("relative_to_no_trade")) for row in samples]), 6
        ),
        "win_rate_vs_no_trade": (
            round(
                sum(1 for row in samples if _float(row.get("relative_to_no_trade")) > 0)
                / sample_count,
                6,
            )
            if sample_count
            else 0.0
        ),
        "avg_max_drawdown": round(
            _avg([_float(row.get("max_drawdown")) for row in samples]), 6
        ),
        "drawdown_delta_vs_no_trade": round(
            _avg([_float(row.get("drawdown_delta_vs_no_trade")) for row in samples]), 6
        ),
        "avg_turnover": round(_avg([_float(row.get("turnover")) for row in samples]), 6),
        "status": status,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_pairwise_comparison(inventory: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    comparisons = []
    for source_mode in SOURCE_MODES:
        for regime in PRESSURE_REGIMES:
            rows = [
                row
                for row in inventory
                if row.get("source_mode") == source_mode
                and regime in _records_to_texts(row.get("regime_tags"))
                and row.get("defensive_validation_relevant") is True
            ]
            samples = [
                _variant_sample_metrics(row, "defensive_limited_adjustment")
                for row in rows
                if "defensive_limited_adjustment" in _mapping(row.get("variant_results"))
                and "no_trade" in _mapping(row.get("variant_results"))
            ]
            return_delta = round(
                _avg([_float(row.get("relative_to_no_trade")) for row in samples]), 6
            )
            drawdown_delta = round(
                _avg([_float(row.get("drawdown_delta_vs_no_trade")) for row in samples]),
                6,
            )
            sample_count = len(samples)
            win_rate = (
                round(
                    sum(1 for row in samples if _float(row.get("relative_to_no_trade")) > 0)
                    / sample_count,
                    6,
                )
                if sample_count
                else 0.0
            )
            comparisons.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "source_mode": source_mode,
                    "regime": regime,
                    "variant_a": "defensive_limited_adjustment",
                    "variant_b": "no_trade",
                    "sample_count": sample_count,
                    "return_delta": return_delta,
                    "drawdown_delta": drawdown_delta,
                    "win_rate": win_rate,
                    "conclusion": _pairwise_conclusion(sample_count, return_delta, drawdown_delta),
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pairwise_comparison",
        "comparisons": comparisons,
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_pressure_summary(
    metrics: Sequence[Mapping[str, Any]], pairwise: Mapping[str, Any]
) -> dict[str, Any]:
    source_breakdown = {
        mode: _source_mode_defensive_status(mode, _records(pairwise.get("comparisons")))
        for mode in SOURCE_MODES
    }
    forward_status = source_breakdown["FORWARD_OUTCOME"]
    sim_status = source_breakdown["BACKTEST_SIMULATION"]
    if forward_status == "PROVEN_DEFENSIVE":
        overall = "PROVEN_DEFENSIVE"
    elif forward_status == "FAILS_DEFENSIVE_EXPECTATION":
        overall = "FAILS_DEFENSIVE_EXPECTATION"
    elif sim_status in {"PROVEN_DEFENSIVE", "PARTIALLY_DEFENSIVE"}:
        overall = "INSUFFICIENT_FORWARD_DATA"
    elif sim_status == "FAILS_DEFENSIVE_EXPECTATION":
        overall = "FAILS_DEFENSIVE_EXPECTATION"
    else:
        overall = "NOT_PROVEN_DEFENSIVE"
    can_approve = forward_status == "PROVEN_DEFENSIVE"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_pressure_summary",
        "defensive_status": overall,
        "source_mode_breakdown": source_breakdown,
        "primary_conclusion": (
            "defensive_limited_adjustment remains research-only and requires "
            "forward pressure samples."
            if not can_approve
            else "forward pressure samples support owner review for defensive behavior."
        ),
        "can_support_rule_approval": can_approve,
        "metrics_row_count": len(metrics),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _defensive_rule_decision_matrix(defensive_summary: Mapping[str, Any]) -> dict[str, Any]:
    status = _text(defensive_summary.get("defensive_status"), "NOT_PROVEN_DEFENSIVE")
    if status == "PROVEN_DEFENSIVE":
        recommended = "OWNER_REVIEW_REQUIRED"
    elif status == "FAILS_DEFENSIVE_EXPECTATION":
        recommended = "RENAME_RECOMMENDED"
    elif status == "INSUFFICIENT_FORWARD_DATA":
        recommended = "RESEARCH_ONLY"
    else:
        recommended = "OBSERVE_ONLY"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_rule_decision_matrix",
        "rule_name": "defensive_limited_adjustment",
        "current_status": status,
        "recommended_status": recommended,
        "rule_approval_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
        "reason": (
            "Simulation evidence is not enough to approve defensive behavior; "
            "forward pressure samples remain insufficient."
        ),
        "required_next_evidence": [
            "at least 5 forward pressure regime events",
            "drawdown_delta_vs_no_trade >= 0 under the local convention that "
            "positive means lower drawdown",
            "win_rate_vs_no_trade >= 0.50",
        ],
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_ops_next_actions(
    matrix: Mapping[str, Any],
    backfill_summary: Mapping[str, Any],
    review_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    forward_count = _int(_mapping(backfill_summary.get("by_source_mode")).get("FORWARD_OUTCOME"))
    actions = [
        {
            "action": "continue_pressure_sample_collection",
            "priority": "HIGH",
            "reason": "Forward pressure regime outcomes remain insufficient."
            if forward_count < 5
            else "Forward pressure outcomes are available but still require owner review.",
        },
        {
            "action": "review_defensive_label",
            "priority": "MEDIUM",
            "reason": "Simulation evidence does not prove production-ready defensive behavior.",
        },
        {
            "action": "do_not_approve_defensive_rule",
            "priority": "HIGH",
            "reason": "Rule approval remains blocked without sufficient forward pressure evidence.",
        },
    ]
    if _text(review_matrix.get("recommended_status")) == "RENAME_RECOMMENDED":
        actions.append(
            {
                "action": "owner_review_defensive_name",
                "priority": "MEDIUM",
                "reason": (
                    "Pressure comparison suggests the current defensive label may be misleading."
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_ops_next_actions",
        "next_actions": actions,
        "weekly_recommendation": _text(matrix.get("weekly_recommendation"), "continue_tracking"),
        "policy_change_allowed": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _variant_sample_metrics(row: Mapping[str, Any], variant: str) -> dict[str, Any]:
    variant_result = _mapping(_mapping(row.get("variant_results")).get(variant))
    no_trade = _mapping(_mapping(row.get("variant_results")).get("no_trade"))
    return_value = _float(variant_result.get("return"))
    no_trade_return = _float(no_trade.get("return"))
    max_drawdown = _float(variant_result.get("max_drawdown"))
    no_trade_drawdown = _float(no_trade.get("max_drawdown"))
    return {
        "return": return_value,
        "relative_to_no_trade": return_value - no_trade_return,
        "max_drawdown": max_drawdown,
        "drawdown_delta_vs_no_trade": max_drawdown - no_trade_drawdown,
        "turnover": _float(variant_result.get("turnover")),
    }


def _variant_results_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result = {}
    for row in rows:
        variant = _normalize_variant_name(_text(row.get("variant")))
        if not variant:
            continue
        result[variant] = {
            "return": _float(row.get("return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": _float(row.get("turnover")),
            "relative_to_no_trade": _float(row.get("relative_to_no_trade")),
        }
    return result


def _forward_variant_results(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "no_trade": {
            "return": _float(row.get("no_trade_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
        "limited_adjustment": {
            "return": _float(row.get("paper_portfolio_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
        "consensus_target": {
            "return": _float(row.get("target_weight_return")),
            "max_drawdown": _float(row.get("max_drawdown")),
            "turnover": 0.0,
        },
    }


def _forward_outcome_rows_by_key(
    advisory_outcome_dir: Path,
) -> dict[tuple[str, str, int], dict[str, Any]]:
    by_key = {}
    for manifest_path in sorted(advisory_outcome_dir.glob("*/advisory_outcome_manifest.json")):
        manifest = _read_optional_json(manifest_path) or {}
        outcome_id = _text(manifest.get("outcome_id"), manifest_path.parent.name)
        for row in _read_jsonl(manifest_path.parent / "outcome_windows.jsonl"):
            by_key[
                (
                    outcome_id,
                    _text(row.get("daily_advisory_id") or manifest.get("daily_advisory_id")),
                    _int(row.get("window_days")),
                )
            ] = row
    return by_key


def _pressure_tags_by_end(
    pressure_window_tags: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, int], list[str]]:
    result = {}
    for row in pressure_window_tags:
        tags = [
            tag
            for tag in _records_to_texts(row.get("regime_tags"))
            if tag in PRESSURE_VALIDATION_TAGS
        ]
        if tags:
            result[(_text(row.get("end_date")), _int(row.get("window_days")))] = tags
    return result


def _source_mode_defensive_status(
    source_mode: str, comparisons: Sequence[Mapping[str, Any]]
) -> str:
    rows = [
        row
        for row in comparisons
        if row.get("source_mode") == source_mode and row.get("regime") in PRESSURE_REGIMES
    ]
    rows_with_samples = [row for row in rows if _int(row.get("sample_count")) > 0]
    if not rows_with_samples:
        return "INSUFFICIENT_DATA"
    better = [
        row
        for row in rows_with_samples
        if _float(row.get("return_delta")) >= 0 and _float(row.get("drawdown_delta")) >= 0
    ]
    worse = [
        row
        for row in rows_with_samples
        if _float(row.get("return_delta")) < 0 and _float(row.get("drawdown_delta")) < 0
    ]
    if len(better) == len(rows_with_samples) and rows_with_samples:
        return "PROVEN_DEFENSIVE"
    if len(worse) == len(rows_with_samples):
        return "FAILS_DEFENSIVE_EXPECTATION"
    return "PARTIALLY_DEFENSIVE"


def _pairwise_conclusion(sample_count: int, return_delta: float, drawdown_delta: float) -> str:
    if sample_count <= 0:
        return "insufficient_data"
    if return_delta >= 0 and drawdown_delta >= 0:
        return "variant_a_better"
    if return_delta < 0 and drawdown_delta < 0:
        return "variant_b_better"
    return "mixed"


def _latest_pressure_payload(pressure_tag_dir: Path) -> dict[str, Any]:
    pressure_dir = _artifact_dir_from_latest(
        output_dir=pressure_tag_dir,
        artifact_id=None,
        pointer_name="latest_pressure_regime_tag",
    )
    return {
        **_read_json(pressure_dir / "pressure_regime_manifest.json"),
        "regime_window_tags": _read_jsonl(pressure_dir / "regime_window_tags.jsonl"),
        "outcome_regime_tags": _read_jsonl(pressure_dir / "outcome_regime_tags.jsonl"),
        "pressure_regime_summary": _read_json(pressure_dir / "pressure_regime_summary.json"),
    }


def _latest_defensive_relevant_outcomes(pressure_tag_dir: Path) -> int:
    try:
        payload = _latest_pressure_payload(pressure_tag_dir)
    except Exception:  # noqa: BLE001
        return 0
    return _int(
        _mapping(payload.get("pressure_regime_summary")).get(
            "defensive_validation_relevant_outcomes"
        )
    )


def _optional_latest_dir(output_dir: Path, pointer_name: str) -> Path | None:
    try:
        return _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name=pointer_name,
        )
    except Exception:  # noqa: BLE001
        return None


def _latest_jsonl_count(output_dir: Path, pointer_name: str, file_name: str) -> int:
    source_dir = _optional_latest_dir(output_dir, pointer_name)
    if source_dir is None:
        return 0
    return len(_read_jsonl(source_dir / file_name))


def _latest_backtest_sim_counts(output_dir: Path) -> tuple[int, int]:
    source_dir = _optional_latest_dir(output_dir, "latest_backtest_sim_outcome")
    if source_dir is None:
        return 0, 0
    rows = _read_jsonl(source_dir / "simulated_outcome_windows.jsonl")
    pressure = sum(1 for row in rows if _text(row.get("regime_label")) in PRESSURE_VALIDATION_TAGS)
    return len(rows), pressure


def _normalize_variant_name(value: str) -> str:
    if value == "candidate_consensus_target":
        return "consensus_target"
    return value


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, Mapping):
        return {}
    return dict(loaded)


def _resolve_project_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _records_to_texts(value: Any) -> list[str]:
    if isinstance(value, str | bytes) or not isinstance(value, Sequence):
        return []
    return [_text(item) for item in value if _text(item)]


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if value == value]
    return sum(clean) / len(clean) if clean else 0.0


def _percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        return 0.0
    clipped = min(max(percentile, 0.0), 1.0)
    index = min(len(values) - 1, int(round((len(values) - 1) * clipped)))
    return values[index]


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
