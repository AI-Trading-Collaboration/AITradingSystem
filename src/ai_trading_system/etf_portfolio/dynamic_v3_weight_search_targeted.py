from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.data.quality import render_data_quality_report
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio.dynamic_v3_weight_search_validation_scope import (
    VALIDATION_SCOPE_VERSION,
)

# Preserve the private resolver seam exercised by the existing hardening contract.
from ai_trading_system.etf_portfolio.dynamic_v3_weight_search_validation_scope import (
    hardened_upstream_validation_scope as _targeted_upstream_validation_scope,  # noqa: F401
)
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
)

DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_search_targeted_v2.yaml"
)
DEFAULT_TARGETED_SEARCH_V3_DIR = _legacy.DEFAULT_TARGETED_SEARCH_V3_DIR
DEFAULT_TARGETED_V3_BACKFILL_DIR = _legacy.DEFAULT_TARGETED_V3_BACKFILL_DIR
DEFAULT_NEAR_MISS_AB_COMPARISON_DIR = _legacy.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR
DEFAULT_SEARCH_COVERAGE_GAP_DIR = diagnostics.DEFAULT_SEARCH_COVERAGE_GAP_DIR
DEFAULT_NEAR_MISS_CANDIDATES_DIR = diagnostics.DEFAULT_NEAR_MISS_CANDIDATES_DIR
DEFAULT_WEIGHT_SCORECARD_DIR = evaluation.DEFAULT_WEIGHT_SCORECARD_DIR

MATRIX_INPUT_SCHEMA = "targeted_search_v3_input_snapshot.v2"
BACKFILL_INPUT_SCHEMA = "targeted_v3_backfill_input_snapshot.v2"
AB_INPUT_SCHEMA = "near_miss_ab_comparison_input_snapshot.v2"

MATRIX_VIEWS = (
    "targeted_search_v3_manifest.json",
    "v3_variant_specs.jsonl",
    "v3_family_coverage.json",
    "targeted_search_v3_report.md",
)
BACKFILL_VIEWS = (
    "targeted_v3_backfill_manifest.json",
    "v3_backfill_progress.json",
    "validate_data_quality_report.md",
    "v3_variant_performance.jsonl",
    "v3_variant_regime_metrics.jsonl",
    "v3_variant_stability_metrics.jsonl",
    "v3_variant_churn_metrics.jsonl",
    "targeted_v3_backfill_report.md",
)
AB_VIEWS = (
    "near_miss_ab_manifest.json",
    "ab_comparison_matrix.jsonl",
    "ab_winner_summary.json",
    "near_miss_ab_comparison_report.md",
)
MATRIX_FILES = (*MATRIX_VIEWS, "targeted_search_v3_input_snapshot.json")
BACKFILL_FILES = (*BACKFILL_VIEWS, "targeted_v3_backfill_input_snapshot.json")
AB_FILES = (*AB_VIEWS, "near_miss_ab_comparison_input_snapshot.json")

_mapping = _legacy._mapping
_records = _legacy._records
_texts = _legacy._texts
_text = _legacy._text
_float = _legacy._float
_stable_id = _legacy._stable_id
_unique_dir = _legacy._unique_dir
_artifact_dir = _legacy._artifact_dir
_read_json = _legacy._read_json
_read_jsonl = _legacy._read_jsonl
_write_json = _legacy._write_json
_write_jsonl = _legacy._write_jsonl
_write_text = _legacy._write_text
_write_latest_pointer = _legacy._write_latest_pointer
_validation_payload = _legacy._validation_payload
_payload_experiment_safe = _legacy._payload_experiment_safe
_payload_safe = _legacy._payload_safe
_records_or_values = _legacy._records_or_values
_variant = _legacy._variant
_batch2_family_coverage = _legacy._batch2_family_coverage
_scorecard_rows = _legacy._scorecard_rows
_failed_gates = _legacy._failed_gates
_coerce_date = _legacy._coerce_date
_latest_common_price_date = _legacy._latest_common_price_date
_variant_lag_metrics = _legacy._variant_lag_metrics


class DynamicV3WeightSearchTargetedError(ValueError):
    """Raised when the targeted-search chain cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchTargetedError(message)


def _number(value: Any, field: str) -> float:
    return diagnostics._number(value, field)


def _integer(value: Any, field: str) -> int:
    return diagnostics._integer(value, field)


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    return diagnostics._aware_datetime(value, field)


def _generated_time(generated_at: datetime | None) -> datetime:
    return diagnostics._generated_time(generated_at)


def _chronology(generated: datetime, *sources: Mapping[str, Any]) -> None:
    diagnostics._chronology(generated, *sources)


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_weight_search_targeted_policy.v2",
        "targeted-search policy schema mismatch",
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
        _require(bool(_text(metadata.get(field))), f"targeted policy metadata missing: {field}")
    _require(
        metadata.get("status") in {"pilot_baseline", "reviewed", "active"},
        "targeted policy status invalid",
    )
    matrix = _mapping(payload.get("matrix"))
    minimum = _integer(matrix.get("minimum_variants"), "minimum_variants")
    maximum = _integer(matrix.get("maximum_variants"), "maximum_variants")
    _require(0 < minimum <= maximum, "targeted variant bounds invalid")
    required_families = _texts(matrix.get("required_targeted_families"))
    _require(len(required_families) >= 2, "targeted families missing")
    _require(
        all(
            0.0 < _number(value, "smoothing_alpha") <= 1.0
            for value in matrix.get("smoothing_alpha_values", [])
        ),
        "smoothing alpha values invalid",
    )
    _require(bool(_texts(matrix.get("consensus_methods"))), "consensus methods missing")
    _require(
        all(
            _integer(value, "cooldown_days") > 0
            for value in matrix.get("sideways_cooldown_days", [])
        ),
        "cooldown days invalid",
    )
    _require(
        all(
            0.0 < _number(value, "recovery_multiplier") <= 1.0
            for value in matrix.get("recovery_restore_multipliers", [])
        ),
        "recovery multipliers invalid",
    )
    backfill = _mapping(payload.get("backfill"))
    _require(
        date.fromisoformat(_text(backfill.get("minimum_requested_start_date")))
        == st.AI_AFTER_CHATGPT_START,
        "targeted market-regime start mismatch",
    )
    _require(
        set(_texts(backfill.get("accepted_data_quality_statuses")))
        == {"PASS", "PASS_WITH_WARNINGS"},
        "accepted data-quality statuses invalid",
    )
    _require(
        _integer(backfill.get("churn_low_max_large_jump_count"), "churn limit") >= 0,
        "churn limit invalid",
    )
    ab = _mapping(payload.get("ab_comparison"))
    v3_win = _mapping(ab.get("v3_win"))
    parent_win = _mapping(ab.get("parent_win"))
    _number(v3_win.get("score_delta_min_exclusive"), "v3 score delta")
    _number(v3_win.get("drawdown_delta_min"), "v3 drawdown delta")
    _number(v3_win.get("return_delta_min"), "v3 return delta")
    _number(parent_win.get("score_delta_max_exclusive"), "parent score delta")
    _number(parent_win.get("return_delta_max_exclusive"), "parent return delta")
    _number(parent_win.get("drawdown_delta_max_exclusive"), "parent drawdown delta")
    for field in (
        "missing_parent_status",
        "mixed_status",
        "v3_win_status",
        "parent_win_status",
        "recommended_next_action",
    ):
        _require(bool(_text(ab.get(field))), f"A/B policy missing: {field}")
    safety = _mapping(payload.get("safety"))
    for field in (
        "research_screening_only",
        "experiment_only",
        "not_formal_research_method",
        "not_official_target_weights",
        "paper_shadow_only",
    ):
        _require(safety.get(field) is True, f"targeted safety must enable {field}")
    for field in (
        "broker_action_allowed",
        "broker_action_taken",
        "order_ticket_generated",
        "auto_apply",
    ):
        _require(safety.get(field) is False, f"targeted safety must disable {field}")
    _require(safety.get("production_effect") == "none", "targeted production effect invalid")
    return payload


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _source_dir(binding: Mapping[str, Any]) -> Path:
    return Path(_text(binding.get("source_dir")))


def _source_id(binding: Mapping[str, Any]) -> str:
    return _text(binding.get("artifact_id"))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(kind=kind, artifact_id=artifact_id, root=root, names=names)


def _validate_binding(binding: Mapping[str, Any], *, kind: str) -> None:
    foundation._validate_artifact_binding(binding, kind=kind)


def _snapshot_preflight(
    *,
    root: Path,
    snapshot_name: str,
    schema: str,
    id_key: str,
    artifact_id: str,
    view_names: Sequence[str],
) -> tuple[list[dict[str, Any]], bool]:
    return diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=snapshot_name,
        schema=schema,
        id_key=id_key,
        artifact_id=artifact_id,
        view_names=view_names,
    )


def _check_bytes(root: Path, expected: Mapping[str, bytes]) -> list[dict[str, Any]]:
    return diagnostics._check_bytes(root, expected)


def _validate_content(
    *,
    report_type: str,
    artifact_id: str,
    checks: list[dict[str, Any]],
    rebuild: Callable[[], list[dict[str, Any]]],
) -> dict[str, Any]:
    return diagnostics._validate_content(
        report_type=report_type,
        artifact_id=artifact_id,
        checks=checks,
        rebuild=rebuild,
    )


def _view_hash_check(root: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return diagnostics._view_hash_check(root, snapshot)


def _validated_upstream_with_hardened_scope(
    *,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    artifact_id: str,
    output_dir: Path,
    snapshot_name: str,
) -> dict[str, Any]:
    """Preserve Targeted's private resolver seam while sharing its implementation."""
    try:
        scope = _targeted_upstream_validation_scope(
            artifact_root=output_dir / artifact_id,
            snapshot_name=snapshot_name,
        )
    except Exception:  # noqa: BLE001 - resolver failure must bypass cache, not validation.
        scope = None
    if scope is None:
        return validator(**{validator_key: artifact_id, "output_dir": output_dir})
    return cached_artifact_validation(
        validator=validator,
        validator_key=validator_key,
        artifact_id=artifact_id,
        root=output_dir,
        validator_version=VALIDATION_SCOPE_VERSION,
        fingerprint_scope=scope,
    )


def _validated_coverage(coverage_gap_id: str, coverage_gap_dir: Path) -> dict[str, Any]:
    validation = _validated_upstream_with_hardened_scope(
        validator=diagnostics.validate_search_coverage_gap_artifact,
        validator_key="coverage_gap_id",
        artifact_id=coverage_gap_id,
        output_dir=coverage_gap_dir,
        snapshot_name="search_coverage_gap_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source coverage-gap validation failed")
    return diagnostics.search_coverage_gap_report_payload(
        coverage_gap_id=coverage_gap_id, output_dir=coverage_gap_dir
    )


def _validated_near_miss(near_miss_id: str, near_miss_dir: Path) -> dict[str, Any]:
    validation = _validated_upstream_with_hardened_scope(
        validator=diagnostics.validate_near_miss_candidates_artifact,
        validator_key="near_miss_id",
        artifact_id=near_miss_id,
        output_dir=near_miss_dir,
        snapshot_name="near_miss_candidates_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source near-miss validation failed")
    return diagnostics.near_miss_candidates_report_payload(
        near_miss_id=near_miss_id, output_dir=near_miss_dir
    )


def _validated_scorecard(scorecard_id: str, scorecard_dir: Path) -> dict[str, Any]:
    validation = _validated_upstream_with_hardened_scope(
        validator=evaluation.validate_weight_scorecard_artifact,
        validator_key="scorecard_id",
        artifact_id=scorecard_id,
        output_dir=scorecard_dir,
        snapshot_name="weight_scorecard_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source scorecard validation failed")
    return evaluation.weight_scorecard_report_payload(
        scorecard_id=scorecard_id, output_dir=scorecard_dir
    )


def _validated_weight_backfill(backfill_id: str, backfill_dir: Path) -> dict[str, Any]:
    validation = _validated_upstream_with_hardened_scope(
        validator=foundation.validate_weight_batch_backfill_artifact,
        validator_key="backfill_id",
        artifact_id=backfill_id,
        output_dir=backfill_dir,
        snapshot_name="weight_batch_backfill_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source weight backfill validation failed")
    return foundation.weight_batch_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )


def _validated_paper_backfill(backfill_id: str, backfill_dir: Path) -> dict[str, Any]:
    validation = _validated_upstream_with_hardened_scope(
        validator=st.validate_paper_shadow_backfill_artifact,
        validator_key="backfill_id",
        artifact_id=backfill_id,
        output_dir=backfill_dir,
        snapshot_name="paper_shadow_backfill_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source paper backfill validation failed")
    return st.paper_shadow_backfill_report_payload(backfill_id=backfill_id, output_dir=backfill_dir)


def _validated_matrix(matrix_id: str, matrix_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_targeted_search_v3_artifact,
        validator_key="v3_matrix_id",
        artifact_id=matrix_id,
        root=matrix_dir,
    )
    _require(validation.get("status") == "PASS", "source targeted matrix validation failed")
    return targeted_search_v3_report_payload(v3_matrix_id=matrix_id, output_dir=matrix_dir)


def _validated_targeted_backfill(backfill_id: str, backfill_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_targeted_v3_backfill_artifact,
        validator_key="v3_backfill_id",
        artifact_id=backfill_id,
        root=backfill_dir,
    )
    _require(validation.get("status") == "PASS", "source targeted backfill validation failed")
    return targeted_v3_backfill_report_payload(v3_backfill_id=backfill_id, output_dir=backfill_dir)


def _variant_spec(
    variant_id: str,
    families: Sequence[str],
    targeted_family: str,
    transforms: Sequence[Mapping[str, Any]],
    near_miss_parent: str,
    gap_reason: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    matrix = _mapping(policy.get("matrix"))
    variant = _variant(
        variant_id,
        families,
        transforms,
        _texts(matrix.get("target_failure_modes")),
        [_text(matrix.get("expected_benefit"))],
        [_text(matrix.get("expected_cost"))],
    )
    variant.update(
        {
            "targeted_family": targeted_family,
            "near_miss_parent": near_miss_parent,
            "coverage_gap_reason": gap_reason,
            "expected_benefit": [_text(matrix.get("expected_benefit"))],
            "expected_cost": [_text(matrix.get("expected_cost"))],
            "targeted_policy_version": _policy_version(policy),
        }
    )
    return variant


def _variant_specs(
    coverage: Mapping[str, Any], near_miss: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    recommendations = _mapping(coverage.get("targeted_v3_recommendations"))
    ranges = _mapping(recommendations.get("new_parameter_ranges"))
    cash_values = [
        _number(value, "cash_buffer") for value in _records_or_values(ranges.get("cash_buffer"), [])
    ]
    windows = [
        _integer(value, "smoothing_window")
        for value in _records_or_values(ranges.get("smoothing_window"), [])
    ]
    thresholds = [
        _number(value, "rebalance_threshold")
        for value in _records_or_values(ranges.get("rebalance_threshold"), [])
    ]
    top_k_values = [
        _integer(value, "top_k") for value in _records_or_values(ranges.get("top_k"), [])
    ]
    _require(
        cash_values and windows and thresholds and top_k_values, "coverage parameter ranges missing"
    )
    near_rows = _records(near_miss.get("near_miss_candidates"))
    matrix_policy = _mapping(policy.get("matrix"))
    default_parent = (
        _text(near_rows[0].get("variant_id"))
        if near_rows
        else _text(matrix_policy.get("default_parent_variant"))
    )
    _require(bool(default_parent), "near-miss parent missing")
    variants: list[dict[str, Any]] = []
    for cash in cash_values:
        for window in windows:
            for alpha_raw in matrix_policy.get("smoothing_alpha_values", []):
                alpha = _number(alpha_raw, "smoothing_alpha")
                variants.append(
                    _variant_spec(
                        (
                            f"cash_buffer_{int(cash * 100)}_plus_smooth_{window}d_"
                            f"alpha_{int(alpha * 100)}"
                        ),
                        ["cash_buffer", "smoothing"],
                        "cash_buffer_smoothing_hybrid",
                        [
                            {"type": "min_cash_weight", "min_cash_weight": cash},
                            {"type": "weight_smoothing", "window_days": window, "alpha": alpha},
                        ],
                        default_parent,
                        "cash_buffer_smoothing_hybrid_gap",
                        policy,
                    )
                )
    for cash in cash_values:
        for threshold in thresholds:
            variants.append(
                _variant_spec(
                    f"cash_buffer_{int(cash * 100)}_plus_rebalance_delta_{int(threshold * 1000)}bp",
                    ["cash_buffer", "rebalance_threshold"],
                    "cash_buffer_threshold_hybrid",
                    [
                        {"type": "min_cash_weight", "min_cash_weight": cash},
                        {"type": "rebalance_threshold", "min_total_abs_delta": threshold},
                    ],
                    default_parent,
                    "cash_buffer_threshold_hybrid_gap",
                    policy,
                )
            )
    consensus_alpha = _number(
        matrix_policy.get("consensus_smoothing_alpha"), "consensus_smoothing_alpha"
    )
    for method in _texts(matrix_policy.get("consensus_methods")):
        for window in windows:
            variants.append(
                _variant_spec(
                    f"{method}_consensus_plus_smooth_{window}d",
                    ["candidate_ensemble", "smoothing"],
                    "median_consensus_smoothing",
                    [
                        {"type": "consensus_aggregation", "method": method},
                        {
                            "type": "weight_smoothing",
                            "window_days": window,
                            "alpha": consensus_alpha,
                        },
                    ],
                    default_parent,
                    "ensemble_smoothing_gap",
                    policy,
                )
            )
    for top_k in top_k_values:
        for threshold in thresholds:
            variants.append(
                _variant_spec(
                    f"top{top_k}_consensus_plus_threshold_{int(threshold * 1000)}bp",
                    ["candidate_ensemble", "rebalance_threshold"],
                    "top_k_consensus_threshold",
                    [
                        {"type": "candidate_subset", "top_k": top_k},
                        {"type": "consensus_aggregation", "method": "weighted_mean"},
                        {"type": "rebalance_threshold", "min_total_abs_delta": threshold},
                    ],
                    default_parent,
                    "top_k_threshold_gap",
                    policy,
                )
            )
    for cooldown_raw in matrix_policy.get("sideways_cooldown_days", []):
        cooldown = _integer(cooldown_raw, "cooldown_days")
        for cash in cash_values:
            variants.append(
                _variant_spec(
                    f"sideways_cooldown_{cooldown}d_plus_cash_buffer_{int(cash * 100)}",
                    ["cooldown", "cash_buffer"],
                    "sideways_cooldown_cash_buffer",
                    [
                        {
                            "type": "regime_cooldown",
                            "regime": "sideways_choppy",
                            "cooldown_days": cooldown,
                        },
                        {"type": "min_cash_weight", "min_cash_weight": cash},
                    ],
                    default_parent,
                    "sideways_cash_buffer_gap",
                    policy,
                )
            )
    restore_alpha = _number(
        matrix_policy.get("recovery_restore_smoothing_alpha"),
        "recovery_restore_smoothing_alpha",
    )
    for window in windows:
        for multiplier_raw in matrix_policy.get("recovery_restore_multipliers", []):
            multiplier = _number(multiplier_raw, "recovery_multiplier")
            variants.append(
                _variant_spec(
                    f"smooth_{window}d_plus_strong_recovery_restore_{int(multiplier * 100)}",
                    ["smoothing", "regime_gating"],
                    "smoothing_recovery_fast_restore",
                    [
                        {"type": "weight_smoothing", "window_days": window, "alpha": restore_alpha},
                        {
                            "type": "regime_gate",
                            "regime": "strong_recovery",
                            "action": "reduce_active_tilt",
                            "multiplier": multiplier,
                        },
                    ],
                    default_parent,
                    "recovery_fast_restore_gap",
                    policy,
                )
            )
    deduped = {str(row.get("variant_id")): row for row in variants}
    return list(deduped.values())


def _family_coverage(
    variants: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    targeted = sorted({_text(row.get("targeted_family")) for row in variants})
    required = _texts(_mapping(policy.get("matrix")).get("required_targeted_families"))
    _require(set(required).issubset(targeted), "required targeted family missing")
    return {
        "schema_version": st.SCHEMA_VERSION,
        "targeted_families_covered": targeted,
        "targeted_family_counts": {
            family: sum(1 for row in variants if row.get("targeted_family") == family)
            for family in targeted
        },
        "required_targeted_families": required,
        "base_family_coverage": _batch2_family_coverage(variants),
        "targeted_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def render_targeted_search_v3_report(
    manifest: Mapping[str, Any], coverage: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Targeted Search v3 Matrix {manifest.get('v3_matrix_id')}",
            "",
            f"- variants：{manifest.get('variant_count')}",
            f"- coverage_gap_id：{manifest.get('coverage_gap_id')}",
            f"- source scorecard：{manifest.get('source_scorecard_id')}",
            f"- targeted policy：{manifest.get('targeted_policy_version')}",
            f"- targeted families：{', '.join(_texts(coverage.get('targeted_families_covered')))}",
            "- every variant has a validated near_miss_parent or coverage_gap_reason",
            "- safety：experiment only / no official target / no broker / no production",
            "",
        ]
    )


def _matrix_material(
    *,
    root: Path,
    matrix_id: str,
    coverage_gap_id: str,
    coverage: Mapping[str, Any],
    near_miss: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, bytes]]:
    _require(
        _text(coverage.get("near_miss_id")) == _text(near_miss.get("near_miss_id")),
        "coverage and near-miss lineage mismatch",
    )
    scorecard_id = _text(near_miss.get("source_scorecard_id"))
    _require(
        scorecard_id == _text(coverage.get("source_scorecard_id")), "coverage scorecard mismatch"
    )
    _require(scorecard_id == _text(scorecard.get("scorecard_id")), "live scorecard mismatch")
    _chronology(generated, coverage, near_miss, scorecard)
    attribution_id = _text(coverage.get("cash_buffer_attribution_id"))
    _require(bool(attribution_id), "coverage cash-buffer attribution lineage missing")
    variants = _variant_specs(coverage, near_miss, policy)
    matrix_policy = _mapping(policy.get("matrix"))
    minimum = _integer(matrix_policy.get("minimum_variants"), "minimum_variants")
    maximum = _integer(matrix_policy.get("maximum_variants"), "maximum_variants")
    coverage_cap = _integer(
        _mapping(coverage.get("targeted_v3_recommendations")).get("max_v3_variants"),
        "coverage max_v3_variants",
    )
    _require(coverage_cap <= maximum, "coverage variant cap exceeds reviewed policy")
    variants = variants[:coverage_cap]
    _require(minimum <= len(variants) <= maximum, "targeted variant count outside policy")
    _require(
        all(row.get("near_miss_parent") or row.get("coverage_gap_reason") for row in variants),
        "targeted variant provenance missing",
    )
    family_coverage = _family_coverage(variants, policy)
    score_snapshot = _mapping(scorecard.get("input_snapshot"))
    weight_backfill_source = _mapping(score_snapshot.get("backfill_source"))
    _require(bool(_source_id(weight_backfill_source)), "scorecard weight-backfill source missing")
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_targeted_search_v3_manifest",
        "v3_matrix_id": matrix_id,
        "coverage_gap_id": coverage_gap_id,
        "near_miss_id": near_miss.get("near_miss_id"),
        "attribution_id": attribution_id,
        "cash_buffer_attribution_id": attribution_id,
        "source_scorecard_id": scorecard_id,
        "source_weight_batch_backfill_id": _source_id(weight_backfill_source),
        "source_weight_batch_backfill_dir": str(_source_dir(weight_backfill_source).parent),
        "source_backfill_id": scorecard.get("source_backfill_id"),
        "search_space_id": coverage.get("search_space_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": st.AI_AFTER_CHATGPT_START.isoformat(),
        "variant_count": len(variants),
        "targeted_policy_version": _policy_version(policy),
        "targeted_search_v3_manifest_path": str(root / MATRIX_VIEWS[0]),
        "v3_variant_specs_path": str(root / MATRIX_VIEWS[1]),
        "v3_family_coverage_path": str(root / MATRIX_VIEWS[2]),
        "targeted_search_v3_report_path": str(root / MATRIX_VIEWS[3]),
        "targeted_search_v3_input_snapshot_path": str(
            root / "targeted_search_v3_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        MATRIX_VIEWS[0]: foundation._json_bytes(manifest),
        MATRIX_VIEWS[1]: foundation._jsonl_bytes(variants),
        MATRIX_VIEWS[2]: foundation._json_bytes(family_coverage),
        MATRIX_VIEWS[3]: foundation._text_file_bytes(
            render_targeted_search_v3_report(manifest, family_coverage)
        ),
    }
    return manifest, variants, family_coverage, views


def build_targeted_search_v3(
    *,
    coverage_gap_id: str,
    coverage_gap_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    coverage = _validated_coverage(coverage_gap_id, coverage_gap_dir)
    near_miss_id = _text(coverage.get("near_miss_id"))
    near_miss = _validated_near_miss(near_miss_id, near_miss_dir)
    scorecard_id = _text(near_miss.get("source_scorecard_id"))
    scorecard_source = _mapping(_mapping(near_miss.get("input_snapshot")).get("scorecard_source"))
    scorecard = _validated_scorecard(scorecard_id, _source_dir(scorecard_source).parent)
    policy = _policy(policy_path)
    policy_binding = foundation._file_binding(policy_path)
    coverage_binding = _binding(
        kind="search_coverage_gap",
        artifact_id=coverage_gap_id,
        root=Path(_text(coverage.get("coverage_gap_dir"))),
        names=diagnostics.COVERAGE_FILES,
    )
    near_miss_binding = _binding(
        kind="near_miss_candidates",
        artifact_id=near_miss_id,
        root=Path(_text(near_miss.get("near_miss_dir"))),
        names=diagnostics.NEAR_MISS_FILES,
    )
    scorecard_binding = _binding(
        kind="weight_scorecard",
        artifact_id=scorecard_id,
        root=Path(_text(scorecard.get("scorecard_dir"))),
        names=evaluation.SCORECARD_FILES,
    )
    matrix_id = _stable_id("targeted-search-v3", coverage_gap_id, generated.isoformat())
    root = _unique_dir(output_dir / matrix_id)
    manifest, variants, family_coverage, views = _matrix_material(
        root=root,
        matrix_id=root.name,
        coverage_gap_id=coverage_gap_id,
        coverage=coverage,
        near_miss=near_miss,
        scorecard=scorecard,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / MATRIX_VIEWS[0], manifest)
    _write_jsonl(root / MATRIX_VIEWS[1], variants)
    _write_json(root / MATRIX_VIEWS[2], family_coverage)
    _write_text(
        root / MATRIX_VIEWS[3],
        render_targeted_search_v3_report(manifest, family_coverage),
    )
    snapshot = {
        "schema_version": MATRIX_INPUT_SCHEMA,
        "v3_matrix_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_binding,
        "coverage_source": coverage_binding,
        "near_miss_source": near_miss_binding,
        "scorecard_source": scorecard_binding,
        "view_hashes": foundation._view_hashes(root, MATRIX_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "targeted_search_v3_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_targeted_search_v3", root.name, root / MATRIX_VIEWS[0])
    return {
        "v3_matrix_id": root.name,
        "v3_matrix_dir": root,
        "manifest": manifest,
        "v3_variant_specs": variants,
        "v3_family_coverage": family_coverage,
    }


def targeted_search_v3_report_payload(
    *,
    v3_matrix_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v3_matrix_id,
        latest_pointer="latest_targeted_search_v3",
        latest=latest,
        output_dir=output_dir,
        required_name=MATRIX_VIEWS[0],
    )
    return {
        **_read_json(root / MATRIX_VIEWS[0]),
        "v3_variant_specs": _read_jsonl(root / MATRIX_VIEWS[1]),
        "v3_family_coverage": _read_json(root / MATRIX_VIEWS[2]),
        "input_snapshot": _read_json(root / "targeted_search_v3_input_snapshot.json"),
        "v3_matrix_dir": str(root),
    }


def _rebuild_matrix(root: Path, matrix_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "targeted_search_v3_input_snapshot.json")
    _require(snapshot.get("schema_version") == MATRIX_INPUT_SCHEMA, "matrix snapshot schema")
    _require(snapshot.get("v3_matrix_id") == matrix_id, "matrix snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    coverage_source = _mapping(snapshot.get("coverage_source"))
    near_source = _mapping(snapshot.get("near_miss_source"))
    score_source = _mapping(snapshot.get("scorecard_source"))
    _validate_binding(coverage_source, kind="search_coverage_gap")
    _validate_binding(near_source, kind="near_miss_candidates")
    _validate_binding(score_source, kind="weight_scorecard")
    coverage = _validated_coverage(_source_id(coverage_source), _source_dir(coverage_source).parent)
    near_miss = _validated_near_miss(_source_id(near_source), _source_dir(near_source).parent)
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _, _, _, expected = _matrix_material(
        root=root,
        matrix_id=matrix_id,
        coverage_gap_id=_source_id(coverage_source),
        coverage=coverage,
        near_miss=near_miss,
        scorecard=scorecard,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


def validate_targeted_search_v3_artifact(
    *, v3_matrix_id: str, output_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR
) -> dict[str, Any]:
    root = output_dir / v3_matrix_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="targeted_search_v3_input_snapshot.json",
        schema=MATRIX_INPUT_SCHEMA,
        id_key="v3_matrix_id",
        artifact_id=v3_matrix_id,
        view_names=MATRIX_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_targeted_search_v3_validation", v3_matrix_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_targeted_search_v3_validation",
        artifact_id=v3_matrix_id,
        checks=checks,
        rebuild=lambda: _rebuild_matrix(root, v3_matrix_id),
    )


def _variant_churn_metrics(
    variant_states: Sequence[Mapping[str, Any]],
    stability_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    stability = {str(row.get("variant_id")): row for row in stability_rows}
    limit = _integer(
        _mapping(policy.get("backfill")).get("churn_low_max_large_jump_count"),
        "churn_low_max_large_jump_count",
    )
    rows = []
    for variant_id in sorted({str(row.get("variant_id")) for row in variant_states}):
        states = [row for row in variant_states if row.get("variant_id") == variant_id]
        turnover = [
            _float(row.get("turnover")) for row in states if row.get("rebalance_event") is True
        ]
        stable = _mapping(stability.get(variant_id))
        large_jump_count = _integer(stable.get("large_jump_count"), "large_jump_count")
        rows.append(
            {
                "variant_id": variant_id,
                "avg_rebalance_turnover": round(sum(turnover) / len(turnover), 10)
                if turnover
                else 0.0,
                "max_rebalance_turnover": round(max(turnover), 10) if turnover else 0.0,
                "signal_churn_count": _integer(
                    stable.get("weight_flip_count"), "weight_flip_count"
                ),
                "large_jump_count": large_jump_count,
                "churn_status": "LOW" if large_jump_count <= limit else "REVIEW_REQUIRED",
                "targeted_policy_version": _policy_version(policy),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return rows


def render_targeted_v3_backfill_report(
    manifest: Mapping[str, Any], progress: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Targeted v3 Backfill {manifest.get('v3_backfill_id')}",
            "",
            f"- status：{manifest.get('status')}",
            f"- date range：{manifest.get('date_start')} -> {manifest.get('date_end')}",
            (
                f"- requested range：{manifest.get('requested_start_date')} -> "
                f"{manifest.get('requested_end_date')}"
            ),
            f"- data quality：{manifest.get('data_quality_status')}",
            f"- latest_valid_as_of：{manifest.get('latest_valid_as_of')}",
            f"- targeted policy：{manifest.get('targeted_policy_version')}",
            (
                f"- variants completed：{progress.get('variants_completed')} / "
                f"{progress.get('variants_total')}"
            ),
            "- safety：research screening only；no official target / no broker / no production",
            "",
        ]
    )


def _backfill_material(
    *,
    root: Path,
    backfill_id: str,
    matrix: Mapping[str, Any],
    weight_backfill: Mapping[str, Any],
    paper_backfill: Mapping[str, Any],
    policy: Mapping[str, Any],
    prices_path: Path,
    rates_path: Path,
    generated: datetime,
    frozen_quality_checked_at: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[dict[str, Any]]], Any, dict[str, bytes]]:
    matrix_id = _text(matrix.get("v3_matrix_id"))
    _require(
        _text(matrix.get("source_weight_batch_backfill_id"))
        == _text(weight_backfill.get("batch_backfill_id")),
        "targeted matrix and weight-backfill lineage mismatch",
    )
    _require(
        _text(matrix.get("source_backfill_id")) == _text(weight_backfill.get("source_backfill_id")),
        "targeted matrix paper-backfill lineage mismatch",
    )
    _require(
        _text(weight_backfill.get("source_backfill_id"))
        == _text(paper_backfill.get("backfill_id")),
        "weight and paper backfill lineage mismatch",
    )
    _chronology(generated, matrix, weight_backfill, paper_backfill)
    baseline_states = _records(paper_backfill.get("backfill_method_states"))
    _require(bool(baseline_states), "paper backfill states missing")
    config = st._load_backfill_config_from_manifest(paper_backfill)
    minimum_start = date.fromisoformat(
        _text(_mapping(policy.get("backfill")).get("minimum_requested_start_date"))
    )
    start = max(_coerce_date(paper_backfill.get("date_start"), minimum_start), minimum_start)
    requested_end = _coerce_date(paper_backfill.get("date_end"), generated.date())
    symbols = st._symbols_from_state_paths(baseline_states)
    pivot = st._load_price_pivot(prices_path, symbols, start)
    latest_valid_as_of = _latest_common_price_date(pivot, symbols)
    end = min(requested_end, latest_valid_as_of, generated.date())
    _require(start <= end, "targeted backfill date range invalid")
    used_latest_valid_as_of = end < requested_end
    pivot = pivot.loc[(pivot.index.date >= start) & (pivot.index.date <= end)]
    # This is a historical replay. Freshness is evaluated at the bounded replay
    # cutoff while the live file bindings still detect cache mutation.
    quality_as_of = end
    quality = st._run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        expected_symbols=symbols,
        as_of=quality_as_of,
    )
    accepted = _texts(_mapping(policy.get("backfill")).get("accepted_data_quality_statuses"))
    _require(
        quality.passed and quality.status in accepted, f"data quality gate failed: {quality.status}"
    )
    if frozen_quality_checked_at is not None:
        quality = replace(quality, checked_at=frozen_quality_checked_at)
    returns = pivot.pct_change().fillna(0.0)
    labels = {
        idx.date().isoformat(): st._risk_capped_regime_context_for_return(row, config)
        for idx, row in returns.iterrows()
    }
    variant_specs = _records(matrix.get("v3_variant_specs"))
    variant_states: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for variant in variant_specs:
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
            failures.append({"variant_id": _text(variant.get("variant_id")), "error": str(exc)})
    performance = st._variant_performance_metrics(variant_states, baseline_states)
    regime = st._variant_regime_metrics(variant_states, baseline_states, labels, config)
    stability = st._variant_stability_metrics(variant_states, baseline_states, config)
    churn = _variant_churn_metrics(variant_states, stability, policy)
    _require(not failures, f"targeted variants failed: {failures}")
    completed = {str(row.get("variant_id")) for row in performance}
    expected = {str(row.get("variant_id")) for row in variant_specs}
    _require(completed == expected, "targeted variants incomplete")
    material = {
        "performance": performance,
        "regime": regime,
        "stability": stability,
        "churn": churn,
    }
    quality_report_path = root / BACKFILL_VIEWS[2]
    progress = {
        "schema_version": st.SCHEMA_VERSION,
        "v3_backfill_id": backfill_id,
        "variants_total": len(variant_specs),
        "variants_completed": len(completed),
        "variants_failed": 0,
        "failed_variants": [],
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_date_end": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_targeted_v3_backfill_manifest",
        "v3_backfill_id": backfill_id,
        "v3_matrix_id": matrix_id,
        "source_weight_batch_backfill_id": weight_backfill.get("batch_backfill_id"),
        "source_backfill_id": paper_backfill.get("backfill_id"),
        "source_scorecard_id": matrix.get("source_scorecard_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": _mapping(policy.get("backfill")).get("market_regime"),
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "requested_start_date": start.isoformat(),
        "requested_end_date": requested_end.isoformat(),
        "latest_valid_as_of": latest_valid_as_of.isoformat(),
        "data_quality_status": quality.status,
        "data_quality_as_of": quality_as_of.isoformat(),
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "validate_data_quality_report_path": str(quality_report_path),
        "used_latest_valid_as_of": used_latest_valid_as_of,
        "variants_total": len(variant_specs),
        "variants_completed": len(completed),
        "variants_failed": 0,
        "targeted_policy_version": _policy_version(policy),
        "targeted_v3_backfill_manifest_path": str(root / BACKFILL_VIEWS[0]),
        "v3_backfill_progress_path": str(root / BACKFILL_VIEWS[1]),
        "v3_variant_performance_path": str(root / BACKFILL_VIEWS[3]),
        "v3_variant_regime_metrics_path": str(root / BACKFILL_VIEWS[4]),
        "v3_variant_stability_metrics_path": str(root / BACKFILL_VIEWS[5]),
        "v3_variant_churn_metrics_path": str(root / BACKFILL_VIEWS[6]),
        "targeted_v3_backfill_report_path": str(root / BACKFILL_VIEWS[7]),
        "targeted_v3_backfill_input_snapshot_path": str(
            root / "targeted_v3_backfill_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        BACKFILL_VIEWS[0]: foundation._json_bytes(manifest),
        BACKFILL_VIEWS[1]: foundation._json_bytes(progress),
        BACKFILL_VIEWS[2]: foundation._text_file_bytes(render_data_quality_report(quality)),
        BACKFILL_VIEWS[3]: foundation._jsonl_bytes(performance),
        BACKFILL_VIEWS[4]: foundation._jsonl_bytes(regime),
        BACKFILL_VIEWS[5]: foundation._jsonl_bytes(stability),
        BACKFILL_VIEWS[6]: foundation._jsonl_bytes(churn),
        BACKFILL_VIEWS[7]: foundation._text_file_bytes(
            render_targeted_v3_backfill_report(manifest, progress)
        ),
    }
    return manifest, progress, material, quality, views


def run_targeted_v3_backfill(
    *,
    v3_matrix_id: str,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    matrix = _validated_matrix(v3_matrix_id, v3_matrix_dir)
    policy = _policy(policy_path)
    weight_backfill_id = _text(matrix.get("source_weight_batch_backfill_id"))
    weight_backfill_dir = Path(_text(matrix.get("source_weight_batch_backfill_dir")))
    weight_backfill = _validated_weight_backfill(weight_backfill_id, weight_backfill_dir)
    paper_backfill_id = _text(matrix.get("source_backfill_id"))
    paper_backfill = _validated_paper_backfill(paper_backfill_id, baseline_backfill_dir)
    baseline_states = _records(paper_backfill.get("backfill_method_states"))
    config = st._load_backfill_config_from_manifest(paper_backfill)
    source = _mapping(config.get("source"))
    prices_path = price_cache_path or st._resolve_project_path(
        source.get("price_cache_path"), st.DEFAULT_PRICE_CACHE_PATH
    )
    policy_binding = foundation._file_binding(policy_path)
    matrix_binding = _binding(
        kind="targeted_search_v3",
        artifact_id=v3_matrix_id,
        root=Path(_text(matrix.get("v3_matrix_dir"))),
        names=MATRIX_FILES,
    )
    weight_backfill_binding = _binding(
        kind="weight_batch_backfill",
        artifact_id=weight_backfill_id,
        root=Path(_text(weight_backfill.get("backfill_dir"))),
        names=evaluation.BACKFILL_FILES,
    )
    paper_backfill_binding = _binding(
        kind="paper_shadow_backfill",
        artifact_id=paper_backfill_id,
        root=baseline_backfill_dir / paper_backfill_id,
        names=(
            "paper_shadow_backfill_input_snapshot.json",
            "paper_shadow_backfill_manifest.json",
            "backfill_rebalance_calendar.json",
            "backfill_method_states.jsonl",
            "backfill_trade_ledger.jsonl",
            "backfill_data_quality.json",
            "paper_shadow_backfill_report.md",
            "validate_data_quality_report.md",
        ),
    )
    price_binding = foundation._file_binding(prices_path)
    rates_binding = foundation._file_binding(rates_cache_path)
    backfill_id = _stable_id("targeted-v3-backfill", v3_matrix_id, generated.isoformat())
    root = _unique_dir(output_dir / backfill_id)
    manifest, progress, material, quality, views = _backfill_material(
        root=root,
        backfill_id=root.name,
        matrix=matrix,
        weight_backfill=weight_backfill,
        paper_backfill=paper_backfill,
        policy=policy,
        prices_path=prices_path,
        rates_path=rates_cache_path,
        generated=generated,
    )
    _require(bool(baseline_states), "baseline states missing before output")
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / BACKFILL_VIEWS[0], manifest)
    _write_json(root / BACKFILL_VIEWS[1], progress)
    _write_text(root / BACKFILL_VIEWS[2], render_data_quality_report(quality))
    _write_jsonl(root / BACKFILL_VIEWS[3], material["performance"])
    _write_jsonl(root / BACKFILL_VIEWS[4], material["regime"])
    _write_jsonl(root / BACKFILL_VIEWS[5], material["stability"])
    _write_jsonl(root / BACKFILL_VIEWS[6], material["churn"])
    _write_text(
        root / BACKFILL_VIEWS[7],
        render_targeted_v3_backfill_report(manifest, progress),
    )
    snapshot = {
        "schema_version": BACKFILL_INPUT_SCHEMA,
        "v3_backfill_id": root.name,
        "generated_at": generated.isoformat(),
        "quality_checked_at": quality.checked_at.isoformat(),
        "policy_source": policy_binding,
        "matrix_source": matrix_binding,
        "weight_backfill_source": weight_backfill_binding,
        "paper_backfill_source": paper_backfill_binding,
        "price_source": price_binding,
        "rates_source": rates_binding,
        "view_hashes": foundation._view_hashes(root, BACKFILL_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "targeted_v3_backfill_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_targeted_v3_backfill", root.name, root / BACKFILL_VIEWS[0])
    return {
        "v3_backfill_id": root.name,
        "v3_backfill_dir": root,
        "manifest": manifest,
        "v3_backfill_progress": progress,
        "v3_variant_performance": material["performance"],
        "v3_variant_regime_metrics": material["regime"],
        "v3_variant_stability_metrics": material["stability"],
        "v3_variant_churn_metrics": material["churn"],
    }


def resume_targeted_v3_backfill(
    *, v3_backfill_id: str, output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR
) -> dict[str, Any]:
    validation = validate_targeted_v3_backfill_artifact(
        v3_backfill_id=v3_backfill_id, output_dir=output_dir
    )
    _require(validation.get("status") == "PASS", "backfill validation failed before resume")
    payload = targeted_v3_backfill_report_payload(
        v3_backfill_id=v3_backfill_id, output_dir=output_dir
    )
    progress = _mapping(payload.get("v3_backfill_progress"))
    policy_source = _mapping(_mapping(payload.get("input_snapshot")).get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    backfill_policy = _mapping(policy.get("backfill"))
    completed = _integer(progress.get("variants_completed"), "variants_completed")
    total = _integer(progress.get("variants_total"), "variants_total")
    return {
        "v3_backfill_id": v3_backfill_id,
        "resume_status": (
            _text(backfill_policy.get("resume_complete_status"))
            if completed >= total
            else _text(backfill_policy.get("resume_incomplete_status"))
        ),
        "progress": progress,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def targeted_v3_backfill_report_payload(
    *,
    v3_backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=v3_backfill_id,
        latest_pointer="latest_targeted_v3_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name=BACKFILL_VIEWS[0],
    )
    regime = _read_jsonl(root / BACKFILL_VIEWS[4])
    return {
        **_read_json(root / BACKFILL_VIEWS[0]),
        "v3_backfill_progress": _read_json(root / BACKFILL_VIEWS[1]),
        "v3_variant_performance": _read_jsonl(root / BACKFILL_VIEWS[3]),
        "v3_variant_regime_metrics": regime,
        "v3_variant_stability_metrics": _read_jsonl(root / BACKFILL_VIEWS[5]),
        "v3_variant_churn_metrics": _read_jsonl(root / BACKFILL_VIEWS[6]),
        "v3_variant_lag_metrics": _variant_lag_metrics(regime),
        "input_snapshot": _read_json(root / "targeted_v3_backfill_input_snapshot.json"),
        "v3_backfill_dir": str(root),
    }


def _rebuild_backfill(root: Path, backfill_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "targeted_v3_backfill_input_snapshot.json")
    _require(snapshot.get("schema_version") == BACKFILL_INPUT_SCHEMA, "backfill snapshot schema")
    _require(snapshot.get("v3_backfill_id") == backfill_id, "backfill snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    weight_source = _mapping(snapshot.get("weight_backfill_source"))
    paper_source = _mapping(snapshot.get("paper_backfill_source"))
    _validate_binding(matrix_source, kind="targeted_search_v3")
    _validate_binding(weight_source, kind="weight_batch_backfill")
    _validate_binding(paper_source, kind="paper_shadow_backfill")
    price_source = _mapping(snapshot.get("price_source"))
    rates_source = _mapping(snapshot.get("rates_source"))
    foundation._validate_file_binding(price_source)
    foundation._validate_file_binding(rates_source)
    matrix = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    weight_backfill = _validated_weight_backfill(
        _source_id(weight_source), _source_dir(weight_source).parent
    )
    paper_backfill = _validated_paper_backfill(
        _source_id(paper_source), _source_dir(paper_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    quality_checked = _aware_datetime(
        _text(snapshot.get("quality_checked_at")), "snapshot.quality_checked_at"
    )
    _, _, _, _, expected = _backfill_material(
        root=root,
        backfill_id=backfill_id,
        matrix=matrix,
        weight_backfill=weight_backfill,
        paper_backfill=paper_backfill,
        policy=policy,
        prices_path=Path(_text(price_source.get("path"))),
        rates_path=Path(_text(rates_source.get("path"))),
        generated=generated,
        frozen_quality_checked_at=quality_checked,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


def validate_targeted_v3_backfill_artifact(
    *, v3_backfill_id: str, output_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR
) -> dict[str, Any]:
    root = output_dir / v3_backfill_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="targeted_v3_backfill_input_snapshot.json",
        schema=BACKFILL_INPUT_SCHEMA,
        id_key="v3_backfill_id",
        artifact_id=v3_backfill_id,
        view_names=BACKFILL_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_targeted_v3_backfill_validation", v3_backfill_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_targeted_v3_backfill_validation",
        artifact_id=v3_backfill_id,
        checks=checks,
        rebuild=lambda: _rebuild_backfill(root, v3_backfill_id),
    )


def _targeted_scorecard_rows(
    backfill: Mapping[str, Any], matrix: Mapping[str, Any]
) -> list[dict[str, Any]]:
    payload = {
        "data_quality_status": backfill.get("data_quality_status"),
        "variant_performance_metrics": backfill.get("v3_variant_performance"),
        "variant_stability_metrics": backfill.get("v3_variant_stability_metrics"),
        "variant_churn_metrics": backfill.get("v3_variant_churn_metrics"),
        "variant_lag_metrics": backfill.get("v3_variant_lag_metrics"),
        "variant_regime_metrics": backfill.get("v3_variant_regime_metrics"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return _scorecard_rows(payload, _records(matrix.get("v3_variant_specs")))


def _ab_status(row: Mapping[str, Any], parent: Mapping[str, Any], policy: Mapping[str, Any]) -> str:
    ab = _mapping(policy.get("ab_comparison"))
    if not parent:
        return _text(ab.get("missing_parent_status"))
    score_delta = _number(row.get("overall_score"), "overall_score") - _number(
        parent.get("overall_score"), "parent.overall_score"
    )
    return_delta = _number(row.get("total_return"), "total_return") - _number(
        parent.get("total_return"), "parent.total_return"
    )
    drawdown_delta = _number(row.get("max_drawdown"), "max_drawdown") - _number(
        parent.get("max_drawdown"), "parent.max_drawdown"
    )
    v3_win = _mapping(ab.get("v3_win"))
    if (
        score_delta > _number(v3_win.get("score_delta_min_exclusive"), "v3 score delta")
        and drawdown_delta >= _number(v3_win.get("drawdown_delta_min"), "v3 drawdown delta")
        and return_delta >= _number(v3_win.get("return_delta_min"), "v3 return delta")
    ):
        return _text(ab.get("v3_win_status"))
    parent_win = _mapping(ab.get("parent_win"))
    if (
        score_delta < _number(parent_win.get("score_delta_max_exclusive"), "parent score delta")
        and return_delta
        < _number(parent_win.get("return_delta_max_exclusive"), "parent return delta")
        and drawdown_delta
        < _number(parent_win.get("drawdown_delta_max_exclusive"), "parent drawdown delta")
    ):
        return _text(ab.get("parent_win_status"))
    return _text(ab.get("mixed_status"))


def _comparison_rows(
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
    source_scorecard: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    targeted_rows = _targeted_scorecard_rows(backfill, matrix)
    source_rows = {
        _text(row.get("variant_id")): row
        for row in _records(source_scorecard.get("variant_scorecard"))
    }
    specs = {_text(row.get("variant_id")): row for row in _records(matrix.get("v3_variant_specs"))}
    rows = []
    for row in targeted_rows:
        spec = _mapping(specs.get(_text(row.get("variant_id"))))
        parent_id = _text(spec.get("near_miss_parent"))
        parent = _mapping(source_rows.get(parent_id))
        smooth = _mapping(
            source_rows.get("smooth_weights_3d")
            or source_rows.get("smooth_3d_plus_rebalance_delta_3pct")
        )
        rows.append(
            {
                "variant_id": row.get("variant_id"),
                "near_miss_parent": parent_id,
                "overall_score": row.get("overall_score"),
                "parent_overall_score": parent.get("overall_score") if parent else None,
                "smooth_reference_score": smooth.get("overall_score") if smooth else None,
                "score_delta_vs_parent": (
                    round(
                        _number(row.get("overall_score"), "overall_score")
                        - _number(parent.get("overall_score"), "parent.overall_score"),
                        6,
                    )
                    if parent
                    else None
                ),
                "return_delta_vs_parent": (
                    round(
                        _number(row.get("total_return"), "total_return")
                        - _number(parent.get("total_return"), "parent.total_return"),
                        10,
                    )
                    if parent
                    else None
                ),
                "drawdown_delta_vs_parent": (
                    round(
                        _number(row.get("max_drawdown"), "max_drawdown")
                        - _number(parent.get("max_drawdown"), "parent.max_drawdown"),
                        10,
                    )
                    if parent
                    else None
                ),
                "turnover_delta_vs_parent": (
                    round(
                        _number(row.get("turnover"), "turnover")
                        - _number(parent.get("turnover"), "parent.turnover"),
                        10,
                    )
                    if parent
                    else None
                ),
                "ab_status": _ab_status(row, parent, policy),
                "failed_gates": _failed_gates(row),
                "targeted_policy_version": _policy_version(policy),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return sorted(rows, key=lambda item: _float(item.get("overall_score")), reverse=True)


def _winner_summary(rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]) -> dict[str, Any]:
    _require(bool(rows), "A/B comparison rows missing")
    best = max(rows, key=lambda row: _float(row.get("overall_score")))
    ab = _mapping(policy.get("ab_comparison"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "best_v3_variant": best.get("variant_id"),
        "best_v3_score": best.get("overall_score"),
        "v3_win_count": sum(1 for row in rows if row.get("ab_status") == ab.get("v3_win_status")),
        "parent_win_count": sum(
            1 for row in rows if row.get("ab_status") == ab.get("parent_win_status")
        ),
        "inconclusive_count": sum(
            1
            for row in rows
            if row.get("ab_status") in {ab.get("mixed_status"), ab.get("missing_parent_status")}
        ),
        "recommended_next_action": ab.get("recommended_next_action"),
        "targeted_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def render_near_miss_ab_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Near-Miss A/B Comparison {manifest.get('ab_id')}",
            "",
            f"- best_v3_variant：{summary.get('best_v3_variant')}",
            f"- v3_win_count：{summary.get('v3_win_count')}",
            f"- parent_win_count：{summary.get('parent_win_count')}",
            f"- inconclusive_count：{summary.get('inconclusive_count')}",
            f"- targeted policy：{manifest.get('targeted_policy_version')}",
            "- safety：A/B result is diagnostics only, not promotion approval.",
            "",
        ]
    )


def _ab_material(
    *,
    root: Path,
    ab_id: str,
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
    near_miss: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, bytes]]:
    _require(
        _text(backfill.get("v3_matrix_id")) == _text(matrix.get("v3_matrix_id")),
        "A/B backfill and matrix mismatch",
    )
    _require(
        _text(matrix.get("near_miss_id")) == _text(near_miss.get("near_miss_id")),
        "A/B matrix and near-miss mismatch",
    )
    scorecard_id = _text(scorecard.get("scorecard_id"))
    _require(
        scorecard_id
        == _text(matrix.get("source_scorecard_id"))
        == _text(near_miss.get("source_scorecard_id")),
        "A/B scorecard lineage mismatch",
    )
    _require(
        _text(backfill.get("source_weight_batch_backfill_id"))
        == _text(matrix.get("source_weight_batch_backfill_id")),
        "A/B weight-backfill lineage mismatch",
    )
    _chronology(generated, backfill, matrix, near_miss, scorecard)
    rows = _comparison_rows(backfill, matrix, scorecard, policy)
    summary = _winner_summary(rows, policy)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_near_miss_ab_manifest",
        "ab_id": ab_id,
        "v3_backfill_id": backfill.get("v3_backfill_id"),
        "v3_matrix_id": matrix.get("v3_matrix_id"),
        "near_miss_id": near_miss.get("near_miss_id"),
        "source_scorecard_id": scorecard_id,
        "source_weight_batch_backfill_id": matrix.get("source_weight_batch_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime"),
        "targeted_policy_version": _policy_version(policy),
        "near_miss_ab_manifest_path": str(root / AB_VIEWS[0]),
        "ab_comparison_matrix_path": str(root / AB_VIEWS[1]),
        "ab_winner_summary_path": str(root / AB_VIEWS[2]),
        "near_miss_ab_comparison_report_path": str(root / AB_VIEWS[3]),
        "near_miss_ab_comparison_input_snapshot_path": str(
            root / "near_miss_ab_comparison_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        AB_VIEWS[0]: foundation._json_bytes(manifest),
        AB_VIEWS[1]: foundation._jsonl_bytes(rows),
        AB_VIEWS[2]: foundation._json_bytes(summary),
        AB_VIEWS[3]: foundation._text_file_bytes(render_near_miss_ab_report(manifest, summary)),
    }
    return manifest, rows, summary, views


def run_near_miss_ab_comparison(
    *,
    v3_backfill_id: str,
    near_miss_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    backfill = _validated_targeted_backfill(v3_backfill_id, v3_backfill_dir)
    matrix_id = _text(backfill.get("v3_matrix_id"))
    matrix = _validated_matrix(matrix_id, v3_matrix_dir)
    near_miss = _validated_near_miss(near_miss_id, near_miss_dir)
    scorecard_id = _text(matrix.get("source_scorecard_id"))
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    policy = _policy(policy_path)
    policy_binding = foundation._file_binding(policy_path)
    backfill_binding = _binding(
        kind="targeted_v3_backfill",
        artifact_id=v3_backfill_id,
        root=Path(_text(backfill.get("v3_backfill_dir"))),
        names=BACKFILL_FILES,
    )
    matrix_binding = _binding(
        kind="targeted_search_v3",
        artifact_id=matrix_id,
        root=Path(_text(matrix.get("v3_matrix_dir"))),
        names=MATRIX_FILES,
    )
    near_miss_binding = _binding(
        kind="near_miss_candidates",
        artifact_id=near_miss_id,
        root=Path(_text(near_miss.get("near_miss_dir"))),
        names=diagnostics.NEAR_MISS_FILES,
    )
    scorecard_binding = _binding(
        kind="weight_scorecard",
        artifact_id=scorecard_id,
        root=Path(_text(scorecard.get("scorecard_dir"))),
        names=evaluation.SCORECARD_FILES,
    )
    ab_id = _stable_id(
        "near-miss-ab-comparison", v3_backfill_id, near_miss_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / ab_id)
    manifest, rows, summary, views = _ab_material(
        root=root,
        ab_id=root.name,
        backfill=backfill,
        matrix=matrix,
        near_miss=near_miss,
        scorecard=scorecard,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / AB_VIEWS[0], manifest)
    _write_jsonl(root / AB_VIEWS[1], rows)
    _write_json(root / AB_VIEWS[2], summary)
    _write_text(root / AB_VIEWS[3], render_near_miss_ab_report(manifest, summary))
    snapshot = {
        "schema_version": AB_INPUT_SCHEMA,
        "ab_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_binding,
        "backfill_source": backfill_binding,
        "matrix_source": matrix_binding,
        "near_miss_source": near_miss_binding,
        "scorecard_source": scorecard_binding,
        "view_hashes": foundation._view_hashes(root, AB_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "near_miss_ab_comparison_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_near_miss_ab_comparison", root.name, root / AB_VIEWS[0])
    return {
        "ab_id": root.name,
        "ab_dir": root,
        "manifest": manifest,
        "ab_comparison_matrix": rows,
        "ab_winner_summary": summary,
    }


def near_miss_ab_comparison_report_payload(
    *,
    ab_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=ab_id,
        latest_pointer="latest_near_miss_ab_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name=AB_VIEWS[0],
    )
    return {
        **_read_json(root / AB_VIEWS[0]),
        "ab_comparison_matrix": _read_jsonl(root / AB_VIEWS[1]),
        "ab_winner_summary": _read_json(root / AB_VIEWS[2]),
        "input_snapshot": _read_json(root / "near_miss_ab_comparison_input_snapshot.json"),
        "ab_dir": str(root),
    }


def _rebuild_ab(root: Path, ab_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "near_miss_ab_comparison_input_snapshot.json")
    _require(snapshot.get("schema_version") == AB_INPUT_SCHEMA, "A/B snapshot schema")
    _require(snapshot.get("ab_id") == ab_id, "A/B snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    near_source = _mapping(snapshot.get("near_miss_source"))
    score_source = _mapping(snapshot.get("scorecard_source"))
    _validate_binding(backfill_source, kind="targeted_v3_backfill")
    _validate_binding(matrix_source, kind="targeted_search_v3")
    _validate_binding(near_source, kind="near_miss_candidates")
    _validate_binding(score_source, kind="weight_scorecard")
    backfill = _validated_targeted_backfill(
        _source_id(backfill_source), _source_dir(backfill_source).parent
    )
    matrix = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    near_miss = _validated_near_miss(_source_id(near_source), _source_dir(near_source).parent)
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _, _, _, expected = _ab_material(
        root=root,
        ab_id=ab_id,
        backfill=backfill,
        matrix=matrix,
        near_miss=near_miss,
        scorecard=scorecard,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


def validate_near_miss_ab_comparison_artifact(
    *, ab_id: str, output_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR
) -> dict[str, Any]:
    root = output_dir / ab_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="near_miss_ab_comparison_input_snapshot.json",
        schema=AB_INPUT_SCHEMA,
        id_key="ab_id",
        artifact_id=ab_id,
        view_names=AB_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_near_miss_ab_comparison_validation", ab_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_near_miss_ab_comparison_validation",
        artifact_id=ab_id,
        checks=checks,
        rebuild=lambda: _rebuild_ab(root, ab_id),
    )


__all__ = [
    "DEFAULT_NEAR_MISS_AB_COMPARISON_DIR",
    "DEFAULT_NEAR_MISS_CANDIDATES_DIR",
    "DEFAULT_SEARCH_COVERAGE_GAP_DIR",
    "DEFAULT_TARGETED_SEARCH_V3_DIR",
    "DEFAULT_TARGETED_V3_BACKFILL_DIR",
    "DEFAULT_WEIGHT_SCORECARD_DIR",
    "DEFAULT_WEIGHT_SEARCH_TARGETED_POLICY_PATH",
    "build_targeted_search_v3",
    "near_miss_ab_comparison_report_payload",
    "resume_targeted_v3_backfill",
    "run_near_miss_ab_comparison",
    "run_targeted_v3_backfill",
    "targeted_search_v3_report_payload",
    "targeted_v3_backfill_report_payload",
    "validate_near_miss_ab_comparison_artifact",
    "validate_targeted_search_v3_artifact",
    "validate_targeted_v3_backfill_artifact",
]
