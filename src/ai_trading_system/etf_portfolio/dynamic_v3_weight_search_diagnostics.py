from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation

DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_search_diagnostics_v1.yaml"
)
DEFAULT_NO_PROMOTION_REVIEW_DIR = _legacy.DEFAULT_NO_PROMOTION_REVIEW_DIR
DEFAULT_NEAR_MISS_CANDIDATES_DIR = _legacy.DEFAULT_NEAR_MISS_CANDIDATES_DIR
DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR = _legacy.DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR
DEFAULT_SEARCH_COVERAGE_GAP_DIR = _legacy.DEFAULT_SEARCH_COVERAGE_GAP_DIR
DEFAULT_WEIGHT_SCORECARD_DIR = evaluation.DEFAULT_WEIGHT_SCORECARD_DIR
DEFAULT_WEIGHT_SEARCH_SPACE_DIR = foundation.DEFAULT_WEIGHT_SEARCH_SPACE_DIR

REVIEW_INPUT_SCHEMA = "no_promotion_review_input_snapshot.v2"
NEAR_MISS_INPUT_SCHEMA = "near_miss_candidates_input_snapshot.v2"
CASH_INPUT_SCHEMA = "cash_buffer_attribution_input_snapshot.v2"
COVERAGE_INPUT_SCHEMA = "search_coverage_gap_input_snapshot.v2"

REVIEW_VIEWS = (
    "no_promotion_review_manifest.json",
    "no_promotion_reason_summary.json",
    "gate_failure_distribution.json",
    "score_component_failure_matrix.json",
    "no_promotion_review_report.md",
    "reader_brief_section.md",
)
NEAR_MISS_VIEWS = (
    "near_miss_manifest.json",
    "near_miss_candidates.jsonl",
    "near_miss_family_summary.json",
    "near_miss_report.md",
    "reader_brief_section.md",
)
CASH_VIEWS = (
    "cash_buffer_attribution_manifest.json",
    "cash_buffer_effect_summary.json",
    "cash_buffer_failure_reason.json",
    "cash_buffer_variant_recommendations.json",
    "cash_buffer_attribution_report.md",
)
COVERAGE_VIEWS = (
    "search_coverage_gap_manifest.json",
    "family_coverage_gap.json",
    "parameter_coverage_gap.json",
    "targeted_v3_recommendations.json",
    "search_coverage_gap_report.md",
)

REVIEW_FILES = (*REVIEW_VIEWS, "no_promotion_review_input_snapshot.json")
NEAR_MISS_FILES = (*NEAR_MISS_VIEWS, "near_miss_candidates_input_snapshot.json")
CASH_FILES = (*CASH_VIEWS, "cash_buffer_attribution_input_snapshot.json")
COVERAGE_FILES = (*COVERAGE_VIEWS, "search_coverage_gap_input_snapshot.json")
SEARCH_SPACE_FILES = (
    "weight_search_space_manifest.json",
    "normalized_search_space.yaml",
    "search_family_inventory.json",
    "weight_search_space_report.md",
    "weight_search_space_input_snapshot.json",
)

_mapping = _legacy._mapping
_records = _legacy._records
_texts = _legacy._texts
_text = _legacy._text
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
render_no_promotion_reader_brief = _legacy.render_no_promotion_reader_brief
render_no_promotion_review_report = _legacy.render_no_promotion_review_report
render_near_miss_reader_brief = _legacy.render_near_miss_reader_brief
render_near_miss_report = _legacy.render_near_miss_report
render_cash_buffer_attribution_report = _legacy.render_cash_buffer_attribution_report
render_search_coverage_gap_report = _legacy.render_search_coverage_gap_report


class DynamicV3WeightSearchDiagnosticsError(ValueError):
    """Raised when the diagnostics chain cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchDiagnosticsError(message)


def _number(value: Any, field: str) -> float:
    _require(not isinstance(value, bool), f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DynamicV3WeightSearchDiagnosticsError(f"{field} must be numeric") from exc
    _require(math.isfinite(result), f"{field} must be finite")
    return result


def _optional_number(value: Any, field: str) -> float | None:
    if value is None or value == "":
        return None
    return _number(value, field)


def _integer(value: Any, field: str) -> int:
    number = _number(value, field)
    _require(number.is_integer(), f"{field} must be an integer")
    return int(number)


def _aware_datetime(value: datetime | str, field: str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _generated_time(generated_at: datetime | None) -> datetime:
    return _aware_datetime(generated_at or datetime.now(UTC), "generated_at")


def _chronology(generated: datetime, *sources: Mapping[str, Any]) -> None:
    for index, source in enumerate(sources):
        source_time = _aware_datetime(
            _text(source.get("generated_at")), f"source[{index}].generated_at"
        )
        _require(source_time <= generated, "diagnostics source chronology invalid")


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_weight_search_diagnostics_policy.v1",
        "diagnostics policy schema mismatch",
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
        _require(bool(_text(metadata.get(field))), f"diagnostics policy metadata missing: {field}")
    _require(
        metadata.get("status") in {"pilot_baseline", "reviewed", "active"},
        "diagnostics policy status invalid",
    )
    no_promotion = _mapping(payload.get("no_promotion"))
    near_miss = _mapping(payload.get("near_miss"))
    cash = _mapping(payload.get("cash_buffer"))
    coverage = _mapping(payload.get("coverage"))
    promote = _number(no_promotion.get("promotion_score"), "promotion_score")
    keep = _number(no_promotion.get("keep_testing_score"), "keep_testing_score")
    _require(0.0 <= keep < promote <= 1.0, "scorecard thresholds invalid")
    _require(
        0.0 <= _number(no_promotion.get("near_miss_margin"), "near_miss_margin") < promote,
        "near-miss margin invalid",
    )
    _require(
        _integer(no_promotion.get("primary_reason_limit"), "primary_reason_limit") > 0,
        "primary reason limit invalid",
    )
    _require(
        _integer(near_miss.get("maximum_failed_gates"), "maximum_failed_gates") >= 0,
        "failed gate limit invalid",
    )
    _require(
        _integer(near_miss.get("maximum_candidates"), "maximum_candidates") > 0,
        "candidate limit invalid",
    )
    _require(
        0.0 <= _number(near_miss.get("minimum_overall_score"), "minimum_overall_score") <= 1.0,
        "near-miss overall threshold invalid",
    )
    _require(
        0.0 <= _number(near_miss.get("minimum_component_score"), "minimum_component_score") <= 1.0,
        "near-miss component threshold invalid",
    )
    _require(bool(_texts(no_promotion.get("gate_universe"))), "gate universe missing")
    _require(
        bool(_mapping(no_promotion.get("hard_reject_gate_map"))), "hard-reject gate map missing"
    )
    _require(bool(_mapping(no_promotion.get("gate_reason_map"))), "gate reason map missing")
    _require(bool(_texts(cash.get("recommended_variants"))), "cash recommendations missing")
    _require(bool(_texts(coverage.get("recommended_focus"))), "coverage focus missing")
    _require(
        _integer(coverage.get("maximum_targeted_v3_variants"), "maximum_targeted_v3_variants") > 0,
        "coverage variant cap invalid",
    )
    safety = _mapping(payload.get("safety"))
    for field in (
        "research_screening_only",
        "experiment_only",
        "not_formal_research_method",
        "not_official_target_weights",
        "paper_shadow_only",
    ):
        _require(safety.get(field) is True, f"diagnostics safety must enable {field}")
    for field in (
        "broker_action_allowed",
        "broker_action_taken",
        "order_ticket_generated",
        "auto_apply",
    ):
        _require(safety.get(field) is False, f"diagnostics safety must disable {field}")
    _require(safety.get("production_effect") == "none", "diagnostics production effect invalid")
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


def _validated_scorecard(scorecard_id: str, scorecard_dir: Path) -> dict[str, Any]:
    validation = evaluation.validate_weight_scorecard_artifact(
        scorecard_id=scorecard_id, output_dir=scorecard_dir
    )
    _require(validation.get("status") == "PASS", "source scorecard validation failed")
    payload = evaluation.weight_scorecard_report_payload(
        scorecard_id=scorecard_id, output_dir=scorecard_dir
    )
    _require(payload.get("status") == "PASS", "source scorecard status failed")
    return payload


def _validated_search_space(search_space_id: str, search_space_dir: Path) -> dict[str, Any]:
    validation = foundation.validate_weight_search_space_artifact(
        search_space_id=search_space_id, output_dir=search_space_dir
    )
    _require(validation.get("status") == "PASS", "source search-space validation failed")
    payload = foundation.weight_search_space_report_payload(
        search_space_id=search_space_id, output_dir=search_space_dir
    )
    _require(payload.get("status") == "PASS", "source search-space status failed")
    return payload


def _validated_review(review_id: str, review_dir: Path) -> dict[str, Any]:
    validation = validate_no_promotion_review_artifact(review_id=review_id, output_dir=review_dir)
    _require(validation.get("status") == "PASS", "source no-promotion review validation failed")
    return no_promotion_review_report_payload(review_id=review_id, output_dir=review_dir)


def _validated_near_miss(near_miss_id: str, near_miss_dir: Path) -> dict[str, Any]:
    validation = validate_near_miss_candidates_artifact(
        near_miss_id=near_miss_id, output_dir=near_miss_dir
    )
    _require(validation.get("status") == "PASS", "source near-miss validation failed")
    return near_miss_candidates_report_payload(near_miss_id=near_miss_id, output_dir=near_miss_dir)


def _validated_attribution(attribution_id: str, attribution_dir: Path) -> dict[str, Any]:
    validation = validate_cash_buffer_attribution_artifact(
        attribution_id=attribution_id, output_dir=attribution_dir
    )
    _require(validation.get("status") == "PASS", "source cash-buffer attribution validation failed")
    return cash_buffer_attribution_report_payload(
        attribution_id=attribution_id, output_dir=attribution_dir
    )


def _snapshot_preflight(
    *,
    root: Path,
    snapshot_name: str,
    schema: str,
    id_key: str,
    artifact_id: str,
    view_names: Sequence[str],
) -> tuple[list[dict[str, Any]], bool]:
    checks = _legacy._required_file_checks(root, (*view_names, snapshot_name))
    try:
        snapshot = _read_json(root / snapshot_name)
        schema_ok = snapshot.get("schema_version") == schema
        id_ok = snapshot.get(id_key) == artifact_id
        hashes = _mapping(snapshot.get("view_hashes"))
        keys_ok = set(hashes) == set(view_names)
        errors = foundation._validate_view_hashes(root, hashes)
        checks.extend(
            (
                st._check("snapshot_schema", schema_ok, _text(snapshot.get("schema_version"))),
                st._check("snapshot_artifact_id", id_ok, _text(snapshot.get(id_key))),
                st._check("snapshot_view_hash_keys", keys_ok, ""),
                st._check("snapshot_view_hashes", not errors, "; ".join(errors)),
            )
        )
        return checks, schema_ok and id_ok and keys_ok and not errors
    except Exception as exc:
        checks.append(st._check("snapshot_preflight", False, str(exc)))
        return checks, False


def _check_bytes(root: Path, expected: Mapping[str, bytes]) -> list[dict[str, Any]]:
    return [
        st._check(
            f"content_rebuild_{name}",
            (root / name).is_file() and (root / name).read_bytes() == payload,
            name,
        )
        for name, payload in expected.items()
    ]


def _validate_content(
    *,
    report_type: str,
    artifact_id: str,
    checks: list[dict[str, Any]],
    rebuild: Callable[[], list[dict[str, Any]]],
) -> dict[str, Any]:
    try:
        checks.extend(rebuild())
    except Exception as exc:
        checks.append(st._check("content_rebuild", False, str(exc)))
    return _validation_payload(report_type, artifact_id, checks)


def _view_hash_check(root: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    errors = foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes")))
    return st._check("view_hashes", not errors, "; ".join(errors))


def _gate_universe(policy: Mapping[str, Any]) -> list[str]:
    return _texts(_mapping(policy.get("no_promotion")).get("gate_universe"))


def _failed_gates(row: Mapping[str, Any], policy: Mapping[str, Any]) -> list[str]:
    no_promotion = _mapping(policy.get("no_promotion"))
    near_miss = _mapping(policy.get("near_miss"))
    gate_map = _mapping(no_promotion.get("hard_reject_gate_map"))
    failed: set[str] = {
        _text(gate_map.get(flag))
        for flag in _texts(row.get("hard_reject_flags"))
        if _text(gate_map.get(flag))
    }
    overall = _optional_number(row.get("overall_score"), "overall_score")
    if overall is None or overall < _number(no_promotion.get("promotion_score"), "promotion_score"):
        failed.add("composite_score_gate")
    return_component = _optional_number(
        _mapping(row.get("score_components")).get("return"), "score_components.return"
    )
    if return_component is None or return_component < _number(
        near_miss.get("return_preservation_gate_score"), "return_preservation_gate_score"
    ):
        failed.add("return_preservation_gate")
    return [gate for gate in _gate_universe(policy) if gate in failed]


def _is_near_miss(row: Mapping[str, Any], policy: Mapping[str, Any]) -> bool:
    if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION":
        return False
    near = _mapping(policy.get("near_miss"))
    failed = _failed_gates(row, policy)
    if len(failed) > _integer(near.get("maximum_failed_gates"), "maximum_failed_gates"):
        return False
    overall = _optional_number(row.get("overall_score"), "overall_score")
    components = [
        value
        for key, raw in _mapping(row.get("score_components")).items()
        if (value := _optional_number(raw, f"score_components.{key}")) is not None
    ]
    strongest = max(components) if components else None
    return (
        overall is not None
        and overall >= _number(near.get("minimum_overall_score"), "minimum_overall_score")
    ) or (
        strongest is not None
        and strongest >= _number(near.get("minimum_component_score"), "minimum_component_score")
    )


def _gate_failure_distribution(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    failures = []
    for gate in _gate_universe(policy):
        failed_rows = [row for row in rows if gate in _failed_gates(row, policy)]
        failures.append(
            {
                "gate": gate,
                "failed_count": len(failed_rows),
                "near_miss_count": sum(1 for row in failed_rows if _is_near_miss(row, policy)),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "failures": failures,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    clean = sorted(values)
    if not clean:
        return None
    index = min(len(clean) - 1, max(0, int(round((len(clean) - 1) * percentile))))
    return clean[index]


def _component_matrix(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    no_promotion = _mapping(policy.get("no_promotion"))
    weak_limit = _number(no_promotion.get("weak_component_score"), "weak_component_score")
    percentile = _number(no_promotion.get("component_percentile"), "component_percentile")
    component_names = sorted({key for row in rows for key in _mapping(row.get("score_components"))})
    components = []
    for component in component_names:
        scored = [
            (row, value)
            for row in rows
            if (
                value := _optional_number(
                    _mapping(row.get("score_components")).get(component),
                    f"score_components.{component}",
                )
            )
            is not None
        ]
        values = [value for _, value in scored]
        ranked = sorted(scored, key=lambda pair: pair[1], reverse=True)
        p_value = _percentile(values, percentile)
        components.append(
            {
                "component": component,
                "avg_score": round(sum(values) / len(values), 6) if values else None,
                "p90_score": round(p_value, 6) if p_value is not None else None,
                "top_variant": _text(ranked[0][0].get("variant_id")) if ranked else "",
                "weak_count": sum(1 for value in values if value < weak_limit),
                "sample_count": len(values),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    components.sort(key=lambda row: (row.get("avg_score") is None, row.get("avg_score") or 0.0))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "components": components,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _gate_assessment(
    scorecard: Mapping[str, Any], distribution: Mapping[str, Any], policy: Mapping[str, Any]
) -> str:
    rows = _records(scorecard.get("variant_scorecard"))
    if not rows or scorecard.get("data_quality_status") == "FAIL":
        return "INCONCLUSIVE"
    no_promotion = _mapping(policy.get("no_promotion"))
    scores = [_optional_number(row.get("overall_score"), "overall_score") for row in rows]
    finite_scores = [score for score in scores if score is not None]
    if not finite_scores:
        return "INCONCLUSIVE"
    threshold = _number(no_promotion.get("promotion_score"), "promotion_score") - _number(
        no_promotion.get("near_miss_margin"), "near_miss_margin"
    )
    near_count = sum(1 for row in rows if _is_near_miss(row, policy))
    if max(finite_scores) >= threshold and near_count >= _integer(
        no_promotion.get("minimum_near_miss_count_for_too_strict"),
        "minimum_near_miss_count_for_too_strict",
    ):
        return "TOO_STRICT"
    severe_gates = set(_texts(no_promotion.get("severe_gate_ids")))
    severe_count = sum(
        _integer(item.get("failed_count"), "failed_count")
        for item in _records(distribution.get("failures"))
        if item.get("gate") in severe_gates
    )
    severe_floor = max(
        1,
        math.ceil(
            len(rows)
            * _number(no_promotion.get("severe_failure_fraction"), "severe_failure_fraction")
        ),
    )
    if severe_count >= severe_floor or max(finite_scores) < _number(
        no_promotion.get("keep_testing_score"), "keep_testing_score"
    ):
        return "REASONABLE"
    return "INCONCLUSIVE"


def _reason_summary(
    scorecard: Mapping[str, Any],
    distribution: Mapping[str, Any],
    matrix: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("variant_scorecard"))
    no_promotion = _mapping(policy.get("no_promotion"))
    reason_map = _mapping(no_promotion.get("gate_reason_map"))
    high_gates = set(_texts(no_promotion.get("high_severity_gate_ids")))
    severity_fraction = _number(
        no_promotion.get("severe_failure_fraction"), "severe_failure_fraction"
    )
    primary = []
    for row in _records(distribution.get("failures")):
        count = _integer(row.get("failed_count"), "failed_count")
        if count <= 0:
            continue
        gate = _text(row.get("gate"))
        primary.append(
            {
                "reason": _text(reason_map.get(gate)) or gate,
                "variant_count": count,
                "severity": "HIGH"
                if gate in high_gates or (rows and count / len(rows) >= severity_fraction)
                else "MEDIUM",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    if not primary:
        weak_limit = _number(no_promotion.get("weak_component_score"), "weak_component_score")
        weak = [
            row
            for row in _records(matrix.get("components"))
            if row.get("avg_score") is not None
            and _number(row.get("avg_score"), "avg_score") < weak_limit
        ]
        primary = [
            {
                "reason": f"{row.get('component')}_weak",
                "variant_count": len(rows),
                "severity": "MEDIUM",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
            for row in weak
        ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "review_id": "",
        "source_scorecard_id": scorecard.get("scorecard_id"),
        "variants_reviewed": len(rows),
        "promoted_candidate_count": sum(
            1 for row in rows if row.get("scorecard_decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
        ),
        "primary_reasons": primary[
            : _integer(no_promotion.get("primary_reason_limit"), "primary_reason_limit")
        ],
        "gate_assessment": _gate_assessment(scorecard, distribution, policy),
        "recommended_next_action": no_promotion.get("recommended_next_action"),
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _review_outputs(
    scorecard: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    rows = _records(scorecard.get("variant_scorecard"))
    distribution = _gate_failure_distribution(rows, policy)
    matrix = _component_matrix(rows, policy)
    return _reason_summary(scorecard, distribution, matrix, policy), distribution, matrix


def _review_manifest(
    *,
    root: Path,
    review_id: str,
    scorecard: Mapping[str, Any],
    generated_at: str,
    reason: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_no_promotion_review_manifest",
        "review_id": review_id,
        "source_scorecard_id": scorecard.get("scorecard_id"),
        "source_search_space_id": scorecard.get("search_space_id"),
        "generated_at": generated_at,
        "status": "PASS" if _records(scorecard.get("variant_scorecard")) else "FAIL",
        "market_regime": scorecard.get("market_regime"),
        "date_start": scorecard.get("date_start"),
        "date_end": scorecard.get("date_end"),
        "data_quality_status": scorecard.get("data_quality_status"),
        "variants_reviewed": len(_records(scorecard.get("variant_scorecard"))),
        "promoted_candidate_count": reason.get("promoted_candidate_count"),
        "diagnostics_policy_version": _policy_version(policy),
        "no_promotion_review_manifest_path": str(root / REVIEW_VIEWS[0]),
        "no_promotion_reason_summary_path": str(root / REVIEW_VIEWS[1]),
        "gate_failure_distribution_path": str(root / REVIEW_VIEWS[2]),
        "score_component_failure_matrix_path": str(root / REVIEW_VIEWS[3]),
        "no_promotion_review_report_path": str(root / REVIEW_VIEWS[4]),
        "reader_brief_section_path": str(root / REVIEW_VIEWS[5]),
        "no_promotion_review_input_snapshot_path": str(
            root / "no_promotion_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def run_no_promotion_review(
    *,
    scorecard_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    policy = _policy(policy_path)
    _chronology(generated, scorecard)
    reason, distribution, matrix = _review_outputs(scorecard, policy)
    review_id = _stable_id("no-promotion-review", scorecard_id, generated.isoformat())
    root = _unique_dir(output_dir / review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _review_manifest(
        root=root,
        review_id=root.name,
        scorecard=scorecard,
        generated_at=generated.isoformat(),
        reason=reason,
        policy=policy,
    )
    reader = render_no_promotion_reader_brief(manifest, reason)
    _write_json(root / REVIEW_VIEWS[0], manifest)
    _write_json(root / REVIEW_VIEWS[1], reason)
    _write_json(root / REVIEW_VIEWS[2], distribution)
    _write_json(root / REVIEW_VIEWS[3], matrix)
    _write_text(
        root / REVIEW_VIEWS[4],
        render_no_promotion_review_report(manifest, reason, distribution, matrix),
    )
    _write_text(root / REVIEW_VIEWS[5], reader)
    snapshot = {
        "schema_version": REVIEW_INPUT_SCHEMA,
        "review_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(_text(scorecard.get("scorecard_dir"))),
            names=evaluation.SCORECARD_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, REVIEW_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "no_promotion_review_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_no_promotion_review", root.name, root / REVIEW_VIEWS[0])
    return {
        "review_id": root.name,
        "review_dir": root,
        "manifest": manifest,
        "no_promotion_reason_summary": reason,
        "gate_failure_distribution": distribution,
        "score_component_failure_matrix": matrix,
        "reader_brief_section": reader,
    }


def no_promotion_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_no_promotion_review",
        latest=latest,
        output_dir=output_dir,
        required_name=REVIEW_VIEWS[0],
    )
    return {
        **_read_json(root / REVIEW_VIEWS[0]),
        "no_promotion_reason_summary": _read_json(root / REVIEW_VIEWS[1]),
        "gate_failure_distribution": _read_json(root / REVIEW_VIEWS[2]),
        "score_component_failure_matrix": _read_json(root / REVIEW_VIEWS[3]),
        "reader_brief_section": (root / REVIEW_VIEWS[5]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "no_promotion_review_input_snapshot.json"),
        "review_dir": str(root),
    }


def _rebuild_review(root: Path, review_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "no_promotion_review_input_snapshot.json")
    _require(snapshot.get("schema_version") == REVIEW_INPUT_SCHEMA, "review snapshot schema")
    _require(snapshot.get("review_id") == review_id, "review snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    score_source = _mapping(snapshot.get("scorecard_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, scorecard)
    reason, distribution, matrix = _review_outputs(scorecard, policy)
    manifest = _review_manifest(
        root=root,
        review_id=review_id,
        scorecard=scorecard,
        generated_at=generated.isoformat(),
        reason=reason,
        policy=policy,
    )
    reader = render_no_promotion_reader_brief(manifest, reason)
    expected = {
        REVIEW_VIEWS[0]: foundation._json_bytes(manifest),
        REVIEW_VIEWS[1]: foundation._json_bytes(reason),
        REVIEW_VIEWS[2]: foundation._json_bytes(distribution),
        REVIEW_VIEWS[3]: foundation._json_bytes(matrix),
        REVIEW_VIEWS[4]: foundation._text_file_bytes(
            render_no_promotion_review_report(manifest, reason, distribution, matrix)
        ),
        REVIEW_VIEWS[5]: foundation._text_file_bytes(reader),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "review_safety", _payload_experiment_safe(manifest, reason, distribution, matrix), ""
        )
    )
    return checks


def validate_no_promotion_review_artifact(
    *, review_id: str, output_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / review_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="no_promotion_review_input_snapshot.json",
        schema=REVIEW_INPUT_SCHEMA,
        id_key="review_id",
        artifact_id=review_id,
        view_names=REVIEW_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_no_promotion_review_validation", review_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_no_promotion_review_validation",
        artifact_id=review_id,
        checks=checks,
        rebuild=lambda: _rebuild_review(root, review_id),
    )


def _near_miss_reason(row: Mapping[str, Any], failed: Sequence[str]) -> str:
    families = set(_texts(row.get("families")))
    if "cash_buffer" in families and "return_preservation_gate" in failed:
        return "strong_drawdown_but_weak_return"
    if "smoothing" in families and "recovery_lag_gate" in failed:
        return "smoothing_helped_stability_but_recovery_lagged"
    if failed == ["composite_score_gate"]:
        return "below_promotion_score_but_no_hard_reject"
    return (
        "limited_gate_failures_with_positive_components"
        if failed
        else "requires_forward_confirmation"
    )


def _suggested_adjustment(
    row: Mapping[str, Any], failed: Sequence[str], policy: Mapping[str, Any]
) -> str:
    near = _mapping(policy.get("near_miss"))
    by_family = _mapping(near.get("suggested_adjustment_by_family"))
    for family in _texts(row.get("families")):
        if _text(by_family.get(family)):
            return _text(by_family.get(family))
    if "return_preservation_gate" in failed:
        return _text(near.get("return_failure_adjustment"))
    return _text(near.get("default_suggested_adjustment"))


def _near_miss_rows(
    scorecard: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    candidates = []
    universe = _gate_universe(policy)
    for row in _records(scorecard.get("variant_scorecard")):
        if not _is_near_miss(row, policy):
            continue
        failed = _failed_gates(row, policy)
        families = _texts(row.get("families"))
        candidates.append(
            {
                "variant_id": row.get("variant_id"),
                "family": families[0] if families else "UNKNOWN",
                "families": families,
                "overall_score": row.get("overall_score"),
                "near_miss_rank": 0,
                "passed_gates": [gate for gate in universe if gate not in set(failed)],
                "failed_gates": failed,
                "near_miss_reason": _near_miss_reason(row, failed),
                "suggested_adjustment": _suggested_adjustment(row, failed, policy),
                "candidate_status": "NEAR_MISS",
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    candidates.sort(
        key=lambda item: _optional_number(item.get("overall_score"), "overall_score") or -math.inf,
        reverse=True,
    )
    for rank, row in enumerate(candidates, start=1):
        row["near_miss_rank"] = rank
    maximum = _integer(
        _mapping(policy.get("near_miss")).get("maximum_candidates"), "maximum_candidates"
    )
    return candidates[:maximum]


def _common_failed_gate(rows: Sequence[Mapping[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        for gate in _texts(row.get("failed_gates")):
            counts[gate] = counts.get(gate, 0) + 1
    return max(counts, key=counts.get) if counts else "none"


def _rank_families(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    scores: dict[str, list[float]] = {}
    for row in rows:
        score = _optional_number(row.get("overall_score"), "overall_score")
        if score is None:
            continue
        for family in _texts(row.get("families")):
            scores.setdefault(family, []).append(score)
    return [
        item[0]
        for item in sorted(
            scores.items(), key=lambda item: sum(item[1]) / len(item[1]), reverse=True
        )
    ]


def _near_miss_summary(
    candidates: Sequence[Mapping[str, Any]], scorecard: Mapping[str, Any], policy: Mapping[str, Any]
) -> dict[str, Any]:
    family_rows = []
    families = sorted({family for row in candidates for family in _texts(row.get("families"))})
    for family in families:
        rows = [row for row in candidates if family in _texts(row.get("families"))]
        best = max(
            rows,
            key=lambda row: (
                _optional_number(row.get("overall_score"), "overall_score") or -math.inf
            ),
        )
        family_rows.append(
            {
                "family": family,
                "near_miss_count": len(rows),
                "best_variant": best.get("variant_id"),
                "common_failure": _common_failed_gate(rows),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    near = _mapping(policy.get("near_miss"))
    recommended = [
        family
        for family in _texts(near.get("required_focus_families"))
        if any(family in _texts(row.get("families")) for row in candidates)
    ]
    if not recommended:
        recommended = _rank_families(_records(scorecard.get("variant_scorecard")))
    for family in _texts(near.get("required_focus_families")):
        if family not in recommended:
            recommended.append(family)
    recommended = recommended[
        : _integer(near.get("maximum_focus_families"), "maximum_focus_families")
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "families": family_rows,
        "recommended_focus_families": recommended,
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _near_manifest(
    *,
    root: Path,
    near_miss_id: str,
    scorecard: Mapping[str, Any],
    review: Mapping[str, Any],
    generated_at: str,
    candidates: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_near_miss_manifest",
        "near_miss_id": near_miss_id,
        "source_scorecard_id": scorecard.get("scorecard_id"),
        "source_search_space_id": scorecard.get("search_space_id"),
        "no_promotion_review_id": review.get("review_id"),
        "generated_at": generated_at,
        "status": "PASS" if candidates else "PASS_WITH_WARNINGS",
        "market_regime": scorecard.get("market_regime"),
        "date_start": scorecard.get("date_start"),
        "date_end": scorecard.get("date_end"),
        "candidate_count": len(candidates),
        "cash_buffer_10_near_miss": any(
            row.get("variant_id") == "cash_buffer_10" for row in candidates
        ),
        "source_review_gate_assessment": _mapping(review.get("no_promotion_reason_summary")).get(
            "gate_assessment"
        ),
        "diagnostics_policy_version": _policy_version(policy),
        "near_miss_manifest_path": str(root / NEAR_MISS_VIEWS[0]),
        "near_miss_candidates_path": str(root / NEAR_MISS_VIEWS[1]),
        "near_miss_family_summary_path": str(root / NEAR_MISS_VIEWS[2]),
        "near_miss_report_path": str(root / NEAR_MISS_VIEWS[3]),
        "reader_brief_section_path": str(root / NEAR_MISS_VIEWS[4]),
        "near_miss_candidates_input_snapshot_path": str(
            root / "near_miss_candidates_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def extract_near_miss_candidates(
    *,
    scorecard_id: str,
    no_promotion_review_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    review_dir: Path = DEFAULT_NO_PROMOTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    review = _validated_review(no_promotion_review_id, review_dir)
    policy = _policy(policy_path)
    _require(
        review.get("source_scorecard_id") == scorecard_id,
        "near-miss review/scorecard lineage mismatch",
    )
    _require(
        review.get("diagnostics_policy_version") == _policy_version(policy),
        "near-miss policy lineage mismatch",
    )
    _chronology(generated, scorecard, review)
    candidates = _near_miss_rows(scorecard, policy)
    summary = _near_miss_summary(candidates, scorecard, policy)
    near_miss_id = _stable_id(
        "near-miss-candidates", scorecard_id, no_promotion_review_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / near_miss_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _near_manifest(
        root=root,
        near_miss_id=root.name,
        scorecard=scorecard,
        review=review,
        generated_at=generated.isoformat(),
        candidates=candidates,
        policy=policy,
    )
    reader = render_near_miss_reader_brief(manifest, summary)
    _write_json(root / NEAR_MISS_VIEWS[0], manifest)
    _write_jsonl(root / NEAR_MISS_VIEWS[1], candidates)
    _write_json(root / NEAR_MISS_VIEWS[2], summary)
    _write_text(root / NEAR_MISS_VIEWS[3], render_near_miss_report(manifest, candidates, summary))
    _write_text(root / NEAR_MISS_VIEWS[4], reader)
    snapshot = {
        "schema_version": NEAR_MISS_INPUT_SCHEMA,
        "near_miss_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(_text(scorecard.get("scorecard_dir"))),
            names=evaluation.SCORECARD_FILES,
        ),
        "review_source": _binding(
            kind="no_promotion_review",
            artifact_id=no_promotion_review_id,
            root=Path(_text(review.get("review_dir"))),
            names=REVIEW_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, NEAR_MISS_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "near_miss_candidates_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_near_miss_candidates", root.name, root / NEAR_MISS_VIEWS[0])
    return {
        "near_miss_id": root.name,
        "near_miss_dir": root,
        "manifest": manifest,
        "near_miss_candidates": candidates,
        "near_miss_family_summary": summary,
        "reader_brief_section": reader,
    }


def near_miss_candidates_report_payload(
    *,
    near_miss_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=near_miss_id,
        latest_pointer="latest_near_miss_candidates",
        latest=latest,
        output_dir=output_dir,
        required_name=NEAR_MISS_VIEWS[0],
    )
    return {
        **_read_json(root / NEAR_MISS_VIEWS[0]),
        "near_miss_candidates": _read_jsonl(root / NEAR_MISS_VIEWS[1]),
        "near_miss_family_summary": _read_json(root / NEAR_MISS_VIEWS[2]),
        "reader_brief_section": (root / NEAR_MISS_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "near_miss_candidates_input_snapshot.json"),
        "near_miss_dir": str(root),
    }


def _rebuild_near_miss(root: Path, near_miss_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "near_miss_candidates_input_snapshot.json")
    _require(snapshot.get("schema_version") == NEAR_MISS_INPUT_SCHEMA, "near-miss snapshot schema")
    _require(snapshot.get("near_miss_id") == near_miss_id, "near-miss snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    score_source = _mapping(snapshot.get("scorecard_source"))
    review_source = _mapping(snapshot.get("review_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(review_source, kind="no_promotion_review")
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    review = _validated_review(_source_id(review_source), _source_dir(review_source).parent)
    _require(
        review.get("source_scorecard_id") == scorecard.get("scorecard_id"),
        "near-miss review/scorecard lineage mismatch",
    )
    _require(
        review.get("diagnostics_policy_version") == _policy_version(policy),
        "near-miss policy lineage mismatch",
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, scorecard, review)
    candidates = _near_miss_rows(scorecard, policy)
    summary = _near_miss_summary(candidates, scorecard, policy)
    manifest = _near_manifest(
        root=root,
        near_miss_id=near_miss_id,
        scorecard=scorecard,
        review=review,
        generated_at=generated.isoformat(),
        candidates=candidates,
        policy=policy,
    )
    reader = render_near_miss_reader_brief(manifest, summary)
    expected = {
        NEAR_MISS_VIEWS[0]: foundation._json_bytes(manifest),
        NEAR_MISS_VIEWS[1]: foundation._jsonl_bytes(candidates),
        NEAR_MISS_VIEWS[2]: foundation._json_bytes(summary),
        NEAR_MISS_VIEWS[3]: foundation._text_file_bytes(
            render_near_miss_report(manifest, candidates, summary)
        ),
        NEAR_MISS_VIEWS[4]: foundation._text_file_bytes(reader),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check("near_miss_safety", _payload_experiment_safe(manifest, summary, *candidates), "")
    )
    return checks


def validate_near_miss_candidates_artifact(
    *, near_miss_id: str, output_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR
) -> dict[str, Any]:
    root = output_dir / near_miss_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="near_miss_candidates_input_snapshot.json",
        schema=NEAR_MISS_INPUT_SCHEMA,
        id_key="near_miss_id",
        artifact_id=near_miss_id,
        view_names=NEAR_MISS_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_near_miss_candidates_validation", near_miss_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_near_miss_candidates_validation",
        artifact_id=near_miss_id,
        checks=checks,
        rebuild=lambda: _rebuild_near_miss(root, near_miss_id),
    )


def _scorecard_row(scorecard: Mapping[str, Any], variant_id: str) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in _records(scorecard.get("variant_scorecard"))
        if row.get("variant_id") == variant_id
    ]
    _require(len(matches) == 1, "cash-buffer variant must match exactly one scorecard row")
    return matches[0]


def _band(value: float | None, *, high: float, medium: float, labels: tuple[str, str, str]) -> str:
    if value is None:
        return "INSUFFICIENT_DATA"
    if value >= high:
        return labels[0]
    if value >= medium:
        return labels[1]
    return labels[2]


def _cash_outputs(
    row: Mapping[str, Any], near_miss: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    cash = _mapping(policy.get("cash_buffer"))
    components = _mapping(row.get("score_components"))
    component_bands = _mapping(cash.get("component_labels"))
    cost_bands = _mapping(cash.get("return_cost_labels"))
    lag_bands = _mapping(cash.get("recovery_lag_labels"))
    interpretation = _mapping(cash.get("interpretation"))

    def component(name: str) -> float | None:
        return _optional_number(components.get(name), f"score_components.{name}")

    drawdown = component("drawdown")
    return_score = component("return")
    effect = {
        "schema_version": st.SCHEMA_VERSION,
        "variant_id": row.get("variant_id"),
        "family": cash.get("source_family"),
        "improvements": {
            name: _band(
                component(name),
                high=_number(component_bands.get("improved_minimum"), "improved_minimum"),
                medium=_number(component_bands.get("mixed_minimum"), "mixed_minimum"),
                labels=("IMPROVED", "MIXED", "WORSE"),
            )
            for name in ("drawdown", "turnover", "rolling_consistency", "sideways_choppy")
        },
        "costs": {
            "return_preservation": _band(
                return_score,
                high=_number(cost_bands.get("good_minimum"), "good_minimum"),
                medium=_number(cost_bands.get("acceptable_minimum"), "acceptable_minimum"),
                labels=("GOOD", "ACCEPTABLE", "POOR"),
            ),
            "strong_recovery_lag": _band(
                component("strong_recovery_lag"),
                high=_number(lag_bands.get("low_minimum"), "low_minimum"),
                medium=_number(lag_bands.get("medium_minimum"), "medium_minimum"),
                labels=("LOW", "MEDIUM", "HIGH"),
            ),
        },
        "overall_interpretation": (
            "defensive_buffer_helped_but_return_cost_too_high"
            if drawdown is not None
            and drawdown
            >= _number(interpretation.get("drawdown_helped_minimum"), "drawdown_helped_minimum")
            and (
                return_score is None
                or return_score
                < _number(interpretation.get("return_cost_threshold"), "return_cost_threshold")
            )
            else "defensive_buffer_helped_but_needs_confirmation"
            if drawdown is not None
            and drawdown
            >= _number(interpretation.get("drawdown_helped_minimum"), "drawdown_helped_minimum")
            else "cash_buffer_effect_mixed"
        ),
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    failed = _failed_gates(row, policy)
    reason_map = _mapping(cash.get("failure_reason_by_gate"))
    primary = _text(cash.get("default_failure_reason"))
    for gate in _texts(cash.get("failure_gate_precedence")):
        if gate in failed:
            primary = _text(reason_map.get(gate))
            break
    near_status = (
        "NEAR_MISS"
        if any(
            item.get("variant_id") == row.get("variant_id")
            for item in _records(near_miss.get("near_miss_candidates"))
        )
        else "NOT_SELECTED"
    )
    failure = {
        "schema_version": st.SCHEMA_VERSION,
        "variant_id": row.get("variant_id"),
        "promotion_failed": row.get("scorecard_decision") != "PROMOTE_TO_FORMAL_IMPLEMENTATION",
        "failed_gates": failed,
        "primary_failure_reason": primary,
        "can_be_refined": True,
        "recommended_refinement": _texts(cash.get("recommended_variants")),
        "near_miss_status": near_status,
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    recommendations = {
        "schema_version": st.SCHEMA_VERSION,
        "source_variant_id": row.get("variant_id"),
        "recommended_variants": _texts(cash.get("recommended_variants")),
        "recommended_direction": cash.get("recommended_direction"),
        "reason": _texts(cash.get("recommendation_reasons")),
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return effect, failure, recommendations


def _cash_manifest(
    *,
    root: Path,
    attribution_id: str,
    variant_id: str,
    scorecard: Mapping[str, Any],
    near_miss: Mapping[str, Any],
    generated_at: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cash_buffer_attribution_manifest",
        "attribution_id": attribution_id,
        "variant_id": variant_id,
        "source_scorecard_id": scorecard.get("scorecard_id"),
        "source_search_space_id": scorecard.get("search_space_id"),
        "near_miss_id": near_miss.get("near_miss_id"),
        "generated_at": generated_at,
        "status": "PASS",
        "market_regime": scorecard.get("market_regime"),
        "diagnostics_policy_version": _policy_version(policy),
        "cash_buffer_attribution_manifest_path": str(root / CASH_VIEWS[0]),
        "cash_buffer_effect_summary_path": str(root / CASH_VIEWS[1]),
        "cash_buffer_failure_reason_path": str(root / CASH_VIEWS[2]),
        "cash_buffer_variant_recommendations_path": str(root / CASH_VIEWS[3]),
        "cash_buffer_attribution_report_path": str(root / CASH_VIEWS[4]),
        "cash_buffer_attribution_input_snapshot_path": str(
            root / "cash_buffer_attribution_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def run_cash_buffer_attribution(
    *,
    variant_id: str,
    scorecard_id: str,
    near_miss_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    near_miss = _validated_near_miss(near_miss_id, near_miss_dir)
    policy = _policy(policy_path)
    _require(
        near_miss.get("source_scorecard_id") == scorecard_id,
        "cash attribution scorecard lineage mismatch",
    )
    _require(
        near_miss.get("diagnostics_policy_version") == _policy_version(policy),
        "cash attribution policy lineage mismatch",
    )
    _chronology(generated, scorecard, near_miss)
    row = _scorecard_row(scorecard, variant_id)
    effect, failure, recommendations = _cash_outputs(row, near_miss, policy)
    attribution_id = _stable_id(
        "cash-buffer-attribution", variant_id, scorecard_id, near_miss_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _cash_manifest(
        root=root,
        attribution_id=root.name,
        variant_id=variant_id,
        scorecard=scorecard,
        near_miss=near_miss,
        generated_at=generated.isoformat(),
        policy=policy,
    )
    _write_json(root / CASH_VIEWS[0], manifest)
    _write_json(root / CASH_VIEWS[1], effect)
    _write_json(root / CASH_VIEWS[2], failure)
    _write_json(root / CASH_VIEWS[3], recommendations)
    _write_text(
        root / CASH_VIEWS[4],
        render_cash_buffer_attribution_report(manifest, effect, failure, recommendations),
    )
    snapshot = {
        "schema_version": CASH_INPUT_SCHEMA,
        "attribution_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(_text(scorecard.get("scorecard_dir"))),
            names=evaluation.SCORECARD_FILES,
        ),
        "near_miss_source": _binding(
            kind="near_miss_candidates",
            artifact_id=near_miss_id,
            root=Path(_text(near_miss.get("near_miss_dir"))),
            names=NEAR_MISS_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, CASH_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "cash_buffer_attribution_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_cash_buffer_attribution", root.name, root / CASH_VIEWS[0])
    return {
        "attribution_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "cash_buffer_effect_summary": effect,
        "cash_buffer_failure_reason": failure,
        "cash_buffer_variant_recommendations": recommendations,
    }


def cash_buffer_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_cash_buffer_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name=CASH_VIEWS[0],
    )
    return {
        **_read_json(root / CASH_VIEWS[0]),
        "cash_buffer_effect_summary": _read_json(root / CASH_VIEWS[1]),
        "cash_buffer_failure_reason": _read_json(root / CASH_VIEWS[2]),
        "cash_buffer_variant_recommendations": _read_json(root / CASH_VIEWS[3]),
        "input_snapshot": _read_json(root / "cash_buffer_attribution_input_snapshot.json"),
        "attribution_dir": str(root),
    }


def _rebuild_cash(root: Path, attribution_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "cash_buffer_attribution_input_snapshot.json")
    _require(snapshot.get("schema_version") == CASH_INPUT_SCHEMA, "cash snapshot schema")
    _require(snapshot.get("attribution_id") == attribution_id, "cash snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    score_source = _mapping(snapshot.get("scorecard_source"))
    near_source = _mapping(snapshot.get("near_miss_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(near_source, kind="near_miss_candidates")
    scorecard = _validated_scorecard(_source_id(score_source), _source_dir(score_source).parent)
    near_miss = _validated_near_miss(_source_id(near_source), _source_dir(near_source).parent)
    _require(
        near_miss.get("source_scorecard_id") == scorecard.get("scorecard_id"),
        "cash attribution scorecard lineage mismatch",
    )
    _require(
        near_miss.get("diagnostics_policy_version") == _policy_version(policy),
        "cash attribution policy lineage mismatch",
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, scorecard, near_miss)
    stored = _read_json(root / CASH_VIEWS[0])
    variant_id = _text(stored.get("variant_id"))
    row = _scorecard_row(scorecard, variant_id)
    effect, failure, recommendations = _cash_outputs(row, near_miss, policy)
    manifest = _cash_manifest(
        root=root,
        attribution_id=attribution_id,
        variant_id=variant_id,
        scorecard=scorecard,
        near_miss=near_miss,
        generated_at=generated.isoformat(),
        policy=policy,
    )
    expected = {
        CASH_VIEWS[0]: foundation._json_bytes(manifest),
        CASH_VIEWS[1]: foundation._json_bytes(effect),
        CASH_VIEWS[2]: foundation._json_bytes(failure),
        CASH_VIEWS[3]: foundation._json_bytes(recommendations),
        CASH_VIEWS[4]: foundation._text_file_bytes(
            render_cash_buffer_attribution_report(manifest, effect, failure, recommendations)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "cash_safety", _payload_experiment_safe(manifest, effect, failure, recommendations), ""
        )
    )
    return checks


def validate_cash_buffer_attribution_artifact(
    *, attribution_id: str, output_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR
) -> dict[str, Any]:
    root = output_dir / attribution_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="cash_buffer_attribution_input_snapshot.json",
        schema=CASH_INPUT_SCHEMA,
        id_key="attribution_id",
        artifact_id=attribution_id,
        view_names=CASH_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_cash_buffer_attribution_validation", attribution_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_cash_buffer_attribution_validation",
        artifact_id=attribution_id,
        checks=checks,
        rebuild=lambda: _rebuild_cash(root, attribution_id),
    )


def _coverage_outputs(
    search_space: Mapping[str, Any],
    near_miss: Mapping[str, Any],
    attribution: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    coverage = _mapping(policy.get("coverage"))
    normalized = _mapping(search_space.get("normalized_search_space"))
    families = _mapping(normalized.get("families"))
    covered = sorted(_texts(search_space.get("families")))
    near_focus = sorted(
        _texts(
            _mapping(near_miss.get("near_miss_family_summary")).get("recommended_focus_families")
        )
    )
    family_gaps = [
        {
            "gap": row.get("gap"),
            "status": "MISSING_TARGETED_GRID",
            "reason": row.get("reason"),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for row in _records(coverage.get("family_gap_templates"))
    ]
    current_values = {
        "cash_buffer": _mapping(families.get("cash_buffer")).get("min_cash")
        or _mapping(families.get("cash_buffer")).get("min_cash_weight"),
        "smoothing_window": _mapping(families.get("smoothing")).get("windows"),
        "rebalance_threshold": _mapping(families.get("rebalance_threshold")).get("min_total_delta"),
        "top_k": None,
    }
    parameter_gaps = []
    new_ranges: dict[str, Any] = {}
    for row in _records(coverage.get("parameter_gap_templates")):
        parameter = _text(row.get("parameter"))
        current = current_values.get(parameter) or row.get("current_default_values") or []
        recommended = list(row.get("recommended_values") or [])
        new_ranges[parameter] = recommended
        parameter_gaps.append(
            {
                "parameter": parameter,
                "current_values": list(current),
                "recommended_values": recommended,
                "reason": row.get("recommendation"),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    family = {
        "schema_version": st.SCHEMA_VERSION,
        "covered_families": covered,
        "near_miss_focus_families": near_focus,
        "gaps": family_gaps,
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    parameter = {
        "schema_version": st.SCHEMA_VERSION,
        "attribution_id": attribution.get("attribution_id"),
        "gaps": parameter_gaps,
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    recommendations = {
        "schema_version": st.SCHEMA_VERSION,
        "recommended_focus": _texts(coverage.get("recommended_focus")),
        "new_parameter_ranges": new_ranges,
        "max_v3_variants": _integer(
            coverage.get("maximum_targeted_v3_variants"), "maximum_targeted_v3_variants"
        ),
        "diagnostics_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return family, parameter, recommendations


def _coverage_manifest(
    *,
    root: Path,
    coverage_gap_id: str,
    search_space: Mapping[str, Any],
    near_miss: Mapping[str, Any],
    attribution: Mapping[str, Any],
    generated_at: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_search_coverage_gap_manifest",
        "coverage_gap_id": coverage_gap_id,
        "search_space_id": search_space.get("search_space_id"),
        "near_miss_id": near_miss.get("near_miss_id"),
        "cash_buffer_attribution_id": attribution.get("attribution_id"),
        "source_scorecard_id": near_miss.get("source_scorecard_id"),
        "source_backfill_id": search_space.get("source_backfill_id")
        or _mapping(_mapping(search_space.get("normalized_search_space")).get("search")).get(
            "source_backfill_id"
        ),
        "generated_at": generated_at,
        "status": "PASS",
        "diagnostics_policy_version": _policy_version(policy),
        "search_coverage_gap_manifest_path": str(root / COVERAGE_VIEWS[0]),
        "family_coverage_gap_path": str(root / COVERAGE_VIEWS[1]),
        "parameter_coverage_gap_path": str(root / COVERAGE_VIEWS[2]),
        "targeted_v3_recommendations_path": str(root / COVERAGE_VIEWS[3]),
        "search_coverage_gap_report_path": str(root / COVERAGE_VIEWS[4]),
        "search_coverage_gap_input_snapshot_path": str(
            root / "search_coverage_gap_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def run_search_coverage_gap(
    *,
    search_space_id: str,
    near_miss_id: str,
    cash_buffer_attribution_id: str,
    search_space_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    near_miss_dir: Path = DEFAULT_NEAR_MISS_CANDIDATES_DIR,
    attribution_dir: Path = DEFAULT_CASH_BUFFER_ATTRIBUTION_DIR,
    output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    search_space = _validated_search_space(search_space_id, search_space_dir)
    near_miss = _validated_near_miss(near_miss_id, near_miss_dir)
    attribution = _validated_attribution(cash_buffer_attribution_id, attribution_dir)
    policy = _policy(policy_path)
    _require(
        near_miss.get("source_search_space_id") == search_space_id,
        "coverage search-space lineage mismatch",
    )
    _require(
        attribution.get("source_scorecard_id") == near_miss.get("source_scorecard_id"),
        "coverage scorecard lineage mismatch",
    )
    _require(attribution.get("near_miss_id") == near_miss_id, "coverage near-miss lineage mismatch")
    _require(
        near_miss.get("diagnostics_policy_version")
        == _policy_version(policy)
        == attribution.get("diagnostics_policy_version"),
        "coverage policy lineage mismatch",
    )
    _chronology(generated, search_space, near_miss, attribution)
    family, parameter, recommendations = _coverage_outputs(
        search_space, near_miss, attribution, policy
    )
    coverage_gap_id = _stable_id(
        "search-coverage-gap",
        search_space_id,
        near_miss_id,
        cash_buffer_attribution_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / coverage_gap_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _coverage_manifest(
        root=root,
        coverage_gap_id=root.name,
        search_space=search_space,
        near_miss=near_miss,
        attribution=attribution,
        generated_at=generated.isoformat(),
        policy=policy,
    )
    _write_json(root / COVERAGE_VIEWS[0], manifest)
    _write_json(root / COVERAGE_VIEWS[1], family)
    _write_json(root / COVERAGE_VIEWS[2], parameter)
    _write_json(root / COVERAGE_VIEWS[3], recommendations)
    _write_text(
        root / COVERAGE_VIEWS[4],
        render_search_coverage_gap_report(manifest, family, parameter, recommendations),
    )
    snapshot = {
        "schema_version": COVERAGE_INPUT_SCHEMA,
        "coverage_gap_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "search_space_source": _binding(
            kind="weight_search_space",
            artifact_id=search_space_id,
            root=Path(_text(search_space.get("search_space_dir"))),
            names=SEARCH_SPACE_FILES,
        ),
        "near_miss_source": _binding(
            kind="near_miss_candidates",
            artifact_id=near_miss_id,
            root=Path(_text(near_miss.get("near_miss_dir"))),
            names=NEAR_MISS_FILES,
        ),
        "attribution_source": _binding(
            kind="cash_buffer_attribution",
            artifact_id=cash_buffer_attribution_id,
            root=Path(_text(attribution.get("attribution_dir"))),
            names=CASH_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, COVERAGE_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "search_coverage_gap_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_search_coverage_gap", root.name, root / COVERAGE_VIEWS[0])
    return {
        "coverage_gap_id": root.name,
        "coverage_gap_dir": root,
        "manifest": manifest,
        "family_coverage_gap": family,
        "parameter_coverage_gap": parameter,
        "targeted_v3_recommendations": recommendations,
    }


def search_coverage_gap_report_payload(
    *,
    coverage_gap_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=coverage_gap_id,
        latest_pointer="latest_search_coverage_gap",
        latest=latest,
        output_dir=output_dir,
        required_name=COVERAGE_VIEWS[0],
    )
    return {
        **_read_json(root / COVERAGE_VIEWS[0]),
        "family_coverage_gap": _read_json(root / COVERAGE_VIEWS[1]),
        "parameter_coverage_gap": _read_json(root / COVERAGE_VIEWS[2]),
        "targeted_v3_recommendations": _read_json(root / COVERAGE_VIEWS[3]),
        "input_snapshot": _read_json(root / "search_coverage_gap_input_snapshot.json"),
        "coverage_gap_dir": str(root),
    }


def _rebuild_coverage(root: Path, coverage_gap_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "search_coverage_gap_input_snapshot.json")
    _require(snapshot.get("schema_version") == COVERAGE_INPUT_SCHEMA, "coverage snapshot schema")
    _require(snapshot.get("coverage_gap_id") == coverage_gap_id, "coverage snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    search_source = _mapping(snapshot.get("search_space_source"))
    near_source = _mapping(snapshot.get("near_miss_source"))
    attribution_source = _mapping(snapshot.get("attribution_source"))
    _validate_binding(search_source, kind="weight_search_space")
    _validate_binding(near_source, kind="near_miss_candidates")
    _validate_binding(attribution_source, kind="cash_buffer_attribution")
    search_space = _validated_search_space(
        _source_id(search_source), _source_dir(search_source).parent
    )
    near_miss = _validated_near_miss(_source_id(near_source), _source_dir(near_source).parent)
    attribution = _validated_attribution(
        _source_id(attribution_source), _source_dir(attribution_source).parent
    )
    _require(
        near_miss.get("source_search_space_id") == search_space.get("search_space_id"),
        "coverage search-space lineage mismatch",
    )
    _require(
        attribution.get("source_scorecard_id") == near_miss.get("source_scorecard_id"),
        "coverage scorecard lineage mismatch",
    )
    _require(
        attribution.get("near_miss_id") == near_miss.get("near_miss_id"),
        "coverage near-miss lineage mismatch",
    )
    _require(
        near_miss.get("diagnostics_policy_version")
        == _policy_version(policy)
        == attribution.get("diagnostics_policy_version"),
        "coverage policy lineage mismatch",
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _chronology(generated, search_space, near_miss, attribution)
    family, parameter, recommendations = _coverage_outputs(
        search_space, near_miss, attribution, policy
    )
    manifest = _coverage_manifest(
        root=root,
        coverage_gap_id=coverage_gap_id,
        search_space=search_space,
        near_miss=near_miss,
        attribution=attribution,
        generated_at=generated.isoformat(),
        policy=policy,
    )
    expected = {
        COVERAGE_VIEWS[0]: foundation._json_bytes(manifest),
        COVERAGE_VIEWS[1]: foundation._json_bytes(family),
        COVERAGE_VIEWS[2]: foundation._json_bytes(parameter),
        COVERAGE_VIEWS[3]: foundation._json_bytes(recommendations),
        COVERAGE_VIEWS[4]: foundation._text_file_bytes(
            render_search_coverage_gap_report(manifest, family, parameter, recommendations)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "coverage_safety",
            _payload_experiment_safe(manifest, family, parameter, recommendations),
            "",
        )
    )
    return checks


def validate_search_coverage_gap_artifact(
    *, coverage_gap_id: str, output_dir: Path = DEFAULT_SEARCH_COVERAGE_GAP_DIR
) -> dict[str, Any]:
    root = output_dir / coverage_gap_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="search_coverage_gap_input_snapshot.json",
        schema=COVERAGE_INPUT_SCHEMA,
        id_key="coverage_gap_id",
        artifact_id=coverage_gap_id,
        view_names=COVERAGE_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_search_coverage_gap_validation", coverage_gap_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_search_coverage_gap_validation",
        artifact_id=coverage_gap_id,
        checks=checks,
        rebuild=lambda: _rebuild_coverage(root, coverage_gap_id),
    )
