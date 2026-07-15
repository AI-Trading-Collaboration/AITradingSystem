from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_WEIGHT_SEARCH_FOLLOWUP_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "weight_search_followup_v1.yaml"
)
DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR = (
    _legacy.DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR
)
DEFAULT_CANDIDATE_PROMOTION_V2_DIR = _legacy.DEFAULT_CANDIDATE_PROMOTION_V2_DIR
DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR = _legacy.DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR
DEFAULT_TARGETED_SEARCH_V3_DIR = targeted.DEFAULT_TARGETED_SEARCH_V3_DIR
DEFAULT_TARGETED_V3_BACKFILL_DIR = targeted.DEFAULT_TARGETED_V3_BACKFILL_DIR
DEFAULT_NEAR_MISS_AB_COMPARISON_DIR = targeted.DEFAULT_NEAR_MISS_AB_COMPARISON_DIR

SENSITIVITY_INPUT_SCHEMA = "promotion_threshold_sensitivity_input_snapshot.v2"
PROMOTION_INPUT_SCHEMA = "candidate_promotion_v2_input_snapshot.v2"
NEXT_PLAN_INPUT_SCHEMA = "next_formal_or_search_plan_input_snapshot.v2"

SENSITIVITY_VIEWS = (
    "threshold_sensitivity_manifest.json",
    "threshold_scenarios.jsonl",
    "threshold_candidate_impact.json",
    "threshold_sensitivity_report.md",
)
PROMOTION_VIEWS = (
    "candidate_promotion_v2_manifest.json",
    "promotion_v2_decision.json",
    "promoted_candidates_v2.jsonl",
    "rejected_candidates_v2.jsonl",
    "keep_testing_candidates_v2.jsonl",
    "candidate_promotion_v2_report.md",
    "reader_brief_section.md",
)
NEXT_PLAN_VIEWS = (
    "next_formal_or_search_manifest.json",
    "next_plan_decision.json",
    "formal_method_candidates.json",
    "continue_search_plan.json",
    "owner_next_action_checklist.md",
    "next_formal_or_search_plan_report.md",
    "reader_brief_section.md",
)
SENSITIVITY_FILES = (*SENSITIVITY_VIEWS, "promotion_threshold_sensitivity_input_snapshot.json")
PROMOTION_FILES = (*PROMOTION_VIEWS, "candidate_promotion_v2_input_snapshot.json")
NEXT_PLAN_FILES = (*NEXT_PLAN_VIEWS, "next_formal_or_search_plan_input_snapshot.json")

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
_targeted_v3_scorecard_rows = _legacy._targeted_v3_scorecard_rows
_failed_gates = _legacy._failed_gates
_high_risk_gate_failure = _legacy._high_risk_gate_failure
render_threshold_sensitivity_report = _legacy.render_threshold_sensitivity_report
render_candidate_promotion_v2_reader_brief = _legacy.render_candidate_promotion_v2_reader_brief
render_candidate_promotion_v2_report = _legacy.render_candidate_promotion_v2_report
render_owner_next_action_checklist = _legacy.render_owner_next_action_checklist
render_next_plan_reader_brief = _legacy.render_next_plan_reader_brief
render_next_formal_or_search_plan_report = _legacy.render_next_formal_or_search_plan_report


class DynamicV3WeightSearchFollowupError(ValueError):
    """Raised when the targeted-search follow-up chain is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchFollowupError(message)


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


def _view_hash_check(root: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return diagnostics._view_hash_check(root, snapshot)


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


def _policy(path: Path) -> dict[str, Any]:
    payload = st._load_yaml_mapping(path)
    _require(
        payload.get("schema_version") == "dynamic_v3_weight_search_followup_policy.v1",
        "follow-up policy schema mismatch",
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
        _require(bool(_text(metadata.get(field))), f"follow-up policy metadata missing: {field}")
    _require(metadata.get("status") in {"pilot_baseline", "reviewed", "active"}, "policy status")

    sensitivity = _mapping(payload.get("sensitivity"))
    scenarios = _records(sensitivity.get("scenarios"))
    _require(len(scenarios) >= 2, "threshold scenarios missing")
    names = [_text(row.get("name")) for row in scenarios]
    _require(len(set(names)) == len(names) and all(names), "threshold scenario names invalid")
    authoritative = _text(sensitivity.get("authoritative_scenario"))
    floor_name = _text(sensitivity.get("relaxed_candidate_floor_scenario"))
    _require(authoritative in names and floor_name in names, "threshold scenario binding invalid")
    for row in scenarios:
        threshold = _number(row.get("score_threshold"), "score_threshold")
        _require(0.0 <= threshold <= 1.0, "score threshold outside [0, 1]")
        recommended = row.get("recommended")
        _require(isinstance(recommended, bool), "threshold recommended must be boolean")
        _require(bool(_text(row.get("reason"))), "threshold scenario reason missing")
        if _text(row.get("name")) == authoritative:
            _require(recommended is True, "authoritative threshold must be recommended")
        else:
            _require(recommended is False, "relaxed threshold cannot be recommended")
    _require(
        _integer(sensitivity.get("maximum_relaxed_candidates"), "maximum_relaxed_candidates")
        > 0,
        "relaxed candidate cap invalid",
    )
    for field in (
        "base_scorecard_decision",
        "relaxed_candidate_status",
        "policy_effect",
    ):
        _require(bool(_text(sensitivity.get(field))), f"sensitivity policy missing: {field}")

    promotion = _mapping(payload.get("candidate_promotion"))
    _require(bool(_texts(promotion.get("promotable_scorecard_decisions"))), "promotable decisions")
    _require(bool(_texts(promotion.get("promotable_ab_statuses"))), "promotable A/B statuses")
    _require(bool(_texts(promotion.get("keep_testing_scorecard_decisions"))), "keep decisions")
    for field in (
        "promoted_candidate_status",
        "keep_testing_candidate_status",
        "rejected_candidate_status",
    ):
        _require(bool(_text(promotion.get(field))), f"promotion status missing: {field}")
    _require(
        _integer(promotion.get("maximum_promoted_candidates"), "maximum_promoted_candidates")
        > 0,
        "promoted cap invalid",
    )
    _require(
        _integer(
            promotion.get("maximum_keep_testing_candidates"),
            "maximum_keep_testing_candidates",
        )
        > 0,
        "keep-testing cap invalid",
    )
    decision_rules = _mapping(promotion.get("decisions"))
    for key in ("promoted", "keep_testing", "rejected_only", "empty"):
        rule = _mapping(decision_rules.get(key))
        _require(bool(_text(rule.get("decision"))), f"promotion decision missing: {key}")
        _require(
            bool(_text(rule.get("recommended_next_action"))),
            f"promotion action missing: {key}",
        )

    next_plan = _mapping(payload.get("next_plan"))
    plan_rules = _mapping(next_plan.get("decisions"))
    for source_decision in (
        "PROMOTE_CANDIDATE",
        "KEEP_TESTING",
        "RUN_ANOTHER_TARGETED_SEARCH",
        "NO_CANDIDATE",
    ):
        rule = _mapping(plan_rules.get(source_decision))
        for field in ("decision", "recommended_next_action", "priority"):
            _require(
                bool(_text(rule.get(field))),
                f"next-plan rule missing: {source_decision}.{field}",
            )
        _require(
            isinstance(rule.get("should_continue_parameter_search"), bool),
            f"next-plan continue flag invalid: {source_decision}",
        )
        _require(bool(_texts(rule.get("recommended_actions"))), "next-plan actions missing")
    formal = _mapping(next_plan.get("formal_candidate"))
    for field in ("implementation_scope", "implementation_complexity"):
        _require(bool(_text(formal.get(field))), f"formal candidate policy missing: {field}")
    _require(
        formal.get("implementation_allowed_without_owner_approval") is False,
        "formal candidate owner approval must be required",
    )

    safety = _mapping(payload.get("safety"))
    for field in (
        "research_screening_only",
        "experiment_only",
        "not_formal_research_method",
        "not_official_target_weights",
        "paper_shadow_only",
        "owner_review_required",
    ):
        _require(safety.get(field) is True, f"follow-up safety must enable {field}")
    for field in (
        "implemented",
        "formal_method_task_created",
        "paper_shadow_primary_changed",
        "broker_action_allowed",
        "broker_action_taken",
        "order_ticket_generated",
        "auto_apply",
    ):
        _require(safety.get(field) is False, f"follow-up safety must disable {field}")
    _require(safety.get("production_effect") == "none", "follow-up production effect invalid")
    return payload


def _policy_version(policy: Mapping[str, Any]) -> str:
    return _text(_mapping(policy.get("policy_metadata")).get("policy_version"))


def _validated_backfill(v3_backfill_id: str, output_dir: Path) -> dict[str, Any]:
    payload = targeted._validated_targeted_backfill(v3_backfill_id, output_dir)
    _require(payload.get("status") == "PASS", "source targeted backfill status failed")
    return payload


def _validated_matrix(v3_matrix_id: str, output_dir: Path) -> dict[str, Any]:
    payload = targeted._validated_matrix(v3_matrix_id, output_dir)
    _require(payload.get("status") == "PASS", "source targeted matrix status failed")
    return payload


def _validated_ab(ab_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=targeted.validate_near_miss_ab_comparison_artifact,
        validator_key="ab_id",
        artifact_id=ab_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source A/B validation failed")
    payload = targeted.near_miss_ab_comparison_report_payload(
        ab_id=ab_id, output_dir=output_dir
    )
    _require(payload.get("status") == "PASS", "source A/B status failed")
    return payload


def _validated_sensitivity(sensitivity_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_promotion_threshold_sensitivity_artifact,
        validator_key="sensitivity_id",
        artifact_id=sensitivity_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source sensitivity validation failed")
    return promotion_threshold_sensitivity_report_payload(
        sensitivity_id=sensitivity_id, output_dir=output_dir
    )


def _validated_promotion(promotion_v2_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_candidate_promotion_v2_artifact,
        validator_key="promotion_v2_id",
        artifact_id=promotion_v2_id,
        root=output_dir,
    )
    _require(validation.get("status") == "PASS", "source promotion v2 validation failed")
    return candidate_promotion_v2_report_payload(
        promotion_v2_id=promotion_v2_id, output_dir=output_dir
    )


def _ensure_common_lineage(
    backfill: Mapping[str, Any], matrix: Mapping[str, Any], ab: Mapping[str, Any]
) -> None:
    _require(
        _text(backfill.get("v3_matrix_id")) == _text(matrix.get("v3_matrix_id")),
        "backfill/matrix lineage mismatch",
    )
    _require(
        _text(ab.get("v3_backfill_id")) == _text(backfill.get("v3_backfill_id")),
        "A/B/backfill lineage mismatch",
    )
    _require(
        _text(ab.get("v3_matrix_id")) == _text(matrix.get("v3_matrix_id")),
        "A/B/matrix lineage mismatch",
    )
    _require(
        _text(ab.get("source_scorecard_id")) == _text(matrix.get("source_scorecard_id")),
        "A/B/scorecard lineage mismatch",
    )


def _threshold_scenarios(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    scenarios: list[dict[str, Any]] = []
    for spec in _records(_mapping(policy.get("sensitivity")).get("scenarios")):
        threshold = _number(spec.get("score_threshold"), "score_threshold")
        promoted = [
            row
            for row in rows
            if _float(row.get("overall_score")) >= threshold and not _high_risk_gate_failure(row)
        ]
        scenarios.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "scenario": _text(spec.get("name")),
                "score_threshold": round(threshold, 6),
                "promote_count": len(promoted),
                "high_risk_promote_count": 0,
                "recommended": spec.get("recommended"),
                "reason": _text(spec.get("reason")),
                "followup_policy_version": _policy_version(policy),
                **st.EXPERIMENT_FACTORY_SAFETY,
            }
        )
    return scenarios


def _threshold_candidate_impact(
    rows: Sequence[Mapping[str, Any]],
    ab: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    sensitivity = _mapping(policy.get("sensitivity"))
    specs = {
        _text(row.get("name")): row for row in _records(sensitivity.get("scenarios"))
    }
    floor_spec = _mapping(specs.get(_text(sensitivity.get("relaxed_candidate_floor_scenario"))))
    floor = _number(floor_spec.get("score_threshold"), "relaxed score threshold")
    base_decision = _text(sensitivity.get("base_scorecard_decision"))
    ab_rows = {
        _text(row.get("variant_id")): row for row in _records(ab.get("ab_comparison_matrix"))
    }
    base = {
        _text(row.get("variant_id"))
        for row in rows
        if row.get("scorecard_decision") == base_decision and not _high_risk_gate_failure(row)
    }
    relaxed = [
        row
        for row in rows
        if _float(row.get("overall_score")) >= floor
        and not _high_risk_gate_failure(row)
        and _text(row.get("variant_id")) not in base
    ]
    maximum = _integer(sensitivity.get("maximum_relaxed_candidates"), "maximum relaxed")
    relaxed_rows = [
        {
            "variant_id": row.get("variant_id"),
            "overall_score": row.get("overall_score"),
            "failed_gates": _failed_gates(row),
            "ab_status": _mapping(ab_rows.get(_text(row.get("variant_id")))).get(
                "ab_status", "MIXED"
            ),
            "candidate_status": _text(sensitivity.get("relaxed_candidate_status")),
            "followup_policy_version": _policy_version(policy),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for row in relaxed[:maximum]
    ]
    return {
        "schema_version": st.SCHEMA_VERSION,
        "base_promoted_candidates": sorted(base),
        "relaxed_only_candidates": relaxed_rows,
        "relaxed_only_count": len(relaxed_rows),
        "policy_effect": _text(sensitivity.get("policy_effect")),
        "authoritative_scenario": _text(sensitivity.get("authoritative_scenario")),
        "followup_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _sensitivity_material(
    *,
    root: Path,
    sensitivity_id: str,
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
    ab: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, bytes]]:
    _ensure_common_lineage(backfill, matrix, ab)
    _chronology(generated, backfill, matrix, ab)
    rows = _targeted_v3_scorecard_rows(backfill, matrix)
    _require(bool(rows), "targeted score rows missing")
    scenarios = _threshold_scenarios(rows, policy)
    impact = _threshold_candidate_impact(rows, ab, policy)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_threshold_sensitivity_manifest",
        "sensitivity_id": sensitivity_id,
        "v3_backfill_id": backfill.get("v3_backfill_id"),
        "v3_matrix_id": matrix.get("v3_matrix_id"),
        "ab_id": ab.get("ab_id"),
        "source_scorecard_id": matrix.get("source_scorecard_id"),
        "source_near_miss_id": ab.get("near_miss_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime"),
        "followup_policy_version": _policy_version(policy),
        "threshold_sensitivity_manifest_path": str(root / SENSITIVITY_VIEWS[0]),
        "threshold_scenarios_path": str(root / SENSITIVITY_VIEWS[1]),
        "threshold_candidate_impact_path": str(root / SENSITIVITY_VIEWS[2]),
        "threshold_sensitivity_report_path": str(root / SENSITIVITY_VIEWS[3]),
        "promotion_threshold_sensitivity_input_snapshot_path": str(
            root / "promotion_threshold_sensitivity_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    views = {
        SENSITIVITY_VIEWS[0]: foundation._json_bytes(manifest),
        SENSITIVITY_VIEWS[1]: foundation._jsonl_bytes(scenarios),
        SENSITIVITY_VIEWS[2]: foundation._json_bytes(impact),
        SENSITIVITY_VIEWS[3]: foundation._text_file_bytes(
            render_threshold_sensitivity_report(manifest, scenarios, impact)
        ),
    }
    return manifest, scenarios, impact, views


@with_artifact_validation_session
def run_promotion_threshold_sensitivity(
    *,
    v3_backfill_id: str,
    ab_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_FOLLOWUP_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    backfill = _validated_backfill(v3_backfill_id, v3_backfill_dir)
    matrix_id = _text(backfill.get("v3_matrix_id"))
    matrix = _validated_matrix(matrix_id, v3_matrix_dir)
    ab = _validated_ab(ab_id, ab_dir)
    policy = _policy(policy_path)
    policy_binding = foundation._file_binding(policy_path)
    backfill_binding = _binding(
        kind="targeted_v3_backfill",
        artifact_id=v3_backfill_id,
        root=Path(_text(backfill.get("v3_backfill_dir"))),
        names=targeted.BACKFILL_FILES,
    )
    matrix_binding = _binding(
        kind="targeted_search_v3",
        artifact_id=matrix_id,
        root=Path(_text(matrix.get("v3_matrix_dir"))),
        names=targeted.MATRIX_FILES,
    )
    ab_binding = _binding(
        kind="near_miss_ab_comparison",
        artifact_id=ab_id,
        root=Path(_text(ab.get("ab_dir"))),
        names=targeted.AB_FILES,
    )
    artifact_id = _stable_id(
        "promotion-threshold-sensitivity", v3_backfill_id, ab_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, scenarios, impact, _ = _sensitivity_material(
        root=root,
        sensitivity_id=root.name,
        backfill=backfill,
        matrix=matrix,
        ab=ab,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / SENSITIVITY_VIEWS[0], manifest)
    _write_jsonl(root / SENSITIVITY_VIEWS[1], scenarios)
    _write_json(root / SENSITIVITY_VIEWS[2], impact)
    _write_text(
        root / SENSITIVITY_VIEWS[3],
        render_threshold_sensitivity_report(manifest, scenarios, impact),
    )
    snapshot = {
        "schema_version": SENSITIVITY_INPUT_SCHEMA,
        "sensitivity_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_binding,
        "backfill_source": backfill_binding,
        "matrix_source": matrix_binding,
        "ab_source": ab_binding,
        "view_hashes": foundation._view_hashes(root, SENSITIVITY_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "promotion_threshold_sensitivity_input_snapshot.json", snapshot
    )
    _write_latest_pointer(
        "latest_promotion_threshold_sensitivity", root.name, root / SENSITIVITY_VIEWS[0]
    )
    return {
        "sensitivity_id": root.name,
        "sensitivity_dir": root,
        "manifest": manifest,
        "threshold_scenarios": scenarios,
        "threshold_candidate_impact": impact,
    }


def promotion_threshold_sensitivity_report_payload(
    *,
    sensitivity_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=sensitivity_id,
        latest_pointer="latest_promotion_threshold_sensitivity",
        latest=latest,
        output_dir=output_dir,
        required_name=SENSITIVITY_VIEWS[0],
    )
    return {
        **_read_json(root / SENSITIVITY_VIEWS[0]),
        "threshold_scenarios": _read_jsonl(root / SENSITIVITY_VIEWS[1]),
        "threshold_candidate_impact": _read_json(root / SENSITIVITY_VIEWS[2]),
        "input_snapshot": _read_json(
            root / "promotion_threshold_sensitivity_input_snapshot.json"
        ),
        "sensitivity_dir": str(root),
    }


def _rebuild_sensitivity(root: Path, sensitivity_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "promotion_threshold_sensitivity_input_snapshot.json")
    _require(
        snapshot.get("schema_version") == SENSITIVITY_INPUT_SCHEMA,
        "sensitivity snapshot schema",
    )
    _require(snapshot.get("sensitivity_id") == sensitivity_id, "sensitivity snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    ab_source = _mapping(snapshot.get("ab_source"))
    _validate_binding(backfill_source, kind="targeted_v3_backfill")
    _validate_binding(matrix_source, kind="targeted_search_v3")
    _validate_binding(ab_source, kind="near_miss_ab_comparison")
    backfill = _validated_backfill(_source_id(backfill_source), _source_dir(backfill_source).parent)
    matrix = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    ab = _validated_ab(_source_id(ab_source), _source_dir(ab_source).parent)
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    _, _, _, expected = _sensitivity_material(
        root=root,
        sensitivity_id=sensitivity_id,
        backfill=backfill,
        matrix=matrix,
        ab=ab,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_promotion_threshold_sensitivity_artifact(
    *, sensitivity_id: str, output_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR
) -> dict[str, Any]:
    root = output_dir / sensitivity_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="promotion_threshold_sensitivity_input_snapshot.json",
        schema=SENSITIVITY_INPUT_SCHEMA,
        id_key="sensitivity_id",
        artifact_id=sensitivity_id,
        view_names=SENSITIVITY_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_promotion_threshold_sensitivity_validation",
            sensitivity_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_promotion_threshold_sensitivity_validation",
        artifact_id=sensitivity_id,
        checks=checks,
        rebuild=lambda: _rebuild_sensitivity(root, sensitivity_id),
    )


def _promotion_candidate_lists(
    rows: Sequence[Mapping[str, Any]],
    ab: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    promotion = _mapping(policy.get("candidate_promotion"))
    ab_rows = {
        _text(row.get("variant_id")): row for row in _records(ab.get("ab_comparison_matrix"))
    }
    relaxed = {
        _text(row.get("variant_id"))
        for row in _records(
            _mapping(sensitivity.get("threshold_candidate_impact")).get(
                "relaxed_only_candidates"
            )
        )
    }
    promotable_decisions = set(_texts(promotion.get("promotable_scorecard_decisions")))
    promotable_ab = set(_texts(promotion.get("promotable_ab_statuses")))
    keep_decisions = set(_texts(promotion.get("keep_testing_scorecard_decisions")))
    promoted: list[dict[str, Any]] = []
    keep: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in rows:
        variant_id = _text(row.get("variant_id"))
        ab_status = _text(_mapping(ab_rows.get(variant_id)).get("ab_status"), "MIXED")
        payload = {
            "variant_id": variant_id,
            "overall_score": row.get("overall_score"),
            "scorecard_decision": row.get("scorecard_decision"),
            "failed_gates": _failed_gates(row),
            "ab_status": ab_status,
            "candidate_status": "",
            "owner_review_required": True,
            "implemented": False,
            "followup_policy_version": _policy_version(policy),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        if (
            _text(row.get("scorecard_decision")) in promotable_decisions
            and ab_status in promotable_ab
            and not _high_risk_gate_failure(row)
        ):
            payload["candidate_status"] = _text(promotion.get("promoted_candidate_status"))
            promoted.append(payload)
        elif _text(row.get("scorecard_decision")) in keep_decisions or variant_id in relaxed:
            payload["candidate_status"] = _text(promotion.get("keep_testing_candidate_status"))
            keep.append(payload)
        else:
            payload["candidate_status"] = _text(promotion.get("rejected_candidate_status"))
            rejected.append(payload)
    promoted_cap = _integer(promotion.get("maximum_promoted_candidates"), "promoted cap")
    keep_cap = _integer(promotion.get("maximum_keep_testing_candidates"), "keep cap")
    return promoted[:promoted_cap], keep[:keep_cap], rejected


def _promotion_decision(
    promoted: Sequence[Mapping[str, Any]],
    keep: Sequence[Mapping[str, Any]],
    rejected: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    rules = _mapping(_mapping(policy.get("candidate_promotion")).get("decisions"))
    if promoted:
        key = "promoted"
    elif keep:
        key = "keep_testing"
    elif rejected:
        key = "rejected_only"
    else:
        key = "empty"
    rule = _mapping(rules.get(key))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "promotion_v2_id": "",
        "decision": rule.get("decision"),
        "promoted_count": len(promoted),
        "keep_testing_count": len(keep),
        "rejected_count": len(rejected),
        "recommended_next_action": rule.get("recommended_next_action"),
        "owner_review_required": True,
        "implemented": False,
        "formal_method_task_created": False,
        "paper_shadow_primary_changed": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        "followup_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def _promotion_material(
    *,
    root: Path,
    promotion_v2_id: str,
    backfill: Mapping[str, Any],
    matrix: Mapping[str, Any],
    ab: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, bytes],
]:
    _ensure_common_lineage(backfill, matrix, ab)
    _require(
        _text(sensitivity.get("v3_backfill_id")) == _text(backfill.get("v3_backfill_id")),
        "sensitivity/backfill lineage mismatch",
    )
    _require(
        _text(sensitivity.get("v3_matrix_id")) == _text(matrix.get("v3_matrix_id")),
        "sensitivity/matrix lineage mismatch",
    )
    _require(
        _text(sensitivity.get("ab_id")) == _text(ab.get("ab_id")),
        "sensitivity/A-B lineage mismatch",
    )
    _require(
        _text(sensitivity.get("followup_policy_version")) == _policy_version(policy),
        "sensitivity policy lineage mismatch",
    )
    _chronology(generated, backfill, matrix, ab, sensitivity)
    rows = _targeted_v3_scorecard_rows(backfill, matrix)
    _require(bool(rows), "targeted score rows missing")
    promoted, keep, rejected = _promotion_candidate_lists(rows, ab, sensitivity, policy)
    decision = _promotion_decision(promoted, keep, rejected, policy)
    decision["promotion_v2_id"] = promotion_v2_id
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_promotion_v2_manifest",
        "promotion_v2_id": promotion_v2_id,
        "v3_backfill_id": backfill.get("v3_backfill_id"),
        "v3_matrix_id": matrix.get("v3_matrix_id"),
        "ab_id": ab.get("ab_id"),
        "sensitivity_id": sensitivity.get("sensitivity_id"),
        "source_scorecard_id": matrix.get("source_scorecard_id"),
        "source_near_miss_id": ab.get("near_miss_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime"),
        "followup_policy_version": _policy_version(policy),
        "candidate_promotion_v2_manifest_path": str(root / PROMOTION_VIEWS[0]),
        "promotion_v2_decision_path": str(root / PROMOTION_VIEWS[1]),
        "promoted_candidates_v2_path": str(root / PROMOTION_VIEWS[2]),
        "rejected_candidates_v2_path": str(root / PROMOTION_VIEWS[3]),
        "keep_testing_candidates_v2_path": str(root / PROMOTION_VIEWS[4]),
        "candidate_promotion_v2_report_path": str(root / PROMOTION_VIEWS[5]),
        "reader_brief_section_path": str(root / PROMOTION_VIEWS[6]),
        "candidate_promotion_v2_input_snapshot_path": str(
            root / "candidate_promotion_v2_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    report = render_candidate_promotion_v2_report(manifest, decision)
    reader = render_candidate_promotion_v2_reader_brief(decision)
    views = {
        PROMOTION_VIEWS[0]: foundation._json_bytes(manifest),
        PROMOTION_VIEWS[1]: foundation._json_bytes(decision),
        PROMOTION_VIEWS[2]: foundation._jsonl_bytes(promoted),
        PROMOTION_VIEWS[3]: foundation._jsonl_bytes(rejected),
        PROMOTION_VIEWS[4]: foundation._jsonl_bytes(keep),
        PROMOTION_VIEWS[5]: foundation._text_file_bytes(report),
        PROMOTION_VIEWS[6]: foundation._text_file_bytes(reader),
    }
    return manifest, decision, promoted, rejected, keep, views


@with_artifact_validation_session
def run_candidate_promotion_v2(
    *,
    v3_backfill_id: str,
    ab_id: str,
    sensitivity_id: str,
    v3_backfill_dir: Path = DEFAULT_TARGETED_V3_BACKFILL_DIR,
    v3_matrix_dir: Path = DEFAULT_TARGETED_SEARCH_V3_DIR,
    ab_dir: Path = DEFAULT_NEAR_MISS_AB_COMPARISON_DIR,
    sensitivity_dir: Path = DEFAULT_PROMOTION_THRESHOLD_SENSITIVITY_DIR,
    output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_FOLLOWUP_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    backfill = _validated_backfill(v3_backfill_id, v3_backfill_dir)
    matrix_id = _text(backfill.get("v3_matrix_id"))
    matrix = _validated_matrix(matrix_id, v3_matrix_dir)
    ab = _validated_ab(ab_id, ab_dir)
    sensitivity = _validated_sensitivity(sensitivity_id, sensitivity_dir)
    policy = _policy(policy_path)
    bindings = {
        "policy_source": foundation._file_binding(policy_path),
        "backfill_source": _binding(
            kind="targeted_v3_backfill",
            artifact_id=v3_backfill_id,
            root=Path(_text(backfill.get("v3_backfill_dir"))),
            names=targeted.BACKFILL_FILES,
        ),
        "matrix_source": _binding(
            kind="targeted_search_v3",
            artifact_id=matrix_id,
            root=Path(_text(matrix.get("v3_matrix_dir"))),
            names=targeted.MATRIX_FILES,
        ),
        "ab_source": _binding(
            kind="near_miss_ab_comparison",
            artifact_id=ab_id,
            root=Path(_text(ab.get("ab_dir"))),
            names=targeted.AB_FILES,
        ),
        "sensitivity_source": _binding(
            kind="promotion_threshold_sensitivity",
            artifact_id=sensitivity_id,
            root=Path(_text(sensitivity.get("sensitivity_dir"))),
            names=SENSITIVITY_FILES,
        ),
    }
    artifact_id = _stable_id(
        "candidate-promotion-v2",
        v3_backfill_id,
        ab_id,
        sensitivity_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / artifact_id)
    manifest, decision, promoted, rejected, keep, _ = _promotion_material(
        root=root,
        promotion_v2_id=root.name,
        backfill=backfill,
        matrix=matrix,
        ab=ab,
        sensitivity=sensitivity,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / PROMOTION_VIEWS[0], manifest)
    _write_json(root / PROMOTION_VIEWS[1], decision)
    _write_jsonl(root / PROMOTION_VIEWS[2], promoted)
    _write_jsonl(root / PROMOTION_VIEWS[3], rejected)
    _write_jsonl(root / PROMOTION_VIEWS[4], keep)
    _write_text(root / PROMOTION_VIEWS[5], render_candidate_promotion_v2_report(manifest, decision))
    reader = render_candidate_promotion_v2_reader_brief(decision)
    _write_text(root / PROMOTION_VIEWS[6], reader)
    snapshot = {
        "schema_version": PROMOTION_INPUT_SCHEMA,
        "promotion_v2_id": root.name,
        "generated_at": generated.isoformat(),
        **bindings,
        "view_hashes": foundation._view_hashes(root, PROMOTION_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "candidate_promotion_v2_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_candidate_promotion_v2", root.name, root / PROMOTION_VIEWS[0])
    return {
        "promotion_v2_id": root.name,
        "promotion_v2_dir": root,
        "manifest": manifest,
        "promotion_v2_decision": decision,
        "promoted_candidates_v2": promoted,
        "rejected_candidates_v2": rejected,
        "keep_testing_candidates_v2": keep,
        "reader_brief_section": reader,
    }


def candidate_promotion_v2_report_payload(
    *,
    promotion_v2_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_v2_id,
        latest_pointer="latest_candidate_promotion_v2",
        latest=latest,
        output_dir=output_dir,
        required_name=PROMOTION_VIEWS[0],
    )
    return {
        **_read_json(root / PROMOTION_VIEWS[0]),
        "promotion_v2_decision": _read_json(root / PROMOTION_VIEWS[1]),
        "promoted_candidates_v2": _read_jsonl(root / PROMOTION_VIEWS[2]),
        "rejected_candidates_v2": _read_jsonl(root / PROMOTION_VIEWS[3]),
        "keep_testing_candidates_v2": _read_jsonl(root / PROMOTION_VIEWS[4]),
        "reader_brief_section": (root / PROMOTION_VIEWS[6]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "candidate_promotion_v2_input_snapshot.json"),
        "promotion_v2_dir": str(root),
    }


def _rebuild_promotion(root: Path, promotion_v2_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "candidate_promotion_v2_input_snapshot.json")
    _require(snapshot.get("schema_version") == PROMOTION_INPUT_SCHEMA, "promotion snapshot schema")
    _require(snapshot.get("promotion_v2_id") == promotion_v2_id, "promotion snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    policy = _policy(Path(_text(policy_source.get("path"))))
    sources = {
        "backfill_source": "targeted_v3_backfill",
        "matrix_source": "targeted_search_v3",
        "ab_source": "near_miss_ab_comparison",
        "sensitivity_source": "promotion_threshold_sensitivity",
    }
    for name, kind in sources.items():
        _validate_binding(_mapping(snapshot.get(name)), kind=kind)
    backfill_source = _mapping(snapshot.get("backfill_source"))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    ab_source = _mapping(snapshot.get("ab_source"))
    sensitivity_source = _mapping(snapshot.get("sensitivity_source"))
    backfill = _validated_backfill(_source_id(backfill_source), _source_dir(backfill_source).parent)
    matrix = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    ab = _validated_ab(_source_id(ab_source), _source_dir(ab_source).parent)
    sensitivity = _validated_sensitivity(
        _source_id(sensitivity_source), _source_dir(sensitivity_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    *_, expected = _promotion_material(
        root=root,
        promotion_v2_id=promotion_v2_id,
        backfill=backfill,
        matrix=matrix,
        ab=ab,
        sensitivity=sensitivity,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_candidate_promotion_v2_artifact(
    *, promotion_v2_id: str, output_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR
) -> dict[str, Any]:
    root = output_dir / promotion_v2_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="candidate_promotion_v2_input_snapshot.json",
        schema=PROMOTION_INPUT_SCHEMA,
        id_key="promotion_v2_id",
        artifact_id=promotion_v2_id,
        view_names=PROMOTION_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_candidate_promotion_v2_validation", promotion_v2_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_candidate_promotion_v2_validation",
        artifact_id=promotion_v2_id,
        checks=checks,
        rebuild=lambda: _rebuild_promotion(root, promotion_v2_id),
    )


def _next_plan_parts(
    promotion: Mapping[str, Any], policy: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    promotion_decision = _text(
        _mapping(promotion.get("promotion_v2_decision")).get("decision")
    )
    rule = _mapping(
        _mapping(_mapping(policy.get("next_plan")).get("decisions")).get(promotion_decision)
    )
    _require(bool(rule), "promotion decision has no next-plan rule")
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "plan_id": "",
        "decision": rule.get("decision"),
        "source_promotion_v2_decision": promotion_decision,
        "recommended_next_action": rule.get("recommended_next_action"),
        "should_continue_parameter_search": rule.get("should_continue_parameter_search"),
        "owner_review_required": True,
        "implemented": False,
        "formal_method_task_created": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        "followup_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    formal_policy = _mapping(_mapping(policy.get("next_plan")).get("formal_candidate"))
    formal_rows = [
        {
            "variant_id": row.get("variant_id"),
            "implementation_scope": formal_policy.get("implementation_scope"),
            "transform_composable": formal_policy.get("transform_composable"),
            "requires_external_data": formal_policy.get("requires_external_data"),
            "implementation_complexity": formal_policy.get("implementation_complexity"),
            "implementation_allowed_without_owner_approval": formal_policy.get(
                "implementation_allowed_without_owner_approval"
            ),
            "owner_review_required": True,
            "implemented": False,
            "formal_method_task_created": False,
            "not_official_target_weights": True,
            "broker_action_allowed": False,
            "production_effect": st.PRODUCTION_EFFECT,
            "followup_policy_version": _policy_version(policy),
            **st.EXPERIMENT_FACTORY_SAFETY,
        }
        for row in _records(promotion.get("promoted_candidates_v2"))
    ]
    formal = {
        "schema_version": st.SCHEMA_VERSION,
        "candidates": formal_rows,
        "owner_review_required": True,
        "implemented": False,
        "followup_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    continue_plan = {
        "schema_version": st.SCHEMA_VERSION,
        "recommended_actions": _texts(rule.get("recommended_actions")),
        "priority": rule.get("priority"),
        "should_continue_parameter_search": rule.get("should_continue_parameter_search"),
        "owner_review_required": True,
        "implemented": False,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        "followup_policy_version": _policy_version(policy),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return decision, formal, continue_plan


def _next_plan_material(
    *,
    root: Path,
    plan_id: str,
    promotion: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, bytes]]:
    _require(
        _text(promotion.get("followup_policy_version")) == _policy_version(policy),
        "promotion policy lineage mismatch",
    )
    _chronology(generated, promotion)
    decision, formal, continue_plan = _next_plan_parts(promotion, policy)
    decision["plan_id"] = plan_id
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_next_formal_or_search_manifest",
        "plan_id": plan_id,
        "promotion_v2_id": promotion.get("promotion_v2_id"),
        "sensitivity_id": promotion.get("sensitivity_id"),
        "v3_backfill_id": promotion.get("v3_backfill_id"),
        "v3_matrix_id": promotion.get("v3_matrix_id"),
        "ab_id": promotion.get("ab_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": promotion.get("market_regime"),
        "followup_policy_version": _policy_version(policy),
        "next_formal_or_search_manifest_path": str(root / NEXT_PLAN_VIEWS[0]),
        "next_plan_decision_path": str(root / NEXT_PLAN_VIEWS[1]),
        "formal_method_candidates_path": str(root / NEXT_PLAN_VIEWS[2]),
        "continue_search_plan_path": str(root / NEXT_PLAN_VIEWS[3]),
        "owner_next_action_checklist_path": str(root / NEXT_PLAN_VIEWS[4]),
        "next_formal_or_search_plan_report_path": str(root / NEXT_PLAN_VIEWS[5]),
        "reader_brief_section_path": str(root / NEXT_PLAN_VIEWS[6]),
        "next_formal_or_search_plan_input_snapshot_path": str(
            root / "next_formal_or_search_plan_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    checklist = render_owner_next_action_checklist(decision, formal, continue_plan)
    report = render_next_formal_or_search_plan_report(manifest, decision, formal, continue_plan)
    reader = render_next_plan_reader_brief(decision)
    views = {
        NEXT_PLAN_VIEWS[0]: foundation._json_bytes(manifest),
        NEXT_PLAN_VIEWS[1]: foundation._json_bytes(decision),
        NEXT_PLAN_VIEWS[2]: foundation._json_bytes(formal),
        NEXT_PLAN_VIEWS[3]: foundation._json_bytes(continue_plan),
        NEXT_PLAN_VIEWS[4]: foundation._text_file_bytes(checklist),
        NEXT_PLAN_VIEWS[5]: foundation._text_file_bytes(report),
        NEXT_PLAN_VIEWS[6]: foundation._text_file_bytes(reader),
    }
    return manifest, decision, formal, continue_plan, views


@with_artifact_validation_session
def run_next_formal_or_search_plan(
    *,
    promotion_v2_id: str,
    promotion_v2_dir: Path = DEFAULT_CANDIDATE_PROMOTION_V2_DIR,
    output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
    generated_at: datetime | None = None,
    policy_path: Path = DEFAULT_WEIGHT_SEARCH_FOLLOWUP_POLICY_PATH,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    promotion = _validated_promotion(promotion_v2_id, promotion_v2_dir)
    policy = _policy(policy_path)
    policy_binding = foundation._file_binding(policy_path)
    promotion_binding = _binding(
        kind="candidate_promotion_v2",
        artifact_id=promotion_v2_id,
        root=Path(_text(promotion.get("promotion_v2_dir"))),
        names=PROMOTION_FILES,
    )
    artifact_id = _stable_id("next-formal-or-search-plan", promotion_v2_id, generated.isoformat())
    root = _unique_dir(output_dir / artifact_id)
    manifest, decision, formal, continue_plan, _ = _next_plan_material(
        root=root,
        plan_id=root.name,
        promotion=promotion,
        policy=policy,
        generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_json(root / NEXT_PLAN_VIEWS[0], manifest)
    _write_json(root / NEXT_PLAN_VIEWS[1], decision)
    _write_json(root / NEXT_PLAN_VIEWS[2], formal)
    _write_json(root / NEXT_PLAN_VIEWS[3], continue_plan)
    checklist = render_owner_next_action_checklist(decision, formal, continue_plan)
    _write_text(root / NEXT_PLAN_VIEWS[4], checklist)
    _write_text(
        root / NEXT_PLAN_VIEWS[5],
        render_next_formal_or_search_plan_report(manifest, decision, formal, continue_plan),
    )
    reader = render_next_plan_reader_brief(decision)
    _write_text(root / NEXT_PLAN_VIEWS[6], reader)
    snapshot = {
        "schema_version": NEXT_PLAN_INPUT_SCHEMA,
        "plan_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_binding,
        "promotion_source": promotion_binding,
        "view_hashes": foundation._view_hashes(root, NEXT_PLAN_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "next_formal_or_search_plan_input_snapshot.json", snapshot)
    _write_latest_pointer("latest_next_formal_or_search_plan", root.name, root / NEXT_PLAN_VIEWS[0])
    return {
        "plan_id": root.name,
        "plan_dir": root,
        "manifest": manifest,
        "next_plan_decision": decision,
        "formal_method_candidates": formal,
        "continue_search_plan": continue_plan,
        "owner_next_action_checklist": checklist,
        "reader_brief_section": reader,
    }


def next_formal_or_search_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=plan_id,
        latest_pointer="latest_next_formal_or_search_plan",
        latest=latest,
        output_dir=output_dir,
        required_name=NEXT_PLAN_VIEWS[0],
    )
    return {
        **_read_json(root / NEXT_PLAN_VIEWS[0]),
        "next_plan_decision": _read_json(root / NEXT_PLAN_VIEWS[1]),
        "formal_method_candidates": _read_json(root / NEXT_PLAN_VIEWS[2]),
        "continue_search_plan": _read_json(root / NEXT_PLAN_VIEWS[3]),
        "owner_next_action_checklist": (root / NEXT_PLAN_VIEWS[4]).read_text(encoding="utf-8"),
        "reader_brief_section": (root / NEXT_PLAN_VIEWS[6]).read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "next_formal_or_search_plan_input_snapshot.json"),
        "plan_dir": str(root),
    }


def _rebuild_next_plan(root: Path, plan_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "next_formal_or_search_plan_input_snapshot.json")
    _require(snapshot.get("schema_version") == NEXT_PLAN_INPUT_SCHEMA, "next-plan snapshot schema")
    _require(snapshot.get("plan_id") == plan_id, "next-plan snapshot id")
    policy_source = _mapping(snapshot.get("policy_source"))
    promotion_source = _mapping(snapshot.get("promotion_source"))
    foundation._validate_file_binding(policy_source)
    _validate_binding(promotion_source, kind="candidate_promotion_v2")
    policy = _policy(Path(_text(policy_source.get("path"))))
    promotion = _validated_promotion(
        _source_id(promotion_source), _source_dir(promotion_source).parent
    )
    generated = _aware_datetime(_text(snapshot.get("generated_at")), "snapshot.generated_at")
    *_, expected = _next_plan_material(
        root=root,
        plan_id=plan_id,
        promotion=promotion,
        policy=policy,
        generated=generated,
    )
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    return checks


@with_artifact_validation_session
def validate_next_formal_or_search_plan_artifact(
    *, plan_id: str, output_dir: Path = DEFAULT_NEXT_FORMAL_OR_SEARCH_PLAN_DIR
) -> dict[str, Any]:
    root = output_dir / plan_id
    checks, ok = _snapshot_preflight(
        root=root,
        snapshot_name="next_formal_or_search_plan_input_snapshot.json",
        schema=NEXT_PLAN_INPUT_SCHEMA,
        id_key="plan_id",
        artifact_id=plan_id,
        view_names=NEXT_PLAN_VIEWS,
    )
    if not ok:
        return _validation_payload(
            "etf_dynamic_v3_next_formal_or_search_plan_validation", plan_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_next_formal_or_search_plan_validation",
        artifact_id=plan_id,
        checks=checks,
        rebuild=lambda: _rebuild_next_plan(root, plan_id),
    )
