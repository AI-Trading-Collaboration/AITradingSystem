from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_evaluation as evaluation
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR = _legacy.DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR
DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR = (
    _legacy.DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR
)
DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR = _legacy.DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR
DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR = _legacy.DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR
DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR = _legacy.DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR
DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR = _legacy.DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR
DEFAULT_WEIGHT_SCORECARD_DIR = evaluation.DEFAULT_WEIGHT_SCORECARD_DIR
DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR = evaluation.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR
DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR = evaluation.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR

CLUSTER_INPUT_SCHEMA = "weight_candidate_cluster_input_snapshot.v2"
INTERPRETATION_INPUT_SCHEMA = "weight_top_candidate_interpretation_input_snapshot.v2"
GATE_INPUT_SCHEMA = "weight_method_promotion_gate_input_snapshot.v2"
PLAN_INPUT_SCHEMA = "formal_method_auto_plan_input_snapshot.v2"
DASHBOARD_INPUT_SCHEMA = "weight_search_dashboard_input_snapshot.v2"
OWNER_INPUT_SCHEMA = "owner_research_decision_pack_input_snapshot.v2"

CLUSTER_VIEWS = (
    "candidate_cluster_manifest.json",
    "candidate_clusters.json",
    "cluster_representatives.json",
    "candidate_cluster_report.md",
)
INTERPRETATION_VIEWS = (
    "top_candidate_interpretation_manifest.json",
    "top_candidate_explanations.jsonl",
    "failure_mode_coverage.json",
    "top_candidate_interpretation_report.md",
    "reader_brief_section.md",
)
GATE_VIEWS = (
    "promotion_gate_manifest.json",
    "promotion_gate_decision.json",
    "promoted_candidate_specs.json",
    "promotion_gate_report.md",
)
PLAN_VIEWS = (
    "formal_method_auto_plan_manifest.json",
    "formal_method_specs.json",
    "implementation_plan.md",
    "validation_plan.json",
    "formal_method_auto_plan_report.md",
)
DASHBOARD_VIEWS = (
    "search_dashboard_manifest.json",
    "search_summary.json",
    "top_candidates.json",
    "rejected_summary.json",
    "next_actions.json",
    "reader_brief_section.md",
)
OWNER_VIEWS = (
    "owner_decision_pack_manifest.json",
    "owner_decision_options.json",
    "owner_decision_pack_report.md",
)

CLUSTER_FILES = (*CLUSTER_VIEWS, "weight_candidate_cluster_input_snapshot.json")
INTERPRETATION_FILES = (
    *INTERPRETATION_VIEWS,
    "weight_top_candidate_interpretation_input_snapshot.json",
)
GATE_FILES = (*GATE_VIEWS, "weight_method_promotion_gate_input_snapshot.json")
PLAN_FILES = (*PLAN_VIEWS, "formal_method_auto_plan_input_snapshot.json")
DASHBOARD_FILES = (*DASHBOARD_VIEWS, "weight_search_dashboard_input_snapshot.json")
OWNER_FILES = (*OWNER_VIEWS, "owner_research_decision_pack_input_snapshot.json")

_mapping = _legacy._mapping
_records = _legacy._records
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
_candidate_clusters = _legacy._candidate_clusters
_candidate_explanation = _legacy._candidate_explanation
_failure_mode_coverage_from_explanations = _legacy._failure_mode_coverage_from_explanations
_promotion_gate_decisions = _legacy._promotion_gate_decisions
_promotion_decision_summary = _legacy._promotion_decision_summary
_promoted_candidate_spec = _legacy._promoted_candidate_spec
_formal_method_specs = _legacy._formal_method_specs
_formal_validation_plan = _legacy._formal_validation_plan
_dashboard_summary = _legacy._dashboard_summary
_owner_decision_options = _legacy._owner_decision_options
render_candidate_cluster_report = _legacy.render_candidate_cluster_report
render_top_candidate_interpretation_report = _legacy.render_top_candidate_interpretation_report
render_top_candidate_reader_brief = _legacy.render_top_candidate_reader_brief
render_promotion_gate_report = _legacy.render_promotion_gate_report
render_formal_method_implementation_plan = _legacy.render_formal_method_implementation_plan
render_formal_method_auto_plan_report = _legacy.render_formal_method_auto_plan_report
render_dashboard_reader_brief = _legacy.render_dashboard_reader_brief
render_owner_decision_pack_report = _legacy.render_owner_decision_pack_report


class DynamicV3WeightSearchDecisionError(ValueError):
    """Raised when the decision chain cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchDecisionError(message)


def _source_dir(binding: Mapping[str, Any]) -> Path:
    return Path(str(binding.get("source_dir", "")))


def _source_id(binding: Mapping[str, Any]) -> str:
    return str(binding.get("artifact_id", ""))


def _binding(*, kind: str, artifact_id: str, root: Path, names: Sequence[str]) -> dict[str, Any]:
    return foundation._artifact_binding(
        kind=kind,
        artifact_id=artifact_id,
        root=root,
        names=names,
    )


def _validate_binding(binding: Mapping[str, Any], *, kind: str) -> None:
    foundation._validate_artifact_binding(binding, kind=kind)


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
    base_checks: list[dict[str, Any]],
    rebuild: Callable[[], list[dict[str, Any]]],
) -> dict[str, Any]:
    try:
        base_checks.extend(rebuild())
    except Exception as exc:
        base_checks.append(st._check("content_rebuild", False, str(exc)))
    return _validation_payload(report_type, artifact_id, base_checks)


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
        view_hashes = _mapping(snapshot.get("view_hashes"))
        view_keys_ok = set(view_hashes) == set(view_names)
        view_errors = foundation._validate_view_hashes(root, view_hashes)
        checks.extend(
            [
                st._check("snapshot_schema", schema_ok, _text(snapshot.get("schema_version"))),
                st._check("snapshot_artifact_id", id_ok, _text(snapshot.get(id_key))),
                st._check("snapshot_view_hash_keys", view_keys_ok, ""),
                st._check("snapshot_view_hashes", not view_errors, "; ".join(view_errors)),
            ]
        )
        return checks, schema_ok and id_ok and view_keys_ok and not view_errors
    except Exception as exc:
        checks.append(st._check("snapshot_preflight", False, str(exc)))
        return checks, False


def _view_hash_check(root: Path, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    errors = foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes")))
    return st._check("view_hashes", not errors, "; ".join(errors))


def _validated_scorecard(scorecard_id: str, scorecard_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=evaluation.validate_weight_scorecard_artifact,
        validator_key="scorecard_id",
        artifact_id=scorecard_id,
        root=scorecard_dir,
    )
    _require(validation.get("status") == "PASS", "source scorecard validation failed")
    return evaluation.weight_scorecard_report_payload(
        scorecard_id=scorecard_id,
        output_dir=scorecard_dir,
    )


def _validated_robustness(robustness_id: str, robustness_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=evaluation.validate_weight_robustness_review_artifact,
        validator_key="robustness_id",
        artifact_id=robustness_id,
        root=robustness_dir,
    )
    _require(validation.get("status") == "PASS", "source robustness validation failed")
    return evaluation.weight_robustness_review_report_payload(
        robustness_id=robustness_id,
        output_dir=robustness_dir,
    )


def _validated_adaptive(branch_id: str, branch_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=evaluation.validate_weight_adaptive_branch_artifact,
        validator_key="branch_id",
        artifact_id=branch_id,
        root=branch_dir,
    )
    _require(validation.get("status") == "PASS", "source adaptive branch validation failed")
    return evaluation.weight_adaptive_branch_report_payload(
        branch_id=branch_id,
        output_dir=branch_dir,
    )


@with_artifact_validation_session
def run_weight_candidate_cluster(
    *,
    scorecard_id: str,
    robustness_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    robustness_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    robustness = _validated_robustness(robustness_id, robustness_dir)
    _require(robustness.get("scorecard_id") == scorecard_id, "cluster scorecard lineage mismatch")
    _require(
        robustness.get("batch_backfill_id") == scorecard.get("batch_backfill_id"),
        "cluster backfill lineage mismatch",
    )
    clusters, representatives = _candidate_clusters(scorecard, robustness)
    cluster_id = _stable_id(
        "weight-candidate-cluster",
        scorecard_id,
        robustness_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / cluster_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _cluster_manifest(
        root=root,
        cluster_id=root.name,
        scorecard_id=scorecard_id,
        robustness_id=robustness_id,
        generated_at=generated.isoformat(),
        clusters=clusters,
        representatives=representatives,
    )
    _write_json(root / "candidate_cluster_manifest.json", manifest)
    _write_json(root / "candidate_clusters.json", clusters)
    _write_json(root / "cluster_representatives.json", representatives)
    _write_text(
        root / "candidate_cluster_report.md",
        render_candidate_cluster_report(manifest, representatives),
    )
    snapshot = {
        "schema_version": CLUSTER_INPUT_SCHEMA,
        "cluster_id": root.name,
        "generated_at": generated.isoformat(),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(str(scorecard.get("scorecard_dir", ""))),
            names=evaluation.SCORECARD_FILES,
        ),
        "robustness_source": _binding(
            kind="weight_robustness_review",
            artifact_id=robustness_id,
            root=Path(str(robustness.get("robustness_dir", ""))),
            names=evaluation.ROBUSTNESS_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, CLUSTER_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_candidate_cluster_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_candidate_cluster",
        root.name,
        root / "candidate_cluster_manifest.json",
    )
    return {
        "cluster_id": root.name,
        "cluster_dir": root,
        "manifest": manifest,
        "candidate_clusters": clusters,
        "cluster_representatives": representatives,
    }


def _cluster_manifest(
    *,
    root: Path,
    cluster_id: str,
    scorecard_id: str,
    robustness_id: str,
    generated_at: str,
    clusters: Mapping[str, Any],
    representatives: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_candidate_cluster_manifest",
        "cluster_id": cluster_id,
        "scorecard_id": scorecard_id,
        "robustness_id": robustness_id,
        "generated_at": generated_at,
        "status": "PASS" if _records(representatives.get("representatives")) else "FAIL",
        "cluster_count": len(_records(clusters.get("clusters"))),
        "candidate_cluster_manifest_path": str(root / "candidate_cluster_manifest.json"),
        "candidate_clusters_path": str(root / "candidate_clusters.json"),
        "cluster_representatives_path": str(root / "cluster_representatives.json"),
        "candidate_cluster_report_path": str(root / "candidate_cluster_report.md"),
        "weight_candidate_cluster_input_snapshot_path": str(
            root / "weight_candidate_cluster_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def weight_candidate_cluster_report_payload(
    *,
    cluster_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=cluster_id,
        latest_pointer="latest_weight_candidate_cluster",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_cluster_manifest.json",
    )
    return {
        **_read_json(root / "candidate_cluster_manifest.json"),
        "candidate_clusters": _read_json(root / "candidate_clusters.json"),
        "cluster_representatives": _read_json(root / "cluster_representatives.json"),
        "input_snapshot": _read_json(root / "weight_candidate_cluster_input_snapshot.json"),
        "cluster_dir": str(root),
    }


def _rebuild_cluster(root: Path, cluster_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_candidate_cluster_input_snapshot.json")
    _require(snapshot.get("schema_version") == CLUSTER_INPUT_SCHEMA, "cluster snapshot schema")
    _require(snapshot.get("cluster_id") == cluster_id, "cluster snapshot id")
    score_source = _mapping(snapshot.get("scorecard_source"))
    robust_source = _mapping(snapshot.get("robustness_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(robust_source, kind="weight_robustness_review")
    score_id = _source_id(score_source)
    robust_id = _source_id(robust_source)
    scorecard = _validated_scorecard(score_id, _source_dir(score_source).parent)
    robustness = _validated_robustness(robust_id, _source_dir(robust_source).parent)
    _require(robustness.get("scorecard_id") == score_id, "cluster scorecard lineage mismatch")
    _require(
        robustness.get("batch_backfill_id") == scorecard.get("batch_backfill_id"),
        "cluster backfill lineage mismatch",
    )
    clusters, representatives = _candidate_clusters(scorecard, robustness)
    manifest = _cluster_manifest(
        root=root,
        cluster_id=cluster_id,
        scorecard_id=score_id,
        robustness_id=robust_id,
        generated_at=_text(snapshot.get("generated_at")),
        clusters=clusters,
        representatives=representatives,
    )
    expected = {
        "candidate_cluster_manifest.json": foundation._json_bytes(manifest),
        "candidate_clusters.json": foundation._json_bytes(clusters),
        "cluster_representatives.json": foundation._json_bytes(representatives),
        "candidate_cluster_report.md": foundation._text_file_bytes(
            render_candidate_cluster_report(manifest, representatives)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "cluster_safety",
            _payload_experiment_safe(manifest, clusters, representatives),
            "",
        )
    )
    return checks


@with_artifact_validation_session
def validate_weight_candidate_cluster_artifact(
    *,
    cluster_id: str,
    output_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
) -> dict[str, Any]:
    root = output_dir / cluster_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_candidate_cluster_input_snapshot.json",
        schema=CLUSTER_INPUT_SCHEMA,
        id_key="cluster_id",
        artifact_id=cluster_id,
        view_names=CLUSTER_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_candidate_cluster_validation",
            cluster_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_candidate_cluster_validation",
        artifact_id=cluster_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_cluster(root, cluster_id),
    )


@with_artifact_validation_session
def run_weight_top_candidate_interpretation(
    *,
    cluster_id: str,
    cluster_dir: Path = DEFAULT_WEIGHT_CANDIDATE_CLUSTER_DIR,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    cluster_validation = cached_artifact_validation(
        validator=validate_weight_candidate_cluster_artifact,
        validator_key="cluster_id",
        artifact_id=cluster_id,
        root=cluster_dir,
    )
    _require(cluster_validation.get("status") == "PASS", "source cluster validation failed")
    cluster = weight_candidate_cluster_report_payload(cluster_id=cluster_id, output_dir=cluster_dir)
    explanations, coverage = _interpretation_outputs(cluster)
    interpretation_id = _stable_id(
        "weight-top-candidate-interpretation",
        cluster_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / interpretation_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _interpretation_manifest(
        root=root,
        interpretation_id=root.name,
        cluster=cluster,
        generated_at=generated.isoformat(),
        explanations=explanations,
    )
    reader = render_top_candidate_reader_brief(manifest, explanations)
    _write_json(root / "top_candidate_interpretation_manifest.json", manifest)
    _write_jsonl(root / "top_candidate_explanations.jsonl", explanations)
    _write_json(root / "failure_mode_coverage.json", coverage)
    _write_text(
        root / "top_candidate_interpretation_report.md",
        render_top_candidate_interpretation_report(manifest, explanations),
    )
    _write_text(root / "reader_brief_section.md", reader)
    snapshot = {
        "schema_version": INTERPRETATION_INPUT_SCHEMA,
        "interpretation_id": root.name,
        "generated_at": generated.isoformat(),
        "cluster_source": _binding(
            kind="weight_candidate_cluster",
            artifact_id=cluster_id,
            root=Path(str(cluster.get("cluster_dir", ""))),
            names=CLUSTER_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, INTERPRETATION_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "weight_top_candidate_interpretation_input_snapshot.json",
        snapshot,
    )
    _write_latest_pointer(
        "latest_weight_top_candidate_interpretation",
        root.name,
        root / "top_candidate_interpretation_manifest.json",
    )
    return {
        "interpretation_id": root.name,
        "interpretation_dir": root,
        "manifest": manifest,
        "top_candidate_explanations": explanations,
        "failure_mode_coverage": coverage,
        "reader_brief_section": reader,
    }


def _interpretation_outputs(
    cluster: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    reps = _records(_mapping(cluster.get("cluster_representatives")).get("representatives"))
    explanations = [_candidate_explanation(row) for row in reps[:5]]
    return explanations, _failure_mode_coverage_from_explanations(explanations)


def _interpretation_manifest(
    *,
    root: Path,
    interpretation_id: str,
    cluster: Mapping[str, Any],
    generated_at: str,
    explanations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_top_candidate_interpretation_manifest",
        "interpretation_id": interpretation_id,
        "cluster_id": cluster.get("cluster_id"),
        "scorecard_id": cluster.get("scorecard_id"),
        "robustness_id": cluster.get("robustness_id"),
        "generated_at": generated_at,
        "status": "PASS" if explanations else "FAIL",
        "recommended_variant": _text(explanations[0].get("variant_id")) if explanations else "",
        "top_candidate_interpretation_manifest_path": str(
            root / "top_candidate_interpretation_manifest.json"
        ),
        "top_candidate_explanations_path": str(root / "top_candidate_explanations.jsonl"),
        "failure_mode_coverage_path": str(root / "failure_mode_coverage.json"),
        "top_candidate_interpretation_report_path": str(
            root / "top_candidate_interpretation_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "weight_top_candidate_interpretation_input_snapshot_path": str(
            root / "weight_top_candidate_interpretation_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def weight_top_candidate_interpretation_report_payload(
    *,
    interpretation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=interpretation_id,
        latest_pointer="latest_weight_top_candidate_interpretation",
        latest=latest,
        output_dir=output_dir,
        required_name="top_candidate_interpretation_manifest.json",
    )
    return {
        **_read_json(root / "top_candidate_interpretation_manifest.json"),
        "top_candidate_explanations": _read_jsonl(root / "top_candidate_explanations.jsonl"),
        "failure_mode_coverage": _read_json(root / "failure_mode_coverage.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(
            root / "weight_top_candidate_interpretation_input_snapshot.json"
        ),
        "interpretation_dir": str(root),
    }


def _rebuild_interpretation(root: Path, interpretation_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_top_candidate_interpretation_input_snapshot.json")
    _require(
        snapshot.get("schema_version") == INTERPRETATION_INPUT_SCHEMA,
        "interpretation snapshot schema",
    )
    _require(snapshot.get("interpretation_id") == interpretation_id, "interpretation snapshot id")
    cluster_source = _mapping(snapshot.get("cluster_source"))
    _validate_binding(cluster_source, kind="weight_candidate_cluster")
    cluster_id = _source_id(cluster_source)
    validation = cached_artifact_validation(
        validator=validate_weight_candidate_cluster_artifact,
        validator_key="cluster_id",
        artifact_id=cluster_id,
        root=_source_dir(cluster_source).parent,
    )
    _require(validation.get("status") == "PASS", "source cluster validation failed")
    cluster = weight_candidate_cluster_report_payload(
        cluster_id=cluster_id,
        output_dir=_source_dir(cluster_source).parent,
    )
    explanations, coverage = _interpretation_outputs(cluster)
    manifest = _interpretation_manifest(
        root=root,
        interpretation_id=interpretation_id,
        cluster=cluster,
        generated_at=_text(snapshot.get("generated_at")),
        explanations=explanations,
    )
    expected = {
        "top_candidate_interpretation_manifest.json": foundation._json_bytes(manifest),
        "top_candidate_explanations.jsonl": foundation._jsonl_bytes(explanations),
        "failure_mode_coverage.json": foundation._json_bytes(coverage),
        "top_candidate_interpretation_report.md": foundation._text_file_bytes(
            render_top_candidate_interpretation_report(manifest, explanations)
        ),
        "reader_brief_section.md": foundation._text_file_bytes(
            render_top_candidate_reader_brief(manifest, explanations)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "interpretation_safety",
            _payload_experiment_safe(manifest, coverage, *explanations),
            "",
        )
    )
    return checks


@with_artifact_validation_session
def validate_weight_top_candidate_interpretation_artifact(
    *,
    interpretation_id: str,
    output_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
) -> dict[str, Any]:
    root = output_dir / interpretation_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_top_candidate_interpretation_input_snapshot.json",
        schema=INTERPRETATION_INPUT_SCHEMA,
        id_key="interpretation_id",
        artifact_id=interpretation_id,
        view_names=INTERPRETATION_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_top_candidate_interpretation_validation",
            interpretation_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_top_candidate_interpretation_validation",
        artifact_id=interpretation_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_interpretation(root, interpretation_id),
    )


@with_artifact_validation_session
def run_weight_method_promotion_gate(
    *,
    interpretation_id: str,
    interpretation_dir: Path = DEFAULT_WEIGHT_TOP_CANDIDATE_INTERPRETATION_DIR,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    validation = cached_artifact_validation(
        validator=validate_weight_top_candidate_interpretation_artifact,
        validator_key="interpretation_id",
        artifact_id=interpretation_id,
        root=interpretation_dir,
    )
    _require(validation.get("status") == "PASS", "source interpretation validation failed")
    interpretation = weight_top_candidate_interpretation_report_payload(
        interpretation_id=interpretation_id,
        output_dir=interpretation_dir,
    )
    decision_payload, specs = _gate_outputs(interpretation)
    gate_id = _stable_id(
        "weight-method-promotion-gate",
        interpretation_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / gate_id)
    root.mkdir(parents=True, exist_ok=False)
    decision_payload["promotion_gate_id"] = root.name
    manifest = _gate_manifest(
        root=root,
        promotion_gate_id=root.name,
        interpretation=interpretation,
        generated_at=generated.isoformat(),
        decision_payload=decision_payload,
        specs=specs,
    )
    _write_json(root / "promotion_gate_manifest.json", manifest)
    _write_json(root / "promotion_gate_decision.json", decision_payload)
    _write_json(root / "promoted_candidate_specs.json", specs)
    _write_text(
        root / "promotion_gate_report.md",
        render_promotion_gate_report(manifest, decision_payload),
    )
    snapshot = {
        "schema_version": GATE_INPUT_SCHEMA,
        "promotion_gate_id": root.name,
        "generated_at": generated.isoformat(),
        "interpretation_source": _binding(
            kind="weight_top_candidate_interpretation",
            artifact_id=interpretation_id,
            root=Path(str(interpretation.get("interpretation_dir", ""))),
            names=INTERPRETATION_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, GATE_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_method_promotion_gate_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_method_promotion_gate",
        root.name,
        root / "promotion_gate_manifest.json",
    )
    return {
        "promotion_gate_id": root.name,
        "promotion_gate_dir": root,
        "manifest": manifest,
        "promotion_gate_decision": decision_payload,
        "promoted_candidate_specs": specs,
    }


def _gate_outputs(
    interpretation: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    decisions = _promotion_gate_decisions(
        _records(interpretation.get("top_candidate_explanations"))
    )
    promoted = [
        row for row in decisions if row.get("decision") == "PROMOTE_TO_FORMAL_IMPLEMENTATION"
    ][:3]
    decision_payload = {
        "schema_version": st.SCHEMA_VERSION,
        "promotion_gate_id": "",
        "decision_summary": _promotion_decision_summary(decisions),
        "decisions": decisions,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    specs = {
        "schema_version": st.SCHEMA_VERSION,
        "promoted_candidates": [_promoted_candidate_spec(row) for row in promoted],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    return decision_payload, specs


def _gate_manifest(
    *,
    root: Path,
    promotion_gate_id: str,
    interpretation: Mapping[str, Any],
    generated_at: str,
    decision_payload: Mapping[str, Any],
    specs: Mapping[str, Any],
) -> dict[str, Any]:
    decisions = _records(decision_payload.get("decisions"))
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_method_promotion_gate_manifest",
        "promotion_gate_id": promotion_gate_id,
        "interpretation_id": interpretation.get("interpretation_id"),
        "cluster_id": interpretation.get("cluster_id"),
        "scorecard_id": interpretation.get("scorecard_id"),
        "robustness_id": interpretation.get("robustness_id"),
        "generated_at": generated_at,
        "status": "PASS" if decisions else "FAIL",
        "promoted_candidate_count": len(_records(specs.get("promoted_candidates"))),
        "promotion_gate_manifest_path": str(root / "promotion_gate_manifest.json"),
        "promotion_gate_decision_path": str(root / "promotion_gate_decision.json"),
        "promoted_candidate_specs_path": str(root / "promoted_candidate_specs.json"),
        "promotion_gate_report_path": str(root / "promotion_gate_report.md"),
        "weight_method_promotion_gate_input_snapshot_path": str(
            root / "weight_method_promotion_gate_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def weight_method_promotion_gate_report_payload(
    *,
    promotion_gate_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=promotion_gate_id,
        latest_pointer="latest_weight_method_promotion_gate",
        latest=latest,
        output_dir=output_dir,
        required_name="promotion_gate_manifest.json",
    )
    return {
        **_read_json(root / "promotion_gate_manifest.json"),
        "promotion_gate_decision": _read_json(root / "promotion_gate_decision.json"),
        "promoted_candidate_specs": _read_json(root / "promoted_candidate_specs.json"),
        "input_snapshot": _read_json(root / "weight_method_promotion_gate_input_snapshot.json"),
        "promotion_gate_dir": str(root),
    }


def _rebuild_gate(root: Path, promotion_gate_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_method_promotion_gate_input_snapshot.json")
    _require(snapshot.get("schema_version") == GATE_INPUT_SCHEMA, "gate snapshot schema")
    _require(snapshot.get("promotion_gate_id") == promotion_gate_id, "gate snapshot id")
    source = _mapping(snapshot.get("interpretation_source"))
    _validate_binding(source, kind="weight_top_candidate_interpretation")
    interpretation_id = _source_id(source)
    validation = cached_artifact_validation(
        validator=validate_weight_top_candidate_interpretation_artifact,
        validator_key="interpretation_id",
        artifact_id=interpretation_id,
        root=_source_dir(source).parent,
    )
    _require(validation.get("status") == "PASS", "source interpretation validation failed")
    interpretation = weight_top_candidate_interpretation_report_payload(
        interpretation_id=interpretation_id,
        output_dir=_source_dir(source).parent,
    )
    decision_payload, specs = _gate_outputs(interpretation)
    decision_payload["promotion_gate_id"] = promotion_gate_id
    manifest = _gate_manifest(
        root=root,
        promotion_gate_id=promotion_gate_id,
        interpretation=interpretation,
        generated_at=_text(snapshot.get("generated_at")),
        decision_payload=decision_payload,
        specs=specs,
    )
    expected = {
        "promotion_gate_manifest.json": foundation._json_bytes(manifest),
        "promotion_gate_decision.json": foundation._json_bytes(decision_payload),
        "promoted_candidate_specs.json": foundation._json_bytes(specs),
        "promotion_gate_report.md": foundation._text_file_bytes(
            render_promotion_gate_report(manifest, decision_payload)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check(
            "gate_safety",
            _payload_experiment_safe(manifest, decision_payload, specs),
            "",
        )
    )
    return checks


@with_artifact_validation_session
def validate_weight_method_promotion_gate_artifact(
    *,
    promotion_gate_id: str,
    output_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
) -> dict[str, Any]:
    root = output_dir / promotion_gate_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_method_promotion_gate_input_snapshot.json",
        schema=GATE_INPUT_SCHEMA,
        id_key="promotion_gate_id",
        artifact_id=promotion_gate_id,
        view_names=GATE_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_method_promotion_gate_validation",
            promotion_gate_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_method_promotion_gate_validation",
        artifact_id=promotion_gate_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_gate(root, promotion_gate_id),
    )


@with_artifact_validation_session
def run_formal_method_auto_plan(
    *,
    promotion_gate_id: str,
    promotion_gate_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    gate = _validated_gate(promotion_gate_id, promotion_gate_dir)
    specs, validation_plan = _plan_outputs(gate)
    plan_id = _stable_id(
        "formal-method-auto-plan",
        promotion_gate_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / plan_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _plan_manifest(
        root=root,
        plan_id=root.name,
        gate=gate,
        generated_at=generated.isoformat(),
        specs=specs,
    )
    implementation_plan = render_formal_method_implementation_plan(
        manifest,
        specs,
        validation_plan,
    )
    _write_json(root / "formal_method_auto_plan_manifest.json", manifest)
    _write_json(root / "formal_method_specs.json", specs)
    _write_text(root / "implementation_plan.md", implementation_plan)
    _write_json(root / "validation_plan.json", validation_plan)
    _write_text(
        root / "formal_method_auto_plan_report.md",
        render_formal_method_auto_plan_report(manifest, specs),
    )
    snapshot = {
        "schema_version": PLAN_INPUT_SCHEMA,
        "plan_id": root.name,
        "generated_at": generated.isoformat(),
        "promotion_gate_source": _binding(
            kind="weight_method_promotion_gate",
            artifact_id=promotion_gate_id,
            root=Path(str(gate.get("promotion_gate_dir", ""))),
            names=GATE_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, PLAN_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "formal_method_auto_plan_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_formal_method_auto_plan",
        root.name,
        root / "formal_method_auto_plan_manifest.json",
    )
    return {
        "plan_id": root.name,
        "plan_dir": root,
        "manifest": manifest,
        "formal_method_specs": specs,
        "validation_plan": validation_plan,
        "implementation_plan": implementation_plan,
    }


def _validated_gate(promotion_gate_id: str, promotion_gate_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=validate_weight_method_promotion_gate_artifact,
        validator_key="promotion_gate_id",
        artifact_id=promotion_gate_id,
        root=promotion_gate_dir,
    )
    _require(validation.get("status") == "PASS", "source promotion gate validation failed")
    return weight_method_promotion_gate_report_payload(
        promotion_gate_id=promotion_gate_id,
        output_dir=promotion_gate_dir,
    )


def _plan_outputs(gate: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    candidates = _records(_mapping(gate.get("promoted_candidate_specs")).get("promoted_candidates"))
    specs = _formal_method_specs(candidates)
    return specs, _formal_validation_plan(specs)


def _plan_manifest(
    *,
    root: Path,
    plan_id: str,
    gate: Mapping[str, Any],
    generated_at: str,
    specs: Mapping[str, Any],
) -> dict[str, Any]:
    status = "PLAN_READY" if _records(specs.get("methods")) else "SKIPPED_NO_PROMOTED_CANDIDATE"
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_formal_method_auto_plan_manifest",
        "plan_id": plan_id,
        "promotion_gate_id": gate.get("promotion_gate_id"),
        "scorecard_id": gate.get("scorecard_id"),
        "generated_at": generated_at,
        "status": status,
        "implemented": False,
        "implementation_reason": (
            "auto-plan only; no official target, broker, production, or owner approval action"
        ),
        "formal_method_auto_plan_manifest_path": str(
            root / "formal_method_auto_plan_manifest.json"
        ),
        "formal_method_specs_path": str(root / "formal_method_specs.json"),
        "implementation_plan_path": str(root / "implementation_plan.md"),
        "validation_plan_path": str(root / "validation_plan.json"),
        "formal_method_auto_plan_report_path": str(root / "formal_method_auto_plan_report.md"),
        "formal_method_auto_plan_input_snapshot_path": str(
            root / "formal_method_auto_plan_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def formal_method_auto_plan_report_payload(
    *,
    plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=plan_id,
        latest_pointer="latest_formal_method_auto_plan",
        latest=latest,
        output_dir=output_dir,
        required_name="formal_method_auto_plan_manifest.json",
    )
    return {
        **_read_json(root / "formal_method_auto_plan_manifest.json"),
        "formal_method_specs": _read_json(root / "formal_method_specs.json"),
        "validation_plan": _read_json(root / "validation_plan.json"),
        "implementation_plan": (root / "implementation_plan.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "formal_method_auto_plan_input_snapshot.json"),
        "plan_dir": str(root),
    }


def _rebuild_plan(root: Path, plan_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "formal_method_auto_plan_input_snapshot.json")
    _require(snapshot.get("schema_version") == PLAN_INPUT_SCHEMA, "plan snapshot schema")
    _require(snapshot.get("plan_id") == plan_id, "plan snapshot id")
    source = _mapping(snapshot.get("promotion_gate_source"))
    _validate_binding(source, kind="weight_method_promotion_gate")
    gate = _validated_gate(_source_id(source), _source_dir(source).parent)
    specs, validation_plan = _plan_outputs(gate)
    manifest = _plan_manifest(
        root=root,
        plan_id=plan_id,
        gate=gate,
        generated_at=_text(snapshot.get("generated_at")),
        specs=specs,
    )
    expected = {
        "formal_method_auto_plan_manifest.json": foundation._json_bytes(manifest),
        "formal_method_specs.json": foundation._json_bytes(specs),
        "implementation_plan.md": foundation._text_file_bytes(
            render_formal_method_implementation_plan(manifest, specs, validation_plan)
        ),
        "validation_plan.json": foundation._json_bytes(validation_plan),
        "formal_method_auto_plan_report.md": foundation._text_file_bytes(
            render_formal_method_auto_plan_report(manifest, specs)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.extend(
        [
            st._check("implemented_false", manifest.get("implemented") is False, ""),
            st._check("plan_safety", _payload_experiment_safe(manifest, specs), ""),
            st._check(
                "method_specs_safe",
                all(
                    row.get("broker_action_allowed") is False
                    and row.get("production_effect") == st.PRODUCTION_EFFECT
                    for row in _records(specs.get("methods"))
                ),
                "",
            ),
        ]
    )
    return checks


@with_artifact_validation_session
def validate_formal_method_auto_plan_artifact(
    *,
    plan_id: str,
    output_dir: Path = DEFAULT_FORMAL_METHOD_AUTO_PLAN_DIR,
) -> dict[str, Any]:
    root = output_dir / plan_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="formal_method_auto_plan_input_snapshot.json",
        schema=PLAN_INPUT_SCHEMA,
        id_key="plan_id",
        artifact_id=plan_id,
        view_names=PLAN_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_formal_method_auto_plan_validation",
            plan_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_formal_method_auto_plan_validation",
        artifact_id=plan_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_plan(root, plan_id),
    )


@with_artifact_validation_session
def build_weight_search_dashboard(
    *,
    scorecard_id: str,
    branch_id: str,
    promotion_gate_id: str | None = None,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    branch_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    promotion_gate_dir: Path = DEFAULT_WEIGHT_METHOD_PROMOTION_GATE_DIR,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard, branch, gate = _validated_dashboard_sources(
        scorecard_id=scorecard_id,
        branch_id=branch_id,
        promotion_gate_id=promotion_gate_id,
        scorecard_dir=scorecard_dir,
        branch_dir=branch_dir,
        promotion_gate_dir=promotion_gate_dir,
    )
    summary = _dashboard_summary(scorecard, branch, gate)
    dashboard_id = _stable_id(
        "weight-search-dashboard",
        scorecard_id,
        branch_id,
        promotion_gate_id or "",
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / dashboard_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _dashboard_manifest(
        root=root,
        dashboard_id=root.name,
        scorecard_id=scorecard_id,
        branch_id=branch_id,
        promotion_gate_id=promotion_gate_id,
        generated_at=generated.isoformat(),
    )
    reader = render_dashboard_reader_brief(summary)
    _write_json(root / "search_dashboard_manifest.json", manifest)
    _write_json(root / "search_summary.json", summary["search_summary"])
    _write_json(root / "top_candidates.json", summary["top_candidates"])
    _write_json(root / "rejected_summary.json", summary["rejected_summary"])
    _write_json(root / "next_actions.json", summary["next_actions"])
    _write_text(root / "reader_brief_section.md", reader)
    gate_binding = (
        _binding(
            kind="weight_method_promotion_gate",
            artifact_id=promotion_gate_id,
            root=Path(str(gate.get("promotion_gate_dir", ""))),
            names=GATE_FILES,
        )
        if promotion_gate_id
        else None
    )
    snapshot = {
        "schema_version": DASHBOARD_INPUT_SCHEMA,
        "dashboard_id": root.name,
        "generated_at": generated.isoformat(),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(str(scorecard.get("scorecard_dir", ""))),
            names=evaluation.SCORECARD_FILES,
        ),
        "adaptive_branch_source": _binding(
            kind="weight_adaptive_branch",
            artifact_id=branch_id,
            root=Path(str(branch.get("branch_dir", ""))),
            names=evaluation.ADAPTIVE_FILES,
        ),
        "promotion_gate_source": gate_binding,
        "view_hashes": foundation._view_hashes(root, DASHBOARD_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_search_dashboard_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_search_dashboard",
        root.name,
        root / "search_dashboard_manifest.json",
    )
    return {
        "dashboard_id": root.name,
        "dashboard_dir": root,
        "manifest": manifest,
        **summary,
        "reader_brief_section": reader,
    }


def _validated_dashboard_sources(
    *,
    scorecard_id: str,
    branch_id: str,
    promotion_gate_id: str | None,
    scorecard_dir: Path,
    branch_dir: Path,
    promotion_gate_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    branch = _validated_adaptive(branch_id, branch_dir)
    _require(branch.get("scorecard_id") == scorecard_id, "dashboard branch lineage mismatch")
    gate = _validated_gate(promotion_gate_id, promotion_gate_dir) if promotion_gate_id else {}
    if gate:
        _require(gate.get("scorecard_id") == scorecard_id, "dashboard gate lineage mismatch")
    return scorecard, branch, gate


def _dashboard_manifest(
    *,
    root: Path,
    dashboard_id: str,
    scorecard_id: str,
    branch_id: str,
    promotion_gate_id: str | None,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_search_dashboard_manifest",
        "dashboard_id": dashboard_id,
        "scorecard_id": scorecard_id,
        "branch_id": branch_id,
        "promotion_gate_id": promotion_gate_id or "",
        "generated_at": generated_at,
        "status": "PASS",
        "search_dashboard_manifest_path": str(root / "search_dashboard_manifest.json"),
        "search_summary_path": str(root / "search_summary.json"),
        "top_candidates_path": str(root / "top_candidates.json"),
        "rejected_summary_path": str(root / "rejected_summary.json"),
        "next_actions_path": str(root / "next_actions.json"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "weight_search_dashboard_input_snapshot_path": str(
            root / "weight_search_dashboard_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def weight_search_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=dashboard_id,
        latest_pointer="latest_weight_search_dashboard",
        latest=latest,
        output_dir=output_dir,
        required_name="search_dashboard_manifest.json",
    )
    return {
        **_read_json(root / "search_dashboard_manifest.json"),
        "search_summary": _read_json(root / "search_summary.json"),
        "top_candidates": _read_json(root / "top_candidates.json"),
        "rejected_summary": _read_json(root / "rejected_summary.json"),
        "next_actions": _read_json(root / "next_actions.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "weight_search_dashboard_input_snapshot.json"),
        "dashboard_dir": str(root),
    }


def _rebuild_dashboard(root: Path, dashboard_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_search_dashboard_input_snapshot.json")
    _require(snapshot.get("schema_version") == DASHBOARD_INPUT_SCHEMA, "dashboard snapshot schema")
    _require(snapshot.get("dashboard_id") == dashboard_id, "dashboard snapshot id")
    score_source = _mapping(snapshot.get("scorecard_source"))
    branch_source = _mapping(snapshot.get("adaptive_branch_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(branch_source, kind="weight_adaptive_branch")
    gate_value = snapshot.get("promotion_gate_source")
    gate_source = _mapping(gate_value) if isinstance(gate_value, Mapping) else {}
    if gate_source:
        _validate_binding(gate_source, kind="weight_method_promotion_gate")
    score_id = _source_id(score_source)
    branch_id = _source_id(branch_source)
    gate_id = _source_id(gate_source) if gate_source else None
    scorecard, branch, gate = _validated_dashboard_sources(
        scorecard_id=score_id,
        branch_id=branch_id,
        promotion_gate_id=gate_id,
        scorecard_dir=_source_dir(score_source).parent,
        branch_dir=_source_dir(branch_source).parent,
        promotion_gate_dir=_source_dir(gate_source).parent if gate_source else Path("."),
    )
    summary = _dashboard_summary(scorecard, branch, gate)
    manifest = _dashboard_manifest(
        root=root,
        dashboard_id=dashboard_id,
        scorecard_id=score_id,
        branch_id=branch_id,
        promotion_gate_id=gate_id,
        generated_at=_text(snapshot.get("generated_at")),
    )
    expected = {
        "search_dashboard_manifest.json": foundation._json_bytes(manifest),
        "search_summary.json": foundation._json_bytes(summary["search_summary"]),
        "top_candidates.json": foundation._json_bytes(summary["top_candidates"]),
        "rejected_summary.json": foundation._json_bytes(summary["rejected_summary"]),
        "next_actions.json": foundation._json_bytes(summary["next_actions"]),
        "reader_brief_section.md": foundation._text_file_bytes(
            render_dashboard_reader_brief(summary)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.append(
        st._check("dashboard_safety", _payload_experiment_safe(manifest, *summary.values()), "")
    )
    return checks


@with_artifact_validation_session
def validate_weight_search_dashboard_artifact(
    *,
    dashboard_id: str,
    output_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = output_dir / dashboard_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_search_dashboard_input_snapshot.json",
        schema=DASHBOARD_INPUT_SCHEMA,
        id_key="dashboard_id",
        artifact_id=dashboard_id,
        view_names=DASHBOARD_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_search_dashboard_validation",
            dashboard_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_search_dashboard_validation",
        artifact_id=dashboard_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_dashboard(root, dashboard_id),
    )


@with_artifact_validation_session
def build_owner_research_decision_pack(
    *,
    dashboard_id: str,
    dashboard_dir: Path = DEFAULT_WEIGHT_SEARCH_DASHBOARD_DIR,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    validation = cached_artifact_validation(
        validator=validate_weight_search_dashboard_artifact,
        validator_key="dashboard_id",
        artifact_id=dashboard_id,
        root=dashboard_dir,
    )
    _require(validation.get("status") == "PASS", "source dashboard validation failed")
    dashboard = weight_search_dashboard_report_payload(
        dashboard_id=dashboard_id,
        output_dir=dashboard_dir,
    )
    options = _owner_decision_options(dashboard)
    pack_id = _stable_id(
        "owner-research-decision-pack",
        dashboard_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / pack_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = _owner_manifest(
        root=root,
        owner_pack_id=root.name,
        dashboard_id=dashboard_id,
        generated_at=generated.isoformat(),
        options=options,
    )
    _write_json(root / "owner_decision_pack_manifest.json", manifest)
    _write_json(root / "owner_decision_options.json", options)
    _write_text(
        root / "owner_decision_pack_report.md",
        render_owner_decision_pack_report(manifest, options),
    )
    snapshot = {
        "schema_version": OWNER_INPUT_SCHEMA,
        "owner_pack_id": root.name,
        "generated_at": generated.isoformat(),
        "dashboard_source": _binding(
            kind="weight_search_dashboard",
            artifact_id=dashboard_id,
            root=Path(str(dashboard.get("dashboard_dir", ""))),
            names=DASHBOARD_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, OWNER_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(
        root / "owner_research_decision_pack_input_snapshot.json",
        snapshot,
    )
    _write_latest_pointer(
        "latest_owner_research_decision_pack",
        root.name,
        root / "owner_decision_pack_manifest.json",
    )
    return {
        "owner_pack_id": root.name,
        "owner_pack_dir": root,
        "manifest": manifest,
        "owner_decision_options": options,
    }


def _owner_manifest(
    *,
    root: Path,
    owner_pack_id: str,
    dashboard_id: str,
    generated_at: str,
    options: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_research_decision_pack_manifest",
        "owner_pack_id": owner_pack_id,
        "dashboard_id": dashboard_id,
        "generated_at": generated_at,
        "status": "PASS",
        "recommended_owner_decision": options.get("recommended_decision"),
        "owner_decision_pack_manifest_path": str(root / "owner_decision_pack_manifest.json"),
        "owner_decision_options_path": str(root / "owner_decision_options.json"),
        "owner_decision_pack_report_path": str(root / "owner_decision_pack_report.md"),
        "owner_research_decision_pack_input_snapshot_path": str(
            root / "owner_research_decision_pack_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }


def owner_research_decision_pack_report_payload(
    *,
    owner_pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=owner_pack_id,
        latest_pointer="latest_owner_research_decision_pack",
        latest=latest,
        output_dir=output_dir,
        required_name="owner_decision_pack_manifest.json",
    )
    return {
        **_read_json(root / "owner_decision_pack_manifest.json"),
        "owner_decision_options": _read_json(root / "owner_decision_options.json"),
        "input_snapshot": _read_json(root / "owner_research_decision_pack_input_snapshot.json"),
        "owner_pack_dir": str(root),
    }


def _rebuild_owner(root: Path, owner_pack_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "owner_research_decision_pack_input_snapshot.json")
    _require(snapshot.get("schema_version") == OWNER_INPUT_SCHEMA, "owner snapshot schema")
    _require(snapshot.get("owner_pack_id") == owner_pack_id, "owner snapshot id")
    source = _mapping(snapshot.get("dashboard_source"))
    _validate_binding(source, kind="weight_search_dashboard")
    dashboard_id = _source_id(source)
    validation = cached_artifact_validation(
        validator=validate_weight_search_dashboard_artifact,
        validator_key="dashboard_id",
        artifact_id=dashboard_id,
        root=_source_dir(source).parent,
    )
    _require(validation.get("status") == "PASS", "source dashboard validation failed")
    dashboard = weight_search_dashboard_report_payload(
        dashboard_id=dashboard_id,
        output_dir=_source_dir(source).parent,
    )
    options = _owner_decision_options(dashboard)
    manifest = _owner_manifest(
        root=root,
        owner_pack_id=owner_pack_id,
        dashboard_id=dashboard_id,
        generated_at=_text(snapshot.get("generated_at")),
        options=options,
    )
    expected = {
        "owner_decision_pack_manifest.json": foundation._json_bytes(manifest),
        "owner_decision_options.json": foundation._json_bytes(options),
        "owner_decision_pack_report.md": foundation._text_file_bytes(
            render_owner_decision_pack_report(manifest, options)
        ),
    }
    allowed = {
        "continue_search",
        "implement_top_candidate",
        "defer_for_forward_data",
        "reject_all_candidates",
        "run_expanded_search",
    }
    checks = _check_bytes(root, expected)
    checks.append(_view_hash_check(root, snapshot))
    checks.extend(
        [
            st._check(
                "recommended_decision_valid",
                options.get("recommended_decision") in allowed,
                _text(options.get("recommended_decision")),
            ),
            st._check("owner_pack_safety", _payload_experiment_safe(manifest, options), ""),
        ]
    )
    return checks


@with_artifact_validation_session
def validate_owner_research_decision_pack_artifact(
    *,
    owner_pack_id: str,
    output_dir: Path = DEFAULT_OWNER_RESEARCH_DECISION_PACK_DIR,
) -> dict[str, Any]:
    root = output_dir / owner_pack_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="owner_research_decision_pack_input_snapshot.json",
        schema=OWNER_INPUT_SCHEMA,
        id_key="owner_pack_id",
        artifact_id=owner_pack_id,
        view_names=OWNER_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_owner_research_decision_pack_validation",
            owner_pack_id,
            checks,
        )
    return _validate_content(
        report_type="etf_dynamic_v3_owner_research_decision_pack_validation",
        artifact_id=owner_pack_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_owner(root, owner_pack_id),
    )
