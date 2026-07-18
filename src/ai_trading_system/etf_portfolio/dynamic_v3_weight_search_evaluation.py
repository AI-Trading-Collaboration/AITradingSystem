from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as _legacy
from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_foundation as foundation,
)
from ai_trading_system.etf_portfolio.dynamic_v3_weight_search_validation_scope import (
    validate_upstream_with_hardened_scope,
)

DEFAULT_WEIGHT_SCORECARD_DIR = _legacy.DEFAULT_WEIGHT_SCORECARD_DIR
DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR = _legacy.DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR
DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR = _legacy.DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR
DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR = _legacy.DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR
DEFAULT_WEIGHT_BATCH_BACKFILL_DIR = foundation.DEFAULT_WEIGHT_BATCH_BACKFILL_DIR
DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR = foundation.DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR
DEFAULT_WEIGHT_SEARCH_SPACE_DIR = foundation.DEFAULT_WEIGHT_SEARCH_SPACE_DIR

SCORECARD_INPUT_SCHEMA = "weight_scorecard_input_snapshot.v2"
ROBUSTNESS_INPUT_SCHEMA = "weight_robustness_review_input_snapshot.v2"
ADAPTIVE_INPUT_SCHEMA = "weight_adaptive_branch_input_snapshot.v2"

SCORECARD_VIEWS = (
    "weight_scorecard_manifest.json",
    "variant_scorecard.jsonl",
    "pareto_frontier.json",
    "score_distribution.json",
    "weight_scorecard_report.md",
)
ROBUSTNESS_VIEWS = (
    "robustness_manifest.json",
    "rolling_robustness.jsonl",
    "regime_robustness.jsonl",
    "stability_robustness.jsonl",
    "robustness_summary.json",
    "weight_robustness_review_report.md",
)
ADAPTIVE_VIEWS = (
    "adaptive_branch_manifest.json",
    "branch_decision.json",
    "weight_adaptive_branch_report.md",
)
MATRIX_FILES = (
    "batch2_matrix_manifest.json",
    "batch2_variant_specs.jsonl",
    "batch2_family_coverage.json",
    "batch2_matrix_report.md",
    "weight_experiment_batch2_input_snapshot.json",
)
BACKFILL_FILES = (
    "batch_backfill_manifest.json",
    "weight_batch_backfill_input_snapshot.json",
    "batch_backfill_progress.json",
    "variant_weight_paths.jsonl",
    "variant_performance_metrics.jsonl",
    "variant_regime_metrics.jsonl",
    "variant_stability_metrics.jsonl",
    "variant_churn_metrics.jsonl",
    "variant_lag_metrics.jsonl",
    "batch_backfill_report.md",
)
SCORECARD_FILES = (*SCORECARD_VIEWS, "weight_scorecard_input_snapshot.json")
ROBUSTNESS_FILES = (*ROBUSTNESS_VIEWS, "weight_robustness_review_input_snapshot.json")
ADAPTIVE_FILES = (*ADAPTIVE_VIEWS, "weight_adaptive_branch_input_snapshot.json")
SEARCH_FILES = (
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
_payload_safe = _legacy._payload_safe
_payload_experiment_safe = _legacy._payload_experiment_safe
_scorecard_rows = _legacy._scorecard_rows
_pareto_frontier = _legacy._pareto_frontier
_score_distribution = _legacy._score_distribution
_top_by = _legacy._top_by
_top_stability = _legacy._top_stability
_rolling_robustness_rows = _legacy._rolling_robustness_rows
_robustness_summary = _legacy._robustness_summary
_adaptive_branch_decision = _legacy._adaptive_branch_decision
render_weight_scorecard_report = _legacy.render_weight_scorecard_report
render_robustness_report = _legacy.render_robustness_report
render_adaptive_branch_report = _legacy.render_adaptive_branch_report


class DynamicV3WeightSearchEvaluationError(ValueError):
    """Raised when the evaluation chain cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3WeightSearchEvaluationError(message)


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
        view_errors = foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes")))
        view_keys_ok = set(_mapping(snapshot.get("view_hashes"))) == set(view_names)
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


def _validated_matrix(matrix_id: str, matrix_dir: Path) -> dict[str, Any]:
    validation = foundation.validate_weight_experiment_batch2_artifact(
        matrix_id=matrix_id,
        output_dir=matrix_dir,
    )
    _require(validation.get("status") == "PASS", "source matrix validation failed")
    return foundation.weight_experiment_batch2_report_payload(
        matrix_id=matrix_id,
        output_dir=matrix_dir,
    )


def _validated_backfill(backfill_id: str, backfill_dir: Path) -> dict[str, Any]:
    validation = validate_upstream_with_hardened_scope(
        validator=foundation.validate_weight_batch_backfill_artifact,
        validator_key="backfill_id",
        artifact_id=backfill_id,
        output_dir=backfill_dir,
        snapshot_name="weight_batch_backfill_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source backfill validation failed")
    return foundation.weight_batch_backfill_report_payload(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )


def run_weight_scorecard(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    matrix_dir: Path = DEFAULT_WEIGHT_EXPERIMENT_BATCH2_DIR,
    output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = _validated_backfill(backfill_id, backfill_dir)
    matrix_id = _text(backfill.get("matrix_id"))
    matrix = _validated_matrix(matrix_id, matrix_dir)
    _require(matrix_id == _text(matrix.get("matrix_id")), "matrix id mismatch")
    _require(
        _text(backfill.get("source_backfill_id")) == _text(matrix.get("source_backfill_id")),
        "backfill and matrix source lineage mismatch",
    )
    scorecard = _scorecard_rows(backfill, _records(matrix.get("variant_specs")))
    pareto = _pareto_frontier(scorecard)
    distribution = _score_distribution(scorecard)
    scorecard_id = _stable_id("weight-scorecard", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / scorecard_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_scorecard_manifest",
        "scorecard_id": root.name,
        "batch_backfill_id": backfill_id,
        "batch2_matrix_id": matrix.get("matrix_id"),
        "search_space_id": matrix.get("search_space_id"),
        "source_backfill_id": matrix.get("source_backfill_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if scorecard else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "top_return_candidate": _top_by(scorecard, "total_return"),
        "top_drawdown_candidate": _top_by(scorecard, "max_drawdown"),
        "top_stability_candidate": _top_stability(scorecard),
        "weight_scorecard_manifest_path": str(root / "weight_scorecard_manifest.json"),
        "variant_scorecard_path": str(root / "variant_scorecard.jsonl"),
        "pareto_frontier_path": str(root / "pareto_frontier.json"),
        "score_distribution_path": str(root / "score_distribution.json"),
        "weight_scorecard_report_path": str(root / "weight_scorecard_report.md"),
        "weight_scorecard_input_snapshot_path": str(root / "weight_scorecard_input_snapshot.json"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "weight_scorecard_manifest.json", manifest)
    _write_jsonl(root / "variant_scorecard.jsonl", scorecard)
    _write_json(root / "pareto_frontier.json", pareto)
    _write_json(root / "score_distribution.json", distribution)
    _write_text(
        root / "weight_scorecard_report.md",
        render_weight_scorecard_report(manifest, distribution, pareto),
    )
    snapshot = {
        "schema_version": SCORECARD_INPUT_SCHEMA,
        "scorecard_id": root.name,
        "generated_at": generated.isoformat(),
        "backfill_source": _binding(
            kind="weight_batch_backfill",
            artifact_id=backfill_id,
            root=Path(str(backfill.get("backfill_dir", ""))),
            names=BACKFILL_FILES,
        ),
        "matrix_source": _binding(
            kind="weight_experiment_batch2",
            artifact_id=matrix_id,
            root=Path(str(matrix.get("matrix_dir", ""))),
            names=MATRIX_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, SCORECARD_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_scorecard_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_scorecard", root.name, root / "weight_scorecard_manifest.json"
    )
    return {
        "scorecard_id": root.name,
        "scorecard_dir": root,
        "manifest": manifest,
        "variant_scorecard": scorecard,
        "pareto_frontier": pareto,
        "score_distribution": distribution,
    }


def weight_scorecard_report_payload(
    *,
    scorecard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=scorecard_id,
        latest_pointer="latest_weight_scorecard",
        latest=latest,
        output_dir=output_dir,
        required_name="weight_scorecard_manifest.json",
    )
    return {
        **_read_json(root / "weight_scorecard_manifest.json"),
        "variant_scorecard": _read_jsonl(root / "variant_scorecard.jsonl"),
        "pareto_frontier": _read_json(root / "pareto_frontier.json"),
        "score_distribution": _read_json(root / "score_distribution.json"),
        "input_snapshot": _read_json(root / "weight_scorecard_input_snapshot.json"),
        "scorecard_dir": str(root),
    }


def _rebuild_scorecard(root: Path, scorecard_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_scorecard_input_snapshot.json")
    _require(snapshot.get("schema_version") == SCORECARD_INPUT_SCHEMA, "scorecard snapshot schema")
    _require(snapshot.get("scorecard_id") == scorecard_id, "scorecard snapshot id")
    backfill_source = _mapping(snapshot.get("backfill_source"))
    matrix_source = _mapping(snapshot.get("matrix_source"))
    _validate_binding(backfill_source, kind="weight_batch_backfill")
    _validate_binding(matrix_source, kind="weight_experiment_batch2")
    backfill = _validated_backfill(_source_id(backfill_source), _source_dir(backfill_source).parent)
    matrix = _validated_matrix(_source_id(matrix_source), _source_dir(matrix_source).parent)
    _require(_text(backfill.get("matrix_id")) == _text(matrix.get("matrix_id")), "matrix lineage")
    rows = _scorecard_rows(backfill, _records(matrix.get("variant_specs")))
    pareto = _pareto_frontier(rows)
    distribution = _score_distribution(rows)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_scorecard_manifest",
        "scorecard_id": scorecard_id,
        "batch_backfill_id": _source_id(backfill_source),
        "batch2_matrix_id": matrix.get("matrix_id"),
        "search_space_id": matrix.get("search_space_id"),
        "source_backfill_id": matrix.get("source_backfill_id"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS" if rows else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "top_return_candidate": _top_by(rows, "total_return"),
        "top_drawdown_candidate": _top_by(rows, "max_drawdown"),
        "top_stability_candidate": _top_stability(rows),
        "weight_scorecard_manifest_path": str(root / "weight_scorecard_manifest.json"),
        "variant_scorecard_path": str(root / "variant_scorecard.jsonl"),
        "pareto_frontier_path": str(root / "pareto_frontier.json"),
        "score_distribution_path": str(root / "score_distribution.json"),
        "weight_scorecard_report_path": str(root / "weight_scorecard_report.md"),
        "weight_scorecard_input_snapshot_path": str(root / "weight_scorecard_input_snapshot.json"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    expected = {
        "weight_scorecard_manifest.json": foundation._json_bytes(manifest),
        "variant_scorecard.jsonl": foundation._jsonl_bytes(rows),
        "pareto_frontier.json": foundation._json_bytes(pareto),
        "score_distribution.json": foundation._json_bytes(distribution),
        "weight_scorecard_report.md": foundation._text_file_bytes(
            render_weight_scorecard_report(manifest, distribution, pareto)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.extend(
        st._check(f"view_hash_{name}", check == [], name)
        for name, check in [
            ("all", foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes"))))
        ]
    )
    checks.append(
        st._check("scorecard_safety", _payload_experiment_safe(manifest, pareto, *rows), "")
    )
    return checks


def validate_weight_scorecard_artifact(
    *, scorecard_id: str, output_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR
) -> dict[str, Any]:
    root = output_dir / scorecard_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_scorecard_input_snapshot.json",
        schema=SCORECARD_INPUT_SCHEMA,
        id_key="scorecard_id",
        artifact_id=scorecard_id,
        view_names=SCORECARD_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_scorecard_validation", scorecard_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_scorecard_validation",
        artifact_id=scorecard_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_scorecard(root, scorecard_id),
    )


def _validated_scorecard(scorecard_id: str, scorecard_dir: Path) -> dict[str, Any]:
    validation = validate_upstream_with_hardened_scope(
        validator=validate_weight_scorecard_artifact,
        validator_key="scorecard_id",
        artifact_id=scorecard_id,
        output_dir=scorecard_dir,
        snapshot_name="weight_scorecard_input_snapshot.json",
    )
    _require(validation.get("status") == "PASS", "source scorecard validation failed")
    return weight_scorecard_report_payload(scorecard_id=scorecard_id, output_dir=scorecard_dir)


def run_weight_robustness_review(
    *,
    scorecard_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    backfill_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    backfill_id = _text(scorecard.get("batch_backfill_id"))
    backfill = _validated_backfill(backfill_id, backfill_dir)
    _require(backfill_id == _text(scorecard.get("batch_backfill_id")), "scorecard backfill lineage")
    top_ids = [
        _text(row.get("variant_id")) for row in _records(scorecard.get("variant_scorecard"))[:12]
    ]
    rolling = _rolling_robustness_rows(scorecard, backfill, top_ids)
    regime = [
        row
        for row in _records(backfill.get("variant_regime_metrics"))
        if row.get("variant_id") in top_ids
    ]
    stability = [
        row
        for row in _records(backfill.get("variant_stability_metrics"))
        if row.get("variant_id") in top_ids
    ]
    summary = _robustness_summary(top_ids, rolling, regime, stability)
    robustness_id = _stable_id("weight-robustness-review", scorecard_id, generated.isoformat())
    root = _unique_dir(output_dir / robustness_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_robustness_review_manifest",
        "robustness_id": root.name,
        "scorecard_id": scorecard_id,
        "batch_backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if top_ids else "FAIL",
        "robust_candidate_count": len(_texts(summary.get("robust_candidates"))),
        "robustness_manifest_path": str(root / "robustness_manifest.json"),
        "rolling_robustness_path": str(root / "rolling_robustness.jsonl"),
        "regime_robustness_path": str(root / "regime_robustness.jsonl"),
        "stability_robustness_path": str(root / "stability_robustness.jsonl"),
        "robustness_summary_path": str(root / "robustness_summary.json"),
        "weight_robustness_review_report_path": str(root / "weight_robustness_review_report.md"),
        "weight_robustness_review_input_snapshot_path": str(
            root / "weight_robustness_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "robustness_manifest.json", manifest)
    _write_jsonl(root / "rolling_robustness.jsonl", rolling)
    _write_jsonl(root / "regime_robustness.jsonl", regime)
    _write_jsonl(root / "stability_robustness.jsonl", stability)
    _write_json(root / "robustness_summary.json", summary)
    _write_text(
        root / "weight_robustness_review_report.md", render_robustness_report(manifest, summary)
    )
    snapshot = {
        "schema_version": ROBUSTNESS_INPUT_SCHEMA,
        "robustness_id": root.name,
        "generated_at": generated.isoformat(),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(str(scorecard.get("scorecard_dir", ""))),
            names=SCORECARD_FILES,
        ),
        "backfill_source": _binding(
            kind="weight_batch_backfill",
            artifact_id=backfill_id,
            root=Path(str(backfill.get("backfill_dir", ""))),
            names=BACKFILL_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, ROBUSTNESS_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_robustness_review_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_robustness_review", root.name, root / "robustness_manifest.json"
    )
    return {
        "robustness_id": root.name,
        "robustness_dir": root,
        "manifest": manifest,
        "rolling_robustness": rolling,
        "regime_robustness": regime,
        "stability_robustness": stability,
        "robustness_summary": summary,
    }


def weight_robustness_review_report_payload(
    *,
    robustness_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=robustness_id,
        latest_pointer="latest_weight_robustness_review",
        latest=latest,
        output_dir=output_dir,
        required_name="robustness_manifest.json",
    )
    return {
        **_read_json(root / "robustness_manifest.json"),
        "rolling_robustness": _read_jsonl(root / "rolling_robustness.jsonl"),
        "regime_robustness": _read_jsonl(root / "regime_robustness.jsonl"),
        "stability_robustness": _read_jsonl(root / "stability_robustness.jsonl"),
        "robustness_summary": _read_json(root / "robustness_summary.json"),
        "input_snapshot": _read_json(root / "weight_robustness_review_input_snapshot.json"),
        "robustness_dir": str(root),
    }


def _rebuild_robustness(root: Path, robustness_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_robustness_review_input_snapshot.json")
    _require(
        snapshot.get("schema_version") == ROBUSTNESS_INPUT_SCHEMA, "robustness snapshot schema"
    )
    _require(snapshot.get("robustness_id") == robustness_id, "robustness snapshot id")
    score_source = _mapping(snapshot.get("scorecard_source"))
    backfill_source = _mapping(snapshot.get("backfill_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(backfill_source, kind="weight_batch_backfill")
    score_id = _source_id(score_source)
    scorecard = _validated_scorecard(score_id, _source_dir(score_source).parent)
    backfill_id = _source_id(backfill_source)
    backfill = _validated_backfill(backfill_id, _source_dir(backfill_source).parent)
    _require(
        backfill_id == _text(scorecard.get("batch_backfill_id")), "robustness backfill lineage"
    )
    top_ids = [
        _text(row.get("variant_id")) for row in _records(scorecard.get("variant_scorecard"))[:12]
    ]
    rolling = _rolling_robustness_rows(scorecard, backfill, top_ids)
    regime = [
        row
        for row in _records(backfill.get("variant_regime_metrics"))
        if row.get("variant_id") in top_ids
    ]
    stability = [
        row
        for row in _records(backfill.get("variant_stability_metrics"))
        if row.get("variant_id") in top_ids
    ]
    summary = _robustness_summary(top_ids, rolling, regime, stability)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_robustness_review_manifest",
        "robustness_id": robustness_id,
        "scorecard_id": score_id,
        "batch_backfill_id": backfill_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS" if top_ids else "FAIL",
        "robust_candidate_count": len(_texts(summary.get("robust_candidates"))),
        "robustness_manifest_path": str(root / "robustness_manifest.json"),
        "rolling_robustness_path": str(root / "rolling_robustness.jsonl"),
        "regime_robustness_path": str(root / "regime_robustness.jsonl"),
        "stability_robustness_path": str(root / "stability_robustness.jsonl"),
        "robustness_summary_path": str(root / "robustness_summary.json"),
        "weight_robustness_review_report_path": str(root / "weight_robustness_review_report.md"),
        "weight_robustness_review_input_snapshot_path": str(
            root / "weight_robustness_review_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    expected = {
        "robustness_manifest.json": foundation._json_bytes(manifest),
        "rolling_robustness.jsonl": foundation._jsonl_bytes(rolling),
        "regime_robustness.jsonl": foundation._jsonl_bytes(regime),
        "stability_robustness.jsonl": foundation._jsonl_bytes(stability),
        "robustness_summary.json": foundation._json_bytes(summary),
        "weight_robustness_review_report.md": foundation._text_file_bytes(
            render_robustness_report(manifest, summary)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(
        st._check(
            "view_hashes",
            not foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes"))),
            "",
        )
    )
    checks.append(st._check("robustness_safety", _payload_experiment_safe(manifest, summary), ""))
    return checks


def validate_weight_robustness_review_artifact(
    *, robustness_id: str, output_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR
) -> dict[str, Any]:
    root = output_dir / robustness_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_robustness_review_input_snapshot.json",
        schema=ROBUSTNESS_INPUT_SCHEMA,
        id_key="robustness_id",
        artifact_id=robustness_id,
        view_names=ROBUSTNESS_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_robustness_review_validation", robustness_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_robustness_review_validation",
        artifact_id=robustness_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_robustness(root, robustness_id),
    )


def run_weight_adaptive_branch(
    *,
    scorecard_id: str,
    robustness_id: str,
    scorecard_dir: Path = DEFAULT_WEIGHT_SCORECARD_DIR,
    robustness_dir: Path = DEFAULT_WEIGHT_ROBUSTNESS_REVIEW_DIR,
    output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scorecard = _validated_scorecard(scorecard_id, scorecard_dir)
    robust_validation = validate_weight_robustness_review_artifact(
        robustness_id=robustness_id, output_dir=robustness_dir
    )
    _require(robust_validation.get("status") == "PASS", "source robustness validation failed")
    robustness = weight_robustness_review_report_payload(
        robustness_id=robustness_id, output_dir=robustness_dir
    )
    _require(robustness.get("scorecard_id") == scorecard_id, "adaptive scorecard lineage mismatch")
    decision = _adaptive_branch_decision(scorecard, robustness)
    branch_id = _stable_id(
        "weight-adaptive-branch", scorecard_id, robustness_id, generated.isoformat()
    )
    root = _unique_dir(output_dir / branch_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_adaptive_branch_manifest",
        "branch_id": root.name,
        "scorecard_id": scorecard_id,
        "robustness_id": robustness_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "branch_decision": decision["branch_decision"],
        "weight_adaptive_branch_manifest_path": str(root / "adaptive_branch_manifest.json"),
        "branch_decision_path": str(root / "branch_decision.json"),
        "weight_adaptive_branch_report_path": str(root / "weight_adaptive_branch_report.md"),
        "weight_adaptive_branch_input_snapshot_path": str(
            root / "weight_adaptive_branch_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    _write_json(root / "adaptive_branch_manifest.json", manifest)
    _write_json(root / "branch_decision.json", decision)
    _write_text(
        root / "weight_adaptive_branch_report.md", render_adaptive_branch_report(manifest, decision)
    )
    snapshot = {
        "schema_version": ADAPTIVE_INPUT_SCHEMA,
        "branch_id": root.name,
        "generated_at": generated.isoformat(),
        "scorecard_source": _binding(
            kind="weight_scorecard",
            artifact_id=scorecard_id,
            root=Path(str(scorecard.get("scorecard_dir", ""))),
            names=SCORECARD_FILES,
        ),
        "robustness_source": _binding(
            kind="weight_robustness_review",
            artifact_id=robustness_id,
            root=Path(str(robustness.get("robustness_dir", ""))),
            names=ROBUSTNESS_FILES,
        ),
        "view_hashes": foundation._view_hashes(root, ADAPTIVE_VIEWS),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    foundation._write_snapshot(root / "weight_adaptive_branch_input_snapshot.json", snapshot)
    _write_latest_pointer(
        "latest_weight_adaptive_branch", root.name, root / "adaptive_branch_manifest.json"
    )
    return {
        "branch_id": root.name,
        "branch_dir": root,
        "manifest": manifest,
        "branch_decision": decision,
    }


def weight_adaptive_branch_report_payload(
    *,
    branch_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=branch_id,
        latest_pointer="latest_weight_adaptive_branch",
        latest=latest,
        output_dir=output_dir,
        required_name="adaptive_branch_manifest.json",
    )
    return {
        **_read_json(root / "adaptive_branch_manifest.json"),
        "branch_decision_payload": _read_json(root / "branch_decision.json"),
        "input_snapshot": _read_json(root / "weight_adaptive_branch_input_snapshot.json"),
        "branch_dir": str(root),
    }


def _rebuild_adaptive(root: Path, branch_id: str) -> list[dict[str, Any]]:
    snapshot = _read_json(root / "weight_adaptive_branch_input_snapshot.json")
    _require(snapshot.get("schema_version") == ADAPTIVE_INPUT_SCHEMA, "adaptive snapshot schema")
    _require(snapshot.get("branch_id") == branch_id, "adaptive snapshot id")
    score_source = _mapping(snapshot.get("scorecard_source"))
    robust_source = _mapping(snapshot.get("robustness_source"))
    _validate_binding(score_source, kind="weight_scorecard")
    _validate_binding(robust_source, kind="weight_robustness_review")
    score_id = _source_id(score_source)
    robust_id = _source_id(robust_source)
    scorecard = _validated_scorecard(score_id, _source_dir(score_source).parent)
    _require(
        validate_weight_robustness_review_artifact(
            robustness_id=robust_id, output_dir=_source_dir(robust_source).parent
        ).get("status")
        == "PASS",
        "source robustness validation failed",
    )
    robustness = weight_robustness_review_report_payload(
        robustness_id=robust_id, output_dir=_source_dir(robust_source).parent
    )
    _require(robustness.get("scorecard_id") == score_id, "adaptive scorecard lineage mismatch")
    decision = _adaptive_branch_decision(scorecard, robustness)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weight_adaptive_branch_manifest",
        "branch_id": branch_id,
        "scorecard_id": score_id,
        "robustness_id": robust_id,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "branch_decision": decision["branch_decision"],
        "weight_adaptive_branch_manifest_path": str(root / "adaptive_branch_manifest.json"),
        "branch_decision_path": str(root / "branch_decision.json"),
        "weight_adaptive_branch_report_path": str(root / "weight_adaptive_branch_report.md"),
        "weight_adaptive_branch_input_snapshot_path": str(
            root / "weight_adaptive_branch_input_snapshot.json"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    expected = {
        "adaptive_branch_manifest.json": foundation._json_bytes(manifest),
        "branch_decision.json": foundation._json_bytes(decision),
        "weight_adaptive_branch_report.md": foundation._text_file_bytes(
            render_adaptive_branch_report(manifest, decision)
        ),
    }
    checks = _check_bytes(root, expected)
    checks.append(
        st._check(
            "view_hashes",
            not foundation._validate_view_hashes(root, _mapping(snapshot.get("view_hashes"))),
            "",
        )
    )
    checks.append(st._check("adaptive_safety", _payload_experiment_safe(manifest, decision), ""))
    return checks


def validate_weight_adaptive_branch_artifact(
    *, branch_id: str, output_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR
) -> dict[str, Any]:
    root = output_dir / branch_id
    checks, preflight_ok = _snapshot_preflight(
        root=root,
        snapshot_name="weight_adaptive_branch_input_snapshot.json",
        schema=ADAPTIVE_INPUT_SCHEMA,
        id_key="branch_id",
        artifact_id=branch_id,
        view_names=ADAPTIVE_VIEWS,
    )
    if not preflight_ok:
        return _validation_payload(
            "etf_dynamic_v3_weight_adaptive_branch_validation", branch_id, checks
        )
    return _validate_content(
        report_type="etf_dynamic_v3_weight_adaptive_branch_validation",
        artifact_id=branch_id,
        base_checks=checks,
        rebuild=lambda: _rebuild_adaptive(root, branch_id),
    )


def _validate_expanded_matrix_authorization(matrix_id: str, matrix_dir: Path) -> dict[str, Any]:
    matrix = _validated_matrix(matrix_id, matrix_dir)
    _require(matrix.get("expanded") is True, "matrix is not an expanded search matrix")
    snapshot_path = matrix_dir / matrix_id / "weight_experiment_batch2_input_snapshot.json"
    snapshot = _read_json(snapshot_path)
    branch_source = _mapping(snapshot.get("adaptive_branch_source"))
    _validate_binding(branch_source, kind="weight_adaptive_branch")
    branch_id = _source_id(branch_source)
    branch_validation = validate_weight_adaptive_branch_artifact(
        branch_id=branch_id, output_dir=_source_dir(branch_source).parent
    )
    _require(branch_validation.get("status") == "PASS", "expanded branch validation failed")
    branch = weight_adaptive_branch_report_payload(
        branch_id=branch_id, output_dir=_source_dir(branch_source).parent
    )
    decision = _mapping(branch.get("branch_decision_payload"))
    _require(
        decision.get("branch_decision") == "RUN_EXPANDED_SEARCH",
        "branch did not authorize expanded search",
    )
    _require(
        decision.get("search_space_id") == matrix.get("search_space_id"),
        "expanded search lineage mismatch",
    )
    return matrix


def build_weight_expanded_search(
    *,
    branch_id: str,
    branch_dir: Path = DEFAULT_WEIGHT_ADAPTIVE_BRANCH_DIR,
    search_space_dir: Path = DEFAULT_WEIGHT_SEARCH_SPACE_DIR,
    output_dir: Path = DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validation = validate_weight_adaptive_branch_artifact(
        branch_id=branch_id, output_dir=branch_dir
    )
    _require(validation.get("status") == "PASS", "source adaptive branch validation failed")
    branch = weight_adaptive_branch_report_payload(branch_id=branch_id, output_dir=branch_dir)
    decision = _mapping(branch.get("branch_decision_payload"))
    _require(
        decision.get("branch_decision") == "RUN_EXPANDED_SEARCH",
        "branch did not authorize expanded search",
    )
    search_space_id = _text(decision.get("search_space_id"))
    _require(bool(search_space_id), "adaptive branch is missing exact search space id")
    search_validation = foundation.validate_weight_search_space_artifact(
        search_space_id=search_space_id, output_dir=search_space_dir
    )
    _require(search_validation.get("status") == "PASS", "source search space validation failed")
    result = foundation.build_weight_experiment_batch2(
        search_space_id=search_space_id,
        search_space_dir=search_space_dir,
        output_dir=output_dir,
        generated_at=generated_at,
        expanded=True,
    )
    root = Path(result["matrix_dir"])
    snapshot_path = root / "weight_experiment_batch2_input_snapshot.json"
    snapshot = _read_json(snapshot_path)
    snapshot["adaptive_branch_source"] = _binding(
        kind="weight_adaptive_branch",
        artifact_id=branch_id,
        root=Path(str(branch.get("branch_dir", ""))),
        names=ADAPTIVE_FILES,
    )
    foundation._write_snapshot(snapshot_path, snapshot)
    _validate_expanded_matrix_authorization(result["matrix_id"], output_dir)
    return result


def run_weight_expanded_search(
    *,
    expanded_matrix_id: str,
    expanded_matrix_dir: Path = DEFAULT_WEIGHT_EXPANDED_SEARCH_DIR,
    baseline_backfill_dir: Path = st.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_WEIGHT_BATCH_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = st.DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    _validate_expanded_matrix_authorization(expanded_matrix_id, expanded_matrix_dir)
    return foundation.run_weight_batch_backfill(
        matrix_id=expanded_matrix_id,
        matrix_dir=expanded_matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        generated_at=generated_at,
    )
