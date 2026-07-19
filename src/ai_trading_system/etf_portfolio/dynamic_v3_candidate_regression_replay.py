from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as baseline_control,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation

DEFAULT_CANDIDATE_REGRESSION_REPLAY_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "candidate_regression_replay_v1.yaml"
)
DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "candidate_regression_replay"
)
CANDIDATE_REGRESSION_REPLAY_STATUSES = (
    "REGRESSION_REPLAY_PASS",
    "ACCEPTABLE_CHANGE_REVIEW",
    "BREAKING_CHANGE_DETECTED",
    "BLOCKED_MISSING_CURRENT_BEHAVIOR",
    "BLOCKED_EXPECTED_BEHAVIOR",
    "BLOCKED_POLICY",
)
COMPARISON_CATEGORIES = (
    "candidate_outputs",
    "decisions",
    "safety_metadata",
    "artifact_schema",
    "reader_brief_fields",
)
CANDIDATE_REGRESSION_REPLAY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "regression_guard_only": True,
    "strategy_behavior_changed": False,
    "data_downloaded_by_replay": False,
    "pipelines_executed_by_replay": False,
    "execution_model_ready": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
_MISSING = object()
CANDIDATE_REPLAY_INPUT_SCHEMA = "candidate_regression_replay_input_snapshot.v2"
CANDIDATE_REPLAY_VIEWS = (
    "candidate_regression_replay_manifest.json",
    "candidate_regression_replay_report.json",
    "candidate_regression_replay_report.md",
    "reader_brief_section.md",
)
CANDIDATE_REPLAY_SNAPSHOT = "candidate_regression_replay_input_snapshot.json"


def load_candidate_regression_replay_policy(
    config_path: Path = DEFAULT_CANDIDATE_REGRESSION_REPLAY_CONFIG_PATH,
) -> dict[str, Any]:
    return _normalized_policy(st._load_yaml_mapping(config_path), config_path=config_path)


def run_candidate_regression_replay(
    *,
    as_of: date | None = None,
    current_behavior_path: Path | None = None,
    benchmark_baseline_control_id: str | None = None,
    benchmark_baseline_control_dir: Path = (
        baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR
    ),
    config_path: Path = DEFAULT_CANDIDATE_REGRESSION_REPLAY_CONFIG_PATH,
    output_dir: Path = DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
    generated_at: datetime | None = None,
    _validate_output: bool = True,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    policy_source = foundation._file_binding(config_path)
    policy = load_candidate_regression_replay_policy(config_path)
    replay_window = _mapping(policy.get("replay_window"))
    effective_as_of = (
        as_of
        or _parse_optional_date(replay_window.get("end_date"))
        or generated.date()
    )
    current_source = _current_behavior_source(
        current_behavior_path=current_behavior_path,
        benchmark_baseline_control_id=benchmark_baseline_control_id,
        benchmark_baseline_control_dir=benchmark_baseline_control_dir,
    )
    current_bindings = _current_source_bindings(current_source)
    _validate_current_source(
        current_source,
        expected_candidate=_text(
            _mapping(policy.get("expected_behavior")).get("candidate")
        ),
        effective_as_of=effective_as_of,
        generated=generated,
    )
    policy_blockers = _policy_blockers(policy)
    comparisons = _comparison_rows(policy=policy, current_source=current_source)
    classification_counts = _classification_counts(comparisons)
    changed = _changed_groups(comparisons)
    status = _replay_status(
        current_source=current_source,
        policy_blockers=policy_blockers,
        classification_counts=classification_counts,
    )
    candidate = _text(
        _mapping(current_source.get("summary")).get("candidate"),
        _text(_mapping(policy.get("expected_behavior")).get("candidate"), "UNKNOWN"),
    )
    replay_id = st._stable_id(
        "candidate-regression-replay",
        candidate,
        effective_as_of.isoformat(),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        _text(_mapping(policy.get("expected_behavior")).get("behavior_id")),
        _text(current_source.get("artifact_id")),
        status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / replay_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_regression_replay_report",
        "replay_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "policy": policy,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "expected_behavior_id": _text(
            _mapping(policy.get("expected_behavior")).get("behavior_id")
        ),
        "candidate_regression_replay_status": status,
        "replay_window": replay_window,
        "current_behavior_source": current_source,
        "comparison_summary": {
            "comparison_count": len(comparisons),
            "breaking_change_count": classification_counts["BREAKING_CHANGE"],
            "acceptable_change_count": classification_counts["ACCEPTABLE_CHANGE"],
            "unchanged_count": classification_counts["UNCHANGED"],
            "missing_current_field_count": sum(
                1 for row in comparisons if row.get("comparison_status") == "MISSING"
            ),
        },
        "classification_counts": classification_counts,
        "comparisons": comparisons,
        "changed_outputs": changed["candidate_outputs"],
        "changed_decisions": changed["decisions"],
        "changed_safety_metadata": changed["safety_metadata"],
        "changed_artifact_schema": changed["artifact_schema"],
        "changed_reader_brief_fields": changed["reader_brief_fields"],
        "policy_blockers": policy_blockers,
        "blocking_reasons": _blocking_reasons(
            status=status,
            current_source=current_source,
            policy_blockers=policy_blockers,
            classification_counts=classification_counts,
        ),
        "warnings": _warnings(
            status=status,
            classification_counts=classification_counts,
        ),
        "next_required_action": _next_required_action(status),
        "limitations": [
            "regression guard only",
            "schema and behavior contract comparison only; no strategy performance claim",
            "does not run strategy optimization or backtests",
            "does not refresh data or rerun upstream paper-shadow artifacts",
            "does not approve candidate promotion or production target weights",
        ],
        "evidence_scope": "SCHEMA_BEHAVIOR_CONTRACT",
        "strategy_performance_claimed": False,
        **CANDIDATE_REGRESSION_REPLAY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_regression_replay_manifest",
        "replay_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "candidate_regression_replay_status": status,
        "expected_behavior_id": report["expected_behavior_id"],
        "candidate_regression_replay_manifest_path": str(
            root / "candidate_regression_replay_manifest.json"
        ),
        "candidate_regression_replay_report_path": str(
            root / "candidate_regression_replay_report.json"
        ),
        "candidate_regression_replay_markdown_path": str(
            root / "candidate_regression_replay_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "candidate_regression_replay_validation.json"),
        **CANDIDATE_REGRESSION_REPLAY_SAFETY,
    }
    reader = render_candidate_regression_replay_reader_brief(report)
    st._write_json(root / "candidate_regression_replay_manifest.json", manifest)
    st._write_json(root / "candidate_regression_replay_report.json", report)
    st._write_text(
        root / "candidate_regression_replay_report.md",
        render_candidate_regression_replay_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    snapshot = {
        "schema_version": CANDIDATE_REPLAY_INPUT_SCHEMA,
        "replay_id": root.name,
        "generated_at": generated.isoformat(),
        "effective_as_of": effective_as_of.isoformat(),
        "policy_source": policy_source,
        "policy_lineage": {
            "policy_id": policy.get("policy_id"),
            "policy_version": policy.get("version"),
            "expected_behavior_id": report.get("expected_behavior_id"),
        },
        "current_source_bindings": current_bindings,
        "current_source_lineage": {
            "artifact_id": current_source.get("artifact_id"),
            "candidate": _mapping(current_source.get("summary")).get("candidate"),
            "candidate_lineage_id": _mapping(current_source.get("summary")).get(
                "candidate_lineage_id"
            ),
            "as_of": _mapping(current_source.get("summary")).get("as_of"),
            "validation_status": current_source.get("validation_status"),
        },
        "replay": {
            "current_behavior_path": None
            if current_behavior_path is None
            else str(current_behavior_path.resolve()),
            "benchmark_baseline_control_id": benchmark_baseline_control_id,
            "benchmark_baseline_control_dir": str(benchmark_baseline_control_dir.resolve()),
        },
        "view_hashes": foundation._view_hashes(root, CANDIDATE_REPLAY_VIEWS),
    }
    foundation._write_snapshot(root / CANDIDATE_REPLAY_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_candidate_regression_replay",
        root.name,
        root / "candidate_regression_replay_manifest.json",
    )
    validation = (
        validate_candidate_regression_replay_artifact(
            replay_id=root.name,
            output_dir=output_dir,
            write_output=True,
        )
        if _validate_output
        else {"status": "NOT_RUN", "failed_check_count": 0, "checks": []}
    )
    return {
        "replay_id": root.name,
        "replay_dir": root,
        "manifest": manifest,
        "candidate_regression_replay_report": report,
        "reader_brief_section": reader,
        "input_snapshot": snapshot,
        "candidate_regression_replay_validation": validation,
    }


def candidate_regression_replay_report_payload(
    *,
    replay_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=replay_id,
        latest_pointer="latest_candidate_regression_replay",
        latest=latest,
        output_dir=output_dir,
        required_name="candidate_regression_replay_manifest.json",
    )
    payload = {
        **st._read_json(root / "candidate_regression_replay_manifest.json"),
        "candidate_regression_replay_report": st._read_json(
            root / "candidate_regression_replay_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "replay_dir": str(root),
    }
    snapshot = st._read_optional_json(root / CANDIDATE_REPLAY_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(
        root / "candidate_regression_replay_validation.json"
    )
    if validation:
        payload["candidate_regression_replay_validation"] = validation
    return payload


def validate_candidate_regression_replay_artifact(
    *,
    replay_id: str,
    output_dir: Path = DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / replay_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=CANDIDATE_REPLAY_SNAPSHOT,
        schema=CANDIDATE_REPLAY_INPUT_SCHEMA,
        id_key="replay_id",
        artifact_id=replay_id,
        view_names=CANDIDATE_REPLAY_VIEWS,
    )
    validation = (
        diagnostics._validate_content(
            report_type="etf_dynamic_v3_candidate_regression_replay_validation",
            artifact_id=replay_id,
            checks=checks,
            rebuild=lambda: _rebuild_candidate_regression(root, replay_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_candidate_regression_replay_validation", replay_id, checks
        )
    )
    if write_output:
        st._write_json(root / "candidate_regression_replay_validation.json", validation)
        st._write_text(
            root / "candidate_regression_replay_validation.md",
            render_candidate_regression_replay_validation_report(validation),
        )
    return validation


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    return _aware_utc(generated, "generated_at")


def _aware_utc(value: object, field: str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(_text(value))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _current_source_bindings(source: Mapping[str, Any]) -> list[dict[str, Any]]:
    if source.get("exists") is not True:
        return []
    path = Path(_text(source.get("source_path")))
    bindings = [{"role": "current_behavior", **foundation._file_binding(path)}]
    reader_path = path.parent / "reader_brief_section.md"
    if reader_path.is_file():
        bindings.append({"role": "reader_brief", **foundation._file_binding(reader_path)})
    validation_path = _validation_path_for(path)
    if validation_path.is_file():
        bindings.append({"role": "source_validation", **foundation._file_binding(validation_path)})
    return bindings


def _validate_current_source(
    source: Mapping[str, Any],
    *,
    expected_candidate: str,
    effective_as_of: date,
    generated: datetime,
) -> None:
    if effective_as_of > generated.date():
        raise ValueError("candidate regression requested as_of occurs after generated_at")
    if source.get("exists") is not True:
        return
    if source.get("validation_status") != "PASS":
        raise ValueError("candidate regression current source validation must PASS")
    summary = _mapping(source.get("summary"))
    candidate = _text(summary.get("candidate"))
    if not candidate:
        raise ValueError("candidate regression current source candidate is required")
    if expected_candidate and candidate != expected_candidate:
        raise ValueError("candidate regression current source candidate mismatch")
    if not _text(summary.get("candidate_lineage_id")):
        raise ValueError("candidate regression current source lineage is required")
    source_date = _parse_optional_date(summary.get("as_of"))
    if source_date is None:
        raise ValueError("candidate regression current source as_of is required")
    if source_date > effective_as_of:
        raise ValueError("candidate regression source occurs after requested as_of")
    if source_date > generated.date():
        raise ValueError("candidate regression source occurs after generated_at")


def _rebuild_candidate_regression(root: Path, replay_id: str) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / CANDIDATE_REPLAY_SNAPSHOT)
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    for binding in _records(snapshot.get("current_source_bindings")):
        foundation._validate_file_binding(binding)
    generated = _aware_utc(snapshot.get("generated_at"), "snapshot.generated_at")
    effective_as_of = date.fromisoformat(_text(snapshot.get("effective_as_of")))
    replay_args = _mapping(snapshot.get("replay"))
    current_path = next(
        (
            Path(_text(binding.get("path")))
            for binding in _records(snapshot.get("current_source_bindings"))
            if binding.get("role") == "current_behavior"
        ),
        None,
    )
    with TemporaryDirectory(prefix="eb4-candidate-replay-") as temp_dir:
        result = run_candidate_regression_replay(
            as_of=effective_as_of,
            current_behavior_path=current_path,
            benchmark_baseline_control_id=replay_args.get("benchmark_baseline_control_id"),
            benchmark_baseline_control_dir=Path(
                _text(replay_args.get("benchmark_baseline_control_dir"))
            ),
            config_path=Path(_text(policy_source.get("path"))),
            output_dir=Path(temp_dir),
            generated_at=generated,
            _validate_output=False,
        )
        expected_root = Path(result["replay_dir"])
        expected = {
            name: _normalize_replay_root(
                (expected_root / name).read_bytes(), expected_root=expected_root, actual_root=root
            )
            for name in CANDIDATE_REPLAY_VIEWS
        }
    if result["replay_id"] != replay_id:
        raise ValueError("candidate regression replay id is not reproducible")
    return diagnostics._check_bytes(root, expected)


def _normalize_replay_root(payload: bytes, *, expected_root: Path, actual_root: Path) -> bytes:
    old = str(expected_root)
    new = str(actual_root)
    return payload.replace(old.encode(), new.encode()).replace(
        old.replace("\\", "\\\\").encode(),
        new.replace("\\", "\\\\").encode(),
    )


def render_candidate_regression_replay_reader_brief(report: Mapping[str, Any]) -> str:
    summary = _mapping(report.get("comparison_summary"))
    return "\n".join(
        [
            "## Candidate Regression Replay",
            "",
            f"- candidate_regression_replay_id: {report.get('replay_id')}",
            f"- candidate: {report.get('candidate')}",
            f"- candidate_regression_replay_status: "
            f"{report.get('candidate_regression_replay_status')}",
            f"- expected_behavior_id: {report.get('expected_behavior_id')}",
            f"- replay_window: {_replay_window_text(report.get('replay_window'))}",
            f"- comparison_count: {summary.get('comparison_count')}",
            f"- breaking_change_count: {summary.get('breaking_change_count')}",
            f"- acceptable_change_count: {summary.get('acceptable_change_count')}",
            f"- unchanged_count: {summary.get('unchanged_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only regression guard / no strategy optimization / "
            "no broker / no order / no official target / no production",
            "",
        ]
    )


def render_candidate_regression_replay_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    rows = [
        (
            f"| {row.get('category')} | `{row.get('field_path')}` | "
            f"{_markdown_value(row.get('expected_value'))} | "
            f"{_markdown_value(row.get('current_value'))} | "
            f"{row.get('change_classification')} | {row.get('comparison_status')} |"
        )
        for row in _records(report.get("comparisons"))
    ]
    summary = _mapping(report.get("comparison_summary"))
    source = _mapping(report.get("current_behavior_source"))
    return "\n".join(
        [
            f"# Candidate Regression Replay {manifest.get('replay_id')}",
            "",
            "## Purpose",
            "Compare current paper-shadow candidate behavior against stored expected "
            "behavior over a fixed regression window.",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- candidate_regression_replay_status: "
            f"{report.get('candidate_regression_replay_status')}",
            f"- expected_behavior_id: {report.get('expected_behavior_id')}",
            f"- replay_window: {_replay_window_text(report.get('replay_window'))}",
            f"- current_behavior_source: {source.get('artifact_id')} "
            f"status={source.get('status')} path={source.get('source_path')}",
            f"- comparison_count: {summary.get('comparison_count')}",
            f"- breaking_change_count: {summary.get('breaking_change_count')}",
            f"- acceptable_change_count: {summary.get('acceptable_change_count')}",
            f"- unchanged_count: {summary.get('unchanged_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Comparisons",
            "| category | field | expected | current | classification | status |",
            "|---|---|---|---|---|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- regression guard only",
            "- no strategy optimization or backtest execution",
            "- no data refresh or upstream rerun",
            "- no broker integration or order ticket",
            "- no official target weights",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_candidate_regression_replay_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Candidate Regression Replay Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *checks,
            "",
        ]
    )


def _normalized_policy(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {
        **CANDIDATE_REGRESSION_REPLAY_SAFETY,
        **_mapping(config.get("safety_boundaries")),
    }
    return {
        "schema_version": st.SCHEMA_VERSION,
        "policy_id": _text(
            config.get("policy_id"),
            "dynamic_v3_rescue_candidate_regression_replay_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_manual_review_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "replay_window": _mapping(config.get("replay_window")),
        "current_behavior_source": _mapping(config.get("current_behavior_source")),
        "expected_behavior": _mapping(config.get("expected_behavior")),
        "acceptable_change_policy": _mapping(config.get("acceptable_change_policy")),
        "safety_boundaries": safety,
        **safety,
    }


def _current_behavior_source(
    *,
    current_behavior_path: Path | None,
    benchmark_baseline_control_id: str | None,
    benchmark_baseline_control_dir: Path,
) -> dict[str, Any]:
    if current_behavior_path is not None:
        return _current_behavior_from_path(current_behavior_path)
    if not benchmark_baseline_control_id:
        return _missing_source(
            "candidate_current_behavior",
            "explicit current behavior path or benchmark baseline control id is required",
        )
    try:
        payload = baseline_control.benchmark_baseline_report_payload(
            control_id=benchmark_baseline_control_id,
            latest=False,
            output_dir=benchmark_baseline_control_dir,
        )
    except Exception as exc:
        return _missing_source(
            "candidate_current_behavior",
            f"benchmark baseline control missing: {exc}",
        )
    pack = _mapping(payload.get("benchmark_baseline_control_pack"))
    return _source(
        "candidate_current_behavior",
        exists=True,
        artifact_id=_text(pack.get("control_id"), _text(payload.get("control_id"))),
        status=_text(pack.get("benchmark_baseline_status"), "UNKNOWN"),
        validation_status=_text(
            _mapping(payload.get("benchmark_baseline_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("benchmark_baseline_control_pack_path"))),
        summary=_behavior_summary(pack),
        payload=pack,
        reader_brief=_text(payload.get("reader_brief_section")),
    )


def _current_behavior_from_path(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _missing_source(
            "candidate_current_behavior",
            f"current behavior artifact missing: {path}",
        )
    payload = st._read_json(path)
    reader_path = path.parent / "reader_brief_section.md"
    validation_path = _validation_path_for(path)
    return _source(
        "candidate_current_behavior",
        exists=True,
        artifact_id=_text(
            payload.get("control_id"),
            _text(payload.get("artifact_id"), path.parent.name),
        ),
        status=_text(
            payload.get("benchmark_baseline_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        validation_status=_text(
            _mapping(st._read_optional_json(validation_path)).get("status"),
            "NOT_RUN",
        ),
        source_path=path,
        summary=_behavior_summary(payload),
        payload=payload,
        reader_brief=reader_path.read_text(encoding="utf-8") if reader_path.exists() else "",
    )


def _comparison_rows(
    *,
    policy: Mapping[str, Any],
    current_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if current_source.get("exists") is False:
        return []
    payload = _mapping(current_source.get("payload"))
    reader = _text(current_source.get("reader_brief"))
    expected_behavior = _mapping(policy.get("expected_behavior"))
    rows: list[dict[str, Any]] = []
    for row in _records(expected_behavior.get("expected_fields")):
        rows.append(
            _field_comparison(
                category=_text(row.get("category"), "candidate_outputs"),
                field_path=_text(row.get("field_path")),
                expected=row.get("expected"),
                current=_dotted_get(payload, _text(row.get("field_path"))),
                classification_if_changed=_text(
                    row.get("classification_if_changed"),
                    "BREAKING_CHANGE",
                ),
                reason=_text(row.get("reason")),
            )
        )
    for row in _records(expected_behavior.get("safety_fields")):
        rows.append(
            _field_comparison(
                category="safety_metadata",
                field_path=_text(row.get("field_path")),
                expected=row.get("expected"),
                current=_dotted_get(payload, _text(row.get("field_path"))),
                classification_if_changed="BREAKING_CHANGE",
                reason="Safety boundary changed from expected replay contract.",
            )
        )
    for field in _texts(expected_behavior.get("required_schema_fields")):
        rows.append(
            _field_comparison(
                category="artifact_schema",
                field_path=f"schema.required.{field}",
                expected=True,
                current=_dotted_get(payload, field) is not _MISSING,
                classification_if_changed="BREAKING_CHANGE",
                reason="Required source artifact schema field missing.",
            )
        )
    for field in _texts(expected_behavior.get("reader_brief_fields")):
        rows.append(
            _field_comparison(
                category="reader_brief_fields",
                field_path=f"reader_brief_section.contains.{field}",
                expected=True,
                current=field in reader,
                classification_if_changed="BREAKING_CHANGE",
                reason="Reader Brief field missing from current source section.",
            )
        )
    return rows


def _field_comparison(
    *,
    category: str,
    field_path: str,
    expected: Any,
    current: Any,
    classification_if_changed: str,
    reason: str,
) -> dict[str, Any]:
    current_missing = current is _MISSING
    expected_value = _json_value(expected)
    current_value = "MISSING" if current_missing else _json_value(current)
    if current_missing:
        status = "MISSING"
        classification = "BREAKING_CHANGE"
    elif _values_equal(expected, current):
        status = "MATCH"
        classification = "UNCHANGED"
    elif classification_if_changed == "ACCEPTABLE_CHANGE":
        status = "ACCEPTABLE_CHANGE"
        classification = "ACCEPTABLE_CHANGE"
    else:
        status = "BREAKING_CHANGE"
        classification = "BREAKING_CHANGE"
    return {
        "category": category,
        "field_path": field_path,
        "expected_value": expected_value,
        "current_value": current_value,
        "comparison_status": status,
        "change_classification": classification,
        "changed": classification != "UNCHANGED",
        "reason": reason,
    }


def _classification_counts(rows: list[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "UNCHANGED": sum(1 for row in rows if row.get("change_classification") == "UNCHANGED"),
        "ACCEPTABLE_CHANGE": sum(
            1 for row in rows if row.get("change_classification") == "ACCEPTABLE_CHANGE"
        ),
        "BREAKING_CHANGE": sum(
            1 for row in rows if row.get("change_classification") == "BREAKING_CHANGE"
        ),
    }


def _changed_groups(rows: list[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        category: [
            dict(row)
            for row in rows
            if row.get("category") == category and row.get("changed") is True
        ]
        for category in COMPARISON_CATEGORIES
    }


def _replay_status(
    *,
    current_source: Mapping[str, Any],
    policy_blockers: list[str],
    classification_counts: Mapping[str, int],
) -> str:
    if policy_blockers:
        if "expected_behavior:missing" in policy_blockers:
            return "BLOCKED_EXPECTED_BEHAVIOR"
        return "BLOCKED_POLICY"
    if current_source.get("exists") is False:
        return "BLOCKED_MISSING_CURRENT_BEHAVIOR"
    if int(classification_counts.get("BREAKING_CHANGE") or 0):
        return "BREAKING_CHANGE_DETECTED"
    if int(classification_counts.get("ACCEPTABLE_CHANGE") or 0):
        return "ACCEPTABLE_CHANGE_REVIEW"
    return "REGRESSION_REPLAY_PASS"


def _policy_blockers(policy: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if not _text(policy.get("policy_id")):
        blockers.append("policy_id:missing")
    if not _text(policy.get("version")):
        blockers.append("policy_version:missing")
    expected = _mapping(policy.get("expected_behavior"))
    if not _text(expected.get("behavior_id")) or not _records(
        expected.get("expected_fields")
    ):
        blockers.append("expected_behavior:missing")
    window = _mapping(policy.get("replay_window"))
    if not _text(window.get("start_date")) or not _text(window.get("end_date")):
        blockers.append("replay_window:missing")
    return blockers


def _blocking_reasons(
    *,
    status: str,
    current_source: Mapping[str, Any],
    policy_blockers: list[str],
    classification_counts: Mapping[str, int],
) -> list[str]:
    reasons: list[str] = []
    if current_source.get("exists") is False:
        reasons.append("current_behavior:missing")
    reasons.extend(policy_blockers)
    if status == "BREAKING_CHANGE_DETECTED":
        reasons.append(
            f"breaking_changes:{int(classification_counts.get('BREAKING_CHANGE') or 0)}"
        )
    return reasons


def _warnings(*, status: str, classification_counts: Mapping[str, int]) -> list[str]:
    warnings: list[str] = []
    if status == "ACCEPTABLE_CHANGE_REVIEW":
        warnings.append(
            "acceptable_changes_require_manual_review:"
            f"{int(classification_counts.get('ACCEPTABLE_CHANGE') or 0)}"
        )
    return warnings


def _next_required_action(status: str) -> str:
    if status == "REGRESSION_REPLAY_PASS":
        return "continue_research_governance_observation"
    if status == "ACCEPTABLE_CHANGE_REVIEW":
        return "manual_review_acceptable_regression_change"
    if status == "BREAKING_CHANGE_DETECTED":
        return "stop_and_review_candidate_regression_change"
    if status == "BLOCKED_MISSING_CURRENT_BEHAVIOR":
        return "provide_current_candidate_behavior_artifact_before_replay"
    if status == "BLOCKED_EXPECTED_BEHAVIOR":
        return "fix_expected_behavior_policy_before_replay"
    return "fix_replay_policy_before_replay"


def _behavior_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(payload.get("comparison_summary"))
    return {
        "artifact_id": _text(payload.get("control_id"), _text(payload.get("artifact_id"))),
        "candidate": _text(payload.get("candidate")),
        "candidate_lineage_id": _text(
            payload.get("candidate_lineage_id"),
            _text(payload.get("control_id"), _text(payload.get("artifact_id"))),
        ),
        "as_of": _text(payload.get("as_of")),
        "status": _text(
            payload.get("benchmark_baseline_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "baseline_count": payload.get("baseline_count"),
        "outperformed_baseline_count": summary.get("outperformed_baseline_count"),
        "underperformed_baseline_count": summary.get("underperformed_baseline_count"),
        "insufficient_metric_baseline_count": summary.get(
            "insufficient_metric_baseline_count"
        ),
        "next_required_action": _text(payload.get("next_required_action")),
    }


def _source(
    source_id: str,
    *,
    exists: bool,
    artifact_id: str,
    status: str,
    validation_status: str,
    source_path: Path | None,
    summary: Mapping[str, Any],
    payload: Mapping[str, Any],
    reader_brief: str,
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "payload": dict(payload),
        "reader_brief": reader_brief,
        **CANDIDATE_REGRESSION_REPLAY_SAFETY,
    }


def _missing_source(source_id: str, limitation: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {},
        "payload": {},
        "reader_brief": "",
        "limitation": limitation,
        **CANDIDATE_REGRESSION_REPLAY_SAFETY,
    }


def _validation_path_for(path: Path) -> Path:
    candidates = [
        path.parent / "candidate_regression_replay_validation.json",
        path.parent / "benchmark_baseline_validation.json",
        path.parent / "validation.json",
    ]
    return next((candidate for candidate in candidates if candidate.exists()), candidates[0])


def _dotted_get(payload: Mapping[str, Any], field_path: str) -> Any:
    current: Any = payload
    for part in field_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _values_equal(expected: Any, current: Any) -> bool:
    return _json_value(expected) == _json_value(current)


def _json_value(value: Any) -> Any:
    if value is _MISSING:
        return "MISSING"
    if isinstance(value, Mapping):
        return {str(key): _json_value(inner) for key, inner in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_json_value(inner) for inner in value]
    return value


def _markdown_value(value: Any) -> str:
    if isinstance(value, list | dict):
        return "`" + str(value).replace("|", "\\|") + "`"
    return "`" + _text(value).replace("|", "\\|") + "`"


def _replay_window_text(value: Any) -> str:
    window = _mapping(value)
    return (
        f"{window.get('regime', 'UNKNOWN')} "
        f"{window.get('start_date', 'UNKNOWN')}..{window.get('end_date', 'UNKNOWN')}"
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [_text(item) for item in value if _text(item)]
    return []


def _joined_texts(value: Any) -> str:
    return ",".join(_texts(value)) or "none"


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return str(value)
    text = str(value)
    return text if text else default


def _parse_optional_date(value: Any) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None
