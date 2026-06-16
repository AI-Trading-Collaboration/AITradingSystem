from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as baseline_control,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

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
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
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
            "does not run strategy optimization or backtests",
            "does not refresh data or rerun upstream paper-shadow artifacts",
            "does not approve candidate promotion or production target weights",
        ],
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
    st._write_latest_pointer(
        "latest_candidate_regression_replay",
        root.name,
        root / "candidate_regression_replay_manifest.json",
    )
    validation = validate_candidate_regression_replay_artifact(
        replay_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "replay_id": root.name,
        "replay_dir": root,
        "manifest": manifest,
        "candidate_regression_replay_report": report,
        "reader_brief_section": reader,
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
    manifest = (
        st._read_optional_json(root / "candidate_regression_replay_manifest.json") or {}
    )
    report = (
        st._read_optional_json(root / "candidate_regression_replay_report.json") or {}
    )
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    comparisons = _records(report.get("comparisons"))
    categories = {_text(row.get("category")) for row in comparisons}
    summary = _mapping(report.get("comparison_summary"))
    current_source = _mapping(report.get("current_behavior_source"))
    checks = st._required_file_checks(
        root,
        (
            "candidate_regression_replay_manifest.json",
            "candidate_regression_replay_report.json",
            "candidate_regression_replay_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_report_id_match",
                manifest.get("replay_id") == report.get("replay_id") == replay_id,
                "",
            ),
            st._check(
                "status_allowed",
                report.get("candidate_regression_replay_status")
                in CANDIDATE_REGRESSION_REPLAY_STATUSES,
                _text(report.get("candidate_regression_replay_status")),
            ),
            st._check(
                "replay_window_visible",
                bool(_mapping(report.get("replay_window")).get("start_date"))
                and bool(_mapping(report.get("replay_window")).get("end_date")),
                "",
            ),
            st._check(
                "expected_behavior_visible",
                bool(report.get("expected_behavior_id")),
                _text(report.get("expected_behavior_id")),
            ),
            st._check(
                "current_source_visible",
                bool(current_source.get("source_id")),
                _text(current_source.get("source_id")),
            ),
            st._check(
                "comparison_categories_complete",
                set(COMPARISON_CATEGORIES).issubset(categories)
                or report.get("candidate_regression_replay_status")
                in {"BLOCKED_MISSING_CURRENT_BEHAVIOR", "BLOCKED_EXPECTED_BEHAVIOR"},
                ",".join(sorted(categories)),
            ),
            st._check(
                "breaking_changes_fail_closed",
                int(summary.get("breaking_change_count") or 0) == 0
                or report.get("candidate_regression_replay_status")
                == "BREAKING_CHANGE_DETECTED",
                str(summary.get("breaking_change_count")),
            ),
            st._check(
                "missing_current_source_fail_closed",
                current_source.get("exists") is not False
                or report.get("candidate_regression_replay_status")
                == "BLOCKED_MISSING_CURRENT_BEHAVIOR",
                _text(current_source.get("limitation")),
            ),
            st._check(
                "reader_brief_fields",
                "candidate_regression_replay_status" in reader
                and "breaking_change_count" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "read_only_regression_guard",
                report.get("regression_guard_only") is True
                and report.get("strategy_behavior_changed") is False
                and report.get("data_downloaded_by_replay") is False
                and report.get("pipelines_executed_by_replay") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_candidate_regression_replay_validation",
        replay_id,
        checks,
    )
    if write_output:
        st._write_json(root / "candidate_regression_replay_validation.json", validation)
        st._write_text(
            root / "candidate_regression_replay_validation.md",
            render_candidate_regression_replay_validation_report(validation),
        )
    return validation


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
    try:
        payload = baseline_control.benchmark_baseline_report_payload(
            control_id=benchmark_baseline_control_id,
            latest=benchmark_baseline_control_id is None,
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
