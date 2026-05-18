from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DAILY_WEIGHT_ADJUSTMENT_SCHEMA_VERSION = 1
DAILY_WEIGHT_ADJUSTMENT_REPORT_TYPE = "daily_weight_adjustment_summary"
MODE_OBSERVE_ONLY = "observe_only"
GATE_MODE_MANUAL_REVIEW_ONLY = "manual_review_only"
PRODUCTION_EFFECT_NONE = "none"
STATUS_LIMITED = "LIMITED"
STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
STATUS_OBSERVE_ONLY = "OBSERVE_ONLY"
PROMO_READY_FOR_MANUAL_REVIEW = "READY_FOR_MANUAL_REVIEW"
REPO_ROOT = Path(__file__).resolve().parents[4]

FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS = {
    "AUTO_PROMOTE",
    "PROMOTE_TO_PRODUCTION",
    "READY_FOR_LIVE",
    "SHOULD_TRADE",
    "APPROVED_FOR_TRADING",
}

MISSING_REASON_BY_ARTIFACT = {
    "weight_adjustment_candidates.json": "missing_weight_adjustment_candidates",
    "weight_adjustment_candidates.markdown": "missing_weight_adjustment_candidates_markdown",
    "weight_candidate_evaluation.json": "missing_weight_candidate_evaluation",
    "weight_candidate_evaluation.markdown": "missing_weight_candidate_evaluation_markdown",
    "weight_promotion_gate.json": "missing_weight_promotion_gate",
    "weight_promotion_gate.markdown": "missing_weight_promotion_gate_markdown",
}


def default_daily_weight_adjustment_summary_json_path(
    reports_dir: Path,
    as_of: date,
) -> Path:
    return reports_dir / f"daily_weight_adjustment_summary_{as_of.isoformat()}.json"


def build_daily_weight_adjustment_summary_payload(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    weight_adjustment_candidates_path: Path | None = None,
    weight_adjustment_candidates_md_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    weight_candidate_evaluation_md_path: Path | None = None,
    weight_promotion_gate_path: Path | None = None,
    weight_promotion_gate_md_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    suffix = as_of.isoformat()
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_daily_weight_adjustment_summary_json_path(
        reports_dir,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")

    resolved_candidates_path = weight_adjustment_candidates_path or (
        reports_dir / f"weight_adjustment_candidates_{suffix}.json"
    )
    resolved_candidates_md_path = weight_adjustment_candidates_md_path or (
        reports_dir / f"weight_adjustment_candidates_{suffix}.md"
    )
    resolved_evaluation_path = weight_candidate_evaluation_path or (
        reports_dir / f"weight_candidate_evaluation_{suffix}.json"
    )
    resolved_evaluation_md_path = weight_candidate_evaluation_md_path or (
        reports_dir / f"weight_candidate_evaluation_{suffix}.md"
    )
    resolved_gate_path = weight_promotion_gate_path or (
        reports_dir / f"weight_promotion_gate_{suffix}.json"
    )
    resolved_gate_md_path = weight_promotion_gate_md_path or (
        reports_dir / f"weight_promotion_gate_{suffix}.md"
    )

    candidates_payload = _read_json_object(resolved_candidates_path)
    evaluation_payload = _read_json_object(resolved_evaluation_path)
    gate_payload = _read_json_object(resolved_gate_path)
    artifact_specs = (
        (
            "weight_adjustment_candidates",
            "json",
            resolved_candidates_path,
            candidates_payload,
            "weight_adjustment_candidates",
        ),
        (
            "weight_adjustment_candidates",
            "markdown",
            resolved_candidates_md_path,
            {},
            None,
        ),
        (
            "weight_candidate_evaluation",
            "json",
            resolved_evaluation_path,
            evaluation_payload,
            "weight_candidate_evaluation",
        ),
        (
            "weight_candidate_evaluation",
            "markdown",
            resolved_evaluation_md_path,
            {},
            None,
        ),
        (
            "weight_promotion_gate",
            "json",
            resolved_gate_path,
            gate_payload,
            "weight_promotion_gate",
        ),
        (
            "weight_promotion_gate",
            "markdown",
            resolved_gate_md_path,
            {},
            None,
        ),
    )
    source_artifacts = _source_artifacts(artifact_specs, reports_dir)
    missing_artifacts = _missing_artifacts(artifact_specs)
    invalid_artifacts = _invalid_json_artifacts(artifact_specs)
    missing_reasons = _missing_reasons(artifact_specs)
    safety_warnings = _source_safety_warnings(
        candidates_payload=candidates_payload,
        evaluation_payload=evaluation_payload,
        gate_payload=gate_payload,
    )
    raw_promotion_gate_status = _promotion_gate_status(gate_payload)
    inputs_complete = not missing_artifacts and not invalid_artifacts and not safety_warnings
    promotion_gate_status = (
        raw_promotion_gate_status if inputs_complete else STATUS_INSUFFICIENT_DATA
    )
    ready_count = (
        _ready_for_manual_review_count(gate_payload)
        if inputs_complete and promotion_gate_status == PROMO_READY_FOR_MANUAL_REVIEW
        else 0
    )
    blocked_count = _blocked_count(gate_payload) if inputs_complete else 0
    warnings = _warnings(
        missing_artifacts=missing_artifacts,
        invalid_artifacts=invalid_artifacts,
        safety_warnings=safety_warnings,
        gate_payload=gate_payload,
        inputs_complete=inputs_complete,
    )
    main_blocked_by = _main_blocked_by(
        missing_reasons=missing_reasons,
        invalid_artifacts=invalid_artifacts,
        safety_warnings=safety_warnings,
        gate_payload=gate_payload,
        evaluation_payload=evaluation_payload,
        candidates_payload=candidates_payload,
    )
    payload = {
        "schema_version": DAILY_WEIGHT_ADJUSTMENT_SCHEMA_VERSION,
        "report_type": DAILY_WEIGHT_ADJUSTMENT_REPORT_TYPE,
        "generated_at": generated.isoformat(),
        "as_of": suffix,
        "market_regime": "ai_after_chatgpt",
        "status": STATUS_OBSERVE_ONLY if inputs_complete else STATUS_LIMITED,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "mode": MODE_OBSERVE_ONLY,
        "manual_review_only": True,
        "candidate_status": _candidate_status(candidates_payload),
        "evaluation_status": _evaluation_status(evaluation_payload),
        "promotion_gate_status": promotion_gate_status,
        "candidate_count": _candidate_count(candidates_payload, evaluation_payload, gate_payload),
        "evaluable_candidate_count": _evaluable_candidate_count(evaluation_payload),
        "ready_for_manual_review_count": ready_count,
        "blocked_count": blocked_count,
        "top_candidate_id": _top_candidate_id(
            candidates_payload,
            evaluation_payload,
            gate_payload,
        ),
        "main_blocked_by": main_blocked_by,
        "warnings": warnings,
        "source_artifacts": source_artifacts,
        "missing_artifacts": missing_artifacts,
        "invalid_artifacts": invalid_artifacts,
        "required_manual_review_items": _required_manual_review_items(
            missing_reasons=missing_reasons,
            inputs_complete=inputs_complete,
            gate_payload=gate_payload,
            ready_count=ready_count,
        ),
        "recommendation": _recommendation(
            inputs_complete=inputs_complete,
            promotion_gate_status=promotion_gate_status,
            ready_count=ready_count,
            blocked_count=blocked_count,
        ),
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "runs_weight_adjustment_candidate_generator": False,
            "runs_weight_candidate_evaluation": False,
            "runs_weight_promotion_gate": False,
            "runs_replay_runner": False,
            "changes_daily_dashboard_main_conclusion": False,
            "manual_review_only": True,
            "production_effect": PRODUCTION_EFFECT_NONE,
        },
        "safety_boundary": {
            "calls_ibkr": False,
            "calls_paperbroker": False,
            "calls_real_broker": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "runs_controlled_fill_script": False,
            "runs_order_lifecycle_script": False,
            "runs_broker_comparison_script": False,
            "writes_production_profile": False,
            "writes_approved_profile": False,
            "changes_production_parameters": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
        },
        "notes": [
            "本 summary 只串联既有权重候选、评估和人工复核 gate 产物。",
            "缺失上游产物时只标记 LIMITED / INSUFFICIENT_DATA，不补造 improvement。",
            "本 summary 不修改 production profile，不写 reviewed profile。",
            "本 summary 不触发 IBKR、PaperBroker、paper runner、replay runner 或交易。",
            "dashboard 只读读取本 summary JSON，不改变主投资结论。",
        ],
    }
    _assert_forbidden_terms_absent(payload, render_daily_weight_adjustment_summary_report(payload))
    return payload


def write_daily_weight_adjustment_summary_report(
    *,
    as_of: date,
    reports_dir: Path = REPO_ROOT / "outputs" / "reports",
    weight_adjustment_candidates_path: Path | None = None,
    weight_adjustment_candidates_md_path: Path | None = None,
    weight_candidate_evaluation_path: Path | None = None,
    weight_candidate_evaluation_md_path: Path | None = None,
    weight_promotion_gate_path: Path | None = None,
    weight_promotion_gate_md_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_daily_weight_adjustment_summary_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        weight_adjustment_candidates_path=weight_adjustment_candidates_path,
        weight_adjustment_candidates_md_path=weight_adjustment_candidates_md_path,
        weight_candidate_evaluation_path=weight_candidate_evaluation_path,
        weight_candidate_evaluation_md_path=weight_candidate_evaluation_md_path,
        weight_promotion_gate_path=weight_promotion_gate_path,
        weight_promotion_gate_md_path=weight_promotion_gate_md_path,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated_at,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(
        render_daily_weight_adjustment_summary_report(payload),
        encoding="utf-8",
    )
    return payload


def render_daily_weight_adjustment_summary_report(payload: dict[str, Any]) -> str:
    recommendation = _mapping(payload.get("recommendation"))
    source_artifacts = _mapping(payload.get("source_artifacts"))
    lines = [
        "# Daily Weight Adjustment Summary",
        "",
        f"- 评估日期：{payload.get('as_of')}",
        "- 市场阶段：`ai_after_chatgpt`",
        f"- mode：`{payload.get('mode')}`",
        f"- production_effect：`{payload.get('production_effect')}`",
        "- manual_review_only：true",
        "- production profile 修改：无",
        "- reviewed profile 写入：无",
        "- IBKR / PaperBroker / replay runner：未触发",
        "",
        "## Summary",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| candidate_status | `{payload.get('candidate_status')}` |",
        f"| evaluation_status | `{payload.get('evaluation_status')}` |",
        f"| promotion_gate_status | `{payload.get('promotion_gate_status')}` |",
        f"| candidate_count | {payload.get('candidate_count', 0)} |",
        f"| evaluable_candidate_count | {payload.get('evaluable_candidate_count', 0)} |",
        (
            "| ready_for_manual_review_count | "
            f"{payload.get('ready_for_manual_review_count', 0)} |"
        ),
        f"| blocked_count | {payload.get('blocked_count', 0)} |",
        f"| top_candidate_id | `{payload.get('top_candidate_id', '')}` |",
        f"| main_blocked_by | `{payload.get('main_blocked_by', 'none')}` |",
        f"| recommendation | `{recommendation.get('action', '')}` |",
        "",
        "## Source Artifacts",
        "",
        "| Artifact | JSON | Markdown |",
        "|---|---|---|",
    ]
    for key in (
        "weight_adjustment_candidates",
        "weight_candidate_evaluation",
        "weight_promotion_gate",
    ):
        pair = _mapping(source_artifacts.get(key))
        json_artifact = _mapping(pair.get("json"))
        markdown_artifact = _mapping(pair.get("markdown"))
        lines.append(
            "| "
            f"{key} | "
            f"{_artifact_cell(json_artifact)} | "
            f"{_artifact_cell(markdown_artifact)} |"
        )

    missing = _strings(payload.get("missing_artifacts"))
    review_items = _strings(payload.get("required_manual_review_items"))
    warnings = _strings(payload.get("warnings"))
    lines.extend(
        [
            "",
            "## Missing Artifacts",
            "",
            _bullet_list(missing, "none"),
            "",
            "## Required Manual Review Items",
            "",
            _bullet_list(review_items, "none"),
            "",
            "## Warnings",
            "",
            _bullet_list(warnings, "none"),
            "",
            "## Safety Boundary",
            "",
            "- 只读取既有 015/016/017 artifact。",
            "- 不修改 production profile。",
            "- 不写 reviewed profile。",
            (
                "- 不触发 IBKR、PaperBroker、paper runner、replay runner、controlled "
                "fill、lifecycle 或 comparison 脚本。"
            ),
            "- 不改变 dashboard 主投资结论。",
            "",
        ]
    )
    return "\n".join(lines)


def _source_artifacts(
    artifact_specs: tuple[tuple[str, str, Path, dict[str, Any], str | None], ...],
    reports_dir: Path,
) -> dict[str, dict[str, dict[str, Any]]]:
    artifacts: dict[str, dict[str, dict[str, Any]]] = {}
    for group, kind, path, payload, expected_report_type in artifact_specs:
        artifacts.setdefault(group, {})[kind] = _artifact_record(
            path,
            reports_dir=reports_dir,
            payload=payload,
            expected_report_type=expected_report_type,
        )
    return artifacts


def _artifact_record(
    path: Path,
    *,
    reports_dir: Path,
    payload: dict[str, Any] | None = None,
    expected_report_type: str | None = None,
) -> dict[str, Any]:
    exists = path.exists()
    checksum = _sha256(path) if exists else ""
    report_type = _string_value((payload or {}).get("report_type"))
    valid_report_type = (
        True if expected_report_type is None else report_type == expected_report_type
    )
    return {
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else 0,
        "checksum_sha256": checksum,
        "expected_report_type": expected_report_type or "",
        "report_type": report_type,
        "report_type_valid": bool(exists and valid_report_type),
    }


def _missing_artifacts(
    artifact_specs: tuple[tuple[str, str, Path, dict[str, Any], str | None], ...],
) -> list[str]:
    return [
        str(path)
        for _group, _kind, path, _payload, _expected in artifact_specs
        if not path.exists()
    ]


def _invalid_json_artifacts(
    artifact_specs: tuple[tuple[str, str, Path, dict[str, Any], str | None], ...],
) -> list[str]:
    invalid = []
    for _group, kind, path, payload, expected_report_type in artifact_specs:
        if kind != "json" or not path.exists() or expected_report_type is None:
            continue
        if payload.get("report_type") != expected_report_type:
            invalid.append(str(path))
    return invalid


def _missing_reasons(
    artifact_specs: tuple[tuple[str, str, Path, dict[str, Any], str | None], ...],
) -> list[str]:
    reasons = []
    for group, kind, path, _payload, _expected in artifact_specs:
        if path.exists():
            continue
        reason_key = f"{group}.{kind}"
        reason = MISSING_REASON_BY_ARTIFACT.get(reason_key, f"missing_{group}_{kind}")
        reasons.append(reason)
    return reasons


def _source_safety_warnings(
    *,
    candidates_payload: dict[str, Any],
    evaluation_payload: dict[str, Any],
    gate_payload: dict[str, Any],
) -> list[str]:
    warnings = []
    if candidates_payload.get("report_type") == "weight_adjustment_candidates":
        if _string_value(candidates_payload.get("production_effect")) != PRODUCTION_EFFECT_NONE:
            warnings.append("weight_adjustment_candidates_production_effect_not_none")
        if _string_value(candidates_payload.get("mode")) != MODE_OBSERVE_ONLY:
            warnings.append("weight_adjustment_candidates_mode_not_observe_only")
    if evaluation_payload.get("report_type") == "weight_candidate_evaluation":
        if _string_value(evaluation_payload.get("production_effect")) != PRODUCTION_EFFECT_NONE:
            warnings.append("weight_candidate_evaluation_production_effect_not_none")
        if _string_value(evaluation_payload.get("evaluation_mode")) != MODE_OBSERVE_ONLY:
            warnings.append("weight_candidate_evaluation_mode_not_observe_only")
    if gate_payload.get("report_type") == "weight_promotion_gate":
        if _string_value(gate_payload.get("production_effect")) != PRODUCTION_EFFECT_NONE:
            warnings.append("weight_promotion_gate_production_effect_not_none")
        if _string_value(gate_payload.get("gate_mode")) != GATE_MODE_MANUAL_REVIEW_ONLY:
            warnings.append("weight_promotion_gate_mode_not_manual_review_only")
    return warnings


def _warnings(
    *,
    missing_artifacts: list[str],
    invalid_artifacts: list[str],
    safety_warnings: list[str],
    gate_payload: dict[str, Any],
    inputs_complete: bool,
) -> list[str]:
    warnings = []
    if missing_artifacts:
        warnings.append("missing_upstream_artifacts")
    if invalid_artifacts:
        warnings.append("invalid_upstream_artifacts")
    warnings.extend(safety_warnings)
    if (
        not inputs_complete
        and _promotion_gate_status(gate_payload) == PROMO_READY_FOR_MANUAL_REVIEW
    ):
        warnings.append("manual_review_status_ignored_due_to_incomplete_inputs")
    warnings.extend(_candidate_gate_warnings(gate_payload))
    return _dedupe(warnings)


def _candidate_gate_warnings(gate_payload: dict[str, Any]) -> list[str]:
    if gate_payload.get("report_type") != "weight_promotion_gate":
        return []
    counter: Counter[str] = Counter()
    for candidate in _records(gate_payload.get("candidates")):
        counter.update(_strings(candidate.get("warnings")))
    return [value for value, _count in counter.most_common()]


def _candidate_status(candidates_payload: dict[str, Any]) -> str:
    if candidates_payload.get("report_type") != "weight_adjustment_candidates":
        return STATUS_LIMITED
    summary = _mapping(candidates_payload.get("summary"))
    return (
        _string_value(candidates_payload.get("gate_status"))
        or _string_value(summary.get("gate_status"))
        or _string_value(candidates_payload.get("status"))
        or STATUS_LIMITED
    )


def _evaluation_status(evaluation_payload: dict[str, Any]) -> str:
    if evaluation_payload.get("report_type") != "weight_candidate_evaluation":
        return STATUS_INSUFFICIENT_DATA
    summary = _mapping(evaluation_payload.get("summary"))
    return (
        _string_value(evaluation_payload.get("evaluation_status"))
        or _string_value(summary.get("evaluation_status"))
        or STATUS_INSUFFICIENT_DATA
    )


def _promotion_gate_status(gate_payload: dict[str, Any]) -> str:
    if gate_payload.get("report_type") != "weight_promotion_gate":
        return STATUS_INSUFFICIENT_DATA
    summary = _mapping(gate_payload.get("summary"))
    return (
        _string_value(summary.get("promotion_gate_status"))
        or _string_value(summary.get("gate_status"))
        or _string_value(gate_payload.get("promotion_gate_status"))
        or STATUS_INSUFFICIENT_DATA
    )


def _candidate_count(
    candidates_payload: dict[str, Any],
    evaluation_payload: dict[str, Any],
    gate_payload: dict[str, Any],
) -> int:
    candidate_summary = _mapping(candidates_payload.get("summary"))
    evaluation_summary = _mapping(evaluation_payload.get("summary"))
    gate_summary = _mapping(gate_payload.get("summary"))
    return (
        _optional_int(candidate_summary.get("candidate_count"))
        or _optional_int(candidates_payload.get("candidate_count"))
        or _optional_int(evaluation_summary.get("candidate_count"))
        or _optional_int(gate_summary.get("candidate_count"))
        or len(_records(candidates_payload.get("candidates")))
        or len(_records(evaluation_payload.get("candidates")))
        or len(_records(gate_payload.get("candidates")))
    )


def _evaluable_candidate_count(evaluation_payload: dict[str, Any]) -> int:
    if evaluation_payload.get("report_type") != "weight_candidate_evaluation":
        return 0
    summary = _mapping(evaluation_payload.get("summary"))
    return _optional_int(summary.get("evaluable_candidate_count")) or 0


def _ready_for_manual_review_count(gate_payload: dict[str, Any]) -> int:
    if gate_payload.get("report_type") != "weight_promotion_gate":
        return 0
    summary = _mapping(gate_payload.get("summary"))
    return _optional_int(summary.get("ready_for_manual_review_count")) or sum(
        1
        for candidate in _records(gate_payload.get("candidates"))
        if _string_value(candidate.get("promotion_gate_status")) == PROMO_READY_FOR_MANUAL_REVIEW
    )


def _blocked_count(gate_payload: dict[str, Any]) -> int:
    if gate_payload.get("report_type") != "weight_promotion_gate":
        return 0
    summary = _mapping(gate_payload.get("summary"))
    if _optional_int(summary.get("blocked_count")) is not None:
        return _optional_int(summary.get("blocked_count")) or 0
    return sum(
        1 for candidate in _records(gate_payload.get("candidates")) if candidate.get("blocked")
    )


def _top_candidate_id(
    candidates_payload: dict[str, Any],
    evaluation_payload: dict[str, Any],
    gate_payload: dict[str, Any],
) -> str:
    for payload in (candidates_payload, evaluation_payload, gate_payload):
        summary = _mapping(payload.get("summary"))
        value = _string_value(summary.get("top_candidate_id")) or _string_value(
            payload.get("top_candidate_id")
        )
        if value:
            return value
    for payload in (candidates_payload, evaluation_payload, gate_payload):
        records = _records(payload.get("candidates"))
        if records:
            return _string_value(records[0].get("candidate_id"))
    return ""


def _main_blocked_by(
    *,
    missing_reasons: list[str],
    invalid_artifacts: list[str],
    safety_warnings: list[str],
    gate_payload: dict[str, Any],
    evaluation_payload: dict[str, Any],
    candidates_payload: dict[str, Any],
) -> str:
    if missing_reasons:
        return missing_reasons[0]
    if invalid_artifacts:
        return "invalid_upstream_artifact"
    if safety_warnings:
        return safety_warnings[0]
    for payload in (gate_payload, evaluation_payload, candidates_payload):
        summary = _mapping(payload.get("summary"))
        blocked_by = _string_value(summary.get("main_blocked_by"))
        if blocked_by:
            return blocked_by
    gate_blockers = _gate_blockers(gate_payload)
    return gate_blockers[0] if gate_blockers else "none"


def _gate_blockers(gate_payload: dict[str, Any]) -> list[str]:
    summary = _mapping(gate_payload.get("summary"))
    summary_blockers = _strings(summary.get("blocked_by"))
    if summary_blockers:
        return summary_blockers
    counter: Counter[str] = Counter()
    for candidate in _records(gate_payload.get("candidates")):
        counter.update(_strings(candidate.get("blocked_by")))
    return [value for value, _count in counter.most_common()]


def _required_manual_review_items(
    *,
    missing_reasons: list[str],
    inputs_complete: bool,
    gate_payload: dict[str, Any],
    ready_count: int,
) -> list[str]:
    if not inputs_complete:
        return [f"restore_{reason}" for reason in missing_reasons] or [
            "resolve_invalid_or_unsafe_upstream_artifacts"
        ]
    items = []
    for candidate in _records(gate_payload.get("candidates")):
        if (
            ready_count > 0
            and _string_value(candidate.get("promotion_gate_status"))
            != PROMO_READY_FOR_MANUAL_REVIEW
        ):
            continue
        items.extend(_strings(candidate.get("required_manual_review_items")))
    return _dedupe(items) or ["manual_owner_review"] if ready_count > 0 else []


def _recommendation(
    *,
    inputs_complete: bool,
    promotion_gate_status: str,
    ready_count: int,
    blocked_count: int,
) -> dict[str, str]:
    if not inputs_complete:
        return {
            "action": "collect_missing_or_invalid_upstream_artifacts",
            "explanation": "缺少或不可信的上游产物，保持只读观察。",
        }
    if promotion_gate_status == PROMO_READY_FOR_MANUAL_REVIEW and ready_count > 0:
        return {
            "action": "manual_review_only",
            "explanation": "可以准备人工复核材料；summary 不应用权重。",
        }
    if blocked_count > 0:
        return {
            "action": "continue_observation",
            "explanation": "存在 gate 阻断项，继续观察并补充证据。",
        }
    return {
        "action": "continue_observation",
        "explanation": "保持 observe-only 日常观察。",
    }


def _assert_forbidden_terms_absent(payload: dict[str, Any], markdown: str) -> None:
    combined = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n" + markdown
    for term in FORBIDDEN_DAILY_WEIGHT_ADJUSTMENT_TERMS:
        if term in combined:
            raise ValueError(f"Forbidden daily weight adjustment term present: {term}")


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [str(value)] if str(value) else []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_href(path: Path, reports_dir: Path) -> str:
    try:
        return path.relative_to(reports_dir).as_posix()
    except ValueError:
        return str(path)


def _artifact_cell(artifact: dict[str, Any]) -> str:
    exists = "yes" if artifact.get("exists") else "no"
    href = _string_value(artifact.get("href")) or _string_value(artifact.get("path"))
    status = "valid" if artifact.get("report_type_valid", True) else "invalid"
    return f"{exists} / {status} / `{href}`"


def _bullet_list(values: list[str], empty: str) -> str:
    if not values:
        return f"- {empty}"
    return "\n".join(f"- `{value}`" for value in values)


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._=-]+", "_", value.strip())
    return slug.strip("._-") or "artifact"
