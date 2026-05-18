from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_vs_production_multi_day_review"
TASK_ID = "TRADING-018C2"
COMPARISON_REPORT_TYPE = "daily_shadow_vs_production_comparison"
COMPARISON_AVAILABLE = "COMPARISON_AVAILABLE"
PRODUCTION_EFFECT_NONE = "none"

DECISION_CONTINUE_OBSERVATION = "CONTINUE_OBSERVATION"
DECISION_SHADOW_LOOKS_BETTER = "SHADOW_LOOKS_BETTER"
DECISION_SHADOW_LOOKS_WORSE = "SHADOW_LOOKS_WORSE"
DECISION_INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"

# TRADING-018C2 pilot review policy. These thresholds only classify manual
# review evidence; they must not be reused as production promotion criteria.
MIN_COMPARABLE_DAYS = 3
SAFETY_BLOCKED_DAY_LIMIT = 2
DECISION_DIFFERENCE_RATIO_LIMIT = 0.50
DOMINANT_WEIGHT_KEY_LIMIT = 5
SCORE_EQUAL_EPSILON = 1e-9

REPO_ROOT = Path(__file__).resolve().parents[4]


def default_weight_iteration_comparison_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "comparison"


def default_shadow_vs_production_review_root(data_root: Path) -> Path:
    return default_weight_iteration_comparison_root(data_root) / "reviews"


def default_shadow_vs_production_review_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_shadow_vs_production_review_root(data_root)
        / f"shadow_vs_production_review_{as_of.isoformat()}.json"
    )


def build_shadow_vs_production_multi_day_review_payload(
    *,
    as_of: date,
    lookback_days: int = 7,
    data_root: Path = REPO_ROOT / "data",
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_shadow_vs_production_review_json_path(
        data_root,
        as_of,
    )
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    comparison_root = default_weight_iteration_comparison_root(data_root)
    day_records = [
        _comparison_day_record(day, comparison_root / f"daily_shadow_vs_production_{day}.json")
        for day in _lookback_dates(as_of, lookback_days)
    ]
    comparable_records = [
        record for record in day_records if record.get("status") == COMPARISON_AVAILABLE
    ]
    score_deltas = [
        delta
        for delta in (_optional_float(record.get("score_delta")) for record in comparable_records)
        if delta is not None
    ]
    available_comparison_days = len(score_deltas)
    missing_comparison_days = [
        _string_value(record.get("date"))
        for record in day_records
        if record.get("status") == "MISSING"
    ]
    insufficient_data_days = sum(
        1
        for record in day_records
        if record.get("exists") and record.get("status") == "INSUFFICIENT_DATA"
    )
    safety_blocked_days = sum(
        1 for record in day_records if record.get("status") == "SAFETY_BLOCKED"
    )
    decision_difference_count = sum(
        1 for record in comparable_records if record.get("decision_changed") is True
    )
    shadow_risk_flag_delta_total = sum(
        _int_value(record.get("risk_flag_delta"), default=0) for record in comparable_records
    )
    average_score_delta = round(sum(score_deltas) / len(score_deltas), 10) if score_deltas else 0.0
    shadow_better_score_days = sum(1 for delta in score_deltas if delta > SCORE_EQUAL_EPSILON)
    shadow_worse_score_days = sum(1 for delta in score_deltas if delta < -SCORE_EQUAL_EPSILON)
    shadow_equal_score_days = len(score_deltas) - shadow_better_score_days - shadow_worse_score_days
    production_decision_change_count = _decision_change_count(
        _string_value(record.get("production_decision")) for record in comparable_records
    )
    shadow_decision_change_count = _decision_change_count(
        _string_value(record.get("shadow_decision")) for record in comparable_records
    )
    shadow_more_frequent_decision_changes = (
        shadow_decision_change_count > production_decision_change_count
    )
    decision_difference_ratio = (
        decision_difference_count / available_comparison_days if available_comparison_days else 0.0
    )
    over_sensitive_signal = (
        decision_difference_ratio > DECISION_DIFFERENCE_RATIO_LIMIT
        or shadow_more_frequent_decision_changes
    )
    dominant_changed_weight_keys = _dominant_changed_weight_keys(comparable_records)
    new_shadow_risk_flags = sorted(
        {
            flag
            for record in comparable_records
            for flag in _strings(record.get("new_shadow_risk_flags"))
        }
    )
    review_decision = _review_decision(
        available_comparison_days=available_comparison_days,
        safety_blocked_days=safety_blocked_days,
        average_score_delta=average_score_delta,
        shadow_risk_flag_delta_total=shadow_risk_flag_delta_total,
        decision_difference_ratio=decision_difference_ratio,
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "generated_at": generated.isoformat(),
        "date": as_of.isoformat(),
        "lookback_days": lookback_days,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "review_decision": review_decision,
        "available_comparison_days": available_comparison_days,
        "missing_comparison_days": missing_comparison_days,
        "insufficient_data_days": insufficient_data_days,
        "safety_blocked_days": safety_blocked_days,
        "decision_difference_count": decision_difference_count,
        "decision_difference_ratio": round(decision_difference_ratio, 10),
        "production_decision_change_count": production_decision_change_count,
        "shadow_decision_change_count": shadow_decision_change_count,
        "shadow_more_frequent_decision_changes": shadow_more_frequent_decision_changes,
        "over_sensitive_signal": over_sensitive_signal,
        "average_score_delta": average_score_delta,
        "shadow_better_score_days": shadow_better_score_days,
        "shadow_worse_score_days": shadow_worse_score_days,
        "shadow_equal_score_days": shadow_equal_score_days,
        "shadow_risk_flag_delta_total": shadow_risk_flag_delta_total,
        "new_shadow_risk_flags": new_shadow_risk_flags,
        "dominant_changed_weight_keys": dominant_changed_weight_keys,
        "promotion_readiness": {
            "ready": False,
            "reason": (
                "Promotion is not allowed in TRADING-018C2. "
                "This report only provides review evidence."
            ),
        },
        "review_policy": {
            "policy_id": "trading_018c2_multi_day_review_pilot",
            "policy_version": "1.0",
            "status": "pilot_manual_review_only",
            "minimum_comparable_days": MIN_COMPARABLE_DAYS,
            "safety_blocked_day_limit": SAFETY_BLOCKED_DAY_LIMIT,
            "decision_difference_ratio_limit": DECISION_DIFFERENCE_RATIO_LIMIT,
            "rationale": (
                "Pilot thresholds classify multi-day review evidence only; "
                "TRADING-018C2 cannot promote shadow weights."
            ),
        },
        "input_artifacts": [_input_artifact_record(record) for record in day_records],
        "daily_records": day_records,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
        },
        "pipeline_contract": {
            "reads_existing_comparison_artifacts_only": True,
            "runs_scoring_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_vs_production_multi_day_review.py",
            "safe_for_production": False,
        },
    }
    _assert_safety_invariants(payload)
    return payload


def write_shadow_vs_production_multi_day_review_report(
    *,
    as_of: date,
    lookback_days: int = 7,
    data_root: Path = REPO_ROOT / "data",
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    payload = build_shadow_vs_production_multi_day_review_payload(
        as_of=as_of,
        lookback_days=lookback_days,
        data_root=data_root,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated_at,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    _write_json(json_path, payload)
    _write_text(md_path, render_shadow_vs_production_multi_day_review_report(payload))
    return payload


def render_shadow_vs_production_multi_day_review_report(payload: dict[str, Any]) -> str:
    lines = [
        f"# Multi-day Shadow vs Production Review - {payload.get('date')}",
        "",
        "## 1. Run Summary",
        "",
        f"- Review decision：`{payload.get('review_decision')}`",
        f"- Lookback days：{payload.get('lookback_days')}",
        f"- Available comparison days：{payload.get('available_comparison_days')}",
        "- Production effect：`none`",
        "- Manual review only：`true`",
        "",
        "## 2. Data Coverage",
        "",
        (
            f"- Missing comparison days："
            f"{', '.join(_strings(payload.get('missing_comparison_days'))) or 'none'}"
        ),
        f"- Insufficient data days：{payload.get('insufficient_data_days')}",
        f"- Safety blocked days：{payload.get('safety_blocked_days')}",
        "",
        "| Date | Status | Artifact | score_delta | decision_changed |",
        "|---|---|---|---:|---|",
    ]
    for record in _list_mappings(payload.get("daily_records")):
        lines.append(
            "| "
            f"{record.get('date')} | "
            f"`{record.get('status')}` | "
            f"{_markdown_path(record.get('path'))} | "
            f"{_format_signed_float(record.get('score_delta'))} | "
            f"`{record.get('decision_changed', 'NA')}` |"
        )
    lines.extend(
        [
            "",
            "## 3. Score Comparison",
            "",
            f"- average_score_delta：{_format_signed_float(payload.get('average_score_delta'))}",
            f"- shadow_better_score_days：{payload.get('shadow_better_score_days')}",
            f"- shadow_worse_score_days：{payload.get('shadow_worse_score_days')}",
            f"- shadow_equal_score_days：{payload.get('shadow_equal_score_days')}",
            "",
            "## 4. Decision Stability",
            "",
            f"- decision_difference_count：{payload.get('decision_difference_count')}",
            (
                "- production_decision_change_count："
                f"{payload.get('production_decision_change_count')}"
            ),
            f"- shadow_decision_change_count：{payload.get('shadow_decision_change_count')}",
            (
                "- shadow 更频繁改变判断："
                f"`{payload.get('shadow_more_frequent_decision_changes')}`"
            ),
            f"- 过度敏感迹象：`{payload.get('over_sensitive_signal')}`",
            "",
            "## 5. Risk Comparison",
            "",
            f"- shadow_risk_flag_delta_total：{payload.get('shadow_risk_flag_delta_total')}",
            (
                f"- 新增 shadow risk flags："
                f"{', '.join(_strings(payload.get('new_shadow_risk_flags'))) or 'none'}"
            ),
            "- shadow risk flags 是否更多："
            f"`{_int_value(payload.get('shadow_risk_flag_delta_total'), default=0) > 0}`",
            "",
            "## 6. Weight Change Drivers",
            "",
            (
                "- dominant_changed_weight_keys："
                f"{', '.join(_strings(payload.get('dominant_changed_weight_keys'))) or 'none'}"
            ),
            "",
            "## 7. Promotion Readiness",
            "",
            "This task does not promote shadow weights to production.",
            "TRADING-018C2 only provides multi-day review evidence.",
            (
                f"- promotion_readiness.ready："
                f"`{_mapping(payload.get('promotion_readiness')).get('ready')}`"
            ),
            (f"- reason：" f"{_mapping(payload.get('promotion_readiness')).get('reason', '')}"),
            "",
            "## 8. Next Step",
            "",
            (
                "- 后续可进入 TRADING-018D manual promotion gate，但只有在足够多天 "
                "review 稳定、人工复核和单独 gate 任务具备后才考虑。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _comparison_day_record(day: date, path: Path) -> dict[str, Any]:
    base_record: dict[str, Any] = {
        "date": day.isoformat(),
        "path": str(path),
        "exists": path.exists(),
        "checksum_sha256": _sha256(path) if path.exists() and path.is_file() else "",
        "status": "MISSING",
        "comparison_status": "MISSING",
        "production_decision": "MISSING",
        "shadow_decision": "MISSING",
        "score_delta": None,
        "decision_changed": None,
        "risk_flag_delta": 0,
        "new_shadow_risk_flags": [],
        "changed_weight_keys": [],
        "weight_delta_abs_by_key": {},
        "safety_reasons": [],
        "blocking_reasons": [],
    }
    if not path.exists():
        return base_record

    payload = _read_json_object(path)
    if not payload:
        return {**base_record, "status": "ERROR", "comparison_status": "ERROR"}
    if payload.get("report_type") != COMPARISON_REPORT_TYPE:
        return {
            **base_record,
            "status": "ERROR",
            "comparison_status": "ERROR",
            "blocking_reasons": ["unexpected_report_type"],
        }

    production = _mapping(payload.get("production"))
    shadow = _mapping(payload.get("shadow"))
    difference = _mapping(payload.get("difference"))
    validation = _mapping(payload.get("input_validation"))
    shadow_iteration = _mapping(payload.get("shadow_iteration"))
    comparison_status = _string_value(payload.get("comparison_status")) or "INSUFFICIENT_DATA"
    safety_reasons = _comparison_safety_reasons(payload)
    if _string_value(shadow_iteration.get("decision")) == "SAFETY_BLOCKED":
        safety_reasons.append("shadow_iteration_safety_blocked")
    blocking_reasons = _strings(validation.get("blocking_reasons"))
    score_delta = _optional_float(difference.get("score_delta"))
    status = comparison_status
    if safety_reasons:
        status = "SAFETY_BLOCKED"
    elif comparison_status != COMPARISON_AVAILABLE or score_delta is None:
        status = "INSUFFICIENT_DATA"

    production_flags = _triggered_risk_flags(production)
    shadow_flags = _triggered_risk_flags(shadow)
    new_shadow_flags = sorted(set(shadow_flags) - set(production_flags))
    weight_delta_abs_by_key = _weight_delta_abs_by_key(difference)
    return {
        **base_record,
        "status": status,
        "comparison_status": comparison_status,
        "production_decision": _string_value(production.get("decision")) or "MISSING",
        "shadow_decision": _string_value(shadow.get("decision")) or "MISSING",
        "production_score": production.get("score"),
        "shadow_score": shadow.get("score"),
        "score_delta": score_delta,
        "decision_changed": _bool_or_none(difference.get("decision_changed")),
        "risk_flags_changed": _bool_or_none(difference.get("risk_flags_changed")),
        "risk_flag_delta": len(shadow_flags) - len(production_flags),
        "new_shadow_risk_flags": new_shadow_flags,
        "changed_weight_keys": sorted(weight_delta_abs_by_key),
        "weight_delta_abs_by_key": weight_delta_abs_by_key,
        "safety_reasons": sorted(set(safety_reasons)),
        "blocking_reasons": blocking_reasons,
    }


def _comparison_safety_reasons(payload: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        reasons.append("comparison_production_effect_not_none")
    if payload.get("manual_review_only") is not True:
        reasons.append("comparison_not_manual_review_only")
    contract = _mapping(payload.get("pipeline_contract"))
    for field in (
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "triggers_trade",
    ):
        if contract.get(field) is not False:
            reasons.append(f"unsafe_contract:{field}")
    return reasons


def _triggered_risk_flags(profile: dict[str, Any]) -> list[str]:
    flags = []
    for item in _list_mappings(profile.get("risk_flags")):
        if item.get("triggered") is True:
            flags.append(_string_value(item.get("gate_id")) or "unknown")
    return flags


def _weight_delta_abs_by_key(difference: dict[str, Any]) -> dict[str, float]:
    deltas: dict[str, float] = {}
    for item in _list_mappings(difference.get("weight_deltas")):
        key = _string_value(item.get("component"))
        value = _optional_float(item.get("weight_delta"))
        if key and value is not None and abs(value) > SCORE_EQUAL_EPSILON:
            deltas[key] = max(deltas.get(key, 0.0), abs(value))
    if deltas:
        return deltas
    for item in _list_mappings(difference.get("contribution_deltas")):
        key = _string_value(item.get("component"))
        value = _optional_float(item.get("weight_delta"))
        if key and value is not None and abs(value) > SCORE_EQUAL_EPSILON:
            deltas[key] = max(deltas.get(key, 0.0), abs(value))
    return deltas


def _dominant_changed_weight_keys(records: list[dict[str, Any]]) -> list[str]:
    totals: Counter[str] = Counter()
    for record in records:
        for key, value in _mapping(record.get("weight_delta_abs_by_key")).items():
            parsed = _optional_float(value)
            if parsed is not None:
                totals[key] += parsed
    return [
        key
        for key, _ in sorted(totals.items(), key=lambda item: (-item[1], item[0]))[
            :DOMINANT_WEIGHT_KEY_LIMIT
        ]
    ]


def _review_decision(
    *,
    available_comparison_days: int,
    safety_blocked_days: int,
    average_score_delta: float,
    shadow_risk_flag_delta_total: int,
    decision_difference_ratio: float,
) -> str:
    if safety_blocked_days >= SAFETY_BLOCKED_DAY_LIMIT:
        return DECISION_SAFETY_BLOCKED
    if available_comparison_days < MIN_COMPARABLE_DAYS:
        return DECISION_INSUFFICIENT_HISTORY
    if average_score_delta < -SCORE_EQUAL_EPSILON or shadow_risk_flag_delta_total > 0:
        return DECISION_SHADOW_LOOKS_WORSE
    if (
        average_score_delta > SCORE_EQUAL_EPSILON
        and shadow_risk_flag_delta_total <= 0
        and decision_difference_ratio <= DECISION_DIFFERENCE_RATIO_LIMIT
    ):
        return DECISION_SHADOW_LOOKS_BETTER
    return DECISION_CONTINUE_OBSERVATION


def _decision_change_count(values: Any) -> int:
    previous = ""
    count = 0
    for value in values:
        if not value or value == "MISSING":
            continue
        if previous and value != previous:
            count += 1
        previous = value
    return count


def _input_artifact_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": record.get("date"),
        "path": record.get("path"),
        "exists": record.get("exists"),
        "status": record.get("status"),
        "comparison_status": record.get("comparison_status"),
        "checksum_sha256": record.get("checksum_sha256", ""),
    }


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("multi-day review production_effect must be none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("multi-day review manual_review_only must be true")
    readiness = _mapping(payload.get("promotion_readiness"))
    if readiness.get("ready") is not False:
        raise ValueError("TRADING-018C2 cannot mark promotion readiness ready")
    contract = _mapping(payload.get("pipeline_contract"))
    for field in (
        "runs_scoring_pipeline",
        "runs_comparison_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is not False:
            raise ValueError(f"unsafe multi-day review contract field: {field}")
    audit = _mapping(payload.get("audit"))
    if audit.get("safe_for_production") is not False:
        raise ValueError("multi-day review audit.safe_for_production must be false")


def _lookback_dates(as_of: date, lookback_days: int) -> list[date]:
    return [as_of - timedelta(days=offset) for offset in range(lookback_days - 1, -1, -1)]


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any, *, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _format_signed_float(value: Any, *, digits: int = 2) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NA"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.{digits}f}"


def _markdown_path(value: Any) -> str:
    text = str(value or "")
    return f"`{text}`" if text else "`missing`"
