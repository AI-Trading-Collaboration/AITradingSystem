from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from typing import Any

from ai_trading_system.contracts.report_spec import (
    ReaderTier,
    ReportSectionSpec,
    ReportSectionViewModel,
)
from ai_trading_system.contracts.status import CanonicalStatus

PRODUCTION_EFFECT = "none"
DATA_QUALITY_AND_PIT_SECTION_ID = "data_quality_and_pit"
DATA_QUALITY_AND_PIT_SOURCE_KEYS = (
    "data_quality_pit_safety",
    "pit_source_manifest",
    "data_refresh_audit",
)

_PASS_STATES = {"PASS", "READY", "AVAILABLE", "OK", "COMPLETE", "DONE"}
_BLOCKED_STATES = {"BLOCKED", "FAILED", "FAIL", "ERROR", "UNSAFE"}
_LIMITED_STATES = {
    "LIMITED",
    "MISSING",
    "NOT_AVAILABLE",
    "REVIEW",
    "UNKNOWN",
}
_DECLARED_STATUS_KEYS = (
    "status",
    "validation_status",
    "safety_status",
    "availability",
)


def project_data_quality_pit_safety(
    *,
    as_of: date,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
) -> dict[str, Any]:
    """Project existing DQ/PIT facts without reading sources or recomputing them."""

    quality = _mapping(snapshot.get("quality"))
    data_gate = _mapping(daily_decision_summary.get("data_gate"))
    data_gate_status = _text(data_gate.get("status"), _quality_status(snapshot))
    signal_date = _text(snapshot.get("signal_date"), as_of.isoformat())
    future_data_status = (
        "PASS"
        if _leading_status(data_gate_status).upper() in {"PASS", "PASS_WITH_WARNINGS"}
        else "REVIEW_REQUIRED"
    )
    return {
        "as_of_date": signal_date,
        "decision_snapshot_id": _text(snapshot.get("snapshot_id"), "UNKNOWN"),
        "data_gate_status": data_gate_status,
        "market_data_status": _text(quality.get("market_data_status"), "UNKNOWN"),
        "market_data_latest_date": _text(
            quality.get("market_data_latest_date"),
            _text(quality.get("latest_market_data_date"), "UNKNOWN_IN_SNAPSHOT"),
        ),
        "market_data_error_count": _text(quality.get("market_data_error_count"), "UNKNOWN"),
        "market_data_warning_count": _text(quality.get("market_data_warning_count"), "UNKNOWN"),
        "feature_status": _text(quality.get("feature_status"), "UNKNOWN"),
        "sec_feature_status": _text(quality.get("sec_feature_status"), "UNKNOWN"),
        "sec_data_latest_filing": _text(
            quality.get("sec_data_latest_filing"),
            _text(quality.get("latest_sec_filing"), "UNKNOWN_IN_SNAPSHOT"),
        ),
        "fmp_valuation_snapshot_timestamp": _text(
            quality.get("fmp_valuation_snapshot_timestamp"),
            _text(
                quality.get("latest_fmp_valuation_timestamp"),
                "UNKNOWN_IN_SNAPSHOT",
            ),
        ),
        "future_data_check": future_data_status,
        "carried_forward_fields": _texts(quality.get("carried_forward_fields")),
        "stale_fields": _texts(quality.get("stale_fields")),
        "blocking_reasons": _texts(data_gate.get("blocking_reasons")),
        "stale_report_count": report_index_summary.get("stale_count"),
        "missing_report_count": report_index_summary.get("missing_count"),
        "pit_visibility_note": (
            "UNKNOWN_IN_SNAPSHOT 表示该源的可见时间未在当前 decision snapshot 明确披露；"
            "不得据此补造 PIT 结论。"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def provide_data_quality_and_pit_section(
    payload: Mapping[str, object], *, spec: ReportSectionSpec
) -> ReportSectionViewModel:
    """Build the native typed section solely from already-materialized report facts."""

    if spec.section_id != DATA_QUALITY_AND_PIT_SECTION_ID:
        raise ValueError("data-quality/PIT provider requires section_id=data_quality_and_pit")
    if spec.reader_tier is not ReaderTier.OWNER_DAILY_BRIEF:
        raise ValueError("data-quality/PIT provider requires reader_tier=owner_daily_brief")
    if spec.source_keys != DATA_QUALITY_AND_PIT_SOURCE_KEYS:
        raise ValueError("data-quality/PIT provider requires the frozen three source keys")

    facts: list[tuple[str, str]] = []
    statuses: list[CanonicalStatus] = []
    missing: list[str] = []
    for key in spec.source_keys:
        if key not in payload:
            facts.append((key, "MISSING"))
            statuses.append(CanonicalStatus.LIMITED)
            missing.append(key)
            continue
        value = payload.get(key)
        facts.append((key, _source_display_value(key, value)))
        statuses.append(_source_status(key, value))
    status = _aggregate_status(tuple(statuses))
    summary = _section_summary(
        section_id=spec.section_id,
        facts=tuple(facts),
        statuses=tuple(statuses),
        aggregate_status=status,
    )
    caveats = ("缺少source keys：" + ",".join(missing),) if missing else ()
    return ReportSectionViewModel(
        section_spec_id=spec.spec_id,
        section_id=spec.section_id,
        title=spec.title,
        reader_tier=spec.reader_tier,
        status=status,
        summary=summary,
        facts=tuple(facts),
        source_keys=spec.source_keys,
        caveats=caveats,
    )


def _quality_status(snapshot: Mapping[str, Any]) -> str:
    return _text(_mapping(snapshot.get("quality")).get("market_data_status"), "UNKNOWN")


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _texts(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {None, ""}]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _leading_status(value: object) -> str:
    text = _text(value, "UNKNOWN").strip()
    for separator in ("；", ";", "，", ",", " "):
        if separator in text:
            text = text.split(separator, maxsplit=1)[0]
            break
    return text or "UNKNOWN"


def _source_display_value(source_key: str, value: object) -> str:
    if isinstance(value, Mapping):
        if source_key == "data_quality_pit_safety":
            dq_states = tuple(
                candidate.strip()
                for key in (
                    "data_gate_status",
                    "future_data_check",
                    "market_data_status",
                )
                if isinstance((candidate := value.get(key)), str) and candidate.strip()
            )
            unsafe = next(
                (
                    candidate
                    for candidate in dq_states
                    if _state_status(candidate) is not CanonicalStatus.PASS
                ),
                None,
            )
            if unsafe is not None:
                return unsafe
            if dq_states:
                return dq_states[0]
        for key in _DECLARED_STATUS_KEYS:
            candidate = value.get(key)
            if (
                isinstance(candidate, str)
                and candidate.strip()
                and _state_status(candidate) is not CanonicalStatus.PASS
            ):
                return candidate.strip()
        for key in (
            "summary_sentence",
            "today_conclusion",
            "recommended_action",
            "summary",
            *_DECLARED_STATUS_KEYS,
        ):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return "AVAILABLE" if value else "MISSING"
    if isinstance(value, (list, tuple)):
        return f"items={len(value)}"
    if value is None:
        return "MISSING"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        return value.strip() or "MISSING"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _source_status(source_key: str, value: object) -> CanonicalStatus:
    if value is None or value == {} or value == []:
        return CanonicalStatus.LIMITED
    if isinstance(value, Mapping):
        declared = _declared_mapping_status(value)
        if source_key == "data_quality_pit_safety":
            projected = _projected_data_quality_status(value)
            return _aggregate_status(
                tuple(status for status in (declared, projected) if status is not None)
            )
        return declared or CanonicalStatus.LIMITED
    return CanonicalStatus.LIMITED


def _declared_mapping_status(
    value: Mapping[str, object],
) -> CanonicalStatus | None:
    statuses = tuple(
        _state_status(candidate)
        for key in _DECLARED_STATUS_KEYS
        if isinstance((candidate := value.get(key)), str) and candidate.strip()
    )
    return _aggregate_status(statuses) if statuses else None


def _state_status(value: object) -> CanonicalStatus:
    state = _leading_status(value).upper()
    if state in _BLOCKED_STATES | {"REVIEW_REQUIRED"} or state.startswith(
        ("BLOCKED", "FAIL", "ERROR", "UNSAFE")
    ):
        return CanonicalStatus.BLOCKED
    if state in _PASS_STATES | {"SAFE", "VALID"} or state.startswith("PASS"):
        return CanonicalStatus.PASS
    if state in _LIMITED_STATES or not state:
        return CanonicalStatus.LIMITED
    return CanonicalStatus.LIMITED


def _projected_data_quality_status(value: Mapping[str, object]) -> CanonicalStatus:
    return _aggregate_status(
        tuple(
            _state_status(value.get(key))
            for key in (
                "data_gate_status",
                "future_data_check",
                "market_data_status",
            )
        )
    )


def _aggregate_status(
    statuses: tuple[CanonicalStatus, ...],
) -> CanonicalStatus:
    if any(item is CanonicalStatus.BLOCKED for item in statuses):
        return CanonicalStatus.BLOCKED
    if any(item is CanonicalStatus.LIMITED for item in statuses):
        return CanonicalStatus.LIMITED
    return CanonicalStatus.PASS


def _section_summary(
    *,
    section_id: str,
    facts: tuple[tuple[str, str], ...],
    statuses: tuple[CanonicalStatus, ...],
    aggregate_status: CanonicalStatus,
) -> str:
    placeholders = {"AVAILABLE", "LIMITED", "MISSING", "NOT_AVAILABLE", "UNKNOWN"}
    return next(
        (
            value
            for (_, value), source_status in zip(facts, statuses, strict=True)
            if source_status is aggregate_status and value not in placeholders
        ),
        f"{section_id}: {aggregate_status.value}",
    )


__all__ = [
    "DATA_QUALITY_AND_PIT_SECTION_ID",
    "DATA_QUALITY_AND_PIT_SOURCE_KEYS",
    "project_data_quality_pit_safety",
    "provide_data_quality_and_pit_section",
]
