from __future__ import annotations

import html
import json
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.contracts.report_spec import (
    OwnerActionItem,
    OwnerDailyBriefViewModel,
    ReaderTier,
    ReportSectionSpec,
    ReportSectionViewModel,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import EntrypointRef
from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic
from ai_trading_system.platform.reporting.inventory import (
    ReportingArchitecturePolicy,
    load_reporting_architecture_policy,
)
from ai_trading_system.platform.reporting.reader_brief_native import (
    DATA_QUALITY_AND_PIT_SECTION_ID,
    provide_data_quality_and_pit_section,
)

_PASS_STATES = {"PASS", "READY", "AVAILABLE", "OK", "COMPLETE", "DONE"}
_BLOCKED_STATES = {"BLOCKED", "FAILED", "FAIL", "ERROR", "UNSAFE"}


def build_owner_daily_brief_view_model(
    legacy_payload: Mapping[str, object],
    *,
    policy: ReportingArchitecturePolicy | None = None,
    generated_at: datetime | None = None,
) -> OwnerDailyBriefViewModel:
    resolved_policy = policy or load_reporting_architecture_policy()
    specs = tuple(
        _section_spec(
            section_id=item.section_id,
            source_keys=item.source_keys,
            order=index,
        )
        for index, item in enumerate(resolved_policy.core_sections, start=1)
    )
    sections = tuple(_section_provider(spec)(legacy_payload, spec=spec) for spec in specs)
    queue = _typed_owner_queue(legacy_payload.get("owner_action_queue"))
    timestamp = generated_at or _payload_generated_at(legacy_payload)
    status = _aggregate_status(tuple(item.status for item in sections))
    return OwnerDailyBriefViewModel(
        policy_id=resolved_policy.policy_id,
        as_of=date.fromisoformat(str(legacy_payload.get("as_of", ""))),
        generated_at=timestamp,
        status=status,
        sections=sections,
        owner_queue=queue,
    )


def render_owner_daily_brief_html(view: OwnerDailyBriefViewModel) -> str:
    parts = [
        "<!doctype html>",
        '<html lang="zh-CN"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>Owner Daily Brief {html.escape(view.as_of.isoformat())}</title>",
        f"<style>{_css()}</style></head><body><main>",
        f"<header><p>Owner Daily Brief</p><h1>{html.escape(view.as_of.isoformat())}</h1>",
        f"<strong>{html.escape(view.status.value)}</strong></header>",
    ]
    for section in view.sections:
        facts = "".join(
            f"<dt>{html.escape(key)}</dt><dd>{html.escape(value)}</dd>"
            for key, value in section.facts
        )
        caveats = "".join(f"<li>{html.escape(item)}</li>" for item in section.caveats)
        parts.append(
            f'<section data-section-id="{html.escape(section.section_id)}">'
            f"<h2>{html.escape(section.title)}</h2>"
            f'<p class="status">{html.escape(section.status.value)}</p>'
            f"<p>{html.escape(section.summary)}</p><dl>{facts}</dl>"
            + (f"<ul>{caveats}</ul>" if caveats else "")
            + "</section>"
        )
    queue_rows = "".join(
        f"<li><strong>{html.escape(item.title)}</strong> — "
        f"{html.escape(item.owner_action)}</li>"
        for item in view.owner_queue
    )
    parts.extend(
        [
            '<aside data-owner-queue="true"><h2>Owner Action Queue</h2>',
            f"<ol>{queue_rows}</ol>" if queue_rows else "<p>当前无typed due + actionable事项。</p>",
            "</aside>",
            "<footer>production_effect=none；本报告只读投影既有结论。</footer>",
            "</main></body></html>",
        ]
    )
    return "\n".join(parts) + "\n"


def default_owner_daily_brief_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_daily_brief_{as_of.isoformat()}.json"


def default_owner_daily_brief_html_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"owner_daily_brief_{as_of.isoformat()}.html"


def write_owner_daily_brief_sidecars(
    legacy_payload: Mapping[str, object],
    *,
    output_dir: Path,
    policy: ReportingArchitecturePolicy | None = None,
) -> tuple[Path, Path]:
    view = build_owner_daily_brief_view_model(legacy_payload, policy=policy)
    json_path = default_owner_daily_brief_json_path(output_dir, view.as_of)
    html_path = default_owner_daily_brief_html_path(output_dir, view.as_of)
    write_json_atomic(json_path, view.to_dict())
    write_text_atomic(html_path, render_owner_daily_brief_html(view))
    return json_path, html_path


def _section_spec(
    *, section_id: str, source_keys: tuple[str, ...], order: int
) -> ReportSectionSpec:
    provider = (
        EntrypointRef(
            module="ai_trading_system.platform.reporting.reader_brief_native",
            callable_name="provide_data_quality_and_pit_section",
        )
        if section_id == DATA_QUALITY_AND_PIT_SECTION_ID
        else EntrypointRef(
            module="ai_trading_system.platform.reporting.owner_daily",
            callable_name="_provide_legacy_payload_section",
        )
    )
    return ReportSectionSpec(
        section_id=section_id,
        title=section_id.replace("_", " ").title(),
        owner="reporting_governance",
        reader_tier=ReaderTier.OWNER_DAILY_BRIEF,
        provider=provider,
        provider_version="1.0.0",
        source_keys=source_keys,
        core_order=order,
    )


def _section_provider(
    spec: ReportSectionSpec,
) -> Callable[..., ReportSectionViewModel]:
    if spec.section_id == DATA_QUALITY_AND_PIT_SECTION_ID:
        return provide_data_quality_and_pit_section
    return _provide_legacy_payload_section


def _provide_legacy_payload_section(
    payload: Mapping[str, object], *, spec: ReportSectionSpec
) -> ReportSectionViewModel:
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
        facts.append((key, _source_display_value(value)))
        statuses.append(_source_status(value))
    status = _aggregate_status(tuple(statuses))
    summary = next(
        (value for _, value in facts if value not in {"MISSING", "AVAILABLE", "UNKNOWN"}),
        f"{spec.section_id}: {status.value}",
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


def _typed_owner_queue(value: object) -> tuple[OwnerActionItem, ...]:
    if not isinstance(value, list):
        return ()
    eligible: list[OwnerActionItem] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        try:
            item = OwnerActionItem.from_dict(raw)
        except (ValueError, TypeError):
            continue
        if item.eligible_for_owner_queue:
            eligible.append(item)
    return tuple(sorted(eligible, key=lambda item: (item.priority, item.action_id)))


def _payload_generated_at(payload: Mapping[str, object]) -> datetime:
    raw = str(payload.get("generated_at", ""))
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(tz=UTC)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return datetime.now(tz=UTC)
    return parsed


def _source_display_value(value: object) -> str:
    if isinstance(value, Mapping):
        for key in (
            "summary_sentence",
            "today_conclusion",
            "recommended_action",
            "summary",
            "status",
            "availability",
        ):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return "AVAILABLE" if value else "MISSING"
    if isinstance(value, list | tuple):
        return f"items={len(value)}"
    if value is None:
        return "MISSING"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        return value.strip() or "MISSING"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _source_status(value: object) -> CanonicalStatus:
    if value is None or value == {} or value == []:
        return CanonicalStatus.LIMITED
    if isinstance(value, Mapping):
        state = str(value.get("status") or value.get("availability") or "").upper()
        if state in _BLOCKED_STATES or state.startswith("BLOCKED"):
            return CanonicalStatus.BLOCKED
        if state in _PASS_STATES or state.startswith("PASS"):
            return CanonicalStatus.PASS
        if state in {"MISSING", "LIMITED", "UNKNOWN"}:
            return CanonicalStatus.LIMITED
        return CanonicalStatus.PASS
    return CanonicalStatus.PASS


def _aggregate_status(statuses: tuple[CanonicalStatus, ...]) -> CanonicalStatus:
    if any(item is CanonicalStatus.BLOCKED for item in statuses):
        return CanonicalStatus.BLOCKED
    if any(item is CanonicalStatus.LIMITED for item in statuses):
        return CanonicalStatus.LIMITED
    return CanonicalStatus.PASS


def _css() -> str:
    return """
body{font-family:system-ui,sans-serif;margin:0;background:#f5f7fa;color:#172033}
main{max-width:1040px;margin:auto;padding:24px}
header,section,aside{background:white;border:1px solid #dce3ec;border-radius:12px;
padding:18px;margin:12px 0}
h1,h2{margin-top:0}.status{font-weight:700}dl{display:grid;grid-template-columns:220px 1fr;gap:6px}
dt{font-weight:600}dd{margin:0;overflow-wrap:anywhere}footer{color:#526174;padding:12px}
""".strip()
