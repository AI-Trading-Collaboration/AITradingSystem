from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ai_trading_system.contracts.report_audit import ReportAuditIndexViewModel
from ai_trading_system.contracts.report_spec import ReportCatalogAssessment
from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic
from ai_trading_system.platform.reporting.inventory import (
    ReportingArchitecturePolicy,
    load_reporting_architecture_policy,
)


def build_report_audit_index(
    catalog: ReportCatalogAssessment,
    *,
    generated_at: datetime,
    policy: ReportingArchitecturePolicy | None = None,
) -> ReportAuditIndexViewModel:
    resolved_policy = policy or load_reporting_architecture_policy()
    return ReportAuditIndexViewModel(
        policy_id=resolved_policy.policy_id,
        generated_at=generated_at,
        catalog=catalog,
        include_all_registry_entries=resolved_policy.audit_include_all_registry_entries,
        include_legacy_unclassified=resolved_policy.audit_include_legacy_unclassified,
    )


def render_report_audit_index_markdown(view: ReportAuditIndexViewModel) -> str:
    payload = view.to_dict()
    return "\n".join(
        [
            "# Report Audit Index",
            "",
            f"- status: `{view.status.value}`",
            f"- source: `{view.catalog.source_path}`",
            f"- entry_count: `{payload['entry_count']}`",
            f"- typed_count: `{payload['typed_count']}`",
            f"- limited_count: `{payload['limited_count']}`",
            f"- blocked_count: `{payload['blocked_count']}`",
            "- include_all_registry_entries: `true`",
            "- include_legacy_unclassified: `true`",
            "- production_effect: `none`",
            "",
            "> legacy unclassified条目保留为LIMITED，不静默推断tier/actionability。",
            "",
        ]
    )


def write_report_audit_index(
    view: ReportAuditIndexViewModel,
    *,
    output_dir: Path,
) -> tuple[Path, Path]:
    json_path = output_dir / "report_audit_index.json"
    markdown_path = output_dir / "report_audit_index.md"
    write_json_atomic(json_path, view.to_dict())
    write_text_atomic(markdown_path, render_report_audit_index_markdown(view))
    return json_path, markdown_path


__all__ = [
    "build_report_audit_index",
    "render_report_audit_index_markdown",
    "write_report_audit_index",
]
