from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

from ai_trading_system.contracts.research_lifecycle import (
    ResearchLifecycleRecord,
    ResearchLifecycleStage,
)
from ai_trading_system.contracts.research_review import (
    ResearchReviewItem,
    ResearchReviewPackViewModel,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic
from ai_trading_system.platform.reporting.inventory import (
    ReportingArchitecturePolicy,
    load_reporting_architecture_policy,
)


def build_research_review_pack(
    records: Iterable[ResearchLifecycleRecord],
    *,
    as_of: date,
    generated_at: datetime,
    policy: ReportingArchitecturePolicy | None = None,
) -> ResearchReviewPackViewModel:
    resolved_policy = policy or load_reporting_architecture_policy()
    items = tuple(
        sorted(
            (_review_item(record) for record in records),
            key=lambda item: (item.updated_at, item.lifecycle_id),
            reverse=True,
        )
    )
    return ResearchReviewPackViewModel(
        policy_id=resolved_policy.policy_id,
        as_of=as_of,
        generated_at=generated_at,
        status=_aggregate_status(tuple(item.status for item in items)),
        items=items,
        auto_tune_allowed=resolved_policy.research_auto_tune_allowed,
        proposal_may_equal_adoption=resolved_policy.proposal_may_equal_adoption,
    )


def render_research_review_pack_markdown(view: ResearchReviewPackViewModel) -> str:
    rows = [
        "# Research Review Pack",
        "",
        f"- as_of: `{view.as_of.isoformat()}`",
        f"- status: `{view.status.value}`",
        "- auto_tune_allowed: `false`",
        "- proposal_may_equal_adoption: `false`",
        "- production_effect: `none`",
        "",
        "|lifecycle|stage|review|validation|owner decision|status|",
        "|---|---|---|---|---|---|",
    ]
    rows.extend(
        "|{lifecycle}|{stage}|{review}|{validation}|{owner}|{status}|".format(
            lifecycle=item.lifecycle_id,
            stage=item.stage.value,
            review="-" if item.review_decision is None else item.review_decision.value,
            validation=(
                "-" if item.validation_status is None else item.validation_status.value
            ),
            owner="-" if item.owner_decision is None else item.owner_decision.value,
            status=item.status.value,
        )
        for item in view.items
    )
    rows.extend(
        [
            "",
            "> 本报告只传播lifecycle状态与证据引用；proposal不等于adoption，不自动调参。",
        ]
    )
    return "\n".join(rows) + "\n"


def write_research_review_pack(
    view: ResearchReviewPackViewModel,
    *,
    output_dir: Path,
) -> tuple[Path, Path]:
    stem = f"research_review_pack_{view.as_of.isoformat()}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    write_json_atomic(json_path, view.to_dict())
    write_text_atomic(markdown_path, render_research_review_pack_markdown(view))
    return json_path, markdown_path


def _review_item(record: ResearchLifecycleRecord) -> ResearchReviewItem:
    status = _record_status(record)
    blockers = tuple(
        sorted(
            {
                reason
                for event in record.events
                if event.to_stage
                in {
                    ResearchLifecycleStage.VALIDATION_BLOCKED,
                    ResearchLifecycleStage.VALIDATION_FAILED,
                    ResearchLifecycleStage.INVESTIGATING,
                }
                for reason in event.reason_codes
            }
        )
    )
    return ResearchReviewItem(
        lifecycle_id=record.lifecycle_id,
        owner=record.owner,
        stage=record.stage,
        updated_at=record.updated_at,
        observation_ref=record.observation_ref,
        evidence_refs=record.evidence_refs,
        status=status,
        review_decision=record.review_decision,
        preregistration_ref=(
            None
            if record.preregistration is None
            else record.preregistration.preregistration_id
        ),
        validation_status=record.validation_status,
        owner_decision=record.owner_decision,
        blocker_codes=blockers,
    )


def _record_status(record: ResearchLifecycleRecord) -> CanonicalStatus:
    if record.stage is ResearchLifecycleStage.VALIDATION_FAILED:
        return CanonicalStatus.FAILED
    if record.stage in {
        ResearchLifecycleStage.VALIDATION_BLOCKED,
        ResearchLifecycleStage.INVESTIGATING,
    }:
        return CanonicalStatus.BLOCKED
    if record.stage in {
        ResearchLifecycleStage.OBSERVATION,
        ResearchLifecycleStage.EVIDENCE_SNAPSHOT,
        ResearchLifecycleStage.CHANGE_PROPOSAL_PENDING,
        ResearchLifecycleStage.CHANGE_PROPOSAL_FROZEN,
    }:
        return CanonicalStatus.LIMITED
    return CanonicalStatus.PASS


def _aggregate_status(statuses: tuple[CanonicalStatus, ...]) -> CanonicalStatus:
    if not statuses:
        return CanonicalStatus.LIMITED
    if CanonicalStatus.FAILED in statuses:
        return CanonicalStatus.FAILED
    if CanonicalStatus.BLOCKED in statuses:
        return CanonicalStatus.BLOCKED
    if CanonicalStatus.LIMITED in statuses:
        return CanonicalStatus.LIMITED
    return CanonicalStatus.PASS


__all__ = [
    "build_research_review_pack",
    "render_research_review_pack_markdown",
    "write_research_review_pack",
]
