from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from ai_trading_system.contracts import (
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffEntitySummary,
    ExplorerDiffFieldChange,
    ExplorerDiffSignificance,
    ExplorerEntityChange,
    StrategyResearchExplorerDiff,
    StrategyResearchExplorerDiffContractError,
)

BEFORE_ID = "1" * 64
AFTER_ID = "2" * 64
BEFORE_SHA = "3" * 64
AFTER_SHA = "4" * 64
AT = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)


def _change(
    *,
    entity_kind: ExplorerDiffEntityKind = ExplorerDiffEntityKind.RESULT,
    entity_id: str = "result-1",
    significance: ExplorerDiffSignificance = ExplorerDiffSignificance.SEMANTIC,
) -> ExplorerEntityChange:
    return ExplorerEntityChange.build(
        entity_kind=entity_kind,
        entity_id=entity_id,
        change_kind=ExplorerDiffChangeKind.CHANGED,
        significance=significance,
        before_sha256=BEFORE_SHA,
        after_sha256=AFTER_SHA,
        field_changes=(
            ExplorerDiffFieldChange.build(
                field_name="reader_summary",
                before_value="旧结论",
                after_value="新结论",
            ),
        ),
    )


def _summaries(
    change: ExplorerEntityChange,
) -> tuple[ExplorerDiffEntitySummary, ...]:
    result: list[ExplorerDiffEntitySummary] = []
    for kind in ExplorerDiffEntityKind:
        changed = int(kind is change.entity_kind)
        result.append(
            ExplorerDiffEntitySummary(
                entity_kind=kind,
                before_count=changed,
                after_count=changed,
                unchanged_count=0,
                added_count=0,
                removed_count=0,
                changed_count=changed,
            )
        )
    return tuple(result)


def _diff(
    *,
    change: ExplorerEntityChange | None = None,
) -> StrategyResearchExplorerDiff:
    actual_change = change or _change()
    return StrategyResearchExplorerDiff.build(
        before_snapshot_id=BEFORE_ID,
        after_snapshot_id=AFTER_ID,
        before_generated_at=AT,
        after_generated_at=AT,
        changes=(actual_change,),
        entity_summaries=_summaries(actual_change),
    )


def test_diff_contract_round_trip_is_byte_deterministic() -> None:
    diff = _diff()
    rebuilt = StrategyResearchExplorerDiff.from_dict(diff.to_dict())
    assert rebuilt == diff
    assert rebuilt.diff_id == diff.compute_diff_id()
    assert rebuilt.canonical_json_bytes() == diff.canonical_json_bytes()
    assert rebuilt.total_change_count == 1
    assert rebuilt.semantic_count == 1


def test_diff_rejects_tampered_identity_and_summary() -> None:
    payload = copy.deepcopy(_diff().to_dict())
    payload["after_snapshot_id"] = "9" * 64
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_ID_MISMATCH",
    ):
        StrategyResearchExplorerDiff.from_dict(payload)

    payload = copy.deepcopy(_diff().to_dict())
    payload["summary"]["changed_count"] = 2
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_TOP_LEVEL_SUMMARY_MISMATCH",
    ):
        StrategyResearchExplorerDiff.from_dict(payload)


def test_same_snapshot_and_empty_diff_fail_closed() -> None:
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_SAME_SNAPSHOT_FORBIDDEN",
    ):
        StrategyResearchExplorerDiff.build(
            before_snapshot_id=BEFORE_ID,
            after_snapshot_id=BEFORE_ID,
            before_generated_at=AT,
            after_generated_at=AT,
            changes=(_change(),),
            entity_summaries=_summaries(_change()),
        )

    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_EMPTY_FORBIDDEN",
    ):
        StrategyResearchExplorerDiff.build(
            before_snapshot_id=BEFORE_ID,
            after_snapshot_id=AFTER_ID,
            before_generated_at=AT,
            after_generated_at=AT,
            changes=(),
            entity_summaries=tuple(
                ExplorerDiffEntitySummary(
                    entity_kind=kind,
                    before_count=0,
                    after_count=0,
                    unchanged_count=0,
                    added_count=0,
                    removed_count=0,
                    changed_count=0,
                )
                for kind in ExplorerDiffEntityKind
            ),
        )


def test_added_removed_and_changed_transition_contracts_are_strict() -> None:
    added = ExplorerEntityChange.build(
        entity_kind=ExplorerDiffEntityKind.NODE,
        entity_id="node-added",
        change_kind=ExplorerDiffChangeKind.ADDED,
        significance=ExplorerDiffSignificance.STRUCTURAL,
        before_sha256=None,
        after_sha256=AFTER_SHA,
    )
    assert added.changed_fields == ()

    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_CHANGE_TRANSITION_INVALID",
    ):
        ExplorerEntityChange.build(
            entity_kind=ExplorerDiffEntityKind.NODE,
            entity_id="node-bad",
            change_kind=ExplorerDiffChangeKind.ADDED,
            significance=ExplorerDiffSignificance.SEMANTIC,
            before_sha256=BEFORE_SHA,
            after_sha256=AFTER_SHA,
        )


def test_lineage_only_is_limited_to_source_identity_fields() -> None:
    lineage_change = ExplorerEntityChange.build(
        entity_kind=ExplorerDiffEntityKind.SOURCE,
        entity_id="source-1",
        change_kind=ExplorerDiffChangeKind.CHANGED,
        significance=ExplorerDiffSignificance.LINEAGE_ONLY,
        before_sha256=BEFORE_SHA,
        after_sha256=AFTER_SHA,
        field_changes=(
            ExplorerDiffFieldChange.build(
                field_name="exact_commit",
                before_value="a" * 40,
                after_value="b" * 40,
            ),
        ),
    )
    assert lineage_change.significance is ExplorerDiffSignificance.LINEAGE_ONLY

    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_LINEAGE_CLASSIFICATION_INVALID",
    ):
        ExplorerEntityChange.build(
            entity_kind=ExplorerDiffEntityKind.RESULT,
            entity_id="result-bad",
            change_kind=ExplorerDiffChangeKind.CHANGED,
            significance=ExplorerDiffSignificance.LINEAGE_ONLY,
            before_sha256=BEFORE_SHA,
            after_sha256=AFTER_SHA,
            field_changes=(
                ExplorerDiffFieldChange.build(
                    field_name="reader_summary",
                    before_value="before",
                    after_value="after",
                ),
            ),
        )


def test_duplicate_entity_change_and_summary_drift_fail_closed() -> None:
    change = _change()
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_DUPLICATE_ENTITY_CHANGE",
    ):
        StrategyResearchExplorerDiff.build(
            before_snapshot_id=BEFORE_ID,
            after_snapshot_id=AFTER_ID,
            before_generated_at=AT,
            after_generated_at=AT,
            changes=(change, change),
            entity_summaries=tuple(
                ExplorerDiffEntitySummary(
                    entity_kind=kind,
                    before_count=(2 if kind is ExplorerDiffEntityKind.RESULT else 0),
                    after_count=(2 if kind is ExplorerDiffEntityKind.RESULT else 0),
                    unchanged_count=0,
                    added_count=0,
                    removed_count=0,
                    changed_count=(2 if kind is ExplorerDiffEntityKind.RESULT else 0),
                )
                for kind in ExplorerDiffEntityKind
            ),
        )

    summaries = list(_summaries(change))
    result_index = next(
        index
        for index, summary in enumerate(summaries)
        if summary.entity_kind is ExplorerDiffEntityKind.RESULT
    )
    summaries[result_index] = ExplorerDiffEntitySummary(
        entity_kind=ExplorerDiffEntityKind.RESULT,
        before_count=1,
        after_count=1,
        unchanged_count=1,
        added_count=0,
        removed_count=0,
        changed_count=0,
    )
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_SUMMARY_CHANGE_BINDING_INVALID",
    ):
        StrategyResearchExplorerDiff.build(
            before_snapshot_id=BEFORE_ID,
            after_snapshot_id=AFTER_ID,
            before_generated_at=AT,
            after_generated_at=AT,
            changes=(change,),
            entity_summaries=summaries,
        )

    duplicate_summaries = (*_summaries(change), _summaries(change)[0])
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_DUPLICATE_SUMMARY_KIND",
    ):
        StrategyResearchExplorerDiff.build(
            before_snapshot_id=BEFORE_ID,
            after_snapshot_id=AFTER_ID,
            before_generated_at=AT,
            after_generated_at=AT,
            changes=(change,),
            entity_summaries=duplicate_summaries,
        )


def test_read_only_boundary_fails_closed() -> None:
    payload = copy.deepcopy(_diff().to_dict())
    payload["commands_executed"] = True
    with pytest.raises(
        StrategyResearchExplorerDiffContractError,
        match="STRATEGY_EXPLORER_DIFF_READ_ONLY_BOUNDARY_VIOLATION",
    ):
        StrategyResearchExplorerDiff.from_dict(payload)
