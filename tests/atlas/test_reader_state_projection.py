from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.atlas.reader_state_projection import (
    ReaderStateProjectionError,
    load_reader_state_semantics,
    project_reader_state,
)
from ai_trading_system.contracts.strategy_research_reader_state import (
    ReaderChangeKind,
    ReaderStateProjection,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _project(raw_status: str = "VALIDATED") -> ReaderStateProjection:
    return project_reader_state(
        policy=load_reader_state_semantics(repository_root=PROJECT_ROOT),
        status_object_zh="页面工程校验",
        raw_status=raw_status,
        reason_zh="结构、来源与安全边界满足当前工程合同。",
        data_as_of="2026-08-14",
        evidence_evaluated_at="2026-08-15T08:00:00+09:00",
        page_generated_at="2026-08-16T02:00:00+09:00",
        next_legal_action_zh="继续 Owner 视觉验收。",
        prohibited_inference_zh="不能据此推出策略有效或允许下单。",
        change_kind=ReaderChangeKind.CHANGED,
        comparison_base_id="snapshot-before",
        comparison_base_date="2026-08-15",
        change_explanation_zh="页面新增 why-first reader projection。",
        source_refs=("outputs/atlas/strategy_research_cited_query/manifest.json",),
    )


def test_reader_state_projection_binds_object_dates_change_and_safety() -> None:
    projection = _project()

    assert projection.raw_status == "VALIDATED"
    assert projection.reader_label_zh == "页面工程校验：当前对象工程校验通过"
    assert projection.dates.data_as_of == "2026-08-14"
    assert projection.change.comparison_base_id == "snapshot-before"
    assert projection.strategy_validity_supported is False
    assert len(projection.content_sha256) == 64


@pytest.mark.parametrize("raw_status", ["LIMITED", "PASS", "ANSWERED", "CURRENT"])
def test_reader_state_projection_never_silently_upgrades_strategy(raw_status: str) -> None:
    projection = _project(raw_status)

    assert projection.strategy_validity_supported is False
    assert "不能据此推出策略有效" in projection.prohibited_inference_zh


def test_reader_state_projection_rejects_unknown_raw_status() -> None:
    with pytest.raises(ReaderStateProjectionError, match="READER_STATE_RAW_STATUS_UNKNOWN"):
        _project("MAGIC_SUCCESS")


def test_reader_state_projection_rejects_naive_datetime() -> None:
    policy = load_reader_state_semantics(repository_root=PROJECT_ROOT)

    with pytest.raises(ReaderStateProjectionError, match="READER_STATE_DATE_INVALID"):
        project_reader_state(
            policy=policy,
            status_object_zh="页面工程校验",
            raw_status="PASS",
            reason_zh="仅用于 negative case。",
            data_as_of="2026-08-14",
            evidence_evaluated_at="2026-08-15T08:00:00",
            page_generated_at="2026-08-16T02:00:00+09:00",
            next_legal_action_zh="停止并补齐时区。",
            prohibited_inference_zh="不能推出策略有效。",
            change_kind=ReaderChangeKind.UNKNOWN,
            comparison_base_id=None,
            comparison_base_date=None,
            change_explanation_zh="比较基线未知。",
            source_refs=("source.json",),
        )


def test_reader_state_projection_requires_comparison_identity() -> None:
    policy = load_reader_state_semantics(repository_root=PROJECT_ROOT)

    with pytest.raises(ReaderStateProjectionError, match="READER_STATE_REQUIRED"):
        project_reader_state(
            policy=policy,
            status_object_zh="页面工程校验",
            raw_status="PASS",
            reason_zh="仅用于 negative case。",
            data_as_of=None,
            evidence_evaluated_at=None,
            page_generated_at="2026-08-16",
            next_legal_action_zh="停止并补齐比较身份。",
            prohibited_inference_zh="不能推出策略有效。",
            change_kind=ReaderChangeKind.CHANGED,
            comparison_base_id=None,
            comparison_base_date=None,
            change_explanation_zh="声称变化但缺少基线。",
            source_refs=("source.json",),
        )
