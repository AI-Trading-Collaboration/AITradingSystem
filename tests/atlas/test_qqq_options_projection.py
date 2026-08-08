from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas.qqq_options_projection import (
    QQQOptionsProjectionError,
    build_qqq_options_projection,
    load_qqq_options_projection_policy,
    validate_qqq_options_projection,
)
from ai_trading_system.contracts.strategy_research_qqq_options_projection import (
    QQQ_OPTIONS_PROJECTION_GROUP_IDS,
    QQQ_OPTIONS_PROJECTION_TASK_IDS,
    QQQOptionsProjectionContractError,
    StrategyResearchQQQOptionsProjectionBundle,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_ID = "a" * 64


def test_projection_replays_exact_owner_accepted_authority() -> None:
    policy = load_qqq_options_projection_policy(repository_root=PROJECT_ROOT)
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
        policy=policy,
    )
    validation = validate_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        bundle=bundle,
        policy=policy,
    )

    assert validation.status == "PASS"
    assert tuple(item.group_id for item in bundle.groups) == QQQ_OPTIONS_PROJECTION_GROUP_IDS
    assert tuple(item.task_id for item in bundle.cards) == QQQ_OPTIONS_PROJECTION_TASK_IDS
    assert [item.layer.value for item in bundle.cards] == [
        "A",
        "A",
        "A",
        "B",
        "C",
        "C",
        "C",
        "C",
        "B",
        "B",
        "B",
        "A",
        "A",
    ]
    assert bundle.primary_research_start == "2021-02-22"
    assert bundle.aggregate_conclusion == "NO_GO_KEEP_BLOCKED"
    assert bundle.investment_conclusion_generated is False
    assert bundle.external_action == bundle.production_effect == bundle.broker_action == "none"
    assert (
        StrategyResearchQQQOptionsProjectionBundle.from_json_bytes(bundle.canonical_bytes) == bundle
    )


def test_projection_exposes_reader_first_five_part_copy_for_every_card() -> None:
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
    )
    for card in bundle.cards:
        assert card.positioning_zh
        assert card.completed_zh
        assert card.not_proven_zh
        assert card.blocker_zh
        assert card.next_reader_action_zh
        assert card.status_layers.strategy_conclusion != "PASS"


def test_projection_freezes_2492_reader_order_and_2493_dominance() -> None:
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
    )
    by_task = {item.task_id: item for item in bundle.cards}
    assert by_task["TRADING-2492"].priority_facts == (
        "PILOT_NO_GO_LICENSE_OR_EVIDENCE",
        "唯一 scope violation 是 PROCESSED_DATA_POINTS",
        "734127 > 250000",
        "1 order / 1 fill",
    )
    assert by_task["TRADING-2493"].priority_facts[:3] == (
        "NO_GO_KEEP_BLOCKED",
        "SIGNED_NO_GO",
        "subordinate capability/technical axes are CONDITIONAL_GO only",
    )
    assert by_task["TRADING-2489"].source_status_note == ("SOURCE_STATUS_MISMATCH_REVIEW_REQUIRED")


def test_projection_rejects_source_identity_drift() -> None:
    policy = load_qqq_options_projection_policy(repository_root=PROJECT_ROOT)
    first = policy.cards[0]
    tampered = replace(first, source=replace(first.source, byte_count=first.source.byte_count + 1))
    invalid_policy = replace(policy, cards=(tampered, *policy.cards[1:]))
    with pytest.raises(
        QQQOptionsProjectionError,
        match="SOURCE_BYTE_COUNT_DRIFT",
    ):
        build_qqq_options_projection(
            repository_root=PROJECT_ROOT,
            snapshot_id=SNAPSHOT_ID,
            policy=invalid_policy,
        )


def test_projection_rejects_policy_drift_after_seal() -> None:
    policy = load_qqq_options_projection_policy(repository_root=PROJECT_ROOT)
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
        policy=policy,
    )
    changed = replace(
        policy.cards[0],
        completed_zh=policy.cards[0].completed_zh + " 未经重新审阅的文本。",
    )
    changed_policy = replace(policy, cards=(changed, *policy.cards[1:]))
    with pytest.raises(QQQOptionsProjectionError, match="POLICY_DRIFT"):
        validate_qqq_options_projection(
            repository_root=PROJECT_ROOT,
            bundle=bundle,
            policy=changed_policy,
        )


def test_projection_rejects_noncanonical_json() -> None:
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
    )
    with pytest.raises(
        QQQOptionsProjectionContractError,
        match="CANONICAL_BYTES_REQUIRED",
    ):
        StrategyResearchQQQOptionsProjectionBundle.from_json_bytes(
            bundle.canonical_bytes.replace(b'"broker_action":"none"', b'"broker_action": "none"')
        )


def test_projection_rejects_strategy_pass_even_with_rehashed_payload() -> None:
    bundle = build_qqq_options_projection(
        repository_root=PROJECT_ROOT,
        snapshot_id=SNAPSHOT_ID,
    )
    with pytest.raises(
        QQQOptionsProjectionContractError,
        match="STRATEGY_PASS_PROHIBITED",
    ):
        replace(bundle.cards[0].status_layers, strategy_conclusion="PASS")
