from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
    emits_action,
)


def test_collect_unsafe_fields_keeps_nested_paths_and_action_fields() -> None:
    payload = {
        "scheduler_enabled": True,
        "nested": [
            {
                "broker_action": "send_order",
                "target_weight": {"QQQ": 1.0},
            }
        ],
    }

    violations = collect_unsafe_fields(
        payload,
        false_fields={"scheduler_enabled"},
        forbidden_emit_fields={"target_weight"},
    )

    assert violations == [
        "scheduler_enabled",
        "nested[0].broker_action",
        "nested[0].target_weight",
    ]


def test_collect_unsafe_fields_keeps_inactive_action_contract() -> None:
    payload = {
        "action": "blocked",
        "empty_list": [],
        "empty_mapping": {},
        "spaced_action": " none ",
    }

    violations = collect_unsafe_fields(
        payload,
        false_fields=set(),
        forbidden_emit_fields={"action", "empty_list", "empty_mapping", "spaced_action"},
    )

    assert violations == ["spaced_action"]
    assert emits_action("blocked") is False
    assert emits_action(" none ") is True


def test_collect_real_scheduler_creation_fields_keeps_nested_paths() -> None:
    payload = {
        "jobs": [
            {
                "windows_task_created": True,
                "cron_created": False,
            }
        ],
        "metadata": {"real_scheduler_created": True},
    }

    violations = collect_real_scheduler_creation_fields(payload)

    assert violations == [
        "jobs[0].windows_task_created",
        "metadata.real_scheduler_created",
    ]
