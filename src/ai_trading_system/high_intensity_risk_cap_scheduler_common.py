from __future__ import annotations

from collections.abc import Collection, Mapping

DEFAULT_REAL_SCHEDULER_FIELDS = frozenset(
    {
        "external_scheduler_entry_created",
        "real_scheduler_created",
        "cron_created",
        "cron_entry_created",
        "windows_task_created",
        "github_actions_schedule_created",
        "github_action_schedule_created",
        "daily_scheduler_entry_created",
    }
)

DEFAULT_INACTIVE_ACTION_STRINGS = frozenset(
    {
        "",
        "none",
        "false",
        "not_applicable",
        "blocked",
    }
)


def collect_unsafe_fields(
    value: object,
    *,
    false_fields: Collection[str],
    forbidden_emit_fields: Collection[str],
    prefix: str = "",
    inactive_action_strings: Collection[str] = DEFAULT_INACTIVE_ACTION_STRINGS,
) -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in false_fields and item is True:
                violations.append(path)
            if key_text == "broker_action" and str(item).lower() not in {"", "none"}:
                violations.append(path)
            if key_text in forbidden_emit_fields and emits_action(
                item,
                inactive_action_strings=inactive_action_strings,
            ):
                violations.append(path)
            violations.extend(
                collect_unsafe_fields(
                    item,
                    false_fields=false_fields,
                    forbidden_emit_fields=forbidden_emit_fields,
                    prefix=path,
                    inactive_action_strings=inactive_action_strings,
                )
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                collect_unsafe_fields(
                    item,
                    false_fields=false_fields,
                    forbidden_emit_fields=forbidden_emit_fields,
                    prefix=f"{prefix}[{index}]",
                    inactive_action_strings=inactive_action_strings,
                )
            )
    return violations


def collect_real_scheduler_creation_fields(
    value: object,
    *,
    prefix: str = "",
    real_scheduler_fields: Collection[str] = DEFAULT_REAL_SCHEDULER_FIELDS,
) -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in real_scheduler_fields and item is True:
                violations.append(path)
            violations.extend(
                collect_real_scheduler_creation_fields(
                    item,
                    prefix=path,
                    real_scheduler_fields=real_scheduler_fields,
                )
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                collect_real_scheduler_creation_fields(
                    item,
                    prefix=f"{prefix}[{index}]",
                    real_scheduler_fields=real_scheduler_fields,
                )
            )
    return violations


def emits_action(
    value: object,
    *,
    inactive_action_strings: Collection[str] = DEFAULT_INACTIVE_ACTION_STRINGS,
) -> bool:
    if value is False or value is None or value == "" or value == [] or value == {}:
        return False
    if isinstance(value, str):
        return value.lower() not in inactive_action_strings
    return True
