from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_reader_state import (
    ReaderChangeContext,
    ReaderChangeKind,
    ReaderDateContext,
    ReaderStateContractError,
    ReaderStateKind,
    ReaderStateProjection,
)

DEFAULT_READER_STATE_SEMANTICS_PATH = "config/atlas/reader_state_semantics.yaml"
READER_STATE_SEMANTICS_SCHEMA = "atlas_reader_state_semantics.v1"


class ReaderStateProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class ReaderStateMapping:
    raw_status: str
    reader_state: ReaderStateKind
    reader_label_zh: str


@dataclass(frozen=True)
class ReaderStateSemanticsPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    policy_sha256: str
    state_mappings: tuple[ReaderStateMapping, ...]
    date_fields: tuple[str, ...]
    comparison_states: tuple[ReaderChangeKind, ...]
    source_binding: Mapping[str, object]
    safety: Mapping[str, object]

    def __post_init__(self) -> None:
        if self.status != "REVIEWED_ENGINEERING_BASELINE":
            raise ReaderStateProjectionError("READER_STATE_POLICY_STATUS_INVALID")
        if not re.fullmatch(r"[0-9a-f]{64}", self.policy_sha256):
            raise ReaderStateProjectionError("READER_STATE_POLICY_SHA256_INVALID")
        raw_statuses = tuple(item.raw_status for item in self.state_mappings)
        if len(raw_statuses) != len(set(raw_statuses)) or not raw_statuses:
            raise ReaderStateProjectionError("READER_STATE_MAPPING_SET_INVALID")
        if self.date_fields != (
            "data_as_of",
            "evidence_evaluated_at",
            "page_generated_at",
        ):
            raise ReaderStateProjectionError("READER_STATE_DATE_FIELD_SET_INVALID")
        if self.comparison_states != tuple(ReaderChangeKind):
            raise ReaderStateProjectionError("READER_STATE_CHANGE_SET_INVALID")
        expected_binding = {
            "source_refs_required": True,
            "comparison_base_identity_required": True,
            "timezone_required_for_datetime": True,
            "null_date_status": "UNKNOWN",
        }
        if dict(self.source_binding) != expected_binding:
            raise ReaderStateProjectionError("READER_STATE_SOURCE_BINDING_INVALID")
        expected_safety: dict[str, object] = {
            "primary_research_start": "2021-02-22",
            "raw_enum_rewrite_allowed": False,
            "limited_to_pass_upgrade_allowed": False,
            "engineering_to_strategy_upgrade_allowed": False,
            "importance_cutoff_introduced": False,
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        if dict(self.safety) != expected_safety:
            raise ReaderStateProjectionError("READER_STATE_SAFETY_INVALID")

    @property
    def mapping_by_raw_status(self) -> Mapping[str, ReaderStateMapping]:
        return {item.raw_status: item for item in self.state_mappings}


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReaderStateProjectionError(f"READER_STATE_MAPPING_REQUIRED:{field}")
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ReaderStateProjectionError(f"READER_STATE_LIST_REQUIRED:{field}")
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result) or len(result) != len(set(result)):
        raise ReaderStateProjectionError(f"READER_STATE_LIST_INVALID:{field}")
    return result


def load_reader_state_semantics(
    *,
    repository_root: Path,
    policy_path: str = DEFAULT_READER_STATE_SEMANTICS_PATH,
) -> ReaderStateSemanticsPolicy:
    root = repository_root.resolve()
    normalized = policy_path.replace("\\", "/")
    if normalized.startswith("/") or ".." in normalized.split("/"):
        raise ReaderStateProjectionError("READER_STATE_POLICY_PATH_INVALID")
    path = (root / normalized).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ReaderStateProjectionError("READER_STATE_POLICY_PATH_OUTSIDE_REPOSITORY") from exc
    raw = path.read_bytes()
    try:
        payload = _mapping(yaml.safe_load(raw.decode("utf-8")), "policy")
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ReaderStateProjectionError("READER_STATE_POLICY_YAML_INVALID") from exc
    expected = {
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "owner",
        "state_mappings",
        "date_fields",
        "comparison_states",
        "source_binding",
        "safety",
    }
    if set(payload) != expected or payload["schema_version"] != READER_STATE_SEMANTICS_SCHEMA:
        raise ReaderStateProjectionError("READER_STATE_POLICY_SCHEMA_INVALID")
    rows = payload["state_mappings"]
    if not isinstance(rows, list):
        raise ReaderStateProjectionError("READER_STATE_MAPPING_LIST_REQUIRED")
    mappings: list[ReaderStateMapping] = []
    for row in rows:
        item = _mapping(row, "state_mapping")
        if set(item) != {"raw_status", "reader_state", "reader_label_zh"}:
            raise ReaderStateProjectionError("READER_STATE_MAPPING_KEYS_INVALID")
        mappings.append(
            ReaderStateMapping(
                raw_status=str(item["raw_status"]),
                reader_state=ReaderStateKind(str(item["reader_state"])),
                reader_label_zh=str(item["reader_label_zh"]),
            )
        )
    return ReaderStateSemanticsPolicy(
        policy_id=str(payload["policy_id"]),
        policy_version=str(payload["policy_version"]),
        status=str(payload["status"]),
        owner=str(payload["owner"]),
        policy_sha256=hashlib.sha256(raw).hexdigest(),
        state_mappings=tuple(mappings),
        date_fields=_string_tuple(payload["date_fields"], "date_fields"),
        comparison_states=tuple(
            ReaderChangeKind(item)
            for item in _string_tuple(payload["comparison_states"], "comparison_states")
        ),
        source_binding=_mapping(payload["source_binding"], "source_binding"),
        safety=_mapping(payload["safety"], "safety"),
    )


def project_reader_state(
    *,
    policy: ReaderStateSemanticsPolicy,
    status_object_zh: str,
    raw_status: str,
    reason_zh: str,
    data_as_of: str | None,
    evidence_evaluated_at: str | None,
    page_generated_at: str,
    next_legal_action_zh: str,
    prohibited_inference_zh: str,
    change_kind: ReaderChangeKind,
    comparison_base_id: str | None,
    comparison_base_date: str | None,
    change_explanation_zh: str,
    source_refs: tuple[str, ...],
) -> ReaderStateProjection:
    mapping = policy.mapping_by_raw_status.get(raw_status)
    if mapping is None:
        raise ReaderStateProjectionError(f"READER_STATE_RAW_STATUS_UNKNOWN:{raw_status}")
    try:
        return ReaderStateProjection(
            status_object_zh=status_object_zh,
            raw_status=raw_status,
            reader_state=mapping.reader_state,
            reader_label_zh=f"{status_object_zh}：{mapping.reader_label_zh}",
            reason_zh=reason_zh,
            dates=ReaderDateContext(
                data_as_of=data_as_of,
                evidence_evaluated_at=evidence_evaluated_at,
                page_generated_at=page_generated_at,
            ),
            next_legal_action_zh=next_legal_action_zh,
            prohibited_inference_zh=prohibited_inference_zh,
            change=ReaderChangeContext(
                change_kind=change_kind,
                comparison_base_id=comparison_base_id,
                comparison_base_date=comparison_base_date,
                explanation_zh=change_explanation_zh,
            ),
            source_refs=source_refs,
            strategy_validity_supported=False,
        )
    except ReaderStateContractError as exc:
        raise ReaderStateProjectionError(str(exc)) from exc


__all__ = [
    "DEFAULT_READER_STATE_SEMANTICS_PATH",
    "READER_STATE_SEMANTICS_SCHEMA",
    "ReaderStateMapping",
    "ReaderStateProjectionError",
    "ReaderStateSemanticsPolicy",
    "load_reader_state_semantics",
    "project_reader_state",
]
