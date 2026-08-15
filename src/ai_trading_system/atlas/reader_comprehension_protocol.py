from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_READER_COMPREHENSION_PROTOCOL_PATH = (
    "config/atlas/reader_comprehension_protocol.yaml"
)
READER_COMPREHENSION_PROTOCOL_SCHEMA = "atlas_reader_comprehension_protocol.v1"
PENDING_OWNER_POLICY = "PENDING_OWNER_POLICY"


class ReaderComprehensionProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class ReaderComprehensionProtocol:
    protocol_id: str
    protocol_version: str
    status: str
    owner: str
    policy_sha256: str
    identity_binding_fields: tuple[str, ...]
    scenarios: tuple[str, ...]
    answer_categories: tuple[str, ...]
    owner_policy_slots: Mapping[str, str]
    recording: Mapping[str, bool]
    safety: Mapping[str, object]

    def __post_init__(self) -> None:
        if self.status != PENDING_OWNER_POLICY:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_STATUS_INVALID")
        if not re.fullmatch(r"[0-9a-f]{64}", self.policy_sha256):
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_SHA_INVALID")
        expected_identity = (
            "html_sha256",
            "source_commit",
            "manifest_sha256",
            "reader_projection_contract_sha256",
            "viewport",
            "browser",
            "operating_system",
            "assistive_technology",
        )
        if self.identity_binding_fields != expected_identity:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_IDENTITY_FIELDS_INVALID")
        expected_scenarios = (
            "CURRENT_RESEARCH_MAINLINE",
            "LARGEST_CURRENT_BLOCKER",
            "ENGINEERING_VS_RESEARCH_EVIDENCE",
            "PROHIBITED_INFERENCES",
            "NEXT_OWNER_AND_ACTION",
            "INVESTMENT_ORDER_ENGINE_AUTHORITY",
            "DATE_AND_FRESHNESS",
            "SNAPSHOT_CHANGE",
        )
        if self.scenarios != expected_scenarios:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_SCENARIO_SET_INVALID")
        if set(self.owner_policy_slots.values()) != {PENDING_OWNER_POLICY}:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_OWNER_POLICY_PREEMPTED")
        expected_recording = {
            "verbatim_required": True,
            "first_path_required": True,
            "first_misunderstanding_required": True,
            "unknown_terms_required": True,
            "two_independent_reviewers_required": True,
            "disagreement_log_required": True,
            "identity_change_requires_new_round": True,
        }
        if dict(self.recording) != expected_recording:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_RECORDING_INVALID")
        expected_safety: dict[str, object] = {
            "participant_recruitment_authorized": False,
            "pilot_execution_authorized": False,
            "automated_pass_allowed": False,
            "investment_conclusion_generated": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        if dict(self.safety) != expected_safety:
            raise ReaderComprehensionProtocolError("READER_PROTOCOL_SAFETY_INVALID")


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReaderComprehensionProtocolError(f"READER_PROTOCOL_MAPPING_REQUIRED:{field}")
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ReaderComprehensionProtocolError(f"READER_PROTOCOL_LIST_REQUIRED:{field}")
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result) or len(result) != len(set(result)):
        raise ReaderComprehensionProtocolError(f"READER_PROTOCOL_LIST_INVALID:{field}")
    return result


def load_reader_comprehension_protocol(
    *,
    repository_root: Path,
    protocol_path: str = DEFAULT_READER_COMPREHENSION_PROTOCOL_PATH,
) -> ReaderComprehensionProtocol:
    root = repository_root.resolve()
    normalized = protocol_path.replace("\\", "/")
    if normalized.startswith("/") or ".." in normalized.split("/"):
        raise ReaderComprehensionProtocolError("READER_PROTOCOL_PATH_INVALID")
    selected = (root / normalized).resolve()
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise ReaderComprehensionProtocolError("READER_PROTOCOL_PATH_OUTSIDE_REPOSITORY") from exc
    raw = selected.read_bytes()
    try:
        payload = _mapping(yaml.safe_load(raw.decode("utf-8")), "protocol")
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ReaderComprehensionProtocolError("READER_PROTOCOL_YAML_INVALID") from exc
    expected = {
        "schema_version",
        "protocol_id",
        "protocol_version",
        "status",
        "owner",
        "identity_binding_fields",
        "scenarios",
        "answer_categories",
        "owner_policy_slots",
        "recording",
        "safety",
    }
    if (
        set(payload) != expected
        or payload["schema_version"] != READER_COMPREHENSION_PROTOCOL_SCHEMA
    ):
        raise ReaderComprehensionProtocolError("READER_PROTOCOL_SCHEMA_INVALID")
    slots = _mapping(payload["owner_policy_slots"], "owner_policy_slots")
    recording_payload = _mapping(payload["recording"], "recording")
    if not all(isinstance(value, bool) for value in recording_payload.values()):
        raise ReaderComprehensionProtocolError("READER_PROTOCOL_RECORDING_BOOLEAN_REQUIRED")
    return ReaderComprehensionProtocol(
        protocol_id=str(payload["protocol_id"]),
        protocol_version=str(payload["protocol_version"]),
        status=str(payload["status"]),
        owner=str(payload["owner"]),
        policy_sha256=hashlib.sha256(raw).hexdigest(),
        identity_binding_fields=_string_tuple(
            payload["identity_binding_fields"], "identity_binding_fields"
        ),
        scenarios=_string_tuple(payload["scenarios"], "scenarios"),
        answer_categories=_string_tuple(payload["answer_categories"], "answer_categories"),
        owner_policy_slots={str(key): str(value) for key, value in slots.items()},
        recording={str(key): bool(value) for key, value in recording_payload.items()},
        safety=_mapping(payload["safety"], "safety"),
    )


__all__ = [
    "DEFAULT_READER_COMPREHENSION_PROTOCOL_PATH",
    "PENDING_OWNER_POLICY",
    "READER_COMPREHENSION_PROTOCOL_SCHEMA",
    "ReaderComprehensionProtocol",
    "ReaderComprehensionProtocolError",
    "load_reader_comprehension_protocol",
]
