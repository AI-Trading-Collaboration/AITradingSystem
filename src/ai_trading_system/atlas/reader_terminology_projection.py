from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_reader_terminology import (
    ReaderProfile,
    ReaderTermDefinition,
)

DEFAULT_READER_PROFILE_PATH = "config/atlas/reader_profile.yaml"
DEFAULT_READER_TERMINOLOGY_PATH = "config/atlas/reader_terminology.yaml"
READER_PROFILE_SCHEMA = "atlas_reader_profile.v1"
READER_TERMINOLOGY_POLICY_SCHEMA = "atlas_reader_terminology_policy.v1"
_EXPECTED_UNKNOWN_PATTERNS = (
    "TASK_ID",
    "UPPER_SNAKE_IDENTIFIER",
    "GIT_OR_CONTENT_HASH",
    "REPOSITORY_PATH",
    "RUNTIME_COMPOSED_IDENTIFIER",
)
_EXPECTED_EXCLUSIONS = (
    "head/style",
    "head/script",
    "body/script",
    "body/template",
)
_EXPECTED_SAFETY: Mapping[str, object] = {
    "primary_research_start": "2021-02-22",
    "unknown_terms_fail_closed": True,
    "hover_only_explanation_allowed": False,
    "investment_conclusion_generated": False,
    "order_authorized": False,
    "real_engine_authorized": False,
    "production_effect": "none",
    "broker_action": "none",
}
_EXPECTED_PROFILE_SAFETY: Mapping[str, object] = {
    "primary_research_start": "2021-02-22",
    "investment_conclusion_generated": False,
    "order_authorized": False,
    "real_engine_authorized": False,
    "production_effect": "none",
    "broker_action": "none",
}


class ReaderTerminologyProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class ReaderTerminologyPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    reader_profile: ReaderProfile
    reader_profile_sha256: str
    terminology_policy_sha256: str
    unknown_identifier_patterns: tuple[str, ...]
    excluded_non_reader_regions: tuple[str, ...]
    raw_reader_replacements: tuple[tuple[str, str], ...]
    terms: tuple[ReaderTermDefinition, ...]
    safety: Mapping[str, object]

    def __post_init__(self) -> None:
        term_ids = tuple(item.term_id for item in self.terms)
        if not term_ids or len(term_ids) != len(set(term_ids)):
            raise ReaderTerminologyProjectionError(
                "READER_TERMINOLOGY_TERM_ID_SET_INVALID"
            )
        alias_owners: dict[str, str] = {}
        aliases: set[str] = set()
        for term in self.terms:
            for alias in term.aliases:
                aliases.add(alias)
                normalized_alias = alias.casefold()
                owner = alias_owners.setdefault(normalized_alias, term.term_id)
                if owner != term.term_id:
                    raise ReaderTerminologyProjectionError(
                        f"READER_TERMINOLOGY_ALIAS_AMBIGUOUS:{alias}:{owner}:{term.term_id}"
                    )
        replacement_aliases = tuple(item[0] for item in self.raw_reader_replacements)
        if (
            len(replacement_aliases) != len(set(replacement_aliases))
            or any(alias not in aliases for alias in replacement_aliases)
            or any(not replacement.strip() for _, replacement in self.raw_reader_replacements)
        ):
            raise ReaderTerminologyProjectionError(
                "READER_TERMINOLOGY_RAW_REPLACEMENT_SET_INVALID"
            )
        if self.unknown_identifier_patterns != _EXPECTED_UNKNOWN_PATTERNS:
            raise ReaderTerminologyProjectionError(
                "READER_TERMINOLOGY_UNKNOWN_PATTERN_SET_INVALID"
            )
        if self.excluded_non_reader_regions != _EXPECTED_EXCLUSIONS:
            raise ReaderTerminologyProjectionError(
                "READER_TERMINOLOGY_EXCLUSION_SET_INVALID"
            )
        if dict(self.safety) != dict(_EXPECTED_SAFETY):
            raise ReaderTerminologyProjectionError("READER_TERMINOLOGY_SAFETY_INVALID")
        for field, value in (
            ("reader_profile_sha256", self.reader_profile_sha256),
            ("terminology_policy_sha256", self.terminology_policy_sha256),
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ReaderTerminologyProjectionError(
                    f"READER_TERMINOLOGY_SHA256_INVALID:{field}"
                )


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_MAPPING_REQUIRED:{field}"
        )
    return value


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_LIST_REQUIRED:{field}"
        )
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result) or len(result) != len(set(result)):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_LIST_INVALID:{field}"
        )
    return result


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_MAPPING_LIST_REQUIRED:{field}"
        )
    return tuple(item for item in value if isinstance(item, Mapping))


def _string_mapping_tuple(value: object, field: str) -> tuple[tuple[str, str], ...]:
    payload = _mapping(value, field)
    result = tuple((str(key).strip(), str(item).strip()) for key, item in payload.items())
    if not result or any(not key or not item for key, item in result):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_STRING_MAPPING_INVALID:{field}"
        )
    return result


def _alias_pattern(alias: str) -> re.Pattern[str]:
    escaped = re.escape(alias)
    if re.search(r"[A-Za-z0-9]", alias):
        return re.compile(rf"(?<![A-Za-z0-9_]){escaped}(?![A-Za-z0-9_])")
    return re.compile(escaped)


def project_reader_text(*, text: str, policy: ReaderTerminologyPolicy) -> str:
    projected = text
    for alias, replacement in sorted(
        policy.raw_reader_replacements,
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        projected = _alias_pattern(alias).sub(replacement, projected)
    return projected


def _load_yaml(root: Path, relative_path: str, field: str) -> tuple[Mapping[str, Any], bytes]:
    normalized = relative_path.replace("\\", "/")
    if normalized.startswith("/") or ".." in normalized.split("/"):
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_PATH_INVALID:{field}"
        )
    selected = (root / normalized).resolve()
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_PATH_OUTSIDE_REPOSITORY:{field}"
        ) from exc
    raw = selected.read_bytes()
    try:
        decoded = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ReaderTerminologyProjectionError(
            f"READER_TERMINOLOGY_YAML_INVALID:{field}"
        ) from exc
    return _mapping(decoded, field), raw


def load_reader_terminology_policy(
    *,
    repository_root: Path,
    reader_profile_path: str = DEFAULT_READER_PROFILE_PATH,
    terminology_path: str = DEFAULT_READER_TERMINOLOGY_PATH,
) -> ReaderTerminologyPolicy:
    root = repository_root.resolve()
    profile_payload, profile_raw = _load_yaml(root, reader_profile_path, "profile")
    _exact_keys(
        profile_payload,
        {
            "schema_version",
            "profile_id",
            "profile_version",
            "status",
            "owner",
            "audience_zh",
            "assumed_knowledge_zh",
            "not_assumed_knowledge_zh",
            "safety",
        },
        "profile",
    )
    if profile_payload["schema_version"] != READER_PROFILE_SCHEMA:
        raise ReaderTerminologyProjectionError("READER_PROFILE_SCHEMA_INVALID")
    if str(profile_payload["status"]) != "REVIEWED_BASELINE":
        raise ReaderTerminologyProjectionError("READER_PROFILE_STATUS_INVALID")
    if dict(_mapping(profile_payload["safety"], "profile.safety")) != dict(
        _EXPECTED_PROFILE_SAFETY
    ):
        raise ReaderTerminologyProjectionError("READER_PROFILE_SAFETY_INVALID")
    profile = ReaderProfile(
        profile_id=str(profile_payload["profile_id"]),
        profile_version=str(profile_payload["profile_version"]),
        audience_zh=str(profile_payload["audience_zh"]),
        assumed_knowledge_zh=_string_tuple(
            profile_payload["assumed_knowledge_zh"], "profile.assumed_knowledge_zh"
        ),
        not_assumed_knowledge_zh=_string_tuple(
            profile_payload["not_assumed_knowledge_zh"],
            "profile.not_assumed_knowledge_zh",
        ),
    )

    policy_payload, policy_raw = _load_yaml(root, terminology_path, "policy")
    _exact_keys(
        policy_payload,
        {
            "schema_version",
            "policy_id",
            "policy_version",
            "status",
            "owner",
            "reader_profile_id",
            "unknown_identifier_patterns",
            "excluded_non_reader_regions",
            "raw_reader_replacements",
            "terms",
            "safety",
        },
        "policy",
    )
    if policy_payload["schema_version"] != READER_TERMINOLOGY_POLICY_SCHEMA:
        raise ReaderTerminologyProjectionError("READER_TERMINOLOGY_POLICY_SCHEMA_INVALID")
    if str(policy_payload["status"]) != "REVIEWED_ENGINEERING_BASELINE":
        raise ReaderTerminologyProjectionError("READER_TERMINOLOGY_POLICY_STATUS_INVALID")
    if str(policy_payload["reader_profile_id"]) != profile.profile_id:
        raise ReaderTerminologyProjectionError("READER_TERMINOLOGY_PROFILE_BINDING_INVALID")
    terms = tuple(
        ReaderTermDefinition.from_dict(item)
        for item in _mapping_tuple(policy_payload["terms"], "policy.terms")
    )
    return ReaderTerminologyPolicy(
        policy_id=str(policy_payload["policy_id"]),
        policy_version=str(policy_payload["policy_version"]),
        status=str(policy_payload["status"]),
        owner=str(policy_payload["owner"]),
        reader_profile=profile,
        reader_profile_sha256=hashlib.sha256(profile_raw).hexdigest(),
        terminology_policy_sha256=hashlib.sha256(policy_raw).hexdigest(),
        unknown_identifier_patterns=_string_tuple(
            policy_payload["unknown_identifier_patterns"],
            "policy.unknown_identifier_patterns",
        ),
        excluded_non_reader_regions=_string_tuple(
            policy_payload["excluded_non_reader_regions"],
            "policy.excluded_non_reader_regions",
        ),
        raw_reader_replacements=_string_mapping_tuple(
            policy_payload["raw_reader_replacements"],
            "policy.raw_reader_replacements",
        ),
        terms=terms,
        safety=_mapping(policy_payload["safety"], "policy.safety"),
    )


__all__ = [
    "DEFAULT_READER_PROFILE_PATH",
    "DEFAULT_READER_TERMINOLOGY_PATH",
    "READER_PROFILE_SCHEMA",
    "READER_TERMINOLOGY_POLICY_SCHEMA",
    "ReaderTerminologyPolicy",
    "ReaderTerminologyProjectionError",
    "load_reader_terminology_policy",
    "project_reader_text",
]
