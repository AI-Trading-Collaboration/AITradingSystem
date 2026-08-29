from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
US_EQUITY_SPECIAL_CLOSURE_ARCHIVE_1_0_0_RELATIVE_PATH = Path(
    "config/data/archive/us_equity_special_closure_registry_1_0_0.yaml"
)
# Historical v1 option modules import this exact name and are frozen by file hash.
# Keep the legacy symbol bound to its immutable 1.0.0 bytes; current consumers
# must use the explicitly named current path below.
US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH = (
    US_EQUITY_SPECIAL_CLOSURE_ARCHIVE_1_0_0_RELATIVE_PATH
)
CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH = Path(
    "config/data/us_equity_special_closure_registry.yaml"
)
US_EQUITY_SPECIAL_CLOSURE_ARCHIVE_1_0_0_SHA256 = (
    "c0469a17a775df2dcde503c254c22db0cc7d8ad6e3a5884f2ed43c88e4dfbda4"
)
DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH = (
    PROJECT_ROOT / CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH
)
US_EQUITY_SPECIAL_CLOSURE_SCHEMA_VERSION = "us_equity_special_closure_registry.v1"
US_EQUITY_SPECIAL_CLOSURE_POLICY_ID = "us_equity_special_closure_registry"
US_EQUITY_SPECIAL_CLOSURE_REVIEWED_STATUS = "reviewed_active"
US_EQUITY_DECISION_CALENDAR_ID = "XNYS"
US_EQUITY_SPECIAL_FULL_DAY_CLOSURE_TYPE = "FULL_DAY"
_SEMANTIC_VERSION_PATTERN = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")

_POLICY_KEYS = frozenset(
    {
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "calendar_id",
        "owner",
        "reviewed_at",
        "rationale",
        "intended_effect",
        "review_condition",
        "source_requirements",
        "closures",
    }
)
_SOURCE_REQUIREMENT_KEYS = frozenset({"accepted_source_classes", "accepted_https_hosts"})
_CLOSURE_KEYS = frozenset({"date", "calendar_id", "closure_type", "reason", "source"})
_SOURCE_KEYS = frozenset(
    {
        "source_class",
        "publisher",
        "document_title",
        "url",
        "published_on",
    }
)
_STRICT_YAML_OPTIONS = StrictYamlOptions(
    key_policy="HASHABLE",
    flatten_mapping=True,
    reject_non_finite=False,
)


@dataclass(frozen=True)
class CalendarAuthoritativeSource:
    source_class: str
    publisher: str
    document_title: str
    url: str
    published_on: date


@dataclass(frozen=True)
class UsEquitySpecialClosure:
    closure_date: date
    calendar_id: str
    closure_type: str
    reason: str
    source: CalendarAuthoritativeSource


@dataclass(frozen=True)
class UsEquitySpecialClosurePolicy:
    schema_version: str
    policy_id: str
    policy_version: str
    status: str
    calendar_id: str
    owner: str
    reviewed_at: date
    rationale: str
    intended_effect: str
    review_condition: tuple[str, ...]
    accepted_source_classes: tuple[str, ...]
    accepted_https_hosts: tuple[str, ...]
    closures: tuple[UsEquitySpecialClosure, ...]
    path: Path
    sha256: str

    def closure_on(self, value: date) -> UsEquitySpecialClosure | None:
        return next(
            (closure for closure in self.closures if closure.closure_date == value),
            None,
        )

    def closures_for_year(self, year: int) -> tuple[UsEquitySpecialClosure, ...]:
        return tuple(closure for closure in self.closures if closure.closure_date.year == year)

    @property
    def policy_identity(self) -> str:
        return f"{self.policy_id}@{self.policy_version}"


def load_us_equity_special_closure_policy(
    path: Path = DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH,
) -> UsEquitySpecialClosurePolicy:
    resolved_path = path.resolve()
    try:
        policy_bytes = resolved_path.read_bytes()
        raw = load_strict_yaml_text(
            policy_bytes.decode("utf-8"),
            options=_STRICT_YAML_OPTIONS,
            label=str(resolved_path),
        )
    except (OSError, UnicodeDecodeError, StrictYamlError) as exc:
        raise ValueError(
            f"unable to load US equity special-closure policy: {resolved_path}"
        ) from exc

    policy_payload = _required_mapping_payload(raw, context="policy")
    _require_exact_keys(policy_payload, expected=_POLICY_KEYS, context="policy")

    if _required_text(policy_payload, "schema_version") != US_EQUITY_SPECIAL_CLOSURE_SCHEMA_VERSION:
        raise ValueError("unsupported US equity special-closure policy schema_version")
    if _required_text(policy_payload, "policy_id") != US_EQUITY_SPECIAL_CLOSURE_POLICY_ID:
        raise ValueError("unknown US equity special-closure policy_id")
    policy_version = _required_text(policy_payload, "policy_version")
    if _SEMANTIC_VERSION_PATTERN.fullmatch(policy_version) is None:
        raise ValueError("US equity special-closure policy_version must be semantic")
    status = _required_text(policy_payload, "status")
    if status != US_EQUITY_SPECIAL_CLOSURE_REVIEWED_STATUS:
        raise ValueError("US equity special-closure policy must have reviewed_active status")
    calendar_id = _required_text(policy_payload, "calendar_id")
    if calendar_id != US_EQUITY_DECISION_CALENDAR_ID:
        raise ValueError("unknown US equity decision calendar_id")

    reviewed_at = _required_iso_date(policy_payload, "reviewed_at")
    review_condition = _required_unique_text_sequence(
        policy_payload,
        "review_condition",
    )
    source_requirements = _required_mapping(policy_payload, "source_requirements")
    _require_exact_keys(
        source_requirements,
        expected=_SOURCE_REQUIREMENT_KEYS,
        context="source_requirements",
    )
    accepted_source_classes = _required_unique_text_sequence(
        source_requirements,
        "accepted_source_classes",
    )
    accepted_https_hosts = tuple(
        value.lower()
        for value in _required_unique_text_sequence(
            source_requirements,
            "accepted_https_hosts",
        )
    )
    if len(accepted_https_hosts) != len(set(accepted_https_hosts)):
        raise ValueError("accepted_https_hosts must not contain case-insensitive duplicates")
    for host in accepted_https_hosts:
        if urlsplit(f"https://{host}").hostname != host or "/" in host:
            raise ValueError("accepted_https_hosts must contain valid hostnames")

    closures_payload = policy_payload.get("closures")
    if not isinstance(closures_payload, list) or not closures_payload:
        raise ValueError("closures must be a non-empty list")
    closures = tuple(
        _parse_closure(
            closure_payload,
            policy_calendar_id=calendar_id,
            reviewed_at=reviewed_at,
            accepted_source_classes=accepted_source_classes,
            accepted_https_hosts=accepted_https_hosts,
        )
        for closure_payload in closures_payload
    )
    closure_keys = tuple((closure.calendar_id, closure.closure_date) for closure in closures)
    if len(closure_keys) != len(set(closure_keys)):
        raise ValueError("duplicate calendar_id/date in special-closure policy")

    return UsEquitySpecialClosurePolicy(
        schema_version=US_EQUITY_SPECIAL_CLOSURE_SCHEMA_VERSION,
        policy_id=US_EQUITY_SPECIAL_CLOSURE_POLICY_ID,
        policy_version=policy_version,
        status=status,
        calendar_id=calendar_id,
        owner=_required_text(policy_payload, "owner"),
        reviewed_at=reviewed_at,
        rationale=_required_text(policy_payload, "rationale"),
        intended_effect=_required_text(policy_payload, "intended_effect"),
        review_condition=review_condition,
        accepted_source_classes=accepted_source_classes,
        accepted_https_hosts=accepted_https_hosts,
        closures=closures,
        path=resolved_path,
        sha256=sha256(policy_bytes).hexdigest(),
    )


def load_us_equity_special_closure_policy_by_identity(
    *,
    policy_version: str,
    policy_sha256: str,
    project_root: Path = PROJECT_ROOT,
) -> UsEquitySpecialClosurePolicy:
    """Resolve current or archived reviewed calendar bytes by exact identity."""

    candidates = [
        project_root / CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH
    ]
    if (
        policy_version == "1.0.0"
        and policy_sha256 == US_EQUITY_SPECIAL_CLOSURE_ARCHIVE_1_0_0_SHA256
    ):
        candidates.append(
            project_root / US_EQUITY_SPECIAL_CLOSURE_ARCHIVE_1_0_0_RELATIVE_PATH
        )
    for candidate in candidates:
        loaded = load_us_equity_special_closure_policy(candidate)
        if loaded.policy_version == policy_version and loaded.sha256 == policy_sha256:
            return loaded
    raise ValueError(
        "reviewed US equity special-closure policy identity is unavailable: "
        f"version={policy_version}; sha256={policy_sha256}"
    )


@lru_cache(maxsize=1)
def default_us_equity_special_closure_policy() -> UsEquitySpecialClosurePolicy:
    """Load one immutable calendar authority for the lifetime of a CLI process."""

    return load_us_equity_special_closure_policy(DEFAULT_US_EQUITY_SPECIAL_CLOSURE_POLICY_PATH)


def _parse_closure(
    raw: Any,
    *,
    policy_calendar_id: str,
    reviewed_at: date,
    accepted_source_classes: tuple[str, ...],
    accepted_https_hosts: tuple[str, ...],
) -> UsEquitySpecialClosure:
    payload = _required_mapping_payload(raw, context="closure")
    _require_exact_keys(payload, expected=_CLOSURE_KEYS, context="closure")
    calendar_id = _required_text(payload, "calendar_id")
    if calendar_id != policy_calendar_id:
        raise ValueError("closure calendar_id must match policy calendar_id")
    closure_type = _required_text(payload, "closure_type")
    if closure_type != US_EQUITY_SPECIAL_FULL_DAY_CLOSURE_TYPE:
        raise ValueError("unknown special-closure closure_type")
    closure_date = _required_iso_date(payload, "date")
    if closure_date.weekday() >= 5:
        raise ValueError("special full-day closure must fall on a weekday")

    source_payload = _required_mapping(payload, "source")
    _require_exact_keys(source_payload, expected=_SOURCE_KEYS, context="closure.source")
    source_class = _required_text(source_payload, "source_class")
    if source_class not in accepted_source_classes:
        raise ValueError("closure source_class is not accepted by policy")
    source_url = _required_text(source_payload, "url")
    parsed_url = urlsplit(source_url)
    if (
        parsed_url.scheme != "https"
        or parsed_url.hostname is None
        or parsed_url.hostname.lower() not in accepted_https_hosts
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.fragment
    ):
        raise ValueError("closure source URL is not an accepted authoritative HTTPS source")
    published_on = _required_iso_date(source_payload, "published_on")
    if published_on > closure_date:
        raise ValueError("closure source published_on must not follow closure date")
    if reviewed_at < published_on:
        raise ValueError("policy reviewed_at must not precede source publication")

    return UsEquitySpecialClosure(
        closure_date=closure_date,
        calendar_id=calendar_id,
        closure_type=closure_type,
        reason=_required_text(payload, "reason"),
        source=CalendarAuthoritativeSource(
            source_class=source_class,
            publisher=_required_text(source_payload, "publisher"),
            document_title=_required_text(source_payload, "document_title"),
            url=source_url,
            published_on=published_on,
        ),
    )


def _required_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    return _required_mapping_payload(payload.get(field), context=field)


def _required_mapping_payload(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{context} must be a mapping")
    return value


def _require_exact_keys(
    payload: Mapping[str, Any],
    *,
    expected: frozenset[str],
    context: str,
) -> None:
    actual = set(payload)
    unknown = sorted(actual - expected)
    missing = sorted(expected - actual)
    if unknown:
        raise ValueError(f"{context} contains unknown fields: {', '.join(unknown)}")
    if missing:
        raise ValueError(f"{context} is missing required fields: {', '.join(missing)}")


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _required_iso_date(payload: Mapping[str, Any], field: str) -> date:
    value = _required_text(payload, field)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date") from exc


def _required_unique_text_sequence(
    payload: Mapping[str, Any],
    field: str,
) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise ValueError(f"{field} must be a non-empty sequence")
    parsed = tuple(_sequence_text(item, field=field) for item in value)
    if len(parsed) != len(set(parsed)):
        raise ValueError(f"{field} must not contain duplicates")
    return parsed


def _sequence_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} entries must be non-empty strings")
    return value.strip()
