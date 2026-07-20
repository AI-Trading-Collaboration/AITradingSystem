from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT

EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_SCHEMA_VERSION = (
    "external_request_cache_lifecycle_policy.v1"
)
DEFAULT_EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "data" / "external_request_cache_lifecycle_policy.yaml"
)


@dataclass(frozen=True)
class HttpStatusLifecycleRule:
    lifecycle_class: str
    statuses: frozenset[int]
    excluded_statuses: frozenset[int]
    ranges: tuple[tuple[int, int], ...]
    ttl_seconds: int
    honor_retry_after: bool

    def matches(self, status_code: int) -> bool:
        return status_code not in self.excluded_statuses and (
            status_code in self.statuses
            or any(minimum <= status_code <= maximum for minimum, maximum in self.ranges)
        )


@dataclass(frozen=True)
class ExternalRequestCacheLifecyclePolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    positive_minimum: int
    positive_maximum: int
    positive_lifecycle_class: str
    negative_rules: tuple[HttpStatusLifecycleRule, ...]
    nonstandard_lifecycle_class: str
    nonstandard_ttl_seconds: int
    retry_after_maximum_seconds: int


@dataclass(frozen=True)
class ExternalRequestCacheLifecycleDecision:
    policy_id: str
    policy_version: str
    lifecycle_class: str
    ttl_seconds: int | None
    expires_at: datetime | None
    retry_after_seconds: int | None

    @property
    def persistent(self) -> bool:
        return self.expires_at is None


def load_external_request_cache_lifecycle_policy(
    path: Path = DEFAULT_EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_PATH,
) -> ExternalRequestCacheLifecyclePolicy:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"unable to load external request cache lifecycle policy: {path}") from exc
    if not isinstance(raw, Mapping):
        raise ValueError("external request cache lifecycle policy must be a mapping")
    if raw.get("schema_version") != EXTERNAL_REQUEST_CACHE_LIFECYCLE_POLICY_SCHEMA_VERSION:
        raise ValueError("unsupported external request cache lifecycle policy schema_version")

    positive = _required_mapping(raw, "positive_status")
    nonstandard = _required_mapping(raw, "nonstandard_status")
    retry_after = _required_mapping(raw, "retry_after")
    if retry_after.get("combination") != "maximum":
        raise ValueError("retry_after.combination must be maximum")

    rules_payload = raw.get("negative_status_rules")
    if not isinstance(rules_payload, list) or not rules_payload:
        raise ValueError("negative_status_rules must be a non-empty list")
    rules = tuple(_parse_rule(rule) for rule in rules_payload)
    policy = ExternalRequestCacheLifecyclePolicy(
        policy_id=_required_text(raw, "policy_id"),
        policy_version=_required_text(raw, "policy_version"),
        status=_required_text(raw, "status"),
        owner=_required_text(raw, "owner"),
        positive_minimum=_required_nonnegative_int(positive, "minimum"),
        positive_maximum=_required_nonnegative_int(positive, "maximum"),
        positive_lifecycle_class=_required_text(positive, "lifecycle_class"),
        negative_rules=rules,
        nonstandard_lifecycle_class=_required_text(nonstandard, "lifecycle_class"),
        nonstandard_ttl_seconds=_required_nonnegative_int(nonstandard, "ttl_seconds"),
        retry_after_maximum_seconds=_required_nonnegative_int(retry_after, "maximum_seconds"),
    )
    if policy.status != "pilot_baseline":
        raise ValueError("external request cache lifecycle policy status must be pilot_baseline")
    if policy.positive_minimum > policy.positive_maximum:
        raise ValueError("positive status range is invalid")
    _validate_http_policy_coverage(policy)
    return policy


def evaluate_external_request_cache_lifecycle(
    *,
    status_code: int,
    response_headers: Mapping[str, str],
    observed_at: datetime,
    policy: ExternalRequestCacheLifecyclePolicy | None = None,
) -> ExternalRequestCacheLifecycleDecision:
    if not isinstance(status_code, int) or isinstance(status_code, bool):
        raise ValueError("status_code must be an integer")
    lifecycle_policy = policy or load_external_request_cache_lifecycle_policy()
    observed = _aware_utc(observed_at, field="observed_at")
    if lifecycle_policy.positive_minimum <= status_code <= lifecycle_policy.positive_maximum:
        return ExternalRequestCacheLifecycleDecision(
            policy_id=lifecycle_policy.policy_id,
            policy_version=lifecycle_policy.policy_version,
            lifecycle_class=lifecycle_policy.positive_lifecycle_class,
            ttl_seconds=None,
            expires_at=None,
            retry_after_seconds=None,
        )

    rule = next(
        (
            candidate
            for candidate in lifecycle_policy.negative_rules
            if candidate.matches(status_code)
        ),
        None,
    )
    if rule is None or status_code < 200 or status_code > 599:
        ttl_seconds = lifecycle_policy.nonstandard_ttl_seconds
        lifecycle_class = lifecycle_policy.nonstandard_lifecycle_class
        parsed_retry_after = None
    else:
        parsed_retry_after = (
            _retry_after_seconds(response_headers, observed_at=observed)
            if rule.honor_retry_after
            else None
        )
        capped_retry_after = (
            min(parsed_retry_after, lifecycle_policy.retry_after_maximum_seconds)
            if parsed_retry_after is not None
            else None
        )
        ttl_seconds = max(rule.ttl_seconds, capped_retry_after or 0)
        lifecycle_class = rule.lifecycle_class
        parsed_retry_after = capped_retry_after

    return ExternalRequestCacheLifecycleDecision(
        policy_id=lifecycle_policy.policy_id,
        policy_version=lifecycle_policy.policy_version,
        lifecycle_class=lifecycle_class,
        ttl_seconds=ttl_seconds,
        expires_at=observed + timedelta(seconds=ttl_seconds),
        retry_after_seconds=parsed_retry_after,
    )


def _parse_rule(raw: Any) -> HttpStatusLifecycleRule:
    if not isinstance(raw, Mapping):
        raise ValueError("negative status rule must be a mapping")
    statuses_payload = raw.get("statuses", [])
    excluded_statuses_payload = raw.get("excluded_statuses", [])
    ranges_payload = raw.get("ranges", [])
    if (
        not isinstance(statuses_payload, list)
        or not isinstance(excluded_statuses_payload, list)
        or not isinstance(ranges_payload, list)
    ):
        raise ValueError("negative status rule statuses/ranges must be lists")
    statuses = frozenset(_http_status(value, field="statuses") for value in statuses_payload)
    excluded_statuses = frozenset(
        _http_status(value, field="excluded_statuses") for value in excluded_statuses_payload
    )
    ranges: list[tuple[int, int]] = []
    for entry in ranges_payload:
        if not isinstance(entry, Mapping):
            raise ValueError("negative status range must be a mapping")
        minimum = _http_status(entry.get("minimum"), field="range.minimum")
        maximum = _http_status(entry.get("maximum"), field="range.maximum")
        if minimum > maximum:
            raise ValueError("negative status range is invalid")
        ranges.append((minimum, maximum))
    if not statuses and not ranges:
        raise ValueError("negative status rule must select at least one status")
    honor_retry_after = raw.get("honor_retry_after")
    if not isinstance(honor_retry_after, bool):
        raise ValueError("negative status rule honor_retry_after must be boolean")
    return HttpStatusLifecycleRule(
        lifecycle_class=_required_text(raw, "lifecycle_class"),
        statuses=statuses,
        excluded_statuses=excluded_statuses,
        ranges=tuple(ranges),
        ttl_seconds=_required_nonnegative_int(raw, "ttl_seconds"),
        honor_retry_after=honor_retry_after,
    )


def _validate_http_policy_coverage(policy: ExternalRequestCacheLifecyclePolicy) -> None:
    for status_code in range(400, 600):
        matches = [rule for rule in policy.negative_rules if rule.matches(status_code)]
        if len(matches) != 1:
            raise ValueError(
                f"HTTP status {status_code} must match exactly one negative lifecycle rule"
            )


def _retry_after_seconds(headers: Mapping[str, str], *, observed_at: datetime) -> int | None:
    value = next(
        (str(item) for key, item in headers.items() if str(key).lower() == "retry-after"),
        None,
    )
    if value is None:
        return None
    token = value.strip()
    if token.isdigit():
        return int(token)
    try:
        retry_at = parsedate_to_datetime(token)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None or retry_at.utcoffset() is None:
        retry_at = retry_at.replace(tzinfo=UTC)
    return max(0, int((retry_at.astimezone(UTC) - observed_at).total_seconds()))


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _required_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be non-empty text")
    return value.strip()


def _required_nonnegative_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _http_status(value: Any, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 100 <= value <= 599:
        raise ValueError(f"{field} must contain standard HTTP status codes")
    return value
