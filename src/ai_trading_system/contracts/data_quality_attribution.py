from __future__ import annotations

import ast
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from numbers import Integral, Real
from pathlib import Path
from typing import Any

from ai_trading_system.trading_calendar import NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE
from ai_trading_system.us_equity_special_closure_policy import (
    load_us_equity_special_closure_policy,
)
from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_PATH = (
    PROJECT_ROOT
    / "config/data_quality/price_non_market_session_attribution_decision_v1.yaml"
)

PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_SCHEMA_VERSION = (
    "data_quality_price_non_market_session_attribution_decision.v1"
)
PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_ID = (
    "owner_decision:DATA-GOV-002C3P:2026-07-27:"
    "approve_price_non_market_session_contract_wave_v1"
)
PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_VERSION = "1.0.1"
PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_STATUS = "REVIEWED_APPROVED"
PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION = "APPROVE_FOR_CONTRACT_WAVE"
PRICE_NON_MARKET_SESSION_REVIEW_PACK_ID = (
    "dq_price_issue_attribution_review_0731caba2f2b6280dda3385b"
)
PRICE_NON_MARKET_SESSION_SITE_ID = "dq_issue_site_312625a26da21428b763"
PRICE_NON_MARKET_SESSION_ISSUE_CODE = "prices_non_market_session_date"
PRIMARY_MARKET_PRICES_SOURCE_ROLE = "primary_market_prices"
SECONDARY_MARKET_PRICES_SOURCE_ROLE = "secondary_market_prices"
PRICE_NON_MARKET_SESSION_SCOPE_TAXONOMY = "DISTINCT_NON_SESSION_DATE_ROW_SET"
PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION = (
    "price_non_market_session_row_digest.v1"
)
PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS = (
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
)
SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT = "EXACT_SOURCE_SNAPSHOT_ONLY"
ATTRIBUTION_SCOPE_COMPLETE = "COMPLETE"
ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN = "GLOBAL_OR_UNKNOWN_SCOPE"
US_EQUITY_CALENDAR_FUNCTION = "is_us_equity_trading_day"

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SEMANTIC_VERSION_PATTERN = re.compile(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)")
_STRICT_YAML_OPTIONS = StrictYamlOptions(
    key_policy="HASHABLE",
    flatten_mapping=True,
    reject_non_finite=True,
)
_ROOT_KEYS = frozenset(
    {
        "schema_version",
        "decision_id",
        "decision_version",
        "status",
        "decision",
        "decided_at",
        "owner",
        "review_pack",
        "approved_site_id",
        "approved_issue_code",
        "approved_source_role",
        "scope_taxonomy",
        "row_digest",
        "reviewed_calendar",
        "conditions",
        "review_condition",
        "production_effect",
        "broker_action",
    }
)
_REVIEW_PACK_KEYS = frozenset({"path", "pack_id", "sha256"})
_ROW_DIGEST_KEYS = frozenset(
    {
        "schema_version",
        "fields",
        "source_ordinal_scope",
        "canonical_json_encoding",
        "canonical_json_key_order",
        "canonical_json_separators",
        "value_encoding",
    }
)
_CALENDAR_KEYS = frozenset(
    {
        "calendar_id",
        "calendar_source",
        "runtime_source_path",
        "calendar_function",
        "calendar_function_ast_sha256",
        "special_closure_policy_path",
        "special_closure_policy_id",
        "special_closure_policy_version",
        "special_closure_policy_sha256",
    }
)
_CONDITION_KEYS = frozenset(
    {
        "primary_market_prices_us_equity_calendar_only",
        "row_attribution_requires_exact_source_artifact_checksum",
        "source_ordinal_scope",
        "canonical_row_digest_defined_in_c3",
        "calendar_or_special_closure_drift_requires_review",
        "incomplete_attribution_scope",
        "pre_c3_policy_consumer_production_change_authorized",
    }
)


class DataQualityAttributionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class PriceNonMarketSessionAttributionDecision:
    decision_id: str
    decision_version: str
    status: str
    decision: str
    decided_at: date
    owner: str
    review_pack_path: str
    review_pack_id: str
    review_pack_sha256: str
    approved_site_id: str
    approved_issue_code: str
    approved_source_role: str
    scope_taxonomy: str
    row_digest_schema_version: str
    row_digest_fields: tuple[str, ...]
    source_ordinal_scope: str
    calendar_id: str
    calendar_source: str
    calendar_runtime_source_path: str
    calendar_function: str
    calendar_function_ast_sha256: str
    special_closure_policy_path: str
    special_closure_policy_id: str
    special_closure_policy_version: str
    special_closure_policy_sha256: str
    review_condition: tuple[str, ...]
    path: Path
    sha256: str

    @property
    def authority_id(self) -> str:
        return f"{self.decision_id}@{self.decision_version}"


@dataclass(frozen=True)
class DataQualitySourceArtifactBinding:
    source_role: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        _require_non_empty_text(self.source_role, "source.source_role")
        _require_non_empty_text(self.path, "source.path")
        _require_sha256(self.sha256, "source.sha256")

    def to_dict(self) -> dict[str, str]:
        return {
            "source_role": self.source_role,
            "path": self.path,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class DataQualityCalendarBinding:
    calendar_id: str
    calendar_source: str
    calendar_function: str
    calendar_function_ast_sha256: str
    special_closure_policy_id: str
    special_closure_policy_version: str
    special_closure_policy_sha256: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.calendar_id, "calendar.calendar_id"),
            (self.calendar_source, "calendar.calendar_source"),
            (self.calendar_function, "calendar.calendar_function"),
            (self.special_closure_policy_id, "calendar.special_closure_policy_id"),
            (self.special_closure_policy_version, "calendar.special_closure_policy_version"),
        ):
            _require_non_empty_text(value, field)
        _require_sha256(
            self.calendar_function_ast_sha256,
            "calendar.calendar_function_ast_sha256",
        )
        _require_sha256(
            self.special_closure_policy_sha256,
            "calendar.special_closure_policy_sha256",
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "calendar_id": self.calendar_id,
            "calendar_source": self.calendar_source,
            "calendar_function": self.calendar_function,
            "calendar_function_ast_sha256": self.calendar_function_ast_sha256,
            "special_closure_policy_id": self.special_closure_policy_id,
            "special_closure_policy_version": self.special_closure_policy_version,
            "special_closure_policy_sha256": self.special_closure_policy_sha256,
        }


@dataclass(frozen=True)
class DataQualityAffectedPriceRow:
    source_ordinal: int
    canonical_row_digest: str
    observed_date: date
    ticker: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_ordinal, int)
            or isinstance(self.source_ordinal, bool)
            or self.source_ordinal < 0
        ):
            raise DataQualityAttributionContractError(
                "INVALID_SOURCE_ORDINAL",
                "source_ordinal must be a non-negative integer",
            )
        _require_sha256(self.canonical_row_digest, "row.canonical_row_digest")
        normalized_ticker = self.ticker.strip()
        if not normalized_ticker:
            raise DataQualityAttributionContractError(
                "MISSING_TRIGGER_ROW_TICKER",
                "every affected price row requires a non-empty ticker",
            )
        object.__setattr__(self, "ticker", normalized_ticker)

    def to_dict(self) -> dict[str, object]:
        return {
            "source_ordinal": self.source_ordinal,
            "canonical_row_digest": self.canonical_row_digest,
            "observed_date": self.observed_date.isoformat(),
            "ticker": self.ticker,
        }


@dataclass(frozen=True)
class DataQualityIssueAttribution:
    schema_version: str
    scope_status: str
    decision_id: str
    decision_version: str
    decision_path: str
    decision_sha256: str
    site_id: str
    issue_code: str
    scope_taxonomy: str
    source: DataQualitySourceArtifactBinding
    requested_window_start: date
    requested_window_end: date
    calendar: DataQualityCalendarBinding
    affected_price_tickers: tuple[str, ...]
    affected_rate_series: tuple[str, ...]
    affected_source_roles: tuple[str, ...]
    affected_dates: tuple[date, ...]
    affected_fields: tuple[str, ...]
    affected_rows: tuple[DataQualityAffectedPriceRow, ...]
    row_digest_schema_version: str
    row_digest_fields: tuple[str, ...]
    source_ordinal_scope: str

    def __post_init__(self) -> None:
        if self.schema_version != "data_quality_issue_attribution.v1":
            raise DataQualityAttributionContractError(
                "UNSUPPORTED_ATTRIBUTION_SCHEMA",
                self.schema_version,
            )
        if self.scope_status != ATTRIBUTION_SCOPE_COMPLETE:
            raise DataQualityAttributionContractError(
                "INVALID_ATTRIBUTION_SCOPE_STATUS",
                self.scope_status,
            )
        _require_sha256(self.decision_sha256, "decision_sha256")
        if self.requested_window_start > self.requested_window_end:
            raise DataQualityAttributionContractError(
                "INVALID_REQUESTED_WINDOW",
                "requested window start must not follow end",
            )
        if not self.affected_rows:
            raise DataQualityAttributionContractError(
                "EMPTY_TRIGGER_ROW_SET",
                "complete attribution requires affected rows",
            )
        _require_sorted_unique_non_empty(
            self.affected_price_tickers,
            "affected_price_tickers",
        )
        _require_sorted_unique_non_empty(
            self.affected_source_roles,
            "affected_source_roles",
        )
        _require_sorted_unique_dates(self.affected_dates, "affected_dates")
        _require_sorted_unique_non_empty(self.affected_fields, "affected_fields")
        if self.affected_rate_series:
            raise DataQualityAttributionContractError(
                "UNEXPECTED_RATE_SCOPE",
                "price issue attribution cannot affect rate series",
            )
        ordinals = tuple(row.source_ordinal for row in self.affected_rows)
        if len(ordinals) != len(set(ordinals)):
            raise DataQualityAttributionContractError(
                "DUPLICATE_SOURCE_ORDINAL",
                "affected row source ordinals must be unique",
            )
        row_tickers = tuple(sorted({row.ticker for row in self.affected_rows}))
        row_dates = tuple(sorted({row.observed_date for row in self.affected_rows}))
        if row_tickers != self.affected_price_tickers:
            raise DataQualityAttributionContractError(
                "AFFECTED_TICKER_SCOPE_MISMATCH",
                "affected tickers must equal the trigger-row ticker set",
            )
        if row_dates != self.affected_dates:
            raise DataQualityAttributionContractError(
                "AFFECTED_DATE_SCOPE_MISMATCH",
                "affected dates must equal the trigger-row date set",
            )
        if any(
            row.observed_date < self.requested_window_start
            or row.observed_date > self.requested_window_end
            for row in self.affected_rows
        ):
            raise DataQualityAttributionContractError(
                "TRIGGER_DATE_OUTSIDE_REQUESTED_WINDOW",
                "every affected row must be inside the requested window",
            )
        if self.row_digest_schema_version != PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION:
            raise DataQualityAttributionContractError(
                "ROW_DIGEST_SCHEMA_MISMATCH",
                self.row_digest_schema_version,
            )
        if self.row_digest_fields != PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS:
            raise DataQualityAttributionContractError(
                "ROW_DIGEST_FIELDS_MISMATCH",
                ",".join(self.row_digest_fields),
            )
        if self.source_ordinal_scope != SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT:
            raise DataQualityAttributionContractError(
                "SOURCE_ORDINAL_SCOPE_MISMATCH",
                self.source_ordinal_scope,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "scope_status": self.scope_status,
            "decision": {
                "decision_id": self.decision_id,
                "decision_version": self.decision_version,
                "path": self.decision_path,
                "sha256": self.decision_sha256,
            },
            "site_id": self.site_id,
            "issue_code": self.issue_code,
            "scope_taxonomy": self.scope_taxonomy,
            "source": self.source.to_dict(),
            "requested_window": {
                "start": self.requested_window_start.isoformat(),
                "end": self.requested_window_end.isoformat(),
            },
            "calendar": self.calendar.to_dict(),
            "affected_price_tickers": list(self.affected_price_tickers),
            "affected_rate_series": list(self.affected_rate_series),
            "affected_source_roles": list(self.affected_source_roles),
            "affected_dates": [value.isoformat() for value in self.affected_dates],
            "affected_fields": list(self.affected_fields),
            "affected_rows": [row.to_dict() for row in self.affected_rows],
            "row_identity": {
                "digest_schema_version": self.row_digest_schema_version,
                "digest_fields": list(self.row_digest_fields),
                "source_ordinal_scope": self.source_ordinal_scope,
            },
        }


def load_price_non_market_session_attribution_decision(
    path: Path = DEFAULT_PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> PriceNonMarketSessionAttributionDecision:
    resolved_path = path.resolve()
    resolved_root = project_root.resolve()
    try:
        decision_bytes = resolved_path.read_bytes()
        raw = load_strict_yaml_text(
            decision_bytes.decode("utf-8"),
            options=_STRICT_YAML_OPTIONS,
            label=str(resolved_path),
        )
    except (OSError, UnicodeDecodeError, StrictYamlError) as exc:
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_UNAVAILABLE",
            str(resolved_path),
        ) from exc

    payload = _required_mapping_payload(raw, "decision")
    _require_exact_keys(payload, _ROOT_KEYS, "decision")
    _require_exact_value(
        payload,
        "schema_version",
        PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_SCHEMA_VERSION,
    )
    _require_exact_value(
        payload,
        "decision_id",
        PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_ID,
    )
    decision_version = _required_text(payload, "decision_version")
    if (
        decision_version != PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_VERSION
        or _SEMANTIC_VERSION_PATTERN.fullmatch(decision_version) is None
    ):
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_VERSION_MISMATCH",
            decision_version,
        )
    _require_exact_value(
        payload,
        "status",
        PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_STATUS,
    )
    _require_exact_value(
        payload,
        "decision",
        PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION,
    )
    _require_exact_value(payload, "approved_site_id", PRICE_NON_MARKET_SESSION_SITE_ID)
    _require_exact_value(payload, "approved_issue_code", PRICE_NON_MARKET_SESSION_ISSUE_CODE)
    _require_exact_value(payload, "approved_source_role", PRIMARY_MARKET_PRICES_SOURCE_ROLE)
    _require_exact_value(
        payload,
        "scope_taxonomy",
        PRICE_NON_MARKET_SESSION_SCOPE_TAXONOMY,
    )
    _require_exact_value(payload, "production_effect", "none")
    _require_exact_value(payload, "broker_action", "none")

    review_pack = _required_mapping(payload, "review_pack")
    _require_exact_keys(review_pack, _REVIEW_PACK_KEYS, "review_pack")
    review_pack_path = _required_text(review_pack, "path")
    review_pack_id = _required_text(review_pack, "pack_id")
    review_pack_sha256 = _required_sha256(review_pack, "sha256")
    if review_pack_id != PRICE_NON_MARKET_SESSION_REVIEW_PACK_ID:
        raise DataQualityAttributionContractError(
            "REVIEW_PACK_ID_MISMATCH",
            review_pack_id,
        )
    bound_review_pack_path = _resolve_bound_path(resolved_root, review_pack_path)
    _verify_file_sha256(
        bound_review_pack_path,
        review_pack_sha256,
        "REVIEW_PACK_BYTES_DRIFTED",
    )
    try:
        review_pack_payload = _required_mapping_payload(
            _load_strict_json_path(bound_review_pack_path),
            "review_pack_payload",
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise DataQualityAttributionContractError(
            "REVIEW_PACK_BYTES_DRIFTED",
            str(bound_review_pack_path),
        ) from exc
    if review_pack_payload.get("review_pack_id") != review_pack_id:
        raise DataQualityAttributionContractError(
            "REVIEW_PACK_CONTENT_ID_MISMATCH",
            review_pack_id,
        )

    row_digest = _required_mapping(payload, "row_digest")
    _require_exact_keys(row_digest, _ROW_DIGEST_KEYS, "row_digest")
    _require_exact_value(
        row_digest,
        "schema_version",
        PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION,
    )
    row_digest_fields = _required_text_sequence(row_digest, "fields")
    if row_digest_fields != PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS:
        raise DataQualityAttributionContractError(
            "ROW_DIGEST_FIELDS_MISMATCH",
            ",".join(row_digest_fields),
        )
    _require_exact_value(
        row_digest,
        "source_ordinal_scope",
        SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
    )
    _require_exact_value(row_digest, "canonical_json_encoding", "UTF-8")
    _require_exact_value(row_digest, "canonical_json_key_order", "SORTED")
    _require_exact_value(
        row_digest,
        "canonical_json_separators",
        "COMMA_COLON_NO_WHITESPACE",
    )
    _require_exact_value(row_digest, "value_encoding", "EXPLICIT_TYPE_TAGGED")

    calendar = _required_mapping(payload, "reviewed_calendar")
    _require_exact_keys(calendar, _CALENDAR_KEYS, "reviewed_calendar")
    _require_exact_value(calendar, "calendar_id", "XNYS")
    _require_exact_value(
        calendar,
        "calendar_source",
        NYSE_REGULAR_HOLIDAY_CALENDAR_SOURCE,
    )
    calendar_runtime_source_path = _required_text(calendar, "runtime_source_path")
    calendar_function = _required_text(calendar, "calendar_function")
    if calendar_function != US_EQUITY_CALENDAR_FUNCTION:
        raise DataQualityAttributionContractError(
            "CALENDAR_FUNCTION_MISMATCH",
            calendar_function,
        )
    calendar_function_ast_sha256 = _required_sha256(
        calendar,
        "calendar_function_ast_sha256",
    )
    observed_calendar_ast_sha256 = _function_ast_hash(
        _resolve_bound_path(resolved_root, calendar_runtime_source_path),
        calendar_function,
    )
    if observed_calendar_ast_sha256 != calendar_function_ast_sha256:
        raise DataQualityAttributionContractError(
            "CALENDAR_POLICY_REVIEW_REQUIRED",
            "calendar function bytes drifted from the reviewed authority",
        )

    special_closure_policy_path = _required_text(
        calendar,
        "special_closure_policy_path",
    )
    try:
        special_policy = load_us_equity_special_closure_policy(
            _resolve_bound_path(resolved_root, special_closure_policy_path)
        )
    except ValueError as exc:
        raise DataQualityAttributionContractError(
            "SPECIAL_CLOSURE_POLICY_REVIEW_REQUIRED",
            "special-closure policy is unavailable or invalid",
        ) from exc
    special_closure_policy_id = _required_text(
        calendar,
        "special_closure_policy_id",
    )
    special_closure_policy_version = _required_text(
        calendar,
        "special_closure_policy_version",
    )
    special_closure_policy_sha256 = _required_sha256(
        calendar,
        "special_closure_policy_sha256",
    )
    if (
        special_policy.policy_id != special_closure_policy_id
        or special_policy.policy_version != special_closure_policy_version
        or special_policy.sha256 != special_closure_policy_sha256
    ):
        raise DataQualityAttributionContractError(
            "SPECIAL_CLOSURE_POLICY_REVIEW_REQUIRED",
            "special-closure policy identity or bytes drifted",
        )

    conditions = _required_mapping(payload, "conditions")
    _require_exact_keys(conditions, _CONDITION_KEYS, "conditions")
    for field in (
        "primary_market_prices_us_equity_calendar_only",
        "row_attribution_requires_exact_source_artifact_checksum",
        "canonical_row_digest_defined_in_c3",
        "calendar_or_special_closure_drift_requires_review",
    ):
        _require_exact_value(conditions, field, True)
    _require_exact_value(
        conditions,
        "source_ordinal_scope",
        SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
    )
    _require_exact_value(
        conditions,
        "incomplete_attribution_scope",
        ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN,
    )
    _require_exact_value(
        conditions,
        "pre_c3_policy_consumer_production_change_authorized",
        False,
    )

    return PriceNonMarketSessionAttributionDecision(
        decision_id=PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_ID,
        decision_version=decision_version,
        status=PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_STATUS,
        decision=PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION,
        decided_at=_required_iso_date(payload, "decided_at"),
        owner=_required_text(payload, "owner"),
        review_pack_path=review_pack_path,
        review_pack_id=review_pack_id,
        review_pack_sha256=review_pack_sha256,
        approved_site_id=PRICE_NON_MARKET_SESSION_SITE_ID,
        approved_issue_code=PRICE_NON_MARKET_SESSION_ISSUE_CODE,
        approved_source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE,
        scope_taxonomy=PRICE_NON_MARKET_SESSION_SCOPE_TAXONOMY,
        row_digest_schema_version=PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION,
        row_digest_fields=row_digest_fields,
        source_ordinal_scope=SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
        calendar_id=_required_text(calendar, "calendar_id"),
        calendar_source=_required_text(calendar, "calendar_source"),
        calendar_runtime_source_path=calendar_runtime_source_path,
        calendar_function=calendar_function,
        calendar_function_ast_sha256=calendar_function_ast_sha256,
        special_closure_policy_path=special_closure_policy_path,
        special_closure_policy_id=special_closure_policy_id,
        special_closure_policy_version=special_closure_policy_version,
        special_closure_policy_sha256=special_closure_policy_sha256,
        review_condition=_required_text_sequence(payload, "review_condition"),
        path=resolved_path,
        sha256=sha256(decision_bytes).hexdigest(),
    )


def build_reviewed_calendar_binding(
    decision: PriceNonMarketSessionAttributionDecision,
) -> DataQualityCalendarBinding:
    return DataQualityCalendarBinding(
        calendar_id=decision.calendar_id,
        calendar_source=decision.calendar_source,
        calendar_function=decision.calendar_function,
        calendar_function_ast_sha256=decision.calendar_function_ast_sha256,
        special_closure_policy_id=decision.special_closure_policy_id,
        special_closure_policy_version=decision.special_closure_policy_version,
        special_closure_policy_sha256=decision.special_closure_policy_sha256,
    )


def build_price_non_market_session_attribution(
    *,
    decision: PriceNonMarketSessionAttributionDecision,
    source: DataQualitySourceArtifactBinding,
    requested_window: tuple[date, date],
    calendar: DataQualityCalendarBinding,
    trigger_rows: Sequence[Mapping[str, object]],
) -> DataQualityIssueAttribution:
    if source.source_role != decision.approved_source_role:
        raise DataQualityAttributionContractError(
            "UNAPPROVED_SOURCE_ROLE",
            source.source_role,
        )
    expected_calendar = build_reviewed_calendar_binding(decision)
    if calendar != expected_calendar:
        raise DataQualityAttributionContractError(
            "CALENDAR_BINDING_MISMATCH",
            "calendar binding does not match the reviewed decision",
        )
    if len(requested_window) != 2 or requested_window[0] > requested_window[1]:
        raise DataQualityAttributionContractError(
            "INVALID_REQUESTED_WINDOW",
            "requested window must be an inclusive ordered pair",
        )
    if not trigger_rows:
        raise DataQualityAttributionContractError(
            "EMPTY_TRIGGER_ROW_SET",
            "trigger rows are required",
        )

    affected_rows: list[DataQualityAffectedPriceRow] = []
    for raw_row in trigger_rows:
        source_ordinal = raw_row.get("_source_ordinal")
        if (
            type(source_ordinal) is bool
            or not isinstance(source_ordinal, Integral)
            or int(source_ordinal) < 0
        ):
            raise DataQualityAttributionContractError(
                "INVALID_SOURCE_ORDINAL",
                repr(source_ordinal),
            )
        observed_date = _canonical_date(raw_row.get("_date", raw_row.get("date")), "row.date")
        ticker_value = raw_row.get("ticker")
        if not isinstance(ticker_value, str) or not ticker_value.strip():
            raise DataQualityAttributionContractError(
                "MISSING_TRIGGER_ROW_TICKER",
                f"source_ordinal={int(source_ordinal)}",
            )
        affected_rows.append(
            DataQualityAffectedPriceRow(
                source_ordinal=int(source_ordinal),
                canonical_row_digest=canonical_price_row_digest(raw_row),
                observed_date=observed_date,
                ticker=ticker_value,
            )
        )

    affected_rows_tuple = tuple(sorted(affected_rows, key=lambda row: row.source_ordinal))
    return DataQualityIssueAttribution(
        schema_version="data_quality_issue_attribution.v1",
        scope_status=ATTRIBUTION_SCOPE_COMPLETE,
        decision_id=decision.decision_id,
        decision_version=decision.decision_version,
        decision_path=decision.path.as_posix(),
        decision_sha256=decision.sha256,
        site_id=decision.approved_site_id,
        issue_code=decision.approved_issue_code,
        scope_taxonomy=decision.scope_taxonomy,
        source=source,
        requested_window_start=requested_window[0],
        requested_window_end=requested_window[1],
        calendar=calendar,
        affected_price_tickers=tuple(sorted({row.ticker for row in affected_rows_tuple})),
        affected_rate_series=(),
        affected_source_roles=(source.source_role,),
        affected_dates=tuple(sorted({row.observed_date for row in affected_rows_tuple})),
        affected_fields=("date",),
        affected_rows=affected_rows_tuple,
        row_digest_schema_version=decision.row_digest_schema_version,
        row_digest_fields=decision.row_digest_fields,
        source_ordinal_scope=decision.source_ordinal_scope,
    )


def canonical_price_row_digest(row: Mapping[str, object]) -> str:
    fields: list[dict[str, object]] = []
    for field in PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS:
        if field not in row:
            raise DataQualityAttributionContractError(
                "ROW_DIGEST_FIELD_MISSING",
                field,
            )
        value = row[field]
        if field == "date":
            canonical_value: dict[str, object] = {
                "type": "date",
                "value": _canonical_date(value, "row.date").isoformat(),
            }
        elif field == "ticker":
            if not isinstance(value, str) or not value.strip():
                raise DataQualityAttributionContractError(
                    "MISSING_TRIGGER_ROW_TICKER",
                    "ticker",
                )
            canonical_value = {"type": "string", "value": value.strip()}
        else:
            canonical_value = _canonical_typed_value(value, field)
        fields.append({"name": field, "value": canonical_value})
    material = json.dumps(
        {
            "schema_version": PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION,
            "fields": fields,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(material).hexdigest()


def _canonical_typed_value(value: object, field: str) -> dict[str, object]:
    item_method = getattr(value, "item", None)
    if callable(item_method) and type(value).__module__.startswith("numpy"):
        value = item_method()
    if value is None or type(value).__name__ in {"NAType", "NaTType"}:
        return {"type": "null", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, Integral):
        return {"type": "integer", "value": str(int(value))}
    if isinstance(value, Real):
        numeric = float(value)
        if math.isnan(numeric):
            return {"type": "null", "value": None}
        if not math.isfinite(numeric):
            raise DataQualityAttributionContractError(
                "ROW_DIGEST_NON_FINITE_VALUE",
                field,
            )
        if numeric == 0:
            numeric = 0.0
        return {"type": "finite_float", "value": numeric.hex()}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    raise DataQualityAttributionContractError(
        "ROW_DIGEST_UNSUPPORTED_VALUE_TYPE",
        f"{field}:{type(value).__name__}",
    )


def _canonical_date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        converted = to_pydatetime()
        if isinstance(converted, datetime):
            return converted.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise DataQualityAttributionContractError(
                "INVALID_TRIGGER_ROW_DATE",
                f"{field}={value!r}",
            ) from exc
    raise DataQualityAttributionContractError(
        "INVALID_TRIGGER_ROW_DATE",
        f"{field}={value!r}",
    )


def _function_ast_hash(source_path: Path, function_name: str) -> str:
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=source_path.as_posix(),
        )
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise DataQualityAttributionContractError(
            "CALENDAR_POLICY_REVIEW_REQUIRED",
            f"cannot parse {source_path}",
        ) from exc
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    ]
    if len(matches) != 1:
        raise DataQualityAttributionContractError(
            "CALENDAR_POLICY_REVIEW_REQUIRED",
            f"expected one function named {function_name}",
        )
    try:
        canonical_dump = ast.dump(
            matches[0],
            annotate_fields=True,
            include_attributes=False,
            show_empty=True,
        )
    except TypeError:
        # Python <3.13 always emitted empty AST fields and has no show_empty
        # argument. Exclude the newer empty PEP 695 field from the authority.
        canonical_dump = ast.dump(
            matches[0],
            annotate_fields=True,
            include_attributes=False,
        )
    material = canonical_dump.replace(", type_params=[]", "").encode("utf-8")
    return sha256(material).hexdigest()


def _resolve_bound_path(project_root: Path, relative_path: str) -> Path:
    candidate = (project_root / relative_path).resolve()
    try:
        candidate.relative_to(project_root)
    except ValueError as exc:
        raise DataQualityAttributionContractError(
            "BOUND_PATH_OUTSIDE_PROJECT",
            relative_path,
        ) from exc
    return candidate


def _verify_file_sha256(path: Path, expected: str, code: str) -> None:
    try:
        observed = sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise DataQualityAttributionContractError(code, str(path)) from exc
    if observed != expected:
        raise DataQualityAttributionContractError(
            code,
            f"{path}: expected={expected} observed={observed}",
        )


def _load_strict_json_path(path: Path) -> object:
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DataQualityAttributionContractError(
            "REVIEW_PACK_BYTES_DRIFTED",
            str(path),
        ) from exc


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise DataQualityAttributionContractError(
                "REVIEW_PACK_BYTES_DRIFTED",
                f"duplicate JSON key: {key}",
            )
        payload[key] = value
    return payload


def _reject_json_constant(value: str) -> None:
    raise DataQualityAttributionContractError(
        "REVIEW_PACK_BYTES_DRIFTED",
        f"non-standard JSON constant: {value}",
    )


def _required_mapping(payload: Mapping[str, Any], field: str) -> Mapping[str, Any]:
    return _required_mapping_payload(payload.get(field), field)


def _required_mapping_payload(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{context} must be a mapping",
        )
    return value


def _require_exact_keys(
    payload: Mapping[str, Any],
    expected: frozenset[str],
    context: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            (
                f"{context} keys mismatch; "
                f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}"
            ),
        )


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} must be non-empty text",
        )
    return value.strip()


def _required_iso_date(payload: Mapping[str, Any], field: str) -> date:
    value = _required_text(payload, field)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} must be an ISO date",
        ) from exc


def _required_sha256(payload: Mapping[str, Any], field: str) -> str:
    value = _required_text(payload, field)
    _require_sha256(value, field)
    return value


def _required_text_sequence(
    payload: Mapping[str, Any],
    field: str,
) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or not value:
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} must be a non-empty sequence",
        )
    result = tuple(
        item.strip()
        for item in value
        if isinstance(item, str) and item.strip()
    )
    if len(result) != len(value) or len(result) != len(set(result)):
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} entries must be unique non-empty text",
        )
    return result


def _require_exact_value(
    payload: Mapping[str, Any],
    field: str,
    expected: object,
) -> None:
    value = payload.get(field)
    if value != expected or type(value) is not type(expected):
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_DECISION_VALUE_MISMATCH",
            f"{field}: expected={expected!r} observed={value!r}",
        )


def _require_non_empty_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise DataQualityAttributionContractError(
            "ATTRIBUTION_FIELD_EMPTY",
            field,
        )


def _require_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise DataQualityAttributionContractError(
            "INVALID_SHA256",
            field,
        )


def _require_sorted_unique_non_empty(values: tuple[str, ...], field: str) -> None:
    if not values or values != tuple(sorted(set(values))) or any(not value for value in values):
        raise DataQualityAttributionContractError(
            "INVALID_ATTRIBUTION_DIMENSION",
            field,
        )


def _require_sorted_unique_dates(values: tuple[date, ...], field: str) -> None:
    if not values or values != tuple(sorted(set(values))):
        raise DataQualityAttributionContractError(
            "INVALID_ATTRIBUTION_DIMENSION",
            field,
        )


__all__ = [
    "ATTRIBUTION_SCOPE_COMPLETE",
    "ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN",
    "DEFAULT_PRICE_NON_MARKET_SESSION_ATTRIBUTION_DECISION_PATH",
    "DataQualityAffectedPriceRow",
    "DataQualityAttributionContractError",
    "DataQualityCalendarBinding",
    "DataQualityIssueAttribution",
    "DataQualitySourceArtifactBinding",
    "PRICE_NON_MARKET_SESSION_ISSUE_CODE",
    "PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS",
    "PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION",
    "PRIMARY_MARKET_PRICES_SOURCE_ROLE",
    "PriceNonMarketSessionAttributionDecision",
    "SECONDARY_MARKET_PRICES_SOURCE_ROLE",
    "SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT",
    "build_price_non_market_session_attribution",
    "build_reviewed_calendar_binding",
    "canonical_price_row_digest",
    "load_price_non_market_session_attribution_decision",
]
