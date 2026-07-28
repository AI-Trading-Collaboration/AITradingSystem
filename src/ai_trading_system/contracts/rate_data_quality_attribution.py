from __future__ import annotations

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

from ai_trading_system.contracts.data_quality_attribution import (
    ATTRIBUTION_SCOPE_COMPLETE,
    ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN,
    SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
    DataQualitySourceArtifactBinding,
)
from ai_trading_system.yaml_loader import (
    StrictYamlError,
    StrictYamlOptions,
    load_strict_yaml_text,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RATE_ROW_ATTRIBUTION_DECISION_PATH = (
    PROJECT_ROOT / "config/data_quality/rate_row_issue_attribution_decision_v1.yaml"
)

RATE_ROW_ATTRIBUTION_DECISION_SCHEMA_VERSION = "data_quality_rate_row_issue_attribution_decision.v1"
RATE_ROW_ATTRIBUTION_DECISION_ID = (
    "owner_decision:DATA-GOV-002C3:2026-07-28:"
    "approve_rate_row_issue_attribution_contract_wave_v1"
)
RATE_ROW_ATTRIBUTION_DECISION_VERSION = "1.0.0"
RATE_ROW_ATTRIBUTION_DECISION_STATUS = "REVIEWED_APPROVED"
RATE_ROW_ATTRIBUTION_DECISION = "APPROVE_FOR_CONTRACT_WAVE"
RATE_ROW_ATTRIBUTION_REVIEW_PACK_ID = "dq_rate_issue_attribution_review_216045a1ebe282194028e1f8"
PRIMARY_MACRO_RATES_SOURCE_ROLE = "primary_macro_rates"
RATE_ROW_DIGEST_SCHEMA_VERSION = "rate_row_digest.v1"
RATE_ROW_DIGEST_FIELDS = ("date", "series", "value")
RATE_SERIES_ONLY_ISOLATION_SCOPE = "RATE_SERIES_ONLY"
RATE_ROW_ROLE_TRIGGER = "TRIGGER"
RATE_ROW_ROLE_PREVIOUS_VALID = "PREVIOUS_VALID"

APPROVED_RATE_ISSUES: dict[str, tuple[str, str]] = {
    "rates_invalid_date": (
        "dq_issue_site_0e7f3d74bfa489801c83",
        "SINGLE_SOURCE_ROW",
    ),
    "rates_invalid_value": (
        "dq_issue_site_f337897b3d0d0b8e2842",
        "SINGLE_SOURCE_ROW",
    ),
    "rates_non_finite_value": (
        "dq_issue_site_dcc6dcab7a17c225b404",
        "SINGLE_SOURCE_ROW",
    ),
    "rates_out_of_range": (
        "dq_issue_site_6421117ee905a6da1438",
        "SINGLE_SOURCE_ROW",
    ),
    "rates_extreme_daily_change": (
        "dq_issue_site_85549de0f1e9ab739a74",
        "CURRENT_AND_PREVIOUS_VALID_OBSERVATION",
    ),
    "rates_suspicious_daily_change": (
        "dq_issue_site_df1c184d09e3c55d3e71",
        "CURRENT_AND_PREVIOUS_VALID_OBSERVATION",
    ),
}

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
        "approved_source_role",
        "approved_sites",
        "row_digest",
        "conditions",
        "review_condition",
        "production_effect",
        "broker_action",
    }
)
_REVIEW_PACK_KEYS = frozenset({"path", "pack_id", "sha256"})
_APPROVED_SITE_KEYS = frozenset({"site_id", "issue_code", "scope_taxonomy"})
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
_CONDITION_KEYS = frozenset(
    {
        "rate_series_isolation_only",
        "invalid_date_never_window_isolated",
        "invalid_value_or_non_finite_unparseable_date_degrades_to_series",
        "range_evidence_requires_actual_series_thresholds",
        "move_evidence_requires_trigger_predecessor_and_actual_thresholds",
        "warning_does_not_expand_isolation_authority",
        "row_attribution_requires_exact_source_artifact_checksum",
        "incomplete_attribution_scope",
        "active_capability_policy_adoption_authorized",
        "consumer_migration_authorized",
    }
)


class RateDataQualityAttributionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class ApprovedRateIssue:
    site_id: str
    issue_code: str
    scope_taxonomy: str


@dataclass(frozen=True)
class RateRowIssueAttributionDecision:
    decision_id: str
    decision_version: str
    status: str
    decision: str
    decided_at: date
    owner: str
    review_pack_path: str
    review_pack_id: str
    review_pack_sha256: str
    approved_source_role: str
    approved_sites: tuple[ApprovedRateIssue, ...]
    row_digest_schema_version: str
    row_digest_fields: tuple[str, ...]
    source_ordinal_scope: str
    review_condition: tuple[str, ...]
    path: Path
    sha256: str

    @property
    def authority_id(self) -> str:
        return f"{self.decision_id}@{self.decision_version}"

    def approved_issue(self, issue_code: str) -> ApprovedRateIssue:
        matches = tuple(item for item in self.approved_sites if item.issue_code == issue_code)
        if len(matches) != 1:
            raise RateDataQualityAttributionContractError(
                "UNAPPROVED_RATE_ISSUE_CODE",
                issue_code,
            )
        return matches[0]


@dataclass(frozen=True)
class DataQualityAffectedRateRow:
    source_ordinal: int
    canonical_row_digest: str
    observed_date: date | None
    rate_series: str
    row_role: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_ordinal, int)
            or isinstance(self.source_ordinal, bool)
            or self.source_ordinal < 0
        ):
            raise RateDataQualityAttributionContractError(
                "INVALID_SOURCE_ORDINAL",
                repr(self.source_ordinal),
            )
        _require_sha256(self.canonical_row_digest, "row.canonical_row_digest")
        normalized_series = self.rate_series.strip()
        if not normalized_series:
            raise RateDataQualityAttributionContractError(
                "MISSING_RATE_SERIES",
                f"source_ordinal={self.source_ordinal}",
            )
        if self.row_role not in {RATE_ROW_ROLE_TRIGGER, RATE_ROW_ROLE_PREVIOUS_VALID}:
            raise RateDataQualityAttributionContractError(
                "INVALID_RATE_ROW_ROLE",
                self.row_role,
            )
        object.__setattr__(self, "rate_series", normalized_series)

    def to_dict(self) -> dict[str, object]:
        return {
            "source_ordinal": self.source_ordinal,
            "canonical_row_digest": self.canonical_row_digest,
            "observed_date": (
                None if self.observed_date is None else self.observed_date.isoformat()
            ),
            "rate_series": self.rate_series,
            "row_role": self.row_role,
        }


@dataclass(frozen=True)
class RateIssuePolicyEvidence:
    trigger_source_ordinal: int
    policy_values: tuple[tuple[str, float], ...]
    observed_change: float | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.trigger_source_ordinal, int)
            or isinstance(self.trigger_source_ordinal, bool)
            or self.trigger_source_ordinal < 0
        ):
            raise RateDataQualityAttributionContractError(
                "INVALID_POLICY_EVIDENCE_ORDINAL",
                repr(self.trigger_source_ordinal),
            )
        keys = tuple(key for key, _ in self.policy_values)
        if not keys or keys != tuple(sorted(set(keys))) or any(not key for key in keys):
            raise RateDataQualityAttributionContractError(
                "INVALID_POLICY_EVIDENCE",
                "policy keys must be sorted unique non-empty text",
            )
        if any(not math.isfinite(value) for _, value in self.policy_values):
            raise RateDataQualityAttributionContractError(
                "INVALID_POLICY_EVIDENCE",
                "policy values must be finite",
            )
        if self.observed_change is not None and not math.isfinite(self.observed_change):
            raise RateDataQualityAttributionContractError(
                "INVALID_POLICY_EVIDENCE",
                "observed change must be finite",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "trigger_source_ordinal": self.trigger_source_ordinal,
            "policy_values": {key: value for key, value in self.policy_values},
            "observed_change": self.observed_change,
        }


@dataclass(frozen=True)
class RateDataQualityIssueAttribution:
    schema_version: str
    scope_status: str
    decision_id: str
    decision_version: str
    decision_path: str
    decision_sha256: str
    site_id: str
    issue_code: str
    scope_taxonomy: str
    isolation_scope: str
    source: DataQualitySourceArtifactBinding
    requested_window_start: date
    requested_window_end: date
    affected_price_tickers: tuple[str, ...]
    affected_rate_series: tuple[str, ...]
    affected_source_roles: tuple[str, ...]
    affected_dates: tuple[date, ...]
    affected_fields: tuple[str, ...]
    affected_rows: tuple[DataQualityAffectedRateRow, ...]
    policy_evidence: tuple[RateIssuePolicyEvidence, ...]
    row_digest_schema_version: str
    row_digest_fields: tuple[str, ...]
    source_ordinal_scope: str

    def __post_init__(self) -> None:
        if self.schema_version != "data_quality_rate_issue_attribution.v1":
            raise RateDataQualityAttributionContractError(
                "UNSUPPORTED_RATE_ATTRIBUTION_SCHEMA",
                self.schema_version,
            )
        if self.scope_status != ATTRIBUTION_SCOPE_COMPLETE:
            raise RateDataQualityAttributionContractError(
                "INVALID_ATTRIBUTION_SCOPE_STATUS",
                self.scope_status,
            )
        if self.issue_code not in APPROVED_RATE_ISSUES:
            raise RateDataQualityAttributionContractError(
                "UNAPPROVED_RATE_ISSUE_CODE",
                self.issue_code,
            )
        expected_site, expected_taxonomy = APPROVED_RATE_ISSUES[self.issue_code]
        if self.site_id != expected_site or self.scope_taxonomy != expected_taxonomy:
            raise RateDataQualityAttributionContractError(
                "RATE_ISSUE_IDENTITY_MISMATCH",
                self.issue_code,
            )
        if self.isolation_scope != RATE_SERIES_ONLY_ISOLATION_SCOPE:
            raise RateDataQualityAttributionContractError(
                "WINDOW_OR_ROW_ISOLATION_NOT_AUTHORIZED",
                self.isolation_scope,
            )
        _require_sha256(self.decision_sha256, "decision_sha256")
        if self.requested_window_start > self.requested_window_end:
            raise RateDataQualityAttributionContractError(
                "INVALID_REQUESTED_WINDOW",
                "start follows end",
            )
        if self.affected_price_tickers:
            raise RateDataQualityAttributionContractError(
                "UNEXPECTED_PRICE_SCOPE",
                self.issue_code,
            )
        _require_sorted_unique_non_empty(
            self.affected_rate_series,
            "affected_rate_series",
        )
        _require_sorted_unique_non_empty(
            self.affected_source_roles,
            "affected_source_roles",
        )
        if self.affected_source_roles != (PRIMARY_MACRO_RATES_SOURCE_ROLE,):
            raise RateDataQualityAttributionContractError(
                "UNAPPROVED_SOURCE_ROLE",
                ",".join(self.affected_source_roles),
            )
        if not self.affected_rows:
            raise RateDataQualityAttributionContractError(
                "EMPTY_TRIGGER_ROW_SET",
                self.issue_code,
            )
        ordinals = tuple(row.source_ordinal for row in self.affected_rows)
        if len(ordinals) != len(set(ordinals)):
            raise RateDataQualityAttributionContractError(
                "DUPLICATE_SOURCE_ORDINAL",
                self.issue_code,
            )
        row_series = tuple(sorted({row.rate_series for row in self.affected_rows}))
        if row_series != self.affected_rate_series:
            raise RateDataQualityAttributionContractError(
                "AFFECTED_RATE_SERIES_SCOPE_MISMATCH",
                self.issue_code,
            )
        row_dates = tuple(
            sorted(
                {row.observed_date for row in self.affected_rows if row.observed_date is not None}
            )
        )
        if row_dates != self.affected_dates:
            raise RateDataQualityAttributionContractError(
                "AFFECTED_DATE_SCOPE_MISMATCH",
                self.issue_code,
            )
        if any(
            value < self.requested_window_start or value > self.requested_window_end
            for value in self.affected_dates
        ):
            raise RateDataQualityAttributionContractError(
                "TRIGGER_DATE_OUTSIDE_REQUESTED_WINDOW",
                self.issue_code,
            )
        if self.issue_code not in {
            "rates_invalid_date",
            "rates_invalid_value",
            "rates_non_finite_value",
        } and any(row.observed_date is None for row in self.affected_rows):
            raise RateDataQualityAttributionContractError(
                "RATE_ROW_DATE_REQUIRED",
                self.issue_code,
            )
        expected_roles = (
            {RATE_ROW_ROLE_TRIGGER}
            if self.scope_taxonomy == "SINGLE_SOURCE_ROW"
            else {RATE_ROW_ROLE_TRIGGER, RATE_ROW_ROLE_PREVIOUS_VALID}
        )
        if {row.row_role for row in self.affected_rows} != expected_roles:
            raise RateDataQualityAttributionContractError(
                "RATE_ROW_DEPENDENCY_MISMATCH",
                self.issue_code,
            )
        trigger_ordinals = {
            row.source_ordinal
            for row in self.affected_rows
            if row.row_role == RATE_ROW_ROLE_TRIGGER
        }
        evidence_ordinals = {item.trigger_source_ordinal for item in self.policy_evidence}
        requires_evidence = self.issue_code in {
            "rates_out_of_range",
            "rates_extreme_daily_change",
            "rates_suspicious_daily_change",
        }
        if requires_evidence and evidence_ordinals != trigger_ordinals:
            raise RateDataQualityAttributionContractError(
                "RATE_POLICY_EVIDENCE_INCOMPLETE",
                self.issue_code,
            )
        if self.issue_code in {
            "rates_extreme_daily_change",
            "rates_suspicious_daily_change",
        } and any(item.observed_change is None for item in self.policy_evidence):
            raise RateDataQualityAttributionContractError(
                "RATE_POLICY_EVIDENCE_INCOMPLETE",
                f"{self.issue_code}: observed change unavailable",
            )
        if not requires_evidence and self.policy_evidence:
            raise RateDataQualityAttributionContractError(
                "UNEXPECTED_RATE_POLICY_EVIDENCE",
                self.issue_code,
            )
        if self.row_digest_schema_version != RATE_ROW_DIGEST_SCHEMA_VERSION:
            raise RateDataQualityAttributionContractError(
                "ROW_DIGEST_SCHEMA_MISMATCH",
                self.row_digest_schema_version,
            )
        if self.row_digest_fields != RATE_ROW_DIGEST_FIELDS:
            raise RateDataQualityAttributionContractError(
                "ROW_DIGEST_FIELDS_MISMATCH",
                ",".join(self.row_digest_fields),
            )
        if self.source_ordinal_scope != SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT:
            raise RateDataQualityAttributionContractError(
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
            "isolation_scope": self.isolation_scope,
            "source": self.source.to_dict(),
            "requested_window": {
                "start": self.requested_window_start.isoformat(),
                "end": self.requested_window_end.isoformat(),
            },
            "affected_price_tickers": [],
            "affected_rate_series": list(self.affected_rate_series),
            "affected_source_roles": list(self.affected_source_roles),
            "affected_dates": [value.isoformat() for value in self.affected_dates],
            "affected_fields": list(self.affected_fields),
            "affected_rows": [row.to_dict() for row in self.affected_rows],
            "policy_evidence": [item.to_dict() for item in self.policy_evidence],
            "row_identity": {
                "digest_schema_version": self.row_digest_schema_version,
                "digest_fields": list(self.row_digest_fields),
                "source_ordinal_scope": self.source_ordinal_scope,
            },
        }


def load_rate_row_issue_attribution_decision(
    path: Path = DEFAULT_RATE_ROW_ATTRIBUTION_DECISION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> RateRowIssueAttributionDecision:
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
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_UNAVAILABLE",
            str(resolved_path),
        ) from exc
    payload = _required_mapping_payload(raw, "decision")
    _require_exact_keys(payload, _ROOT_KEYS, "decision")
    _require_exact_value(
        payload,
        "schema_version",
        RATE_ROW_ATTRIBUTION_DECISION_SCHEMA_VERSION,
    )
    _require_exact_value(payload, "decision_id", RATE_ROW_ATTRIBUTION_DECISION_ID)
    decision_version = _required_text(payload, "decision_version")
    if (
        decision_version != RATE_ROW_ATTRIBUTION_DECISION_VERSION
        or _SEMANTIC_VERSION_PATTERN.fullmatch(decision_version) is None
    ):
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_VERSION_MISMATCH",
            decision_version,
        )
    _require_exact_value(payload, "status", RATE_ROW_ATTRIBUTION_DECISION_STATUS)
    _require_exact_value(payload, "decision", RATE_ROW_ATTRIBUTION_DECISION)
    _require_exact_value(payload, "approved_source_role", PRIMARY_MACRO_RATES_SOURCE_ROLE)
    _require_exact_value(payload, "production_effect", "none")
    _require_exact_value(payload, "broker_action", "none")

    review_pack = _required_mapping(payload, "review_pack")
    _require_exact_keys(review_pack, _REVIEW_PACK_KEYS, "review_pack")
    review_pack_path = _required_text(review_pack, "path")
    review_pack_id = _required_text(review_pack, "pack_id")
    review_pack_sha256 = _required_sha256(review_pack, "sha256")
    if review_pack_id != RATE_ROW_ATTRIBUTION_REVIEW_PACK_ID:
        raise RateDataQualityAttributionContractError(
            "RATE_REVIEW_PACK_ID_MISMATCH",
            review_pack_id,
        )
    bound_review_pack_path = _resolve_bound_path(resolved_root, review_pack_path)
    _verify_file_sha256(
        bound_review_pack_path,
        review_pack_sha256,
        "RATE_REVIEW_PACK_BYTES_DRIFTED",
    )
    review_pack_payload = _load_strict_json_path(bound_review_pack_path)
    if review_pack_payload.get("review_pack_id") != review_pack_id:
        raise RateDataQualityAttributionContractError(
            "RATE_REVIEW_PACK_CONTENT_ID_MISMATCH",
            review_pack_id,
        )

    approved_sites_raw = payload.get("approved_sites")
    if not isinstance(approved_sites_raw, Sequence) or isinstance(
        approved_sites_raw,
        (str, bytes),
    ):
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
            "approved_sites must be a sequence",
        )
    approved_sites: list[ApprovedRateIssue] = []
    for index, value in enumerate(approved_sites_raw):
        item = _required_mapping_payload(value, f"approved_sites[{index}]")
        _require_exact_keys(item, _APPROVED_SITE_KEYS, f"approved_sites[{index}]")
        approved_sites.append(
            ApprovedRateIssue(
                site_id=_required_text(item, "site_id"),
                issue_code=_required_text(item, "issue_code"),
                scope_taxonomy=_required_text(item, "scope_taxonomy"),
            )
        )
    observed_approved = {
        item.issue_code: (item.site_id, item.scope_taxonomy) for item in approved_sites
    }
    if len(observed_approved) != len(approved_sites) or observed_approved != APPROVED_RATE_ISSUES:
        raise RateDataQualityAttributionContractError(
            "RATE_APPROVED_SITE_SET_MISMATCH",
            repr(observed_approved),
        )
    pack_candidates = review_pack_payload.get("candidates")
    if not isinstance(pack_candidates, list):
        raise RateDataQualityAttributionContractError(
            "RATE_REVIEW_PACK_SCHEMA_INVALID",
            "candidates",
        )
    pack_identity = {
        str(item.get("issue_code")): (
            str(item.get("site_id")),
            str(item.get("scope_taxonomy")),
        )
        for item in pack_candidates
        if isinstance(item, Mapping)
    }
    if pack_identity != APPROVED_RATE_ISSUES:
        raise RateDataQualityAttributionContractError(
            "RATE_REVIEW_PACK_SITE_SET_MISMATCH",
            repr(pack_identity),
        )

    row_digest = _required_mapping(payload, "row_digest")
    _require_exact_keys(row_digest, _ROW_DIGEST_KEYS, "row_digest")
    _require_exact_value(row_digest, "schema_version", RATE_ROW_DIGEST_SCHEMA_VERSION)
    row_digest_fields = _required_text_sequence(row_digest, "fields")
    if row_digest_fields != RATE_ROW_DIGEST_FIELDS:
        raise RateDataQualityAttributionContractError(
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
    _require_exact_value(
        row_digest,
        "value_encoding",
        "EXPLICIT_TYPE_TAGGED_WITH_NON_FINITE_TOKENS",
    )

    conditions = _required_mapping(payload, "conditions")
    _require_exact_keys(conditions, _CONDITION_KEYS, "conditions")
    for field in (
        "rate_series_isolation_only",
        "invalid_date_never_window_isolated",
        "invalid_value_or_non_finite_unparseable_date_degrades_to_series",
        "range_evidence_requires_actual_series_thresholds",
        "move_evidence_requires_trigger_predecessor_and_actual_thresholds",
        "warning_does_not_expand_isolation_authority",
        "row_attribution_requires_exact_source_artifact_checksum",
    ):
        _require_exact_value(conditions, field, True)
    _require_exact_value(
        conditions,
        "incomplete_attribution_scope",
        ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN,
    )
    _require_exact_value(
        conditions,
        "active_capability_policy_adoption_authorized",
        False,
    )
    _require_exact_value(conditions, "consumer_migration_authorized", False)

    return RateRowIssueAttributionDecision(
        decision_id=RATE_ROW_ATTRIBUTION_DECISION_ID,
        decision_version=decision_version,
        status=RATE_ROW_ATTRIBUTION_DECISION_STATUS,
        decision=RATE_ROW_ATTRIBUTION_DECISION,
        decided_at=_required_iso_date(payload, "decided_at"),
        owner=_required_text(payload, "owner"),
        review_pack_path=review_pack_path,
        review_pack_id=review_pack_id,
        review_pack_sha256=review_pack_sha256,
        approved_source_role=PRIMARY_MACRO_RATES_SOURCE_ROLE,
        approved_sites=tuple(approved_sites),
        row_digest_schema_version=RATE_ROW_DIGEST_SCHEMA_VERSION,
        row_digest_fields=row_digest_fields,
        source_ordinal_scope=SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
        review_condition=_required_text_sequence(payload, "review_condition"),
        path=resolved_path,
        sha256=sha256(decision_bytes).hexdigest(),
    )


def build_rate_issue_attribution(
    *,
    decision: RateRowIssueAttributionDecision,
    issue_code: str,
    source: DataQualitySourceArtifactBinding,
    requested_window: tuple[date, date],
    row_groups: Sequence[Sequence[Mapping[str, object]]],
    policy_evidence: Sequence[RateIssuePolicyEvidence] = (),
) -> RateDataQualityIssueAttribution:
    approved = decision.approved_issue(issue_code)
    if source.source_role != decision.approved_source_role:
        raise RateDataQualityAttributionContractError(
            "UNAPPROVED_SOURCE_ROLE",
            source.source_role,
        )
    if len(requested_window) != 2 or requested_window[0] > requested_window[1]:
        raise RateDataQualityAttributionContractError(
            "INVALID_REQUESTED_WINDOW",
            repr(requested_window),
        )
    if not row_groups:
        raise RateDataQualityAttributionContractError(
            "EMPTY_TRIGGER_ROW_SET",
            issue_code,
        )
    expected_group_size = 1 if approved.scope_taxonomy == "SINGLE_SOURCE_ROW" else 2
    affected_rows: list[DataQualityAffectedRateRow] = []
    affected_fields: set[str] = set()
    for group in row_groups:
        if len(group) != expected_group_size:
            raise RateDataQualityAttributionContractError(
                "RATE_ROW_DEPENDENCY_MISMATCH",
                issue_code,
            )
        for index, raw_row in enumerate(group):
            role = (
                RATE_ROW_ROLE_TRIGGER
                if expected_group_size == 1 or index == 1
                else RATE_ROW_ROLE_PREVIOUS_VALID
            )
            source_ordinal = raw_row.get("_source_ordinal")
            if (
                type(source_ordinal) is bool
                or not isinstance(source_ordinal, Integral)
                or int(source_ordinal) < 0
            ):
                raise RateDataQualityAttributionContractError(
                    "INVALID_SOURCE_ORDINAL",
                    repr(source_ordinal),
                )
            series_value = raw_row.get("series")
            if not isinstance(series_value, str) or not series_value.strip():
                raise RateDataQualityAttributionContractError(
                    "MISSING_RATE_SERIES",
                    f"source_ordinal={int(source_ordinal)}",
                )
            observed_date = _optional_canonical_date(raw_row.get("_date", raw_row.get("date")))
            affected_rows.append(
                DataQualityAffectedRateRow(
                    source_ordinal=int(source_ordinal),
                    canonical_row_digest=canonical_rate_row_digest(raw_row),
                    observed_date=observed_date,
                    rate_series=series_value,
                    row_role=role,
                )
            )
        affected_fields.add("value")
    if issue_code == "rates_invalid_date":
        affected_fields = {"date"}

    affected_rows_tuple = tuple(
        sorted(affected_rows, key=lambda row: (row.source_ordinal, row.row_role))
    )
    policy_evidence_tuple = tuple(
        sorted(policy_evidence, key=lambda item: item.trigger_source_ordinal)
    )
    return RateDataQualityIssueAttribution(
        schema_version="data_quality_rate_issue_attribution.v1",
        scope_status=ATTRIBUTION_SCOPE_COMPLETE,
        decision_id=decision.decision_id,
        decision_version=decision.decision_version,
        decision_path=decision.path.as_posix(),
        decision_sha256=decision.sha256,
        site_id=approved.site_id,
        issue_code=issue_code,
        scope_taxonomy=approved.scope_taxonomy,
        isolation_scope=RATE_SERIES_ONLY_ISOLATION_SCOPE,
        source=source,
        requested_window_start=requested_window[0],
        requested_window_end=requested_window[1],
        affected_price_tickers=(),
        affected_rate_series=tuple(sorted({row.rate_series for row in affected_rows_tuple})),
        affected_source_roles=(source.source_role,),
        affected_dates=tuple(
            sorted(
                {row.observed_date for row in affected_rows_tuple if row.observed_date is not None}
            )
        ),
        affected_fields=tuple(sorted(affected_fields)),
        affected_rows=affected_rows_tuple,
        policy_evidence=policy_evidence_tuple,
        row_digest_schema_version=decision.row_digest_schema_version,
        row_digest_fields=decision.row_digest_fields,
        source_ordinal_scope=decision.source_ordinal_scope,
    )


def rate_series_disjoint_isolation_eligible(
    attribution: RateDataQualityIssueAttribution | None,
    *,
    required_rate_series: Sequence[str],
) -> bool:
    if attribution is None:
        return False
    required = {value.strip() for value in required_rate_series if value.strip()}
    return (
        attribution.scope_status == ATTRIBUTION_SCOPE_COMPLETE
        and attribution.isolation_scope == RATE_SERIES_ONLY_ISOLATION_SCOPE
        and bool(attribution.affected_rate_series)
        and set(attribution.affected_rate_series).isdisjoint(required)
    )


def canonical_rate_row_digest(row: Mapping[str, object]) -> str:
    fields: list[dict[str, object]] = []
    for field in RATE_ROW_DIGEST_FIELDS:
        if field not in row:
            raise RateDataQualityAttributionContractError(
                "ROW_DIGEST_FIELD_MISSING",
                field,
            )
        value = row[field]
        if field == "series":
            if not isinstance(value, str) or not value.strip():
                raise RateDataQualityAttributionContractError(
                    "MISSING_RATE_SERIES",
                    "series",
                )
            canonical_value: dict[str, object] = {
                "type": "string",
                "value": value.strip(),
            }
        else:
            canonical_value = _canonical_typed_value(value)
        fields.append({"name": field, "value": canonical_value})
    material = json.dumps(
        {
            "schema_version": RATE_ROW_DIGEST_SCHEMA_VERSION,
            "fields": fields,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(material).hexdigest()


def _canonical_typed_value(value: object) -> dict[str, object]:
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
            return {"type": "non_finite", "value": "nan"}
        if math.isinf(numeric):
            return {
                "type": "non_finite",
                "value": "+inf" if numeric > 0 else "-inf",
            }
        if numeric == 0:
            numeric = 0.0
        return {"type": "finite_float", "value": numeric.hex()}
    if isinstance(value, (date, datetime)):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    raise RateDataQualityAttributionContractError(
        "ROW_DIGEST_UNSUPPORTED_VALUE_TYPE",
        type(value).__name__,
    )


def _optional_canonical_date(value: object) -> date | None:
    if value is None or type(value).__name__ in {"NAType", "NaTType"}:
        return None
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
        except ValueError:
            return None
    return None


def _resolve_bound_path(project_root: Path, relative_path: str) -> Path:
    candidate = (project_root / relative_path).resolve()
    try:
        candidate.relative_to(project_root)
    except ValueError as exc:
        raise RateDataQualityAttributionContractError(
            "BOUND_PATH_OUTSIDE_PROJECT",
            relative_path,
        ) from exc
    return candidate


def _verify_file_sha256(path: Path, expected: str, code: str) -> None:
    try:
        observed = sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise RateDataQualityAttributionContractError(code, str(path)) from exc
    if observed != expected:
        raise RateDataQualityAttributionContractError(
            code,
            f"{path}: expected={expected} observed={observed}",
        )


def _load_strict_json_path(path: Path) -> Mapping[str, Any]:
    try:
        raw = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RateDataQualityAttributionContractError(
            "RATE_REVIEW_PACK_BYTES_DRIFTED",
            str(path),
        ) from exc
    return _required_mapping_payload(raw, "review_pack")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise RateDataQualityAttributionContractError(
                "RATE_REVIEW_PACK_BYTES_DRIFTED",
                f"duplicate JSON key: {key}",
            )
        payload[key] = value
    return payload


def _reject_json_constant(value: str) -> None:
    raise RateDataQualityAttributionContractError(
        "RATE_REVIEW_PACK_BYTES_DRIFTED",
        f"non-standard JSON constant: {value}",
    )


def _required_mapping(
    payload: Mapping[str, Any],
    field: str,
) -> Mapping[str, Any]:
    return _required_mapping_payload(payload.get(field), field)


def _required_mapping_payload(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
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
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
            (
                f"{context} keys mismatch; "
                f"missing={sorted(expected - actual)} unknown={sorted(actual - expected)}"
            ),
        )


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} must be non-empty text",
        )
    return value.strip()


def _required_iso_date(payload: Mapping[str, Any], field: str) -> date:
    value = _required_text(payload, field)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
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
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
            f"{field} must be a non-empty sequence",
        )
    result = tuple(item.strip() for item in value if isinstance(item, str) and item.strip())
    if len(result) != len(value) or len(result) != len(set(result)):
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_SCHEMA_INVALID",
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
        raise RateDataQualityAttributionContractError(
            "RATE_ATTRIBUTION_DECISION_VALUE_MISMATCH",
            f"{field}: expected={expected!r} observed={value!r}",
        )


def _require_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise RateDataQualityAttributionContractError(
            "INVALID_SHA256",
            field,
        )


def _require_sorted_unique_non_empty(
    values: tuple[str, ...],
    field: str,
) -> None:
    if not values or values != tuple(sorted(set(values))) or any(not value for value in values):
        raise RateDataQualityAttributionContractError(
            "INVALID_ATTRIBUTION_DIMENSION",
            field,
        )


__all__ = [
    "APPROVED_RATE_ISSUES",
    "DEFAULT_RATE_ROW_ATTRIBUTION_DECISION_PATH",
    "DataQualityAffectedRateRow",
    "PRIMARY_MACRO_RATES_SOURCE_ROLE",
    "RATE_ROW_ATTRIBUTION_DECISION_ID",
    "RATE_ROW_DIGEST_FIELDS",
    "RATE_ROW_DIGEST_SCHEMA_VERSION",
    "RATE_SERIES_ONLY_ISOLATION_SCOPE",
    "RateDataQualityAttributionContractError",
    "RateDataQualityIssueAttribution",
    "RateIssuePolicyEvidence",
    "RateRowIssueAttributionDecision",
    "build_rate_issue_attribution",
    "canonical_rate_row_digest",
    "load_rate_row_issue_attribution_decision",
    "rate_series_disjoint_isolation_eligible",
]
