from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum


class CanonicalStatus(StrEnum):
    NOT_DUE = "NOT_DUE"
    DUE = "DUE"
    RUNNING = "RUNNING"
    PASS = "PASS"
    LIMITED = "LIMITED"
    SKIPPED = "SKIPPED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


class ContextResolutionStatus(StrEnum):
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"


class EvidenceRole(StrEnum):
    PRIMARY_DECISION_EVIDENCE = "PRIMARY_DECISION_EVIDENCE"
    LEGACY_COMPARISON_EVIDENCE = "LEGACY_COMPARISON_EVIDENCE"
    SENSITIVITY_EVIDENCE_WITH_CAVEAT = "SENSITIVITY_EVIDENCE_WITH_CAVEAT"
    DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"
    METADATA_ONLY = "METADATA_ONLY"


class ResearchWindowRole(StrEnum):
    PRIMARY_VALIDATED = "primary_validated"
    LEGACY_COMPARISON = "legacy_comparison"
    SENSITIVITY = "sensitivity"
    PROXY_ROBUSTNESS = "proxy_robustness"
    METADATA_ONLY = "metadata_only"


class PolicyRole(StrEnum):
    MARKET_REGIME = "market_regime"
    RESEARCH_WINDOW = "research_window"
    RESEARCH_WINDOW_POLICY = "research_window_policy"
    DATA_QUALITY = "data_quality"
    TRADING_CALENDAR = "trading_calendar"
    STRATEGY = "strategy"
    EXECUTION = "execution"
    THRESHOLD = "threshold"


class UnknownLegacyStatusError(ValueError):
    def __init__(self, status: str) -> None:
        self.status = status
        super().__init__(f"UNKNOWN_LEGACY_STATUS: {status!r}")


def canonical_status_from_legacy(
    value: str,
    *,
    explicit_mapping: Mapping[str, CanonicalStatus | str],
) -> CanonicalStatus:
    """Resolve only explicitly governed legacy statuses; never infer from substrings."""
    normalized = value.strip()
    if normalized not in explicit_mapping:
        raise UnknownLegacyStatusError(normalized)
    return CanonicalStatus(explicit_mapping[normalized])
