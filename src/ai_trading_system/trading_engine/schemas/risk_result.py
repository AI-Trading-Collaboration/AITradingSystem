from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RiskSeverity(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCK = "BLOCK"


class RiskRuleEvaluation(BaseModel):
    rule_id: str = Field(min_length=1)
    passed: bool
    actual: Any = None
    limit: Any = None
    message: str = ""


class RiskCheckResult(BaseModel):
    intent_id: str = Field(min_length=1)
    checked_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    approved: bool
    severity: RiskSeverity
    blocked_by: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    evaluated_rules: list[RiskRuleEvaluation] = Field(default_factory=list)
    risk_config_version: str = Field(min_length=1)

    @classmethod
    def from_evaluations(
        cls,
        *,
        intent_id: str,
        evaluations: list[RiskRuleEvaluation],
        risk_config_version: str,
        warnings: list[str] | None = None,
    ) -> RiskCheckResult:
        blocked_by = [rule.rule_id for rule in evaluations if not rule.passed]
        approved = not blocked_by
        severity = RiskSeverity.PASS if approved else RiskSeverity.BLOCK
        return cls(
            intent_id=intent_id,
            approved=approved,
            severity=severity,
            blocked_by=blocked_by,
            warnings=warnings or [],
            evaluated_rules=evaluations,
            risk_config_version=risk_config_version,
        )
