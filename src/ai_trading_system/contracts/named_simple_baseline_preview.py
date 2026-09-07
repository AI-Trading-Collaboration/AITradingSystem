"""Serializable descriptions of a pure preview, never a verified-input capability."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from ai_trading_system.contracts.data_quality_execution import canonical_json_value
from ai_trading_system.contracts.named_data_quality_execution import (
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    NamedDQExecutionReceipt,
    NamedDQScope,
    NamedDQSuccessfulDispatchBinding,
    NamedEqualRiskPriceScope,
)

# Existing forward-aging candidate identities/roles, not a new selection policy.
# See TRADING-2564_S2c2_Five_Candidate_Read_Only_Preview_V1.md §4.
FROZEN_PREVIEW_CANDIDATES = (
    ("equal_risk_qqq_sgov", "PRIMARY_FORWARD_AGING", "equal_risk_qqq_sgov"),
    ("qqq_50_sgov_50", "STATIC_COMPARATOR", "qqq_50_sgov_50"),
    ("qqq_60_sgov_40", "STATIC_COMPARATOR", "qqq_60_sgov_40"),
    ("100_qqq", "RISK_REFERENCE", "qqq_100_static"),
    ("dyn_tqqq_capped_trend", "DYNAMIC_CHALLENGER", "dyn_tqqq_capped_trend"),
)


@dataclass(frozen=True)
class SimpleBaselineCandidatePreview:
    candidate_id: str
    candidate_role: str
    registry_strategy_id: str
    rebalance_frequency: str
    target_session: date
    target_rebalance_session: date
    signal_price_through: date | None
    legacy_applied_session: date
    target_weights: tuple[tuple[str, float], ...]
    required_lookback_price_rows: int
    available_at_rebalance_price_rows: int
    zero_volatility_initialization: bool

    def __post_init__(self) -> None:
        if (self.candidate_id, self.candidate_role, self.registry_strategy_id) not in (
            FROZEN_PREVIEW_CANDIDATES
        ):
            raise ValueError("unknown frozen preview candidate identity")
        dynamic = self.candidate_id == "dyn_tqqq_capped_trend"
        if self.rebalance_frequency != ("daily" if dynamic else "monthly"):
            raise ValueError("unknown frozen rebalance frequency")
        if (
            type(self.target_weights) is not tuple
            or not self.target_weights
            or any(type(pair) is not tuple or len(pair) != 2 for pair in self.target_weights)
            or len(dict(self.target_weights)) != len(self.target_weights)
            or any(
                ticker not in {"QQQ", "TQQQ", "SGOV"}
                or type(weight) is not float
                or not math.isfinite(weight)
                or not 0 <= weight <= 1
                for ticker, weight in self.target_weights
            )
        ):
            raise ValueError("invalid finite target weights")
        # One machine-ulp allowance per summand is a floating-point invariant,
        # not an allocation tolerance or a configurable investment threshold.
        if abs(math.fsum(weight for _, weight in self.target_weights) - 1) > (
            math.ulp(1.0) * len(self.target_weights)
        ):
            raise ValueError("target weights do not sum to one")
        if (
            any(
                type(value) is not date
                for value in (
                    self.target_session,
                    self.target_rebalance_session,
                    self.legacy_applied_session,
                )
            )
            or (
                self.signal_price_through is not None
                and type(self.signal_price_through) is not date
            )
            or self.target_rebalance_session > self.target_session
            or self.legacy_applied_session <= self.target_session
            or (dynamic and self.target_rebalance_session != self.target_session)
            or (
                (self.signal_price_through is not None)
                != (dynamic or self.candidate_id == "equal_risk_qqq_sgov")
            )
            or (
                self.signal_price_through is not None
                and self.signal_price_through >= self.target_rebalance_session
            )
            or type(self.required_lookback_price_rows) is not int
            or type(self.available_at_rebalance_price_rows) is not int
            or self.required_lookback_price_rows < 1
            or self.available_at_rebalance_price_rows < self.required_lookback_price_rows
            or type(self.zero_volatility_initialization) is not bool
        ):
            raise ValueError("invalid preview date/lookback semantics")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_role": self.candidate_role,
            "registry_strategy_id": self.registry_strategy_id,
            "rebalance_frequency": self.rebalance_frequency,
            "target_session": self.target_session.isoformat(),
            "target_rebalance_session": self.target_rebalance_session.isoformat(),
            "signal_price_through": (
                self.signal_price_through.isoformat() if self.signal_price_through else None
            ),
            "legacy_applied_session": self.legacy_applied_session.isoformat(),
            "target_weights": dict(self.target_weights),
            "required_lookback_price_rows": self.required_lookback_price_rows,
            "available_at_rebalance_price_rows": self.available_at_rebalance_price_rows,
            "zero_volatility_initialization": self.zero_volatility_initialization,
        }


@dataclass(frozen=True)
class NamedSimpleBaselinePreview:
    """Finite result DTO. It has no from-receipt constructor or execution method."""

    schema_version: ClassVar[str] = "named_simple_baseline_preview.v1"
    scope: NamedEqualRiskPriceScope
    receipt: NamedDQExecutionReceipt
    successful_dispatch: NamedDQSuccessfulDispatchBinding
    candidates: tuple[SimpleBaselineCandidatePreview, ...]
    source_row_count: int
    consumed_row_count: int
    excluded_before_window_row_count: int
    excluded_other_ticker_row_count: int
    calculation_environment: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if (
            type(self.scope) is not NamedEqualRiskPriceScope
            or type(self.receipt) is not NamedDQExecutionReceipt
            or type(self.successful_dispatch) is not NamedDQSuccessfulDispatchBinding
        ):
            raise ValueError("preview requires exact evidence DTO types")
        self.successful_dispatch.assert_matches_receipt(
            self.receipt, receipt_path=self.successful_dispatch.receipt.relative_path
        )
        required_dq = NamedDQScope(
            as_of=self.scope.as_of,
            requested_window=self.scope.requested_window,
            expected_price_tickers=self.scope.expected_price_tickers,
            expected_rate_series=self.scope.guard_rate_series,
            input_roles=("prices", "rates"),
            require_secondary_prices=False,
        )
        if (
            not self.receipt.request.scope.covers(required_dq)
            or self.scope.registry_binding not in self.receipt.execution_dependencies
            or self.receipt.execution.source_manifest_path
            != FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH
            or self.receipt.execution.source_manifest_sha256
            != FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256
        ):
            raise ValueError("preview scope/registry/source identity differs from its receipt")
        if (
            type(self.calculation_environment) is not tuple
            or any(
                type(pair) is not tuple
                or len(pair) != 2
                or any(type(value) is not str or not value for value in pair)
                for pair in self.calculation_environment
            )
            or len(self.calculation_environment) != 4
            or set(dict(self.calculation_environment))
            != {"python_implementation", "python_version", "pandas_version", "numpy_version"}
        ):
            raise ValueError("preview requires complete immutable calculation environment")
        if (
            type(self.candidates) is not tuple
            or any(type(item) is not SimpleBaselineCandidatePreview for item in self.candidates)
            or tuple(
                (item.candidate_id, item.candidate_role, item.registry_strategy_id)
                for item in self.candidates
            )
            != FROZEN_PREVIEW_CANDIDATES
        ):
            raise ValueError("preview requires all five frozen candidates in reviewed order")
        counts = (
            self.source_row_count,
            self.consumed_row_count,
            self.excluded_before_window_row_count,
            self.excluded_other_ticker_row_count,
        )
        if any(type(count) is not int or count < 0 for count in counts) or counts[0] != sum(
            counts[1:]
        ):
            raise ValueError("preview row coverage does not reconcile")
        if any(item.target_session != self.scope.as_of for item in self.candidates):
            raise ValueError("preview target differs from the requested as-of")
        if self.receipt.report.status != "PASS" or not self.receipt.data_quality_evidence.ready:
            raise ValueError("preview description requires strict data-quality PASS")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "status": "FIVE_CANDIDATE_PREVIEW_READY",
            "scope": self.scope.to_dict(),
            "research_window": "unified_primary_2021",
            "requested_window": self.scope.requested_window.to_dict(),
            "original_dq_requested_window": self.receipt.request.scope.requested_window.to_dict(),
            "original_dq_evaluated_window": self.receipt.evaluated_window.to_dict(),
            "consumed_price_window": self.scope.requested_window.to_dict(),
            "data_quality_status": self.receipt.report.status,
            "data_quality_receipt_id": self.receipt.receipt_id,
            "data_quality_request_id": self.receipt.request.request_id,
            "data_quality_report": self.receipt.report.to_dict(),
            "data_quality_policy": self.receipt.policy.to_dict(),
            "calendar": self.receipt.calendar.to_dict(),
            "registry_binding": self.scope.registry_binding.to_dict(),
            "price_binding": next(
                item.member.to_dict() for item in self.receipt.inputs if item.role == "prices"
            ),
            "execution_identity": self.receipt.execution.to_dict(),
            "successful_original_dispatch": self.successful_dispatch.to_dict(),
            "calculation_environment": dict(self.calculation_environment),
            "source_row_count": self.source_row_count,
            "consumed_row_count": self.consumed_row_count,
            "excluded_before_window_row_count": self.excluded_before_window_row_count,
            "excluded_other_ticker_row_count": self.excluded_other_ticker_row_count,
            "candidates": [item.to_dict() for item in self.candidates],
            "read_only_plan": {
                "status": "PREVIEW_ONLY",
                "candidate_ids": [item.candidate_id for item in self.candidates],
                "prices_role": "FEATURE_INPUT",
                "rates_role": "DQ_GUARD_ONLY",
                "calendar_role": "VERIFIER_SEALED_CANONICAL_SESSIONS",
                "execution_allowed": False,
                "next_gate": "S3_PIT_ACTIVATION_AND_EXACT_CAPTURE_SCOPE",
                "legacy_applied_session_is_execution_evidence": False,
                "summary_zh": (
                    "五候选权重仅为只读计算预览；" "真实前瞻记录仍须通过输入时点、激活与采集准入。"
                ),
            },
            "pit_status": "NOT_ESTABLISHED",
            "oos_status": "NOT_ESTABLISHED",
            "verified_input_seal_exported": False,
            "consumer_cutover_allowed": False,
            "dispatch_allowed": False,
            "dq_validation_executed": False,
            "observation_created": False,
            "outcome_read": False,
            "returns_computed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        payload["preview_id"] = (
            "named_simple_baseline_preview_"
            + hashlib.sha256(canonical_json_value(payload).encode("utf-8")).hexdigest()
        )
        return payload

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_value(self.to_dict()).encode("utf-8")
