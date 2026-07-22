from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.research_context import (
    DateRange,
    ResearchContextError,
    ResearchEvaluationContext,
)
from ai_trading_system.contracts.research_lifecycle import (
    ResearchLifecycleError,
    ResearchPreregistration,
    ResultVisibility,
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

SCHEMA_VERSION = "uncontaminated_selection_protocol.v1"
VALIDATION_SCHEMA_VERSION = "uncontaminated_selection_protocol_validation.v1"
POLICY_SCHEMA_VERSION = "uncontaminated_selection_protocol_foundation_policy.v1"


class SelectionProtocolError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class SelectionDataRole(StrEnum):
    DISCOVERY_HISTORICAL_KNOWN = "DISCOVERY_HISTORICAL_KNOWN"
    TRAIN_SELECTION = "TRAIN_SELECTION"
    HISTORICAL_SEEN_VALIDATION = "HISTORICAL_SEEN_VALIDATION"
    PROSPECTIVE_UNTOUCHED = "PROSPECTIVE_UNTOUCHED"


class MarketOutcomeVisibility(StrEnum):
    KNOWN = "KNOWN"
    UNTOUCHED = "UNTOUCHED"


class SelectionPolicyRole(StrEnum):
    HARD_ELIGIBILITY = "HARD_ELIGIBILITY"
    SOFT_RANKING = "SOFT_RANKING"
    OBSERVE_ONLY_ENABLEMENT = "OBSERVE_ONLY_ENABLEMENT"
    REPORTING_ONLY = "REPORTING_ONLY"


class SelectionProtocolFoundationStatus(StrEnum):
    FOUNDATION_ONLY = "FOUNDATION_ONLY"
    NOT_PREREGISTERED = "NOT_PREREGISTERED"


@dataclass(frozen=True)
class SelectionProtocolFoundationPolicy:
    schema_version: str
    policy_id: str
    version: str
    status: str
    owner: str
    rationale: str
    review_condition: str
    active_market_regime_id: str
    active_market_regime_start: date
    active_research_window_id: str
    active_research_window_start: date
    required_data_roles: tuple[SelectionDataRole, ...]
    forbidden_overlap_pairs: tuple[tuple[SelectionDataRole, SelectionDataRole], ...]
    prohibited_candidate_universe_origins: tuple[str, ...]
    required_false_safety_flags: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "required_data_roles",
            tuple(sorted(set(self.required_data_roles), key=lambda item: item.value)),
        )
        normalized_pairs = {
            tuple(sorted(pair, key=lambda item: item.value))
            for pair in self.forbidden_overlap_pairs
        }
        object.__setattr__(
            self,
            "forbidden_overlap_pairs",
            tuple(sorted(normalized_pairs, key=lambda pair: (pair[0].value, pair[1].value))),
        )
        object.__setattr__(
            self,
            "prohibited_candidate_universe_origins",
            _normalized_text(self.prohibited_candidate_universe_origins),
        )
        object.__setattr__(
            self,
            "required_false_safety_flags",
            _normalized_text(self.required_false_safety_flags),
        )

    @property
    def semantic_sha256(self) -> str:
        return _semantic_sha256(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "version": self.version,
            "status": self.status,
            "owner": self.owner,
            "rationale": self.rationale,
            "review_condition": self.review_condition,
            "active_primary": {
                "market_regime_id": self.active_market_regime_id,
                "market_regime_start": self.active_market_regime_start.isoformat(),
                "research_window_id": self.active_research_window_id,
                "research_window_start": self.active_research_window_start.isoformat(),
            },
            "required_data_roles": [item.value for item in self.required_data_roles],
            "forbidden_overlap_pairs": [
                [left.value, right.value] for left, right in self.forbidden_overlap_pairs
            ],
            "prohibited_candidate_universe_origins": list(
                self.prohibited_candidate_universe_origins
            ),
            "required_false_safety_flags": list(self.required_false_safety_flags),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> SelectionProtocolFoundationPolicy:
        active = _mapping(payload.get("active_primary"), "active_primary")
        return cls(
            schema_version=str(payload.get("schema_version", "")),
            policy_id=str(payload.get("policy_id", "")),
            version=str(payload.get("version", "")),
            status=str(payload.get("status", "")),
            owner=str(payload.get("owner", "")),
            rationale=str(payload.get("rationale", "")),
            review_condition=str(payload.get("review_condition", "")),
            active_market_regime_id=str(active.get("market_regime_id", "")),
            active_market_regime_start=_date_value(
                active.get("market_regime_start"), "active_primary.market_regime_start"
            ),
            active_research_window_id=str(active.get("research_window_id", "")),
            active_research_window_start=_date_value(
                active.get("research_window_start"), "active_primary.research_window_start"
            ),
            required_data_roles=tuple(
                SelectionDataRole(str(item))
                for item in _sequence(payload.get("required_data_roles"), "required_data_roles")
            ),
            forbidden_overlap_pairs=tuple(
                _role_pair(item)
                for item in _sequence(
                    payload.get("forbidden_overlap_pairs"), "forbidden_overlap_pairs"
                )
            ),
            prohibited_candidate_universe_origins=_text_tuple(
                payload.get("prohibited_candidate_universe_origins"),
                "prohibited_candidate_universe_origins",
            ),
            required_false_safety_flags=_text_tuple(
                payload.get("required_false_safety_flags"),
                "required_false_safety_flags",
            ),
        )


@dataclass(frozen=True)
class SelectionDataBinding:
    role: SelectionDataRole
    window_id: str
    date_range: DateRange
    source_id: str
    source_sha256: str
    market_outcome_visibility: MarketOutcomeVisibility
    evaluation_result_visibility_at_freeze: ResultVisibility
    first_evaluation_result_visible_at: datetime | None
    accessed: bool
    candidate_generation_input: bool
    selection_input: bool

    def __post_init__(self) -> None:
        if self.first_evaluation_result_visible_at is not None and (
            self.first_evaluation_result_visible_at.tzinfo is None
            or self.first_evaluation_result_visible_at.utcoffset() is None
        ):
            raise SelectionProtocolError(
                "PROTOCOL_DATETIME_TZ_REQUIRED",
                "first_evaluation_result_visible_at",
            )
        for field in ("accessed", "candidate_generation_input", "selection_input"):
            if not isinstance(getattr(self, field), bool):
                raise SelectionProtocolError("PROTOCOL_BOOLEAN_REQUIRED", field)

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "window_id": self.window_id,
            "date_range": self.date_range.to_dict(),
            "source_id": self.source_id,
            "source_sha256": self.source_sha256,
            "market_outcome_visibility": self.market_outcome_visibility.value,
            "evaluation_result_visibility_at_freeze": (
                self.evaluation_result_visibility_at_freeze.value
            ),
            "first_evaluation_result_visible_at": (
                None
                if self.first_evaluation_result_visible_at is None
                else self.first_evaluation_result_visible_at.isoformat()
            ),
            "accessed": self.accessed,
            "candidate_generation_input": self.candidate_generation_input,
            "selection_input": self.selection_input,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> SelectionDataBinding:
        first_visible = payload.get("first_evaluation_result_visible_at")
        return cls(
            role=SelectionDataRole(str(payload.get("role", ""))),
            window_id=str(payload.get("window_id", "")),
            date_range=DateRange.from_dict(_mapping(payload.get("date_range"), "date_range")),
            source_id=str(payload.get("source_id", "")),
            source_sha256=str(payload.get("source_sha256", "")),
            market_outcome_visibility=MarketOutcomeVisibility(
                str(payload.get("market_outcome_visibility", ""))
            ),
            evaluation_result_visibility_at_freeze=ResultVisibility(
                str(payload.get("evaluation_result_visibility_at_freeze", ""))
            ),
            first_evaluation_result_visible_at=(
                None
                if first_visible is None
                else _datetime_value(first_visible, "first_evaluation_result_visible_at")
            ),
            accessed=_bool_value(payload.get("accessed"), "accessed"),
            candidate_generation_input=_bool_value(
                payload.get("candidate_generation_input"), "candidate_generation_input"
            ),
            selection_input=_bool_value(payload.get("selection_input"), "selection_input"),
        )


@dataclass(frozen=True)
class CandidateUniverseCommitment:
    universe_id: str
    origin: str
    generator_id: str
    generator_version: str
    generator_sha256: str
    universe_sha256: str
    derivation_source_ids: tuple[str, ...]
    derived_from_full_period_ranking: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "derivation_source_ids",
            _normalized_text(self.derivation_source_ids),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "universe_id": self.universe_id,
            "origin": self.origin,
            "generator_id": self.generator_id,
            "generator_version": self.generator_version,
            "generator_sha256": self.generator_sha256,
            "universe_sha256": self.universe_sha256,
            "derivation_source_ids": list(self.derivation_source_ids),
            "derived_from_full_period_ranking": self.derived_from_full_period_ranking,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CandidateUniverseCommitment:
        return cls(
            universe_id=str(payload.get("universe_id", "")),
            origin=str(payload.get("origin", "")),
            generator_id=str(payload.get("generator_id", "")),
            generator_version=str(payload.get("generator_version", "")),
            generator_sha256=str(payload.get("generator_sha256", "")),
            universe_sha256=str(payload.get("universe_sha256", "")),
            derivation_source_ids=_text_tuple(
                payload.get("derivation_source_ids"), "derivation_source_ids"
            ),
            derived_from_full_period_ranking=_bool_value(
                payload.get("derived_from_full_period_ranking"),
                "derived_from_full_period_ranking",
            ),
        )


@dataclass(frozen=True)
class SelectionPolicyBinding:
    policy_id: str
    version: str
    status: str
    owner: str
    rationale: str
    review_condition: str
    source_sha256: str
    intended_roles: tuple[SelectionPolicyRole, ...]
    consumed_roles: tuple[SelectionPolicyRole, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "intended_roles",
            tuple(sorted(set(self.intended_roles), key=lambda item: item.value)),
        )
        object.__setattr__(
            self,
            "consumed_roles",
            tuple(sorted(set(self.consumed_roles), key=lambda item: item.value)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "status": self.status,
            "owner": self.owner,
            "rationale": self.rationale,
            "review_condition": self.review_condition,
            "source_sha256": self.source_sha256,
            "intended_roles": [item.value for item in self.intended_roles],
            "consumed_roles": [item.value for item in self.consumed_roles],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> SelectionPolicyBinding:
        return cls(
            policy_id=str(payload.get("policy_id", "")),
            version=str(payload.get("version", "")),
            status=str(payload.get("status", "")),
            owner=str(payload.get("owner", "")),
            rationale=str(payload.get("rationale", "")),
            review_condition=str(payload.get("review_condition", "")),
            source_sha256=str(payload.get("source_sha256", "")),
            intended_roles=tuple(
                SelectionPolicyRole(str(item))
                for item in _sequence(payload.get("intended_roles"), "intended_roles")
            ),
            consumed_roles=tuple(
                SelectionPolicyRole(str(item))
                for item in _sequence(payload.get("consumed_roles"), "consumed_roles")
            ),
        )


@dataclass(frozen=True)
class SelectionProtocolSafety:
    evaluator_execution_allowed: bool = False
    backtest_execution_allowed: bool = False
    parameter_search_allowed: bool = False
    prospective_holdout_access_allowed: bool = False
    unbiased_oos_claim_allowed: bool = False
    paper_shadow_change_allowed: bool = False
    promotion_allowed: bool = False
    production_weight_change_allowed: bool = False
    broker_action_allowed: bool = False
    manual_review_required: bool = True
    production_effect: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            field: getattr(self, field)
            for field in (
                "evaluator_execution_allowed",
                "backtest_execution_allowed",
                "parameter_search_allowed",
                "prospective_holdout_access_allowed",
                "unbiased_oos_claim_allowed",
                "paper_shadow_change_allowed",
                "promotion_allowed",
                "production_weight_change_allowed",
                "broker_action_allowed",
                "manual_review_required",
                "production_effect",
            )
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> SelectionProtocolSafety:
        bool_fields = (
            "evaluator_execution_allowed",
            "backtest_execution_allowed",
            "parameter_search_allowed",
            "prospective_holdout_access_allowed",
            "unbiased_oos_claim_allowed",
            "paper_shadow_change_allowed",
            "promotion_allowed",
            "production_weight_change_allowed",
            "broker_action_allowed",
            "manual_review_required",
        )
        values = {field: _bool_value(payload.get(field), field) for field in bool_fields}
        return cls(**values, production_effect=str(payload.get("production_effect", "")))


@dataclass(frozen=True)
class UncontaminatedSelectionProtocol:
    schema_version: ClassVar[str] = SCHEMA_VERSION

    protocol_version: str
    foundation_status: SelectionProtocolFoundationStatus
    owner: str
    rationale: str
    review_condition: str
    foundation_policy_id: str
    foundation_policy_sha256: str
    research_context: ResearchEvaluationContext
    preregistration: ResearchPreregistration
    candidate_universe: CandidateUniverseCommitment
    data_bindings: tuple[SelectionDataBinding, ...]
    policy_bindings: tuple[SelectionPolicyBinding, ...]
    safety: SelectionProtocolSafety = SelectionProtocolSafety()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "data_bindings",
            tuple(
                sorted(
                    self.data_bindings,
                    key=lambda item: (item.role.value, item.window_id, item.source_id),
                )
            ),
        )
        object.__setattr__(
            self,
            "policy_bindings",
            tuple(sorted(self.policy_bindings, key=lambda item: item.policy_id)),
        )

    @property
    def protocol_id(self) -> str:
        return f"selection_protocol_{_semantic_sha256(self._semantic_payload())[:20]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "protocol_version": self.protocol_version,
            "foundation_status": self.foundation_status.value,
            "owner": self.owner,
            "rationale": self.rationale,
            "review_condition": self.review_condition,
            "foundation_policy_id": self.foundation_policy_id,
            "foundation_policy_sha256": self.foundation_policy_sha256,
            "research_context": self.research_context.to_dict(),
            "preregistration": self.preregistration.to_dict(),
            "candidate_universe": self.candidate_universe.to_dict(),
            "data_bindings": [item.to_dict() for item in self.data_bindings],
            "policy_bindings": [item.to_dict() for item in self.policy_bindings],
            "safety": self.safety.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {"protocol_id": self.protocol_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> UncontaminatedSelectionProtocol:
        schema = str(payload.get("schema_version", ""))
        if schema != SCHEMA_VERSION:
            raise SelectionProtocolError("PROTOCOL_SCHEMA_MISMATCH", schema)
        protocol = cls(
            protocol_version=str(payload.get("protocol_version", "")),
            foundation_status=SelectionProtocolFoundationStatus(
                str(payload.get("foundation_status", ""))
            ),
            owner=str(payload.get("owner", "")),
            rationale=str(payload.get("rationale", "")),
            review_condition=str(payload.get("review_condition", "")),
            foundation_policy_id=str(payload.get("foundation_policy_id", "")),
            foundation_policy_sha256=str(payload.get("foundation_policy_sha256", "")),
            research_context=ResearchEvaluationContext.from_dict(
                _mapping(payload.get("research_context"), "research_context")
            ),
            preregistration=ResearchPreregistration.from_dict(
                _mapping(payload.get("preregistration"), "preregistration")
            ),
            candidate_universe=CandidateUniverseCommitment.from_dict(
                _mapping(payload.get("candidate_universe"), "candidate_universe")
            ),
            data_bindings=tuple(
                SelectionDataBinding.from_dict(_mapping(item, "data_bindings[]"))
                for item in _sequence(payload.get("data_bindings"), "data_bindings")
            ),
            policy_bindings=tuple(
                SelectionPolicyBinding.from_dict(_mapping(item, "policy_bindings[]"))
                for item in _sequence(payload.get("policy_bindings"), "policy_bindings")
            ),
            safety=SelectionProtocolSafety.from_dict(_mapping(payload.get("safety"), "safety")),
        )
        supplied_id = payload.get("protocol_id")
        if supplied_id is not None and str(supplied_id) != protocol.protocol_id:
            raise SelectionProtocolError(
                "PROTOCOL_ID_MISMATCH",
                f"supplied={supplied_id} actual={protocol.protocol_id}",
            )
        return protocol


def validate_uncontaminated_selection_protocol(
    protocol: UncontaminatedSelectionProtocol,
    *,
    policy: SelectionProtocolFoundationPolicy,
) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    _append_check(
        checks,
        "foundation_policy_governance_complete",
        policy.schema_version == POLICY_SCHEMA_VERSION
        and all(
            value.strip()
            for value in (
                policy.schema_version,
                policy.policy_id,
                policy.version,
                policy.status,
                policy.owner,
                policy.rationale,
                policy.review_condition,
            )
        )
        and policy.status in {"owner_reviewed", "owner_reviewed_foundation", "reviewed"},
        "foundation policy requires schema/id/version/status/owner/rationale/review_condition",
    )
    _append_check(
        checks,
        "foundation_policy_binding_fresh",
        protocol.foundation_policy_id == policy.policy_id
        and protocol.foundation_policy_sha256 == policy.semantic_sha256,
        f"expected={policy.policy_id}:{policy.semantic_sha256}",
    )
    _append_check(
        checks,
        "protocol_governance_complete",
        all(
            value.strip()
            for value in (
                protocol.protocol_version,
                protocol.owner,
                protocol.rationale,
                protocol.review_condition,
            )
        )
        and protocol.foundation_status
        in {
            SelectionProtocolFoundationStatus.FOUNDATION_ONLY,
            SelectionProtocolFoundationStatus.NOT_PREREGISTERED,
        },
        "protocol requires version/owner/rationale/review_condition",
    )
    _append_check(
        checks,
        "foundation_status_ready_for_authoring",
        protocol.foundation_status is SelectionProtocolFoundationStatus.FOUNDATION_ONLY,
        f"foundation_status={protocol.foundation_status.value}",
    )

    context = protocol.research_context
    _append_check(
        checks,
        "active_primary_context",
        context.market_regime_id == policy.active_market_regime_id
        and context.regime_start == policy.active_market_regime_start
        and context.research_window_id == policy.active_research_window_id
        and context.research_window_start == policy.active_research_window_start,
        (
            f"actual={context.market_regime_id}:{context.regime_start.isoformat()}/"
            f"{context.research_window_id}:{context.research_window_start.isoformat()}"
        ),
    )
    preregistration = protocol.preregistration
    _append_check(
        checks,
        "canonical_lifecycle_binding",
        preregistration.research_context_id == context.context_id
        and preregistration.owner == protocol.owner
        and preregistration.candidate_id == protocol.candidate_universe.universe_id
        and preregistration.result_visibility is ResultVisibility.NONE,
        "context/owner/candidate identity and result_visibility=NONE must match",
    )

    universe = protocol.candidate_universe
    discovery_source_ids = {
        item.source_id
        for item in protocol.data_bindings
        if item.role is SelectionDataRole.DISCOVERY_HISTORICAL_KNOWN
    }
    candidate_fields_complete = all(
        value.strip()
        for value in (
            universe.universe_id,
            universe.origin,
            universe.generator_id,
            universe.generator_version,
        )
    ) and all(
        _SHA256_PATTERN.fullmatch(value)
        for value in (universe.generator_sha256, universe.universe_sha256)
    )
    _append_check(
        checks,
        "candidate_universe_commitments_complete",
        candidate_fields_complete and bool(universe.derivation_source_ids),
        "candidate universe requires identity, generator/version/hash, universe hash and sources",
    )
    _append_check(
        checks,
        "candidate_universe_not_result_ranked",
        universe.origin not in policy.prohibited_candidate_universe_origins
        and not universe.derived_from_full_period_ranking,
        f"origin={universe.origin} full_period_ranked={universe.derived_from_full_period_ranking}",
    )
    _append_check(
        checks,
        "candidate_universe_uses_discovery_sources_only",
        bool(universe.derivation_source_ids)
        and set(universe.derivation_source_ids) <= discovery_source_ids,
        f"derivation_sources={','.join(universe.derivation_source_ids)}",
    )

    required_roles = set(policy.required_data_roles)
    role_counts = {
        role: sum(item.role is role for item in protocol.data_bindings) for role in required_roles
    }
    _append_check(
        checks,
        "four_data_roles_complete",
        required_roles == set(SelectionDataRole)
        and all(count == 1 for count in role_counts.values())
        and len(protocol.data_bindings) == len(required_roles),
        ",".join(f"{role.value}={role_counts.get(role, 0)}" for role in sorted(required_roles)),
    )

    binding_details: list[str] = []
    expected_role_flags = {
        SelectionDataRole.DISCOVERY_HISTORICAL_KNOWN: (
            MarketOutcomeVisibility.KNOWN,
            True,
            False,
            True,
        ),
        SelectionDataRole.TRAIN_SELECTION: (
            MarketOutcomeVisibility.KNOWN,
            False,
            True,
            True,
        ),
        SelectionDataRole.HISTORICAL_SEEN_VALIDATION: (
            MarketOutcomeVisibility.KNOWN,
            False,
            False,
            True,
        ),
        SelectionDataRole.PROSPECTIVE_UNTOUCHED: (
            MarketOutcomeVisibility.UNTOUCHED,
            False,
            False,
            False,
        ),
    }
    for binding in protocol.data_bindings:
        expected_visibility, candidate_input, selection_input, accessed = expected_role_flags[
            binding.role
        ]
        valid = (
            bool(binding.window_id.strip())
            and bool(binding.source_id.strip())
            and bool(_SHA256_PATTERN.fullmatch(binding.source_sha256))
            and binding.market_outcome_visibility is expected_visibility
            and binding.candidate_generation_input is candidate_input
            and binding.selection_input is selection_input
            and binding.accessed is accessed
        )
        if not valid:
            binding_details.append(binding.role.value)
    _append_check(
        checks,
        "data_role_semantics",
        not binding_details,
        ",".join(binding_details) or "all role semantics match",
    )

    overlap_details: list[str] = []
    by_role = {item.role: item for item in protocol.data_bindings}
    for left_role, right_role in policy.forbidden_overlap_pairs:
        left = by_role.get(left_role)
        right = by_role.get(right_role)
        if left is not None and right is not None and _overlaps(left.date_range, right.date_range):
            overlap_details.append(f"{left_role.value}<->{right_role.value}")
    _append_check(
        checks,
        "role_windows_do_not_overlap",
        not overlap_details,
        ",".join(overlap_details) or "no forbidden overlap",
    )

    visible_details: list[str] = []
    for binding in protocol.data_bindings:
        if binding.role is SelectionDataRole.DISCOVERY_HISTORICAL_KNOWN:
            continue
        if binding.evaluation_result_visibility_at_freeze is not ResultVisibility.NONE:
            visible_details.append(f"{binding.role.value}:visibility")
        first_visible = binding.first_evaluation_result_visible_at
        if first_visible is not None and first_visible <= preregistration.frozen_at:
            visible_details.append(f"{binding.role.value}:visible_at_or_before_freeze")
    _append_check(
        checks,
        "results_unseen_at_freeze",
        not visible_details,
        ",".join(visible_details) or "selection and validation results unseen at freeze",
    )

    prospective = by_role.get(SelectionDataRole.PROSPECTIVE_UNTOUCHED)
    _append_check(
        checks,
        "prospective_holdout_untouched",
        prospective is not None
        and not prospective.accessed
        and prospective.market_outcome_visibility is MarketOutcomeVisibility.UNTOUCHED
        and prospective.evaluation_result_visibility_at_freeze is ResultVisibility.NONE
        and prospective.first_evaluation_result_visible_at is None,
        "prospective role must remain inaccessible and result-free",
    )

    policy_ids = [item.policy_id for item in protocol.policy_bindings]
    metadata_failures: list[str] = []
    role_failures: list[str] = []
    for binding in protocol.policy_bindings:
        if not all(
            value.strip()
            for value in (
                binding.policy_id,
                binding.version,
                binding.status,
                binding.owner,
                binding.rationale,
                binding.review_condition,
            )
        ) or not _SHA256_PATTERN.fullmatch(binding.source_sha256):
            metadata_failures.append(binding.policy_id or "missing_policy_id")
        if not binding.consumed_roles or not set(binding.consumed_roles) <= set(
            binding.intended_roles
        ):
            role_failures.append(binding.policy_id or "missing_policy_id")
        if (
            SelectionPolicyRole.HARD_ELIGIBILITY in binding.consumed_roles
            and SelectionPolicyRole.HARD_ELIGIBILITY not in binding.intended_roles
        ):
            role_failures.append(binding.policy_id or "missing_policy_id")
    _append_check(
        checks,
        "selection_policy_governance_complete",
        bool(protocol.policy_bindings)
        and len(policy_ids) == len(set(policy_ids))
        and not metadata_failures,
        ",".join(sorted(set(metadata_failures))) or "all policy metadata complete",
    )
    _append_check(
        checks,
        "selection_policy_roles_compatible",
        not role_failures,
        ",".join(sorted(set(role_failures))) or "consumption is within intended roles",
    )
    selection_binding = next(
        (
            item
            for item in protocol.policy_bindings
            if item.policy_id == preregistration.selection_rule_id
        ),
        None,
    )
    _append_check(
        checks,
        "selection_rule_content_binding",
        selection_binding is not None
        and selection_binding.source_sha256 == preregistration.selection_rule_sha256
        and set(policy_ids) <= set(preregistration.policy_ref_ids),
        "selection rule and all governed policies must be bound by canonical preregistration",
    )

    safety_payload = protocol.safety.to_dict()
    invalid_safety = [
        field
        for field in policy.required_false_safety_flags
        if safety_payload.get(field) is not False
    ]
    if protocol.safety.manual_review_required is not True:
        invalid_safety.append("manual_review_required")
    if protocol.safety.production_effect != "none":
        invalid_safety.append("production_effect")
    _append_check(
        checks,
        "foundation_safety_closed",
        not invalid_safety,
        ",".join(sorted(set(invalid_safety))) or "all execution and effect flags closed",
    )

    failed = [item for item in checks if item["status"] == "FAIL"]
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "protocol_id": protocol.protocol_id,
        "status": "PASS" if not failed else "BLOCKED",
        "foundation_status": protocol.foundation_status.value,
        "admission_status": (
            "READY_FOR_OWNER_PROTOCOL_AUTHORING" if not failed else "BLOCKED_PROTOCOL_CONTRACT"
        ),
        "failed_check_count": len(failed),
        "checks": checks,
        "execution_unblocked": False,
        "prospective_holdout_access_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_uncontaminated_selection_protocol_payload(
    payload: Mapping[str, object],
    *,
    policy: SelectionProtocolFoundationPolicy,
) -> dict[str, object]:
    try:
        protocol = UncontaminatedSelectionProtocol.from_dict(payload)
    except (
        KeyError,
        TypeError,
        ValueError,
        ResearchContextError,
        ResearchLifecycleError,
        SelectionProtocolError,
    ) as exc:
        code = getattr(exc, "code", type(exc).__name__)
        return {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "protocol_id": payload.get("protocol_id"),
            "status": "BLOCKED",
            "admission_status": "BLOCKED_PROTOCOL_TAMPER_OR_SCHEMA",
            "failed_check_count": 1,
            "checks": [
                {
                    "check_id": "protocol_content_recomputed",
                    "status": "FAIL",
                    "details": [f"{code}:{exc}"],
                }
            ],
            "execution_unblocked": False,
            "prospective_holdout_access_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    return validate_uncontaminated_selection_protocol(protocol, policy=policy)


def _append_check(
    checks: list[dict[str, object]],
    check_id: str,
    passed: bool,
    detail: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "details": [detail],
        }
    )


def _semantic_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _overlaps(left: DateRange, right: DateRange) -> bool:
    return left.start <= right.end and right.start <= left.end


def _normalized_text(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({value.strip() for value in values if value.strip()}))


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SelectionProtocolError("PROTOCOL_MAPPING_REQUIRED", field)
    return value


def _sequence(value: object, field: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SelectionProtocolError("PROTOCOL_SEQUENCE_REQUIRED", field)
    return value


def _text_tuple(value: object, field: str) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value, field))


def _date_value(value: object, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise SelectionProtocolError("PROTOCOL_DATE_INVALID", field) from exc


def _datetime_value(value: object, field: str) -> datetime:
    try:
        result = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise SelectionProtocolError("PROTOCOL_DATETIME_INVALID", field) from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise SelectionProtocolError("PROTOCOL_DATETIME_TZ_REQUIRED", field)
    return result


def _bool_value(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise SelectionProtocolError("PROTOCOL_BOOLEAN_REQUIRED", field)
    return value


def _role_pair(value: object) -> tuple[SelectionDataRole, SelectionDataRole]:
    items = _sequence(value, "forbidden_overlap_pairs[]")
    if len(items) != 2:
        raise SelectionProtocolError(
            "PROTOCOL_ROLE_PAIR_REQUIRED", "forbidden overlap pair must have two roles"
        )
    return SelectionDataRole(str(items[0])), SelectionDataRole(str(items[1]))
