from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

ATLAS_STATUS_EXPLANATION_STAGE_IDS = (
    "DATA_INPUTS",
    "DATA_QUALITY_GATE",
    "RESEARCH_MAINLINE",
    "BACKTEST_AND_EVALUATION",
    "RESULT_ATTRIBUTION",
    "ATLAS_SNAPSHOT_DIFF",
    "CITATION_FIRST_QUERY",
    "OWNER_DECISION_BOUNDARY",
)


class StrategyResearchStatusExplanationContractError(ValueError):
    pass


class ExplanationValueState(StrEnum):
    PRESENT = "PRESENT"
    NOT_RECORDED = "NOT_RECORDED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    NOT_YET_DUE = "NOT_YET_DUE"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    OWNER_DECISION_PENDING = "OWNER_DECISION_PENDING"


class ExplanationFactKind(StrEnum):
    CURRENT_WORK = "CURRENT_WORK"
    COMPLETED_MILESTONE = "COMPLETED_MILESTONE"
    UNMET_CONDITION = "UNMET_CONDITION"
    EVIDENCE_GAP = "EVIDENCE_GAP"
    READER_IMPACT = "READER_IMPACT"
    RESPONSIBLE_ROLE = "RESPONSIBLE_ROLE"


class ExplanationAuthorityKind(StrEnum):
    CANONICAL_SNAPSHOT = "CANONICAL_SNAPSHOT"
    CANONICAL_NODE = "CANONICAL_NODE"
    CANONICAL_RESULT = "CANONICAL_RESULT"
    ATTRIBUTION = "ATTRIBUTION"
    GOVERNED_TASK = "GOVERNED_TASK"
    OWNER_DECISION = "OWNER_DECISION"
    INDEPENDENT_VALIDATION = "INDEPENDENT_VALIDATION"
    PAGE_EXECUTION_BOUNDARY = "PAGE_EXECUTION_BOUNDARY"
    STABLE_READER_SEMANTICS = "STABLE_READER_SEMANTICS"


class ExplanationTargetKind(StrEnum):
    NODE = "NODE"
    RESULT = "RESULT"
    ATTRIBUTION = "ATTRIBUTION"
    RENDERER_STAGE = "RENDERER_STAGE"


class ExplanationValidationSummary(StrEnum):
    PASS = "PASS"
    INSUFFICIENT_AUTHORITY = "INSUFFICIENT_AUTHORITY"


def _required_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_REQUIRED_FIELD:{field}"
        )


def _require_exact_keys(
    payload: Mapping[str, object],
    expected: set[str],
    field: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_SCHEMA_KEYS_INVALID:{field}:missing={missing}:extra={extra}"
        )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_MAPPING_REQUIRED:{field}"
        )
    return value


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_LIST_REQUIRED:{field}"
        )
    return tuple(_mapping(item, field) for item in value)


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_LIST_REQUIRED:{field}"
        )
    result = tuple(str(item) for item in value)
    if any(not item.strip() for item in result):
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_EMPTY_LIST_ITEM:{field}"
        )
    return result


def _require_unique(values: Sequence[str], field: str) -> None:
    if len(set(values)) != len(values):
        raise StrategyResearchStatusExplanationContractError(
            f"STATUS_EXPLANATION_DUPLICATE_VALUE:{field}"
        )


@dataclass(frozen=True)
class ExplanationAuthorityBinding:
    authority_kind: ExplanationAuthorityKind
    authority_id: str
    source_ref_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _required_text(self.authority_id, "authority_binding.authority_id")
        if not self.source_ref_ids:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_AUTHORITY_SOURCE_REF_REQUIRED"
            )
        _require_unique(self.source_ref_ids, "authority_binding.source_ref_ids")

    def to_dict(self) -> dict[str, object]:
        return {
            "authority_kind": self.authority_kind.value,
            "authority_id": self.authority_id,
            "source_ref_ids": list(self.source_ref_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplanationAuthorityBinding:
        _require_exact_keys(
            payload,
            {"authority_kind", "authority_id", "source_ref_ids"},
            "authority_binding",
        )
        return cls(
            authority_kind=ExplanationAuthorityKind(str(payload["authority_kind"])),
            authority_id=str(payload["authority_id"]),
            source_ref_ids=_string_tuple(
                payload["source_ref_ids"], "authority_binding.source_ref_ids"
            ),
        )


@dataclass(frozen=True)
class CitedExplanationFact:
    fact_id: str
    fact_kind: ExplanationFactKind
    value_state: ExplanationValueState
    text_zh: str
    authority_kind: ExplanationAuthorityKind
    authority_id: str
    source_ref_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _required_text(self.fact_id, "fact.fact_id")
        _required_text(self.text_zh, "fact.text_zh")
        _required_text(self.authority_id, "fact.authority_id")
        _require_unique(self.source_ref_ids, f"fact:{self.fact_id}.source_ref_ids")
        if self.value_state is ExplanationValueState.PRESENT and not self.source_ref_ids:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_PRESENT_FACT_UNCITED:{self.fact_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "fact_id": self.fact_id,
            "fact_kind": self.fact_kind.value,
            "value_state": self.value_state.value,
            "text_zh": self.text_zh,
            "authority_kind": self.authority_kind.value,
            "authority_id": self.authority_id,
            "source_ref_ids": list(self.source_ref_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CitedExplanationFact:
        _require_exact_keys(
            payload,
            {
                "fact_id",
                "fact_kind",
                "value_state",
                "text_zh",
                "authority_kind",
                "authority_id",
                "source_ref_ids",
            },
            "fact",
        )
        return cls(
            fact_id=str(payload["fact_id"]),
            fact_kind=ExplanationFactKind(str(payload["fact_kind"])),
            value_state=ExplanationValueState(str(payload["value_state"])),
            text_zh=str(payload["text_zh"]),
            authority_kind=ExplanationAuthorityKind(str(payload["authority_kind"])),
            authority_id=str(payload["authority_id"]),
            source_ref_ids=_string_tuple(payload["source_ref_ids"], "fact.source_ref_ids"),
        )


@dataclass(frozen=True)
class ExplanationTransitionCondition:
    condition_id: str
    value_state: ExplanationValueState
    description_zh: str
    current_state: str | None
    observable_event: str | None
    deciding_authority_kind: ExplanationAuthorityKind | None
    deciding_authority_id: str | None
    target_status: str | None
    source_ref_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _required_text(self.condition_id, "transition.condition_id")
        _required_text(self.description_zh, "transition.description_zh")
        _require_unique(self.source_ref_ids, f"transition:{self.condition_id}.source_ref_ids")
        optional_values = (
            self.current_state,
            self.observable_event,
            self.deciding_authority_kind,
            self.deciding_authority_id,
            self.target_status,
        )
        if self.value_state is ExplanationValueState.PRESENT:
            if any(value is None for value in optional_values) or not self.source_ref_ids:
                raise StrategyResearchStatusExplanationContractError(
                    f"STATUS_EXPLANATION_PRESENT_TRANSITION_INCOMPLETE:{self.condition_id}"
                )
            for value, field in (
                (self.current_state or "", "current_state"),
                (self.observable_event or "", "observable_event"),
                (self.deciding_authority_id or "", "deciding_authority_id"),
                (self.target_status or "", "target_status"),
            ):
                _required_text(value, f"transition.{field}")
        elif any(value is not None for value in optional_values) or self.source_ref_ids:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_NON_PRESENT_TRANSITION_HAS_HIDDEN_FACTS:{self.condition_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "condition_id": self.condition_id,
            "value_state": self.value_state.value,
            "description_zh": self.description_zh,
            "current_state": self.current_state,
            "observable_event": self.observable_event,
            "deciding_authority_kind": (
                None if self.deciding_authority_kind is None else self.deciding_authority_kind.value
            ),
            "deciding_authority_id": self.deciding_authority_id,
            "target_status": self.target_status,
            "source_ref_ids": list(self.source_ref_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplanationTransitionCondition:
        _require_exact_keys(
            payload,
            {
                "condition_id",
                "value_state",
                "description_zh",
                "current_state",
                "observable_event",
                "deciding_authority_kind",
                "deciding_authority_id",
                "target_status",
                "source_ref_ids",
            },
            "transition",
        )
        authority_kind = payload["deciding_authority_kind"]
        return cls(
            condition_id=str(payload["condition_id"]),
            value_state=ExplanationValueState(str(payload["value_state"])),
            description_zh=str(payload["description_zh"]),
            current_state=(
                None if payload["current_state"] is None else str(payload["current_state"])
            ),
            observable_event=(
                None if payload["observable_event"] is None else str(payload["observable_event"])
            ),
            deciding_authority_kind=(
                None if authority_kind is None else ExplanationAuthorityKind(str(authority_kind))
            ),
            deciding_authority_id=(
                None
                if payload["deciding_authority_id"] is None
                else str(payload["deciding_authority_id"])
            ),
            target_status=(
                None if payload["target_status"] is None else str(payload["target_status"])
            ),
            source_ref_ids=_string_tuple(payload["source_ref_ids"], "transition.source_ref_ids"),
        )


@dataclass(frozen=True)
class StatusExplanationRecord:
    schema_version: ClassVar[str] = "strategy_research_status_explanation_record.v1"

    explanation_id: str
    stage_id: str
    target_kind: ExplanationTargetKind
    target_id: str
    status_code: str
    status_object_scope: str
    plain_summary: str
    derived_from_fact_ids: tuple[str, ...]
    facts: tuple[CitedExplanationFact, ...]
    transition_conditions: tuple[ExplanationTransitionCondition, ...]
    responsible_role: CitedExplanationFact
    next_reader_action: str
    technical_refs: tuple[str, ...]
    checked_authority_scope: tuple[str, ...]
    checked_authority_ids: tuple[str, ...]
    authority_bindings: tuple[ExplanationAuthorityBinding, ...]
    template_version: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.explanation_id, "record.explanation_id"),
            (self.stage_id, "record.stage_id"),
            (self.target_id, "record.target_id"),
            (self.status_code, "record.status_code"),
            (self.status_object_scope, "record.status_object_scope"),
            (self.plain_summary, "record.plain_summary"),
            (self.next_reader_action, "record.next_reader_action"),
            (self.template_version, "record.template_version"),
        ):
            _required_text(value, field)
        if self.stage_id not in ATLAS_STATUS_EXPLANATION_STAGE_IDS:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_STAGE_ID_INVALID:{self.stage_id}"
            )
        if self.target_kind is ExplanationTargetKind.RENDERER_STAGE and (
            self.target_id != self.stage_id
        ):
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_RENDERER_STAGE_TARGET_INVALID:{self.stage_id}"
            )
        fact_ids = tuple(item.fact_id for item in self.facts) + (self.responsible_role.fact_id,)
        _require_unique(fact_ids, f"record:{self.stage_id}.fact_ids")
        if self.responsible_role.fact_kind is not ExplanationFactKind.RESPONSIBLE_ROLE:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_ROLE_FACT_KIND_INVALID:{self.stage_id}"
            )
        required_fact_kinds = {
            ExplanationFactKind.CURRENT_WORK,
            ExplanationFactKind.COMPLETED_MILESTONE,
            ExplanationFactKind.UNMET_CONDITION,
            ExplanationFactKind.EVIDENCE_GAP,
            ExplanationFactKind.READER_IMPACT,
        }
        actual_fact_kinds = {item.fact_kind for item in self.facts}
        missing_kinds = required_fact_kinds - actual_fact_kinds
        if missing_kinds:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_REQUIRED_FACT_KIND_MISSING:"
                + self.stage_id
                + ":"
                + ",".join(sorted(item.value for item in missing_kinds))
            )
        if not self.derived_from_fact_ids:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_SUMMARY_FACT_REQUIRED:{self.stage_id}"
            )
        if not set(self.derived_from_fact_ids).issubset(fact_ids):
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_SUMMARY_FACT_UNKNOWN:{self.stage_id}"
            )
        transition_ids = tuple(item.condition_id for item in self.transition_conditions)
        if not transition_ids:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_TRANSITION_REQUIRED:{self.stage_id}"
            )
        _require_unique(transition_ids, f"record:{self.stage_id}.transition_ids")
        for values, field in (
            (self.technical_refs, "technical_refs"),
            (self.checked_authority_scope, "checked_authority_scope"),
            (self.checked_authority_ids, "checked_authority_ids"),
        ):
            if not values:
                raise StrategyResearchStatusExplanationContractError(
                    f"STATUS_EXPLANATION_RECORD_LIST_REQUIRED:{self.stage_id}:{field}"
                )
            _require_unique(values, f"record:{self.stage_id}.{field}")
        if not self.authority_bindings:
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_AUTHORITY_BINDING_REQUIRED:{self.stage_id}"
            )
        if any(
            item.value_state is ExplanationValueState.NOT_RECORDED
            for item in (*self.facts, self.responsible_role)
        ) and (not self.checked_authority_scope or not self.checked_authority_ids):
            raise StrategyResearchStatusExplanationContractError(
                f"STATUS_EXPLANATION_NOT_RECORDED_SCOPE_REQUIRED:{self.stage_id}"
            )

    @property
    def has_insufficient_authority(self) -> bool:
        return any(
            item.value_state is not ExplanationValueState.PRESENT
            for item in (*self.facts, self.responsible_role)
        ) or any(
            item.value_state is not ExplanationValueState.PRESENT
            for item in self.transition_conditions
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "explanation_id": self.explanation_id,
            "stage_id": self.stage_id,
            "target_kind": self.target_kind.value,
            "target_id": self.target_id,
            "status_code": self.status_code,
            "status_object_scope": self.status_object_scope,
            "plain_summary": self.plain_summary,
            "derived_from_fact_ids": list(self.derived_from_fact_ids),
            "facts": [item.to_dict() for item in self.facts],
            "transition_conditions": [item.to_dict() for item in self.transition_conditions],
            "responsible_role": self.responsible_role.to_dict(),
            "next_reader_action": self.next_reader_action,
            "technical_refs": list(self.technical_refs),
            "checked_authority_scope": list(self.checked_authority_scope),
            "checked_authority_ids": list(self.checked_authority_ids),
            "authority_bindings": [item.to_dict() for item in self.authority_bindings],
            "template_version": self.template_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> StatusExplanationRecord:
        _require_exact_keys(
            payload,
            {
                "schema_version",
                "explanation_id",
                "stage_id",
                "target_kind",
                "target_id",
                "status_code",
                "status_object_scope",
                "plain_summary",
                "derived_from_fact_ids",
                "facts",
                "transition_conditions",
                "responsible_role",
                "next_reader_action",
                "technical_refs",
                "checked_authority_scope",
                "checked_authority_ids",
                "authority_bindings",
                "template_version",
            },
            "record",
        )
        if payload["schema_version"] != cls.schema_version:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_RECORD_SCHEMA_VERSION_INVALID"
            )
        return cls(
            explanation_id=str(payload["explanation_id"]),
            stage_id=str(payload["stage_id"]),
            target_kind=ExplanationTargetKind(str(payload["target_kind"])),
            target_id=str(payload["target_id"]),
            status_code=str(payload["status_code"]),
            status_object_scope=str(payload["status_object_scope"]),
            plain_summary=str(payload["plain_summary"]),
            derived_from_fact_ids=_string_tuple(
                payload["derived_from_fact_ids"], "record.derived_from_fact_ids"
            ),
            facts=tuple(
                CitedExplanationFact.from_dict(item)
                for item in _mapping_tuple(payload["facts"], "record.facts")
            ),
            transition_conditions=tuple(
                ExplanationTransitionCondition.from_dict(item)
                for item in _mapping_tuple(
                    payload["transition_conditions"], "record.transition_conditions"
                )
            ),
            responsible_role=CitedExplanationFact.from_dict(
                _mapping(payload["responsible_role"], "record.responsible_role")
            ),
            next_reader_action=str(payload["next_reader_action"]),
            technical_refs=_string_tuple(payload["technical_refs"], "record.technical_refs"),
            checked_authority_scope=_string_tuple(
                payload["checked_authority_scope"], "record.checked_authority_scope"
            ),
            checked_authority_ids=_string_tuple(
                payload["checked_authority_ids"], "record.checked_authority_ids"
            ),
            authority_bindings=tuple(
                ExplanationAuthorityBinding.from_dict(item)
                for item in _mapping_tuple(
                    payload["authority_bindings"], "record.authority_bindings"
                )
            ),
            template_version=str(payload["template_version"]),
        )


@dataclass(frozen=True)
class StrategyResearchStatusExplanationBundle:
    schema_id: ClassVar[str] = "strategy_research_status_explanation.v1"
    schema_version: ClassVar[str] = "1.0.0"

    snapshot_id: str
    snapshot_fingerprint: str
    primary_research_start: str
    excluded_task_ids: tuple[str, ...]
    explanation_records: tuple[StatusExplanationRecord, ...]
    validation_summary: ExplanationValidationSummary
    policy_id: str
    policy_version: str
    policy_sha256: str
    content_sha256: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.snapshot_id, "bundle.snapshot_id"),
            (self.snapshot_fingerprint, "bundle.snapshot_fingerprint"),
            (self.primary_research_start, "bundle.primary_research_start"),
            (self.policy_id, "bundle.policy_id"),
            (self.policy_version, "bundle.policy_version"),
        ):
            _required_text(value, field)
        if not _SHA256_PATTERN.fullmatch(self.snapshot_id):
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_SNAPSHOT_ID_INVALID"
            )
        if self.snapshot_fingerprint != self.snapshot_id:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_SNAPSHOT_FINGERPRINT_MISMATCH"
            )
        if not _SHA256_PATTERN.fullmatch(self.policy_sha256):
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_POLICY_SHA256_INVALID"
            )
        if not _SHA256_PATTERN.fullmatch(self.content_sha256):
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_CONTENT_SHA256_INVALID"
            )
        if not self.excluded_task_ids:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_EXCLUDED_TASK_IDS_REQUIRED"
            )
        _require_unique(self.excluded_task_ids, "bundle.excluded_task_ids")
        stage_ids = tuple(item.stage_id for item in self.explanation_records)
        if stage_ids != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_STAGE_SET_OR_ORDER_INVALID"
            )
        explanation_ids = tuple(item.explanation_id for item in self.explanation_records)
        _require_unique(explanation_ids, "bundle.explanation_ids")
        expected_summary = (
            ExplanationValidationSummary.INSUFFICIENT_AUTHORITY
            if any(item.has_insufficient_authority for item in self.explanation_records)
            else ExplanationValidationSummary.PASS
        )
        if self.validation_summary is not expected_summary:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_VALIDATION_SUMMARY_INVALID"
            )
        if self.compute_content_sha256() != self.content_sha256:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_CONTENT_SHA256_MISMATCH"
            )

    @classmethod
    def seal(
        cls,
        *,
        snapshot_id: str,
        primary_research_start: str,
        excluded_task_ids: tuple[str, ...],
        explanation_records: tuple[StatusExplanationRecord, ...],
        policy_id: str,
        policy_version: str,
        policy_sha256: str,
    ) -> StrategyResearchStatusExplanationBundle:
        validation_summary = (
            ExplanationValidationSummary.INSUFFICIENT_AUTHORITY
            if any(item.has_insufficient_authority for item in explanation_records)
            else ExplanationValidationSummary.PASS
        )
        provisional = object.__new__(cls)
        object.__setattr__(provisional, "snapshot_id", snapshot_id)
        object.__setattr__(provisional, "snapshot_fingerprint", snapshot_id)
        object.__setattr__(provisional, "primary_research_start", primary_research_start)
        object.__setattr__(provisional, "excluded_task_ids", excluded_task_ids)
        object.__setattr__(provisional, "explanation_records", explanation_records)
        object.__setattr__(provisional, "validation_summary", validation_summary)
        object.__setattr__(provisional, "policy_id", policy_id)
        object.__setattr__(provisional, "policy_version", policy_version)
        object.__setattr__(provisional, "policy_sha256", policy_sha256)
        object.__setattr__(provisional, "content_sha256", "0" * 64)
        content_sha256 = provisional.compute_content_sha256()
        return cls(
            snapshot_id=snapshot_id,
            snapshot_fingerprint=snapshot_id,
            primary_research_start=primary_research_start,
            excluded_task_ids=excluded_task_ids,
            explanation_records=explanation_records,
            validation_summary=validation_summary,
            policy_id=policy_id,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
            content_sha256=content_sha256,
        )

    def _payload_without_content_sha256(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "primary_research_start": self.primary_research_start,
            "excluded_task_ids": list(self.excluded_task_ids),
            "explanation_records": [item.to_dict() for item in self.explanation_records],
            "validation_summary": self.validation_summary.value,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
        }

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self._payload_without_content_sha256(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            **self._payload_without_content_sha256(),
            "content_sha256": self.content_sha256,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> StrategyResearchStatusExplanationBundle:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_JSON_INVALID"
            ) from exc
        data = _mapping(decoded, "bundle")
        _require_exact_keys(
            data,
            {
                "schema_id",
                "schema_version",
                "snapshot_id",
                "snapshot_fingerprint",
                "primary_research_start",
                "excluded_task_ids",
                "explanation_records",
                "validation_summary",
                "policy_id",
                "policy_version",
                "policy_sha256",
                "content_sha256",
            },
            "bundle",
        )
        if data["schema_id"] != cls.schema_id or data["schema_version"] != cls.schema_version:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_BUNDLE_SCHEMA_INVALID"
            )
        bundle = cls(
            snapshot_id=str(data["snapshot_id"]),
            snapshot_fingerprint=str(data["snapshot_fingerprint"]),
            primary_research_start=str(data["primary_research_start"]),
            excluded_task_ids=_string_tuple(data["excluded_task_ids"], "bundle.excluded_task_ids"),
            explanation_records=tuple(
                StatusExplanationRecord.from_dict(item)
                for item in _mapping_tuple(
                    data["explanation_records"], "bundle.explanation_records"
                )
            ),
            validation_summary=ExplanationValidationSummary(str(data["validation_summary"])),
            policy_id=str(data["policy_id"]),
            policy_version=str(data["policy_version"]),
            policy_sha256=str(data["policy_sha256"]),
            content_sha256=str(data["content_sha256"]),
        )
        if bundle.canonical_bytes != payload:
            raise StrategyResearchStatusExplanationContractError(
                "STATUS_EXPLANATION_CANONICAL_BYTES_REQUIRED"
            )
        return bundle


__all__ = [
    "ATLAS_STATUS_EXPLANATION_STAGE_IDS",
    "CitedExplanationFact",
    "ExplanationAuthorityBinding",
    "ExplanationAuthorityKind",
    "ExplanationFactKind",
    "ExplanationTargetKind",
    "ExplanationTransitionCondition",
    "ExplanationValidationSummary",
    "ExplanationValueState",
    "StatusExplanationRecord",
    "StrategyResearchStatusExplanationBundle",
    "StrategyResearchStatusExplanationContractError",
]
