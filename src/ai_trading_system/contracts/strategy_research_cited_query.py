from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import ClassVar

from ai_trading_system.core.production_effect import ProductionEffect

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
_REASON_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


class StrategyResearchCitedQueryContractError(ValueError):
    pass


def _required_text(value: str, field: str) -> None:
    if not value.strip() or value != value.strip():
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_REQUIRED_TEXT_INVALID:{field}"
        )


class CitedQueryQuestionId(StrEnum):
    RESEARCH_MAINLINE_SUMMARY = "RESEARCH_MAINLINE_SUMMARY"
    RESULT_AND_STATUS = "RESULT_AND_STATUS"
    ATTRIBUTION_AND_LIMITATIONS = "ATTRIBUTION_AND_LIMITATIONS"
    SNAPSHOT_CHANGE_EXPLANATION = "SNAPSHOT_CHANGE_EXPLANATION"
    SOURCE_LINEAGE = "SOURCE_LINEAGE"


class CitedQueryTargetKind(StrEnum):
    SOURCE = "SOURCE"
    NODE = "NODE"
    RESULT = "RESULT"
    ATTRIBUTION = "ATTRIBUTION"
    DIFF_CHANGE = "DIFF_CHANGE"


class CitedQueryInputKind(StrEnum):
    SNAPSHOT = "SNAPSHOT"
    DIFF = "DIFF"


class CitedQueryAnswerStatus(StrEnum):
    ANSWERED = "ANSWERED"
    LIMITED = "LIMITED"
    BLOCKED = "BLOCKED"


class CitedQueryReaderProfile(StrEnum):
    LOW_FINANCE_KNOWLEDGE = "LOW_FINANCE_KNOWLEDGE"


@dataclass(frozen=True)
class CitedQueryQuestionSpec:
    schema_version: ClassVar[str] = "strategy_research_cited_query_question_spec.v1"

    question_id: CitedQueryQuestionId
    input_kind: CitedQueryInputKind
    target_kind: CitedQueryTargetKind
    reader_prompt_zh: str

    def __post_init__(self) -> None:
        _required_text(self.reader_prompt_zh, "reader_prompt_zh")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "question_id": self.question_id.value,
            "input_kind": self.input_kind.value,
            "target_kind": self.target_kind.value,
            "reader_prompt_zh": self.reader_prompt_zh,
        }


CITED_QUERY_QUESTION_CATALOG: tuple[CitedQueryQuestionSpec, ...] = (
    CitedQueryQuestionSpec(
        question_id=CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY,
        input_kind=CitedQueryInputKind.SNAPSHOT,
        target_kind=CitedQueryTargetKind.NODE,
        reader_prompt_zh="这条研究主线在研究什么，当前走到了哪里？",
    ),
    CitedQueryQuestionSpec(
        question_id=CitedQueryQuestionId.RESULT_AND_STATUS,
        input_kind=CitedQueryInputKind.SNAPSHOT,
        target_kind=CitedQueryTargetKind.RESULT,
        reader_prompt_zh="这项研究实际得到什么结果，目前是什么状态？",
    ),
    CitedQueryQuestionSpec(
        question_id=CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS,
        input_kind=CitedQueryInputKind.SNAPSHOT,
        target_kind=CitedQueryTargetKind.ATTRIBUTION,
        reader_prompt_zh="哪些因素解释了结果，还有哪些限制？",
    ),
    CitedQueryQuestionSpec(
        question_id=CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION,
        input_kind=CitedQueryInputKind.DIFF,
        target_kind=CitedQueryTargetKind.DIFF_CHANGE,
        reader_prompt_zh="两个研究快照之间发生了什么变化？",
    ),
    CitedQueryQuestionSpec(
        question_id=CitedQueryQuestionId.SOURCE_LINEAGE,
        input_kind=CitedQueryInputKind.SNAPSHOT,
        target_kind=CitedQueryTargetKind.SOURCE,
        reader_prompt_zh="这条信息来自哪里，何时可知？",
    ),
)

_QUESTION_SPEC_BY_ID = {item.question_id: item for item in CITED_QUERY_QUESTION_CATALOG}


@dataclass(frozen=True)
class StrategyResearchCitedQueryRequest:
    schema_version: ClassVar[str] = "strategy_research_cited_query_request.v1"

    request_id: str
    question_id: CitedQueryQuestionId
    input_kind: CitedQueryInputKind
    target_kind: CitedQueryTargetKind
    target_id: str
    snapshot_id: str | None
    diff_id: str | None
    before_snapshot_id: str | None
    after_snapshot_id: str | None
    locale: str = "zh-CN"
    reader_profile: CitedQueryReaderProfile = CitedQueryReaderProfile.LOW_FINANCE_KNOWLEDGE

    def __post_init__(self) -> None:
        _sha256(self.request_id, "request_id")
        _required_text(self.target_id, "target_id")
        if self.locale != "zh-CN":
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_LOCALE_UNSUPPORTED"
            )
        spec = _QUESTION_SPEC_BY_ID[self.question_id]
        if self.input_kind is not spec.input_kind or self.target_kind is not spec.target_kind:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_QUESTION_TARGET_UNSUPPORTED"
            )
        self._validate_input_identity()
        if self.request_id != self.compute_request_id():
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_REQUEST_ID_MISMATCH"
            )

    def _validate_input_identity(self) -> None:
        if self.input_kind is CitedQueryInputKind.SNAPSHOT:
            _sha256(self.snapshot_id, "snapshot_id", required=True)
            if any(
                value is not None
                for value in (self.diff_id, self.before_snapshot_id, self.after_snapshot_id)
            ):
                raise StrategyResearchCitedQueryContractError(
                    "STRATEGY_CITED_QUERY_SNAPSHOT_IDENTITY_INVALID"
                )
            return
        _sha256(self.diff_id, "diff_id", required=True)
        _sha256(self.before_snapshot_id, "before_snapshot_id", required=True)
        _sha256(self.after_snapshot_id, "after_snapshot_id", required=True)
        if self.snapshot_id is not None or self.before_snapshot_id == self.after_snapshot_id:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_DIFF_IDENTITY_INVALID"
            )

    @classmethod
    def build(
        cls,
        *,
        question_id: CitedQueryQuestionId,
        target_id: str,
        snapshot_id: str | None = None,
        diff_id: str | None = None,
        before_snapshot_id: str | None = None,
        after_snapshot_id: str | None = None,
        locale: str = "zh-CN",
        reader_profile: CitedQueryReaderProfile = (
            CitedQueryReaderProfile.LOW_FINANCE_KNOWLEDGE
        ),
    ) -> StrategyResearchCitedQueryRequest:
        spec = _QUESTION_SPEC_BY_ID[question_id]
        values: dict[str, object] = {
            "request_id": "0" * 64,
            "question_id": question_id,
            "input_kind": spec.input_kind,
            "target_kind": spec.target_kind,
            "target_id": target_id,
            "snapshot_id": snapshot_id,
            "diff_id": diff_id,
            "before_snapshot_id": before_snapshot_id,
            "after_snapshot_id": after_snapshot_id,
            "locale": locale,
            "reader_profile": reader_profile,
        }
        provisional = _provisional(cls, values)
        values["request_id"] = provisional.compute_request_id()
        return cls(**values)  # type: ignore[arg-type]

    def _payload_without_request_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "question_id": self.question_id.value,
            "input_kind": self.input_kind.value,
            "target_kind": self.target_kind.value,
            "target_id": self.target_id,
            "snapshot_id": self.snapshot_id,
            "diff_id": self.diff_id,
            "before_snapshot_id": self.before_snapshot_id,
            "after_snapshot_id": self.after_snapshot_id,
            "locale": self.locale,
            "reader_profile": self.reader_profile.value,
        }

    def compute_request_id(self) -> str:
        return _identity(self._payload_without_request_id())

    def to_dict(self) -> dict[str, object]:
        return {"request_id": self.request_id, **self._payload_without_request_id()}

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, object],
    ) -> StrategyResearchCitedQueryRequest:
        return cls(
            request_id=str(payload.get("request_id", "")),
            question_id=CitedQueryQuestionId(str(payload.get("question_id", ""))),
            input_kind=CitedQueryInputKind(str(payload.get("input_kind", ""))),
            target_kind=CitedQueryTargetKind(str(payload.get("target_kind", ""))),
            target_id=str(payload.get("target_id", "")),
            snapshot_id=_optional_text(payload.get("snapshot_id")),
            diff_id=_optional_text(payload.get("diff_id")),
            before_snapshot_id=_optional_text(payload.get("before_snapshot_id")),
            after_snapshot_id=_optional_text(payload.get("after_snapshot_id")),
            locale=str(payload.get("locale", "")),
            reader_profile=CitedQueryReaderProfile(str(payload.get("reader_profile", ""))),
        )


@dataclass(frozen=True)
class CitedQueryCitation:
    schema_version: ClassVar[str] = "strategy_research_cited_query_citation.v1"

    citation_id: str
    target_kind: CitedQueryTargetKind
    target_id: str
    entity_sha256: str | None
    before_entity_sha256: str | None
    after_entity_sha256: str | None
    source_ref_id: str
    source_path: str
    exact_commit: str
    source_sha256: str
    as_of: datetime
    known_at: datetime
    available_at: datetime
    snapshot_id: str | None
    diff_id: str | None

    def __post_init__(self) -> None:
        _sha256(self.citation_id, "citation_id")
        _required_text(self.target_id, "target_id")
        _required_text(self.source_ref_id, "source_ref_id")
        _repository_path(self.source_path)
        if not _GIT_COMMIT_PATTERN.fullmatch(self.exact_commit):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_EXACT_COMMIT_INVALID"
            )
        _sha256(self.source_sha256, "source_sha256", required=True)
        for field, value in (
            ("as_of", self.as_of),
            ("known_at", self.known_at),
            ("available_at", self.available_at),
        ):
            _aware_datetime(value, field)
        if not self.as_of <= self.known_at <= self.available_at:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CITATION_TIME_ORDER_INVALID"
            )
        self._validate_entity_identity()
        if self.citation_id != self.compute_citation_id():
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CITATION_ID_MISMATCH"
            )

    def _validate_entity_identity(self) -> None:
        _sha256(self.entity_sha256, "entity_sha256")
        _sha256(self.before_entity_sha256, "before_entity_sha256")
        _sha256(self.after_entity_sha256, "after_entity_sha256")
        if self.diff_id is None:
            _sha256(self.snapshot_id, "snapshot_id", required=True)
            if (
                self.entity_sha256 is None
                or self.before_entity_sha256 is not None
                or self.after_entity_sha256 is not None
            ):
                raise StrategyResearchCitedQueryContractError(
                    "STRATEGY_CITED_QUERY_SNAPSHOT_CITATION_IDENTITY_INVALID"
                )
            return
        _sha256(self.diff_id, "diff_id", required=True)
        if (
            self.snapshot_id is not None
            or self.entity_sha256 is not None
            or (
                self.before_entity_sha256 is None
                and self.after_entity_sha256 is None
            )
        ):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_DIFF_CITATION_IDENTITY_INVALID"
            )

    @classmethod
    def build(
        cls,
        *,
        target_kind: CitedQueryTargetKind,
        target_id: str,
        source_ref_id: str,
        source_path: str,
        exact_commit: str,
        source_sha256: str,
        as_of: datetime,
        known_at: datetime,
        available_at: datetime,
        entity_sha256: str | None = None,
        before_entity_sha256: str | None = None,
        after_entity_sha256: str | None = None,
        snapshot_id: str | None = None,
        diff_id: str | None = None,
    ) -> CitedQueryCitation:
        values: dict[str, object] = {
            "citation_id": "0" * 64,
            "target_kind": target_kind,
            "target_id": target_id,
            "entity_sha256": entity_sha256,
            "before_entity_sha256": before_entity_sha256,
            "after_entity_sha256": after_entity_sha256,
            "source_ref_id": source_ref_id,
            "source_path": source_path,
            "exact_commit": exact_commit,
            "source_sha256": source_sha256,
            "as_of": as_of,
            "known_at": known_at,
            "available_at": available_at,
            "snapshot_id": snapshot_id,
            "diff_id": diff_id,
        }
        provisional = _provisional(cls, values)
        values["citation_id"] = provisional.compute_citation_id()
        return cls(**values)  # type: ignore[arg-type]

    def _payload_without_citation_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "target_kind": self.target_kind.value,
            "target_id": self.target_id,
            "entity_sha256": self.entity_sha256,
            "before_entity_sha256": self.before_entity_sha256,
            "after_entity_sha256": self.after_entity_sha256,
            "source_ref_id": self.source_ref_id,
            "source_path": self.source_path,
            "exact_commit": self.exact_commit,
            "source_sha256": self.source_sha256,
            "as_of": self.as_of.isoformat(),
            "known_at": self.known_at.isoformat(),
            "available_at": self.available_at.isoformat(),
            "snapshot_id": self.snapshot_id,
            "diff_id": self.diff_id,
        }

    def compute_citation_id(self) -> str:
        return _identity(self._payload_without_citation_id())

    def to_dict(self) -> dict[str, object]:
        return {"citation_id": self.citation_id, **self._payload_without_citation_id()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CitedQueryCitation:
        return cls(
            citation_id=str(payload.get("citation_id", "")),
            target_kind=CitedQueryTargetKind(str(payload.get("target_kind", ""))),
            target_id=str(payload.get("target_id", "")),
            entity_sha256=_optional_text(payload.get("entity_sha256")),
            before_entity_sha256=_optional_text(payload.get("before_entity_sha256")),
            after_entity_sha256=_optional_text(payload.get("after_entity_sha256")),
            source_ref_id=str(payload.get("source_ref_id", "")),
            source_path=str(payload.get("source_path", "")),
            exact_commit=str(payload.get("exact_commit", "")),
            source_sha256=str(payload.get("source_sha256", "")),
            as_of=_parse_datetime(payload.get("as_of"), "as_of"),
            known_at=_parse_datetime(payload.get("known_at"), "known_at"),
            available_at=_parse_datetime(payload.get("available_at"), "available_at"),
            snapshot_id=_optional_text(payload.get("snapshot_id")),
            diff_id=_optional_text(payload.get("diff_id")),
        )


@dataclass(frozen=True)
class CitedQueryClaim:
    schema_version: ClassVar[str] = "strategy_research_cited_query_claim.v1"

    claim_id: str
    ordinal: int
    text_zh: str
    citation_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _sha256(self.claim_id, "claim_id")
        if type(self.ordinal) is not int or self.ordinal < 1:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_ORDINAL_INVALID"
            )
        _required_text(self.text_zh, "text_zh")
        if not self.citation_ids:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_CITATION_REQUIRED"
            )
        if tuple(sorted(self.citation_ids)) != self.citation_ids:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_CITATION_ORDER_INVALID"
            )
        if len(self.citation_ids) != len(set(self.citation_ids)):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_CITATION_DUPLICATE"
            )
        for citation_id in self.citation_ids:
            _sha256(citation_id, "claim.citation_id", required=True)
        if self.claim_id != self.compute_claim_id():
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_ID_MISMATCH"
            )

    @classmethod
    def build(
        cls,
        *,
        ordinal: int,
        text_zh: str,
        citation_ids: Sequence[str],
    ) -> CitedQueryClaim:
        values: dict[str, object] = {
            "claim_id": "0" * 64,
            "ordinal": ordinal,
            "text_zh": text_zh,
            "citation_ids": tuple(sorted(citation_ids)),
        }
        provisional = _provisional(cls, values)
        values["claim_id"] = provisional.compute_claim_id()
        return cls(**values)  # type: ignore[arg-type]

    def _payload_without_claim_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "ordinal": self.ordinal,
            "text_zh": self.text_zh,
            "citation_ids": list(self.citation_ids),
        }

    def compute_claim_id(self) -> str:
        return _identity(self._payload_without_claim_id())

    def to_dict(self) -> dict[str, object]:
        return {"claim_id": self.claim_id, **self._payload_without_claim_id()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CitedQueryClaim:
        raw_citations = payload.get("citation_ids")
        if not isinstance(raw_citations, list):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_CITATION_LIST_REQUIRED"
            )
        return cls(
            claim_id=str(payload.get("claim_id", "")),
            ordinal=_integer(payload.get("ordinal"), "ordinal"),
            text_zh=str(payload.get("text_zh", "")),
            citation_ids=tuple(str(value) for value in raw_citations),
        )


@dataclass(frozen=True)
class StrategyResearchCitedQueryResponse:
    schema_version: ClassVar[str] = "strategy_research_cited_query_response.v1"

    response_id: str
    request: StrategyResearchCitedQueryRequest
    answer_status: CitedQueryAnswerStatus
    claims: tuple[CitedQueryClaim, ...]
    citations: tuple[CitedQueryCitation, ...]
    limitations: tuple[str, ...]
    reason_codes: tuple[str, ...]
    canonical_input_validated: bool = True
    investment_advice_generated: bool = False
    source_state_mutated: bool = False
    production_effect: ProductionEffect = ProductionEffect.NONE
    broker_action: str = "none"

    def __post_init__(self) -> None:
        _sha256(self.response_id, "response_id")
        self._validate_order_and_closure()
        self._validate_status()
        if (
            not self.canonical_input_validated
            or self.investment_advice_generated
            or self.source_state_mutated
            or self.production_effect is not ProductionEffect.NONE
            or self.broker_action != "none"
        ):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_READ_ONLY_BOUNDARY_VIOLATION"
            )
        if self.response_id != self.compute_response_id():
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_RESPONSE_ID_MISMATCH"
            )

    def _validate_order_and_closure(self) -> None:
        ordinals = tuple(item.ordinal for item in self.claims)
        if ordinals != tuple(range(1, len(self.claims) + 1)):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CLAIM_ORDER_INVALID"
            )
        citation_ids = tuple(item.citation_id for item in self.citations)
        if citation_ids != tuple(sorted(citation_ids)) or len(citation_ids) != len(
            set(citation_ids)
        ):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CITATION_ORDER_OR_DUPLICATE_INVALID"
            )
        cited = {citation_id for claim in self.claims for citation_id in claim.citation_ids}
        if cited != set(citation_ids):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_CITATION_CLOSURE_INVALID"
            )
        for citation in self.citations:
            if (
                citation.target_kind is not self.request.target_kind
                or citation.target_id != self.request.target_id
            ):
                raise StrategyResearchCitedQueryContractError(
                    "STRATEGY_CITED_QUERY_CITATION_TARGET_MISMATCH"
                )
            if self.request.input_kind is CitedQueryInputKind.SNAPSHOT:
                valid_identity = (
                    citation.snapshot_id == self.request.snapshot_id
                    and citation.diff_id is None
                )
            else:
                valid_identity = (
                    citation.diff_id == self.request.diff_id
                    and citation.snapshot_id is None
                )
            if not valid_identity:
                raise StrategyResearchCitedQueryContractError(
                    "STRATEGY_CITED_QUERY_CITATION_INPUT_MISMATCH"
                )

    def _validate_status(self) -> None:
        for field, values in (
            ("limitations", self.limitations),
            ("reason_codes", self.reason_codes),
        ):
            if tuple(sorted(values, key=str.casefold)) != values or len(values) != len(
                set(values)
            ):
                raise StrategyResearchCitedQueryContractError(
                    f"STRATEGY_CITED_QUERY_{field.upper()}_ORDER_OR_DUPLICATE_INVALID"
                )
            for value in values:
                _required_text(value, field)
        for reason in self.reason_codes:
            if not _REASON_CODE_PATTERN.fullmatch(reason):
                raise StrategyResearchCitedQueryContractError(
                    "STRATEGY_CITED_QUERY_REASON_CODE_INVALID"
                )
        if self.answer_status is CitedQueryAnswerStatus.ANSWERED:
            valid = bool(self.claims) and bool(self.citations) and not self.reason_codes
        elif self.answer_status is CitedQueryAnswerStatus.LIMITED:
            valid = (
                bool(self.claims)
                and bool(self.citations)
                and bool(self.limitations)
                and bool(self.reason_codes)
            )
        else:
            valid = (
                not self.claims
                and not self.citations
                and bool(self.limitations)
                and bool(self.reason_codes)
            )
        if not valid:
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_ANSWER_STATUS_INVALID"
            )

    @classmethod
    def build(
        cls,
        *,
        request: StrategyResearchCitedQueryRequest,
        answer_status: CitedQueryAnswerStatus,
        claims: Sequence[CitedQueryClaim] = (),
        citations: Sequence[CitedQueryCitation] = (),
        limitations: Sequence[str] = (),
        reason_codes: Sequence[str] = (),
    ) -> StrategyResearchCitedQueryResponse:
        values: dict[str, object] = {
            "response_id": "0" * 64,
            "request": request,
            "answer_status": answer_status,
            "claims": tuple(sorted(claims, key=lambda item: item.ordinal)),
            "citations": tuple(sorted(citations, key=lambda item: item.citation_id)),
            "limitations": tuple(sorted(limitations, key=str.casefold)),
            "reason_codes": tuple(sorted(reason_codes, key=str.casefold)),
            "canonical_input_validated": True,
            "investment_advice_generated": False,
            "source_state_mutated": False,
            "production_effect": ProductionEffect.NONE,
            "broker_action": "none",
        }
        provisional = _provisional(cls, values)
        values["response_id"] = provisional.compute_response_id()
        return cls(**values)  # type: ignore[arg-type]

    def _payload_without_response_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request": self.request.to_dict(),
            "answer_status": self.answer_status.value,
            "claims": [item.to_dict() for item in self.claims],
            "citations": [item.to_dict() for item in self.citations],
            "limitations": list(self.limitations),
            "reason_codes": list(self.reason_codes),
            "canonical_input_validated": self.canonical_input_validated,
            "investment_advice_generated": self.investment_advice_generated,
            "source_state_mutated": self.source_state_mutated,
            "production_effect": self.production_effect.value,
            "broker_action": self.broker_action,
        }

    def compute_response_id(self) -> str:
        return _identity(self._payload_without_response_id())

    def to_dict(self) -> dict[str, object]:
        return {"response_id": self.response_id, **self._payload_without_response_id()}

    def canonical_json_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, object],
    ) -> StrategyResearchCitedQueryResponse:
        raw_claims = payload.get("claims")
        raw_citations = payload.get("citations")
        raw_limitations = payload.get("limitations")
        raw_reason_codes = payload.get("reason_codes")
        if not all(
            isinstance(value, list)
            for value in (
                raw_claims,
                raw_citations,
                raw_limitations,
                raw_reason_codes,
            )
        ):
            raise StrategyResearchCitedQueryContractError(
                "STRATEGY_CITED_QUERY_RESPONSE_COLLECTION_LIST_REQUIRED"
            )
        return cls(
            response_id=str(payload.get("response_id", "")),
            request=StrategyResearchCitedQueryRequest.from_dict(
                _mapping(payload.get("request"), "request")
            ),
            answer_status=CitedQueryAnswerStatus(str(payload.get("answer_status", ""))),
            claims=tuple(
                CitedQueryClaim.from_dict(_mapping(value, "claims"))
                for value in raw_claims
            ),
            citations=tuple(
                CitedQueryCitation.from_dict(_mapping(value, "citations"))
                for value in raw_citations
            ),
            limitations=tuple(str(value) for value in raw_limitations),
            reason_codes=tuple(str(value) for value in raw_reason_codes),
            canonical_input_validated=payload.get("canonical_input_validated") is True,
            investment_advice_generated=payload.get("investment_advice_generated") is True,
            source_state_mutated=payload.get("source_state_mutated") is True,
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
            broker_action=str(payload.get("broker_action", "")),
        )


def _sha256(value: str | None, field: str, *, required: bool = False) -> None:
    if value is None:
        if required:
            raise StrategyResearchCitedQueryContractError(
                f"STRATEGY_CITED_QUERY_SHA256_REQUIRED:{field}"
            )
        return
    if not _SHA256_PATTERN.fullmatch(value):
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_SHA256_INVALID:{field}"
        )


def _aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_TIMEZONE_REQUIRED:{field}"
        )


def _parse_datetime(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_DATETIME_INVALID:{field}"
        ) from exc
    _aware_datetime(parsed, field)
    return parsed


def _repository_path(value: str) -> None:
    _required_text(value, "source_path")
    path = PurePosixPath(value)
    if (
        "\\" in value
        or path.is_absolute()
        or not path.parts
        or any(part in {"", ".", ".."} for part in path.parts)
        or ":" in path.parts[0]
    ):
        raise StrategyResearchCitedQueryContractError(
            "STRATEGY_CITED_QUERY_SOURCE_PATH_INVALID"
        )


def _identity(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _canonical_json_bytes(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _provisional(cls: type, values: Mapping[str, object]) -> object:
    item = object.__new__(cls)
    for key, value in values.items():
        object.__setattr__(item, key, value)
    return item


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_MAPPING_REQUIRED:{field}"
        )
    return value


def _integer(value: object, field: str) -> int:
    if type(value) is not int:
        raise StrategyResearchCitedQueryContractError(
            f"STRATEGY_CITED_QUERY_INTEGER_REQUIRED:{field}"
        )
    return value


__all__ = [
    "CITED_QUERY_QUESTION_CATALOG",
    "CitedQueryAnswerStatus",
    "CitedQueryCitation",
    "CitedQueryClaim",
    "CitedQueryInputKind",
    "CitedQueryQuestionId",
    "CitedQueryQuestionSpec",
    "CitedQueryReaderProfile",
    "CitedQueryTargetKind",
    "StrategyResearchCitedQueryContractError",
    "StrategyResearchCitedQueryRequest",
    "StrategyResearchCitedQueryResponse",
]
