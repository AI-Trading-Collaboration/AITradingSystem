from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")


class PageEffectivenessContractError(ValueError):
    pass


class PageFreshnessStatus(StrEnum):
    CURRENT = "CURRENT"
    REPOSITORY_AHEAD_NO_RELEVANT_DRIFT = "REPOSITORY_AHEAD_NO_RELEVANT_DRIFT"
    STALE_REBUILD_REQUIRED = "STALE_REBUILD_REQUIRED"
    UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED = "UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED"


class PageAcceptanceTrack(StrEnum):
    ENGINEERING_VALIDATION = "ENGINEERING_VALIDATION"
    OWNER_VISUAL_REVIEW = "OWNER_VISUAL_REVIEW"
    READER_COMPREHENSION_REVIEW = "READER_COMPREHENSION_REVIEW"


class PageAcceptanceStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    PENDING_REVIEW = "PENDING_REVIEW"
    NOT_EXECUTED = "NOT_EXECUTED"


class PageValidityLayer(StrEnum):
    SOURCE_TRUTH = "SOURCE_TRUTH"
    SEMANTIC_PROJECTION = "SEMANTIC_PROJECTION"
    VISUAL_RENDERING = "VISUAL_RENDERING"
    READER_COMPREHENSION = "READER_COMPREHENSION"


def canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _required(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_REQUIRED:{field}")
    return normalized


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise PageEffectivenessContractError(
            f"PAGE_EFFECTIVENESS_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_MAPPING_REQUIRED:{field}")
    return value


def _mapping_list(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_LIST_REQUIRED:{field}")
    values = tuple(_required(str(item), field) for item in value)
    if len(values) != len(set(values)):
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_DUPLICATE:{field}")
    return values


def _portable_path(value: str, field: str) -> str:
    path = _required(value.replace("\\", "/"), field)
    if path.startswith("/") or re.match(r"^[A-Za-z]:", path) or ".." in path.split("/"):
        raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_PATH_INVALID:{field}:{path}")
    return path


@dataclass(frozen=True)
class PageArtifactIdentity:
    role: str
    locator: str
    sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        _required(self.role, "artifact.role")
        _portable_path(self.locator, "artifact.locator")
        if not _SHA256.fullmatch(self.sha256):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_ARTIFACT_SHA256_INVALID")
        if self.byte_count <= 0:
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_ARTIFACT_BYTES_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "locator": self.locator,
            "sha256": self.sha256,
            "byte_count": self.byte_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> PageArtifactIdentity:
        _exact_keys(payload, {"role", "locator", "sha256", "byte_count"}, "artifact")
        return cls(
            role=str(payload["role"]),
            locator=str(payload["locator"]),
            sha256=str(payload["sha256"]),
            byte_count=int(str(payload["byte_count"])),
        )


@dataclass(frozen=True)
class PageTaskCoverage:
    task_id: str
    requirement_path: str
    requirement_sha256: str
    task_status: str
    coverage: str
    reader_summary_zh: str

    def __post_init__(self) -> None:
        if not self.task_id.startswith("TRADING-"):
            raise PageEffectivenessContractError(
                f"PAGE_EFFECTIVENESS_TASK_ID_INVALID:{self.task_id}"
            )
        _portable_path(self.requirement_path, "coverage.requirement_path")
        if not self.requirement_path.startswith("docs/requirements/"):
            raise PageEffectivenessContractError(
                f"PAGE_EFFECTIVENESS_REQUIREMENT_PATH_INVALID:{self.task_id}"
            )
        if not _SHA256.fullmatch(self.requirement_sha256):
            raise PageEffectivenessContractError(
                f"PAGE_EFFECTIVENESS_REQUIREMENT_SHA_INVALID:{self.task_id}"
            )
        for field in ("task_status", "coverage", "reader_summary_zh"):
            _required(str(getattr(self, field)), f"coverage.{field}")

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "requirement_path": self.requirement_path,
            "requirement_sha256": self.requirement_sha256,
            "task_status": self.task_status,
            "coverage": self.coverage,
            "reader_summary_zh": self.reader_summary_zh,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> PageTaskCoverage:
        expected = {
            "task_id",
            "requirement_path",
            "requirement_sha256",
            "task_status",
            "coverage",
            "reader_summary_zh",
        }
        _exact_keys(payload, expected, "coverage")
        return cls(**{key: str(payload[key]) for key in expected})


@dataclass(frozen=True)
class PageAcceptanceRecord:
    track: PageAcceptanceTrack
    status: PageAcceptanceStatus
    evidence_refs: tuple[str, ...]
    reviewer_id: str | None = None
    reviewed_at: str | None = None
    decision_id: str | None = None

    def __post_init__(self) -> None:
        for ref in self.evidence_refs:
            _portable_path(ref, "acceptance.evidence_ref")
        if len(self.evidence_refs) != len(set(self.evidence_refs)):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_ACCEPTANCE_EVIDENCE_DUPLICATE")
        human = self.track is not PageAcceptanceTrack.ENGINEERING_VALIDATION
        if human and self.status is PageAcceptanceStatus.PASS:
            for field, value in (
                ("reviewer_id", self.reviewer_id),
                ("reviewed_at", self.reviewed_at),
                ("decision_id", self.decision_id),
            ):
                _required(value or "", f"acceptance.{field}")
        if not human and any((self.reviewer_id, self.reviewed_at, self.decision_id)):
            raise PageEffectivenessContractError(
                "PAGE_EFFECTIVENESS_ENGINEERING_CANNOT_IMPERSONATE_HUMAN_REVIEW"
            )
        if self.status is PageAcceptanceStatus.PASS and not self.evidence_refs:
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_PASS_EVIDENCE_REQUIRED")

    def to_dict(self) -> dict[str, object]:
        return {
            "track": self.track.value,
            "status": self.status.value,
            "evidence_refs": list(self.evidence_refs),
            "reviewer_id": self.reviewer_id,
            "reviewed_at": self.reviewed_at,
            "decision_id": self.decision_id,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> PageAcceptanceRecord:
        expected = {
            "track",
            "status",
            "evidence_refs",
            "reviewer_id",
            "reviewed_at",
            "decision_id",
        }
        _exact_keys(payload, expected, "acceptance")
        return cls(
            track=PageAcceptanceTrack(str(payload["track"])),
            status=PageAcceptanceStatus(str(payload["status"])),
            evidence_refs=_string_list(payload["evidence_refs"], "acceptance.evidence_refs"),
            reviewer_id=(None if payload["reviewer_id"] is None else str(payload["reviewer_id"])),
            reviewed_at=(None if payload["reviewed_at"] is None else str(payload["reviewed_at"])),
            decision_id=(None if payload["decision_id"] is None else str(payload["decision_id"])),
        )


@dataclass(frozen=True)
class StrategyResearchPageEffectivenessManifest:
    schema_version: ClassVar[str] = "strategy_research_page_effectiveness.v1"

    page_id: str
    repository_commit: str
    source_snapshot_commit: str
    policy_id: str
    policy_version: str
    policy_sha256: str
    primary_research_start: str
    freshness_status: PageFreshnessStatus
    validity_layers: tuple[PageValidityLayer, ...]
    reader_questions: tuple[str, ...]
    task_coverage: tuple[PageTaskCoverage, ...]
    source_artifacts: tuple[PageArtifactIdentity, ...]
    rendered_artifacts: tuple[PageArtifactIdentity, ...]
    acceptance: tuple[PageAcceptanceRecord, ...]
    investment_conclusion_generated: bool
    order_authorized: bool
    real_engine_authorized: bool
    external_action: str
    production_effect: str
    broker_action: str
    content_sha256: str

    def __post_init__(self) -> None:
        _required(self.page_id, "manifest.page_id")
        for field, value in (
            ("repository_commit", self.repository_commit),
            ("source_snapshot_commit", self.source_snapshot_commit),
        ):
            if not _GIT_SHA.fullmatch(value):
                raise PageEffectivenessContractError(f"PAGE_EFFECTIVENESS_GIT_SHA_INVALID:{field}")
        if not _SHA256.fullmatch(self.policy_sha256):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_POLICY_SHA_INVALID")
        if self.primary_research_start != "2021-02-22":
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_PRIMARY_START_INVALID")
        if self.validity_layers != tuple(PageValidityLayer):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_VALIDITY_LAYER_SET_INVALID")
        if (
            len(self.task_coverage) != 38
            or len({item.task_id for item in self.task_coverage}) != 38
        ):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_TASK_COVERAGE_SET_INVALID")
        if len({item.locator for item in self.source_artifacts}) != len(self.source_artifacts):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_SOURCE_DUPLICATE")
        if len({item.locator for item in self.rendered_artifacts}) != len(self.rendered_artifacts):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_RENDERED_DUPLICATE")
        if tuple(item.track for item in self.acceptance) != tuple(PageAcceptanceTrack):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_ACCEPTANCE_TRACK_SET_INVALID")
        if any(
            (
                self.investment_conclusion_generated,
                self.order_authorized,
                self.real_engine_authorized,
            )
        ):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_SAFETY_BOOLEAN_INVALID")
        if (self.external_action, self.production_effect, self.broker_action) != (
            "none",
            "none",
            "none",
        ):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_EXTERNAL_EFFECT_INVALID")
        if not _SHA256.fullmatch(self.content_sha256):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_CONTENT_SHA_INVALID")
        if (
            self.content_sha256
            != hashlib.sha256(canonical_json_bytes(self._payload(include_hash=False))).hexdigest()
        ):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_CONTENT_SHA_MISMATCH")

    def _payload(self, *, include_hash: bool) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "page_id": self.page_id,
            "repository_commit": self.repository_commit,
            "source_snapshot_commit": self.source_snapshot_commit,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
            "primary_research_start": self.primary_research_start,
            "freshness_status": self.freshness_status.value,
            "validity_layers": [item.value for item in self.validity_layers],
            "reader_questions": list(self.reader_questions),
            "task_coverage": [item.to_dict() for item in self.task_coverage],
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
            "rendered_artifacts": [item.to_dict() for item in self.rendered_artifacts],
            "acceptance": [item.to_dict() for item in self.acceptance],
            "investment_conclusion_generated": self.investment_conclusion_generated,
            "order_authorized": self.order_authorized,
            "real_engine_authorized": self.real_engine_authorized,
            "external_action": self.external_action,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }
        if include_hash:
            payload["content_sha256"] = self.content_sha256
        return payload

    def to_dict(self) -> dict[str, object]:
        return self._payload(include_hash=True)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @classmethod
    def seal(
        cls,
        *,
        page_id: str,
        repository_commit: str,
        source_snapshot_commit: str,
        policy_id: str,
        policy_version: str,
        policy_sha256: str,
        primary_research_start: str,
        freshness_status: PageFreshnessStatus,
        reader_questions: Sequence[str],
        task_coverage: Sequence[PageTaskCoverage],
        source_artifacts: Sequence[PageArtifactIdentity],
        rendered_artifacts: Sequence[PageArtifactIdentity],
        acceptance: Sequence[PageAcceptanceRecord],
    ) -> StrategyResearchPageEffectivenessManifest:
        base = {
            "schema_version": cls.schema_version,
            "page_id": page_id,
            "repository_commit": repository_commit,
            "source_snapshot_commit": source_snapshot_commit,
            "policy_id": policy_id,
            "policy_version": policy_version,
            "policy_sha256": policy_sha256,
            "primary_research_start": primary_research_start,
            "freshness_status": freshness_status.value,
            "validity_layers": [item.value for item in PageValidityLayer],
            "reader_questions": list(reader_questions),
            "task_coverage": [item.to_dict() for item in task_coverage],
            "source_artifacts": [item.to_dict() for item in source_artifacts],
            "rendered_artifacts": [item.to_dict() for item in rendered_artifacts],
            "acceptance": [item.to_dict() for item in acceptance],
            "investment_conclusion_generated": False,
            "order_authorized": False,
            "real_engine_authorized": False,
            "external_action": "none",
            "production_effect": "none",
            "broker_action": "none",
        }
        return cls.from_dict(
            {**base, "content_sha256": hashlib.sha256(canonical_json_bytes(base)).hexdigest()}
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> StrategyResearchPageEffectivenessManifest:
        expected = {
            "schema_version",
            "page_id",
            "repository_commit",
            "source_snapshot_commit",
            "policy_id",
            "policy_version",
            "policy_sha256",
            "primary_research_start",
            "freshness_status",
            "validity_layers",
            "reader_questions",
            "task_coverage",
            "source_artifacts",
            "rendered_artifacts",
            "acceptance",
            "investment_conclusion_generated",
            "order_authorized",
            "real_engine_authorized",
            "external_action",
            "production_effect",
            "broker_action",
            "content_sha256",
        }
        _exact_keys(payload, expected, "manifest")
        if payload["schema_version"] != cls.schema_version:
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_SCHEMA_INVALID")
        return cls(
            page_id=str(payload["page_id"]),
            repository_commit=str(payload["repository_commit"]),
            source_snapshot_commit=str(payload["source_snapshot_commit"]),
            policy_id=str(payload["policy_id"]),
            policy_version=str(payload["policy_version"]),
            policy_sha256=str(payload["policy_sha256"]),
            primary_research_start=str(payload["primary_research_start"]),
            freshness_status=PageFreshnessStatus(str(payload["freshness_status"])),
            validity_layers=tuple(
                PageValidityLayer(item)
                for item in _string_list(payload["validity_layers"], "validity_layers")
            ),
            reader_questions=_string_list(payload["reader_questions"], "reader_questions"),
            task_coverage=tuple(
                PageTaskCoverage.from_dict(item)
                for item in _mapping_list(payload["task_coverage"], "task_coverage")
            ),
            source_artifacts=tuple(
                PageArtifactIdentity.from_dict(item)
                for item in _mapping_list(payload["source_artifacts"], "source_artifacts")
            ),
            rendered_artifacts=tuple(
                PageArtifactIdentity.from_dict(item)
                for item in _mapping_list(payload["rendered_artifacts"], "rendered_artifacts")
            ),
            acceptance=tuple(
                PageAcceptanceRecord.from_dict(item)
                for item in _mapping_list(payload["acceptance"], "acceptance")
            ),
            investment_conclusion_generated=bool(payload["investment_conclusion_generated"]),
            order_authorized=bool(payload["order_authorized"]),
            real_engine_authorized=bool(payload["real_engine_authorized"]),
            external_action=str(payload["external_action"]),
            production_effect=str(payload["production_effect"]),
            broker_action=str(payload["broker_action"]),
            content_sha256=str(payload["content_sha256"]),
        )

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> StrategyResearchPageEffectivenessManifest:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_JSON_INVALID") from exc
        if not isinstance(decoded, Mapping):
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_JSON_OBJECT_REQUIRED")
        result = cls.from_dict(decoded)
        if result.canonical_bytes != payload:
            raise PageEffectivenessContractError("PAGE_EFFECTIVENESS_NON_CANONICAL_BYTES")
        return result


__all__ = [
    "PageAcceptanceRecord",
    "PageAcceptanceStatus",
    "PageAcceptanceTrack",
    "PageArtifactIdentity",
    "PageEffectivenessContractError",
    "PageFreshnessStatus",
    "PageTaskCoverage",
    "PageValidityLayer",
    "StrategyResearchPageEffectivenessManifest",
    "canonical_json_bytes",
]
