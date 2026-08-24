from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_page_effectiveness import (
    PageAcceptanceRecord,
    PageAcceptanceStatus,
    PageAcceptanceTrack,
    PageArtifactIdentity,
    PageEffectivenessContractError,
    PageFreshnessStatus,
    PageTaskCoverage,
    StrategyResearchPageEffectivenessManifest,
    canonical_json_bytes,
    page_task_identity_sort_key,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    CanonicalTaskRegistry,
    validate_canonical_registry,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic

DEFAULT_PAGE_EFFECTIVENESS_POLICY_PATH = "config/atlas/page_effectiveness.yaml"
PAGE_EFFECTIVENESS_POLICY_SCHEMA = "atlas_page_effectiveness_policy.v1"
_SELF_TASK_ID = "TRADING-2505_ATLAS_PAGE_EFFECTIVENESS_FRESHNESS_VISUAL_REGRESSION_V1"
_TRADING_NUMBER = re.compile(r"^TRADING-(\d+)")
_EXPECTED_READER_QUESTIONS = (
    "CURRENT_RESEARCH_MAINLINE",
    "LARGEST_CURRENT_BLOCKER",
    "ENGINEERING_VS_RESEARCH_EVIDENCE",
    "PROHIBITED_INFERENCES",
    "NEXT_OWNER_AND_ACTION",
    "INVESTMENT_ORDER_ENGINE_AUTHORITY",
)
_EXPECTED_SAFETY: Mapping[str, object] = {
    "investment_conclusion_generated": False,
    "order_authorized": False,
    "real_engine_authorized": False,
    "external_action": "none",
    "production_effect": "none",
    "broker_action": "none",
}


@dataclass(frozen=True)
class _RenderedReaderDecision:
    item_id: str
    text_zh: str
    source_task_ids: tuple[str, ...]
    projection_sha256: str


class _ReaderDecisionHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.records: dict[tuple[str, str], _RenderedReaderDecision] = {}
        self.errors: list[str] = []
        self._active_kind: str | None = None
        self._active_id: str | None = None
        self._active_sources: tuple[str, ...] = ()
        self._active_sha256 = ""
        self._strong_depth = 0
        self._text_parts: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attributes = {key: value or "" for key, value in attrs}
        if tag == "article":
            reader_id = attributes.get("data-reader-decision")
            quick_id = attributes.get("data-page-effectiveness-question")
            if reader_id or quick_id:
                if self._active_id is not None:
                    self.errors.append("READER_DECISION_HTML_NESTED_RECORD")
                    return
                self._active_kind = "reader_cards" if reader_id else "quick_answers"
                self._active_id = reader_id or quick_id
                self._active_sources = tuple(
                    attributes.get("data-reader-claim-source-refs", "").split()
                )
                self._active_sha256 = attributes.get(
                    "data-reader-decision-projection-sha256", ""
                )
                self._text_parts = []
        elif tag == "strong" and self._active_id is not None:
            self._strong_depth += 1

    def handle_data(self, data: str) -> None:
        if self._active_id is not None and self._strong_depth:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "strong" and self._active_id is not None and self._strong_depth:
            self._strong_depth -= 1
            return
        if tag != "article" or self._active_id is None or self._active_kind is None:
            return
        key = (self._active_kind, self._active_id)
        if key in self.records:
            self.errors.append("READER_DECISION_HTML_DUPLICATE_RECORD:" + ":".join(key))
        else:
            self.records[key] = _RenderedReaderDecision(
                item_id=self._active_id,
                text_zh="".join(self._text_parts).strip(),
                source_task_ids=self._active_sources,
                projection_sha256=self._active_sha256,
            )
        self._active_kind = None
        self._active_id = None
        self._active_sources = ()
        self._active_sha256 = ""
        self._strong_depth = 0
        self._text_parts = []


class PageEffectivenessError(ValueError):
    pass


@dataclass(frozen=True)
class PageEffectivenessTaskPolicy:
    task_id: str
    requirement_path: str
    coverage: str
    reader_summary_zh: str


@dataclass(frozen=True)
class PageEffectivenessPolicy:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    primary_research_start: str
    canonical_page_path: str
    manifest_path: str
    validation_path: str
    reader_questions: tuple[str, ...]
    relevant_source_paths: tuple[str, ...]
    task_sources: tuple[PageEffectivenessTaskPolicy, ...]
    acceptance_defaults: Mapping[str, object]
    safety: Mapping[str, object]
    policy_sha256: str


@dataclass(frozen=True)
class PageEffectivenessValidation:
    schema_version: str
    status: str
    freshness_status: PageFreshnessStatus
    manifest_sha256: str
    page_sha256: str | None
    checks: tuple[str, ...]
    errors: tuple[str, ...]
    browser_evidence: tuple[PageArtifactIdentity, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "freshness_status": self.freshness_status.value,
            "manifest_sha256": self.manifest_sha256,
            "page_sha256": self.page_sha256,
            "checks": list(self.checks),
            "errors": list(self.errors),
            "browser_evidence": [item.to_dict() for item in self.browser_evidence],
            **_EXPECTED_SAFETY,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_POLICY_MAPPING_REQUIRED:{field}")
    return value


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_POLICY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _list_of_mappings(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_POLICY_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_POLICY_LIST_REQUIRED:{field}")
    values = tuple(str(item) for item in value)
    if any(not item.strip() for item in values) or len(values) != len(set(values)):
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_POLICY_LIST_INVALID:{field}")
    return values


def _portable_path(root: Path, relative: str, field: str) -> Path:
    normalized = relative.replace("\\", "/")
    if (
        not normalized
        or normalized.startswith("/")
        or re.match(r"^[A-Za-z]:", normalized)
        or ".." in normalized.split("/")
    ):
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_PATH_INVALID:{field}:{relative}")
    resolved = (root / normalized).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_PATH_OUTSIDE_REPOSITORY:{field}:{relative}"
        ) from exc
    return resolved


def load_page_effectiveness_policy(
    *, repository_root: Path, policy_path: Path | None = None
) -> PageEffectivenessPolicy:
    root = repository_root.resolve()
    selected = (
        root / DEFAULT_PAGE_EFFECTIVENESS_POLICY_PATH
        if policy_path is None
        else policy_path.resolve()
    )
    try:
        selected.relative_to(root)
    except ValueError as exc:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_OUTSIDE_REPOSITORY") from exc
    raw = selected.read_bytes()
    try:
        decoded = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_INVALID") from exc
    payload = _mapping(decoded, "policy")
    expected = {
        "schema_version",
        "policy_id",
        "policy_version",
        "status",
        "owner",
        "primary_research_start",
        "canonical_page_path",
        "manifest_path",
        "validation_path",
        "reader_questions",
        "relevant_source_paths",
        "task_sources",
        "acceptance_defaults",
        "safety",
    }
    _exact_keys(payload, expected, "policy")
    if payload["schema_version"] != PAGE_EFFECTIVENESS_POLICY_SCHEMA:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_SCHEMA_INVALID")
    reader_questions = _string_list(payload["reader_questions"], "reader_questions")
    if reader_questions != _EXPECTED_READER_QUESTIONS:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_READER_QUESTION_ORDER_INVALID")
    sources = _string_list(payload["relevant_source_paths"], "relevant_source_paths")
    tasks: list[PageEffectivenessTaskPolicy] = []
    for item in _list_of_mappings(payload["task_sources"], "task_sources"):
        _exact_keys(
            item,
            {"task_id", "requirement_path", "coverage", "reader_summary_zh"},
            "task_source",
        )
        tasks.append(
            PageEffectivenessTaskPolicy(
                task_id=str(item["task_id"]),
                requirement_path=str(item["requirement_path"]),
                coverage=str(item["coverage"]),
                reader_summary_zh=str(item["reader_summary_zh"]),
            )
        )
    if not tasks or len({item.task_id for item in tasks}) != len(tasks):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_TASK_SET_INVALID")
    try:
        task_keys = tuple(page_task_identity_sort_key(item.task_id) for item in tasks)
    except PageEffectivenessContractError as exc:
        raise PageEffectivenessError(str(exc)) from exc
    if len(set(task_keys)) != len(task_keys):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_TASK_IDENTITY_DUPLICATE")
    if task_keys != tuple(sorted(task_keys)):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_TASK_ORDER_INVALID")
    defaults = _mapping(payload["acceptance_defaults"], "acceptance_defaults")
    _exact_keys(
        defaults,
        {"engineering_validation", "owner_visual_review", "reader_comprehension_review"},
        "acceptance_defaults",
    )
    if dict(defaults) != {
        "engineering_validation": "NOT_EXECUTED",
        "owner_visual_review": "PENDING_REVIEW",
        "reader_comprehension_review": "PENDING_REVIEW",
    }:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_ACCEPTANCE_DEFAULT_INVALID")
    safety = _mapping(payload["safety"], "safety")
    if dict(safety) != dict(_EXPECTED_SAFETY):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_POLICY_SAFETY_INVALID")
    if str(payload["primary_research_start"]) != "2021-02-22":
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_PRIMARY_START_INVALID")
    return PageEffectivenessPolicy(
        policy_id=str(payload["policy_id"]),
        policy_version=str(payload["policy_version"]),
        status=str(payload["status"]),
        owner=str(payload["owner"]),
        primary_research_start=str(payload["primary_research_start"]),
        canonical_page_path=str(payload["canonical_page_path"]),
        manifest_path=str(payload["manifest_path"]),
        validation_path=str(payload["validation_path"]),
        reader_questions=reader_questions,
        relevant_source_paths=sources,
        task_sources=tuple(tasks),
        acceptance_defaults=defaults,
        safety=safety,
        policy_sha256=hashlib.sha256(raw).hexdigest(),
    )


def repository_head(repository_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        check=True,
    )
    value = result.stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_REPOSITORY_HEAD_INVALID")
    return value


def _task_status(registry: CanonicalTaskRegistry, task_id: str) -> str:
    projection = _mapping(registry.fragment(task_id).get("projection"), "task.projection")
    cells = projection.get("legacy_first_eight_cells")
    if not isinstance(cells, list) or len(cells) != 8:
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_TASK_PROJECTION_INVALID:{task_id}")
    return str(cells[3])


def _task_fragment_record(
    registry: CanonicalTaskRegistry, task_id: str
) -> Mapping[str, object]:
    records = registry.index.get("fragments")
    if not isinstance(records, list):
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_TASK_INDEX_INVALID")
    matches = tuple(
        _mapping(item, "task.index_record")
        for item in records
        if isinstance(item, Mapping) and str(item.get("task_id", "")) == task_id
    )
    if len(matches) != 1:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_INDEX_BINDING_INVALID:{task_id}"
        )
    return matches[0]


def _task_event(fragment: Mapping[str, object], task_id: str) -> Mapping[str, object]:
    last_event_id = str(fragment.get("last_event_id", ""))
    events = fragment.get("events")
    if not last_event_id or not isinstance(events, list):
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_EVENT_BINDING_INVALID:{task_id}"
        )
    matches = tuple(
        _mapping(item, "task.event")
        for item in events
        if isinstance(item, Mapping) and str(item.get("event_id", "")) == last_event_id
    )
    if len(matches) != 1:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_EVENT_BINDING_INVALID:{task_id}"
        )
    return matches[0]


def _task_event_time(
    *, root: Path, event: Mapping[str, object], task_id: str
) -> tuple[str, str]:
    occurred_at = event.get("occurred_at")
    if occurred_at is not None:
        return str(occurred_at), "EVENT_OCCURRED_AT"
    base_commit = str(event.get("base_commit", ""))
    if not re.fullmatch(r"[0-9a-f]{40}", base_commit):
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_EVENT_TIME_UNAVAILABLE:{task_id}"
        )
    value = _commit_time(root.as_posix(), base_commit)
    if not value:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_EVENT_BASE_COMMIT_TIME_EMPTY:{task_id}"
        )
    return value, "EVENT_BASE_COMMIT_AT"


@lru_cache(maxsize=512)
def _commit_time(root: str, commit: str) -> str:
    try:
        result = subprocess.run(
            ["git", "show", "-s", "--format=%cI", commit],
            cwd=Path(root),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.SubprocessError as exc:
        raise PageEffectivenessError(
            f"PAGE_EFFECTIVENESS_TASK_EVENT_BASE_COMMIT_UNAVAILABLE:{commit}"
        ) from exc
    return result.stdout.strip()


def _sha_identity(root: Path, path: str, role: str) -> PageArtifactIdentity:
    selected = _portable_path(root, path, role)
    if not selected.is_file() or selected.is_symlink():
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_SOURCE_MISSING:{path}")
    raw = selected.read_bytes()
    if not raw:
        raise PageEffectivenessError(f"PAGE_EFFECTIVENESS_SOURCE_EMPTY:{path}")
    return PageArtifactIdentity(
        role=role,
        locator=path.replace("\\", "/"),
        sha256=hashlib.sha256(raw).hexdigest(),
        byte_count=len(raw),
    )


def build_page_task_coverage(
    *, root: Path, policy: PageEffectivenessPolicy, registry: CanonicalTaskRegistry
) -> tuple[PageTaskCoverage, ...]:
    rows: list[PageTaskCoverage] = []
    for item in policy.task_sources:
        fragment = registry.fragment(item.task_id)
        record = _mapping(fragment.get("task_record"), "task_record")
        refs = record.get("requirement_refs")
        if not isinstance(refs, list) or item.requirement_path not in refs:
            raise PageEffectivenessError(
                f"PAGE_EFFECTIVENESS_TASK_REQUIREMENT_BINDING_INVALID:{item.task_id}"
            )
        identity = _sha_identity(root, item.requirement_path, "TASK_REQUIREMENT")
        index_record = _task_fragment_record(registry, item.task_id)
        fragment_path = str(index_record.get("path", ""))
        fragment_identity = _sha_identity(root, fragment_path, "TASK_FRAGMENT")
        if fragment_identity.sha256 != str(index_record.get("file_sha256", "")):
            raise PageEffectivenessError(
                f"PAGE_EFFECTIVENESS_TASK_FRAGMENT_INDEX_HASH_DRIFT:{item.task_id}"
            )
        event = _task_event(fragment, item.task_id)
        event_at, event_time_basis = _task_event_time(
            root=root, event=event, task_id=item.task_id
        )
        rows.append(
            PageTaskCoverage(
                task_id=item.task_id,
                requirement_path=item.requirement_path,
                requirement_sha256=identity.sha256,
                task_fragment_path=fragment_path,
                task_fragment_sha256=fragment_identity.sha256,
                task_event_id=str(event.get("event_id", "")),
                task_event_at=event_at,
                task_event_time_basis=event_time_basis,
                task_status=_task_status(registry, item.task_id),
                coverage=item.coverage,
                reader_summary_zh=item.reader_summary_zh,
            )
        )
    return tuple(rows)


def unclassified_page_successors(
    registry: CanonicalTaskRegistry, policy: PageEffectivenessPolicy
) -> tuple[str, ...]:
    covered = {item.task_id for item in policy.task_sources} | {_SELF_TASK_ID}
    unknown: list[str] = []
    for fragment in registry.fragments:
        task_id = str(_mapping(fragment.get("stable_task_identity"), "identity")["task_id"])
        match = _TRADING_NUMBER.match(task_id)
        if match and int(match.group(1)) > 2504 and task_id not in covered:
            unknown.append(task_id)
    return tuple(
        sorted(
            unknown,
            key=lambda task_id: (*page_task_identity_sort_key(task_id), task_id),
        )
    )


def _acceptance(
    *,
    engineering_status: PageAcceptanceStatus,
    engineering_evidence_refs: Sequence[str],
    owner_visual_review: PageAcceptanceRecord | None,
    reader_comprehension_review: PageAcceptanceRecord | None,
) -> tuple[PageAcceptanceRecord, ...]:
    def human_review(
        track: PageAcceptanceTrack,
        review: PageAcceptanceRecord | None,
    ) -> PageAcceptanceRecord:
        if review is None:
            return PageAcceptanceRecord(
                track=track,
                status=PageAcceptanceStatus.PENDING_REVIEW,
                evidence_refs=(),
            )
        if review.track is not track:
            raise PageEffectivenessError(
                "PAGE_EFFECTIVENESS_HUMAN_REVIEW_TRACK_INVALID:"
                f"expected={track.value}:actual={review.track.value}"
            )
        return review

    return (
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.ENGINEERING_VALIDATION,
            status=engineering_status,
            evidence_refs=tuple(engineering_evidence_refs),
        ),
        human_review(
            PageAcceptanceTrack.OWNER_VISUAL_REVIEW,
            owner_visual_review,
        ),
        human_review(
            PageAcceptanceTrack.READER_COMPREHENSION_REVIEW,
            reader_comprehension_review,
        ),
    )


def build_page_effectiveness_manifest(
    *,
    repository_root: Path,
    repository_commit: str | None = None,
    source_snapshot_commit: str | None = None,
    rendered_artifacts: Sequence[PageArtifactIdentity] = (),
    engineering_status: PageAcceptanceStatus = PageAcceptanceStatus.NOT_EXECUTED,
    engineering_evidence_refs: Sequence[str] = (),
    owner_visual_review: PageAcceptanceRecord | None = None,
    reader_comprehension_review: PageAcceptanceRecord | None = None,
) -> StrategyResearchPageEffectivenessManifest:
    root = repository_root.resolve()
    policy = load_page_effectiveness_policy(repository_root=root)
    registry = validate_canonical_registry(project_root=root)
    current = repository_commit or repository_head(root)
    source_commit = source_snapshot_commit or current
    unknown = unclassified_page_successors(registry, policy)
    freshness = (
        PageFreshnessStatus.UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED
        if unknown
        else (
            PageFreshnessStatus.CURRENT
            if current == source_commit
            else PageFreshnessStatus.REPOSITORY_AHEAD_NO_RELEVANT_DRIFT
        )
    )
    sources = tuple(
        _sha_identity(root, path, "SEMANTIC_SOURCE") for path in policy.relevant_source_paths
    )
    manifest = StrategyResearchPageEffectivenessManifest.seal(
        page_id="ATLAS_STRATEGY_RESEARCH_CITED_QUERY_TRADING_2470_V1",
        repository_commit=current,
        source_snapshot_commit=source_commit,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy.policy_sha256,
        primary_research_start=policy.primary_research_start,
        freshness_status=freshness,
        reader_questions=policy.reader_questions,
        task_coverage=build_page_task_coverage(root=root, policy=policy, registry=registry),
        source_artifacts=sources,
        rendered_artifacts=rendered_artifacts,
        acceptance=_acceptance(
            engineering_status=engineering_status,
            engineering_evidence_refs=engineering_evidence_refs,
            owner_visual_review=owner_visual_review,
            reader_comprehension_review=reader_comprehension_review,
        ),
    )
    replay = StrategyResearchPageEffectivenessManifest.from_json_bytes(manifest.canonical_bytes)
    if replay != manifest:
        raise PageEffectivenessError("PAGE_EFFECTIVENESS_CANONICAL_REPLAY_MISMATCH")
    return manifest


def validate_page_effectiveness_manifest(
    *,
    repository_root: Path,
    manifest: StrategyResearchPageEffectivenessManifest,
    current_repository_commit: str | None = None,
    browser_evidence: Sequence[PageArtifactIdentity] = (),
    rendered_payloads: Mapping[str, bytes] | None = None,
) -> PageEffectivenessValidation:
    root = repository_root.resolve()
    current = current_repository_commit or repository_head(root)
    errors: list[str] = []
    checks: list[str] = []
    page: PageArtifactIdentity | None = None
    try:
        policy = load_page_effectiveness_policy(repository_root=root)
        expected = build_page_effectiveness_manifest(
            repository_root=root,
            repository_commit=current,
            source_snapshot_commit=manifest.source_snapshot_commit,
            rendered_artifacts=manifest.rendered_artifacts,
            engineering_status=manifest.acceptance[0].status,
            engineering_evidence_refs=manifest.acceptance[0].evidence_refs,
            owner_visual_review=manifest.acceptance[1],
            reader_comprehension_review=manifest.acceptance[2],
        )
        if manifest.policy_sha256 != expected.policy_sha256:
            errors.append("POLICY_DRIFT")
        else:
            checks.append("POLICY_EXACT_BYTES_MATCH")
        if manifest.task_coverage != expected.task_coverage:
            errors.append("TASK_COVERAGE_OR_STATUS_DRIFT")
        else:
            checks.append("TASK_COVERAGE_EXACT_SET_MATCH")
        if manifest.source_artifacts != expected.source_artifacts:
            errors.append("SEMANTIC_SOURCE_DRIFT")
        else:
            checks.append("SEMANTIC_SOURCE_HASHES_MATCH")
        if expected.freshness_status is PageFreshnessStatus.UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED:
            errors.append("UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED")
        for artifact in manifest.rendered_artifacts:
            basename = artifact.locator.rsplit("/", 1)[-1]
            raw = None if rendered_payloads is None else rendered_payloads.get(basename)
            if raw is None:
                target = _portable_path(root, artifact.locator, "rendered_artifact")
                raw = target.read_bytes() if target.is_file() else None
            if raw is not None:
                if (
                    len(raw) != artifact.byte_count
                    or hashlib.sha256(raw).hexdigest() != artifact.sha256
                ):
                    errors.append("RENDERED_ARTIFACT_DRIFT:" + artifact.locator)
            elif artifact.locator.startswith("outputs/"):
                errors.append("RENDERED_ARTIFACT_MISSING:" + artifact.locator)
        canonical_pages = tuple(
            item
            for item in manifest.rendered_artifacts
            if item.locator == policy.canonical_page_path
        )
        if len(canonical_pages) != 1:
            errors.append("CANONICAL_HTML_IDENTITY_MISSING")
        else:
            page = canonical_pages[0]
            checks.append("CANONICAL_HTML_IDENTITY_MATCH")
            live_errors, live_checks = _validate_live_bundle_payloads(
                root=root,
                manifest=manifest,
                rendered_payloads=rendered_payloads,
            )
            errors.extend(live_errors)
            checks.extend(live_checks)
        if tuple(item.track for item in manifest.acceptance) == tuple(PageAcceptanceTrack):
            checks.append("THREE_ACCEPTANCE_TRACKS_INDEPENDENT")
        if (
            manifest.acceptance[1].status is PageAcceptanceStatus.PASS
            or manifest.acceptance[2].status is PageAcceptanceStatus.PASS
        ):
            mismatched_tracks = tuple(
                item.track.value
                for item in manifest.acceptance[1:]
                if item.status is PageAcceptanceStatus.PASS
                and (page is None or item.reviewed_page_sha256 != page.sha256)
            )
            if mismatched_tracks:
                errors.extend(
                    "HUMAN_REVIEW_PAGE_IDENTITY_MISMATCH:" + track
                    for track in mismatched_tracks
                )
            else:
                checks.append("HUMAN_REVIEW_EXPLICITLY_ATTESTED")
        else:
            checks.append("HUMAN_REVIEW_REMAINS_PENDING")
        if manifest.primary_research_start == "2021-02-22":
            checks.append("PRIMARY_RESEARCH_WINDOW_MATCH")
        checks.append("NO_INVESTMENT_ORDER_ENGINE_OR_EXTERNAL_EFFECT")
        freshness = expected.freshness_status
        if errors and freshness not in {
            PageFreshnessStatus.UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED,
        }:
            freshness = PageFreshnessStatus.STALE_REBUILD_REQUIRED
    except (
        OSError,
        PageEffectivenessContractError,
        PageEffectivenessError,
        subprocess.SubprocessError,
    ) as exc:
        errors.append(str(exc))
        freshness = PageFreshnessStatus.STALE_REBUILD_REQUIRED
    return PageEffectivenessValidation(
        schema_version="atlas_page_effectiveness_validation.v1",
        status="PASS" if not errors else "FAIL",
        freshness_status=freshness,
        manifest_sha256=manifest.content_sha256,
        page_sha256=None if page is None else page.sha256,
        checks=tuple(checks),
        errors=tuple(errors),
        browser_evidence=tuple(browser_evidence),
    )


def _rendered_payload(
    *,
    root: Path,
    manifest: StrategyResearchPageEffectivenessManifest,
    basename: str,
    rendered_payloads: Mapping[str, bytes] | None,
) -> bytes | None:
    matches = tuple(
        item for item in manifest.rendered_artifacts if item.locator.rsplit("/", 1)[-1] == basename
    )
    if len(matches) != 1:
        return None
    if rendered_payloads is not None and basename in rendered_payloads:
        return rendered_payloads[basename]
    selected = _portable_path(root, matches[0].locator, "live_bundle_artifact")
    return selected.read_bytes() if selected.is_file() else None


def _validate_reader_decision_html(
    *,
    html_bytes: bytes,
    projection: Any,
) -> tuple[list[str], list[str]]:
    try:
        text = html_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return ["READER_DECISION_HTML_UTF8_INVALID"], []
    parser = _ReaderDecisionHTMLParser()
    parser.feed(text)
    parser.close()
    errors = list(parser.errors)
    expected_records: dict[tuple[str, str], object] = {}
    for item in projection.reader_cards:
        expected_records[("reader_cards", item.item_id)] = item
    actual_keys = set(parser.records)
    expected_keys = set(expected_records)
    for kind, item_id in sorted(expected_keys - actual_keys):
        errors.append(f"READER_DECISION_HTML_RECORD_MISSING:{kind}:{item_id}")
    for kind, item_id in sorted(actual_keys - expected_keys):
        errors.append(f"READER_DECISION_HTML_RECORD_UNEXPECTED:{kind}:{item_id}")
    for key in sorted(expected_keys & actual_keys):
        expected = expected_records[key]
        actual = parser.records[key]
        if actual.text_zh != expected.text_zh:
            errors.append("READER_DECISION_HTML_TEXT_DRIFT:" + ":".join(key))
        if actual.source_task_ids != expected.source_task_ids:
            errors.append("READER_DECISION_HTML_SOURCE_DRIFT:" + ":".join(key))
        if actual.projection_sha256 != projection.content_sha256:
            errors.append("READER_DECISION_HTML_PROJECTION_HASH_DRIFT:" + ":".join(key))
    if projection.dq_pit_promoted is not False:
        errors.append("READER_DECISION_DQ_PIT_PROMOTION_BOUNDARY_INVALID")
    if errors:
        return errors, []
    return [], [
        "VISIBLE_READER_DECISIONS_MATCH_LIVE_PROJECTION",
        "TRANSPORT_AND_DQ_PIT_AXES_SEPARATED",
        "SUCCESSOR_STATE_DOMINATES_HISTORICAL_TRANSPORT_FACT",
    ]


def _validate_live_bundle_payloads(
    *,
    root: Path,
    manifest: StrategyResearchPageEffectivenessManifest,
    rendered_payloads: Mapping[str, bytes] | None,
) -> tuple[list[str], list[str]]:
    required = (
        "comparison_snapshot.json",
        "current_snapshot.json",
        "current_diff.json",
        "reader_state.json",
        "index.html",
    )
    payloads = {
        name: _rendered_payload(
            root=root,
            manifest=manifest,
            basename=name,
            rendered_payloads=rendered_payloads,
        )
        for name in required
    }
    missing = tuple(name for name, payload in payloads.items() if payload is None)
    if missing:
        return (["LIVE_SOURCE_BUNDLE_MISSING:" + ",".join(missing)], [])
    try:
        from ai_trading_system.atlas.live_snapshot import (
            build_live_snapshot_bundle,
            build_reader_decision_projection,
            load_live_snapshot_policy,
            reader_safe_task_summary,
        )
        from ai_trading_system.atlas.reader_state_projection import (
            load_reader_state_semantics,
            project_reader_state,
        )
        from ai_trading_system.contracts.strategy_research_reader_state import (
            ReaderChangeKind,
        )

        expected = build_live_snapshot_bundle(
            repository_root=root,
            exact_commit=manifest.source_snapshot_commit,
        )
        expected_payloads = {
            "comparison_snapshot.json": expected.comparison_snapshot.canonical_json_bytes(),
            "current_snapshot.json": expected.current_snapshot.canonical_json_bytes(),
            "current_diff.json": expected.current_diff.canonical_json_bytes(),
        }
        errors = [
            "LIVE_SOURCE_BUNDLE_REPLAY_DRIFT:" + name
            for name, expected_bytes in expected_payloads.items()
            if payloads[name] != expected_bytes
        ]
        reader_state = _mapping(
            json.loads((payloads["reader_state.json"] or b"").decode("utf-8")),
            "reader_state",
        )
        live_policy = load_live_snapshot_policy(repository_root=root)
        coverage_by_id = {item.task_id: item for item in manifest.task_coverage}
        mainline = coverage_by_id[live_policy.current_mainline_task_id]
        blocker = coverage_by_id[live_policy.largest_blocker_task_id]
        next_step = coverage_by_id[live_policy.next_legal_action_task_id]
        change = _mapping(reader_state.get("change"), "reader_state.change")
        expected_reader_state = project_reader_state(
            policy=load_reader_state_semantics(repository_root=root),
            status_object_zh=live_policy.status_object_zh,
            raw_status=live_policy.task_status_mapping[mainline.task_status],
            reason_zh=reader_safe_task_summary(blocker.reader_summary_zh),
            research_state_as_of=expected.research_state_as_of,
            evidence_evaluated_at=live_policy.evidence_evaluated_at,
            page_source_commit_at=expected.page_source_commit_at,
            next_legal_action_zh=reader_safe_task_summary(next_step.reader_summary_zh),
            prohibited_inference_zh=(
                "不能把工程校验、页面可读或一次外部运行解释为策略有效、收益稳健或风险可接受。"
            ),
            change_kind=ReaderChangeKind.CHANGED,
            comparison_base_id=expected.comparison_snapshot.snapshot_id,
            comparison_base_date=expected.comparison_snapshot.generated_at.isoformat(),
            change_explanation_zh=str(change.get("explanation_zh", "")),
            source_refs=tuple(
                dict.fromkeys(
                    (
                        mainline.requirement_path,
                        blocker.requirement_path,
                        next_step.requirement_path,
                    )
                )
            ),
        )
        if dict(reader_state) != expected_reader_state.to_dict():
            errors.append("READER_STATE_LIVE_TASK_OR_DATE_SEMANTICS_DRIFT")
        decision_projection = build_reader_decision_projection(
            repository_root=root,
            coverage=manifest.task_coverage,
            policy=live_policy,
        )
        decision_errors, decision_checks = _validate_reader_decision_html(
            html_bytes=payloads["index.html"] or b"",
            projection=decision_projection,
        )
        errors.extend(decision_errors)
        checks = [] if errors else [
            "LIVE_SOURCE_BUNDLE_CANONICAL_REPLAY_MATCH",
            "TASK_EVENT_AND_FRAGMENT_IDENTITIES_MATCH",
            "RESEARCH_EVIDENCE_PAGE_DATES_SEPARATED",
            *decision_checks,
        ]
        return errors, checks
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        return (["LIVE_SOURCE_BUNDLE_REPLAY_FAILED:" + str(exc)], [])


def write_page_effectiveness_sidecars(
    *,
    output_directory: Path,
    manifest: StrategyResearchPageEffectivenessManifest,
    validation: PageEffectivenessValidation,
) -> tuple[PageArtifactIdentity, PageArtifactIdentity]:
    output_directory.mkdir(parents=True, exist_ok=True)
    manifest_result = write_bytes_atomic(
        output_directory / "page_effectiveness.json", manifest.canonical_bytes
    )
    validation_result = write_bytes_atomic(
        output_directory / "page_effectiveness_validation.json", validation.canonical_bytes
    )
    return (
        PageArtifactIdentity(
            role="PAGE_EFFECTIVENESS_MANIFEST",
            locator="outputs/atlas/strategy_research_cited_query/trading_2470_v1/page_effectiveness.json",
            sha256=manifest_result.sha256,
            byte_count=manifest_result.size_bytes,
        ),
        PageArtifactIdentity(
            role="PAGE_EFFECTIVENESS_VALIDATION",
            locator="outputs/atlas/strategy_research_cited_query/trading_2470_v1/page_effectiveness_validation.json",
            sha256=validation_result.sha256,
            byte_count=validation_result.size_bytes,
        ),
    )


def browser_evidence_identities(
    *, repository_root: Path, evidence_paths: Sequence[str]
) -> tuple[PageArtifactIdentity, ...]:
    root = repository_root.resolve()
    return tuple(_sha_identity(root, path, "BROWSER_EVIDENCE") for path in evidence_paths)


__all__ = [
    "DEFAULT_PAGE_EFFECTIVENESS_POLICY_PATH",
    "PAGE_EFFECTIVENESS_POLICY_SCHEMA",
    "PageEffectivenessError",
    "PageEffectivenessPolicy",
    "PageEffectivenessTaskPolicy",
    "PageEffectivenessValidation",
    "browser_evidence_identities",
    "build_page_task_coverage",
    "build_page_effectiveness_manifest",
    "load_page_effectiveness_policy",
    "repository_head",
    "unclassified_page_successors",
    "validate_page_effectiveness_manifest",
    "write_page_effectiveness_sidecars",
]
