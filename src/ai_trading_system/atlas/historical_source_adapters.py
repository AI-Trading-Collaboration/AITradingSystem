"""Typed, Git-canonical adapters for approved Atlas historical sources.

The adapters in this module are evidence readers.  They do not project Atlas
nodes/results, rerun research, infer missing facts, or authorize any investment
or production action.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, TypeAlias

import yaml

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_SOURCE_PATHS = frozenset({"docs/research/next_research_program_roadmap.json"})


class HistoricalSourceAdapterError(ValueError):
    """Raised when approved historical evidence cannot be adapted safely."""


class HistoricalSourceRole(StrEnum):
    BASELINE = "BASELINE"
    COMPONENT_ATTRIBUTION = "COMPONENT_ATTRIBUTION"
    BRANCH_DECISION = "BRANCH_DECISION"
    MONTHLY_REVIEW = "MONTHLY_REVIEW"
    PROGRAM_SNAPSHOT = "PROGRAM_SNAPSHOT"


@dataclass(frozen=True)
class HistoricalWindowRecord:
    window_id: str
    source_field: str
    requested_start: str | None = None
    requested_end: str | None = None
    evaluated_start: str | None = None
    evaluated_end: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "source_field": self.source_field,
            "requested_start": self.requested_start,
            "requested_end": self.requested_end,
            "evaluated_start": self.evaluated_start,
            "evaluated_end": self.evaluated_end,
        }


@dataclass(frozen=True)
class HistoricalDataQualityReceipt:
    passed: bool
    status: str
    report_path: str
    error_count: int
    warning_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "status": self.status,
            "report_path": self.report_path,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
        }


@dataclass(frozen=True)
class HistoricalReaderBrief:
    summary: str
    key_result: str
    blocking_issues: str
    warnings: str
    next_action: str
    safety_boundary: str

    def to_dict(self) -> dict[str, str]:
        return {
            "summary": self.summary,
            "key_result": self.key_result,
            "blocking_issues": self.blocking_issues,
            "warnings": self.warnings,
            "next_action": self.next_action,
            "safety_boundary": self.safety_boundary,
        }


@dataclass(frozen=True)
class BaselinePayload:
    benchmark_id: str
    benchmark_name: str
    weights: tuple[tuple[str, float], ...]
    return_proxy: float
    drawdown_proxy: float
    turnover: float
    holdout_accessed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_type": "BASELINE",
            "benchmark_id": self.benchmark_id,
            "benchmark_name": self.benchmark_name,
            "weights": dict(self.weights),
            "return_proxy": self.return_proxy,
            "drawdown_proxy": self.drawdown_proxy,
            "turnover": self.turnover,
            "holdout_accessed": self.holdout_accessed,
        }


@dataclass(frozen=True)
class ComponentAttributionPayload:
    comparison_ids: tuple[str, ...]
    module_statuses: tuple[tuple[str, str], ...]
    diagnostic_window_ids: tuple[str, ...]
    holdout_accessed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_type": "COMPONENT_ATTRIBUTION",
            "comparison_ids": list(self.comparison_ids),
            "module_statuses": dict(self.module_statuses),
            "diagnostic_window_ids": list(self.diagnostic_window_ids),
            "holdout_accessed": self.holdout_accessed,
        }


@dataclass(frozen=True)
class BranchDecisionPayload:
    selected_branch: str
    synthesis_status: str
    b5_allowed: bool
    b6_allowed: bool
    v3_allowed: bool
    paper_shadow_allowed: bool
    holdout_accessed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_type": "BRANCH_DECISION",
            "selected_branch": self.selected_branch,
            "synthesis_status": self.synthesis_status,
            "b5_allowed": self.b5_allowed,
            "b6_allowed": self.b6_allowed,
            "v3_allowed": self.v3_allowed,
            "paper_shadow_allowed": self.paper_shadow_allowed,
            "holdout_accessed": self.holdout_accessed,
        }


@dataclass(frozen=True)
class MonthlyReviewPayload:
    layer_statuses: tuple[tuple[str, str], ...]
    active_blockers: tuple[str, ...]
    next_month_research_plan: tuple[str, ...]
    b5_allowed: bool
    b6_allowed: bool
    v3_allowed: bool
    holdout_accessed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_type": "MONTHLY_REVIEW",
            "layer_statuses": dict(self.layer_statuses),
            "active_blockers": list(self.active_blockers),
            "next_month_research_plan": list(self.next_month_research_plan),
            "b5_allowed": self.b5_allowed,
            "b6_allowed": self.b6_allowed,
            "v3_allowed": self.v3_allowed,
            "holdout_accessed": self.holdout_accessed,
        }


@dataclass(frozen=True)
class ProgramSnapshotPayload:
    layer_statuses: tuple[tuple[str, str], ...]
    phase_statuses: tuple[tuple[str, str], ...]
    blocked_modules: tuple[str, ...]
    conditional_modules: tuple[str, ...]
    inconclusive_modules: tuple[str, ...]
    selected_modules: tuple[str, ...]
    v3_candidate_status: str
    v3_mini_gate_status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload_type": "PROGRAM_SNAPSHOT",
            "layer_statuses": dict(self.layer_statuses),
            "phase_statuses": dict(self.phase_statuses),
            "blocked_modules": list(self.blocked_modules),
            "conditional_modules": list(self.conditional_modules),
            "inconclusive_modules": list(self.inconclusive_modules),
            "selected_modules": list(self.selected_modules),
            "v3_candidate_status": self.v3_candidate_status,
            "v3_mini_gate_status": self.v3_mini_gate_status,
        }


HistoricalRolePayload: TypeAlias = (
    BaselinePayload
    | ComponentAttributionPayload
    | BranchDecisionPayload
    | MonthlyReviewPayload
    | ProgramSnapshotPayload
)


@dataclass(frozen=True)
class HistoricalSourceAdapterRecord:
    candidate_family_id: str
    source_ref_id: str
    role: HistoricalSourceRole
    source_path: str
    artifact_identity: str
    git_blob_sha1: str
    content_sha256: str
    schema_version: int
    task_id: str
    report_type: str
    raw_status: str
    as_of: str
    data_quality: HistoricalDataQualityReceipt | None
    windows: tuple[HistoricalWindowRecord, ...]
    lineage_paths: tuple[str, ...]
    reader_brief: HistoricalReaderBrief
    limitations: tuple[str, ...]
    role_payload: HistoricalRolePayload
    research_context_complete: bool
    data_quality_ready: bool
    legacy_history_partial: bool
    research_only: bool
    manual_review_only: bool
    historical_record: bool
    current_primary_default: bool
    result_projection_allowed: bool
    page_projection_allowed: bool
    investment_conclusion_generated: bool
    production_effect: str
    broker_action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_family_id": self.candidate_family_id,
            "source_ref_id": self.source_ref_id,
            "role_code": self.role.value,
            "source_path": self.source_path,
            "artifact_identity": self.artifact_identity,
            "git_blob_sha1": self.git_blob_sha1,
            "content_sha256": self.content_sha256,
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "report_type": self.report_type,
            "raw_status": self.raw_status,
            "as_of": self.as_of,
            "data_quality": (
                self.data_quality.to_dict() if self.data_quality is not None else None
            ),
            "windows": [window.to_dict() for window in self.windows],
            "lineage_paths": list(self.lineage_paths),
            "reader_brief": self.reader_brief.to_dict(),
            "limitations": list(self.limitations),
            "role_payload": self.role_payload.to_dict(),
            "research_context_complete": self.research_context_complete,
            "data_quality_ready": self.data_quality_ready,
            "legacy_history_partial": self.legacy_history_partial,
            "research_only": self.research_only,
            "manual_review_only": self.manual_review_only,
            "historical_record": self.historical_record,
            "current_primary_default": self.current_primary_default,
            "result_projection_allowed": self.result_projection_allowed,
            "page_projection_allowed": self.page_projection_allowed,
            "investment_conclusion_generated": self.investment_conclusion_generated,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }


@dataclass(frozen=True)
class HistoricalSourceAdapterBundle:
    schema_version: str
    registry_id: str
    policy_version: str
    approval_token: str
    review_pack_id: str
    review_exact_commit: str
    evidence_exact_commit: str
    primary_research_start: str
    source_registry_id: str
    records: tuple[HistoricalSourceAdapterRecord, ...]
    production_effect: str = "none"
    broker_action: str = "none"
    historical_record: bool = True
    current_primary_default: bool = False
    result_projection_allowed: bool = False
    investment_conclusion_generated: bool = False
    investment_conclusion_allowed: bool = False
    backtest_execution_allowed: bool = False
    model_execution_allowed: bool = False
    page_projection_allowed: bool = False

    @property
    def bundle_id(self) -> str:
        canonical = json.dumps(
            self.to_dict(include_bundle_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"atlas-historical-adapters-{sha256(canonical).hexdigest()[:16]}"

    def to_dict(self, *, include_bundle_id: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "policy_version": self.policy_version,
            "approval_token": self.approval_token,
            "review_pack_id": self.review_pack_id,
            "review_exact_commit": self.review_exact_commit,
            "evidence_exact_commit": self.evidence_exact_commit,
            "primary_research_start": self.primary_research_start,
            "source_registry_id": self.source_registry_id,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
            "historical_record": self.historical_record,
            "current_primary_default": self.current_primary_default,
            "result_projection_allowed": self.result_projection_allowed,
            "investment_conclusion_generated": self.investment_conclusion_generated,
            "investment_conclusion_allowed": self.investment_conclusion_allowed,
            "backtest_execution_allowed": self.backtest_execution_allowed,
            "model_execution_allowed": self.model_execution_allowed,
            "page_projection_allowed": self.page_projection_allowed,
            "records": [record.to_dict() for record in self.records],
        }
        if include_bundle_id:
            payload["bundle_id"] = self.bundle_id
        return payload


def build_historical_source_adapter_bundle(
    *,
    repository_root: Path,
    exact_commit: str,
    adapter_registry_path: Path | None = None,
    source_registry_path: Path | None = None,
) -> HistoricalSourceAdapterBundle:
    """Build the approved bundle from exact Git blobs and tracked registries."""

    root = repository_root.resolve()
    _require_exact_commit(exact_commit)
    _ensure_git_commit(root, exact_commit)
    adapter_path = adapter_registry_path or (
        root / "config" / "atlas" / "historical_source_adapters.yaml"
    )
    adapter_registry = _load_yaml_mapping(_read_inside_repository(root, adapter_path))
    configured_source_path = _required_str(adapter_registry, "source_registry_path")
    source_path = source_registry_path or (root / PurePosixPath(configured_source_path))
    source_registry = _load_yaml_mapping(_read_inside_repository(root, source_path))

    source_payloads: dict[str, bytes] = {}
    source_blob_sha1s: dict[str, str] = {}
    for item in _mapping_sequence(adapter_registry, "adapters"):
        path = _safe_repository_path(_required_str(item, "source_path"))
        source_payloads[path] = _git_blob_bytes(root, exact_commit, path)
        source_blob_sha1s[path] = _git_blob_sha1(root, exact_commit, path)

    return build_historical_source_adapter_bundle_from_payloads(
        exact_commit=exact_commit,
        adapter_registry=adapter_registry,
        source_registry=source_registry,
        source_payloads=source_payloads,
        source_blob_sha1s=source_blob_sha1s,
    )


def build_historical_source_adapter_bundle_from_payloads(
    *,
    exact_commit: str,
    adapter_registry: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    source_payloads: Mapping[str, bytes],
    source_blob_sha1s: Mapping[str, str],
) -> HistoricalSourceAdapterBundle:
    """Pure builder used by runtime and deterministic tamper tests."""

    _require_exact_commit(exact_commit)
    schema_version = _required_str(adapter_registry, "schema_version")
    if schema_version != "atlas_historical_source_adapter_registry.v1":
        raise HistoricalSourceAdapterError(f"unsupported adapter registry schema: {schema_version}")
    _require_bool(adapter_registry, "investment_conclusion_allowed", False)
    _require_bool(adapter_registry, "backtest_execution_allowed", False)
    _require_bool(adapter_registry, "model_execution_allowed", False)
    _require_bool(adapter_registry, "page_projection_allowed", False)
    if _required_str(adapter_registry, "production_effect") != "none":
        raise HistoricalSourceAdapterError("production_effect must remain none")
    if _required_str(adapter_registry, "broker_action") != "none":
        raise HistoricalSourceAdapterError("broker_action must remain none")
    _require_bool(adapter_registry, "historical_record", True)
    _require_bool(adapter_registry, "current_primary_default", False)
    _require_bool(adapter_registry, "result_projection_allowed", False)
    _require_bool(adapter_registry, "investment_conclusion_generated", False)
    if _required_str(adapter_registry, "primary_research_start") != "2021-02-22":
        raise HistoricalSourceAdapterError(
            "primary_research_start must remain the governed 2021-02-22 default"
        )

    registered_sources = _index_registered_sources(source_registry)
    expected_source_count = _required_int(adapter_registry, "expected_registered_source_count")
    if len(registered_sources) != expected_source_count:
        raise HistoricalSourceAdapterError(
            "source registry count mismatch: "
            f"expected {expected_source_count}, got {len(registered_sources)}"
        )

    configured_entries = _mapping_sequence(adapter_registry, "adapters")
    if len(configured_entries) != 5:
        raise HistoricalSourceAdapterError("exactly five approved adapters are required")

    records: list[HistoricalSourceAdapterRecord] = []
    candidate_ids: set[str] = set()
    source_ref_ids: set[str] = set()
    source_paths: set[str] = set()
    roles: set[HistoricalSourceRole] = set()
    for entry in configured_entries:
        candidate_id = _required_str(entry, "candidate_family_id")
        source_ref_id = _required_str(entry, "source_ref_id")
        if candidate_id in candidate_ids or source_ref_id in source_ref_ids:
            raise HistoricalSourceAdapterError("duplicate adapter identity")
        candidate_ids.add(candidate_id)
        source_ref_ids.add(source_ref_id)
        source_path = _safe_repository_path(_required_str(entry, "source_path"))
        role = _parse_role(_required_str(entry, "role_code"))
        if source_path in source_paths or role in roles:
            raise HistoricalSourceAdapterError("duplicate adapter path or role")
        source_paths.add(source_path)
        roles.add(role)
        if source_path in _FORBIDDEN_SOURCE_PATHS:
            raise HistoricalSourceAdapterError(
                f"excluded historical source cannot be adapted: {source_path}"
            )
        try:
            payload_bytes = source_payloads[source_path]
            blob_sha1 = source_blob_sha1s[source_path]
        except KeyError as exc:
            raise HistoricalSourceAdapterError(
                f"missing exact Git payload for {source_path}"
            ) from exc
        _verify_approved_identity(entry, payload_bytes, blob_sha1)
        payload = _load_json_mapping(payload_bytes)
        _verify_payload_contract(entry, payload)
        registered_source = registered_sources.get(source_ref_id)
        if registered_source is None:
            raise HistoricalSourceAdapterError(
                f"missing Atlas source registration: {source_ref_id}"
            )
        _verify_source_registration(entry, registered_source)
        records.append(
            _build_record(
                entry=entry,
                registered_source=registered_source,
                payload=payload,
                payload_bytes=payload_bytes,
                blob_sha1=blob_sha1,
                role=role,
            )
        )

    if roles != set(HistoricalSourceRole):
        raise HistoricalSourceAdapterError("approved historical role set is incomplete")

    return HistoricalSourceAdapterBundle(
        schema_version="atlas_historical_source_adapter_bundle.v1",
        registry_id=_required_str(adapter_registry, "registry_id"),
        policy_version=_required_str(adapter_registry, "policy_version"),
        approval_token=_required_str(adapter_registry, "approval_token"),
        review_pack_id=_required_str(adapter_registry, "review_pack_id"),
        review_exact_commit=_required_str(adapter_registry, "review_exact_commit"),
        evidence_exact_commit=exact_commit,
        primary_research_start=_required_str(adapter_registry, "primary_research_start"),
        source_registry_id=_required_str(source_registry, "registry_id"),
        records=tuple(records),
    )


def _build_record(
    *,
    entry: Mapping[str, Any],
    registered_source: Mapping[str, Any],
    payload: Mapping[str, Any],
    payload_bytes: bytes,
    blob_sha1: str,
    role: HistoricalSourceRole,
) -> HistoricalSourceAdapterRecord:
    role_payload, windows = _parse_role_payload(role, payload)
    safety = _required_mapping(payload, "safety_boundary")
    _require_bool(safety, "research_only", True)
    _require_bool(safety, "manual_review_only", True)
    _require_bool(safety, "official_target_weights", False)
    _require_bool(safety, "broker_action_allowed", False)
    if _required_str(safety, "production_effect") != "none":
        raise HistoricalSourceAdapterError("historical source has production effect")
    if "forbidden_outputs_absent" in payload:
        _require_bool(payload, "forbidden_outputs_absent", True)

    reader = _required_mapping(payload, "reader_brief")
    reader_brief = HistoricalReaderBrief(
        summary=_required_str(reader, "summary"),
        key_result=_required_str(reader, "key_result"),
        blocking_issues=_required_str(reader, "blocking_issues"),
        warnings=_required_str(reader, "warnings"),
        next_action=_required_str(reader, "next_action"),
        safety_boundary=_required_str(reader, "safety_boundary"),
    )
    dq_value = payload.get("data_quality_gate")
    data_quality: HistoricalDataQualityReceipt | None = None
    if dq_value is not None:
        if not isinstance(dq_value, Mapping):
            raise HistoricalSourceAdapterError("data_quality_gate must be an object")
        dq_status = str(dq_value.get("status") or dq_value.get("run_status") or "").strip()
        if not dq_status:
            raise HistoricalSourceAdapterError("data_quality_gate status is missing")
        data_quality = HistoricalDataQualityReceipt(
            passed=_required_bool(dq_value, "passed"),
            status=dq_status,
            report_path=_canonical_artifact_path(_required_str(dq_value, "report_path")),
            error_count=_required_int(dq_value, "error_count"),
            warning_count=_required_int(dq_value, "warning_count"),
        )
    limitations = _collect_limitations(payload, registered_source, reader_brief)

    return HistoricalSourceAdapterRecord(
        candidate_family_id=_required_str(entry, "candidate_family_id"),
        source_ref_id=_required_str(entry, "source_ref_id"),
        role=role,
        source_path=_required_str(entry, "source_path"),
        artifact_identity=_required_str(entry, "artifact_identity"),
        git_blob_sha1=blob_sha1,
        content_sha256=sha256(payload_bytes).hexdigest(),
        schema_version=_required_int(payload, "schema_version"),
        task_id=_required_str(payload, "task_id"),
        report_type=_required_str(payload, "report_type"),
        raw_status=_required_str(payload, "status"),
        as_of=str(payload.get("as_of") or payload.get("generated_at") or ""),
        data_quality=data_quality,
        windows=windows,
        lineage_paths=_extract_lineage(role, payload),
        reader_brief=reader_brief,
        limitations=limitations,
        role_payload=role_payload,
        research_context_complete=_required_bool(registered_source, "research_context_complete"),
        data_quality_ready=_required_bool(registered_source, "data_quality_ready"),
        legacy_history_partial=_required_bool(registered_source, "legacy_history_partial"),
        research_only=True,
        manual_review_only=True,
        historical_record=True,
        current_primary_default=False,
        result_projection_allowed=False,
        page_projection_allowed=False,
        investment_conclusion_generated=False,
        production_effect="none",
        broker_action="none",
    )


def _parse_role_payload(
    role: HistoricalSourceRole, payload: Mapping[str, Any]
) -> tuple[HistoricalRolePayload, tuple[HistoricalWindowRecord, ...]]:
    role_payload: HistoricalRolePayload
    windows: tuple[HistoricalWindowRecord, ...]
    if role is HistoricalSourceRole.BASELINE:
        window = _required_mapping(payload, "window")
        baseline = _required_mapping(payload, "baseline_source")
        metrics = _required_mapping(payload, "required_outputs")
        weights = _required_mapping(baseline, "weights")
        role_payload = BaselinePayload(
            benchmark_id=_required_str(baseline, "benchmark_id"),
            benchmark_name=_required_str(baseline, "benchmark_name"),
            weights=tuple(
                (str(key), _required_number_value(value, f"weights.{key}"))
                for key, value in sorted(weights.items())
            ),
            return_proxy=_required_number(metrics, "return_proxy"),
            drawdown_proxy=_required_number(metrics, "drawdown_proxy"),
            turnover=_required_number(metrics, "turnover"),
            holdout_accessed=_required_bool(window, "holdout_usage"),
        )
        windows = (
            HistoricalWindowRecord(
                window_id=_required_str(window, "window_id"),
                source_field="window",
                requested_start=_required_str(window, "start_date"),
                requested_end=_required_str(window, "end_date"),
                evaluated_start=_required_str(window, "effective_signal_start"),
                evaluated_end=_required_str(window, "effective_signal_end"),
            ),
        )
        return role_payload, windows

    if role is HistoricalSourceRole.COMPONENT_ATTRIBUTION:
        comparisons = _mapping_sequence(payload, "comparisons")
        modules = _mapping_sequence(payload, "module_usefulness")
        window_ids = tuple(sorted({_required_str(item, "window_result") for item in comparisons}))
        role_payload = ComponentAttributionPayload(
            comparison_ids=tuple(_required_str(item, "comparison_id") for item in comparisons),
            module_statuses=tuple(
                (
                    _required_str(item, "layer_id"),
                    _required_str(item, "independently_useful_status"),
                )
                for item in modules
            ),
            diagnostic_window_ids=window_ids,
            holdout_accessed=_required_bool(payload, "holdout_accessed"),
        )
        windows = tuple(
            HistoricalWindowRecord(
                window_id=window_id,
                source_field="comparisons[].window_result",
            )
            for window_id in window_ids
        )
        return role_payload, windows

    if role is HistoricalSourceRole.BRANCH_DECISION:
        role_payload = BranchDecisionPayload(
            selected_branch=_required_str(payload, "selected_branch"),
            synthesis_status=_required_str(payload, "synthesis_status"),
            b5_allowed=_required_bool(payload, "b5_allowed"),
            b6_allowed=_required_bool(payload, "b6_allowed"),
            v3_allowed=_required_bool(payload, "v3_allowed"),
            paper_shadow_allowed=_required_bool(payload, "paper_shadow_allowed"),
            holdout_accessed=_required_bool(payload, "holdout_accessed"),
        )
        return role_payload, (_requested_window(payload),)

    if role is HistoricalSourceRole.MONTHLY_REVIEW:
        role_payload = MonthlyReviewPayload(
            layer_statuses=tuple(
                (layer, _required_str(payload, f"{layer}_status"))
                for layer in ("B0", "B1", "B2", "B3", "B4")
            ),
            active_blockers=_string_sequence(payload, "active_blockers"),
            next_month_research_plan=_string_sequence(payload, "next_month_research_plan"),
            b5_allowed=_required_bool(payload, "b5_allowed"),
            b6_allowed=_required_bool(payload, "b6_allowed"),
            v3_allowed=_required_bool(payload, "v3_allowed"),
            holdout_accessed=_required_bool(payload, "holdout_accessed"),
        )
        return role_payload, (_requested_window(payload),)

    if role is HistoricalSourceRole.PROGRAM_SNAPSHOT:
        layers = _mapping_sequence(payload, "b0_to_b6_results")
        phases = _mapping_sequence(payload, "phase_statuses")
        role_payload = ProgramSnapshotPayload(
            layer_statuses=tuple(
                (_required_str(item, "layer_id"), _required_str(item, "status")) for item in layers
            ),
            phase_statuses=tuple(
                (_required_str(item, "phase"), _required_str(item, "status")) for item in phases
            ),
            blocked_modules=_string_sequence(payload, "blocked_modules"),
            conditional_modules=_string_sequence(payload, "conditional_modules"),
            inconclusive_modules=_string_sequence(payload, "inconclusive_modules"),
            selected_modules=_string_sequence(payload, "selected_modules"),
            v3_candidate_status=_required_str(payload, "v3_candidate_status"),
            v3_mini_gate_status=_required_str(payload, "v3_mini_gate_status"),
        )
        return role_payload, ()

    raise HistoricalSourceAdapterError(f"unsupported historical role: {role.value}")


def _requested_window(payload: Mapping[str, Any]) -> HistoricalWindowRecord:
    window = _required_mapping(payload, "requested_date_range")
    start = _required_str(window, "start_date")
    end = _required_str(window, "end_date")
    return HistoricalWindowRecord(
        window_id=f"requested_{start}_to_{end}",
        source_field="requested_date_range",
        requested_start=start,
        requested_end=end,
    )


def _extract_lineage(role: HistoricalSourceRole, payload: Mapping[str, Any]) -> tuple[str, ...]:
    paths: list[str] = []
    if role is HistoricalSourceRole.BASELINE:
        baseline = _required_mapping(payload, "baseline_source")
        artifacts = _required_mapping(payload, "backtest_artifacts")
        dq = _required_mapping(payload, "data_quality_gate")
        for key in ("source_config_path", "backtest_config_path"):
            paths.append(_required_str(baseline, key))
        for key in ("summary_json", "metrics_json"):
            paths.append(_required_str(artifacts, key))
        paths.append(_required_str(dq, "report_path"))
        audit_path = dq.get("audit_record_path")
        if isinstance(audit_path, str) and audit_path.strip():
            paths.append(audit_path)
    elif role in {
        HistoricalSourceRole.COMPONENT_ATTRIBUTION,
        HistoricalSourceRole.BRANCH_DECISION,
        HistoricalSourceRole.MONTHLY_REVIEW,
    }:
        source_artifacts = _required_mapping(payload, "source_artifacts")
        paths.extend(
            _required_str_value(value, "source_artifacts") for value in source_artifacts.values()
        )
        paths.append(_required_str(_required_mapping(payload, "data_quality_gate"), "report_path"))
    else:
        paths.extend(_string_sequence(payload, "included_artifacts"))
    return tuple(dict.fromkeys(_canonical_artifact_path(path) for path in paths))


def _collect_limitations(
    payload: Mapping[str, Any],
    registered_source: Mapping[str, Any],
    reader: HistoricalReaderBrief,
) -> tuple[str, ...]:
    limitations: list[str] = [_required_str(registered_source, "limitation")]
    raw_limitations = payload.get("limitations", [])
    if raw_limitations:
        if not isinstance(raw_limitations, Sequence) or isinstance(raw_limitations, (str, bytes)):
            raise HistoricalSourceAdapterError("limitations must be a list")
        limitations.extend(_required_str_value(item, "limitations") for item in raw_limitations)
    limitations.extend([reader.warnings, reader.blocking_issues])
    return tuple(dict.fromkeys(item for item in limitations if item != "none"))


def _verify_approved_identity(
    entry: Mapping[str, Any], payload_bytes: bytes, blob_sha1: str
) -> None:
    expected_blob = _required_str(entry, "approved_git_blob_sha1")
    expected_sha256 = _required_str(entry, "approved_sha256")
    if not _SHA1_RE.fullmatch(expected_blob) or not _SHA1_RE.fullmatch(blob_sha1):
        raise HistoricalSourceAdapterError("invalid Git blob SHA-1")
    if not _SHA256_RE.fullmatch(expected_sha256):
        raise HistoricalSourceAdapterError("invalid approved SHA-256")
    if blob_sha1 != expected_blob:
        raise HistoricalSourceAdapterError(
            f"Git blob mismatch for {_required_str(entry, 'source_path')}"
        )
    actual_sha256 = sha256(payload_bytes).hexdigest()
    if actual_sha256 != expected_sha256:
        raise HistoricalSourceAdapterError(
            f"content SHA-256 mismatch for {_required_str(entry, 'source_path')}"
        )


def _verify_payload_contract(entry: Mapping[str, Any], payload: Mapping[str, Any]) -> None:
    if _required_int(payload, "schema_version") != _required_int(entry, "expected_schema_version"):
        raise HistoricalSourceAdapterError("historical schema version mismatch")
    if _required_str(payload, "task_id") != _required_str(entry, "expected_task_id"):
        raise HistoricalSourceAdapterError("historical task id mismatch")
    if _required_str(payload, "report_type") != _required_str(entry, "expected_report_type"):
        raise HistoricalSourceAdapterError("historical report type mismatch")
    for field in _string_sequence(entry, "required_fields"):
        if field not in payload:
            raise HistoricalSourceAdapterError(f"missing required historical field: {field}")


def _verify_source_registration(entry: Mapping[str, Any], registered: Mapping[str, Any]) -> None:
    expected = {
        "source_kind": "PUBLISHED_ARTIFACT",
        "source_path": _required_str(entry, "source_path"),
        "artifact_identity": _required_str(entry, "artifact_identity"),
    }
    for key, value in expected.items():
        if _required_str(registered, key) != value:
            raise HistoricalSourceAdapterError(f"source registration mismatch for {key}: {value}")
    _require_bool(registered, "research_context_complete", False)
    _require_bool(registered, "data_quality_ready", False)
    _require_bool(registered, "legacy_history_partial", True)


def _index_registered_sources(
    source_registry: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    sources = _mapping_sequence(source_registry, "sources")
    indexed: dict[str, Mapping[str, Any]] = {}
    for source in sources:
        source_id = _required_str(source, "source_ref_id")
        if source_id in indexed:
            raise HistoricalSourceAdapterError(f"duplicate Atlas source_ref_id: {source_id}")
        indexed[source_id] = source
    return indexed


def _parse_role(value: str) -> HistoricalSourceRole:
    try:
        return HistoricalSourceRole(value)
    except ValueError as exc:
        raise HistoricalSourceAdapterError(f"unsupported role_code: {value}") from exc


def _canonical_artifact_path(value: str) -> str:
    normalized = value.replace("\\", "/")
    marker = "/AITradingSystem/"
    if marker in normalized:
        normalized = normalized.split(marker, maxsplit=1)[1]
    return normalized


def _safe_repository_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise HistoricalSourceAdapterError(f"unsafe repository path: {value}")
    return path.as_posix()


def _read_inside_repository(repository_root: Path, path: Path) -> bytes:
    resolved = path.resolve()
    try:
        resolved.relative_to(repository_root)
    except ValueError as exc:
        raise HistoricalSourceAdapterError(f"registry path escapes repository: {path}") from exc
    return resolved.read_bytes()


def _git_blob_bytes(repository_root: Path, exact_commit: str, source_path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{exact_commit}:{source_path}"],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise HistoricalSourceAdapterError(
            f"cannot read exact Git blob {exact_commit}:{source_path}"
        )
    return result.stdout


def _git_blob_sha1(repository_root: Path, exact_commit: str, source_path: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", f"{exact_commit}:{source_path}"],
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise HistoricalSourceAdapterError(
            f"cannot resolve exact Git blob {exact_commit}:{source_path}"
        )
    value = result.stdout.strip()
    if not _SHA1_RE.fullmatch(value):
        raise HistoricalSourceAdapterError(f"invalid resolved Git blob SHA-1: {value}")
    return value


def _ensure_git_commit(repository_root: Path, exact_commit: str) -> None:
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{exact_commit}^{{commit}}"],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise HistoricalSourceAdapterError(f"unknown exact Git commit: {exact_commit}")


def _require_exact_commit(value: str) -> None:
    if not _COMMIT_RE.fullmatch(value):
        raise HistoricalSourceAdapterError("exact_commit must be a 40-character SHA")


def _load_yaml_mapping(payload: bytes) -> Mapping[str, Any]:
    loaded = yaml.safe_load(payload.decode("utf-8"))
    if not isinstance(loaded, Mapping):
        raise HistoricalSourceAdapterError("YAML registry root must be an object")
    return loaded


def _load_json_mapping(payload: bytes) -> Mapping[str, Any]:
    try:
        loaded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HistoricalSourceAdapterError("historical source is not valid UTF-8 JSON") from exc
    if not isinstance(loaded, Mapping):
        raise HistoricalSourceAdapterError("historical JSON root must be an object")
    return loaded


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise HistoricalSourceAdapterError(f"{key} must be an object")
    return value


def _mapping_sequence(payload: Mapping[str, Any], key: str) -> tuple[Mapping[str, Any], ...]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise HistoricalSourceAdapterError(f"{key} must be a list")
    result: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise HistoricalSourceAdapterError(f"{key} entries must be objects")
        result.append(item)
    return tuple(result)


def _string_sequence(payload: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise HistoricalSourceAdapterError(f"{key} must be a list")
    return tuple(_required_str_value(item, key) for item in value)


def _required_str(payload: Mapping[str, Any], key: str) -> str:
    return _required_str_value(payload.get(key), key)


def _required_str_value(value: Any, key: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HistoricalSourceAdapterError(f"{key} must be a non-empty string")
    return value


def _required_bool(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise HistoricalSourceAdapterError(f"{key} must be a boolean")
    return value


def _require_bool(payload: Mapping[str, Any], key: str, expected: bool) -> None:
    value = _required_bool(payload, key)
    if value is not expected:
        raise HistoricalSourceAdapterError(f"{key} must remain {expected}")


def _required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise HistoricalSourceAdapterError(f"{key} must be an integer")
    return value


def _required_number(payload: Mapping[str, Any], key: str) -> float:
    return _required_number_value(payload.get(key), key)


def _required_number_value(value: Any, key: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise HistoricalSourceAdapterError(f"{key} must be numeric")
    return float(value)


__all__ = [
    "BaselinePayload",
    "BranchDecisionPayload",
    "ComponentAttributionPayload",
    "HistoricalDataQualityReceipt",
    "HistoricalReaderBrief",
    "HistoricalSourceAdapterBundle",
    "HistoricalSourceAdapterError",
    "HistoricalSourceAdapterRecord",
    "HistoricalSourceRole",
    "HistoricalWindowRecord",
    "MonthlyReviewPayload",
    "ProgramSnapshotPayload",
    "build_historical_source_adapter_bundle",
    "build_historical_source_adapter_bundle_from_payloads",
]
