"""Pinned immutable-input DQ execution and zero-DQ process-local verification.

The trusted coordinator owns the existing lease and dispatch provenance.  A
receipt/hash is not a signature or an external-action authorization.  Only the
fresh Git-byte bootstrap supplies the execution context used by this module.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import NoReturn, Protocol, cast

import pandas as pd
from pydantic import TypeAdapter

from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_attribution import (
    DataQualityCalendarBinding,
    _function_ast_hash,
    build_reviewed_calendar_binding,
    canonical_price_row_digest,
    load_price_non_market_session_attribution_decision,
)
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityDateWindow,
    DataQualityImplementationSourceBinding,
    DataQualityInvocationParameter,
    DataQualityReportBinding,
    DataQualityValidatorBinding,
    _strict_json_loads,
    canonical_json_value,
)
from ai_trading_system.contracts.named_data_quality_execution import (
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
    FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
    PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    NamedArtifactBinding,
    NamedDQExecutionReceipt,
    NamedDQExecutionRequest,
    NamedDQInputBinding,
    NamedDQSuccessfulDispatchBinding,
    NamedExecutionObservation,
    NamedManifestBinding,
    NamedManifestRowBinding,
    NamedPublicationBinding,
    VerifiedNamedInputs,
    _verified_named_inputs_from_receipt,
)
from ai_trading_system.contracts.named_execution_context import (
    NamedExecutionContext,
    require_named_execution_context,
)
from ai_trading_system.contracts.rate_data_quality_attribution import (
    canonical_rate_row_digest,
    load_rate_row_issue_attribution_decision,
)
from ai_trading_system.data.download_publication import (
    ValidatedNamedDownloadPublication,
    resolve_named_download_publication,
)
from ai_trading_system.data.immutable_publish import (
    read_contained_artifact_bytes,
    validate_named_snapshot,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality import (
    DataFileSnapshot,
    DataFileSummary,
    DataQualityReport,
    Severity,
    render_data_quality_report,
    validate_data_cache,
)
from ai_trading_system.data.quality_execution import (
    DATA_QUALITY_CONTRACT_ID,
    ReviewedDataQualityPolicy,
    _parse_report,
    load_reviewed_data_quality_policy,
)
from ai_trading_system.data.quality_provenance import (
    inspect_csv_content,
    manifest_record_ref,
    match_named_manifest_member,
    parse_manifest_content,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day, us_equity_calendar_source
from ai_trading_system.us_equity_special_closure_policy import (
    CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH,
    default_us_equity_special_closure_policy,
)

VALIDATOR_VERSION = "named_quality_execution.run_named_data_quality_execution.v1"
VALIDATOR_ENTRYPOINT = (
    "ai_trading_system.data.named_quality_execution:run_named_data_quality_execution"
)
REPORT_SCHEMA = "named_data_quality_report_bundle.v1"
# Protocol mappings from the existing download-publication role contract.  These
# are filenames, not policy knobs or an inference from a relocated basename.
_ROLE_FILES = {
    "prices": ("prices_daily.csv", "prices_daily.v1", "primary_market_prices"),
    "rates": ("rates_daily.csv", "rates_daily.v1", "primary_macro_rates"),
    "secondary_prices": (
        "prices_marketstack_daily.csv",
        "prices_daily.v1",
        "secondary_market_prices",
    ),
}


class NamedDataQualityExecutionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _fail(code: str, message: str) -> NoReturn:
    raise NamedDataQualityExecutionError(code, message)


class _CapturedDependency(Protocol):
    @property
    def content(self) -> bytes: ...


class NamedBootstrapAuthority(Protocol):
    """Narrow structural interface; the live context is independently required."""

    @property
    def operation(self) -> str: ...

    @property
    def context(self) -> NamedExecutionContext | None: ...

    @property
    def dependencies(self) -> Mapping[str, _CapturedDependency]: ...

    @property
    def source_lease_id(self) -> str: ...

    @property
    def imports_completed_at(self) -> str | None: ...

    @property
    def pre_dq_checked_at(self) -> str | None: ...

    @property
    def canonical_dq_call_count(self) -> int: ...

    def assert_execution_unchanged(self, *, stage: str) -> str: ...

    def note_canonical_dq_dispatch(self) -> None: ...


@dataclass(frozen=True)
class CapturedNamedPublication:
    named: ValidatedNamedDownloadPublication
    publication: NamedPublicationBinding
    manifest: NamedManifestBinding
    inputs: tuple[NamedDQInputBinding, ...]
    snapshots: Mapping[str, DataFileSnapshot]
    captured: tuple[tuple[str, bytes], ...]


@dataclass(frozen=True)
class NamedDataQualityExecutionResult:
    receipt: NamedDQExecutionReceipt
    receipt_relative_path: str
    report: DataQualityReport


def _json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _object(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        _fail("NAMED_DQ_REPORT_FIELDS_INVALID", label)
    return cast(Mapping[str, object], value)


def _artifact(role: str, path: str, content: bytes) -> NamedArtifactBinding:
    return NamedArtifactBinding(role, path, sha256(content).hexdigest(), len(content))


def _publication_artifact(root: Path, path: Path) -> NamedArtifactBinding:
    relative = path.relative_to(root).as_posix()
    return _artifact(
        "PUBLICATION", relative, read_contained_artifact_bytes(root=root, relative_path=relative)
    )


def _observe_dates(content: bytes) -> tuple[date | None, date | None]:
    frame = pd.read_csv(io.BytesIO(content))
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date(), dates.max().date()


def capture_named_publication(request: NamedDQExecutionRequest) -> CapturedNamedPublication:
    """Read the exact named publication and capture its actual immutable members.

    This is structural provenance, not a DQ execution or a verified-input seal.
    The original source root is never read and may be unavailable after relocation.
    """
    root = Path(request.roots.publication_root)
    selector = request.selector
    named = resolve_named_download_publication(
        output_dir=root,
        pointer_id=selector.pointer_id,
        expected_pointer_sha256=selector.pointer_sha256,
        expected_transaction_id=selector.transaction_id,
        expected_transaction_sha256=selector.transaction_sha256,
    )
    pub, snapshot = named.publication, named.snapshot
    transaction_window = DataQualityDateWindow(pub.requested_start, pub.requested_end)
    if not transaction_window.contains(request.scope.requested_window):
        _fail("NAMED_DQ_PUBLICATION_WINDOW_MISMATCH", request.request_id)
    anchor = snapshot.commit_anchor
    pointer_relative = snapshot.pointer_path.relative_to(root).as_posix()
    pointer_content = read_contained_artifact_bytes(root=root, relative_path=pointer_relative)
    if sha256(pointer_content).hexdigest() != selector.pointer_sha256:
        _fail("NAMED_DQ_PUBLICATION_CHANGED", "selected pointer changed before capture")
    pointer_payload = _object(_strict_json_loads(pointer_content.decode("utf-8")), "pointer")
    publication = NamedPublicationBinding(
        dataset_id=snapshot.dataset_id,
        snapshot_id=snapshot.snapshot_id,
        generation=snapshot.generation,
        pointer_id=snapshot.pointer_id,
        pointer=_artifact("PUBLICATION", pointer_relative, pointer_content),
        transaction_id=pub.transaction_id,
        transaction=_publication_artifact(root, pub.transaction_manifest_path),
        snapshot_manifest_id=snapshot.manifest_id,
        snapshot_manifest=_publication_artifact(root, snapshot.manifest_path),
        source_event_id=snapshot.source_event_id,
        source_event=_publication_artifact(root, snapshot.source_event_path),
        transaction_window=transaction_window,
        anchor_dataset_id=anchor.dataset_id,
        anchor_pointer_id=anchor.pointer_id,
        anchor_generation=anchor.generation,
        anchor_pointer=_publication_artifact(root, anchor.pointer_path),
    )
    if (
        publication.pointer.sha256 != selector.pointer_sha256
        or publication.transaction.sha256 != selector.transaction_sha256
        or publication.anchor_pointer.sha256 != anchor.pointer_sha256
    ):
        _fail("NAMED_DQ_PUBLICATION_CHANGED", "resolved publication changed during capture")
    store_relative = anchor.pointer_path.parent.parent.relative_to(root)
    for field, observed in (
        ("manifest", publication.snapshot_manifest),
        ("source_event", publication.source_event),
        ("snapshot", publication.transaction),
    ):
        reference = _object(pointer_payload.get(field), f"pointer.{field}")
        referenced_path = reference.get("path")
        if (
            not isinstance(referenced_path, str)
            or observed.relative_path != (store_relative / referenced_path).as_posix()
            or observed.sha256 != reference.get("sha256")
            or observed.size_bytes != reference.get("size_bytes")
        ):
            _fail("NAMED_DQ_PUBLICATION_METADATA_MISMATCH", field)
    manifest_content = read_contained_artifact_bytes(
        root=root, relative_path=pub.manifest_path.relative_to(root).as_posix()
    )
    if sha256(manifest_content).hexdigest() != pub.manifest_sha256:
        _fail("NAMED_DQ_MANIFEST_CHANGED", "captured manifest differs from resolved bytes")
    columns, rows = parse_manifest_content(manifest_content)
    manifest = NamedManifestBinding(
        member=_artifact(
            "PUBLICATION", pub.manifest_path.relative_to(root).as_posix(), manifest_content
        ),
        columns=columns,
        rows=tuple(
            NamedManifestRowBinding(
                source_ordinal=index,
                fields=tuple((key, row[key]) for key in columns),
                row_sha256=manifest_record_ref(row).removeprefix("manifest_record_"),
                record_ref=manifest_record_ref(row),
            )
            for index, row in enumerate(rows)
        ),
    )
    paths = {"prices": pub.prices_path, "rates": pub.rates_path}
    if pub.secondary_prices_path is not None:
        paths["secondary_prices"] = pub.secondary_prices_path
    if not set(request.scope.input_roles).issubset(paths):
        _fail("NAMED_DQ_INPUT_ROLE_MISSING", request.request_id)
    inputs: list[NamedDQInputBinding] = []
    captured: list[tuple[str, bytes]] = []
    snapshots = {"manifest": DataFileSnapshot(pub.manifest_path, True, manifest_content)}
    for role in sorted(request.scope.input_roles):
        path = paths[role]
        relative = path.relative_to(root).as_posix()
        content = read_contained_artifact_bytes(root=root, relative_path=relative)
        member = _artifact("PUBLICATION", relative, content)
        _, row_count = inspect_csv_content(content)
        if member.sha256 != pub.artifact_sha256[role] or row_count != pub.artifact_row_count[role]:
            _fail("NAMED_DQ_MEMBER_CHANGED", role)
        filename, schema, source_role = _ROLE_FILES[role]
        source_relative = request.source_output_relative_path + "/" + filename
        original_path = (Path(request.roots.source_root) / source_relative).as_posix()
        match = match_named_manifest_member(
            manifest_content,
            transaction_id=pub.transaction_id,
            role=role,
            original_output_path=original_path,
            member_sha256=member.sha256,
            member_row_count=row_count,
        )
        first, last = _observe_dates(content)
        inputs.append(
            NamedDQInputBinding(
                role=role,
                schema_id=schema,
                source_role=source_role,
                member=member,
                row_count=row_count,
                observed_min_date=first,
                observed_max_date=last,
                manifest_row_ordinal=match.ordinal,
                manifest_row_sha256=manifest.rows[match.ordinal].row_sha256,
                original_output_path=match.row["output_path"],
                source_relative_path=source_relative,
            )
        )
        captured.append((role, content))
        snapshots[role] = DataFileSnapshot(path, True, content)
    return CapturedNamedPublication(
        named, publication, manifest, tuple(inputs), snapshots, tuple(captured)
    )


def _dependencies(bootstrap: NamedBootstrapAuthority) -> tuple[NamedArtifactBinding, ...]:
    return tuple(
        _artifact("EXECUTION", path, item.content)
        for path, item in sorted(bootstrap.dependencies.items())
    )


def _execution_binding(
    request: NamedDQExecutionRequest, bootstrap: NamedBootstrapAuthority
) -> NamedExecutionContext:
    context = require_named_execution_context()
    identity = context.identity
    if (
        bootstrap.context is not context
        or identity.execution_root != request.roots.execution_root
        or identity.candidate_commit != request.candidate_commit
        or identity.source_manifest_path != request.source_manifest_path
        or identity.source_manifest_sha256 != request.source_manifest_sha256
    ):
        _fail("NAMED_DQ_EXECUTION_IDENTITY_MISMATCH", request.request_id)
    return context


def _policy_and_calendar(
    request: NamedDQExecutionRequest,
    bootstrap: NamedBootstrapAuthority,
) -> tuple[ReviewedDataQualityPolicy, DataQualityCalendarBinding, NamedArtifactBinding]:
    root = Path(request.roots.execution_root)
    dependencies = {item.relative_path: item for item in _dependencies(bootstrap)}
    policy = load_reviewed_data_quality_policy(Path(request.policy_path), project_root=root)
    if dependencies.get(request.policy_path) is None or (
        dependencies[request.policy_path].sha256 != policy.binding.sha256
    ):
        _fail("NAMED_DQ_POLICY_IDENTITY_MISMATCH", request.policy_path)
    # This is the actual lru-cached object used by trading_calendar, not a fresh
    # unrelated load. Never clear/reload its cache to manufacture agreement.
    actual = default_us_equity_special_closure_policy()
    calendar_path = CURRENT_US_EQUITY_SPECIAL_CLOSURE_POLICY_RELATIVE_PATH.as_posix()
    calendar_artifact = dependencies.get(calendar_path)
    if (
        calendar_artifact is None
        or actual.path != root / calendar_path
        or actual.sha256 != calendar_artifact.sha256
    ):
        _fail("NAMED_DQ_CALENDAR_IDENTITY_MISMATCH", calendar_path)
    calendar = DataQualityCalendarBinding(
        calendar_id=actual.calendar_id,
        calendar_source=us_equity_calendar_source(),
        calendar_function="is_us_equity_trading_day",
        calendar_function_ast_sha256=_function_ast_hash(
            root / "src/ai_trading_system/trading_calendar.py", "is_us_equity_trading_day"
        ),
        special_closure_policy_id=actual.policy_id,
        special_closure_policy_version=actual.policy_version,
        special_closure_policy_sha256=actual.sha256,
    )
    return policy, calendar, calendar_artifact


def _validator(context: NamedExecutionContext) -> DataQualityValidatorBinding:
    return DataQualityValidatorBinding(
        validator_id="aits.validate-data",
        validator_version=VALIDATOR_VERSION,
        entrypoint=VALIDATOR_ENTRYPOINT,
        implementation_sources=tuple(
            DataQualityImplementationSourceBinding(path=item.source_path, sha256=item.sha256)
            for item in context.identity.modules
        ),
    )


def _evaluated_window(
    request: NamedDQExecutionRequest, inputs: tuple[NamedDQInputBinding, ...]
) -> DataQualityDateWindow:
    start, end = request.scope.requested_window.start, request.scope.requested_window.end
    for item in inputs:
        if item.role == "secondary_prices" and not request.scope.require_secondary_prices:
            continue
        if item.observed_min_date is None or item.observed_max_date is None:
            _fail("NAMED_DQ_WINDOW_EMPTY", item.role)
        start, end = max(start, item.observed_min_date), min(end, item.observed_max_date)
    if start > end:
        _fail("NAMED_DQ_WINDOW_EMPTY", "no common required-input coverage")
    window = DataQualityDateWindow(start, end)
    if (
        request.expected_evaluated_window is not None
        and request.expected_evaluated_window != window
    ):
        _fail("NAMED_DQ_EVALUATED_WINDOW_MISMATCH", request.request_id)
    return window


def _summary_payload(summary: DataFileSummary) -> dict[str, object]:
    return {
        "path": summary.path.as_posix(),
        "exists": summary.exists,
        "rows": summary.rows,
        "sha256": summary.sha256,
        "min_date": None if summary.min_date is None else summary.min_date.isoformat(),
        "max_date": None if summary.max_date is None else summary.max_date.isoformat(),
    }


def _expected_summaries(capture: CapturedNamedPublication) -> dict[str, object]:
    summaries: dict[str, object] = {}
    for item in capture.inputs:
        summaries[item.role] = _summary_payload(
            DataFileSummary(
                path=capture.snapshots[item.role].path,
                exists=True,
                rows=item.row_count,
                sha256=item.member.sha256,
                min_date=item.observed_min_date,
                max_date=item.observed_max_date,
            )
        )
    summaries["manifest"] = _summary_payload(
        DataFileSummary(
            path=capture.snapshots["manifest"].path,
            exists=True,
            rows=len(capture.manifest.rows),
            sha256=capture.manifest.member.sha256,
        )
    )
    return summaries


def _check_attribution_payloads(
    values: object,
    *,
    capture: CapturedNamedPublication,
    request: NamedDQExecutionRequest,
    dependencies: tuple[NamedArtifactBinding, ...],
) -> None:
    """Recheck complete attribution row identity without rerunning a DQ rule."""
    if not isinstance(values, list):
        _fail("NAMED_DQ_ATTRIBUTION_MISMATCH", "expected an array")
    role_bindings = {item.source_role: item for item in capture.inputs}
    raw = dict(capture.captured)
    frames: dict[str, pd.DataFrame] = {}
    dependency_hashes = {
        (Path(request.roots.execution_root) / item.relative_path).as_posix(): item.sha256
        for item in dependencies
    }
    for value in values:
        attribution = _object(value, "complete attribution")
        source = _object(attribution.get("source"), "attribution source")
        source_role = source.get("source_role")
        item = role_bindings.get(source_role) if isinstance(source_role, str) else None
        if (
            item is None
            or source
            != {
                "source_role": item.source_role,
                "path": capture.snapshots[item.role].path.as_posix(),
                "sha256": item.member.sha256,
            }
            or attribution.get("scope_status") != "COMPLETE"
            or attribution.get("requested_window") != request.scope.requested_window.to_dict()
        ):
            _fail("NAMED_DQ_ATTRIBUTION_SOURCE_MISMATCH", str(source_role))
        decision = _object(attribution.get("decision"), "attribution decision")
        decision_path = decision.get("path")
        if not isinstance(decision_path, str) or (
            dependency_hashes.get(decision_path) != decision.get("sha256")
        ):
            _fail("NAMED_DQ_ATTRIBUTION_POLICY_MISMATCH", str(decision_path))
        is_price = attribution.get("schema_version") == "data_quality_issue_attribution.v1"
        if is_price:
            if item.role != "prices":
                _fail("NAMED_DQ_ATTRIBUTION_ROLE_MISMATCH", item.role)
            price_decision = load_price_non_market_session_attribution_decision(
                Path(request.roots.execution_root)
                / "config/data_quality/price_non_market_session_attribution_decision_v1.yaml",
                project_root=Path(request.roots.execution_root),
            )
            if (
                decision
                != {
                    "decision_id": price_decision.decision_id,
                    "decision_version": price_decision.decision_version,
                    "path": price_decision.path.as_posix(),
                    "sha256": price_decision.sha256,
                }
                or attribution.get("site_id") != price_decision.approved_site_id
                or attribution.get("issue_code") != price_decision.approved_issue_code
                or attribution.get("scope_taxonomy") != price_decision.scope_taxonomy
                or attribution.get("calendar")
                != build_reviewed_calendar_binding(price_decision).to_dict()
            ):
                _fail("NAMED_DQ_ATTRIBUTION_POLICY_MISMATCH", "reviewed price decision/calendar")
        elif (
            attribution.get("schema_version") != "data_quality_rate_issue_attribution.v1"
            or item.role != "rates"
        ):
            _fail("NAMED_DQ_ATTRIBUTION_ROLE_MISMATCH", item.role)
        else:
            rate_decision = load_rate_row_issue_attribution_decision(
                Path(request.roots.execution_root)
                / "config/data_quality/rate_row_issue_attribution_decision_v1.yaml",
                project_root=Path(request.roots.execution_root),
            )
            issue_code = attribution.get("issue_code")
            if not isinstance(issue_code, str):
                _fail("NAMED_DQ_ATTRIBUTION_POLICY_MISMATCH", "missing rate issue")
            approved = rate_decision.approved_issue(issue_code)
            if (
                decision
                != {
                    "decision_id": rate_decision.decision_id,
                    "decision_version": rate_decision.decision_version,
                    "path": rate_decision.path.as_posix(),
                    "sha256": rate_decision.sha256,
                }
                or attribution.get("site_id") != approved.site_id
                or attribution.get("scope_taxonomy") != approved.scope_taxonomy
            ):
                _fail("NAMED_DQ_ATTRIBUTION_POLICY_MISMATCH", "reviewed rate decision/site")
        if item.role not in frames:
            frames[item.role] = pd.read_csv(io.BytesIO(raw[item.role]))
        frame = frames[item.role]
        affected_rows = attribution.get("affected_rows")
        if not isinstance(affected_rows, list) or not affected_rows:
            _fail("NAMED_DQ_ATTRIBUTION_ROW_MISMATCH", item.role)
        ordinals: set[int] = set()
        for affected in affected_rows:
            row = _object(affected, "affected row")
            ordinal = row.get("source_ordinal")
            if type(ordinal) is not int or not 0 <= ordinal < len(frame) or ordinal in ordinals:
                _fail("NAMED_DQ_ATTRIBUTION_ROW_MISMATCH", "invalid source ordinal")
            ordinals.add(ordinal)
            original = cast(dict[str, object], frame.iloc[ordinal].to_dict())
            digest = (
                canonical_price_row_digest(original)
                if is_price
                else canonical_rate_row_digest(original)
            )
            parsed_date = pd.to_datetime(frame["date"], errors="coerce").iloc[ordinal]
            observed_date = None if pd.isna(parsed_date) else parsed_date.date().isoformat()
            identity_key, csv_key = ("ticker", "ticker") if is_price else ("rate_series", "series")
            if (
                digest != row.get("canonical_row_digest")
                or row.get("observed_date") != observed_date
                or row.get(identity_key) != str(original[csv_key]).strip()
            ):
                _fail("NAMED_DQ_ATTRIBUTION_ROW_MISMATCH", f"{item.role}:{ordinal}")


def _report_bundle(
    report: DataQualityReport,
    *,
    capture: CapturedNamedPublication,
    request: NamedDQExecutionRequest,
    dependencies: tuple[NamedArtifactBinding, ...],
) -> bytes:
    summaries = {
        "prices": report.price_summary,
        "rates": report.rate_summary,
        "manifest": report.manifest_summary,
    }
    if report.secondary_price_summary is not None:
        summaries["secondary_prices"] = report.secondary_price_summary
    actual = {
        role: _summary_payload(summary)
        for role, summary in summaries.items()
        if summary is not None
    }
    if (
        actual != _expected_summaries(capture)
        or report.expected_price_tickers != request.scope.expected_price_tickers
        or report.expected_rate_series != request.scope.expected_rate_series
        or (report.requested_window_start, report.requested_window_end)
        != (request.scope.requested_window.start, request.scope.requested_window.end)
    ):
        _fail("NAMED_DQ_REPORT_SUMMARY_MISMATCH", request.request_id)
    attributions = [
        issue.typed_attribution.to_dict()
        for issue in report.issues
        if issue.typed_attribution is not None
    ]
    _check_attribution_payloads(
        attributions, capture=capture, request=request, dependencies=dependencies
    )
    return _json_bytes(
        {
            "schema_version": REPORT_SCHEMA,
            "canonical_report": TypeAdapter(DataQualityReport).dump_python(report, mode="json"),
            "canonical_report_markdown": render_data_quality_report(report),
            "input_summaries": actual,
            "complete_attributions": attributions,
            "dispatch_allowed": False,
            "production_effect": "none",
        }
    )


def _invocation(
    request: NamedDQExecutionRequest, evaluated: DataQualityDateWindow
) -> tuple[DataQualityInvocationParameter, ...]:
    payload = {
        "request_id": request.request_id,
        "request": request.to_dict(),
        "evaluated_window": evaluated.to_dict(),
        "canonical_dq_call_count": 1,
    }
    return tuple(
        DataQualityInvocationParameter(name, canonical_json_value(value))
        for name, value in sorted(payload.items())
    )


def run_named_data_quality_execution(
    request: NamedDQExecutionRequest,
    *,
    bootstrap: NamedBootstrapAuthority,
) -> NamedDataQualityExecutionResult:
    context = _execution_binding(request, bootstrap)
    capture = capture_named_publication(request)
    policy, calendar, calendar_artifact = _policy_and_calendar(request, bootstrap)
    deps = _dependencies(bootstrap)
    bootstrap.assert_execution_unchanged(stage="PRE_DQ")
    started = datetime.now(UTC)
    bootstrap.note_canonical_dq_dispatch()
    # Exactly one genuine canonical call. No legacy runner or current discovery.
    report = validate_data_cache(
        prices_path=capture.snapshots["prices"].path,
        rates_path=capture.snapshots["rates"].path,
        expected_price_tickers=list(request.scope.expected_price_tickers),
        expected_rate_series=list(request.scope.expected_rate_series),
        quality_config=policy.config,
        as_of=request.scope.as_of,
        manifest_path=capture.snapshots["manifest"].path,
        secondary_prices_path=(
            capture.snapshots["secondary_prices"].path
            if "secondary_prices" in capture.snapshots
            else None
        ),
        require_secondary_prices=request.scope.require_secondary_prices,
        file_snapshots=capture.snapshots,
        requested_window=(request.scope.requested_window.start, request.scope.requested_window.end),
        named_download_publication=capture.named,
        named_source_root=Path(request.roots.source_root),
        named_source_output_relative_path=request.source_output_relative_path,
    )
    ended = datetime.now(UTC)
    evaluated = _evaluated_window(request, capture.inputs)
    bundle = _report_bundle(report, capture=capture, request=request, dependencies=deps)
    report_hash = sha256(bundle).hexdigest()
    report_binding = DataQualityReportBinding(
        path=f"named_data_quality/reports/{report_hash}/report.json",
        sha256=report_hash,
        size_bytes=len(bundle),
        status=report.status,
        error_count=report.error_count,
        warning_count=report.warning_count,
        info_count=report.info_count,
        issue_codes=tuple(sorted({issue.code for issue in report.issues})),
        blocking_issue_codes=tuple(
            sorted({issue.code for issue in report.issues if issue.severity == Severity.ERROR})
        ),
    )
    # Re-observe immutable inputs and the selected chain without running DQ.
    # A changing current anchor is fail-closed during this one dispatch.
    recaptured = capture_named_publication(request)
    if (
        recaptured.publication != capture.publication
        or recaptured.manifest != capture.manifest
        or recaptured.inputs != capture.inputs
        or recaptured.captured != capture.captured
    ):
        _fail("NAMED_DQ_INPUT_CHANGED_DURING_RUN", request.request_id)
    if _policy_and_calendar(request, bootstrap) != (policy, calendar, calendar_artifact):
        _fail("NAMED_DQ_POLICY_CHANGED_DURING_RUN", request.request_id)
    terminal = datetime.fromisoformat(bootstrap.assert_execution_unchanged(stage="TERMINAL"))
    if bootstrap.imports_completed_at is None or bootstrap.pre_dq_checked_at is None:
        _fail("NAMED_DQ_EXECUTION_OBSERVATION_MISSING", request.request_id)
    if report.price_consistency_start_date is None or report.rate_consistency_start_date is None:
        _fail("NAMED_DQ_POLICY_COVERAGE_UNBOUND", request.policy_path)
    evidence = DataQualityEvidence(
        contract_id=DATA_QUALITY_CONTRACT_ID,
        policy_id=policy.binding.policy_id,
        policy_version=policy.binding.policy_version,
        status=report.status,
        passed=report.passed,
        checked_at=report.checked_at,
        as_of=report.as_of,
        report_path=report_binding.path,
        report_sha256=report_binding.sha256,
        error_count=report.error_count,
        warning_count=report.warning_count,
        checked_input_count=len(capture.inputs),
        blocking_issues=report_binding.blocking_issue_codes,
    )
    receipt = NamedDQExecutionReceipt(
        run_id="named_dq_" + started.strftime("%Y%m%dT%H%M%S%fZ") + f"_{os.getpid()}",
        contract_id=DATA_QUALITY_CONTRACT_ID,
        request=request,
        started_at=started,
        checked_at=report.checked_at,
        ended_at=ended,
        evaluated_window=evaluated,
        publication=capture.publication,
        manifest=capture.manifest,
        inputs=capture.inputs,
        policy=policy.binding,
        validator=_validator(context),
        execution=context.identity,
        execution_observation=NamedExecutionObservation(
            execution_pid=os.getpid(),
            source_lease_id=bootstrap.source_lease_id,
            import_completed_at=datetime.fromisoformat(bootstrap.imports_completed_at),
            pre_dq_checked_at=datetime.fromisoformat(bootstrap.pre_dq_checked_at),
            terminal_checked_at=terminal,
        ),
        execution_dependencies=deps,
        calendar=calendar,
        calendar_policy=calendar_artifact,
        invocation=_invocation(request, evaluated),
        report=report_binding,
        data_quality_evidence=evidence,
        price_consistency_start_date=report.price_consistency_start_date,
        rate_consistency_start_date=report.rate_consistency_start_date,
    )
    evidence_root = Path(request.roots.evidence_root)
    write_contained_artifact_bytes(
        root=evidence_root, relative_path=report_binding.path, content=bundle, immutable=True
    )
    receipt_relative = f"named_data_quality/executions/{receipt.receipt_id}/receipt.json"
    write_contained_artifact_bytes(
        root=evidence_root,
        relative_path=receipt_relative,
        content=receipt.canonical_bytes,
        immutable=True,
    )
    return NamedDataQualityExecutionResult(receipt, receipt_relative, report)


def _verify_retained_publication(
    receipt: NamedDQExecutionReceipt,
    capture: CapturedNamedPublication,
) -> None:
    retained, current = receipt.publication, capture.publication
    if (
        replace(
            current,
            anchor_pointer_id=retained.anchor_pointer_id,
            anchor_generation=retained.anchor_generation,
            anchor_pointer=retained.anchor_pointer,
        )
        != retained
    ):
        _fail("NAMED_DQ_RETAINED_PUBLICATION_MISMATCH", receipt.receipt_id)
    if current.anchor_generation < retained.anchor_generation:
        _fail("NAMED_DQ_COMMIT_ANCHOR_ROLLBACK", receipt.receipt_id)
    if current.anchor_pointer_id != retained.anchor_pointer_id:
        # Reprove the old anchor as named committed history; never treat its
        # mutable current path as immutable or let it select another input set.
        validate_named_snapshot(
            store_root=Path(receipt.request.roots.publication_root)
            / Path(retained.anchor_pointer.relative_path).parent.parent,
            dataset_id=retained.dataset_id,
            pointer_id=retained.anchor_pointer_id,
            expected_pointer_sha256=retained.anchor_pointer.sha256,
        )
    elif (
        current.anchor_pointer.sha256 != retained.anchor_pointer.sha256
        or current.anchor_generation != retained.anchor_generation
    ):
        _fail("NAMED_DQ_RETAINED_ANCHOR_MISMATCH", receipt.receipt_id)


def _verify_report_bundle(
    receipt: NamedDQExecutionReceipt, capture: CapturedNamedPublication
) -> None:
    content = read_contained_artifact_bytes(
        root=Path(receipt.request.roots.evidence_root), relative_path=receipt.report.path
    )
    if (
        len(content) != receipt.report.size_bytes
        or sha256(content).hexdigest() != receipt.report.sha256
    ):
        _fail("NAMED_DQ_REPORT_SHA_MISMATCH", receipt.report.path)
    bundle = _object(_strict_json_loads(content.decode("utf-8")), "report bundle")
    if (
        set(bundle)
        != {
            "schema_version",
            "canonical_report",
            "canonical_report_markdown",
            "input_summaries",
            "complete_attributions",
            "dispatch_allowed",
            "production_effect",
        }
        or bundle.get("schema_version") != REPORT_SCHEMA
        or bundle.get("dispatch_allowed") is not False
        or bundle.get("production_effect") != "none"
        or _json_bytes(bundle) != content
        or bundle.get("input_summaries") != _expected_summaries(capture)
    ):
        _fail("NAMED_DQ_REPORT_FIELDS_INVALID", receipt.report.path)
    # Parse only the fixed existing report dataclass, then re-render every
    # projection. Exact bytes reject ignored unknown fields as well as a
    # Markdown/typed-attribution substitution with recomputed outer hashes.
    report = TypeAdapter(DataQualityReport).validate_json(
        _json_bytes(bundle["canonical_report"]), strict=True
    )
    if (
        report.price_consistency_start_date != receipt.price_consistency_start_date
        or report.rate_consistency_start_date != receipt.rate_consistency_start_date
        or _report_bundle(
            report,
            capture=capture,
            request=receipt.request,
            dependencies=receipt.execution_dependencies,
        )
        != content
    ):
        _fail("NAMED_DQ_REPORT_FULL_PROJECTION_MISMATCH", receipt.report.path)
    markdown = bundle.get("canonical_report_markdown")
    if not isinstance(markdown, str):
        _fail("NAMED_DQ_REPORT_FIELDS_INVALID", "canonical_report_markdown")
    parsed = _parse_report(markdown.encode("utf-8"))
    if (
        parsed.status,
        parsed.checked_at,
        parsed.as_of,
        parsed.error_count,
        parsed.warning_count,
        parsed.info_count,
        parsed.issue_codes,
        parsed.blocking_issue_codes,
        parsed.expected_price_tickers,
        parsed.expected_rate_series,
    ) != (
        receipt.report.status,
        receipt.checked_at,
        receipt.request.scope.as_of,
        receipt.report.error_count,
        receipt.report.warning_count,
        receipt.report.info_count,
        receipt.report.issue_codes,
        receipt.report.blocking_issue_codes,
        receipt.request.scope.expected_price_tickers,
        receipt.request.scope.expected_rate_series,
    ):
        _fail("NAMED_DQ_REPORT_PROJECTION_MISMATCH", receipt.report.path)
    values = bundle["complete_attributions"]
    _check_attribution_payloads(
        values,
        capture=capture,
        request=receipt.request,
        dependencies=receipt.execution_dependencies,
    )
    if not isinstance(values, list) or markdown.count("- Scope status：`COMPLETE`") != len(values):
        _fail("NAMED_DQ_REPORT_ATTRIBUTION_COUNT_MISMATCH", receipt.report.path)


def _read_parent_artifact(value: object, *, root: Path) -> bytes:
    binding = _object(value, "parent artifact binding")
    if set(binding) != {"path", "sha256", "size_bytes"} or not isinstance(binding.get("path"), str):
        _fail("NAMED_DQ_DISPATCH_PARENT_INVALID", "strict parent artifact binding required")
    path = Path(cast(str, binding["path"]))
    if not path.is_absolute() or ".." in path.parts:
        _fail("NAMED_DQ_DISPATCH_PARENT_INVALID", "explicit contained parent path required")
    if (
        not path.is_relative_to(root)
        or type(binding["size_bytes"]) is not int
        or binding["size_bytes"] < 0
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_INVALID", "parent root/size binding invalid")
    relative = path.relative_to(root).as_posix()
    content = read_contained_artifact_bytes(root=root, relative_path=relative)
    if sha256(content).hexdigest() != binding["sha256"] or len(content) != binding["size_bytes"]:
        _fail("NAMED_DQ_DISPATCH_PARENT_SHA_MISMATCH", relative)
    return content


def _verify_successful_run_dispatch(
    receipt: NamedDQExecutionReceipt,
    *,
    receipt_path: str,
    dispatch_path: str | None,
    dispatch_sha256: str | None,
) -> NamedDQSuccessfulDispatchBinding:
    """Bind a trusted coordinator's successful terminal attestation, not a signature.

    The coordinator remains responsible for real guard replay/lease validity;
    this child verifies the attested bytes and their exact execution correlation.
    A naked or failed-dispatch PASS receipt is insufficient to mint input bytes.
    """
    if dispatch_path is None or dispatch_sha256 is None:
        _fail("NAMED_DQ_SUCCESSFUL_DISPATCH_REQUIRED", receipt_path)
    root = Path(receipt.request.roots.execution_root)
    content = read_contained_artifact_bytes(root=root, relative_path=dispatch_path)
    if sha256(content).hexdigest() != dispatch_sha256:
        _fail("NAMED_DQ_DISPATCH_SHA_MISMATCH", dispatch_path)
    proof = NamedDQSuccessfulDispatchBinding.from_json_bytes(content)
    proof.assert_matches_receipt(receipt, receipt_path=receipt_path)
    parent_content = read_contained_artifact_bytes(
        root=root, relative_path=proof.parent_receipt.relative_path
    )
    if (
        sha256(parent_content).hexdigest() != proof.parent_receipt.sha256
        or len(parent_content) != proof.parent_receipt.size_bytes
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_SHA_MISMATCH", proof.parent_receipt.relative_path)
    parent = _object(_strict_json_loads(parent_content.decode("utf-8")), "parent receipt")
    expected_parent = {
        "schema_version": "named_data_quality_parent_dispatch.v1",
        "status_semantics": "PARENT_ASSOCIATION_AND_PROCESS_OBSERVATION_ONLY",
        "status": "PASS",
        "operation": "run",
        "terminal_state": "EXITED",
        "returncode": 0,
        "candidate_commit": receipt.request.candidate_commit,
        "execution_root": receipt.request.roots.execution_root,
        "request_id": receipt.request.request_id,
        "source_lease_id": proof.source_lease_id,
        "child_pid": proof.execution_pid,
        "observed_canonical_dq_call_count": 1,
        "counter_observation_state": "KNOWN",
        "failure": None,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if any(
        type(parent.get(key)) is not type(value) or parent.get(key) != value
        for key, value in expected_parent.items()
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_UNSUCCESSFUL", proof.parent_receipt.relative_path)
    request_content = _read_parent_artifact(parent.get("request"), root=root)
    if request_content != receipt.request.canonical_bytes:
        _fail("NAMED_DQ_DISPATCH_PARENT_REQUEST_MISMATCH", receipt.request.request_id)
    stdout = _read_parent_artifact(parent.get("child_stdout"), root=root)
    result = _object(_strict_json_loads(stdout.decode("utf-8")), "original child stdout")
    if result != parent.get("child_result"):
        _fail("NAMED_DQ_DISPATCH_PARENT_RESULT_MISMATCH", receipt.receipt_id)
    expected_result = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "verified_input_seal_exported": False,
        "status": receipt.report.status,
        "request_id": receipt.request.request_id,
        "process_id": proof.execution_pid,
        "source_lease_id": proof.source_lease_id,
        "receipt_id": receipt.receipt_id,
        "receipt_path": receipt_path,
        "receipt_sha256": receipt.canonical_sha256,
        "canonical_dq_call_count": 1,
        "child_started_at": proof.child_started_at.isoformat(),
        "child_terminal_checked_at": proof.child_terminal_checked_at.isoformat(),
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if any(
        type(result.get(key)) is not type(value) or result.get(key) != value
        for key, value in expected_result.items()
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_RESULT_MISMATCH", receipt.receipt_id)
    post_content = _read_parent_artifact(parent.get("post_dispatch_proof"), root=root)
    post = _object(_strict_json_loads(post_content.decode("utf-8")), "parent postguard")
    if (
        post.get("schema_version") != "named_dq_existing_parent_proof.v1"
        or post.get("status") != "PASS"
        or post.get("checked_at") != proof.parent_postchecked_at.isoformat()
        or post.get("candidate_commit") != receipt.request.candidate_commit
        or post.get("execution_root") != receipt.request.roots.execution_root
        or post.get("request_sha256") != receipt.request.canonical_sha256
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_POSTGUARD_MISMATCH", receipt.receipt_id)
    active_lease = _object(post.get("active_lease"), "parent active lease")
    if (
        active_lease.get("lease_id") != proof.source_lease_id
        or active_lease.get("state") != "ACTIVE"
    ):
        _fail("NAMED_DQ_DISPATCH_PARENT_POSTGUARD_MISMATCH", "lease correlation")
    return proof


def verify_named_data_quality_execution_receipt(
    request: NamedDQExecutionRequest,
    *,
    receipt_path: str,
    receipt_sha256: str,
    bootstrap: NamedBootstrapAuthority,
    run_dispatch_path: str | None = None,
    run_dispatch_sha256: str | None = None,
) -> VerifiedNamedInputs:
    """Zero DQ, zero writes. Mint only local immutable bytes after strict PASS."""
    context = _execution_binding(request, bootstrap)
    if bootstrap.canonical_dq_call_count != 0:
        _fail("NAMED_DQ_VERIFY_DISPATCH_COUNT", "verifier requires its own zero-DQ child")
    content = read_contained_artifact_bytes(
        root=Path(request.roots.evidence_root), relative_path=receipt_path
    )
    if sha256(content).hexdigest() != receipt_sha256:
        _fail("NAMED_DQ_RECEIPT_SHA_MISMATCH", receipt_path)
    receipt = NamedDQExecutionReceipt.from_json_bytes(content)
    successful_dispatch = _verify_successful_run_dispatch(
        receipt,
        receipt_path=receipt_path,
        dispatch_path=run_dispatch_path,
        dispatch_sha256=run_dispatch_sha256,
    )
    if receipt.request != request or receipt.execution != context.identity:
        _fail("NAMED_DQ_RECEIPT_REQUEST_MISMATCH", receipt_path)
    if receipt.validator != _validator(context) or receipt.execution_dependencies != _dependencies(
        bootstrap
    ):
        _fail("NAMED_DQ_RECEIPT_IMPLEMENTATION_MISMATCH", receipt_path)
    policy, calendar, calendar_artifact = _policy_and_calendar(request, bootstrap)
    if (
        receipt.policy != policy.binding
        or receipt.calendar != calendar
        or receipt.calendar_policy != calendar_artifact
        or receipt.price_consistency_start_date != policy.config.prices.consistency_start_date
        or receipt.rate_consistency_start_date != policy.config.rates.consistency_start_date
    ):
        _fail("NAMED_DQ_RECEIPT_POLICY_MISMATCH", receipt_path)
    capture = capture_named_publication(request)
    _verify_retained_publication(receipt, capture)
    if (
        receipt.manifest != capture.manifest
        or receipt.inputs != capture.inputs
        or receipt.evaluated_window != _evaluated_window(request, capture.inputs)
        or receipt.invocation != _invocation(request, receipt.evaluated_window)
    ):
        _fail("NAMED_DQ_RECEIPT_INPUT_MISMATCH", receipt_path)
    _verify_report_bundle(receipt, capture)
    if receipt.report.status != "PASS" or not receipt.data_quality_evidence.ready:
        _fail("NAMED_DQ_STRICT_PASS_REQUIRED", receipt.report.status)
    preview_sessions, preview_next_session = _preview_calendar_witness(request)
    recording_artifacts = _prospective_recording_metadata(
        receipt,
        successful_dispatch,
        receipt_path=receipt_path,
        dispatch_path=run_dispatch_path,
        dispatch_sha256=run_dispatch_sha256,
        capture=capture,
    )
    bootstrap.assert_execution_unchanged(stage="TERMINAL")
    if bootstrap.canonical_dq_call_count != 0:
        _fail("NAMED_DQ_VERIFY_DISPATCH_COUNT", "verifier dispatched DQ")
    return _verified_named_inputs_from_receipt(
        receipt,
        captured_inputs=capture.captured,
        successful_dispatch=successful_dispatch,
        receipt_path=receipt_path,
        captured_dependencies=tuple(
            (path, item.content) for path, item in sorted(bootstrap.dependencies.items())
        ),
        preview_sessions=preview_sessions,
        preview_next_session=preview_next_session,
        captured_recording_artifacts=recording_artifacts,
    )


def _preview_calendar_witness(
    request: NamedDQExecutionRequest,
) -> tuple[tuple[date, ...], date | None]:
    """Seal canonical dates after calendar verification, never load them in a consumer.

    This is calendar enumeration, not a data-quality/feature/outcome evaluation.
    Legacy profiles do not acquire the new consumer witness. All date decisions
    reuse the same canonical function already bound by _policy_and_calendar.
    """
    if (request.source_manifest_path, request.source_manifest_sha256) not in {
        (
            FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_PATH,
            FIVE_CANDIDATE_PREVIEW_SOURCE_MANIFEST_SHA256,
        ),
        (
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
        ),
    }:
        return (), None
    day = request.scope.requested_window.start
    sessions = []
    while day <= request.scope.as_of:
        if is_us_equity_trading_day(day):
            sessions.append(day)
        day += timedelta(days=1)
    while not is_us_equity_trading_day(day):
        day += timedelta(days=1)
    return tuple(sessions), day


def _prospective_recording_metadata(
    receipt: NamedDQExecutionReceipt,
    successful: NamedDQSuccessfulDispatchBinding,
    *,
    receipt_path: str,
    dispatch_path: str | None,
    dispatch_sha256: str | None,
    capture: CapturedNamedPublication,
) -> tuple[tuple[str, NamedArtifactBinding, bytes], ...]:
    """Finish S3b preservation capture during verification, before minting a seal.

    Old profiles capture no new metadata and retain their old authority. This
    records the original anchor bytes even when current has since advanced; the
    ordinary named verifier already proved that anchor's committed membership.
    """
    request = receipt.request
    if (request.source_manifest_path, request.source_manifest_sha256) != (
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
        PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
    ):
        return ()
    roots = {
        "EXECUTION": Path(request.roots.execution_root),
        "PUBLICATION": Path(request.roots.publication_root),
        "EVIDENCE": Path(request.roots.evidence_root),
    }
    rows: list[tuple[str, NamedArtifactBinding, bytes]] = []

    def preserve(role: str, binding: NamedArtifactBinding, content: bytes | None = None) -> bytes:
        if content is None:
            content = read_contained_artifact_bytes(
                root=roots[binding.root_role], relative_path=binding.relative_path
            )
        if len(content) != binding.size_bytes or sha256(content).hexdigest() != binding.sha256:
            _fail("NAMED_DQ_RECORDING_METADATA_DRIFT", role)
        rows.append((role, binding, content))
        return content

    publication = receipt.publication
    for role, binding in (
        ("publication_pointer", publication.pointer),
        ("publication_transaction", publication.transaction),
        ("snapshot_manifest", publication.snapshot_manifest),
        ("source_event", publication.source_event),
        ("source_manifest", receipt.manifest.member),
    ):
        preserve(role, binding)
    if capture.publication.anchor_pointer_id == publication.anchor_pointer_id:
        preserve("commit_anchor", publication.anchor_pointer)
    else:
        old_anchor = validate_named_snapshot(
            store_root=roots["PUBLICATION"]
            / Path(publication.anchor_pointer.relative_path).parent.parent,
            dataset_id=publication.dataset_id,
            pointer_id=publication.anchor_pointer_id,
            expected_pointer_sha256=publication.anchor_pointer.sha256,
        )
        preserve(
            "commit_anchor",
            publication.anchor_pointer,
            read_contained_artifact_bytes(
                root=roots["PUBLICATION"],
                relative_path=old_anchor.pointer_path.relative_to(roots["PUBLICATION"]).as_posix(),
            ),
        )
    preserve(
        "dq_report",
        NamedArtifactBinding(
            "EVIDENCE", receipt.report.path, receipt.report.sha256, receipt.report.size_bytes
        ),
    )
    preserve("dq_receipt", successful.receipt, receipt.canonical_bytes)
    if dispatch_path is None or dispatch_sha256 is None:
        _fail("NAMED_DQ_SUCCESSFUL_DISPATCH_REQUIRED", receipt_path)
    preserve(
        "dq_successful_dispatch",
        NamedArtifactBinding(
            "EXECUTION", dispatch_path, dispatch_sha256, len(successful.canonical_bytes)
        ),
        successful.canonical_bytes,
    )
    parent_content = preserve("dq_parent", successful.parent_receipt)
    parent = _object(_strict_json_loads(parent_content.decode("utf-8")), "DQ parent")
    for role, key in (
        ("dq_parent_request", "request"),
        ("dq_child_stdout", "child_stdout"),
        ("dq_child_stderr", "child_stderr"),
        ("dq_pre_guard", "pre_dispatch_proof"),
        ("dq_post_guard", "post_dispatch_proof"),
    ):
        value = _object(parent.get(key), "parent artifact")
        content = _read_parent_artifact(value, root=roots["EXECUTION"])
        preserve(
            role,
            _artifact(
                "EXECUTION",
                Path(cast(str, value["path"])).relative_to(roots["EXECUTION"]).as_posix(),
                content,
            ),
            content,
        )
    return tuple(sorted(rows))


def bootstrap_worker(
    request: Mapping[str, object],
    *,
    operation: str,
    receipt_path: str | None,
    receipt_sha256: str | None,
    bootstrap: NamedBootstrapAuthority,
    run_dispatch_path: str | None = None,
    run_dispatch_sha256: str | None = None,
) -> dict[str, object]:
    if operation in {"activate", "capture"}:
        # Only the fixed new profile can dispatch this fixed production parent;
        # old profiles never import its additional modules or acquire its scope.
        if (request.get("source_manifest_path"), request.get("source_manifest_sha256")) != (
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_PATH,
            PROSPECTIVE_FIVE_CANDIDATE_SOURCE_MANIFEST_SHA256,
        ) or any(
            value is not None
            for value in (receipt_path, receipt_sha256, run_dispatch_path, run_dispatch_sha256)
        ):
            _fail("NAMED_DQ_PROSPECTIVE_PROFILE_REQUIRED", operation)
        from ai_trading_system.prospective_capture_execution import (
            bootstrap_worker as capture_worker,
        )

        return capture_worker(request, operation=operation, bootstrap=bootstrap)
    typed = NamedDQExecutionRequest.from_dict(request)
    common: dict[str, object] = {
        "schema_version": "named_data_quality_bootstrap_result.v1",
        "request_id": typed.request_id,
        "process_id": os.getpid(),
        "source_lease_id": bootstrap.source_lease_id,
        "dispatch_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if operation == "run":
        result = run_named_data_quality_execution(typed, bootstrap=bootstrap)
        return {
            **common,
            "status": result.receipt.report.status,
            "receipt_path": result.receipt_relative_path,
            "receipt_id": result.receipt.receipt_id,
            "receipt_sha256": result.receipt.canonical_sha256,
            "canonical_dq_call_count": bootstrap.canonical_dq_call_count,
            "verified_input_seal_exported": False,
        }
    if operation == "verify" and receipt_path is not None and receipt_sha256 is not None:
        verified = verify_named_data_quality_execution_receipt(
            typed,
            receipt_path=receipt_path,
            receipt_sha256=receipt_sha256,
            bootstrap=bootstrap,
            run_dispatch_path=run_dispatch_path,
            run_dispatch_sha256=run_dispatch_sha256,
        )
        # This is a DTO summary of bytes accessed in this child, never a seal
        # export. A consumer in a different process must verify for itself.
        verified.assert_scope_covered(typed.scope)
        digests = {
            role: sha256(verified.bytes_for(role, required_scope=typed.scope)).hexdigest()
            for role in typed.scope.input_roles
        }
        return {
            **common,
            "status": "PASS",
            "receipt_id": verified.receipt.receipt_id,
            "original_dq_pid": verified.receipt.execution_observation.execution_pid,
            "verifier_pid": verified.verifier_pid,
            "input_sha256": digests,
            "canonical_dq_call_count": 0,
            "verified_input_seal_exported": False,
        }
    _fail("NAMED_DQ_OPERATION_INVALID", operation)
