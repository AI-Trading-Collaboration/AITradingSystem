from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.contracts import (
    ArtifactEnvelope,
    ArtifactLifecycle,
    ArtifactPointer,
    ArtifactVisibility,
    CanonicalStatus,
    EntrypointRef,
    PolicyRef,
    PolicyRole,
    ResearchLifecycleRecord,
    RunLedger,
    WorkflowCadence,
    WorkflowSpec,
    WorkflowStepSpec,
)
from ai_trading_system.platform.artifacts import (
    ArtifactWriteResult,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.research_framework.plugins import (
    ExperimentExecutionContext,
    PluginRegistry,
)
from ai_trading_system.research_framework.spec import (
    ExperimentSpec,
    InputDocumentKind,
    OutputArtifactKind,
    OutputRoot,
    ResolvedExperimentSpec,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


@dataclass(frozen=True)
class ExperimentRunRequest:
    project_root: Path
    output_root: Path
    docs_root: Path
    as_of: date
    input_overrides: Mapping[str, Path]
    strict: bool | None = None
    generated_at: datetime | None = None


@dataclass(frozen=True)
class ExperimentRunResult:
    payload: dict[str, Any]
    output_paths: Mapping[str, Path]
    envelope: ArtifactEnvelope
    ledger: RunLedger
    lifecycle: ResearchLifecycleRecord | None = None


def run_experiment(
    *,
    resolved_spec: ResolvedExperimentSpec,
    plugins: PluginRegistry,
    request: ExperimentRunRequest,
) -> ExperimentRunResult:
    spec = resolved_spec.value
    calculator = plugins.calculator(spec.calculator_plugin)
    report_plugin = plugins.report(spec.report_plugin)
    spec.report_spec()
    generated_at = _generated_at(request.generated_at)
    sources, source_paths, source_errors = _load_inputs(spec, request)
    source_artifacts = tuple(
        _source_artifact_record(path) for path in source_paths if path.exists()
    )
    context = ExperimentExecutionContext(
        spec=spec,
        sources=sources,
        source_artifacts=source_artifacts,
        as_of=request.as_of,
    )
    payload = calculator.calculate(context)
    payload.update(
        {
            "as_of": request.as_of.isoformat(),
            "generated_at": _utc_iso(generated_at),
            "source_validation_errors": source_errors,
            "source_validation_error_count": len(source_errors),
            "task_register_id": spec.task_register_id,
            "manual_review_required": spec.manual_review_required,
            "production_effect": spec.production_effect.value,
            "broker_action": spec.broker_action,
        }
    )
    output_paths = _output_paths(spec, request)
    payload["artifact_paths"] = {
        output.artifact_path_key: str(output_paths[output.output_id])
        for output in spec.outputs
        if output.artifact_path_key is not None
    }
    primary_output = spec.output(OutputArtifactKind.PRIMARY_JSON)
    primary_result = write_json_atomic(
        output_paths[primary_output.output_id],
        payload,
        trailing_newline=False,
    )
    _write_section_output(spec, payload, output_paths, report_plugin)
    markdown_output = spec.output(OutputArtifactKind.MARKDOWN)
    write_markdown_atomic(
        output_paths[markdown_output.output_id],
        report_plugin.render_markdown(payload),
    )

    canonical_status = spec.canonical_status(str(payload.get("status") or ""))
    envelope = _build_envelope(
        spec=spec,
        resolved_spec=resolved_spec,
        payload=payload,
        primary_result=primary_result,
        source_artifacts=source_artifacts,
        generated_at=generated_at,
        as_of=request.as_of,
        canonical_status=canonical_status,
    )
    envelope_output = spec.output(OutputArtifactKind.ENVELOPE_JSON)
    write_json_atomic(output_paths[envelope_output.output_id], envelope.to_dict())
    ledger = _build_ledger(
        spec=spec,
        payload=payload,
        primary_result=primary_result,
        generated_at=generated_at,
        as_of=request.as_of,
        canonical_status=canonical_status,
    )
    ledger_output = spec.output(OutputArtifactKind.RUN_LEDGER_JSON)
    write_json_atomic(output_paths[ledger_output.output_id], ledger.to_dict())
    lifecycle = _write_optional_lifecycle(
        plugins=plugins,
        context=context,
        payload=payload,
        primary_result=primary_result,
        generated_at=generated_at,
        output_paths=output_paths,
    )

    strict_errors = [*source_errors, *_string_list(payload.get("strict_validation_errors"))]
    strict = spec.strict_default if request.strict is None else request.strict
    if strict and strict_errors:
        raise ValueError("; ".join(strict_errors))
    return ExperimentRunResult(
        payload=payload,
        output_paths=output_paths,
        envelope=envelope,
        ledger=ledger,
        lifecycle=lifecycle,
    )


def _write_optional_lifecycle(
    *,
    plugins: PluginRegistry,
    context: ExperimentExecutionContext,
    payload: Mapping[str, Any],
    primary_result: ArtifactWriteResult,
    generated_at: datetime,
    output_paths: dict[str, Path],
) -> ResearchLifecycleRecord | None:
    plugin = plugins.optional_lifecycle(context.spec.report_plugin)
    if plugin is None:
        return None
    primary_pointer = primary_result.to_pointer(
        schema_version=str(payload.get("schema_version") or "")
    )
    lifecycle = plugin.build(context, payload, primary_pointer, generated_at)
    primary_path = Path(primary_result.path)
    lifecycle_path = primary_path.with_name(f"{primary_path.stem}.lifecycle.json")
    write_json_atomic(lifecycle_path, lifecycle.to_dict())
    output_paths["research_lifecycle"] = lifecycle_path
    return lifecycle


def _load_inputs(
    spec: ExperimentSpec, request: ExperimentRunRequest
) -> tuple[dict[str, Any], tuple[Path, ...], list[str]]:
    unknown_overrides = sorted(
        set(request.input_overrides) - {item.input_id for item in spec.inputs}
    )
    if unknown_overrides:
        raise ValueError(f"unknown experiment input override: {','.join(unknown_overrides)}")
    sources: dict[str, Any] = {}
    paths: list[Path] = []
    errors: list[str] = []
    for input_spec in spec.inputs:
        configured = request.input_overrides.get(input_spec.input_id)
        path = configured or _resolve_project_path(request.project_root, input_spec.default_path)
        paths.append(path)
        if not path.exists():
            if input_spec.required:
                errors.append(f"{input_spec.input_id} missing: {path}")
            sources[input_spec.input_id] = (
                {"_missing": True, "_path": str(path)}
                if input_spec.document_kind is InputDocumentKind.STRUCTURED
                else ""
            )
            continue
        if input_spec.document_kind is InputDocumentKind.TEXT:
            sources[input_spec.input_id] = path.read_text(encoding="utf-8")
        elif path.suffix.lower() in {".yaml", ".yml"}:
            sources[input_spec.input_id] = safe_load_yaml_path(path)
        else:
            sources[input_spec.input_id] = json.loads(path.read_text(encoding="utf-8"))
    return sources, tuple(paths), errors


def _output_paths(spec: ExperimentSpec, request: ExperimentRunRequest) -> dict[str, Path]:
    return {
        output.output_id: (
            request.output_root if output.root is OutputRoot.OUTPUT else request.docs_root
        )
        / output.filename
        for output in spec.outputs
    }


def _write_section_output(
    spec: ExperimentSpec,
    payload: Mapping[str, Any],
    output_paths: Mapping[str, Path],
    report_plugin: Any,
) -> None:
    output = spec.output(OutputArtifactKind.SECTION_JSON)
    assert output.payload_key is not None
    assert output.section_report_type is not None
    section = report_plugin.section(payload, output.payload_key)
    content = {
        "task_id": payload.get("task_id"),
        "status": payload.get("status"),
        "report_type": output.section_report_type,
        "schema_version": str(section.get("schema_version") or payload.get("schema_version")),
        output.payload_key: section,
        "production_effect": spec.production_effect.value,
        "broker_action": spec.broker_action,
    }
    write_json_atomic(
        output_paths[output.output_id],
        content,
        trailing_newline=False,
    )


def _build_envelope(
    *,
    spec: ExperimentSpec,
    resolved_spec: ResolvedExperimentSpec,
    payload: Mapping[str, Any],
    primary_result: ArtifactWriteResult,
    source_artifacts: tuple[Mapping[str, object], ...],
    generated_at: datetime,
    as_of: date,
    canonical_status: CanonicalStatus,
) -> ArtifactEnvelope:
    policy = resolved_spec.reference
    return ArtifactEnvelope(
        artifact_id=spec.experiment_id,
        producer="ai_trading_system.research_framework.runner:run_experiment",
        run_id=f"{spec.experiment_id}:{as_of.isoformat()}",
        generated_at=generated_at,
        as_of=as_of,
        status=canonical_status,
        production_effect=spec.production_effect,
        payload=primary_result.to_pointer(schema_version=str(payload.get("schema_version") or "")),
        owner=spec.owner,
        lifecycle=ArtifactLifecycle.CURRENT,
        visibility=ArtifactVisibility.RESEARCH,
        investment_facing=spec.investment_facing_envelope,
        data_quality_required=spec.data_quality_required,
        input_artifacts=tuple(_source_pointer(item) for item in source_artifacts),
        policy_refs=(
            PolicyRef(
                policy_id=policy.policy_id,
                role=PolicyRole.STRATEGY,
                version=policy.version,
                status=policy.status,
                path=policy.path,
                sha256=policy.sha256,
            ),
        ),
        limitations=("data_quality_not_applicable_governance_artifact_closure_only",),
        next_actions=(str(payload.get("next_route") or "manual_review"),),
    )


def _build_ledger(
    *,
    spec: ExperimentSpec,
    payload: Mapping[str, Any],
    primary_result: ArtifactWriteResult,
    generated_at: datetime,
    as_of: date,
    canonical_status: CanonicalStatus,
) -> RunLedger:
    step = WorkflowStepSpec(
        step_id="evaluate_and_render",
        entrypoint=EntrypointRef(
            module="ai_trading_system.research_framework.runner",
            callable_name="run_experiment",
        ),
        expected_artifact_types=(str(payload.get("report_type") or spec.experiment_id),),
        quality_gate_required=spec.data_quality_required,
        production_effect=spec.production_effect,
    )
    workflow = WorkflowSpec(
        workflow_id=f"experiment:{spec.experiment_id}",
        owner=spec.owner,
        cadence=WorkflowCadence.MANUAL,
        timezone="UTC",
        due_policy_id="manual_explicit_invocation.v1",
        steps=(step,),
    )
    ledger = RunLedger.initialize(
        workflow,
        run_id=f"{spec.experiment_id}:{as_of.isoformat()}",
        as_of=as_of,
        created_at=generated_at,
    )
    entry = ledger.entry(step.step_id).transition(CanonicalStatus.DUE, at=generated_at)
    ledger = ledger.with_entry(workflow, entry)
    entry = entry.transition(CanonicalStatus.RUNNING, at=generated_at)
    ledger = ledger.with_entry(workflow, entry)
    pointer = primary_result.to_pointer(schema_version=str(payload.get("schema_version") or ""))
    if canonical_status is CanonicalStatus.BLOCKED:
        blockers = tuple(_string_list(payload.get("strict_validation_errors"))) or (
            "EXPERIMENT_BLOCKED",
        )
        entry = entry.transition(
            CanonicalStatus.BLOCKED,
            at=generated_at,
            artifacts=(pointer,),
            blocker_codes=blockers,
        )
    elif canonical_status is CanonicalStatus.PASS:
        entry = entry.transition(
            CanonicalStatus.PASS,
            at=generated_at,
            artifacts=(pointer,),
        )
    else:
        raise ValueError(f"unsupported reference experiment terminal status: {canonical_status}")
    return ledger.with_entry(workflow, entry)


def _source_artifact_record(path: Path) -> dict[str, object]:
    content = path.read_bytes()
    return {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _source_pointer(record: Mapping[str, object]) -> ArtifactPointer:
    path = Path(str(record["path"]))
    return ArtifactPointer(
        path=str(path),
        artifact_type=path.suffix.lower().lstrip(".") or "file",
        sha256=str(record["sha256"]),
        size_bytes=_required_int(record.get("size_bytes"), "source_artifact.size_bytes"),
        schema_version="source_artifact.v1",
    )


def _resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def _generated_at(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(tz=UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")
    return timestamp.astimezone(UTC).replace(microsecond=0)


def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _required_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    return value
