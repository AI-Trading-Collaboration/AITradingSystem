from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ai_trading_system.contracts import (
    ArtifactLifecycle,
    CanonicalStatus,
    EntrypointRef,
    ReaderTier,
    ReportAudience,
    ReportSpec,
    WorkflowCadence,
)
from ai_trading_system.core import ProductionEffect
from ai_trading_system.platform.config import ResolvedConfig, resolve_yaml_config


class ExperimentSpecError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class InputDocumentKind(StrEnum):
    STRUCTURED = "structured"
    TEXT = "text"


class OutputArtifactKind(StrEnum):
    PRIMARY_JSON = "primary_json"
    SECTION_JSON = "section_json"
    MARKDOWN = "markdown"
    ENVELOPE_JSON = "envelope_json"
    RUN_LEDGER_JSON = "run_ledger_json"


class OutputRoot(StrEnum):
    OUTPUT = "output"
    DOCS = "docs"


class PluginRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    plugin_id: str = Field(min_length=1, pattern=r"^[a-z0-9_.-]+$")
    version: str = Field(min_length=1)


class ExperimentInputSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    input_id: str = Field(min_length=1, pattern=r"^[a-z0-9_]+$")
    document_kind: InputDocumentKind
    role: str = Field(min_length=1)
    required: bool = True
    default_path: str = Field(min_length=1)


class ExperimentOutputSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    output_id: str = Field(min_length=1, pattern=r"^[a-z0-9_]+$")
    artifact_kind: OutputArtifactKind
    root: OutputRoot
    filename: str = Field(min_length=1)
    artifact_path_key: str | None = None
    payload_key: str | None = None
    section_report_type: str | None = None

    @model_validator(mode="after")
    def validate_section_contract(self) -> Self:
        if self.artifact_kind is OutputArtifactKind.SECTION_JSON:
            if not self.payload_key or not self.section_report_type:
                raise ValueError("section_json requires payload_key and section_report_type")
        return self


class LegacyStatusMapping(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    legacy_status: str = Field(min_length=1)
    canonical_status: CanonicalStatus


class ExperimentReportContract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    report_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    audience: ReportAudience
    reader_tier: ReaderTier
    cadence: WorkflowCadence
    canonical_source: str = Field(min_length=3)
    section_provider: str = Field(min_length=3)
    view_model: str = Field(min_length=3)
    renderer: str = Field(min_length=3)
    artifact_globs: tuple[str, ...] = Field(min_length=1)
    freshness_sla_days: int = Field(ge=0)
    owner_action: str = Field(min_length=1)
    actionable: bool
    lifecycle: ArtifactLifecycle = ArtifactLifecycle.CURRENT

    def to_contract(self, *, owner: str, production_effect: ProductionEffect) -> ReportSpec:
        return ReportSpec(
            report_id=self.report_id,
            title=self.title,
            owner=owner,
            audience=self.audience,
            reader_tier=self.reader_tier,
            cadence=self.cadence,
            canonical_source=_entrypoint(self.canonical_source),
            section_provider=_entrypoint(self.section_provider),
            view_model=_entrypoint(self.view_model),
            renderer=_entrypoint(self.renderer),
            artifact_globs=self.artifact_globs,
            freshness_sla_days=self.freshness_sla_days,
            owner_action=self.owner_action,
            actionable=self.actionable,
            lifecycle=self.lifecycle,
            production_effect=production_effect,
        )


class ExperimentSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["experiment_spec.v1"] = "experiment_spec.v1"
    experiment_id: str = Field(min_length=1, pattern=r"^[a-z0-9_.-]+$")
    spec_version: str = Field(min_length=1)
    status: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    task_register_id: str = Field(min_length=1)
    calculator_plugin: PluginRef
    report_plugin: PluginRef
    inputs: tuple[ExperimentInputSpec, ...] = Field(min_length=1)
    outputs: tuple[ExperimentOutputSpec, ...] = Field(min_length=5)
    legacy_status_mappings: tuple[LegacyStatusMapping, ...] = Field(min_length=1)
    report: ExperimentReportContract
    strict_default: bool = False
    data_quality_required: bool = False
    investment_facing_envelope: bool = False
    manual_review_required: bool = True
    production_effect: ProductionEffect = ProductionEffect.NONE
    broker_action: Literal["none"] = "none"

    @model_validator(mode="after")
    def validate_unique_contracts(self) -> Self:
        _require_unique([item.input_id for item in self.inputs], "input id")
        _require_unique([item.output_id for item in self.outputs], "output id")
        _require_unique(
            [item.legacy_status for item in self.legacy_status_mappings],
            "legacy status",
        )
        kinds = {item.artifact_kind for item in self.outputs}
        required_kinds = set(OutputArtifactKind)
        if kinds != required_kinds:
            missing = sorted(item.value for item in required_kinds - kinds)
            extra = sorted(item.value for item in kinds - required_kinds)
            raise ValueError(f"output artifact kinds mismatch missing={missing} extra={extra}")
        path_keys = [item.artifact_path_key for item in self.outputs if item.artifact_path_key]
        _require_unique(path_keys, "artifact path key")
        if self.production_effect is not ProductionEffect.NONE:
            raise ValueError("reference experiment production_effect must remain none")
        self.report.to_contract(
            owner=self.owner,
            production_effect=self.production_effect,
        )
        return self

    @property
    def spec_id(self) -> str:
        content = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"experiment_spec_{hashlib.sha256(content).hexdigest()[:20]}"

    def canonical_status(self, legacy_status: str) -> CanonicalStatus:
        mapping = {
            item.legacy_status: item.canonical_status for item in self.legacy_status_mappings
        }
        if legacy_status not in mapping:
            raise ExperimentSpecError("UNKNOWN_EXPERIMENT_STATUS", legacy_status)
        return mapping[legacy_status]

    def input(self, input_id: str) -> ExperimentInputSpec:
        for item in self.inputs:
            if item.input_id == input_id:
                return item
        raise ExperimentSpecError("UNKNOWN_EXPERIMENT_INPUT", input_id)

    def output(self, kind: OutputArtifactKind) -> ExperimentOutputSpec:
        matches = [item for item in self.outputs if item.artifact_kind is kind]
        if len(matches) != 1:
            raise ExperimentSpecError("EXPERIMENT_OUTPUT_NOT_UNIQUE", kind.value)
        return matches[0]

    def report_spec(self) -> ReportSpec:
        return self.report.to_contract(
            owner=self.owner,
            production_effect=self.production_effect,
        )

    def to_dict(self) -> dict[str, object]:
        return {"spec_id": self.spec_id, **self.model_dump(mode="json")}

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ExperimentSpec:
        content = dict(payload)
        supplied_id = content.pop("spec_id", None)
        spec = cls.model_validate(content)
        if supplied_id is not None and str(supplied_id) != spec.spec_id:
            raise ExperimentSpecError(
                "EXPERIMENT_SPEC_ID_MISMATCH",
                f"supplied={supplied_id} actual={spec.spec_id}",
            )
        return spec


ResolvedExperimentSpec = ResolvedConfig[ExperimentSpec]


def resolve_experiment_spec(path: Path | str) -> ResolvedExperimentSpec:
    spec_path = Path(path)
    return resolve_yaml_config(
        spec_path,
        ExperimentSpec,
        policy_id=f"experiment:{spec_path.stem}",
    )


def _entrypoint(value: str) -> EntrypointRef:
    module, separator, callable_name = value.partition(":")
    if separator != ":":
        raise ValueError(f"entrypoint must be module:callable: {value}")
    return EntrypointRef(module=module, callable_name=callable_name)


def _require_unique(values: list[str], label: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label}")
