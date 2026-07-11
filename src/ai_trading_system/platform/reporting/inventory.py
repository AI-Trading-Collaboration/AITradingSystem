from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_REPORTING_ARCHITECTURE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "reporting" / "reporting_architecture.yaml"
)
DEFAULT_REPORTING_INVENTORY_PATH = (
    PROJECT_ROOT / "inputs" / "architecture" / "arch_004f3_reporting_inventory.yaml"
)
DEFAULT_READER_BRIEF_SOURCE_PATH = (
    PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "reader_brief.py"
)
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_REPORT_FRAGMENT_ROOT = PROJECT_ROOT / "config" / "architecture" / "fragments" / "reports"


class ReportingArchitectureError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class ReportingCoreSectionPolicy:
    section_id: str
    source_keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.section_id.strip() or not self.source_keys:
            raise ReportingArchitectureError("REPORTING_CORE_SECTION_INVALID", self.section_id)
        if any(not key.strip() for key in self.source_keys):
            raise ReportingArchitectureError("REPORTING_CORE_SOURCE_KEY_INVALID", self.section_id)


@dataclass(frozen=True)
class ReportingArchitecturePolicy:
    policy_id: str
    owner: str
    version: str
    max_core_sections: int
    core_sections: tuple[ReportingCoreSectionPolicy, ...]
    owner_queue_requires_due: bool
    owner_queue_requires_actionable: bool
    research_auto_tune_allowed: bool
    proposal_may_equal_adoption: bool
    audit_include_all_registry_entries: bool
    audit_include_legacy_unclassified: bool
    legacy_unclassified_disposition: str
    reader_brief_cut_in_enabled: bool
    additive_sidecars_only: bool
    preserve_legacy_path_schema_status: bool
    reporting_layer_recompute_allowed: bool
    production_effect: str
    broker_action: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.policy_id, "policy_id"),
            (self.owner, "owner"),
            (self.version, "version"),
            (self.legacy_unclassified_disposition, "legacy_unclassified_disposition"),
        ):
            if not value.strip():
                raise ReportingArchitectureError("REPORTING_POLICY_FIELD_MISSING", field)
        if isinstance(self.max_core_sections, bool) or not 1 <= self.max_core_sections <= 10:
            raise ReportingArchitectureError(
                "REPORTING_CORE_SECTION_LIMIT_INVALID", str(self.max_core_sections)
            )
        section_ids = [item.section_id for item in self.core_sections]
        if len(section_ids) != len(set(section_ids)):
            raise ReportingArchitectureError("REPORTING_CORE_SECTION_DUPLICATE", self.policy_id)
        if len(section_ids) > self.max_core_sections:
            raise ReportingArchitectureError(
                "REPORTING_CORE_SECTION_LIMIT_EXCEEDED", self.policy_id
            )
        if not self.owner_queue_requires_due or not self.owner_queue_requires_actionable:
            raise ReportingArchitectureError("REPORTING_OWNER_QUEUE_GATE_REQUIRED", self.policy_id)
        if self.research_auto_tune_allowed or self.proposal_may_equal_adoption:
            raise ReportingArchitectureError("REPORTING_RESEARCH_SAFETY_INVALID", self.policy_id)
        if (
            not self.audit_include_all_registry_entries
            or not self.audit_include_legacy_unclassified
        ):
            raise ReportingArchitectureError("REPORTING_AUDIT_COVERAGE_REQUIRED", self.policy_id)
        if self.legacy_unclassified_disposition != "AUDIT_INDEX_LIMITED_UNCLASSIFIED":
            raise ReportingArchitectureError("REPORTING_LEGACY_DISPOSITION_INVALID", self.policy_id)
        if self.reader_brief_cut_in_enabled or not self.additive_sidecars_only:
            raise ReportingArchitectureError("REPORTING_PREMATURE_CUT_IN", self.policy_id)
        if not self.preserve_legacy_path_schema_status:
            raise ReportingArchitectureError("REPORTING_LEGACY_PARITY_REQUIRED", self.policy_id)
        if self.reporting_layer_recompute_allowed:
            raise ReportingArchitectureError("REPORTING_RECOMPUTE_FORBIDDEN", self.policy_id)
        if self.production_effect != "none" or self.broker_action != "none":
            raise ReportingArchitectureError("REPORTING_SAFETY_BOUNDARY_INVALID", self.policy_id)


@dataclass(frozen=True)
class ReportingArchitectureInventory:
    reader_brief_path: str
    reader_brief_sha256: str
    reader_brief_line_count: int
    reader_brief_top_level_function_count: int
    report_registry_path: str
    report_registry_sha256: str
    report_registry_entry_count: int
    explicit_production_effect_count: int
    missing_explicit_production_effect_count: int
    explicit_reader_tier_count: int
    explicit_actionable_count: int
    explicit_section_provider_count: int
    explicit_view_model_count: int
    explicit_renderer_count: int
    explicit_canonical_source_count: int
    cadence_counts: tuple[tuple[str, int], ...]
    audience_counts: tuple[tuple[str, int], ...]
    report_fragment_root: str
    report_fragment_count: int
    active_report_fragment_count: int
    legacy_unclassified_entry_count: int

    @property
    def inventory_id(self) -> str:
        material = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"reporting_inventory_{hashlib.sha256(material).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": "reporting_architecture_inventory.v1",
            "reader_brief": {
                "path": self.reader_brief_path,
                "sha256": self.reader_brief_sha256,
                "line_count": self.reader_brief_line_count,
                "top_level_function_count": self.reader_brief_top_level_function_count,
            },
            "report_registry": {
                "path": self.report_registry_path,
                "sha256": self.report_registry_sha256,
                "entry_count": self.report_registry_entry_count,
                "explicit_production_effect_count": self.explicit_production_effect_count,
                "missing_explicit_production_effect_count": (
                    self.missing_explicit_production_effect_count
                ),
                "explicit_reader_tier_count": self.explicit_reader_tier_count,
                "explicit_actionable_count": self.explicit_actionable_count,
                "explicit_section_provider_count": self.explicit_section_provider_count,
                "explicit_view_model_count": self.explicit_view_model_count,
                "explicit_renderer_count": self.explicit_renderer_count,
                "explicit_canonical_source_count": self.explicit_canonical_source_count,
                "cadence_counts": dict(self.cadence_counts),
                "audience_counts": dict(self.audience_counts),
            },
            "generated_report_fragments": {
                "root": self.report_fragment_root,
                "fragment_count": self.report_fragment_count,
                "active_source_of_truth_count": self.active_report_fragment_count,
            },
            "legacy_unclassified_entry_count": self.legacy_unclassified_entry_count,
        }
        return {"inventory_id": self.inventory_id, **payload} if include_id else payload


def load_reporting_architecture_policy(
    path: Path = DEFAULT_REPORTING_ARCHITECTURE_POLICY_PATH,
) -> ReportingArchitecturePolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping) or payload.get("schema_version") != (
        "reporting_architecture_policy.v1"
    ):
        raise ReportingArchitectureError("REPORTING_POLICY_SCHEMA_INVALID", str(path))
    owner = _mapping(payload, "owner_daily_brief")
    research = _mapping(payload, "research_review_pack")
    audit = _mapping(payload, "audit_index")
    legacy = _mapping(payload, "legacy_compatibility")
    safety = _mapping(payload, "safety_boundary")
    sections = tuple(
        ReportingCoreSectionPolicy(
            section_id=str(_as_mapping(item, "core_section").get("section_id", "")),
            source_keys=tuple(
                str(key) for key in _list(_as_mapping(item, "core_section"), "source_keys")
            ),
        )
        for item in _list(owner, "core_sections")
    )
    return ReportingArchitecturePolicy(
        policy_id=str(payload.get("policy_id", "")),
        owner=str(payload.get("owner", "")),
        version=str(payload.get("version", "")),
        max_core_sections=_int(owner, "max_core_sections"),
        core_sections=sections,
        owner_queue_requires_due=_bool(owner, "owner_queue_requires_due"),
        owner_queue_requires_actionable=_bool(owner, "owner_queue_requires_actionable"),
        research_auto_tune_allowed=_bool(research, "auto_tune_allowed"),
        proposal_may_equal_adoption=_bool(research, "proposal_may_equal_adoption"),
        audit_include_all_registry_entries=_bool(audit, "include_all_registry_entries"),
        audit_include_legacy_unclassified=_bool(audit, "include_legacy_unclassified"),
        legacy_unclassified_disposition=str(legacy.get("unclassified_disposition", "")),
        reader_brief_cut_in_enabled=_bool(legacy, "reader_brief_cut_in_enabled"),
        additive_sidecars_only=_bool(legacy, "additive_sidecars_only"),
        preserve_legacy_path_schema_status=_bool(legacy, "preserve_legacy_path_schema_status"),
        reporting_layer_recompute_allowed=_bool(safety, "reporting_layer_recompute_allowed"),
        production_effect=str(safety.get("production_effect", "")),
        broker_action=str(safety.get("broker_action", "")),
    )


def scan_reporting_architecture(
    *,
    reader_brief_path: Path = DEFAULT_READER_BRIEF_SOURCE_PATH,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    report_fragment_root: Path = DEFAULT_REPORT_FRAGMENT_ROOT,
) -> ReportingArchitectureInventory:
    reader_bytes = reader_brief_path.read_bytes()
    reader_text = reader_bytes.decode("utf-8")
    tree = ast.parse(reader_text)
    function_count = sum(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in tree.body
    )
    registry_bytes = report_registry_path.read_bytes()
    registry = safe_load_yaml_path(report_registry_path)
    if not isinstance(registry, Mapping):
        raise ReportingArchitectureError("REPORTING_REGISTRY_INVALID", str(report_registry_path))
    entries = _list(registry, "reports")
    mapped_entries = tuple(_as_mapping(item, "report") for item in entries)
    cadence_counts = Counter(str(item.get("cadence", "")) for item in mapped_entries)
    audience_counts = Counter(str(item.get("audience", "")) for item in mapped_entries)
    explicit_typed = sum(
        all(
            field in item
            for field in (
                "reader_tier",
                "actionable",
                "section_provider",
                "view_model",
                "renderer",
                "canonical_source",
            )
        )
        for item in mapped_entries
    )
    fragments = tuple(sorted(report_fragment_root.glob("*.yaml")))
    active_fragments = 0
    for fragment in fragments:
        fragment_payload = safe_load_yaml_path(fragment)
        if isinstance(fragment_payload, Mapping) and (
            fragment_payload.get("generated_source_of_truth_active") is True
        ):
            active_fragments += 1
    return ReportingArchitectureInventory(
        reader_brief_path=_project_path(reader_brief_path),
        reader_brief_sha256=hashlib.sha256(reader_bytes).hexdigest(),
        reader_brief_line_count=len(reader_text.splitlines()),
        reader_brief_top_level_function_count=function_count,
        report_registry_path=_project_path(report_registry_path),
        report_registry_sha256=hashlib.sha256(registry_bytes).hexdigest(),
        report_registry_entry_count=len(mapped_entries),
        explicit_production_effect_count=sum(
            "production_effect" in item for item in mapped_entries
        ),
        missing_explicit_production_effect_count=sum(
            "production_effect" not in item for item in mapped_entries
        ),
        explicit_reader_tier_count=sum("reader_tier" in item for item in mapped_entries),
        explicit_actionable_count=sum("actionable" in item for item in mapped_entries),
        explicit_section_provider_count=sum("section_provider" in item for item in mapped_entries),
        explicit_view_model_count=sum("view_model" in item for item in mapped_entries),
        explicit_renderer_count=sum("renderer" in item for item in mapped_entries),
        explicit_canonical_source_count=sum("canonical_source" in item for item in mapped_entries),
        cadence_counts=tuple(sorted(cadence_counts.items())),
        audience_counts=tuple(sorted(audience_counts.items())),
        report_fragment_root=_project_path(report_fragment_root),
        report_fragment_count=len(fragments),
        active_report_fragment_count=active_fragments,
        legacy_unclassified_entry_count=len(mapped_entries) - explicit_typed,
    )


def assert_frozen_reporting_inventory(
    actual: ReportingArchitectureInventory,
    path: Path = DEFAULT_REPORTING_INVENTORY_PATH,
) -> None:
    frozen = safe_load_yaml_path(path)
    if not isinstance(frozen, Mapping):
        raise ReportingArchitectureError("REPORTING_INVENTORY_INVALID", str(path))
    expected = {
        "reader_brief": frozen.get("reader_brief"),
        "report_registry": frozen.get("report_registry"),
        "generated_report_fragments": frozen.get("generated_report_fragments"),
        "legacy_unclassified_entry_count": frozen.get("legacy_unclassified_entry_count"),
    }
    observed = actual.to_dict(include_id=False)
    if observed != {"schema_version": "reporting_architecture_inventory.v1", **expected}:
        raise ReportingArchitectureError("REPORTING_INVENTORY_DRIFT", str(path))


def _project_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _mapping(payload: object, field: str) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise ReportingArchitectureError("REPORTING_MAPPING_REQUIRED", field)
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise ReportingArchitectureError("REPORTING_MAPPING_REQUIRED", field)
    return value


def _as_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ReportingArchitectureError("REPORTING_MAPPING_REQUIRED", field)
    return value


def _list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise ReportingArchitectureError("REPORTING_LIST_REQUIRED", field)
    return value


def _int(payload: Mapping[str, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ReportingArchitectureError("REPORTING_INT_REQUIRED", field)
    return value


def _bool(payload: Mapping[str, object], field: str) -> bool:
    value = payload.get(field)
    if not isinstance(value, bool):
        raise ReportingArchitectureError("REPORTING_BOOL_REQUIRED", field)
    return value
