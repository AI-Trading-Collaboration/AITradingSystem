from __future__ import annotations

import ast
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final, NoReturn, cast

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.reporting.reader_brief_native import (
    project_data_quality_pit_safety,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_G3_CLOSE_READINESS_POLICY_PATH: Final = Path(
    "config/architecture/arch_004_wave15_g3_close_readiness.yaml"
)
REPORTING_ARCHITECTURE_POLICY_PATH: Final = Path("config/reporting/reporting_architecture.yaml")


class G3CloseReadinessError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class RemainingGenericProvider:
    section_id: str
    owner: str
    source_keys: tuple[str, ...]
    prerequisite_contract: str

    def to_dict(self) -> dict[str, object]:
        return {
            "section_id": self.section_id,
            "owner": self.owner,
            "source_keys": list(self.source_keys),
            "prerequisite_contract": self.prerequisite_contract,
            "migration_executed": False,
        }


@dataclass(frozen=True)
class G3CloseReadinessEvidence:
    policy_id: str
    policy_version: str
    owner_decision_id: str
    historical_f3_inventory_path: str
    historical_f3_raw_sha256: str
    reader_brief_path: str
    reader_brief_sha256: str
    reader_brief_line_count: int
    reader_brief_top_level_function_count: int
    legacy_projector_definition_count: int
    native_projector_import_count: int
    native_projector_call_count: int
    projected_field_count: int
    native_provider_count: int
    generic_provider_count: int
    fragment_paths: tuple[str, ...]
    report_fragment_count: int
    active_source_of_truth_count: int
    remaining_generic_providers: tuple[RemainingGenericProvider, ...]
    status: str = "PASS"
    bounded_slice_complete: bool = True
    migration_executed: bool = False
    g5_authorized: bool = False
    reporting_recompute_allowed: bool = False
    production_effect: str = "none"
    broker_action: str = "none"

    @property
    def evidence_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"arch_004_wave15_g3_close_{hashlib.sha256(material).hexdigest()[:24]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": "arch_004_wave15_g3_close_readiness_evidence.v1",
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "owner_decision_id": self.owner_decision_id,
            "status": self.status,
            "historical_f3_inventory": {
                "path": self.historical_f3_inventory_path,
                "raw_sha256": self.historical_f3_raw_sha256,
            },
            "reader_brief_source": {
                "path": self.reader_brief_path,
                "sha256": self.reader_brief_sha256,
                "line_count": self.reader_brief_line_count,
                "top_level_function_count": self.reader_brief_top_level_function_count,
            },
            "single_owner_ratchet": {
                "legacy_projector_definition_count": self.legacy_projector_definition_count,
                "native_projector_import_count": self.native_projector_import_count,
                "native_projector_call_count": self.native_projector_call_count,
            },
            "bounded_slice": {
                "projected_field_count": self.projected_field_count,
                "native_provider_count": self.native_provider_count,
                "generic_provider_count": self.generic_provider_count,
                "complete": self.bounded_slice_complete,
            },
            "fragments": {
                "paths": list(self.fragment_paths),
                "report_fragment_count": self.report_fragment_count,
                "active_source_of_truth_count": self.active_source_of_truth_count,
            },
            "remaining_generic_providers": [
                item.to_dict() for item in self.remaining_generic_providers
            ],
            "migration_executed": self.migration_executed,
            "g5_authorized": self.g5_authorized,
            "reporting_recompute_allowed": self.reporting_recompute_allowed,
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
        }

    def to_dict(self) -> dict[str, object]:
        return {"evidence_id": self.evidence_id, **self._semantic_payload()}


def build_g3_close_readiness_evidence(
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_G3_CLOSE_READINESS_POLICY_PATH,
) -> G3CloseReadinessEvidence:
    root = project_root.resolve()
    policy_relative = _repo_relative(root, policy_path)
    raw_policy = safe_load_yaml_path(root / policy_relative)
    policy = _mapping(raw_policy, "policy")
    if policy.get("schema_version") != "arch_004_wave15_g3_close_readiness_policy.v1":
        _fail("G3_CLOSE_POLICY_SCHEMA_INVALID", policy_relative)
    if policy.get("status") != "REVIEWED":
        _fail("G3_CLOSE_POLICY_NOT_REVIEWED", str(policy.get("status")))
    bounded = _mapping(policy.get("bounded_slice"), "bounded_slice")
    ratchets = _mapping(policy.get("ratchets"), "ratchets")
    historical = _mapping(ratchets.get("historical_f3_inventory"), "historical_f3_inventory")
    reader_expected = _mapping(ratchets.get("reader_brief_source"), "reader_brief_source")
    fragments_expected = _mapping(ratchets.get("fragments"), "fragments")
    safety = _mapping(policy.get("safety"), "safety")
    if (
        safety.get("g3_bounded_slice_close_only") is not True
        or safety.get("g5_authorized") is not False
        or safety.get("reporting_recompute_allowed") is not False
        or safety.get("investment_interpretation_change_allowed") is not False
        or safety.get("production_effect") != "none"
        or safety.get("broker_action") != "none"
    ):
        _fail("G3_CLOSE_SAFETY_BOUNDARY_INVALID", "policy safety fields expanded")
    if bounded.get("migration_executed") is not False:
        _fail("G3_CLOSE_PREMATURE_MIGRATION", "migration_executed must remain false")

    historical_path = _portable_path(historical.get("path"), "historical path")
    historical_bytes = (root / historical_path).read_bytes()
    historical_sha = hashlib.sha256(historical_bytes).hexdigest()
    if historical_sha != historical.get("raw_sha256"):
        _fail("G3_CLOSE_F3_INVENTORY_DRIFT", historical_path)

    reader_path = _portable_path(reader_expected.get("path"), "reader path")
    reader_bytes = (root / reader_path).read_bytes()
    reader_text = reader_bytes.decode("utf-8")
    reader_tree = ast.parse(reader_text, filename=reader_path)
    reader_sha = hashlib.sha256(reader_bytes).hexdigest()
    line_count = len(reader_text.splitlines())
    function_count = sum(
        isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) for node in reader_tree.body
    )
    observed_reader = {
        "path": reader_path,
        "sha256": reader_sha,
        "line_count": line_count,
        "top_level_function_count": function_count,
    }
    if observed_reader != dict(reader_expected):
        _fail("G3_CLOSE_READER_BRIEF_RATCHET_DRIFT", json.dumps(observed_reader))

    legacy_name = _text(bounded.get("legacy_projector_name"), "legacy projector name")
    legacy_definitions = sum(
        isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == legacy_name
        for node in reader_tree.body
    )
    native_imports = sum(
        alias.name == "project_data_quality_pit_safety"
        for node in ast.walk(reader_tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "ai_trading_system.platform.reporting.reader_brief_native"
        for alias in node.names
    )
    native_calls = sum(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "project_data_quality_pit_safety"
        for node in ast.walk(reader_tree)
    )
    if (legacy_definitions, native_imports, native_calls) != (0, 1, 1):
        _fail(
            "G3_CLOSE_SINGLE_OWNER_DRIFT",
            f"legacy/import/call={legacy_definitions}/{native_imports}/{native_calls}",
        )

    projected_count = _integer(bounded.get("projected_field_count"), "projected field count")
    observed_projection = project_data_quality_pit_safety(
        as_of=date(2026, 7, 23),
        snapshot={},
        daily_decision_summary={},
        report_index_summary={},
    )
    if projected_count != len(observed_projection) or projected_count != 19:
        _fail("G3_CLOSE_FIELD_PARITY_DRIFT", str(projected_count))
    native_count = _integer(bounded.get("native_provider_count"), "native provider count")
    generic_count = _integer(bounded.get("generic_provider_count"), "generic provider count")
    if (native_count, generic_count) != (1, 9):
        _fail("G3_CLOSE_PROVIDER_COUNT_DRIFT", f"{native_count}/{generic_count}")

    remaining = _remaining_inventory(policy)
    _verify_reporting_policy_inventory(root, bounded, remaining)
    fragment_paths = tuple(
        _portable_path(item, "fragment path")
        for item in _list(fragments_expected.get("paths"), "fragment paths")
    )
    _verify_g3_fragments(root, fragment_paths)
    report_fragment_root = root / "config/architecture/fragments/reports"
    report_fragments = tuple(sorted(report_fragment_root.glob("*.yaml")))
    active_count = sum(
        _mapping(safe_load_yaml_path(path), "fragment").get("generated_source_of_truth_active")
        is True
        for path in report_fragments
    )
    expected_fragment_count = _integer(
        fragments_expected.get("expected_total_report_fragment_count"),
        "report fragment count",
    )
    expected_active = _integer(
        fragments_expected.get("expected_active_source_of_truth_count"),
        "active fragment count",
    )
    if (len(report_fragments), active_count) != (
        expected_fragment_count,
        expected_active,
    ):
        _fail(
            "G3_CLOSE_FRAGMENT_RATCHET_DRIFT",
            f"{len(report_fragments)}/{active_count}",
        )

    return G3CloseReadinessEvidence(
        policy_id=_text(policy.get("policy_id"), "policy_id"),
        policy_version=_text(policy.get("policy_version"), "policy_version"),
        owner_decision_id=_text(policy.get("owner_decision_id"), "owner_decision_id"),
        historical_f3_inventory_path=historical_path,
        historical_f3_raw_sha256=historical_sha,
        reader_brief_path=reader_path,
        reader_brief_sha256=reader_sha,
        reader_brief_line_count=line_count,
        reader_brief_top_level_function_count=function_count,
        legacy_projector_definition_count=legacy_definitions,
        native_projector_import_count=native_imports,
        native_projector_call_count=native_calls,
        projected_field_count=projected_count,
        native_provider_count=native_count,
        generic_provider_count=generic_count,
        fragment_paths=fragment_paths,
        report_fragment_count=len(report_fragments),
        active_source_of_truth_count=active_count,
        remaining_generic_providers=remaining,
    )


def _remaining_inventory(
    policy: Mapping[str, object],
) -> tuple[RemainingGenericProvider, ...]:
    rows = _list(policy.get("remaining_generic_providers"), "remaining providers")
    result: list[RemainingGenericProvider] = []
    for raw in rows:
        item = _mapping(raw, "remaining provider")
        if set(item) != {
            "section_id",
            "owner",
            "source_keys",
            "prerequisite_contract",
        }:
            _fail("G3_CLOSE_REMAINING_INVENTORY_INVALID", str(sorted(item)))
        source_keys = tuple(
            _text(value, "source key") for value in _list(item.get("source_keys"), "source_keys")
        )
        if not source_keys:
            _fail("G3_CLOSE_REMAINING_INVENTORY_INVALID", "source_keys empty")
        result.append(
            RemainingGenericProvider(
                section_id=_text(item.get("section_id"), "section_id"),
                owner=_text(item.get("owner"), "owner"),
                source_keys=source_keys,
                prerequisite_contract=_text(
                    item.get("prerequisite_contract"), "prerequisite_contract"
                ),
            )
        )
    if len(result) != 9 or len({item.section_id for item in result}) != 9:
        _fail("G3_CLOSE_REMAINING_INVENTORY_INVALID", f"count={len(result)}")
    return tuple(result)


def _verify_reporting_policy_inventory(
    root: Path,
    bounded: Mapping[str, object],
    remaining: tuple[RemainingGenericProvider, ...],
) -> None:
    reporting = _mapping(
        safe_load_yaml_path(root / REPORTING_ARCHITECTURE_POLICY_PATH),
        "reporting policy",
    )
    owner_daily = _mapping(reporting.get("owner_daily_brief"), "owner_daily_brief")
    core_rows = _list(owner_daily.get("core_sections"), "core_sections")
    observed = {
        _text(_mapping(item, "core section").get("section_id"), "section_id"): tuple(
            _text(value, "source key")
            for value in _list(_mapping(item, "core section").get("source_keys"), "source_keys")
        )
        for item in core_rows
    }
    bounded_id = _text(bounded.get("section_id"), "bounded section id")
    expected_remaining = {item.section_id: item.source_keys for item in remaining}
    observed.pop(bounded_id, None)
    if observed != expected_remaining:
        _fail("G3_CLOSE_REMAINING_INVENTORY_DRIFT", str(sorted(observed)))


def _verify_g3_fragments(root: Path, paths: tuple[str, ...]) -> None:
    if len(paths) != 3:
        _fail("G3_CLOSE_FRAGMENT_RATCHET_DRIFT", f"expected=3 actual={len(paths)}")
    for relative in paths:
        fragment = _mapping(safe_load_yaml_path(root / relative), "fragment")
        if (
            fragment.get("current_state_ratchet_id") != "arch_004g3_reader_brief_native_current.v1"
            or fragment.get("generated_source_of_truth_active") is not False
            or fragment.get("production_effect") != "none"
        ):
            _fail("G3_CLOSE_FRAGMENT_RATCHET_DRIFT", relative)


def _repo_relative(root: Path, value: Path) -> str:
    candidate = value if value.is_absolute() else root / value
    try:
        return candidate.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise G3CloseReadinessError("G3_CLOSE_POLICY_PATH_INVALID", str(value)) from exc


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail("G3_CLOSE_POLICY_INVALID", f"{field} must be mapping")
    return dict(cast(Mapping[str, object], value))


def _list(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        _fail("G3_CLOSE_POLICY_INVALID", f"{field} must be list")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("G3_CLOSE_POLICY_INVALID", f"{field} must be non-empty text")
    return value


def _portable_path(value: object, field: str) -> str:
    text = _text(value, field)
    candidate = Path(text)
    if (
        candidate.is_absolute()
        or "\\" in text
        or text != candidate.as_posix()
        or any(part in {"", ".", ".."} for part in candidate.parts)
    ):
        _fail("G3_CLOSE_POLICY_PATH_INVALID", text)
    return text


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail("G3_CLOSE_POLICY_INVALID", f"{field} must be non-negative integer")
    return value


def _fail(code: str, message: str) -> NoReturn:
    raise G3CloseReadinessError(code, message)


__all__ = [
    "DEFAULT_G3_CLOSE_READINESS_POLICY_PATH",
    "G3CloseReadinessError",
    "G3CloseReadinessEvidence",
    "RemainingGenericProvider",
    "build_g3_close_readiness_evidence",
]
