from __future__ import annotations

import ast
import json
import re
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import (
    load_strict_json_path,
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

INVENTORY_SCHEMA_VERSION = "data_quality_issue_attribution_readiness_inventory.v1"
VALIDATION_SCHEMA_VERSION = "data_quality_issue_attribution_readiness_validation.v1"
TASK_ID = "DATA-GOV-002C1_DQ_ISSUE_ATTRIBUTION_READINESS_INVENTORY"
OWNER_DECISION_ID = (
    "owner_decision:DATA-GOV-002:2026-07-26:" "approve_long_term_capability_receipt_engineering_v1"
)
CANONICAL_QUALITY_PATH = "src/ai_trading_system/data/quality.py"
CANONICAL_EXECUTION_PATH = "src/ai_trading_system/data/quality_execution.py"
CANONICAL_SOURCE_PATHS = (CANONICAL_QUALITY_PATH, CANONICAL_EXECUTION_PATH)
CAPABILITY_POLICY_PATHS = (
    "config/data_quality/decision_target_label_core_capability_v1.yaml",
    "config/data_quality/regime_label_generator_capability_v1.yaml",
)
DEFAULT_JSON_PATH = "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.json"
DEFAULT_MARKDOWN_PATH = "docs/data_quality/dq_issue_attribution_readiness_inventory_v1.md"
DEFAULT_VALIDATION_PATH = (
    "inputs/data_quality/dq_issue_attribution_readiness_inventory_v1.validation.json"
)
EXPECTED_POLICY_SCHEMA = "data_quality_consumer_capability_policy.v1"
GLOBAL_OR_UNKNOWN_SCOPE = "GLOBAL_OR_UNKNOWN_SCOPE"
OWNER_REVIEW_REQUIRED = "OWNER_REVIEW_REQUIRED"
EXISTING_POLICY_SCOPE = "EXISTING_POLICY_AUTHORIZED_INSTRUMENT_SCOPE"
EXISTING_OWNER_REVIEW = "EXISTING_OWNER_REVIEWED_PILOT"
REQUIRED_PHASE_C_DIMENSIONS = (
    "affected_price_tickers",
    "affected_rate_series",
    "affected_source_roles",
    "affected_window",
    "affected_fields",
    "affected_rows",
)


class AttributionInventoryError(ValueError):
    pass


@dataclass(frozen=True)
class InventoryPaths:
    repo_root: Path
    canonical_source_paths: tuple[str, ...] = CANONICAL_SOURCE_PATHS
    capability_policy_paths: tuple[str, ...] = CAPABILITY_POLICY_PATHS

    def resolve(self, relative_path: str) -> Path:
        root = self.repo_root.resolve()
        resolved = (root / relative_path).resolve()
        if root != resolved and root not in resolved.parents:
            raise AttributionInventoryError(f"path escapes repository: {relative_path}")
        if not resolved.is_file():
            raise AttributionInventoryError(f"required file missing: {relative_path}")
        return resolved


def build_attribution_readiness_inventory(
    *,
    repo_root: Path,
    canonical_source_paths: tuple[str, ...] = CANONICAL_SOURCE_PATHS,
    capability_policy_paths: tuple[str, ...] = CAPABILITY_POLICY_PATHS,
) -> dict[str, Any]:
    paths = InventoryPaths(
        repo_root=repo_root,
        canonical_source_paths=canonical_source_paths,
        capability_policy_paths=capability_policy_paths,
    )
    source_bindings = [
        _file_binding(paths, relative_path) for relative_path in paths.canonical_source_paths
    ]
    policy_bindings, policy_codes = _load_policy_bindings(paths)

    quality_sites, quality_support = _scan_source(
        paths,
        CANONICAL_QUALITY_PATH,
        target_emitter="DataQualityIssue",
        emitter_kind="DIRECT_CONSTRUCTOR",
    )
    execution_sites, execution_support = _scan_source(
        paths,
        CANONICAL_EXECUTION_PATH,
        target_emitter="_provenance_issue",
        emitter_kind="FACTORY_CALL",
    )
    canonical_sites = _finalize_sites(
        [*quality_sites, *execution_sites],
        policy_codes=policy_codes,
    )
    factory_implementation_sites = [
        site
        for site in execution_support
        if site["emitter"] == "DataQualityIssue"
        and site["enclosing_function"] == "_provenance_issue"
    ]
    noncanonical_constructor_sites = _scan_noncanonical_constructors(
        paths,
        excluded_paths=set(paths.canonical_source_paths),
    )

    code_kind_counts = Counter(str(site["code_kind"]) for site in canonical_sites)
    policy_authorized_site_count = sum(
        bool(site["existing_policy_authorized"]) for site in canonical_sites
    )
    typed_site_count = sum(
        bool(site["legacy_affected_instruments_present"]) for site in canonical_sites
    )
    summary = {
        "canonical_site_count": len(canonical_sites),
        "direct_constructor_site_count": sum(
            site["emitter_kind"] == "DIRECT_CONSTRUCTOR" for site in canonical_sites
        ),
        "factory_call_site_count": sum(
            site["emitter_kind"] == "FACTORY_CALL" for site in canonical_sites
        ),
        "static_site_count": code_kind_counts["STATIC_LITERAL"],
        "template_site_count": code_kind_counts["TEMPLATE_EXPRESSION"],
        "dynamic_site_count": code_kind_counts["DYNAMIC_EXPRESSION"],
        "unique_static_code_count": len(
            {
                str(site["static_code"])
                for site in canonical_sites
                if site["static_code"] is not None
            }
        ),
        "policy_authorized_code_count": len(policy_codes),
        "policy_authorized_site_count": policy_authorized_site_count,
        "legacy_affected_instruments_site_count": typed_site_count,
        "owner_review_required_site_count": sum(
            site["owner_review_status"] == OWNER_REVIEW_REQUIRED for site in canonical_sites
        ),
        "factory_implementation_constructor_count": len(factory_implementation_sites),
        "noncanonical_constructor_site_count": len(noncanonical_constructor_sites),
    }
    payload_without_id: dict[str, Any] = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "task_id": TASK_ID,
        "owner_decision_id": OWNER_DECISION_ID,
        "status": "SOURCE_OWNER_REVIEW_REQUIRED",
        "authority": {
            "phase": "DATA-GOV-002_PHASE_C",
            "inventory_is_authorization": False,
            "new_issue_migration_authorized": False,
            "message_or_sample_scope_inference_allowed": False,
            "unreviewed_default_scope_status": GLOBAL_OR_UNKNOWN_SCOPE,
        },
        "source_bindings": source_bindings,
        "policy_bindings": policy_bindings,
        "policy_authorized_issue_codes": sorted(policy_codes),
        "required_phase_c_dimensions": list(REQUIRED_PHASE_C_DIMENSIONS),
        "summary": summary,
        "sites": canonical_sites,
        "factory_implementation_sites": factory_implementation_sites,
        "noncanonical_constructor_sites": noncanonical_constructor_sites,
        "safety": {
            "data_quality_behavior_changed": False,
            "capability_policy_changed": False,
            "new_issue_isolation_authorized": False,
            "consumer_migration_executed": False,
            "cached_data_mutated": False,
            "strategy_logic_changed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    inventory_id = (
        "dq_issue_attribution_inventory_"
        + sha256(_canonical_json_bytes(payload_without_id)).hexdigest()[:24]
    )
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "inventory_id": inventory_id,
        **{key: value for key, value in payload_without_id.items() if key != "schema_version"},
    }


def validate_attribution_readiness_inventory(
    payload: dict[str, Any],
    *,
    repo_root: Path,
    canonical_source_paths: tuple[str, ...] = CANONICAL_SOURCE_PATHS,
    capability_policy_paths: tuple[str, ...] = CAPABILITY_POLICY_PATHS,
) -> dict[str, Any]:
    errors: list[str] = []
    try:
        expected = build_attribution_readiness_inventory(
            repo_root=repo_root,
            canonical_source_paths=canonical_source_paths,
            capability_policy_paths=capability_policy_paths,
        )
    except (AttributionInventoryError, OSError, SyntaxError, ValueError) as exc:
        return _validation_payload(
            status="FAIL",
            inventory_id=str(payload.get("inventory_id", "")),
            expected_inventory_id=None,
            errors=[f"rebuild_failed:{type(exc).__name__}:{exc}"],
        )

    if payload.get("schema_version") != INVENTORY_SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if payload.get("inventory_id") != expected["inventory_id"]:
        errors.append("inventory_id_mismatch")
    if payload != expected:
        errors.append("content_derived_rebuild_mismatch")
    site_ids = [
        str(site.get("site_id", "")) for site in payload.get("sites", []) if isinstance(site, dict)
    ]
    if not site_ids or len(site_ids) != len(set(site_ids)):
        errors.append("site_identity_missing_or_duplicate")
    if any(
        site.get("phase_c_migration_eligible") is not False
        for site in payload.get("sites", [])
        if isinstance(site, dict)
    ):
        errors.append("phase_c_migration_authority_illegal")
    if payload.get("safety") != expected["safety"]:
        errors.append("safety_boundary_mismatch")
    return _validation_payload(
        status="FAIL" if errors else "PASS",
        inventory_id=str(payload.get("inventory_id", "")),
        expected_inventory_id=str(expected["inventory_id"]),
        errors=errors,
    )


def load_and_validate_attribution_readiness_inventory(
    *,
    repo_root: Path,
    inventory_path: Path,
) -> dict[str, Any]:
    raw = load_strict_json_path(inventory_path)
    if not isinstance(raw, dict):
        raise AttributionInventoryError("inventory root must be an object")
    return validate_attribution_readiness_inventory(raw, repo_root=repo_root)


def write_attribution_readiness_artifacts(
    *,
    repo_root: Path,
    json_path: Path,
    markdown_path: Path,
    validation_path: Path,
) -> dict[str, Any]:
    inventory = build_attribution_readiness_inventory(repo_root=repo_root)
    write_json_atomic(json_path, inventory)
    write_markdown_atomic(markdown_path, render_attribution_readiness_markdown(inventory))
    validation = validate_attribution_readiness_inventory(
        inventory,
        repo_root=repo_root,
    )
    write_json_atomic(validation_path, validation)
    return {
        "inventory": inventory,
        "validation": validation,
        "paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
            "validation": str(validation_path),
        },
    }


def render_attribution_readiness_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# DATA-GOV-002 Phase C：DQ Issue Attribution Readiness Inventory",
        "",
        f"- inventory_id：`{payload['inventory_id']}`",
        f"- status：`{payload['status']}`",
        "- 当前结论：本 inventory 不是新 issue 隔离授权；未 review 项继续 "
        f"`{GLOBAL_OR_UNKNOWN_SCOPE}`。",
        "- message/sample scope inference：`false`",
        "- production_effect：`none`",
        "- broker_action：`none`",
        "",
        "## 汇总",
        "",
        "|指标|值|",
        "|---|---:|",
    ]
    for key in (
        "canonical_site_count",
        "direct_constructor_site_count",
        "factory_call_site_count",
        "static_site_count",
        "template_site_count",
        "dynamic_site_count",
        "unique_static_code_count",
        "policy_authorized_code_count",
        "policy_authorized_site_count",
        "legacy_affected_instruments_site_count",
        "owner_review_required_site_count",
        "factory_implementation_constructor_count",
        "noncanonical_constructor_site_count",
    ):
        lines.append(f"|`{key}`|{summary[key]}|")
    lines.extend(
        [
            "",
            "## 当前 reviewed policy code",
            "",
            *[f"- `{code}`" for code in payload["policy_authorized_issue_codes"]],
            "",
            "## Canonical emission sites",
            "",
            "|site_id|code kind / expression|emitter|scope status|owner review|legacy typed field|",
            "|---|---|---|---|---|---|",
        ]
    )
    for site in payload["sites"]:
        expression = _escape_markdown(str(site["code_expression"]))
        lines.append(
            "|"
            f"`{site['site_id']}`|"
            f"`{site['code_kind']}` / `{expression}`|"
            f"`{site['source_path']}::{site['enclosing_function']}`|"
            f"`{site['scope_status']}`|"
            f"`{site['owner_review_status']}`|"
            f"`affected_instruments={str(site['legacy_affected_instruments_present']).lower()}`"
            "|"
        )
    lines.extend(
        [
            "",
            "## Non-canonical constructors",
            "",
            "这些调用不在 capability attribution 权威内；数量可见用于防止扫描边界被误读。",
            "",
            "|path|function|line|code expression|",
            "|---|---|---:|---|",
        ]
    )
    for site in payload["noncanonical_constructor_sites"]:
        lines.append(
            f"|`{site['source_path']}`|`{site['enclosing_function']}`|"
            f"{site['line']}|`{_escape_markdown(str(site['code_expression']))}`|"
        )
    lines.extend(
        [
            "",
            "## 下一步",
            "",
            "Source owner 必须逐 exact site/code 审查 attribution domain、source-wide/row-scoped "
            "taxonomy 和完整性生成方式。只有独立 reviewed contract wave 可以新增 typed schema "
            "字段或扩大 allowed issue code。",
            "",
        ]
    )
    return "\n".join(lines)


def _scan_source(
    paths: InventoryPaths,
    relative_path: str,
    *,
    target_emitter: str,
    emitter_kind: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_path = paths.resolve(relative_path)
    text = source_path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=relative_path)
    parents = _parent_map(tree)
    target_sites: list[dict[str, Any]] = []
    support_sites: list[dict[str, Any]] = []
    calls = sorted(
        (node for node in ast.walk(tree) if isinstance(node, ast.Call)),
        key=lambda node: (node.lineno, node.col_offset),
    )
    for call in calls:
        emitter = _call_name(call.func)
        if emitter not in {target_emitter, "DataQualityIssue"}:
            continue
        site = _raw_site(
            call,
            parents=parents,
            source_path=relative_path,
            emitter_kind=emitter_kind if emitter == target_emitter else "SUPPORT_CONSTRUCTOR",
            emitter=emitter,
        )
        if emitter == target_emitter:
            target_sites.append(site)
        else:
            support_sites.append(site)
    return target_sites, support_sites


def _scan_noncanonical_constructors(
    paths: InventoryPaths,
    *,
    excluded_paths: set[str],
) -> list[dict[str, Any]]:
    root = paths.repo_root.resolve()
    source_root = root / "src" / "ai_trading_system"
    sites: list[dict[str, Any]] = []
    for path in sorted(source_root.rglob("*.py"), key=lambda item: item.as_posix()):
        relative_path = path.relative_to(root).as_posix()
        if relative_path in excluded_paths:
            continue
        source_text = path.read_text(encoding="utf-8")
        if "DataQualityIssue" not in source_text:
            continue
        tree = ast.parse(source_text, filename=relative_path)
        parents = _parent_map(tree)
        for call in sorted(
            (
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call) and _call_name(node.func) == "DataQualityIssue"
            ),
            key=lambda node: (node.lineno, node.col_offset),
        ):
            sites.append(
                _raw_site(
                    call,
                    parents=parents,
                    source_path=relative_path,
                    emitter_kind="NONCANONICAL_CONSTRUCTOR",
                    emitter="DataQualityIssue",
                )
            )
    return _assign_site_ids(sites)


def _raw_site(
    call: ast.Call,
    *,
    parents: dict[ast.AST, ast.AST],
    source_path: str,
    emitter_kind: str,
    emitter: str,
) -> dict[str, Any]:
    enclosing_function = _enclosing_function(call, parents)
    if emitter == "_provenance_issue":
        code_node = _argument(call, position=0, keyword="code")
        severity_expression = "Severity.ERROR"
        source_expression = "D0B canonical execution provenance"
        rows_present = False
        sample_present = True
        affected_present = False
    else:
        code_node = _argument(call, position=1, keyword="code")
        severity_expression = _expression(_argument(call, position=0, keyword="severity"))
        source_expression = _expression(_argument(call, position=5, keyword="source"))
        rows_present = _argument(call, position=3, keyword="rows") is not None
        sample_present = _argument(call, position=4, keyword="sample") is not None
        affected_present = (
            _argument(
                call,
                position=6,
                keyword="affected_instruments",
            )
            is not None
        )
    code_kind, code_expression, static_code = _code_expression(code_node)
    return {
        "source_path": source_path,
        "enclosing_function": enclosing_function,
        "line": call.lineno,
        "column": call.col_offset,
        "emitter_kind": emitter_kind,
        "emitter": emitter,
        "code_kind": code_kind,
        "code_expression": code_expression,
        "static_code": static_code,
        "severity_expression": severity_expression,
        "source_expression": source_expression,
        "rows_present": rows_present,
        "sample_present": sample_present,
        "legacy_affected_instruments_present": affected_present,
    }


def _finalize_sites(
    sites: list[dict[str, Any]],
    *,
    policy_codes: set[str],
) -> list[dict[str, Any]]:
    finalized: list[dict[str, Any]] = []
    for site in _assign_site_ids(sites):
        policy_authorized = (
            site["static_code"] in policy_codes and site["legacy_affected_instruments_present"]
        )
        finalized.append(
            {
                **site,
                "existing_policy_authorized": policy_authorized,
                "scope_status": (
                    EXISTING_POLICY_SCOPE if policy_authorized else GLOBAL_OR_UNKNOWN_SCOPE
                ),
                "owner_review_status": (
                    EXISTING_OWNER_REVIEW if policy_authorized else OWNER_REVIEW_REQUIRED
                ),
                "phase_c_migration_eligible": False,
                "required_review_dimensions": list(REQUIRED_PHASE_C_DIMENSIONS),
                "message_or_sample_scope_inference_allowed": False,
            }
        )
    return finalized


def _assign_site_ids(sites: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        sites,
        key=lambda site: (
            str(site["source_path"]),
            str(site["enclosing_function"]),
            int(site["line"]),
            int(site["column"]),
            str(site["emitter"]),
        ),
    )
    occurrences: Counter[tuple[str, ...]] = Counter()
    result: list[dict[str, Any]] = []
    for site in ordered:
        key = (
            str(site["source_path"]),
            str(site["enclosing_function"]),
            str(site["emitter_kind"]),
            str(site["emitter"]),
            str(site["code_kind"]),
            str(site["code_expression"]),
        )
        occurrence = occurrences[key]
        occurrences[key] += 1
        identity = {
            "source_path": key[0],
            "enclosing_function": key[1],
            "emitter_kind": key[2],
            "emitter": key[3],
            "code_kind": key[4],
            "code_expression": key[5],
            "occurrence": occurrence,
        }
        result.append(
            {
                "site_id": "dq_issue_site_"
                + sha256(_canonical_json_bytes(identity)).hexdigest()[:20],
                **site,
                "occurrence": occurrence,
            }
        )
    return result


def _load_policy_bindings(
    paths: InventoryPaths,
) -> tuple[list[dict[str, Any]], set[str]]:
    bindings: list[dict[str, Any]] = []
    rules_by_code: dict[str, set[str]] = {}
    for relative_path in paths.capability_policy_paths:
        resolved = paths.resolve(relative_path)
        raw = safe_load_yaml_path(resolved)
        if not isinstance(raw, dict):
            raise AttributionInventoryError(f"policy root must be a mapping: {relative_path}")
        if raw.get("schema_version") != EXPECTED_POLICY_SCHEMA:
            raise AttributionInventoryError(f"unsupported policy schema: {relative_path}")
        codes = raw.get("allowed_global_error_codes")
        rule = raw.get("global_error_attribution_rule")
        if (
            not isinstance(codes, list)
            or not codes
            or any(not isinstance(code, str) or not code.strip() for code in codes)
        ):
            raise AttributionInventoryError(
                f"policy allowed_global_error_codes invalid: {relative_path}"
            )
        if not isinstance(rule, str) or not rule.strip():
            raise AttributionInventoryError(f"policy attribution rule invalid: {relative_path}")
        for code in codes:
            rules_by_code.setdefault(code.strip(), set()).add(rule.strip())
        binding = _file_binding(paths, relative_path)
        bindings.append(
            {
                **binding,
                "policy_id": raw.get("policy_id"),
                "policy_version": raw.get("policy_version"),
                "status": raw.get("status"),
                "owner": raw.get("owner"),
                "owner_decision_id": raw.get("owner_decision_id"),
                "allowed_global_error_codes": sorted({code.strip() for code in codes}),
                "global_error_attribution_rule": rule.strip(),
            }
        )
    inconsistent = {code: sorted(rules) for code, rules in rules_by_code.items() if len(rules) != 1}
    if inconsistent:
        raise AttributionInventoryError(f"inconsistent reviewed attribution rules: {inconsistent}")
    return bindings, set(rules_by_code)


def _file_binding(paths: InventoryPaths, relative_path: str) -> dict[str, Any]:
    content = paths.resolve(relative_path).read_bytes()
    return {
        "path": relative_path,
        "sha256": sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _validation_payload(
    *,
    status: str,
    inventory_id: str,
    expected_inventory_id: str | None,
    errors: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": status,
        "task_id": TASK_ID,
        "inventory_id": inventory_id,
        "expected_inventory_id": expected_inventory_id,
        "error_count": len(errors),
        "errors": errors,
        "checks": {
            "content_derived_rebuild": status == "PASS",
            "source_and_policy_bindings": status == "PASS",
            "site_identity_unique": status == "PASS",
            "no_new_migration_authority": status == "PASS",
            "safety_boundary": status == "PASS",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _enclosing_function(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> str:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return "<module>"


def _call_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ast.unparse(node)


def _argument(
    call: ast.Call,
    *,
    position: int,
    keyword: str,
) -> ast.expr | None:
    for item in call.keywords:
        if item.arg == keyword:
            return item.value
    return call.args[position] if len(call.args) > position else None


def _code_expression(
    node: ast.expr | None,
) -> tuple[str, str, str | None]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return "STATIC_LITERAL", node.value, node.value
    if isinstance(node, ast.JoinedStr):
        return "TEMPLATE_EXPRESSION", _expression(node), None
    return "DYNAMIC_EXPRESSION", _expression(node), None


def _expression(node: ast.expr | None) -> str:
    if node is None:
        return "<absent>"
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return re.sub(r"\s+", " ", ast.unparse(node)).strip()


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _escape_markdown(value: str) -> str:
    return value.replace("|", "\\|").replace("`", "\\`").replace("\n", " ")
