from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Final, NoReturn

import yaml

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_universe,
)
from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data.access_control import load_acl_policy
from ai_trading_system.data.download_publication import resolve_download_publication
from ai_trading_system.data.foundation_consumer_migration import (
    DEFAULT_ACL_POLICY_PATH,
    DEFAULT_DQ_POLICY_PATH,
    DEFAULT_POLICY_PATH,
    ConsumerMigrationPolicy,
    load_consumer_migration_policy,
    materialize_isolated_candidate,
    validate_candidate_copy_manifest,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    run_canonical_data_quality_execution,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_path,
    write_json_atomic,
)

O1_DQ_GATE_SCHEMA_VERSION: Final = "o1_relative_opportunity_dq_gate.v1"
O1_AUDIT_POLICY_SCHEMA_VERSION: Final = "o1_relative_opportunity_capability_audit_policy.v1"
O1_AUDIT_POLICY_ID: Final = "TRADING_2464_O1_CAPABILITY_AUDIT_V1"
O1_TASK_ID: Final = "TRADING-2464"
DEFAULT_O1_AUDIT_POLICY_PATH: Final = Path(
    "config/research/o1_relative_opportunity_capability_audit_v1.yaml"
)
_EXPECTED_INPUT_ROLES: Final = ("prices", "rates", "secondary_prices")
_MEMBER_PATHS: Final = {
    "prices": "data/raw/prices_daily.csv",
    "rates": "data/raw/rates_daily.csv",
    "secondary_prices": "data/raw/prices_marketstack_daily.csv",
}
_GATE_FIELDS: Final = frozenset(
    {
        "schema_version",
        "gate_id",
        "task_id",
        "status",
        "generated_at",
        "audit_policy",
        "source_workspace",
        "candidate_workspace",
        "historical_acceptance",
        "publication",
        "copy_manifest",
        "candidate_objects",
        "fresh_data_quality",
        "claim_boundary",
    }
)


class O1RelativeOpportunityDqError(RuntimeError):
    def __init__(self, code: str, message: str, *, path: Path | None = None) -> None:
        self.code = code
        self.message = message
        self.path = path
        location = "" if path is None else f" [{path}]"
        super().__init__(f"{code}{location}: {message}")


@dataclass(frozen=True)
class O1DqCandidateResult:
    gate: Mapping[str, object]
    gate_path: Path
    candidate_project_root: Path
    receipt_path: Path


def materialize_and_validate_o1_candidate(
    *,
    source_project_root: Path,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    audit_policy_path: Path = DEFAULT_O1_AUDIT_POLICY_PATH,
    migration_policy_path: Path = DEFAULT_POLICY_PATH,
    acl_policy_path: Path = DEFAULT_ACL_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DQ_POLICY_PATH,
) -> O1DqCandidateResult:
    """Materialize one historical candidate and stop immediately after strict DQ."""

    timestamp = _aware_utc(generated_at)
    root = project_root.resolve(strict=True)
    audit_path = _contained_file(root, audit_policy_path, "O1_DQ_POLICY_MISSING")
    audit = _load_mapping(audit_path, "O1_DQ_POLICY_INVALID")
    _validate_audit_policy(audit)

    recovery = _mapping(_mapping(audit["data_contract"], "data_contract")["recovery"], "recovery")
    expected_source = Path(_text(recovery["source_workspace_path"], "source_workspace_path"))
    expected_source = expected_source.resolve(strict=True)
    source_root = source_project_root.resolve(strict=True)
    if source_root != expected_source:
        _fail(
            "O1_DQ_SOURCE_WORKSPACE_MISMATCH",
            f"expected={expected_source.as_posix()} actual={source_root.as_posix()}",
            path=source_root,
        )

    migration = load_consumer_migration_policy(
        migration_policy_path,
        project_root=root,
    )
    _validate_cross_policy_binding(audit, migration)
    _validate_capability_evidence(migration, root)
    acl_policy = load_acl_policy(_contained_file(root, acl_policy_path, "O1_DQ_ACL_MISSING"))

    source_before = _source_inventory(source_root, migration)
    materialized = materialize_isolated_candidate(
        source_project_root=source_root,
        output_root=output_root,
        project_root=root,
        policy=migration,
        acl_policy=acl_policy,
        generated_at=timestamp,
    )
    candidate = materialized.candidate_project_root.resolve(strict=True)
    candidate_objects = _verify_candidate_objects(
        candidate,
        _mapping(
            _mapping(
                _mapping(audit["data_contract"], "data_contract")["publication"],
                "publication",
            )["immutable_members"],
            "immutable_members",
        ),
    )
    source_after_materialization = _source_inventory(source_root, migration)
    _require_inventory_unchanged(source_before, source_after_materialization)

    universe = load_universe(candidate / "config/universe.yaml")
    data_contract = _mapping(audit["data_contract"], "data_contract")
    start = _iso_date(data_contract["primary_research_start"], "primary_research_start")
    requested_end = _iso_date(data_contract["requested_end"], "requested_end")
    as_of = migration.historical.as_of
    if requested_end != as_of:
        _fail(
            "O1_DQ_WINDOW_MISMATCH",
            f"audit requested_end={requested_end.isoformat()} migration as_of={as_of.isoformat()}",
        )
    request = CanonicalDataQualityExecutionRequest(
        as_of=as_of,
        requested_window=DataQualityDateWindow(start, requested_end),
        prices_path=candidate / _MEMBER_PATHS["prices"],
        rates_path=candidate / _MEMBER_PATHS["rates"],
        manifest_path=candidate / "data/raw/download_manifest.csv",
        expected_price_tickers=tuple(configured_price_tickers(universe)),
        expected_rate_series=tuple(configured_rate_series(universe)),
        execution_profile_id=migration.execution_profile_id,
        secondary_prices_path=candidate / _MEMBER_PATHS["secondary_prices"],
        require_secondary_prices=True,
        policy_path=data_quality_policy_path,
    )
    execution = run_canonical_data_quality_execution(request, project_root=candidate)
    preflight = verify_data_quality_execution_receipt(
        execution.receipt_path,
        expected_as_of=as_of,
        expected_policy_path=data_quality_policy_path,
        expected_input_roles=migration.required_input_roles,
        project_root=candidate,
    )
    expected_status = _text(data_contract["canonical_dq_strict_status"], "dq status")
    expected_errors = _integer(data_contract["canonical_dq_error_count"], "dq errors")
    expected_warnings = _integer(data_contract["canonical_dq_warning_count"], "dq warnings")
    if (
        preflight.status != expected_status
        or preflight.status not in migration.accepted_data_quality_statuses
        or execution.report.error_count != expected_errors
        or execution.report.warning_count != expected_warnings
    ):
        _fail(
            "O1_DQ_NOT_STRICT_PASS",
            (
                f"status={preflight.status} errors={execution.report.error_count} "
                f"warnings={execution.report.warning_count}"
            ),
        )
    evaluated_end = _iso_date(data_contract["evaluated_end"], "evaluated_end")
    if execution.receipt.evaluated_window.end != evaluated_end:
        _fail(
            "O1_DQ_EVALUATED_WINDOW_MISMATCH",
            (
                f"expected={evaluated_end.isoformat()} "
                f"actual={execution.receipt.evaluated_window.end.isoformat()}"
            ),
        )

    source_after_dq = _source_inventory(source_root, migration)
    _require_inventory_unchanged(source_before, source_after_dq)
    gate = _build_gate(
        audit=audit,
        audit_path=audit_path,
        source_root=source_root,
        source_inventory=source_after_dq,
        candidate=candidate,
        output_root=output_root.resolve(strict=True),
        recovery_mode="FRESH_MATERIALIZATION",
        copy_manifest=materialized.copy_manifest,
        copy_manifest_path=materialized.copy_manifest_path,
        publication=materialized.publication,
        historical_receipt_id=materialized.historical_receipt.receipt_id,
        historical_authorization_id=materialized.historical_authorization.authorization_id,
        candidate_objects=candidate_objects,
        receipt=execution.receipt,
        receipt_path=execution.receipt_path,
        receipt_sha256=preflight.receipt_sha256,
        report_error_count=execution.report.error_count,
        report_warning_count=execution.report.warning_count,
        generated_at=timestamp,
        project_root=root,
    )
    validate_o1_dq_gate(gate)
    gate_path = output_root.resolve(strict=True) / "o1_dq_gate.json"
    write_json_atomic(gate_path, gate)
    if sha256_path(gate_path) != hashlib.sha256(gate_path.read_bytes()).hexdigest():
        _fail("O1_DQ_GATE_WRITE_INVALID", "gate checksum verification failed", path=gate_path)
    return O1DqCandidateResult(
        gate=gate,
        gate_path=gate_path,
        candidate_project_root=candidate,
        receipt_path=execution.receipt_path,
    )


def resume_existing_o1_candidate(
    *,
    source_project_root: Path,
    output_root: Path,
    project_root: Path,
    generated_at: datetime,
    audit_policy_path: Path = DEFAULT_O1_AUDIT_POLICY_PATH,
    migration_policy_path: Path = DEFAULT_POLICY_PATH,
    data_quality_policy_path: Path = DEFAULT_DQ_POLICY_PATH,
) -> O1DqCandidateResult:
    """Verify an interrupted candidate and write only its missing O1 gate summary."""

    timestamp = _aware_utc(generated_at)
    root = project_root.resolve(strict=True)
    audit_path = _contained_file(root, audit_policy_path, "O1_DQ_POLICY_MISSING")
    audit = _load_mapping(audit_path, "O1_DQ_POLICY_INVALID")
    _validate_audit_policy(audit)
    recovery = _mapping(_mapping(audit["data_contract"], "data_contract")["recovery"], "recovery")
    expected_source = Path(_text(recovery["source_workspace_path"], "source_workspace_path"))
    expected_source = expected_source.resolve(strict=True)
    source_root = source_project_root.resolve(strict=True)
    if source_root != expected_source:
        _fail(
            "O1_DQ_SOURCE_WORKSPACE_MISMATCH",
            f"expected={expected_source.as_posix()} actual={source_root.as_posix()}",
            path=source_root,
        )
    migration = load_consumer_migration_policy(
        migration_policy_path,
        project_root=root,
    )
    _validate_cross_policy_binding(audit, migration)
    _validate_capability_evidence(migration, root)
    output = _existing_output_root(output_root, root, migration)
    gate_path = output / "o1_dq_gate.json"
    if gate_path.exists():
        _fail("O1_DQ_GATE_ALREADY_EXISTS", "resume requires a missing gate", path=gate_path)
    candidate = (output / "candidate_project").resolve(strict=True)
    if not candidate.is_dir():
        _fail("O1_DQ_CANDIDATE_MISSING", "candidate_project is not a directory", path=candidate)

    source_before = _source_inventory(source_root, migration)
    copy_paths = sorted(
        (candidate / "outputs/data_foundation_consumer_migration").glob(
            "consumer_copy_*/copy_manifest.json"
        )
    )
    if len(copy_paths) != 1:
        _fail("O1_DQ_COPY_MANIFEST_SET_INVALID", f"count={len(copy_paths)}")
    copy_path = _contained_file(candidate, copy_paths[0], "O1_DQ_COPY_MANIFEST_MISSING")
    copy_manifest = _load_json_mapping(copy_path, "O1_DQ_COPY_MANIFEST_INVALID")
    validate_candidate_copy_manifest(
        copy_manifest,
        candidate_project_root=candidate,
    )
    if (
        copy_manifest.get("source_project_root") != source_root.as_posix()
        or copy_manifest.get("historical_receipt_id") != migration.historical.receipt_id
        or copy_manifest.get("historical_authorization_id") != migration.historical.authorization_id
    ):
        _fail("O1_DQ_COPY_MANIFEST_BINDING_MISMATCH", "source or historical identity mismatch")

    publication = resolve_download_publication(
        output_dir=candidate / migration.candidate_publication_dir
    )
    selected_publication = _mapping(
        copy_manifest.get("selected_publication"),
        "selected_publication",
    )
    publication_projection = {
        "transaction_id": publication.transaction_id,
        "transaction_sha256": publication.transaction_manifest_sha256,
        "discovery_pointer_sha256": publication.discovery_pointer_sha256,
        "requested_start": publication.requested_start.isoformat(),
        "requested_end": publication.requested_end.isoformat(),
        "artifact_sha256": dict(sorted(publication.artifact_sha256.items())),
        "manifest_sha256": publication.manifest_sha256,
        "manifest_row_count": publication.manifest_row_count,
        "legacy_projection_verified": publication.legacy_projection_verified,
        "consumer_cutover_allowed": publication.consumer_cutover_allowed,
        "production_effect": publication.production_effect,
    }
    if any(
        selected_publication.get(field) != value for field, value in publication_projection.items()
    ):
        _fail("O1_DQ_PUBLICATION_BINDING_MISMATCH", publication.transaction_id)
    candidate_objects = _verify_candidate_objects(
        candidate,
        _mapping(
            _mapping(
                _mapping(audit["data_contract"], "data_contract")["publication"],
                "publication",
            )["immutable_members"],
            "immutable_members",
        ),
    )
    receipt_paths = sorted(
        (candidate / "outputs/data_quality/executions").glob("dq_execution_*/receipt.json")
    )
    if len(receipt_paths) != 1:
        _fail("O1_DQ_RECEIPT_SET_INVALID", f"count={len(receipt_paths)}")
    receipt_path = _contained_file(candidate, receipt_paths[0], "O1_DQ_RECEIPT_MISSING")
    preflight = verify_data_quality_execution_receipt(
        receipt_path,
        expected_as_of=migration.historical.as_of,
        expected_policy_path=data_quality_policy_path,
        expected_input_roles=migration.required_input_roles,
        project_root=candidate,
    )
    receipt = preflight.receipt
    data_contract = _mapping(audit["data_contract"], "data_contract")
    expected_start = _iso_date(data_contract["primary_research_start"], "primary_research_start")
    expected_requested_end = _iso_date(data_contract["requested_end"], "requested_end")
    expected_evaluated_end = _iso_date(data_contract["evaluated_end"], "evaluated_end")
    if (
        preflight.status != data_contract["canonical_dq_strict_status"]
        or receipt.report.error_count != data_contract["canonical_dq_error_count"]
        or receipt.report.warning_count != data_contract["canonical_dq_warning_count"]
        or receipt.requested_window.start != expected_start
        or receipt.requested_window.end != expected_requested_end
        or receipt.evaluated_window.end != expected_evaluated_end
    ):
        _fail(
            "O1_DQ_EXISTING_RECEIPT_NOT_STRICT_PASS",
            (
                f"status={preflight.status} errors={receipt.report.error_count} "
                f"warnings={receipt.report.warning_count} "
                f"requested={receipt.requested_window.start}/{receipt.requested_window.end} "
                f"evaluated_end={receipt.evaluated_window.end}"
            ),
        )
    source_after = _source_inventory(source_root, migration)
    _require_inventory_unchanged(source_before, source_after)
    gate = _build_gate(
        audit=audit,
        audit_path=audit_path,
        source_root=source_root,
        source_inventory=source_after,
        candidate=candidate,
        output_root=output,
        recovery_mode="VERIFIED_EXISTING_CANDIDATE_AFTER_SUMMARY_INTERRUPTION",
        copy_manifest=copy_manifest,
        copy_manifest_path=copy_path,
        publication=publication,
        historical_receipt_id=migration.historical.receipt_id,
        historical_authorization_id=migration.historical.authorization_id,
        candidate_objects=candidate_objects,
        receipt=receipt,
        receipt_path=receipt_path,
        receipt_sha256=preflight.receipt_sha256,
        report_error_count=receipt.report.error_count,
        report_warning_count=receipt.report.warning_count,
        generated_at=timestamp,
        project_root=root,
    )
    validate_o1_dq_gate(gate)
    write_json_atomic(gate_path, gate)
    return O1DqCandidateResult(
        gate=gate,
        gate_path=gate_path,
        candidate_project_root=candidate,
        receipt_path=receipt_path,
    )


def validate_o1_dq_gate(payload: Mapping[str, object]) -> None:
    _exact_fields(payload, _GATE_FIELDS, "gate")
    if payload["schema_version"] != O1_DQ_GATE_SCHEMA_VERSION:
        _fail("O1_DQ_GATE_SCHEMA_INVALID", str(payload["schema_version"]))
    if payload["task_id"] != O1_TASK_ID or payload["status"] != "PASS":
        _fail("O1_DQ_GATE_STATUS_INVALID", "task/status mismatch")
    boundary = _mapping(payload["claim_boundary"], "claim_boundary")
    _exact_fields(
        boundary,
        frozenset(
            {
                "source_workspace_mutated",
                "daily_consumer_dispatched",
                "coverage_audit_executed",
                "model_training_executed",
                "new_o1_result_read",
                "production_effect",
                "broker_action",
            }
        ),
        "claim_boundary",
    )
    expected_boundary = {
        "source_workspace_mutated": False,
        "daily_consumer_dispatched": False,
        "coverage_audit_executed": False,
        "model_training_executed": False,
        "new_o1_result_read": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    if dict(boundary) != expected_boundary:
        _fail("O1_DQ_GATE_SCOPE_VIOLATION", str(dict(boundary)))
    dq = _mapping(payload["fresh_data_quality"], "fresh_data_quality")
    if dq.get("status") != "PASS" or dq.get("error_count") != 0 or dq.get("warning_count") != 0:
        _fail("O1_DQ_NOT_STRICT_PASS", str(dict(dq)))
    body = dict(payload)
    supplied_id = _text(body.pop("gate_id"), "gate_id")
    expected_id = f"o1_dq_gate_{_digest(body)[:32]}"
    if supplied_id != expected_id:
        _fail("O1_DQ_GATE_ID_MISMATCH", f"expected={expected_id} actual={supplied_id}")


def _build_gate(
    *,
    audit: Mapping[str, object],
    audit_path: Path,
    source_root: Path,
    source_inventory: Mapping[str, object],
    candidate: Path,
    output_root: Path,
    recovery_mode: str,
    copy_manifest: Mapping[str, object],
    copy_manifest_path: Path,
    publication: object,
    historical_receipt_id: str,
    historical_authorization_id: str,
    candidate_objects: Mapping[str, object],
    receipt: object,
    receipt_path: Path,
    receipt_sha256: str,
    report_error_count: int,
    report_warning_count: int,
    generated_at: datetime,
    project_root: Path,
) -> dict[str, object]:
    data_contract = _mapping(audit["data_contract"], "data_contract")
    copy_path = copy_manifest_path.resolve(strict=True)
    receipt_path = receipt_path.resolve(strict=True)
    body: dict[str, object] = {
        "schema_version": O1_DQ_GATE_SCHEMA_VERSION,
        "task_id": O1_TASK_ID,
        "status": "PASS",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "audit_policy": {
            "policy_id": audit["policy_id"],
            "version": audit["version"],
            "path": audit_path.relative_to(project_root).as_posix(),
            "sha256": sha256_path(audit_path),
            "owner_decision": audit["owner_decision"],
        },
        "source_workspace": {
            "workspace_id": _mapping(data_contract["recovery"], "recovery")["source_workspace_id"],
            "path": source_root.as_posix(),
            "inventory_sha256": source_inventory["sha256"],
            "inventory_file_count": source_inventory["file_count"],
            "inventory_size_bytes": source_inventory["size_bytes"],
            "mutation_allowed": False,
            "mutation_observed": False,
        },
        "candidate_workspace": {
            "output_root": output_root.as_posix(),
            "project_root": candidate.as_posix(),
            "retained_for_same_store_coverage": True,
            "recovery_mode": recovery_mode,
        },
        "historical_acceptance": {
            "receipt_id": historical_receipt_id,
            "authorization_id": historical_authorization_id,
        },
        "publication": {
            "transaction_id": publication.transaction_id,
            "transaction_sha256": publication.transaction_manifest_sha256,
            "discovery_pointer_sha256": publication.discovery_pointer_sha256,
            "requested_start": publication.requested_start.isoformat(),
            "requested_end": publication.requested_end.isoformat(),
            "legacy_projection_verified": publication.legacy_projection_verified,
            "consumer_cutover_allowed": publication.consumer_cutover_allowed,
            "production_effect": publication.production_effect,
        },
        "copy_manifest": {
            "copy_manifest_id": copy_manifest["copy_manifest_id"],
            "path": copy_path.relative_to(output_root).as_posix(),
            "sha256": sha256_path(copy_path),
            "all_objects_checksum_verified": copy_manifest["all_objects_checksum_verified"],
        },
        "candidate_objects": candidate_objects,
        "fresh_data_quality": {
            "receipt_id": receipt.receipt_id,
            "receipt_path": receipt_path.relative_to(candidate).as_posix(),
            "receipt_sha256": receipt_sha256,
            "as_of": receipt.as_of.isoformat(),
            "requested_start": receipt.requested_window.start.isoformat(),
            "requested_end": receipt.requested_window.end.isoformat(),
            "evaluated_start": receipt.evaluated_window.start.isoformat(),
            "evaluated_end": receipt.evaluated_window.end.isoformat(),
            "status": receipt.report.status,
            "error_count": report_error_count,
            "warning_count": report_warning_count,
            "execution_profile_id": _invocation_value(
                receipt.invocation,
                "execution_profile_id",
            ),
            "provenance_verified": receipt.dq_execution_provenance_verified,
        },
        "claim_boundary": {
            "source_workspace_mutated": False,
            "daily_consumer_dispatched": False,
            "coverage_audit_executed": False,
            "model_training_executed": False,
            "new_o1_result_read": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }
    return {"gate_id": f"o1_dq_gate_{_digest(body)[:32]}", **body}


def _validate_audit_policy(audit: Mapping[str, object]) -> None:
    if audit.get("schema_version") != O1_AUDIT_POLICY_SCHEMA_VERSION:
        _fail("O1_DQ_POLICY_INVALID", "schema_version mismatch")
    if audit.get("policy_id") != O1_AUDIT_POLICY_ID:
        _fail("O1_DQ_POLICY_INVALID", "policy_id mismatch")
    if audit.get("status") != "OWNER_APPROVED_SERIAL_CONTRACT_FROZEN_DATA_GATES_PENDING":
        _fail("O1_DQ_POLICY_NOT_ACTIVE", str(audit.get("status")))
    execution = _mapping(audit.get("execution_binding"), "execution_binding")
    safety = _mapping(audit.get("safety"), "safety")
    recovery = _mapping(
        _mapping(audit.get("data_contract"), "data_contract").get("recovery"),
        "recovery",
    )
    required_false = {
        "real_coverage_read_allowed_now": execution.get("real_coverage_read_allowed_now"),
        "model_training_allowed_now": execution.get("model_training_allowed_now"),
        "coverage_audit_executed": safety.get("coverage_audit_executed"),
        "model_training_executed": safety.get("model_training_executed"),
        "new_o1_result_read": safety.get("new_o1_result_read"),
        "source_workspace_mutation_allowed": recovery.get("source_workspace_mutation_allowed"),
        "overwrite_live_data_raw_allowed": recovery.get("overwrite_live_data_raw_allowed"),
    }
    if any(value is not False for value in required_false.values()):
        _fail("O1_DQ_POLICY_SCOPE_VIOLATION", str(required_false))
    required_true = {
        "isolated_candidate_required": recovery.get("isolated_candidate_required"),
        "exact_object_reverification_required": recovery.get(
            "exact_object_reverification_required"
        ),
        "fresh_candidate_strict_dq_required": recovery.get("fresh_candidate_strict_dq_required"),
    }
    if any(value is not True for value in required_true.values()):
        _fail("O1_DQ_POLICY_SCOPE_VIOLATION", str(required_true))


def _validate_cross_policy_binding(
    audit: Mapping[str, object],
    migration: ConsumerMigrationPolicy,
) -> None:
    data_contract = _mapping(audit["data_contract"], "data_contract")
    historical = _mapping(data_contract["historical_receipt"], "historical_receipt")
    publication = _mapping(data_contract["publication"], "publication")
    expected = {
        "receipt_id": (historical["receipt_id"], migration.historical.receipt_id),
        "receipt_sha256": (historical["receipt_sha256"], migration.historical.receipt_sha256),
        "authorization_id": (
            historical["authorization_id"],
            migration.historical.authorization_id,
        ),
        "authorization_sha256": (
            historical["authorization_sha256"],
            migration.historical.authorization_sha256,
        ),
        "transaction_id": (
            publication["transaction_id"],
            migration.historical.publication_transaction_id,
        ),
        "transaction_sha256": (
            publication["transaction_sha256"],
            migration.historical.publication_transaction_sha256,
        ),
        "discovery_pointer_sha256": (
            publication["discovery_pointer_sha256"],
            migration.historical.publication_discovery_pointer_sha256,
        ),
    }
    mismatches = [name for name, (left, right) in expected.items() if left != right]
    if mismatches:
        _fail("O1_DQ_CROSS_POLICY_MISMATCH", ",".join(mismatches))
    if tuple(sorted(migration.required_input_roles)) != tuple(sorted(_EXPECTED_INPUT_ROLES)):
        _fail(
            "O1_DQ_INPUT_ROLE_MISMATCH",
            f"actual={sorted(migration.required_input_roles)}",
        )


def _validate_capability_evidence(policy: ConsumerMigrationPolicy, project_root: Path) -> None:
    bindings = (
        (policy.capabilities.d0c_bundle_path, policy.capabilities.d0c_bundle_sha256),
        (policy.capabilities.d0d_bundle_path, policy.capabilities.d0d_bundle_sha256),
    )
    for relative, expected_sha in bindings:
        path = _contained_file(project_root, Path(relative), "O1_DQ_CAPABILITY_MISSING")
        if sha256_path(path) != expected_sha:
            _fail("O1_DQ_CAPABILITY_TAMPERED", relative, path=path)


def _verify_candidate_objects(
    candidate: Path,
    expected_members: Mapping[str, object],
) -> dict[str, object]:
    if set(expected_members) != set(_MEMBER_PATHS):
        _fail("O1_DQ_MEMBER_SET_MISMATCH", str(sorted(expected_members)))
    observed: dict[str, object] = {}
    for role, relative in _MEMBER_PATHS.items():
        binding = _mapping(expected_members[role], f"immutable_members.{role}")
        expected_sha = _sha(binding["sha256"], f"{role}.sha256")
        expected_size = _integer(binding["size_bytes"], f"{role}.size_bytes")
        path = _contained_file(candidate, Path(relative), "O1_DQ_MEMBER_MISSING")
        actual_sha = sha256_path(path)
        actual_size = path.stat().st_size
        if actual_sha != expected_sha or actual_size != expected_size:
            _fail(
                "O1_DQ_MEMBER_TAMPERED",
                (
                    f"role={role} expected_sha={expected_sha} actual_sha={actual_sha} "
                    f"expected_size={expected_size} actual_size={actual_size}"
                ),
                path=path,
            )
        observed[role] = {
            "path": relative,
            "sha256": actual_sha,
            "size_bytes": actual_size,
            "verified": True,
        }
    return observed


def _source_inventory(
    source_root: Path,
    migration: ConsumerMigrationPolicy,
) -> dict[str, object]:
    roots = [
        source_root / migration.source_publication_dir / migration.publication_store_dir,
        source_root / migration.historical.receipt_path,
        source_root / migration.historical.authorization_path,
    ]
    records: list[dict[str, object]] = []
    for inventory_root in roots:
        if not inventory_root.exists():
            _fail("O1_DQ_SOURCE_EVIDENCE_MISSING", "inventory path missing", path=inventory_root)
        candidates = (
            [inventory_root] if inventory_root.is_file() else _regular_files(inventory_root)
        )
        for path in candidates:
            resolved = path.resolve(strict=True)
            try:
                relative = resolved.relative_to(source_root).as_posix()
            except ValueError:
                _fail("O1_DQ_SOURCE_ESCAPE", "source evidence escaped root", path=resolved)
            records.append(
                {
                    "path": relative,
                    "sha256": sha256_path(resolved),
                    "size_bytes": resolved.stat().st_size,
                }
            )
    records.sort(key=lambda item: str(item["path"]))
    raw = canonical_json_bytes(records, indent=None, trailing_newline=False)
    return {
        "sha256": hashlib.sha256(raw).hexdigest(),
        "file_count": len(records),
        "size_bytes": sum(_integer(item["size_bytes"], "size_bytes") for item in records),
    }


def _regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for directory, names, filenames in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in names:
            if (base / name).is_symlink():
                _fail("O1_DQ_SOURCE_SYMLINK_FORBIDDEN", name, path=base / name)
        for filename in filenames:
            path = base / filename
            if path.is_symlink() or not path.is_file():
                _fail("O1_DQ_SOURCE_SYMLINK_FORBIDDEN", filename, path=path)
            files.append(path)
    return files


def _require_inventory_unchanged(
    before: Mapping[str, object],
    after: Mapping[str, object],
) -> None:
    if dict(before) != dict(after):
        _fail(
            "O1_DQ_SOURCE_MUTATION_DETECTED",
            f"before={dict(before)} after={dict(after)}",
        )


def _invocation_value(invocation: object, name: str) -> object:
    for item in invocation:
        if item.name == name:
            try:
                return json.loads(item.value_json)
            except (AttributeError, TypeError, json.JSONDecodeError) as exc:
                raise O1RelativeOpportunityDqError(
                    "O1_DQ_RECEIPT_INVALID",
                    f"invalid invocation parameter={name}",
                ) from exc
    _fail("O1_DQ_RECEIPT_INVALID", f"missing invocation parameter={name}")


def _load_mapping(path: Path, code: str) -> Mapping[str, object]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        _fail(code, str(exc), path=path)
    return _mapping(payload, "document")


def _load_json_mapping(path: Path, code: str) -> Mapping[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        _fail(code, str(exc), path=path)
    mapping = _mapping(payload, "document")
    if raw != canonical_json_bytes(mapping):
        _fail(code, "JSON bytes are not canonical", path=path)
    return mapping


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _fail("O1_DQ_FIELDS_INVALID", f"{field} must be a mapping")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail("O1_DQ_FIELDS_INVALID", f"{field} must be non-empty text")
    return value


def _integer(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        _fail("O1_DQ_FIELDS_INVALID", f"{field} must be a non-negative integer")
    return value


def _sha(value: object, field: str) -> str:
    text = _text(value, field)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        _fail("O1_DQ_FIELDS_INVALID", f"{field} must be lowercase SHA-256")
    return text


def _iso_date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(_text(value, field))
    except ValueError as exc:
        raise O1RelativeOpportunityDqError(
            "O1_DQ_FIELDS_INVALID",
            f"{field} must be ISO date",
        ) from exc


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail("O1_DQ_TIMESTAMP_INVALID", "generated_at must be timezone-aware")
    return value.astimezone(UTC)


def _contained_file(root: Path, path: Path, code: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError:
        _fail("O1_DQ_PATH_ESCAPE", str(path), path=resolved)
    if not resolved.is_file():
        _fail(code, "expected regular file", path=resolved)
    return resolved


def _existing_output_root(
    output_root: Path,
    project_root: Path,
    migration: ConsumerMigrationPolicy,
) -> Path:
    output = output_root.resolve(strict=True)
    allowed = (project_root / migration.allowed_output_parent).resolve(strict=True)
    try:
        relative = output.relative_to(allowed)
    except ValueError:
        _fail("O1_DQ_OUTPUT_ROOT_INVALID", "output escaped allowed parent", path=output)
    if not relative.parts or not output.is_dir():
        _fail("O1_DQ_OUTPUT_ROOT_INVALID", "output must be a child directory", path=output)
    return output


def _exact_fields(
    payload: Mapping[str, object],
    expected: frozenset[str],
    field: str,
) -> None:
    actual = set(payload)
    if actual != set(expected):
        missing = sorted(set(expected) - actual)
        extra = sorted(actual - set(expected))
        _fail(
            "O1_DQ_FIELDS_INVALID",
            f"{field} missing={missing} extra={extra}",
        )


def _digest(payload: object) -> str:
    raw = canonical_json_bytes(payload, sort_keys=True, indent=None, trailing_newline=False)
    return hashlib.sha256(raw).hexdigest()


def _fail(code: str, message: str, *, path: Path | None = None) -> NoReturn:
    raise O1RelativeOpportunityDqError(code, message, path=path)
