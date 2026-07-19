from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ai_trading_system.platform.architecture import (
    CHANGE_MANIFEST_SCHEMA_VERSION,
    VALIDATION_EVIDENCE_SCHEMA_VERSION,
    ParallelControlError,
    build_deterministic_lane_plan,
    detect_change_conflicts,
    parse_change_manifest,
    parse_validation_evidence,
    validate_evidence_binding,
)

BASE_COMMIT = "f" * 40


def _manifest_payload(
    change_id: str,
    *,
    lane_role: str = "DOMAIN",
    base_commit: str = BASE_COMMIT,
    owned_paths: list[str] | None = None,
    shared_paths: list[str] | None = None,
    module_ids: list[str] | None = None,
    contract_claims: list[dict[str, str]] | None = None,
    required_validation_tiers: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": CHANGE_MANIFEST_SCHEMA_VERSION,
        "change_id": change_id,
        "task_id": f"task:{change_id}",
        "lane_role": lane_role,
        "base_commit": base_commit,
        "owner": "architecture_owner",
        "production_effect": "none",
        "owned_paths": owned_paths if owned_paths is not None else [f"src/{change_id}.py"],
        "shared_paths": shared_paths or [],
        "module_ids": module_ids if module_ids is not None else [f"module.{change_id}"],
        "contract_claims": contract_claims or [],
        "required_validation_tiers": required_validation_tiers
        if required_validation_tiers is not None
        else ["architecture-fitness", "contract-validation"],
    }


def _evidence_payload(
    *,
    manifest_sha256: str,
    artifact_path: str,
    artifact_sha256: str,
    tier: str,
    status: str = "PASS",
    change_id: str = "change-a",
    base_commit: str = BASE_COMMIT,
) -> dict[str, str]:
    return {
        "schema_version": VALIDATION_EVIDENCE_SCHEMA_VERSION,
        "evidence_id": f"evidence:{tier}",
        "change_id": change_id,
        "tier": tier,
        "status": status,
        "artifact_path": artifact_path,
        "artifact_sha256": artifact_sha256,
        "base_commit": base_commit,
        "change_manifest_sha256": manifest_sha256,
        "production_effect": "none",
    }


def test_change_manifest_is_canonical_and_input_order_independent() -> None:
    first = parse_change_manifest(
        _manifest_payload(
            "change-a",
            owned_paths=["src/z.py", "src/a.py"],
            module_ids=["module.z", "module.a"],
            contract_claims=[
                {"contract_id": "contract.z", "version": "v1", "access": "READ"},
                {"contract_id": "contract.a", "version": "v2", "access": "WRITE"},
            ],
            required_validation_tiers=["contract-validation", "architecture-fitness"],
        )
    )
    second = parse_change_manifest(
        _manifest_payload(
            "change-a",
            owned_paths=["src/a.py", "src/z.py"],
            module_ids=["module.a", "module.z"],
            contract_claims=[
                {"contract_id": "contract.a", "version": "v2", "access": "WRITE"},
                {"contract_id": "contract.z", "version": "v1", "access": "READ"},
            ],
            required_validation_tiers=["architecture-fitness", "contract-validation"],
        )
    )

    assert first == second
    assert first.to_dict() == second.to_dict()
    assert first.sha256 == second.sha256
    assert first.owned_paths == ("src/a.py", "src/z.py")


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ({"schema_version": "change_manifest.v2"}, "CHANGE_MANIFEST_SCHEMA"),
        ({"base_commit": "f" * 39}, "INVALID_COMMIT"),
        ({"owned_paths": ["../outside.py"]}, "UNSAFE_PATH"),
        ({"owned_paths": ["C:/outside.py"]}, "UNSAFE_PATH"),
        ({"owned_paths": ["src\\bad.py"]}, "NON_CANONICAL_PATH"),
        ({"production_effect": "strategy_change"}, "UNSAFE_PRODUCTION_EFFECT"),
        ({"required_validation_tiers": []}, "MISSING_VALIDATION_REQUIREMENTS"),
    ],
)
def test_change_manifest_rejects_unsafe_or_ambiguous_inputs(
    mutation: dict[str, object], code: str
) -> None:
    payload = _manifest_payload("change-a")
    payload.update(mutation)

    with pytest.raises(ParallelControlError) as error:
        parse_change_manifest(payload)

    assert error.value.code == code


def test_change_manifest_rejects_intra_scope_and_contract_duplicates() -> None:
    overlap = _manifest_payload(
        "change-a",
        owned_paths=["src/a.py"],
        shared_paths=["src/a.py"],
    )
    with pytest.raises(ParallelControlError, match="INTRA_MANIFEST_PATH_OVERLAP"):
        parse_change_manifest(overlap)

    duplicate_contract = _manifest_payload(
        "change-a",
        contract_claims=[
            {"contract_id": "contract.a", "version": "v1", "access": "READ"},
            {"contract_id": "contract.a", "version": "v1", "access": "WRITE"},
        ],
    )
    with pytest.raises(ParallelControlError, match="DUPLICATE_CONTRACT_CLAIM"):
        parse_change_manifest(duplicate_contract)


def test_conflicts_cover_paths_modules_and_contract_access_or_version() -> None:
    first = parse_change_manifest(
        _manifest_payload(
            "change-a",
            owned_paths=["src/shared.py", "src/a.py"],
            module_ids=["module.shared"],
            contract_claims=[
                {"contract_id": "contract.shared", "version": "v1", "access": "WRITE"},
                {"contract_id": "contract.versioned", "version": "v1", "access": "READ"},
            ],
        )
    )
    second = parse_change_manifest(
        _manifest_payload(
            "change-b",
            owned_paths=["src/shared.py", "src/b.py"],
            module_ids=["module.shared"],
            contract_claims=[
                {"contract_id": "contract.shared", "version": "v1", "access": "READ"},
                {"contract_id": "contract.versioned", "version": "v2", "access": "READ"},
            ],
        )
    )

    conflicts = detect_change_conflicts([second, first])

    assert {(item.code, item.resource, item.serializable) for item in conflicts} == {
        ("OWNED_PATH_OVERLAP", "src/shared.py", True),
        ("MODULE_CONFLICT", "module.shared", True),
        ("CONTRACT_ACCESS_CONFLICT", "contract.shared", True),
        ("CONTRACT_VERSION_CONFLICT", "contract.versioned", False),
    }


def test_lane_plan_is_deterministic_and_serializes_conflicting_domain_changes() -> None:
    change_a = parse_change_manifest(
        _manifest_payload("change-a", module_ids=["module.shared"])
    )
    change_b = parse_change_manifest(_manifest_payload("change-b"))
    change_c = parse_change_manifest(
        _manifest_payload("change-c", module_ids=["module.shared"])
    )
    coordinator = parse_change_manifest(
        _manifest_payload(
            "change-coordinator",
            lane_role="COORDINATOR",
            owned_paths=[],
            shared_paths=["docs/task_register.md"],
            module_ids=[],
        )
    )
    kwargs = {
        "current_base_commit": BASE_COMMIT,
        "coordinator_only_paths": ["docs/task_register.md"],
        "max_parallel_domain_lanes": 2,
    }

    first = build_deterministic_lane_plan(
        [change_c, coordinator, change_b, change_a],
        **kwargs,
    )
    second = build_deterministic_lane_plan(
        [change_a, change_b, change_c, coordinator],
        **kwargs,
    )

    assert first.to_dict() == second.to_dict()
    assert first.status == "PASS"
    assert [wave["kind"] for wave in first.waves] == ["DOMAIN", "DOMAIN", "COORDINATOR"]
    assert [row["change_id"] for row in first.waves[0]["assignments"]] == [
        "change-a",
        "change-b",
    ]
    assert [row["change_id"] for row in first.waves[1]["assignments"]] == ["change-c"]
    assert first.waves[2]["assignments"][0]["lane_id"] == "integration-coordinator"
    assert first.to_dict()["dispatch_allowed"] is False
    assert first.to_dict()["lease_acquisition_allowed"] is False


def test_lane_plan_blocks_base_drift_and_coordinator_only_path_violation() -> None:
    stale = parse_change_manifest(
        _manifest_payload("change-stale", base_commit="e" * 40)
    )
    unauthorized = parse_change_manifest(
        _manifest_payload("change-unsafe", owned_paths=["docs/task_register.md"])
    )

    plan = build_deterministic_lane_plan(
        [unauthorized, stale],
        current_base_commit=BASE_COMMIT,
        coordinator_only_paths=["docs/task_register.md"],
        max_parallel_domain_lanes=2,
    )

    assert plan.status == "BLOCKED"
    assert plan.waves == ()
    assert {issue.code for issue in plan.blocking_issues} == {
        "BASE_DRIFT",
        "COORDINATOR_ONLY_PATH_VIOLATION",
    }


def test_lane_plan_rejects_empty_input_and_implicit_capacity() -> None:
    plan = build_deterministic_lane_plan(
        [],
        current_base_commit=BASE_COMMIT,
        coordinator_only_paths=[],
        max_parallel_domain_lanes=2,
    )
    assert plan.status == "BLOCKED"
    assert plan.blocking_issues[0].code == "EMPTY_LANE_PLAN"

    with pytest.raises(ParallelControlError, match="LANE_CAPACITY"):
        build_deterministic_lane_plan(
            [],
            current_base_commit=BASE_COMMIT,
            coordinator_only_paths=[],
            max_parallel_domain_lanes=0,
        )


def test_lane_plan_blocks_unserializable_contract_version_conflict() -> None:
    first = parse_change_manifest(
        _manifest_payload(
            "change-a",
            contract_claims=[
                {"contract_id": "contract.shared", "version": "v1", "access": "READ"}
            ],
        )
    )
    second = parse_change_manifest(
        _manifest_payload(
            "change-b",
            contract_claims=[
                {"contract_id": "contract.shared", "version": "v2", "access": "READ"}
            ],
        )
    )

    plan = build_deterministic_lane_plan(
        [first, second],
        current_base_commit=BASE_COMMIT,
        coordinator_only_paths=[],
        max_parallel_domain_lanes=2,
    )

    assert plan.status == "BLOCKED"
    assert plan.blocking_issues[0].code == "CONTRACT_VERSION_CONFLICT"


def test_validation_evidence_binds_required_tiers_and_real_artifact_bytes(
    tmp_path: Path,
) -> None:
    manifest = parse_change_manifest(_manifest_payload("change-a"))
    evidence = []
    for tier in manifest.required_validation_tiers:
        artifact = tmp_path / f"{tier}.json"
        artifact.write_bytes(f'{{"tier":"{tier}","status":"PASS"}}'.encode())
        evidence.append(
            parse_validation_evidence(
                _evidence_payload(
                    manifest_sha256=manifest.sha256,
                    artifact_path=artifact.name,
                    artifact_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    tier=tier,
                )
            )
        )

    result = validate_evidence_binding(manifest, evidence, project_root=tmp_path)

    assert result.status == "PASS"
    assert result.bound_tiers == manifest.required_validation_tiers
    assert result.issues == ()
    assert result.to_dict()["task_registry_mutated"] is False
    assert result.to_dict()["production_effect"] == "none"


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        ("status", "FAIL", "VALIDATION_STATUS"),
        ("change_id", "change-b", "CHANGE_ID_BINDING"),
        ("base_commit", "e" * 40, "BASE_COMMIT_BINDING"),
        ("change_manifest_sha256", "0" * 64, "CHANGE_MANIFEST_SHA256_BINDING"),
        ("artifact_sha256", "0" * 64, "ARTIFACT_SHA256"),
    ],
)
def test_validation_evidence_fails_closed_on_binding_or_artifact_drift(
    tmp_path: Path,
    field: str,
    value: str,
    expected_code: str,
) -> None:
    manifest = parse_change_manifest(
        _manifest_payload("change-a", required_validation_tiers=["architecture-fitness"])
    )
    artifact = tmp_path / "summary.json"
    artifact.write_text('{"status":"PASS"}', encoding="utf-8")
    payload = _evidence_payload(
        manifest_sha256=manifest.sha256,
        artifact_path=artifact.name,
        artifact_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
        tier="architecture-fitness",
    )
    payload[field] = value
    record = parse_validation_evidence(payload)

    result = validate_evidence_binding(manifest, [record], project_root=tmp_path)

    assert result.status == "FAIL"
    assert expected_code in {issue.code for issue in result.issues}
    assert result.bound_tiers == ()


def test_validation_evidence_rejects_missing_undeclared_and_outside_artifacts(
    tmp_path: Path,
) -> None:
    manifest = parse_change_manifest(
        _manifest_payload("change-a", required_validation_tiers=["architecture-fitness"])
    )
    extra = parse_validation_evidence(
        _evidence_payload(
            manifest_sha256=manifest.sha256,
            artifact_path="missing-summary.json",
            artifact_sha256="0" * 64,
            tier="contract-validation",
        )
    )

    result = validate_evidence_binding(manifest, [extra], project_root=tmp_path)

    assert result.status == "FAIL"
    assert {issue.code for issue in result.issues} == {
        "ARTIFACT_MISSING",
        "MISSING_REQUIRED_VALIDATION_TIER",
        "UNDECLARED_VALIDATION_TIER",
    }

    unsafe = _evidence_payload(
        manifest_sha256=manifest.sha256,
        artifact_path="../outside-summary.json",
        artifact_sha256="0" * 64,
        tier="architecture-fitness",
    )
    with pytest.raises(ParallelControlError, match="UNSAFE_PATH"):
        parse_validation_evidence(unsafe)
