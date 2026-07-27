from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

import ai_trading_system.ops_release_promotion as promotion
from ai_trading_system.ops_release_promotion import (
    OpsReleasePromotionError,
    activate_ops_deployment,
    build_ops_deployment_acceptance,
    build_ops_release_candidate,
    inspect_runtime_provenance,
    install_ops_runtime_git_exclusions,
    load_ops_release_promotion_policy,
    promote_ops_release,
    validate_ops_deployment_acceptance,
    validate_ops_release_candidate,
    validate_scheduler_observation,
)
from ai_trading_system.platform.artifacts import sha256_path, write_json_atomic

EXPECTED_REMOTE = "git@github.com:AI-Trading-Collaboration/AITradingSystem.git"
OBSERVED_AT = datetime(2026, 7, 27, 2, 0, tzinfo=UTC)
REQUIRED_VALIDATION_TIERS = (
    "fast-unit",
    "architecture-fitness",
    "contract-validation",
    "integration",
    "reproducibility",
    "full",
)
REQUIRED_CRITICAL_PATHS = (
    "config/architecture/arch_005_parallel_control_policy.yaml",
    "config/architecture/arch_005_s4d_checkout_guard.yaml",
    "config/operations/ops_release_promotion.yaml",
    "config/operations/ops_scheduler_checkout.yaml",
    "docs/operations/operations_runbook.md",
    "pyproject.toml",
    "src/ai_trading_system/cli_commands/ops.py",
    "src/ai_trading_system/ops_release_promotion.py",
    "src/ai_trading_system/ops_scheduler_checkout.py",
)
ReleaseRepository = tuple[Path, str, tuple[Path, ...], tuple[Path, ...]]


@pytest.fixture
def release_repository(tmp_path: Path) -> ReleaseRepository:
    root = tmp_path / "development"
    commit = _init_release_repo(root)
    validations = _write_validation_artifacts(root, commit)
    critical = tuple(root / relative for relative in REQUIRED_CRITICAL_PATHS)
    return root, commit, validations, critical


def test_policy_fails_closed_on_latest_and_scheduler_duplication() -> None:
    policy = load_ops_release_promotion_policy()

    assert policy.automatic_latest_selection is False
    assert policy.automatic_stash_clean_reset is False
    assert policy.previous_release_retained is True
    assert policy.independent_git_common_dir_required is True
    assert policy.required_validation_tiers == REQUIRED_VALIDATION_TIERS
    assert policy.required_critical_paths == REQUIRED_CRITICAL_PATHS
    assert policy.installed_distribution_inventory_required is True
    assert policy.git_exclude_managed is True
    assert policy.git_exclude_patterns == (
        "/outputs/",
        "/artifacts/",
        "/data/derived/",
    )
    assert policy.pre_switch_checkout_policy_source == "coordinator_candidate"
    assert policy.scheduler_entry_count == 1
    assert policy.windows_task_scheduler_entries_allowed is False


def test_release_candidate_binds_remote_validation_and_critical_hashes(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository

    payload = build_ops_release_candidate(
        project_root=root,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )

    validate_ops_release_candidate(
        payload,
        verify_live_artifacts=True,
        artifact_root=root,
    )
    assert payload["candidate_commit"] == commit
    assert payload["remote"]["reviewed_ref_commit"] == commit
    assert payload["validation_artifacts"][0]["validation_status"] == "PASS"
    assert payload["validation_artifacts"][0]["validation_git_commit"] == commit
    assert {
        row["validation_tier"] for row in payload["validation_artifacts"]
    } == set(REQUIRED_VALIDATION_TIERS)
    assert all(
        "absolute_path" not in row
        for row in (
            *payload["validation_artifacts"],
            *payload["critical_path_commitments"],
        )
    )
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] is False


def test_release_candidate_rejects_failed_validation(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository
    write_json_atomic(validations[0], {"status": "FAIL"})

    with pytest.raises(OpsReleasePromotionError, match="RELEASE_VALIDATION_NOT_PASS"):
        build_ops_release_candidate(
            project_root=root,
            candidate_commit=commit,
            validation_artifact_paths=validations,
            critical_path_commitments=critical,
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
            observed_at=OBSERVED_AT,
        )


def test_release_candidate_rejects_validation_from_another_commit(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository
    write_json_atomic(
        validations[0],
        {
            "status": "PASS",
            "git_commit": "0" * 40,
            "tier": REQUIRED_VALIDATION_TIERS[0],
            "production_effect": "none",
        },
    )

    with pytest.raises(
        OpsReleasePromotionError,
        match="RELEASE_VALIDATION_COMMIT_MISMATCH",
    ):
        build_ops_release_candidate(
            project_root=root,
            candidate_commit=commit,
            validation_artifact_paths=validations,
            critical_path_commitments=critical,
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
            observed_at=OBSERVED_AT,
        )


def test_release_candidate_rejects_incomplete_or_duplicate_validation_tiers(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository

    with pytest.raises(
        OpsReleasePromotionError,
        match="RELEASE_VALIDATION_TIER_SET_MISMATCH",
    ):
        build_ops_release_candidate(
            project_root=root,
            candidate_commit=commit,
            validation_artifact_paths=validations[:-1],
            critical_path_commitments=critical,
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
            observed_at=OBSERVED_AT,
        )

    with pytest.raises(
        OpsReleasePromotionError,
        match="RELEASE_VALIDATION_TIER_DUPLICATE",
    ):
        build_ops_release_candidate(
            project_root=root,
            candidate_commit=commit,
            validation_artifact_paths=(*validations[:-1], validations[0]),
            critical_path_commitments=critical,
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
            observed_at=OBSERVED_AT,
        )


def test_release_candidate_rejects_incomplete_critical_path_set(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository

    with pytest.raises(
        OpsReleasePromotionError,
        match="RELEASE_CRITICAL_PATH_SET_MISMATCH",
    ):
        build_ops_release_candidate(
            project_root=root,
            candidate_commit=commit,
            validation_artifact_paths=validations,
            critical_path_commitments=critical[:-1],
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
            observed_at=OBSERVED_AT,
        )


def test_release_candidate_rejects_receipt_tamper(
    release_repository: ReleaseRepository,
) -> None:
    root, commit, validations, critical = release_repository
    payload = build_ops_release_candidate(
        project_root=root,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )
    payload["candidate_commit"] = "0" * 40

    with pytest.raises(OpsReleasePromotionError):
        validate_ops_release_candidate(payload)


def test_runtime_provenance_rejects_linked_worktree(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, _validation, _critical = release_repository
    runtime = tmp_path / "linked-runtime"
    _git(development, "worktree", "add", "--detach", str(runtime), commit)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: _probe_payload(runtime, runtime_python),
    )

    with pytest.raises(OpsReleasePromotionError, match="RUNTIME_GIT_COMMON_DIR_SHARED"):
        inspect_runtime_provenance(
            runtime_root=runtime,
            development_root=development,
            runtime_python=runtime_python,
            candidate_commit=commit,
        )


def test_runtime_provenance_rejects_global_editable_import(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, _validation, _critical = release_repository
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: {
            "executable": str(runtime_python),
            "module_file": str(development / "src" / "ai_trading_system" / "__init__.py"),
            "project_root": str(development),
            "installed_distributions": [
                {"name": "ai-trading-system", "version": "0.1.0"}
            ],
        },
    )

    with pytest.raises(OpsReleasePromotionError, match="RUNTIME_PACKAGE_OUTSIDE_CHECKOUT"):
        inspect_runtime_provenance(
            runtime_root=runtime,
            development_root=development,
            runtime_python=runtime_python,
            candidate_commit=commit,
        )


def test_runtime_git_exclusion_install_is_exact_and_rejects_unknown_rules(
    release_repository: ReleaseRepository,
    tmp_path: Path,
) -> None:
    development, commit, _validation, _critical = release_repository
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)

    repeated = install_ops_runtime_git_exclusions(
        runtime_root=runtime,
        development_root=development,
        policy_path=development / "config" / "operations" / "ops_release_promotion.yaml",
        observed_at=OBSERVED_AT,
    )

    assert repeated["status"] == "PASS"
    assert repeated["action"] == "REUSED_EXACT"
    assert repeated["git_exclude"]["patterns"] == [
        "/outputs/",
        "/artifacts/",
        "/data/derived/",
    ]
    exclude_path = Path(repeated["git_exclude"]["absolute_path"])
    exclude_path.write_text("/unknown/\n", encoding="utf-8")
    with pytest.raises(
        OpsReleasePromotionError,
        match="RUNTIME_GIT_EXCLUDE_EXISTING_RULES",
    ):
        install_ops_runtime_git_exclusions(
            runtime_root=runtime,
            development_root=development,
            policy_path=development
            / "config"
            / "operations"
            / "ops_release_promotion.yaml",
            observed_at=OBSERVED_AT,
        )


def test_deployment_acceptance_binds_unique_scheduler_and_credentials(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, validations, critical = release_repository
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)
    _copy_validation_artifacts(development, runtime, validations)
    candidate_path = (
        runtime
        / "outputs"
        / "operations"
        / "deployment"
        / "evidence"
        / "candidate.json"
    )
    write_json_atomic(candidate_path, candidate)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: _probe_payload(runtime, runtime_python),
    )
    scheduler = _scheduler_observation(runtime, runtime_python)
    promotion_event = _promotion_event(runtime, candidate, candidate_path)

    payload = build_ops_deployment_acceptance(
        release_candidate=candidate,
        release_candidate_path=candidate_path,
        runtime_root=runtime,
        development_root=development,
        runtime_python=runtime_python,
        scheduler_observation=scheduler,
        promotion_event_path=promotion_event,
        credential_names=["FMP_API_KEY", "SEC_USER_AGENT"],
        credential_attestation_ref="owner_attestation:OPS-070:credentials:minimal-v1",
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:accept-deployment",
        observed_at=OBSERVED_AT,
    )

    validate_ops_deployment_acceptance(payload)
    assert payload["status"] == "ACTIVE_OWNER_ACCEPTED"
    assert payload["runtime"]["git_common_dir"] != payload["runtime"][
        "development_git_common_dir"
    ]
    assert payload["runtime"]["installed_distributions"] == [
        {"name": "ai-trading-system", "version": "0.1.0"},
        {"name": "PyYAML", "version": "6.0.2"},
    ]
    assert payload["runtime"]["git_exclude"]["patterns"] == [
        "/outputs/",
        "/artifacts/",
        "/data/derived/",
    ]
    assert len(payload["runtime"]["environment_fingerprint"]) == 64
    environment_fingerprint = payload["runtime"]["environment_fingerprint"]
    payload["runtime"]["environment_fingerprint"] = "0" * 64
    with pytest.raises(
        OpsReleasePromotionError,
        match="DEPLOYMENT_ENVIRONMENT_FINGERPRINT_MISMATCH",
    ):
        validate_ops_deployment_acceptance(payload)
    payload["runtime"]["environment_fingerprint"] = environment_fingerprint
    assert payload["scheduler"]["entry_count"] == 1
    assert payload["credentials"]["secret_values_recorded"] is False
    assert payload["production_effect"] == "none"
    active_path = activate_ops_deployment(payload, runtime_root=runtime)
    repeated_path = activate_ops_deployment(payload, runtime_root=runtime)
    assert active_path == repeated_path
    assert active_path.is_file()


def test_deployment_acceptance_rejects_forbidden_credential_name(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, validations, critical = release_repository
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)
    _copy_validation_artifacts(development, runtime, validations)
    candidate_path = (
        runtime
        / "outputs"
        / "operations"
        / "deployment"
        / "evidence"
        / "candidate.json"
    )
    write_json_atomic(candidate_path, candidate)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: _probe_payload(runtime, runtime_python),
    )
    promotion_event = _promotion_event(runtime, candidate, candidate_path)

    with pytest.raises(
        OpsReleasePromotionError,
        match="DEPLOYMENT_CREDENTIAL_SCOPE_UNKNOWN",
    ):
        build_ops_deployment_acceptance(
            release_candidate=candidate,
            release_candidate_path=candidate_path,
            runtime_root=runtime,
            development_root=development,
            runtime_python=runtime_python,
            scheduler_observation=_scheduler_observation(runtime, runtime_python),
            promotion_event_path=promotion_event,
            credential_names=["IBKR_PASSWORD"],
            credential_attestation_ref="owner_attestation:OPS-070:credentials:minimal-v1",
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:accept-deployment",
            observed_at=OBSERVED_AT,
        )


def test_deployment_acceptance_requires_provider_and_sec_identity(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, validations, critical = release_repository
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)
    _copy_validation_artifacts(development, runtime, validations)
    candidate_path = runtime / "outputs" / "operations" / "deployment" / "candidate.json"
    write_json_atomic(candidate_path, candidate)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: _probe_payload(runtime, runtime_python),
    )
    promotion_event = _promotion_event(runtime, candidate, candidate_path)

    with pytest.raises(
        OpsReleasePromotionError,
        match="DEPLOYMENT_REQUIRED_CREDENTIAL_MISSING",
    ):
        build_ops_deployment_acceptance(
            release_candidate=candidate,
            release_candidate_path=candidate_path,
            runtime_root=runtime,
            development_root=development,
            runtime_python=runtime_python,
            scheduler_observation=_scheduler_observation(runtime, runtime_python),
            promotion_event_path=promotion_event,
            credential_names=["FMP_API_KEY"],
            credential_attestation_ref="owner_attestation:OPS-070:credentials:minimal-v1",
            owner_decision_ref="owner_decision:OPS-070:2026-07-27:accept-deployment",
            observed_at=OBSERVED_AT,
        )


def test_scheduler_observation_rejects_duplicate_or_wrong_entry(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    payload = _scheduler_observation(runtime, runtime_python)
    payload["entry_count"] = 2

    with pytest.raises(OpsReleasePromotionError, match="SCHEDULER_OBSERVATION_MISMATCH"):
        validate_scheduler_observation(
            payload,
            runtime_root=runtime,
            runtime_python=runtime_python,
        )


def test_promotion_rejects_missing_runtime_without_creating_it(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "missing-runtime"

    with pytest.raises(
        OpsReleasePromotionError,
        match="PROMOTION_RUNTIME_ROOT_MISSING",
    ):
        promote_ops_release(
            coordinator_root=tmp_path,
            runtime_root=runtime,
            development_root=tmp_path,
            release_candidate_path=tmp_path / "missing-candidate.json",
            runtime_python=runtime / ".venv" / "Scripts" / "python.exe",
            observed_at=OBSERVED_AT,
        )

    assert not runtime.exists()


def test_promotion_switches_exact_commit_and_writes_append_only_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development = tmp_path / "development"
    _init_release_repo(development)
    checkout_policy = (
        development
        / "config"
        / "architecture"
        / "arch_005_s4d_checkout_guard.yaml"
    )
    checkout_policy.write_text(
        "schema_version: arch_005_s4d_checkout_guard_policy.v1\n",
        encoding="utf-8",
    )
    _git(development, "add", checkout_policy.relative_to(development).as_posix())
    _git(development, "commit", "-m", "old runtime checkout policy")
    previous_commit = _git(development, "rev-parse", "HEAD")
    repository_root = Path(__file__).resolve().parents[1]
    checkout_policy.write_text(
        (
            repository_root
            / "config"
            / "architecture"
            / "arch_005_s4d_checkout_guard.yaml"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (development / "tracked.txt").write_text("release\n", encoding="utf-8")
    _git(
        development,
        "add",
        "tracked.txt",
        checkout_policy.relative_to(development).as_posix(),
    )
    _git(development, "commit", "-m", "release")
    candidate_commit = _git(development, "rev-parse", "HEAD")
    _git(development, "update-ref", "refs/remotes/origin/main", candidate_commit)
    validations = _write_validation_artifacts(development, candidate_commit)
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=candidate_commit,
        validation_artifact_paths=validations,
        critical_path_commitments=tuple(
            development / relative for relative in REQUIRED_CRITICAL_PATHS
        ),
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        previous_release_commit=previous_commit,
        observed_at=OBSERVED_AT,
    )
    candidate_path = tmp_path / "candidate.json"
    write_json_atomic(candidate_path, candidate)
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, previous_commit)
    _git(runtime, "update-ref", "refs/remotes/origin/main", candidate_commit)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(promotion, "_active_checkout_leases", lambda **_: ())
    monkeypatch.setattr(
        promotion,
        "_runtime_probe",
        lambda **_: _probe_payload(runtime, runtime_python),
    )
    original_git_run = promotion._git_run

    def git_run_without_network(root: Path, *args: str) -> None:
        if args[:3] == ("fetch", "--no-tags", "origin"):
            return
        original_git_run(root, *args)

    monkeypatch.setattr(promotion, "_git_run", git_run_without_network)

    payload, event_path = promote_ops_release(
        coordinator_root=development,
        runtime_root=runtime,
        development_root=development,
        release_candidate_path=candidate_path,
        runtime_python=runtime_python,
        observed_at=OBSERVED_AT,
    )

    assert payload["state"] == "PROMOTED_NOT_ACTIVATED"
    assert _git(runtime, "rev-parse", "HEAD") == candidate_commit
    for source in validations:
        copied = runtime / source.relative_to(development)
        assert copied.read_bytes() == source.read_bytes()
    canonical_candidate = Path(
        payload["release_candidate_receipt"]["absolute_path"]
    )
    assert canonical_candidate.is_relative_to(runtime)
    assert canonical_candidate.is_file()
    event_names = sorted(path.name for path in event_path.parent.glob("*.json"))
    assert event_names == [
        "01_PREPARED.json",
        "02_SWITCHED.json",
        "03_PROMOTED_NOT_ACTIVATED.json",
    ]
    assert not (
        runtime / "outputs" / "operations" / "deployment" / "promotion.lock"
    ).exists()


def test_coordinator_policy_still_blocks_dirty_old_runtime(
    tmp_path: Path,
) -> None:
    development = tmp_path / "development"
    _init_release_repo(development)
    checkout_policy = (
        development
        / "config"
        / "architecture"
        / "arch_005_s4d_checkout_guard.yaml"
    )
    checkout_policy.write_text(
        "schema_version: arch_005_s4d_checkout_guard_policy.v1\n",
        encoding="utf-8",
    )
    _git(development, "add", checkout_policy.relative_to(development).as_posix())
    _git(development, "commit", "-m", "old runtime checkout policy")
    previous_commit = _git(development, "rev-parse", "HEAD")
    repository_root = Path(__file__).resolve().parents[1]
    checkout_policy.write_text(
        (
            repository_root
            / "config"
            / "architecture"
            / "arch_005_s4d_checkout_guard.yaml"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _git(development, "add", checkout_policy.relative_to(development).as_posix())
    _git(development, "commit", "-m", "candidate checkout policy")
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, previous_commit)
    (runtime / "unexpected.txt").write_text("dirty\n", encoding="utf-8")

    assert promotion._governed_dirty_paths(
        runtime,
        policy_source_root=development,
    ) == ("unexpected.txt",)


def test_promotion_failure_rolls_back_previous_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development = tmp_path / "development"
    previous_commit = _init_release_repo(development)
    (development / "tracked.txt").write_text("release\n", encoding="utf-8")
    _git(development, "add", "tracked.txt")
    _git(development, "commit", "-m", "release")
    candidate_commit = _git(development, "rev-parse", "HEAD")
    _git(development, "update-ref", "refs/remotes/origin/main", candidate_commit)
    validations = _write_validation_artifacts(development, candidate_commit)
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=candidate_commit,
        validation_artifact_paths=validations,
        critical_path_commitments=tuple(
            development / relative for relative in REQUIRED_CRITICAL_PATHS
        ),
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        previous_release_commit=previous_commit,
        observed_at=OBSERVED_AT,
    )
    candidate_path = tmp_path / "candidate.json"
    write_json_atomic(candidate_path, candidate)
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, previous_commit)
    _git(runtime, "update-ref", "refs/remotes/origin/main", candidate_commit)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(promotion, "_active_checkout_leases", lambda **_: ())
    original_git_run = promotion._git_run

    def git_run_without_network(root: Path, *args: str) -> None:
        if args[:3] == ("fetch", "--no-tags", "origin"):
            return
        original_git_run(root, *args)

    monkeypatch.setattr(promotion, "_git_run", git_run_without_network)
    monkeypatch.setattr(
        promotion,
        "inspect_runtime_provenance",
        lambda **_: (_ for _ in ()).throw(
            OpsReleasePromotionError("FAULT_INJECTION", "post-switch")
        ),
    )

    with pytest.raises(OpsReleasePromotionError, match="FAULT_INJECTION"):
        promote_ops_release(
            coordinator_root=development,
            runtime_root=runtime,
            development_root=development,
            release_candidate_path=candidate_path,
            runtime_python=runtime_python,
            observed_at=OBSERVED_AT,
        )

    assert _git(runtime, "rev-parse", "HEAD") == previous_commit
    transaction_root = runtime / "outputs" / "operations" / "deployment" / "transactions"
    assert len(list(transaction_root.rglob("04_ROLLED_BACK.json"))) == 1


def test_promotion_blocks_active_daily_lease_before_switch(
    release_repository: ReleaseRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    development, commit, validations, critical = release_repository
    candidate = build_ops_release_candidate(
        project_root=development,
        candidate_commit=commit,
        validation_artifact_paths=validations,
        critical_path_commitments=critical,
        owner_decision_ref="owner_decision:OPS-070:2026-07-27:approve-release",
        observed_at=OBSERVED_AT,
    )
    candidate_path = tmp_path / "candidate.json"
    write_json_atomic(candidate_path, candidate)
    runtime = tmp_path / "runtime"
    _clone_independent(development, runtime, commit)
    runtime_python = runtime / ".venv" / "Scripts" / "python.exe"
    runtime_python.parent.mkdir(parents=True)
    runtime_python.write_bytes(b"runtime")
    monkeypatch.setattr(
        promotion,
        "_active_checkout_leases",
        lambda **_: ("daily-active-lease",),
    )

    with pytest.raises(OpsReleasePromotionError, match="PROMOTION_ACTIVE_DAILY_LEASE"):
        promote_ops_release(
            coordinator_root=development,
            runtime_root=runtime,
            development_root=development,
            release_candidate_path=candidate_path,
            runtime_python=runtime_python,
            observed_at=OBSERVED_AT,
        )

    assert _git(runtime, "rev-parse", "HEAD") == commit


def _scheduler_observation(runtime: Path, runtime_python: Path) -> dict[str, object]:
    return {
        "schema_version": "ops_scheduler_observation.v1",
        "provider": "codex_automation",
        "scheduler_id": "aitradingsystem-pit",
        "entry_count": 1,
        "enabled": True,
        "windows_task_scheduler_entry_count": 0,
        "unified_external_trigger": ["aits", "ops", "daily-run"],
        "working_directory": str(runtime),
        "runtime_python": str(runtime_python),
        "environment_names": [
            "AITS_EXTERNAL_SCHEDULER",
            "AITS_OPS_CHECKOUT_ROOT",
            "AITS_OPS_RELEASE_COMMIT",
            "AITS_DEVELOPMENT_CHECKOUT_ROOT",
            "AITS_OPS_DEPLOYMENT_RECEIPT",
            "AITS_OPS_PYTHON",
        ],
        "observed_at": OBSERVED_AT.isoformat(),
    }


def _promotion_event(
    runtime: Path,
    candidate: dict[str, object],
    candidate_path: Path,
) -> Path:
    path = (
        runtime
        / "outputs"
        / "operations"
        / "deployment"
        / "transactions"
        / "ops_promotion_fixture"
        / "03_PROMOTED_NOT_ACTIVATED.json"
    )
    write_json_atomic(
        path,
        {
            "schema_version": "ops_release_promotion_transaction.v1",
            "transaction_id": "ops_promotion_fixture",
            "sequence": 3,
            "state": "PROMOTED_NOT_ACTIVATED",
            "release_id": candidate["release_id"],
            "candidate_commit": candidate["candidate_commit"],
            "release_candidate_receipt": {
                "path": candidate_path.name,
                "absolute_path": str(candidate_path.resolve()),
                "size_bytes": candidate_path.stat().st_size,
                "sha256": sha256_path(candidate_path),
            },
            "production_effect": "none",
        },
    )
    return path


def _probe_payload(runtime: Path, runtime_python: Path) -> dict[str, object]:
    return {
        "executable": str(runtime_python),
        "module_file": str(runtime / "src" / "ai_trading_system" / "__init__.py"),
        "project_root": str(runtime),
        "installed_distributions": [
            {"name": "ai-trading-system", "version": "0.1.0"},
            {"name": "PyYAML", "version": "6.0.2"},
        ],
    }


def _write_validation_artifacts(root: Path, commit: str) -> tuple[Path, ...]:
    output = root / "outputs" / "validation_runtime"
    paths: list[Path] = []
    for tier in REQUIRED_VALIDATION_TIERS:
        path = output / f"{tier}.json"
        write_json_atomic(
            path,
            {
                "schema_version": "validation_fixture.v1",
                "status": "PASS",
                "git_commit": commit,
                "tier": tier,
                "production_effect": "none",
            },
        )
        paths.append(path)
    return tuple(paths)


def _copy_validation_artifacts(
    source_root: Path,
    target_root: Path,
    paths: tuple[Path, ...],
) -> None:
    for source in paths:
        target = target_root / source.relative_to(source_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def _init_release_repo(root: Path) -> str:
    root.mkdir()
    repository_root = Path(__file__).resolve().parents[1]
    for relative in REQUIRED_CRITICAL_PATHS:
        source = repository_root / relative
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    package = root / "src" / "ai_trading_system"
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (root / ".gitignore").write_text("outputs/\n.venv/\n", encoding="utf-8")
    (root / "tracked.txt").write_text("base\n", encoding="utf-8")
    _git(root, "init")
    _git(root, "config", "user.email", "ops-release@example.com")
    _git(root, "config", "user.name", "Ops Release")
    _git(root, "remote", "add", "origin", EXPECTED_REMOTE)
    _git(root, "add", ".")
    _git(root, "commit", "-m", "base")
    commit = _git(root, "rev-parse", "HEAD")
    _git(root, "update-ref", "refs/remotes/origin/main", commit)
    return commit


def _clone_independent(source: Path, target: Path, commit: str) -> None:
    subprocess.run(
        ("git", "clone", "--no-hardlinks", str(source), str(target)),
        check=True,
        text=True,
        capture_output=True,
    )
    _git(target, "remote", "set-url", "origin", EXPECTED_REMOTE)
    _git(target, "checkout", "--detach", commit)
    _git(target, "update-ref", "refs/remotes/origin/main", _git(source, "rev-parse", "HEAD"))
    install_ops_runtime_git_exclusions(
        runtime_root=target,
        development_root=source,
        policy_path=source / "config" / "operations" / "ops_release_promotion.yaml",
        observed_at=OBSERVED_AT,
    )


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(root), *args),
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()
