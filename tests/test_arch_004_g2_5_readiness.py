from __future__ import annotations

import base64
import copy
import hashlib
import json
import subprocess
from contextlib import nullcontext
from pathlib import Path

import pytest

import ai_trading_system.platform.architecture.arch_004_g2_5_readiness as readiness_module
from ai_trading_system.platform.architecture.arch_004_g2_5_readiness import (
    CANONICAL_MERGE_ORDER,
    DEFAULT_POLICY_PATH,
    G25ReadinessError,
    build_fragment_preview,
    build_g2_5_readiness_evidence,
    load_g2_5_readiness_policy,
    load_source_validation_bundle,
    policy_fragments,
    validate_g2_5_readiness_evidence,
    write_g2_5_readiness_evidence,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _commit_ref(ref: str) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--verify", ref],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        shell=False,
    )
    return result.stdout.strip()


def _is_ancestor(ancestor: str, descendant: str) -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        shell=False,
    )
    return result.returncode == 0


def _report_checksum(payload: dict[str, object]) -> str:
    body = {key: value for key, value in payload.items() if key != "report_checksum"}
    encoded = json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


BASE_COMMIT = _commit_ref("HEAD")


@pytest.fixture(scope="module")
def policy():
    return load_g2_5_readiness_policy(PROJECT_ROOT / DEFAULT_POLICY_PATH)


@pytest.fixture(scope="module")
def evidence() -> dict[str, object]:
    return build_g2_5_readiness_evidence(
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )


def test_policy_freezes_three_domains_two_workers_and_coordinator_only_shared_paths(
    policy,
) -> None:
    assert policy.max_active_domain_workers == 2
    assert policy.merge_order == CANONICAL_MERGE_ORDER
    assert [item["domain_id"] for item in policy.domains] == [
        "G4_OPERATIONS",
        "G3_REPORTING",
        "G5_RESEARCH_WRAPPER",
    ]
    assert [item["batch"] for item in policy.domains] == [1, 1, 2]
    assert all(
        set(item["shared_paths_requested"]).issubset(policy.coordinator["coordinator_only_paths"])
        for item in policy.domains
    )
    assert policy.safety["source_of_truth"] == "LEGACY_MARKDOWN_ONLY"
    assert policy.safety["dispatch_allowed"] is False
    assert policy.safety["lease_acquisition_allowed"] is False


def test_policy_binds_complete_tracked_validation_bundle(policy) -> None:
    bundle_path = PROJECT_ROOT / policy.source_validation_bundle_path
    assert bundle_path.is_file()
    assert hashlib.sha256(bundle_path.read_bytes()).hexdigest() == (
        policy.source_validation_bundle_sha256
    )
    handoff_path = PROJECT_ROOT / policy.source_handoff_path
    handoff = readiness_module._mapping(
        readiness_module._load_unique_yaml_path(handoff_path, "handoff"), "handoff"
    )
    frozen = load_source_validation_bundle(
        path=bundle_path,
        expected_sha256=policy.source_validation_bundle_sha256,
        handoff=handoff,
        handoff_path=handoff_path,
        project_root=PROJECT_ROOT,
    )
    assert len(frozen) == 4
    assert all(content.count(b"\r\n") > 0 for content in frozen.values())


def test_live_ownership_snapshot_accepts_completed_canonical_transitions(
    evidence: dict[str, object],
) -> None:
    tracked = json.loads(
        (PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json").read_text(
            encoding="utf-8"
        )
    )
    migrated_paths = {
        "src/ai_trading_system/platform/reporting/audit_index.py",
        "src/ai_trading_system/platform/reporting/owner_daily.py",
        "src/ai_trading_system/platform/reporting/research_review.py",
    }

    def source_rows(payload: dict[str, object]) -> dict[str, dict[str, object]]:
        return {
            row["path"]: row
            for domain in payload["ownership_snapshot"]["domains"]
            for row in domain["source_inventory"]
        }

    historical = source_rows(tracked)
    current = source_rows(evidence)
    assert all(historical[path]["current_owner_profile"] == "platform" for path in migrated_paths)
    assert all(historical[path]["ownership_transition_required"] is True for path in migrated_paths)
    assert all(current[path]["current_owner_profile"] == "reporting" for path in migrated_paths)
    assert all(current[path]["ownership_transition_required"] is False for path in migrated_paths)


@pytest.mark.parametrize(
    ("mode", "code"),
    [
        ("unknown", "SOURCE_OWNER_UNKNOWN"),
        ("third_owner", "SOURCE_OWNER_DRIFT"),
    ],
)
def test_ownership_snapshot_rejects_unknown_or_third_owner(
    monkeypatch: pytest.MonkeyPatch,
    policy,
    mode: str,
    code: str,
) -> None:
    original = readiness_module.build_module_manifest

    def drifted_manifest(**kwargs):
        payload = copy.deepcopy(original(**kwargs))
        target = "src/ai_trading_system/platform/reporting/audit_index.py"
        if mode == "unknown":
            payload["modules"] = [row for row in payload["modules"] if row["path"] != target]
        else:
            next(row for row in payload["modules"] if row["path"] == target)[
                "owner_profile"
            ] = "operations"
        return payload

    monkeypatch.setattr(readiness_module, "build_module_manifest", drifted_manifest)
    with pytest.raises(G25ReadinessError, match=code):
        readiness_module.build_ownership_snapshot(
            project_root=PROJECT_ROOT,
            policy=policy,
            current_base_commit=BASE_COMMIT,
        )


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ("missing", "VALIDATION_BUNDLE_TIER_SET"),
        ("extra", "VALIDATION_BUNDLE_UNKNOWN_TIER"),
        ("tamper", "VALIDATION_BUNDLE_CONTENT_HASH_DRIFT"),
        ("file_hash", "VALIDATION_BUNDLE_FILE_HASH_DRIFT"),
    ],
)
def test_validation_bundle_rejects_missing_extra_tamper_or_file_hash_drift(
    tmp_path: Path,
    policy,
    mutation: str,
    code: str,
) -> None:
    source = PROJECT_ROOT / policy.source_validation_bundle_path
    payload = json.loads(source.read_text(encoding="utf-8"))
    if mutation == "missing":
        payload["artifacts"].pop()
        payload["artifact_count"] = len(payload["artifacts"])
    elif mutation == "extra":
        extra = copy.deepcopy(payload["artifacts"][0])
        extra["tier"] = "unexpected"
        extra["original_path"] = "outputs/validation_runtime/unexpected.json"
        payload["artifacts"].append(extra)
        payload["artifact_count"] = len(payload["artifacts"])
    elif mutation == "tamper":
        row = payload["artifacts"][0]
        content = base64.b64decode(row["content_base64"], validate=True) + b"\n"
        row["content_base64"] = base64.b64encode(content).decode("ascii")
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    expected_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    if mutation == "file_hash":
        expected_sha = policy.source_validation_bundle_sha256
    handoff_path = PROJECT_ROOT / policy.source_handoff_path
    handoff = readiness_module._mapping(
        readiness_module._load_unique_yaml_path(handoff_path, "handoff"), "handoff"
    )

    with pytest.raises(G25ReadinessError, match=code):
        load_source_validation_bundle(
            path=path,
            expected_sha256=expected_sha,
            handoff=handoff,
            handoff_path=handoff_path,
            project_root=PROJECT_ROOT,
        )


@pytest.mark.parametrize("label", ["policy", "handoff", "current_matrix"])
def test_bounded_yaml_loader_rejects_duplicate_keys(tmp_path: Path, label: str) -> None:
    path = tmp_path / f"{label}.yaml"
    path.write_text("root:\n  status: PASS\n  status: FAIL\n", encoding="utf-8")

    with pytest.raises(G25ReadinessError, match="YAML_DUPLICATE_KEY"):
        readiness_module._load_unique_yaml_path(path, label)


def test_ci_checkout_keeps_full_git_history_for_lineage_validation() -> None:
    workflow = readiness_module._mapping(
        readiness_module._load_unique_yaml_path(PROJECT_ROOT / ".github/workflows/ci.yml", "ci"),
        "ci",
    )
    jobs = readiness_module._mapping(workflow["jobs"], "ci.jobs")
    job = readiness_module._mapping(jobs["test"], "ci.jobs.test")
    steps = readiness_module._maps(job["steps"], "ci.jobs.test.steps")
    checkout = next(row for row in steps if row.get("uses") == "actions/checkout@v4")
    checkout_with = readiness_module._mapping(checkout.get("with"), "checkout.with")
    assert checkout_with["fetch-depth"] == 0


@pytest.mark.parametrize(
    ("mode", "code"),
    [
        ("base_unknown", "HANDOFF_BASE_UNKNOWN"),
        ("head_unknown", "HANDOFF_HEAD_UNKNOWN"),
        ("base_head_nonancestor", "HANDOFF_BASE_HEAD_LINEAGE"),
        ("head_source_nonancestor", "HANDOFF_HEAD_SOURCE_BASE_LINEAGE"),
    ],
)
def test_source_gate_rejects_unknown_or_nonancestor_handoff_lineage(
    monkeypatch: pytest.MonkeyPatch,
    policy,
    mode: str,
    code: str,
) -> None:
    handoff = readiness_module._mapping(
        readiness_module._load_unique_yaml_path(
            PROJECT_ROOT / policy.source_handoff_path, "handoff"
        ),
        "handoff",
    )
    handoff_base = handoff["base_commit"]
    handoff_head = handoff["head_commit"]
    if mode in {"base_unknown", "head_unknown"}:
        missing = handoff_base if mode == "base_unknown" else handoff_head
        original = readiness_module._git_commit_exists
        monkeypatch.setattr(
            readiness_module,
            "_git_commit_exists",
            lambda root, commit: False if commit == missing else original(root, commit),
        )
    elif mode == "base_head_nonancestor":
        monkeypatch.setattr(readiness_module, "_git_is_ancestor", lambda *_args: False)
    else:
        outcomes = iter((True, False))
        monkeypatch.setattr(readiness_module, "_git_is_ancestor", lambda *_args: next(outcomes))

    with pytest.raises(G25ReadinessError, match=code):
        readiness_module._source_gate(PROJECT_ROOT, policy, BASE_COMMIT)


def test_non_conflicting_fixture_uses_existing_control_plane_and_fixed_batches(
    evidence: dict[str, object],
) -> None:
    lane = evidence["non_conflicting_lane_plan"]
    assert isinstance(lane, dict)
    assert lane["status"] == "PASS"
    plan = lane["lane_plan"]
    assert isinstance(plan, dict)
    assert [wave["kind"] for wave in plan["waves"]] == ["DOMAIN", "DOMAIN", "COORDINATOR"]
    assert [len(wave["assignments"]) for wave in plan["waves"]] == [2, 1, 1]

    scheduler = evidence["two_active_domain_worker_fixture"]
    assert isinstance(scheduler, dict)
    assert scheduler["fixture_status"] == "PASS"
    assert scheduler["active_domain_worker_count"] == 2
    assert scheduler["max_active_domain_workers"] == 2
    assert {row["change_id"] for row in scheduler["selected"]} == {
        "arch-004-g2-5-01-g4-operations",
        "arch-004-g2-5-02-g3-reporting",
    }
    assert scheduler["not_selected"][0]["change_id"] == ("arch-004-g2-5-03-g5-research-wrapper")
    assert "DOMAIN_CAPACITY_REACHED" in scheduler["not_selected"][0]["reason_codes"]
    assert scheduler["dispatch_allowed"] is False
    assert scheduler["lease_acquisition_allowed"] is False


def test_fail_closed_rehearsal_covers_required_architecture_conflicts(
    evidence: dict[str, object],
) -> None:
    cases = {row["case_id"]: row for row in evidence["fail_closed_rehearsals"]}
    assert set(cases) == {
        "base_commit_drift",
        "conflicting_owned_path",
        "contract_version_conflict",
        "coordinator_base_drift",
        "coordinator_change_binding_drift",
        "coordinator_contract_drift",
        "coordinator_lane_role_drift",
        "coordinator_module_drift",
        "coordinator_owned_path_drift",
        "coordinator_owner_drift",
        "coordinator_shared_path_drift",
        "coordinator_task_binding_drift",
        "coordinator_validation_tier_drift",
        "coordinator_only_path_claim",
        "domain_shared_path_claim",
        "missing_validation_tier",
        "unknown_module",
        "unknown_owned_path",
        "unknown_owner",
        "unsafe_production_effect",
    }
    assert all(row["rehearsal_status"] == "PASS" for row in cases.values())
    assert "OWNED_PATH_OVERLAP" in cases["conflicting_owned_path"]["observed_reason_codes"]
    assert "UNKNOWN_OWNED_PATH" in cases["unknown_owned_path"]["observed_reason_codes"]
    assert "DOMAIN_SHARED_PATH_CLAIM" in cases["domain_shared_path_claim"]["observed_reason_codes"]
    assert (
        "COORDINATOR_ONLY_PATH_VIOLATION"
        in cases["coordinator_only_path_claim"]["observed_reason_codes"]
    )
    assert (
        "CONTRACT_VERSION_CONFLICT" in cases["contract_version_conflict"]["observed_reason_codes"]
    )
    assert "BASE_DRIFT" in cases["base_commit_drift"]["observed_reason_codes"]
    assert cases["unsafe_production_effect"]["observed_reason_codes"] == [
        "UNSAFE_PRODUCTION_EFFECT"
    ]


def test_fragment_preview_is_deterministic_and_never_writes_aggregate(policy) -> None:
    fragments = policy_fragments(policy)
    first = build_fragment_preview(
        project_root=PROJECT_ROOT,
        policy=policy,
        fragments=fragments,
    )
    second = build_fragment_preview(
        project_root=PROJECT_ROOT,
        policy=policy,
        fragments=tuple(reversed(fragments)),
    )

    assert first == second
    assert first["status"] == "PASS"
    assert first["aggregate_source_of_truth_changed"] is False
    assert first["aggregate_write_performed"] is False
    assert all(
        row["source_of_truth_diff_status"] == "UNCHANGED_SHADOW_PREVIEW"
        and row["source_of_truth_write_performed"] is False
        for row in first["source_of_truth_diffs"]
    )


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        ({"owner": "unknown"}, "UNKNOWN_FRAGMENT_OWNER"),
        ({"target_id": "unknown"}, "UNKNOWN_FRAGMENT_TARGET"),
        ({"production_effect": "production"}, "UNSAFE_FRAGMENT_EFFECT"),
        ({"generated_source_of_truth_active": True}, "FRAGMENT_SOURCE_CUTOVER_FORBIDDEN"),
    ],
)
def test_fragment_preview_fails_closed_on_unknown_or_unsafe_input(
    policy, mutation: dict[str, object], code: str
) -> None:
    fragments = [dict(fragment) for fragment in policy_fragments(policy)]
    fragments[0].update(mutation)

    with pytest.raises(G25ReadinessError) as error:
        build_fragment_preview(
            project_root=PROJECT_ROOT,
            policy=policy,
            fragments=fragments,
        )

    assert error.value.code == code


def test_fragment_preview_rejects_duplicate_id(policy) -> None:
    fragments = [dict(fragment) for fragment in policy_fragments(policy)]
    duplicate = {**fragments[1], "fragment_id": fragments[0]["fragment_id"]}

    with pytest.raises(G25ReadinessError, match="DUPLICATE_FRAGMENT_ID"):
        build_fragment_preview(
            project_root=PROJECT_ROOT,
            policy=policy,
            fragments=[fragments[0], duplicate],
        )


def test_formal_fragment_preview_rejects_incomplete_set(policy) -> None:
    fragments = policy_fragments(policy)

    with pytest.raises(G25ReadinessError, match="FRAGMENT_SET_INCOMPLETE"):
        build_fragment_preview(
            project_root=PROJECT_ROOT,
            policy=policy,
            fragments=fragments[:-1],
            require_complete=True,
        )


def test_evidence_is_reproducible_and_tamper_or_base_drift_fails_closed(
    tmp_path: Path,
    evidence: dict[str, object],
) -> None:
    rebuilt = build_g2_5_readiness_evidence(
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )
    assert evidence == rebuilt
    path = tmp_path / "g2_5_readiness.json"
    write_g2_5_readiness_evidence(
        path,
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_g2_5_readiness_evidence(
        payload,
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )

    tampered = copy.deepcopy(payload)
    tampered["dispatch_allowed"] = True
    with pytest.raises(G25ReadinessError, match="EVIDENCE_CHECKSUM_DRIFT"):
        validate_g2_5_readiness_evidence(
            tampered,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )
    with pytest.raises(G25ReadinessError, match="HEAD_DRIFT"):
        validate_g2_5_readiness_evidence(
            payload,
            project_root=PROJECT_ROOT,
            current_base_commit="7" * 40,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )

    with pytest.raises(G25ReadinessError, match="HEAD_DRIFT"):
        build_g2_5_readiness_evidence(
            project_root=PROJECT_ROOT,
            current_base_commit="7" * 40,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )


def test_evidence_rejects_unknown_or_non_ancestor_source_base(
    monkeypatch: pytest.MonkeyPatch,
    evidence: dict[str, object],
) -> None:
    unknown = copy.deepcopy(evidence)
    unknown["source_base_commit"] = "0" * 40
    unknown["report_checksum"] = _report_checksum(unknown)
    with pytest.raises(G25ReadinessError, match="SOURCE_BASE_UNKNOWN"):
        validate_g2_5_readiness_evidence(
            unknown,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )

    monkeypatch.setattr(readiness_module, "_git_is_ancestor", lambda *_args: False)
    with pytest.raises(G25ReadinessError, match="SOURCE_BASE_NOT_ANCESTOR"):
        validate_g2_5_readiness_evidence(
            evidence,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )


def test_evidence_accepts_reproducible_ancestor_source_base_at_descendant_head() -> None:
    ancestor = _commit_ref("HEAD^")
    payload = readiness_module._build_g2_5_readiness_evidence(
        project_root=PROJECT_ROOT,
        source_base_commit=ancestor,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )

    validate_g2_5_readiness_evidence(
        payload,
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )
    assert payload["source_base_commit"] == ancestor
    assert ancestor != BASE_COMMIT


def test_tracked_evidence_preserves_non_production_and_legacy_truth_source() -> None:
    path = PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    validate_g2_5_readiness_evidence(
        payload,
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )

    carrier = readiness_module._first_parent_direct_child(
        project_root=PROJECT_ROOT,
        source_base_commit=payload["source_base_commit"],
        current_head_commit=BASE_COMMIT,
    )
    assert payload["status"] == "PASS"
    assert _is_ancestor(payload["source_base_commit"], BASE_COMMIT)
    assert _commit_ref(f"{carrier}^1") == payload["source_base_commit"]
    assert payload["source_gate"]["canonical_handoff_validation"] == "PASS"
    assert payload["source_gate"]["git_lineage"]["status"] == "PASS"
    assert payload["source_gate"]["validation_bundle"]["complete"] is True
    assert payload["source_gate"]["validation_bundle"]["artifact_count"] == 4
    frozen = payload["source_gate"]["frozen_phase_exit_matrix"]
    current = payload["source_gate"]["current_matrix"]
    assert frozen["sha256"] != current["sha256"]
    assert current["matches_frozen_phase_exit_sha256"] is False
    assert payload["parallel_control_policy"]["policy_version"].startswith(
        "arch_005_s2_s4_controlled_parallelism@"
    )
    assert len(payload["parallel_control_policy"]["sha256"]) == 64
    assert payload["source_of_truth"] == "LEGACY_MARKDOWN_ONLY"
    assert payload["dispatch_allowed"] is False
    assert payload["lease_acquisition_allowed"] is False
    assert payload["automatic_merge_allowed"] is False
    assert payload["aggregate_source_of_truth_changed"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_tracked_evidence_replays_carrier_before_live_owner_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    original = readiness_module._build_g2_5_readiness_evidence

    def reject_live_snapshot(**kwargs):
        if Path(kwargs["project_root"]).resolve() == PROJECT_ROOT:
            raise G25ReadinessError("SOURCE_OWNER_DRIFT", "synthetic future live owner")
        return original(**kwargs)

    monkeypatch.setattr(
        readiness_module,
        "_build_g2_5_readiness_evidence",
        reject_live_snapshot,
    )
    validate_g2_5_readiness_evidence(
        payload,
        project_root=PROJECT_ROOT,
        current_base_commit=BASE_COMMIT,
        policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
    )


def test_tracked_evidence_rejects_rechecksummed_carrier_blob_tamper() -> None:
    path = PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source_of_truth"] = "TAMPERED_LEGACY_MARKDOWN_ONLY"
    payload["report_checksum"] = _report_checksum(payload)

    with pytest.raises(G25ReadinessError, match="EVIDENCE_CARRIER_BLOB_DRIFT"):
        validate_g2_5_readiness_evidence(
            payload,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )


def test_tracked_evidence_rejects_non_reproducible_carrier_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    monkeypatch.setattr(
        readiness_module,
        "_g2_5_carrier_snapshot",
        lambda *_args, **_kwargs: nullcontext(PROJECT_ROOT),
    )

    with pytest.raises(G25ReadinessError, match="EVIDENCE_REPRODUCIBILITY_DRIFT"):
        validate_g2_5_readiness_evidence(
            payload,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )


def test_tracked_evidence_rejects_carrier_snapshot_owner_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = PROJECT_ROOT / "inputs/architecture/arch_004g2_5_parallel_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    original = readiness_module.build_module_manifest

    def drifted_carrier_manifest(**kwargs):
        manifest = copy.deepcopy(original(**kwargs))
        if Path(kwargs["project_root"]).resolve() != PROJECT_ROOT:
            target = "src/ai_trading_system/platform/reporting/audit_index.py"
            next(row for row in manifest["modules"] if row["path"] == target)[
                "owner_profile"
            ] = "operations"
        return manifest

    monkeypatch.setattr(
        readiness_module,
        "build_module_manifest",
        drifted_carrier_manifest,
    )
    with pytest.raises(G25ReadinessError, match="SOURCE_OWNER_DRIFT"):
        validate_g2_5_readiness_evidence(
            payload,
            project_root=PROJECT_ROOT,
            current_base_commit=BASE_COMMIT,
            policy_path=PROJECT_ROOT / DEFAULT_POLICY_PATH,
        )


def test_carrier_locator_rejects_non_direct_first_parent_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_base = "1" * 40
    carrier = "2" * 40
    wrong_parent = "3" * 40
    responses = iter(
        (
            subprocess.CompletedProcess(
                args=["git", "rev-list"],
                returncode=0,
                stdout=f"{carrier}\n".encode(),
                stderr=b"",
            ),
            subprocess.CompletedProcess(
                args=["git", "rev-parse"],
                returncode=0,
                stdout=f"{wrong_parent}\n".encode(),
                stderr=b"",
            ),
        )
    )
    monkeypatch.setattr(readiness_module, "_git_process", lambda *_args: next(responses))

    with pytest.raises(G25ReadinessError, match="EVIDENCE_CARRIER_DIRECT_CHILD_REQUIRED"):
        readiness_module._first_parent_direct_child(
            project_root=PROJECT_ROOT,
            source_base_commit=source_base,
            current_head_commit="4" * 40,
        )
