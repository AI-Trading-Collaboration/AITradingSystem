from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ai_trading_system.platform.architecture import compatibility_authority as authority
from ai_trading_system.platform.architecture.compatibility_authority import (
    CompatibilityAuthorityError,
    load_compatibility_authority,
    load_compatibility_policy,
    render_fragment,
    render_index,
    validate_repository_authority,
)
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes
from ai_trading_system.yaml_loader import safe_load_yaml_text

REAL_LEGACY_PATH = Path("inputs/architecture/arch_004_compatibility_baseline.yaml")
DEVX_006C_SECTION = "phase_devx_006c_compatibility_authority_fragmentation"
DEVX_006D_SECTION = "phase_devx_006d_report_catalog_flow_lossless_fragmentation"
ARCH_005_S5_SECTION = "phase_arch_005_s5_canonical_task_source_cutover"
DEVX_007_V2_SECTION = "phase_devx_007_web_pro_git_review_skill_explicit_submission_v2"
TRADING_2542C_SECTION = (
    "phase_trading_2542c_growth_action_value_independent_review_remediation_and_freeze_readiness_v1"
)
DEVX_009_SECTION = (
    "phase_devx_009_parallel_integration_publication_fence_and_generated_state_rebuild_v1"
)
TRADING_2542D_SECTION = (
    "phase_trading_2542d_growth_action_value_dq_pit_and_sample_semantics_freeze_correction_v1"
)
PROD_004_SECTION = "phase_prod_004_pit_cumulative_archive_consumption_v1"
DEVX_011_SECTION = "phase_devx_011_governed_workflow_health_control_loop_v1"
DEVX_012_SECTION = "phase_devx_012_automatic_workflow_health_trigger_and_outcome_review_v1"
RISK_012_SECTION = "phase_risk_012_unknown_risk_event_id_fail_closed_v1"
OPS_077_SECTION = "phase_ops_077_atomic_release_scheduler_binding_and_canary_v1"


def _write_fixture_authority(
    root: Path,
    *,
    sections: list[tuple[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    legacy_path = root / REAL_LEGACY_PATH
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_bytes = b"schema_version: fixture.v1\nlegacy_section:\n  status: PASS\n"
    legacy_path.write_bytes(legacy_bytes)
    legacy = safe_load_yaml_text(legacy_bytes.decode("utf-8"))
    legacy_ids = list(legacy)
    legacy_seal = {
        "path": REAL_LEGACY_PATH.as_posix(),
        "byte_count": len(legacy_bytes),
        "file_sha256": hashlib.sha256(legacy_bytes).hexdigest(),
        "lf_sha256": hashlib.sha256(legacy_bytes).hexdigest(),
        "git_blob": authority._git_blob_id(legacy_bytes),
        "top_level_entry_count": len(legacy_ids),
        "ordered_entry_ids_sha256": hashlib.sha256(
            ("\n".join(legacy_ids) + "\n").encode("utf-8")
        ).hexdigest(),
        "mapping_replay_sha256": hashlib.sha256(
            authority._canonical_mapping_bytes(legacy, sort_keys=False)
        ).hexdigest(),
        "grandfathered_duplicate_key_count": 0,
        "grandfathered_duplicate_key_behavior": "NONE",
    }
    policy = {
        "schema_version": "devx_006c_compatibility_authority_policy.v1",
        "status": "ACTIVE",
        "task_id": "DEVX-006C_COMPATIBILITY_AUTHORITY_FRAGMENTATION",
        "exact_start_base": "0" * 40,
        "owner_decision": "fixture-owner-decision",
        "legacy_prefix": legacy_seal,
        "fragment_root": "registry/architecture_compatibility_authority/fragments",
        "index_path": "inputs/architecture/devx_006c_compatibility_authority_index.json",
        "consumer_inventory_path": (
            "inputs/architecture/devx_006c_compatibility_consumer_inventory.json"
        ),
        "contract": {
            "legacy_append_allowed": False,
            "fragment_source_active": True,
            "dual_write": False,
            "fragment_identity": "CANONICAL_SECTION_SHA256",
            "index_chain": "SHA256",
            "rollback_mode": "FROZEN_LEGACY_PREFIX_ONLY",
        },
        "production_effect": "none",
        "broker_action": "none",
    }
    policy_path = root / authority.DEFAULT_POLICY_PATH
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        yaml.safe_dump(policy, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    loaded_policy = load_compatibility_policy(root)
    requested = sections or [("new_section", {"status": "PASS"})]
    fragments: list[tuple[str, str, dict[str, Any], bytes]] = []
    fragment_paths: list[Path] = []
    for section_id, section in requested:
        relative, record, content = render_fragment(
            section_id=section_id,
            section=section,
        )
        fragment_path = root / Path(loaded_policy["fragment_root"]) / Path(relative)
        fragment_path.parent.mkdir(parents=True, exist_ok=True)
        fragment_path.write_bytes(content)
        fragment_paths.append(fragment_path)
        fragments.append((section_id, relative, record, content))
    index, index_bytes = render_index(policy=loaded_policy, fragments=fragments)
    index_path = root / Path(loaded_policy["index_path"])
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_bytes(index_bytes)
    return {
        "legacy_path": legacy_path,
        "legacy_bytes": legacy_bytes,
        "policy": loaded_policy,
        "fragment_paths": fragment_paths,
        "index_path": index_path,
        "index": index,
    }


def _write_index(path: Path, index: dict[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(index, sort_keys=True, indent=2, ensure_ascii=False))


def _rehash_entry(index: dict[str, Any], position: int) -> None:
    entry = index["entries"][position]
    entry_without_hash = {key: entry[key] for key in authority._ENTRY_FIELDS - {"entry_sha256"}}
    entry["entry_sha256"] = authority._entry_hash(entry_without_hash)
    index["final_chain_sha256"] = index["entries"][-1]["entry_sha256"]


def test_repository_authority_is_fresh_and_cut_over() -> None:
    result = validate_repository_authority()
    merged = load_compatibility_authority()
    legacy_only = load_compatibility_authority(include_fragments=False)

    assert result["status"] == "PASS"
    assert len(legacy_only) == 306
    assert len(merged) == 318
    assert next(reversed(legacy_only)) == (
        "phase_trading_2504_qqq_options_owner_decision_manifest_v1"
    )
    assert next(reversed(merged)) == OPS_077_SECTION
    assert DEVX_006C_SECTION in merged
    assert DEVX_006D_SECTION in merged
    assert merged[ARCH_005_S5_SECTION]["task_registry_authority"]["source_of_truth"] == (
        "ARCH_005_TASK_REGISTRY"
    )
    assert merged[DEVX_007_V2_SECTION]["submission_authorization"] == {
        "explicit_current_request_is_submission_authority": True,
        "non_sensitive_public_or_authorized_exact_commit_only": True,
        "repeat_send_confirmation_required": False,
        "second_submission_requires_separate_recovery_or_authorization": True,
        "sensitive_private_unscoped_fail_closed": True,
        "scope_expansion_is_new_authority_scope": True,
    }
    assert merged[TRADING_2542C_SECTION]["research_boundary"] == {
        "threshold_bundle_frozen": False,
        "real_dq_or_empirical_run_authorized": False,
        "second_independent_review_required": True,
        "owner_exact_value_approval_required": True,
    }
    assert merged[DEVX_009_SECTION]["publication_contract"] == {
        "single_active_coordinator": True,
        "expected_main_compare_and_set": True,
        "generated_state_rebuild_once": True,
        "full_dispatch_claim_is_atomic": True,
        "closeout_receipt_is_replayable": True,
    }
    assert merged[TRADING_2542D_SECTION]["engineering_contract"] == {
        "target_session_count": 1202,
        "target_session_inventory_lf_sha256": (
            "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
        ),
        "pre_window_prior_session": "2021-02-19",
        "excluded_invalid_propagates": True,
        "zero_expected_terminal": "FAIL",
        "expected_nonempty_zero_observed_terminal": "INVALID",
        "right_censor_after_transitive_cluster_merge": True,
        "cost_reconciliation_session_keyed": True,
    }
    assert merged[PROD_004_SECTION]["pit_consumption_contract"] == {
        "primary_research_start": "2021-02-22",
        "default_manifest_mode": "CUMULATIVE_ARCHIVE_DISCOVERY",
        "explicit_manifest_compatibility_preserved": True,
        "daily_capture_date_directories_must_be_iso": True,
        "conflicting_snapshot_id_fails_closed": True,
        "valuation_history_recursive_pattern_restricted": True,
        "duplicate_valuation_snapshot_id_fails_closed": True,
        "strict_pit_grade_a_inferred": False,
    }
    assert merged[DEVX_011_SECTION]["workflow_health_contract"] == {
        "window_days": 7,
        "window_timezone": "UTC",
        "telemetry_sources": [
            "validation_runtime",
            "publication_transactions",
            "git_main_history",
        ],
        "candidate_fingerprint_stable_across_dates": True,
        "candidate_review_only": True,
        "weekly_self_trigger_enabled": True,
        "automatic_dispatch_enabled": False,
        "task_or_code_mutation_allowed": False,
        "validation_gate_change_allowed": False,
    }
    assert merged[DEVX_012_SECTION]["workflow_health_automatic_cycle"] == {
        "existing_automation_id": "aitradingsystem-pit",
        "second_scheduler_created": False,
        "iso_week_validated_bundle_deduplication": True,
        "failed_or_blocked_retry_on_next_existing_invocation": True,
        "main_origin_head_identity_required": True,
        "automatic_report_generation_enabled": True,
        "automatic_optimization_execution_enabled": False,
        "candidate_task_or_code_mutation_allowed": False,
        "validation_gate_change_allowed": False,
        "prior_validated_week_metric_comparison": True,
        "candidate_lifecycle_reported": True,
    }
    assert merged[RISK_012_SECTION]["risk_event_admission_contract"] == {
        "reviewed_config_required": True,
        "unknown_matched_id_is_error": True,
        "batch_error_writes_zero_occurrences": True,
        "batch_error_writes_zero_attestations": True,
        "known_match_preferred_from_mixed_list": True,
        "validator_relaxed": False,
    }
    assert merged[DEVX_006C_SECTION]["authority_contract"] == {
        "dual_write": False,
        "fragment_identity": "CANONICAL_SECTION_SHA256",
        "fragment_source_active": True,
        "index_chain": "SHA256",
        "legacy_append_allowed": False,
        "rollback_mode": "FROZEN_LEGACY_PREFIX_ONLY",
    }


def test_legacy_prefix_bytes_equal_exact_start_base() -> None:
    current = REAL_LEGACY_PATH.read_bytes()
    frozen = authority._git_text(
        Path(".").resolve(),
        "cb437a4d4be178180f60cb3ee2d2994c1be45f94",
        REAL_LEGACY_PATH.as_posix(),
    ).encode("utf-8")

    assert current == frozen
    assert hashlib.sha256(current).hexdigest() == (
        "253b976b2740f0097e1d8949ec8eaf3846c82809f88fdf3a47fb76fae6023842"
    )


def test_consumer_inventory_has_no_growth_reader_or_runtime_writer() -> None:
    inventory = json.loads(
        Path("inputs/architecture/devx_006c_compatibility_consumer_inventory.json").read_text(
            encoding="utf-8"
        )
    )

    assert inventory["status"] == "PASS"
    assert inventory["consumer_count"] == 8
    assert inventory["growth_assuming_direct_consumer_count"] == 0
    assert inventory["runtime_legacy_append_writer_count"] == 0
    assert inventory["fixture_legacy_writer_count"] == 1
    assert sum(row["base_growth_assuming_read_count"] for row in inventory["consumers"]) == 135
    assert all(row["migration_status"] == "MIGRATED" for row in inventory["consumers"])


def test_fragment_and_index_render_are_repeatable() -> None:
    policy = load_compatibility_policy()
    first = render_fragment(section_id="repeatable_section", section={"value": [2, 1]})
    second = render_fragment(section_id="repeatable_section", section={"value": [2, 1]})
    first_index = render_index(
        policy=policy,
        fragments=[("repeatable_section", first[0], first[1], first[2])],
    )
    second_index = render_index(
        policy=policy,
        fragments=[("repeatable_section", second[0], second[1], second[2])],
    )

    assert first == second
    assert first_index == second_index


def test_legacy_one_fragment_and_multi_fragment_preserve_order(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )
    merged = load_compatibility_authority(tmp_path)

    assert list(merged) == [
        "schema_version",
        "legacy_section",
        "fragment_one",
        "fragment_two",
    ]
    assert fixture["legacy_path"].read_bytes() == fixture["legacy_bytes"]


def test_new_fragment_does_not_change_existing_fragment_or_legacy(tmp_path: Path) -> None:
    first = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1})],
    )
    legacy_before = first["legacy_path"].read_bytes()
    fragment_before = first["fragment_paths"][0].read_bytes()
    _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )

    assert first["legacy_path"].read_bytes() == legacy_before
    assert first["fragment_paths"][0].read_bytes() == fragment_before


def test_missing_fragment_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fixture["fragment_paths"][0].unlink()

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FILE_MISSING"


def test_fragment_tamper_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment = fixture["fragment_paths"][0]
    fragment.write_bytes(fragment.read_bytes() + b" ")

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FRAGMENT_HASH_DRIFT"


def test_fragment_unknown_field_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment_path = fixture["fragment_paths"][0]
    fragment = json.loads(fragment_path.read_text(encoding="utf-8"))
    fragment["unexpected"] = True
    fragment_bytes = canonical_json_bytes(fragment, sort_keys=True, indent=2, ensure_ascii=False)
    fragment_path.write_bytes(fragment_bytes)
    index = fixture["index"]
    index["entries"][0]["fragment_sha256"] = hashlib.sha256(fragment_bytes).hexdigest()
    _rehash_entry(index, 0)
    _write_index(fixture["index_path"], index)

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_FIELDS_INVALID"


def test_duplicate_section_and_fragment_path_fail_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    relative, record, content = render_fragment(section_id="same", section={"value": 1})

    with pytest.raises(CompatibilityAuthorityError) as duplicate_section:
        render_index(
            policy=fixture["policy"],
            fragments=[
                ("same", relative, record, content),
                ("same", relative, record, content),
            ],
        )
    assert duplicate_section.value.code == "AUTHORITY_DUPLICATE_SECTION_ID"

    other_record = dict(record)
    other_record["section_id"] = "other"
    with pytest.raises(CompatibilityAuthorityError) as duplicate_path:
        render_index(
            policy=fixture["policy"],
            fragments=[
                ("same", relative, record, content),
                ("other", relative, other_record, content),
            ],
        )
    assert duplicate_path.value.code == "AUTHORITY_DUPLICATE_FRAGMENT_PATH"


def test_reorder_and_broken_chain_fail_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(
        tmp_path,
        sections=[("fragment_one", {"value": 1}), ("fragment_two", {"value": 2})],
    )
    reordered = fixture["index"]
    reordered["entries"] = list(reversed(reordered["entries"]))
    _write_index(fixture["index_path"], reordered)

    with pytest.raises(CompatibilityAuthorityError) as reorder_error:
        load_compatibility_authority(tmp_path)
    assert reorder_error.value.code == "AUTHORITY_INDEX_ORDER_INVALID"

    fixture = _write_fixture_authority(tmp_path)
    fixture["index"]["entries"][0]["previous_entry_sha256"] = "f" * 64
    _write_index(fixture["index_path"], fixture["index"])
    with pytest.raises(CompatibilityAuthorityError) as chain_error:
        load_compatibility_authority(tmp_path)
    assert chain_error.value.code == "AUTHORITY_INDEX_CHAIN_BROKEN"


def test_path_escape_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    index = fixture["index"]
    index["entries"][0]["fragment_path"] = "../outside.json"
    _rehash_entry(index, 0)
    _write_index(fixture["index_path"], index)

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_PATH_INVALID"


def test_symlink_fragment_fails_closed(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    fragment_path = fixture["fragment_paths"][0]
    content = fragment_path.read_bytes()
    outside = tmp_path / "outside.json"
    outside.write_bytes(content)
    fragment_path.unlink()
    try:
        fragment_path.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_PATH_SYMLINK"


def test_legacy_drift_and_rollback_are_explicit(tmp_path: Path) -> None:
    fixture = _write_fixture_authority(tmp_path)
    legacy_only = load_compatibility_authority(tmp_path, include_fragments=False)
    assert list(legacy_only) == ["schema_version", "legacy_section"]

    fixture["legacy_path"].write_bytes(fixture["legacy_bytes"] + b"# drift\n")
    with pytest.raises(CompatibilityAuthorityError) as caught:
        load_compatibility_authority(tmp_path)
    assert caught.value.code == "AUTHORITY_LEGACY_SIZE_DRIFT"
