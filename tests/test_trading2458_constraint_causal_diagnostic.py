from __future__ import annotations

import gzip
import json
import shutil
from hashlib import sha256
from pathlib import Path

import pytest

from ai_trading_system.trading2453_constraint_hit_diagnosis import (
    DEFAULT_PACKAGE_ROOT,
    DEFAULT_RUN_ID,
    EXPECTED_RUN_HASHES,
    build_trading2453_diagnosis,
)
from ai_trading_system.trading2458_constraint_causal_diagnostic import (
    DEFAULT_POLICY_PATH,
    SAFETY,
    TARGET_AXES,
    Trading2458ConstraintCausalDiagnosticError,
    build_matched_contrasts,
    build_trading2458_diagnostic,
    load_trading2458_policy,
    validate_trading2458_diagnostic,
    write_trading2458_diagnostic,
)

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "trading2453_constraint_hit_diagnosis"


@pytest.fixture(scope="session")
def trading2458_run_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    manifest_bytes = gzip.decompress(
        FIXTURE_ROOT.joinpath("evaluator_manifest.json.gz").read_bytes()
    )
    assert sha256(manifest_bytes).hexdigest() == EXPECTED_RUN_HASHES["evaluator_manifest.json"]
    manifest = json.loads(manifest_bytes)
    expected_hashes = dict(EXPECTED_RUN_HASHES)
    expected_hashes.update(manifest["output_artifact_checksums"])
    empty_hash = sha256(b"").hexdigest()

    run_dir = tmp_path_factory.mktemp("trading2458_frozen_run") / DEFAULT_RUN_ID
    run_dir.mkdir()
    for filename, expected_hash in sorted(expected_hashes.items()):
        fixture_path = FIXTURE_ROOT / (
            filename if expected_hash == empty_hash else f"{filename}.gz"
        )
        fixture_bytes = fixture_path.read_bytes()
        content = fixture_bytes if expected_hash == empty_hash else gzip.decompress(fixture_bytes)
        destination = run_dir / filename
        destination.write_bytes(content)
        assert sha256(content).hexdigest() == expected_hash
    return run_dir


def test_policy_is_reviewed_and_keeps_all_safety_boundaries() -> None:
    policy = load_trading2458_policy()

    assert policy["status"] == "REVIEWED_NARROW_DIAGNOSTIC"
    assert tuple(policy["matched_contrast"]["target_axes"]) == TARGET_AXES
    assert policy["matched_contrast"]["required_fold_count"] == 6
    assert policy["matched_contrast"]["required_nonzero_direction_consistency"] == 1.0
    assert policy["safety"] == SAFETY


def test_frozen_diagnostic_finds_common_mode_across_all_seven_axes(
    trading2458_run_dir: Path,
) -> None:
    bundle = build_trading2458_diagnostic(run_dir=trading2458_run_dir)
    diagnostic = bundle["diagnostic"]
    summaries = {row["axis"]: row for row in diagnostic["axis_summaries"]}

    assert bundle["manifest"]["status"] == "PASS"
    assert diagnostic["structure_checks"] == {
        "evaluation_count_is_1800": True,
        "all_recomputations_exact": True,
        "six_folds_present": True,
        "all_candidate_axes_exact": True,
        "all_ranges_historical_seen": True,
        "all_rows_rejected_under_frozen_gate": True,
    }
    assert diagnostic["matched_contrast_summary"]["pair_count"] == 7716
    assert set(summaries) == set(TARGET_AXES)
    assert all(
        row["classification"] == "COMMON_MODE_SATURATION_NO_AXIS_DISCRIMINATION"
        for row in summaries.values()
    )
    assert all(row["covered_fold_count"] == 6 for row in summaries.values())
    assert all(row["nonzero_covered_fold_count"] == 0 for row in summaries.values())
    assert all(
        row["constraint_hit_rate_delta"]["minimum"] == 0.0
        and row["constraint_hit_rate_delta"]["maximum"] == 0.0
        for row in summaries.values()
    )
    assert {axis: row["pair_count"] for axis, row in summaries.items()} == {
        "constraint_buffer_bps": 864,
        "drawdown_guard": 798,
        "rebalance_cooldown_days": 900,
        "rescue_intensity": 1842,
        "risk_off_confirmation_days": 888,
        "smooth_window_days": 1536,
        "turnover_penalty": 888,
    }
    assert diagnostic["conclusion"] == {
        "classification": "CURRENT_FAMILY_COMMON_MODE_NO_CONSTRAINT_DISCRIMINATION",
        "recommended_owner_action": "RETIRE_CURRENT_FAMILY",
        "role_correct_gate_policy_option": "AUTHOR_ROLE_CORRECT_GATE_POLICY",
        "threshold_change_supported": False,
        "same_package_rerun_supported": False,
        "candidate_expansion_executed": False,
        "causal_claim_supported": False,
    }
    owner_pack = bundle["owner_pack"]
    assert owner_pack["recommended_option_id"] == "RETIRE_CURRENT_FAMILY"
    assert sum(option["recommended"] for option in owner_pack["options"]) == 1
    assert all(
        option["same_package_rerun_allowed"] is False
        and option["prospective_access_allowed"] is False
        for option in owner_pack["options"]
    )
    assert bundle["manifest"]["safety"] == SAFETY
    assert bundle["manifest"]["original_package_reopened"] is False
    assert bundle["manifest"]["threshold_or_gate_modified"] is False
    assert bundle["manifest"]["candidate_or_search_space_modified"] is False
    assert bundle["manifest"]["prospective_holdout_accessed"] is False


def test_matched_contrasts_are_order_invariant_and_hold_other_axes_equal(
    trading2458_run_dir: Path,
) -> None:
    policy = load_trading2458_policy()
    prior_rows = build_trading2453_diagnosis(run_dir=trading2458_run_dir)["recomputations"]

    forward = build_matched_contrasts(rows=prior_rows, policy=policy)
    reverse = build_matched_contrasts(rows=list(reversed(prior_rows)), policy=policy)

    assert forward == reverse
    assert len(forward) == 7716
    for row in forward:
        axis = row["axis"]
        assert set(row["matched_other_axes"]) == set(TARGET_AXES) - {axis}
        assert row["left"]["axis_value"] != row["right"]["axis_value"]
        assert row["interpretation"] == "MATCHED_ASSOCIATION_NOT_PROVEN_CAUSALITY"
        assert row["effect"]["constraint_hit_rate_direction"] == "ZERO"
        assert row["safety"] == SAFETY


@pytest.mark.parametrize(
    "target",
    ["axis_diagnostic.json", "matched_contrasts.jsonl", "owner_decision_pack.md"],
)
def test_content_derived_validator_rejects_output_tamper(
    tmp_path: Path,
    trading2458_run_dir: Path,
    target: str,
) -> None:
    output_dir = tmp_path / "diagnostic"
    result = write_trading2458_diagnostic(
        output_dir=output_dir,
        run_dir=trading2458_run_dir,
    )
    assert result["validation"]["status"] == "PASS"

    target_path = output_dir / target
    target_path.write_bytes(target_path.read_bytes() + b"tampered\n")
    validation = validate_trading2458_diagnostic(
        output_dir=output_dir,
        run_dir=trading2458_run_dir,
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1


def test_validator_rejects_extra_output_and_writer_rejects_nonempty_directory(
    tmp_path: Path,
    trading2458_run_dir: Path,
) -> None:
    output_dir = tmp_path / "diagnostic"
    write_trading2458_diagnostic(
        output_dir=output_dir,
        run_dir=trading2458_run_dir,
    )
    (output_dir / "stale.json").write_text("{}\n", encoding="utf-8")

    validation = validate_trading2458_diagnostic(
        output_dir=output_dir,
        run_dir=trading2458_run_dir,
    )
    assert validation["status"] == "FAIL"
    with pytest.raises(
        Trading2458ConstraintCausalDiagnosticError,
        match="absent or empty",
    ):
        write_trading2458_diagnostic(
            output_dir=output_dir,
            run_dir=trading2458_run_dir,
        )


def test_frozen_source_tamper_fails_closed(
    tmp_path: Path,
    trading2458_run_dir: Path,
) -> None:
    run_dir = tmp_path / DEFAULT_RUN_ID
    package_root = tmp_path / "package"
    shutil.copytree(trading2458_run_dir, run_dir)
    shutil.copytree(DEFAULT_PACKAGE_ROOT, package_root)
    train_path = run_dir / "train_evaluations.jsonl"
    train_path.write_bytes(train_path.read_bytes() + b"\n")

    with pytest.raises(
        Trading2458ConstraintCausalDiagnosticError,
        match="frozen run input drift",
    ):
        build_trading2458_diagnostic(
            run_dir=run_dir,
            package_root=package_root,
        )


def test_policy_axis_tamper_fails_closed(tmp_path: Path) -> None:
    policy_path = tmp_path / "policy.yaml"
    text = DEFAULT_POLICY_PATH.read_text(encoding="utf-8")
    policy_path.write_text(
        text.replace("    - turnover_penalty\n", ""),
        encoding="utf-8",
    )

    with pytest.raises(
        Trading2458ConstraintCausalDiagnosticError,
        match="target axis set/order drift",
    ):
        load_trading2458_policy(policy_path)
