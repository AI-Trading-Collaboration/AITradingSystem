from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ai_trading_system import first_layer_matched_placebo_falsification as subject
from ai_trading_system.yaml_loader import load_strict_yaml_text

ROOT = Path(__file__).resolve().parents[1]


def _targets() -> tuple[float, ...]:
    return (0.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0)


def test_preregistration_freezes_identity_statistics_and_zero_external_actions() -> None:
    policy = subject.load_preregistration(project_root=ROOT)

    assert policy.payload["matched_placebo_contract"]["random_seed"] == 2558
    assert policy.payload["matched_placebo_contract"]["draws"] == 10_000
    assert policy.payload["pilot_reducer"]["alpha"] == 0.05
    assert policy.payload["known_result_boundary"]["pristine_out_of_sample_claim_allowed"] is False
    for field in (
        "data_downloads",
        "cache_mutations",
        "quantconnect_actions",
        "option_backtests",
        "external_provider_actions",
        "orders",
        "fills",
        "positions",
    ):
        assert policy.payload["run_envelope"][field] == 0


def test_real_authorization_and_manifest_bind_frozen_implementation_and_inputs() -> None:
    policy = subject.load_preregistration(project_root=ROOT)
    authorization = subject.load_run_authorization(project_root=ROOT, policy=policy)
    manifest = subject.load_execution_manifest(project_root=ROOT, authorization=authorization)

    assert authorization.payload["owner_decision"]["authorization_state"] == (
        "STANDING_OWNER_SCOPE"
    )
    assert authorization.payload["run_envelope"] == subject.EXPECTED_COUNTERS
    assert manifest.payload["code_binding"] == {
        "implementation_commit_sha": "bd00069d0cd56dc2135ab61e70663f7e38b7d037",
        "module_path": ("src/ai_trading_system/first_layer_matched_placebo_falsification.py"),
        "module_sha256": ("d72f7b9294cc7b8030a9eb8a3a04559bb64b433c0179810a12f83d6807e3e20d"),
    }
    assert tuple(binding.role for binding in manifest.inputs) == subject.INPUT_ROLES
    assert manifest.payload["random_seed"] == subject.RANDOM_SEED
    assert manifest.payload["placebo_draws"] == subject.PLACEBO_DRAWS


def test_terminal_result_admission_binds_write_once_evidence() -> None:
    path = ROOT / "config/research/first_layer_composer_v2_matched_placebo_result_admission_v1.yaml"
    admission = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=path.as_posix())

    assert admission["technical_validation_state"] == "PASS"
    assert admission["result"]["reducer_status"] == (
        "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO"
    )
    assert admission["result"]["one_sided_p_value"] == 0.20007999200079993
    assert admission["invariant_validation"]["independent_replay_status"] == "PASS"
    assert admission["interpretation_boundary"]["supports_signal_validity_claim"] is False
    assert admission["interpretation_boundary"]["same_manifest_rerun_allowed"] is False
    for binding in admission["evidence_bindings"]:
        evidence = ROOT / binding["path"]
        assert evidence.is_file()
        assert hashlib.sha256(evidence.read_bytes()).hexdigest() == binding["file_sha256"]


def test_shape_extraction_and_reconstruction_preserve_all_invariants() -> None:
    targets = _targets()
    shape = subject.extract_exposure_shape(targets)

    assert shape.leading_flat_count == 2
    assert shape.trailing_flat_count == 1
    assert shape.long_run_lengths == (2, 1, 3)
    assert shape.interior_flat_gap_lengths == (1, 2)
    assert shape.long_interval_count == 6
    assert shape.accounting_trade_event_count == 6

    for draw in range(20):
        placebo = subject.reconstruct_placebo_targets(shape, seed=2558, draw=draw)
        rebuilt = subject.extract_exposure_shape(placebo)
        assert len(placebo) == len(targets)
        assert rebuilt.leading_flat_count == shape.leading_flat_count
        assert rebuilt.trailing_flat_count == shape.trailing_flat_count
        assert rebuilt.long_interval_count == shape.long_interval_count
        assert sorted(rebuilt.long_run_lengths) == sorted(shape.long_run_lengths)
        assert sorted(rebuilt.interior_flat_gap_lengths) == sorted(shape.interior_flat_gap_lengths)
        assert rebuilt.accounting_trade_event_count == shape.accounting_trade_event_count


def test_reconstruction_is_seeded_and_streams_are_independent() -> None:
    shape = subject.extract_exposure_shape(_targets())
    first = [subject.reconstruct_placebo_targets(shape, seed=2558, draw=i) for i in range(10)]
    second = [subject.reconstruct_placebo_targets(shape, seed=2558, draw=i) for i in range(10)]
    changed_seed = [
        subject.reconstruct_placebo_targets(shape, seed=2559, draw=i) for i in range(10)
    ]

    assert first == second
    assert first != changed_seed
    assert len(set(first)) > 1


@pytest.mark.parametrize(
    "targets",
    [
        (),
        (0.0, 0.0),
        (0.0, 0.5, 1.0),
    ],
)
def test_shape_extraction_fails_closed_on_invalid_targets(
    targets: tuple[float, ...],
) -> None:
    with pytest.raises(subject.MatchedPlaceboExecutionError, match="MPF_SHAPE_INVALID"):
        subject.extract_exposure_shape(targets)


def test_distribution_and_independent_accounting_replay_match() -> None:
    targets = _targets()
    prices = tuple(100.0 + value for value in (0, 1, 3, 2, 5, 4, 8, 7, 10, 9, 12, 11, 15))
    distribution = subject.build_placebo_distribution(
        prices,
        targets,
        sum(targets) / len(targets),
        seed=subject.RANDOM_SEED,
        draws=subject.PLACEBO_DRAWS,
    )

    first_summary = subject.summarize_distribution(distribution)
    second_summary = subject.summarize_distribution(distribution)
    replay = subject.independently_replay_distribution(
        prices, targets, sum(targets) / len(targets), distribution
    )

    assert first_summary == second_summary
    assert replay["status"] == "PASS"
    assert replay["one_sided_p_value"] == first_summary["one_sided_p_value"]
    assert replay["reducer_status"] == first_summary["reducer_status"]
    assert replay["maximum_placebo_excess_abs_diff"] <= subject.RECONCILIATION_TOLERANCE
    assert replay["maximum_placebo_drawdown_abs_diff"] <= subject.RECONCILIATION_TOLERANCE


def test_one_sided_p_value_and_reducer_are_mechanical(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "PLACEBO_DRAWS", 3)
    distribution = subject.PlaceboDistribution(
        observed_excess_percentage_points=2.0,
        observed_max_drawdown_magnitude_pct=4.0,
        comparator_net_total_return_pct=1.0,
        placebo_excess_percentage_points=(1.0, 2.0, 3.0),
        placebo_max_drawdown_magnitude_pct=(3.0, 4.0, 5.0),
        target_inventory_sha256="a" * 64,
    )

    summary = subject.summarize_distribution(distribution)

    assert summary["placebo_excess_greater_than_or_equal_observed_count"] == 2
    assert summary["one_sided_p_value"] == 0.75
    assert summary["observed_excess_percentile"] == pytest.approx(200.0 / 3.0)
    assert summary["reducer_status"] == "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO"


def test_linear_type_7_quantiles_are_frozen() -> None:
    values = (0.0, 10.0, 20.0, 30.0, 40.0)

    assert subject._linear_type_7(values, 2.5) == pytest.approx(1.0)
    assert subject._linear_type_7(values, 50.0) == 20.0
    assert subject._linear_type_7(values, 97.5) == pytest.approx(39.0)
