from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from ai_trading_system import first_layer_foundational_falsification_execution as f1
from ai_trading_system import first_layer_temporal_influence_falsification as tif


def _synthetic_plan() -> f1.DiagnosticPlan:
    long_runs = [25, *([9] * 40)]
    targets: list[float] = [0.0] * 20
    for index, run_length in enumerate(long_runs):
        targets.extend([1.0] * run_length)
        if index != len(long_runs) - 1:
            targets.extend([0.0] * 10)
    targets.extend([0.0] * (tif.EXPECTED_INTERVALS - len(targets)))
    assert len(targets) == tif.EXPECTED_INTERVALS
    assert sum(targets) == tif.EXPECTED_LONG_INTERVALS

    start = date(2021, 2, 22)
    sessions = tuple(start + timedelta(days=index) for index in range(1202))
    interval_returns = tuple(
        0.0015 * math.sin(index / 17.0) + 0.0003
        for index in range(tif.EXPECTED_INTERVALS)
    )
    prices = [100.0]
    for value in interval_returns:
        prices.append(prices[-1] * (1.0 + value))
    return f1.DiagnosticPlan(
        sessions=sessions,
        qqq_prices=tuple(prices),
        sgov_prices=tuple(100.0 for _ in sessions),
        states=tuple("neutral" for _ in sessions),
        interval_targets=tuple(targets),
        long_interval_count=tif.EXPECTED_LONG_INTERVALS,
        comparator_weight=tif.EXPECTED_LONG_INTERVALS / tif.EXPECTED_INTERVALS,
        action_counts={},
    )


def _summary_rows(
    *,
    shift_zero: float = 3.0,
    shift_plus_one: float = 2.0,
    best_negative: bool = False,
    nonpositive_episode: bool = False,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    temporal = []
    for shift in tif.SHIFT_SESSIONS:
        value = 1.0
        if shift == 0:
            value = shift_zero
        elif shift == 1:
            value = shift_plus_one
        elif shift == -1 and best_negative:
            value = shift_zero + 1.0
        temporal.append(
            {"shift_sessions": shift, "paired_excess_percentage_points": value}
        )
    episodes = []
    for episode_id in range(1, tif.EXPECTED_LONG_EPISODES + 1):
        excess = -0.1 if nonpositive_episode and episode_id == 7 else 1.0
        episodes.append(
            {
                "episode_id": episode_id,
                "paired_excess_percentage_points": excess,
                "paired_excess_drop_from_original_percentage_points": 3.0 - excess,
            }
        )
    return temporal, episodes


def test_preregistration_strict_loader_and_shift_tamper(tmp_path: Path) -> None:
    loaded = tif.load_preregistration()
    assert loaded.payload["task_id"] == tif.TASK_ID
    assert loaded.payload["run_envelope"] == tif.EXPECTED_COUNTERS

    text = tif.DEFAULT_POLICY_PATH.read_text(encoding="utf-8")
    tampered = tmp_path / tif.DEFAULT_POLICY_PATH.name
    tampered.write_text(
        text.replace(
            "shift_sessions: [-10, -5, -2, -1, 0, 1, 2, 5, 10]",
            "shift_sessions: [-10, -5, -2, -1, 0, 1, 2, 5]",
        ),
        encoding="utf-8",
    )
    with pytest.raises(tif.TemporalInfluenceExecutionError, match="TIF_IDENTITY_MISMATCH"):
        tif.load_preregistration(Path(tampered.name), project_root=tmp_path)


def test_temporal_displacement_uses_one_common_window() -> None:
    plan = _synthetic_plan()
    rows, digest = tif.build_temporal_displacement(plan)

    assert tuple(row["shift_sessions"] for row in rows) == tif.SHIFT_SESSIONS
    assert {row["interval_count"] for row in rows} == {tif.EXPECTED_COMMON_INTERVALS}
    assert all(0 <= row["long_interval_count"] <= tif.EXPECTED_COMMON_INTERVALS for row in rows)
    assert len(digest) == 64


def test_episode_influence_rematches_each_deleted_episode() -> None:
    plan = _synthetic_plan()
    baseline, rows, digest = tif.build_episode_influence(plan)

    assert baseline["long_interval_count"] == tif.EXPECTED_LONG_INTERVALS
    assert len(rows) == tif.EXPECTED_LONG_EPISODES
    assert sum(row["removed_interval_count"] for row in rows) == tif.EXPECTED_LONG_INTERVALS
    for row in rows:
        assert row["long_interval_count"] == (
            tif.EXPECTED_LONG_INTERVALS - row["removed_interval_count"]
        )
        assert row["exposure_matched_comparator_weight"] == pytest.approx(
            row["long_interval_count"] / tif.EXPECTED_INTERVALS
        )
    assert len(digest) == 64


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {
                "shift_plus_one": -0.2,
                "best_negative": True,
                "nonpositive_episode": True,
            },
            "SINGLE_EPISODE_DEPENDENT",
        ),
        ({"shift_plus_one": -0.2}, "ONE_SESSION_DELAY_FRAGILE"),
        ({"best_negative": True}, "ANTICIPATORY_ALIGNMENT_DOMINATES"),
        ({}, "LOW_COST_ROBUSTNESS_NOT_DISCONFIRMED_DIAGNOSTIC_ONLY"),
    ],
)
def test_reducer_precedence(kwargs: dict[str, object], expected: str) -> None:
    temporal, episodes = _summary_rows(**kwargs)
    summary = tif.summarize_diagnostics(temporal, episodes)
    assert summary["reducer_status"] == expected


def test_best_shift_tie_does_not_create_anticipatory_dominance() -> None:
    temporal, episodes = _summary_rows(shift_zero=1.0, shift_plus_one=1.0)
    summary = tif.summarize_diagnostics(temporal, episodes)
    assert summary["best_shift_sessions"] == -10
    assert summary["anticipatory_alignment_dominates"] is False
    assert summary["reducer_status"] == (
        "LOW_COST_ROBUSTNESS_NOT_DISCONFIRMED_DIAGNOSTIC_ONLY"
    )


def test_independent_accounting_replays_all_outputs() -> None:
    plan = _synthetic_plan()
    baseline, episodes, episode_digest = tif.build_episode_influence(plan)
    temporal, temporal_digest = tif.build_temporal_displacement(plan)
    summary = tif.summarize_diagnostics(temporal, episodes)

    replay = tif.independently_replay_diagnostics(
        plan,
        primary_baseline=baseline,
        primary_temporal=temporal,
        primary_episodes=episodes,
        primary_summary=summary,
        primary_temporal_digest=temporal_digest,
        primary_episode_digest=episode_digest,
    )

    assert replay["status"] == "PASS"
    assert replay["maximum_metric_abs_diff"] <= tif.RECONCILIATION_TOLERANCE
    assert replay["reducer_status"] == summary["reducer_status"]


def test_independent_replay_rejects_changed_primary_metric() -> None:
    plan = _synthetic_plan()
    baseline, episodes, episode_digest = tif.build_episode_influence(plan)
    temporal, temporal_digest = tif.build_temporal_displacement(plan)
    summary = tif.summarize_diagnostics(temporal, episodes)
    changed = [dict(row) for row in temporal]
    changed[0]["candidate_net_total_return_pct"] = (
        float(changed[0]["candidate_net_total_return_pct"]) + 0.01
    )

    with pytest.raises(
        tif.TemporalInfluenceExecutionError,
        match="TIF_INDEPENDENT_REPLAY_MISMATCH",
    ):
        tif.independently_replay_diagnostics(
            plan,
            primary_baseline=baseline,
            primary_temporal=changed,
            primary_episodes=episodes,
            primary_summary=summary,
            primary_temporal_digest=temporal_digest,
            primary_episode_digest=episode_digest,
        )


def test_run_envelope_forbids_every_external_or_trading_action() -> None:
    assert tif.EXPECTED_COUNTERS == {
        "manifest_replays": 1,
        "canonical_dq_runs": 1,
        "local_temporal_influence_runs": 1,
        "independent_replays": 1,
        "data_downloads": 0,
        "cache_mutations": 0,
        "quantconnect_actions": 0,
        "option_backtests": 0,
        "external_provider_actions": 0,
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def test_authorization_binds_owner_scope_and_all_inputs() -> None:
    authorization = tif.load_run_authorization()
    assert authorization.payload["owner_decision"]["authorization_state"] == (
        "STANDING_OWNER_SCOPE"
    )
    assert authorization.payload["run_envelope"] == tif.EXPECTED_COUNTERS
    assert tuple(authorization.payload["input_allowlist"]) == tif.INPUT_ROLES


def test_consumed_manifest_rejects_changed_implementation() -> None:
    authorization = tif.load_run_authorization()
    with pytest.raises(
        tif.TemporalInfluenceExecutionError,
        match="TIF_IDENTITY_MISMATCH.*code.module_sha256",
    ):
        tif.load_execution_manifest(authorization=authorization)


def test_trading_2558_drawdown_projection_uses_nested_matched_placebo_schema() -> None:
    path = Path(
        "outputs/research/first_layer_composer_v2_matched_placebo_v1/aggregate_result.json"
    )
    result = json.loads(path.read_bytes())

    assert "observed_max_drawdown_magnitude_pct" not in result
    assert tif._trading_2558_observed_max_drawdown(result) == pytest.approx(
        9.602605144610187
    )


def test_failure_fix_authorization_profile_is_exact_and_separate() -> None:
    assert tif._authorization_profile(tif.FAILURE_FIX_AUTHORIZATION_PATH) == (
        "first_layer_composer_v2_temporal_influence_failure_fix_run_authorization.v1",
        "OWNER_EXACT_PREAUTHORIZED_TEMPORAL_INFLUENCE_FAILURE_FIX",
        "EXACT_PREAUTHORIZED",
        "owner_instruction:TRADING-2559:2026-09-03:failure_fix_exact_approved",
    )
    with pytest.raises(
        tif.TemporalInfluenceExecutionError,
        match="TIF_AUTHORIZATION_PATH_INVALID",
    ):
        tif._authorization_profile(Path("config/research/unapproved.yaml"))


def test_failure_fix_authorization_loads_exact_preauthorization() -> None:
    authorization = tif.load_run_authorization(tif.FAILURE_FIX_AUTHORIZATION_PATH)

    assert authorization.payload["owner_decision"]["authorization_state"] == (
        "EXACT_PREAUTHORIZED"
    )
    assert authorization.payload["owner_decision"]["exact_bounded_run_granted"] is True
    assert authorization.payload["run_envelope"] == tif.EXPECTED_COUNTERS
    assert tuple(authorization.payload["input_allowlist"]) == tif.INPUT_ROLES
    assert authorization.payload["failure_parent"]["sha256"] == (
        "4d4516737551ed7328d2925482c39c09ed299e1db7da46220d8541f87e2f6ef0"
    )


def test_failure_fix_manifest_binds_approved_code_and_scope() -> None:
    authorization = tif.load_run_authorization(tif.FAILURE_FIX_AUTHORIZATION_PATH)
    manifest = tif.load_execution_manifest(
        tif.FAILURE_FIX_MANIFEST_PATH,
        authorization=authorization,
    )

    assert manifest.payload["authorization_binding"]["path"] == (
        tif.FAILURE_FIX_AUTHORIZATION_PATH.as_posix()
    )
    assert manifest.payload["code_binding"] == {
        "implementation_commit_sha": (
            "6cf877641922618661d1765b0bd08e4198f17582"
        ),
        "module_path": (
            "src/ai_trading_system/first_layer_temporal_influence_falsification.py"
        ),
        "module_sha256": (
            "8ee76a16b80a9b7c2ae490f78a7c4ed610dc268e3c1230c3e24f6f01dfdfbfc9"
        ),
    }
    assert manifest.payload["shift_sessions"] == list(tif.SHIFT_SESSIONS)
    assert manifest.payload["run_envelope"] == tif.EXPECTED_COUNTERS
    assert tuple(item.role for item in manifest.inputs) == tif.INPUT_ROLES
