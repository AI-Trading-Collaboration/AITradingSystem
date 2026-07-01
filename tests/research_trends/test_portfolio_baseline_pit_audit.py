from __future__ import annotations

from pathlib import Path

from portfolio_baseline_source_decision_fixtures import (
    write_paper_portfolio_fixture,
    write_portfolio_config_fixture,
    write_simulation_policy_fixture,
    write_source_binding_fixture,
)

from ai_trading_system.portfolio_baseline_source_decision import (
    build_portfolio_baseline_candidate_matrix,
    build_portfolio_baseline_pit_reproducibility_audit,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
)


def test_synthetic_observe_only_pit_status(tmp_path: Path) -> None:
    row = _row(_pit_rows(tmp_path), "synthetic_observe_only_baseline")

    assert row["pit_status"] == "SYNTHETIC_OBSERVE_ONLY"
    assert row["replayable"] is True


def test_static_etf_allocation_is_replayable_pit_approximation(
    tmp_path: Path,
) -> None:
    row = _row(_pit_rows(tmp_path), "static_etf_allocation_baseline")

    assert row["pit_status"] == "PIT_APPROXIMATION_READY"
    assert row["replayable"] is True


def test_dynamic_target_exposure_missing_artifact_is_blocked(tmp_path: Path) -> None:
    row = _row(_pit_rows(tmp_path), "dynamic_strategy_target_exposure_baseline")

    assert row["pit_status"] == "BLOCKED"
    assert row["replayable"] is False


def test_actual_holdings_is_manual_reference_only_when_source_exists(
    tmp_path: Path,
) -> None:
    holdings = tmp_path / "holdings.csv"
    holdings.write_text("date,symbol,quantity\n2026-01-01,QQQ,1\n", encoding="utf-8")
    row = _row(
        _pit_rows(tmp_path, actual_holdings_source=holdings),
        "actual_holdings_derived_baseline",
    )

    assert row["pit_status"] == "MANUAL_REFERENCE_ONLY"
    assert row["replayable"] is False


def test_static_source_hash_and_version_fields_are_checked(tmp_path: Path) -> None:
    row = _row(_pit_rows(tmp_path), "static_etf_allocation_baseline")

    assert row["artifact_hash_available"] is True
    assert row["source_version_available"] is True


def _pit_rows(
    tmp_path: Path,
    *,
    actual_holdings_source: Path | None = None,
) -> list[dict[str, object]]:
    rows = build_portfolio_baseline_candidate_matrix(
        source_binding=load_source_binding_outputs(write_source_binding_fixture(tmp_path)),
        simulation_policy=load_simulation_policy_outputs(
            write_simulation_policy_fixture(tmp_path)
        ),
        portfolio_config_dir=write_portfolio_config_fixture(tmp_path),
        paper_portfolio_config=write_paper_portfolio_fixture(tmp_path),
        actual_holdings_source=actual_holdings_source,
        allow_synthetic_baseline=True,
        target_assets=["QQQ", "SPY", "SMH"],
    )
    return build_portfolio_baseline_pit_reproducibility_audit(rows)


def _row(
    rows: list[dict[str, object]],
    baseline_id: str,
) -> dict[str, object]:
    return next(row for row in rows if row["baseline_id"] == baseline_id)
