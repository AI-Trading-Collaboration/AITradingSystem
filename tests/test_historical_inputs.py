from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.config import load_risk_events, load_universe, load_watchlist
from ai_trading_system.historical_inputs import (
    build_historical_risk_event_occurrence_review_report,
    build_historical_valuation_review_report,
    risk_event_occurrence_store_as_of,
    valuation_snapshot_store_as_of,
)
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    RiskEventEvidenceSource,
    RiskEventOccurrence,
    RiskEventOccurrenceStatus,
    RiskEventOccurrenceStore,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    SnapshotMetric,
    ValuationSnapshot,
    ValuationSnapshotStore,
)


def test_valuation_store_as_of_filters_future_snapshots_and_keeps_latest_per_ticker(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 2)
    store = ValuationSnapshotStore(
        input_path=tmp_path,
        loaded=(
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_old",
                    ticker="NVDA",
                    as_of=date(2026, 4, 29),
                    captured_at=date(2026, 4, 30),
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_latest_a",
                    ticker="NVDA",
                    as_of=date(2026, 5, 1),
                    captured_at=date(2026, 5, 1),
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_latest_b",
                    ticker="NVDA",
                    as_of=date(2026, 5, 1),
                    captured_at=date(2026, 5, 1),
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_future_as_of",
                    ticker="NVDA",
                    as_of=date(2026, 5, 3),
                    captured_at=date(2026, 5, 1),
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="amd_future_capture",
                    ticker="AMD",
                    as_of=date(2026, 5, 1),
                    captured_at=date(2026, 5, 3),
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="amd_visible",
                    ticker="AMD",
                    as_of=date(2026, 4, 30),
                    captured_at=date(2026, 5, 2),
                ),
            ),
        ),
        load_errors=(),
    )

    filtered = valuation_snapshot_store_as_of(store=store, as_of=as_of)

    assert {loaded.snapshot.snapshot_id for loaded in filtered.loaded} == {
        "amd_visible",
        "nvda_latest_b",
    }


def test_historical_valuation_report_validates_and_scores_without_future_errors(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 2)
    store = ValuationSnapshotStore(
        input_path=tmp_path,
        loaded=(
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_visible",
                    ticker="NVDA",
                    as_of=date(2026, 5, 1),
                    captured_at=date(2026, 5, 1),
                    valuation_percentile=82.0,
                ),
            ),
            _loaded_snapshot(
                tmp_path,
                _valuation_snapshot(
                    snapshot_id="nvda_future",
                    ticker="NVDA",
                    as_of=date(2026, 5, 3),
                    captured_at=date(2026, 5, 3),
                    valuation_percentile=99.0,
                ),
            ),
        ),
        load_errors=(),
    )

    report = build_historical_valuation_review_report(
        store=store,
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=as_of,
    )

    assert report.validation_report.status == "PASS"
    assert [item.snapshot_id for item in report.items] == ["nvda_visible"]
    assert report.items[0].health == "EXPENSIVE_OR_CROWDED"
    assert "valuation_date_in_future" not in {
        issue.code for issue in report.validation_report.issues
    }


def test_risk_event_store_as_of_filters_evidence_and_reinterprets_future_resolution(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 2)
    store = RiskEventOccurrenceStore(
        input_path=tmp_path,
        loaded=(
            _loaded_occurrence(
                tmp_path,
                _risk_occurrence(
                    occurrence_id="future_evidence_only",
                    status="active",
                    triggered_at=date(2026, 5, 1),
                    last_confirmed_at=date(2026, 5, 3),
                    evidence_sources=[
                        _evidence(captured_at=date(2026, 5, 3)),
                    ],
                ),
            ),
            _loaded_occurrence(
                tmp_path,
                _risk_occurrence(
                    occurrence_id="already_resolved",
                    status="resolved",
                    triggered_at=date(2026, 4, 28),
                    last_confirmed_at=date(2026, 5, 1),
                    resolved_at=date(2026, 5, 1),
                    evidence_sources=[
                        _evidence(captured_at=date(2026, 5, 1)),
                    ],
                ),
            ),
            _loaded_occurrence(
                tmp_path,
                _risk_occurrence(
                    occurrence_id="resolved_later",
                    status="resolved",
                    triggered_at=date(2026, 5, 1),
                    last_confirmed_at=date(2026, 5, 4),
                    resolved_at=date(2026, 5, 4),
                    evidence_sources=[
                        _evidence(captured_at=date(2026, 5, 1)),
                        _evidence(captured_at=date(2026, 5, 4)),
                    ],
                ),
            ),
            _loaded_occurrence(
                tmp_path,
                _risk_occurrence(
                    occurrence_id="dismissed_later",
                    status="dismissed",
                    triggered_at=date(2026, 5, 1),
                    last_confirmed_at=date(2026, 5, 5),
                    resolved_at=date(2026, 5, 5),
                    evidence_sources=[
                        _evidence(captured_at=date(2026, 5, 2)),
                        _evidence(captured_at=date(2026, 5, 5)),
                    ],
                ),
            ),
        ),
        load_errors=(),
    )

    filtered = risk_event_occurrence_store_as_of(store=store, as_of=as_of)
    occurrences = {loaded.occurrence.occurrence_id: loaded.occurrence for loaded in filtered.loaded}

    assert set(occurrences) == {"resolved_later", "dismissed_later"}
    assert occurrences["resolved_later"].status == "active"
    assert occurrences["resolved_later"].resolved_at is None
    assert occurrences["resolved_later"].last_confirmed_at == date(2026, 5, 1)
    assert [source.captured_at for source in occurrences["resolved_later"].evidence_sources] == [
        date(2026, 5, 1)
    ]
    assert occurrences["dismissed_later"].status == "watch"
    assert occurrences["dismissed_later"].resolved_at is None
    assert occurrences["dismissed_later"].last_confirmed_at == as_of
    assert [source.captured_at for source in occurrences["dismissed_later"].evidence_sources] == [
        as_of
    ]


def test_historical_risk_event_report_has_no_future_date_validation_errors(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 2)
    store = RiskEventOccurrenceStore(
        input_path=tmp_path,
        loaded=(
            _loaded_occurrence(
                tmp_path,
                _risk_occurrence(
                    occurrence_id="resolved_later",
                    status="resolved",
                    triggered_at=date(2026, 5, 1),
                    last_confirmed_at=date(2026, 5, 4),
                    resolved_at=date(2026, 5, 4),
                    evidence_sources=[
                        _evidence(captured_at=date(2026, 5, 1)),
                        _evidence(captured_at=date(2026, 5, 4)),
                    ],
                ),
            ),
        ),
        load_errors=(),
    )

    report = build_historical_risk_event_occurrence_review_report(
        store=store,
        risk_events=load_risk_events(),
        as_of=as_of,
    )

    assert report.validation_report.passed is True
    assert [item.occurrence_id for item in report.items] == ["resolved_later"]
    assert report.items[0].status == "active"
    assert "risk_event_occurrence_date_in_future" not in {
        issue.code for issue in report.validation_report.issues
    }
    assert "risk_event_resolved_date_in_future" not in {
        issue.code for issue in report.validation_report.issues
    }
    assert "risk_event_evidence_date_in_future" not in {
        issue.code for issue in report.validation_report.issues
    }


def _valuation_snapshot(
    snapshot_id: str,
    ticker: str,
    as_of: date,
    captured_at: date,
    valuation_percentile: float = 50.0,
) -> ValuationSnapshot:
    return ValuationSnapshot(
        snapshot_id=snapshot_id,
        ticker=ticker,
        as_of=as_of,
        source_type="manual_input",
        source_name="manual_valuation_sheet",
        captured_at=captured_at,
        valuation_metrics=[
            SnapshotMetric(
                metric_id="forward_pe",
                value=36.0,
                unit="ratio",
                period="next_12m",
            )
        ],
        valuation_percentile=valuation_percentile,
        overall_assessment="expensive",
    )


def _loaded_snapshot(
    tmp_path: Path,
    snapshot: ValuationSnapshot,
) -> LoadedValuationSnapshot:
    return LoadedValuationSnapshot(
        snapshot=snapshot,
        path=tmp_path / f"{snapshot.snapshot_id}.yaml",
    )


def _risk_occurrence(
    occurrence_id: str,
    status: RiskEventOccurrenceStatus,
    triggered_at: date,
    last_confirmed_at: date,
    evidence_sources: list[RiskEventEvidenceSource],
    resolved_at: date | None = None,
) -> RiskEventOccurrence:
    return RiskEventOccurrence(
        occurrence_id=occurrence_id,
        event_id="ai_chip_export_control_upgrade",
        status=status,
        triggered_at=triggered_at,
        last_confirmed_at=last_confirmed_at,
        resolved_at=resolved_at,
        evidence_sources=evidence_sources,
        summary="人工确认的测试风险事件。",
    )


def _loaded_occurrence(
    tmp_path: Path,
    occurrence: RiskEventOccurrence,
) -> LoadedRiskEventOccurrence:
    return LoadedRiskEventOccurrence(
        occurrence=occurrence,
        path=tmp_path / f"{occurrence.occurrence_id}.yaml",
    )


def _evidence(captured_at: date) -> RiskEventEvidenceSource:
    return RiskEventEvidenceSource(
        source_name="manual_policy_review",
        source_type="manual_input",
        captured_at=captured_at,
        published_at=captured_at,
    )
