from __future__ import annotations

from datetime import date

from ai_trading_system.config import RiskEventsConfig, UniverseConfig, WatchlistConfig
from ai_trading_system.risk_events import (
    LoadedRiskEventOccurrence,
    RiskEventOccurrence,
    RiskEventOccurrenceReviewReport,
    RiskEventOccurrenceStatus,
    RiskEventOccurrenceStore,
    build_risk_event_occurrence_review_report,
    validate_risk_event_occurrence_store,
)
from ai_trading_system.valuation import (
    LoadedValuationSnapshot,
    ValuationReviewReport,
    ValuationSnapshotStore,
    build_valuation_review_report,
    validate_valuation_snapshot_store,
)


def valuation_snapshot_store_as_of(
    store: ValuationSnapshotStore,
    as_of: date,
) -> ValuationSnapshotStore:
    """Return latest knowable valuation snapshot per ticker for a historical date."""
    latest_by_ticker: dict[str, LoadedValuationSnapshot] = {}
    for loaded in store.loaded:
        snapshot = loaded.snapshot
        if snapshot.as_of > as_of or snapshot.captured_at > as_of:
            continue

        current = latest_by_ticker.get(snapshot.ticker)
        if current is None or _valuation_snapshot_sort_key(loaded) > (
            _valuation_snapshot_sort_key(current)
        ):
            latest_by_ticker[snapshot.ticker] = loaded

    return ValuationSnapshotStore(
        input_path=store.input_path,
        loaded=tuple(
            sorted(
                latest_by_ticker.values(),
                key=lambda loaded: (loaded.snapshot.ticker, loaded.snapshot.snapshot_id),
            )
        ),
        load_errors=store.load_errors,
    )


def build_historical_valuation_review_report(
    store: ValuationSnapshotStore,
    universe: UniverseConfig,
    watchlist: WatchlistConfig,
    as_of: date,
    max_snapshot_age_days: int = 45,
) -> ValuationReviewReport:
    """Build a valuation review report using only snapshots knowable at as_of."""
    filtered_store = valuation_snapshot_store_as_of(store=store, as_of=as_of)
    validation_report = validate_valuation_snapshot_store(
        store=filtered_store,
        universe=universe,
        watchlist=watchlist,
        as_of=as_of,
        max_snapshot_age_days=max_snapshot_age_days,
    )
    return build_valuation_review_report(validation_report)


def risk_event_occurrence_store_as_of(
    store: RiskEventOccurrenceStore,
    as_of: date,
) -> RiskEventOccurrenceStore:
    """Return risk event occurrences with only evidence visible at a historical date."""
    loaded_occurrences: list[LoadedRiskEventOccurrence] = []
    for loaded in store.loaded:
        historical_occurrence = _risk_event_occurrence_as_of(
            occurrence=loaded.occurrence,
            as_of=as_of,
        )
        if historical_occurrence is None:
            continue
        loaded_occurrences.append(
            LoadedRiskEventOccurrence(
                occurrence=historical_occurrence,
                path=loaded.path,
            )
        )

    return RiskEventOccurrenceStore(
        input_path=store.input_path,
        loaded=tuple(loaded_occurrences),
        load_errors=store.load_errors,
    )


def build_historical_risk_event_occurrence_review_report(
    store: RiskEventOccurrenceStore,
    risk_events: RiskEventsConfig,
    as_of: date,
    max_active_age_days: int = 14,
) -> RiskEventOccurrenceReviewReport:
    """Build a risk event review report using only occurrences knowable at as_of."""
    filtered_store = risk_event_occurrence_store_as_of(store=store, as_of=as_of)
    validation_report = validate_risk_event_occurrence_store(
        store=filtered_store,
        risk_events=risk_events,
        as_of=as_of,
        max_active_age_days=max_active_age_days,
    )
    return build_risk_event_occurrence_review_report(validation_report)


def _valuation_snapshot_sort_key(
    loaded: LoadedValuationSnapshot,
) -> tuple[date, date, str]:
    snapshot = loaded.snapshot
    return (snapshot.as_of, snapshot.captured_at, snapshot.snapshot_id)


def _risk_event_occurrence_as_of(
    occurrence: RiskEventOccurrence,
    as_of: date,
) -> RiskEventOccurrence | None:
    if occurrence.triggered_at > as_of:
        return None
    if occurrence.resolved_at is not None and occurrence.resolved_at <= as_of:
        return None

    visible_evidence = [
        source
        for source in occurrence.evidence_sources
        if source.captured_at <= as_of
        and (source.published_at is None or source.published_at <= as_of)
    ]
    if not visible_evidence:
        return None

    visible_captured_at = max(source.captured_at for source in visible_evidence)
    last_confirmed_at = max(occurrence.triggered_at, min(as_of, visible_captured_at))
    status = _risk_event_status_as_of(occurrence=occurrence, as_of=as_of)

    return occurrence.model_copy(
        update={
            "status": status,
            "last_confirmed_at": last_confirmed_at,
            "reviewed_at": last_confirmed_at,
            "resolved_at": None,
            "evidence_sources": visible_evidence,
        }
    )


def _risk_event_status_as_of(
    occurrence: RiskEventOccurrence,
    as_of: date,
) -> RiskEventOccurrenceStatus:
    if occurrence.resolved_at is not None and occurrence.resolved_at > as_of:
        if occurrence.status == "resolved":
            return "active"
        if occurrence.status == "dismissed":
            return "watch"
    return occurrence.status
