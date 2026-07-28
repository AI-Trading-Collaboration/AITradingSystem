from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.contracts.data_quality_attribution import (
    ATTRIBUTION_SCOPE_COMPLETE,
    ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN,
    PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS,
    PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION,
    PRIMARY_MARKET_PRICES_SOURCE_ROLE,
    SECONDARY_MARKET_PRICES_SOURCE_ROLE,
    SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT,
    DataQualityAttributionContractError,
    DataQualitySourceArtifactBinding,
    build_price_non_market_session_attribution,
    build_reviewed_calendar_binding,
    canonical_price_row_digest,
    load_price_non_market_session_attribution_decision,
)
from ai_trading_system.data import quality as quality_module
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityReport,
    Severity,
    render_data_quality_report,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DECISION_PATH = (
    PROJECT_ROOT
    / "config/data_quality/price_non_market_session_attribution_decision_v1.yaml"
)


def test_reviewed_decision_binds_exact_pack_calendar_and_digest_contract() -> None:
    decision = load_price_non_market_session_attribution_decision()

    assert decision.review_pack_id == (
        "dq_price_issue_attribution_review_0731caba2f2b6280dda3385b"
    )
    assert decision.approved_source_role == PRIMARY_MARKET_PRICES_SOURCE_ROLE
    assert decision.row_digest_schema_version == (
        PRICE_NON_MARKET_SESSION_ROW_DIGEST_SCHEMA_VERSION
    )
    assert decision.row_digest_fields == PRICE_NON_MARKET_SESSION_ROW_DIGEST_FIELDS
    assert decision.source_ordinal_scope == SOURCE_ORDINAL_SCOPE_EXACT_SNAPSHOT
    assert decision.calendar_id == "XNYS"
    assert decision.special_closure_policy_version == "1.0.0"
    assert decision.path == DECISION_PATH
    assert len(decision.sha256) == 64


@pytest.mark.parametrize(
    ("needle", "replacement", "error_code"),
    [
        (
            "approved_source_role: primary_market_prices",
            "approved_source_role: unreviewed_prices",
            "ATTRIBUTION_DECISION_VALUE_MISMATCH",
        ),
        (
            "e1f3841dc27a9bee78c79fe07250acfe006941ed252235c23c13b3b8017a3449",
            "f" * 64,
            "REVIEW_PACK_BYTES_DRIFTED",
        ),
        (
            "23ab933d7013e15b73d912aa09258adc4c7ba252a36330190d66735d1b70f01c",
            "f" * 64,
            "CALENDAR_POLICY_REVIEW_REQUIRED",
        ),
        (
            "c0469a17a775df2dcde503c254c22db0cc7d8ad6e3a5884f2ed43c88e4dfbda4",
            "f" * 64,
            "SPECIAL_CLOSURE_POLICY_REVIEW_REQUIRED",
        ),
    ],
)
def test_decision_or_bound_authority_drift_requires_review(
    tmp_path: Path,
    needle: str,
    replacement: str,
    error_code: str,
) -> None:
    tampered = tmp_path / DECISION_PATH.name
    tampered.write_text(
        DECISION_PATH.read_text(encoding="utf-8").replace(needle, replacement),
        encoding="utf-8",
    )

    with pytest.raises(DataQualityAttributionContractError, match=error_code):
        load_price_non_market_session_attribution_decision(
            tampered,
            project_root=PROJECT_ROOT,
        )


def test_canonical_row_digest_is_versioned_typed_and_content_deterministic() -> None:
    row = _price_row("2026-05-02", " MSFT ", close=100.0)
    reordered = {field: row[field] for field in reversed(tuple(row))}

    digest = canonical_price_row_digest(row)

    assert digest == canonical_price_row_digest(reordered)
    assert digest == canonical_price_row_digest({**row, "ticker": "MSFT"})
    assert digest != canonical_price_row_digest({**row, "close": 101.0})
    assert len(digest) == 64


def test_canonical_row_digest_rejects_non_finite_value() -> None:
    with pytest.raises(
        DataQualityAttributionContractError,
        match="ROW_DIGEST_NON_FINITE_VALUE",
    ):
        canonical_price_row_digest(
            _price_row("2026-05-02", "MSFT", close=float("inf"))
        )


def test_complete_primary_scope_includes_all_rows_and_distinct_date_count() -> None:
    frame = _trigger_frame(
        [
            _price_row("2026-05-02", "MSFT"),
            _price_row("2026-05-02", "NVDA"),
            _price_row("2026-05-02", "NVDA"),
        ]
    )

    issue = _emit_issue(frame, source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE)

    assert issue.rows == 1
    assert issue.attribution_scope_status == ATTRIBUTION_SCOPE_COMPLETE
    assert issue.attribution_incomplete_reasons == ()
    assert issue.affected_instruments == ("MSFT", "NVDA")
    assert issue.typed_attribution is not None
    assert issue.typed_attribution.affected_dates == (date(2026, 5, 2),)
    assert issue.typed_attribution.affected_fields == ("date",)
    assert issue.typed_attribution.affected_rate_series == ()
    assert tuple(
        row.source_ordinal for row in issue.typed_attribution.affected_rows
    ) == (0, 1, 2)
    assert (
        issue.typed_attribution.affected_rows[1].canonical_row_digest
        == issue.typed_attribution.affected_rows[2].canonical_row_digest
    )


def test_one_ticker_on_multiple_non_session_dates_keeps_exact_date_set() -> None:
    issue = _emit_issue(
        _trigger_frame(
            [
                _price_row("2026-05-02", "MSFT"),
                _price_row("2026-05-03", "MSFT"),
            ]
        ),
        source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE,
        requested_window=(date(2026, 5, 1), date(2026, 5, 3)),
    )

    assert issue.rows == 2
    assert issue.typed_attribution is not None
    assert issue.typed_attribution.affected_dates == (
        date(2026, 5, 2),
        date(2026, 5, 3),
    )


@pytest.mark.parametrize(
    ("source_role", "checksum", "ticker", "reason"),
    [
        (
            SECONDARY_MARKET_PRICES_SOURCE_ROLE,
            "a" * 64,
            "MSFT",
            "UNAPPROVED_SOURCE_ROLE",
        ),
        (
            PRIMARY_MARKET_PRICES_SOURCE_ROLE,
            None,
            "MSFT",
            "INVALID_SHA256",
        ),
        (
            PRIMARY_MARKET_PRICES_SOURCE_ROLE,
            "a" * 64,
            " ",
            "MISSING_TRIGGER_ROW_TICKER",
        ),
    ],
)
def test_incomplete_attribution_clears_legacy_scope_and_remains_global(
    source_role: str,
    checksum: str | None,
    ticker: str,
    reason: str,
) -> None:
    issue = _emit_issue(
        _trigger_frame([_price_row("2026-05-02", ticker)]),
        source_role=source_role,
        checksum=checksum,
    )

    assert issue.attribution_scope_status == ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN
    assert issue.attribution_incomplete_reasons == (reason,)
    assert issue.affected_instruments == ()
    assert issue.typed_attribution is None


def test_source_reorder_changes_snapshot_local_ordinals_not_content_digest() -> None:
    decision = load_price_non_market_session_attribution_decision()
    rows = [
        {**_price_row("2026-05-02", "MSFT"), "_source_ordinal": 0},
        {**_price_row("2026-05-02", "NVDA"), "_source_ordinal": 1},
    ]
    for row in rows:
        row["_date"] = pd.Timestamp(str(row["date"]))
    reversed_rows = [
        {**rows[1], "_source_ordinal": 0},
        {**rows[0], "_source_ordinal": 1},
    ]
    source = DataQualitySourceArtifactBinding(
        source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE,
        path=(PROJECT_ROOT / "cache/prices_daily.csv").as_posix(),
        sha256="a" * 64,
    )

    first = build_price_non_market_session_attribution(
        decision=decision,
        source=source,
        requested_window=(date(2026, 5, 1), date(2026, 5, 3)),
        calendar=build_reviewed_calendar_binding(decision),
        trigger_rows=rows,
    )
    second = build_price_non_market_session_attribution(
        decision=decision,
        source=source,
        requested_window=(date(2026, 5, 1), date(2026, 5, 3)),
        calendar=build_reviewed_calendar_binding(decision),
        trigger_rows=reversed_rows,
    )

    first_by_ticker = {
        row.ticker: (row.source_ordinal, row.canonical_row_digest)
        for row in first.affected_rows
    }
    second_by_ticker = {
        row.ticker: (row.source_ordinal, row.canonical_row_digest)
        for row in second.affected_rows
    }
    assert first_by_ticker["MSFT"][0] != second_by_ticker["MSFT"][0]
    assert first_by_ticker["MSFT"][1] == second_by_ticker["MSFT"][1]


def test_trigger_date_outside_requested_window_fails_closed() -> None:
    decision = load_price_non_market_session_attribution_decision()
    row = {
        **_price_row("2026-05-02", "MSFT"),
        "_date": pd.Timestamp("2026-05-02"),
        "_source_ordinal": 0,
    }

    with pytest.raises(
        DataQualityAttributionContractError,
        match="TRIGGER_DATE_OUTSIDE_REQUESTED_WINDOW",
    ):
        build_price_non_market_session_attribution(
            decision=decision,
            source=DataQualitySourceArtifactBinding(
                source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE,
                path="prices.csv",
                sha256="a" * 64,
            ),
            requested_window=(date(2026, 5, 3), date(2026, 5, 3)),
            calendar=build_reviewed_calendar_binding(decision),
            trigger_rows=[row],
        )


def test_markdown_report_exposes_complete_and_global_attribution() -> None:
    complete = _emit_issue(
        _trigger_frame([_price_row("2026-05-02", "MSFT")]),
        source_role=PRIMARY_MARKET_PRICES_SOURCE_ROLE,
    )
    incomplete = _emit_issue(
        _trigger_frame([_price_row("2026-05-02", "NVDA")]),
        source_role=SECONDARY_MARKET_PRICES_SOURCE_ROLE,
    )
    summary = DataFileSummary(
        path=PROJECT_ROOT / "cache/prices.csv",
        exists=True,
        rows=1,
        sha256="a" * 64,
    )
    report = DataQualityReport(
        checked_at=pd.Timestamp("2026-05-03T12:00:00Z").to_pydatetime(),
        as_of=date(2026, 5, 3),
        price_summary=summary,
        rate_summary=summary,
        expected_price_tickers=("MSFT",),
        expected_rate_series=(),
        requested_window_start=date(2026, 5, 1),
        requested_window_end=date(2026, 5, 3),
        issues=(complete, incomplete),
    )

    markdown = render_data_quality_report(report)

    assert "## Typed issue attribution" in markdown
    assert "price_non_market_session_row_digest.v1" in markdown
    assert "Source ordinal" in markdown
    assert complete.typed_attribution is not None
    assert complete.typed_attribution.source.sha256 in markdown
    assert "`GLOBAL_OR_UNKNOWN_SCOPE`" in markdown
    assert "`UNAPPROVED_SOURCE_ROLE`" in markdown
    assert "Legacy affected instruments：已清空；保持 global。" in markdown


def _emit_issue(
    frame: pd.DataFrame,
    *,
    source_role: str,
    checksum: str | None = "a" * 64,
    requested_window: tuple[date, date] = (
        date(2026, 5, 1),
        date(2026, 5, 2),
    ),
):
    issues = []
    quality_module._check_price_market_calendar_dates(
        frame,
        requested_window,
        issues,
        source="test",
        source_role=source_role,
        source_summary=DataFileSummary(
            path=PROJECT_ROOT / "cache/prices.csv",
            exists=True,
            rows=len(frame),
            sha256=checksum,
        ),
        severity=Severity.ERROR,
    )
    assert len(issues) == 1
    return issues[0]


def _trigger_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["_source_ordinal"] = range(len(frame))
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    return frame


def _price_row(
    value_date: str,
    ticker: str,
    *,
    close: float = 100.0,
) -> dict[str, object]:
    return {
        "date": value_date,
        "ticker": ticker,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": close,
        "volume": 1_000_000,
    }
