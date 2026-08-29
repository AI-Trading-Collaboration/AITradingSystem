from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from ai_trading_system.config import load_data_quality
from ai_trading_system.contracts.data_quality_attribution import (
    ATTRIBUTION_SCOPE_COMPLETE,
    ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN,
    DataQualitySourceArtifactBinding,
)
from ai_trading_system.contracts.rate_data_quality_attribution import (
    PRIMARY_MACRO_RATES_SOURCE_ROLE,
    RATE_ROW_ROLE_PREVIOUS_VALID,
    RATE_ROW_ROLE_TRIGGER,
    RATE_SERIES_ONLY_ISOLATION_SCOPE,
    RateDataQualityAttributionContractError,
    RateDataQualityIssueAttribution,
    RateIssuePolicyEvidence,
    build_rate_issue_attribution,
    canonical_rate_row_digest,
    load_rate_row_issue_attribution_decision,
    rate_series_disjoint_isolation_eligible,
)
from ai_trading_system.data.quality import (
    DataFileSummary,
    DataQualityReport,
    _validate_rates,
    render_data_quality_report,
)


def _source() -> DataQualitySourceArtifactBinding:
    return DataQualitySourceArtifactBinding(
        source_role=PRIMARY_MACRO_RATES_SOURCE_ROLE,
        path="data/cache/rates.csv",
        sha256="a" * 64,
    )


def _row(
    ordinal: int,
    *,
    observed_date: object,
    series: object = "DGS10",
    value: object = 4.25,
) -> dict[str, object]:
    return {
        "_source_ordinal": ordinal,
        "_date": pd.to_datetime(observed_date, errors="coerce"),
        "date": observed_date,
        "series": series,
        "value": value,
    }


def test_decision_binds_exact_current_review_pack() -> None:
    decision = load_rate_row_issue_attribution_decision()

    assert decision.authority_id.endswith("@1.0.0")
    assert decision.review_pack_id == ("dq_rate_issue_attribution_review_34ea0d1bce5e7a0bc67d83b5")
    assert len(decision.approved_sites) == 6
    assert decision.approved_source_role == PRIMARY_MACRO_RATES_SOURCE_ROLE


def test_invalid_date_is_complete_series_only_and_never_window_scope() -> None:
    decision = load_rate_row_issue_attribution_decision()
    attribution = build_rate_issue_attribution(
        decision=decision,
        issue_code="rates_invalid_date",
        source=_source(),
        requested_window=(date(2021, 2, 22), date(2026, 7, 24)),
        row_groups=((_row(0, observed_date="not-a-date"),),),
    )

    assert attribution.scope_status == ATTRIBUTION_SCOPE_COMPLETE
    assert attribution.isolation_scope == RATE_SERIES_ONLY_ISOLATION_SCOPE
    assert attribution.affected_rate_series == ("DGS10",)
    assert attribution.affected_dates == ()
    assert attribution.affected_fields == ("date",)
    assert rate_series_disjoint_isolation_eligible(
        attribution,
        required_rate_series=["DGS2"],
    )
    assert not rate_series_disjoint_isolation_eligible(
        attribution,
        required_rate_series=["DGS10"],
    )


def test_rate_row_digest_is_versioned_and_non_finite_explicit() -> None:
    positive_inf = canonical_rate_row_digest(
        _row(0, observed_date="2026-04-01", value=float("inf"))
    )
    negative_inf = canonical_rate_row_digest(
        _row(0, observed_date="2026-04-01", value=float("-inf"))
    )
    missing = canonical_rate_row_digest(_row(0, observed_date="2026-04-01", value=float("nan")))

    assert len({positive_inf, negative_inf, missing}) == 3
    assert all(len(value) == 64 for value in (positive_inf, negative_inf, missing))


def test_move_contract_requires_predecessor_and_threshold_evidence() -> None:
    decision = load_rate_row_issue_attribution_decision()
    previous = _row(3, observed_date="2026-04-01", value=1.0)
    trigger = _row(5, observed_date="2026-04-02", value=4.0)

    attribution = build_rate_issue_attribution(
        decision=decision,
        issue_code="rates_extreme_daily_change",
        source=_source(),
        requested_window=(date(2021, 2, 22), date(2026, 7, 24)),
        row_groups=((previous, trigger),),
        policy_evidence=(
            RateIssuePolicyEvidence(
                trigger_source_ordinal=5,
                policy_values=(("extreme_daily_change_abs", 2.0),),
                observed_change=3.0,
            ),
        ),
    )

    assert {row.row_role for row in attribution.affected_rows} == {
        RATE_ROW_ROLE_PREVIOUS_VALID,
        RATE_ROW_ROLE_TRIGGER,
    }
    assert attribution.policy_evidence[0].observed_change == 3.0

    with pytest.raises(
        RateDataQualityAttributionContractError,
        match="RATE_ROW_DEPENDENCY_MISMATCH",
    ):
        build_rate_issue_attribution(
            decision=decision,
            issue_code="rates_extreme_daily_change",
            source=_source(),
            requested_window=(date(2021, 2, 22), date(2026, 7, 24)),
            row_groups=((trigger,),),
        )


def test_blank_series_and_unapproved_source_fail_closed() -> None:
    decision = load_rate_row_issue_attribution_decision()
    with pytest.raises(
        RateDataQualityAttributionContractError,
        match="MISSING_RATE_SERIES",
    ):
        build_rate_issue_attribution(
            decision=decision,
            issue_code="rates_invalid_value",
            source=_source(),
            requested_window=(date(2021, 2, 22), date(2026, 7, 24)),
            row_groups=((_row(0, observed_date="2026-04-01", series=" "),),),
        )

    with pytest.raises(
        RateDataQualityAttributionContractError,
        match="UNAPPROVED_SOURCE_ROLE",
    ):
        build_rate_issue_attribution(
            decision=decision,
            issue_code="rates_invalid_value",
            source=DataQualitySourceArtifactBinding(
                source_role="secondary_macro_rates",
                path="data/cache/rates.csv",
                sha256="a" * 64,
            ),
            requested_window=(date(2021, 2, 22), date(2026, 7, 24)),
            row_groups=((_row(0, observed_date="2026-04-01"),),),
        )


def test_decision_rejects_review_pack_tamper(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    decision_target = tmp_path / "config/data_quality/rate_row_issue_attribution_decision_v1.yaml"
    pack_target = tmp_path / "inputs/data_quality/rate_issue_attribution_review_pack_v1.json"
    decision_target.parent.mkdir(parents=True)
    pack_target.parent.mkdir(parents=True)
    decision_target.write_bytes(
        (
            project_root / "config/data_quality/rate_row_issue_attribution_decision_v1.yaml"
        ).read_bytes()
    )
    pack_target.write_bytes(
        (
            project_root / "inputs/data_quality/rate_issue_attribution_review_pack_v1.json"
        ).read_bytes()
        + b"\n"
    )

    with pytest.raises(
        RateDataQualityAttributionContractError,
        match="RATE_REVIEW_PACK_BYTES_DRIFTED",
    ):
        load_rate_row_issue_attribution_decision(
            decision_target,
            project_root=tmp_path,
        )


def test_canonical_runtime_emits_all_six_approved_rate_attributions(
    tmp_path: Path,
) -> None:
    rates = pd.DataFrame(
        [
            {"date": "not-a-date", "series": "DGS1", "value": 1.0},
            {"date": "2026-04-01", "series": "DGS2", "value": "bad"},
            {"date": "2026-04-01", "series": "DGS3", "value": float("inf")},
            {"date": "2026-04-01", "series": "DGS4", "value": 30.0},
            {"date": "2026-04-01", "series": "DGS5", "value": 1.0},
            {"date": "2026-04-02", "series": "DGS5", "value": 4.0},
            {"date": "2026-04-01", "series": "DGS6", "value": 1.0},
            {"date": "2026-04-02", "series": "DGS6", "value": 2.0},
        ]
    )
    path = tmp_path / "rates.csv"
    rates.to_csv(path, index=False)
    issues = []
    summary = DataFileSummary(
        path=path,
        exists=True,
        rows=len(rates),
        sha256="b" * 64,
    )

    _validate_rates(
        rates,
        summary,
        ["DGS1", "DGS2", "DGS3", "DGS4", "DGS5", "DGS6"],
        load_data_quality(),
        date(2026, 4, 2),
        issues,
        requested_window=(date(2021, 2, 22), date(2026, 4, 2)),
    )

    by_code = {issue.code: issue for issue in issues}
    approved_codes = {
        "rates_invalid_date",
        "rates_invalid_value",
        "rates_non_finite_value",
        "rates_out_of_range",
        "rates_extreme_daily_change",
        "rates_suspicious_daily_change",
    }
    assert approved_codes <= set(by_code)
    for code in approved_codes:
        issue = by_code[code]
        assert issue.attribution_scope_status == ATTRIBUTION_SCOPE_COMPLETE
        assert isinstance(issue.typed_attribution, RateDataQualityIssueAttribution)

    extreme = by_code["rates_extreme_daily_change"].typed_attribution
    suspicious = by_code["rates_suspicious_daily_change"].typed_attribution
    assert isinstance(extreme, RateDataQualityIssueAttribution)
    assert isinstance(suspicious, RateDataQualityIssueAttribution)
    assert len(extreme.affected_rows) == 2
    assert len(suspicious.affected_rows) == 2
    assert extreme.policy_evidence[0].policy_values == (("extreme_daily_change_abs", 2.0),)
    assert suspicious.policy_evidence[0].policy_values == (
        ("extreme_daily_change_abs", 2.0),
        ("suspicious_daily_change_abs", 0.75),
    )


def test_missing_window_and_unapproved_runtime_source_remain_global(
    tmp_path: Path,
) -> None:
    rates = pd.DataFrame([{"date": "not-a-date", "series": "DGS10", "value": 1.0}])
    path = tmp_path / "rates.csv"
    rates.to_csv(path, index=False)
    summary = DataFileSummary(
        path=path,
        exists=True,
        rows=1,
        sha256="c" * 64,
    )
    issues = []

    _validate_rates(
        rates,
        summary,
        ["DGS10"],
        load_data_quality(),
        date(2026, 4, 2),
        issues,
    )
    issue = next(item for item in issues if item.code == "rates_invalid_date")
    assert issue.attribution_scope_status == ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN
    assert issue.attribution_incomplete_reasons == ("REQUESTED_WINDOW_UNAVAILABLE",)
    assert issue.typed_attribution is None

    issues = []
    _validate_rates(
        rates,
        summary,
        ["DGS10"],
        load_data_quality(),
        date(2026, 4, 2),
        issues,
        requested_window=(date(2021, 2, 22), date(2026, 4, 2)),
        source_role="secondary_macro_rates",
    )
    issue = next(item for item in issues if item.code == "rates_invalid_date")
    assert issue.attribution_scope_status == ATTRIBUTION_SCOPE_GLOBAL_OR_UNKNOWN
    assert issue.attribution_incomplete_reasons == ("UNAPPROVED_SOURCE_ROLE",)


def test_rate_attribution_markdown_exposes_rows_and_thresholds(
    tmp_path: Path,
) -> None:
    rates = pd.DataFrame(
        [
            {"date": "2026-04-01", "series": "DGS10", "value": 1.0},
            {"date": "2026-04-02", "series": "DGS10", "value": 4.0},
        ]
    )
    path = tmp_path / "rates.csv"
    rates.to_csv(path, index=False)
    summary = DataFileSummary(
        path=path,
        exists=True,
        rows=2,
        sha256="d" * 64,
    )
    issues = []
    _validate_rates(
        rates,
        summary,
        ["DGS10"],
        load_data_quality(),
        date(2026, 4, 2),
        issues,
        requested_window=(date(2021, 2, 22), date(2026, 4, 2)),
    )
    report = DataQualityReport(
        checked_at=pd.Timestamp("2026-04-02T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 2),
        price_summary=DataFileSummary(
            path=tmp_path / "prices.csv",
            exists=True,
            rows=0,
            sha256="e" * 64,
        ),
        rate_summary=summary,
        expected_price_tickers=(),
        expected_rate_series=("DGS10",),
        issues=tuple(issues),
    )

    markdown = render_data_quality_report(report)

    assert "## Typed issue attribution" in markdown
    assert "Affected rate series：`DGS10`" in markdown
    assert "PREVIOUS_VALID" in markdown
    assert "extreme_daily_change_abs=2.0" in markdown


def test_active_capability_policies_do_not_adopt_rate_issue_codes() -> None:
    project_root = Path(__file__).resolve().parents[1]
    approved_codes = {
        item.issue_code for item in load_rate_row_issue_attribution_decision().approved_sites
    }
    capability_policies = sorted(
        (project_root / "config/data_quality").glob("*_capability_v1.yaml")
    )

    assert capability_policies
    for path in capability_policies:
        text = path.read_text(encoding="utf-8")
        assert approved_codes.isdisjoint(text.split())
