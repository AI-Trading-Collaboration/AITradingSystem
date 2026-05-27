from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands import sec_pit as sec_pit_cli
from ai_trading_system.config import (
    FundamentalDerivedMetricConfig,
    FundamentalFeaturesConfig,
    FundamentalMetricConceptConfig,
    FundamentalMetricConfig,
    FundamentalMetricsConfig,
    FundamentalRatioFeatureConfig,
    SecCompaniesConfig,
    SecCompanyConfig,
)
from ai_trading_system.fundamentals.sec_filing_timeline import build_filing_timeline
from ai_trading_system.fundamentals.sec_pit_backfill import (
    SEC_PIT_BACKTEST_DATA_GRADE,
    SEC_PIT_CURRENT_HISTORY_GRADE,
    SEC_PIT_RAW_MANIFEST_COLUMNS,
    SecPitBackfillConfig,
    sec_pit_companyfacts_path,
    sec_pit_safety_metadata,
    sec_pit_submissions_path,
)
from ai_trading_system.fundamentals.sec_pit_metrics import build_mapped_metrics
from ai_trading_system.fundamentals.sec_pit_panel import (
    SEC_PIT_FEATURE_PANEL_COLUMNS,
    build_fundamental_pit_intervals,
    build_sec_pit_feature_panel,
    load_sec_pit_feature_panel,
)
from ai_trading_system.fundamentals.sec_pit_validation import validate_sec_pit_backfill
from ai_trading_system.fundamentals.sec_xbrl_facts import build_xbrl_facts_long


def test_filing_timeline_uses_acceptance_t_plus_one_availability(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    _write_raw_payloads(raw_dir, _submissions_payload("2023-04-20T20:30:00.000Z"))

    timeline = build_filing_timeline(
        sec_companies=_companies(),
        raw_dir=raw_dir,
        start=date(2023, 4, 1),
        end=date(2023, 4, 30),
    )

    row = timeline.iloc[0].to_dict()
    assert row["acceptance_datetime_utc"] == "2023-04-20T20:30:00+00:00"
    assert row["available_for_signal_date"] == "2023-04-21"
    assert row["pit_data_grade"] == SEC_PIT_BACKTEST_DATA_GRADE
    assert row["confidence_level"] == "high"


def test_missing_acceptance_downgrades_to_current_history_approx(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    _write_raw_payloads(raw_dir, _submissions_payload(""))

    timeline = build_filing_timeline(
        sec_companies=_companies(),
        raw_dir=raw_dir,
        start=date(2023, 4, 1),
        end=date(2023, 4, 30),
    )

    row = timeline.iloc[0].to_dict()
    assert row["pit_data_grade"] == SEC_PIT_CURRENT_HISTORY_GRADE
    assert row["confidence_level"] == "low"
    assert "missing acceptanceDateTime" in row["confidence_reason"]


def test_amendment_restatement_supersedes_only_after_its_availability(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw"
    _write_raw_payloads(
        raw_dir,
        {
            **_submissions_payload("2023-04-20T20:30:00.000Z"),
            "filings": {
                "recent": _recent_filings(
                    [
                        ("0001045810-23-000001", "10-Q", "2023-04-20T20:30:00.000Z"),
                        ("0001045810-23-000002", "10-Q/A", "2023-05-25T20:30:00.000Z"),
                    ]
                )
            },
        },
        _companyfacts_payload(include_amendment=True),
    )
    timeline_path = tmp_path / "timeline.csv"
    timeline = build_filing_timeline(
        sec_companies=_companies(),
        raw_dir=raw_dir,
        start=date(2023, 4, 1),
        end=date(2023, 5, 31),
    )
    timeline.to_csv(timeline_path, index=False)
    facts = build_xbrl_facts_long(
        sec_companies=_companies(),
        raw_dir=raw_dir,
        filing_timeline_path=timeline_path,
        end=date(2023, 5, 31),
    )
    mapped = build_mapped_metrics(
        facts=facts,
        metrics=_metrics(),
        policy=_policy(),
        end=date(2023, 5, 31),
    )
    intervals = build_fundamental_pit_intervals(mapped)

    revenue_rows = mapped.loc[mapped["metric_id"] == "revenue"]
    amended = revenue_rows.loc[
        revenue_rows["source_accession_number"] == "0001045810-23-000002"
    ].iloc[0]
    original_interval = intervals.loc[
        (intervals["metric_id"] == "revenue")
        & (intervals["source_accession_number"] == "0001045810-23-000001")
    ].iloc[0]
    amended_interval = intervals.loc[
        (intervals["metric_id"] == "revenue")
        & (intervals["source_accession_number"] == "0001045810-23-000002")
    ].iloc[0]

    assert amended["is_restated_fact"] == "true"
    assert amended["supersedes_accession_number"] == "0001045810-23-000001"
    assert original_interval["available_until_signal_date"] == "2023-05-25"
    assert amended_interval["available_from_signal_date"] == "2023-05-26"


def test_cross_currency_ratio_is_blocked() -> None:
    intervals = pd.DataFrame(
        [
            _interval_record("gross_profit", "EUR", 60),
            _interval_record("revenue", "USD", 100),
        ]
    )

    panel = build_sec_pit_feature_panel(
        intervals=intervals,
        features=_features(),
        sec_companies=_companies(),
        start=date(2023, 4, 21),
        end=date(2023, 4, 21),
    )

    assert panel.empty


def test_feature_panel_resolves_duplicate_feature_observations() -> None:
    features = FundamentalFeaturesConfig(
        features=[
            FundamentalRatioFeatureConfig(
                feature_id="gross_margin",
                name="Gross Margin",
                description="Gross profit divided by revenue.",
                numerator_metric_id="gross_profit",
                denominator_metric_id="revenue",
                preferred_periods=["annual", "quarterly"],
            )
        ]
    )
    intervals = pd.DataFrame(
        [
            {
                **_interval_record("gross_profit", "USD", 60),
                "period_type": "annual",
                "period_end": "2022-12-31",
                "available_time_utc": "2023-02-01T20:30:00+00:00",
                "available_from_signal_date": "2023-02-02",
            },
            {
                **_interval_record("revenue", "USD", 100),
                "period_type": "annual",
                "period_end": "2022-12-31",
                "available_time_utc": "2023-02-01T20:30:00+00:00",
                "available_from_signal_date": "2023-02-02",
            },
            {
                **_interval_record("gross_profit", "USD", 40),
                "period_type": "quarterly",
                "period_end": "2023-03-31",
                "available_time_utc": "2023-04-20T20:30:00+00:00",
                "available_from_signal_date": "2023-04-21",
            },
            {
                **_interval_record("revenue", "USD", 100),
                "period_type": "quarterly",
                "period_end": "2023-03-31",
                "available_time_utc": "2023-04-20T20:30:00+00:00",
                "available_from_signal_date": "2023-04-21",
            },
        ]
    )

    panel = build_sec_pit_feature_panel(
        intervals=intervals,
        features=features,
        sec_companies=_companies(),
        start=date(2023, 4, 21),
        end=date(2023, 4, 21),
    )

    assert len(panel) == 1
    row = panel.iloc[0]
    assert row["period_type"] == "quarterly"
    assert row["feature_value"] == 0.4


def test_duplicate_interval_overlap_fails_validation(tmp_path: Path) -> None:
    paths = _minimal_validation_inputs(tmp_path)
    intervals = pd.DataFrame(
        [
            {
                **_interval_record("revenue", "USD", 100),
                "available_from_signal_date": "2023-04-21",
                "available_until_signal_date": "2023-05-30",
            },
            {
                **_interval_record("revenue", "USD", 110),
                "available_from_signal_date": "2023-05-01",
                "available_until_signal_date": "",
                "source_accession_number": "0001045810-23-000002",
            },
        ]
    )
    intervals.to_csv(paths["intervals"], index=False)

    report = validate_sec_pit_backfill(
        as_of=date(2023, 5, 31),
        raw_manifest_path=paths["raw_manifest"],
        filing_timeline_path=paths["timeline"],
        facts_path=paths["facts"],
        mapped_metrics_path=paths["mapped"],
        intervals_path=paths["intervals"],
        feature_panel_path=paths["feature_panel"],
        policy=_policy(),
    )

    assert any(issue.code == "duplicate_overlapping_active_interval" for issue in report.issues)


def test_feature_panel_loader_rejects_future_available_time(tmp_path: Path) -> None:
    feature_panel_path = tmp_path / "sec_pit_feature_panel.csv"
    pd.DataFrame(
        [
            {
                "decision_date": "2023-04-21",
                "ticker": "NVDA",
                "feature_id": "gross_margin",
                "feature_value": 0.6,
                "feature_unit": "ratio",
                "input_metric_ids": "gross_profit,revenue",
                "input_accession_numbers": "a,b",
                "input_available_times_utc": "2023-04-22T00:00:00+00:00",
                "max_input_available_time_utc": "2023-04-22T00:00:00+00:00",
                "pit_data_grade": SEC_PIT_BACKTEST_DATA_GRADE,
                "confidence_level": "high",
                "confidence_reason": "",
                "period_type": "quarterly",
                "period_end": "2023-03-31",
                "input_metric_units": "USD,USD",
            }
        ],
        columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS),
    ).to_csv(feature_panel_path, index=False)

    try:
        load_sec_pit_feature_panel(feature_panel_path, date(2023, 4, 21), ["NVDA"])
    except ValueError as exc:
        assert "future available_time" in str(exc)
    else:
        raise AssertionError("future feature availability should fail")


def test_sec_pit_cli_full_pipeline_with_mocked_sec_json(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeProvider:
        def __init__(self, user_agent: str, *, max_requests_per_second: float) -> None:
            assert user_agent == "test@example.com"
            assert max_requests_per_second == 5.0

        def download_submissions_raw(
            self,
            ticker: str,
            cik: str,
            *,
            use_cache: bool = True,
        ) -> bytes:
            assert ticker == "NVDA"
            assert cik == "0001045810"
            assert use_cache is True
            return json.dumps(_submissions_payload("2023-04-20T20:30:00.000Z")).encode()

        def download_companyfacts_raw(
            self,
            ticker: str,
            cik: str,
            *,
            use_cache: bool = True,
        ) -> bytes:
            assert ticker == "NVDA"
            assert cik == "0001045810"
            assert use_cache is True
            return json.dumps(_companyfacts_payload()).encode()

        def submissions_endpoint_for(self, cik: str) -> str:
            return f"https://data.sec.gov/submissions/CIK{cik}.json"

        def companyfacts_endpoint_for(self, cik: str) -> str:
            return f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    monkeypatch.setattr(sec_pit_cli, "SecPitEdgarProvider", FakeProvider)
    sec_companies_path = tmp_path / "sec_companies.yaml"
    metrics_path = tmp_path / "fundamental_metrics.yaml"
    features_path = tmp_path / "fundamental_features.yaml"
    config_path = tmp_path / "sec_pit_backfill.yaml"
    sec_companies_path.write_text(
        "companies:\n"
        "  - ticker: NVDA\n"
        "    cik: '0001045810'\n"
        "    company_name: NVIDIA Corporation\n"
        "    active: true\n",
        encoding="utf-8",
    )
    metrics_path.write_text(
        "metrics:\n"
        "  - metric_id: revenue\n"
        "    name: Revenue\n"
        "    description: Revenue.\n"
        "    preferred_periods: [quarterly]\n"
        "    concepts:\n"
        "      - taxonomy: us-gaap\n"
        "        concept: Revenues\n"
        "        unit: USD\n"
        "  - metric_id: gross_profit\n"
        "    name: Gross Profit\n"
        "    description: Gross profit.\n"
        "    preferred_periods: [quarterly]\n"
        "    concepts:\n"
        "      - taxonomy: us-gaap\n"
        "        concept: GrossProfit\n"
        "        unit: USD\n",
        encoding="utf-8",
    )
    features_path.write_text(
        "features:\n"
        "  - feature_id: gross_margin\n"
        "    name: Gross Margin\n"
        "    description: Gross profit divided by revenue.\n"
        "    numerator_metric_id: gross_profit\n"
        "    denominator_metric_id: revenue\n"
        "    preferred_periods: [quarterly]\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "sec_pit_backfill:\n"
        "  max_requests_per_second: 5\n"
        "  allowed_forms: [10-Q]\n"
        "  metric_panel_forms: [10-Q]\n"
        "  coverage_warning_threshold: 0.7\n"
        "  coverage_error_threshold: 0.4\n"
        "  stale_quarterly_days: 180\n"
        "  stale_annual_days: 540\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        sec_pit_cli.sec_pit_app,
        [
            "backfill",
            "--from",
            "2023-04-21",
            "--to",
            "2023-04-21",
            "--ticker",
            "NVDA",
            "--user-agent",
            "test@example.com",
            "--raw-dir",
            str(tmp_path / "raw"),
            "--processed-dir",
            str(tmp_path / "processed"),
            "--report-dir",
            str(tmp_path / "reports"),
            "--sec-companies-path",
            str(sec_companies_path),
            "--metrics-path",
            str(metrics_path),
            "--features-path",
            str(features_path),
            "--config-path",
            str(config_path),
        ],
    )

    assert result.exit_code == 0, result.output
    validation = json.loads(
        (tmp_path / "reports" / "sec_pit_validation_2023-04-21.json").read_text(encoding="utf-8")
    )
    assert validation["status"] == "PASS"
    assert validation["backtest_data_grade"] == SEC_PIT_BACKTEST_DATA_GRADE
    assert (tmp_path / "processed" / "sec_pit_coverage_summary.csv").exists()


def _companies() -> SecCompaniesConfig:
    return SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="NVDA",
                cik="0001045810",
                company_name="NVIDIA Corporation",
                active=True,
                sec_metric_periods=["quarterly"],
            )
        ]
    )


def _metrics() -> FundamentalMetricsConfig:
    return FundamentalMetricsConfig(
        metrics=[
            FundamentalMetricConfig(
                metric_id="revenue",
                name="Revenue",
                description="Revenue.",
                preferred_periods=["quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="Revenues",
                        unit="USD",
                    )
                ],
            ),
            FundamentalMetricConfig(
                metric_id="gross_profit",
                name="Gross Profit",
                description="Gross profit.",
                preferred_periods=["quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="GrossProfit",
                        unit="USD",
                    )
                ],
            ),
        ],
        supporting_metrics=[
            FundamentalMetricConfig(
                metric_id="cost_of_revenue",
                name="Cost of Revenue",
                description="Cost of revenue.",
                preferred_periods=["quarterly"],
                concepts=[
                    FundamentalMetricConceptConfig(
                        taxonomy="us-gaap",
                        concept="CostOfRevenue",
                        unit="USD",
                    )
                ],
            )
        ],
        derived_metrics=[
            FundamentalDerivedMetricConfig(
                metric_id="gross_profit",
                operation="difference",
                minuend_metric_id="revenue",
                subtrahend_metric_id="cost_of_revenue",
            )
        ],
    )


def _features() -> FundamentalFeaturesConfig:
    return FundamentalFeaturesConfig(
        features=[
            FundamentalRatioFeatureConfig(
                feature_id="gross_margin",
                name="Gross Margin",
                description="Gross profit divided by revenue.",
                numerator_metric_id="gross_profit",
                denominator_metric_id="revenue",
                preferred_periods=["quarterly"],
            )
        ]
    )


def _policy() -> SecPitBackfillConfig:
    return SecPitBackfillConfig(
        daily_availability_policy="next_trading_day",
        intraday_policy_enabled=False,
        max_requests_per_second=5.0,
        allowed_forms=("10-Q", "10-Q/A"),
        metric_panel_forms=("10-Q", "10-Q/A"),
        six_k_default_grade=SEC_PIT_CURRENT_HISTORY_GRADE,
        coverage_warning_threshold=0.70,
        coverage_error_threshold=0.40,
        stale_quarterly_days=180,
        stale_annual_days=540,
    )


def _submissions_payload(acceptance: str) -> dict[str, object]:
    return {
        "cik": "0001045810",
        "name": "NVIDIA Corporation",
        "filings": {"recent": _recent_filings([("0001045810-23-000001", "10-Q", acceptance)])},
    }


def _recent_filings(accessions: list[tuple[str, str, str]]) -> dict[str, list[object]]:
    return {
        "accessionNumber": [item[0] for item in accessions],
        "filingDate": [
            "2023-04-20" if index == 0 else "2023-05-25" for index, _ in enumerate(accessions)
        ],
        "reportDate": ["2023-03-31" for _item in accessions],
        "acceptanceDateTime": [item[2] for item in accessions],
        "form": [item[1] for item in accessions],
        "primaryDocument": ["nvda-20230331.htm" for _item in accessions],
        "primaryDocDescription": [item[1] for item in accessions],
        "isXBRL": [1 for _item in accessions],
        "isInlineXBRL": [1 for _item in accessions],
    }


def _companyfacts_payload(*, include_amendment: bool = False) -> dict[str, object]:
    revenue_facts = [_fact("Revenues", 100, "0001045810-23-000001", "10-Q", "2023-04-20")]
    gross_profit_facts = [_fact("GrossProfit", 60, "0001045810-23-000001", "10-Q", "2023-04-20")]
    if include_amendment:
        revenue_facts.append(_fact("Revenues", 110, "0001045810-23-000002", "10-Q/A", "2023-05-25"))
        gross_profit_facts.append(
            _fact("GrossProfit", 66, "0001045810-23-000002", "10-Q/A", "2023-05-25")
        )
    return {
        "cik": 1045810,
        "entityName": "NVIDIA Corporation",
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": revenue_facts}},
                "GrossProfit": {"units": {"USD": gross_profit_facts}},
            }
        },
    }


def _fact(
    concept: str,
    value: float,
    accession: str,
    form: str,
    filed: str,
) -> dict[str, object]:
    del concept
    return {
        "start": "2023-01-01",
        "end": "2023-03-31",
        "fy": 2023,
        "fp": "Q1",
        "form": form,
        "filed": filed,
        "val": value,
        "accn": accession,
        "frame": "CY2023Q1",
    }


def _interval_record(metric_id: str, unit: str, value: float) -> dict[str, object]:
    return {
        "ticker": "NVDA",
        "metric_id": metric_id,
        "period_type": "quarterly",
        "period_end": "2023-03-31",
        "value": value,
        "unit": unit,
        "source_accession_number": "0001045810-23-000001",
        "available_from_signal_date": "2023-04-21",
        "available_until_signal_date": "",
        "available_time_utc": "2023-04-20T20:30:00+00:00",
        "superseded_by_accession_number": "",
        "pit_data_grade": SEC_PIT_BACKTEST_DATA_GRADE,
        "confidence_level": "high",
    }


def _write_raw_payloads(
    raw_dir: Path,
    submissions: dict[str, object],
    companyfacts: dict[str, object] | None = None,
) -> None:
    submissions_path = sec_pit_submissions_path(raw_dir, "NVDA", "0001045810")
    companyfacts_path = sec_pit_companyfacts_path(raw_dir, "NVDA", "0001045810")
    _write_json(submissions_path, submissions)
    _write_json(companyfacts_path, companyfacts or _companyfacts_payload())
    _write_manifest(raw_dir, [submissions_path, companyfacts_path])


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_manifest(raw_dir: Path, paths: list[Path]) -> None:
    records = []
    for path in paths:
        payload_type = "submissions" if "submissions" in path.name else "companyfacts"
        records.append(
            {
                "downloaded_at": "2023-04-20T20:31:00+00:00",
                "provider": "SEC EDGAR",
                "source_id": "sec_edgar_reconstructed_pit_raw",
                "source_endpoint": f"https://example.test/{payload_type}",
                "request_parameters": "{}",
                "ticker": "NVDA",
                "cik": "0001045810",
                "payload_type": payload_type,
                "output_path": str(path),
                "row_count": 1,
                "checksum_sha256": _sha256_file(path),
                **sec_pit_safety_metadata(),
            }
        )
    manifest = raw_dir / "manifest" / "sec_edgar_raw_manifest.csv"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records, columns=list(SEC_PIT_RAW_MANIFEST_COLUMNS)).to_csv(
        manifest,
        index=False,
    )


def _minimal_validation_inputs(tmp_path: Path) -> dict[str, Path]:
    raw_file = tmp_path / "raw.json"
    raw_file.write_text("{}", encoding="utf-8")
    raw_manifest = tmp_path / "raw_manifest.csv"
    pd.DataFrame(
        [
            {
                "output_path": str(raw_file),
                "checksum_sha256": _sha256_file(raw_file),
                "row_count": 1,
                "source_endpoint": "https://example.test/sec",
                "ticker": "NVDA",
            }
        ]
    ).to_csv(raw_manifest, index=False)
    timeline = tmp_path / "timeline.csv"
    facts = tmp_path / "facts.csv"
    mapped = tmp_path / "mapped.csv"
    intervals = tmp_path / "intervals.csv"
    feature_panel = tmp_path / "feature_panel.csv"
    pd.DataFrame().to_csv(timeline, index=False)
    pd.DataFrame().to_csv(facts, index=False)
    pd.DataFrame({"source_form": ["10-Q"]}).to_csv(mapped, index=False)
    pd.DataFrame().to_csv(intervals, index=False)
    pd.DataFrame(columns=list(SEC_PIT_FEATURE_PANEL_COLUMNS)).to_csv(feature_panel, index=False)
    return {
        "raw_manifest": raw_manifest,
        "timeline": timeline,
        "facts": facts,
        "mapped": mapped,
        "intervals": intervals,
        "feature_panel": feature_panel,
    }


def _sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
