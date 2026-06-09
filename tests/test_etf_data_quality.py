from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.data_quality import (
    DATA_QUALITY_REPORT_SCHEMA_VERSION,
    DATA_QUALITY_VALIDATION_SCHEMA_VERSION,
    DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    DataQualityPolicyError,
    EvidenceCompletenessPolicy,
    EvidenceSetPolicy,
    GateFreshnessPolicy,
    ValidationGateFreshnessPolicy,
    build_data_quality_report,
    build_data_quality_validation_report,
    check_config_model_drift,
    check_evidence_completeness,
    check_missing_bar_coverage,
    check_price_freshness,
    check_reader_brief_links,
    check_report_staleness,
    check_return_outliers,
    check_validation_gate_freshness,
    expected_latest_trading_date,
    load_data_quality_policy_config,
    render_data_quality_report_markdown,
    render_data_quality_validation_report_markdown,
    write_data_quality_report,
    write_data_quality_validation_report,
)
from ai_trading_system.reports import reader_brief


def test_data_quality_policy_config_loads_default() -> None:
    policy = load_data_quality_policy_config()

    assert policy.schema_version == "etf_data_quality_policy_v1"
    assert policy.policy_metadata.version == "etf_data_quality_policy_v0_1"
    assert policy.data_quality.price_freshness.required_assets
    assert "SPY" in policy.data_quality.price_freshness.required_assets
    assert policy.data_quality.safety.observe_only is True
    assert policy.data_quality.safety.candidate_only is True
    assert policy.data_quality.safety.production_effect == "none"
    assert policy.data_quality.safety.broker_action == "none"
    assert policy.data_quality.safety.manual_review_required is True


def test_data_quality_policy_rejects_unsafe_production_effect(tmp_path: Path) -> None:
    raw = yaml.safe_load(DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["data_quality"]["safety"]["production_effect"] = "apply_weights"
    path = tmp_path / "unsafe.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DataQualityPolicyError, match="production_effect"):
        load_data_quality_policy_config(path)


def test_price_freshness_handles_fresh_stale_missing_optional_and_holiday() -> None:
    policy = _small_policy(required=("SPY", "QQQ"), optional=("NVDA",))
    prices = _price_rows(
        {
            "SPY": [date(2026, 1, 16)],
            "QQQ": [date(2026, 1, 14)],
        }
    )

    assert expected_latest_trading_date(
        date(2026, 1, 19),
        holidays=set(policy.data_quality.price_freshness.market_holidays),
    ) == date(2026, 1, 16)

    result = check_price_freshness(prices, policy=policy, as_of=date(2026, 1, 16))
    records = {item["asset"]: item for item in result["records"]}

    assert records["SPY"]["freshness_status"] == "fresh"
    assert records["SPY"]["blocking_status"] == "pass"
    assert records["QQQ"]["freshness_status"] == "stale"
    assert records["QQQ"]["blocking_status"] == "block"
    assert records["NVDA"]["freshness_status"] == "missing"
    assert records["NVDA"]["blocking_status"] == "warn"
    assert result["summary"]["blocking_count"] == 1
    assert result["summary"]["warning_count"] == 1


def test_calendar_coverage_detects_missing_bars_without_counting_holiday() -> None:
    policy = _small_policy(required=("SPY",), optional=("QQQ",))
    policy.data_quality.calendar_coverage.lookback_trading_days = 3
    dates = [date(2026, 1, 15), date(2026, 1, 16), date(2026, 1, 20)]
    prices = _price_rows({"SPY": dates[:-1], "QQQ": dates})

    result = check_missing_bar_coverage(prices, policy=policy, as_of=date(2026, 1, 20))
    records = {item["asset"]: item for item in result["records"]}

    assert "2026-01-19" not in records["SPY"]["missing_dates"]
    assert records["SPY"]["coverage_status"] in {"major_gap", "insufficient_coverage"}
    assert records["SPY"]["blocking_status"] == "block"
    assert records["QQQ"]["coverage_status"] == "complete"


def test_return_outlier_detects_warning_critical_and_adjacent_reversal() -> None:
    policy = _small_policy(required=("SPY", "QQQ"), optional=())
    policy.data_quality.return_outliers.lookback_trading_days = 5
    prices = _price_rows(
        {
            "SPY": [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)],
            "QQQ": [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)],
        },
        prices={
            ("SPY", date(2026, 1, 2)): 100,
            ("SPY", date(2026, 1, 5)): 109,
            ("SPY", date(2026, 1, 6)): 110,
            ("QQQ", date(2026, 1, 2)): 100,
            ("QQQ", date(2026, 1, 5)): 125,
            ("QQQ", date(2026, 1, 6)): 100,
        },
    )

    result = check_return_outliers(prices, policy=policy, as_of=date(2026, 1, 6))
    statuses = [(item["asset"], item["outlier_status"]) for item in result["records"]]

    assert ("SPY", "warning_outlier") in statuses
    assert ("QQQ", "possible_adjustment_issue") in statuses
    assert result["summary"]["blocking_count"] == 1


def test_config_hash_model_version_drift_blocks_required_artifact(tmp_path: Path) -> None:
    policy = _small_policy(required=("SPY",), optional=())
    policy.data_quality.config_hash_drift.required_report_ids = ["test_report"]
    path = _write_json(
        tmp_path / "artifact_2026-01-06.json",
        {"config_hash": "old", "model_version": "model_v1"},
    )
    report_index = _report_index_record("test_report", path, artifact_date="2026-01-06")

    result = check_config_model_drift(
        report_index,
        policy=policy,
        current_config_hash="new",
        current_model_version="model_v1",
    )

    record = result["records"][0]
    assert record["drift_status"] == "config_drift"
    assert record["blocking_status"] == "block"


def test_evidence_completeness_blocks_required_insufficient_sample(tmp_path: Path) -> None:
    policy = _small_policy(required=("SPY",), optional=())
    policy.data_quality.evidence_completeness = EvidenceCompletenessPolicy(
        evidence_sets=[
            EvidenceSetPolicy(
                evidence_type="forward_dashboard",
                report_id="forward_report",
                required=True,
                required_fields=["status", "summary.sample_count"],
                sample_count_paths=["summary.sample_count"],
                minimum_sample_count=2,
            )
        ]
    )
    path = _write_json(
        tmp_path / "forward_dashboard_2026-01-06.json",
        {"status": "PASS", "summary": {"sample_count": 1}},
    )
    report_index = _report_index_record("forward_report", path, artifact_date="2026-01-06")

    result = check_evidence_completeness(report_index, policy=policy)

    record = result["records"][0]
    assert record["completeness_status"] == "insufficient"
    assert record["blocking_status"] == "block"


def test_validation_gate_freshness_blocks_failed_required_gate(tmp_path: Path) -> None:
    policy = _small_policy(required=("SPY",), optional=())
    policy.data_quality.validation_gate_freshness = ValidationGateFreshnessPolicy(
        max_age_days=3,
        stale_required_blocks=True,
        gates=[
            GateFreshnessPolicy(
                gate_id="forward",
                report_id="forward_validation",
                required=True,
                max_allowed_age_days=3,
            )
        ],
    )
    path = _write_json(
        tmp_path / "forward_validation_2026-01-06.json",
        {"status": "FAIL", "generated_at": "2026-01-06T12:00:00+00:00"},
    )
    report_index = _report_index_record(
        "forward_validation",
        path,
        artifact_date="2026-01-06",
        artifact_status="FAIL",
    )

    result = check_validation_gate_freshness(
        report_index,
        policy=policy,
        as_of=date(2026, 1, 6),
    )

    record = result["records"][0]
    assert record["freshness_status"] == "failed"
    assert record["blocking_status"] == "block"


def test_report_staleness_and_reader_brief_links_detect_required_issues(tmp_path: Path) -> None:
    policy = _small_policy(required=("SPY",), optional=())
    policy.data_quality.report_staleness.required_report_ids = ["required_report"]
    policy.data_quality.reader_brief_links.required_report_ids = ["required_report"]
    stale_path = _write_json(tmp_path / "required_report_2026-01-01.json", {"status": "PASS"})
    reader_path = _write_json(
        tmp_path / "reader_brief_2026-01-06.json",
        {"status": "PASS", "report_navigation": []},
    )
    report_index = {
        "reports": [
            _report_record("required_report", stale_path, "2026-01-01", freshness="STALE"),
            _report_record("reader_brief", reader_path, "2026-01-06", freshness="FRESH"),
        ]
    }

    staleness = check_report_staleness(report_index, policy=policy)
    links = check_reader_brief_links(report_index, policy=policy)

    assert staleness["records"][0]["staleness_status"] == "stale"
    assert staleness["records"][0]["blocking_status"] == "block"
    assert links["records"][0]["link_status"] == "not_linked"
    assert links["records"][0]["blocking_status"] == "block"


def test_data_quality_report_and_markdown_include_required_sections(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    _complete_price_frame(date(2026, 1, 20)).to_csv(prices_path, index=False)

    payload = build_data_quality_report(
        as_of=date(2026, 1, 20),
        prices_path=prices_path,
        root_path=tmp_path,
    )
    paths = write_data_quality_report(
        payload,
        json_path=tmp_path / "data_quality_report_2026-01-20.json",
        markdown_path=tmp_path / "data_quality_report_2026-01-20.md",
    )
    markdown = render_data_quality_report_markdown(payload)

    assert payload["schema_version"] == DATA_QUALITY_REPORT_SCHEMA_VERSION
    assert payload["safety_banner"]["production_effect"] == "none"
    assert "price_freshness" in payload
    assert "calendar_coverage" in payload
    assert "return_outliers" in payload
    assert "config_hash_model_version_drift" in payload
    assert "evidence_completeness" in payload
    assert "validation_gate_freshness" in payload
    assert "report_staleness" in payload
    assert "reader_brief_links" in payload
    assert "Safety Banner" in markdown
    assert paths["json"].exists()
    assert paths["markdown"].exists()


def test_data_quality_validation_gate_passes_and_writes_reports(tmp_path: Path) -> None:
    payload = build_data_quality_validation_report(as_of=date(2026, 1, 20))
    paths = write_data_quality_validation_report(
        payload,
        json_path=tmp_path / "data_quality_validation_2026-01-20.json",
        markdown_path=tmp_path / "data_quality_validation_2026-01-20.md",
    )
    markdown = render_data_quality_validation_report_markdown(payload)

    assert payload["schema_version"] == DATA_QUALITY_VALIDATION_SCHEMA_VERSION
    assert payload["status"] == "PASS"
    assert payload["failed_check_count"] == 0
    assert "reader_brief_integration_available" in {item["check_id"] for item in payload["checks"]}
    assert "Safety" not in markdown or "production_effect" in markdown
    assert paths["json"].exists()
    assert paths["markdown"].exists()


def test_reader_brief_data_quality_section_summarizes_latest_report(tmp_path: Path) -> None:
    path = _write_json(
        tmp_path / "data_quality_report_2026-01-20.json",
        {
            "schema_version": DATA_QUALITY_REPORT_SCHEMA_VERSION,
            "status": "WARNING",
            "safety_banner": {
                "observe_only": True,
                "candidate_only": True,
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": True,
            },
            "commands_executed": False,
            "production_state_mutated": False,
            "blocking_failures": [],
            "warnings": [{"finding_id": "report_staleness:1"}],
            "price_freshness": {"summary": {"blocking_count": 0, "warning_count": 0}},
            "missing_bars": {"summary": {"blocking_count": 0, "warning_count": 0}},
            "return_outliers": {"summary": {"blocking_count": 0, "warning_count": 1}},
            "config_hash_model_version_drift": {
                "summary": {"blocking_count": 0, "warning_count": 0}
            },
            "evidence_completeness": {"summary": {"blocking_count": 0, "warning_count": 0}},
            "validation_gate_freshness": {"summary": {"blocking_count": 0, "warning_count": 0}},
            "report_staleness": {"summary": {"blocking_count": 0, "warning_count": 1}},
            "reader_brief_links": {"summary": {"blocking_count": 0, "warning_count": 0}},
        },
    )

    summary = reader_brief._etf_data_quality_governance_summary(
        {
            "reports": [
                {
                    "report_id": "etf_data_quality_governance_report",
                    "latest_artifact_path": str(path),
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "WARNING"
    assert summary["return_outliers_status"] == "WARNING"
    assert summary["production_effect"] == "none"
    assert summary["broker_action"] == "none"


def test_data_quality_validate_cli_passes(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "data-quality",
            "validate",
            "--as-of",
            "2026-01-20",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert (tmp_path / "data_quality_validation_2026-01-20.json").exists()


def _small_policy(required: tuple[str, ...], optional: tuple[str, ...]):
    policy = deepcopy(load_data_quality_policy_config())
    policy.data_quality.price_freshness.required_assets = list(required)
    policy.data_quality.price_freshness.optional_assets = list(optional)
    policy.data_quality.price_freshness.max_trading_day_lag = 1
    policy.data_quality.calendar_coverage.lookback_trading_days = 5
    policy.data_quality.config_hash_drift.required_report_ids = []
    policy.data_quality.config_hash_drift.optional_report_ids = []
    policy.data_quality.evidence_completeness.evidence_sets = []
    policy.data_quality.validation_gate_freshness.gates = []
    policy.data_quality.report_staleness.required_report_ids = []
    policy.data_quality.report_staleness.optional_report_ids = []
    policy.data_quality.reader_brief_links.required_report_ids = []
    policy.data_quality.reader_brief_links.optional_report_ids = []
    return policy


def _price_rows(
    dates_by_symbol: dict[str, list[date]],
    *,
    prices: dict[tuple[str, date], float] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol, dates in dates_by_symbol.items():
        for index, item in enumerate(dates):
            price = (prices or {}).get((symbol, item), 100.0 + index)
            if symbol == "CASH":
                price = 1.0
            rows.append(
                {
                    "date": item.isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 0 if symbol == "CASH" else 1000,
                    "source": "fixture",
                    "created_at": "2026-01-01T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _complete_price_frame(as_of: date) -> pd.DataFrame:
    policy = load_data_quality_policy_config()
    holidays = set(policy.data_quality.price_freshness.market_holidays)
    days: list[date] = []
    current = as_of
    while len(days) < 25:
        if current.weekday() < 5 and current not in holidays:
            days.append(current)
        current -= timedelta(days=1)
    days = list(reversed(days))
    symbols = ["SPY", "QQQ", "SMH", "SOXX", "CASH"]
    return _price_rows({symbol: days for symbol in symbols})


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _report_index_record(
    report_id: str,
    path: Path,
    *,
    artifact_date: str,
    artifact_status: str = "PASS",
) -> dict[str, object]:
    return {
        "reports": [
            _report_record(
                report_id,
                path,
                artifact_date,
                freshness="FRESH",
                artifact_status=artifact_status,
            )
        ]
    }


def _report_record(
    report_id: str,
    path: Path,
    artifact_date: str,
    *,
    freshness: str,
    artifact_status: str = "PASS",
) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "artifact_date": artifact_date,
        "freshness_status": freshness,
        "artifact_status": artifact_status,
        "exists": path.exists(),
        "age_days": 0 if freshness == "FRESH" else 10,
        "artifact_production_effect_risk": False,
    }
