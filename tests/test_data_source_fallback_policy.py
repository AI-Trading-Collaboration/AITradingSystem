from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig
from ai_trading_system.data_source_fallback_policy import (
    FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    FALLBACK_STATE_FALLBACK_USED,
    FALLBACK_STATE_PRIMARY_OK,
    build_and_write_data_source_fallback_policy,
    validate_data_source_fallback_policy_payload,
)
from ai_trading_system.reports import reader_brief


def test_data_source_fallback_policy_primary_ok_and_reader_brief(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)

    payload, paths = build_and_write_data_source_fallback_policy(
        config=fixture["config"],
        policy=fixture["policy"],
        as_of=date(2026, 6, 16),
        output_dir=tmp_path / "fallback_policy",
    )
    validation = validate_data_source_fallback_policy_payload(
        payload,
        report_path=paths["report_json"],
    )
    summary = reader_brief._data_source_fallback_policy_summary(
        {
            "reports": [
                {
                    "report_id": "data_source_fallback_policy",
                    "latest_artifact_path": str(paths["report_json"]),
                }
            ]
        }
    )

    assert payload["status"] == "PASS"
    assert validation.status == "PASS"
    assert payload["fallback_status"] == FALLBACK_STATE_PRIMARY_OK
    assert payload["summary"]["primary_ok_count"] == 2
    assert payload["summary"]["fallback_used_count"] == 0
    assert paths["report_json"].exists()
    assert paths["report_markdown"].exists()
    assert paths["validation_json"].exists()
    assert paths["reader_brief_section"].exists()
    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "PASS"
    assert summary["fallback_status"] == FALLBACK_STATE_PRIMARY_OK
    assert summary["production_effect"] == "none"


def test_data_source_fallback_policy_cli_records_explicit_fallback_warning(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    config_path = tmp_path / "data_sources.yaml"
    policy_path = tmp_path / "data_source_fallback_policy.yaml"
    output_dir = tmp_path / "fallback_policy_cli"
    config_path.write_text(
        yaml.safe_dump(fixture["config"].model_dump(), sort_keys=False),
        encoding="utf-8",
    )
    policy_path.write_text(
        yaml.safe_dump(fixture["policy"], sort_keys=False),
        encoding="utf-8",
    )

    run = CliRunner().invoke(
        app,
        [
            "data",
            "fallback-policy",
            "run",
            "--as-of",
            "2026-06-16",
            "--config-path",
            str(config_path),
            "--policy-path",
            str(policy_path),
            "--output-dir",
            str(output_dir),
            "--unavailable-source-id",
            "fmp_eod_daily_prices",
            "--unavailable-source-id",
            "cboe_vix_daily_prices",
            "--fallback-used-source-id",
            "marketstack_eod_daily_prices",
            "--fallback-reason",
            "marketstack_eod_daily_prices=primary outage with explicit metadata",
        ],
    )
    assert run.exit_code == 0, run.output
    assert "Data source fallback policy status=PASS_WITH_WARNINGS" in run.output
    assert "fallback_status=FALLBACK_USED" in run.output
    assert "fallback_used_count=1" in run.output

    validation = CliRunner().invoke(
        app,
        [
            "data",
            "fallback-policy",
            "validate",
            "--latest",
            "--output-dir",
            str(output_dir),
        ],
    )
    report = CliRunner().invoke(
        app,
        [
            "data",
            "fallback-policy",
            "report",
            "--latest",
            "--output-dir",
            str(output_dir),
        ],
    )
    latest_pointer = json.loads(
        (output_dir / "latest_data_source_fallback_policy.json").read_text(
            encoding="utf-8"
        )
    )
    payload = json.loads(Path(latest_pointer["report_path"]).read_text(encoding="utf-8"))
    fallback_record = next(
        row for row in payload["records"] if row["fallback_state"] == FALLBACK_STATE_FALLBACK_USED
    )

    assert validation.exit_code == 0, validation.output
    assert report.exit_code == 0, report.output
    assert "Data source fallback policy validation status=PASS_WITH_WARNINGS" in validation.output
    assert "Data source fallback policy status=PASS_WITH_WARNINGS" in report.output
    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["fallback_used_sources"] == ["marketstack_eod_daily_prices"]
    assert fallback_record["fallback_metadata"]["status"] == "COMPLETE"
    assert fallback_record["fallback_metadata"]["fallback_reason"] == (
        "primary outage with explicit metadata"
    )
    assert fallback_record["fallback_metadata"]["provider"] == "Marketstack"
    assert fallback_record["fallback_metadata"]["endpoint"] == "test.endpoint"
    assert fallback_record["fallback_metadata"]["request_parameters"] == "UNKNOWN"
    assert fallback_record["fallback_metadata"]["downloaded_at"]
    assert fallback_record["fallback_metadata"]["downloaded_at_source"] == "cache_file_mtime_utc"
    assert fallback_record["fallback_metadata"]["row_count"] == 1
    assert fallback_record["fallback_metadata"]["checksum"]
    assert any(
        issue["code"] == "fallback_used_manual_review_required"
        for issue in payload["validation_issues"]
    )


def test_data_source_fallback_policy_fail_closed_when_no_valid_source(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)

    payload, paths = build_and_write_data_source_fallback_policy(
        config=fixture["config"],
        policy=fixture["policy"],
        as_of=date(2026, 6, 16),
        output_dir=tmp_path / "fallback_policy_blocked",
        unavailable_source_ids=[
            "fmp_eod_daily_prices",
            "cboe_vix_daily_prices",
            "fred_daily_rates",
        ],
    )
    validation = validate_data_source_fallback_policy_payload(
        payload,
        report_path=paths["report_json"],
    )

    assert payload["status"] == "FAIL"
    assert validation.status == "FAIL"
    assert payload["fallback_status"] == FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE
    assert payload["summary"]["blocking_data_types"] == ["price_data", "macro_rate_data"]
    assert payload["summary"]["fallback_unavailable_count"] == 1
    assert payload["summary"]["blocked_no_valid_source_count"] == 1
    assert any(
        issue.code == "fallback_policy_blocking_state" for issue in validation.issues
    )


def _fixture(tmp_path: Path) -> dict[str, object]:
    fmp_cache = _write_cache(tmp_path / "fmp_prices.csv")
    cboe_cache = _write_cache(tmp_path / "cboe_vix.csv")
    marketstack_cache = _write_cache(tmp_path / "marketstack_prices.csv")
    fred_cache = _write_cache(tmp_path / "fred_rates.csv")
    yahoo_cache = _write_cache(tmp_path / "yahoo_prices.csv")
    return {
        "config": DataSourcesConfig(
            sources=[
                _source(
                    source_id="fmp_eod_daily_prices",
                    provider="Financial Modeling Prep",
                    source_type="paid_vendor",
                    domains=["market_prices"],
                    cache_paths=[str(fmp_cache)],
                ),
                _source(
                    source_id="cboe_vix_daily_prices",
                    provider="Cboe",
                    source_type="primary_source",
                    domains=["market_prices"],
                    cache_paths=[str(cboe_cache)],
                ),
                _source(
                    source_id="marketstack_eod_daily_prices",
                    provider="Marketstack",
                    source_type="paid_vendor",
                    domains=["market_prices"],
                    cache_paths=[str(marketstack_cache)],
                ),
                _source(
                    source_id="yahoo_finance_daily_prices",
                    provider="Yahoo Finance",
                    source_type="public_convenience",
                    domains=["market_prices"],
                    cache_paths=[str(yahoo_cache)],
                ),
                _source(
                    source_id="fred_daily_rates",
                    provider="FRED",
                    source_type="primary_source",
                    domains=["macro_rates"],
                    cache_paths=[str(fred_cache)],
                ),
            ]
        ),
        "policy": {
            "schema_version": 1,
            "policy_version": "data_source_fallback_policy_v1",
            "policy_metadata": {
                "owner": "test",
                "status": "pilot_baseline",
                "rationale": "test fallback states",
                "intended_effect": "make fallback explicit",
                "validation_evidence": "focused tests",
                "review_condition": "policy change",
            },
            "safety_boundary": {
                "read_only": True,
                "paper_shadow_research_only": True,
                "data_refresh_allowed": False,
                "cache_mutation_allowed": False,
                "score_or_backtest_allowed": False,
                "broker_action_allowed": False,
                "order_ticket_allowed": False,
                "production_state_mutation_allowed": False,
                "production_effect": "none",
            },
            "metadata_requirements": {
                "fallback_used_required_fields": [
                    "fallback_state",
                    "primary_source_id",
                    "fallback_source_id",
                    "fallback_reason",
                    "provider",
                    "endpoint",
                    "request_parameters",
                    "downloaded_at",
                    "row_count",
                    "checksum",
                    "source_priority_rank",
                    "source_eligibility_status",
                    "source_artifact_path",
                    "production_effect",
                ]
            },
            "eligibility_policy": {
                "allowed_source_types": ["primary_source", "paid_vendor"],
                "required_status": "active",
                "disallowed_source_types": ["public_convenience", "manual_input"],
            },
            "source_groups": [
                {
                    "data_type": "price_data",
                    "domain": "market_prices",
                    "primary_sources": [
                        "fmp_eod_daily_prices",
                        "cboe_vix_daily_prices",
                    ],
                    "fallback_sources": ["marketstack_eod_daily_prices"],
                    "ineligible_sources": ["yahoo_finance_daily_prices"],
                    "artifact_metadata_required": True,
                    "fail_closed_when_unavailable": True,
                    "owner_action": (
                        "restore_primary_price_source_or_record_explicit_fallback_metadata"
                    ),
                },
                {
                    "data_type": "macro_rate_data",
                    "domain": "macro_rates",
                    "primary_sources": ["fred_daily_rates"],
                    "fallback_sources": [],
                    "ineligible_sources": [],
                    "artifact_metadata_required": True,
                    "fail_closed_when_unavailable": True,
                    "owner_action": "restore_fred_macro_source_before_interpreting_macro_inputs",
                },
            ],
        },
    }


def _source(
    *,
    source_id: str,
    provider: str,
    source_type: str,
    domains: list[str],
    cache_paths: list[str],
) -> DataSourceConfig:
    return DataSourceConfig(
        source_id=source_id,
        provider=provider,
        source_type=source_type,
        status="active",
        domains=domains,
        endpoint="test.endpoint",
        adapter="TestAdapter",
        cadence="daily",
        requires_credentials=source_type == "paid_vendor",
        cache_paths=cache_paths,
        primary_for=["test_input"],
        audit_fields=[
            "provider",
            "endpoint",
            "request_parameters",
            "downloaded_at",
            "row_count",
            "checksum",
        ],
        validation_checks=["schema", "checksum"],
        limitations=["测试公开便利源限制。"] if source_type == "public_convenience" else [],
        owner_notes="test",
    )


def _write_cache(path: Path) -> Path:
    path.write_text("date,value\n2026-06-15,1\n", encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
