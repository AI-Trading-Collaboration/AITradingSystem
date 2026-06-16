from __future__ import annotations

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cache_catalog import (
    build_and_write_cache_catalog,
    validate_cache_catalog_payload,
)
from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig
from ai_trading_system.reports import reader_brief


def test_cache_catalog_records_checksums_and_reader_brief(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    payload, paths = build_and_write_cache_catalog(
        config=fixture["config"],
        policy=fixture["policy"],
        as_of=date(2026, 6, 16),
        output_dir=tmp_path / "cache_catalog",
        refresh_audit_output_dir=fixture["refresh_audit_dir"],
        validation_audit_dir=fixture["validation_audit_dir"],
        project_root=tmp_path,
    )
    validation = validate_cache_catalog_payload(
        payload,
        catalog_path=paths["catalog_json"],
    )
    summary = reader_brief._cache_catalog_summary(
        {
            "reports": [
                {
                    "report_id": "cache_catalog",
                    "latest_artifact_path": str(paths["catalog_json"]),
                }
            ]
        }
    )
    records = {record["entry_id"]: record for record in payload["records"]}

    assert payload["status"] == "PASS"
    assert validation.status == "PASS"
    assert payload["cache_integrity_status"] == "OK"
    assert payload["summary"]["entry_count"] == 4
    assert records["primary_price_cache"]["row_count"] == 2
    assert records["primary_price_cache"]["column_count"] == 8
    assert records["primary_price_cache"]["checksum"] == _sha256(fixture["price_path"])
    assert records["primary_price_cache"]["source_name"] == "Financial Modeling Prep, Cboe"
    assert records["market_panel_latest"]["row_count"] == 2
    assert records["market_panel_latest"]["as_of"] == "2026-06-12"
    assert paths["catalog_json"].exists()
    assert paths["catalog_markdown"].exists()
    assert paths["validation_json"].exists()
    assert paths["reader_brief_section"].exists()
    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "PASS"
    assert summary["cache_integrity_status"] == "OK"
    assert summary["missing_required_count"] == 0
    assert summary["production_effect"] == "none"


def test_cache_catalog_fails_closed_for_missing_required_entry(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture["policy"]["entries"][0]["path"] = str(tmp_path / "missing_prices.csv")

    payload, paths = build_and_write_cache_catalog(
        config=fixture["config"],
        policy=fixture["policy"],
        as_of=date(2026, 6, 16),
        output_dir=tmp_path / "cache_catalog_missing",
        refresh_audit_output_dir=fixture["refresh_audit_dir"],
        validation_audit_dir=fixture["validation_audit_dir"],
        project_root=tmp_path,
    )
    validation = validate_cache_catalog_payload(
        payload,
        catalog_path=paths["catalog_json"],
    )

    assert payload["status"] == "FAIL"
    assert payload["cache_integrity_status"] == "FAIL"
    assert payload["summary"]["missing_required_count"] == 1
    assert "primary_price_cache" in payload["summary"]["blocking_entry_ids"]
    assert validation.status == "FAIL"
    assert any(issue.code == "required_cache_entry_missing" for issue in validation.issues)


def test_cache_catalog_cli_detects_explicit_checksum_mismatch(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    config_path = tmp_path / "data_sources.yaml"
    policy_path = tmp_path / "cache_catalog.yaml"
    output_dir = tmp_path / "cache_catalog_cli"
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
            "cache-catalog",
            "run",
            "--as-of",
            "2026-06-16",
            "--config-path",
            str(config_path),
            "--policy-path",
            str(policy_path),
            "--output-dir",
            str(output_dir),
            "--refresh-audit-output-dir",
            str(fixture["refresh_audit_dir"]),
            "--validation-audit-dir",
            str(fixture["validation_audit_dir"]),
            "--expected-checksum",
            "primary_price_cache=not-the-observed-checksum",
        ],
    )

    assert run.exit_code == 1, run.output
    assert "Cache catalog status=FAIL" in run.output
    assert "checksum_mismatch_count=1" in run.output

    latest_pointer = json.loads((output_dir / "latest_cache_catalog.json").read_text())
    payload = json.loads(Path(latest_pointer["catalog_path"]).read_text(encoding="utf-8"))
    assert payload["summary"]["checksum_mismatch_count"] == 1
    assert "primary_price_cache" in payload["summary"]["blocking_entry_ids"]


def _fixture(tmp_path: Path) -> dict[str, object]:
    price_path = tmp_path / "prices_daily.csv"
    secondary_price_path = tmp_path / "prices_marketstack_daily.csv"
    rate_path = tmp_path / "rates_daily.csv"
    market_panel_path = tmp_path / "outputs" / "reports" / "market_panel_2026-06-12.json"
    price_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2026-06-12,NVDA,1,2,1,2,2,100\n"
        "2026-06-12,^VIX,20,21,19,20,20,\n",
        encoding="utf-8",
    )
    secondary_price_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n"
        "2026-06-12,NVDA,1,2,1,2,2,100\n",
        encoding="utf-8",
    )
    rate_path.write_text("date,series,value\n2026-06-12,DGS10,4.1\n", encoding="utf-8")
    market_panel_path.parent.mkdir(parents=True)
    market_panel_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "market_panel",
                "status": "PASS",
                "as_of": "2026-06-12",
                "proxies": [
                    {"symbol": "SPY", "role": "benchmark_proxy"},
                    {"symbol": "DGS10", "role": "liquidity_proxy"},
                ],
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    refresh_audit_dir = tmp_path / "refresh_audit"
    validation_audit_dir = tmp_path / "validation_audit"
    _write_refresh_audit(refresh_audit_dir)
    _write_validate_data_audit(validation_audit_dir)
    return {
        "price_path": price_path,
        "secondary_price_path": secondary_price_path,
        "rate_path": rate_path,
        "market_panel_path": market_panel_path,
        "refresh_audit_dir": refresh_audit_dir,
        "validation_audit_dir": validation_audit_dir,
        "config": DataSourcesConfig(
            sources=[
                _source(
                    source_id="fmp_eod_daily_prices",
                    provider="Financial Modeling Prep",
                    source_type="paid_vendor",
                    domains=["market_prices"],
                    cache_paths=[str(price_path)],
                ),
                _source(
                    source_id="cboe_vix_daily_prices",
                    provider="Cboe",
                    source_type="primary_source",
                    domains=["market_prices"],
                    cache_paths=[str(price_path)],
                ),
                _source(
                    source_id="marketstack_eod_daily_prices",
                    provider="Marketstack",
                    source_type="paid_vendor",
                    domains=["market_prices"],
                    cache_paths=[str(secondary_price_path)],
                ),
                _source(
                    source_id="fred_daily_rates",
                    provider="FRED",
                    source_type="primary_source",
                    domains=["macro_rates"],
                    cache_paths=[str(rate_path)],
                ),
            ]
        ),
        "policy": {
            "schema_version": 1,
            "policy_version": "cache_catalog_policy_v1",
            "policy_metadata": {
                "owner": "test",
                "status": "pilot_baseline",
                "rationale": "test cache catalog",
                "intended_effect": "make cache metadata explicit",
                "validation_evidence": "focused tests",
                "review_condition": "policy change",
            },
            "safety_boundary": {
                "read_only": True,
                "data_refresh_allowed": False,
                "cache_mutation_allowed": False,
                "cache_repair_allowed": False,
                "score_or_backtest_allowed": False,
                "broker_action_allowed": False,
                "order_ticket_allowed": False,
                "production_state_mutation_allowed": False,
                "production_effect": "none",
            },
            "required_metadata_fields": [
                "entry_id",
                "data_type",
                "cache_path",
                "artifact_id",
                "as_of",
                "checksum",
                "row_count",
                "column_count",
                "created_at",
                "validated_at",
                "source_name",
                "refresh_audit_id",
            ],
            "entries": [
                _entry(
                    "primary_price_cache",
                    "price_data",
                    price_path,
                    ["fmp_eod_daily_prices", "cboe_vix_daily_prices"],
                ),
                _entry(
                    "secondary_price_cache",
                    "secondary_price_data",
                    secondary_price_path,
                    ["marketstack_eod_daily_prices"],
                ),
                _entry("macro_rate_cache", "macro_rate_data", rate_path, ["fred_daily_rates"]),
                {
                    "entry_id": "market_panel_latest",
                    "data_type": "market_panel_data",
                    "required": True,
                    "path_glob": str(tmp_path / "outputs" / "reports" / "market_panel_*.json"),
                    "format": "json",
                    "as_of_json_path": "as_of",
                    "row_count_json_path": "proxies",
                    "source_name": "aits reports market-panel",
                    "source_ids": [],
                },
            ],
        },
    }


def _entry(entry_id: str, data_type: str, path: Path, source_ids: list[str]) -> dict[str, object]:
    return {
        "entry_id": entry_id,
        "data_type": data_type,
        "required": True,
        "path": str(path),
        "format": "csv",
        "date_column": "date",
        "source_ids": source_ids,
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
        adapter="test",
        cadence="daily",
        cache_paths=cache_paths,
        primary_for=["test"],
        audit_fields=["provider", "endpoint", "checksum"],
        validation_checks=["schema", "checksum"],
    )


def _write_refresh_audit(output_dir: Path) -> None:
    audit_dir = output_dir / "data_refresh_audit_2026-06-16_test"
    audit_dir.mkdir(parents=True)
    audit_path = audit_dir / "data_refresh_audit.json"
    audit_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "data_refresh_audit",
                "audit_id": "data_refresh_audit_2026-06-16_test",
                "as_of": "2026-06-16",
                "generated_at": datetime(2026, 6, 16, tzinfo=UTC).isoformat(),
                "status": "PASS",
                "validation_status": "PASS",
                "production_effect": "none",
                "summary": {"audit_record_count": 1},
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "latest_data_refresh_audit.json").write_text(
        json.dumps(
            {
                "audit_id": "data_refresh_audit_2026-06-16_test",
                "audit_path": str(audit_path),
                "status": "PASS",
                "schema_version": 1,
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )


def _write_validate_data_audit(output_dir: Path) -> None:
    output_dir.mkdir(parents=True)
    record_path = output_dir / "validate_data_2026-06-16_test.json"
    record_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "data_refresh_audit_record",
                "audit_record_id": "data_validation_2026-06-16_test",
                "status": "SUCCESS",
                "as_of": "2026-06-16",
                "start_time": "2026-06-16T10:00:00+00:00",
                "end_time": "2026-06-16T10:01:00+00:00",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "latest_validate_data_audit.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_type": "data_refresh_audit_record",
                "audit_record_id": "data_validation_2026-06-16_test",
                "record_path": str(record_path),
                "as_of": "2026-06-16",
                "status": "SUCCESS",
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
