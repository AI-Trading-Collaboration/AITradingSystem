from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    run_data_foundation_acceptance,
    run_data_source_qualification_remediation,
    run_data_source_remediation_execution,
    run_data_source_requirement_matrix,
)
from ai_trading_system.data_source_subscription_audit import (
    run_current_subscription_data_coverage_audit,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

SECRET_ENV = {
    "FINANCIAL_MODELING_PREP_API_KEY": "FMP_SECRET_VALUE_12345",
    "FMP_API_KEY": "FMP_ALT_SECRET_VALUE_12345",
    "MARKETSTACK_API_KEY": "MARKETSTACK_SECRET_VALUE_12345",
    "EODHD_API_KEY": "EODHD_SECRET_VALUE_12345",
    "ALPHA_VANTAGE_API_KEY": "ALPHA_SECRET_VALUE_12345",
    "FRED_API_KEY": "FRED_SECRET_VALUE_12345",
    "CONGRESS_API_KEY": "CONGRESS_SECRET_VALUE_12345",
    "GOVINFO_API_KEY": "GOVINFO_SECRET_VALUE_12345",
}

REQUIRED_ENDPOINT_FIELDS = {
    "endpoint_name",
    "accessible",
    "coverage_for_representative_universe",
    "historical_depth_observed",
    "raw_price_supported",
    "adjusted_price_supported",
    "splits_supported",
    "dividends_supported",
    "delisted_supported",
    "fundamentals_supported",
    "event_calendar_supported",
    "available_time_supported",
    "source_manifest_possible",
    "current_view_only_risk",
    "PIT_qualification_gap",
    "likely_allowed_use",
}


def test_current_subscription_data_coverage_contract(tmp_path: Path) -> None:
    requirement_path = _build_requirement_matrix(tmp_path)
    output_root = tmp_path / "subscription"

    result = run_current_subscription_data_coverage_audit(
        source_requirement_matrix_path=requirement_path,
        output_root=output_root,
        env=SECRET_ENV,
        http_get=fake_http_get,
        timeout_seconds=1.0,
    )

    assert result["report_type"] == "current_subscription_data_coverage_matrix"
    assert result["status"] == "COVERAGE_AUDIT_RECORDED_NO_SOURCE_UPGRADE"
    _assert_safety_boundary(result)
    assert result["api_key_material_recorded"] is False
    assert result["status_upgrade_attempted"] is False
    assert result["summary"]["provider_count"] == 9
    assert result["summary"]["endpoint_probe_count"] == 29
    assert result["summary"]["requirement_match_count"] == 9
    assert result["summary"]["accessible_endpoint_count"] >= 20

    serialized = json.dumps(result, ensure_ascii=False, sort_keys=True)
    for secret in SECRET_ENV.values():
        assert secret not in serialized

    provider_row = result["provider_key_statuses"][0]
    assert set(provider_row) == {
        "provider",
        "key_present",
        "endpoint_accessible",
        "plan_or_limit_info_if_available",
        "sanitized_error_class",
        "allowed_uses_candidate",
    }
    assert all(row["key_present"] is True for row in result["provider_key_statuses"][:5])

    endpoint_rows = result["endpoint_coverage_matrix"]
    assert len(endpoint_rows) == 29
    assert all(REQUIRED_ENDPOINT_FIELDS <= set(row) for row in endpoint_rows)
    assert {
        "promotion_candidate_after_qualification",
        "diagnostic_only",
        "research_label_only",
    } <= {row["likely_allowed_use"] for row in endpoint_rows}
    assert any(row["provider"] == "SEC EDGAR" and row["accessible"] for row in endpoint_rows)

    matches = result["requirement_subscription_matches"]
    assert len(matches) == 9
    assert all(match["status_upgrade_attempted"] is False for match in matches)
    assert all(match["promotion_gate_allowed"] is False for match in matches)
    assert {match["can_current_subscription_cover"] for match in matches} <= {
        "true",
        "false",
        "unknown",
    }
    assert any(match["can_current_subscription_cover"] == "true" for match in matches)
    assert any(match["can_current_subscription_cover"] == "unknown" for match in matches)

    output_path = output_root / "current_subscription_data_coverage_matrix.json"
    assert output_path.exists()
    assert output_path.with_suffix(".md").exists()
    persisted = json.loads(output_path.read_text(encoding="utf-8"))
    _assert_safety_boundary(persisted)
    persisted_text = output_path.read_text(encoding="utf-8")
    for secret in SECRET_ENV.values():
        assert secret not in persisted_text


def test_current_subscription_data_coverage_cli_smoke(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    import requests

    requirement_path = _build_requirement_matrix(tmp_path)
    output_root = tmp_path / "subscription_cli"
    monkeypatch.setattr(requests, "get", fake_http_get)
    result = CliRunner().invoke(
        app,
        [
            "data",
            "source-qualification",
            "subscription-audit",
            "--source-requirement-matrix",
            str(requirement_path),
            "--output-root",
            str(output_root),
            "--timeout-seconds",
            "1",
        ],
        env=SECRET_ENV,
    )

    assert result.exit_code == 0, result.output
    assert "Current subscription data coverage" in result.output
    assert "matrix：COVERAGE_AUDIT_RECORDED_NO_SOURCE_UPGRADE" in result.output
    assert "provider_count=9" in result.output
    assert "endpoint_probe_count=29" in result.output
    assert "api_key_material_recorded=False" not in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    for secret in SECRET_ENV.values():
        assert secret not in result.output
    assert (output_root / "current_subscription_data_coverage_matrix.json").exists()


def test_current_subscription_registry_catalog_schema_and_tiers() -> None:
    test_path = "tests/test_current_subscription_data_coverage_audit.py"
    assert test_path in TIER_SPECS["fast-unit"].paths
    assert test_path in TIER_SPECS["contract-validation"].paths

    registry = safe_load_yaml_path(PROJECT_ROOT / "config" / "report_registry.yaml")
    report_ids = {str(item.get("report_id")): item for item in registry["reports"]}
    entry = report_ids["current_subscription_data_coverage_matrix"]
    assert entry["command"] == "aits data source-qualification subscription-audit"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False

    catalog = (PROJECT_ROOT / "docs" / "artifact_catalog.md").read_text(encoding="utf-8")
    assert "current_subscription_data_coverage_matrix.json/md" in catalog
    assert "不能输出或落盘 API key" in catalog

    system_flow = (PROJECT_ROOT / "docs" / "system_flow.md").read_text(encoding="utf-8")
    assert "aits data source-qualification subscription-audit" in system_flow
    assert "TRADING-738 VALIDATING" in system_flow

    schema = (
        PROJECT_ROOT / "docs" / "schema" / "current_subscription_data_coverage_matrix.schema.json"
    )
    assert schema.exists()


def fake_http_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout: float | None = None,
    headers: dict[str, str] | None = None,
) -> FakeResponse:
    assert timeout is not None
    params = params or {}
    payload: Any
    if "marketstack" in url:
        payload = {"data": [{"date": "2024-01-03", "symbol": "MSFT", "close": 100.0}]}
    elif "alphavantage" in url:
        payload = {
            "Meta Data": {"2. Symbol": params.get("symbol", "MSFT")},
            "Time Series (Daily)": {"2024-01-03": {"4. close": "100.0"}},
        }
    elif "fred/series/observations" in url:
        payload = {"observations": [{"date": "2024-01-03", "value": "4.0"}]}
    elif "cboe.com" in url:
        return FakeResponse(text="DATE,OPEN,HIGH,LOW,CLOSE\n2024-01-03,1,2,1,2\n")
    elif "sec.gov" in url:
        payload = {"cik": "0000789019", "filings": {"recent": {"acceptanceDateTime": []}}}
    elif "govinfo" in url:
        payload = {"packages": [{"packageId": "BILLS-118hr1ih", "lastModified": "2024-01-03"}]}
    elif "congress.gov" in url:
        payload = {"bills": [{"updateDate": "2024-01-03"}]}
    elif "eodhd.com" in url:
        if "fundamentals" in url:
            payload = {"General": {"Code": "MSFT"}, "Financials": {"Income_Statement": {}}}
        else:
            payload = [{"date": "2024-01-03", "close": 100.0, "adjusted_close": 100.0}]
    elif "financialmodelingprep" in url:
        if "delisted" in url or "constituent" in url:
            payload = [{"symbol": "MSFT", "date": "2024-01-03"}]
        else:
            payload = [{"date": "2024-01-03", "symbol": params.get("symbol", "MSFT")}]
    else:
        payload = [{"date": "2024-01-03"}]
    return FakeResponse(payload=payload)


class FakeResponse:
    def __init__(
        self,
        *,
        payload: Any | None = None,
        text: str = "",
        status_code: int = 200,
    ) -> None:
        self._payload = payload
        self.text = text
        self.status_code = status_code

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


def _build_requirement_matrix(tmp_path: Path) -> Path:
    acceptance_root = tmp_path / "acceptance"
    qualification_root = tmp_path / "qualification"
    execution_root = tmp_path / "execution"
    requirement_root = tmp_path / "requirements"
    run_data_foundation_acceptance(output_root=acceptance_root)
    run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        output_root=qualification_root,
    )
    run_data_source_remediation_execution(
        acceptance_report_path=acceptance_root / "data_foundation_acceptance_report.json",
        qualification_matrix_path=qualification_root / "data_source_qualification_matrix.json",
        remediation_plan_path=qualification_root / "data_foundation_remediation_plan.json",
        updated_acceptance_summary_path=(
            qualification_root / "data_foundation_acceptance_summary_updated.json"
        ),
        acceptance_output_root=acceptance_root / "rerun",
        output_root=execution_root,
    )
    run_data_source_requirement_matrix(
        remediation_execution_report_path=(
            execution_root / "data_source_remediation_execution_report.json"
        ),
        remediation_item_results_path=execution_root / "data_source_remediation_item_results.json",
        qualification_matrix_updated_path=(
            execution_root / "data_source_qualification_matrix_updated.json"
        ),
        output_root=requirement_root,
    )
    return requirement_root / "data_source_requirement_matrix.json"


def _assert_safety_boundary(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0
