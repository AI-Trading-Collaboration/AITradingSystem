from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system import first_layer_proxy_coverage_audit
from ai_trading_system.cli_commands.research_trends import trends_app


def test_proxy_coverage_audit_marks_price_proxy_not_true_breadth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path = tmp_path / "prices.csv"
    feature_root = tmp_path / "features"
    feature_root.mkdir()
    _write_prices(prices_path)
    _write_participation_feature(feature_root / "participation_proxy_free_v2.parquet")

    monkeypatch.setattr(
        first_layer_proxy_coverage_audit,
        "validate_cached_market_data",
        lambda **_: {
            "status": "PASS",
            "passed": True,
            "checked_at": "2026-06-28T00:00:00+00:00",
            "as_of": "2023-04-10",
            "price_path": str(prices_path),
            "rates_path": str(tmp_path / "rates.csv"),
            "secondary_prices_path": "",
            "expected_price_tickers": ["QQQ", "SPY", "SMH", "SOXX"],
            "expected_rate_series": [],
            "price_row_count": 280,
            "rate_row_count": 0,
            "price_checksum": "fixture",
            "rate_checksum": "fixture",
            "warning_count": 0,
            "error_count": 0,
        },
    )

    payload = first_layer_proxy_coverage_audit.run_first_layer_proxy_coverage_audit_pack(
        free_feature_registry_path=_free_registry(tmp_path),
        participation_proxy_registry_path=_participation_registry(tmp_path),
        coverage_matrix_path=_coverage_matrix(tmp_path),
        pit_contract_path=_pit_contract(tmp_path),
        fmp_gate_path=_fmp_gate(tmp_path),
        feature_root=feature_root,
        prices_path=prices_path,
        rates_path=tmp_path / "rates.csv",
        marketstack_prices_path=None,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        as_of_date=date(2023, 4, 10),
    )

    rows = {row["proxy_id"]: row for row in payload["rows"]}
    assert payload["status"] == "FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED"
    assert payload["summary"]["replacement_for_true_breadth_count"] == 0
    assert payload["promotion_allowed"] is False
    assert rows["smh_to_qqq"]["data_available"] is True
    assert rows["smh_to_qqq"]["PIT_safe_or_not"] == "PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH"
    assert rows["smh_to_qqq"]["replacement_for_true_breadth"] is False
    assert rows["rsp_to_spy"]["data_available"] is False
    assert rows["rsp_to_spy"]["missing_tickers"] == ["RSP"]
    assert rows["participation_proxy_free_v1"]["data_available"] is True
    assert all(row["replacement_for_true_breadth"] is False for row in payload["rows"])
    assert Path(payload["artifact_paths"]["proxy_coverage_matrix"]).exists()
    assert Path(payload["artifact_paths"]["first_layer_proxy_coverage_audit_doc"]).exists()


def test_first_layer_proxy_coverage_audit_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-proxy-coverage-audit" in result.output


def _write_prices(path: Path) -> None:
    dates = pd.bdate_range("2021-02-22", periods=70)
    rows = []
    for ticker in ["QQQ", "SPY", "SMH", "SOXX"]:
        for idx, day in enumerate(dates):
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "adj_close": 100.0 + idx,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_participation_feature(path: Path) -> None:
    dates = pd.bdate_range("2021-02-22", periods=5)
    pd.DataFrame(
        {
            "date": [day.date().isoformat() for day in dates],
            "feature_family": "participation_proxy_free_v2",
        }
    ).to_parquet(path, index=False)


def _free_registry(tmp_path: Path) -> Path:
    path = tmp_path / "free_registry.yaml"
    path.write_text(
        """
schema_version: test
families:
  rates_liquidity_free_v1:
    source: FRED
    usage: macro context
    PIT_status: PIT_APPROVED_MARKET_SERIES
    allowed_usage: [family_level_reablation]
    blocked_usage: [promotion, paper_shadow, production, broker]
  participation_proxy_free_v1:
    source: ETF ratios
    usage: participation diagnostic only
    PIT_status: NOT_TRUE_PIT_BREADTH
    allowed_usage: [diagnostic_only]
    blocked_usage: [model_ready_breadth, promotion, paper_shadow, production, broker]
""",
        encoding="utf-8",
    )
    return path


def _participation_registry(tmp_path: Path) -> Path:
    path = tmp_path / "participation_registry.yaml"
    path.write_text(
        """
schema_version: test
proxies:
  - proxy_id: smh_to_qqq
    numerator: SMH
    denominator: QQQ
    status: DIAGNOSTIC_ONLY
    caveats: [NOT_TRUE_PIT_BREADTH, ETF_PRICE_ONLY, SURVIVORSHIP_SAFE_IF_ETF_PRICE_ONLY]
  - proxy_id: rsp_to_spy
    numerator: RSP
    denominator: SPY
    status: REGISTRY_ONLY
    caveats: [NOT_TRUE_PIT_BREADTH, PRICE_CACHE_COVERAGE_REQUIRED, DIAGNOSTIC_ONLY]
""",
        encoding="utf-8",
    )
    return path


def _coverage_matrix(tmp_path: Path) -> Path:
    path = tmp_path / "coverage.yaml"
    path.write_text(
        """
schema_version: test
rows:
  - feature_family: rates_liquidity_free_v1
    earliest_available_date: '2021-02-22'
    primary_window_coverage: covered
  - feature_family: participation_proxy_free_v1
    earliest_available_date: source_dependent
    primary_window_coverage: registry_only
""",
        encoding="utf-8",
    )
    return path


def _pit_contract(tmp_path: Path) -> Path:
    path = tmp_path / "pit_contract.yaml"
    path.write_text(
        """
schema_version: test
rows:
  - source: Alpha Vantage listing status
    status_detail: ALPHA_VANTAGE_LISTING_STATUS_NOT_FETCHED_NETWORK_DISABLED
  - source: FMP ETF holdings
    status_detail: FMP_HOLDINGS_NOT_AVAILABLE
""",
        encoding="utf-8",
    )
    return path


def _fmp_gate(tmp_path: Path) -> Path:
    path = tmp_path / "fmp_gate.yaml"
    path.write_text("status: FMP_HOLDINGS_NOT_AVAILABLE\n", encoding="utf-8")
    return path
