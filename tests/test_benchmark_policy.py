from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.benchmark_policy import (
    load_benchmark_policy,
    lookup_benchmark_policy_entry,
    render_benchmark_policy_report,
    render_benchmark_policy_summary_section,
    validate_benchmark_policy,
)
from ai_trading_system.cli import app


def test_default_benchmark_policy_validates_and_explains_roles() -> None:
    report = validate_benchmark_policy(
        load_benchmark_policy(),
        as_of=date(2026, 5, 4),
        selected_strategy_ticker="SMH",
        selected_benchmark_tickers=("SPY", "QQQ", "SMH", "SOXX"),
    )
    markdown = render_benchmark_policy_report(report)
    summary = render_benchmark_policy_summary_section(report)

    assert report.passed is True
    assert report.status == "PASS"
    assert report.instrument_count >= 4
    assert "广义美股 Beta" in markdown
    assert "半导体主题 Beta" in summary
    assert "不能单独代表完整 AI 产业链" in summary


def test_benchmark_policy_rejects_unregistered_selected_benchmark() -> None:
    report = validate_benchmark_policy(
        load_benchmark_policy(),
        as_of=date(2026, 5, 4),
        selected_strategy_ticker="SMH",
        selected_benchmark_tickers=("SPY", "ARKK"),
    )

    assert report.passed is False
    assert "selected_benchmark_not_registered" in {issue.code for issue in report.issues}


def test_benchmark_policy_rejects_duplicate_tickers(tmp_path: Path) -> None:
    input_path = tmp_path / "benchmark_policy.yaml"
    input_path.write_text(
        _policy_yaml(
            instruments=_instrument_yaml("SPY", "SPY", "broad_market_beta")
            + _instrument_yaml("SPY_DUP", "SPY", "broad_market_beta")
        ),
        encoding="utf-8",
    )

    report = validate_benchmark_policy(
        load_benchmark_policy(input_path),
        as_of=date(2026, 5, 4),
    )

    assert report.passed is False
    assert "duplicate_benchmark_ticker" in {issue.code for issue in report.issues}


def test_custom_basket_candidate_requires_pit_lifecycle(tmp_path: Path) -> None:
    input_path = tmp_path / "benchmark_policy.yaml"
    input_path.write_text(
        _policy_yaml(
            custom_baskets="""  - basket_id: ai_chain_basket.v1
    name: AI chain basket
    status: candidate
    description: test basket
    point_in_time_lifecycle: false
    lifecycle_path: ""
    weighting_method: equal_weight
    rebalance_frequency: monthly
    source_config_paths: []
    limitations: []
"""
        ),
        encoding="utf-8",
    )

    report = validate_benchmark_policy(
        load_benchmark_policy(input_path),
        as_of=date(2026, 5, 4),
    )

    assert report.passed is False
    assert "benchmark_policy_load_error" in {issue.code for issue in report.issues}


def test_feedback_benchmark_policy_cli_validates_and_looks_up(tmp_path: Path) -> None:
    report_path = tmp_path / "benchmark_policy.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "validate-benchmark-policy",
            "--as-of",
            "2026-05-04",
            "--strategy-ticker",
            "SMH",
            "--benchmarks",
            "SPY,QQQ,SMH,SOXX",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "基准政策状态：PASS" in result.output
    assert report_path.exists()
    entry = lookup_benchmark_policy_entry("config/benchmark_policy.yaml", "SPY")
    assert entry is not None

    lookup = CliRunner().invoke(
        app,
        [
            "feedback",
            "lookup-benchmark-policy",
            "--id",
            "SMH",
        ],
    )
    assert lookup.exit_code == 0
    assert "半导体主题 Beta" in lookup.output


def _policy_yaml(
    *,
    instruments: str | None = None,
    custom_baskets: str = "",
) -> str:
    return f"""policy_id: test_policy.v1
version: v1
status: production
owner: test
description: test policy
default_ai_proxy: SPY
default_benchmarks:
  - SPY
minimum_roles:
  - broad_market_beta
last_reviewed_at: 2026-05-04
next_review_due: 2026-06-04
instruments:
{instruments or _instrument_yaml("SPY", "SPY", "broad_market_beta")}
custom_ai_baskets:
{custom_baskets or "  []"}
"""


def _instrument_yaml(benchmark_id: str, ticker: str, role: str) -> str:
    return f"""  - benchmark_id: {benchmark_id}
    ticker: {ticker}
    name: {ticker} benchmark
    instrument_type: etf
    role: {role}
    default_ai_proxy_eligible: true
    default_benchmark: true
    source_config_paths: []
    use_cases:
      - test
    interpretation: test interpretation
    limitations: []
"""
