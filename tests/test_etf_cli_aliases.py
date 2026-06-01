from __future__ import annotations

from typer.testing import CliRunner

from ai_trading_system.cli import app


def test_etf_document_style_alias_groups_are_available() -> None:
    result = CliRunner().invoke(app, ["--help"], env={"COLUMNS": "160"}, terminal_width=160)

    assert result.exit_code == 0, result.output
    for command in ("features", "regime", "simulation", "report", "run", "experiments", "etf"):
        assert command in result.output


def test_existing_groups_expose_etf_aliases_without_hiding_existing_commands() -> None:
    runner = CliRunner()

    data = runner.invoke(app, ["data", "--help"], env={"COLUMNS": "160"}, terminal_width=160)
    signals = runner.invoke(app, ["signals", "--help"], env={"COLUMNS": "160"}, terminal_width=160)
    portfolio = runner.invoke(
        app,
        ["portfolio", "--help"],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert data.exit_code == 0, data.output
    assert "ingest" in data.output
    assert "validate" in data.output
    assert "diagnose-backtest-inputs" in data.output
    assert signals.exit_code == 0, signals.output
    assert "generate" in signals.output
    assert "build-snapshot" in signals.output
    assert portfolio.exit_code == 0, portfolio.output
    assert "allocate" in portfolio.output
    assert "exposure" in portfolio.output


def test_etf_compatibility_alias_help_points_to_etf_workflow() -> None:
    runner = CliRunner()
    commands = [
        ["data", "ingest", "--help"],
        ["data", "validate", "--help"],
        ["features", "build", "--help"],
        ["signals", "generate", "--help"],
        ["regime", "generate", "--help"],
        ["portfolio", "allocate", "--help"],
        ["simulation", "record", "--help"],
        ["simulation", "evaluate", "--help"],
        ["simulation", "report", "--help"],
        ["report", "daily", "--help"],
        ["run", "daily", "--help"],
        ["experiments", "register", "--help"],
        ["experiments", "run", "--help"],
        ["experiments", "compare", "--help"],
    ]

    for command in commands:
        result = runner.invoke(app, command, env={"COLUMNS": "160"}, terminal_width=160)
        assert result.exit_code == 0, result.output
        assert "ETF compatibility alias" in result.output


def test_root_backtest_keeps_existing_main_system_command() -> None:
    result = CliRunner().invoke(
        app,
        ["backtest", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "基于每日评分规则运行历史回测" in result.output
    assert "Usage:" in result.output
    assert "COMMAND [ARGS]" not in result.output
    assert "ETF compatibility alias" not in result.output


def test_etf_cli_exposes_documented_option_compatibility() -> None:
    runner = CliRunner()

    features = runner.invoke(
        app,
        ["features", "build", "--help"],
        env={"COLUMNS": "240", "NO_COLOR": "1", "TERM": "dumb"},
        terminal_width=240,
        color=False,
    )
    backtest = runner.invoke(
        app,
        ["etf", "backtest", "run", "--help"],
        env={"COLUMNS": "240", "NO_COLOR": "1", "TERM": "dumb"},
        terminal_width=240,
        color=False,
    )

    assert features.exit_code == 0, features.output
    assert "--end" in features.output
    assert backtest.exit_code == 0, backtest.output
    assert "--config" in backtest.output
