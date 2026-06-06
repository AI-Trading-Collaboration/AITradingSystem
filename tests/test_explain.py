from __future__ import annotations

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.explain import explain_query, render_explain_result


def test_explain_field_from_dictionary() -> None:
    result = explain_query("scores_daily.score", kind="field")

    assert result["found"] is True
    assert result["kind"] == "field"
    assert result["production_effect"] == "production_scoring_record"
    assert "0-100" in result["meaning"]


def test_explain_artifact_from_catalog() -> None:
    result = explain_query("risk_event_prereview_queue", kind="artifact")
    rendered = render_explain_result(result)

    assert result["found"] is True
    assert result["kind"] == "artifact"
    assert "risk_event_prereview_queue" in rendered
    assert "production_effect" in rendered


def test_explain_gate_static_entry() -> None:
    result = explain_query("binding gate", kind="gate")

    assert result["found"] is True
    assert result["kind"] == "gate"
    assert result["production_effect"] == "advisory_position_cap"
    assert "最严格" in result["meaning"]


def test_explain_cli_outputs_field_explanation() -> None:
    result = CliRunner().invoke(app, ["explain", "scores_daily.score"])

    assert result.exit_code == 0
    assert "状态：FOUND" in result.output
    assert "production_scoring_record" in result.output


def test_explain_cli_missing_exits_nonzero() -> None:
    result = CliRunner().invoke(app, ["explain", "missing.field"])

    assert result.exit_code != 0
    assert "状态：MISSING" in result.output
