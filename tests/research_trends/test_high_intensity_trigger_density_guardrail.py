from __future__ import annotations

from high_intensity_threshold_selection_fixtures import sample_selected_candidate

from ai_trading_system.high_intensity_risk_cap_threshold_selection import (
    build_high_intensity_trigger_density_guardrail,
)


def _events(*dates: str) -> list[dict[str, str]]:
    return [{"date": value} for value in dates]


def test_trigger_density_guardrail_passes_below_warning_threshold() -> None:
    candidate = {**sample_selected_candidate(), "trigger_density_estimate": 0.05}

    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=_events("2023-01-03", "2023-02-07"),
    )

    assert guardrail["density_guardrail_status"] == "PASS"
    assert guardrail["density_guardrail_blockers"] == []


def test_trigger_density_guardrail_warns_above_warning_threshold() -> None:
    candidate = {**sample_selected_candidate(), "trigger_density_estimate": 0.09}

    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=_events("2023-01-03", "2023-02-07"),
    )

    assert guardrail["density_guardrail_status"] == "PASS_WITH_WARNINGS"
    assert "DENSITY_ABOVE_WARNING_THRESHOLD" in guardrail["density_guardrail_warnings"]


def test_trigger_density_guardrail_blocks_above_blocking_threshold() -> None:
    candidate = {**sample_selected_candidate(), "trigger_density_estimate": 0.13}

    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=_events("2023-01-03", "2023-02-07"),
    )

    assert guardrail["density_guardrail_status"] == "BLOCKED"
    assert "DENSITY_ABOVE_BLOCKING_THRESHOLD" in guardrail["density_guardrail_blockers"]


def test_trigger_density_guardrail_warns_on_monthly_concentration() -> None:
    candidate = {**sample_selected_candidate(), "trigger_density_estimate": 0.05}

    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=_events(
            "2023-01-03",
            "2023-01-10",
            "2023-01-17",
            "2023-01-24",
        ),
    )

    assert guardrail["density_guardrail_status"] == "PASS_WITH_WARNINGS"
    assert "MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL" in (
        guardrail["density_guardrail_warnings"]
    )


def test_trigger_density_guardrail_blocks_on_consecutive_trigger_days() -> None:
    candidate = {**sample_selected_candidate(), "trigger_density_estimate": 0.05}

    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=candidate,
        selected_event_rows=_events(
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
        ),
    )

    assert guardrail["density_guardrail_status"] == "BLOCKED"
    assert "CONSECUTIVE_TRIGGER_DAYS_ABOVE_GUARDRAIL" in (
        guardrail["density_guardrail_blockers"]
    )
