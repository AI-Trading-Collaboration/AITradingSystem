from __future__ import annotations

from ai_trading_system.breadth_participation_feasibility_audit import build_2303_task_route


def test_breadth_task_route_strict_pit_goes_to_generator_poc() -> None:
    route = build_2303_task_route(
        strict_pit_feasibility=True,
        pit_approximation_feasibility=False,
        current_constituents_proxy_feasibility=False,
    )

    assert route["next_task"] == "TRADING-2305_Breadth_Proxy_Candidate_Generator_POC"
    assert route["caveat"] == "STRICT_PIT_READY"


def test_breadth_task_route_pit_approximation_goes_to_generator_poc_with_caveat() -> None:
    route = build_2303_task_route(
        strict_pit_feasibility=False,
        pit_approximation_feasibility=True,
        current_constituents_proxy_feasibility=False,
    )

    assert route["next_task"] == "TRADING-2305_Breadth_Proxy_Candidate_Generator_POC"
    assert route["caveat"] == "PIT_APPROXIMATION_ONLY"


def test_breadth_task_route_current_proxy_goes_to_diagnostics_only() -> None:
    route = build_2303_task_route(
        strict_pit_feasibility=False,
        pit_approximation_feasibility=False,
        current_constituents_proxy_feasibility=True,
    )

    assert route["next_task"] == "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only"
    assert route["caveat"] == "SURVIVORSHIP_BIAS"


def test_breadth_task_route_no_reliable_data_goes_to_data_source_decision() -> None:
    route = build_2303_task_route(
        strict_pit_feasibility=False,
        pit_approximation_feasibility=False,
        current_constituents_proxy_feasibility=False,
    )

    assert route["next_task"] == "TRADING-2306_Breadth_Data_Source_Investment_Decision"
    assert route["caveat"] == "NO_RELIABLE_DATA"


def test_breadth_task_route_never_outputs_promotion_route() -> None:
    for route in [
        build_2303_task_route(
            strict_pit_feasibility=True,
            pit_approximation_feasibility=False,
            current_constituents_proxy_feasibility=False,
        ),
        build_2303_task_route(
            strict_pit_feasibility=False,
            pit_approximation_feasibility=False,
            current_constituents_proxy_feasibility=True,
        ),
    ]:
        assert "PROMOTION" not in route["next_task"].upper()
        assert route["promotion_allowed"] is False
        assert route["broker_action"] == "none"
