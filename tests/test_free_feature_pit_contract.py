from __future__ import annotations

from pathlib import Path

import yaml

from ai_trading_system.free_data_connectors import (
    AlfredVintageConnector,
    OfficialMacroCalendarConnector,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    run_free_data_source_validation,
)


def test_alfred_connector_requires_api_key_for_vintage_read() -> None:
    connector = AlfredVintageConnector(api_key=None)

    try:
        connector.observations_url("CPIAUCSL", as_of_date="2026-06-01")
    except ValueError as exc:
        assert "API key" in str(exc)
    else:
        raise AssertionError("ALFRED vintage read should require an API key")


def test_calendar_events_have_known_at_or_warning() -> None:
    frame = OfficialMacroCalendarConnector().normalize_events(
        [
            {
                "event_date": "2026-06-10",
                "event_type": "cpi",
                "event_name": "CPI release",
            }
        ]
    )

    assert frame.iloc[0]["known_at"] == "2026-06-10"
    assert frame.iloc[0]["PIT_status"] == "PIT_WARNING"
    assert frame.iloc[0]["allowed_usage"] == "diagnostic_only"


def test_participation_proxy_not_marked_true_breadth() -> None:
    payload = yaml.safe_load(DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH.read_text(encoding="utf-8"))

    assert payload["safety_boundary"]["true_pit_breadth"] is False
    assert all(
        proxy["status"] in {"DIAGNOSTIC_ONLY", "REGISTRY_ONLY"} for proxy in payload["proxies"]
    )
    assert all("NOT_TRUE_PIT_BREADTH" in proxy["caveats"] for proxy in payload["proxies"])


def test_free_features_do_not_enable_promotion() -> None:
    payload = run_free_data_source_validation()

    assert payload["status"] == "PASS"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"


def test_pit_blocked_features_cannot_enter_model_ready_family(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "free_data_source_registry.v1",
                "sources": [
                    {
                        "source_id": "bad_macro",
                        "provider": "Bad Macro",
                        "free_or_paid": "free",
                        "official_source": True,
                        "api_required": False,
                        "api_key_required": False,
                        "earliest_available_date": "series_dependent",
                        "update_frequency": "monthly",
                        "timestamp_timezone": "America/New_York",
                        "PIT_status": "PIT_WARNING",
                        "revision_risk": "high",
                        "vintage_support": False,
                        "request_parameters": {"series": ["PAYEMS"]},
                        "allowed_usage": ["macro_model_ready"],
                        "blocked_usage": ["promotion"],
                        "caveats": ["missing vintage"],
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    payload = run_free_data_source_validation(registry_path=registry_path)

    assert payload["status"] == "FAIL"
    assert any(
        issue["code"] == "revision_sensitive_macro_requires_vintage"
        for issue in payload["issues"]
    )
