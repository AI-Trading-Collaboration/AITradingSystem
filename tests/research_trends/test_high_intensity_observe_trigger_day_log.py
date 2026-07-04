from __future__ import annotations

import json
from pathlib import Path

from high_intensity_event_logger_fixtures import (
    sample_selected_rule,
    sample_trigger_source_rows,
)

from ai_trading_system.high_intensity_risk_cap_forward_observe_event_logger import (
    build_high_intensity_observe_trigger_day_log,
)


def test_observe_trigger_day_log_is_deterministic_and_safe(tmp_path: Path) -> None:
    source_path = tmp_path / "trigger_alignment.json"
    source_path.write_text(
        json.dumps({"rows": sample_trigger_source_rows()}),
        encoding="utf-8",
    )
    dynamic_dry_run = {
        "summary": {
            "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
            "pit_policy": "PIT_APPROXIMATION_READY",
        },
        "pit_boundary": {"known_at_policy": "NEXT_SESSION_DECISION_POLICY"},
        "paths": {"trigger_alignment": str(source_path)},
    }

    rows = build_high_intensity_observe_trigger_day_log(
        trigger_source_rows=sample_trigger_source_rows(),
        selected_rule=sample_selected_rule(),
        dynamic_dry_run=dynamic_dry_run,
    )
    rows_again = build_high_intensity_observe_trigger_day_log(
        trigger_source_rows=sample_trigger_source_rows(),
        selected_rule=sample_selected_rule(),
        dynamic_dry_run=dynamic_dry_run,
    )

    assert len(rows) == 1
    assert rows[0]["trigger_day_id"] == rows_again[0]["trigger_day_id"]
    assert rows[0]["trigger_day_status"] == "TRIGGER_DAY_ACTIVE"
    assert rows[0]["event_status"] == "OBSERVE_PENDING"
    assert rows[0]["manual_review_observation_flag"] is True
    assert rows[0]["as_of_timestamp"] == "2023-01-03T00:00:00+00:00"
    assert rows[0]["promotion_allowed"] is False
    assert rows[0]["paper_shadow_allowed"] is False
    assert rows[0]["production_allowed"] is False
    assert rows[0]["broker_action"] == "none"
    assert rows[0]["target_weight_generated"] is False
    assert rows[0]["rebalance_instruction_generated"] is False
