"""OPS-086-10: the pi lanes read the daily scheduler summaries from the production state root."""

import json
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "scripts" / "agent" / "agent_config.json"
PRODUCTION_STATE_ROOT = "D:\\Work\\AITradingSystem_data\\prod\\state"
RUNTIME_CLONE = "D:\\Work\\AITradingSystem_ops_runtime"


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def test_daily_scheduler_dir_is_the_scheduler_dir_of_the_production_state_root() -> None:
    expected = PRODUCTION_STATE_ROOT + "\\outputs\\run_control\\scheduler"
    assert _config()["daily_scheduler_dir"] == expected


def test_daily_scheduler_dir_is_not_under_the_runtime_clone() -> None:
    assert RUNTIME_CLONE.lower() not in _config()["daily_scheduler_dir"].lower()
