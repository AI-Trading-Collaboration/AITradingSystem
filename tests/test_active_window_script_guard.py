from __future__ import annotations

import ast
import json
import re
from pathlib import Path

from scripts import generate_qqq_plus_growth_closeout_artifacts as qqq_closeout
from scripts import generate_simple_baseline_real_run_decision_pack as simple_baseline_pack

PRIMARY_START = "2021-02-22"
PRIMARY_REGIME = "unified_primary_2021"
LEGACY_COMPARISON_START = "2022-12-01"
ACTIVE_DEFAULT_FIELDS = {
    "default_backtest_start",
    "default_decision_start",
    "default_evaluation_start",
    "default_start",
    "default_start_date",
    "minimum_requested_start_date",
}
LEGACY_OPEN_RANGE = re.compile(
    rf"{re.escape(LEGACY_COMPARISON_START)}\.\.(?:latest|open|present|unspecified)"
)
LEGACY_SINGLE_DAY_DEFAULT = f"{LEGACY_COMPARISON_START}:{LEGACY_COMPARISON_START}"
LEGACY_PRIMARY_CONCLUSION = f"conclusions use {LEGACY_COMPARISON_START} onward"


def _python_policy_paths() -> list[Path]:
    return [
        *sorted(Path("src/ai_trading_system").rglob("*.py")),
        *sorted(Path("scripts").rglob("*.py")),
    ]


def test_active_python_defaults_do_not_reintroduce_legacy_comparison_start() -> None:
    failures: list[str] = []
    for path in _python_policy_paths():
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for key, value in zip(node.keys, node.values, strict=True):
                if not isinstance(key, ast.Constant) or key.value not in ACTIVE_DEFAULT_FIELDS:
                    continue
                if isinstance(value, ast.Constant) and value.value == LEGACY_COMPARISON_START:
                    failures.append(f"{path}:{value.lineno}:{key.value}")
        for pattern, label in (
            (LEGACY_OPEN_RANGE, "open_range_fallback"),
            (re.compile(re.escape(LEGACY_SINGLE_DAY_DEFAULT)), "single_day_default"),
            (re.compile(re.escape(LEGACY_PRIMARY_CONCLUSION)), "primary_conclusion"),
        ):
            for match in pattern.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                failures.append(f"{path}:{line}:{label}")

    assert failures == []


def test_qqq_closeout_uses_primary_metadata_and_empty_ranking_fallback(tmp_path: Path) -> None:
    ranking_path = tmp_path / "qqq_outperformance_ranking_report.json"
    ranking_path.write_text(
        json.dumps({"ranking_rows": [], "data_quality": {"status": "PASS"}}),
        encoding="utf-8",
    )

    context = qqq_closeout._load_context(tmp_path)
    payload = qqq_closeout._base_payload({})

    assert context["requested_date_range"] == f"{PRIMARY_START}..latest"
    assert payload["market_regime"] == PRIMARY_REGIME
    assert payload["default_backtest_start"] == PRIMARY_START
    assert payload["anchor_date"] == PRIMARY_START

    ranking_path.write_text(
        json.dumps(
            {
                "ranking_rows": [
                    {"requested_date_range": f"{LEGACY_COMPARISON_START}..2026-06-29"}
                ],
                "data_quality": {"status": "PASS"},
            }
        ),
        encoding="utf-8",
    )
    historical_context = qqq_closeout._load_context(tmp_path)
    assert historical_context["requested_date_range"] == (f"{LEGACY_COMPARISON_START}..2026-06-29")


def test_simple_baseline_pack_uses_primary_metadata() -> None:
    payload = simple_baseline_pack._payload("test", "Test", "PASS", {})
    source = Path("scripts/generate_simple_baseline_real_run_decision_pack.py").read_text(
        encoding="utf-8"
    )

    assert payload["market_regime"] == PRIMARY_REGIME
    assert payload["summary"]["market_regime"] == PRIMARY_REGIME
    assert payload["default_backtest_start"] == PRIMARY_START
    assert payload["anchor_date"] == PRIMARY_START
    assert LEGACY_PRIMARY_CONCLUSION not in source


def test_frozen_statistical_validation_v1_is_not_a_runtime_active_policy() -> None:
    path = Path("docs/research/statistical_validation_policy.json")
    policy = json.loads(path.read_text(encoding="utf-8"))
    runtime_references = [
        str(candidate)
        for candidate in _python_policy_paths()
        if path.name in candidate.read_text(encoding="utf-8")
    ]

    assert policy["task_id"] == "TRADING-503"
    assert policy["status"] == "VALIDATION_POLICY_FROZEN"
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["default_backtest_start"] == LEGACY_COMPARISON_START
    assert runtime_references == []
