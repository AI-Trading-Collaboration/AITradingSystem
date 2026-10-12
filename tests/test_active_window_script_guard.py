from __future__ import annotations

import ast
import re
from pathlib import Path

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


