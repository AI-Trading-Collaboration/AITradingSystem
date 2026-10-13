"""Preregistration: ``research/<question>/prereg.yaml``, written before any validation result
exists.

The file fixes what a question tests and how it will be judged: the data snapshot, the strategies
and their parameters, which of them face the validation window, the rules that decide each one, what
earlier research leads us to expect, the sample floor, the trial cap and when to stop. The bench
reads it for every run, refuses development trials beyond the cap, and opens the validation window
only when this exact file (and the strategy module) is on main together with the owner decision
that approved it. The file is zone C.
"""

from __future__ import annotations

import keyword
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ai_trading_system.research_bench.falsify import (
    PAIRED_SECTIONS,
    cost_section,
    delay_section,
    paired_fields,
)
from ai_trading_system.research_bench.metrics import (
    EXPOSURE_GAP_FIELDS,
    comparison_fields,
    summary_fields,
)
from ai_trading_system.research_bench.settings import BenchSettings
from ai_trading_system.research_bench.simulate import Rebalance, Schedule
from ai_trading_system.yaml_loader import safe_load_yaml_path

PREREG_FILE = "prereg.yaml"
QUESTION_ID_PATTERN = r"^[A-Z][A-Z0-9]*-[A-Z0-9]+$"
OWNER_DECISION_PATTERN = r"^owner_decision:[A-Z]+-[0-9]+:[0-9]{4}-[0-9]{2}-[0-9]{2}:[a-z0-9_]+$"
# Rule metrics are read from these parts of a run result, and from the falsification sections
# (``research_bench.falsify``); which cost and delay sections exist depends on the bench settings.
RULE_SECTIONS = ("strategy", "comparison", "exposure_gap", *PAIRED_SECTIONS)
_RULE_SECTION = re.compile(rf"^(?:{'|'.join(RULE_SECTIONS)}|cost_[0-9]+(?:p[0-9]+)?|delay_[0-9]+)$")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DataSpec(_Strict):
    snapshot_id: str = Field(min_length=1)
    qqq_series: Literal["proxy", "actual"]

    @field_validator("snapshot_id")
    @classmethod
    def _fixed_snapshot(cls, value: str) -> str:
        if value.upper() == "LATEST":
            raise ValueError("a preregistration names a fixed snapshot, not LATEST")
        return value


class RebalanceSpec(_Strict):
    schedule: Schedule = "daily"
    band: float = Field(default=0.0, ge=0.0, lt=1.0)

    def to_rebalance(self) -> Rebalance:
        return Rebalance(schedule=self.schedule, band=self.band)


class StrategyEntry(_Strict):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    function: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    rebalance: RebalanceSpec = Field(default_factory=RebalanceSpec)
    delay_days: int = Field(default=0, ge=0)
    leveraged_benchmarks: bool = False

    @field_validator("function")
    @classmethod
    def _identifier(cls, value: str) -> str:
        if not value.isidentifier() or keyword.iskeyword(value):
            raise ValueError("function must be a Python identifier in the strategy module")
        return value


class Rule(_Strict):
    """``<section>.<field> <op> <value>`` on the run result. For example
    ``comparison.max_drawdown >= 0.05``: the strategy's drawdown at least 5 points shallower than
    the matched benchmark's; ``ci_low.max_drawdown > 0``: still shallower at the low end of the
    bootstrap interval."""

    metric: str
    op: Literal[">=", "<=", ">", "<"]
    value: float
    note: str = ""

    @field_validator("metric")
    @classmethod
    def _section(cls, value: str) -> str:
        section, _, name = value.partition(".")
        if not _RULE_SECTION.match(section) or not name:
            raise ValueError(
                f"metric must be <section>.<field> with section in {RULE_SECTIONS},"
                " cost_<bps> or delay_<days>"
            )
        return value


class ValidationSpec(_Strict):
    strategies: tuple[str, ...] = Field(min_length=1)
    rules: dict[str, tuple[Rule, ...]]
    # Confidence of the bootstrap interval; the bench default when not given. Raise it to split
    # the error rate across sub-questions (Bonferroni).
    confidence: float | None = Field(default=None, gt=0, lt=1)


class PriorExpectation(_Strict):
    effect: str = Field(min_length=1)
    fails_when: str = Field(min_length=1)
    sources: tuple[str, ...] = Field(min_length=1)


class SampleFloor(_Strict):
    min_return_days: int = Field(gt=0)


class Prereg(_Strict):
    schema_version: Literal["research_prereg.v1"]
    question_id: str = Field(pattern=QUESTION_ID_PATTERN)
    question: str = Field(min_length=1)
    question_type: str = Field(min_length=1)
    owner_decision: str | None = Field(default=None, pattern=OWNER_DECISION_PATTERN)
    data: DataSpec
    strategy_module: str = "strategy.py"
    strategies: tuple[StrategyEntry, ...] = Field(min_length=1)
    validation: ValidationSpec
    sensitivity: tuple[str, ...] = ()
    prior_expectation: PriorExpectation
    sample_floor: SampleFloor
    trial_cap: int = Field(gt=0)
    stop_conditions: tuple[str, ...] = Field(min_length=1)
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("strategy_module")
    @classmethod
    def _module_in_question_dir(cls, value: str) -> str:
        if not value.endswith(".py") or "/" in value or "\\" in value or value.startswith("."):
            raise ValueError("strategy_module is a .py file name inside the question directory")
        return value

    @model_validator(mode="after")
    def _consistent(self) -> Prereg:
        ids = [entry.id for entry in self.strategies]
        if len(set(ids)) != len(ids):
            raise ValueError("strategy ids must be unique")
        unknown = sorted(set(self.validation.strategies) - set(ids))
        if unknown:
            raise ValueError(f"validation names unknown strategies: {unknown}")
        if set(self.validation.rules) != set(self.validation.strategies):
            raise ValueError("validation rules must be given for exactly the validated strategies")
        empty = sorted(key for key, rules in self.validation.rules.items() if not rules)
        if empty:
            raise ValueError(f"validated strategies without rules: {empty}")
        return self

    def strategy(self, strategy_id: str) -> StrategyEntry:
        for entry in self.strategies:
            if entry.id == strategy_id:
                return entry
        raise KeyError(f"{self.question_id} has no strategy {strategy_id}")


def load_prereg(path: Path) -> Prereg:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"preregistration root must be a mapping: {path}")
    return Prereg.model_validate(raw)


def rule_sections(settings: BenchSettings) -> dict[str, tuple[str, ...]]:
    """Every section a rule may read, with its fields, under these bench settings."""

    compared = comparison_fields(settings)
    sections: dict[str, tuple[str, ...]] = {
        "strategy": summary_fields(settings),
        "comparison": compared,
        "exposure_gap": EXPOSURE_GAP_FIELDS,
    }
    sections.update({section: paired_fields(settings) for section in PAIRED_SECTIONS})
    falsification = settings.falsification
    sections.update({cost_section(bps): compared for bps in falsification.cost_bps})
    sections.update({delay_section(days): compared for days in falsification.delay_days})
    return sections


def rule_problems(prereg: Prereg, settings: BenchSettings) -> list[str]:
    """Rules naming a metric no run will have: they could never hold, so a validation read with
    them would be wasted."""

    sections = rule_sections(settings)
    problems = []
    for strategy_id, rules in prereg.validation.rules.items():
        for rule in rules:
            section, _, name = rule.metric.partition(".")
            if name not in sections.get(section, ()):
                problems.append(f"{strategy_id}: no metric {rule.metric}")
    return problems


def evaluate_rules(rules: tuple[Rule, ...], result: dict[str, Any]) -> list[dict[str, Any]]:
    """Each rule with the observed value and whether it holds; a missing value fails."""

    out = []
    for rule in rules:
        section, _, name = rule.metric.partition(".")
        observed = result.get(section, {}).get(name)
        holds = False
        if isinstance(observed, int | float) and observed == observed:
            value = float(observed)
            holds = {
                ">=": value >= rule.value,
                "<=": value <= rule.value,
                ">": value > rule.value,
                "<": value < rule.value,
            }[rule.op]
        out.append(
            {
                "metric": rule.metric,
                "op": rule.op,
                "value": rule.value,
                "observed": observed,
                "holds": holds,
            }
        )
    return out
