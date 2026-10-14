"""Owner-approved S4 settings, kept separate from each question's original bench settings."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SCREEN_SETTINGS_PATH = PROJECT_ROOT / "config" / "research" / "research_screen.yaml"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EventSettings(_Strict):
    derisk_ratio: float = Field(gt=0, lt=1)
    merge_gap_days: int = Field(ge=1)
    protective_loss: float = Field(lt=0)
    false_alarm_gain: float = Field(gt=0)
    short_event_days: int = Field(ge=1)


class ScreenRules(_Strict):
    screen_end: date
    qqq_series: Literal["proxy", "actual"]
    qqq_eval_start: date
    long_eval_start: date
    horizons: tuple[int, ...] = Field(min_length=1)
    groups: int = Field(gt=0)
    warmup_days: int = Field(gt=0)
    min_days: int = Field(gt=0)
    drawdown_threshold: float = Field(lt=0)
    bootstrap_markets: tuple[str, ...]
    bootstrap_draws: int = Field(gt=0)
    confidence: float = Field(gt=0, lt=1)
    seed: int = Field(gt=0)
    lookahead_cut_dates: int = Field(gt=0)
    bh_q: float = Field(gt=0, lt=1)
    primary_market: str
    primary_horizon: int = Field(gt=0)
    candidate_breadth: float = Field(gt=0, le=1)
    eliminate_breadth: float = Field(gt=0, le=1)
    copy_corr: float = Field(gt=0, le=1)

    @model_validator(mode="after")
    def valid_screen(self) -> Self:
        # The validation window starts on 2011-01-03; screening must never enter it.
        if self.screen_end > date(2010, 12, 31):
            raise ValueError("screen_end must be no later than 2010-12-31")
        if any(h <= 0 for h in self.horizons) or len(set(self.horizons)) != len(self.horizons):
            raise ValueError("horizons must be positive and unique")
        if self.primary_horizon not in self.horizons:
            raise ValueError("primary_horizon must be in horizons")
        if self.primary_market not in self.bootstrap_markets or "qqq" not in self.bootstrap_markets:
            raise ValueError("bootstrap_markets must include qqq and primary_market")
        if self.eliminate_breadth > self.candidate_breadth:
            raise ValueError("eliminate_breadth must not exceed candidate_breadth")
        return self


class ScreenSettings(_Strict):
    schema_version: Literal["research_screen.v1"]
    derisk_events: EventSettings
    screen: ScreenRules
    sources: tuple[SourceSpec, ...] = Field(min_length=1)
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @model_validator(mode="after")
    def unique_sources(self) -> Self:
        ids = [source.source_id for source in self.sources]
        if len(set(ids)) != len(ids):
            raise ValueError("source_id must be unique")
        return self


@dataclass(frozen=True)
class LoadedScreenSettings:
    settings: ScreenSettings
    root: Path
    sha256: str


def load_screen_settings(
    path: Path = DEFAULT_SCREEN_SETTINGS_PATH, root: Path = PROJECT_ROOT
) -> LoadedScreenSettings:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"screen settings root must be a mapping: {path}")
    settings = ScreenSettings.model_validate(raw)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return LoadedScreenSettings(settings=settings, root=root, sha256=digest)
