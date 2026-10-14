"""Owner-approved S4 settings, kept separate from each question's original bench settings."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
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


class ScreenSettings(_Strict):
    schema_version: Literal["research_screen.v1"]
    derisk_events: EventSettings
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
