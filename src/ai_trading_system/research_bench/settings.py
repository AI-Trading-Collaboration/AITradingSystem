"""Bench settings, the unified cost and the research windows a question may read."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.paths import join_root
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "config" / "research" / "research_bench.yaml"
OPEN_END = "latest"

WindowRole = Literal["development", "validation", "consistency_check"]
WINDOW_ROLES: tuple[WindowRole, ...] = ("development", "validation", "consistency_check")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EpisodeSpec(_Strict):
    id: str = Field(pattern=r"^[a-z0-9_]+$")
    start: str = Field(pattern=r"^\d{4}-\d{2}$")
    end: str = Field(pattern=r"^\d{4}-\d{2}$")


class BenchSettings(_Strict):
    schema_version: Literal["research_bench.v1"]
    cost_model: str = Field(min_length=1)
    window_policy: str = Field(min_length=1)
    window_registry: str = Field(min_length=1)
    research_root: str = Field(min_length=1)
    approval_ref: str = Field(min_length=1)
    trading_days_per_year: int = Field(gt=0)
    cvar_levels: tuple[float, ...] = Field(min_length=1)
    rolling_years: int = Field(gt=0)
    rolling_percentile: float = Field(gt=0, lt=1)
    lookahead_cut_dates: int = Field(gt=0)
    episodes: tuple[EpisodeSpec, ...]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


@dataclass(frozen=True)
class LoadedSettings:
    settings: BenchSettings
    root: Path
    sha256: str

    @property
    def cost_bps(self) -> float:
        """One-way turnover cost: the sum of the cost model's components."""

        raw = safe_load_yaml_path(join_root(self.root, self.settings.cost_model))
        components = raw.get("cost_components_bps") if isinstance(raw, dict) else None
        if not isinstance(components, dict) or not components:
            raise ValueError(f"{self.settings.cost_model} has no cost_components_bps")
        return float(sum(float(value) for value in components.values()))


@dataclass(frozen=True)
class Window:
    window_id: str
    role: WindowRole
    start: date
    end: date | None


def load_settings(path: Path = DEFAULT_SETTINGS_PATH, root: Path = PROJECT_ROOT) -> LoadedSettings:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"bench settings root must be a mapping: {path}")
    settings = BenchSettings.model_validate(raw)
    for episode in settings.episodes:
        if episode.end < episode.start:
            raise ValueError(f"episode {episode.id} ends before it starts")
    if any(not 0 < level < 1 for level in settings.cvar_levels):
        raise ValueError("cvar levels must lie strictly between 0 and 1")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return LoadedSettings(settings=settings, root=root, sha256=digest)


def resolve_window(loaded: LoadedSettings, question_type: str, role: WindowRole) -> Window:
    """The window a question type uses in a role, from the window policy and registry."""

    policy = _mapping(safe_load_yaml_path(join_root(loaded.root, loaded.settings.window_policy)))
    questions = _mapping(policy.get("question_windows"))
    roles = _mapping(questions.get(question_type))
    if not roles:
        raise KeyError(f"window policy has no question type {question_type}")
    window_id = roles.get(role)
    if not isinstance(window_id, str):
        raise KeyError(f"question type {question_type} has no {role} window")
    registry = _mapping(
        safe_load_yaml_path(join_root(loaded.root, loaded.settings.window_registry))
    )
    entry = _mapping(_mapping(registry.get("windows")).get(window_id))
    if not entry:
        raise KeyError(f"window registry has no window {window_id}")
    end = entry.get("end")
    return Window(
        window_id=window_id,
        role=role,
        start=_as_date(entry.get("start"), f"{window_id}.start"),
        end=None if end == OPEN_END else _as_date(end, f"{window_id}.end"),
    )


def _as_date(value: object, label: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"{label} is not a date: {value!r}")


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}
