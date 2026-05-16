from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from ai_trading_system.core.artifacts import ArtifactRef
from ai_trading_system.core.production_effect import ProductionEffect

StepStatus = Literal["PASS", "WARN", "FAIL", "SKIPPED", "BLOCKED"]


@dataclass(frozen=True)
class WorkflowStep:
    step_id: str
    name: str
    command_name: str
    command: tuple[str, ...] = ()
    production_effect: ProductionEffect = ProductionEffect.NONE
    required_inputs: tuple[ArtifactRef, ...] = ()
    expected_outputs: tuple[ArtifactRef, ...] = ()
    blocking: bool = True
    can_run_on_closed_market: bool = False


@dataclass(frozen=True)
class WorkflowStepResult:
    step_id: str
    status: StepStatus
    started_at: datetime | None
    finished_at: datetime | None
    artifacts: tuple[ArtifactRef, ...] = ()
    key_conclusions: tuple[str, ...] = ()
    risks: tuple[str, ...] = ()
    production_effect: ProductionEffect = ProductionEffect.NONE
