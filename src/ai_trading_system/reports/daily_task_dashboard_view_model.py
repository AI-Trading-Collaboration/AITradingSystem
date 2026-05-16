from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.core import ProductionEffect

TraceRecord = dict[str, Any]


@dataclass(frozen=True)
class DailyTaskDetail:
    step_id: str
    title: str
    status: str
    conclusion: str
    important_risk: str
    risk_level: str
    duration_seconds: float | None
    return_code: int | None
    stdout_line_count: int
    stderr_line_count: int
    command: str
    input_visibility: str
    blocks_downstream: bool
    detail_reports: tuple[TraceRecord, ...]


@dataclass(frozen=True)
class DailyTaskDashboardReport:
    as_of: date
    generated_at: datetime
    run_id: str
    status: str
    metadata_path: Path
    run_report_path: Path | None
    reports_dir: Path
    project_root: Path
    started_at: str
    finished_at: str
    visibility_cutoff: str
    input_visibility_status: str
    git_commit: str
    git_dirty: bool | None
    tasks: tuple[DailyTaskDetail, ...]
    production_effect: str = ProductionEffect.NONE.value

    @property
    def failed_count(self) -> int:
        return sum(1 for task in self.tasks if task.status == "FAIL")

    @property
    def skipped_count(self) -> int:
        return sum(1 for task in self.tasks if task.status == "SKIPPED")

    @property
    def risk_count(self) -> int:
        return sum(1 for task in self.tasks if task.risk_level != "none")


@dataclass(frozen=True)
class DailyTaskKeyConclusion:
    area: str
    title: str
    status: str
    primary: str
    supporting: tuple[str, ...]
    important_risk: str
    risk_level: str
    source_steps: tuple[str, ...]
    result_comparison: tuple[TraceRecord, ...] = ()
    result_methodology: str = ""
    parameter_comparison: tuple[TraceRecord, ...] = ()
