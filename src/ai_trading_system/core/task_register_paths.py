"""Paths of the generated task register views.

Reports that read the active and completed task register used to ask the publication machinery
(``platform.architecture.task_registry_canonical``) for these paths, which also validated the whole
canonical registry. Only the two paths were needed, so they live here, independent of that machinery
(GOV-008 decoupling). When the task register is replaced (GOV-008 P5) this is the one place to change.
"""

from __future__ import annotations

from pathlib import Path

VIEW_FILE_NAMES = {
    "active": "task_register.md",
    "completed": "task_register_completed.md",
}


def task_register_view_path(project_root: Path, partition: str) -> Path:
    """Path of the active or completed task register view under ``project_root/docs``."""
    try:
        name = VIEW_FILE_NAMES[partition]
    except KeyError:
        raise ValueError(f"unknown task register partition: {partition!r}") from None
    return project_root.resolve() / "docs" / name
