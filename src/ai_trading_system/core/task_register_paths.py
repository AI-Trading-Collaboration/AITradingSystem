"""Paths of the rendered task register views.

Tasks live in ``tasks/<ID>.yaml``; ``python tools/tasks.py render`` writes the active and completed
views below ``docs`` in the eight-column table that reports read (GOV-008 P5). Reports only need the
two view paths, so they live here, independent of how the task files are kept.
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
