from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.core.task_register_paths import task_register_view_path


def test_paths_are_the_generated_views_under_docs(tmp_path: Path) -> None:
    assert (
        task_register_view_path(tmp_path, "active") == tmp_path.resolve() / "docs/task_register.md"
    )
    assert (
        task_register_view_path(tmp_path, "completed")
        == tmp_path.resolve() / "docs/task_register_completed.md"
    )


def test_unknown_partition_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown task register partition"):
        task_register_view_path(tmp_path, "archived")


def test_path_does_not_require_a_registry_in_the_project_root(tmp_path: Path) -> None:
    # The old helper validated the whole canonical registry first; a plain directory must now work.
    assert not (tmp_path / "registry").exists()
    assert task_register_view_path(tmp_path, "active").name == "task_register.md"
