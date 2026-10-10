"""Task register (GOV-008 P5): tasks/*.yaml are the source; the two views are rendered from them."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from tools import tasks

ROOT = Path(__file__).resolve().parents[1]


def test_every_task_file_is_valid_and_both_views_are_current() -> None:
    loaded = tasks.load_tasks(ROOT)
    assert len({task.id for task in loaded}) == len(loaded) > 0
    assert tasks.stale_views(ROOT) == [], "run `python tools/tasks.py render`"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "tasks").mkdir()
    (tmp_path / "docs").mkdir()
    shutil.copytree(ROOT / tasks.TEMPLATE_DIR, tmp_path / tasks.TEMPLATE_DIR)
    return tmp_path


def _new(root: Path, task_id: str = "DEMO-001", **changes: str) -> int:
    values = {
        "area": "示例",
        "priority": "P1",
        "next_owner": "owner",
        "next_step": "do it",
        "acceptance": "done when done",
    }
    values.update(changes)
    argv = ["new", task_id]
    for name, value in values.items():
        argv += ["--" + name.replace("_", "-"), value]
    return tasks.main(argv, root=root)


def _rows(root: Path, partition: str) -> list[str]:
    view = (root / tasks.VIEWS[partition]).read_text(encoding="utf-8").split("\n")
    return [line for line in view[4:] if line.startswith("|")][: len(tasks.load_tasks(root))]


def test_new_set_and_completion_move_the_row_between_views(root: Path) -> None:
    assert _new(root) == 0
    assert _rows(root, "active") == ["|DEMO-001|示例|P1|PROPOSED|owner|do it|done when done||"]
    assert tasks.main(["set", "DEMO-001", "status=DONE", "notes=shipped in abc123"], root=root) == 0
    assert _rows(root, "completed")[0] == (
        "|DEMO-001|示例|P1|DONE|owner|do it|done when done|shipped in abc123|"
    )
    assert not any(row.startswith("|DEMO-001|") for row in _rows(root, "active"))
    assert tasks.stale_views(root) == []


def test_active_view_is_ordered_by_priority_then_id(root: Path) -> None:
    _new(root, "B-2", priority="P2")
    _new(root, "A-1", priority="P2")
    _new(root, "Z-9", priority="P0")
    assert [row.split("|")[1] for row in _rows(root, "active")] == ["Z-9", "A-1", "B-2"]


def test_pipes_are_kept_only_in_notes(root: Path) -> None:
    _new(root, notes="a|b|c")
    assert _rows(root, "active")[0].endswith("|done when done|a|b|c|")
    assert _new(root, "BAD-1", next_step="x|y") == 2


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(status="FINISHED"),
        lambda payload: payload.update(next_step="two\nlines"),
        lambda payload: payload.update(extra="field"),
        lambda payload: payload.update(id="OTHER-1"),
        lambda payload: payload.update(schema_version="task.v0"),
        lambda payload: payload.update(priority=1),
    ],
)
def test_invalid_task_files_are_rejected(root: Path, mutate) -> None:  # noqa: ANN001
    _new(root)
    path = root / "tasks" / "DEMO-001.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(payload)
    path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    with pytest.raises(tasks.TaskError):
        tasks.load_tasks(root)


def test_check_reports_a_hand_edited_view(root: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _new(root)
    view = root / tasks.VIEWS["active"]
    view.write_text(view.read_text(encoding="utf-8").replace("do it", "edited"), encoding="utf-8")
    assert tasks.main(["check"], root=root) == 1
    assert "stale views" in capsys.readouterr().out


def test_existing_task_cannot_be_created_twice(root: Path) -> None:
    assert _new(root) == 0
    assert _new(root) == 2
