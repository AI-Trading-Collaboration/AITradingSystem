"""Task register: one YAML file per task under ``tasks/`` (GOV-008 P5).

The task files are the source of truth. ``docs/task_register.md`` (active) and
``docs/task_register_completed.md`` (DONE/DROPPED) are views rendered from them, in the eight-column
table every report already reads. Edit a task file (or use ``set``/``new``), then ``render``; the PR
suite fails when a view is stale. Git history is the audit trail: there is no event chain.

usage:
  python tools/tasks.py list [--status IN_PROGRESS] [--priority P0]
  python tools/tasks.py show <ID>
  python tools/tasks.py new <ID> --area ... --priority P1 --next-owner ... --next-step ...
      --acceptance ... [--status PROPOSED] [--notes ...]
  python tools/tasks.py set <ID> status=DONE next_step="..." [notes="..."]
  python tools/tasks.py render        # rewrite both views
  python tools/tasks.py check         # exit 1 when a task file is invalid or a view is stale
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
TASK_DIR = "tasks"
TEMPLATE_DIR = "tasks/view_templates"
VIEWS = {"active": "docs/task_register.md", "completed": "docs/task_register_completed.md"}
SCHEMA = "task.v1"
FIELDS = ("id", "area", "priority", "status", "next_owner", "next_step", "acceptance", "notes")
STATUSES = frozenset(
    {
        "PROPOSED",
        "READY",
        "IN_PROGRESS",
        "BLOCKED_OWNER_INPUT",
        "BLOCKED_EXTERNAL",
        "BASELINE_DONE",
        "VALIDATING",
        "DEFERRED",
        "DONE",
        "DROPPED",
    }
)
COMPLETED_STATUSES = frozenset({"DONE", "DROPPED"})
PRIORITIES = ("P0", "P1", "P2", "P3")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
BANNER = (
    "<!-- Rendered by `python tools/tasks.py render` from tasks/*.yaml (GOV-008 P5). "
    "Edit the task files, not this view. -->\n\n"
)
HEADER = (
    "|ID|领域 / 任务|优先级|状态|下一责任方|阻塞项 / 下一步|验收标准|备注|\n"
    "|---|---|---|---|---|---|---|---|\n"
)


class TaskError(ValueError):
    pass


@dataclass(frozen=True)
class Task:
    id: str
    area: str
    priority: str
    status: str
    next_owner: str
    next_step: str
    acceptance: str
    notes: str

    @property
    def partition(self) -> str:
        return "completed" if self.status in COMPLETED_STATUSES else "active"

    def row(self) -> str:
        return "|" + "|".join(getattr(self, name) for name in FIELDS) + "|"


class _Dumper(yaml.SafeDumper):
    pass


_Dumper.add_representer(
    str, lambda dumper, value: dumper.represent_scalar("tag:yaml.org,2002:str", value)
)


def _validate(payload: object, source: str) -> Task:
    if not isinstance(payload, dict) or set(payload) != {"schema_version", *FIELDS}:
        raise TaskError(f"{source}: exactly schema_version and {', '.join(FIELDS)} required")
    if payload["schema_version"] != SCHEMA:
        raise TaskError(f"{source}: schema_version must be {SCHEMA}")
    values = {name: payload[name] for name in FIELDS}
    for name, value in values.items():
        if not isinstance(value, str) or "\n" in value or "\r" in value:
            raise TaskError(f"{source}: {name} must be a single-line string")
        if name != "notes" and "|" in value:
            raise TaskError(f"{source}: '|' is only allowed in notes ({name})")
    if _ID.fullmatch(values["id"]) is None:
        raise TaskError(f"{source}: invalid id {values['id']!r}")
    if values["status"] not in STATUSES:
        raise TaskError(f"{source}: unknown status {values['status']!r}")
    return Task(**values)


def task_path(root: Path, task_id: str) -> Path:
    return root / TASK_DIR / f"{task_id}.yaml"


def load_tasks(root: Path = ROOT) -> list[Task]:
    tasks = []
    for path in sorted((root / TASK_DIR).glob("*.yaml")):
        task = _validate(yaml.safe_load(path.read_text(encoding="utf-8")), path.name)
        if path.stem != task.id:
            raise TaskError(f"{path.name}: file name must equal the task id {task.id!r}")
        tasks.append(task)
    return tasks


def write_task(root: Path, task: Task) -> None:
    _validate({"schema_version": SCHEMA, **task.__dict__}, task.id)
    payload = {"schema_version": SCHEMA, **{name: getattr(task, name) for name in FIELDS}}
    text = yaml.dump(payload, Dumper=_Dumper, allow_unicode=True, sort_keys=False, width=100000)
    task_path(root, task.id).write_text(text, encoding="utf-8", newline="\n")


def _sort_key(task: Task) -> tuple[int, str]:
    rank = PRIORITIES.index(task.priority) if task.priority in PRIORITIES else len(PRIORITIES)
    return rank, task.id


def render_view(root: Path, tasks: list[Task], partition: str) -> str:
    selected = [task for task in tasks if task.partition == partition]
    selected.sort(key=_sort_key if partition == "active" else (lambda task: ("", task.id)))
    template = (root / TEMPLATE_DIR / f"{partition}.md").read_text(encoding="utf-8")
    rows = "".join(task.row() + "\n" for task in selected)
    return BANNER + HEADER + rows + "\n" + template


def render(root: Path = ROOT) -> list[str]:
    tasks = load_tasks(root)
    written = []
    for partition, rel in VIEWS.items():
        (root / rel).write_text(render_view(root, tasks, partition), encoding="utf-8", newline="\n")
        written.append(rel)
    return written


def stale_views(root: Path = ROOT) -> list[str]:
    tasks = load_tasks(root)
    return [
        rel
        for partition, rel in VIEWS.items()
        if (root / rel).read_text(encoding="utf-8") != render_view(root, tasks, partition)
    ]


def _find(root: Path, task_id: str) -> Task:
    path = task_path(root, task_id)
    if not path.is_file():
        raise TaskError(f"no task {task_id!r}")
    return _validate(yaml.safe_load(path.read_text(encoding="utf-8")), path.name)


def main(argv: list[str] | None = None, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description="任务登记：tasks/*.yaml 为准，视图由 render 生成")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list")
    listing.add_argument("--status")
    listing.add_argument("--priority")
    show = commands.add_parser("show")
    show.add_argument("task_id")
    new = commands.add_parser("new")
    new.add_argument("task_id")
    for name in ("area", "priority", "next_owner", "next_step", "acceptance"):
        new.add_argument("--" + name.replace("_", "-"), required=True, dest=name)
    new.add_argument("--status", default="PROPOSED")
    new.add_argument("--notes", default="")
    update = commands.add_parser("set")
    update.add_argument("task_id")
    update.add_argument("assignments", nargs="+", help="field=value")
    commands.add_parser("render")
    commands.add_parser("check")
    args = parser.parse_args(argv)
    try:
        if args.command == "list":
            for task in load_tasks(root):
                if args.status and task.status != args.status:
                    continue
                if args.priority and task.priority != args.priority:
                    continue
                print(f"{task.id}\t{task.priority}\t{task.status}\t{task.area}")
        elif args.command == "show":
            print(task_path(root, args.task_id).read_text(encoding="utf-8"), end="")
        elif args.command == "new":
            if task_path(root, args.task_id).exists():
                raise TaskError(f"task {args.task_id!r} already exists")
            write_task(
                root,
                Task(
                    args.task_id,
                    args.area,
                    args.priority,
                    args.status,
                    args.next_owner,
                    args.next_step,
                    args.acceptance,
                    args.notes,
                ),
            )
            render(root)
        elif args.command == "set":
            values = dict(_find(root, args.task_id).__dict__)
            for assignment in args.assignments:
                name, separator, value = assignment.partition("=")
                if not separator or name not in FIELDS or name == "id":
                    raise TaskError(f"use field=value with one of {', '.join(FIELDS[1:])}")
                values[name] = value
            write_task(root, Task(**values))
            render(root)
        elif args.command == "render":
            print("\n".join(render(root)))
        else:
            stale = stale_views(root)
            if stale:
                print("stale views (run `python tools/tasks.py render`): " + ", ".join(stale))
                return 1
            print(f"{len(load_tasks(root))} tasks valid; views current")
    except (TaskError, OSError, yaml.YAMLError) as exc:
        print(f"TASKS ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
