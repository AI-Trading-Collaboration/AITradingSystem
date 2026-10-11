# ruff: noqa: E501
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "agent"))
import runs_board  # noqa: E402


def brief(run: Path, *, template: str = "mechanical-change", zone: str = "B", ship: str = "publish") -> None:
    run.mkdir(parents=True, exist_ok=True)
    run.joinpath("brief.md").write_text(
        f"""---
task: {run.name.rsplit('-', 1)[0]}
run: {run.name}
template: {template}
zone: {zone}
model: cloud
thinking: default
tools: default
owner_decision: none
allow_done: false
ship: {ship}
allowed_paths:
  - scripts/agent/**
read_paths: []
---
# {run.name}

## 目标

测试看板。
""",
        encoding="utf-8",
    )


def event(role: str = "assistant", stop: str = "stop", cost: float = 0.01) -> dict:
    return {
        "type": "message_end",
        "message": {
            "role": role,
            "provider": "openai",
            "model": "gpt-5.5",
            "timestamp": "2026-10-11T00:01:00Z",
            "stopReason": stop,
            "usage": {"input": 10, "output": 5, "cost": {"total": cost}},
            "content": "done",
        },
    }


def attempt(run: Path, nn: str = "01", *, purpose: str = "run", stop: str = "stop", settled: bool = True) -> None:
    d = run / "attempts" / nn
    d.mkdir(parents=True, exist_ok=True)
    events = [event(stop=stop)]
    if settled:
        events.append({"type": "agent_settled", "aborted": False})
    d.joinpath("events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    d.joinpath("exit.json").write_text(
        json.dumps({"attempt": nn, "purpose": purpose, "started_at": "2026-10-11T00:00:00Z", "ended_at": "2026-10-11T00:02:00Z", "exit_code": 0}),
        encoding="utf-8",
    )


def test_synthetic_stages_and_render(tmp_path: Path) -> None:
    awaiting = tmp_path / "DEVX-1-01"
    brief(awaiting)
    attempt(awaiting)

    accepted = tmp_path / "DEVX-2-01"
    brief(accepted, ship="none", template="readonly-check", zone="A")
    attempt(accepted)
    accepted.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:03:00Z", "verdict": "accepted"}]}), encoding="utf-8")

    failed = tmp_path / "DEVX-3-01"
    brief(failed)
    attempt(failed, stop="error")

    runs = {r["id"]: r for r in runs_board.collect(tmp_path)}
    assert runs["DEVX-1-01"]["stage"] == "awaiting"
    assert runs["DEVX-2-01"]["stage"] == "accepted"
    assert runs["DEVX-3-01"]["stage"] == "failed"
    html = runs_board.render(list(runs.values()), tmp_path)
    assert "按任务类型汇总" in html
    assert "云端花费" in html


def test_published_from_publish_json_and_post_ship_issues(tmp_path: Path) -> None:
    run = tmp_path / "DEVX-4-01"
    brief(run)
    attempt(run)
    run.joinpath("publish.json").write_text(json.dumps({"started_at": "2026-10-11T00:04:00Z", "ended_at": "2026-10-11T00:05:00Z", "ok": True}), encoding="utf-8")
    run.joinpath("acceptance.json").write_text(json.dumps({"post_ship_issues": [{"at": "2026-10-11T00:06:00Z", "summary": "x", "ref": "y"}]}), encoding="utf-8")
    parsed = runs_board.parse_run(run)
    assert parsed["stage"] == "shipped"
    assert parsed["summary"]["post_ship_issues"] == 1
    assert parsed["summary"]["phase_durations"]["publish_seconds"] == 60


def test_phase_durations_and_type_summary(tmp_path: Path) -> None:
    run = tmp_path / "DEVX-5-01"
    brief(run, template="records-change", zone="A")
    run.joinpath("queue.json").write_text(json.dumps({"queued_at": "2026-10-10T23:59:00Z"}), encoding="utf-8")
    attempt(run)
    run.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:04:00Z", "verdict": "rejected"}]}), encoding="utf-8")
    parsed = runs_board.parse_run(run)
    phases = parsed["summary"]["phase_durations"]
    assert phases["queued_seconds"] == 60
    assert phases["execution_seconds"] == 120
    assert phases["acceptance_wait_seconds"] == 120
    summary = runs_board.task_type_summaries([parsed])[0]
    assert summary["type"] == "records-change A"
    assert summary["runs"] == 1
