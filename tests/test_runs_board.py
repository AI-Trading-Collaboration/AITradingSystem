# ruff: noqa: E501, E702
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "agent"))
import runs_board  # noqa: E402


def brief(run: Path, *, template: str = "mechanical-change", zone: str = "B", ship: str = "publish", goal: str = "测试看板。") -> None:
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

{goal}
""",
        encoding="utf-8",
    )


def event(role: str = "assistant", stop: str = "stop", cost: float = 0.01) -> dict:
    return {"type": "message_end", "message": {"role": role, "provider": "openai", "model": "gpt-5.5", "timestamp": "2026-10-11T00:01:00Z", "stopReason": stop, "usage": {"input": 10, "output": 5, "cost": {"total": cost}}, "content": "done", "errorMessage": "400" if stop == "error" else None}}


def attempt(run: Path, nn: str = "01", *, purpose: str = "run", stop: str = "stop", settled: bool = True, exit_json: bool = True, extra_events: list[dict] | None = None) -> None:
    d = run / "attempts" / nn
    d.mkdir(parents=True, exist_ok=True)
    events = [*(extra_events or []), event(stop=stop)]
    if settled:
        events.append({"type": "agent_settled", "aborted": False})
    d.joinpath("events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    if exit_json:
        d.joinpath("exit.json").write_text(json.dumps({"attempt": nn, "purpose": purpose, "started_at": "2026-10-11T00:00:00Z", "ended_at": "2026-10-11T00:02:00Z", "exit_code": 0}), encoding="utf-8")


def test_synthetic_stages_and_render(tmp_path: Path) -> None:
    awaiting = tmp_path / "DEVX-1-01"; brief(awaiting); attempt(awaiting)
    accepted = tmp_path / "DEVX-2-01"; brief(accepted, ship="none", template="readonly-check", zone="A"); attempt(accepted)
    accepted.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:03:00Z", "verdict": "accepted"}]}), encoding="utf-8")
    failed = tmp_path / "DEVX-3-01"; brief(failed); attempt(failed, stop="error")
    runs = {r["id"]: r for r in runs_board.collect(tmp_path)}
    assert runs["DEVX-1-01"]["stage"] == "awaiting"
    assert runs["DEVX-2-01"]["stage"] == "accepted"
    assert runs["DEVX-3-01"]["stage"] == "failed"
    html = runs_board.render(list(runs.values()), tmp_path)
    assert "按任务类型汇总" in html and "云端花费" in html


def test_published_from_publish_json_and_post_ship_issues(tmp_path: Path) -> None:
    run = tmp_path / "DEVX-4-01"; brief(run); attempt(run)
    run.joinpath("publish.json").write_text(json.dumps({"started_at": "2026-10-11T00:04:00Z", "ended_at": "2026-10-11T00:05:00Z", "ok": True, "main_sha": "123456789abcdef"}), encoding="utf-8")
    run.joinpath("acceptance.json").write_text(json.dumps({"post_ship_issues": [{"at": "2026-10-11T00:06:00Z", "summary": "x", "ref": "y"}]}), encoding="utf-8")
    parsed = runs_board.parse_run(run)
    assert parsed["stage"] == "shipped" and "123456789" in parsed["stage_detail"]
    assert parsed["summary"]["post_ship_issues"] == 1
    assert parsed["summary"]["phase_durations"]["publish_seconds"] == 60


def test_phase_durations_and_type_summary(tmp_path: Path) -> None:
    run = tmp_path / "DEVX-5-01"; brief(run, template="records-change", zone="A")
    run.joinpath("queue.json").write_text(json.dumps({"queued_at": "2026-10-10T23:59:00Z"}), encoding="utf-8")
    attempt(run)
    run.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:04:00Z", "verdict": "rejected"}]}), encoding="utf-8")
    parsed = runs_board.parse_run(run)
    phases = parsed["summary"]["phase_durations"]
    assert phases["queued_seconds"] == 60 and phases["execution_seconds"] == 120 and phases["acceptance_wait_seconds"] == 120
    summary = runs_board.task_type_summaries([parsed])[0]
    assert summary["type"] == "records-change A" and summary["runs"] == 1


def test_remaining_stage_and_event_edge_cases(tmp_path: Path) -> None:
    running = tmp_path / "DEVX-6-01"; brief(running); attempt(running, settled=False, exit_json=False)
    crashed = tmp_path / "DEVX-7-01"; brief(crashed); attempt(crashed, settled=False, exit_json=True)
    error0 = tmp_path / "DEVX-8-01"; brief(error0); attempt(error0, stop="error")
    fixed = tmp_path / "DEVX-9-01"; brief(fixed); attempt(fixed, "01"); attempt(fixed, "02")
    fixed.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:03:00Z", "verdict": "rejected"}]}), encoding="utf-8")
    runs = {r["id"]: r for r in runs_board.collect(tmp_path)}
    assert runs["DEVX-6-01"]["stage"] == "running"
    assert runs["DEVX-7-01"]["stage"] == "failed"
    assert runs["DEVX-8-01"]["stage"] == "failed" and "400" in runs["DEVX-8-01"]["stage_detail"]
    assert runs["DEVX-9-01"]["stage"] == "awaiting"


def test_blocked_from_events_partial_json_html_escape_and_empty(tmp_path: Path) -> None:
    assert runs_board.collect(tmp_path) == []
    run = tmp_path / "DEVX-10-01"; brief(run, goal="<script>x</script>")
    extra = [{"type": "tool_execution_start", "toolCallId": "1", "toolName": "bash", "args": {"command": "git add -A"}}, {"type": "message_end", "message": {"role": "toolResult", "toolCallId": "1", "isError": True, "content": "blocked"}}]
    attempt(run, extra_events=extra)
    with (run / "attempts/01/events.jsonl").open("a", encoding="utf-8") as f:
        f.write('{"type":')
    parsed = runs_board.parse_run(run)
    assert parsed["summary"]["blocked"] == 1
    assert "&lt;script&gt;" in runs_board.render([parsed], tmp_path)


def test_bom_json_publish_failed_weekly_costs_and_first_pass_rate(tmp_path: Path) -> None:
    run = tmp_path / "DEVX-11-01"; brief(run); attempt(run)
    (run / "attempts/01/exit.json").write_bytes((json.dumps({"attempt": "01", "purpose": "run", "started_at": "2026-10-11T00:00:00Z", "ended_at": "2026-10-11T00:02:00Z", "exit_code": 0})).encode("utf-8-sig"))
    run.joinpath("queue.json").write_bytes(json.dumps({"queued_at": "2026-10-10T23:59:00Z"}).encode("utf-8-sig"))
    run.joinpath("publish.json").write_text(json.dumps({"ok": False, "summary": "no"}), encoding="utf-8")
    parsed = runs_board.parse_run(run)
    assert parsed["stage"] == "ship_failed"
    assert runs_board.weekly_cloud_costs([parsed]) == {"2026-W41": 0.01}
    accepted = tmp_path / "DEVX-12-01"; brief(accepted, template="mechanical-change"); attempt(accepted); accepted.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:03:00Z", "verdict": "accepted"}]}), encoding="utf-8")
    rejected = tmp_path / "DEVX-13-01"; brief(rejected, template="mechanical-change"); attempt(rejected); attempt(rejected, "02"); rejected.joinpath("acceptance.json").write_text(json.dumps({"reviews": [{"after_attempt": "01", "at": "2026-10-11T00:03:00Z", "verdict": "rejected"}, {"after_attempt": "02", "at": "2026-10-11T00:05:00Z", "verdict": "accepted"}]}), encoding="utf-8")
    summary = [s for s in runs_board.task_type_summaries([runs_board.parse_run(accepted), runs_board.parse_run(rejected)]) if s["type"] == "mechanical-change B"][0]
    assert summary["first_pass_rate"] == 0.5
