# ruff: noqa: E501, UP017
"""pi 运行看板：把运行目录汇总成一页静态 HTML。

用法：
    python runs_board.py <运行根目录> [-o runs.html] [--json summary.json] [--fragment]

只读：只读取运行目录里的文件，只写 -o / --json 指定的输出。只用标准库。

每个运行目录（<运行根目录>/<任务ID>-<序号>/）的约定见同目录 README.md：
    brief.md                    任务单（front matter）
    attempts/<NN>/events.jsonl  pi -p --mode json 的 stdout
    attempts/<NN>/exit.json     启动脚本写：started_at、ended_at、exit_code、purpose（run / fix / ship）
    session/*.jsonl             pi 会话（--session-dir）
    session.html                pi --export 的导出
    report.json                 守卫扩展写（可选）
    acceptance.json             Claude 验收写（可选）
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import re
import sys
from pathlib import Path

BOARD_SCHEMA = "aits.pi.runs-board/0"

STAGES = {
    # key: (中文标签, 图标, 颜色 token, 排序：越小越需要关注)
    "failed": ("失败", "✕", "critical", 0),
    "ship_failed": ("发布失败", "✕", "critical", 0),
    "awaiting": ("待验收", "◇", "warning", 1),
    "rejected": ("退回修正", "↺", "serious", 2),
    "running": ("运行中", "▸", "accent", 3),
    "accepted_pending_ship": ("已通过，待发布", "✓", "good", 4),
    "queued": ("未开始", "·", "muted", 5),
    "accepted": ("已通过", "✓", "good", 6),
    "shipped": ("已发布", "⬆", "good", 7),
}

PURPOSE_LABELS = {"run": "首次执行", "fix": "修正", "ship": "发布", "": "执行"}

_SKIP_PREFIXES = ('{"type":"message_update"', '{"type":"tool_execution_update"')


# ---------------------------------------------------------------- reading


def read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def iter_jsonl(path: Path):
    """按 LF 切行；跳过流式增量事件和写了一半的最后一行。"""
    try:
        with path.open("rb") as f:
            for raw in f:
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if not line or line.startswith(_SKIP_PREFIXES):
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict):
                    yield obj
    except OSError:
        return


def parse_brief(path: Path) -> dict:
    out = {"front": {}, "goal": ""}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return out
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n?(.*)$", text, re.S)
    body = text
    if m:
        body = m.group(2)
        for line in m.group(1).splitlines():
            line = line.split("#", 1)[0].strip()
            if ":" in line:
                k, v = line.split(":", 1)
                out["front"][k.strip()] = v.strip()
    g = re.search(r"^##\s*目标\s*\n(.*?)(?=^##\s|\Z)", body, re.S | re.M)
    if g:
        out["goal"] = " ".join(x.strip() for x in g.group(1).strip().splitlines() if x.strip())
    return out


def parse_time(value):
    """ISO 字符串或毫秒时间戳 -> aware datetime。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc)
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _text_of(content) -> str:
    if isinstance(content, str):
        return content
    parts = []
    for block in content or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text", ""))
    return "\n".join(parts)


def _args_summary(name: str, args) -> str:
    if not isinstance(args, dict):
        return ""
    for key in ("command", "path", "pattern"):
        if key in args:
            s = str(args[key])
            if key == "pattern" and "path" in args:
                s += f"  ({args['path']})"
            if name == "edit" and isinstance(args.get("edits"), list):
                s += f"  ({len(args['edits'])} 处)"
            return s
    return json.dumps(args, ensure_ascii=False)[:200]


def parse_attempt(path: Path) -> dict:
    exit_info = read_json(path / "exit.json") or {}
    att = {
        "id": path.name,
        "purpose": exit_info.get("purpose", ""),
        "exit_code": exit_info.get("exit_code"),
        "started_at": exit_info.get("started_at"),
        "ended_at": exit_info.get("ended_at"),
        "session_id": None,
        "settled": False,
        "aborted": False,
        "turns": 0,
        "models": [],
        "tokens": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
        "cost": 0.0,
        "tools": [],
        "last_stop_reason": None,
        "error_message": None,
        "final_text": "",
        "has_events": False,
        "last_event_at": None,
    }
    pending = {}
    first_ts = last_ts = None
    for ev in iter_jsonl(path / "events.jsonl"):
        att["has_events"] = True
        t = ev.get("type")
        if t == "session":
            att["session_id"] = ev.get("id")
            first_ts = first_ts or parse_time(ev.get("timestamp"))
        elif t == "turn_start":
            att["turns"] += 1
        elif t == "agent_settled":
            att["settled"] = True
            att["aborted"] = bool(ev.get("aborted"))
        elif t == "tool_execution_start":
            pending[ev.get("toolCallId")] = {
                "name": ev.get("toolName"),
                "args": _args_summary(ev.get("toolName", ""), ev.get("args")),
            }
        elif t == "message_end":
            msg = ev.get("message") or {}
            ts = parse_time(msg.get("timestamp"))
            if ts:
                first_ts = first_ts or ts
                last_ts = ts
            role = msg.get("role")
            if role == "assistant":
                model = f"{msg.get('provider', '?')}/{msg.get('model', '?')}"
                if model not in att["models"]:
                    att["models"].append(model)
                usage = msg.get("usage") or {}
                for k in att["tokens"]:
                    att["tokens"][k] += int(usage.get(k) or 0)
                att["cost"] += float((usage.get("cost") or {}).get("total") or 0)
                att["last_stop_reason"] = msg.get("stopReason")
                if msg.get("errorMessage"):
                    att["error_message"] = msg["errorMessage"]
                text = _text_of(msg.get("content"))
                if text.strip():
                    att["final_text"] = text.strip()
            elif role == "toolResult":
                call = pending.pop(msg.get("toolCallId"), {})
                duration = msg.get("durationMs")
                is_error = bool(msg.get("isError"))
                # durationMs 缺失 = 工具没有真正执行（被扩展拦截，或参数校验失败）
                status = "blocked" if is_error and duration is None else ("error" if is_error else "ok")
                end_ms = msg.get("timestamp")
                att["tools"].append({
                    "id": msg.get("toolCallId"),
                    "name": call.get("name") or msg.get("toolName"),
                    "args": call.get("args", ""),
                    "duration_ms": duration,
                    "start_ms": (end_ms - duration) if (end_ms is not None and duration is not None) else end_ms,
                    "status": status,
                    "output_tail": _text_of(msg.get("content"))[-600:],
                })
    for call_id, call in pending.items():  # 还在执行的工具
        att["tools"].append({"id": call_id, "name": call["name"], "args": call["args"], "duration_ms": None,
                             "start_ms": None, "status": "running", "output_tail": ""})
    if not att["started_at"] and first_ts:
        att["started_at"] = first_ts.isoformat()
    if last_ts:
        att["last_event_at"] = last_ts.isoformat()
    if not att["ended_at"] and att["settled"] and last_ts:
        att["ended_at"] = last_ts.isoformat()
    att["status"] = _attempt_status(att, exit_info)
    return att


def _attempt_status(att: dict, exit_info: dict) -> str:
    if not att["settled"]:
        return "crashed" if exit_info else "running"
    if att["aborted"] or att["last_stop_reason"] == "aborted":
        return "aborted"
    if att["last_stop_reason"] == "error":
        return "error"
    if exit_info.get("exit_code") not in (None, 0):
        return "crashed"
    return "done"


def compute_stage(run: dict) -> tuple[str, str]:
    """返回 (阶段, 说明)。"""
    attempts = run["attempts"]
    if not attempts:
        return "queued", "还没有执行记录"
    last = attempts[-1]
    status = last["status"]
    if status == "running":
        return "running", f"第 {last['id']} 次执行进行中"
    if status != "done":
        reason = {"crashed": "进程异常退出", "aborted": "被中止", "error": "模型调用报错"}[status]
        detail = f"{reason}：{last['error_message']}" if last["error_message"] else reason
        return ("ship_failed" if last["purpose"] == "ship" else "failed"), detail
    report = run["report"] or {}
    ship = report.get("ship") or {}
    publish = run.get("publish") or {}
    if publish.get("ok") is True:
        return "shipped", publish.get("summary") or "已发布"
    if last["purpose"] == "ship":
        if ship.get("mode") == "execute" and ship.get("ok"):
            return "shipped", ship.get("summary") or f"已发布 {ship.get('sha') or ''}".strip()
        if ship.get("mode") == "execute" or publish:
            return "ship_failed", ship.get("summary") or publish.get("summary") or "ship 未成功"
        return "accepted_pending_ship", "发布执行已结束，但 report.json 里没有 ship 结果"
    reviews = [r for r in (run["acceptance"] or {}).get("reviews", []) if r.get("after_attempt") == last["id"]]
    if not reviews:
        return "awaiting", "等待 Claude 验收"
    review = reviews[-1]
    if review.get("verdict") == "rejected":
        return "rejected", review.get("summary") or "验收退回"
    if ship.get("mode") == "execute" and ship.get("ok"):
        return "shipped", ship.get("summary") or "已发布"
    if run["brief"]["front"].get("ship", "") in ("none", ""):
        return "accepted", review.get("summary") or "验收通过"
    return "accepted_pending_ship", review.get("summary") or "验收通过，等待发布"


def _seconds_between(start, end) -> float:
    s, e = parse_time(start), parse_time(end)
    return max((e - s).total_seconds(), 0.0) if s and e else 0.0


def compute_phase_durations(path: Path, attempts: list[dict], acceptance, publish) -> dict:
    queue = read_json(path / "queue.json") or {}
    queued_seconds = 0.0
    if attempts:
        queued_seconds = _seconds_between(queue.get("queued_at"), attempts[0].get("started_at"))
    execution_seconds = sum(_seconds_between(a.get("started_at"), a.get("ended_at") or a.get("last_event_at")) for a in attempts)
    reviews = (acceptance or {}).get("reviews") or []
    review_by_attempt = {}
    for review in reviews:
        review_by_attempt.setdefault(str(review.get("after_attempt")), []).append(review)
    acceptance_wait_seconds = 0.0
    for att in attempts:
        matching = sorted(review_by_attempt.get(str(att.get("id")), []), key=lambda r: str(r.get("at") or ""))
        if matching:
            acceptance_wait_seconds += _seconds_between(att.get("ended_at") or att.get("last_event_at"), matching[0].get("at"))
    publish_seconds = 0.0
    if publish:
        publish_seconds = _seconds_between(publish.get("started_at") or publish.get("start"), publish.get("ended_at") or publish.get("end"))
    return {
        "queued_seconds": queued_seconds,
        "execution_seconds": execution_seconds,
        "acceptance_wait_seconds": acceptance_wait_seconds,
        "publish_seconds": publish_seconds,
    }


def parse_run(path: Path) -> dict:
    brief = parse_brief(path / "brief.md")
    attempts_dir = path / "attempts"
    attempts = sorted(
        (parse_attempt(p) for p in attempts_dir.iterdir() if p.is_dir()) if attempts_dir.is_dir() else [],
        key=lambda a: a["id"],
    )
    run = {
        "id": path.name,
        "task": brief["front"].get("task") or path.name.rsplit("-", 1)[0],
        "brief": brief,
        "attempts": attempts,
        "report": read_json(path / "report.json"),
        "acceptance": read_json(path / "acceptance.json"),
        "queue": read_json(path / "queue.json"),
        "publish": read_json(path / "publish.json"),
        "links": {name: f"{path.name}/{name}" for name in ("session.html", "brief.md", "report.md") if (path / name).exists()},
    }
    run["stage"], run["stage_detail"] = compute_stage(run)
    run["type"] = f"{brief['front'].get('template', 'unknown')} {brief['front'].get('zone', '—')}".strip()
    tools = [t for a in attempts for t in a["tools"]]
    report = run["report"] or {}
    starts = [parse_time(a["started_at"]) for a in attempts if a["started_at"]]
    ends = [parse_time(a["ended_at"]) for a in attempts if a["ended_at"]]
    phase_durations = compute_phase_durations(path, attempts, run.get("acceptance"), run.get("publish"))
    post_ship_issues = (run.get("acceptance") or {}).get("post_ship_issues") or []
    run["summary"] = {
        "started_at": min(starts).isoformat() if starts else None,
        # 进行中的那次按最后一条事件计时
        "active_seconds": sum(
            (parse_time(a["ended_at"] or a["last_event_at"]) - parse_time(a["started_at"])).total_seconds()
            for a in attempts if a["started_at"] and (a["ended_at"] or a["last_event_at"])
        ),
        "last_activity": max(ends).isoformat() if ends else None,
        "models": list(dict.fromkeys(m for a in attempts for m in a["models"])),
        "turns": sum(a["turns"] for a in attempts),
        "tool_calls": len(tools),
        "tool_errors": sum(1 for t in tools if t["status"] == "error"),
        "blocked": max(sum(1 for t in tools if t["status"] == "blocked"), len(report.get("blocked") or [])),
        "out_of_scope": len(report.get("out_of_scope_files") or []),
        "cost": round(sum(a["cost"] for a in attempts), 6),
        "tokens_in": sum(a["tokens"]["input"] + a["tokens"]["cacheRead"] for a in attempts),
        "tokens_out": sum(a["tokens"]["output"] for a in attempts),
        "phase_durations": phase_durations,
        "post_ship_issues": len(post_ship_issues),
    }
    return run


def collect(root: Path) -> list[dict]:
    runs = [parse_run(p) for p in sorted(root.iterdir()) if p.is_dir() and not p.name.startswith(".")]
    runs = [r for r in runs if r["attempts"] or r["brief"]["front"]]
    runs.sort(key=lambda r: r["summary"]["started_at"] or "", reverse=True)
    return runs


# ---------------------------------------------------------------- rendering

E = html.escape


def fmt_time(value) -> str:
    t = parse_time(value)
    return t.astimezone().strftime("%m-%d %H:%M") if t else "—"


def fmt_duration(seconds) -> str:
    if seconds is None:
        return "—"
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds} 秒"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m} 分 {s:02d} 秒"
    h, m = divmod(m, 60)
    return f"{h} 时 {m:02d} 分"


def fmt_ms(ms) -> str:
    if ms is None:
        return "—"
    return f"{ms} ms" if ms < 1000 else f"{ms / 1000:.1f} s"


def fmt_cost(cost) -> str:
    return "$0" if not cost else f"${cost:.4f}" if cost < 0.01 else f"${cost:.2f}"


def fmt_tokens(n) -> str:
    return f"{n / 1000:.1f}k" if n >= 1000 else str(n)


def chip(stage: str) -> str:
    label, icon, color, _ = STAGES[stage]
    return f'<span class="chip" style="--c: var(--{color})"><span class="chip-icon" aria-hidden="true">{icon}</span>{E(label)}</span>'


def render_tools(att: dict) -> str:
    tools = att["tools"]
    if not tools:
        return '<p class="muted">没有工具调用。</p>'
    longest = max((t["duration_ms"] or 0) for t in tools) or 1
    t0 = min((t["start_ms"] for t in tools if t["start_ms"] is not None), default=None)
    rows = []
    for i, t in enumerate(tools, 1):
        offset = "—"
        if t0 is not None and t["start_ms"] is not None:
            sec = (t["start_ms"] - t0) / 1000
            offset = f"+{int(sec // 60)}:{sec % 60:04.1f}"
        status_label = {"ok": "", "error": "报错", "blocked": "被拦截 / 未执行", "running": "执行中"}[t["status"]]
        if t["duration_ms"] is not None:
            pct = max(t["duration_ms"] / longest * 100, 0.8)
            bar = f'<span class="bar {t["status"]}" style="width:{pct:.1f}%"></span>'
        else:
            bar = ""
        tip = f'{t["name"]}: {t["args"]}\n{t["output_tail"]}'.strip()
        rows.append(
            f'<tr class="tool {t["status"]}" title="{E(tip)}">'
            f'<td class="num">{i}</td><td class="num">{offset}</td>'
            f'<td><code>{E(t["name"] or "?")}</code></td>'
            f'<td class="args"><code>{E(t["args"][:160])}</code></td>'
            f'<td class="dur"><span class="track">{bar}</span><span class="num">{fmt_ms(t["duration_ms"])}</span></td>'
            f'<td class="st">{E(status_label)}</td></tr>'
        )
    return (
        '<div class="scroll"><table class="tools"><thead><tr><th>#</th><th>开始</th><th>工具</th><th>参数</th>'
        '<th>耗时</th><th>结果</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table></div>"
    )


def render_attempt(att: dict) -> str:
    status_text = {"done": "已结束", "running": "进行中", "crashed": "异常退出", "aborted": "被中止", "error": "模型调用报错"}
    dur = None
    end = att["ended_at"] or att["last_event_at"]
    if att["started_at"] and end:
        dur = (parse_time(end) - parse_time(att["started_at"])).total_seconds()
    tok = att["tokens"]
    meta = [
        ("模型", " → ".join(att["models"]) or "—"),
        ("开始", fmt_time(att["started_at"])),
        ("耗时", fmt_duration(dur)),
        ("轮次", str(att["turns"])),
        ("token", f'入 {fmt_tokens(tok["input"] + tok["cacheRead"])} / 出 {fmt_tokens(tok["output"])}'),
        ("花费", fmt_cost(att["cost"])),
    ]
    meta_html = "".join(f"<div><dt>{E(k)}</dt><dd>{E(v)}</dd></div>" for k, v in meta)
    final = ""
    if att["error_message"]:
        final = f'<p class="final error-text">模型报错：{E(att["error_message"][:400])}</p>'
    elif att["final_text"]:
        final = f'<p class="final">{E(att["final_text"][:500])}</p>'
    st = status_text.get(att["status"], att["status"])
    return (
        f'<section class="attempt"><h4>第 {E(att["id"])} 次 · {E(PURPOSE_LABELS.get(att["purpose"], att["purpose"]))}'
        f' <span class="muted">· {E(st)}</span></h4><dl class="meta">{meta_html}</dl>{final}{render_tools(att)}</section>'
    )


def render_report(report) -> str:
    if not report:
        return '<p class="muted">还没有 report.json（守卫扩展在运行结束时写）。</p>'
    items = []
    blocked = report.get("blocked") or []
    if blocked:
        lis = "".join(f'<li><code>{E(str(b.get("command") or b.get("tool")))}</code> <span class="muted">{E(str(b.get("reason", "")))}</span></li>' for b in blocked)
        items.append(f"<h5>被拦截的命令</h5><ul>{lis}</ul>")
    oos = report.get("out_of_scope_files") or []
    if oos:
        lis = "".join(f"<li><code>{E(f)}</code></li>" for f in oos)
        items.append(f'<h5 class="warn-text">超出允许路径的文件</h5><ul>{lis}</ul>')
    changed = report.get("changed_files") or {}
    zones = [(z, changed.get(z) or []) for z in ("A", "B", "C")]
    if any(files for _, files in zones):
        lis = "".join(f'<li><b>{z} 区</b> {", ".join(f"<code>{E(f)}</code>" for f in files)}</li>' for z, files in zones if files)
        items.append(f"<h5>改动的文件</h5><ul>{lis}</ul>")
    conf = report.get("c_zone_confirmations") or []
    if conf:
        lis = "".join(f"<li>{E(json.dumps(c, ensure_ascii=False))}</li>" for c in conf)
        items.append(f"<h5>C 区确认</h5><ul>{lis}</ul>")
    tests = report.get("tests") or []
    if tests:
        lis = "".join(f'<li><code>{E(str(t.get("command", "")))}</code> → {E(str(t.get("result", "")))} <span class="muted">{E(str(t.get("summary", "")))}</span></li>' for t in tests)
        items.append(f"<h5>测试</h5><ul>{lis}</ul>")
    ship = report.get("ship")
    if ship:
        sha = f' · <code>{E(ship["sha"])}</code>' if ship.get("sha") else ""
        items.append(f'<h5>ship</h5><p>{E(str(ship.get("mode")))} · {"成功" if ship.get("ok") else "未成功"}{sha} <span class="muted">{E(str(ship.get("summary", "")))}</span></p>')
    if report.get("branch"):
        items.append(f'<h5>分支</h5><p><code>{E(report["branch"])}</code> {" ".join(f"<code>{E(c)}</code>" for c in report.get("commits") or [])}</p>')
    return "".join(items) or '<p class="muted">report.json 里没有需要注意的项。</p>'


def render_reviews(acceptance) -> str:
    reviews = (acceptance or {}).get("reviews") or []
    if not reviews:
        return '<p class="muted">还没有验收记录。</p>'
    out = []
    for r in reviews:
        stage = "accepted" if r.get("verdict") == "accepted" else "rejected"
        checks = "".join(
            f'<li class="{"ok" if c.get("ok") else "bad"}"><span aria-hidden="true">{"✓" if c.get("ok") else "✕"}</span> {E(str(c.get("name", "")))}</li>'
            for c in r.get("checks") or []
        )
        out.append(
            f'<div class="review">{chip(stage)} <span class="muted">第 {E(str(r.get("after_attempt")))} 次之后 · {E(fmt_time(r.get("at")))} · {E(str(r.get("by", "")))}</span>'
            f'<p>{E(str(r.get("summary", "")))}</p><ul class="checks">{checks}</ul></div>'
        )
    return "".join(out)


def render_run(run: dict) -> str:
    s = run["summary"]
    front = run["brief"]["front"]
    flags = []
    if s["blocked"]:
        flags.append(f'<span class="flag serious">拦截 {s["blocked"]}</span>')
    if s["out_of_scope"]:
        flags.append(f'<span class="flag warning">越界 {s["out_of_scope"]}</span>')
    if s["tool_errors"]:
        flags.append(f'<span class="flag muted-flag">报错 {s["tool_errors"]}</span>')
    links = " · ".join(f'<a href="{E(href)}">{E({"session.html": "会话记录", "brief.md": "任务单", "report.md": "运行报告"}[name])}</a>' for name, href in run["links"].items())
    phases = s.get("phase_durations", {})
    phase_text = " / ".join([
        "排队 " + fmt_duration(phases.get("queued_seconds")),
        "执行 " + fmt_duration(phases.get("execution_seconds")),
        "等验收 " + fmt_duration(phases.get("acceptance_wait_seconds")),
        "发布 " + fmt_duration(phases.get("publish_seconds")),
    ])
    models = ", ".join(m.split("/", 1)[-1] for m in s["models"]) or front.get("model", "—")
    search = " ".join([run["id"], run["task"], run["brief"]["goal"], models]).lower()
    return f"""
<details class="run" data-stage="{run['stage']}" data-search="{E(search)}">
  <summary>
    <span class="c-run"><b>{E(run['id'])}</b><span class="goal">{E(run['brief']['goal'] or '')}</span></span>
    <span class="c-stage">{chip(run['stage'])}</span>
    <span class="c-zone">{E(front.get('zone', '—'))} 区</span>
    <span class="c-model">{E(models)}</span>
    <span class="c-time num">{E(fmt_time(s['started_at']))}</span>
    <span class="c-dur num">{E(fmt_duration(s['active_seconds']))}</span>
    <span class="c-tools num">{s['tool_calls']} 次</span>
    <span class="c-cost num">{E(fmt_cost(s['cost']))}</span>
    <span class="c-flags">{''.join(flags)}</span>
  </summary>
  <div class="detail">
    <p class="stage-detail">{chip(run['stage'])} {E(run['stage_detail'])}</p>
    <div class="cols">
      <section><h3>任务单</h3>
        <dl class="meta">
          <div><dt>任务</dt><dd>{E(run['task'])}</dd></div>
          <div><dt>区</dt><dd>{E(front.get('zone', '—'))}</dd></div>
          <div><dt>模型设定</dt><dd>{E(front.get('model', '—'))}</dd></div>
          <div><dt>工具</dt><dd>{E(front.get('tools', '—'))}</dd></div>
          <div><dt>ship</dt><dd>{E(front.get('ship', '—'))}</dd></div>
          <div><dt>任务类型</dt><dd>{E(run.get('type', 'unknown'))}</dd></div>
          <div><dt>四段耗时</dt><dd>{E(phase_text)}</dd></div>
          <div><dt>发布后问题</dt><dd>{s.get('post_ship_issues', 0)}</dd></div>
          <div><dt>owner 决定</dt><dd>{E(front.get('owner_decision', '—'))}</dd></div>
        </dl>
        <p class="links">{links or '<span class="muted">没有可打开的文件</span>'}</p>
      </section>
      <section><h3>验收</h3>{render_reviews(run['acceptance'])}</section>
      <section><h3>守卫报告</h3>{render_report(run['report'])}</section>
    </div>
    <h3>执行过程</h3>
    {''.join(render_attempt(a) for a in run['attempts']) or '<p class="muted">还没有执行记录。</p>'}
  </div>
</details>"""


CSS = """
/* 布局：顶部一行计数，下面是一张可展开的运行清单；展开后是任务单、验收、守卫报告和每次执行的工具时间线。 */
:root {
  --bg: #f4f6f8; --surface: #ffffff; --ink: #14181d; --ink-2: #48505a; --ink-3: #6c7480; --line: #d9dee4;
  --accent: #1d6f8c; --bar: #2a78d6; --good: #0ca30c; --warning: #fab219; --serious: #ec835a; --critical: #d03b3b; --muted: #9aa1ab;
  /* 本机离线打开，只用系统字体：Windows 上是 Segoe UI + 微软雅黑，等宽用 Cascadia Mono / Consolas */
  --font-ui: "Segoe UI", "Microsoft YaHei UI", "PingFang SC", "Noto Sans CJK SC", system-ui, sans-serif;
  --font-mono: "Cascadia Mono", Consolas, "SFMono-Regular", Menlo, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #111417; --surface: #191d21; --ink: #e9ecef; --ink-2: #b6bdc6; --ink-3: #8a929c; --line: #2b3137;
  --accent: #5db4d3; --bar: #3987e5; --muted: #6c737c; color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #111417; --surface: #191d21; --ink: #e9ecef; --ink-2: #b6bdc6; --ink-3: #8a929c; --line: #2b3137;
  --accent: #5db4d3; --bar: #3987e5; --muted: #6c737c; color-scheme: dark; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); font: 14px/1.55 var(--font-ui); }
.wrap { max-width: 1240px; margin: 0 auto; padding-inline: 16px; padding-block: 20px 48px; display: grid; gap: 16px; }
header { display: flex; flex-wrap: wrap; align-items: baseline; justify-content: space-between; gap: 4px 16px; }
h1 { font-size: 20px; margin: 0; letter-spacing: .01em; text-wrap: balance; }
h3 { font-size: 13px; margin: 0 0 6px; color: var(--ink-2); letter-spacing: .04em; }
h4 { font-size: 13px; margin: 0 0 8px; }
h5 { font-size: 12px; margin: 10px 0 4px; color: var(--ink-2); }
p { margin: 0; }
code { font: 12px/1.4 var(--font-mono); overflow-wrap: anywhere; }
a { color: var(--accent); }
a:focus-visible, summary:focus-visible, button:focus-visible, input:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.muted { color: var(--ink-3); }
.num { font-variant-numeric: tabular-nums; }
.sub { color: var(--ink-3); font-size: 12px; }
.sample { border: 1px dashed var(--warning); border-radius: 6px; padding: 8px 12px; color: var(--ink-2); background: var(--surface); }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 8px; }
.kpi { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; display: grid; gap: 2px; }
.kpi .v { font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
.kpi .k { font-size: 12px; color: var(--ink-3); display: flex; align-items: center; gap: 6px; }
.kpi .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--c, var(--muted)); }
.summaries { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 8px; }
.summary-box { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; overflow-x: auto; }
table.mini { width: 100%; border-collapse: collapse; font-size: 12px; }
table.mini th, table.mini td { text-align: left; padding: 3px 6px; border-bottom: 1px solid color-mix(in srgb, var(--line) 65%, transparent); }
.filters { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.filters button { font: inherit; font-size: 12px; border: 1px solid var(--line); background: var(--surface); color: var(--ink-2); border-radius: 999px; padding: 3px 10px; cursor: pointer; }
.filters button[aria-pressed="true"] { border-color: var(--accent); color: var(--ink); box-shadow: inset 0 0 0 1px var(--accent); }
.filters input { font: inherit; font-size: 13px; padding: 4px 10px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface); color: var(--ink); min-width: 0; flex: 1 1 180px; max-width: 280px; }
.list { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
.cols-head, summary { display: grid; grid-template-columns: minmax(0, 2.6fr) 1.3fr .6fr 1.2fr .9fr .9fr .6fr .7fr 1.2fr; gap: 10px; align-items: center; padding: 10px 14px; }
.cols-head { font-size: 12px; color: var(--ink-3); border-bottom: 1px solid var(--line); }
details.run + details.run { border-top: 1px solid var(--line); }
summary { cursor: pointer; list-style: none; }
summary::-webkit-details-marker { display: none; }
summary:hover { background: color-mix(in srgb, var(--accent) 6%, transparent); }
.c-run { display: grid; min-width: 0; }
.c-run b { font-family: var(--font-mono); font-size: 13px; }
.goal { color: var(--ink-3); font-size: 12px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.c-model, .c-zone { color: var(--ink-2); font-size: 12px; min-width: 0; overflow-wrap: anywhere; }
.c-time, .c-dur, .c-tools, .c-cost { font-size: 12px; color: var(--ink-2); }
.c-flags { display: flex; flex-wrap: wrap; gap: 4px; }
.chip { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; padding: 1px 8px 1px 6px; border-radius: 999px; border: 1px solid color-mix(in srgb, var(--c) 55%, var(--line)); background: color-mix(in srgb, var(--c) 12%, var(--surface)); color: var(--ink); white-space: nowrap; }
.chip-icon { color: var(--c); font-weight: 700; }
.flag { font-size: 11px; padding: 0 6px; border-radius: 4px; border: 1px solid var(--line); color: var(--ink-2); white-space: nowrap; }
.flag.serious { border-color: var(--serious); }
.flag.warning { border-color: var(--warning); }
.detail { padding: 4px 14px 18px; display: grid; gap: 14px; border-top: 1px dashed var(--line); }
.stage-detail { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; padding-top: 10px; color: var(--ink-2); }
.cols { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; }
.cols section { min-width: 0; }
dl.meta { display: grid; grid-template-columns: repeat(auto-fill, minmax(120px, 1fr)); gap: 6px 14px; margin: 0; }
dl.meta div { min-width: 0; }
dt { font-size: 11px; color: var(--ink-3); }
dd { margin: 0; font-size: 13px; overflow-wrap: anywhere; }
ul { margin: 0; padding-left: 18px; display: grid; gap: 2px; }
ul.checks { list-style: none; padding: 0; margin-top: 4px; }
ul.checks li.ok span { color: var(--good); }
ul.checks li.bad span { color: var(--critical); }
.review { display: grid; gap: 4px; justify-items: start; }
.review + .review { margin-top: 10px; padding-top: 10px; border-top: 1px dashed var(--line); }
.warn-text { color: var(--ink); }
.warn-text::before { content: "⚠ "; color: var(--warning); }
.links { margin-top: 8px; }
.attempt { border: 1px solid var(--line); border-radius: 6px; padding: 10px 12px; display: grid; gap: 8px; min-width: 0; }
.final { background: var(--bg); border-radius: 4px; padding: 6px 10px; color: var(--ink-2); white-space: pre-wrap; overflow-wrap: anywhere; }
.error-text { color: var(--ink); border-left: 3px solid var(--critical); }
.scroll { overflow-x: auto; }
table.tools { width: 100%; border-collapse: collapse; font-size: 12px; min-width: 620px; }
table.tools th { text-align: left; font-weight: 500; color: var(--ink-3); padding: 4px 6px; border-bottom: 1px solid var(--line); }
table.tools td { padding: 4px 6px; border-bottom: 1px solid color-mix(in srgb, var(--line) 60%, transparent); vertical-align: middle; }
table.tools td.args { max-width: 360px; }
table.tools td.dur { width: 30%; }
table.tools td.dur .track { display: inline-block; width: calc(100% - 64px); height: 8px; vertical-align: middle; }
table.tools td.dur .num { display: inline-block; width: 60px; text-align: right; color: var(--ink-2); }
.bar { display: block; height: 8px; border-radius: 0 4px 4px 0; background: var(--bar); }
.bar.error { background: var(--critical); }
tr.tool.error td.st { color: var(--ink); }
tr.tool.error td.st::before { content: "✕ "; color: var(--critical); }
tr.tool.blocked td.st::before { content: "⛔ "; }
tr.tool.blocked td { background: color-mix(in srgb, var(--serious) 10%, transparent); }
tr.tool.running td.st::before { content: "▸ "; color: var(--accent); }
.empty { padding: 24px 14px; color: var(--ink-3); }
footer { color: var(--ink-3); font-size: 12px; }
@media (max-width: 900px) {
  .cols-head { display: none; }
  summary { grid-template-columns: minmax(0, 1fr) auto; gap: 6px 10px; }
  .c-zone, .c-time, .c-tools { display: none; }
  .c-model { grid-column: 1; } .c-dur { grid-column: 2; justify-self: end; }
  .c-cost { grid-column: 2; justify-self: end; } .c-flags { grid-column: 1; }
  .cols { grid-template-columns: minmax(0, 1fr); }
}
@media (prefers-reduced-motion: reduce) { * { transition: none !important; } }
"""

JS = """
(function () {
  var buttons = document.querySelectorAll('.filters button');
  var input = document.getElementById('q');
  var runs = document.querySelectorAll('details.run');
  var empty = document.getElementById('empty');
  var stage = 'all';
  function apply() {
    var q = (input.value || '').trim().toLowerCase(), shown = 0;
    runs.forEach(function (r) {
      var ok = (stage === 'all' || r.dataset.stage === stage) && (!q || r.dataset.search.indexOf(q) >= 0);
      r.hidden = !ok; if (ok) shown++;
    });
    empty.hidden = shown > 0;
  }
  buttons.forEach(function (b) {
    b.addEventListener('click', function () {
      stage = b.dataset.stage;
      buttons.forEach(function (x) { x.setAttribute('aria-pressed', x === b ? 'true' : 'false'); });
      apply();
    });
  });
  input.addEventListener('input', apply);
})();
"""


def weekly_cloud_costs(runs: list[dict]) -> dict[str, float]:
    weekly: dict[str, float] = {}
    for run in runs:
        for att in run["attempts"]:
            models = " ".join(att.get("models") or [])
            if "openai/" not in models:
                continue
            started = parse_time(att.get("started_at")) or parse_time(run["summary"].get("started_at"))
            week = started.date().isocalendar() if started else None
            key = f"{week.year}-W{week.week:02d}" if week else "unknown"
            weekly[key] = weekly.get(key, 0.0) + float(att.get("cost") or 0)
    return dict(sorted(weekly.items()))


def task_type_summaries(runs: list[dict]) -> list[dict]:
    out = []
    for key in sorted({r.get("type", "unknown") for r in runs}):
        group = [r for r in runs if r.get("type", "unknown") == key]
        passed_first = sum(1 for r in group if len(r["attempts"]) == 1 and r["stage"] in {"accepted", "accepted_pending_ship", "shipped"})
        out.append({
            "type": key,
            "runs": len(group),
            "first_pass_rate": passed_first / len(group) if group else 0,
            "avg_attempts": sum(len(r["attempts"]) for r in group) / len(group) if group else 0,
            "blocked": sum(r["summary"].get("blocked", 0) for r in group),
            "out_of_scope": sum(r["summary"].get("out_of_scope", 0) for r in group),
        })
    return out


def render_type_summary(runs: list[dict]) -> str:
    rows = []
    for item in task_type_summaries(runs):
        rows.append(
            f"<tr><td><code>{E(item['type'])}</code></td><td class='num'>{item['runs']}</td>"
            f"<td class='num'>{item['first_pass_rate']:.0%}</td><td class='num'>{item['avg_attempts']:.1f}</td>"
            f"<td class='num'>{item['blocked']}</td><td class='num'>{item['out_of_scope']}</td></tr>"
        )
    if not rows:
        return ""
    return "<section class='summary-box'><h3>按任务类型汇总</h3><table class='mini'><thead><tr><th>类型</th><th>运行数</th><th>一次通过率</th><th>平均执行次数</th><th>被拦次数</th><th>越界文件数</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table></section>"


def render_weekly_costs(runs: list[dict]) -> str:
    weekly = weekly_cloud_costs(runs)
    if not weekly:
        return ""
    rows = "".join(f"<tr><td><code>{E(k)}</code></td><td class='num'>{E(fmt_cost(v))}</td></tr>" for k, v in weekly.items())
    return "<section class='summary-box'><h3>云端花费（按周）</h3><table class='mini'><thead><tr><th>周</th><th>花费</th></tr></thead><tbody>" + rows + "</tbody></table></section>"


def render(runs: list[dict], root: Path, sample_note: str | None = None, fragment: bool = False) -> str:
    counts = {k: 0 for k in STAGES}
    for r in runs:
        counts[r["stage"]] += 1
    total_cost = sum(r["summary"]["cost"] for r in runs)
    blocked = sum(r["summary"]["blocked"] for r in runs)
    kpis = [
        ("待验收", counts["awaiting"], "warning"),
        ("退回修正", counts["rejected"], "serious"),
        ("运行中", counts["running"], "accent"),
        ("失败", counts["failed"] + counts["ship_failed"], "critical"),
        ("已通过 / 已发布", counts["accepted"] + counts["accepted_pending_ship"] + counts["shipped"], "good"),
    ]
    kpi_html = "".join(
        f'<div class="kpi"><span class="v">{v}</span><span class="k"><span class="dot" style="--c: var(--{c})"></span>{E(k)}</span></div>'
        for k, v, c in kpis
    )
    post_ship_issues = sum(r["summary"].get("post_ship_issues", 0) for r in runs)
    kpi_html += f'<div class="kpi"><span class="v">{E(fmt_cost(total_cost))}</span><span class="k">累计花费 · 拦截 {blocked} 次</span></div>'
    kpi_html += f'<div class="kpi"><span class="v">{post_ship_issues}</span><span class="k">发布后发现的问题</span></div>'
    present = sorted({r["stage"] for r in runs}, key=lambda k: STAGES[k][3])
    filters = '<button type="button" data-stage="all" aria-pressed="true">全部 ' + str(len(runs)) + "</button>" + "".join(
        f'<button type="button" data-stage="{k}" aria-pressed="false">{E(STAGES[k][0])} {counts[k]}</button>' for k in present
    )
    generated = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M")
    note = f'<p class="sample">{E(sample_note)}</p>' if sample_note else ""
    body = f"""<div class="wrap">
<header><h1>pi 运行看板</h1><span class="sub">{E(str(root))} · 生成于 {generated} · 共 {len(runs)} 次运行</span></header>
{note}
<div class="kpis">{kpi_html}</div>
<div class="summaries">{render_type_summary(runs)}{render_weekly_costs(runs)}</div>
<div class="filters" role="group" aria-label="按阶段筛选">{filters}<input id="q" type="search" placeholder="搜索任务 ID、目标或模型" aria-label="搜索"></div>
<div class="list">
<div class="cols-head"><span>运行 / 目标</span><span>阶段</span><span>区</span><span>模型</span><span>开始</span><span>耗时</span><span>工具</span><span>花费</span><span>注意</span></div>
{''.join(render_run(r) for r in runs)}
<p class="empty" id="empty" hidden>没有符合条件的运行。</p>
</div>
<footer>数据来自每个运行目录里的 events.jsonl、report.json 和 acceptance.json。耗时只统计工具真正执行的时间；没有耗时、标为“被拦截 / 未执行”的调用没有真正运行。</footer>
</div>
<script>{JS}</script>"""
    head = f'<title>pi 运行看板</title>\n<style>{CSS}</style>\n'
    if fragment:
        return head + body + "\n"
    return (
        '<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
        + head + "</head><body>\n" + body + "\n</body></html>\n"
    )


def to_summary(runs: list[dict]) -> dict:
    return {
        "schema": BOARD_SCHEMA,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "runs": [
            {"id": r["id"], "task": r["task"], "type": r.get("type"), "stage": r["stage"], "stage_label": STAGES[r["stage"]][0],
             "stage_detail": r["stage_detail"], **r["summary"],
             "attempts": [{k: a[k] for k in ("id", "purpose", "status", "started_at", "ended_at", "turns", "models", "cost")}
                          for a in r["attempts"]]}
            for r in runs
        ],
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="把 pi 运行目录汇总成一页静态 HTML。")
    ap.add_argument("root", type=Path, help="运行根目录，例如 D:\\Work\\AITradingSystem_agent_runs")
    ap.add_argument("-o", "--output", type=Path, help="HTML 输出路径（默认 <root>/runs.html）")
    ap.add_argument("--json", type=Path, help="同时写一份 JSON 摘要（给 Claude 验收时读）")
    ap.add_argument("--fragment", action="store_true", help="只输出页面内容，不带 doctype/html/head/body")
    ap.add_argument("--sample-note", help="在页首加一行说明（用于样例数据）")
    args = ap.parse_args(argv)
    if not args.root.is_dir():
        print(f"找不到运行根目录：{args.root}", file=sys.stderr)
        return 2
    runs = collect(args.root)
    out = args.output or (args.root / "runs.html")
    out.write_text(render(runs, args.root, args.sample_note, args.fragment), encoding="utf-8")
    if args.json:
        args.json.write_text(json.dumps(to_summary(runs), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{len(runs)} 次运行 -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
