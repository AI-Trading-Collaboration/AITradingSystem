"""Read-only projection of research records; the bench owns chain checks and DSR."""

from __future__ import annotations

import json
import math
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from ai_trading_system.research_bench import ledger as bench
from ai_trading_system.research_bench import trial
from ai_trading_system.research_bench.prereg import load_prereg
from ai_trading_system.research_bench.screen_ledger import screen_ledger_summary
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.yaml_loader import safe_load_yaml_path

from . import zh
from .markdown import cells

SCHEMA_VERSION = "research_site.v1"
TASK_PREFIXES = (
    "TRADING-2565",
    "TRADING-2566",
    "TRADING-2567",
    "TRADING-2568",
    "TRADING-2569",
    "DEVX-030",
)
WORDS = ("保留", "拒绝", "证据不足", "未测成")
_WORD = re.compile("|".join(WORDS))


def _plain(value: Any) -> Any:
    """Keep only JSON values, replacing non-finite numbers with null."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    if value is None or isinstance(value, str | int | bool):
        return value
    return str(value)


def _error(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return "；".join(
            f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}"
            for item in exc.errors(include_url=False)
        )
    return f"{type(exc).__name__}: {exc}"


def _text(path: Path, problems: list[str], label: str) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        problems.append(f"{label}: {_error(exc)}")
        return None


def _git(repo: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (OSError, UnicodeError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def _activity(text: str | None) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for line in (text or "").splitlines():
        fields = line.split("\x1f", 2)
        if len(fields) == 3:
            entries.append(dict(zip(("commit", "time", "subject"), fields, strict=True)))
    return entries


def _decision_words(text: str) -> list[dict[str, str | None]]:
    found: list[tuple[int, dict[str, str | None]]] = [
        (match.start(), {"label": None, "word": match[1]})
        for match in re.finditer(r"结论\s*[:：]\s*\**\s*(保留|拒绝|证据不足|未测成)", text)
    ]
    column: int | None = None
    position = 0
    in_table = False
    for line in text.splitlines(keepends=True):
        if line.startswith("|"):
            row = cells(line)
            if not in_table:
                column = next((i for i, cell in enumerate(row) if cell.strip("* ") == "结论"), None)
            elif column is not None and column < len(row):
                word = _WORD.match(row[column].strip("* "))
                if word:
                    found.append(
                        (
                            position,
                            {
                                "label": row[0].strip("* ") if column else None,
                                "word": word[0],
                            },
                        )
                    )
            in_table = True
        else:
            in_table = False
            column = None
        position += len(line)
    return [entry for _, entry in sorted(found, key=lambda item: item[0])]


def _run(record: dict[str, Any], dsr: Mapping[str, Any]) -> dict[str, Any]:
    def section(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    result = section(record.get("result"))
    strategy = section(result.get("strategy"))
    comparison = section(result.get("comparison"))
    rules = record.get("rules")
    return {
        "run_id": record.get("run_id"),
        "strategy": section(record.get("strategy")).get("id"),
        "role": section(record.get("window")).get("role"),
        "status": record.get("status"),
        "trial": section(record.get("trial")).get("number"),
        **{
            key: strategy.get(key)
            for key in ("geometric_annual_return", "max_drawdown", "sharpe_vs_cash")
        },
        "deflated_sharpe": dsr.get(record.get("run_id", "")),
        "vs_matched_return": comparison.get("geometric_annual_return"),
        "vs_matched_drawdown": comparison.get("max_drawdown"),
        "rules_held": sum(bool(rule.get("holds")) for rule in rules)
        if isinstance(rules, list)
        else None,
        "rules_total": len(rules) if isinstance(rules, list) else None,
        "rules": zh.rule_rows(rules, record.get("falsification_confidence"))
        if isinstance(rules, list)
        else None,
    }


def _question(
    directory: Path,
    requirements: list[str],
    dsr: Mapping[str, Any],
    source: Mapping[str, Any],
    repo: Path,
    names: Mapping[str, Any],
    warmup_days: int | None,
) -> tuple[dict[str, Any], bool]:
    problems: list[str] = []
    question: dict[str, Any] = {
        "question_id": directory.name,
        "question": None,
        "approved": False,
        "owner_decision": None,
        "trial_cap": None,
        "trials": 0,
        "runs_total": 0,
        "runs_failed": 0,
        "strategies": [],
        "runs": [],
        "reads": [],
        "decision": None,
        "last_activity": None,
        "links": {"prereg": None, "decision": None},
        "problems": problems,
        "zh": None,
        "strategy_cards": [],
        "gates": [],
    }
    invalid = False
    try:
        prereg = load_prereg(directory / "prereg.yaml")
        question.update(
            question=prereg.question,
            owner_decision=prereg.owner_decision,
            trial_cap=prereg.trial_cap,
            strategies=[strategy.id for strategy in prereg.strategies],
            approved=prereg.owner_decision is not None
            and any(prereg.owner_decision in text for text in requirements),
        )
        text_zh = zh.load_question_text(directory, question["strategies"], problems)
        strategy_text = text_zh["strategies"] if text_zh else None
        question["zh"] = (
            {key: text_zh[key] for key in ("title", "summary", "verdict")} if text_zh else None
        )
        question["strategy_cards"] = [
            zh.strategy_card(
                strategy.model_dump(),
                repo=repo,
                names=names,
                text=strategy_text,
                validated=strategy.id in prereg.validation.strategies,
                warmup_days=warmup_days,
            )
            for strategy in prereg.strategies
        ]
        card_names = {card["id"]: card["name"] for card in question["strategy_cards"]}
        question["gates"] = [
            {
                "strategy": key,
                "name": card_names[key],
                "rules": zh.rule_rows(
                    [rule.model_dump() for rule in prereg.validation.rules[key]],
                    prereg.validation.confidence,
                ),
            }
            for key in prereg.validation.strategies
        ]
        if prereg.question_id != directory.name:
            problems.append(f"prereg.yaml: question_id {prereg.question_id} 与目录名不一致")
            invalid = True
    except Exception as exc:
        problems.append(f"prereg.yaml: {_error(exc)}")
        invalid = True
    chain_ok = True
    try:
        chain_problems = bench.verify_runs(directory) + bench.verify_reads(directory)
        if chain_problems:
            problems.extend(chain_problems)
            chain_ok = False
            invalid = True
    except Exception as exc:
        problems.append(f"记录链: {_error(exc)}")
        chain_ok = False
        invalid = True
    if chain_ok:
        try:
            records = bench.run_records(directory)
            reads = bench.window_reads(directory)
            question.update(
                trials=len(bench.development_trials(records)),
                runs_total=len(records),
                runs_failed=sum(record.get("status") != "ok" for record in records),
                runs=[_run(record, dsr) for record in records],
                reads=[
                    {key: read.get(key) for key in ("read_id", "role", "window_id", "recorded_at")}
                    for read in reads
                ],
            )
        except Exception as exc:
            problems.append(f"运行记录或窗口读取: {_error(exc)}")
            invalid = True
            question.update(runs=[], reads=[])
    roles = {read["role"] for read in question["reads"]}
    if "consistency" in roles:
        status, step, next_step = "consistency_read", 7, "一致性期已读，等写结论。"
    elif "validation" in roles:
        status, step, next_step = "validation_read", 6, "验证期已读，等写结论。"
    elif any(run["role"] == "development" for run in question["runs"]):
        status, step = "developing", 5
        next_step = f"开发期已跑 {question['trials']} 次试验；下一步 Claude 审实现，再读验证期。"
    elif question["approved"]:
        status, step, next_step = "awaiting_dev_runs", 5, "已批准开题，等 pi 跑开发期。"
    else:
        status, step, next_step = "awaiting_approval", 4, "等你批准开题。"
    decision_path = directory / "decision.md"
    text = _text(decision_path, problems, "decision.md")
    if decision_path.exists():
        status, next_step = "closed", "已结题。"
        if text is not None:
            words = _decision_words(text)
            question["decision"] = {"words": words, "markdown": text}
            if not words:
                problems.append("decision.md: 没有找到结论词")
    question.update(status=status, step=step, next=next_step)
    if invalid:
        question.update(
            status="error", step=None, next="记录有问题，先修：见下面的问题。", runs=[], reads=[]
        )
    if source["commit"]:
        entries = _activity(
            _git(repo, "log", "-1", "--format=%H%x1f%cI%x1f%s", "--", f"research/{directory.name}")
        )
        question["last_activity"] = entries[0] if entries else None
        if source["repo_url"]:
            base = f"{source['repo_url']}/blob/{source['commit']}/research/{directory.name}"
            question["links"] = {
                "prereg": f"{base}/prereg.yaml",
                "decision": f"{base}/decision.md" if decision_path.exists() else None,
            }
    return question, chain_ok


def _ledger(text: str | None, problems: list[str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    in_total = False
    counts = dict.fromkeys(WORDS, 0)
    for line in (text or "").splitlines():
        if line.startswith("## "):
            in_total = line.strip() == "## 总表"
        if not in_total or not line.startswith("|"):
            continue
        row = cells(line)
        if len(row) != 5 or not row[0].isdigit():
            continue
        number = int(row[0])
        words = _WORD.findall(row[2])
        rows.append(
            dict(
                zip(
                    ("number", "line", "verdict", "rating", "fate"), [number, *row[1:]], strict=True
                )
            )
            | {"words": words}
        )
        for word in words:
            counts[word] += 1
        if not words:
            problems.append(f"research/ledger.md 第 {number} 行: 没有找到结论词")
    if not rows:
        problems.append("research/ledger.md: 没有找到总表数据行")
    return {"markdown": text, "parse_ok": bool(rows), "rows": rows, "counts": counts}


def _negative_sections(text: str | None) -> list[dict[str, str]]:
    sections: list[dict[str, str]] = []
    title: str | None = None
    body: list[str] = []
    for line in (text or "").split("\n"):
        if line.startswith("## "):
            if title is not None:
                sections.append({"title": title, "markdown": "\n".join(body).strip("\n")})
            title, body = line[3:].strip(), []
        elif title is not None:
            body.append(line)
    if title is not None:
        sections.append({"title": title, "markdown": "\n".join(body).strip("\n")})
    return sections


def _screens(directory: Path, problems: list[str]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}
    records = 0
    paths = sorted(
        (path for path in directory.glob("*.json") if re.fullmatch(r"[0-9]{4,}\.json", path.name)),
        key=lambda path: int(path.stem),
    )
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(record, dict):
                raise ValueError("筛选记录不是对象")
            key = f"{record.get('module_sha256')}:{record.get('spec_sha256')}"
            groups.setdefault(key, []).append(record)
            records += 1
        except (ValueError, OSError, UnicodeError) as exc:
            problems.append(f"research/_screens/ledger/{path.name}: {_error(exc)}")
    counts = dict.fromkeys(("candidate", "eliminated", "inconclusive"), 0)
    members: list[dict[str, Any]] = []
    for group in groups.values():
        latest = group[-1]
        member = {
            key: latest.get(key) for key in ("signal_id", "number", "verdict", "primary_p_value")
        }
        member["records"] = len(group)
        members.append(member)
        if member["verdict"] in counts:
            counts[member["verdict"]] += 1
    members.sort(key=lambda item: item["number"] if isinstance(item["number"], int) else -1)
    return {
        "present": directory.is_dir(),
        "records": records,
        "family_size": len(members),
        "counts": counts,
        "members": members,
    }


def _screen_settings(
    repo: Path, problems: list[str]
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    path = repo / "config/research/research_screen.yaml"
    if not path.exists():
        return None, {}
    try:
        settings = load_screen_settings(path, root=repo).settings.screen
        criteria = {
            key: getattr(settings, key)
            for key in (
                "primary_market",
                "primary_horizon",
                "eliminate_breadth",
                "candidate_breadth",
                "copy_corr",
                "bh_q",
                "confidence",
                "warmup_days",
            )
        }
        criteria["screen_end"] = settings.screen_end.isoformat()
    except Exception as exc:
        problems.append(f"config/research/research_screen.yaml: {_error(exc)}")
        return None, {}
    try:
        current = screen_ledger_summary(repo, settings)
        return criteria, {str(m["number"]): m for m in current["members"]}
    except Exception as exc:
        problems.append(f"筛选记录的 BH 校正: {_error(exc)}")
        return criteria, {}


def _screen_details(
    repo: Path,
    screens: dict[str, Any],
    names: Mapping[str, Any],
    criteria: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any]:
    details = {}
    counts = dict.fromkeys(("candidate", "eliminated", "inconclusive"), 0)
    for member in screens["members"]:
        key = str(member["number"])
        view = zh.signal_view(repo, member["signal_id"], names)
        now = current.get(key, {})
        results: dict[str, Any] = {}
        try:
            path = repo / "research/_screens/ledger" / f"{member['number']:04d}.json"
            results = json.loads(path.read_text(encoding="utf-8")).get("results") or {}
        except (OSError, ValueError, TypeError):
            pass
        inputs = results.get("verdict_inputs")
        details[key] = {
            **{k: view[k] for k in ("name", "meaning", "expectation")},
            "adjusted_p": now.get("adjusted_p"),
            "verdict_now": now.get("verdict"),
            "direction_matches": results.get("direction_matches"),
            "direction_tested": results.get("direction_tested"),
            "reasons": zh.screen_reasons(inputs, results, now.get("adjusted_p"), criteria)
            if inputs and criteria
            else [],
        }
        verdict = now.get("verdict") or member["verdict"]
        if verdict in counts:
            counts[verdict] += 1
    screens["counts"] = counts
    return details


def _spec_problems(repo: Path, problems: list[str]) -> None:
    """Broken display inputs are visible without importing executable signal modules."""
    directory = repo / "src/ai_trading_system/research_screens/signals"
    for path in sorted(directory.glob("*.yaml")):
        try:
            raw = safe_load_yaml_path(path)
            if (
                not isinstance(raw, dict)
                or not isinstance(raw.get("mechanism"), str)
                or not raw["mechanism"].strip()
                or isinstance(raw.get("expected_sign"), bool)
                or raw.get("expected_sign") not in (-1, 1)
                or raw.get("target") not in ("return", "risk")
            ):
                raise ValueError("信号规格缺少有效的 mechanism、expected_sign 或 target")
        except Exception as exc:
            problems.append(f"{path.relative_to(repo).as_posix()}: {_error(exc)}")


def _bench_settings(repo: Path, problems: list[str]) -> dict[str, Any] | None:
    path = repo / "config/research/research_bench.yaml"
    if not path.exists():
        return None
    try:
        settings = load_settings(path, root=repo).settings
        falsification = settings.falsification
        return {
            **{
                key: getattr(settings, key)
                for key in (
                    "cvar_levels",
                    "rolling_years",
                    "rolling_percentile",
                )
            },
            **{
                key: getattr(falsification, key)
                for key in (
                    "cost_bps",
                    "delay_days",
                    "placebo_block_days",
                    "placebo_draws",
                    "bootstrap_draws",
                    "confidence",
                )
            },
            "bootstrap_block_days": falsification.bootstrap_mean_block_days,
            "episodes": [
                {**episode.model_dump(), "name": zh.EPISODE_NAMES.get(episode.id, episode.id)}
                for episode in settings.episodes
            ],
        }
    except Exception as exc:
        problems.append(f"config/research/research_bench.yaml: {_error(exc)}")
        return None


_MARKETS: dict[Literal["us_market", "qqq"], str] = {
    "us_market": "美股大盘长历史（第一段，主检验）",
    "qqq": "QQQ 开发期（第二段，只查方向）",
}


def _trials(
    repo: Path,
    names: Mapping[str, Any],
    criteria: dict[str, Any] | None,
    problems: list[str],
) -> dict[str, Any]:
    research = repo / "research"
    directory = research / trial.TRIALS_DIR
    view: dict[str, Any] = {
        "present": directory.is_dir(),
        "trial_configs": 0,
        "question_trials": 0,
        "global_trials": 0,
        "gate": None,
        "plans": [],
    }
    if not view["present"]:
        return view
    summary: dict[str, Any] = {}
    try:
        summary = trial.trial_ledger_summary(research)
        for key in ("trial_configs", "question_trials", "global_trials"):
            view[key] = summary[key]
        problems.extend(f"试跑: {problem}" for problem in summary["problems"])
    except Exception as exc:
        problems.append(f"试跑: {_error(exc)}")
    gate = None
    try:
        gate = trial.load_gate(directory / "prereg.yaml")
        view["gate"] = {
            "confidence": gate.confidence,
            "markets": [
                {
                    "market": market,
                    "name": name,
                    "rules": zh.rule_rows(
                        [rule.model_dump() for rule in gate.markets[market].rules],
                        gate.confidence,
                    ),
                }
                for market, name in _MARKETS.items()
            ],
        }
    except Exception as exc:
        problems.append(f"试跑: prereg.yaml: {_error(exc)}")
    records = []
    try:
        records = trial.trial_records(research)
    except Exception as exc:
        problems.append(f"试跑: {_error(exc)}")
    for path in trial.plan_paths(research):
        try:
            plan = trial.load_plan(path, gate)
            primary = plan.config(plan.primary)
            verdict = summary.get("trials", {}).get(plan.trial_id, {}).get("verdict", "incomplete")
            markets = []
            for market, name in _MARKETS.items():
                record = next(
                    (
                        record
                        for record in records
                        if (
                            record.get("trial_id") == plan.trial_id
                            and record.get("config") == plan.primary
                            and record.get("market") == market
                            and record.get("status") == "ok"
                        )
                    ),
                    None,
                )
                rules = record.get("gate") or [] if record else []
                expected = [r.model_dump() for r in gate.markets[market].rules] if gate else []
                keys = ("metric", "op", "value")
                current = [{k: r[k] for k in keys} for r in rules] == [
                    {k: r[k] for k in keys} for r in expected
                ]
                markets.append(
                    {
                        "market": market,
                        "name": name,
                        "holds": record.get("gate_holds") if record else None,
                        "current_gate": current,
                        "rules": zh.rule_rows(rules, gate.confidence if gate else None),
                    }
                )
            view["plans"].append(
                {
                    "trial_id": plan.trial_id,
                    "signals": [zh.signal_view(repo, key, names) for key in plan.candidate.signals]
                    if plan.candidate
                    else [],
                    "owner_decision": plan.owner_decision,
                    "hypothesis_card": plan.hypothesis_card,
                    "from_validation_window": plan.from_validation_window,
                    "primary": plan.primary,
                    "configs": [config.id for config in plan.configs],
                    "verdict": verdict,
                    "verdict_name": trial.VERDICT_NAMES[verdict],
                    "card": zh.strategy_card(
                        primary.model_dump(),
                        repo=repo,
                        names=names,
                        text=None,
                        validated=False,
                        warmup_days=criteria["warmup_days"] if criteria else None,
                    ),
                    "markets": markets,
                }
            )
        except Exception as exc:
            problems.append(f"试跑: {path.name}: {_error(exc)}")
    return view


def collect(repo: Path, *, repo_url: str | None = None) -> dict[str, Any]:
    """Collect repository facts without evaluating strategies or reading market data."""
    problems: list[str] = []
    is_git = _git(repo, "rev-parse", "--show-prefix") == ""
    commit = _git(repo, "rev-parse", "HEAD") if is_git else None
    commit_time = _git(repo, "log", "-1", "--format=%cI") if commit else None
    if not commit or not re.fullmatch(r"[0-9a-f]{40}", commit) or not commit_time:
        commit, commit_time = None, None
    source = {
        "commit": commit,
        "commit_time": commit_time,
        "repo_url": repo_url.rstrip("/") if repo_url is not None else None,
    }
    research = repo / "research"
    summary: dict[str, Any] | None
    try:
        summary = bench.ledger_summary(research)
    except Exception as exc:
        problems.append(f"ledger_summary: {_error(exc)}")
        summary = None
    dsr: dict[str, Any] = {}
    if summary:
        for entry in summary["questions"].values():
            dsr.update({line["run_id"]: line["deflated_sharpe"] for line in entry["runs"]})
    requirements = [
        text
        for path in sorted((repo / "docs" / "requirements").glob("*.md"))
        if (text := _text(path, problems, f"docs/requirements/{path.name}")) is not None
    ]
    _spec_problems(repo, problems)
    names = zh.load_signal_names(research, problems)
    criteria, screen_current = _screen_settings(repo, problems)
    bench_view = _bench_settings(repo, problems)
    trials = _trials(repo, names, criteria, problems)
    questions: list[dict[str, Any]] = []
    chain_ok = True
    for directory in bench.question_directories(research):
        question, valid_chain = _question(
            directory,
            requirements,
            dsr,
            source,
            repo,
            names,
            criteria["warmup_days"] if criteria else None,
        )
        questions.append(question)
        chain_ok = chain_ok and valid_chain
        problems.extend(f"{directory.name}: {problem}" for problem in question["problems"])
    ledger = _ledger(_text(research / "ledger.md", problems, "research/ledger.md"), problems)
    negative = _text(research / "negative_results.md", problems, "research/negative_results.md")
    screens = _screens(research / "_screens" / "ledger", problems)
    screen_details = _screen_details(repo, screens, names, criteria, screen_current)
    tasks: list[dict[str, Any]] = []
    for path in sorted((repo / "tasks").glob("*.yaml")):
        try:
            raw = safe_load_yaml_path(path)
            if not isinstance(raw, dict):
                continue
            task_id = raw.get("id")
            if isinstance(task_id, str) and any(
                task_id == prefix or task_id.startswith(prefix + "_") for prefix in TASK_PREFIXES
            ):
                tasks.append({key: raw.get(key) for key in ("id", "status", "next_step", "area")})
        except Exception as exc:
            problems.append(f"tasks/{path.name}: {_error(exc)}")
    tasks.sort(key=lambda task: task["id"])
    activity = (
        _activity(
            _git(
                repo,
                "log",
                "-n",
                "20",
                "--format=%H%x1f%cI%x1f%s",
                "--",
                "research",
                *(f"tasks/{prefix}*" for prefix in TASK_PREFIXES),
            )
        )
        if commit
        else []
    )
    model = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "summary": {
            "active": sum(question["status"] != "closed" for question in questions),
            "closed": sum(question["status"] == "closed" for question in questions),
            "global_trials": trials["global_trials"]
            if trials["present"]
            else (
                summary["global_trials"]
                if summary is not None
                else sum(question["trials"] for question in questions)
            ),
            "trial_configs": trials["trial_configs"],
            "question_trials": trials["question_trials"]
            if trials["present"]
            else (
                summary["global_trials"]
                if summary is not None
                else sum(question["trials"] for question in questions)
            ),
            "failed_runs": sum(question["runs_failed"] for question in questions),
            "validation_reads": sum(
                read["role"] == "validation" for question in questions for read in question["reads"]
            ),
            "chain_ok": chain_ok,
            "problems": len(problems),
        },
        "questions": questions,
        "ledger": ledger,
        "negative_results": negative,
        "negative_sections": _negative_sections(negative),
        "screens": screens,
        "screen_details": screen_details,
        "screen_criteria": criteria,
        "bench": bench_view,
        "trials": trials,
        "tasks": tasks,
        "activity": activity,
        "problems": problems,
    }
    return {key: _plain(value) for key, value in model.items()}
