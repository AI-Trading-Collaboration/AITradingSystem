"""Display-only Chinese translations of frozen research rules; never evaluate a strategy."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEDULES = {
    "daily": "每个交易日",
    "week_end": "每周最后一个交易日",
    "month_end": "每月最后一个交易日",
}
EPISODE_NAMES = {
    "dotcom_bear": "科网泡沫熊市",
    "global_financial_crisis": "金融危机",
    "us_downgrade_2011": "2011 年美债评级下调",
    "china_oil_2015": "2015–2016 年中国与油价冲击",
    "q4_2018": "2018 年四季度下跌",
    "covid_crash": "新冠急跌",
    "inflation_bear_2022": "2022 年通胀熊市",
}
_CVAR = "cvar_5pct_relative_improvement"
_METRICS = {
    _CVAR: "左尾",
    "max_drawdown": "最大回撤",
    "geometric_annual_return": "几何年化",
    "rolling_geometric_return_low": "滚动 3 年几何年化的低位",
}


def clean(text: str) -> str:
    """Fold YAML whitespace without inserting gaps into Chinese words."""
    chinese = r"[\u3400-\u9fff\u3000-\u303f\uff00-\uffef]"
    return re.sub(rf"(?<={chinese}) (?={chinese})", "", " ".join(text.split()))


def number(value: float, digits: int = 1) -> str:
    text = f"{value:.{digits}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def percent(value: float) -> str:
    return number(value * 100) + "%"


def _words(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("需要非空文字")
    return clean(value)


def _mapping(value: Any, allowed: set[str], required: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - allowed:
        raise ValueError("字段不符合说明文件格式")
    return dict(value)


def load_signal_names(research: Path, problems: list[str]) -> dict[str, Any]:
    path = research / "signal_names.yaml"
    if not path.exists():
        return {}
    try:
        raw = _mapping(
            safe_load_yaml_path(path), {"schema_version", "signals"}, {"schema_version", "signals"}
        )
        if raw["schema_version"] != "research_signal_names.v1" or not isinstance(
            raw["signals"], dict
        ):
            raise ValueError("不是 research_signal_names.v1")
        names = {}
        for key, value in raw["signals"].items():
            _words(key)
            entry = _mapping(value, {"name", "meaning"}, {"name"})
            names[key] = {
                "name": _words(entry["name"]),
                "meaning": _words(entry["meaning"]) if "meaning" in entry else None,
            }
        return names
    except Exception as exc:
        problems.append(f"research/signal_names.yaml: {exc}")
        return {}


def load_question_text(
    directory: Path, strategy_ids: Sequence[str], problems: list[str]
) -> dict[str, Any] | None:
    path = directory / "zh.yaml"
    if not path.exists():
        return None
    try:
        raw = _mapping(
            safe_load_yaml_path(path),
            {"schema_version", "title", "summary", "verdict", "strategies"},
            {"schema_version", "title", "summary"},
        )
        if raw["schema_version"] != "research_zh.v1":
            raise ValueError("不是 research_zh.v1")
        strategies = raw.get("strategies", {})
        if not isinstance(strategies, dict) or strategies.keys() - set(strategy_ids):
            raise ValueError("strategies 含未知策略")
        entries = {}
        for key, value in strategies.items():
            entry = _mapping(value, {"name", "role", "rule"}, {"name"})
            rule = entry.get("rule")
            if "rule" in entry:
                if not isinstance(rule, list) or not rule:
                    raise ValueError("rule 需要非空列表")
                lines = []
                for line in rule:
                    item = _mapping(line, {"label", "text"}, {"label", "text"})
                    lines.append({k: _words(v) for k, v in item.items()})
                rule = lines
            entries[key] = {
                "name": _words(entry["name"]),
                "role": _words(entry["role"]) if "role" in entry else None,
                "rule": rule,
            }
        return {
            "title": _words(raw["title"]),
            "summary": _words(raw["summary"]),
            "verdict": _words(raw["verdict"]) if "verdict" in raw else None,
            "strategies": entries,
        }
    except Exception as exc:
        problems.append(f"zh.yaml: {exc}")
        return None


def signal_view(repo: Path, signal_id: str, names: Mapping[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {}
    # A signal ID is a file name, not a path supplied to the filesystem.
    if re.fullmatch(r"[a-z0-9_]+", signal_id):
        try:
            raw = safe_load_yaml_path(
                repo / "src/ai_trading_system/research_screens/signals" / f"{signal_id}.yaml"
            )
            if (
                isinstance(raw, dict)
                and isinstance(raw.get("mechanism"), str)
                and raw["mechanism"].strip()
                and not isinstance(raw.get("expected_sign"), bool)
                and raw.get("expected_sign") in (-1, 1)
                and raw.get("target") in ("return", "risk")
            ):
                spec = raw
        except Exception:
            pass
    mechanism = spec.get("mechanism")
    name, meaning = signal_id, None
    if isinstance(mechanism, str):
        mechanism = clean(mechanism)
        colon = mechanism.find("：")
        if 0 <= colon < 20:
            name = mechanism[:colon]
        meaning = mechanism.split("：", 1)[-1].split("。", 1)[0]
    curated = names.get(signal_id, {})
    sign, target = spec.get("expected_sign"), spec.get("target")
    favourable, expectation = None, None
    if sign in (-1, 1) and target in ("return", "risk"):
        favourable = sign * (1 if target == "return" else -1)
        ending = (
            ("收益越高" if sign == 1 else "收益越低")
            if target == "return"
            else ("波动越大" if sign == 1 else "波动越小")
        )
        expectation = "信号越高，之后" + ending
    return {
        "id": signal_id,
        "name": curated.get("name") or name,
        "meaning": curated.get("meaning") or meaning,
        "favourable": favourable,
        "expectation": expectation,
    }


def _schedule(entry: Mapping[str, Any], fixed: bool = False) -> str:
    rebalance = entry.get("rebalance", {})
    schedule = rebalance.get("schedule", "daily")
    text = SCHEDULES.get(schedule, str(schedule))
    if fixed:
        text += "把仓位调回目标比例"
    else:
        text += "收盘后算目标仓位，下一个交易日收盘成交"
        if entry.get("delay_days", 0):
            text += f"，再晚 {entry['delay_days']} 天"
        if schedule != "daily":
            text += "；其余日子不动"
    if rebalance.get("band", 0):
        text += f"；只有当前仓位偏离目标超过目标的 {percent(rebalance['band'])} 时才真的交易"
    return text


def strategy_card(
    entry: Mapping[str, Any],
    *,
    repo: Path,
    names: Mapping[str, Any],
    text: Mapping[str, Any] | None,
    validated: bool,
    warmup_days: int | None = None,
) -> dict[str, Any]:
    strategy_id, function = entry["id"], entry.get("function")
    params = entry.get("params", {})
    curated = (text or {}).get(strategy_id, {})
    card: dict[str, Any] = {
        "id": strategy_id,
        "name": curated.get("name") or strategy_id,
        "role": curated.get("role") or ("验证期读取" if validated else "开发期变体"),
        "validated": validated,
        "kind": "custom",
        "signals": [],
        "lines": [],
        "levels": None,
        "source": "unavailable",
    }
    lines: list[tuple[str, str]] = []
    if function == "fixed_exposure" and "exposure" in params:
        card.update(kind="fixed_exposure", source="generated")
        exposure = params["exposure"]
        reserve = (
            " 10 年期国债（用 FRED DGS10 合成）"
            if entry.get("reserve") == "treasury_10y"
            else "现金"
        )
        lines = [
            (
                "仓位",
                f"固定持有 {percent(exposure)} 的 QQQ，其余 {percent(1 - exposure)} "
                f"是{reserve}；不看任何信号",
            ),
            ("调仓", _schedule(entry, True)),
        ]
    elif function == "signal_exposure" and isinstance(params.get("signals"), list):
        card["kind"] = "signal_exposure"
        refs = params["signals"]
        signals = [signal_view(repo, ref["id"], names) for ref in refs]
        card["signals"] = signals
        lines = [("信号", "、".join(f"「{s['name']}」" for s in signals))]
        missing = [s["id"] for s in signals if s["favourable"] is None]
        if missing:
            lines.append(
                (
                    "规则",
                    f"读不到 {'、'.join(missing)} 的信号规格，"
                    "不知道哪个方向有利，页面写不出买卖规则",
                )
            )
        elif signals:
            card["source"] = "generated"
            lines.extend(_signal_lines(entry, signals, warmup_days, card))
        else:
            lines.append(("规则", "没有信号，页面写不出买卖规则"))
    else:
        lines = [("规则", f"由代码里的 {function} 定义，页面暂时没有中文说明")]
    card["lines"] = [{"label": label, "text": words} for label, words in lines]
    if curated.get("rule"):
        card.update(lines=curated["rule"], source="zh.yaml")
    return card


def _signal_lines(
    entry: Mapping[str, Any],
    signals: list[dict[str, Any]],
    warmup: int | None,
    card: dict[str, Any],
) -> list[tuple[str, str]]:
    params = entry["params"]
    refs = params["signals"]
    count = len(signals)
    all_sign = all(ref.get("score", "sign") == "sign" for ref in refs)
    same_sign = all(s["favourable"] == signals[0]["favourable"] for s in signals)
    scores = []
    for ref, signal in zip(refs, signals, strict=True):
        positive = signal["favourable"] == 1
        good, bad = ("正", "负") if positive else ("负", "正")
        high, low = ("最高位", "最低位") if positive else ("最低位", "最高位")
        if ref.get("score", "sign") == "sign":
            words = f"为{good}记 1 分，为{bad}记 0 分"
        else:
            words = f"在自己历史里{high}记 1 分，{low}记 0 分"
        scores.append(f"「{signal['name']}」{words}")
    if all_sign and same_sign:
        good, bad = ("正", "负") if signals[0]["favourable"] == 1 else ("负", "正")
        score = f"信号为{good}记 1 分，为{bad}记 0 分（正好为 0 记 0.5 分）"
        if count > 1:
            score += f"；{count} 个信号的分数取平均"
    elif count == 1:
        high, low = ("最高位", "最低位") if signals[0]["favourable"] == 1 else ("最低位", "最高位")
        score = (
            f"看信号在自己历史（到当天为止）里的位置：处在{high}记 1 分，{low}记 0 分，中间按百分位"
        )
    else:
        score = "；".join(scores) + f"；{count} 个信号的分数取平均"
    if not all_sign and warmup is not None:
        score += f"（信号要先有 {warmup} 个有值的交易日做历史，之后才开始打分）"
    mapping = params.get("mapping", "linear")
    lo, hi = params.get("low", 0.0), params.get("high", 1.0)
    levels: list[dict[str, Any]] = []
    p_lo, p_hi = params.get("p_low", 0.0), params.get("p_high", 1.0)
    if mapping == "threshold":
        cut = params.get("cut", 0.5)
        above, below = f"分数 ≥ {number(cut, 3)}", f"分数 < {number(cut, 3)}"
        if count == 1:
            positive = signals[0]["favourable"] == 1
            if all_sign and cut == 0.5:
                good, bad = ("正", "负") if positive else ("负", "正")
                above, below = f"信号为{good}（正好为 0 也算）", f"信号为{bad}"
            elif not all_sign:
                boundary = percent(cut if positive else 1 - cut)
                upper, lower = ("以上", "以下") if positive else ("以下", "以上")
                above = f"信号在自己历史的 {boundary} 分位{upper}（分数 ≥ {number(cut, 3)}）"
                below = f"信号在自己历史的 {boundary} 分位{lower}（分数 < {number(cut, 3)}）"
        position = f"{above}时仓位 {percent(hi)}；{below}时仓位 {percent(lo)}"
        buy = f"{above}、而仓位还不到 {percent(hi)} 时，调仓日买入 QQQ，补到 {percent(hi)}"
        sell = (
            f"{below}、而仓位还高于 {percent(lo)} 时，调仓日卖出 QQQ 换成现金，降到 {percent(lo)}"
        )
        levels = [{"when": below, "exposure": lo}, {"when": above, "exposure": hi}]
    elif mapping == "steps":
        edges = params["edges"]
        exposures = params["levels"]
        lo, hi = min(exposures), max(exposures)
        bounds = ["-∞", *(number(v, 3) for v in edges), "+∞"]
        levels = [
            {"when": f"分数 [{bounds[i]}, {bounds[i + 1]})", "exposure": v}
            for i, v in enumerate(exposures)
        ]
        position = "；".join(f"{v['when']} 仓位 {percent(v['exposure'])}" for v in levels)
        buy = (
            f"分数越过分界线 {'、'.join(number(v, 3) for v in edges)} "
            f"向上时，仓位升到下一档，最高 {percent(hi)}，买入 QQQ"
        )
        sell = f"分数越过同样的分界线向下时，仓位降到下一档，最低 {percent(lo)}，卖出 QQQ 换成现金"
    else:
        # The bench's sign-score display enumerates at most four binary signals, not outcomes.
        if all_sign and 2 <= count <= 4 and p_lo == 0 and p_hi == 1:
            for n in range(count, -1, -1):
                when = f"{n}/{count} 个信号有利"
                if count == 2 and same_sign:
                    good, bad = ("正", "负") if signals[0]["favourable"] == 1 else ("负", "正")
                    when = (
                        f"2 个信号都为{good}"
                        if n == 2
                        else (f"2 个信号都为{bad}" if n == 0 else f"1 个信号为{good}、1 个为{bad}")
                    )
                levels.append({"when": when, "exposure": lo + (hi - lo) * n / count})
            position = "按有利信号的个数定仓位，见下表"
            buy = f"有利的信号变多时，仓位按下表上调，最高 {percent(hi)}，买入 QQQ"
            sell = f"有利的信号变少时，仓位按下表下调，最低 {percent(lo)}，卖出 QQQ 换成现金"
        else:
            position = (
                f"随分数线性变化：分数 {number(p_lo, 3)} 时仓位 {percent(lo)}，"
                f"分数 {number(p_hi, 3)} 时仓位 {percent(hi)}，中间按比例"
            )
            up, down = "分数升高", "分数降低"
            if count == 1:
                up = (
                    "信号走高（分数升高）"
                    if signals[0]["favourable"] == 1
                    else "信号走低（分数升高）"
                )
                down = (
                    "信号走低（分数降低）"
                    if signals[0]["favourable"] == 1
                    else "信号走高（分数降低）"
                )
            buy = f"{up}时按比例加仓，最多到 {percent(hi)}，买入 QQQ"
            sell = f"{down}时按比例减仓，最低到 {percent(lo)}，卖出 QQQ 换成现金"
    card["levels"] = levels
    other = (
        f"仓位最高 {number(hi)} 倍（用 2 倍、3 倍杠杆产品凑出来）"
        if hi > 1
        else f"不加杠杆，仓位在 {percent(lo)} 到 {percent(hi)} 之间，其余是现金"
    )
    other += "；信号暂时没有值时沿用上一次的仓位；成本和成交规则由评估台统一计算"
    return [
        ("打分", score),
        ("仓位", position),
        ("买入（加仓）", buy),
        ("卖出（减仓）", sell),
        ("调仓", _schedule(entry)),
        ("其他", other),
    ]


def rule_text(metric: str, op: str, value: float, confidence: float | None = None) -> str:
    symbol = {">=": "≥", "<=": "≤"}.get(op, op)
    fallback = f"{metric} {symbol} {number(value, 4)}"
    section, _, field = metric.partition(".")
    if field not in _METRICS:
        return fallback
    name = _METRICS[field]
    if section in ("ci_low", "ci_high"):
        interval = f"{percent(confidence)} 区间" if confidence is not None else "置信区间"
        side = "下限" if section == "ci_low" else "上限"
        if value == 0 and op == ">":
            suffix = "排除「没有改善」" if section == "ci_low" else "还不能说比对照差"
            gap = " " if confidence is not None else ""
            return f"{name}改善的{gap}{interval}{side} > 0（{suffix}）"
        return f"{interval}的{side}：" + rule_text(f"comparison.{field}", op, value)
    if section == "placebo_rank":
        if op == ">=":
            return f"{name}改善胜过 {percent(value)} 的随机择时（同平均仓位）"
        return f"{name}的随机择时排名 {symbol} {number(value, 4)}"
    prefixes = {
        "leave_year_min": "任意去掉一年重算，最差的一次仍然：",
        "leave_episode_min": "任意去掉一个固定行情段重算，最差的一次仍然：",
    }
    if section in prefixes:
        return prefixes[section] + rule_text(f"comparison.{field}", op, value)
    if re.fullmatch(r"cost_\d+(?:p\d+)?", section):
        return f"单边成本 {section[5:].replace('p', '.')} bps 下：" + rule_text(
            f"comparison.{field}", op, value
        )
    if re.fullmatch(r"delay_\d+", section):
        return f"晚 {section[6:]} 天执行：" + rule_text(f"comparison.{field}", op, value)
    if section != "comparison":
        return fallback
    amount = percent(abs(value)) if field == _CVAR else number(abs(value) * 100) + " 个点"
    if op not in (">=", ">"):
        amount = percent(value) if field == _CVAR else number(value * 100) + " 个点"
        return f"{name}相对对照 {symbol} {amount}"
    if field == _CVAR:
        if value > 0:
            return f"左尾亏损比对照{'至少小' if op == '>=' else '小超过'} {amount}"
        if value < 0:
            return f"左尾亏损比对照最多大 {amount}"
        return "左尾亏损比对照小" if op == ">" else "左尾亏损不比对照大"
    positive, negative = ("浅", "深") if field == "max_drawdown" else ("多", "少")
    if value > 0:
        return f"{name}比对照{'至少' + positive if op == '>=' else positive + '超过'} {amount}"
    if value < 0:
        return f"{name}比对照最多{negative} {amount}"
    zero = "浅" if field == "max_drawdown" else "高"
    return (
        f"{name}比对照{zero}"
        if op == ">"
        else f"{name}不比对照{'深' if field == 'max_drawdown' else '低'}"
    )


def observed_text(metric: str, observed: float | None) -> str:
    if observed is None:
        return "—"
    section, _, field = metric.partition(".")
    if section == "placebo_rank":
        return f"胜过 {percent(observed)}"
    if field == _CVAR:
        return f"{observed * 100:+.1f}%"
    if field in _METRICS:
        return f"{observed * 100:+.1f} 点"
    return number(observed, 4)


def rule_rows(
    rules: Sequence[Mapping[str, Any]], confidence: float | None = None
) -> list[dict[str, Any]]:
    out = []
    for rule in rules:
        row = {key: rule[key] for key in ("metric", "op", "value")}
        row["text"] = rule_text(rule["metric"], rule["op"], rule["value"], confidence)
        if "observed" in rule:
            row.update(
                observed=rule["observed"],
                observed_text=observed_text(rule["metric"], rule["observed"]),
                holds=rule.get("holds"),
            )
        out.append(row)
    return out


def screen_reasons(
    inputs: Mapping[str, Any],
    results: Mapping[str, Any],
    adjusted_p: float | None,
    criteria: Mapping[str, Any],
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    def add(kind: str, text: str) -> None:
        out.append({"kind": kind, "text": text})

    qqq_only = inputs.get("qqq_only", False)
    market = "QQQ" if qqq_only or criteria["primary_market"] == "qqq" else "美股大盘"
    interval = inputs.get("qqq_interval" if qqq_only else "primary_interval")
    expected = inputs.get("expected_sign", 1)
    if interval and ((expected == 1 and interval[1] < 0) or (expected == -1 and interval[0] > 0)):
        bounds = ", ".join(f"{v:+.2f}" for v in interval)
        add(
            "eliminate",
            f"{market}{' ' if market == 'QQQ' else ''}上信号和之后结果的秩相关，"
            f"{percent(criteria['confidence'])} 区间 [{bounds}] 整体落在预期方向的反面",
        )
    breadth = inputs.get("breadth")
    tested, matches = results.get("direction_tested"), results.get("direction_matches")
    if not qqq_only and breadth is not None and breadth < criteria["eliminate_breadth"]:
        add(
            "eliminate",
            f"{tested} 个市场里只有 {matches} 个方向和预期一致"
            f"（{percent(breadth)}，淘汰线 {percent(criteria['eliminate_breadth'])}）",
        )
    corr = inputs.get("baseline_max_abs_corr")
    if corr is not None and corr >= criteria["copy_corr"]:
        add(
            "eliminate",
            "和基准信号（已实现波动、12-1 动量、10 个月均线距离）的最大秩相关 "
            f"{number(corr, 3)}，达到重复线 {number(criteria['copy_corr'], 3)}："
            "基本是基准换了个名字",
        )
    if out:
        return out
    if qqq_only:
        add("fail", "只在 QQQ 一个市场上筛，没有跨市场证据，不能直接成为候选")
        return out
    ic = inputs.get("qqq_ic")
    add(
        "ok" if ic is not None and ic * expected > 0 else "fail",
        f"QQQ 上方向和预期一致（秩相关 {ic:+.3f}）"
        if ic is not None
        else "QQQ 上方向和预期一致（秩相关 —）",
    )
    # Same numeric tolerance as the current screen verdict, not a new promotion boundary.
    add(
        "ok" if breadth is not None and breadth >= criteria["candidate_breadth"] - 1e-6 else "fail",
        f"{tested} 个市场里 {matches} 个方向一致"
        f"（{percent(breadth) if breadth is not None else '—'}，"
        f"候选线 {percent(criteria['candidate_breadth'])}）",
    )
    add(
        "ok" if adjusted_p is not None and adjusted_p <= criteria["bh_q"] else "fail",
        f"{market}主检验的调整后 p = "
        f"{number(adjusted_p, 4) if adjusted_p is not None else '—'}"
        f"（要 ≤ {number(criteria['bh_q'], 3)}）",
    )
    add(
        "ok" if inputs.get("leave_out_stable") is True else "fail",
        "去掉任意一个年代后，主市场的方向都不变",
    )
    return out
