"""Static Chinese research progress page, with no external resources."""

from __future__ import annotations

import html
from collections.abc import Mapping, Sequence
from typing import Any

from . import zh
from .collect import WORDS
from .markdown import inline, markdown_to_html

CSS = """:root{
  --bg:#f4f6f7; --surface:#ffffff; --fg:#16212a; --muted:#5a6a77; --line:#d8e0e5;
    --accent:#1c6e8a; --accent-soft:#e1f0f5;
  --keep:#17673f; --keep-bg:#dff3e7; --rej:#a3342a; --rej-bg:#fae4e1; --weak:#8a5a00;
    --weak-bg:#fbefcf; --fail:#4d5b67; --fail-bg:#e6eaee;
  --hope:#0f5f78; --hope-bg:#d9eef5; --drop:#44505b; --drop-bg:#e3e7ea;
  --font:system-ui,-apple-system,"PingFang SC","Microsoft YaHei","Noto Sans SC",sans-serif;
  --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){
  --bg:#0e161c; --surface:#16222a; --fg:#e4ecf1; --muted:#93a5b2; --line:#26373f;
    --accent:#63b8d4; --accent-soft:#16323c;
  --keep:#6fd09a; --keep-bg:#143626; --rej:#f09c92; --rej-bg:#401d19; --weak:#e6bd62;
    --weak-bg:#3b2d0c; --fail:#a9b6c1; --fail-bg:#232f38;
  --hope:#7cc8e0; --hope-bg:#12323d; --drop:#aab5be; --drop-bg:#222c33; color-scheme:dark}}
:root[data-theme="dark"]{
  --bg:#0e161c; --surface:#16222a; --fg:#e4ecf1; --muted:#93a5b2; --line:#26373f;
    --accent:#63b8d4; --accent-soft:#16323c;
  --keep:#6fd09a; --keep-bg:#143626; --rej:#f09c92; --rej-bg:#401d19; --weak:#e6bd62;
    --weak-bg:#3b2d0c; --fail:#a9b6c1; --fail-bg:#232f38;
  --hope:#7cc8e0; --hope-bg:#12323d; --drop:#aab5be; --drop-bg:#222c33; color-scheme:dark}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--fg);font:15px/1.6 var(--font);
  padding-block:20px 56px; padding-left:max(16px,env(safe-area-inset-left,0px));
    padding-right:max(16px,env(safe-area-inset-right,0px))}
main{max-width:980px; margin:0 auto; display:flex; flex-direction:column; gap:28px;
  overflow-wrap:anywhere}
h1{font-size:28px;margin:0;letter-spacing:.01em}
h2{font-size:19px;margin:0 0 6px}
h3{font-size:16px;margin:0}
p{margin:0}
code{font-family:var(--mono); font-size:.88em; background:var(--accent-soft); padding:1px 5px;
  border-radius:4px}
a{color:inherit;text-decoration:none}
.m{color:var(--muted)}
.lead{color:var(--muted);margin-bottom:12px;max-width:68ch}
.meta{color:var(--muted);margin-top:4px}
.legend{margin-top:10px;display:flex;flex-wrap:wrap;gap:6px 6px;align-items:center}
.b{display:inline-block; padding:1px 9px; border-radius:999px; font-size:13px; font-weight:600;
  line-height:1.55; white-space:nowrap}
.b.keep{background:var(--keep-bg);color:var(--keep)}
.b.rej{background:var(--rej-bg);color:var(--rej)}
.b.weak{background:var(--weak-bg);color:var(--weak)}
.b.fail{background:var(--fail-bg);color:var(--fail)}
.b.low{background:var(--fail-bg);color:var(--fail)}
.b.hope{background:var(--hope-bg);color:var(--hope)}
.b.drop{background:var(--drop-bg);color:var(--drop)}
.b.cand{background:var(--hope-bg);color:var(--hope)}
.b.unk{background:var(--weak-bg);color:var(--weak)}
.b.out{background:var(--drop-bg);color:var(--drop)}
.b.st-in_progress{background:var(--hope-bg);color:var(--hope)}
.b.st-done{background:var(--keep-bg);color:var(--keep)}
.b.st-proposed{background:var(--fail-bg);color:var(--fail)}
/* route map */
.map{display:grid;
  grid-template-columns:minmax(0,1.05fr) auto minmax(0,1.1fr) auto minmax(0,1.05fr); gap:10px;
  align-items:stretch}
.col{display:flex;flex-direction:column;gap:10px;min-width:0}
.col.core,.col.out{justify-content:center}
.arrow{align-self:center; text-align:center; color:var(--muted); font-size:12px;
  line-height:1.4; min-width:52px; display:flex; flex-direction:column; align-items:center}
.arrow .ar{font-size:18px}
.node{display:block; background:var(--surface); border:1px solid var(--line);
  border-radius:10px; padding:12px 14px; position:relative; min-width:0}
.node .sub{color:var(--muted);font-size:13px;margin-top:2px}
.node .big{font-size:22px;font-weight:700;margin-top:6px;font-variant-numeric:tabular-nums}
.node .big small{font-size:12px;font-weight:500;color:var(--muted)}
.node .st{color:var(--muted);font-size:13px;margin-top:2px}
.node .chips{margin-top:8px; display:flex; flex-wrap:wrap; gap:4px 8px; align-items:center;
  font-variant-numeric:tabular-nums}
.node.live{border-color:var(--accent)}
@media (max-width:720px){
  .map{grid-template-columns:minmax(0,1fr)}
  .arrow{min-width:0;flex-direction:row;justify-content:center;gap:8px}
  .arrow .ar{display:inline-block;transform:rotate(90deg)}
}
/* cards */
.card{background:var(--surface); border:1px solid var(--line); border-radius:12px;
  padding:16px; display:flex; flex-direction:column; gap:12px; margin-bottom:12px}
.card.idle{border-style:dashed}
.card-h{display:flex; justify-content:space-between; gap:12px; align-items:flex-start;
  flex-wrap:wrap}
.verdicts{margin-top:6px;display:flex;gap:6px;flex-wrap:wrap}
.state{font-weight:600;color:var(--muted)}
.state.wait{color:var(--weak)}
.rail{list-style:none; margin:0; padding:0; display:grid;
  grid-template-columns:repeat(8,minmax(0,1fr)); gap:4px}
.rail li{font-size:12px; text-align:center; padding:5px 2px; border-radius:6px;
  background:var(--fail-bg); color:var(--muted); border-bottom:3px solid transparent}
.rail li.done{background:var(--accent-soft); color:var(--accent);
  border-bottom-color:var(--accent)}
.rail li.now{background:var(--accent);color:var(--surface);font-weight:700}
@media (max-width:560px){.rail{grid-template-columns:repeat(4,minmax(0,1fr))}}
.meters{display:flex;flex-wrap:wrap;gap:8px 24px;align-items:center}
.meters>div{display:flex;gap:8px;align-items:center}
.bar{width:120px; height:8px; background:var(--fail-bg); border-radius:4px; overflow:hidden;
  display:inline-block}
.bar i{display:block;height:100%;background:var(--accent)}
.n{font-variant-numeric:tabular-nums;font-weight:600}
.subs{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
@media (max-width:640px){.subs{grid-template-columns:minmax(0,1fr)}}
.sub-card{border:1px solid var(--line);border-radius:10px;padding:12px;min-width:0}
.sub-h{margin-bottom:8px}
.kv{margin:0;display:flex;flex-direction:column;gap:6px}
.kv div{display:flex;justify-content:space-between;gap:12px;font-size:14px}
.kv dt{color:var(--muted)}
.kv dd{margin:0; font-weight:600; font-variant-numeric:tabular-nums; text-align:right;
  white-space:nowrap}
.why{max-width:70ch}
/* ledger */
.grp{background:var(--surface); border:1px solid var(--line); border-radius:10px;
  padding:10px 14px; margin-bottom:8px}
.grp>summary{cursor:pointer;display:flex;gap:8px;align-items:center;list-style:none}
.grp>summary::-webkit-details-marker{display:none}
.grp>summary::before{content:"▸";color:var(--muted)}
.grp[open]>summary::before{content:"▾"}
.ledger{list-style:none;margin:10px 0 0;padding:0;display:flex;flex-direction:column;gap:10px}
.ledger li{display:flex;gap:12px;padding-top:10px;border-top:1px solid var(--line)}
.ledger .no{color:var(--muted); font-variant-numeric:tabular-nums; min-width:1.6em;
  text-align:right}
.ledger .t{font-weight:600}
.ledger .mm{margin-top:3px; display:flex; flex-wrap:wrap; gap:4px 8px; align-items:center;
  color:var(--muted); font-size:13.5px}
.ledger .rate{font-weight:600;color:var(--fg)}
details>summary{cursor:pointer}
details>summary:focus-visible,a:focus-visible{outline:2px solid var(--accent);
  outline-offset:2px}
.card details{border-top:1px solid var(--line);padding-top:10px}
.card .strat details{border-top:0;padding-top:0}
/* tables */
.wrap{overflow-x:auto;margin-top:8px}
table.narrow{max-width:520px}
table{border-collapse:collapse;width:100%;font-size:14px}
#forward th{white-space:normal}
#forward td:nth-child(-n+2),#forward td:last-child{white-space:nowrap}
th{text-align:left; color:var(--muted); font-weight:600; padding:6px 10px 6px 0;
  border-bottom:1px solid var(--line); white-space:nowrap}
td{padding:7px 10px 7px 0; border-bottom:1px solid var(--line); vertical-align:top;
  font-variant-numeric:tabular-nums}
@media (max-width:640px){
  table.stack thead{display:none}
  table.stack tr{display:block;padding:8px 0;border-bottom:1px solid var(--line)}
  table.stack td{display:flex;gap:10px;border:0;padding:2px 0}
  table.stack td::before{content:attr(data-label);color:var(--muted);min-width:6.5em;flex:none}
}
/* funnel */
.funnel{display:flex;flex-wrap:wrap;gap:8px 22px;align-items:flex-end;margin:6px 0 4px}
.funnel .big{font-size:26px;font-weight:700;font-variant-numeric:tabular-nums}
.funnel .fa{color:var(--muted);font-size:20px;padding-bottom:6px}
.plain{margin:0;padding-left:18px;display:flex;flex-direction:column;gap:6px}
/* not yet online */
.node.idle{border-style:dashed}
.sec-idle{border:1px dashed var(--line); border-radius:12px; padding:16px;
  background:var(--surface)}
.next{color:var(--fg)}
.sum{color:var(--muted);margin-top:4px}
/* rules, gates, screens */
.guide{color:var(--muted);margin-top:8px;max-width:72ch}
.nav{margin-top:10px;display:flex;flex-wrap:wrap;gap:6px 14px}
.nav a{color:var(--accent);font-size:14px}
.zhsum{max-width:72ch}
.verdict-rule{background:var(--accent-soft);border-radius:8px;padding:8px 12px;font-size:14px;
  max-width:72ch}
.sec-h{font-size:14px;margin:6px 0 0;color:var(--muted);letter-spacing:.04em}
.strats{display:flex;flex-direction:column;gap:12px}
.strat{border:1px solid var(--line);border-radius:10px;padding:12px 14px;display:flex;
  flex-direction:column;gap:8px;min-width:0}
.strat h4{margin:0;font-size:15px;display:flex;gap:8px;flex-wrap:wrap;align-items:baseline}
.strat .id{font-family:var(--mono);font-size:12px;color:var(--muted)}
.rule{margin:0;display:flex;flex-direction:column;gap:5px}
.rule>div{display:grid;grid-template-columns:7.5em minmax(0,1fr);gap:8px}
.rule dt{color:var(--muted);font-size:13.5px}
.rule dd{margin:0}
@media (max-width:560px){.rule>div{grid-template-columns:minmax(0,1fr);gap:0}}
.sigs{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:6px}
.sigs p{margin:0;font-size:13.5px}
.b.role{background:var(--accent-soft);color:var(--accent)}
.gate{list-style:none;margin:8px 0 0;padding:0;display:flex;flex-direction:column}
.gate li{display:flex;gap:10px;align-items:baseline;padding:6px 0;
  border-bottom:1px solid var(--line)}
.gate .mk{flex:none;width:1.4em;font-weight:700;color:var(--muted)}
.gate .pass .mk{color:var(--keep)}
.gate .miss .mk{color:var(--rej)}
.gate .t{flex:1;min-width:0}
.gate .v{flex:none;font-variant-numeric:tabular-nums;font-weight:600;white-space:nowrap}
.b.pass{background:var(--keep-bg);color:var(--keep)}
.b.miss{background:var(--rej-bg);color:var(--rej)}
.strat details,.card details.in{border-top:0;padding-top:0}
.criteria{background:var(--surface);border:1px solid var(--line);border-radius:12px;
  padding:14px 16px;margin:10px 0 14px;display:flex;flex-direction:column;gap:8px}
.criteria h3{margin:0}
.criteria ol{margin:0;padding-left:20px;display:flex;flex-direction:column;gap:3px}
.reasons{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:3px;
  font-size:13.5px}
.reasons .mk{display:inline-block;width:1.4em;font-weight:700}
.reasons .ok .mk{color:var(--keep)}
.reasons .fail .mk,.reasons .eliminate .mk{color:var(--rej)}
.sigcell>*{display:block}
.sigcell .m{font-size:13px}
.plan{margin-bottom:12px}
.glossary dl{margin:8px 0 0;display:flex;flex-direction:column;gap:8px}
.glossary dt{font-weight:600}
.glossary dd{margin:0;color:var(--muted);max-width:72ch}
"""

_CLASSES = dict(zip(WORDS, ("keep", "rej", "weak", "fail"), strict=True))


def _e(value: Any, *, quote: bool = False) -> str:
    return html.escape("—" if value is None else str(value), quote=quote)


def _badge(word: str, label: str | None = None) -> str:
    return f'<span class="b {_CLASSES.get(word, "low")}">{_e(label or word)}</span>'


def _number(value: Any, kind: str = "ratio") -> str:
    if value is None:
        return "—"
    if kind == "percent":
        return f"{value * 100:.1f}%"
    if kind == "difference":
        return f"{value * 100:+.1f} 点"
    return f"{value:.3f}" if kind == "p" else f"{value:.2f}"


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    return (
        '<div class="wrap"><table class="stack"><thead><tr>'
        + "".join(f"<th>{_e(header)}</th>" for header in headers)
        + "</tr></thead><tbody>"
        + "".join(
            "<tr>"
            + "".join(
                f'<td data-label="{_e(header, quote=True)}">{value}</td>'
                for header, value in zip(headers, row, strict=True)
            )
            + "</tr>"
            for row in rows
        )
        + "</tbody></table></div>"
    )


def _strategy(card: Mapping[str, Any]) -> str:
    out = []
    generated = card["source"] == "generated"
    if generated and card["signals"]:
        out.append('<ul class="sigs">')
        for signal in card["signals"]:
            out.append("<li><strong>" + _e(signal["name"]) + "</strong>")
            if signal["favourable"] is not None:
                out.append(
                    ' <span class="b role">'
                    + ("走高有利" if signal["favourable"] == 1 else "走低有利")
                    + "</span>"
                )
            if signal["meaning"]:
                out.append('<p class="m">' + _e(signal["meaning"]) + "</p>")
            out.append("</li>")
        out.append("</ul>")
    out.append('<dl class="rule">')
    for line in card["lines"]:
        if generated and line["label"] == "信号":
            continue
        out.append("<div><dt>" + _e(line["label"]) + "</dt><dd>" + _e(line["text"]) + "</dd></div>")
    out.append("</dl>")
    if card["levels"]:
        out.append(
            _table(
                ("情形", "目标仓位"),
                [
                    (_e(level["when"]), _e(zh.percent(level["exposure"])))
                    for level in card["levels"]
                ],
            )
        )
    return "".join(out)


def _gate(rules: Sequence[Mapping[str, Any]], *, measured: bool) -> str:
    title = f"共 {len(rules)} 条（还没有运行结果）"
    if measured:
        title = (
            f"{sum(bool(rule.get('holds')) for rule in rules)} / {len(rules)} 条成立"
            "（实测是相对对照的数，正为更好）"
        )
    out = ["<details><summary>通过门槛：" + title + '</summary><ul class="gate">']
    for rule in rules:
        kind = ("pass" if rule.get("holds") else "miss") if measured else ""
        mark = ("✓" if rule.get("holds") else "✗") if measured else "·"
        tag = f'<li class="{kind}">' if kind else "<li>"
        out.append(
            tag
            + '<span class="mk">'
            + mark
            + '</span><span class="t">'
            + _e(rule["text"])
            + "</span>"
        )
        if measured:
            out.append('<span class="v">' + _e(rule.get("observed_text")) + "</span>")
        out.append("</li>")
    return "".join(out) + "</ul></details>"


def _criteria(criteria: Mapping[str, Any], details: Mapping[str, Any]) -> str:
    tested = max((d.get("direction_tested") or 0 for d in details.values()), default=0)
    market = "QQQ" if criteria["primary_market"] == "qqq" else "美股大盘"
    breadth = zh.percent(criteria["candidate_breadth"])
    # This annotation names the reviewed two-thirds setting, not a new threshold.
    if criteria["candidate_breadth"] == 0.666667:
        breadth += "（三分之二）"
    return (
        '<div class="criteria"><h3>怎么筛、凭什么淘汰</h3><p>每个信号只用 '
        + _e(criteria["screen_end"])
        + " 以前的数据，在 QQQ、美股大盘和美国各行业（共 "
        + _e(tested)
        + " 个市场）上各测一次：信号的高低，和之后收益（或波动）的高低，"
        "方向是否和预先写下的预期一致（用秩相关）。主检验看"
        + _e(market)
        + "、之后 "
        + _e(criteria["primary_horizon"])
        + " 个交易日。</p>"
        "<p><strong>淘汰</strong>（满足任意一条）：</p><ol><li>主检验的 "
        + _e(zh.percent(criteria["confidence"]))
        + " 区间整体落在预期方向的反面；</li>"
        "<li>方向和预期一致的市场不到 "
        + _e(zh.percent(criteria["eliminate_breadth"]))
        + "；</li><li>和某个基准信号（已实现波动、12-1 个月动量、10 个月均线距离）的秩相关绝对值 ≥ "
        + _e(zh.number(criteria["copy_corr"]))
        + "，基本是基准换了个名字。</li></ol>"
        "<p><strong>候选</strong>（四条都要满足）：</p><ol><li>QQQ 上方向和预期一致；</li>"
        "<li>方向一致的市场不少于 "
        + _e(breadth)
        + "；</li><li>"
        + _e(market)
        + "主检验的 p 值，按整个筛选过的信号一起做 Benjamini-Hochberg 校正后 ≤ "
        + _e(zh.number(criteria["bh_q"]))
        + "；</li><li>去掉任意一个年代后，主检验的方向不变。</li>"
        "</ol><p>其余是<strong>证据不足</strong>：既没到淘汰线，也不够格当候选。"
        "候选只是有资格去试跑，不是结论。</p></div>"
    )


def _trials(trials: Mapping[str, Any]) -> str:
    out = ['<section id="trials"><h2>试跑</h2>']
    if not trials.get("present"):
        return "".join(out) + "<p>还没有试跑记录。</p></section>"
    out.append(
        '<p class="lead">候选信号先不开研究题，按预先写好的计划试跑，只用开发期数据，'
        "从不读验证期。主配置要在下面两段门槛都过，才交给 owner 决定要不要开题；"
        "其他配置只做敏感性，不决定。每个配置都算一次试验。</p>"
    )
    out.append(
        f"<p>累计试验 {_e(trials['global_trials'])} = "
        f"研究题的开发试验 {_e(trials['question_trials'])} "
        f"+ 试跑配置 {_e(trials['trial_configs'])}</p>"
    )
    if trials.get("gate"):
        out.append(
            '<details class="grp"><summary><strong>门槛</strong></summary>'
            '<div class="criteria"><h3>试跑门槛</h3>'
        )
        for market in trials["gate"]["markets"]:
            out.append("<p><strong>" + _e(market["name"]) + '</strong></p><ul class="plain">')
            out.extend("<li>" + _e(rule["text"]) + "</li>" for rule in market["rules"])
            out.append("</ul>")
        out.append('<p class="m">对照：平均仓位相同的固定仓位。</p></div></details>')
    for plan in trials.get("plans", []):
        name = "、".join(s["name"] for s in plan["signals"]) or "配置 " + plan["primary"]
        kind = {"pass": "pass", "fail": "miss"}.get(plan["verdict"], "unk")
        out.append(
            '<details class="grp"><summary>'
            + _e(plan["trial_id"])
            + " · "
            + _e(name)
            + " · "
            + f'<span class="b {kind}">'
            + _e(plan["verdict_name"])
            + "</span></summary>"
            + f'<article class="card plan" id="trial-{_e(plan["trial_id"], quote=True)}">'
            '<div class="card-h"><h3>'
            + _e(plan["trial_id"])
            + ' <span class="m">'
            + _e(name)
            + "</span></h3>"
            + f'<span class="b {kind}">'
            + _e(plan["verdict_name"])
            + "</span></div>"
        )
        out.append(
            '<p class="m">主配置 '
            + _e(plan["primary"])
            + f" · 共 {len(plan['configs'])} 个配置"
            + (" · 想法来自读过验证期的题目" if plan["from_validation_window"] else "")
            + "</p>"
        )
        out.append('<p class="m">来源：' + _e(plan["hypothesis_card"]) + "</p>")
        out.append(
            '<h4 class="sec-h">主配置怎么买卖</h4><div class="strat">'
            + _strategy(plan["card"])
            + "</div>"
        )
        for market in plan["markets"]:
            if market.get("outcome") == "no_ruler":
                out.append(
                    "<p><strong>"
                    + _e(market["name"])
                    + "</strong> · 没有尺子（不算过，也不算没过）</p>"
                )
                continue
            if market["holds"] is None:
                continue
            out.append(
                "<p><strong>"
                + _e(market["name"])
                + "</strong> · "
                + ("过" if market["holds"] else "没过")
                + ("" if market["current_gate"] else "（按当时的门判，和现行门不同）")
                + "</p>"
            )
            out.append(_gate(market["rules"], measured=True))
        out.append("</article></details>")
    return "".join(out) + "</section>"


def _glossary(bench: Mapping[str, Any] | None) -> str:
    terms = [
        (
            "对照",
            "平均仓位和策略相同的固定仓位（调仓方式也相同）。策略要在没有多担风险的前提下比它好。",
        ),
        (
            "左尾",
            "最差的 5% 的交易日的平均日收益。「左尾亏损比对照小 10%」是指这个亏损幅度小 10%。",
        ),
        ("最大回撤", "从高点到随后低点的最大跌幅。「浅 5 个点」是少跌 5 个百分点。"),
        ("几何年化", "按复利算的年化收益。"),
    ]
    if bench:
        years = bench["rolling_years"]
        terms.extend(
            [
                (
                    f"滚动 {years} 年几何年化的低位",
                    f"把所有连续 {years} 年的窗口按复利年化，"
                    f"取排在最低 {zh.percent(bench['rolling_percentile'])} 位置的那个："
                    "衡量运气不好的时候还能剩多少。",
                ),
                (
                    "区间（置信区间）",
                    f"把收益按 {zh.number(bench['bootstrap_block_days'])} 天一块反复重抽 "
                    f"{bench['bootstrap_draws']} 次算出来的区间。"
                    "下限 > 0 表示有把握说确实有改善，不只是运气；"
                    "上限 > 0 表示还不能说它比对照差。",
                ),
                (
                    "随机择时",
                    f"把策略的仓位序列按 {bench['placebo_block_days']} 天一块打乱 "
                    f"{bench['placebo_draws']} 次，得到平均仓位相同的随机策略。"
                    f"「胜过 {zh.percent(bench['confidence'])}」表示不是随便择时就能做到。",
                ),
                (
                    "单边成本 bps",
                    "每次买入或卖出按成交额收的成本，"
                    + "、".join(zh.number(v) for v in bench["cost_bps"])
                    + " bps 分别是 "
                    + "、".join(zh.number(v / 100, 2) + "%" for v in bench["cost_bps"])
                    + "；门槛按最高那一档检查。",
                ),
                (
                    "晚 N 天执行",
                    "信号算出后再晚 N 天才成交（检查 "
                    + "、".join(str(v) for v in bench["delay_days"])
                    + " 天），看结论是不是靠抢到当天价格。",
                ),
                (
                    "去掉一年 / 去掉一个固定行情段",
                    "逐一去掉某一年，或评估台事先写死的一段大跌（"
                    + "、".join(
                        f"{episode['name']} {episode['start']}–{episode['end']}"
                        for episode in bench["episodes"]
                    )
                    + "）后重算，最差的一次仍然要满足门槛：防止结论只靠某一年或某一次危机。",
                ),
            ]
        )
    terms.extend(
        [
            (
                "开发期 / 验证期",
                "开发期用来调试和看结果；验证期每道题只读一次，结论由它定；之后的一致性窗口只当复核。",
            ),
            ("预注册", "读到任何结果之前，先把问题、规则、门槛和窗口写下并提交，之后不能改。"),
            (
                "秩相关",
                "信号的排名和之后结果的排名之间的相关系数，-1 到 1，"
                "只看方向和单调性，不受极端值影响。",
            ),
            (
                "Benjamini-Hochberg 校正",
                "同时筛很多信号时，对 p 值做的校正，控制「撞大运」的误报比例。",
            ),
            ("折算夏普", "把「试了很多次才挑出最好的一个」造成的虚高扣掉之后的夏普比率。"),
        ]
    )
    return (
        '<section id="glossary" class="glossary"><details class="grp">'
        "<summary><strong>名词解释</strong></summary><dl>"
        + "".join("<dt>" + _e(term) + "</dt><dd>" + _e(text) + "</dd>" for term, text in terms)
        + "</dl></details></section>"
    )


def _card(question: Mapping[str, Any]) -> str:
    text_zh = question.get("zh")
    title = text_zh["title"] if text_zh else question["question"]
    short = title[:80] + "…" if title and len(title) > 80 else title
    state = {"closed": "已结题", "error": "记录有问题"}.get(question["status"], "进行中")
    opened = " open" if question["status"] != "closed" else ""
    out = [
        f'<details class="grp"{opened}><summary>'
        + _e(question["question_id"])
        + " · "
        + _e(short)
        + " · "
        + _e(state)
        + "</summary>",
        f'<article class="card" id="q-{_e(question["question_id"], quote=True)}">',
        f'<div class="card-h"><div><h3>{_e(question["question_id"])} '
        f'<span class="m">{_e(short)}</span></h3>',
    ]
    decision = question["decision"]
    if decision and decision["words"]:
        out.append(
            '<p class="verdicts">'
            + "".join(
                _badge(item["word"], f"{item['label']} {item['word']}" if item["label"] else None)
                for item in decision["words"]
            )
            + "</p>"
        )
    out.append(f'</div><p class="state">{state}</p></div>')
    if text_zh:
        out.append(f'<p class="zhsum">{_e(text_zh["summary"])}</p>')
        if text_zh["verdict"]:
            out.append(
                '<p class="verdict-rule"><strong>怎么判：</strong>'
                + _e(text_zh["verdict"])
                + "</p>"
            )
        out.append(
            "<details><summary>英文原题</summary><p>" + _e(question["question"]) + "</p></details>"
        )
    else:
        if title and len(title) > 80:
            out.append(f"<details><summary>题目全文</summary><p>{_e(title)}</p></details>")
        if question["status"] != "error":
            out.append(
                '<p class="m">还没有中文说明：research/'
                + _e(question["question_id"])
                + "/zh.yaml</p>"
            )
    out.append('<ol class="rail">')
    for index, label in enumerate(
        ("想法", "数据", "筛选", "开题", "开发期", "验证期", "一致性", "期权层"), start=1
    ):
        step = question["step"]
        progress = "todo"
        if step is not None:
            if index < step or (index == step and question["status"] == "closed"):
                progress = "done"
            elif index == step:
                progress = "now"
        out.append(f'<li class="{progress}"><span>{label}</span></li>')
    trials, cap = question["trials"], question["trial_cap"]
    width = min(100, max(0, 100 * trials / cap)) if cap else 0
    reads = sum(read["role"] == "validation" for read in question["reads"])
    out.extend(
        (
            f'</ol><p class="next">{_e(question["next"])}</p>',
            '<div class="meters"><div><span class="m">试验额度</span>'
            f'<span class="bar"><i style="width:{width:g}%"></i></span>'
            f'<span class="n">{_e(trials)} / {_e(cap)}</span></div>',
            '<div><span class="m">验证期</span><span class="n">'
            + (f"读过 {reads} 次" if reads else "未读")
            + "</span></div>",
            '<div><span class="m">失败运行</span>'
            f'<span class="n">{_e(question["runs_failed"])}</span></div></div>',
        )
    )
    validation_runs = [
        run for run in question["runs"] if run["role"] in ("validation", "consistency")
    ]
    cards = question.get("strategy_cards", [])
    if cards:
        out.append('<h4 class="sec-h">策略怎么买卖</h4><div class="strats">')
        gates = {gate["strategy"]: gate for gate in question.get("gates", [])}
        for card in cards:
            if not card["validated"]:
                continue
            out.append(
                '<div class="strat"><h4>'
                + _e(card["name"])
                + ' <span class="id">'
                + _e(card["id"])
                + '</span> <span class="b role">'
                + _e(card["role"])
                + "</span></h4>"
            )
            out.append(_strategy(card))
            measured = next(
                (
                    run
                    for run in validation_runs
                    if run["strategy"] == card["id"] and run.get("rules") is not None
                ),
                None,
            )
            rules = measured["rules"] if measured else gates.get(card["id"], {}).get("rules", [])
            out.append(_gate(rules, measured=measured is not None))
            out.append("</div>")
        out.append("</div>")
        variants = [card for card in cards if not card["validated"]]
        if variants:
            out.append(
                f"<details><summary>开发期变体 {len(variants)} 个"
                "（只在开发期看，不决定结论）</summary>"
            )
            out.append(
                _table(
                    ("变体", "说明"),
                    [
                        (
                            _e(card["name"]) + ' <span class="m">' + _e(card["id"]) + "</span>",
                            _e(
                                card["role"]
                                if card["role"] != "开发期变体"
                                else "；".join(
                                    line["text"]
                                    for line in card["lines"]
                                    if line["label"] in ("仓位", "规则", "调仓")
                                )
                            ),
                        )
                        for card in variants
                    ],
                )
            )
            out.append("</details>")
    if validation_runs:
        out.append('<h4 class="sec-h">验证期结果</h4><div class="subs">')
        for run in validation_runs:
            rules = (
                f"{_e(run['rules_held'])} / {_e(run['rules_total'])}"
                if run["rules_total"] is not None
                else "—"
            )
            out.append(
                f'<div class="sub-card"><p class="sub-h">{_e(run["run_id"])} '
                f'<span class="m">{_e(run["strategy"])}</span></p><dl class="kv">'
            )
            for label, value in (
                ("规则成立", rules),
                (
                    "最大回撤相对同敞口固定仓位（正为更浅）",
                    _number(run["vs_matched_drawdown"], "difference"),
                ),
                ("年化相对同敞口固定仓位", _number(run["vs_matched_return"], "difference")),
                (
                    "年化 / 最大回撤",
                    _number(run["geometric_annual_return"], "percent")
                    + " / "
                    + _number(run["max_drawdown"], "percent"),
                ),
            ):
                out.append(f"<div><dt>{label}</dt><dd>{value}</dd></div>")
            out.append("</dl></div>")
        out.append("</div>")
    development = [run for run in question["runs"] if run["role"] == "development"]
    if development:
        out.append(f"<details><summary>开发期 {len(development)} 次运行</summary>")
        out.append(
            _table(
                ("运行", "策略", "状态", "年化", "最大回撤", "折算夏普"),
                [
                    (
                        _e(run["run_id"]),
                        _e(run["strategy"]),
                        _e(run["status"]),
                        _number(run["geometric_annual_return"], "percent"),
                        _number(run["max_drawdown"], "percent"),
                        _number(run["deflated_sharpe"]),
                    )
                    for run in development
                ],
            )
        )
        out.append("</details>")
    if decision:
        out.append(
            '<details><summary>结论全文</summary><div class="doc">'
            + markdown_to_html(decision["markdown"])
            + "</div></details>"
        )
    for name, url in question["links"].items():
        if url:
            filename = "prereg.yaml" if name == "prereg" else "decision.md"
            out.append(f'<p><a href="{_e(url, quote=True)}">{filename}</a></p>')
    out.extend(f'<p class="m">{_e(problem)}</p>' for problem in question["problems"])
    return "".join(out) + "</article></details>"


def _board(ledger: Mapping[str, Any]) -> str:
    out = ['<section id="board"><h2>结论板</h2><p class="lead">按子问题计：']
    out.append(" · ".join(f"{word} {_e(ledger['counts'][word])}" for word in WORDS) + "</p>")
    if not ledger["parse_ok"]:
        out.append(markdown_to_html(ledger["markdown"] or ""))
    else:
        groups = [(word, _CLASSES[word]) for word in WORDS]
        if any(not row["words"] for row in ledger["rows"]):
            groups.append(("无结论词", "none"))
        for word, group in groups:
            rows = [
                row
                for row in ledger["rows"]
                if (word in row["words"] if group != "none" else not row["words"])
            ]
            opened = " open" if group == "rej" else ""
            out.append(
                f'<details class="grp" id="grp-{group}"{opened}><summary>'
                f"{_badge(word)} <strong>{len(rows)}</strong> 条</summary>"
            )
            if not rows:
                out.append('<p class="m">还没有。</p>')
            out.append('<ul class="ledger">')
            for row in rows:
                out.append(
                    f'<li><span class="no">{_e(row["number"])}</span><div>'
                    f'<p class="t">{_e(row["line"])}</p><p class="mm">'
                    + " ".join(_badge(item) for item in row["words"])
                )
                if row["verdict"] not in WORDS:
                    out.append(f'<span class="m">{_e(row["verdict"])}</span>')
                out.append(
                    f'<span class="rate">评级 {_e(row["rating"])}</span> '
                    f'<span class="m">· {_e(row["fate"])}</span></p></div></li>'
                )
            out.append("</ul></details>")
    return "".join(out) + "</section>"


def _route(model: Mapping[str, Any]) -> str:
    summary, screens, ledger = model["summary"], model["screens"], model["ledger"]
    counts = screens["counts"]
    screen_state = "live" if screens["present"] else "idle"
    screen_text = (
        f"已筛 {_e(screens['family_size'])} 个 · 淘汰 {_e(counts['eliminated'])} · "
        f"证据不足 {_e(counts['inconclusive'])} · 候选 {_e(counts['candidate'])}"
        if screens["present"]
        else "S4b 上线后启用"
    )
    chain = "完整" if summary["chain_ok"] else "有问题"
    return (
        '<section id="route"><h2>研究路线</h2><p class="lead">'
        "三条入口把想法送进同一条研究流程；每一步的数字都读自仓库里的记录。</p>"
        '<div class="map"><div class="col feeders">'
        f'<a class="node {screen_state}" href="#screens"><h3>信号筛选</h3>'
        '<p class="sub">TRADING-2565 S4 · QQQ 方向信号</p>'
        f'<p class="big">{_e(screens["family_size"]) if screens["present"] else "—"}</p>'
        f'<p class="st">{screen_text}</p></a>'
        '<a class="node idle" href="#quick"><h3>思路快筛</h3>'
        '<p class="sub">TRADING-2569 S3 · 财报前抢跑等假设</p><p class="big">—</p>'
        '<p class="st">J4 上线后启用</p></a>'
        '<a class="node idle" href="#judgment"><h3>判断对账</h3>'
        '<p class="sub">TRADING-2569 S1/S2 · 人工判断按市场结算</p><p class="big">—</p>'
        '<p class="st">J3 上线后启用</p></a></div>'
        '<div class="arrow" aria-hidden="true"><span>候选 / 有希望</span>'
        '<span class="ar">→</span></div>'
        '<div class="col core"><a class="node live" href="#questions"><h3>研究问题</h3>'
        '<p class="sub">预注册 → 开发期 → 验证期（只读一次）→ 结论</p>'
        f'<p class="big">{_e(summary["active"])} <small>进行中</small> · '
        f"{_e(summary['closed'])} <small>已结题</small></p>"
        f'<p class="st">累计试验 {_e(summary["global_trials"])} · '
        f"验证期读取 {_e(summary['validation_reads'])} 次 · 记录链{chain}</p></a></div>"
        '<div class="arrow" aria-hidden="true"><span>结论</span><span class="ar">→</span></div>'
        '<div class="col out"><a class="node live" href="#board"><h3>结论板</h3>'
        f'<p class="sub">{len(ledger["rows"])} 条研究线</p><p class="big"></p><p class="st"></p>'
        '<p class="chips">'
        + " ".join(f"{_badge(word)} {_e(ledger['counts'][word])}" for word in WORDS)
        + '</p></a><a class="node live" href="#negative"><h3>负结果清单</h3>'
        '<p class="sub">新想法开题前必查</p>'
        f'<p class="big">{len(model["negative_sections"])} <small>类</small></p>'
        '<p class="st">只由你审后更新</p></a></div></div></section>'
    )


def _now(model: Mapping[str, Any]) -> str:
    funnel, records = model["funnel"], model["forward_records"]
    ledger, charter = model["forward_ledger"], model["charter"]
    conclusion = " · ".join(f"{word} {model['ledger']['counts'][word]}" for word in WORDS)
    conclusion += (
        f"（按子问题计）；筛选 {funnel['screened']} 个信号 → 候选 {funnel['candidates']}"
        f" → 试跑 {funnel['trials']} → 过门 {funnel['passed']}"
        f" → 开题 {funnel['questions']} → 保留 {funnel['kept']}。"
    )
    forward = "还没有前瞻记录。"
    if records["present"] and records["count"]:
        forward = f"前瞻记录 {records['count']} 条，待核 {records['pending']} 条；"
        if ledger["days"] is not None:
            forward += f"账本 {ledger['days']} 个交易日（{ledger['start']} 到 {ledger['end']}）；"
        forward += "下一次核对要等 QQQ 划出一个完整的慢熊段或长牛段。"
    term = charter["term"] or ""
    if "。" in term:
        term = term.split("。", 1)[0] + "。"
    period = (
        f"第 {charter['period']} 期章程：{term}{charter['direction'] or ''}"
        if charter["present"]
        else "还没有章程。"
    )
    return (
        '<section id="now"><h2>现状</h2>'
        + "".join(
            "<p><strong>" + label + "：</strong>" + _e(text) + "</p>"
            for label, text in (("结论", conclusion), ("前瞻", forward), ("这期", period))
        )
        + "</section>"
    )


def _account(ledger: Mapping[str, Any]) -> str:
    out = [
        '<section id="account"><h2>保险账（前瞻账本）</h2><p class="lead">'
        "每个交易日记三条线的敞口和收益，和同平均敞口的固定敞口比，每周五追加；"
        "只描述，不判，核对要等完整的慢熊段或长牛段。</p>"
    ]
    if not ledger["present"]:
        out.append("<p>还没有前瞻账本：research/_forward/weekly.md</p>")
    else:
        out.append(markdown_to_html(ledger["lead"]))
        for index, section in enumerate(ledger["sections"]):
            if index < 2:
                out.append("<h3>" + _e(section["title"]) + "</h3>")
            else:
                out.append(
                    '<details class="grp"><summary><strong>'
                    + _e(section["title"])
                    + "</strong></summary>"
                )
            out.append(markdown_to_html(section["markdown"]))
            if index >= 2:
                out.append("</details>")
    return "".join(out) + "</section>"


def _forward(records: Mapping[str, Any]) -> str:
    out = [
        '<section id="forward"><h2>前瞻记录</h2><p class="lead">'
        "写在核对之前、写下以后不改的预期；到期核对时在「核对」列追加结果。"
        "不计数、不进验证、不进组合。</p>"
    ]
    if not records["present"]:
        out.append("<p>还没有前瞻记录：research/_loop/forward_records.md</p>")
    else:
        out.append(f"<p>共 {_e(records['count'])} 条，待核 {_e(records['pending'])} 条。</p>")
        for section in records["sections"]:
            out.append("<h3>" + _e(section["title"]) + "</h3>")
            out.extend(markdown_to_html(table) for table in section["tables"])
            if section["prose"]:
                out.append(
                    '<details class="grp"><summary>说明</summary>'
                    + markdown_to_html(section["prose"])
                    + "</details>"
                )
    return "".join(out) + "</section>"


def _charter(charter: Mapping[str, Any], source: Mapping[str, Any]) -> str:
    out = ['<section id="charter"><h2>当期题</h2>']
    if not charter["present"]:
        return "".join(out) + "<p>还没有章程：research/_loop/charter_pN.md</p></section>"
    out.append(
        f'<p class="lead">第 {_e(charter["period"])} 期章程预注册的题：'
        "规则、预期和判法都在跑之前写下，读完填实际和对错。</p>"
    )
    for question in charter["questions"]:
        out.append('<article class="card"><h3>' + _e(question["title"]) + "</h3>")
        if question["rule"]:
            out.append(
                '<p class="verdict-rule"><strong>规则：</strong>' + _e(question["rule"]) + "</p>"
            )
        if question["table"]:
            out.append(markdown_to_html(question["table"]))
        out.append(
            "<details><summary>全文（预期、判法、失败淘汰什么）</summary>"
            + markdown_to_html(question["markdown"])
            + "</details></article>"
        )
    if not charter["questions"]:
        out.append("<p>这期章程没有预注册的题。</p>")
    if source["repo_url"] and source["commit"]:
        url = f"{source['repo_url']}/blob/{source['commit']}/{charter['path']}"
        out.append(
            f'<p><a href="{_e(url, quote=True)}">charter_p{_e(charter["period"])}.md</a></p>'
        )
    return "".join(out) + "</section>"


def _families(families: Mapping[str, Any]) -> str:
    out = [
        '<section id="families"><h2>家族台账</h2><p class="lead">'
        "按机制家族累计失败：家族价和状态按台账里写下的规则机械算出，"
        "关闭的家族再开题必须写新机制。</p>"
    ]
    if not families["present"]:
        out.append("<p>还没有家族台账：research/families.md</p>")
    else:
        if families["counter"]:
            first, stop, rest = families["counter"].partition("。")
            out.append("<p>" + inline(first + stop) + "</p>")
            if rest.strip():
                out.append(
                    '<details class="grp"><summary>横读怎么计</summary><p>'
                    + inline(rest.strip())
                    + "</p></details>"
                )
        if families["rows"]:
            out.append(
                "<p>"
                + " · ".join(
                    f"{status} {_e(families['status_counts'][status])}"
                    for status in ("开放", "停车", "关闭")
                )
                + "</p>"
            )
            out.append(
                _table(
                    (
                        "家族",
                        "状态",
                        "家族价",
                        "已试变体（筛选 / 正式）",
                        "最好的验证结果",
                        "标签分布",
                        "线",
                    ),
                    [
                        [
                            inline(row[key])
                            for key in (
                                "family",
                                "status",
                                "price",
                                "variants",
                                "best",
                                "tags",
                                "line",
                            )
                        ]
                        for row in families["rows"]
                    ],
                )
            )
            for row in families["rows"]:
                if row["reopen"] not in ("", "—"):
                    out.append(
                        '<details class="grp"><summary>重开条件：'
                        + _e(row["family"])
                        + "（"
                        + _e(row["status"])
                        + "）</summary><p>"
                        + inline(row["reopen"])
                        + "</p></details>"
                    )
        if families["notes"]:
            out.append(
                '<details class="grp"><summary>家族价和状态怎么算</summary>'
                + markdown_to_html(families["notes"])
                + "</details>"
            )
    return "".join(out) + "</section>"


def _funnel(funnel: Mapping[str, Any]) -> str:
    cells = [
        f'<a href="#{target}"><span class="big">{_e(funnel[key])}</span>'
        f'<span class="m">{label}</span></a>'
        for target, key, label in (
            ("screens", "screened", "已筛信号"),
            ("screens", "candidates", "候选"),
            ("trials", "trials", "试跑"),
            ("trials", "passed", "过门"),
            ("questions", "questions", "开题"),
            ("board", "kept", "保留"),
        )
    ]
    return (
        '<section id="funnel"><h2>漏斗</h2>'
        '<p class="lead">从筛选到可信的结论，每一格都读自仓库里的记录。</p>'
        '<div class="funnel">' + '<span class="fa">→</span>'.join(cells) + "</div>"
        f'<p class="m">另一条线：前瞻记录 {_e(funnel["forward"])} 条，'
        "不计数、只核对，见「前瞻记录」。</p></section>"
    )


def _fate(fate: Mapping[str, Any], model: Mapping[str, Any]) -> str:
    plans = {plan["trial_id"] for plan in model["trials"]["plans"]}
    questions = {q["question_id"] for q in model["questions"]}

    def link(value: str, prefix: str, targets: set[str]) -> str:
        return (
            f'<a href="#{prefix}{_e(value, quote=True)}">{_e(value)}</a>'
            if value in targets
            else _e(value)
        )

    parts = [
        "试跑 " + link(p["trial_id"], "trial-", plans) + "：" + _e(p["verdict_name"])
        for p in fate["trials"]
    ]
    parts.extend(
        "转 " + link(q["question_id"], "q-", questions) + "（" + _e(q["state"]) + "）"
        for q in fate["questions"]
    )
    return "；".join(parts) if parts else _e(fate["text"])


def _questions(questions: Sequence[Mapping[str, Any]]) -> str:
    out = [
        '<section id="questions"><h2>研究问题</h2><p class="lead">'
        "每张题卡先看一句话问题和「怎么判」，再看策略怎么买卖、通过门槛和验证期结果。</p>"
    ]
    out.extend(
        _card(question)
        for question in sorted(questions, key=lambda q: (q["status"] == "closed", q["question_id"]))
    )
    if not questions:
        out.append("<p>还没有研究题。</p>")
    return "".join(out) + "</section>"


def _negative(model: Mapping[str, Any]) -> str:
    out = [
        '<section id="negative"><h2>负结果清单</h2><p class="lead">'
        "每个研究 brief 和每个新想法开题之前必读。只由你审过之后更新，agent 只能提议。</p>"
    ]
    if model["negative_sections"]:
        for section in model["negative_sections"]:
            out.append(
                '<details class="grp"><summary><strong>'
                + _e(section["title"])
                + "</strong></summary>"
                + markdown_to_html(section["markdown"])
                + "</details>"
            )
    elif model["negative_results"] is not None:
        out.append(markdown_to_html(model["negative_results"]))
    else:
        out.append("<p>没有找到 research/negative_results.md。</p>")
    return "".join(out) + "</section>"


def render(model: Mapping[str, Any]) -> str:
    """Render collected facts, escaping all repository text."""
    source, summary = model["source"], model["summary"]
    meta = "不在 git 仓库里"
    if source["commit"]:
        when = source["commit_time"] or ""
        meta = (
            f"数据截至 main <code>{_e(source['commit'][:9])}</code> · "
            f"{_e(when[:10])} {_e(when[11:16])} {_e(when[19:])}"
        )
    chain = "完整" if summary["chain_ok"] else "有问题"
    trial_count = _e(summary["global_trials"])
    if model.get("trials", {}).get("present"):
        trial_count += (
            f"（题目 {_e(summary['question_trials'])} + 试跑 {_e(summary['trial_configs'])}）"
        )
    out = [
        '<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>研究进展</title><style>{CSS}</style></head><body><main>",
        '<header class="top"><h1>研究进展</h1>',
        f'<p class="meta">{meta}</p>',
        f'<p class="sum">进行中 {_e(summary["active"])} · 已结题 {_e(summary["closed"])} · '
        f"累计试验 {trial_count} · 失败运行 {_e(summary['failed_runs'])} · "
        f"验证期读取 {_e(summary['validation_reads'])} · 记录链{chain}</p>",
        '<p class="legend"><span class="m">结论只有四个词：</span>'
        + "".join(_badge(word) for word in WORDS)
        + '</p><p class="guide">怎么读：每道研究题先把买卖规则和通过门槛写好，再去检验。'
        "下面每张题卡写明策略怎么买、怎么卖、多久调一次仓，以及通过门槛和实测；"
        "信号为什么被淘汰见「信号筛选」；专业名词见页尾「名词解释」。</p>"
        '<nav class="nav">'
        + "".join(
            f'<a href="#{target}">{label}</a>'
            for target, label in (
                ("now", "现状"),
                ("account", "保险账"),
                ("forward", "前瞻记录"),
                ("charter", "当期题"),
                ("families", "家族台账"),
                ("funnel", "漏斗"),
                ("screens", "信号筛选"),
                ("trials", "试跑"),
                ("questions", "研究问题"),
                ("board", "结论板"),
                ("route", "研究路线"),
                ("glossary", "名词解释"),
            )
        )
        + "</nav></header>",
    ]
    if model["problems"]:
        out.append('<section id="problems"><h2>记录问题</h2><ul class="plain">')
        out.extend(f"<li>{_e(problem)}</li>" for problem in model["problems"])
        out.append("</ul></section>")
    out.extend(
        (
            _now(model),
            _account(model["forward_ledger"]),
            _forward(model["forward_records"]),
            _charter(model["charter"], source),
            _families(model["families"]),
            _funnel(model["funnel"]),
            '<section id="screens"><h2>信号筛选</h2>',
        )
    )
    screens = model["screens"]
    if not screens["present"]:
        out.append('<p class="m">S4b 上线后启用</p>')
    else:
        counts = screens["counts"]
        out.append(
            f"<p>已筛 {_e(screens['family_size'])} 个信号"
            f"（共 {_e(screens['records'])} 条记录）："
            f"淘汰 {_e(counts['eliminated'])} · 证据不足 {_e(counts['inconclusive'])} · "
            f"候选 {_e(counts['candidate'])}</p>"
        )
        if model.get("screen_criteria"):
            out.append(
                '<details class="grp"><summary><strong>筛选标准</strong></summary>'
                + _criteria(model["screen_criteria"], model.get("screen_details", {}))
                + "</details>"
            )
        badges = {
            "candidate": ("cand", "候选"),
            "eliminated": ("out", "淘汰"),
            "inconclusive": ("unk", "证据不足"),
        }
        rows = []
        details = model.get("screen_details", {})
        members = sorted(
            screens["members"],
            key=lambda m: (
                {"candidate": 0, "inconclusive": 1, "eliminated": 2}.get(
                    details.get(str(m["number"]), {}).get("verdict_now") or m["verdict"], 3
                ),
                m["number"],
            ),
        )
        for member in members:
            detail = details.get(str(member["number"]), {})
            verdict = detail.get("verdict_now") or member["verdict"]
            kind, label = badges.get(verdict, ("low", verdict))
            signal = (
                '<span class="sigcell"><strong>'
                + _e(detail.get("name") or member["signal_id"])
                + '</strong><span class="m">'
                + _e(member["signal_id"])
                + "</span>"
            )
            if detail.get("expectation"):
                signal += '<span class="m">预期：' + _e(detail["expectation"]) + "</span>"
            signal += "</span>"
            reasons = (
                '<ul class="reasons">'
                + "".join(
                    f'<li class="{_e(reason["kind"], quote=True)}"><span class="mk">'
                    + ("✓" if reason["kind"] == "ok" else "✗")
                    + "</span>"
                    + _e(reason["text"])
                    + "</li>"
                    for reason in detail.get("reasons", [])
                )
                + "</ul>"
            )
            rows.append(
                (
                    signal,
                    f'<span class="b {kind}">{_e(label)}</span>',
                    _fate(model["screen_fates"][str(member["number"])], model),
                    _number(member["primary_p_value"], "p"),
                    _e(member["records"]),
                    reasons,
                )
            )
        out.append(_table(("信号", "裁决", "去向", "主市场 p（未校正）", "记录条数", "依据"), rows))
    out.extend(
        (
            "</section>" + _trials(model.get("trials", {})),
            _questions(model["questions"]),
            _board(model["ledger"]),
            _negative(model),
            _route(model),
            '<section id="quick" class="sec-idle"><h2>思路快筛</h2>'
            '<p class="m">TRADING-2569 J4 上线后启用</p></section>',
            '<section id="judgment" class="sec-idle"><h2>判断对账</h2>'
            '<p class="m">TRADING-2569 J3 上线后启用</p></section>',
            '<section id="work"><h2>研究工程进度</h2><ul class="plain">',
        )
    )
    for task in model["tasks"]:
        next_step = task["next_step"] or ""
        first = next_step.split("。", 1)[0]
        first = first[:96] + "…" if len(first) > 96 else first
        status = task["status"] or ""
        out.append(
            f"<li><strong>{_e(task['id'].split('_', 1)[0])}</strong> "
            f'<span class="m">{_e(task["area"])}</span> '
            f'<span class="b st-{_e(status.lower(), quote=True)}">{_e(task["status"])}</span>'
            f"<details><summary>{_e(first or None)}</summary>"
            f"<p>{_e(next_step or None)}</p></details></li>"
        )
    out.append('</ul><details><summary>最近动态</summary><ul class="plain">')
    for activity in model["activity"]:
        text = f"{_e(activity['time'])} {_e(activity['commit'][:9])} {_e(activity['subject'])}"
        if source["repo_url"] and activity["commit"]:
            url = source["repo_url"] + "/commit/" + activity["commit"]
            text = f'<a href="{_e(url, quote=True)}">{text}</a>'
        out.append(f"<li>{text}</li>")
    out.append(
        "</ul></details></section>" + _glossary(model.get("bench")) + "\n</main></body></html>\n"
    )
    return "\n".join(out)
