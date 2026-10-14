"""Static Chinese research progress page, with no external resources."""

from __future__ import annotations

import html
from collections.abc import Mapping, Sequence
from typing import Any

from .collect import WORDS
from .markdown import markdown_to_html

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
/* tables */
.wrap{overflow-x:auto;margin-top:8px}
table{border-collapse:collapse;width:100%;font-size:14px}
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


def _card(question: Mapping[str, Any]) -> str:
    title = question["question"]
    short = title[:80] + "…" if title and len(title) > 80 else title
    state = {"closed": "已结题", "error": "记录有问题"}.get(question["status"], "进行中")
    out = [
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
    if title and len(title) > 80:
        out.append(f"<details><summary>题目全文</summary><p>{_e(title)}</p></details>")
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
    if validation_runs:
        out.append('<div class="subs">')
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
    return "".join(out) + "</article>"


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
    out = [
        '<!doctype html>\n<html lang="zh-CN"><head><meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>研究进展</title><style>{CSS}</style></head><body><main>",
        '<header class="top"><h1>研究进展</h1>',
        f'<p class="meta">{meta}</p>',
        f'<p class="sum">进行中 {_e(summary["active"])} · 已结题 {_e(summary["closed"])} · '
        f"累计试验 {_e(summary['global_trials'])} · 失败运行 {_e(summary['failed_runs'])} · "
        f"验证期读取 {_e(summary['validation_reads'])} · 记录链{chain}</p>",
        '<p class="legend"><span class="m">结论只有四个词：</span>'
        + "".join(_badge(word) for word in WORDS)
        + "</p></header>",
    ]
    if model["problems"]:
        out.append('<section id="problems"><h2>记录问题</h2><ul class="plain">')
        out.extend(f"<li>{_e(problem)}</li>" for problem in model["problems"])
        out.append("</ul></section>")
    out.extend((_route(model), '<section id="questions"><h2>研究问题</h2>'))
    questions = sorted(
        model["questions"],
        key=lambda question: (question["status"] == "closed", question["question_id"]),
    )
    out.extend(_card(question) for question in questions)
    if not questions:
        out.append("<p>还没有研究题。</p>")
    out.extend(("</section>", _board(model["ledger"])))
    out.append(
        '<section id="negative"><h2>负结果清单</h2><p class="lead">'
        "每个研究 brief 和每个新想法开题之前必读。只由你审过之后更新，agent 只能提议。</p>"
    )
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
    out.append('</section><section id="screens"><h2>信号筛选</h2>')
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
        badges = {
            "candidate": ("cand", "候选"),
            "eliminated": ("out", "淘汰"),
            "inconclusive": ("unk", "证据不足"),
        }
        rows = []
        for member in screens["members"]:
            kind, label = badges.get(member["verdict"], ("low", member["verdict"]))
            rows.append(
                (
                    _e(member["signal_id"]),
                    f'<span class="b {kind}">{_e(label)}</span>',
                    _number(member["primary_p_value"], "p"),
                    _e(member["records"]),
                )
            )
        out.append(_table(("信号", "裁决", "主市场 p", "记录条数"), rows))
    out.extend(
        (
            "</section>",
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
    out.append("</ul></details></section>\n</main></body></html>\n")
    return "\n".join(out)
