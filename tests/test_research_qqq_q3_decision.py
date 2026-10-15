"""QQQ-Q3 decision text (TRADING-2567-23): the owner decision of the third question is written into
the requirement document before its opening batch, and the task card is only appended to."""

from __future__ import annotations

import re

import yaml

from ai_trading_system.config import PROJECT_ROOT

DOC = PROJECT_ROOT / "docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
DECISION = "owner_decision:TRADING-2567:2026-10-15:qqq_q3_prereg"
ANCHOR = (
    "  - 这份决定批准的是设计。预注册和策略模块是 C 区文件，随后另作一批，按这"
    "条决定的记录批准。它不是 paper-shadow 准入，也不是 production 或 broker 批"
    "准。"
)
DECISION_BLOCK = "\n".join(
    (
        ("- `owner_decision:TRADING-2567:2026-10-15:qqq_q3_prereg`"),
        (
            "- 选择（Baker，2026-10-15 18:23 JST 在研究线程回复“同意”）：他回复的是研究"
            "线程里同一条消息请他同意的两份草稿：卖波动代理测量的记录草稿（项目文件 `kn"
            "owledge/claude/qqq-options/option-proxy-record-draft-2026-10-15.md`，记录"
            "批 TRADING-2567-22）和第三题开题草稿（项目文件 `knowledge/claude/qqq-optio"
            "ns/q3-opening/q3-opening-draft-2026-10-15.md` 和同目录 `prereg.draft.yaml`"
            "）。开题草稿的问题是「第三题怎么开？」，说明里列了下面的规则、判定规则、敏"
            "感性，和 Claude 替他做的四个选择：快信号取 21 日、慢信号取 252 日、分歧时"
            "固定半仓；参照用只看慢信号的规则，不重跑 trend4；门槛沿用 Q1a 和 Q2；信号"
            "都是已经钉死的经典模块，不另做筛选。"
        ),
        ("- 含义："),
        (
            "  - 第三题 `research/QQQ-Q3/`：问快、慢两个趋势信号分歧时减半的规则，和平"
            "均敞口相同、同样调仓的固定敞口比，回撤是否更浅、左尾是否更温和、几何年化是"
            "否相近；不问能不能跑赢 QQQ。依据是 Garg、Goulding、Harvey 和 Mazzoleni 202"
            "1（Momentum Turning Points）：转折点之后的月份是传统动量亏钱的主要来源；原"
            "文按历史估计分歧时的权重，这里固定为一半，不估计。"
        ),
        (
            "  - Q3 `fast_slow`：快信号是过去 21 个交易日复利收益的符号，慢信号是过去 2"
            "52 个交易日复利收益的符号；两个都为正满仓，两个都为负空仓，分歧（上升趋势"
            "里的回调、下降趋势里的反弹）半仓；月底定下一个月，不加杠杆。参照 `slow_onl"
            "y`：只看慢信号，为正满仓，为负空仓；它只作参照，不改变 Q3 的结论，用来看快"
            "信号有没有增量。"
        ),
        (
            "  - 它是 Q2 的 `trend4` 的子集（共用 21 日和 252 日两个信号），所以它的结"
            "果不是独立于 Q2a 的证据，读结论时要写明。预期的结论是证据不足，其次是拒绝"
            "：慢信号在 2011–2021 年大部分时间为正（推断），规则近似“上个月跌了就减半”"
            "，会错过快速反弹。Q2 的 `trend4` 不再运行，对照时用 Q2 结论里已经发布的数"
            "字。"
        ),
        (
            "  - 数据：快照 `qlh-20261013-0b0df53cb1a2`，真实 QQQ；不需要信号快照。趋势"
            "信号 `tsmom_21`、`tsmom_63`、`tsmom_126`、`tsmom_252` 由 TRADING-2565-11 "
            "提供，哈希钉在预注册里，改其中任何一个都要新的 owner decision。经典信号是"
            "固定基准，不另做筛选。"
        ),
        (
            "  - 验证窗口上两条规则各自和平均敞口相同、同样按月调仓的固定敞口比，区间 9"
            "5%（一个假设：快慢分歧；参照规则同一族）。两条的规则相同，和 Q2 一样：CVaR"
            " 5% 相对改善至少 10%，区间下限大于 0；最大回撤浅至少 5 个百分点；几何年化"
            "少不超过 1.0 个百分点，25 bps 下仍成立；推迟 2 天，左尾改善仍至少 10%；剔"
            "除任何一年、任何一个固定行情段（2011 年 7–10 月、2015 年 8 月到 2016 年 2 "
            "月、2018 年 10–12 月、2020 年 2–3 月）后，左尾和回撤仍有改善；左尾改善超过"
            " 95% 的同敞口随机择时。门槛沿用 Q1a 和 Q2，事先定，不是看了结果调的。"
        ),
        (
            "  - 结论：全部成立是“保留”；左尾或回撤的点估计不如固定敞口是“拒绝”，只限这"
            "个构造；其余是“证据不足”。`slow_only` 的结果只摆在 `fast_slow` 旁边，不改"
            "变 `fast_slow` 的结论。去风险事件表每次运行都打印，用来读，不进规则。"
        ),
        (
            "  - 敏感性只在开发窗口跑，每个计一次试验，不参与判定：只用快信号（`fast_on"
            "ly`）、按周调仓（`fast_slow_weekly`、`slow_only_weekly`）、快信号改 63 日"
            "（`fast63_slow252`）、慢信号改 126 日（`fast21_slow126`），共 5 个。试验上"
            "限 14：7 个配置，各留一次修 bug 后重跑。"
        ),
        (
            "  - 开发窗口的结果只用来检查实现。看过以后，除记录在案的修 bug 外不改参数"
            "；预注册或策略模块一改，验证窗口要等新的 owner decision 才能读。"
        ),
        (
            "  - 这个决定同时同意之后读一次验证窗口（`owner_decision:TRADING-2567:2026-"
            "10-14:opening_covers_validation_read`）：开发运行跑完、Claude 核对实现没有"
            "问题以后，研究线程先告诉 Baker，再单独一次运行读验证窗口，不等回复；核对发"
            "现问题就停下问 Baker。"
        ),
        (
            "  - 这份决定批准的是设计。预注册和策略模块是 C 区文件，随后另作一批，按这"
            "条决定的记录批准，以 Baker 之后在研究线程打出的、带那份决定记录 SHA-256 的"
            "一行为准。它不是 paper-shadow 准入，也不是 production 或 broker 批准。"
        ),
    )
)
PROGRESS_LINE = (
    "- 2026-10-15：第三题 QQQ-Q3（快慢趋势信号分歧时减半，参照只看慢信号）的开"
    "题草稿获 Baker 同意，决定 `owner_decision:TRADING-2567:2026-10-15:qqq_q3_p"
    "rereg` 写在上面。信号模块已由 TRADING-2565-11 提供；预注册 `research/QQQ-Q"
    "3/prereg.yaml` 和策略模块另作一批（C 区），按这条决定的记录批准。productio"
    "n_effect=none；broker_action=none。"
)
NOTES_TAIL = (
    "；2026-10-15：Baker 同意第三题 QQQ-Q3 的开题草稿（owner_decision:TRADING-2"
    "567:2026-10-15:qqq_q3_prereg）：快（21 日）慢（252 日）趋势信号分歧时减半"
    "，参照只看慢信号，门槛沿用 Q1a 和 Q2，区间 95%；信号模块已由 TRADING-2565-"
    "11 提供，预注册和策略模块另作一批。production_effect=none；broker_action=n"
    "one"
)
NEXT_OWNER = "Claude（第三题 QQQ-Q3：做开题批，请 Baker 批决定记录）"
NEXT_STEP = (
    "第三题 QQQ-Q3 的设计已获 Baker 同意（owner_decision:TRADING-2567:2026-10-1"
    "5:qqq_q3_prereg）；下一步做开题批（预注册和策略模块，C 区，按决定记录批准"
    "，Baker 在研究线程打字批准带 SHA-256 的那一行），开题批准一并覆盖验证窗口"
    "那一次读取。详见 `docs/requirements/TRADING-2567_QQQ_Research_Program_1999"
    "_Window.md`"
)


def test_the_decision_block_is_in_the_requirement_once_after_the_previous_decision() -> None:
    text = DOC.read_text(encoding="utf-8")
    assert text.count(DECISION_BLOCK) == 1
    before = text.split(DECISION_BLOCK)[0]
    assert before.endswith(ANCHOR + "\n\n")
    after = text.split(DECISION_BLOCK)[1]
    assert after.startswith(("\n\n## 背景\n", "\n\n- `owner_decision:"))
    assert len(re.findall(rf"^- `{re.escape(DECISION)}`$", text, flags=re.MULTILINE)) == 1


def test_the_progress_gets_one_new_line_in_the_progress_section() -> None:
    text = DOC.read_text(encoding="utf-8")
    assert text.count(PROGRESS_LINE) == 1
    section = text.split("## 进展\n", 1)[1]
    assert PROGRESS_LINE in section


def test_the_task_card_notes_are_only_appended() -> None:
    card = yaml.safe_load(CARD.read_text(encoding="utf-8"))
    notes = card["notes"]
    assert notes.count(NOTES_TAIL) == 1
    before = notes.split(NOTES_TAIL)[0]
    assert before.endswith("production_effect=none；broker_action=none")
    assert "qqq_q2_prereg" in before
    assert card["status"] == "IN_PROGRESS"
    assert NOTES_TAIL in REGISTER.read_text(encoding="utf-8")
