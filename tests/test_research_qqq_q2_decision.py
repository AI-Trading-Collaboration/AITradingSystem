"""QQQ-Q2 decision text (TRADING-2567-13): the owner decision of the second question is written into
the requirement document before its opening batch, and the task card is only appended to."""

from __future__ import annotations

import re

import yaml

from ai_trading_system.config import PROJECT_ROOT

DOC = PROJECT_ROOT / "docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md"
CARD = PROJECT_ROOT / "tasks/TRADING-2567.yaml"
REGISTER = PROJECT_ROOT / "docs/task_register.md"
DECISION = "owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg"
ANCHOR = "  - 第一题的读取 `QQQ-Q1-w0001` 是 Baker 2026-10-14 09:06 JST 在研究线程单独同意的。"
DECISION_BLOCK = "\n".join(
    (
        "- `owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg`",
        (
            "- 选择（Baker，2026-10-15 09:18 JST 在研究线程回复“同意”）：他回复的是研究线程里发出"
            "的第二题开题草稿，项目文件 `knowledge/claude/qqq-options/q2-opening/q2-opening-draft"
            "-2026-10-15.md` 和同目录 `prereg.draft.yaml`。草稿的问题是「第二题怎么开？」，说明里"
            "列了下面的三条规则、判定规则、敏感性，和 Claude 替他做的四个选择：信用利差做成门，不"
            "并进趋势平均；均线放在同一次读取里当参照，不单独算一个假设；门槛沿用 Q1a；10 个月均"
            "线用 210 个交易日。"
        ),
        "- 含义：",
        (
            "  - 第二题 `research/QQQ-Q2/`：问一条固定的经典趋势规则，和平均敞口相同、同样调仓的"
            "固定敞口比，回撤是否更浅、左尾是否更温和、几何年化是否相近；不问能不能跑赢 QQQ。"
        ),
        (
            "  - Q2a `trend4`：1、3、6、12 个月（21、63、126、252 个交易日）四个回看期，各自看复"
            "利收益是正还是负，敞口等于为正的比例（0、25%、50%、75%、100%），月底定下一个月，不加"
            "杠杆。参照 `ma10`：价格在 210 个交易日简单均线之上满仓，之下空仓；它和 Q2a 是同一族"
            "（均线和时间序列动量，Levine 和 Pedersen 2016），算一个假设，只作参照。"
        ),
        (
            "  - Q2b `credit_gate`：信用利差（FRED BAA10Y，发布滞后 1 个交易日）处在它自己历史（"
            "只用过去，扩展分位）最高的五分之一时敞口减半，否则满仓。依据是第一批筛选里唯一的候选"
            " S0002 `credit_spread_level`，它预测的是之后 21 日的波动，不是收益。“趋势加信用门”的"
            "组合不在本题。"
        ),
        (
            "  - 数据：快照 `qlh-20261013-0b0df53cb1a2`，真实 QQQ；信号快照 `sig-1e9873757a84`。"
            "趋势信号 `tsmom_21`、`tsmom_63`、`tsmom_126`、`tsmom_252` 和均线信号 `ma_distance_21"
            "0` 由 TRADING-2565-11 提供，哈希钉在预注册里，改其中任何一个都要新的 owner decision"
            "。"
        ),
        (
            "  - 验证窗口上三条规则各自和平均敞口相同、同样按月调仓的固定敞口比，区间 97.5%（两个"
            "假设：趋势族、信用门）。三条的规则相同：CVaR 5% 相对改善至少 10%，区间下限大于 0；最"
            "大回撤浅至少 5 个百分点；几何年化少不超过 1.0 个百分点，25 bps 下仍成立；推迟 2 天，"
            "左尾改善仍至少 10%；剔除任何一年、任何一个固定行情段（2011 年 7–10 月、2015 年 8 月"
            "到 2016 年 2 月、2018 年 10–12 月、2020 年 2–3 月）后，左尾和回撤仍有改善；左尾改善"
            "超过 95% 的同敞口随机择时。门槛沿用 Q1a，事先定，不是看了结果调的。"
        ),
        (
            "  - 结论：全部成立是“保留”；左尾或回撤的点估计不如固定敞口是“拒绝”，只限这个构造；其"
            "余是“证据不足”。`ma10` 的结果只摆在 `trend4` 旁边，不改变 `trend4` 的结论。去风险事"
            "件表（假警报、保护性事件、每年来回次数、在固定行情段里的退出和重新进入滞后）每次运行"
            "都打印，用来读，不进规则。"
        ),
        (
            "  - 敏感性只在开发窗口跑，每个计一次试验，不参与判定：按周调仓（三条规则各一个）、去"
            "掉 1 个月回看期（`trend3_long`）、信用门的位置取最高 10% 和 30%、信用门的敞口取 25%"
            "，共 7 个。试验上限 20：10 个配置，各留一次修 bug 后重跑。"
        ),
        (
            "  - 开发窗口的结果只用来检查实现。看过以后，除记录在案的修 bug 外不改参数；预注册或"
            "策略模块一改，验证窗口要等新的 owner decision 才能读。"
        ),
        (
            "  - 这个决定同时同意之后读一次验证窗口（`owner_decision:TRADING-2567:2026-10-14:open"
            "ing_covers_validation_read`）：开发运行跑完、Claude 核对实现没有问题以后，研究线程先"
            "告诉 Baker，再单独一次运行读验证窗口，不等回复；核对发现问题就停下问 Baker。"
        ),
        (
            "  - 这份决定批准的是设计。预注册和策略模块是 C 区文件，随后另作一批，按这条决定的记"
            "录批准。它不是 paper-shadow 准入，也不是 production 或 broker 批准。"
        ),
    )
)
PROGRESS_LINE = (
    "- 2026-10-15：第二题 QQQ-Q2 的开题草稿获 Baker 同意，决定"
    " `owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg` 写在上面：Q2a 四个回看期趋势"
    "（参照 10 个月均线）、Q2b 信用利差门。信号模块由 TRADING-2565-11 提供；预注册"
    " `research/QQQ-Q2/prereg.yaml` 和策略模块等它发布后另作一批（C 区）。"
    "production_effect=none；broker_action=none。"
)
NOTES_TAIL = (
    "；2026-10-15：Baker 同意第二题 QQQ-Q2 的开题草稿（"
    "owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg）：Q2a 四个回看期趋势（参照 10 个月均线）"
    "和 Q2b 信用利差门，门槛沿用 Q1a；信号模块由 TRADING-2565-11 提供，预注册和策略模块等它发布后"
    "另作一批。production_effect=none；broker_action=none"
)
NEXT_OWNER = "Claude（第二题 QQQ-Q2：TRADING-2565-11 发布后做开题批，请 Baker 批决定记录）"
NEXT_STEP = (
    "第二题 QQQ-Q2 的设计已获 Baker 同意（owner_decision:TRADING-2567:2026-10-15:qqq_q2_prereg）；"
    "等信号模块批次 TRADING-2565-11 发布，再做开题批（预注册和策略模块，C 区，按决定记录批准）。"
    "详见 `docs/requirements/TRADING-2567_QQQ_Research_Program_1999_Window.md`"
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
    assert "opening_covers_validation_read" in before
    assert card["status"] == "IN_PROGRESS"
    assert NOTES_TAIL in REGISTER.read_text(encoding="utf-8")
