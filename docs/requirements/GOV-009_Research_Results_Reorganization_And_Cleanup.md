# GOV-009：研究成果重整与清理

## Owner decision

- `owner_decision:GOV-009:2026-10-12:research_cleanup`
- 原话（Baker，2026-10-12 10:01 JST）：「将研究相关的结果基于我们的调研结论重新整理一遍，清理无用的内容以保证后续能在一个干净的基础上讨论推进方向」
- 2026-10-12 10:41 JST，Baker 在方案确认卡上选"按方案全做"。方案默认采用可靠性复核 D4 的建议：Composer 停；equal-risk 改作基准；简单基线只留模拟器；layer1/2 暂停；QQQ 期权归档。

## 背景

2026-10-12 的研究结论可靠性复核结论：2026-05 到 09 约 860 个研究任务里，没有一条正面结论达到 A 或 B 级；很多"已关闭"的方向其实没测成。仓库里的研究内容（main `ead6a629` 盘点）：

- src：研究专用 228 个模块、没有命令用到的 141 个模块，合计约 34 万行；`aits` 启动时会加载全部 research CLI；
- 测试：318 个文件只测这些代码，占非 slow 测试机时的 80%；
- `docs/research/` 1,229 个文件、研究类需求文档 608 份、`config/research/` 269 个文件、`inputs/` 研究目录 388 个文件；
- 研究任务卡开着 128 张。

这些内容会误导后来的读者（包括 pi），也拖慢门禁。

## 设计

1. 整理：新建研究入口 `research/`：
   - `README.md`；
   - `ledger.md`：22 条研究线的台账，结论只用保留、拒绝、证据不足、未测成四个词；
   - `negative_results.md`；
   - `assets.md`。
   - 研究结论以台账为准；任务卡状态只表示工程状态。
2. 留底：在 W0 落地的 commit 上打 tag `research-archive-2026-10-12` 并推送。台账里的 `@archive:<路径>` 都指向这个 tag。
3. 清理：不保留的研究代码、文档、配置、测试整条删除，分批进行。每批一次 ship，删除脚本和期望 tree 由 Claude 准备，pi 执行并跑门禁，Claude 核验。

### 分批

| 批 | 内容 | 区 |
|---|---|---|
| W0 | `research/` 四个文件；本需求文档；根 README 加入口；`docs/research_method_contract.md` 标作废；打 tag | A |
| W1 | `forward_evidence` 删 4 个不在调度里、依赖 `controlled_strategy_batch` 的命令；`contracts/__init__.py` 去掉 10 个 `strategy_research_*` 再导出并删这 10 个模块；关闭 101 张研究任务卡；新建任务卡 GOV-009 | B |
| W2 | 删没有命令用到的模块（QQQ 期权、growth action value、Composer v2 证伪、前瞻采集和 named DQ 子进程等）及其测试、脚本、`config/data_governance/named_*_sources*.json`；改 `AGENTS.md` 和 research-run skill | C |
| W3 | 删只有 `aits research` 用的模块和 13 个 research CLI 模块及其测试；`cli.py` 在同一次改动里去掉 `research` 命令组 | B |
| W4 | 删 `docs/research/`、研究类需求文档、`docs/tasks/completed/`、不再被读取的 `config/research/` 文件、`config/etf_portfolio/` 等哈希固定证据、`inputs/` 研究目录、入库的研究输出、`data/research_campaigns/`；清理 `config/report_registry.yaml`；`system_flow.md`、`artifact_catalog.md` 里已没人检查的历史部分 | A + B |

### 保留

- `simple_baseline_portfolio_control.py`：信号滞后正确的模拟器，评估台（TRADING-2565）的核心。
- `indicator_research.py`：score-daily 写每日指标权重轨迹用。
- `reports/research_governance_summary.py`：汇总的是生产侧内容，日报步骤照常。
- `config/research/` 里日报和其他生产代码读的文件、C 区的窗口政策/窗口登记/晋级门槛、`transaction_cost_model.yaml`。
- 任务卡 TRADING-2565、2566；生产侧的 RISK、SCORE、SHADOW、CALIBRATION、BACKTEST、THESIS 卡（留到第一个研究问题一起处理）。
- `tasks/*.yaml` 全部保留，只改状态。

## 不改的

- 日报、调度、DQ、评分、交易引擎的代码和配置。
- 运行副本、`data/raw`、未入库的 `outputs/`。

## 验收

- 每批：门禁通过（W0 是 A 区，无门禁）；31 个日报步骤的 `cli_direct` 入口逐个 import 成功；`aits ops daily-plan` 空跑通过。
- W3 之后：`aits --help` 正常，没有 `research` 命令组。
- W4 之后：`aits reports index` 空跑没有新增 MISSING。
- tag `research-archive-2026-10-12` 在 origin 上可见，台账里每个 `@archive:` 路径在 tag 下存在。

## 进展

- 2026-10-12：方案 `research-cleanup-plan-2026-10-12`（项目文件）经 Baker 确认。W0 由 Claude 写好，等 DATA-022 之后交给 pi 发布。production_effect=none；broker_action=none。
