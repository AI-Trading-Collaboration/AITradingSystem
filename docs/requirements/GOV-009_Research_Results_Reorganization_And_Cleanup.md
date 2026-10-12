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
- W2：删 139 个模块（方案估的 146 个里，6 个没有命令用到但不属于研究的模块不在本批：`error_taxonomy`、`features/technical`、`legacy/reporting_catalog_adapter`、`market_calendar_freshness`、`platform/artifacts/validation_session`、`etf_portfolio/regime`，最后一个随 W4 的 `config/etf_portfolio/` 处理；`research_quality/growth_tilt_candidate_overlay_executor.py` 还被 W3 的 growth tilt 模块按文件路径读取，和它的测试一起挪到 W3）、131 个测试、5 个脚本（`capture_hold.py`、`run_named_data_quality.py`、3 个 `trading2464_o1_*`）、5 个 `config/data_governance/named_*_sources*.json`。`AGENTS.md`、research-run skill、运行手册第 7 节同批修改。
- W3：`cli.py` 去掉 `research` 命令组；删 242 个模块（含 13 个 research CLI 模块）、`scripts/generate_qqq_plus_growth_closeout_artifacts.py`、`scripts/generate_simple_baseline_real_run_decision_pack.py`、193 个测试文件、18 个测试数据文件；8 个生产侧测试只去掉用到已删代码的部分（`test_simple_baseline_portfolio_control.py` 留下模拟器本身的测试）。`aits` 启动加载的模块从 592 个降到 350 个。
- W4：删 2461 个研究记录文件（docs/research 1166、研究类需求文档 602、docs/tasks/completed 83、config/research 230、inputs 研究目录 344、入库的研究输出 19、data/research_campaigns 13、一次性脚本 3、research_method_contract.md 1）；`config/report_registry.yaml` 去掉 974 条命令已删且没有文件再引用的条目；删 22 个只检查这些记录的研究测试，3 个生产侧测试只去掉读已删文件的部分。`test_arch_004f3_reporting_architecture.py` 的登记条目数改为 398。保留 179 个仍被代码或配置读取、被保留的测试实际打开、或被保留的报告条目匹配的文件。`config/governance/risk_tiered_external_action_evidence_admission_v1.yaml`（DEVX-008）引用的 QQQ 期权证据只留它的测试读取的 5 个，其余引用指向 tag 下的内容。`config/etf_portfolio/`、`etf_portfolio/regime.py`、`registry/development_tasks/2f/` 在 `.pi` 守卫的 NEVER_WRITE 名单里，要先改守卫再删。
- W5（C 区，改 `.pi` 守卫）：守卫的 NEVER_WRITE 去掉 `registry/development_tasks/**` 和 `src/ai_trading_system/etf_portfolio/regime.py`（固定它们字节的 TRADING-2548、QQQ 期权合同已在 W4 删除），`config/etf_portfolio/**` 收窄为 score-daily 读取的 `dynamic_allocation_policy.yaml`、`trend_calibration.yaml`，仍存在的文件受到的保护不变。删 `config/etf_portfolio/` 94 个文件、`etf_portfolio/regime.py` 和 README、`registry/development_tasks/` 2 个文件、`registry/etf_portfolio/` 1 个没人读的文件；`config/config_contract_registry.yaml` 去掉 4 条已删配置的登记；`system_flow.md` 第 6 节同步。`config/research/threshold_registry.yaml` 的 `where_used` 说明字段里仍有指向已删文件的路径，没有代码检查它，没改。
- 方案里 W4 的 `system_flow.md`、`artifact_catalog.md` 历史段落没删：这些段落里的文字仍有测试断言（如 `test_current_subscription_source_qualification.py`），要删得先改测试，留到需要时再做。DEVX-008 政策引用的 QQQ 期权证据等政策复审时再定。GOV-009 到此完成。
