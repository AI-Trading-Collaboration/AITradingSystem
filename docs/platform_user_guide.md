# AITradingSystem 平台用户指南

最后更新：2026-06-19

本文是 TRADING-487_to_504 工程收尾批次的稳定入口说明，面向不想先阅读
task register 的新用户。当前系统状态是工程控制面 ready、研究候选回到 backlog；
Stage C clean-clone release acceptance runner 已建立，但提交后的 clean worktree PASS
尚未完成，不能声明平台冻结完成，也不能宣称 release-ready。

## 三条使用路径

| 角色 | 第一入口 | 主要问题 | 常用命令或文档 |
|---|---|---|---|
| Researcher | `aits system status --as-of YYYY-MM-DD` | 当前候选是否值得继续研究，证据弱在哪里 | `aits reports latest --report-id next_candidate_research_cycle_snapshot`、`docs/research/weight_research_turn_2026-06-19.md`、`docs/system_flow.md` |
| Operator | `aits system doctor --as-of YYYY-MM-DD` | 环境、数据、registry、validation 是否阻断运行 | `aits validate-data`、`aits reports index --as-of YYYY-MM-DD`、`python scripts/run_validation_tier.py --list`、`docs/operations/operations_runbook.md` |
| Owner / Reviewer | `aits system status --as-of YYYY-MM-DD` | 当前结论、blocker、warning、安全边界和下一步 owner action | Reader Brief、`docs/artifact_catalog.md`、`docs/requirements/TRADING-487_to_504_Engineering_Closeout_and_Weight_Research_Turn.md` |

三条路径共享同一原则：先看 canonical status / doctor，再下钻到 source artifact。
不要从 task ID 猜当前状态，也不要把某个 validation PASS 解读为候选研究成功。

## 15 分钟 Quickstart

以下流程只做本地只读检查和最小可理解性验证，不刷新市场数据，不触发 broker/order，
不生成 official target weights。

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,data,dashboard,brokers]"
aits system status --as-of 2026-06-19
aits system doctor --as-of 2026-06-19
aits reports index --as-of 2026-06-19
python scripts/run_validation_tier.py fast-unit --write-runtime-artifact
python scripts/run_validation_tier.py reproducibility --write-runtime-artifact
python scripts/run_clean_clone_release_acceptance.py --as-of 2026-06-19
aits reports engineering-closeout-release-candidate --as-of 2026-06-19
aits reports validate-engineering-closeout-release-candidate --latest
```

如果要运行任何依赖 cached market 或 macro data 的 scoring、feature、backtest、
daily report 或 research workflow，必须先运行：

```powershell
aits validate-data
```

`aits validate-data` 未通过时不要继续解释投资结论。CI 不能验证本地未提交 cache；
本地数据相关命令仍必须执行同一质量门禁。

## 系统架构图

事实源是 `docs/system_flow.md`。高层数据流是：

```text
数据输入 -> 数据质量门禁 -> 特征/信号 -> 候选研究/回测 -> 治理 gate -> 报告/Reader Brief -> owner decision
```

关键边界：

- `aits system status` 是当前状态第一屏，不替代底层 source artifact。
- `aits system doctor` 是工程和操作诊断，不批准 paper-shadow 或 live trading。
- `aits reports latest` 查询 registry latest artifact，不运行上游。
- `python scripts/run_validation_tier.py reproducibility` 验证 artifact lineage、run manifest
  和 runtime summary 复现契约，不替代数据质量门禁。
- `python scripts/run_clean_clone_release_acceptance.py` 只在 clean worktree 上能给出
  clean-clone PASS；dirty snapshot smoke 必须保持 BLOCKED，不能作为 release signoff。
- `aits reports engineering-closeout-release-candidate` 聚合 release tag/changelog、
  stable CLI/schema、compatibility policy 和 closeout Reader Brief；它只读检查 evidence，
  不创建 git tag，也不运行上游。
- 研究默认市场阶段是 `ai_after_chatgpt`，锚点为 2022-11-30，默认回测起点为
  2022-12-01；使用更早历史时必须说明为何相关。

## Artifact 生命周期

报告和研究产物默认不可用文件名猜 latest。优先通过 report index 或 `aits reports latest`
查询 source artifact。

| 状态 | 含义 | 默认阅读处理 |
|---|---|---|
| `CURRENT` | 当前 registry 认为最新且可展示 | 可作为 first drilldown |
| `SUPERSEDED` | 已被更新产物替代 | 保留追溯，不作为默认结论 |
| `ARCHIVED` | 进入归档或长期保留 | 仅按 lineage / audit 读取 |
| `INVALID` | 结构、schema、production boundary 或 validation 不满足 | 不得用于结论 |
| `LEGACY` | 历史格式或旧流程产物 | 必须带限制说明 |

历史研究证据不得为“清 warning”而物理删除。Archive 不得破坏复现所需的 checksum、
input artifact id、command 或 manifest。

## 状态枚举词典

| 状态族 | 典型值 | 解释 |
|---|---|---|
| 工程控制面 | `ENGINEERING_CONTROL_PLANE_READY` / `ENGINEERING_CONTROL_PLANE_READY_WITH_LIMITATIONS` | 第一屏可读；带 limitations 时仍有已披露工程限制 |
| Stage B readiness | `ENGINEERING_STAGE_B_READY` | schema/config/manifest/test/error taxonomy contract 已通过 readiness validation |
| 工程收尾最终态 | `ENGINEERING_CLOSEOUT_READY` / `ENGINEERING_CLOSEOUT_READY_WITH_DOCUMENTED_LIMITATIONS` / `ENGINEERING_CLOSEOUT_BLOCKED` | 平台冻结 gate 的最终判断；当前不得宣称 ready |
| 研究 gate | `V2_RETURN_TO_HYPOTHESIS_BACKLOG` | 候选证据不足，回到假设 backlog |
| 数据门禁 | `PASS` / `PASS_WITH_WARNINGS` / `FAIL` | 只说明数据或检查本身状态，不说明候选有效 |
| Reader Brief | `OK` / `PASS_WITH_WARNINGS` / `LIMITED_READER_CONTEXT` / `FAILED` | 阅读体验和上下文质量，不是投资结论 |

## Reader Brief 标准结构

后续 Reader Brief section 应尽量统一为：

```text
Decision
Why
Evidence quality
Positive evidence
Negative evidence
Blocking issues
Limitations
Safety boundary
Next action
Source artifacts
```

第一屏必须给出结论和下一步。重复 validation 列表应合并为来源链接；工程验证通过和
研究证据有效必须分开写。validation PASS 不得写成 candidate PROMISING、
paper-shadow eligible 或 official target weights ready；带反引号的机器状态也应遵守同一解释边界。
`aits reports reader-brief-consistency` 检查的是 effective Reader Brief view model：
非 daily artifact 可由 report index metadata 补齐标准字段，源 artifact 未原生暴露字段时会记录
`native_template_gap`，作为模板迁移审计项，而不是默认阅读失败。

## Troubleshooting Decision Tree

```text
命令失败
  -> 是否依赖 cached market/macro data?
       -> 是：先运行 aits validate-data；失败则修复数据源或 cache，不继续下游。
       -> 否：继续。
  -> 是否 report index unwaived > 0 或 expired waiver > 0?
       -> 是：运行 aits reports index --as-of YYYY-MM-DD 并查看 source report。
       -> 否：继续。
  -> 是否 report index 里有 LEGACY_* / DEPRECATED_* freshness_status?
       -> 是：查看 report registry visibility_policy；它是历史/可选导航面，不是当前证据。
       -> 否：继续。
  -> 是否缺 latest source artifact?
       -> 是：用 aits reports latest --report-id REPORT_ID --as-of YYYY-MM-DD 定位，
              不手工猜文件名。
       -> 否：继续。
  -> 是否 validation tier 失败?
       -> 是：查看 outputs/validation_runtime/<run_id>/test_runtime_summary.json；
              PRINT_ONLY 不能当 passing evidence。
       -> 否：继续。
  -> 是否 clean-clone release acceptance 为 BLOCKED_UNCOMMITTED_CHANGES?
       -> 是：先提交或移走当前工程收尾变更，再无 --allow-dirty-snapshot 重跑。
       -> 否：继续。
  -> 是否 research gate 为 WEAK/BLOCKED/RETURN_TO_BACKLOG?
       -> 是：这是研究结论，不能用工程 workaround 清掉。
       -> 否：查看具体 traceback 和 structured issue。
```

普通 gate blocker 应显示原因和下一步命令；如果只能看到 traceback，应登记为 Stage B
structured logging / central error taxonomy adoption 缺口。

## 从 Spec 到 Snapshot 的完整例子

TRADING-471_to_485 是当前可追溯的研究例子，不代表候选成功：

1. `candidate_v2_spec_freeze` 固定 research-only v2 spec。
2. `candidate_v2_executable_binding_update` 生成可执行绑定并披露 data quality。
3. `candidate_v2_mini_backfill` 运行 mini backfill，结果为 `V2_MINI_BACKFILL_WEAK`。
4. `candidate_v2_mini_gate` 阻止 full backfill。
5. `candidate_v2_full_backfill_if_approved` 披露 full backfill 未执行。
6. `candidate_v2_research_gate` 输出 `V2_RETURN_TO_HYPOTHESIS_BACKLOG`。
7. `candidate_v2_owner_research_review_packet` 给出 owner 复核选项。
8. `candidate_v2_research_cycle_snapshot` 记录最终快照。

这个例子的关键价值是 fail-closed：mini gate 弱时没有补造 full backfill，没有 append owner
decision，没有创建 paper-shadow/extended/live，没有 official target weights、broker/order 或
production mutation。

## 平台冻结检查

平台冻结前必须全部满足：

1. 新用户从本文三条路径进入，不需要先读 task register。
2. 标准研究流程不依赖 task ID，旧命令只作为兼容入口。
3. 系统状态只有一个 canonical source。
4. 关键 artifact 可追溯并尽可能复现。
5. CLI、schema、config 有兼容策略。
6. Reader Brief 结构统一且区分工程验证和研究证据。
7. 测试层次清晰，`reproducibility` tier 可运行。
8. clean clone 能完成最小端到端流程，且不是 dirty snapshot smoke。
9. 非研究 warning、orphan、expired waiver 清零。
10. release candidate 记录 release tag、changelog、stable CLI/schema 和后续变更准入规则。

截至 2026-06-19，当前只能作为 engineering closeout baseline 继续推进；clean-clone runner
和 release-candidate report 已存在，但最终平台冻结仍需要提交后的 clean worktree PASS 与
`ENGINEERING_CLOSEOUT_READY` release candidate。
