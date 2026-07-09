# TRADING-2442 Daily Incremental Refactor: Growth Tilt PIT Replay CLI Boundary

最后更新：2026-07-10

## 状态

- 任务 ID：`TRADING-2442_DAILY_INCREMENTAL_REFACTOR_GROWTH_TILT_PIT_REPLAY_CLI_BOUNDARY`
- 优先级：P2
- 状态：DONE
- 下一责任方：项目 owner 按需复核 growth tilt PIT replay CLI 模块边界

## 背景

每日增量重构巡检以 `e6028e05dd0c2aa62d5ff0a8de96f08e08184bb9` 为最近一次合格重构基线，评估 `e6028e05..HEAD` 后发现 TRADING-2438A-L 新增 growth tilt PIT replay、blocker closure、persistent blocker escalation 和 runtime remediation 命令族。昨日已拆出的 `src/ai_trading_system/cli_commands/research_execution_growth_tilt.py` 因新增命令重新膨胀到 6000 行以上，并同时承载 TRADING-2406 至 TRADING-2440 的 engine readiness、paper-shadow gate 和 PIT replay adapters。

TRADING-2438A-L command family 具有清晰边界：它们都只是 Typer adapter，委托同名 `dynamic_strategy_growth_tilt_*pit_replay*` / blocker remediation modules 执行，保持 research-only、no-effect production boundary，并通过既有 data-quality gate 或 prior artifact validation 维持原行为。将该 family 拆出独立 CLI adapter module 可以降低后续导入冲突、注册遗漏和 code review 成本。

## 目标

- 新增独立 growth tilt PIT replay CLI adapter module，承载 TRADING-2438A-L 相关 command registration 和 adapter functions。
- 保持外部 `aits research strategies ...` 命令名、参数、默认路径、输出打印、`typer.BadParameter` 转换、FAIL 退出语义、artifact path 和 report schema 不变。
- 让 `research_execution_growth_tilt.py` 继续作为 growth tilt execution command 的聚合入口，并委托 PIT replay submodule 注册该 command family。

## 非目标

- 不改变底层 research builder、candidate replay judgment、data-quality gate、cached market/macro data validation、report output contract、status enum、next route、threshold、score band、promotion gate、position cap、backtest behavior 或 market-regime interpretation。
- 不运行 broker、order、trading action，不写 production weights、active shadow weights、paper-shadow runtime state、candidate tracking state 或 portfolio mutation。
- 不解除 TRADING-2438M blocker，不补造 runtime metric values，不把 BLOCKED candidate 改判为 PASS、FAIL、NO_PASSING_CANDIDATE 或 forward-aging ready。

## 实施步骤

1. 新增 `src/ai_trading_system/cli_commands/research_execution_growth_tilt_pit_replay.py`，迁移 TRADING-2438A-L imports、command registration 和 command adapter functions。
2. 在 `research_execution_growth_tilt.py` 保留 `register_growth_tilt_execution_strategy_commands(strategies_app)`，并调用新模块的 PIT replay registration function。
3. 更新 `docs/system_flow.md` 以记录 growth tilt PIT replay CLI adapter ownership 边界变化；外部 CLI path 和数据流保持兼容。
4. 更新 `docs/refactor_log.md`、`docs/task_register.md` 和 `docs/task_register_completed.md`，记录重构理由、行为影响、验证结果和不运行 `aits validate-data` 的原因。

## 验收标准

- TRADING-2438A-L 迁移后的 CLI help smoke 仍从原 `aits research strategies ...` 路径解析。
- `research_execution_growth_tilt.py` 不再直接导入 TRADING-2438A-L PIT replay / blocker remediation modules，也不直接承载这些 command adapter functions。
- Ruff、compileall、focused parallel pytest、docs freshness、documentation/task-register consistency 和 `git diff --check` 通过。
- 本轮只做 CLI adapter 边界拆分，不生成 cached-data-dependent features、scoring、backtest 或 daily report 输出；因此不运行 `aits validate-data`，但最终重构日志必须披露该不适用原因。

## 进展记录

- 2026-07-10：创建任务和需求文档，准备实施 low-risk CLI adapter boundary split。
- 2026-07-10：实现完成并归档 DONE。新增 `research_execution_growth_tilt_pit_replay.py`，将 TRADING-2438 至 TRADING-2438L PIT replay / blocker closure / persistent blocker escalation / runtime remediation command adapters 从 `research_execution_growth_tilt.py` 拆出；`research_execution_growth_tilt.py` 保留 growth tilt 聚合入口并委托新模块注册。已同步更新 `docs/system_flow.md` 和 `docs/refactor_log.md`；Ruff、compileall、focused parallel pytest、CLI help smoke、docs freshness、documentation/task-register consistency、contract-validation 和 `git diff --check` 作为最终验证。
