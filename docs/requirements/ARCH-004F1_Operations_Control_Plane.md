# ARCH-004F1 Operations Control Plane

## 基本信息

- task id：`ARCH-004F1_OPERATIONS_CONTROL_PLANE`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：system operations / architecture governance
- dependency：ARCH-004E、ARCH-004F2 `DONE`；最近 full parallel `5,430 passed / 0 failed`
- production effect：`none`（daily 已由 canonical runtime control 包裹既有 executor；不新增外部 scheduler，不自动执行 non-daily）

## 为什么现在做

当前周期运维已经有三个重要基础：

1. `config/scheduled_tasks.yaml` 登记 daily / weekly / biweekly / monthly / ad hoc cadence；
2. `aits ops daily-run` 是唯一外部入口，`ops_daily.py` 保持成熟的 daily/closed-market 行为；
3. `WorkflowSpec`、`RunLedger` 已在 ARCH-004C 建立 pure contract。

但三者尚未闭合为同一个运行控制面。F1.2 将先前隐含在 `ops_daily.py` 的 closed-market-only `official_policy_sources` 正式登记后，当前盘点为 78 个登记任务，其中 daily 37、non-daily 41；daily 仍依赖 `ops_daily.py` 硬编码步骤与配置顺序校验，non-daily 多数只有自然语言 `date_gate` / `trigger_condition`，没有统一 typed due resolution、逐次 run ledger、lock/retry/idempotency 证据。继续在现有结构上加任务会放大语义漂移、重复 scheduler gate 和不可审计重跑风险。

F1 不另建第二套 scheduler，也不立即自动执行 weekly/monthly。目标是让既有统一入口背后逐步收敛到 canonical typed DAG，同时以 shadow parity 证明行为未改变。

## 权威边界

- 外部 scheduler 入口仍只有 `aits ops daily-run`。
- `config/scheduled_tasks.yaml` 在切换前仍是 cadence/legacy command source；canonical spec 由显式 adapter 生成，不复制一份手写任务清单。
- `config/etf_portfolio/operations_schedule.yaml` 是 ETF workflow source config，不是外部 scheduler entry；其接入必须通过相同 due/dependency/DQ/owner gates。
- `ops_daily.py` 仍保留为受 canonical lease 驱动的兼容执行 façade；CLI、命令、顺序、closed-market 行为与既有 report contract 保持 parity，runtime state/ledger 只作 additive 控制证据。
- non-daily 自动 dispatch 在 F1.5 以前保持 disabled；manual plan 也不得绕过 DQ、owner 或 production safety gate。
- legacy `local_cache_write` / `local_report_write` 必须由命名兼容表映射为 canonical production boundary，并保留原值；未知值 fail closed。

## 分阶段实施

### F1.1 Inventory、Due Contract 与 Compatibility Adapter

- 建立 typed due policy/context/resolution；输入至少包含 cadence、`as_of`、交易日/周期末状态、daily upstream status、DQ evidence、owner/event trigger、previous ledger。
- 从 `ScheduledTasksConfig` 生成 canonical `WorkflowSpec` shadow representation；不推断缺失 dependency、DQ、owner gate。
- 对 cadence、legacy production effect、entrypoint 和缺失 binding 形成明确兼容性评估；未知语义 `BLOCKED`。
- 输出 deterministic round-trip 与 spec/decision id。

验收：全部 78 个任务均可被 inventory；所有 cadence 有明确 disposition；未知 cadence/effect/due binding fail closed；不执行命令。

### F1.2 Shadow Plan 与 Daily Parity

- 用 due resolution 初始化 canonical `RunLedger`；将 due/not-due/blocked 原因写入 ledger。
- 将 canonical shadow plan 与既有 `DailyOpsPlan` 比较 step id、顺序、command、closed-market 与 safety metadata。
- 输出 additive shadow artifact；不改变既有 daily plan/run artifact。

验收：至少两个 trading-day fixture 与两个 closed-market fixture exact parity；shadow artifact deterministic；旧 bytes/path/status 不变。

### F1.3 Lock、Retry、Idempotency 与 Resume

- 统一 lock ownership、stale lock 检测、attempt budget、idempotency key、resume decision 和 terminal ledger write。
- non-idempotent step 必须有 lock 且 `max_attempts=1`；retry 只允许显式 policy。
- crash/restart 不重复已 PASS 的同一 idempotency key；artifact/ledger 不允许部分写。

验收：并发冲突、stale lock、retry exhausted、partial write、resume 与 duplicate trigger fixtures 全部 fail closed 或按 policy 恢复。

### F1.4 Daily Executor Adapter Cut-in

- 在不改变 CLI 的前提下，让 daily executor 消费 canonical plan/ledger；legacy `ops_daily.py` 保留为有 owner/sunset/parity 的 façade。
- `aits validate-data` 及同一路径质量门禁保持可见；blocked dependency 不得运行。
- daily plan/run/closed-market/Reader Brief final refresh 与现有行为 parity。

验收：两个真实 cadence shadow parity、focused/integration/reproducibility/full gate PASS；无 production/broker/weight 边界变化。

### F1.5 Non-daily Controlled Due Dispatch

- weekly / biweekly / monthly / governance 只能由统一 daily trigger 的 date-and-condition gate 到达，或继续 manual 运行。
- due 必须同时检查交易日历、周期规则、latest daily ledger、required artifacts、DQ、owner gate 和 safety boundary。
- periodic research review 只触发 ARCH-004F2 review lifecycle，不自动 preregister、调参、改权重、promotion 或 adopt。

实施拆分：

1. F1.5a：新增 reviewed non-daily policy manifest，逐 cadence 固化rule、anchor、daily/DQ/artifact/owner要求，并将41个任务逐一映射为one-step WorkflowSpec；不解析自然语言gate。
2. F1.5b：由统一daily trigger为41个任务输出deterministic due resolution + non-executing RunLedger；缺DQ evidence id、artifact ids、owner decision或显式event参数时保持BLOCKED/NOT_DUE。
3. F1.5c：仅对policy明确允许且命令placeholder已完全解析的任务开放controlled dispatch；复用F1.3 lock/idempotency/resume，manual checkpoint与`<...>`/未绑定`{...}`命令必须阻断。
4. F1.5d：证明daily PASS不被NOT_DUE/manual BLOCKED误判为失败，真正DUE执行失败仍fail closed；逐cadence parity、runbook、catalog、system flow和validation闭合。

验收：每个 periodic task 都有 due resolution 与 run ledger；not-due/blocked/limited 可审计；外部仍只有 daily unified trigger。

### F1.6 Validation 与 Closeout

- 更新 operations runbook、scheduled orchestration、artifact catalog 和 system flow。
- 通过 architecture fitness、contract-validation、integration、reproducibility、full parallel pytest。
- 明确 legacy façade sunset 与 ARCH-004G operations lane handoff。

## 输入、输出与计算边界

|环节|输入|计算|输出|失败语义|
|---|---|---|---|---|
|schedule inventory|scheduled tasks、ETF operations config|schema/owner/cadence/safety classification|inventory + compatibility assessment|未知字段/重复 id/unsafe action `BLOCKED`|
|due resolution|policy、as_of、calendar、daily/DQ/artifact/owner state|纯布尔条件与 reason code 组合|`DUE/NOT_DUE/BLOCKED`|不猜测缺失日历、owner 或 DQ|
|shadow plan|WorkflowSpec、DueResolution|topological/order-preserving plan + initial ledger|plan、ledger、parity diff|dependency/spec mismatch `BLOCKED`|
|runtime|plan、lock、previous ledger|idempotency/retry/resume state transition|terminal ledger + artifact refs|并发、超次、partial write fail closed|
|periodic review bridge|due periodic ledger、F2 lifecycle record|只触发 review event|review evidence / owner queue|不自动优化或 adoption|

## 非目标

- 不创建 Windows Task Scheduler / cron / GitHub Actions 条目。
- 不在 F1.1/F1.2 自动执行 weekly、biweekly、monthly 或 ad hoc research。
- 不改变 strategy、threshold、score、weight、backtest、promotion、paper-shadow、production 或 broker 行为。
- 不把 `scheduled_tasks.yaml` 的自然语言 gate 静默翻译成新规则。
- 不以 non-executing shadow plan 冒充执行结果；F1.4 起实际执行必须同时落 canonical execution state 与 terminal RunLedger。

## 当前进展

- 2026-07-11：完整读取 `docs/operations/operations_runbook.md` 与 `docs/runbooks/scheduled_task_orchestration.md`；盘点 scheduled config 为 36 daily + 41 non-daily。确认 canonical WorkflowSpec/RunLedger 已存在但尚未接入 ops runtime；F1.1 开始，non-daily dispatch 继续 disabled。
- 2026-07-11：F1.1 完成，F1.2 进入 `IN_PROGRESS`。新增 pure `operations_due_policy.v1` / `operations_due_resolution.v1` / non-executing `operations_shadow_plan.v1`，支持 daily/period-end/biweekly-anchor/explicit-trigger、daily/DQ/artifact/owner gate、deterministic round-trip 和 blocker propagation；legacy schedule 只有显式 owner/timezone/due binding 才生成 WorkflowSpec，未知 cadence/effect fail closed，dispatcher保持 disabled。77/77 tasks inventoryable；trading-day fixtures parity PASS。Closed-market fixtures 暴露 `official_policy_sources` 是未登记的 conditional legacy-only step，因此状态为 `LIMITED` 而非伪报 PASS；focused=23、scoped mypy PASS、contract-validation=197 PASS，下一步补条件步骤 contract。
- 2026-07-11：F1.2 条件步骤 contract 已闭合但 additive shadow artifact emission 尚未开始。`scheduled_tasks.yaml` 正式登记 `official_policy_sources` 且 `activation_condition=closed_market_only`；loader只接受 `always|trading_day_only|closed_market_only`，legacy validator与shadow WorkflowSpec按相同 session activation选择步骤。Inventory更新为37 daily + 41 non-daily=78；2 trading-day + 2 closed-market exact plan parity PASS，daily/CLI focused=79、fast-unit=198、contract-validation=197、scoped mypy PASS。未改变实际 plan命令、顺序、closed-market执行、artifact或外部trigger。
- 2026-07-11：F1.2 完成，F1.3进入 `IN_PROGRESS`。`daily-plan` / `daily-run` 在原Markdown旁原子写入 additive `*.operations_shadow.json`（`daily_operations_shadow.v1`），包含 scheduled config path/hash、activated WorkflowSpec、DUE resolution、non-executing RunLedger、exact parity和固定 no-production/no-broker/non-daily-disabled边界；同一DailyOpsPlan重复写字节确定。旧Markdown path/bytes不变，parity非PASS时sidecar构建fail closed。Focused=81、scoped mypy/Ruff PASS；lock/retry/idempotency/resume尚未接入runtime。
- 2026-07-11：F1.3完成，F1.4进入 `IN_PROGRESS`。新增governed `config/operations/runtime_control.yaml`、`operations_execution_state.v1`、`operations_run_control_resolution.v1`与atomic lock record；idempotency key固定由workflow id/spec id/as_of生成。Runtime control使用workflow/date lock阻断并发，只回收已过期lock；completed PASS重复trigger返回`ALREADY_COMPLETE`；run attempt与`WorkflowStepSpec.max_attempts`分别受限；只有明确`idempotent=true`的completed/current step可resume，non-idempotent partial、run/step budget exhausted均BLOCKED；state使用canonical atomic writer。Daily shadow sidecar已记录policy path/hash及cut-in disabled边界。并发、stale lock、duplicate、safe/unsafe resume、run/step exhausted、atomic write、owner release fixtures覆盖，focused=91、fast-unit=198、contract-validation=197、Ruff/mypy PASS。F1.4前generic control不替换legacy daily executor。
- 2026-07-11：F1.4完成，F1.5进入 `IN_PROGRESS`。`aits ops daily-run` 在不改变CLI的前提下通过 controlled adapter 获取workflow/date lease；trading-day与closed-market均按activated WorkflowSpec逐步记录started/PASS/SKIPPED，重复完成返回`ALREADY_COMPLETE`，并发、unsafe resume和attempt exhausted在runner前阻断。每次state原子更新同时生成标准`run_ledger.v1`：已完成/条件跳过分别映射PASS/SKIPPED，真实失败步骤映射FAILED，未执行下游映射BLOCKED；`validate-data`失败后`score_daily`不会运行。Resume只禁用已PASS步骤，旧DailyOpsStepResult/report status契约与legacy executor逐字段parity；runtime policy的daily cut-in=true，non-daily dispatch仍=false。验证：focused=105、integration=981、reproducibility=23、fast-unit=198、contract-validation=197、architecture-fitness=125、full parallel=`5467 passed / 0 failed / 642 warnings`，Ruff/mypy PASS。F1.5只处理受控non-daily due dispatch。
- 2026-07-11：F1.5a开始。当前41个non-daily任务中，weekly=14、biweekly=6、monthly=6、ad-hoc=15；其中大量旧项没有结构化date/DQ/artifact/owner binding，ad-hoc命令还含`<sweep_id>`、`{max_candidates}`等人工参数，monthly catalog review是自然语言checkpoint而非可执行CLI。F1.5先建立独立reviewed policy与逐任务one-step spec；统一daily trigger只做due评估，缺证据保持BLOCKED，manual/event任务保持NOT_DUE或BLOCKED，不把自然语言gate翻译成可执行规则。真正command dispatch在F1.5c的explicit allowlist和placeholder validation前继续关闭。
- 2026-07-11：F1.5完成，F1.6进入 `IN_PROGRESS`。新增`periodic_operations_control_policy.v1`与`periodic_operations_plan.v1`：weekly14/biweekly6/monthly6/ad-hoc15全部映射为独立one-step WorkflowSpec、due resolution和non-executing ledger；weekly/monthly用美股最后交易日、biweekly使用reviewed 2026-07-10 anchor、ad-hoc只接受explicit trigger。`daily-run`在canonical run metadata中additive写41项periodic plan；没有DQ evidence id、artifact ids或owner decision时due项BLOCKED，event未触发/非period-end为NOT_DUE，不影响已成功daily结果。新增人工`aits ops periodic-dispatch`，必须显式task id、daily/DQ status、DQ evidence、source artifact、owner decision及confirm flag；runtime non-daily lease=true但automatic periodic command dispatch=false。命令只允许governed `aits`/`python scripts/`前缀，未解析`{...}`/`<...>`和自然语言manual checkpoint在runner前BLOCKED；真正执行复用lock/idempotency/attempt/terminal RunLedger，重复PASS不重跑、失败后attempt exhausted。Focused相关回归=112、Ruff/mypy PASS；F1.6负责最终分层验证、文档和ARCH-004G handoff。
- 2026-07-11：F1.6与F1整体完成。最终验证：focused policy/operations/daily/CLI/schedule=118、integration=983、reproducibility=23、fast-unit=198、contract-validation=197、architecture-fitness=136、full parallel=`5480 passed / 0 failed / 642 warnings`，Ruff/mypy PASS；generated inventory=`785 modules / 1110 tests`、ownership/dependency/direct-writer violation=0。Operations runbook、scheduled orchestration、artifact catalog、system flow、compatibility baseline和worktree attribution均同步。F1只解锁ARCH-004F3/G，不改变strategy、threshold、score、weight、backtest、promotion、paper-shadow、production或broker；legacy daily façade和scheduled adapter的正式删除仍交ARCH-004G/H。
