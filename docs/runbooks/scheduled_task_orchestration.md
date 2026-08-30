# 统一计划任务编排 Runbook

## 目标

`config/scheduled_tasks.yaml` 是 OPS-059 后的统一调度计划源，登记 daily / weekly / biweekly / monthly / ad hoc research 任务。`aits ops daily-plan` 和 `aits ops daily-run` 只执行 `daily_trading_day` 链路，并在生成计划时校验顺序与配置一致。TRADING-099 允许 daily 链路包含 Dynamic v3 rescue lightweight `schedule observe` gate；它只做 due/skip/block 审计和只读检查，不执行参数搜索或 promotion pack。

本 runbook 用于调度审查，不替代各报告自己的审计产物。所有 report / governance / shadow monitor 任务默认 `production_effect=none`，不写 production weights，不写 active shadow weights，不调用 broker，不触发交易。

## Daily Trading-Day Chain

交易日 `aits ops daily-run` 仍按配置中的稳定拓扑顺序展示，但执行语义从线性
fail-fast 改为 `scheduled_tasks_v4` 显式 DAG：

1. capture plane：`ops capture-daily-inputs` 逐源尝试 market/macro、FMP PIT、SEC、
   valuation 与 official sources；
2. independent validation branches：
   `validate-data <- market_macro`、
   `pit build -> pit validate <- fmp_forward_pit`、
   `sec metrics -> merge -> validate <- sec_companyfacts`；
3. score join：`score-daily` 同时依赖 strict DQ/PIT/SEC 和
   `fmp_valuation + official_policy_sources`；
4. report branches：forward evidence、dashboard、SEC PIT observe/monitor、score
   attribution、market panel、freshness/recovery、portfolio tracking、ETF forward、
   artifact lineage、report index、documentation/governance 与 Reader Brief quality
   都声明自己的 upstream；
5. operator closure：`ops health` 与 `security scan-secrets` 为 `always_run`。

一个 branch 的 `FAIL/BLOCKED` 只传播给显式 dependents；无关 sibling 和 always-run
closure 继续。overall 只要存在 required branch 缺口就必须是
`BLOCKED_DEPENDENCY/FAIL`，不会进入 Reader Brief finalization 或 terminal PASS。
`validate-data` 仍是所有 cached market/macro score/report consumer 的必需质量门禁，
不能因 sibling continuation 被绕过。`forward-evidence capture-dry-run-daily` 只在
`score-daily` PASS 后写 dry-run archive 和 append-only ledger，固定
`production_effect=none`，不得触发 broker/order、paper-shadow、official weight 或
production mutation。Portfolio tracking review 的 `needs_more_data` 是 VALIDATING 下的正常
窗口状态，不得作为 production approval。Dynamic v3 rescue `schedule observe` 只允许检查
weekly due、latest pointer、stale 和 optional observe-only shadow monitor；不得自动运行
parameter sweep、promotion pack 或 broker path。

Capture umbrella 内部仍按稳定顺序展示，但每个 source 有独立
`source_idempotency_key/lease/attempt_history`。一个 source 的 active lease、quota、credential、
schema 或 attempt exhaustion 不得扣减其他 source budget。gap ledger 生成
source/session recovery queue；queue 只登记 manual recovery readiness，不是 scheduler task。

外部 scheduler 仍只能调用 `aits ops daily-run`。当 reviewed external-scheduler marker 存在时，
该命令先执行 pinned-clean checkout runtime preflight，再进入 checkout WRITE guard。不得为
preflight、recovery queue 或 non-daily cadence 创建额外 scheduler entry。

## 验证 Daily Plan

只读检查：

```powershell
aits ops daily-plan --as-of 2026-05-06
```

真实执行：

```powershell
aits ops daily-run --as-of 2026-05-06
```

计划和执行器都应显示同一拓扑顺序、dependencies、capture components 与 always-run
标记。若配置与代码步骤/DAG不一致，`daily-plan` / `daily-run` 应 fail closed，而不是继续用隐式顺序运行。

两者还会在原计划Markdown旁写入 `daily_operations_shadow.v1` JSON sidecar，保存source config hash、market-session activated WorkflowSpec、DUE resolution、non-executing RunLedger和exact parity。该sidecar是additive审计证据，不执行命令、不启用non-daily dispatch、不改变原Markdown bytes/path。

Sidecar同时记录`config/operations/runtime_control.yaml`的path/hash和cut-in flags。
`legacy_daily_executor_cut_in_enabled=true` 时，`daily-run`先获取canonical workflow/date
lease，再通过兼容façade执行 DAG；每步结果与terminal状态写入
`outputs/run_control/daily/states/<idempotency-key>.json`和相邻`*.run_ledger.json`。相同
spec/as-of已PASS时不重复运行；active lock、unsafe resume或attempt exhausted在runner前阻断。
branch failure 后 lease 保持 active，failed step slot 被释放以运行 independent sibling；未启动的
dependency-blocked step 在 daily report 为 `BLOCKED`，ledger 为 `SKIPPED`，严格结果必须读取顶层
`run_status/run_blocker_codes`。`validate-data`失败不得执行score/report consumer，但不再阻止PIT/SEC
sibling或always-run closure。该切换不启用non-daily dispatch。

OPS-071 为已经 terminal `FAILED` / `BLOCKED` 的同一 as-of 增加显式 child recovery，而不是
自动重试或删除旧 state。恢复仍通过唯一 `aits ops daily-run` 入口，且必须一次性提供
`--recovery-parent-run-id`、`--recovery-from-step`、`--recovery-reason-code`；control plane 将
parent manifest、旧/新 release、active deployment receipt、workflow spec、attempt budget 与
idempotent replay slice 逐项绑定，并先冻结原 state/ledger bytes 和 recovery receipt。当前 reviewed
boundary 仅允许从 `artifact_lineage` 或更后的 report/finalization step 开始，每个 terminal parent
最多一个 child；capture/provider/DQ/PIT/score 不会重放。Lineage 中未到期、manual 或历史
paper-shadow/weekly/readiness/owner evidence 缺失时保留 placeholder 和 warning，以
`INSUFFICIENT_DATA` 表示 availability；topology、安全边界和 strict data/PIT 仍 fail closed。
该机制不建立第二 scheduler、不扩展 provider budget、不写 weights，也不触发 broker/trading。

F1.5在每次`daily-run`的canonical metadata目录additive写`periodic_operations_plan_YYYY-MM-DD.json`。该文件覆盖14 weekly、6 biweekly、6 monthly和15 ad-hoc任务，每项独立保存one-step WorkflowSpec、typed due resolution、non-executing RunLedger和原command template；缺DQ/artifact/owner evidence的due项BLOCKED，非period-end或event未触发项NOT_DUE。`automatic_command_dispatch_enabled=false`，因此daily trigger不执行这些命令。Operator只有在持有完整evidence/owner decision时才可显式调用`aits ops periodic-dispatch ... --confirm-manual-dispatch`；未解析`{...}`/`<...>`、自然语言manual checkpoint、非allowlist前缀、duplicate/concurrent/attempt exhausted均fail closed。该manual command不是外部scheduler entry。

## Closed-Market Mode

周末或 NYSE 常规整日休市日：

- 仍运行 `validate-data`、PIT fetch/build/validate、SEC companyfacts/metrics、valuation、Dynamic v3 rescue `schedule observe`（输出 closed-market skip audit）、`ops health --non-trading-day` 和 secret scan。
- `official_policy_sources` 以 `config/scheduled_tasks.yaml` 的 `activation_condition=closed_market_only` 在 `validate-data` 后运行；交易日不激活。配置计划、legacy daily plan和canonical shadow plan必须解析为相同步骤顺序。
- 跳过 `score-daily`、forward evidence dry-run archive、dashboard、SEC PIT shadow observe / monitor、score change attribution、market panel、market data freshness review、freshness recovery、portfolio candidate tracking、portfolio tracking review、report index、documentation contract、research governance summary、Reader Brief 和 Reader Brief quality。
- 不生成新的日报评分、decision snapshot、Reader Brief scoring artifacts、prediction ledger 行或执行动作。

## Weekly Cadence

Weekly 任务在 `config/scheduled_tasks.yaml` 中登记；daily-run只生成逐项due/blocked/not-due评估，不自动执行：

- backtest
- backtest robustness
- parameter replay
- parameter candidates
- parameter governance
- weight candidate evaluation
- weight promotion gate
- research governance summary review
- governed developer workflow health review（每个 ISO 周首个非平凡 tracked mutation 前；已有当周 validated artifact 时复用）
- Dynamic v3 rescue artifact validation / stale review / governance validate / research index / observe-only shadow monitor

`weekly_workflow_health_review` 只读取 validation runtime、publication transaction 和 Git
main history，输出只读 health report / validation / `PROPOSED_REVIEW_ONLY` candidates。
它不读取 market cache，不要求 `aits validate-data`，也不自动 dispatch candidate、修改
task register、放宽门禁或触发 production/broker。当前仍由 unified periodic plan 发现 due
状态，`automatic_command_dispatch_enabled=false` 保持不变。

Weekly 输出必须声明实际 research window 与 requested/evaluated range；默认 primary conclusion window 从 `2021-02-22` 开始。若显式使用 `ai_after_chatgpt` / `2022-12-01`，必须标为 historical comparison、AI-cycle attribution 或 sensitivity/stress，而不是默认或更严格的 minimum bound。

## Biweekly Cadence

Biweekly 任务只作为人工或后续 scheduler 接入口登记：

- investment review
- feedback loop review
- shadow lane review
- SEC PIT observe-only review
- manual thesis review
- manual risk review

这些任务不得因为存在于配置中而进入 daily-run。

## Monthly Cadence

Monthly audit 任务只登记，不自动 daily-run：

- documentation contract audit
- artifact catalog review
- report registry audit
- data source coverage review
- PIT coverage review
- long-window backtest review

Monthly 任务适合用于检查文档覆盖、report registry freshness、数据源覆盖和长窗口回测解释是否仍然可审计。

## Ad Hoc Research Chain

以下任务标记为 manual / ad hoc research：

- SEC PIT historical backfill
- SEC PIT cognitive evaluation
- SEC PIT baseline comparison
- SEC PIT diagnostics
- SEC PIT candidate review
- large parameter search
- cache-only replay-window
- Dynamic v3 rescue data audit / profile validation / small_real sweep / injection audit / candidate attribution / walk-forward selection / overfit / promotion pack

这些任务可能成本高、耗时长或需要 owner 明确选择窗口。不得由 daily-run 自动触发。

## Safety Checklist

调度审查时必须检查：

- reader/governance/shadow monitor/report tasks 的 `production_effect` 是否为 `none`。
- 是否存在 production weight write。
- 是否存在 active shadow weight write。
- 是否存在 broker action 或 trading action。
- `reports index` 和 `docs report-contract` 是否在 Reader Brief 之前。
- `research-governance-summary` 是否在 Reader Brief 之前。
- `sec-pit shadow-monitor` 是否在 research governance summary 之前。
- daily Dynamic v3 rescue 任务是否仅为 `schedule observe`，且所有 heavy research 命令仍在 weekly / ad hoc cadence。
- weekly / biweekly / monthly / ad hoc research tasks 是否没有进入 daily plan。

## Windows Task Scheduler

OPS-059 不自动创建或修改 Windows Task Scheduler 任务。现有模板若要接入，应只调用：

```powershell
aits ops daily-run
```

不要把 weekly / monthly / ad hoc research 命令直接塞进 daily trigger；Dynamic v3 rescue 只允许 daily trigger 调用 `schedule observe` gate。若未来需要本地模板更新，先审查 `config/scheduled_tasks.yaml`、本 runbook 和任务登记，再生成独立模板变更。

## 测试

OPS-059 的基础回归：

```powershell
python -m pytest tests/test_scheduled_tasks.py tests/test_ops_daily.py tests/test_cli_direct.py -q
```

测试覆盖 daily 命令顺序、Reader Brief 链执行顺序、closed-market skips、非 daily 任务隔离、direct dispatcher 支持和 safety invariants。
