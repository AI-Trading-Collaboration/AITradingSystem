# TRADING-2294 Evidence Accumulation Extension Plan

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2293 已对 `volatility_regime_scope_narrowed_risk_cap_v1` 完成
scope-narrowed forward observe readiness review，真实 run 结果为
`FORWARD_OBSERVE_READY_WITH_WARNINGS`。Warning 来自
`DATA_QUALITY_PASS_WITH_WARNINGS` 和 `TRIGGER_DIRECTION_SAMPLE_SPARSE`。

TRADING-2294 承接该结论，目标是把 risk-cap forward observe 的 evidence
accumulation runtime 设计成可审计契约，但不启动 runtime、不接生产日报、不进入
paper-shadow、不产生 broker action。

## 目标

新增 CLI：

```bash
aits research trends forward-observe-evidence-accumulation-plan
```

该命令读取 TRADING-2293 readiness outputs，生成 observe-only runtime / evidence
accumulation contract：

- `forward_observe_runtime_design.md`
- `forward_observe_runtime_contract.json`
- `risk_cap_daily_observe_record_schema.json`
- `risk_cap_trigger_followup_schema.json`
- `forward_observe_storage_layout.json`
- `forward_observe_runtime_safety_boundary.json`
- evidence accumulation plan / minimum observation window / weekly review cadence
  / extension decision matrix artifacts。

所有输出必须强制：

```yaml
observe_mode: observe_only
forward_observe_started: false
runtime_started: false
daily_report_integration: design_only
weekly_report_integration: design_only
portfolio_effect: none
production_effect: none
promotion_allowed: false
paper_shadow_allowed: false
production_allowed: false
broker_action: none
manual_review_only: true
```

## 实施拆解

1. 输入 loader 和 fail-closed safety validation。
   - 读取 TRADING-2293 readiness summary、gate checklist、daily/weekly report
     contract、evidence collection spec、stop/continue rules、operational boundary、
     metric spec、trigger interpretation spec 和 next-task recommendation。
   - 要求 candidate 为 `volatility_regime_scope_narrowed_risk_cap_v1`。
   - 要求 readiness gate 为 `FORWARD_OBSERVE_READY_RECOMMENDED` 或
     `FORWARD_OBSERVE_READY_WITH_WARNINGS`。
   - 输入若打开 promotion、paper-shadow、production、broker、owner-review 或
     runtime started gate，立即失败。

2. Runtime evidence contract。
   - 定义每日 observe record schema：trigger、intensity、triggered assets /
     horizons、reason、data quality、source artifact references、allowed action。
   - 定义 trigger follow-up schema：5d / 10d / 20d future path、drawdown、
     realized volatility、false risk-cap、missed stress、follow-up status。
   - 定义 storage layout、append-only audit fields、checksum/source-hash
     requirements 和 data-quality visible fields。

3. Evidence accumulation / review plan。
   - 明确 minimum observation windows、trigger sample sufficiency handling、
     sparse sample extension rules、weekly review cadence 和 stop/continue/extend
     decision matrix。
   - 所有样本下限、review window 和 aging rules 必须作为 named policy fields
     输出 rationale，不能变成隐式投资阈值。

4. 文档和 registry。
   - 更新 research docs、report registry、artifact catalog、system flow 和 task
     register。
   - 文档必须用中文说明：2294 是 observe-only runtime design，不是启动
     runtime，不是 paper-shadow / production / broker readiness。

5. 验证。
   - focused parallel pytest 覆盖 loader、safety boundary、record schema、
     follow-up schema、decision matrix、CLI output 和 docs。
   - 运行 Ruff、compileall、docs freshness / registry checks、task-register
     consistency 和 `git diff --check`。

## 验收标准

- CLI implemented: `aits research trends forward-observe-evidence-accumulation-plan`。
- 读取 TRADING-2293 outputs，并 fail closed 拒绝 non-readiness 或 unsafe input。
- 生成 runtime contract、daily observe record schema、trigger follow-up schema、
  storage layout、runtime safety boundary 和 research docs。
- `forward_observe_started=false`、`runtime_started=false`，daily / weekly report
  integration 均为 `design_only`。
- promotion、paper-shadow、production、broker action 全部保持 false / none。

## 进展记录

- 2026-07-01: 根据 owner 附件和 TRADING-2293 next task recommendation 新增并进入
  `IN_PROGRESS`。当前 worktree 已有两个 research 文档未提交改动；本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现完成并转入 `VALIDATING`。新增
  `aits research trends forward-observe-evidence-accumulation-plan`、2293
  readiness output loader、runtime contract、daily observe record schema、trigger
  follow-up schema、storage layout、minimum observation policy、weekly review
  cadence、decision matrix、runtime safety boundary、report registry / artifact
  catalog / system flow 更新和 focused tests。真实 run status=
  `FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED`，source
  readiness gate=`FORWARD_OBSERVE_READY_WITH_WARNINGS`，source data quality=
  `PASS_WITH_WARNINGS`，source warnings 包含
  `DATA_QUALITY_PASS_WITH_WARNINGS` / `TRIGGER_DIRECTION_SAMPLE_SPARSE`；所有
  outputs 固定 `observe_mode=observe_only`、`forward_observe_started=false`、
  `runtime_started=false`、daily / weekly report integration=`design_only`、
  promotion / paper-shadow / production / broker false / none。验证通过新增
  focused parallel pytest 4 passed、docs / registry / task-register focused
  parallel pytest 35 passed、Ruff、compileall、contract-validation 193 passed 和
  `git diff --check`。
