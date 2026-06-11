# TRADING-199 to 203 Manual Portfolio Snapshot Hardening and Execution Guardrails

最后更新：2026-06-11

## 背景

TRADING-189～198 已把 defensive hypothesis 和 forward pressure evidence 自动化推进到
research-only / shadow monitoring / daily advisory 层。当前仍缺少一个可审计的真实持仓人工输入闭环：
owner-maintained manual portfolio snapshot、组合暴露验证、当前仓位与 shadow target 的 drift、
执行约束检查，以及 owner 可阅读的 manual execution review pack。

本阶段不引入 broker API、broker import、自动下单、自动 owner approval、production candidate、
真实仓位 mutation 或 `position_advisory_v1.yaml` 自动修改。

## 阶段拆解

### TRADING-199 Manual Portfolio Snapshot Schema Hardening

升级 `current_portfolio_snapshot.example.yaml`，新增
`manual_portfolio_snapshot_schema_v1.yaml`，并新增 `manual-portfolio validate/normalize/report`
与 `validate-manual-portfolio`。验收重点是 schema、权重、value、重复 symbol、负数、currency、
`broker_imported=false`、`broker_action_taken=false` 和 owner review pending 都可 fail closed 或显式披露。

### TRADING-200 Portfolio Exposure / Concentration / Currency Validation

新增 `portfolio_exposure_policy_v1.yaml`、`portfolio-exposure validate/report` 与
`validate-portfolio-exposure`。基于 normalized portfolio 输出 tech、semiconductor、defensive、
cash、risk asset、最大单一资产和非 base currency 检查。

### TRADING-201 Target Weight vs Current Position Drift Analysis

新增 `position-drift run/report` 与 `validate-position-drift`。对比 manual snapshot 和
shadow shortlist candidate target weights，输出 candidate-level drift、consensus drift summary
和 action candidates。`HIGH_DISAGREEMENT` 只能触发 manual review，不能生成可执行调整建议。

### TRADING-202 Execution Guardrails & Adjustment Constraint Engine

新增 `execution_guardrails_v1.yaml`、`execution-guardrails check/report` 与
`validate-execution-guardrails`。该层只判断建议调整是否被 cap/block 或需要分步 paper review，
固定 `order_ticket_generation_allowed=false`、`broker_action_allowed=false`。

### TRADING-203 Manual Execution Review Pack

新增 `manual-execution-review pack/report` 与 `validate-manual-execution-review`。整合 snapshot、
exposure、drift 和 guardrails，输出 owner checklist、manual execution decision、中文报告和
Reader Brief section。最终产物不是 order ticket。

## 依赖与顺序

1. 先更新 task register 和本需求文档。
2. 新增配置与核心 artifact workflow。
3. 接入 CLI、report registry、Reader Brief、artifact catalog、system flow、README 和 operations runbook。
4. 使用 example snapshot 与 latest/generated shadow shortlist 跑通真实链路。
5. 运行 focused tests、ruff、compileall、git diff check、dynamic-v3 validation 和 artifact family validation。

## 安全边界

- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generation_allowed=false`
- `order_ticket_generated=false`
- `owner_approval_required=true`
- `production_effect=none`
- `advisory_only=true`
- 不修改 broker、真实 portfolio、official target weights、baseline/production state 或
  `position_advisory_v1.yaml`

## 验收标准

- example snapshot validate PASS。
- invalid weight sum、duplicate symbol、negative weight 会 FAIL。
- manual portfolio normalize/report artifact 可生成并可 validate。
- portfolio exposure validation 可运行并检查 semiconductor/risk/cash/currency。
- position drift 输出 candidate matrix、consensus summary 和 action candidates。
- HIGH_DISAGREEMENT 阻断调整建议，只允许 manual review。
- execution guardrails 对超限 delta cap/block，且 broker/order ticket 始终禁用。
- manual execution review pack 可生成，Reader Brief section 可读。
- `aits etf dynamic-v3-rescue validate` 与
  `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue` 可运行。
- Focused tests、`python -m ruff check src tests`、`python -m compileall -q src tests`、
  `git diff --check` 通过。

## 进展记录

- 2026-06-11：新增需求文档并进入实现。初始范围为附件中的 TRADING-199～203，按一个大功能闭环交付；broker import、order ticket、真实 broker action 和 production mutation 均明确排除。
- 2026-06-11：baseline 实现完成并进入 VALIDATING。真实链路生成 manual snapshot
  `f2d6dd8cbba63619`、portfolio exposure `562ce5641d143dc8`、position drift
  `0c5a083d54b21fa1`、execution guardrail `0ea07df8d5414d79`、manual execution
  review `e8ff7d0d375a216d`。所有新 artifact validator、`aits etf dynamic-v3-rescue
  validate`、`artifacts validate --family dynamic_v3_rescue`、report index、Reader Brief、
  Reader Brief quality、documentation contract、focused tests、ruff、compileall、git diff
  check 和全量 pytest 均通过。全量 pytest 结果为 `2353 passed, 640 warnings`；warnings
  来自既有 numpy/eventkit warning，不改变本任务 safety boundary。当前仍固定
  `broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false`、
  `owner_approval_required=true`、`production_effect=none`；下一步为项目 owner 人工复核
  manual execution review pack。
