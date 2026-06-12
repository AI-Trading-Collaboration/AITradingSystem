# TRADING-229～233 Risk-Capped Limited Adjustment Research Method

最后更新：2026-06-12

## 背景

TRADING-224～228 的 refined method proposal 已确认 `limited_adjustment`
hardening 失败，主要问题集中在：

- rolling consistency = `UNSTABLE`
- unstable windows = 129
- dominant context = `sideways_choppy`
- long-window risk = `RETURN_IMPROVES_RISK_WORSENS`
- risk_worsening_source = `higher_semiconductor_exposure`
- data warning impact = `REVIEW_REQUIRED`

本阶段实现新的 research-only target method：
`risk_capped_limited_adjustment`。它不是 production target、不是 official target
weights、不是 broker instruction；它只用于验证在保留 `limited_adjustment` 部分收益
改善的同时，是否能降低 semiconductor / risk asset exposure 带来的回撤恶化和
rolling instability。

## 安全边界

所有新增配置、artifact、CLI 输出和报告必须固定：

- `research_target_only=true`
- `paper_shadow_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_effect=none`
- `auto_apply=false`

本阶段不得写入 official target weights，不得修改真实仓位，不得生成 order ticket，
不得触发 broker，不得自动 owner approval，也不得自动修改
`position_advisory_v1.yaml`。

## 子任务

### TRADING-229 Risk-Capped Limited Adjustment Spec & Config

新增
`config/etf_portfolio/dynamic_v3_rescue/risk_capped_limited_adjustment_v1.yaml`，
定义 `risk_capped_limited_adjustment` 的 base method、全局 caps、delta caps、
contextual caps、reallocation policy、diagnostics 和 safety boundary。

新增 config validation / report CLI：

- `aits etf dynamic-v3-rescue risk-capped-limited config-validate`
- `aits etf dynamic-v3-rescue risk-capped-limited report-config`
- `aits etf dynamic-v3-rescue validate-risk-capped-limited-config`

校验必须确认 base method 为 `limited_adjustment`，risk caps 不比
`model_target_portfolio_v1.yaml` 的 semiconductor cap 更宽，reallocation destination
存在于可交易 universe，contextual caps 不比全局 caps 更宽松，且 safety locked。

### TRADING-230 Risk-Capped Target Method Implementation

在 system target generation 链路中接入 `risk_capped_limited_adjustment`。该方法以
`limited_adjustment` target weights 为输入，按 config 限制：

- total risk asset exposure
- semiconductor exposure
- single symbol weight
- per-rebalance risk asset increase
- per-rebalance semiconductor increase
- minimum cash buffer
- `sideways_choppy` / `tech_drawdown` / `semiconductor_pullback` contextual caps

输出 `risk_capped_target_weights.jsonl`、`cap_events.jsonl`、
`reallocation_events.jsonl`、`cap_reason_summary.json` 和中文报告。

### TRADING-231 Risk-Capped Paper Shadow Backfill

将 `risk_capped_limited_adjustment` 纳入 historical paper shadow backfill，并新增
dedicated risk-capped backfill wrapper artifacts，覆盖默认 `ai_after_chatgpt`
区间 `2022-12-01` 到 latest available data。Backfill 必须先通过 cached data quality
gate，报告必须披露 data quality status。

新增 CLI：

- `aits etf dynamic-v3-rescue risk-capped-backfill run`
- `aits etf dynamic-v3-rescue risk-capped-backfill report`
- `aits etf dynamic-v3-rescue validate-risk-capped-backfill`

### TRADING-232 Risk-Capped vs Limited Adjustment Comparison

比较 `risk_capped_limited_adjustment`、`limited_adjustment`、
`static_baseline`、`no_trade_baseline`、`consensus_target` 和
`defensive_limited_adjustment`，重点回答 max drawdown、semiconductor exposure、
cash buffer、turnover、sideways_choppy 表现和 rolling consistency 是否改善。

新增 CLI：

- `aits etf dynamic-v3-rescue risk-capped-comparison run`
- `aits etf dynamic-v3-rescue risk-capped-comparison report`
- `aits etf dynamic-v3-rescue validate-risk-capped-comparison`

### TRADING-233 Risk-Capped Research Method Review Pack

汇总 config validation、generation、risk-capped backfill 和 comparison，生成 review
pack，判断 `risk_capped_limited_adjustment` 是否应成为新的 recommended research
method。即使 decision 为 promotion，也只能是 recommended research method，仍需
forward confirmation 和 owner review。

新增 CLI：

- `aits etf dynamic-v3-rescue risk-capped-review pack`
- `aits etf dynamic-v3-rescue risk-capped-review report`
- `aits etf dynamic-v3-rescue validate-risk-capped-review`

## 实施顺序

1. 更新 task register 和本文档，记录 P0 scope 与 safety boundary。
2. 新增 risk-capped config、config validator、normalized config artifact 和 config report。
3. 扩展 model target / backfill method universe，生成 risk-capped target weights 与 cap diagnostics。
4. 新增 risk-capped dedicated backfill、comparison 和 review pack artifacts。
5. 更新 CLI、Reader Brief、report registry、artifact catalog、system flow、operations runbook 和 README。
6. 增加 focused tests，运行 required validate / test / lint 命令。
7. 根据真实链路结果更新本文档和 task register 状态。

## 验收标准

- risk-capped config validation PASS，normalized config 可生成。
- `risk_capped_limited_adjustment` target weights 可生成，权重和保持 1.0，无负权重。
- cap events、reallocation events 和 cap reason summary 可输出。
- risk-capped backfill 可运行，并生成 state history / trade ledger / summary。
- comparison 输出 return / drawdown / volatility / turnover / semiconductor exposure /
  rolling / regime / stability 对比。
- review pack 输出 decision、confidence、owner checklist、reader brief section。
- 所有新增报告固定 no official target / no broker / no production。
- README、operations runbook、system flow、report registry、artifact catalog、
  requirements、task register 和 Reader Brief 同步。
- focused tests、ruff、compileall、`git diff --check` 和新增 validate CLI 通过，或记录
  清晰阻塞原因。

## 状态记录

- 2026-06-12: 新增并进入 `IN_PROGRESS`，原因：owner 要求完成 TRADING-229～233
  risk-capped limited adjustment research method implementation。当前阶段只实现
  risk-capped research method，不实现 `regime_gated_limited_adjustment` 或 hybrid。
- 2026-06-12: baseline implementation 完成并转入 `VALIDATING`。真实链路输出：
  config `risk-capped-config_a2f0f9f5a9c99298`、model target
  `model-target_a4c790df71379190`、risk-capped target
  `risk-capped-limited_e567c0b5135843fd`、source baseline backfill
  `paper-shadow-backfill_981dff0cc53a02b9`、risk-capped backfill
  `risk-capped-backfill_3d41bb93e038bbe4`、comparison
  `risk-capped-comparison_e48889d74b4d13c4`、review
  `risk-capped-review_41c625e30eb6efe7`。
- 2026-06-12: generation 阶段 cap status=`PASS`，cap_event_count=2，
  total_reallocated_to_cash=0.004454685。Backfill 使用
  `market_regime=ai_after_chatgpt`，实际区间 `2022-12-01` 到 `2026-06-11`，
  data_quality=`PASS_WITH_WARNINGS`，cap_event_count=290，
  avg_semiconductor_weight=0.2163382044，max_semiconductor_weight=0.2367086138，
  avg_cash_weight=0.1924497267，min_cash_weight=0.1699007761。
- 2026-06-12: comparison conclusion=`mixed`；相对 `limited_adjustment` 的
  total_return_delta=-0.0808442483，max_drawdown_delta=-0.000636931，
  avg_semiconductor_weight_delta=-0.0074072835，realized_volatility_delta=-0.0019399832，
  rolling_consistency=`WORSE`，stability_conclusion=`WORSE`。Review decision=`REJECT`，
  decision_confidence=`LOW`，improvements_vs_limited 为
  semiconductor_exposure=`REDUCED`、return_preservation=`POOR`、
  max_drawdown=`WORSE`、rolling_consistency=`WORSE`，
  requires_forward_confirmation=true。
- 2026-06-12: 验证通过：新增 config/generate/backfill/comparison/review report 和
  validate CLI，`aits etf dynamic-v3-rescue validate`，
  `aits etf dynamic-v3-rescue artifacts repair-latest`，
  `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`，
  `aits reports index --date 2026-06-12`，Reader Brief latest
  `2026-06-11` 与 Reader Brief quality；focused pytest `12 passed`，
  full pytest `2392 passed, 640 warnings`，`python -m ruff check src tests`，
  `python -m compileall -q src tests`，`git diff --check`。
- 2026-06-12: 已知限制：当前 cap policy 降低 semiconductor exposure，但明显牺牲
  total return，且 rolling / stability 变差，因此 review pack 给出 `REJECT`。本阶段
  不调整 cap calibration、不实现 `regime_gated_limited_adjustment` / hybrid、不加入
  slippage / transaction cost sensitivity、不生成 official target weights、不触发 broker。
  下一步由 owner 复核是否拒绝当前 v1 cap policy，或新开任务研究更保守的
  regime-gated / hybrid calibration。
