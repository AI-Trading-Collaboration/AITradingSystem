# TRADING-688 Realized Outcome Availability Audit

最后更新：2026-06-20

## 背景

TRADING-686/687 已输出 valuation/crowding masking effectiveness review，但当前 20d
forward outcome 大量未成熟，被合并计入 `outcome_missing_count`，容易把“尚未到期”误读为
join bug 或价格缺失。本任务补齐 realized outcome availability audit，并统一 casebook、
ablation scenario 和 backtest bridge 的 outcome join key。

## 安全边界

- 只做 read-only / validation-only artifact。
- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- signal generation 继续使用 PIT/as-of 输入；outcome evaluation 才可读取
  `decision_time` 之后的 realized price。
- outcome 窗口使用交易日窗口，不使用自然日窗口。
- 所有 recommendation 都是 validation recommendation，`promotion_gate_allowed=false`。

## 输出要求

新增 outcome availability audit，覆盖：

- historical trace / masking casebook cases；
- ablation scenario cases；
- backtest bridge scenario cases；
- `as_of_date`、`decision_time`、`asset`、`scenario`、`trace_source`、
  `trace_contract_version` join key。

每个 case 生成 1d / 5d / 10d / 20d realized outcome window。尚未成熟的窗口标记
`outcome_not_mature`，不得计入 hard missing。

Audit summary 至少输出：

- `total_cases`
- `outcome_available_count`
- `outcome_missing_count`
- `outcome_not_mature_count`
- `missing_price_count`
- `missing_asset_mapping_count`
- `missing_calendar_count`
- `missing_join_key_count`
- by-window availability：1d / 5d / 10d / 20d
- by-asset availability
- by-date availability

## Recommendation 规则

本任务使用 validation-only pilot recommendation baseline，不进入 production policy：

- outcome 可用样本仍不足时：`insufficient_evidence`
- capped masking 优于 baseline：`prefer_capped_masking_candidate`
- no-mask 优于 baseline 且 drawdown 未明显恶化：`baseline_over_defensive_candidate`
- baseline 明显减少 drawdown 且 missed upside 可接受：`keep_baseline_masking_candidate`

`promotion_gate_allowed` 必须保持 false。

## 验收

- outcome availability audit schema test 通过；
- `outcome_not_mature` 不计入 hard missing；
- outcome join key completeness test 通过；
- effectiveness review 在 outcome 不足时输出 `insufficient_evidence`；
- 所有新增 artifacts `promotion_gate_allowed=false`；
- 并行 pytest / validation tier 通过或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `valuation_crowding_outcome_availability_audit` builder/CLI、validation pack
  artifact、stability projection 和 focused tests；casebook、ablation scenario
  和 backtest bridge 统一 join key。真实 expanded historical trace rerun 覆盖
  `total_cases=1232`，`outcome_available_count=56`，
  `outcome_missing_count=0`，`outcome_not_mature_count=1176`；by-window 为
  1d available 1176 / not mature 56，5d available 952 / not mature 280，10d
  available 672 / not mature 560，20d available 56 / not mature 1176。重新生成
  masking effectiveness review 后 full advisory layer 成熟样本仍只有 8，
  recommendation=`insufficient_evidence`，且 `promotion_gate_allowed=false`。
  validation pack artifacts=29，validation-pack-stability `PASS` 且
  `outcome_availability_repeatable=true`。验证通过 focused 并行 pytest
  `27 passed`、Ruff、py_compile、`git diff --check` 和全量并行 validation tier
  `2968 passed, 643 warnings`（pytest 153.48s，runner elapsed 167.37s）。
