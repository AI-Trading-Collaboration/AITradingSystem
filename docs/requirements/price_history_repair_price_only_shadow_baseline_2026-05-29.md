# TRADING-049 Price History Repair & Price-only Shadow Baseline

状态：DONE
任务：TRADING-049
最后更新：2026-05-29

## 背景

BTINPUT-002 已补齐 shadow backtest input diagnostics、snapshot manifest、repair
dry-run plan、Dashboard 和 Reader Brief 只读展示。当前本地真实诊断结果已经从
不可定位的输入质量失败，收敛为明确的价格历史缺口：

- `backtest_input_diagnostics status=FAILED`
- blocking reason：缺少 `GOOGL`、`BRK.B`、`SGOV` price history
- signal snapshots：`LIMITED`

本阶段目标不是一步到位补齐完整 PIT 新闻、财报和信号快照，而是先修复价格历史缓存，
让 shadow backtest 在信号快照仍 `LIMITED` 时以 price-only 模式可运行，并明确禁止
candidate promotion。

## 目标

1. 让 `aits data repair-backtest-inputs --latest` 执行实际价格历史 repair。
2. 支持 `--date YYYY-MM-DD`、`--price-only` 和 `--symbols GOOGL BRK.B SGOV`。
3. 复用项目现有 market data adapter，优先使用当前 active 主价格源 FMP；不得把 inactive
   Yahoo 便利源静默写入主价格缓存。
4. 支持 `GOOGL`、`BRK.B`、`SGOV`，其中 `BRK.B` 使用显式 source symbol `BRK-B`，并在
   manifest 中记录 canonical/source mapping。
5. repair 后重新生成：
   - `artifacts/data_quality/YYYY-MM-DD/backtest_input_diagnostics.json`
   - `artifacts/data_quality/YYYY-MM-DD/backtest_input_diagnostics.md`
   - `artifacts/backtest_snapshots/YYYY-MM-DD/backtest_input_manifest.json`
6. 当 price data 为 `OK` 且 signal snapshots 为 `LIMITED` 时，允许
   `price_only_shadow_backtest`。
7. price-only 模式固定 `production_effect=none`、`manual_review_required=true`、
   `auto_promotion=false`，且 candidate promotion disabled。
8. Dashboard 和 Reader Brief 展示 backtest mode、promotion eligibility 和 limited signal
   warning。

## 非目标

- 不接入完整 point-in-time 新闻数据。
- 不接入完整财报 PIT 数据。
- 不做自动 production 参数晋升。
- 不修改 `config/parameters/production/current.yaml`。
- 不接入期权、分钟线或复杂机器学习模型。
- 不优化 hard gate 阈值。

## 阶段拆解

|阶段|优先级|状态|验收标准|
|---|---:|---|---|
|任务登记与需求文档|P0|DONE|当前任务表新增 TRADING-049，本文记录目标、边界、安全约束和验收。|
|价格历史 repair 模块|P0|DONE|新增幂等 repair builder，支持 per-symbol 下载、canonical/source mapping、partial failure 报告、缓存 upsert 和 download manifest 审计。|
|CLI 接线|P0|DONE|`aits data repair-backtest-inputs --latest/--date/--price-only/--symbols` 可用，dry-run 仍只输出 plan。|
|diagnostics / manifest / shadow backtest mode|P0|DONE|diagnostics 输出 `can_run_shadow_backtest=true`、`can_promote_candidate=false` 和 `backtest_mode=price_only_shadow_backtest`；shadow summary 禁止 candidate。|
|Dashboard / Reader Brief / 文档|P1|DONE|Backtest Data Quality 和 Parameter Shadow Review 显示 price-only 模式与 limited signal warning；`docs/system_flow.md` 同步。|
|测试与验证|P0|DONE|新增/更新 price repair、diagnostics、price-only shadow、dashboard 和 Reader Brief 测试；目标验证通过或记录真实 blocker。|

## Price-only promotion policy

price-only shadow baseline 只用于验证框架、价格趋势、仓位逻辑和 walk-forward 可运行性。
它不能作为正式 candidate 晋升依据。

策略约束：

- `max_promotion_status=watch`
- `allow_candidate=false`
- `allow_production_promotion=false`
- `manual_review_required=true`

如果原始 promotion evaluation 触发 hard rejection，可以保持 `rejected`；否则 price-only
模式最高只能输出 `watch`，reason 必须明确说明 signal snapshots 仍为 `LIMITED`。

## 验收标准

- `aits data diagnose-backtest-inputs --latest` 能明确显示缺失资产。
- `aits data repair-backtest-inputs --latest --price-only` 或
  `aits data repair-backtest-inputs --symbols GOOGL BRK.B SGOV` 可补齐价格历史。
- repair 后再次诊断预期：
  - `price_data_status=OK`
  - `asset_coverage_status=OK`
  - `date_coverage_status=OK`
  - `signal_snapshots_status=LIMITED`
  - `overall_status=LIMITED`
  - `can_run_shadow_backtest=true`
  - `can_promote_candidate=false`
- `aits parameters shadow-backtest --latest --dry-run` 输出：
  - `backtest_mode=price_only_shadow_backtest`
  - `production_effect=none`
  - `manual_review_required=true`
  - `promotion_status=watch` 或 `rejected`
  - candidate promotion disabled
- 不修改 `config/parameters/production/current.yaml`。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：BTINPUT-002 已定位真实 blocker 为
  `GOOGL`、`BRK.B`、`SGOV` price history 缺失；本阶段推进到
  `FAILED -> LIMITED but runnable`，并保留 signal snapshot 完整化为后续任务。
- 2026-05-29：完成实现和本地验收。`aits data repair-backtest-inputs --latest
  --price-only` 已用 FMP 补齐 `GOOGL`、`BRK.B`、`SGOV` 主价格缓存，其中 `BRK.B`
  记录 `BRK-B` source symbol mapping；重新诊断结果为 `overall_status=LIMITED`、
  `price_data_status=OK`、`asset_coverage_status=OK`、`date_coverage_status=OK`、
  `signal_snapshots_status=LIMITED`、`backtest_mode=price_only_shadow_backtest`、
  `can_run_shadow_backtest=true`、`can_promote_candidate=false`。shadow backtest dry-run
  已进入 price-only 模式，promotion 结果为 `rejected`，原因明确为 signal snapshots
  LIMITED 且 candidate promotion disabled。
