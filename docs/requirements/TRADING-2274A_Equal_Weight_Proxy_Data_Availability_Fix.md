# TRADING-2274A Equal-Weight Proxy Data Availability Fix

最后更新：2026-06-28

## 背景

TRADING-2271 proxy coverage audit 和 TRADING-2273 challenger matrix 均显示
`rsp_to_spy` / `qqqe_to_qqq` 因 RSP、QQQE 价格缓存缺失而阻塞 equal/cap-weight
divergence 与 combined proxy offline challenger rows。本任务只处理数据可用性诊断与
免费价格历史补齐，不改变 first-layer active policy。

## 范围

- 检查 RSP / QQQE 缺失原因，区分 ticker 未下载、provider 不支持、缓存缺失或
  symbol mapping 问题。
- 优先尝试补齐 RSP / QQQE 免费价格历史，并记录 provider、endpoint、请求参数、
  下载时间、row count、checksum 和 symbol mapping。
- 如 QQQE 免费路径不可用，评估低成本替代：
  - QQQ / QQQE；
  - RSP / SPY；
  - equal-weight sector ETF / cap-weight sector ETF；
  - 自建 top-N equal-weight proxy，仅标注为 diagnostic proxy，不声明 true breadth。
- 更新 proxy coverage matrix，明确 equal/cap proxy 可用性与仍然不是 true breadth。

## 非目标

- 不购买 Norgate / FMP upgrade / 其他 paid source。
- 不把 ETF equal/cap price ratio、sector ETF ratio 或 top-N 自建 proxy 声称为 true
  breadth。
- 不训练 challenger model，不恢复 first-layer validation、promotion、paper-shadow、
  production 或 broker action。
- 不修改 active first-layer selection rule、promotion gate policy 或真实仓位配置。

## 验收标准

- 生成 `docs/research/equal_weight_proxy_data_fix_report.md`。
- 生成 `outputs/research_trends/equal_weight_proxy_data_fix/updated_proxy_coverage_matrix.json`。
- 生成 `outputs/research_trends/equal_weight_proxy_data_fix/blocked_proxy_resolution_status.json`。
- 若补齐价格缓存，必须先后运行同源 cached-data quality gate，并在产物中披露状态。
- 输出必须披露 RSP / QQQE 的 root cause、provider attempt、cache status、symbol mapping
  status、replacement_for_true_breadth=false 和 safety fields。
- 如果 provider 或缓存补齐失败，必须记录 blocker、影响、风险、验证覆盖和移除条件。

## 进展

- 2026-06-28：新增并进入 `IN_PROGRESS`；本批承接 TRADING-2271 / TRADING-2273 的
  RSP / QQQE missing blocker，只做免费价格数据补齐和 coverage matrix 修复审计。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends equal-weight-proxy-data-fix`、data fix module、focused tests、report
  registry、artifact catalog 和 system flow 更新。真实 run 使用 `--as-of 2026-06-26`
  和 `--repair-start 2018-01-01`，Yahoo Finance via yfinance 对 RSP / QQQE 均返回
  2018-01-02～2026-06-26 的 2132 行；写入 price cache 后 data_quality_before/after
  均为 `PASS_WITH_WARNINGS`，`rsp_to_spy` / `qqqe_to_qqq` 在 updated coverage matrix
  中变为 `covered`，但 `replacement_for_true_breadth=false`。
- 2026-06-28：验证通过 `validate-data --as-of 2026-06-26`（`PASS_WITH_WARNINGS`，
  errors=0）、Ruff、compileall、focused parallel pytest（4 passed）、docs freshness、
  `git diff --check` 和 contract-validation tier（193 passed，runtime artifact
  `outputs/validation_runtime/contract-validation_20260628T134538Z/test_runtime_summary.json`）。
