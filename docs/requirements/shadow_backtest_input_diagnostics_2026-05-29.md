# Shadow Backtest Input Diagnostics

状态：DONE
任务：BTINPUT-002
最后更新：2026-05-29

## 背景

`aits parameters shadow-backtest --latest --dry-run` 已能在缓存数据不满足要求时
输出 `DEGRADED`，但当前 summary 只暴露粗粒度 data quality error，无法直接判断
shadow backtest 是因为资产缺失、历史区间不足、价格缺口、信号快照缺失还是缓存过期而
失败。

本任务补齐 shadow backtest 的数据输入闭环。诊断和 repair planning 均为只读
observe-only 行为，不修改 production 参数、不改变 promotion criteria、不触发真实交易。

## 目标

1. 新增 `aits data diagnose-backtest-inputs --latest/--date/--config`。
2. 新增 `aits data repair-backtest-inputs --latest --dry-run`，实际 repair 先返回
   `NOT_IMPLEMENTED` 并输出 repair plan。
3. 输出结构化 JSON、Markdown 和 backtest input snapshot manifest。
4. 检查 asset coverage、date coverage、price history completeness、signal snapshot
   availability 和 local cache freshness。
5. Shadow backtest summary 和 Markdown 在数据质量失败时引用最新 diagnostic report。
6. Daily task dashboard 展示 Backtest Data Quality 卡片，并在 Shadow Parameter Backtest
   卡片显示 data quality 状态。
7. Reader Brief 的 Parameter Shadow Review 补充数据质量摘要。

## 非目标

- 不修改 shadow 参数生成逻辑。
- 不修改 production 参数。
- 不调整 promotion criteria。
- 不接入真实交易。
- 不接入复杂 PIT 新闻数据库、期权数据或分钟级数据。
- 不自动下载或修复外部数据。

## 阶段拆解

1. 诊断模块与 artifact writer：生成 JSON、Markdown、snapshot manifest 和 repair plan。
2. CLI 接线：`aits data diagnose-backtest-inputs` 和 `aits data repair-backtest-inputs`。
3. Shadow backtest 集成：summary JSON/Markdown 引用 diagnostic report 和阻断摘要。
4. Dashboard / Reader Brief 集成：只读消费最新 diagnostic。
5. 测试：覆盖缺资产、历史不足、stale cache、缺信号、价格缺口、全 OK、repair dry-run、
   shadow link、dashboard 和 Reader Brief 摘要。

## 验收标准

- `aits data diagnose-backtest-inputs --latest` 生成：
  - `artifacts/data_quality/YYYY-MM-DD/backtest_input_diagnostics.json`
  - `artifacts/data_quality/YYYY-MM-DD/backtest_input_diagnostics.md`
  - `artifacts/backtest_snapshots/YYYY-MM-DD/backtest_input_manifest.json`
- `aits data repair-backtest-inputs --latest --dry-run` 输出可执行 repair plan。
- `aits parameters shadow-backtest --latest --dry-run` 的 JSON/Markdown 明确引用 diagnostic
  report，不再只展示笼统的 local cached data quality failure。
- 所有输出固定 `production_effect=none`，`can_promote_candidate=false` 直到输入质量满足 gate。

## 进展记录

- 2026-05-29：完成结构化诊断、snapshot manifest、repair dry-run plan、shadow
  backtest 引用、dashboard 和 Reader Brief 只读展示；目标测试通过。当前本地 latest
  诊断为 `FAILED`，真实阻断项为缺少 `GOOGL`、`BRK.B`、`SGOV` price history，实际
  repair 仍按本阶段边界返回 `NOT_IMPLEMENTED`。
