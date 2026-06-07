# TRADING-057A Market Data Refresh & Freshness Recovery Workflow

最后更新：2026-06-07

## 背景

TRADING-057 已完成 market data freshness/readiness gate。真实 latest 结果显示：

```text
aits data freshness --latest
freshness_status=STALE
tracking_date=2026-05-29
effective_data_date=2026-05-28
tracking_readiness=cannot_track
tracking_status_recommendation=tracking_blocked
```

这说明 candidate、review、tracking、manifest 和 registry 链路已经能正确拒绝 stale
数据继续推进。当前缺口是：当 freshness 已判定 `STALE` 时，系统缺少正式、可审计、
fail-closed 的 market data refresh 与 freshness recovery 流程。

## 目标

- 在 freshness=`STALE` 时生成明确 refresh plan。
- 安全刷新 required assets 的最新日线价格数据。
- 优先复用 audited raw cache、项目现有 FMP/Yahoo provider、primary price cache upsert、
  registry update 和 backtest manifest refresh 能力。
- 更新 primary price cache registry 与 backtest input manifest。
- 重新运行 market data freshness 与 portfolio candidate tracking。
- 输出 `artifacts/data_refresh/YYYY-MM-DD/market_data_refresh_plan.json`、
  `market_data_refresh_summary.json` 和 `market_data_refresh_summary.md`。
- Dashboard、Reader Brief、candidate tracking 和 shadow backtest 能读取 refresh 摘要。
- 保持 `production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。

## 非目标和安全边界

- 不修改 `config/parameters/production/current.yaml`。
- 不启用 candidate profile，不解除 promotion 禁止，不做真实交易。
- 不新增信号、不调整组合参数、不降低 data quality gate。
- 不使用 mock 数据，不生成 synthetic latest bar，不把 `GOOG` 价格伪装成 `GOOGL`。
- 外部数据源尚未提供 target date 时，输出 `SOURCE_DELAYED` 并保持 tracking blocked。

## 新增状态

`market_data_refresh_status`：

- `NOT_NEEDED`
- `PLANNED`
- `RUNNING`
- `OK`
- `PARTIAL`
- `SOURCE_DELAYED`
- `FAILED`
- `BLOCKED`

## 实施步骤

1. 新增 `config/data/market_data_refresh.yaml`，记录 refresh mode、source order、
   required assets、registry/manifest 要求和安全字段。
2. 新增 market data refresh module，负责读取 latest freshness report、生成 refresh plan、
   获取 target date required asset bars、写入 primary price cache、更新 registry、
   刷新 manifest、重跑 freshness 与 candidate tracking。
3. 新增 CLI：
   - `aits data refresh-market --latest`
   - `aits data refresh-market --latest --dry-run`
   - `aits data refresh-market --latest --plan-only`
   - `aits data refresh-market --date YYYY-MM-DD`
   - `aits data refresh-market --symbols ...`
   - `aits data validate-refresh --latest`
   - `aits data recover-freshness --latest`
   - `aits reports data-refresh --latest`
4. 集成 candidate tracking、shadow backtest、Dashboard、Reader Brief、report registry
   与 artifact catalog。
5. 更新 `docs/system_flow.md`，明确 freshness stale -> refresh recovery -> manifest ->
   freshness/tracking 的数据流。

## 验收标准

- `aits data refresh-market --latest --dry-run` 生成 refresh plan 且不写 primary price cache。
- `aits data refresh-market --latest` 生成 JSON/Markdown refresh summary。
- `aits data recover-freshness --latest` 执行 freshness -> refresh -> reconcile/manifest ->
  freshness -> tracking 链路。
- 若 target date required bars 可用，freshness 恢复 `OK`，candidate tracking 恢复
  `active_tracking`。
- 若数据源尚未提供 target date required bars，refresh 输出 `SOURCE_DELAYED`，
  freshness 继续 `STALE`，candidate tracking 保持 `tracking_blocked`，原因清晰。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected`，
  并可引用 refresh/tracking artifacts。
- Dashboard 和 Reader Brief 展示 refresh 状态、source、before/after freshness、
  candidate tracking recovery 和 remaining limitations。
- 专项测试、全量 `pytest`、`ruff`、`compileall` 和 `git diff --check` 通过。
- `config/parameters/production/current.yaml` 无 diff。

## 进展记录

- 2026-05-30：新增并进入 `IN_PROGRESS`。原因：TRADING-057 latest 已正确把
  stale market data 阻断为 `tracking_blocked`，下一步需要正式 refresh/recovery
  workflow，区分可恢复 stale cache 与供应商尚未更新的 `SOURCE_DELAYED`。
- 2026-05-30：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成 refresh config、核心
  workflow、CLI、JSON/Markdown 报告、candidate tracking、shadow backtest、
  Dashboard、Reader Brief、report registry、artifact catalog 和 system flow 集成。
  真实 latest refresh 已恢复 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 的 2026-05-29
  日线后，`aits data freshness --latest` 为 `OK`，`aits portfolio track-candidate
  --latest` 为 `active_tracking`，`aits parameters shadow-backtest --latest --dry-run`
  仍保持 `promotion_status=rejected`。全量 `pytest`、`ruff`、`compileall` 和
  `git diff --check` 通过，`config/parameters/production/current.yaml` 无 diff。
- 2026-06-07：从 `VALIDATING` 改为 `DONE`。原因：最新 `aits data validate-refresh
  --latest` 读取 `artifacts/data_refresh/2026-06-05/market_data_refresh_summary.json`
  并返回 `refresh_status=OK`、`production_effect=none`、`manual_review_required=true`
  和 `auto_promotion=false`；refresh/recovery 链路验收已闭合，promotion 仍保持禁用。
