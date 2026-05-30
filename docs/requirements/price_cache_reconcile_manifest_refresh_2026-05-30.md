# TRADING-053B Price Cache Reconcile & Backtest Manifest Refresh

## 状态

- task id: `TRADING-053B`
- priority: `P0`
- status: `VALIDATING`
- owner: system
- started: 2026-05-30

## 背景

`TRADING-053A` 已把 portfolio sensitivity 的阻断从普通 missing price source
收敛为显式的数据生命周期问题：

- `market_data=2026-05-29`
- `latest valid manifest=2026-05-28`
- `latest_resolution=MISMATCH`
- `GOOGL`、`BRK.B`、`SGOV` 为 `MANIFEST_PRICE_CACHE_MISMATCH`

当前要修复的是 primary price cache、repaired price history、backtest input manifest
和 latest resolution 的一致性，而不是 portfolio sensitivity 策略逻辑。

## 目标

- 将 `aits data reconcile-price-cache --latest` 从 `NOT_IMPLEMENTED` 推进为可执行。
- 支持 `--refresh-manifest-only`、`--register-repaired-only`、`--symbols` 和 dry-run。
- 新增 `aits data refresh-backtest-manifest --latest` / `--dry-run`。
- 如果存在可审计 repaired price history，将其注册进统一 price cache registry，并刷新 latest backtest input manifest。
- 生成 `artifacts/data_quality/YYYY-MM-DD/price_cache_reconcile_summary.json/md`。
- 可选生成或更新 `artifacts/data_registry/price_cache_registry.json`。
- reconcile 后 `aits data inspect-registry --latest` 应能解析 latest market data 与 latest valid manifest 的一致状态。
- 不生成伪价格，不降低 data quality gate，不修改 production 参数，不解除 candidate promotion。

## 非目标

- 不修改 portfolio sensitivity 的策略、profile ranking 或参数逻辑。
- 不绕过 `validate-data`。
- 不下载新数据，除非后续 owner 明确批准 repair 数据源执行。
- 不修改 `config/parameters/production/current.yaml`。
- 不新增 trading signal，不触发真实交易。

## 实施步骤

1. 扩展 data registry consistency，支持读取 lightweight price cache registry。
2. 新增 price cache reconcile 模块，执行 latest context 解析、required assets 解析、repaired artifacts 检查、registry 更新、manifest refresh 和 after-state 验证。
3. CLI 接入：
   - `aits data reconcile-price-cache --latest`
   - `aits data reconcile-price-cache --date YYYY-MM-DD`
   - `aits data reconcile-price-cache --latest --refresh-manifest-only`
   - `aits data reconcile-price-cache --latest --register-repaired-only`
   - `aits data reconcile-price-cache --symbols GOOGL BRK.B SGOV`
   - `aits data refresh-backtest-manifest --latest`
4. Dashboard / Reader Brief 展示 price cache reconcile 摘要和 remaining limitations。
5. 更新 system flow、artifact catalog、report registry 和测试。

## 验收标准

- `aits data reconcile-price-cache --latest` 生成 reconcile summary JSON/Markdown。
- `aits data inspect-registry --latest` 不再把已注册 repaired assets 诊断为普通 manifest/cache mismatch；若仍失败，必须输出明确 blocker。
- `aits data validate-backtest-manifest --latest` 显示 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 的最新一致性状态。
- `aits portfolio sensitivity --latest` 使用 refreshed manifest；若数据仍不可用，输出新的明确 error code。
- `aits parameters shadow-backtest --latest --dry-run` 仍保持 `promotion_status=rejected` 和安全字段。
- 专项测试、全量 `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts`、`git diff --check` 通过。

## 开放问题

- 如果当前 workspace 中不存在可审计 repaired price rows，则 actual reconcile 必须 fail closed，并提示需要重新运行 price repair；不得生成假价格。
- 如果 repaired rows 只存在于旧 manifest 语义而不在任何文件中，应由 owner 决定是否重新下载/repair，而不是在 reconcile 中补造。

## 进展记录

- 2026-05-30: 新增需求文档并进入 `IN_PROGRESS`。本任务承接 TRADING-053A 的真实 blocker，目标是修复 cache/manifest/latest lifecycle，不改变投资策略或 production 参数。
- 2026-05-30: 实现可执行 reconcile / manifest refresh 并进入 `VALIDATING`。真实运行从 audited FMP raw cache 注册 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 各 1008 行到主价格缓存，写出 `artifacts/data_registry/price_cache_registry.json` 和 `artifacts/data_quality/2026-05-28/price_cache_reconcile_summary.json/md`。由于可审计 repaired rows 只覆盖到 2026-05-28，系统没有补造 2026-05-29 行，而是把 latest resolution 收敛到 required-asset common date `2026-05-28`；`aits portfolio sensitivity --latest` 已通过 data gate 并返回 `LIMITED`。
