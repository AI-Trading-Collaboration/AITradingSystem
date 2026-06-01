# TRADING-053A Data Repair Persistence & Latest Artifact Consistency

最后更新：2026-05-30

## 状态

- task id: `TRADING-053A`
- priority: `P0`
- status: `VALIDATING`
- owner: system
- started: 2026-05-30

## 背景

`TRADING-053` portfolio sensitivity 框架已完成，但真实 `aits portfolio sensitivity --latest`
被 `aits validate-data` 阻断，原因是主价格源报告缺少 `GOOGL`、`BRK.B`、`SGOV`。
这与 `TRADING-049` price repair 结果矛盾：repair 与 backtest input diagnostics 已能看到这些
标的的补齐行和 `BRK.B -> BRK-B` provider symbol mapping。

当前要修复的不是 portfolio sensitivity 策略逻辑，而是 repair 结果、主价格缓存、
backtest input manifest、`validate-data`、latest resolution 和下游数据门禁之间的数据视图不一致。

## 目标

- 查明并报告 repair write path、validate-data read path、diagnostics read path 和 portfolio sensitivity read path 是否一致。
- 统一或显式记录 `--latest` 在 market data、backtest manifest、portfolio sensitivity artifact 之间的日期解析。
- 引入统一 symbol resolver，保证 `BRK.B` canonical symbol 与 `BRK-B` source symbol mapping 在 repair、diagnostics、validate-data 和 sensitivity 链路中一致。
- 新增 data registry consistency report，解释 repaired artifact 存在但 primary cache 未注册、symbol mapping 缺失、latest mismatch 等情况。
- 让 portfolio sensitivity 优先解析 latest valid backtest input manifest，并把 manifest context 暴露到 data gate 输出中。
- 不降低 `aits validate-data` 质量门禁，不临时补数，不用 mock 数据伪装通过，不修改 production 参数。

## 非目标

- 不修改 portfolio sensitivity 的策略/排名逻辑。
- 不绕过 `aits validate-data`。
- 不下载或伪造 `GOOGL`、`BRK.B`、`SGOV` 数据。
- 不修改 `config/parameters/production/current.yaml`。
- 不新增 trading signal，不解除 candidate promotion，不触发真实交易。

## 实施步骤

1. 新增统一 symbol resolver，复用 price repair 的 source symbol mapping，并输出 canonical/source audit。
2. 新增 data registry consistency 模块和 CLI：
   - `aits data inspect-registry --latest`
   - `aits data validate-backtest-manifest --latest`
   - `aits data reconcile-price-cache --latest --dry-run`
   - `aits data reconcile-price-cache --latest`
3. 生成 `artifacts/data_quality/YYYY-MM-DD/data_registry_consistency.json/md`。
4. 增强 `validate-data` 报告中的 missing price source 诊断，暴露读取路径，并区分 `MISSING_PRICE_SOURCE`、`UNREGISTERED_REPAIR_ARTIFACT`、`SYMBOL_MAPPING_MISSING`、`LATEST_DATE_MISMATCH`、`MANIFEST_PRICE_CACHE_MISMATCH`。
5. 调整 portfolio sensitivity data gate：先解析 latest valid backtest input manifest，记录 manifest context，再调用同一路 validate-data gate；如果失败，输出明确 data_gate error code 和 suggested action。
6. 更新 Dashboard / Reader Brief 对 data registry blocker 的展示。
7. 更新 `docs/system_flow.md`、`docs/artifact_catalog.md`、`docs/task_register.md` 和测试。

## 验收标准

- `aits data inspect-registry --latest` 输出 latest resolution、price cache paths 和 symbol mapping 状态。
- `aits data validate-backtest-manifest --latest` 显示 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 的 manifest/cache 一致性。
- `aits data reconcile-price-cache --latest --dry-run` 输出明确 repair/reconcile plan，不直接下载数据。
- `aits portfolio sensitivity --latest` 不再因为简单的 `missing primary price source for GOOGL, BRK.B, SGOV` 失败；若仍失败，必须输出明确 error code。
- Shadow backtest dry-run 保持 `promotion_status=rejected`、`production_effect=none`、`manual_review_required=true`、`auto_promotion=false`。
- 新增专项测试、全量 `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check` 通过。

## 开放问题

- 如果 future daily-run 又出现部分资产先更新、required assets 未共同覆盖的情况，latest resolution 应继续以 required-asset common date 为 data-dependent command 的回测窗口边界。
- 如果需要使用 2026-05-29 作为全资产回测日，应重新运行受审计的 price repair/download，而不是由 reconcile 生成伪价格。

## 进展记录

- 2026-05-30: 新增需求文档并进入 `IN_PROGRESS`。本任务是 `TRADING-053` 真实验收 blocker 的 P0 数据一致性修复，不降低数据质量门禁，不修改 production 参数。
- 2026-05-30: 实现统一 `symbol_resolver`、data registry consistency report、manifest context 下的 `validate-data` error code、portfolio sensitivity `data_gate`、CLI、Dashboard/Reader Brief 展示和专项测试。真实 `aits data inspect-registry --latest` 现在显示 latest market date `2026-05-29`、latest valid manifest `2026-05-28` 的 `LATEST_DATE_MISMATCH`，并把 `GOOGL`、`BRK.B`、`SGOV` 明确诊断为 `MANIFEST_PRICE_CACHE_MISMATCH`，不再误报为普通 missing primary price source。实际 cache reconcile 仍未自动实现，`aits data reconcile-price-cache --latest --dry-run` 只输出修复计划；非 dry-run 明确 `NOT_IMPLEMENTED`。
- 2026-05-30: 状态从 `IN_PROGRESS` 调整为 `VALIDATING`，原因：代码、文档和测试已完成；全量 `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts`、`git diff --check` 通过。后续需 owner/数据修复路径决定是否真实重建 primary price cache 或重新跑 repair。
- 2026-05-30: TRADING-053B 后复验，actual reconcile 已注册 `GOOGL`、`BRK.B via BRK-B`、`SGOV` 的可审计 repaired rows，`aits data inspect-registry --latest` 与 `aits data validate-backtest-manifest --latest` 不再报告 `MANIFEST_PRICE_CACHE_MISMATCH`；latest resolution 以 required-asset common date `2026-05-28` 收敛为 OK。
