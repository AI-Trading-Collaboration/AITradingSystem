# TRADING-1286 to 1305 Signal Validity Staleness Repair

状态：VALIDATING  
最后更新：2026-06-27

## 背景

TRADING-1256 to 1285 的 actual-path owner review 与 policy sensitivity 结论显示，`limited_adjustment` 与 `dynamic_v0_5_ai_trend_confirmed_only` 仍为 surviving dynamic candidates，但状态只能是 `WATCH_ONLY`。两个候选均受 short-validity / signal staleness sensitivity 与相对 `100_qqq` static baseline underperformance 约束，dynamic promotion 必须继续 `BLOCKED`。

本批只研究这两个 watch-only 候选的 signal validity、staleness cost 和 execution lag sensitivity，不恢复 promotion，不进入 paper-shadow，不让 target-path metrics 进入 ranking 或 readiness 正向结论。

## 范围

- 读取现有 owner review、policy sensitivity 与 actual-path rebacktest evidence，生成 `signal_validity_staleness_input_summary`。
- 固化 signal validity taxonomy，至少包含 `fast_signal`、`medium_signal`、`slow_signal` 和 `persistent_regime_signal`。
- 为 `limited_adjustment` 与 `dynamic_v0_5_ai_trend_confirmed_only` 绑定 signal validity profile。
- 在 execution semantics actual-path 层新增 staleness-aware execution filter，显式区分 signal generation / observation / advisory / executable / actual execution dates。
- 生成 repaired watch-only variants：
  - `limited_adjustment_staleness_aware_v1`
  - `dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1`
- 重跑 actual-path rebacktest，输出 staleness / lag decomposition、repair summaries、owner-readable review 和机器可读 staleness repair matrix。
- 补充 no-future-leakage、actual-path-only promotion 和 stale action semantics tests。
- 更新 report registry、artifact catalog、system flow 与 task register。

## 非目标

- 不恢复 dynamic promotion。
- 不进入 paper-shadow 或 paper-shadow preflight 自动通过。
- 不允许 target-path metrics 参与 promotion、ranking 或 owner decision 正向依据。
- 不新增 unrelated strategy family 或大范围参数搜索。
- 不把单次收益改善解释为 promotion eligibility。

## 阶段拆解

|阶段|任务|验收标准|状态|
|---|---|---|---|
|1|TRADING-1286～1288 输入摘要、taxonomy、profile 绑定|新增 tracked summary/config；两个候选 profile 有 owner/rationale/review condition|DONE|
|2|TRADING-1289～1292 staleness filter 与 decomposition|actual path rows 有标准日期字段；stale action 可复现；输出 per-strategy JSON/Markdown decomposition|DONE|
|3|TRADING-1293～1296 repaired variants、matrix、owner review|两个 repaired variants 完成 actual-path run；matrix verdict 明确；promotion 仍 blocked|DONE|
|4|TRADING-1297～1301 tests、registry/catalog/system flow/CLI|新增 focused tests；CLI 支持 staleness repair 参数；文档索引通过|DONE|
|5|TRADING-1302～1305 final gate、validation、commit/push|final gate status 为 dynamic promotion BLOCKED；指定验证通过；本地提交并按 upstream 规则推送|VALIDATING|

## 设计约束

- Staleness filter 只能使用 `signal_generation_date` 到 `actual_execution_date` 已知的信息，不能读取未来收益、未来价格路径或未来 signal persistence。
- Repaired variants 必须继承原策略 target path 主体逻辑，只调整 signal validity / execution filter / stale action。
- `leaderboard_actual_path.csv` 仍只能以 `actual_path_*` metrics 排序。
- `metrics_target_path.json` 与 target-vs-actual gap 只能作为 diagnostic evidence。
- `dynamic_promotion.final_status` 必须保持 `BLOCKED`，即使 repaired variant 改善也最多输出 `PAPER_SHADOW_PREFLIGHT_CANDIDATE` 作为后续 owner review 输入。

## 验收命令

```bash
python -m ruff check src tests
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py
python -m pytest -n 16 --dist loadfile tests/test_external_validation.py
python -m pytest -n 16 --dist loadfile tests/test_signal_staleness_repair.py
python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py
git diff --check
```

如果 CLI 参数完成，还要执行 staleness repair dry-run：

```bash
python -m ai_trading_system.cli research strategies execution-semantics-rebacktest \
  --policy-registry config/research/strategy_execution_policy_registry.yaml \
  --signal-validity-taxonomy config/research/signal_validity_taxonomy.yaml \
  --enable-staleness-filter \
  --include-repaired-watch-only \
  --emit-staleness-decomposition \
  --emit-lag-decomposition \
  --output-root outputs/research_strategies/execution_semantics
```

## 进展记录

- 2026-06-27：新增任务文档并进入 `IN_PROGRESS`。实现范围限定为 watch-only research evidence；dynamic promotion、paper-shadow、production、broker 全部保持关闭。
- 2026-06-27：实现完成并转入 `VALIDATING`。真实 staleness repair dry-run 已生成 `signal_validity_staleness_input_summary.md`、`staleness_repair_matrix.yaml`、`signal_validity_staleness_repair_review.md` 和 runtime decomposition artifacts；两个 repaired variants 的 matrix verdict 均为 `NO_MATERIAL_IMPROVEMENT`，dynamic promotion 继续 `BLOCKED`，paper-shadow preflight candidate 未识别。验证通过 Ruff、compileall、指定并行 pytest suites 和 `git diff --check`。
