# TRADING-081 Weight Calibration Profiling and Hotspot Baseline

## Status

- 父任务：TRADING-081
- 优先级：P0
- 状态：VALIDATING
- 下一责任方：系统实现 + 项目 owner 人工复核
- 当前阶段：profiling baseline 已实现并通过工程验证；下一步运行 production-like cold diagnostics profiling 样本，由 owner 复核 hotspot evidence 后决定 TRADING-082 方向。

## Context

TRADING-080 已经完成 cache、deterministic cache keys、manifest validation、price / returns matrix cache、candidate / regime / aggregation cache、parallel runner、resume manifest、performance report 和 validation gate。生产级样本显示：

```text
post-fix cold   1378.734s
post-fix warm   2.570s
post-fix resume 2.610s
warm/resume cache_hit_rate = 1.0
```

结论是重复计算问题已经解决，但 cold run 仍处于 20-30 分钟 profiling trigger zone。TRADING-081 的目标是先回答“cold run 时间花在哪里”，再决定 TRADING-082 是否需要 targeted numerical optimization。

## Safety Boundary

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`

本任务只允许生成 profiling/report/validation artifacts，不允许 broker execution、production weight mutation、automatic candidate promotion、baseline config mutation、C/C++/Rust extension、Numba rewrite、Polars migration 或 backtest engine rewrite。

## Stage Breakdown

|Stage|Status|Acceptance|
|---|---|---|
|TRADING-081A Profiling Policy Config|BASELINE_DONE|`config/etf_portfolio/profiling_policy.yaml` 存在；modes/thresholds/top_n/safety 校验 fail closed；invalid mode 和 unsafe safety 会失败。|
|TRADING-081B Step-Level Runtime Profiler|BASELINE_DONE|轻量 step profiler 支持 off/summary/detailed/cprofile mode；记录 duration/status/slow warning；profiling off 低开销。|
|TRADING-081C Candidate-Level Hotspot Table|BASELINE_DONE|Detailed profiling 可输出 deterministic JSON/CSV/Markdown candidate hotspot table，包含 candidate/search/preset/cache/readiness/risk/timing fields。|
|TRADING-081D Optional cProfile Artifact|BASELINE_DONE|`--profile cprofile` 才生成 `cprofile.stats`、top functions JSON/Markdown；normal run 不强制 cProfile。|
|TRADING-081E Cache Timing Breakdown|BASELINE_DONE|按 cache layer 汇总 hit/miss/write/read/write/validation/serialization seconds 和 slow cache entries。|
|TRADING-081F Parallel Worker Timing Breakdown|BASELINE_DONE|按 worker_id 汇总 assigned/completed/failed/runtime/mean/max/cache hit/miss，可用于判断 worker imbalance。|
|TRADING-081G Vectorization Audit Report|BASELINE_DONE|输出 vectorization audit，优先建议 Python/NumPy/precompute/batching；native_extension_needed 默认为 false。|
|TRADING-081H Regime Mask Precomputation Assessment|BASELINE_DONE|按 regime definition 输出 mask build time、reuse count、candidate count、potential saved seconds 和 precompute recommendation。|
|TRADING-081I Profiling Report Generator|BASELINE_DONE|生成 readable JSON/Markdown profiling report，包含 safety、metadata、step/candidate/cache/worker/cProfile/audit/assessment/recommendations。|
|TRADING-081J Reader Brief Profiling Section|BASELINE_DONE|Reader Brief 只读 latest profiling report，缺失时不运行上游、不补造 profiling 结论。|
|TRADING-081K Profiling Validation Gate|BASELINE_DONE|`aits etf weight-calibration profiling-validate` fail closed 校验 A-J availability 和 safety boundary。|

## Design Decisions

1. TRADING-081 保持 measurement-first，不做 kernel rewrite。优化建议只能来自 profiling evidence。
2. Default CLI profile mode 使用 `summary` 或既有 performance-report 级别信息；`cprofile` 必须显式请求。
3. Runtime profiling artifacts 写入 ignored `reports/etf_portfolio/weight_calibration/profiling/` 或用户指定 output path，不进入源码提交。
4. Candidate/worker/cache timing 是 diagnostics 旁路 metadata，不改变 ranking、metrics、robustness、overfit gating 或 candidate readiness。
5. 所有 recommendations 都保持 proposal-only / manual-review-required，不自动注册 candidate、不 enroll shadow、不修改 production weights。

## Acceptance Criteria

- Profiling policy config exists and fails closed on invalid/unsafe fields.
- Step-level profiler exists and can be used by diagnostics runs.
- Candidate hotspot table exists and is deterministic.
- Optional cProfile artifacts exist only when `--profile cprofile` is requested.
- Cache timing breakdown shows read/write/serialization overhead and hit rate.
- Worker timing breakdown shows parallel utilization and imbalance.
- Vectorization audit report exists and does not recommend native extensions without evidence.
- Regime mask precomputation assessment exists and can inform later optimization.
- Profiling report generator writes JSON/Markdown with safety banner and next-step recommendation.
- Reader Brief can surface profiling summary only from existing report artifacts.
- Profiling validation gate passes and fails closed on unsafe policy.
- Existing commands continue working: `diagnostics`, `performance-validate`, `usability-validate`.
- `python -m pytest tests -q`、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `python -m ai_trading_system.cli etf weight-calibration profiling-validate` 通过。

## Progress Notes

- 2026-06-04: 新增并进入 IN_PROGRESS，原因：TRADING-080 生产级 post-fix 证据显示 warm/resume cache 性能优秀，但 cold run 约 1378.7s；需要 profiling baseline 先定位 slowest pipeline steps、functions、candidates、cache overhead、worker imbalance、vectorization opportunities 和 regime slicing cost，再决定 TRADING-082 是否进入 targeted numerical optimization。
- 2026-06-04: TRADING-081A~K baseline 实现完成并转入 VALIDATING，原因：新增 profiling policy、diagnostics `--profile off/summary/detailed/cprofile`、step/candidate/cache/worker timing、optional cProfile artifacts、vectorization audit、regime mask assessment、profiling report、Reader Brief profiling section、report registry entry 和 `profiling-validate` gate；专项与相关测试 `tests/test_etf_weight_calibration_profiling.py tests/test_etf_weight_calibration.py tests/test_reader_brief.py tests/test_report_index.py` 共 129 passed，`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `aits etf weight-calibration profiling-validate` 已通过。下一步需要 production-like cold profiling 样本和 owner manual review 后再决定 TRADING-082 是 shadow-ready review playbook 还是 targeted numerical optimization。
