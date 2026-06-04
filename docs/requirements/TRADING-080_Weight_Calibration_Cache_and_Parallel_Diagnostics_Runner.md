# TRADING-080 Weight Calibration Cache and Parallel Diagnostics Runner

## Status

- 父任务：TRADING-080
- 优先级：P0
- 状态：VALIDATING
- 下一责任方：系统验证 + 项目 owner
- 当前阶段：真实生产 raw price cache cold/warm/resume 已验证；cold startup 仍可作为 TRADING-081 profiling 触发条件候选，warm/resume cache performance 已满足 TRADING-080 目标。

## Context

TRADING-079 已经能运行 multi-preset historical weight search diagnostics，输出 cross-preset stability、near-shadow candidates、rescue suggestions 和 shadow minimum criteria。真实运行覆盖 24 个 preset/search 组合与 240 个 candidate observations 时，外层工具 30 分钟超时但底层 Python 最终完成，说明 workflow 有价值但不适合频繁迭代。

TRADING-080 的目标是让 diagnostics 具备 cache-aware、parallel-capable、resumable、auditable research workflow。所有能力只服务 candidate-only / observe-only research，不允许 broker execution、production weight mutation 或 automatic promotion。

## Safety Boundary

- `observe_only=true`
- `candidate_only=true`
- `production_effect=none`
- `broker_action=none`
- `manual_review_required=true`

任何 cache entry、run manifest、performance report、validation output 或 diagnostics report 都必须保留上述安全字段。并行 worker 不得写 production configs、baseline weights、broker artifacts 或 tracked source files。

## Stage Breakdown

|Stage|Status|Acceptance|
|---|---|---|
|TRADING-080A Cache Policy Config|BASELINE_DONE|`config/etf_portfolio/cache_policy.yaml` 存在并校验 cache roots、layers、TTL、worker bounds、locking、pruning 和 safety fields。|
|TRADING-080B Cache Key and Manifest Schema|BASELINE_DONE|Cache key builder deterministic；key inputs 强制包含 `source_config_hash`、`data_hash`、`engine_version` 和 layer-specific inputs；manifest 校验 schema/safety/config/data/engine。|
|TRADING-080C Price / Returns Matrix Cache|BASELINE_DONE|Diagnostics hot path 已接入 `price_returns_matrix` cache lookup / read / write；同一 prices/config 第二次运行命中，date range 或 data hash 变化会 miss。|
|TRADING-080D Candidate Backtest Result Cache|BASELINE_DONE|Per-candidate metrics / backtest daily payload 已写入 `candidate_backtest` cache layer；第二次相同输入运行命中 cache，并保留 ranking / metrics 投资输出等价。|
|TRADING-080E Regime Robustness Cache|BASELINE_DONE|Per-candidate regime robustness payload 已写入 `regime_robustness` cache layer；第二次相同输入运行命中 cache，并保留 robustness evaluation 输出等价。|
|TRADING-080F Diagnostics Aggregation Cache|BASELINE_DONE|diagnostics aggregation 可按 search ids、preset ids、candidate/result hashes 和 diagnostics config hash 命中或重算。|
|TRADING-080G Parallel Candidate Runner|BASELINE_DONE|worker count 可配置，candidate cache miss path 已通过 parallel runner 执行；`workers=1` 与 `workers=2` 输出 deterministic 等价，异常收集。|
|TRADING-080H Resume / Incremental Run Support|BASELINE_DONE|run manifest 记录 planned/completed/failed steps、candidate statuses、cache summary 和 status transitions；resume cache-hit 会用指定 run id 写入新的 `runs/<run_id>/run_manifest.json`。|
|TRADING-080I Runtime Performance Report|BASELINE_DONE|输出 JSON/Markdown performance report，包含 runtime、worker_count、cache hit/miss/write、slowest step 和 resume status。|
|TRADING-080J Cache and Parallel Runner Validation Gate|BASELINE_DONE|`aits etf weight-calibration performance-validate` fail-closed 校验 A-I availability 和 safety boundary。|

## Design Decisions

1. Cache key 使用 canonical JSON + SHA256，不使用 Python object repr，避免跨进程或字段顺序导致 key drift。
2. 所有 cache key 都要求 common inputs：`source_config_hash`、`data_hash`、`engine_version`。Layer-specific inputs 在 common inputs 之外继续强制校验。
3. Runtime cache 默认位于 `data/cache/weight_calibration/`，并通过 `.gitignore` 保持 untracked。
4. Candidate-level 和 regime-level cache 写入由 coordinator 统一完成；parallel worker 只返回 validated payload，不直接写 cache 文件，避免跨进程写入竞争。
5. `workers=1` 是测试和 debug 的 deterministic path；`workers=auto` 受 cache policy 的 `parallel.max_workers` 上限约束。
6. ETF price standardization 不得用运行时当前时间补齐缺失或空白 `created_at`；缺失 metadata 使用 deterministic placeholder，避免同一价格文件重复加载导致 `data_hash` 漂移。

## Acceptance Criteria

- Cache policy config、loader 和 validation tests 完成。
- Cache key builder 和 manifest schema tests 完成，unsafe safety / missing key inputs fail closed。
- Diagnostics CLI 支持 `--cache`、`--no-cache`、`--force-refresh`、`--workers`、`--resume`、`--run-id` 和 `--include-performance-report`。
- Performance validation gate 可生成 JSON / Markdown，并在 unsafe policy 下 fail closed。
- Price / returns matrix cache 已进入 diagnostics hot path，cache status 和 key 写入 `cache_summary`。
- Candidate backtest cache、regime robustness cache 和 diagnostics aggregation cache 均有 hit / miss / write 可见性；cache hit 不改变 ranking、metrics 或 robustness output。
- Parallel candidate miss runner 在 `workers=1` 与多 worker 下保持 deterministic output。
- 完整 fixture diagnostics 运行覆盖 24 个 preset/search 组合与 240 个 candidate observations；cold / warm / resume / partial-config evidence 已记录。
- README、artifact catalog、system flow、operations runbook、task register 同步。
- `python -m pytest tests/test_etf_portfolio.py tests/test_etf_weight_calibration_cache.py tests/test_etf_weight_calibration.py -q` 通过。
- 全量 `python -m pytest tests -q` 通过。
- `python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 通过。
- 默认 policy 与 fixture policy 的 `python -m ai_trading_system.cli etf weight-calibration performance-validate` 均通过。

## Progress Notes

- 2026-06-04: 新增并进入 IN_PROGRESS，原因：TRADING-079 真实 diagnostics 长运行暴露 full recomputation 成本，需要先建立 cache policy、deterministic key/manifest、runtime cache 目录约束、parallel/resume/performance validation 基础闭环，再逐层接入 candidate 和 regime cache。
- 2026-06-04: 完成 TRADING-080 baseline implementation。新增 cache policy config、`weight_calibration_cache.py`、deterministic cache key / manifest、price/returns cache payload writer、diagnostics aggregation warm cache、run manifest、generic parallel task runner、performance report、`performance-validate` CLI、diagnostics cache/workers/resume/performance flags、README / artifact catalog / system flow / task register updates 和专项测试。验证通过 `tests/test_etf_weight_calibration_cache.py`、`tests/test_etf_weight_calibration.py`、全量 `python -m pytest tests -q`（2120 passed）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `python -m ai_trading_system.cli etf weight-calibration performance-validate`。父任务仍保持 IN_PROGRESS，因为 candidate backtest cache 与 regime robustness cache 尚未接入 per-candidate hot path。
- 2026-06-04: 推进 candidate hot path。`run_historical_weight_search` 现在按 candidate 构建 `candidate_backtest` 与 `regime_robustness` cache key，read-write 模式下可写入 per-candidate payload、后续命中并暴露 cache status；cache miss 的 candidate backtest / robustness 计算接入 parallel runner，workers=1 与 workers=2 输出等价；补充验证 candidate cache 命中但 regime cache 被清理时，可从 cached daily 精确重建 robustness 并写回 regime cache。验证通过 `python -m pytest tests/test_etf_weight_calibration_cache.py tests/test_etf_weight_calibration.py -q`（120 passed）、全量 `python -m pytest tests -q`（2123 passed）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check` 和 `python -m ai_trading_system.cli etf weight-calibration performance-validate`（PASS）；父任务仍保持 IN_PROGRESS，因为还需要完整 multi-preset diagnostics warm-cache speedup / resume evidence。
- 2026-06-04: 完成 TRADING-080 baseline validation 并转入 VALIDATING。补齐 `price_returns_matrix` diagnostics hot path、cache-hit run manifest 写入、diagnostics CLI cache count 输出，并修复 synthetic CASH `created_at` 使用运行时当前时间导致相同 prices file `data_hash` 漂移的问题。由于本机默认 `data/etf_portfolio/prices.csv` 缺失，完整运行证据使用 ignored `data/cache/trading080_validation/prices.csv` fixture，并先通过 `aits etf data validate`。完整 cold run 覆盖 24 个 preset/search 组合、240 个 candidate observations，`worker_count=8`、`price_returns_matrix_cache_status=miss_written`、`diagnostics_aggregation_cache_status=miss_written`、performance runtime 90.681s；相同输入 warm run `price_returns_matrix_cache_status=hit`、`diagnostics_aggregation_cache_status=hit`、`cache_hit_count=2`、runtime 0.156s；`--resume --run-id trading080-full-validation` 生成 `runs/trading080-full-validation/run_manifest.json`，`resume_status=resumed`、runtime 0.154s；partial config `top=5` 使 aggregation miss/write，但 lower cache 命中，`cache_hit_count=481`、`cache_miss_count=1`、runtime 16.6s。同步补充 operations runbook 的 manual/governance cadence 与 scheduler validation 条目。最终验证通过 ETF 专项 `python -m pytest tests/test_etf_portfolio.py tests/test_etf_weight_calibration_cache.py tests/test_etf_weight_calibration.py -q`（159 passed）、全量 `python -m pytest tests -q`（2124 passed, 330 warnings）、`python -m ruff check config src tests scripts docs`、`python -m compileall -q src tests scripts`、`git diff --check`、默认 policy `performance-validate` 和 fixture policy `performance-validate`（均 PASS）。后续进入真实默认 ETF price cache production-like 观察，不再需要新增实现才能进入验证。
- 2026-06-04: 真实生产样本 Route A 首轮验证暴露 cache key 漂移，任务从 VALIDATING 回到 IN_PROGRESS。默认 `data/raw/prices_daily.csv` 通过 `aits etf data validate`，cold run 使用 `--force-refresh` 覆盖 24 个 preset/search 组合、240 个 candidate observations，wall runtime 1112.724s、`worker_count=8`、`shadow_ready_count=37`、`cache_hit_count=0`；紧接着 warm run 仍为 `price_returns_matrix_cache_status=miss_written`、`diagnostics_aggregation_cache_status=miss_written`、`cache_hit_count=0`、wall runtime 1098.194s，且 `shadow_ready_count` 变为 39。直接原因是 production raw price rows 的 `updated_at` 为空，`standardize_price_frame` 用运行时当前时间补齐 `created_at`，导致同一文件每次加载的 `data_hash` 不同。下一步先修复 deterministic metadata normalization，再重跑 production cold/warm/resume；此问题不是 TRADING-081 profiling 条件。
- 2026-06-04: 修复 production raw price metadata determinism 并重新转入 VALIDATING。`standardize_price_frame` 对缺失或空白 `created_at` 改用 deterministic metadata placeholder，不再用运行时当前时间；补充 production-style empty metadata regression test；同一 `data/raw/prices_daily.csv` 连续标准化得到相同 `price_returns_matrix` `data_hash/cache_key`。Post-fix production cold run 覆盖 24 个 preset/search 组合、240 个 candidate observations，`shadow_ready_count=39`、`worker_count=8`、wall runtime 1378.734s、performance runtime 1375.764s、`cache_hit_count=0`、`cache_miss_count=6464`、`cache_write_count=12926`、`slowest_step=run_searches`；紧接 warm run wall runtime 2.570s、performance runtime 0.382s、`price_returns_matrix_cache_status=hit`、`diagnostics_aggregation_cache_status=hit`、`cache_hit_rate=1.0`、`cache_hit_count=2`、`cache_miss_count=0`；`--resume --run-id trading080-production-validation` wall runtime 2.610s、performance runtime 0.390s、`resume_status=resumed`、`cache_hit_rate=1.0`。结论：TRADING-080 的重复计算问题已解决；cold startup 超过 20 分钟，可作为是否启动 TRADING-081 profiling 的 owner 复核输入。
