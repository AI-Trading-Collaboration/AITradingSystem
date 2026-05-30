# TRADING-059 Restricted Backtest Weight Tuning

最后更新：2026-05-30

## 背景

TRADING-049 到 TRADING-058A 已经完成 price repair、signal snapshot、ablation、calibration、portfolio sensitivity、portfolio candidate、manual review、shadow tracking、freshness gate、refresh recovery、tracking review 和 tracking window visibility。当前真实状态仍是：

- price data / registry / freshness：OK
- portfolio candidate tracking：active_tracking
- portfolio candidate：`lower_rebalance_threshold_2pct`
- tracking review：`VALIDATING` / `needs_more_data`
- production_effect：`none`
- auto_promotion：`false`
- `config/parameters/production/current.yaml` 不得修改

当前 baseline signal weights 主要是人工设定，缺少受限历史回测调参闭环。TRADING-059 只回答一个问题：在当前可用输入下，是否可以调出一组历史 walk-forward 表现优于 baseline 的 shadow signal weights。

## 范围

第一版实现 restricted grid search，不使用 LLM 生成权重，不使用 mock 数据，不自动写 production。信号质量仍为 `LIMITED` 时，输出只能是 advisory shadow weight candidate。

允许调参：

- `trend_momentum`：price-derived / real
- `sector_strength`：price-derived / real
- `macro_liquidity`：proxy / limited，受限调参
- `valuation_risk`：fallback 或 weak proxy，只允许 capped tuning

固定：

- `earnings_quality = 0.05`
- `event_risk = 0.05`

默认 portfolio profile 使用 `lower_rebalance_threshold_2pct`，并通过现有 portfolio construction simulation 路径评估 signal weights。

## 配置与安全边界

新增 `config/parameters/weight_tuning_v0_1.yaml`，必须包含：

- `production_effect: none`
- `manual_review_required: true`
- `auto_promotion: false`
- baseline 参数路径
- latest signal snapshot / backtest manifest readiness 要求
- tunable / capped / fixed weight scope
- L1 distance、weight sum、negative weight、fallback free tuning 禁止
- walk-forward 配置
- objective ranking weights
- guardrails

不得修改 `config/parameters/production/current.yaml`，不得把 recommended shadow weights 写入 production，不得降低 `aits validate-data` 质量门禁。

## CLI 与产物

新增：

- `aits parameters tune-weights --latest`
- `aits parameters tune-weights --date YYYY-MM-DD`
- `aits parameters tune-weights --config config/parameters/weight_tuning_v0_1.yaml`
- `aits parameters tune-weights --latest --dry-run`
- `aits parameters tune-weights --latest --portfolio-profile lower_rebalance_threshold_2pct`
- `aits parameters tune-weights --latest --signals trend_momentum sector_strength macro_liquidity`
- `aits parameters validate-weight-tuning --latest`
- `aits parameters explain-weight-tuning --latest`
- `aits reports weight-tuning --latest`

正式产物目录：

- `artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_summary.json`
- `artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_summary.md`
- `artifacts/weight_tuning/YYYY-MM-DD/recommended_shadow_weights.yaml`
- `artifacts/weight_tuning/YYYY-MM-DD/weight_tuning_candidates.json`

dry-run 写入 `outputs/dry_runs/weight_tuning/`。

## 实施步骤

|阶段|状态|验收标准|
|---|---|---|
|1. Governance/config|DONE|任务登记、需求文档、配置和 system flow/catalog/registry 更新完成。|
|2. Core tuning|DONE|读取 baseline、signal snapshot、backtest manifest，运行 data gate，生成 restricted grid candidates，禁止 fallback free tuning。|
|3. Walk-forward/objective|DONE|每个 candidate 与 baseline 做 walk-forward 比较，输出 metrics、relative metrics、objective breakdown 和 guardrails。|
|4. Artifacts/CLI/report|DONE|生成 JSON/Markdown/YAML/candidates，CLI validate/report/explain 能读取并校验。|
|5. UI integration|DONE|Dashboard、Reader Brief、shadow backtest promotion decision 只读引用 weight tuning artifact。|
|6. Validation|DONE|专项测试、pytest、ruff、compileall、diff check 通过；production 参数 hash 不变。|

## 状态规则

- `rejected`：guardrail failed，或多数 walk-forward windows 不优于 baseline。
- `watch`：轻微改善 baseline，但 signal quality 仍为 `LIMITED`。
- `shadow_candidate_only`：明显改善并通过 guardrails，但 signal quality 仍为 `LIMITED`。
- `needs_more_data`：walk-forward windows 或 signal history 不足。
- `insufficient_data`：data gate failed 或 required input missing。

若第一版完整跑通但 signal quality 仍为 `LIMITED`，任务最多标为 `BASELINE_DONE`，不能标为 production DONE。

## 进展记录

- 2026-05-30：新增 TRADING-059 并进入 `IN_PROGRESS`。目标是在 TRADING-058A 的 active tracking / needs_more_data 状态之后，补齐受限 signal weight tuning 的 advisory shadow 闭环。
- 2026-05-30：从 `IN_PROGRESS` 改为 `VALIDATING`。已完成 v0.1 实现、配置、CLI、报告、Dashboard、Reader Brief、shadow backtest supporting artifact 接入和专项测试；真实 `aits parameters tune-weights --latest` 生成 2026-05-28 artifact，结果为 `NO_CANDIDATE`、`candidate_status=rejected`、`candidates_evaluated=240`、`guardrail_status=FAIL`，因此 baseline 仍是参考配置，不修改 production 参数、不自动 promotion。验证通过 `aits parameters validate-weight-tuning --latest`、`aits reports weight-tuning --latest`、`aits parameters explain-weight-tuning --latest`、`aits parameters shadow-backtest --latest --dry-run`、专项 pytest、全量 `python -m pytest -q`、`python -m ruff check scripts src tests`、`python -m compileall src scripts` 和 `git diff --check`。
