# TRADING-2273 First-Layer Proxy Challenger Experiments

最后更新：2026-06-28

## 背景

TRADING-2270/2271/2272 已分别固化 first-layer current-state failure taxonomy、free /
low-cost proxy coverage audit，以及 objective / validation contract。附件下一步要求
把这些输入转成 proxy challenger experiment matrix，但不能把实验 readiness 误读为
promotion readiness。

## 范围

- 生成并审计六类 challenger experiment：
  - `baseline`
  - `baseline_plus_trend_structure`
  - `volatility_regime`
  - `risk_appetite`
  - `equal_cap_weight_divergence`
  - `combined_proxy`
- 每个 experiment 输出 required proxies、missing inputs、target objective terms、
  row-level `validation_ready`、`validation_ready_scope`、promotion blockers 和 fixed gate fields。
- 复用 TRADING-2270 current-state、TRADING-2271 proxy coverage audit 和 TRADING-2272
  objective validation artifacts。

## 非目标

- 不重新训练 first-layer model。
- 不修改 active composer、selection rule、reopen gate 或 objective policy。
- 不把 ETF price ratio、listing status、holdings gate 或 low-cost proxy 当作 true PIT breadth。
- 不让 row-level `validation_ready=true` 打开 promotion、paper-shadow、production 或 broker。

## 验收标准

- 生成 `first_layer_proxy_challenger_experiments.json`。
- 生成 `first_layer_proxy_challenger_experiments.yaml`。
- 生成 `first_layer_proxy_challenger_experiments.md`。
- Experiment rows 覆盖 baseline、trend structure、volatility regime、risk appetite、
  equal/cap-weight divergence 和 combined proxy。
- `equal_cap_weight_divergence` 和 `combined_proxy` 必须显式披露 RSP / QQQE 等缺失 blocker。
- 所有 rows 必须满足 `promotion_allowed=false`、`paper_shadow_allowed=false`、
  `production_allowed=false`、`broker_action=none`；reports 必须说明 row-level
  `validation_ready` 只表示 offline challenger readiness。

## 进展

- 2026-06-28：新增并进入 `IN_PROGRESS`；本批只生成 challenger experiment matrix，
  不训练模型、不恢复 first-layer gates、不进入 paper-shadow、production 或 broker。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-proxy-challenger-experiments`、challenger policy、
  JSON/YAML/Markdown artifacts、report registry、artifact catalog、system flow 和 focused tests。
  真实 run data_quality_status=`PASS_WITH_WARNINGS`，experiment_count=6，
  validation_ready_count=4，promotion_allowed_count=0；baseline、trend structure、
  volatility regime 和 risk appetite row-level offline validation ready，equal/cap-weight
  divergence 与 combined proxy 因 `rsp_to_spy` / `qqqe_to_qqq` missing 被阻塞；所有
  promotion/paper-shadow/production/broker fields 仍为 false/none/BLOCKED。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（2 passed）、
  governance focused parallel pytest（45 passed）、`python -m ai_trading_system.cli docs validate-freshness`、
  `git diff --check` 和 `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  （193 passed）；runtime artifact=`outputs/validation_runtime/contract-validation_20260628T130249Z/test_runtime_summary.json`。
