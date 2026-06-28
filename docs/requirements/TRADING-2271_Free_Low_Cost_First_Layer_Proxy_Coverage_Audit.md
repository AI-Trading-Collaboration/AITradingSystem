# TRADING-2271 Free / Low-Cost First-Layer Proxy Coverage Audit

最后更新：2026-06-28

## 背景

TRADING-2270 已把 current first-layer baseline 的 failure taxonomy 固化为
research-only 诊断证据。附件下一步要求先审计免费 / 低成本 proxy 覆盖，而不是直接把
proxy 接入模型或用 proxy 替代 true breadth。

## 范围

- 汇总 free feature family、ETF ratio participation proxies、Alpha Vantage listing status
  proxy、FMP holdings trial gate 等低成本候选。
- 每个 proxy row 必须输出：
  - `data_available`
  - `history_start_date`
  - `primary_window_coverage`
  - `PIT_safe_or_not`
  - `survivorship_risk`
  - `expected_signal_role`
  - `replacement_for_true_breadth`
- 先复用 cached-data quality gate，读取本地 price cache 和既有 proxy artifacts。

## 非目标

- 不下载新网络数据。
- 不升级 FMP / Norgate / 其他 paid source。
- 不把 ETF price ratio、listing status 或 holdings gate 当成 true PIT breadth。
- 不训练模型、不打开 first-layer reopen、promotion、paper-shadow、production 或 broker。

## 验收标准

- 生成 `proxy_coverage_matrix.json`、`first_layer_proxy_coverage_audit.yaml` 和
  `first_layer_proxy_coverage_audit.md`。
- 所有 proxy rows 都显式披露 `replacement_for_true_breadth=false` 或阻塞原因。
- 可用 price-only proxy 必须标成 `PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH`，而不是
  model-ready breadth。
- 缺失 RSP / QQQE / XLK 等 price coverage 时必须显式列出 missing components。
- reports 披露 `market_regime=ai_after_chatgpt`、actual requested range、data quality
  status 和固定安全边界。

## 进展

- 2026-06-28：新增并进入 `IN_PROGRESS`；本批只做 coverage audit，不进入 TRADING-2272
  objective redesign 或 TRADING-2273 proxy challenger experiments。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-proxy-coverage-audit`、coverage policy、JSON/YAML/Markdown
  audit artifacts 和 focused tests。真实 run data_quality_status=`PASS_WITH_WARNINGS`，
  proxy_count=12，data_available_count=6，primary_window_covered_count=5，
  replacement_for_true_breadth_count=0；SMH/QQQ 和 SOXX/QQQ price proxies 可用，RSP、
  QQQE、XLK 缺失，Alpha Vantage listing status 与 FMP holdings gate 仍不能作为 true breadth。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（2 passed）、
  governance focused parallel pytest（45 passed）、`python -m ai_trading_system.cli docs validate-freshness`、
  `git diff --check` 和 `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
  （193 passed）；runtime artifact=`outputs/validation_runtime/contract-validation_20260628T122456Z/test_runtime_summary.json`。
