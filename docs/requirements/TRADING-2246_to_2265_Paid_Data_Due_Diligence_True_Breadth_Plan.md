# TRADING-2246 to 2265 Paid Data Due Diligence for True Breadth

最后更新：2026-06-28

## Status

- Task id: `TRADING-2246_to_2265_PAID_DATA_DUE_DILIGENCE_TRUE_BREADTH`
- Status: `VALIDATING`
- Last updated: 2026-06-28
- Owner: system

## Scope

本批只做 true breadth / historical constituents 付费数据源尽调，不购买数据、
不恢复 first-layer channel research、不训练模型、不进入 promotion、paper-shadow、
production 或 broker。

研究主窗口继续采用 `ai_after_chatgpt` regime。锚点为 2022-11-30 ChatGPT
公开发布，默认 backtest start 为 2022-12-01；本批 true breadth 契约要求能
覆盖 primary research window 自 2021-02-22 起的历史 membership 需求。

## Stages

1. `TRADING-2246_PAID_DATA_DUE_DILIGENCE_SCOPE`
   - 固化 due-diligence-only 范围、禁止购买和禁止 promotion 边界。
2. `TRADING-2247_TRUE_BREADTH_REQUIREMENT_CONTRACT`
   - 定义 historical constituents、daily membership、delisted securities、
     survivorship-free universe、primary window coverage 和 PIT semantics 的硬要求。
3. `TRADING-2248_to_2253_VENDOR_DUE_DILIGENCE`
   - 建立 paid breadth vendor registry。
   - 分别评估 Norgate、FMP holdings、EODHD、QuantConnect / AlgoSeek、Tiingo /
     Marketstack / Yahoo / price-only sources。
4. `TRADING-2254_to_2258_DECISION_PACKET`
   - 输出 vendor scoring matrix、value-of-information estimate、prototype design、
     trial gate 和 owner decision packet。
5. `TRADING-2259_to_2261_GUARDRAILS_AND_VALIDATION`
   - 新增 guardrail tests。
   - 更新 report registry、artifact catalog、system flow 和 task register。
   - 执行 focused validation 和 contract validation。
6. `TRADING-2262_to_2265_CLOSEOUT`
   - 输出 closeout 和 final matrix。
   - 只允许 research-only final status；owner approval 是任何 trial / purchase
     的硬前置条件。

## Non-Goals

- 不直接购买 Norgate 或任何高价数据。
- 不自动升级 FMP、EODHD、QuantConnect 或其他 provider plan。
- 不把 vendor marketing statement 当成 PIT evidence。
- 不用 current constituents backfill 构造历史 membership。
- 不把 price-only source 标记为 true breadth。
- 不恢复 first-layer channel research、v4、minimal forward diagnostic、promotion、
  paper-shadow、production 或 broker。
- 不输出 target weights、trade advice、allocation 或 broker action。

## Acceptance Criteria

- `config/research/true_breadth_data_contract.yaml` 明确要求 historical
  constituents、daily membership、delisted securities、survivorship-free universe、
  primary window coverage、Python/CLI access、local cache 和 holdings known-at
  semantics；禁止 current constituents backfill。
- `config/data/paid_breadth_data_vendor_registry.yaml` 覆盖 Norgate、FMP、EODHD、
  QuantConnect / AlgoSeek 和 price-only sources。
- Norgate / FMP / EODHD / QuantConnect / price-only sources 均有 review doc 和
  YAML evidence。
- Price-only sources 明确不能作为 true breadth。
- PIT warning sources 必须降级为 diagnostic-only / not model-ready breadth。
- Vendor scoring matrix 生成并登记。
- Trial decision gate 要求 owner manual approval before trial / purchase。
- Owner packet 用中文回答是否建议 trial、为什么不是直接购买、true breadth
  可能解决什么和剩余 blocker。
- Guardrail tests 覆盖 promotion disabled、owner approval、price-only、PIT warning、
  current-constituent backfill、delisted / historical membership requirement 和
  Norgate no auto-purchase。
- `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml`
  和 `docs/task_register.md` 同步更新。

## Open Questions And Blockers

- Norgate、EODHD、QuantConnect / AlgoSeek 的真实 license、trial、export 和 local
  cache 权限需要 owner 人工确认；公开页面只能作为 due-diligence input，不能
  直接作为 PIT proof。
- FMP holdings endpoint 在当前本机 key 下可能存在 plan entitlement blocker；
  若权限不足，不得自动升级。
- Holdings-based sources 必须区分 `holding_date`、`reported_date` 和 `known_at`；
  未确认前只能 diagnostic-only。
- QuantConnect / AlgoSeek 若必须在 LEAN cloud/local ecosystem 中运行，需要额外
  integration spike，不应直接接入当前 repo 的 model-ready pipeline。

## Progress Log

- 2026-06-28: Task created from owner attachment. Implementation starts with
  task register entry, true breadth contract, paid vendor registry, due diligence
  docs, scoring matrix, trial gate, owner packet, closeout and guardrail tests.
- 2026-06-28: Implementation completed as due-diligence-only package.
  Generated `true_breadth_data_contract_v1`, paid breadth vendor registry,
  Norgate/FMP/EODHD/QuantConnect/price-only reviews, vendor scoring matrix,
  value-of-information estimate, prototype design, trial gate, owner packet,
  closeout and final matrix. FMP capability check was sanitized and recorded as
  `HTTP_402_PAYMENT_REQUIRED` for the QQQ holdings endpoint; no API key or raw
  response was stored. Final decision is `NORGATE_TRIAL_RECOMMENDED`, with owner
  manual approval required before any trial, purchase, upgrade, sample download
  or local cache.
- 2026-06-28: Validation passed Ruff, compileall, focused paid-data pytest
  (`7 passed`), first-layer / free PIT / archive / research-audit / governance
  focused pytest (`46 passed`), report / documentation / task-register focused
  pytest (`27 passed`), and `python scripts/run_validation_tier.py
  contract-validation --write-runtime-artifact` (`193 passed`). Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260628T090805Z/test_runtime_summary.json`.
