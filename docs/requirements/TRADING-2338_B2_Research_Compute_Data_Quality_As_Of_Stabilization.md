# TRADING-2338 B2 Research Compute Data Quality As-Of Stabilization

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2336 full validation 初次失败在 `tests/test_research_campaign.py` 的 B2 Campaign compute tests。失败根因不是 2336 event logger，而是旧 B2 research compute builders 默认把 `generated_at.date()` 当作 `aits validate-data` 同源 gate 的评估日期。

在 2026-07-04 运行时，本地 price cache 最新日期为 2026-06-29，FRED rates cache 最新日期为 2026-06-26。B2 research campaign smoke 是复现历史 research evidence，不应随 wall-clock 日期推进而把同一份 cache 变成 stale；但旧逻辑使用 2026-07-04 评估，触发 `rates_stale`，导致 control-window no-trigger evidence 被错误标为 blocked。

## 最佳解决方案

将 B2 targeted evidence、control window 和 full diagnostic research compute path 的 data-quality `as_of` 绑定到 cache-effective date，即 `max_price_date(prices_path)`。这与近期 source-bound exposure-cap dry-run 等 data-dependent research builders 的默认质量评估口径一致。

## 非绕行说明

本修复不放宽 `aits validate-data` 的 stale、future-date、schema、checksum 或 provider reconciliation 规则；只修正 research compute runner 传入 quality gate 的评估日期，使它不依赖 wall-clock runtime。若 cache 本身在 latest price date 口径下失败，B2 compute 仍 fail closed。

## 实施范围

1. `prepare_research_data_context` 增加可选 `data_quality_as_of` 参数，默认仍保持旧行为。
2. B2 control-window research runner 传入 latest price cache date。
3. B2 targeted-evidence research runner 传入 latest price cache date。
4. B2 full-diagnostic research runner 使用 latest price cache date。
5. B2 data-quality payload 披露 `as_of` 与 `as_of_basis=latest_price_cache_date`。
6. 更新 `docs/system_flow.md`，说明 B2 Campaign compute 的 data-quality as-of 口径。

## 验收标准

- B2 targeted/control/full diagnostic compute 不再随 wall-clock 日期推进而错误 stale。
- B2 compute adapter smoke 输出 `B2_TARGETED_EVIDENCE_COMPUTE_PASS`。
- `tests/test_research_campaign.py` focused parallel pytest 通过。
- full validation tier 通过。
- 不改变 B2 策略参数、threshold、evidence interpretation、paper-shadow、production、broker 或 order boundary。

## 验证

- 2026-07-04：B2 compute adapter smoke 输出 `B2_TARGETED_EVIDENCE_COMPUTE_PASS`，adapter status=`B2_TARGETED_EVIDENCE_COMPUTE_PASS`，reason codes 包含 `CONTROL_BEHAVIOR_CLEAN`，data_quality_status=`PASS_WITH_WARNINGS`。
- 2026-07-04：`python -m pytest -n 16 --dist loadfile tests/test_research_campaign.py -q --durations=20 --durations-min=1` 通过，20 passed。
- 2026-07-04：`python scripts/run_validation_tier.py full --write-runtime-artifact` 通过，4161 passed / 643 warnings，runtime artifact=`outputs/validation_runtime/full_20260704T051224Z/test_runtime_summary.json`。
