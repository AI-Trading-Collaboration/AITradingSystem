# TRADING-911 to 922 Simple Baseline Data Repair and Forward-Aging Unblock

## 背景

TRADING-894 to 910 已经冻结 simple baseline forward-aging 候选、比较器和安全边界，但真实本地缓存仍暴露 QQQ/TQQQ/SGOV 数据质量缺口。不能因为候选冻结已经完成就开始正式 forward aging；必须先确认主价格、第二来源、repair manifest、SGOV carry 口径和 validate-data fail-closed 规则。

默认研究 regime 仍为 `ai_after_chatgpt`，anchor date 为 2022-11-30，默认 backtest start 为 2022-12-01。pre-2022 历史只允许用于 warm-up、stress 或 regime comparison，不能作为默认 AI-cycle 结论窗口。

## 安全边界

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`
- Reader Brief preview 只能展示极简观察状态，不得输出买入、卖出、应调仓、实盘建议或目标持仓建议。

## 阶段拆解

|任务|阶段|状态|验收标准|
|---|---|---|---|
|TRADING-911|数据盘点|DATA_SOURCE_INVENTORY_PARTIAL|`simple_baseline_data_source_inventory.json/md` 已生成；QQQ 主源完整，TQQQ/SGOV 主源可用但 Marketstack 第二来源仍缺 TQQQ/SGOV，因此为 partial/warn。|
|TRADING-912|TQQQ 修复|TQQQ_CACHE_REBUILT|复用既有 FMP repair / manifest 路径补齐 TQQQ；`tqqq_rows_before=0`、`tqqq_rows_after=1008`、`validate_data_status=PASS_WITH_WARNINGS`。|
|TRADING-913|SGOV total-return 契约|SGOV_TOTAL_RETURN_CONTRACT_READY|SGOV `adj_close` 可用且存在分红/派息调整证据；明确 raw close 不可当 total return，rates proxy 不可无 owner policy 替代 SGOV。|
|TRADING-914|repair manifest 审计|REPAIR_MANIFEST_WARN|QQQ/TQQQ/SGOV 均有主价格 manifest entry 且无 blocker；TQQQ/SGOV repair manifest date range 不覆盖全历史主缓存，保留 warning。|
|TRADING-915|validate-data hardening|VALIDATE_DATA_HARDENED|`validate_data_cache` 同源结果 `PASS_WITH_WARNINGS`、0 errors；QQQ/SGOV group READY、TQQQ challenger READY；sample_count=0 READY 禁止。|
|TRADING-916|修复后真实 CLI 重跑|POST_REPAIR_REAL_RUN_WARN|865 to 910 + 911/913/914/915 builder 已重跑；0 failed command，formal observation 未写入；options gate 仍 `OPTIONS_RESEARCH_BLOCKED`，914 为 warning。|
|TRADING-917|equal-risk 重算|EQUAL_RISK_RESULT_RECOMPUTED|`equal_risk_qqq_sgov` 修复后仍为 top candidate，rank=1，candidate_role=primary，未触发 candidate changed。|
|TRADING-918|TQQQ challenger 复核|TQQQ_CHALLENGER_STILL_PAUSED|`dyn_tqqq_capped_trend` 指标可用，但仍为 challenger-only；TQQQ-heavy 继续暂停，paper-shadow/production/broker 均 false/none。|
|TRADING-919|forward-aging readiness|FORWARD_AGING_READY_WITH_WARNINGS|9 项 readiness check 无 blocker、2 warnings；data quality 为 `PASS_WITH_WARNINGS`，TQQQ/SGOV 第二来源/manifest warning 保留。|
|TRADING-920|first observation dry-run|FORWARD_OBSERVATION_DRY_RUN_WARN|latest decision date `2026-06-22`，5 个 candidate target weights 仅 dry-run；`observation_written=false`、不接 broker、不写 production config。|
|TRADING-921|Reader Brief safe preview|READER_FORWARD_PREVIEW_SAFE|预览只展示 primary/challenger/data quality/forward status/safety flags；禁用交易建议措辞命中 0。|
|TRADING-922|owner decision pack|OWNER_APPROVE_FORWARD_AGING|Owner decision pack 读取最新 readiness 与 preview，给出 `OWNER_APPROVE_FORWARD_AGING`；仍不允许 paper-shadow、production 或 broker action。|

## 实施顺序

1. 数据盘点和修复：TRADING-911 to 915。
2. 修复后重跑与候选复核：TRADING-916 to 919。
3. forward-aging 启动前预演：TRADING-920 to 922。

## 进展记录

- 2026-06-23: 新增需求拆解并进入 `IN_PROGRESS`。当前已知本地 cache 状态：主价格有 QQQ/SGOV，TQQQ 主价格缺失；Marketstack 第二来源只有 QQQ，TQQQ/SGOV 缺失。下一步实现 CLI/artifacts 并复用既有 FMP repair / validate-data code path。
- 2026-06-23: 实现并执行 TRADING-911～922。TQQQ 通过既有 FMP repair / manifest 路径从 0 行补到 1008 行；标准 `download-data --start 2018-01-01 --end 2026-06-22` 在 Marketstack SSL 阶段 fail-closed 并生成 diagnostics，随后用既有 FMP price-only repair 为 SGOV 补 manifest 审计链。`aits validate-data` 为 `PASS_WITH_WARNINGS` / 0 errors；914 保留 manifest warning，916 为 post-repair warn，919 为 `FORWARD_AGING_READY_WITH_WARNINGS`，920 只 dry-run 不写正式 observation，921 safe preview 无禁用词，922 输出 `OWNER_APPROVE_FORWARD_AGING` 且 safety fields 全部保持 false/none。
