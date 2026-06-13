# TRADING-286 to 305: Weight Optimization Batch Search and Adaptive Promotion Pipeline

最后更新：2026-06-14

## 状态

`VALIDATING`

Owner 要求推进附件中的 TRADING-286～305。本阶段把 dynamic v3 rescue weight search 从上一轮单点实验扩展为 research-only batch search、scorecard、robustness review、adaptive branch、expanded search、candidate cluster、top interpretation、promotion gate、formal method auto plan、dashboard 和 owner decision pack。

## 范围与边界

- Market regime 固定披露为 `ai_after_chatgpt`，anchor event 为 ChatGPT public launch on 2022-11-30，默认 backtest start 为 2022-12-01。
- 所有 outputs 均为 research screening / paper-shadow evidence，不是 official target weights、broker order、portfolio mutation、production baseline mutation 或 owner approval。
- Cached market / macro data dependent backfill 必须运行 `aits validate-data` 等价质量门禁。若 requested end 晚于 cache 可验证最新日期，允许使用 `latest_valid_as_of` 做历史 backfill 并显式披露，但真实 data quality failure 必须停止。
- Formal method auto plan 只生成实施计划和验证清单；未获得 owner approval 前不得写入正式 method config 或 production state。

## 阶段拆解

|Task|阶段|验收标准|
|---|---|---|
|TRADING-286|Weight search space config|`config/etf_portfolio/dynamic_v3_rescue/weight_search_space_v2.yaml` 覆盖 smoothing、cooldown、regime gating、rebalance threshold、candidate ensemble、cash buffer、risk exposure、turnover、hybrid families；包含 policy owner/version/rationale/review condition 和 safety fields。|
|TRADING-287|Search space validation artifact|`weight-search-space validate/report` 生成 manifest、normalized config、family inventory、Markdown report，并有 validator。|
|TRADING-288|Batch2 experiment matrix|`weight-experiment-batch2 build/report` 生成 50～80 个 research-only variants，覆盖 >=8 families 和 failure modes。|
|TRADING-289|Batch backfill runner|`weight-batch-backfill run/resume/report` 对 batch variants 运行 data-quality-gated historical backfill，输出 progress、weight paths、performance/regime/stability/churn/lag metrics。|
|TRADING-290|Scorecard|`weight-scorecard run/report` 输出 composite score、hard reject flags、pareto frontier 和 score distribution。|
|TRADING-291|Robustness review|`weight-robustness-review run/report` 汇总 regime split、pressure sensitivity、rolling consistency、turnover/churn/lag 并生成 robust candidates。|
|TRADING-292|Adaptive branch|`weight-adaptive-branch run/report` 给出 deepen/expand/stop 决策和下一轮 search-space 调整建议。|
|TRADING-293|Expanded search|`weight-expanded-search build/run` 在 adaptive branch 允许时生成 expanded matrix 并可继续 backfill。|
|TRADING-294|Candidate cluster|`weight-candidate-cluster run/report` 聚类 top candidates，输出 cluster summaries、representatives 和 diversification notes。|
|TRADING-295|Top candidate interpretation|`weight-top-candidate-interpretation run/report` 解释代表候选的收益来源、失败模式覆盖、tradeoff 和 implementation risk。|
|TRADING-296|Promotion gate|`weight-method-promotion-gate run/report` 只给出 research promotion gate，不能自动应用正式 method。|
|TRADING-297|Formal method auto plan|`formal-method-auto-plan run/report` 为 promoted candidates 生成 formal implementation plan；无 promoted candidate 时输出 fail-closed skip 状态。|
|TRADING-298～300|Formal method implementation placeholder|本阶段只生成 auto plan，不实现正式 method、不改 production/config；后续 owner approval 后另开任务。|
|TRADING-301|Dashboard|`weight-search-dashboard build/report` 汇总 scorecard、branch、gate、data quality 和 next actions。|
|TRADING-302|Owner research decision pack|`owner-research-decision-pack build/report` 生成 owner-readable options/checklist/summary。|
|TRADING-303|Reader Brief and report registry|新增 report registry / artifact catalog / Reader Brief 可索引入口，缺 artifact 时不得补造结果。|
|TRADING-304|Runbook and docs|更新 operations runbook、system flow、README、artifact catalog 和 task register。|
|TRADING-305|Validation and commit discipline|Focused tests、ruff、compileall、dynamic-v3 validators、artifact family validation、Reader Brief/report contract、full pytest 尽量完成；完成后按 local commit discipline 提交并在允许时 push。|

## 开放问题

- `weight_batch_search_scorecard_pilot_v1` 仍是 pilot baseline。需要在第一轮真实 batch2 + expanded search 和足够 forward samples 后复核 score weights / thresholds。
- 如果 no candidate crosses promotion gate，formal method auto plan 应保持 `SKIPPED_NO_PROMOTED_CANDIDATE`，而不是降低阈值。
- Formal method implementation、official target weight mutation、paper/real portfolio mutation 和 broker/order integration 均不在本阶段范围内。

## 进展记录

- 2026-06-14：新增任务并进入 `IN_PROGRESS`；开始实现 search space config、CLI/artifact family、batch backfill、scorecard、robustness、adaptive branch、expanded search、cluster、interpretation、promotion gate、formal auto plan、dashboard、owner decision pack、docs 和 focused tests。
- 2026-06-14：实现完成并转入 `VALIDATING`。真实默认链路输出 search space `weight-search-space_9ddc568f3cc0f2b1`、initial matrix `weight-experiment-batch2_ebea5d5b2be39b13`（64 variants）、initial backfill `weight-batch-backfill_f0867ebebee2541c`、expanded matrix `weight-experiment-batch2_e2bd3934921bc3a0`（87 variants）、expanded backfill `weight-batch-backfill_a20ee342b47edc7b`、expanded scorecard `weight-scorecard_62b9d331d9b34229`、robustness review `weight-robustness-review_abf1c5104d32fb10`、adaptive branch `weight-adaptive-branch_c78bf42b2d542beb`、cluster `weight-candidate-cluster_3ca3f7b88b89450b`、top interpretation `weight-top-candidate-interpretation_df5148143e80448e`、promotion gate `weight-method-promotion-gate_660d8071714d287a`、formal auto plan `formal-method-auto-plan_c98fbed9e725d551`、dashboard `weight-search-dashboard_7508784554b8b8c4` 和 owner pack `owner-research-decision-pack_1dde521736ba3980`。本轮 expanded search 的 top representative 为 `cash_buffer_10`，promoted candidate count 为 0，formal auto plan 为 `SKIPPED_NO_PROMOTED_CANDIDATE` / `implemented=false`，owner pack 建议 `run_expanded_search`。新增 validators、dynamic-v3 root validation、artifact family validation、documentation contract、Reader Brief、Reader Brief quality、ruff、compileall、git diff check、focused pytest 和 full pytest `2457 passed, 640 warnings` 已通过；report index 为既有 `PASS_WITH_WARNINGS`（missing/stale artifacts），不阻断本任务。剩余 owner 复核点为是否接受继续 expanded/deeper search，或是否调整 pilot scorecard policy 后另开校准任务。
