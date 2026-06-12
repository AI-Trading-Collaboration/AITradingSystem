# TRADING-239 to 245 Weight Optimization Experiment Factory and Hypothesis Screening

状态：VALIDATING
最后更新：2026-06-13

## 背景

TRADING-229 to 233 已证明 `risk_capped_limited_adjustment` 能降低 semiconductor exposure，但没有改善 drawdown、return preservation 或 rolling consistency。继续一次只完整工程化一个正式 method 的成本过高，也容易把单个假设过早解释成可推广结论。

本阶段把流程升级为 research screening factory：

1. failure taxonomy
2. hypothesis backlog
3. lightweight transform spec
4. experiment matrix
5. batch paper-shadow backfill scorecard
6. triage gate
7. top variant interpretation
8. formal research method promotion plan

## Safety Boundary

本阶段所有 outputs 必须固定为：

- `experiment_only=true`
- `research_screening_only=true`
- `not_formal_research_method=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `auto_apply=false`
- `production_effect=none`

通过 promotion gate 只表示后续可另开正式 research method implementation。TRADING-245 只输出计划，不实现 method，不修改 `position_advisory_v1.yaml`、official target weights、paper/real portfolio、baseline/production state、policy、order ticket 或 broker。

## Stage Breakdown

TRADING-239: 建立 failure mode taxonomy 和 hypothesis backlog。Artifact 包含 manifest、taxonomy、hypotheses.jsonl、priority summary 和 report。

TRADING-240: 建立 lightweight transform type catalog 和安全验证。Transform 只能作用于 existing target weight path 或 experiment candidate aggregation，不能注册为正式 method。

TRADING-241: 从 backlog/spec 生成第一批 experiment matrix，至少覆盖 regime gating、cooldown、smoothing、rebalance threshold、ensemble/candidate selection、exposure cap、cash buffer 和 signal persistence。

TRADING-242: 对 matrix 批量生成 variant weight paths，并输出 performance/regime/stability metrics。使用 `ai_after_chatgpt` regime 和 `2022-12-01` 起点，依赖 cached data quality gate。

TRADING-243: 统一 triage gate。初始评分权重作为 pilot baseline 记录在 matrix/config/report 中；hard reject 规则必须可解释。

TRADING-244: 对 promoted / keep-testing top variants 生成 interpretation pack，解释 solved failure modes、收益来源、代价、适用 regime 和 remaining gaps。

TRADING-245: 生成 formal research method promotion plan 和 owner review checklist。计划只建议下一阶段实现哪些 research-only method，不自动实现。

## Acceptance Criteria

- `hypothesis-backlog build/report` 和 `validate-hypothesis-backlog` 可运行。
- `variant-transform validate-spec/report-spec` 和 `validate-variant-transform-spec` 可运行。
- `experiment-matrix build/report` 和 `validate-experiment-matrix` 可运行。
- `batch-experiment run/report` 和 `validate-batch-experiment` 可运行。
- `experiment-triage run/report` 和 `validate-experiment-triage` 可运行。
- `top-variant-interpretation run/report` 和 `validate-top-variant-interpretation` 可运行。
- `method-promotion-plan run/report` 和 `validate-method-promotion-plan` 可运行。
- Reports 用中文说明 selected market regime、actual requested date range、data quality status 和 no broker/no production boundary。
- Reader Brief、report registry、artifact catalog、system flow、operations runbook、README 和 task register 同步。
- Focused tests、ruff、compileall 和 `git diff --check` 通过；全量 pytest 尽量运行，超时则记录已完成范围。

## Progress Notes

- 2026-06-13: 根据 owner 附件创建本需求文档和任务登记，状态设为 IN_PROGRESS。实现范围限定为 P0 lightweight experiment factory；P1/P2 延后。
- 2026-06-13: baseline 实现完成并转入 VALIDATING。真实链路输出：
  - hypothesis backlog `hypothesis-backlog_fbbcbf2271748d54`，failure modes=13，hypotheses=15，HIGH priority=7；
  - transform spec `variant-transform-spec_373a9aab08a36d73`；
  - experiment matrix `experiment-matrix_430acfe2f5803112`，variants=15，families=8，failure modes covered=12；
  - batch experiment `batch-experiment_09e60902b65d7fe9`，variants_completed=15，date range=`2022-12-01` 到 `2026-06-10`，data_quality_status=`PASS_WITH_WARNINGS`；
  - experiment triage `experiment-triage_b310cdfb9d2a0e3d`，promote=3，keep_testing=3，reject=9，top_variant=`sideways_choppy_hold_previous`；
  - top variant interpretation `top-variant-interpretation_eb3cf6b9546ff74b`，recommended_variant=`smooth_weights_3d`；
  - method promotion plan `method-promotion-plan_3f47bf96388289ec`，proposed methods=`smooth_weights_3d_limited_adjustment_research_method`、`smooth_weights_5d_limited_adjustment_research_method`，scope=`research_only`。
- 2026-06-13: 验证通过全部新增 validate CLI、`aits etf dynamic-v3-rescue validate`、`aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`、`aits reports index --latest`、`aits reports reader-brief --latest`、`aits reports validate-reader-brief --latest`、`aits docs report-contract --as-of 2026-05-28`、focused pytest `7 passed`、`python -m ruff check src tests`、`python -m compileall -q src tests`、`git diff --check` 和 full pytest `2400 passed, 640 warnings`。`git diff --check` 仅提示 `docs/task_register.md` CRLF 将被 Git 规范化。
- 2026-06-13: 修复验证中暴露的两个问题：batch data quality gate 不应把历史 experiment end date 当作缓存当前日，现记录 `data_quality_as_of=max(date_end, generated_at.date())`；promotion plan method names 现在基于 source variant 生成并由 validator 检查唯一性。另修复既有 paperbroker/IBKR comparison redaction 对 `output_paths` 的误替换，避免 pytest 临时目录中的数字片段被当作 broker order id。

## Known Limits And Next Step

- 本阶段仍是 lightweight research screening；triage `PROMOTE_TO_FORMAL_RESEARCH_CANDIDATE` 不等于 formal method implementation、owner approval、official target weights、paper/real portfolio mutation、order ticket 或 broker action。
- `data_quality_status=PASS_WITH_WARNINGS` 的现有限制来自本地数据质量报告中的 provider/checksum warnings；batch artifact 已披露门禁状态和评估日，但没有修复数据源审计 warning。
- 下一个 owner decision 是是否为 `smooth_weights_3d_limited_adjustment_research_method` 或 `smooth_weights_5d_limited_adjustment_research_method` 新开 formal research method implementation，并按计划补跑 paper shadow backfill、rolling eval、regime review、stability diagnostics 和 forward confirmation。
