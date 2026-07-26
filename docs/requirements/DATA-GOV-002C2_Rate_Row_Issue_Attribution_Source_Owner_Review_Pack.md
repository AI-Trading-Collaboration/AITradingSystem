# DATA-GOV-002C2：Rate Row Issue Attribution Source-Owner Review Pack

最后更新：2026-07-26

稳定任务 ID：`DATA-GOV-002C2_RATE_ROW_ISSUE_ATTRIBUTION_SOURCE_OWNER_REVIEW_PACK`

上位 Owner 决定：
`owner_decision:DATA-GOV-002:2026-07-26:approve_long_term_capability_receipt_engineering_v1`

Owner continuation：
`owner_continuation:DATA-GOV-002C2:2026-07-26:continue_long_term_engineering_goal`

状态：`BASELINE_DONE_SOURCE_OWNER_DECISION_PENDING`

## 1. 背景与问题

DATA-GOV-002C1 已机械枚举 69 个 canonical DQ emission sites，并建议先审查 6 个
static rate row-level value/move sites。C1 只证明这些 site 存在、稳定且尚未获得迁移
权威；它没有证明每个 issue 的 exact affected scope，也没有授权新增 typed schema、
capability isolation 或 consumer migration。

首批 6 项不能按一个粗粒度“rate row issue”直接处理：

- `rates_invalid_date`、`rates_invalid_value`、`rates_non_finite_value` 和
  `rates_out_of_range` 由单行 predicate 触发，但该行的 `date` 或 `series` 仍可能无法
  形成完整 attribution；
- `rates_extreme_daily_change` 与 `rates_suspicious_daily_change` 由同一 series 的当前
  有效行和前一有效观测共同决定，不能只把当前 sample 行声明为完整受影响范围；
- 现有 `DataQualityIssue` 只有 `affected_instruments`，capability classifier 也只理解
  price instrument disjointness；C2 不得在 source-owner review 前擅自扩展此运行时权威。

## 2. 本切片目标

建立 deterministic、content-derived、非授权性的 source-owner review pack：

1. 固定 C1 建议的 6 个 exact site id/code，不允许自动扩大候选集；
2. 绑定 C1 inventory、canonical `quality.py`、`config/data_quality.yaml` 和 proposal
   manifest 的 bytes/SHA-256；
3. 对每个 site 记录 predicate、source role、row dependency、可证明 attribution
   维度、完整性前置条件和 fail-closed 条件；
4. 明确区分 single-row 与 current-plus-previous-observation scope；
5. 输出 source owner 需要逐项回答的 `APPROVE_FOR_CONTRACT_WAVE`、`REVISE` 或
   `REJECT` 决策槽，但不代填 Owner 决定；
6. validator 从全部绑定来源重建 pack，拒绝 site、mapping、source、policy 或输出漂移；
7. 为后续独立 C3 typed contract wave 形成最小、可审核的输入边界。

## 3. Exact review scope

只包含以下 6 个 C1 site：

- `dq_issue_site_0e7f3d74bfa489801c83` / `rates_invalid_date`；
- `dq_issue_site_f337897b3d0d0b8e2842` / `rates_invalid_value`；
- `dq_issue_site_dcc6dcab7a17c225b404` / `rates_non_finite_value`；
- `dq_issue_site_6421117ee905a6da1438` / `rates_out_of_range`；
- `dq_issue_site_85549de0f1e9ab739a74` / `rates_extreme_daily_change`；
- `dq_issue_site_df1c184d09e3c55d3e71` / `rates_suspicious_daily_change`。

`rates_future_dates`、`rates_stale`、`rates_empty`、missing/duplicate/expected-series 和
所有 price/provenance issue 均不在本切片。它们继续保持
`GLOBAL_OR_UNKNOWN_SCOPE`。

## 4. Review contract

Proposal manifest 状态固定为 `PROPOSED_FOR_SOURCE_OWNER_REVIEW`，不得使用
`REVIEWED`、`OWNER_APPROVED` 或等价词。每个候选至少记录：

- exact site id、issue code、emitter function 和 severity；
- `scope_taxonomy`；
- exact canonical source role=`primary_macro_rates`；
- `affected_price_tickers=[]` 的 rate-only source assertion；
- candidate `affected_rate_series` extraction rule；
- defect fields、identity fields、derived fields；
- current/predecessor row dependency；
- affected-window derivation与无法形成窗口的条件；
- attribution completeness requirements；
- false-isolation risk 与 required contract tests；
- source-owner decision slot=`PENDING_SOURCE_OWNER_DECISION`。

Review pack 只能给出 `CONTRACT_WAVE_CANDIDATE`、`REVISION_REQUIRED` 或
`REJECT_RECOMMENDED` 的工程建议，不得把任何建议解释为现行隔离权威。

## 5. 实施步骤

1. 新增 proposal manifest，冻结 6 项 mapping 和非授权状态；
2. 实现 builder/validator/Markdown projector 和独立 CLI；
3. 绑定 C1 inventory、source、DQ policy 和 manifest hashes；
4. 增加 exact-site、mapping、single-row、row-pair、缺失维度和 tamper tests；
5. 生成 tracked JSON、validation JSON 和中文 reader brief；
6. 更新 parent requirement、task register、system flow、artifact/report registry；
7. 刷新 task/architecture generated views与 append-only compatibility authority；
8. 运行 focused、architecture、contract、report、reproducibility、integration 和适用
   natural-boundary Full；
9. 验证后提交任务分支、fast-forward local `main`、普通推送并清理任务分支。

## 6. 验收标准

- pack 恰好覆盖 6 个 exact C1 sites，site/code/function/severity 与 C1/source 一致；
- single-row 与 row-pair dependency 不得混淆；
- invalid/blank series、无法解析 date、缺少 predecessor 或 source-role drift 都必须显式
  标记为 attribution incomplete，后续不得隔离；
- 不解析 message/sample 推导 scope；
- proposal、C1 inventory、source、DQ policy 或输出任一漂移均由 validator 拒绝；
- 所有 decision slots 保持 pending，`new_issue_isolation_authorized=false`；
- 不修改 `DataQualityIssue`、capability policy/classifier、`validate_data_cache`、
  cached data、consumer runner 或 full/scoped DQ 行为；
- `production_effect=none`、`broker_action=none`。

## 7. 安全、生命周期与退出条件

- 本任务使用 `SINGLE_LANE`，计划分支
  `codex/data-gov-002c2-rate-review-pack`；
- 不创建额外 worktree、clone、stash 或 cache；
- 不读取、修改或提交
  `docs/research/growth_tilt_owner_diagnosis_pack.md`；
- C2 完成条件是 review pack 与验证基线可供 source owner 决策，不是 Owner 已批准；
- 只有 source owner 对 6 项逐项形成显式决定后，才能另建 C3 serial contract wave；
- C3 仍需独立扩展 typed attribution/runtime classifier，并验证 false isolation，不由 C2
  自动启动。

## 8. 进度

- 2026-07-26：在 C1 closeout 后复核 canonical source。确认 4 个 single-row issue 与
  2 个 row-pair move issue 的 dependency 不同；现有 runtime contract 只支持
  `affected_instruments`，不能承载 rate/window/field/row authority。任务建立并进入
  `IN_PROGRESS`，首轮只生成非授权性的 source-owner review pack。
- 2026-07-26：C2 工程基线完成并转
  `BASELINE_DONE_SOURCE_OWNER_DECISION_PENDING`。Pack ID=
  `dq_rate_issue_attribution_review_0957cfde306cb37b760f1005`，恰好包含 6 个候选：
  4 个 `SINGLE_SOURCE_ROW`、2 个
  `CURRENT_AND_PREVIOUS_VALID_OBSERVATION`；所有 decision slots 仍为
  `PENDING_SOURCE_OWNER_DECISION`，runtime/隔离授权计数均为 0。Focused DQ regression=
  `166 passed`，强化后的 C2 exact contract=`11 passed`；Black、Ruff、strict mypy、
  task registry、DevEx 与 compatibility focused 均 PASS。Formal
  architecture/contract/report/reproducibility/integration=`676/275/57/23/995 passed`；
  本批唯一 natural-boundary Full=`7389 passed / 3 skipped / 643 warnings`，artifact=
  `outputs/validation_runtime/full_20260726T142413Z/test_runtime_summary.json`。本结果只证明
  source-owner review pack 已可决策，不构成逐 site 批准；C3、typed runtime attribution、
  capability policy 变更和 consumer migration 继续锁定。
