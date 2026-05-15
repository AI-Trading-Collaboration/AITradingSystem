# 参数配置治理 manifest 与证据建议流程

状态：VALIDATING

最后更新：2026-05-15

关联任务：`CALIBRATION-005`、`CALIBRATION-003`、`CALIBRATION-004`、`GOV-004`

## 背景

当前系统已经把评分权重、回测稳健性阈值、feedback 样本门槛和 candidate 多目标 veto 从散落硬编码迁移到配置和报告中。新的问题是：owner 暂时无法给出可量化参数输入，但系统仍需要持续迭代“哪些参数该保持、哪些参数该继续收集证据、哪些参数可进入 shadow 准备”。

如果在没有 owner 量化输入时直接改配置值，本质上会把新的主观判断伪装成 evidence-backed policy。正确做法是新增一层参数治理 manifest：它不生成 approved overlay，不修改 production 参数，只把参数面、证据来源、当前候选状态、行动建议和 owner 阻塞显式化。

## 目标

- 新增 `config/parameter_governance.yaml`，登记投资解释相关的可调参数面、来源等级、owner/status/rationale、验证证据、review/expiry 条件、production 边界和缺 owner 输入时的动作规则。
- 新增只读 CLI 报告，读取 `data/processed/parameter_candidates.json`、当前 sample/backtest/scoring/weight 配置和治理 manifest，生成 `outputs/reports/parameter_governance_YYYY-MM-DD.md/json`。
- 在 owner 暂缺量化参数时，只输出保守行动建议：保持当前、继续收集证据、准备 forward shadow、需要 owner 决策、被数据阻断。
- 报告接入 `aits feedback optimize-market-feedback` 摘要，让市场反馈优化报告能显示参数治理状态，而不是只显示候选台账状态。

## 非目标

- 不自动调整 `config/scoring_rules.yaml`、`config/backtest_validation_policy.yaml`、`config/feedback_sample_policy.yaml` 或 `config/weights/weight_profile_current.yaml` 的生产数值。
- 不生成 `approved_calibration_overlay.json`，不把 candidate 写成 production overlay。
- 不绕过 coverage、random baseline、OOS、统计证据、rule card 或 owner approval。
- 不把缺失 owner 输入视为数据通过；缺 owner 输入只能降低为 `PASS_WITH_LIMITATIONS` 或 owner-required 行动。

## 来源等级

|等级|含义|生产影响|
|---|---|---|
|`owner_policy`|owner 已明确批准且有记录的政策值|可作为 production policy，但仍需回滚条件。|
|`empirical_calibrated`|通过 replay、shadow、OOS、random/benchmark 和数据覆盖证据支持|可提交 owner review；未批准前仍无 production effect。|
|`pilot_prior`|当前 pilot baseline，有 rationale 和验证计划，但证据不足|只能用于诊断、候选准备和报告披露。|
|`temporary_baseline`|为了让流程可审计运行而保留的初始值|必须有退出条件；不得作为长期结论依据。|
|`invariant`|协议、尺度、schema 或审计不变量|不可由市场反馈自动调参。|

## 行动建议口径

|建议|触发语义|
|---|---|
|`KEEP_CURRENT`|当前参数面没有未阻断候选，或 manifest 声明为 invariant / production policy。|
|`COLLECT_MORE_EVIDENCE`|样本、统计证据、shadow 或 replay 仍不足；继续运行现有闭环。|
|`PREPARE_FORWARD_SHADOW`|候选未被数据/OOS/random/architecture 等证据阻断，可准备 shadow 但仍无 production effect。|
|`OWNER_DECISION_REQUIRED`|证据足以进入 owner review，或 manifest 要求 owner 定量输入后才能继续。|
|`BLOCKED_BY_DATA`|candidate ledger 已被数据质量、数据可信度、coverage 或 placeholder veto 阻断。|
|`BLOCKED_BY_POLICY`|manifest 缺必需治理字段、source level 不允许调参，或 production boundary 不清。|

## 阶段拆解

|阶段|状态|验收|
|---|---|---|
|1. 任务登记与需求文档|VALIDATING|本文件和 `docs/task_register.md` 记录 owner 暂缺输入边界、生产影响和验收标准。|
|2. Governance manifest|VALIDATING|`config/parameter_governance.yaml` 覆盖 weight profile、scoring rules、sample policy、backtest validation、LLM 成本和 candidate ledger 等参数面；字段足以满足 heuristic governance。|
|3. 只读报告模块和 CLI|VALIDATING|新增 `aits feedback evaluate-parameter-governance`，写出 Markdown/JSON；缺字段 fail closed，缺 owner 定量输入不伪造参数。|
|4. 市场反馈优化接入|VALIDATING|`optimize-market-feedback` 报告显示 parameter governance 状态、manifest 版本和行动分布。|
|5. 验证与真实 smoke|VALIDATING|目标单测、ruff、`git diff --check`、全量 pytest 和真实 CLI smoke 通过；真实数据阻断按 `PASS_WITH_LIMITATIONS` 披露。|

## 验收标准

- 新 manifest 每个参数面必须有 `parameter_id`、`config_path`、`surface`、`source_level`、`owner`、`status`、`rationale`、`validation_evidence`、`review_after`、`exit_condition`、`production_effect`。
- 报告输出 manifest version/status、owner quantitative input 状态、candidate ledger 状态、行动建议分布、blocked reasons、关键配置路径和 `production_effect=none`。
- 当所有候选都被数据或 benchmark 阻断时，报告必须明确下游影响，不得提出调生产参数。
- 当 owner 无法给出量化值时，报告可以建议继续收集证据或准备 shadow，但不得把缺失输入补成具体数值。
- `optimize-market-feedback` 摘要应显示参数治理报告是否 connected；缺报告时为 `PASS_WITH_LIMITATIONS`，不改变日报评分或交易动作。

## 状态记录

- 2026-05-15：新增任务和设计文档。owner 确认暂时无法提供可量化配置输入，本轮采用只读 governance manifest + evidence-driven action 建议，不自动改生产参数。
- 2026-05-15：基础实现完成并进入 VALIDATING。新增 `config/parameter_governance.yaml`、`aits feedback evaluate-parameter-governance`、`parameter_governance_YYYY-MM-DD.md/json`，并把摘要接入 `optimize-market-feedback` 和交易日 `daily-run` 的 dashboard 前置流程。真实 2026-05-13 smoke 显示 manifest `parameter_governance_v1`、owner input `unavailable`、candidate_count=16、parameter_count=5、action 分布为 `BLOCKED_BY_DATA=2`、`COLLECT_MORE_EVIDENCE=3`；没有生成 approved overlay 或修改生产配置。验证通过 `ruff check src tests`、目标测试 33 passed、全量 `pytest -q` 528 passed、`git diff --check` 和 `aits ops daily-plan --as-of 2026-05-13 --skip-risk-event-openai-precheck` smoke。
