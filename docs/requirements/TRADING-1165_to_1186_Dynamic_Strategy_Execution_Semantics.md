# TRADING-1165 to 1186 Dynamic Strategy Execution Semantics

## 背景

近期策略复核发现，多个动态策略可能在回测和报告链路中被隐含压缩为
`monthly rebalance` 或 `monthly execution`。这会混淆信号生成、目标权重、建议有效期、
实际执行路径和真实持仓路径，可能导致动态策略响应能力被低估，也可能导致 target
weight 被误读为 actual position。

本批任务覆盖 TRADING-1165～1186，目标是在 research-only 边界内建立动态策略执行语义
契约、execution policy registry、target-vs-actual path、执行频率敏感性审查、此前结论
影响复核、forward-aging/external-validation/reporting 契约升级和 owner review pack。

## 范围

### 第一阶段：隐含假设审计

- TRADING-1165：定义 dynamic strategy execution semantics contract。
- TRADING-1166：审计隐含 monthly rebalance / monthly execution 假设。
- TRADING-1167：建立 `config/research/strategy_execution_policy_registry.yaml`。
- TRADING-1168：审计 recommendation validity period 是否被隐含。

### 第二阶段：target vs actual 能力

- TRADING-1169：构建 target weight path 与 execution-constrained actual position path。
- TRADING-1170：生成 rebalance / execution frequency sensitivity suite。
- TRADING-1171：复核 monthly + threshold / override hybrid execution。
- TRADING-1172：量化 signal staleness cost。
- TRADING-1173：复核 signal close、decision date、execution date 的 latency / lag。

### 第三阶段：此前结论复核

- TRADING-1174：复核此前策略结论受 execution policy 的影响。
- TRADING-1175：识别可能被 monthly execution 误杀的候选。
- TRADING-1176：执行语义数据 lineage 和 anti-leakage 审计。
- TRADING-1177：成本与换手归一化。

### 第四阶段：接入外部验证、forward-aging 和 reporting

- TRADING-1178：外部 replay 支持 execution policy 与 actual position path。
- TRADING-1179：forward-aging observation contract 增加 execution-aware 字段。
- TRADING-1180：为 equal-risk 与 balanced-core 选择默认 execution policy。
- TRADING-1181：动态回测引擎契约要求 execution semantics。
- TRADING-1182：策略报告披露 execution assumption。

### 第五阶段：owner / roadmap / Reader Brief

- TRADING-1183：生成 rebalance assumption owner review pack。
- TRADING-1184：生成 execution semantics master review。
- TRADING-1185：更新 research roadmap。
- TRADING-1186：预览 Reader Brief 中 execution semantics 的安全展示方式。

## 非目标和安全边界

- 不新增实盘交易逻辑。
- 不进入 paper-shadow。
- 不进入 production。
- 不接入 broker。
- 不输出真实交易建议。
- 不修改原始 `equal_risk_qqq_sgov` forward-aging 历史 observation。
- 不因为高频或 hybrid execution 表现更好就自动提升任何策略状态。
- 不绕过 data quality、anti-leakage 或 external validation gate。

全局安全字段必须保持：

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`

## 设计决策

- `target_weight`、`recommendation_weight` 和 `actual_position_weight` 必须分离。
- 同一 `strategy_id` 在不同执行语义下必须显式绑定 `execution_policy_id`。
- monthly execution 不再作为未声明默认值使用；若使用必须来自 registry policy。
- 高频、threshold、override、validity、minimum holding 和 hysteresis policy 只进入
  research-only sensitivity，不构成实盘执行建议。
- 对需要重跑历史结果的结论，报告必须输出 `REBACKTEST_REQUIRED` / `NEEDS_REVIEW`
  类状态，而不是伪造完整重新回测结论。

## 验收标准

- 新增 22 个 `aits research strategies ...` CLI，均写出 JSON/Markdown artifacts。
- 新增 `config/research/strategy_execution_policy_registry.yaml`，包含附件要求的全部 policy
  和字段。
- 新增 report registry entries，覆盖 22 个 report ids。
- 更新 `docs/artifact_catalog.md` 和 `docs/system_flow.md`。
- 新增 `tests/test_execution_semantics.py`，覆盖 CLI、registry、artifact schema、safety fields、
  target-vs-actual path、owner/master/Reader Brief 输出。
- 指定验证通过：
  - `python -m ruff check src tests`
  - `python -m compileall -q src tests`
  - `python -m pytest -n 16 --dist loadfile tests/test_execution_semantics.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_equal_risk_growth_tilt.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_external_validation.py`
  - `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`
  - `git diff --check`

## 进展记录

- 2026-06-27：新增需求文档并进入实现。实现边界限定为 research-only execution semantics
  contract/audit/reporting layer，不批准 paper-shadow、production 或 broker action。
- 2026-06-27：实现完成并转入 `VALIDATING`。新增
  `config/research/strategy_execution_policy_registry.yaml`、22 个
  `aits research strategies ...` CLI/report artifacts、target-vs-actual actual position path、
  rebalance/threshold/validity/lag/cost-turnover sensitivity、owner review pack、master review、
  roadmap update 和 Reader Brief safe preview。真实 master status 为
  `EXECUTION_SEMANTICS_REQUIRES_REBACKTEST`，roadmap status 为
  `ROADMAP_REBACKTEST_REQUIRED`，表示部分旧结论需要在 explicit execution policy 和 actual
  position path 下重跑/复核；安全边界继续保持 no paper-shadow、no production、no broker。
