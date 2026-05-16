# Shadow Parameter Iteration MVP

最后更新：2026-05-17

## 背景

现有 shadow parameter search 已能输出 trials、manifest、factorial attribution、
cap-level attribution、position change attribution 和 promotion contract 报告。
下一步需要一条独立 shadow-only 持续迭代流程，把 search 结果转成可追踪候选、
每日报告和 dashboard 只读 JSON，但不得改变 production 参数、gate、approved
overlay 或正式 prediction ledger。

## 范围

P0 MVP 实现：

1. 新增 `aits feedback run-shadow-iteration --as-of YYYY-MM-DD [--search-output-dir PATH]`。
2. 读取已有 search output，不重新计算 shadow backtest。
3. 分类候选为 `weight_only`、`gate_only`、`weight_gate_bundle`。
4. 维护 `data/processed/shadow_iteration_registry.csv`。
5. 生成 `outputs/reports/shadow_iteration_YYYY-MM-DD.md/json`。
6. 生成 `outputs/shadow_iterations/<run_id>/*` 作为本次运行的审计副产物。
7. 复用 `shadow_parameter_promotion_contract.yaml` 做状态和 blocked reason 输出。
8. 报告展示 attribution、position change 和 lineage；缺失时明确 `unavailable`。
9. 保持 `production_effect=none`，不写 production 权重、scoring、portfolio、
   approved overlay 或正式 prediction ledger。

P1 后续：

1. Dashboard 只读 Shadow Iteration Status 卡片。
2. `register-forward-shadow` 命令，仅更新 registry 状态。
3. 更完整 retirement 规则。
4. 新增字段字典或引用 `docs/schema/fields.yaml`。

P2 guardrails：

P2 不是本轮要启用的 production mutation，而是必须继续保持禁止：

1. 不自动修改 production `weight_profile_current.yaml`。
2. 不自动生成 approved calibration overlay。
3. 不实现 `approved_hard`。
4. 不将 `gate_only` 或 `weight_gate_bundle` 作为权重晋级候选。
5. 不生成 shrinkage production proposal。

## 状态规则

- `OBSERVED`：首次出现在本次 top group。
- `CANDIDATE`：满足基础可观察条件，但 promotion contract 仍未通过。
- `FORWARD_SHADOW_ACTIVE`：已经在 registry 中显式登记持续观察。
- `BLOCKED`：违反关键规则，或 candidate type 与 primary driver 不匹配。
- `RETIRED`：本次不再出现在 top group，或后续规则判定长期恶化。

## 验收标准

- 命令可基于既有 `outputs/parameter_search/<run_id>` 生成 registry、Markdown 和 JSON。
- 报告明确 production 参数未改变。
- 报告列出 best weight-only、best gate-only、best bundle、primary driver、
  blocked reasons 和 next action。
- JSON 可由 dashboard 只读消费，不要求 dashboard 重新计算 shadow 结果。
- production safety 测试覆盖关键禁止写入面。
- 缺少 cap-level attribution 或 position change data 时报告安全降级。

## 进展记录

- 2026-05-17：新增需求文档并进入 P0 MVP 实现。
- 2026-05-17：P0 MVP 进入 VALIDATING。已实现
  `aits feedback run-shadow-iteration`、registry 更新、Markdown/JSON 报告、
  run 目录 Trial Card/lineage/summary 输出、candidate 分类、promotion contract
  只读检查和 production safety 测试；当前样本 smoke 结论为 gate policy review，
  不产生权重晋级候选。P1 dashboard/register-forward-shadow/retirement/字段字典
  增强保留为后续范围。
- 2026-05-17：按 owner 要求继续推进 P1/P2。P1 将补 dashboard 只读卡片、
  `register-forward-shadow`、retirement 规则和字段说明；P2 按 guardrails
  验证，不启用 production mutation。
- 2026-05-17：P1/P2 基础实现完成待验证。Dashboard 已读取
  `shadow_iteration_YYYY-MM-DD.json` 并展示只读 Shadow Iteration Status 卡片；
  `register-forward-shadow` 只更新 registry 状态；retirement evidence 记录连续
  缺席 top group、连续表现恶化、primary_driver 从 weight 转 gate、drawdown/turnover
  contract 违反；字段说明已写入 `docs/schema/fields.yaml`；P2 guardrails 已进入
  JSON/Markdown 和测试。
- 2026-05-17：进入 VALIDATING。目标测试、全量 pytest 569 passed、ruff、
  窄范围 mypy、shadow iteration smoke、register-forward-shadow smoke、daily task dashboard smoke 和
  `ai_after_chatgpt` 回测验证已通过；首次使用 `quality_as_of=2026-05-14`
  被数据质量门因 2026-05-15 价格缓存正确阻断，改用与缓存一致的
  `quality_as_of=2026-05-15` 后数据质量报告 PASS，回测状态
  `PASS_WITH_LIMITATIONS`。P2 继续作为 production mutation guardrails，不启用
  production 权重、gate、approved overlay、approved_hard 或 shrinkage proposal 修改。
