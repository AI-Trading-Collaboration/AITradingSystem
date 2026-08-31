# DEVX-012 Automatic Workflow Health Trigger And Outcome Review V1

## 1. 任务身份

- task id：`DEVX-012_AUTOMATIC_WORKFLOW_HEALTH_TRIGGER_AND_OUTCOME_REVIEW_V1`
- owner decision：`owner_decision:DEVX-012:2026-08-31:auto-trigger-workflow-health-existing-daily-automation-v1`
- owner instruction：`最好还是想办法自动触发，定期梳理优化成果`
- priority：`P1`
- 初始状态：`IN_PROGRESS`
- 开发模式：`SINGLE_LANE`
- frozen base：`a2ddfac116fc54a2b14026b4373427dd70663141`
- task branch：`codex/devx-012-workflow-health-auto-trigger`
- production effect：`none`
- broker action：`none`

本任务把 DEVX-011 的“自动发现、人工执行”提升为受控的“自动生成周报、人工决定优化落地”。
它复用现有 Codex automation `aitradingsystem-pit` 的同一次 daily invocation，不创建第二个
cron、Windows Task Scheduler、GitHub schedule 或独立业务 scheduler entry。自动行为只允许生成、
校验和去重 developer telemetry 报告；候选代码变更、task-register mutation、validation gate
调整、production 或 broker 行为继续禁止自动执行。

## 2. 问题与目标

DEVX-011 已提供 `aits reports workflow-health`、7 日工程 telemetry、稳定 candidate fingerprint
和 weekly registry，但 `automatic_command_dispatch_enabled=false`，因此 owner 仍需记住并人工触发。
另外，单周报告主要展示当前窗口，没有把本周相对上周的改进、回退、新增候选、持续候选和已消退
候选汇总为“优化成果”。

V1 目标：

1. 现有 active daily Codex automation 每次 invocation 在完成或依规跳过唯一业务 `daily-run`
   后，调用一个 R0 developer-telemetry gate；
2. gate 按 ISO week 检查已有 report/candidate bundle，并重新执行 independent validation；
3. 当周已有有效 bundle 时返回可审计 `ALREADY_CURRENT`，不重写报告；
4. 当周缺失时生成一次新报告并 fail-closed 校验；无效同日期 bundle 不覆盖，下一 daily date 重试；
5. 报告比较最近一个先前 validated weekly bundle，汇总指标改善/回退与 candidate lifecycle；
6. 自动阶段失败只影响 developer telemetry receipt，不覆盖 daily investment/DQ/PIT 结论；下一次
   daily invocation 可在相同 ISO week 重试；
7. 自动化保持一个 existing schedule，且任何优化落地仍需 owner 接受并另行登记 canonical task。

## 3. 设计决策

### 3.1 触发路径

- external scheduler：复用 `aitradingsystem-pit`；不得创建新 automation；
- business trigger：仍然每次最多调用一次 runtime-local `aits ops daily-run`；
- developer telemetry post-stage：daily-run 后切换到 exact development checkout，调用
  `aits reports ensure-workflow-health --as-of YYYY-MM-DD`；
- post-stage 必须要求 `local main = origin/main = HEAD`、当前 branch 为 `main`、policy 明确允许
  automatic report generation；不满足时写 `BLOCKED` receipt，不能切换、stash、pull、clean 或猜测；
- automation update 是 owner 已明确授权的 R2 local automation change；状态、schedule、model、
  notification policy 和原 daily-run prompt 其余字段必须保持不变。

### 3.2 Once-per-week 与恢复

- 周身份固定为 ISO year/week；
- 当前周任意日期生成且 independently validated 为 `PASS` 的 bundle 都满足本周；
- `PASS_WITH_WARNINGS` 仍是有效 developer telemetry，但 receipt 必须披露 warnings；
- invalid/tampered/missing candidate bundle 不得被当作 dedup evidence；同日期残留不得覆盖，后续
  daily invocation 使用新的 `as_of` 留存重试证据；
- failed/blocked receipt 不算完成，下一次 existing daily automation invocation 可重试；
- 不使用文件 mtime 判定日期或 freshness。

### 3.3 优化成果口径

当前报告与最近一个更早、可独立重验的 weekly bundle 比较：

- failed validation runtime ratio；
- failed Full runtime ratio 与 failed Full count；
- non-admin publication failure ratio；
- authority-only commit ratio；
- duplicate validation group count；
- optimization candidate count；
- candidate lifecycle：`new`、`recurring`、`resolved`。

每个指标必须声明方向（越低越好或只披露），输出 delta、improved/regressed/unchanged 分类；没有可信
baseline 时明确 `NO_BASELINE`。趋势只是研发流程观测，不证明因果关系，也不自动关闭任务。

## 4. 实施步骤

### S0 — 任务登记与 preflight

- 创建本 requirement；
- 通过 publication fence 登记 canonical task；
- 运行 `SINGLE_LANE / coordinator / LANE` preflight。

### S1 — Idempotent gate 与 receipt

- 增加 `ensure-workflow-health` CLI；
- 增加 current-week bundle discovery、independent validation 和 receipt schema；
- 校验 main/origin/branch identity、policy automatic-report boundary 与 artifact commitments；
- current-week valid bundle 不重写，invalid bundle 重新生成并留痕。

### S2 — Outcome review

- 加载最近一个更早的 validated report/candidate bundle；
- 生成 metric deltas、improved/regressed/unchanged 和 candidate lifecycle；
- JSON、Markdown、Reader Brief 同步披露，validation 覆盖 trend binding。

### S3 — Orchestration、文档与 automation

- `workflow_health_policy.yaml` 记录 existing-automation binding、ISO-week dedup、retry 和 standing
  owner decision；
- 更新 scheduled task、runbook、scheduled orchestration、report registry、artifact catalog、system
  flow；
- 在 repo 通过正式验证并发布后，更新现有 `aitradingsystem-pit` automation prompt；不创建新 id。

### S4 — 验证与发布

- focused parallel pytest、Ruff、generated authority validation；
- Architecture / Contract / Integration / Reproducibility / Full；
- local main fast-forward、ordinary push、SHA equality；
- automation `view` 复核 ACTIVE、原 rrule/model/notification 保持不变且 prompt 包含新 post-stage；
- 清理 task branch，worktree-audit PASS。

## 5. Path ownership

Task-owned：

- `config/architecture/workflow_health_policy.yaml`
- `src/ai_trading_system/reports/workflow_health.py`
- `src/ai_trading_system/cli_commands/workflow_health_reports.py`
- `tests/test_workflow_health.py`
- `docs/requirements/DEVX-012_Automatic_Workflow_Health_Trigger_And_Outcome_Review_V1.md`

Coordinator/shared：

- `config/scheduled_tasks.yaml`
- `config/report_registry.yaml`
- `docs/artifact_catalog.md`
- `docs/system_flow.md`
- `docs/operations/operations_runbook.md`
- `docs/runbooks/scheduled_task_orchestration.md`
- canonical task fragment/index/generated views；
- `config/architecture/devx_006d_report_catalog_flow_authority.yaml`
- `registry/report_catalog_flow_authority/fragments/**`
- `inputs/architecture/devx_006d_report_catalog_flow_authority_index.json`
- `inputs/architecture/devx_006d_report_catalog_flow_consumer_inventory.json`
- `registry/architecture_compatibility_authority/fragments/**`
- `inputs/architecture/devx_006c_compatibility_authority_index.json`
- `inputs/architecture/devx_006c_compatibility_consumer_inventory.json`
- `inputs/architecture/arch_004g_deprecation_inventory.yaml`
- compatibility/report-flow/deprecation authority 与相关 contract tests。

Local automation state（Git 外、发布后更新）：

- `C:\Users\32739\.codex\automations\aitradingsystem-pit\automation.toml`

## 6. 安全、证据与退出边界

- `production_effect=none`、`broker_action=none`、`market_data_read=false`；
- 不运行 `aits validate-data`，固定
  `data_quality_status=NOT_APPLICABLE_DEVELOPER_TELEMETRY`；
- 自动生成报告不等于自动执行 candidate；`automatic_execution_allowed=false`、
  `task_register_mutation_allowed=false`、`gate_relaxation_allowed=false` 保持；
- automation 更新前保存 tool view 中的完整字段；更新失败不以手工编辑 TOML 绕过；
- repo 发布失败时不更新 automation；automation 更新后验证失败时恢复原完整 prompt；
- 不创建临时 worktree、clone 或 cache；退出条件为 validated candidate 已发布、automation binding
  复核通过、task branch 删除且 governed audit 无本任务独有残留。

## 7. 验收标准

- [x] canonical task 已登记并通过 SINGLE_LANE preflight；
- [x] current-week valid bundle 被自动复用且 bytes 不变；
- [x] missing bundle 自动生成、独立校验并写 receipt；invalid 同日期 bundle 留存不覆盖；
- [x] 失败 receipt 可在同周后续 invocation 重试；
- [x] outcome review 对可信 previous bundle 输出 metric delta 和 candidate lifecycle；
- [x] 无 baseline 时明确 `NO_BASELINE`；
- [x] report/candidate safety boundary 未放宽；
- [ ] 只更新 existing `aitradingsystem-pit`，没有第二个 automation/scheduler；
- [ ] focused/formal/Full PASS；
- [ ] local main = origin/main = final candidate；
- [ ] automation ACTIVE 且 schedule/model/notification 未被意外改变；
- [ ] production、broker、market data、strategy、weights 均无变化。

## 8. Progress log

- 2026-08-31：Owner 要求将 workflow-health 改为自动触发并定期梳理优化成果；确定采用 existing
  daily automation post-stage、ISO-week independent validation/dedup、trend comparison 与
  review-only candidate boundary。
- 2026-08-31：focused workflow-health tests 与 Ruff PASS；开发期间 local `main` 从 frozen base
  `a2ddfac1` 前进，按治理要求保留 frozen lane 并在自然集成边界生成 base-drift plan，不执行隐式
  rebase/merge。
- 2026-08-31：最终候选 `30fc15ad` 的 Architecture / Contract / Integration /
  Reproducibility 分别为 `882 / 278 / 995 / 24` PASS；首次 Full 为
  `10039 passed / 1 failed / 3 skipped`。唯一失败来自本地 ignored Atlas canonical bundle 仍绑定
  `a2ddfac1`，按官方 renderer 刷新到最终候选后，live validation 已恢复 `CURRENT / PASS`；聚焦
  复现进一步确认 `tests/atlas/test_historical_projection_review.py` 的 reviewed-successor 白名单只到
  `TRADING-2548`，而当前合法 coverage 已包含 `TRADING-2549` 与 `TRADING-2550`。v12 已绑定原始
  Full FAIL artifact 并按失败释放；后续只扩展该显式测试 authority、刷新 generated authority 与
  final-commit Atlas ignored bundle，再以失败 Full artifact 为 parent 执行正式重验和 Full rerun。
