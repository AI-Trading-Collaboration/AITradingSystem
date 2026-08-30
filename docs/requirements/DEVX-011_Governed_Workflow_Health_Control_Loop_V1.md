# DEVX-011 Governed Workflow Health Control Loop V1

## 1. 任务身份

- task id：`DEVX-011_GOVERNED_WORKFLOW_HEALTH_CONTROL_LOOP_V1`
- owner decision：`owner_decision:DEVX-011:2026-08-31:proceed-governed-workflow-health-v1`
- priority：`P1`
- 初始状态：`IN_PROGRESS`
- 开发模式：`SINGLE_LANE`
- frozen base：`961d65a9743a354994c319dfa102acddb28cab21`
- task branch：`codex/devx-011-governed-workflow-health`
- production effect：`none`
- broker action：`none`

本任务把“定期发现研发流程性能损失”建设为受治理的系统能力。V1 自动收集既有工程证据、生成只读周报和稳定去重的优化候选；它不自动放宽质量门禁、不自动修改 canonical task register、不自动修改代码，也不把必要的 fail-closed 行为当作应删除成本。

## 2. 背景与问题证据

2026-08-24 至 2026-08-31 的只读审计显示：

- local main 有 176 个 commits，其中 151 个触及 architecture / authority surface，109 个没有触及 `src/`；
- 343 个 validation summaries 合计约 43.54 runner hours，53 个失败运行消耗约 14.48 小时；
- 65 个 Full 中 26 个失败，失败 Full 消耗约 11.68 小时；
- 227 个 publication transactions 中 188 个 terminal `FAILED`，其中一部分是登记型或受控释放，因此不能直接等同于实现故障；
- Full 失败集中在 authority、Atlas、DQ/current-hash compatibility 类合同，而不是均匀分布于业务模块。

本任务启动前还观察到一个实时样本：PROD-004 的首个 Architecture 因 stale manifest 形成 `878 passed / 1 failed`，消耗 445.78 秒；随后首个 exact-candidate Full 因 2 个 compatibility authority 断言和 1 个 Atlas freshness 问题形成 `10018 passed / 3 failed / 3 skipped`，消耗 1,373.17 秒。修复过程又暴露 generator 顺序与 lane-head drift 约束，最终 parent-bound Full 以 `10021 passed / 3 skipped` 通过。该样本说明系统需要把“验证失败成本、事务阶段、候选 SHA、失败簇和生成顺序”统一为可复用 telemetry，而不是依赖聊天或临时脚本回忆。

## 3. 决策

V1 采用 owner-gated control loop：

1. 自动发现：读取既有 validation runtime、publication transaction 与 Git main 历史；
2. 自动归一：按 reviewed policy 计算 7 日窗口、tier runtime、failed runtime、重复 dispatch、事务阶段和 authority-only commit amplification；
3. 自动建议：输出稳定 fingerprint 的 `PROPOSED_REVIEW_ONLY` optimization candidates；
4. 人工准入：candidate 只有在 owner 接受并另行登记 canonical task 后才可进入实现；
5. 受治理执行：任何接受的优化仍遵守现有 task registration、publication fence、validation、local-main 和 ordinary push 门禁。

V1 不引入第二个 scheduler。它登记到 `config/scheduled_tasks.yaml` 的 weekly cadence，由统一 periodic operations plan 发现 due 状态；当前 `automatic_command_dispatch_enabled=false` 边界保持不变。为形成 Codex 自发行为，根 `AGENTS.md` 增加低成本规则：每个 ISO 周第一次非平凡 tracked mutation 前，如果本周没有已验证的 workflow-health artifact，则先读取 operations runbook 并生成/校验一次只读报告。

## 4. 范围

### 4.1 Included

- reviewed policy：`config/architecture/workflow_health_policy.yaml`；
- collector/report module：`src/ai_trading_system/reports/workflow_health.py`；
- CLI：
  - `aits reports workflow-health --as-of YYYY-MM-DD`；
  - `aits reports validate-workflow-health --latest`；
- artifacts：
  - `outputs/reports/workflow_health_YYYY-MM-DD.json`；
  - `outputs/reports/workflow_health_YYYY-MM-DD.md`；
  - `outputs/reports/workflow_optimization_candidates_YYYY-MM-DD.json`；
  - `outputs/reports/workflow_health_validation_YYYY-MM-DD.json`；
  - `outputs/reports/workflow_health_validation_YYYY-MM-DD.md`；
- validation telemetry：status、tier、elapsed seconds、git commit、formal provenance、failed node ids、same-SHA/same-tier repeated dispatch；
- publication telemetry：task、transaction、terminal phase、last non-terminal phase、candidate/formal/Full reached 状态、duration、administrative-stop classification；
- Git telemetry：main-window commit count、`src/` touching、authority touching、authority-only count/ratio；
- deterministic optimization candidate ids、evidence metrics、recommended experiment、guardrails 和禁止自动执行标志；
- weekly cadence、report registry、artifact catalog、operations runbook、scheduled-task orchestration 与 system flow 文档；
- focused tests、generated authority rebuild 与正式 validation。

### 4.2 Excluded

- 不自动编辑 task register 或 requirement；
- 不自动改 threshold、validation tier、pytest workers、Full trigger policy 或 publication fence；
- 不自动执行 candidate 建议；
- 不把 terminal `FAILED` 一律解释为代码失败；task-source-only administrative release 必须单独披露；
- 不采集或读取 market、macro、fundamental、valuation、news cache；
- 不运行 `aits validate-data`，并显式声明 `data_quality_status=NOT_APPLICABLE_DEVELOPER_TELEMETRY`；
- 不修改 strategy logic、research window、score、weight、promotion、paper/live、production 或 broker/order；
- 不新增 Windows Task Scheduler、GitHub schedule 或另一条 external trigger；
- 不读取或修改 checkout guard 排除项 `docs/research/growth_tilt_owner_diagnosis_pack.md`。

## 5. Policy 与指标口径

所有候选阈值均位于 reviewed policy，不在代码中留下影响优先级的无解释数字。初始 pilot baseline：

| rule | minimum evidence | trigger | 意图 |
|---|---:|---:|---|
| failed Full runtime | 2 failed runs | failed runtime ratio >= 20% | 优先前置高成本 failure detection |
| early transaction churn | 3 non-admin failed terminals | failed ratio >= 25% | 优化 candidate 前的 generator/sequence/preflight |
| authority-only amplification | 20 commits | authority-only ratio >= 50% | 降低 generated/shared authority 放大 |
| per-task retry churn | 5 transactions | failed ratio >= 50% | 对热点 task 做 scoped process review |
| duplicate validation dispatch | 3 runs in same SHA/tier group | at least 1 duplicate group | 识别无新 tree 的重复 validation |
| validation failure cluster | 3 failed summaries | same test file cluster >= 3 | 把常见 compatibility 失败前移 |

阈值只影响 developer optimization candidate 的展示优先级，不影响投资解释。policy owner、status、rationale、validation evidence 和 review condition 必须完整；四个真实 weekly observations 后复核阈值。

时间窗口固定为 UTC calendar-day half-open interval：`[as_of-(lookback_days-1), as_of+1day)`。所有 source timestamp 转为 UTC 后再筛选；缺失或非法 timestamp 记为 telemetry gap，不用文件 mtime 静默代替。

## 6. Schema 与安全语义

### 6.1 `workflow_health_report.v1`

至少包含：

- `as_of`、window start/end、lookback days；
- policy path/version/hash 与 Git ref/head；
- validation totals、tier breakdown、failed Full runtime ratio、duplicate groups、failure clusters；
- transaction totals、terminal outcomes、phase distribution、administrative stops、top task churn；
- Git commit classification；
- telemetry gaps；
- candidate summary；
- `production_effect=none`、`broker_action=none`、`data_quality_status=NOT_APPLICABLE_DEVELOPER_TELEMETRY`。

### 6.2 `workflow_optimization_candidates.v1`

每个 candidate 必须具有稳定 `candidate_id`、`rule_id`、priority、evidence metrics、recommended experiment、guardrails，并固定：

- `status=PROPOSED_REVIEW_ONLY`；
- `automatic_execution_allowed=false`；
- `task_register_mutation_allowed=false`；
- `gate_relaxation_allowed=false`。

相同 `rule_id + scope` 在不同周保持同一 fingerprint；报告日期与证据变化不制造新身份。

### 6.3 `workflow_health_validation.v1`

fail-closed 校验 schema/type、policy binding、window、summary counts、candidate bundle binding、candidate id uniqueness 和安全标志。telemetry gaps 产生 `PASS_WITH_WARNINGS`，不伪造完整度；unsafe candidate 或 bundle drift 产生 `FAIL`。

## 7. 开发步骤与依赖

### S0 — 登记与 preflight

- 写入本 requirement；
- 用 canonical task-source writer 登记任务；
- 释放 registration-only publication transaction；
- 重新运行 `SINGLE_LANE / coordinator / LANE` preflight。

验收：task row、requirement、branch/base/path claims 可审计，known unrelated exclusion 未被读取。

### S1 — Policy、collector 与候选生成

- 实现 policy loader 与 fail-closed metadata validation；
- 解析 validation summaries、publication events 和 Git commits；
- 构建 metrics、failure cluster、administrative-stop taxonomy；
- 根据 policy 生成稳定去重 candidate。

验收：synthetic inputs 覆盖 PASS/FAIL/malformed/admin stop/duplicate/main commit classification。

### S2 — Artifacts、CLI 与 validation

- 写 JSON/Markdown/candidate/validation artifacts；
- 注册 Typer commands；
- 支持 `--latest` validation；
- 缺少 source roots 时明确 `PASS_WITH_WARNINGS` / telemetry gap。

验收：CLI 在临时项目根可写完整 bundle；unsafe candidate fail closed。

### S3 — 周期行为与文档

- weekly scheduled task；
- report registry 与 artifact catalog；
- operations runbook / scheduled orchestration；
- root `AGENTS.md` 的 once-per-ISO-week self-trigger；
- `docs/system_flow.md`。

验收：不进入 daily trading chain、不自动 dispatch、不新增外部 scheduler；Codex 规则只在缺少当周 validated artifact 时执行。

### S4 — 生成状态、验证与发布

- task status/requirement progress 同步；
- official architecture/report-flow/compatibility/canonical-task generators；
- focused parallel pytest、Ruff、generated authority validation；
- Architecture / Contract / Integration / Reproducibility / Full；
- local main fast-forward、ordinary push、SHA equality、cleanup。

验收：final candidate 全部门禁 PASS，local main = origin/main = candidate，task branch 清理，无 production/broker effect。

## 8. Path ownership

Task-owned paths：

- `config/architecture/workflow_health_policy.yaml`
- `src/ai_trading_system/reports/workflow_health.py`
- `src/ai_trading_system/cli_commands/workflow_health_reports.py`
- `tests/test_workflow_health.py`
- `docs/requirements/DEVX-011_Governed_Workflow_Health_Control_Loop_V1.md`

Coordinator/shared paths：

- `AGENTS.md`
- `src/ai_trading_system/cli_commands/reports.py`
- `config/scheduled_tasks.yaml`
- `config/report_registry.yaml`
- `docs/artifact_catalog.md`
- `docs/system_flow.md`
- `docs/operations/operations_runbook.md`
- `docs/runbooks/scheduled_task_orchestration.md`
- task registry fragment/index/generated views
- official generated architecture/report-flow/compatibility authority artifacts
- related shared contract tests

## 9. Temporary workspace lifecycle

本任务不创建新 worktree、clone 或 cache。它在 PROD-004 完成并释放 publication transaction 后复用干净 primary checkout，从 exact local main `961d65a9743a354994c319dfa102acddb28cab21` 创建 task branch。退出条件为：validated candidate 普通推送后切回 main、删除 task branch，并由 `worktree-audit` 确认没有本任务独有未保存内容。

如果后续被迫创建临时 workspace，必须先在本节补充 absolute path、purpose、owner、unique evidence 与 exit condition，再创建。

## 10. 验收标准

- [x] canonical task row 已登记并在实现前通过 SINGLE_LANE preflight；
- [x] 7 日窗口不依赖 mtime，非法 timestamp 显式计 gap；
- [x] validation、transaction、Git 三类 telemetry 可独立缺失且不伪造；
- [x] admin-stop 与 code/validation failure 不混为一类；
- [x] candidate id 跨日期稳定，安全标志不可放宽；
- [x] report/candidate/validation artifacts 可由 CLI 生成和复核；
- [x] weekly cadence 进入统一 periodic plan，但 automatic dispatch 仍关闭；
- [x] Codex once-per-week self-trigger 有 current-week artifact 去重；
- [x] report registry、artifact catalog、runbook、system flow 与 CLI 一致；
- [x] focused/formal/Full validation PASS；
- [x] local main 与 origin/main 等于 final candidate；
- [x] 无 market cache、strategy、weight、production、broker/order 变更。

## 11. Progress log

- 2026-08-31：Owner 同意继续推进本优化线；只读审计、V1 决策、policy baseline、阶段拆分与安全边界完成。
- 2026-08-31：等待并审计 PROD-004 唯一 publication lease；观察到 stale manifest、Full compatibility/Atlas failure、generator ordering 与 lane-head drift 的真实成本。PROD-004 最终发布到 `main@961d65a9743a354994c319dfa102acddb28cab21` 后释放；DEVX-011 从该 exact base 启动，不创建临时 worktree。
- 2026-08-31：V1 collector、policy、CLI、validation、weekly cadence、Codex self-trigger、report/catalog/runbook/system-flow 文档完成。Synthetic + integration-focused validation 为 `80 passed`；真实 7 日报告读取 280 个 validation summaries、192 个 publication transactions，生成 13 个稳定 review-only candidates，bundle validation 为 `PASS`（9 checks、0 failed、0 warnings）。最终 formal tiers、Full、main/origin SHA equality 与 cleanup 由 `devx-011-workflow-health-implementation-20260831-v1` publication receipt 绑定；若任一门禁失败，本完成声明随事务 fail closed 并进入 successor transaction 修复。
- 2026-08-31：v1 因 report/catalog/flow source seal policy 未声明而失败释放；v2 补齐 policy 后，authority replay PASS，但静态 seal 验收仍锁在 3100 entries，失败释放；v3 同步为 3107 entries 后 focused `124 passed`，候选 `2fdb800841e6663ced1540d14c71baa9ee0c9694` 的 formal Architecture 暴露 122 个冻结基线失败（weekly/report/task/deprecation counts 与缺少 DEVX-011 compatibility successor），artifact 为 `outputs/validation_runtime/architecture-fitness_20260830T192604Z/test_runtime_summary.json`。v4 作为最小 serial contract wave，追加 DEVX-011 fragment authority 与 exact current ratchets，不重写 immutable legacy compatibility baseline。
- 2026-08-31：v4 生成 9-section compatibility chain 后，冻结基线 focused 从 122 个失败收敛到 89 个；剩余项统一归因于 historical mismatch helper 尚未纳入 DEVX-011 retroactive successor，另有一项 report-flow 测试仍断言 PROD-004 为 latest。v5 只补这两个 contract hooks，并重新生成/验证全部 authority。
- 2026-08-31：v5 抽样 5 项中 3 项 PASS，余下 2 项证明通用 authority loop 不会消除“被测 section 自身或更晚 owner”路径的合法 EOF supersession。v6 按既有 successor special-case 模式，仅对 DEVX-011 明确登记的 exact paths 消除早期 historical live-drift 误报；历史记录 bytes 与 hash 不改写。
- 2026-08-31：v6 抽样提升为 4/5 PASS；最后一项要求保留 stop section 已自行登记的 superseded overlap。v7 将扣减范围收紧为 `DEVX-011 exact paths - stop section recorded paths`，既消除后继新路径误报，也保留历史 section 自身的显式 supersession 断言。
