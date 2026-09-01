# LLM 正式风险评估准入

状态：IN_PROGRESS

最后更新：2026-09-01

## 背景

Owner 决策：高优先级官方候选可以参考 LLM 的复核结果作为正式评估结果，短期内不要求每条候选先人工复核。

这会改变政策/地缘模块的正式输入口径。实现必须保留审计边界：LLM formal assessment 是正式评估来源，但不是人工复核，也不能伪装成人工复核。

## 范围

新增命令：

- `aits risk-events apply-llm-formal-assessment`

输入：

- `data/processed/risk_event_prereview_queue.json`

输出：

- `data/external/risk_event_occurrences/*.yaml`
- `outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md`
- `outputs/reports/risk_event_occurrences_YYYY-MM-DD.md`

## 口径

- LLM formal assessment 可以写入正式 risk occurrence YAML。
- `reviewer` 必须写成 `llm_formal_assessment:<model>`，不得写成人工 reviewer。
- evidence sources 同时保留原始官方来源和 `llm_extracted` 评估来源。
- LLM formal evidence 默认最高按 `B` 级处理，可进入普通评分，但不能单独触发 position gate。
- `status_suggestion=active_candidate` 才转换为 `active`；`watch/candidate` 默认转换为 `watch`。
- 可以写入 LLM formal attestation，表示 LLM 已覆盖本次队列中的高优先级候选；它不是人工全量复核声明。
- 日报来源类型必须显示为 `llm_formal_assessment`，置信度低于人工复核。

## 2026-05-12 临时运行口径

Owner 决策：短期没有精力每日人工复核政策/地缘候选；如果日报卡在人工复核缺口，可以放宽该限制，把 LLM formal assessment 作为可信结论使用。

该口径是接受的临时绕行方案，不是人工复核完成：

- 存在原因：每日人工复核 SLA 暂不可用，但政策/地缘模块长期停留 `insufficient_data` 会压低 5 月 12 日及后续日报判断置信度。
- 行为影响：`score-daily` 在 OpenAI 官方来源预审成功后，可以自动写入 LLM formal occurrence 和 LLM formal attestation；full coverage 的 `llm_formal_assessment` 模块置信度从 55% 调整为 65%，不再触发 `低置信度模块：policy_geopolitics`。
- 风险：LLM formal 只覆盖本次官方候选和预审队列，不等同人工全量风险消除证明；它仍不得伪装为 `manual_input`，不得单独触发 `position_gate`，L2/L3 或 active_candidate 仍需在报告中保留“未人工复核”边界。
- 验证覆盖：单元测试覆盖 LLM formal 写入、风险事件校验、日报 `policy_geopolitics` 来源类型和置信度；目标 CLI 测试覆盖自动 formal 写入路径。
- 退出条件：建立稳定人工复核责任人、来源范围和每日/交易日复核 SLA 后，重新评估是否把 LLM formal 置信度降回低于 60%，或只保留为人工复核前置队列。

## 验收标准

- 命令能读取 prereview queue，生成 occurrence YAML、LLM formal attestation 和报告。
- 生成的 occurrence 保留 model、reasoning effort、request id、input/output checksum、status/level suggestion、confidence、precheck id 和“未人工复核”说明。
- `validate-occurrences` 能读取并校验 LLM formal occurrence。
- 日报政策/地缘模块在只存在 LLM formal attestation 时不再显示 `insufficient_data`，但来源类型为 `llm_formal_assessment`。
- LLM formal assessment 默认不单独触发 position gate。
- `score-daily` 成功完成 OpenAI 官方来源预审后，可自动写入 LLM formal occurrence/attestation；重复运行同一 as-of 时必须可审计且可覆盖同名 LLM formal 输出。
- README、系统流图、任务登记和测试同步更新。

## 2026-09-01 unknown risk ID 运行事故与耐久修复

### 事故证据

- terminal run：`daily_ops_run:2026-08-31:20260901T003624Z`；
- 失败步骤：`score_daily`；
- validator code：`unknown_risk_event_id`；
- invalid event id：`data_center_farmland_federal_benefit_restriction_candidate`；
- retained occurrence SHA-256：
  `30ab1e691c3cca671435c43e8567679f7bd78553fe4ea27ebacc90858e3d4c2a`；
- 该 occurrence 固定为 `monitor_only`、`used_in_alpha=false`、`used_in_gate=false`，但这不允许
  绕过 reviewed risk taxonomy 或降低 validator。

根因是 `_select_event_id` 在找不到 configured `event_id` 时退回首个 LLM
`matched_risk_ids`，使 producer 写出 validator 必然拒绝的 occurrence。该行为违反
`risk_events_path` 的准入语义，也使同一 daily 在写件后才 fail closed。

### 决策与安全边界

Owner 于 2026-09-01 明确要求先修复再重跑。本轮把该指令解释为：

1. 修复 producer，使 reviewed risk config 成为唯一 admission authority；
2. 对已经写出的 invalid occurrence 执行保留原始 bytes 的 governed quarantine；
3. 不自动把 unknown ID 加入 risk rule，不把 LLM candidate 提升为投资结论；
4. 不降低 `unknown_risk_event_id` validator，不删除或改写旧 evidence；
5. 不使用同 `as_of` ordinary 或不在 allowlist 的 recovery；只有 exact release promotion 后、
   resolver 返回严格更新的 provider-ready trading day，才允许下一次 ordinary daily。

### 实施步骤与依赖

1. **Strict admission**：只要任一达到 confidence 门槛的 record 没有 configured matched ID，
   report 必须产生 typed `ERROR`，并在 write API 前保证整批零 occurrence、零 attestation；
   matched list 同时含 unknown 与 known 时只允许选择 known ID。
2. **Quarantine contract**：新增 hash-bound quarantine API/CLI；源文件必须是 occurrence root 的
   direct child，目标固定在该 root 的 `quarantine/`，receipt 记录 source/target、size、SHA-256、
   reason、authorization、timestamp、`production_effect=none` 与 `broker_action=none`。读取并绑定源
   bytes/hash 后执行同 volume 原子 move，复核 target，再以临时 receipt + 原子 replace 发布 receipt；
   target 或 receipt 发布失败时回滚 source，并复核回滚 bytes，且不得留下两个 active source。
3. **Post validation**：quarantine 后重读 active non-recursive occurrence store，要求 unknown-ID
   error 消失；quarantine 目录继续保留原始文件和 receipt，但不进入 active store。
4. **文档与测试**：覆盖 unknown-only、mixed known/unknown、零写件、CLI quarantine、tamper/
   containment、重复 quarantine 和 post-validation；同步 operations runbook 与 system flow。
5. **发布与验收**：focused 并行 pytest 后走 Architecture、Contract、Integration、
   Reproducibility、Full；普通 push、exact release promotion、deployment acceptance；最后仅在合法
   新 `as_of` 用 runtime-local unified daily trigger 验收。

### 临时工作区生命周期

- owner task：`RISK-012`；
- path：`D:\Work\AITradingSystem_risk012_unknown_id`；
- branch：`codex/risk-012-unknown-id-failclosed`；
- frozen base：`9e2e3f04a0092c5fe1477b88842135aa01834654`；
- purpose：本节步骤 1～5 的单 lane 实现、验证与集成；
- exit condition：main/origin/runtime release 均等于 validated candidate，canonical evidence 已保全，
  tracked/untracked/ignored inventory 无唯一内容且无进程依赖后，使用 `git worktree remove` 与
  `git worktree prune` 清理；若中止，先在本要求与 canonical task event 记录 blocker、next owner、
  retained evidence 和新的退出条件。

## 进展记录

- 2026-05-10：任务创建并进入实现。
- 2026-05-10：基础版完成。新增 `aits risk-events apply-llm-formal-assessment`，可把 `risk_event_prereview_queue.json` 写为正式 risk occurrence YAML 和 LLM formal attestation；日报政策/地缘模块新增 `llm_formal_assessment` 来源类型，置信度上限低于人工复核。真实 2026-05-10 队列写入 5 条 `watch` occurrence 和 1 条 LLM formal attestation，`validate-occurrences` 校验 PASS；报告为 `outputs/reports/risk_event_llm_formal_assessment_2026-05-10.md` 与 `outputs/reports/risk_event_occurrences_2026-05-10.md`。验证通过 `python -m ruff check src tests`、CLI help 和完整 `python -m pytest -q` 444 passed。
- 2026-05-12：owner 批准短期放宽人工复核限制。`score-daily` OpenAI 预审成功后会自动写入 LLM formal assessment，并将 full coverage `llm_formal_assessment` 置信度提升到 65%，以解除 `policy_geopolitics` 低置信提示；人工复核声明仍只能由真实 reviewer 显式写入。验证：`python -m ruff check` 目标文件通过，目标 pytest 41 passed，完整 `python -m pytest -q` 467 passed。
- 2026-06-07：`RISK-013` 收口为 `DONE`。最新真实
  `aits ops daily-run --as-of 2026-06-05 --run-id codex_20260605_20260607103901`
  中 OpenAI 预审和 LLM formal 自动写入均通过，`risk_event_llm_formal_assessment_2026-06-05.md`
  写入 5 条，日报 `policy_geopolitics` 覆盖率 100%、置信度 65%，未触发风险事件
  position gate。人工复核边界、LLM formal 非人工声明、不得单独触发仓位闸门和退出条件继续保留。
- 2026-09-01：任务因上述 terminal daily 事故从 `BASELINE_DONE` 重开为 `IN_PROGRESS`；canonical
  task event 已记录 strict admission、quarantine、formal validation、release promotion 与合法新日期
  ordinary daily 验收要求。工程 lane 与 publication transaction
  `risk-012-unknown-id-failclosed-20260901-v4` 已建立。
- 2026-09-01：strict producer admission、unknown-ID governed quarantine API/CLI、receipt replay、
  exact-byte preservation、containment/tamper/idempotency 和 post-validation 已实现。`ruff check`、
  `ruff format --check` 通过；risk/formal/daily/CLI/ops 相关并行 pytest 共 `134 passed`。对 terminal
  incident retained YAML 的离线副本执行受治理隔离演练，source/target SHA-256 均为
  `30ab1e691c3cca671435c43e8567679f7bd78553fe4ea27ebacc90858e3d4c2a`，active store 校验通过。
  本记录不表示 runtime 已 promotion，也不授权在 provider-ready gate 前重跑 daily。
- 2026-09-01：首轮正式 Architecture tier 在 `e10cb357a25866158702c49fc64bdde821d0ddb3`
  上 fail closed（`119 failed / 763 passed`）；所有失败由本轮 source bytes 尚未进入最新 append-only
  compatibility hash authority 级联产生，evidence 为
  `outputs/validation_runtime/architecture-fitness_20260901T022528Z/test_runtime_summary.json`。publication
  transaction v4 已按 FAILED 释放。后续 v6 只通过官方 compatibility authority builder 追加 RISK-012
  successor fragment 并复跑正式 parallel gate，不重写历史 section、不把失败降级为串行 PASS。
- 2026-09-01：v6 已追加 successor，但第二轮 Architecture 在 `59cd330981c524cc70fd4baffaf0ce9a9bbda2c4`
  上仍 fail closed（`116 failed / 767 passed`）；root cause 是 historical test helper 尚未把新 section
  登记为 retroactive current-hash authority，导致 RISK-012 delta 没有从早期 section mismatch 中扣除。
  evidence 为 `outputs/validation_runtime/architecture-fitness_20260901T024108Z/test_runtime_summary.json`，
  v6 已按 FAILED 释放。v7 补齐新 authority 的优先级、inherited supersession union 与 historical
  mismatch subtraction 后，必须再次运行相同 parallel gate。
- 2026-09-01：v7 的 historical compatibility focused replay 已通过；正式 Architecture 只剩
  `module_manifest_fresh`、`test_manifest_fresh` 与 `aggregate_shadow_index_reproducible` 三项失败，
  证明新 source/test authority 已被正确消费，但由这些 bytes 派生的官方 architecture manifests
  需要重建。v7 因未声明 `architecture-manifests` generator 而按 FAILED 释放；v8 明确声明先运行
  `architecture-manifests`、后运行 `compatibility-authority`，并在最终树上重跑完整正式验证。
- 2026-09-01：v8 的 Architecture、Contract、Integration、Reproducibility 分别以 `883 passed`、
  `278 passed`、`995 passed`、`24 passed` 通过；Full 在候选
  `49f581380d52b847ae3cb9417ef6f576f3aed802` 上 fail closed（`10062 passed / 34 failed / 6 skipped`），
  evidence 为 `outputs/validation_runtime/full_20260901T034124Z/test_runtime_summary.json`。其中 2 项
  是另一份 historical compatibility helper 未登记 RISK-012 successor，6 项是 report/catalog/flow
  authority 及其文档 shadow 在本轮文档变更后未重建，余下 26 项来自隔离 worktree 未携带 Full
  测试显式依赖的 ignored canonical O1、Atlas 与 QQQ exact-signal evidence。v8 已按 FAILED 释放；
  后续 transaction 必须修复两类 tracked authority 缺口，并以 exact path/size/SHA-256 将所需
  ignored evidence 纳入验证工作树，禁止把缺失 evidence 伪装成测试 PASS 或提交进 repository。
- 2026-09-01：v12 因 `report-flow-authority` 检出旧 source seal 而失败；v13 显式更新
  `docs/artifact_catalog.md` 与 `docs/system_flow.md` 的 exact byte/hash/blob seals，并完成三类官方
  generator replay，但 focused rerun 继续暴露 DEVX-006D 测试仍冻结旧 section/hash、以及 QQQ
  exact-signal 的第二个 canonical signal-package 未纳入隔离验证工作树。两次 transaction 均按
  FAILED 释放。v14 将这些精确路径加入 scope，所有 tracked test/helper 格式化必须先于 generator，
  O1 与两组 QQQ ignored evidence 仅作为 exact-byte validation inputs。Atlas 仍独立 fail closed 于
  未分类 successor `TRADING-2552_QQQ_OPTIONS_CONDITIONAL_PAIRED_COMPARISON_OWNER_REVIEW_V1`；不得由
  RISK-012 自动代替 owner/page classification 决策。
- 2026-09-01：TRADING-2552 与 DEVX-010 serial wave 推进 main 后，v17 在 reviewed base-drift plan
  `integration-revalidation-045da087d369a218b5e6` 上完成 task/source/generated reconciliation；首轮
  focused replay 为 `192 passed / 126 failed`。失败全部集中于 historical compatibility helper 仍把
  DEVX-012 视为最新 section、且未把 RISK-012 successor 纳入 mismatch subtraction；核心 risk/CLI
  路径未暴露同类批量失败。v17 因 generator-post 后才由 focused evidence 暴露该 tracked helper
  缺口而按 FAILED 释放；v18 必须先合并 helper 语义、重新生成全部 authority，再提交候选。
