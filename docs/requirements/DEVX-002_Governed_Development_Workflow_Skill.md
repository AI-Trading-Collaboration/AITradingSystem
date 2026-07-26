# DEVX-002：Governed Development Workflow Skill

最后更新：2026-07-26

稳定任务 ID：`DEVX-002_GOVERNED_DEVELOPMENT_WORKFLOW_SKILL`

Owner 决定：
`owner_decision:DEVX-002:2026-07-26:adopt_governed_local_main_skill_v1`

后续 Owner 决定：
`owner_decision:DEVX-002:2026-07-26:default_ordinary_push_after_local_main_v2`

状态：`BASELINE_DONE`

## 1. 问题与目标

项目已经分别建立 task-register、known-unrelated worktree audit、temporary workspace
lifecycle、ARCH-005 dual-lane control plane、S4D checkout lease guard 和 formal validation
纪律，但执行入口分散。v1 owner决定曾把默认收口边界从历史 S4C 的自动 ordinary push
收窄到local `main`。v2 owner决定在验证这条本地集成链后恢复默认ordinary push，因此当前流程为：

1. 从本地 `main` 的 exact HEAD 创建任务分支；
2. 先完成任务登记、归属、实现和验证；
3. 单线任务把已验证分支 fast-forward 到本地 `main`；
4. 双线任务不得逐分支直接 fast-forward `main`，必须先进入 coordinator integration
   branch，按固定顺序形成单一候选 tree，再一次性 fast-forward 本地 `main`；
5. local-main fast-forward后fetch remote main，只有远端仍是candidate祖先时执行普通
   non-force push并复核双SHA；
6. 已合并分支和临时工作区只在完成内容、证据、进程和可恢复性审计后清理。

PR、force-push、history rewrite、自动merge/rebase、remote divergence修复或发布含无关改动的
candidate不在默认授权内。Owner显式要求no-push、无remote/upstream或上述门禁失败时跳过push并
报告，不能把默认push解释为修复远端历史的授权。

本任务把上述流程固化为项目规则和可复用 Codex skill，降低工作区污染、base drift、第二
并行分支无法 fast-forward、shared writer 冲突、重复 Full、临时工作区遗留和远端误发布风险。

## 2. 权威边界

- `AGENTS.md` 保存必须遵守的项目级硬规则；
- `docs/task_register.md` 与 supporting requirements 保存任务状态、授权和验收；
- `scripts/architecture_arch005_checkout_guard.py`、validation tier runner 和 Git ancestry
  保存可执行的 fail-closed 门禁；
- `tools/codex_skills/run-governed-development/` 保存 skill 的 Git canonical bundle；
- `$CODEX_HOME/skills/run-governed-development/` 保存本机可发现的 installed bundle；
- skill 不得复制或替代 task/source-of-truth、lease authority、DQ/PIT、validation 或 Git
  历史，只能读取权威规则并编排现有门禁。

如果 installed skill 缺失、损坏或不能读取，研发任务必须按 `AGENTS.md` 的等价手工流程
继续 fail closed 并报告；不得把 skill 不可用解释为绕过项目规则。

## 3. 触发范围与三种模式

任何会修改 tracked repository content 的非平凡研发任务，在首次写入前必须使用
`run-governed-development` skill 或执行其等价 preflight。纯问答、状态汇报和不改变文件的
只读诊断不要求创建分支，但在判断当前 checkout 安全性时仍使用 `READ_ONLY` preflight。

### 3.1 `READ_ONLY`

- 不创建任务分支、worktree 或 write lease；
- 运行 governed worktree audit 和 lease replay；
- 披露 local main、remote divergence、known-unrelated exclusion、现有 worktree 和 active
  lease；
- 不修改 task status 或生成业务 artifact。

### 3.2 `SINGLE_LANE`

- 非平凡任务必须已登记 task ID、priority、status、owner、blocker 和 acceptance；
- 从本地 `main` exact HEAD 创建任务分支；
- worker 只写 task-owned paths，shared/coordinator-only paths 由该单线任务的 coordinator
  scope 显式声明；
- lane-local focused validation 通过后提交任务分支；
- final tree 完成 required validation、generated freshness 与 audit 后，使用 `--ff-only`
  合入本地 `main`；
- local-main fast-forward后默认fetch并普通push remote main，push前后都做ancestor/SHA复核；
- owner no-push、无upstream、无关改动、divergence、non-fast-forward或需要history rewrite时
  fail closed并报告，不自动pull/rebase/merge/force-push。

### 3.3 `DUAL_LANE`

- engineering 与 strategy-evidence 从同一本地 `main` exact commit 创建独立
  branch/worktree；
- 两条 lane 的 owned path、module、public contract、runtime resource 和 evidence lineage
  必须互斥；
- global policy、shared schema、public contract 或消费语义发生变化时，先执行最小串行
  `CONTRACT_WAVE`，两条 lane 从其新 base 再启动；
- worker 不写 task register、system flow、catalog/registry、root/shared wiring、generated
  manifests/views 或 formal validation artifacts；
- coordinator 从共同 base 创建 integration branch，按
  `contract -> adapter -> domain -> tests/fragments -> shared wiring/docs -> generated views`
  顺序吸收两条 lane；
- 只对 integration candidate 运行 combined/formal gates；PASS 后一次性 fast-forward 本地
  `main`；
- local-main集成后由coordinator执行同一套默认remote-main ordinary-push门禁；
- 不允许两个 sibling lane 依次直接 fast-forward `main`，也不自动 rebase、merge commit、
  force-push 或删除用户改动。

## 4. 实施阶段

### S0：规则冻结与任务登记

- 更新 `AGENTS.md` 的 skill trigger、local-main closeout、dual-lane integration branch 和
  default ordinary-push boundary；
- 更新 ARCH-005 requirement 与 dual-lane operating model，保留历史 S4C push 证据但明确
  新默认；
- 修正 Wave15 已完成但 operating model 仍写 `VALIDATING` 的状态滞后。

### S1：Skill canonical bundle 与 installed bundle

- 使用 `skill-creator/scripts/init_skill.py` 标准初始化；
- skill name 固定为 `run-governed-development`；
- `SKILL.md` 只保留触发、三模式选择和必须顺序；
- detailed mode/checklist 放入一层 `references/`；
- deterministic preflight 放入 `scripts/`；
- 生成 `agents/openai.yaml`，不添加未授权 icon、brand 或 MCP dependency；
- canonical 与 installed bundle 的相对文件集合和 SHA-256 必须一致。

### S2：Preflight 与场景验证

Preflight 至少输出：

- schema/status/mode/task/role/stage；
- current branch、local main SHA、origin main SHA 与 ahead/behind；
- governed worktree audit status 和 known-unrelated exclusions；
- lease replay status 与 active lease IDs；
- existing worktree inventory；
- task-register presence；
- lane/coordinator path claims；
- exact/ancestor-descendant path conflicts；
- coordinator-only violations；
- `PASS`、`BLOCKED` 或 `SERIAL_CONTRACT_WAVE_REQUIRED` 结论；
- production/broker/remote action boundary。

必须验证：

1. `READ_ONLY`：当前 repository audit/replay PASS，不要求 task ID；
2. `SINGLE_LANE`：DEVX-002 coordinator-owned task paths通过，未登记 task ID fail closed；
3. `DUAL_LANE`：互斥 engineering/strategy paths通过；同 path、ancestor/descendant、
   coordinator-only 或 contract change 得到 typed blocked/serial conclusion。

### S3：项目验证与本地收口

- skill `quick_validate.py` PASS；
- canonical/installed byte parity PASS；
- skill script focused tests PASS；
- AGENTS/docs/task consistency、architecture/contract 等适用项目验证 PASS；
- governed worktree audit PASS；
- task branch提交后 fast-forward 本地 `main`；
- 删除已合并任务分支；默认ordinary push完成并复核remote SHA，或记录合法skip/block原因。

## 5. 验收标准

- 任意非平凡 mutation 在 `AGENTS.md` 中有强制 skill/equivalent preflight 入口；
- pure read-only 不被强迫创建分支或运行 Full；
- single-lane 与 dual-lane Git topology 明确且不会让第二 sibling branch天然阻塞；
- known-unrelated exclusions只经 governed audit处理；
- active lease、base drift、task absence、path conflict和coordinator-only violation fail closed；
- Full只在自然 integration boundary运行，失败修复保持 parent provenance；
- historical S4C ordinary-push证据保留；v2恢复validated local-main后的default ordinary push，
  但PR、force-push与remote divergence修复仍需单独授权；
- skill canonical/installed bundles可验证一致；
- 无策略、DQ/PIT、报告结论、production或broker行为变化。

## 6. 生命周期与安全边界

- 本任务分支：`codex/devx-002-governed-development-skill`；
- 本轮不创建额外 repository worktree；
- installed skill是本任务期望保留的本机能力，不属于临时工作区；
- canonical skill由Git恢复；installed bundle可由canonical bundle重新部署；
- 既有 `D:\Work\AITradingSystem_ops_runtime_20260725` 继续受 DEVX-001/OPS-070
  exit condition约束，本任务不得使用或删除；
- `production_effect=none`、`broker_action=none`。

## 7. 进度记录

- 2026-07-26：Owner确认项目内研发流程、local-main边界、parallel integration topology、
  workspace cleanup与skill强制入口均需明确推进；任务建立并进入`IN_PROGRESS`。
- 2026-07-26：`AGENTS.md`、ARCH-005 requirement和dual-lane operating model已统一到
  v1 local-main默认边界；该历史状态由v2后续owner决定显式取代。
- 2026-07-26：canonical与installed skill已按`skill-creator`标准初始化并实现；
  `quick_validate`、byte parity、ruff、strict mypy、py_compile与11项focused tests通过。
- 2026-07-26：已实际运行`READ_ONLY`、`SINGLE_LANE`和`DUAL_LANE`正例，以及task缺失、
  ancestor path冲突和contract-wave负例；结论分别为typed `PASS`、`BLOCKED`和
  `SERIAL_CONTRACT_WAVE_REQUIRED`。
- 2026-07-26：第一轮architecture-fitness为`640 passed / 14 failed`；失败集中于本任务
  修改受冻结哈希保护的流程文档后，最新compatibility/deprecation authority尚未append，
  不属于runtime或skill功能缺陷。修复采用新增DEVX-002 append-only authority，不改写
  历史段。Validation runner只允许failed Full使用`--parent-run`，architecture-fitness没有
  Full profile sidecar，因而不能伪装成machine parent；首轮失败runtime artifact保留为直接
  诊断证据，修复重跑使用带task/boundary的`ci_change_validation` provenance。
- 2026-07-26：任务进入`BASELINE_DONE`；剩余长期观察项是在后续首个真实非平凡研发任务
  中复核触发命中、lane ownership和local-main closeout体验，如暴露新边界再登记后续任务，
  不阻塞当前skill启用。
- 2026-07-26：Owner明确要求立即推送当前local `main`，并把后续默认改为：final-tree
  validation与local-main fast-forward通过后，自动执行普通`origin/main` push。DEVX-002以
  `default_ordinary_push_after_local_main_v2`重新进入`IN_PROGRESS`；PR、force-push、
  history rewrite、remote divergence修复和混入无关提交仍不自动授权。
- 2026-07-26：v2规则已同步到AGENTS、ARCH-005、dual-lane operating model与canonical skill；
  增加default-push一致性回归测试，并将任务转回`BASELINE_DONE`。本轮必须完成focused/formal
  validation、canonical/installed parity、local-main fast-forward、默认ordinary push与remote
  SHA复核后才能提交收口报告。
