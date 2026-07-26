# DEVX-006：主干漂移感知的一次性集成与复验

最后更新：2026-07-27

- 稳定任务 ID：`DEVX-006_BASE_DRIFT_AWARE_INTEGRATION_AND_REVALIDATION`
- 优先级：`P0`
- 状态：`VALIDATING_FINAL_TREE`
- owner：developer workflow / architecture control-plane owner
- owner decision：
  `owner_decision:DEVX-006:2026-07-27:approve_base_drift_aware_single_integration_v1`
- exact base：`bc8496b11039f3d6a8d2bc837e821c298e04c9cf`
- implementation branch：`codex/devx-006-base-drift-integration`
- production effect：`none`
- broker action：`none`

## 1. 问题与目标

ARCH-005 已经提供 frozen base、change manifest、path/module/contract conflict、coordinator
integration candidate、formal validation 和 S4E source reconciliation。但当前 governed
workflow 把任何 base drift 都当作 lane 启动或集成前的硬阻断，没有区分：

1. 主干只修改了与任务无关的路径；
2. 主干修改了可由 coordinator 在最终树上重建的 generated/shared view；
3. 主干与任务修改了同一 domain path；
4. 主干改变了任务依赖的 public contract、DQ/PIT、cache identity 或其他消费语义。

TRADING-2462 因 main 多次前进形成 v1、v2、v3、v4 worktree/branch 链。重复重建不仅消耗
formal validation，还扩大临时工作区、证据迁移和误清理风险。目标不是放宽 base drift 门禁，
而是把它升级为可审计的分类与单次集成协议：

- domain lane 在 frozen base 上只完成 lane-focused validation；
- 集成边界读取 frozen base、lane head 和 latest main 的真实 Git delta；
- compatible drift 只创建一个 latest-main coordinator integration candidate；
- task delta 只迁移一次，generated/shared view 只刷新一次，formal tiers 只对最终树执行一次；
- path、contract、lineage 或语义冲突继续 fail closed，不自动 rebase、merge、cherry-pick、
  commit、push、force-push 或改写历史。

## 2. 权威边界

- Git commit ancestry 和 `base..head` diff 是路径漂移事实；
- `change_manifest.v1` 是任务 frozen scope、module、contract 和 validation claim；
- reviewed policy 只定义 coordinator-refreshable paths、contract-sensitive paths 和
  coordinator-only paths，不推断投资语义；
- `integration_revalidation_plan.v1` 是 read-only 决策证据，不是 Git mutation authority；
- S4E handoff/reconciliation 继续负责 source residue 和 exact-byte lineage；DEVX-006 不复制
  或替代它；
- `run-governed-development` 继续负责分支拓扑、final-tree validation、local-main
  fast-forward 和普通 remote push。

known-unrelated exact path
`docs/research/growth_tilt_owner_diagnosis_pack.md` 必须保持 zero-read、zero-hash、
zero-copy、zero-modification；它不得进入 drift path inventory 或 candidate scope。

## 3. v1 合同

### 3.1 输入

- repository identity 与 Git common dir；
- frozen base commit；
- lane head commit；
- latest main commit；
- `change_manifest.v1`；
- reviewed drift policy；
- 可选 mainline contract-change claims。

所有 commit 必须是完整 40 字符 SHA；base 必须同时是 lane head 和 latest main 的 ancestor。
lane head 不得等于 latest main；repository 必须 clean，且三个 commit 在同一 repository 可解析。

### 3.2 路径集合

- `task_delta_paths`：`base..lane_head` 的 tracked path delta；
- `mainline_delta_paths`：`base..latest_main` 的 tracked path delta；
- `declared_task_paths`：manifest owned/shared paths；
- `undeclared_task_paths`：task delta 未被 manifest 的 exact 或 ancestor scope 覆盖；
- `overlap_paths`：task/mainline delta 的 exact 或 ancestor/descendant overlap；
- `coordinator_refresh_paths`：overlap 中被 reviewed policy 精确授权、且只在最终 candidate
  重建的 generated/shared paths；
- `contract_sensitive_overlap_paths`：命中 contract-sensitive scope 的 overlap；
- `domain_overlap_paths`：其余 overlap。

Git rename/copy 必须展开为 old/new 两端；known-unrelated exclusion 必须在调用 Git 前从完整
pathspec 排除，不能先读取后过滤。

### 3.3 决策

- `READY_FOR_SINGLE_INTEGRATION_CANDIDATE`
  - ancestry/identity/manifest全部有效；
  - 无 undeclared task path；
  - 无 domain 或 contract-sensitive overlap；
  - 允许存在无关 mainline drift；
  - 允许存在 reviewed coordinator-refresh path。
- `RECONCILIATION_REQUIRED`
  - domain path overlap，但没有证据证明 public contract/consumer semantics 改变；
  - 保留 lane，不要求重建新版本；
  - coordinator 必须在一个 latest-main candidate 中人工 reconcile，并重新运行受影响 focused
    与全部 final-tree gates；
  - integration preflight 只有在显式提供与validated plan完全一致的
    `--reviewed-reconciliation-plan-id` 时才允许继续，并把该id写入审计结果。
- `SERIAL_CONTRACT_WAVE_REQUIRED`
  - contract-sensitive path overlap；
  - contract version/access claim 冲突；
  - DQ/PIT、cache identity、research window、threshold 或 consumer-visible semantics 改变；
  - 先形成最小 reviewed contract wave，再从其 exact base 重新计算受影响 lane。
- `BLOCKED`
  - wrong repository、ancestry failure、dirty repository、manifest mismatch、undeclared task
    path、unknown exclusion、Git identity drift 或证据不完整。

任何非 READY 决策的 `candidate_creation_allowed=false`。READY 也只表示 coordinator 可以进入
既有 governed integration 流程，`automatic_git_mutation_allowed=false`。

### 3.4 输出

`integration_revalidation_plan.v1` 至少包含：

- repository top-level/common-dir identity；
- frozen base、lane head、latest main；
- manifest body checksum 与 policy checksum；
- task/mainline delta，及每项 classification/reason；
- ancestry、scope、path overlap、contract 和 exclusion checks；
- decision、required next stage、required validation tiers；
- lane-focused evidence reuse boundary；
- final-tree validation boundary；
- `task_branch_rebuild_required`；
- `candidate_creation_allowed`；
- `reviewed_reconciliation_required`；
- `automatic_git_mutation_allowed=false`；
- `production_effect=none`、`broker_action=none`；
- canonical checksum。

validator 必须从同一 repository 和同一 input bytes 重建输出；任意 path、commit、classification、
decision 或 checksum tamper 都必须 fail closed。

## 4. 实施步骤

### S0：任务登记与 policy 冻结

- 登记本 requirement 和 task row；
- 冻结 schema、typed decisions、path relation、exclusion 和安全边界；
- 不修改 TRADING-2462 worktree，不推进 main。

退出：SINGLE_LANE preflight PASS。

### S1：纯函数 planner 与严格 validator

- 新增 drift policy loader；
- 新增 Git identity/ancestry/diff collector；
- 新增 deterministic classifier、canonical checksum 和 validator；
- 新增 read-only CLI `plan` / `validate`；
- 不创建或删除 branch/worktree，不运行 Git mutation。

退出：unrelated drift、refresh-only overlap、domain overlap、contract overlap、undeclared path、
rename、wrong ancestry、dirty target、known-unrelated zero-read 与 tamper tests PASS。

### S2：governed workflow 接线

- 更新 canonical/installed `run-governed-development`：
  lane 的 frozen base 发生 drift 时先运行 DEVX-006 planner，不自动创建 v2/v3/v4 重建链；
- READY 只解锁一个 coordinator integration candidate；
- RECONCILIATION 与 SERIAL_CONTRACT_WAVE_REQUIRED 保持 typed stop；
- formal validation 仍只在 final candidate 的自然边界执行。

退出：skill parity、preflight scenario 和 project focused tests PASS。

### S3：真实 pilot 与 formal closeout

- 在不写 TRADING-2462 v4 的前提下，对其 committed predecessor 运行只读 characterization；
- v4 收口后再以 final committed lane head 生成正式 pilot plan；
- 对最终 implementation tree 运行 architecture、contract、reproducibility/integration 和风险
  相称的 Full；
- 只有 v4 已安全进入 main 或 owner 明确接受 main drift 时，DEVX-006 才允许 fast-forward
  local main 和执行默认 ordinary push。

退出：真实 pilot 证明无须创建新的策略 rebuild worktree；final-tree gates PASS；main/remote
集成不使活动策略线再次漂移。

## 5. 验收标准

1. unrelated main drift 不再强迫 task branch 重建；
2. reviewed generated/shared overlap 明确要求 coordinator final-tree refresh，不复用 lane bytes；
3. domain overlap、contract-sensitive overlap 和 undeclared task path 有不同 typed decision；
4. exact/ancestor/descendant path关系、rename old/new、ancestry和repository identity可重复；
5. known-unrelated exclusion在Git读取前生效；
6. planner/validator结果 deterministic、content-addressed、tamper fail closed；
7. 不自动 rebase、merge、cherry-pick、commit、push、cleanup 或 task mutation；
8. lane focused evidence与final formal evidence边界可审计，formal只对最终 candidate运行一次；
9. focused、skill parity、architecture、contract、integration/reproducibility与required Full PASS；
10. `production_effect=none`、`broker_action=none`。

## 6. 生命周期与集成边界

- 本任务只创建 branch `codex/devx-006-base-drift-integration`，不创建新 worktree；
- branch 从 exact main `bc8496b11039f3d6a8d2bc837e821c298e04c9cf` 创建；
- TRADING-2462 v1～v4 与 OPS-070 runtime workspace 均不属于本任务，不读取其 excluded user
  content、不修改、不删除；
- 若任务跨 Codex turn 保留，恢复来源为本 requirement、task row 和 Git branch；
- branch 退出条件：implementation/formal validation完成，并且不会在活动 TRADING-2462 v5
  收口前推进 main；若必须等待，保留已提交工程 branch，并在 task row 记录 integration blocker；
- branch 只有在 ancestry、unique content、validation evidence、process dependency和可恢复性
  审计完成后才能删除。
- final integration workspace：`D:\Work\AITradingSystem_integration_devx006_arch005m1`；
  owner=`DEVX-006_BASE_DRIFT_AWARE_INTEGRATION_AND_REVALIDATION`，purpose=在不读取、stash、
  覆盖或提交primary checkout中known-unrelated用户文档的前提下，把已提交candidate迁移到
  `6dc8a643a` latest main并重建唯一compatibility authority；exit condition=final candidate
  通过formal/Full、ff-only进入local main并普通push后，确认runtime evidence已在canonical
  location、无unique tracked/untracked/ignored bytes及active process依赖，再执行
  `git worktree remove`与`git worktree prune`。清理前candidate commit和main提供恢复边界。

## 7. 进度记录

- 2026-07-27：Owner同意继续按“工程与策略互不阻塞、最终可审计汇合”的长期目标推进。
  DEVX-005 已使 target-bound audit 可用；TRADING-2462 v1～v4 的重复重建表明 base drift 分类
  和 single-candidate revalidation 是当前最高价值缺口。任务登记并进入 `IN_PROGRESS`；
  本轮先在隔离工程分支实施，不推进 main。
- 2026-07-27：S1/S2实现完成并进入`VALIDATING`。planner/validator、read-only CLI、strict
  policy loader、rename old/new、path/contract分类、canonical atomic writer、preflight frozen-lane
  continuation、exact reviewed reconciliation plan id和canonical/installed skill接线完成；
  focused planner+skill=`50 passed`，skill parity=`PASS/5 files`，architecture devex在修复一次
  direct-writer gate后PASS。兼容性首轮=`77 passed / 29 failed`，失败全部来自尚未append
  DEVX-006 hash authority及deprecation inventory freshness，未发现planner/runtime失败。
- 2026-07-27：只读target-bound audit确认TRADING-2462 v5仍有15项归属dirty paths，branch
  `codex/trading-2462-tail-risk-robustness-audit-v5@1b21c2721`从exact main
  `bc8496b1`前进1个登记commit。为避免本任务本身再次制造strategy base drift，DEVX-006只在
  工程branch提交并保留；待v5先收口到main后，以DEVX-006自身frozen branch执行第一个真实
  latest-main pilot、刷新shared/generated一次并运行final-tree formal gates。
- 2026-07-27：隔离分支收敛验证完成：planner+skill=`50 passed`、compatibility/deprecation/
  reporting architecture=`108 passed`、DevEx/task registry/docs governance=`69 passed`，
  Ruff、architecture devex、task shadow byte identity、checkout audit及canonical/installed
  skill parity均PASS。任务保持`VALIDATING`；architecture/contract/required Full与真实
  latest-main self-pilot明确留到v5进入main后的final candidate自然边界，不以当前frozen tree
  的重复重测冒充集成证据。
- 2026-07-27：TRADING-2462 v5已在exact base完成`7420 passed / 4 skipped`并进入
  `main=origin/main=4e6eb8aa6`。DEVX-006首次真实self-pilot以
  frozen=`bc8496b1`、lane=`98cac4eee`、latest-main=`4e6eb8aa6`生成并重建验证
  `integration-revalidation-927fbb12af85ec52da7a`；机械决定为
  `RECONCILIATION_REQUIRED`，仅包含3个domain overlap及8个reviewed coordinator-refresh
  overlap，0 blocker、0 contract conflict、0 undeclared path。带exact reviewed plan id的
  INTEGRATION preflight PASS，单一候选branch为
  `codex/integration-20260727-devx006-arch005m1`。该候选同时接入已focused验证的
  ARCH-005M1 Batch 1，shared/generated只在最新final tree刷新一次；状态进入
  `VALIDATING_FINAL_TREE`。
