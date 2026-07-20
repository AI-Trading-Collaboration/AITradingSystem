# ARCH-005 S4A 受监督自动化

最后更新：2026-07-20

状态：`IN_PROGRESS_OWNER_APPROVED`

## 1. 任务信息

- task id：`ARCH-005_PARALLEL_DEVELOPMENT_CONTROL_PLANE`
- boundary id：`ARCH-005-S4A-SUPERVISED-AUTOMATION`
- priority：`P0`
- owner：architecture coordinator / developer platform owner / integration coordinator
- authorization：project owner 于 2026-07-20 批准先推进较窄的受监督自动化版本
- dependency：ARCH-005 S2～S4 `BASELINE_DONE`
- source of truth：`LEGACY_MARKDOWN_ONLY`
- production effect：`none`

## 2. 目标与边界

本批把已经验证的 dependency/readiness/scheduler/lease 内核连接到真实、隔离、可审计的命令执行
环境，但不进入 S5 canonical cutover。目标闭环是：

```text
legacy task facts + reviewed manifest
  -> readiness / conflict / lease
  -> isolated git worktree + branch
  -> engineering worker + research-evidence worker
  -> execution/validation evidence binding
  -> integration queue
  -> human coordinator review
```

本批明确禁止：

- 自动 merge、rebase、commit、push、PR 或删除含变更的 worktree；
- 自动修改 task governance status 或把 Markdown 改为 generated source；
- 自动扩展 candidate、参数搜索、paper-shadow、promotion、production weights 或 broker/order；
- 使用 shell string、未审核命令、任意环境变量或未绑定 base commit 启动 worker；
- 把测试 fixture PASS 解释为投资证据 PASS。

## 3. 实施切片

### S4A.1 Policy 与执行契约

- 冻结 `supervised_automation_policy.v1`；
- 命令使用 argv list 和 exact allowlist，不通过 shell；
- 冻结 worktree root、branch prefix、timeout、环境变量 allowlist、stdout/stderr 大小上限；
- worker manifest 绑定 task/change/base/command/validation/artifact/resource claims。

退出：unknown command、path escape、branch escape、stale base、unsafe effect 和未审核 actor 全部
fail closed。

### S4A.2 Worktree 与 Worker Executor

- 从 exact base 创建两条隔离 branch/worktree；
- 并发启动 engineering 与 research-evidence worker；
- 记录 PID、开始/结束时间、exit code、timeout、stdout/stderr SHA、初始/最终 HEAD、dirty paths；
- 单 lane 失败不终止无依赖 lane；失败 lane 保留 worktree 和 evidence 等待人工判断。

退出：真实临时 Git 仓库测试覆盖 create/run/timeout/failure/path safety/idempotency；没有跨 lane
写入。

### S4A.3 Integration Queue 与 Human Gate

- 两条 lane 通过后生成 deterministic integration candidate；
- 校验 evidence bytes、base/head/changed paths、lease replay 和 worktree identity；
- 状态固定 `AWAITING_HUMAN_COORDINATOR_APPROVAL`；
- human approval 前 `merge_allowed=false`，不运行 merge/push。

退出：篡改 evidence、缺 worker artifact、base drift、active lease、dirty untracked secret-like path 或
worktree/branch 不匹配全部 fail closed。

### S4A.4 Recovery、Cleanup 与真实 Pilot

- orphan scan 识别 active/expired lease、missing worktree、missing process result 和 unreviewed dirty worktree；
- cleanup 只允许无变更 worktree，或带显式 coordinator approval 的已审计路径；
- 真实 pilot 使用一条 engineering validation command 与一条 research contract validation command；
- pilot 只验证自动执行基础设施，不启动新策略研究或 agent 自动写代码。

退出：pilot report/validator PASS、两 lane 证据可复算、integration queue 等待人工、所有运行期 lease
关闭；`production_effect=none`。

## 4. 验收标准

- 自动完成 readiness → worktree → worker → evidence → integration queue；
- 两条 domain lane 真正在不同 Git worktree 并发执行；
- command/actor/base/path/environment 全部由 reviewed policy 限制；
- stdout/stderr、exit status、duration、Git state 和 artifact bytes 可复算；
- failure/timeout 只影响本 lane，保留证据且不自动重试有副作用命令；
- human coordinator 明确是最终 integration gate；
- cleanup 不删除未审计变更；
- focused、architecture-fitness、contract-validation、reproducibility 和风险相称的 full PASS；
- S5 仍未解锁，ARCH-004 G2.5 不因本批自动恢复。

## 5. 开放问题与后续

- Codex、其他 agent runtime 或 CI runner 通过 adapter 接入，不成为 canonical source；本批只冻结通用
  subprocess contract 和真实 validation-worker pilot。
- 自动 commit/merge/push、remote queue、多主仲裁和跨机器 lease 属于后续单独授权范围。
- S5 是否在两轮真实 supervised telemetry 后启动，由 owner 根据 conflict/failure/rework 数据决定。

## 6. 状态记录

- 2026-07-20：owner 选择先实现较窄的受监督自动化版本。任务进入
  `IN_PROGRESS_OWNER_APPROVED`；先保留人工 coordinator 与 legacy Markdown 事实源，不自动进入 S5。
- 2026-07-20：S4A.1～S4A.3 implementation complete，新增 reviewed policy、两 task pilot、
  exact-base worktree/branch lifecycle、argv-only worker、timeout/process-tree termination、manifest/resource/
  Git/log evidence binding、human-gated integration candidate、validator、orphan audit 与 clean-only cleanup。
  focused regression=`26 passed / 15.45s`。当前进入 S4A.4 formal validation 与真实 pilot；在 pilot evidence、
  generated manifests、architecture/contract/reproducibility/full 全部闭合前维持 `IN_PROGRESS`。
