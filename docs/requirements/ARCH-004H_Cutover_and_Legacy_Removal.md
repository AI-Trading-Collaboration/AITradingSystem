# ARCH-004H Cutover 与 Legacy Removal

最后更新：2026-07-12

## 任务信息

- task id：`ARCH-004H_CUTOVER_AND_LEGACY_REMOVAL`
- parent：`ARCH-004`
- priority：`P0`
- status：`PROPOSED`
- owner：architecture coordinator / 各 surface owner / project owner
- dependency：ARCH-004G7 handoff 完成；H entry gate 全部通过
- production effect：`none`

## 目标

ARCH-004H 负责把 G 阶段已经建立并通过 parity 的 canonical paths 正式切换为唯一 active path，并按受治理 lifecycle 将 legacy surface 从 `DEPRECATED` 依次推进到 `FROZEN` 和 `REMOVED`。H 不再创建新的 domain architecture，也不通过删除历史 evidence 来清理 warning。

## H Entry Gate

开始任何 cutover 前必须同时满足：

- G0～G6 完成，G7 已生成 lane-level deprecation/reachability/removal ledger；
- 每个 G wave 为 `FROZEN|REMOVED|BLOCKED_WITH_OWNER`，且不存在永久 compatibility TODO；
- 2 个 daily、2 个 weekly、1 个 monthly parity evidence 完整；
- 关键 CLI、schema、artifact、Reader Brief semantic parity PASS；
- DQ、PIT、cost、production-effect、owner-decision signoff 完整；
- architecture-fitness、contract-validation、report-validation、integration、reproducibility 和 full parallel PASS；
- 每个准备删除的 surface 独立满足 reviewed deprecation policy 的 12 项 removal gates；
- 任一投资解释差异未获 owner signoff 时，不得切换相关 surface。

`BLOCKED_WITH_OWNER` 可以作为 G 阶段的诚实终态，但不能视为 removal-ready，也不能在 H 中删除。

## 分阶段实施

### H0：Cutover contract 与 matrix

- 冻结逐 surface cutover matrix、canonical replacement、compatibility window、observation window、rollback condition、owner 和证据引用；
- 把 ARCH-005 canonical task registry、active/completed generated views、legacy Markdown readers/writers 作为独立 cutover surface，要求 S0～S4 shadow/consumer/parity/no-dual-write 证据；
- 明确 code removal 与 historical artifact retention 的独立生命周期；
- 建立 versioned H entry decision artifact，不复用 2026-06-19 historical closeout snapshot 作为当前证据。

### H1：只读 reporting / artifact consumers

- 先切换 no-recompute、no-production 的 read-only consumers；
- task-register consistency、Research Roadmap、Reader Brief/Safety Boundary 等 task consumers 必须先经 canonical reader/compatibility layer 完成 parity，再切换 generated active/completed views；
- Reader Brief core、legacy annex 和 generated aggregate source-of-truth 必须分成独立 gate；
- 缺 typed provenance 的内容继续显示 `LEGACY|LIMITED|BLOCKED`，不得在 cutover 时补造语义。

### H2：CLI thin facade

- canonical command owners 成为唯一实现；
- legacy CLI 仅在明确 compatibility window 内保持 thin facade；
- command path/options/default/help/exit、duplicate count 和真实 fixture parity 保持。

### H3：Operations facade

- canonical WorkflowSpec/RunLedger 和 due resolver 成为 runtime consumer；
- 唯一外部入口仍为 `aits ops daily-run`；
- non-daily automatic dispatch 不因 cutover 自动启用；
- lock/retry/resume/DQ failure propagation 和 cadence artifacts 保持一致。

### H4：Research wrappers

- 按 selected wave 将 task-shaped wrappers 切换到 ExperimentSpec/plugin/lifecycle；
- primary、section、Markdown、envelope、ledger 和 lifecycle parity 先通过；
- 删除 wrapper code 时保留 historical artifact、checksum、schema、commit 和 runner refs。

### H5：Portfolio / decision-sensitive legacy

- 最后切换 system target、parameter research、controlled batch、scoring、position gate 和 backtest；
- characterization、golden、DQ、PIT、cost、risk、production effect 和 owner signoff 缺一不可；
- 结构切换不得同时修改 strategy、threshold 或 weight policy。

### H6：Removal 与 successor release acceptance

- 每批重新运行 reachability scanner 和 12 项 removal gates；
- 只允许 `DEPRECATED -> FROZEN -> REMOVED` 顺序迁移；
- 提交后在 clean worktree 运行 clean-clone acceptance；dirty snapshot 不能作为 PASS；
- 生成 versioned ARCH-004 successor release candidate 和 final closeout evidence。

## 验收标准

- 所有目标 surface 有唯一 lifecycle 终态、replacement、owner、usage evidence、window、sunset 和 gate evidence；
- removal-ready surface runtime/import/CLI/report reachability 为零，或 compatibility facade 已按合同结束；
- 0 permanent dual track，0 indefinite compatibility TODO；
- historical reproducibility 和 safety contracts 保持；
- 2 daily、2 weekly、1 monthly parity 与所有正式 validation tiers PASS；
- clean-clone、canonical status/doctor、successor release candidate 和 owner signoff 完整；
- `production_effect=none`、`broker_action=none`，不自动启用 paper-shadow、production 或 broker/order。

## 非目标

- 不删除历史失败研究或 evidence；
- 不在 H 中进行策略调优、阈值校准或新候选研究；
- 不用 waiver、路径猜测或串行 PASS 掩盖未通过的并行/full gate；
- 不一次性翻转所有 cutover flags。

## 状态记录

- 2026-07-12：根据 owner 确认登记为 `PROPOSED`。当前 ARCH-004 仍处于 G，`next_phase_unblocked=false`；只有 G7 handoff 和全部 H entry gates 闭合后才可转为 `READY`。
