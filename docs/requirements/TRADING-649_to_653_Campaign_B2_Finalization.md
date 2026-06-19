# TRADING-649 to 653 Campaign B2 Finalization

版本：2026-06-19

## 背景

TRADING-641~648 已把 B2 Campaign compute adapter 扩展到 targeted evidence、
full diagnostic 和 gate，并输出 rc3 validation pack。当前仍需要把 B2 当前状态、
最后一轮 repeatability、final gate、owner packet 和 branch snapshot 固化为 Campaign
管理的最终研究阶段 artifacts，避免继续依赖旧 B2 task-specific runner。

## 范围

- TRADING-649：冻结 B2 当前 Campaign stage/outcome、reason codes、evidence budget
  和 allowed/blocked/owner actions。
- TRADING-650：通过 `campaign run --stage next` 执行 B2 最后一轮 targeted evidence，
  必须使用 B2 compute adapter 并消耗 evidence budget。
- TRADING-651：用 Campaign gate policy 生成 B2 current-form final gate。
- TRADING-652：生成 owner 可读 B2 final research packet，不自动 append owner decision。
- TRADING-653：基于 final gate 和 owner packet 生成 B2 branch finalization snapshot。

## 非目标和安全边界

- 不调用旧 B2 task-specific runner 作为 orchestration 入口。
- 不调 B2 参数，不执行 tuning。
- 不访问 untouched holdout。
- 不生成 paper-shadow、official target weights、broker/order artifact 或 production mutation。
- 不启用 B4 retest、B5、B6、v3。
- 不自动 append owner decision。

## 实施顺序

1. 新增 B2 next-action freeze report，直接读取 Campaign plan/status。
2. 新增 Campaign-managed final repeatability report，在临时 Campaign root 中通过
   `requested_stage="next"` 执行最后 targeted evidence，并验证 compute adapter、预算消耗
   和 safety metadata。
3. 新增 final gate report，把 Campaign gate/adapter decision 映射为 B2 current-form
   taxonomy；预算耗尽时不得输出泛化 `NEEDS_MORE_EVIDENCE`。
4. 新增 owner review packet report，汇总 B2 hypothesis、fast-risk、slow-drawdown、
   control windows、re-entry lag、utility、final gate 和 owner options。
5. 新增 branch finalization snapshot，固定 B4/B5/B6/v3/paper-shadow 全部 blocked。
6. 将 artifacts 接入 validation-pack、文档和 focused tests。

## 验收标准

- Next-action freeze 输出 current stage/outcome、reason codes、evidence budget、
  allowed/blocked/owner actions，并包含 final repeatability、narrow role、return to design。
- Final repeatability run 输出 `B2_FINAL_REPEATABILITY_RUN_COMPLETE`、
  `B2_FINAL_REPEATABILITY_RUN_PARTIAL` 或 `B2_FINAL_REPEATABILITY_RUN_BLOCKED`。
- Final repeatability run 证明使用 `campaign run --stage next`、B2 compute adapter、
  evidence budget 消耗、holdout/paper-shadow/official weights/production mutation 均为 false。
- Final gate 输出允许 taxonomy 之一，预算耗尽时不输出 generic `NEEDS_MORE_EVIDENCE`。
- Owner packet 输出 owner options，不 append owner decision。
- Branch finalization 输出允许 branch taxonomy 之一，并明确 B4/B5/B6/v3/paper-shadow
  全部 false。
- focused tests、Ruff、compileall、B2/B3 campaign validate、validation-pack 和
  `git diff --check` 通过。

## 当前状态

2026-06-19：实现完成并进入 `VALIDATING`。Next-action freeze 输出
`CAMPAIGN_B2_NEXT_ACTION_FREEZE_READY`；Campaign-managed final repeatability 通过
`campaign run --stage next` 路径和 B2 compute adapter 输出
`B2_FINAL_REPEATABILITY_RUN_COMPLETE`，并把 targeted budget 从 1/2 消耗到 2/2；
final gate 输出 `OWNER_OVERRIDE_REQUIRED`，不输出 generic `NEEDS_MORE_EVIDENCE`；
owner packet 输出 `B2_OWNER_REVIEW_PACKET_READY` 且不 append owner decision；branch
finalization 输出 `OWNER_REVIEW_REQUIRED` 并固定 B4/B5/B6/v3/paper-shadow false。rc4
validation pack 输出 `RESEARCH_CAMPAIGN_CONTROL_PLANE_V1_READY_WITH_LIMITATIONS`、
artifacts=23。本批次未扩大研究范围，未触碰 untouched holdout，未放开
B4/B5/B6/v3、paper-shadow/live、official weights、broker/order 或 production mutation。
