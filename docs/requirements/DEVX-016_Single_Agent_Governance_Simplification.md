# DEVX-016：单 agent 默认治理与 ratchet 单调化

最后更新：2026-09-23

稳定任务 ID：`DEVX-016_SINGLE_AGENT_GOVERNANCE_SIMPLIFICATION`

状态：`PROPOSED`

总控：`docs/requirements/GOV-007_Pre_Migration_Convergence_Program.md`

## 1. 问题

- 测试把任务数、fragment 数、SHA 等精确值写死。任何无关的登记或生成物刷新都会让这些断言失败，
  导致 26–54 分钟的 Full 一次次重跑。本地 publication 事务 639 个里有 554 个 FAILED（P0-A 盘点）。
- DUAL_LANE 和多阶段 publication fence 是为多 agent 并行设计的；迁移前的默认模式改为单 agent 后，
  这部分开销大多不再必要。

## 2. Owner 决定

`owner_decision:DEVX-016:2026-09-23:single_agent_default_v1`：迁移前的默认治理模式是
"单 agent + 受保护 main + CI"。

## 3. 目标

1. 默认模式改为 SINGLE_LANE；DUAL_LANE 改为显式 opt-in，并要求写明路径互不相交的理由。
2. 所有精确计数、SHA 参数类 ratchet 改为单调检查（只禁止变差，不锁死当前值）。
3. 生成的 authority 只在最终候选上重建一次，由 CI 校验一致性。
4. publication fence 保留 lease 和 main fast-forward 的核心约束，精简中间阶段。
5. 同一候选只跑一次 Full。

## 4. 不变量

安全边界、DQ/PIT 门禁、研究窗口、阈值治理、production/broker 边界、canonical 任务事件的 append-only
语义不变。

## 5. 依赖

DEVX-015 为 DONE（避免在 DEVX-015 最终候选阶段改动 ratchet 与 fence 合同，触发串行合同波次）。

## 6. 验收标准

- 挑三次历史失败样本（DEVX-010、TRADING-2555、OPS-081）重放，新规则下不再误报；
- 一次无关的任务登记不再引起任何测试失败；
- 安全边界与 DQ/PIT 门禁的测试覆盖不减少；
- 相关文档（AGENTS.md、run-governed-development skill、runbook）同步更新。

## 7. 进度

- 2026-09-23：登记（GOV-007 P0-B）。DEVX-015 执行期间，每次 Full 失败若属于计数/SHA 类 ratchet 不同步，
  在本节追加一条样本，作为简化的证据。
