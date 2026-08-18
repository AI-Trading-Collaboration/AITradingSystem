# TRADING-2533 — QQQ Options session-finalization V2 export-safe DQ/PIT evidence admission V1

- priority: `P0`
- status: `DONE`（publication 仍以 final-tree formal PASS 为前置条件）
- owner: Codex capability coordinator（离线 admission 与实现）；Project Owner（后续任何外部动作）
- production effect: `none`
- broker action: `none`
- governed mode: `SINGLE_LANE`
- contract change: `false`（只消费 2482 frozen semantics，不修改 shared enum、threshold 或 canonical evaluator）

## 1. 为什么需要这个后继任务

TRADING-2532 已通过唯一一次 zero-order Cloud run 证明：1202 个 session 中，1201 个最终出现
option chain，1 个最终从未出现；其余逐轴 export-safe aggregate 在 1201 个 chain-present session
上为 present，在 never-chain session 上为 not-evaluated。该结果还证明 2530 的 1020 个早期 missing
中有 1019 个属于 collector 提前终结混淆。

这些 transport facts 不能直接变成 2482 canonical DQ/PIT PASS。2482 的 15-check evaluator 还要求
quote freshness、OI/Greeks exact-prior-session、calendar/mapping identity、cache/engine/evidence identity、
provider raw checksum availability，以及 signal/order/fill chronology。2532 的 export-safe aggregate 没有
这些逐观察字段，而且已经明确 `dq_pit_admission_authorized=false`。

本任务的目标不是把信息不足包装成 PASS，而是完成一次可审计的离线 evidence admission：

1. 独立复核 2532 raw Results 与 tracked execution package 的 exact identity；
2. 对 2482 的每个 required check 说明现有 evidence 是充分、明确失败，还是未评估；
3. 生成 deterministic、sealed、export-safe 的 admission report 与 manifest；
4. 明确当前 canonical 结论及下一证据缺口；
5. 在 Atlas 首层用普通语言解释为何研究仍不能重新开放。

## 2. 冻结输入与身份

- registration/frozen base: `bf7fbfd489b9c3eb8dbe22248cc45385f7c56347`；
- TRADING-2532 task:
  `TRADING-2532_QC_QQQ_OPTIONS_DAILY_TRANSPORT_SESSION_FINALIZATION_V2_ZERO_ORDER_EXTERNAL_VALIDATION_ADMISSION_AND_EXECUTION_V1`；
- backtest id: `acf111f24d09a41870f9a23e93fcbe3b`；
- requested range: `2021-02-22..2025-12-02`；
- expected/observed sessions: `1202 / 1202`；
- raw Results byte count: `814999`；
- raw Results SHA-256:
  `5d3220342c96217f2c4a4d624b0dc7fbbcad98427de728e749dc2e4f3168d50d`；
- 2532 export-safe evidence content seal:
  `ffa9faafd1d480282bcfe1c07c896f538f26d2b23d7d7d8356460bc881e0bc49`；
- 2532 execution manifest content seal:
  `258ee1339b7f6b70a4d55d9e128f8c393052da97d627e52122b5e75788f544c1`；
- 2482 policy file SHA-256:
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- 2481 shared contract SHA-256:
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`。

Tracked 2532 execution package file identities:

| file | byte count | SHA-256 |
|---|---:|---|
| `authorization_admission.json` | 1796 | `22c6cf83e82333eb8042257001fd5c6dc1d4695937b5177625817a255b49d285` |
| `run_attempt_consumption_receipt.json` | 1093 | `9248ba19069abb82d4ce1a47374c53cb4a7a01a9abf7fd6c44d8f868d45d3bf9` |
| `export_safe_aggregate_evidence.json` | 3523 | `c25029918785ccd9321cb9087620f989b3f6b951e2c242c279bf42cc99b25671` |
| `external_action_ledger.json` | 1342 | `96042f266243f6a21c59ed341fdc04c7981a02af8e9ce0dffe4adc3b5005642b` |
| `execution_evidence_manifest.json` | 1088 | `02f0cab42a4594ae5a0693692ff3308e8e22020f97a60f5838b9557de3e29978` |

Any input drift, seal mismatch, count mismatch, unregistered check id or noncanonical serialization fails closed.

## 3. 2482 check coverage 决策规则

本任务不得创建一个看似 canonical、实际由窗口 aggregate 伪造的 `DQReportRecord`。它只生成
`dq_pit_evidence_admission.v1`，并逐项复用 2482 exact check ids：

- `chain_presence`: `FAIL`，原因是存在 1 个 final never-chain session；
- `local_cache_dq_scope_separation`: `PASS`，因为 2532 明确禁止以 transport evidence 替代 local cache
  或 option-event DQ admission；
- `quote_integrity`: `NOT_EVALUATED`；1201/1202 的 present aggregate 不是逐 candidate bid/ask 数值证据，
  且 1 个 session 未评估；
- 其余 12 项：在 exact required evidence 未出现时保持 `NOT_EVALUATED`，不得由字段存在计数、
  `orders=0` 或 source Result 文件哈希推导 PASS。

整体决定必须为：

- `dq_status=FAIL`（至少 `chain_presence` 明确失败）；
- `pit_status=NOT_EVALUATED`；
- `admission_status=BLOCKED_INSUFFICIENT_CANONICAL_DQ_PIT_EVIDENCE`；
- `selection_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `investment_conclusion_authorized=false`。

这个结论不否定策略，也不表示 1201 个 session 的 transport facts 无效；它只说明当前 evidence 不能
支持策略研究准入。

## 4. 最小后续证据缺口

报告必须把缺口按用途分组，不允许只写“需要更多数据”：

1. **可用性**：1 个 never-chain session 的 provider/transport attribution；
2. **时点新鲜度**：quote freshness assessment、prior-day model/Greeks as-of、OI as-of；
3. **市场身份**：reviewed exchange calendar 与 symbol mapping id/version/hash；
4. **缓存与运行身份**：cache key/material、repository/engine/bundle/platform evidence identity；
5. **源校验**：provider raw checksum availability；manual Results hash 不能冒充 provider raw checksum；
6. **策略时序**：signal/selection/order/fill chronology 当前因 zero-order run 正确未发生，不能补写；
7. **策略阈值**：quote age、spread、min OI/volume 仍是 `UNKNOWN_REQUIRES_POLICY_REVIEW`，本任务不校准。

后续若要采集新的外部证据，必须另建 proposal/admission，并由 Project Owner 对 exact scope、hash、expiry、
单次动作上限重新授权。本任务不继承或复用已消耗的 2532 token。

## 5. 实施范围

Task-owned:

- 本 supporting requirement；
- `config/research/qc_qqq_options_session_finalization_dq_pit_evidence_admission_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/session_finalization_dq_pit_evidence_admission.py`；
- `tests/test_qqq_options_session_finalization_dq_pit_evidence_admission.py`；
- `inputs/research/qqq_options/trading_2533_session_finalization_dq_pit_evidence_admission_v1/**`；
- Atlas successor classification 与相邻 renderer tests（只投影 canonical report）；
- 必需 architecture fragments。

Coordinator-owned:

- canonical task registry/index 与 generated compatibility views；
- `docs/system_flow.md`；
- DevEx/report-flow/compatibility generated authority；
- formal validation artifacts。

明确不修改：

- 2481 shared contract、2482 policy/evaluator/enums/reason codes/thresholds；
- 2532 immutable execution package、raw Results bytes、external counters；
- QuantConnect project/API/browser、Cloud run、raw option rows、Object Store；
- selection、engine、paper/live/broker、订单或成交逻辑。

## 6. 阶段与验收

### S0 — Registration and frozen mapping

- canonical task register 与 supporting requirement 登记完成；
- exact 15-check mapping、input hashes、safety boundary 冻结；
- governed SINGLE_LANE preflight PASS 后才允许 implementation mutation。

### S1 — Deterministic admission implementation

- exact-byte loader 拒绝 policy/evidence/manifest drift；
- raw Results 只做 byte count/SHA verification，不提取 raw option rows；
- 15 个 check 恰好一次且按 canonical order 输出；
- check status、reason、evidence locator 与 missing requirement 可重放；
- report/manifest 使用 canonical JSON + self-excluding content seal；
- build/validate/replay/tamper tests PASS。

### S2 — Reader projection and closeout

- Atlas 首层先解释“为什么仍关闭”，不堆叠 15 个术语；详细 check 放进展开层；
- `docs/system_flow.md` 与 architecture/generated authority 同步；
- focused、Architecture、Contract、Integration、Reproducibility 与 required Full 在最终候选通过；
- task terminal、local-main ff-only、ordinary non-force push、SHA verification 与 branch/lease cleanup 完成。

## 7. 当前状态与生命周期

`DONE`：离线 admission 已完成，且没有执行新的外部动作。deterministic validator 已复核 retained
raw Results exact `814999` bytes / SHA-256 `5d322034...d50d`，同时验证 2532 tracked package、
2482 policy 与 2481 contract 的冻结身份。生成结果为：

- policy file SHA-256：`1b6fc081fcd442acc125c5077422b941522b1e0fa90926835afa7e1b57d2539b`；
- admission report content seal：`58a80cf8c0c7678dd1eab0cc8b3297fc1c27a6aace45f46d6789efc2446d7c0a`；
- package manifest content seal：`6029d78faf9fb0c752ef030ac399707cb04ebf053a8ff88d392fc327f14b62f3`；
- canonical coverage：`1 PASS / 1 FAIL / 13 NOT_EVALUATED`；
- overall：`DQ=FAIL`、`PIT=NOT_EVALUATED`、selection/engine 继续 cash-preservation blocked；
- Atlas 首层已改为先解释研究上下文、当前门槛和证据缺口，15 项检查留在展开层。

本任务的 terminal 语义是“离线判定已完成”，不是“策略准入通过”。ordinary publication 仍由最终
候选的 focused/generated/formal validation 结果约束；任一级非 PASS 都禁止发布。

忽略的 replay workspace
`D:/Work/AITradingSystem/outputs/external_validation/trading_2532_session_finalization_v2_once_20260817/`
和 `G:/Download/Upgraded Magenta Gorilla.json` 继续保留。它们属于唯一 raw Results 复核证据；本任务只读使用，
不移动、不覆盖、不删除。退出条件是 2533 final report 已独立验证 source hash，且 Project Owner 另行决定
永久保留或清理。在此之前两份证据均保持可恢复。
