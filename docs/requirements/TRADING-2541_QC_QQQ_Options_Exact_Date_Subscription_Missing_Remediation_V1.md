# TRADING-2541 — QQQ Options exact-date subscription missing remediation V1

- priority: `P0`
- status: `READY`
- owner: Codex（contract / implementation / offline validation）；Project Owner（后续新的 R1 Cloud validation）
- governed mode: `SINGLE_LANE`
- predecessor evidence: `TRADING-2537` V2 terminal attribution
- production effect: `none`
- broker action: `none`

## 1. 已确认问题

TRADING-2537 V2 在 existing clone `35444189` 的唯一 zero-order Cloud backtest 中完整评估
`2021-02-22..2025-12-02` 的 `1202` 个 XNYS session，并确定唯一 subscribed-chain 缺失日为
`2022-08-26`。当日 QQQ equity Slice 存在、subscribed option-chain event count=`0`；同一次、同一
source trading date 的 `History[OptionUniverse]` probe 返回 `1` 个 record、`6496` 个 contracts，
`non_target_record_count=0` 且没有 cross-date fallback。

因此 provider catalog 对 exact source date 实际可用，terminal attribution 已终结为
`EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING / RESOLVED`。这排除了“provider 当日目录为空”作为
该 session 的根因，但没有修复 subscription/transport 缺失；`chain_presence=FAIL`、DQ=`FAIL`、
PIT=`NOT_EVALUATED` 和 `POLICY_BLOCKED_CASH_PRESERVATION` 继续成立。

封存证据：
`inputs/research/qqq_options/trading_2537_existing_clone_exact_date_execution_v2/`。

## 2. Intended best solution

实现一个可审计的 same-date recovery adapter：常规 subscribed Slice chain 是主路径；只有当完整 session
finalization 确认当日 subscribed chain 从未交付、且 equity session 身份有效时，才允许对同一 target
source date 执行受控 `History[OptionUniverse]` recovery。recovery 必须：

1. 精确匹配 `OptionUniverse.Time` 与 target trading date；
2. 独立验证 `EndTime = Time + 1 day` 的 availability 语义；
3. 对 cross-date、最近可用日、duplicate/partial/invalid record fail closed；
4. 进入与主路径相同的 canonical schema、cache identity、DQ/PIT 和 lineage contract；
5. 显式标记 `delivery_path=EXACT_DATE_PROVIDER_HISTORY_RECOVERY`，不得冒充正常 Slice delivery；
6. 不得 forward-fill、backfill 其他日期、删除该 session 或缩短 primary window。

## 3. 分阶段实施

### S1 — contract freeze

- 定义 Slice 主路径与 exact-date recovery 的 precedence、session-finalization 时点和单次 probe 上限；
- 冻结 typed delivery status、cache identity、source/availability time、lineage 与失败枚举；
- 明确 6496 只是本次 attribution count，不能硬编码成未来接受阈值。

### S2 — adapter 与离线验证

- 在 QQQ options transport/collector path 实现 same-date recovery adapter；
- 添加 source-date、availability、duplicate、partial、cross-date、no-equity-session 与主路径优先级测试；
- 更新 `docs/system_flow.md` 和受影响 authority；
- 不访问 QuantConnect、不运行 Cloud backtest、不创建订单或成交。

### S3 — 新的 bounded R1 validation

- S1/S2 通过正式 repository validation 后，另建 exact manifest；
- 在 existing clone 上执行一次 zero-order Cloud validation，实际计数和 terminal evidence 自动封存；
- 只有 1202/1202 session 通过同一 schema/DQ/PIT contract，且该日 recovery 有 exact identity、无跨日
  fallback、orders/fills=`0/0`，才能考虑把 chain presence 与研究 readiness 向前推进。

## 4. Acceptance criteria

- `2022-08-26` 只接受 source date 完全相同的 provider-history recovery；
- 正常 subscribed Slice delivery 始终优先，recovery 不得重复或覆盖正常 chain；
- cross-date/recent-date fallback、重复记录、错误 availability、缺少 equity session 全部 fail closed；
- recovered contracts 走同一 schema、cache、DQ/PIT、lineage 与 downstream gate；
- primary requested/evaluated range 保持 `2021-02-22..2025-12-02`，expected session count=`1202`；
- 未经新的 reviewed R1 validation，不得把 `chain_presence=FAIL`、DQ/PIT 或 engine readiness 改为 PASS；
- orders/fills=`0/0`，production effect=`none`，broker action=`none`。

## 5. 当前边界

本任务当前只授权 repository 内 contract、实现和离线验证。它不沿用已消费的 TRADING-2537 external scope，
不授权新的 Cloud mutation/build/backtest/provider query，也不授权 paper/live、broker、order 或 fill。
