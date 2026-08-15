# TRADING-2528 — QQQ Options daily transport per-axis diagnostic contract V1

- status: `IN_PROGRESS`
- priority: `P0`
- governed mode: `SINGLE_LANE`（2522 release 后独立启动）
- registration base: `f876ec853c1431e760bc4cf5b89123265a32080f`
- predecessor: `TRADING-2522`
- production effect: `none`
- broker action: `none`
- external action: `none`

## 目标

为 2522 的 typed failure
`DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS` 建立严格离线、逐轴可解释的诊断合同。
合同必须区分 option chain presence、underlying、bid/ask、Greeks、IV、open interest、volume 与
cross-field consistency 等观测轴，保留 session-level count/unknown/rejection reason 的 derived aggregate，
使后续工程能定位究竟是哪一轴或哪组交叉条件导致 `1201/1201` chain sessions 被拒绝。

本任务不重跑 QuantConnect、不读取 raw option rows、不下载新数据、不填策略阈值、不改变 2482 DQ/PIT
语义，也不把诊断结果解释为数据质量 PASS、策略有效或 engine 获批。

## 冻结继承事实

- 2522 backtest id：`60ce7e0bec3ad2d83a4d1341e0221492`；
- requested/evaluated range：`2021-02-22..2025-12-02`，expected sessions=`1202`；
- observed chain sessions=`1201`，valid candidate sessions=`0`，transport rejected sessions=`1201`；
- orders/fills=`0/0`，raw rows/log/Object Store/API/CLI/HTTP=`none`；
- v4 authorization 已消费，`further_cloud_run_authorized=false`；
- 2522 Results admission=`FAIL`，local aggregate 与 option-event DQ/PIT 均为 `NOT_EVALUATED`；
- 现有 aggregate 不能可靠区分具体失败轴，因此 root cause 必须保持 unresolved。

## 合同范围

1. 定义稳定 public enum / record / envelope，分别表达每一诊断轴的 `PRESENT`、`MISSING`、
   `INVALID`、`NOT_EVALUATED` 与 typed reason；`UNKNOWN` 永不升级为 PASS。
2. 输入只允许 2522 export-safe derived aggregates、已冻结 runtime diagnostic 与 canonical hashes；
   禁止接受调用者自报的 axis PASS，禁止把 aggregate 反演成 raw option rows。
3. 输出必须绑定 repository/policy/contract/source/result/backtest/range/session identity，并提供
   canonical bytes、content SHA-256、seal/from-json replay 与输入排列不变性。
4. 区分单轴 reject 与 cross-axis reject；无法唯一定位时输出 typed unresolved combination，不能猜测
   quote、Greeks、OI、volume 中任一轴为根因。
5. negative/property/golden tests 覆盖 forged PASS、extra/missing axis、duplicate/reordered inputs、
   checksum/range/session/count mismatch、UNKNOWN promotion 与 raw-row leakage。
6. 默认结果保持 `POLICY_BLOCKED_CASH_PRESERVATION`；合同完成不自动授权 external action、Cloud run、
   selection、engine、回测、订单或投资解释。

## Path claims

2528 task-owned paths 在独立 START/LANE preflight 后冻结；预计包含 requirement、versioned policy、
diagnostic contract module 与 focused tests。task registry/index/shadow、`docs/system_flow.md`、architecture、
Atlas/generated/compatibility 仍由该任务的单一 coordinator 在最终候选统一接线。

## 当前边界

本次 2522 coordinator 只登记该后继，不实现任何 2528 code/policy/test，不启动新外部动作。2528 必须从
2522 ordinary-pushed exact main 独立启动并重新执行 governed preflight。
