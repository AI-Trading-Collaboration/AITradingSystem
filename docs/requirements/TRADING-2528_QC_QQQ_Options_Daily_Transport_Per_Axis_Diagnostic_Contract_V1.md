# TRADING-2528 — QQQ Options daily transport per-axis diagnostic contract V1

- status: `BASELINE_DONE`
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

2528 task-owned paths 已在独立 START/LANE preflight 后冻结为：

- `docs/requirements/TRADING-2528_QC_QQQ_Options_Daily_Transport_Per_Axis_Diagnostic_Contract_V1.md`；
- `config/research/qc_qqq_options_daily_transport_per_axis_diagnostic_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/daily_transport_per_axis_diagnostic.py`；
- `tests/test_qqq_options_daily_transport_per_axis_diagnostic.py`。

task registry/index、`docs/system_flow.md`、`docs/artifact_catalog.md`、architecture fragments/manifests 与
compatibility authority 仍由该任务的单一 coordinator 在最终候选统一接线。

## 当前边界

2522 coordinator 只登记了该后继，没有实现任何 2528 code/policy/test，也没有启动新外部动作。2528 已在
2524 独立发布后从 exact main `06b0b29fac5d77e011d5dbe0151f566c8c030d0d` 启动：
`SINGLE_LANE` START/LANE preflight 均为 `PASS`，branch=
`codex/trading-2528-daily-transport-axis-diagnostic`。当前仍保持 `external_action=none`；本轮只允许严格离线
policy/contract/focused tests，不授权新的 QuantConnect/Cloud/API/CLI/HTTP/Results/raw-row 动作。

## 实现进度

- 2026-08-16：versioned policy 已冻结 2522 repository/result/backtest/range/session 与六个 canonical source
  hashes；typed contract 定义八个 axis、四种 axis status、single/cross/unresolved reject scope、canonical
  content seal/from-JSON replay 与输入排列不变性。冻结 2522 事实只产生 `OPTION_CHAIN_PRESENCE=PRESENT`；
  underlying、bid/ask、Greeks、IV、OI、volume、cross-field 全部为 `NOT_EVALUATED`，最终分类固定为
  `UNRESOLVED_COMBINATION`。forged `PASS`/`UNKNOWN`、axis set/order、hash/range/session/count、raw-row
  carrier 与 unknown promotion negatives 已覆盖；core focused=`19 passed in 7.81s`，Ruff=`PASS`。
- architecture/system-flow/task/generated/compatibility authority 已完成接线并验证新鲜；core、predecessor replay
  与 architecture focused 同批为 `61 passed in 21.50s`，strict mypy 与 Ruff 均为 `PASS`。离线合同因此进入
  `BASELINE_DONE`；发布前仍必须在 exact candidate 上完成五级 formal validation。
- 下一边界只属于未来独立授权：若 Project Owner 提供新的 exact proposal hashes 与授权 token，后继任务才可
  采集逐轴 export-safe aggregate。当前没有下载数据、读取 raw option rows、运行 Cloud、改变 DQ/PIT、授权
  selection/engine 或产生投资解释。
