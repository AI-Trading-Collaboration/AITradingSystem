# TRADING-2541 — QQQ Options exact-date subscription missing remediation V1

- priority: `P0`
- status: `IN_PROGRESS`（S1/S2 已发布；S3 standing-scope exact manifest 已验证，待发布后 dispatch）
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

## 6. S1/S2 实现结果（2026-08-22）

repository 内的 durable recovery 基线已经实现，不再停留在方案描述：

- policy：`config/research/qc_qqq_options_exact_date_subscription_recovery_v1.yaml`，file SHA-256
  `05e6daafed6d891e0db1c590ed3750a01e86c91b78d786cf0d585d9fabdb5ce9`；
- pure adapter / candidate builder：
  `src/ai_trading_system/qqq_options_research/exact_date_subscription_recovery.py`，file SHA-256
  `cadc80f53287798fe638d63d34abc8caa04983c4c10bf3ee9a55342e905e6004`；
- sealed package：
  `inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_v1/`；
- generated `main.py`：LF byte count=`31720`，SHA-256
  `d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3`；
- recovery contract content SHA-256：
  `167c8bf0f80b9e29293dda7fd1d536f95eff858349576e09145b7836f8f5ed21`；
- package manifest content SHA-256：
  `465961f8bb040968d0d49f1753aa40d8160ae2d35333d3dee1d025e358f49188`；
- 聚焦验证：`tests/test_qqq_options_exact_date_subscription_recovery.py` 共 `17 passed`；Ruff PASS；
- formal repository validation：Architecture=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed`、Reproducibility=`24 passed`，全部使用 pytest-xdist `16` workers / `loadfile`
  并写入对应 runtime artifact。

pure adapter 将 accepted provider record 转换为既有 `SessionSliceObservation`，交给同一个
`DailyTransportSessionReducer` 计算八个 axis；normal subscribed Slice 一旦存在就不计划 provider query。
生成的 zero-order candidate 仅在 `on_end` 确认唯一缺链目标日和有效 equity session 后计划最多一次
`History[OptionUniverse]`，并要求 `record.Time.date()=2022-08-26`、
`record.EndTime=record.Time+1 day`。cross-date、duplicate、missing、empty 或 availability identity 错误
均进入 typed fail-closed terminal。

## 7. 尚未完成的实证边界

本轮没有访问 QuantConnect、没有执行 Cloud build/backtest/provider query，也没有订单或成交。因而当前只可得出
“可审计的离线 recovery 路径已实现并可生成候选代码”，不能得出“Cloud transport 已恢复”或“数据质量已通过”。
在新的 S3 bounded R1 validation 完成以前，`cloud_validation_status=NOT_EXECUTED`、
`chain_presence=FAIL`、DQ=`FAIL`、PIT=`NOT_EVALUATED`、
`engine_status=POLICY_BLOCKED_CASH_PRESERVATION`，orders/fills=`0/0` 均保持不变。

## 8. S3 standing owner scope（2026-08-22）

Project Owner 已在当前对话明确要求 Codex 继续修复该工程问题。依据 DEVX-008 的
`R1_BOUNDED_RESEARCH_SANDBOX` 规则，该意图与本任务已审阅的 S3 边界构成
`STANDING_OWNER_SCOPE`，不要求 Owner 机械回贴机器生成的长 token。执行前必须发布并自动重放：

- `config/research/qc_qqq_options_exact_date_subscription_recovery_execution_v1.yaml`；
- `inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_execution_v1/`；
- exact candidate：`31720` LF bytes / SHA-256
  `d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3`；
- existing clone `35444189`；原项目 `34808569` mutation=`0`、new clone=`0`；
- clone mutation/save/automatic build/zero-order backtest/provider query 上限均为 `1`；
- orders/fills=`0/0`，无 retry、raw rows、contract identifiers、individual fields、Object Store、
  public share、migration、cloud deletion、paper/live、broker 或 production action。

standing scope 在首个 backtest dispatch 时消费；不论 terminal 成功或失败都必须停止并封存 exact
readback、actual counters、build/run identity 和 terminal statistics。authorization state 与
technical validation state 分轴记录；只有 S3 technical terminal 满足 acceptance criteria 才能推进
chain presence/DQ/PIT，不能用 standing scope 本身代替数据正确性证明。

pre-dispatch manifest identities：

- execution policy file SHA-256：
  `e121d3bde9968a1c344f1d389675cd436c35ae1df21bf935af1f571932d6dca0`；
- standing-scope admission content/file SHA-256：
  `02f5dfbaa85c58fd7abec2598bb90c73df699bca13762b44d535d265797d69ce` /
  `96cabc23f01a53ca2f20c1a9bf02395060ed619df72e0248f4bb2ce62f1a08af`；
- run-scope content/file SHA-256：
  `79b1f4a322d893a74b7ef13d48f36bc0de18c29500f997308c88a15f9af726ff` /
  `63f6fe0a46e585021feb9f3cbf3a85eac616ea1590ebf6d35a8975fc2f728634`；
- execution-manifest content/file SHA-256：
  `72e326fca3677d9cb4516b447003165be3ff470cca27ccfe6e6ac7d6f5a366cb` /
  `3379942295b97499b661e9fc70adbf4f4f978cfbe871b0e4516ab9d63862721b`；
- pre-dispatch focused=`67 passed`、Architecture=`865 passed`、Contract=`276 passed`、Ruff PASS。

这些 artifacts 当前仍为 `READY_UNUSED / NOT_EXECUTED`，actual external counters 全部为 `0`；只有其
ordinary-pushed main SHA 完成三方相等验证后才允许开始浏览器 readback/mutation/dispatch。
