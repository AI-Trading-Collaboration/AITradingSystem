# TRADING-2541 — QQQ Options exact-date subscription missing remediation V1

- priority: `P0`
- status: `DONE`（S3 v3 exact-date recovery Cloud terminal PASS）
- owner: Codex（正式证据已封存并关闭）
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

## 9. S3 pre-dispatch coding-session 阻塞（2026-08-22）

S3 manifest 已以 commit `91ab61074eca8037402dec457f870cf6cbeb3feb` 完成 ordinary push，且
`HEAD = local main = origin/main`。随后自动 manifest replay PASS：execution-manifest
content/file SHA-256 仍为
`72e326fca3677d9cb4516b447003165be3ff470cca27ccfe6e6ac7d6f5a366cb` /
`3379942295b97499b661e9fc70adbf4f4f978cfbe871b0e4516ab9d63862721b`，candidate 仍为
`31720` LF bytes /
`d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3`。

浏览器在 candidate input 以前精确核验了 clone URL
`https://www.quantconnect.com/project/35444189`，但 QuantConnect 先停留于
`Requesting coding environment...`，随后返回：

- `No Coding Session Available`；
- `We got an error retrieving your session information, please retry`。

官方页面 `Retry` 仅执行一次，仍返回同一终态；因此没有继续重试。QuantConnect 官方资源文档确认
Free tier 具有一个全局 coding-session quota，所以不把付费升级当作本任务前置条件。当前 blocker 是
free coding session 可用性或 session-information retrieval，而不是 candidate compile/backtest 失败。

本次没有完成 editor readback，也没有把 candidate 输入 Web IDE；clone/original mutation、save、
automatic build、Cloud backtest、provider query、orders、fills 和 new clone 的 actual counters 全部为
`0`。standing scope 仅在 backtest dispatch 时消费，故仍为
`UNCONSUMED_NO_BACKTEST_DISPATCH`；technical validation 仍为
`BLOCKED_PRE_DISPATCH_NOT_EXECUTED`。精确证据见
`inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_execution_v1/predispatch_environment_evidence.json`。

解除条件：QuantConnect 对该免费账户暴露一个可用 coding session。解除后 Codex 必须重新自动 replay
同一未消费 manifest，再执行既定的一次 clone mutation/save/build/backtest/provider-query 序列；不得把
本次页面故障计为算法失败，也不得据此提升 chain presence、DQ、PIT 或 engine readiness。

## 10. S3 v2 环境启动 build 分轴（2026-08-23）

Project Owner 重装 Chrome plugin 后，Codex 已恢复对 existing clone `35444189` 的稳定控制和只读
editor copyback。云端 `main.py` 精确匹配 repository 内 TRADING-2537 v2：LF byte count=`26587`、
SHA-256=`06b26262823c8c56ebceb4c90356086e07b050f9192e087b5e35a3dc43c5eac2`；因此 TRADING-2541
candidate 仍未输入，clone mutation/save/backtest/provider query/orders/fills 均为 `0`。

QuantConnect 在 coding session 启动、candidate input 以前自动构建了当前旧代码，Cloud Terminal 显示
LEAN `2.5.0.0.18024`、build id=`11e9d4-8b195b`。这不是算法 retry，也不是 candidate build，但属于
真实的 external resource action，不能继续记为 `0`。v1 的单一 automatic-build 上限已经被该环境动作
占用；直接保存 candidate 会使总计数越界。

因此新增不可回写 v1 历史 artifact 的 S3 v2 accounting：环境启动 build 上限/实绩=`1/1`，candidate
automatic build 上限/剩余=`1/1`，总 automatic build 上限=`2`。其余边界保持不变：existing clone
mutation/save/backtest/provider query 各最多 `1`，无 retry，orders/fills=`0/0`，原项目 mutation=`0`，
production effect=`none`，broker action=`none`。v2 发布并完成三方 SHA 相等后必须重新 replay；首个
backtest dispatch 仍消费 standing scope，且不论 terminal 结果均立即停止并封存证据。

S3 v2 pre-dispatch identities：

- execution policy file SHA-256：
  `90764f5d63f5d045f285668d1f0fc81e81e49afe4afe5d7b29d16e5706005988`；
- startup evidence content/file SHA-256：
  `6c8ae6e5b2c6571e8d5b1e20f5755d7b9a78522b945fa589989fd3a34d570f1c` /
  `57581e308e7d52c33844d2697b60f05ff6631af708e86eb0ab20a291725f7a08`；
- standing-scope admission content/file SHA-256：
  `b1ef7730409c9f8e70599e3a46a59539f2df4b160a945b556295f35f9ac2111b` /
  `9a521dbf4daed4fb2e1629d427919e3137f4f4a930cbd961bb241912351e8258`；
- run-scope content/file SHA-256：
  `48219c45a4b804da1362d3ebe78b14f04eb3a9695c6ec6f0184222c6b8e181bd` /
  `79f5c3554cbe48ab2a7d71f23d01b231191a6c06061eb17057c7b8654658027e`；
- execution-manifest content/file SHA-256：
  `e18c2dcf867de606a21d02f885e53e6a134a75a4a5b506a3cb7bd1e4cf6759aa` /
  `6cb0ad1450dc65e3ca4b1520343ada29b66cc9b6acd7f9ebb9cfef805894bd98`。

## 11. S3 v3 稳定启动 baseline（2026-08-23）

v2 发布后的 replay 在 candidate input 前发现第二个旧代码 background build：
`684f9c-8b195b`（Cloud Terminal time=`3:26:56`，LEAN `2.5.0.0.18024`）。两个 build 均绑定同一
TRADING-2537 v2 readback，candidate mutation/save/backtest/provider query 仍全为 `0`；因此不是
candidate retry，但 v2 的单个 environment-build baseline 已失效，必须 fail closed。

v3 不改写 v1/v2，锁定 environment startup build 上限/实绩=`2/2`、candidate build 上限/剩余=`1/1`、
总 automatic build 上限=`3`。其他边界与 v2 完全相同。只有 v3 发布、三方 SHA 相等、manifest replay
与页面 build lineage 都通过后才能执行一次 candidate mutation/save/backtest；任何新增 pre-candidate
build 都再次 fail closed。

S3 v3 pre-dispatch identities：policy file SHA-256
`a619908bb5db2dc67704b67bfa9b59f05a9c8c6365a582e1bdb4e54d88684608`；startup evidence
content/file SHA-256 `8d44956dd7ad3f89665a90b1232c8dffa124d2bd33ad4f0798b82351a49e03aa` /
`727b7980d3500fe83683a0a1881082da04e58ee743fd2e0adef87441f36d8d8b`；admission content/file
SHA-256 `d459284e4c2a998b3e10983219801c6d11cffeb06f20718ff484ec2e9ab1bbb3` /
`85977aa84b381c5ac7bf3ec812ba8f24077a75990fdb7bc8bfc813bc22fd85b7`；run-scope content/file
SHA-256 `5aee2bd98127800dea2643c92b667c09b8026bff0a196b01bf99ba6fcff09944` /
`429599db20609b26fe854ced925e31e3672165ece5c0261b53f49da9684b676e`；execution-manifest
content/file SHA-256 `1871b6005adebeefe615f24dd1686efd939e85eeb18fb940a5df82aa67fa7304` /
`481a6f4aca6c05b965693f067cd6ebb45685cbd6524bc151f2394aecde3dc6d8`。

## 12. S3 v3 正式 Cloud 终态（2026-08-23）

v3 以 pre-dispatch commit `31bac57176a335545b11cca6f0f4055650e3ffdd` 完成
`HEAD = local main = origin/main` 和 manifest replay。页面在 candidate input 前仍只有两个已封存的旧代码
environment startup build。随后 existing clone `35444189` 只执行一次 candidate mutation/autosave，云端回读
精确匹配 `31720` LF bytes / SHA-256
`d8836be2165b56a8e9d56fb16eefb4e80c9be9225f9c8ffba93833bb1e69c9b3`；唯一 candidate build id 为
`d65491-f6b483`，LEAN=`2.5.0.0.18024`。

唯一 zero-order Cloud backtest：

- name=`Hyper Active Red Barracuda`；id=`8142b39f1c76a10471a355fc1eb27a1d`；
- requested/evaluated range=`2021-02-22..2025-12-02`；expected/observed sessions=`1202/1202`；
- terminal=`COMPLETE`，duration=`5643.88s`，data points=`38,396,279`；
- target/recovery source date=`2022-08-26`，availability date=`2022-08-27`；
- recovery status=`ACCEPTED`，delivery path=`EXACT_DATE_PROVIDER_HISTORY_RECOVERY`；
- provider query=`1`，exact-date record=`1`，contracts=`6496`，non-target record=`0`，invalid availability=`0`；
- normal Slice sessions=`1201`，recovered sessions=`1`，unresolved sessions=`0`；
- orders/fills=`0/0`，portfolio invested=`false`，raw rows/log data/Object Store/public share/migration 均未使用；
- 原项目 `34808569` mutation=`0`，production effect=`none`，broker action=`none`。

因此缺失日已经从“定位与归因”推进到实质修复：正常 subscribed Slice 仍是主路径，唯一缺失 session 由同 source
date、正确 availability date 的 provider history 精确补齐，没有跨日替代。`chain_presence` 可在明确标记
`EXACT_DATE_PROVIDER_HISTORY_RECOVERY` 的研究 transport contract 下判定为 PASS；exact-source/availability PIT
identity 和 1202-session transport completeness 通过。该 run 是 zero-order 诊断，未授权把结果直接提升为策略
engine、生产或 broker readiness；terminal 也明确记录 `dq_pit_promoted=false`。

性能观察与正确性结论分开记录：免费 B-MICRO 处理完整窗口耗时约 94 分钟、处理 3839 万数据点，结果有效，
但后续若把该诊断常态化，应另建性能优化任务，不能通过缩短 primary window、删除 session 或跨日 fallback
掩盖成本。本任务 acceptance criteria 已全部满足，正式 export-safe evidence 位于
`inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_execution_v3/export_safe_terminal_evidence.json`。
