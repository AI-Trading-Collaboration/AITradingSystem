# ARCH-005 S4A 受监督自动化 Closeout

最后更新：2026-07-20

状态：`BASELINE_DONE_S4A_S5_PENDING`

## 1. 结论

S4A 已把 S2～S4 的 readiness、deterministic scheduler 与 execution lease 连接到两条真实、隔离的
Git worktree/branch，并完成 engineering 与 research-evidence command worker、manifest/resource/Git/
log evidence binding、human-gated integration queue、validator、orphan audit 和 clean-only cleanup。

该增量仍是 pre-S5 受监督自动化：Markdown 是唯一可写任务事实源，integration candidate 必须等待
human coordinator；没有自动 commit、merge、push、PR、task status mutation、strategy candidate
expansion、paper-shadow、production 或 broker/order。

## 2. 实现范围

- policy：`arch_005_supervised_automation_policy.v1`，固定两 worker/两 lease、exact sibling worktree
  root、`codex/arch-005-s4a/` branch prefix、argv allowlist、`shell=false`、环境 allowlist、timeout 与
  stdout/stderr byte budget；
- pilot：一条 engineering focused-validation worker 与一条 research-evidence worker；
- controller：clean coordinator + exact HEAD precondition，创建/校验 worktree 与 branch，申请 lease 后
  并发执行，单 lane 失败不取消无依赖 lane；
- evidence：task/change/base/manifest SHA/resource claims/exact argv/environment SHA/PID/exit/timeout/
  stdout/stderr SHA+bytes/初始与最终 HEAD/branch/dirty paths；
- integration：两 worker 与 replay PASS 后只生成
  `AWAITING_HUMAN_COORDINATOR_APPROVAL`，固定 `merge_allowed=false`；
- recovery：失败 lease expire，成功 lease release；orphan audit 检查 active lease、缺失 evidence/
  worktree 和 dirty worktree；cleanup 需显式 coordinator approval，拒绝 dirty path，不 force、不删 branch。

## 3. 真实 Pilot

### 3.1 Fail-closed run

- run：`supervised-4dd5bee3720db78b`；
- base：`26a15951c28b42b167717dba8c29d88185559b1b`；
- report：`supervised-run-a78509ed3e9ec980a344`，`FAIL`；
- duration：`5.013308s`；
- 根因：sanitized Windows environment 未包含 `APPDATA`，两条 worker 均无法定位 user-site
  `pytest`，exit code=`1`；
- 同时发现 CLI wrapper 曾固定打印 `status=PASS`，而内部 report 已正确为 `FAIL`；独立 validator
  拒绝 report（report status、worker artifacts、integration candidate 三项失败）；
- 处理：保留 report/log/6-event lease chain；两 lease 均 `EXPIRED`、active lease=`0`。新增
  CLI status propagation regression，并将实际 environment keys/SHA 纳入 worker evidence；没有重写
  失败 artifact 或降低 validator。

### 3.2 Successful run

- run：`supervised-5de95c5f37821ac3`；
- base：`50a765fe3176b53b308adc92fdf7b66e96269386`；
- report：`supervised-run-d21df0bbca3d00da9708`，`PASS`；
- total duration：`19.789205s`；
- engineering worker：`17 passed / 1.78s pytest`，worker duration=`3.343505s`；
- research-evidence worker：`9 passed / 14.82s pytest`，worker duration=`16.094196s`；
- replay：6 events、2 `RELEASED`、active lease=`0`、issue=`0`；
- validator：13/13 checks PASS；orphan audit：0 issue；
- integration candidate SHA-256：
  `c6b09c12165376cc698debe72ba1535a4325b72ab16e97ff7d12b7fa3a01f1c5`；状态为
  `AWAITING_HUMAN_COORDINATOR_APPROVAL`，human approved=`false`，merge allowed=`false`。

两次 run 的四条 worktree/branch 暂时保留为 review evidence；它们均无 dirty path，未执行 force
cleanup 或 branch deletion。运行耗时主要由 research-evidence tests 决定，未出现 timeout、output
budget 超限或异常长尾。

## 4. 验证

- implementation focused：`40 passed / 16.15s`；
- P1 status/environment fix focused：`10 passed / 15.17s`；
- generated DevEx：987 modules、1,138 test/support、0 orphan、0 overlap、0 direct-writer violation；
- architecture-fitness：`446 passed / 35.04s pytest / 35.82s wall`，artifact
  `architecture-fitness_20260720T094327Z`；
- contract-validation：`265 passed / 148.42s pytest / 149.02s wall`，artifact
  `contract-validation_20260720T091223Z`；同口径上一 artifact wall=`151.82s`，本次约快 1.8%，没有
  新的 evidence-staleness/paper-shadow scan regression；
- reproducibility：`23 passed / 8.73s pytest / 9.31s wall`，artifact
  `reproducibility_20260720T091536Z`；
- formal full：`6,430 passed / 2 skipped / 642 warnings / 969.60s pytest / 970.42s wall`，artifact
  `full_20260720T091601Z`；相较上一 S2～S4 wall=`956.37s` 增加约 1.47%，属小幅波动；新增 S4A
  tests 未进入 slowest 50；
- full telemetry：6,432 nodes / 1,080 files / 16 workers，profile/performance/telemetry/provenance=
  `PASS`，scheduler applied、duration order verified、fallback=false，tail idle max=`15.13s`、total=
  `215.37s`；不据单次 run 宣称稳定性能改善。

正式 gate 与 freshness closeout 均 PASS 后，S4A 转 `BASELINE_DONE`；完整 S5 自动化仍待新授权。

## 5. 剩余边界

S4A closeout 不解锁 S5。自动 commit/merge/push/PR、canonical task registry cutover、remote queue、
cross-machine lease、多主仲裁和 agent 自动写代码均需新的 owner 授权与独立验收。ARCH-004 继续停在
G2.5 前；`production_effect=none`、`broker_action=none`。
