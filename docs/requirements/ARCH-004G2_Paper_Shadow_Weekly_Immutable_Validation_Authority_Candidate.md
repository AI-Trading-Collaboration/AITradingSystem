# ARCH-004G2：Paper Shadow Weekly Immutable Validation Authority Candidate

最后更新：2026-07-26

上位任务：`ARCH-004G2_VALIDATION_RUNTIME_BUDGET_AND_FIXTURE_REUSE`

增量 ID：
`ARCH-004G2_PAPER_SHADOW_WEEKLY_IMMUTABLE_VALIDATION_AUTHORITY_CANDIDATE`

状态：`REJECTED_THRESHOLD_MISS`

## 1. 证据与目标

最近 6 个自然 Full 中，`tests/test_paper_shadow_weekly_review.py` 的 file duration 分别为
`696.1/1184.3/766.2/747.6/920.2/730.7s`。最新
`full_20260726T142413Z` 的 5 个既有 node 合计 `730.746786s`：

- 首个 node setup/call=`148.623/120.992s`；
- missing-input call=`129.652s`；
- CLI build/report/validate call=`161.847s`；
- illegal-decision/tamper call=`151.435s`；
- recovery-window call=`18.195s`。

测试已用 module-scoped fixture 只构建一次 daily/drift/contract/ledger source DAG，但当前
`artifact_validation_session` 仍是 function scope。五个 node 因而在 shared source bytes 未变时
重复执行相同的 content-fingerprint validation；missing-input 和 illegal-decision node 又已经用
`try/finally` byte-exact 恢复共享 source。

本候选只评估一个更高杠杆的 test authority：让同一个 loadfile worker 在整个 module 生命周期内
持有一个 outer PASS-only `artifact_validation_session`。它不建立跨进程或持久化 cache，不修改
producer、validator、fixture 内容、DQ/PIT、策略或 production 行为。

## 2. 冻结范围

允许修改：

- `tests/test_paper_shadow_weekly_review.py`：仅把现有 autouse validation session 从
  function scope 提升为 module scope；既有 module source fixture 的内层 session 必须复用该
  outer state。

Coordinator 负责本需求、上位需求、task register、generated task/DevEx views、append-only
compatibility authority 和正式验证。不得修改 production source。

## 3. 预冻结收益门槛

Baseline 和 after 使用完全相同命令：

`python -m pytest -n 16 --dist loadfile tests/test_paper_shadow_weekly_review.py -q
--durations=10 --durations-min=1
--junitxml=outputs/validation_runtime/arch004g2_paper_weekly_candidate/pytest.xml`

记录 pytest wall、5 个既有 node 的 setup/call duration 和命令输出 SHA-256。设 baseline wall 为
`B`，候选保留条件同时为：

- after worst wall `<= 0.80 * B`；
- after worst wall `<= B - 30s`。

至少运行两个 after 样本，以较慢样本判断。不得用 Full 的共享资源波动替代 same-command
baseline/after，也不得为性能比较额外运行多个 Full。

## 4. 安全门槛

- 5 个既有 nodeid、assertion、fixture date/source、CLI 路径和 output validation 全部保留；
- shared source 的 unlink/tamper 必须产生新的 fingerprint、执行真实 validator 并 FAIL；
- `finally` restore 后只能在 bytes 完全相同时复用原 PASS；
- `FAIL`、`PASS_WITH_WARNINGS`、exception 和不可复制结果仍不得缓存；
- test 单独运行和完整文件运行都必须 PASS；
- `tests/test_artifact_validation_session.py` 的 cache identity、drift、tamper、FAIL/exception、
  owner 和 lifecycle contract 保持 PASS；
- 不新增 order-only assertion，不隐藏 test，不减少 node，不改 xdist worker/distribution；
- `strategy_logic_changed=false`、`cached_data_mutated=false`、
  `production_effect=none`、`broker_action=none`。

任一安全或收益门槛失败，必须 byte-exact 撤回 test implementation，只保留拒绝证据和治理记录。

## 5. 验证与退出

候选保留时：

1. Ruff、Black 与目标文件 focused PASS；
2. validation-session contract 和相关 paper-shadow regression PASS；
3. generated task/DevEx views、compatibility hashes 和 append-only prefix PASS；
4. architecture、contract、integration 与本批唯一 natural-boundary Full PASS；
5. 回填 exact before/after、正式 artifact 和 retained/rejected 结论；
6. 任务分支提交后 fast-forward local `main`，通过 remote closeout preflight 后普通推送并清理分支。

本候选完成不代表 validation runtime 总任务完成，也不支持 stable global Full improvement claim。

临时 runtime evidence 目录固定为
`D:\Work\AITradingSystem\outputs\validation_runtime\arch004g2_paper_weekly_candidate`，owner 为本增量，
用途仅为同命令 JUnit timing；每轮在覆盖前记录 SHA-256 和统计。退出条件是最终 tracked
requirement 已保存 exact baseline/after 结果且正式验证闭合；closeout 前审计并删除该临时目录，
其内容不作为唯一 canonical correctness evidence。

## 6. 进度

- 2026-07-26：pre-change same-command baseline=`5 passed / 299.34s`。Slow phases=
  illegal-decision call `97.79s`、CLI call `61.60s`、missing-input call `45.94s`、
  primary setup/call `38.49/38.90s`、recovery call `10.87s`。JUnit=
  `outputs/validation_runtime/arch004g2_paper_weekly_candidate/baseline.xml`，
  SHA-256=`22e482ec31fda6059ee4f05b8527c400ae4b8d96124f2d6b785301f861d73b18`，
  size=`949 bytes`。候选保留上限冻结为较慢 after `<=239.472s`；同时满足
  `<=269.34s` 的绝对 30 秒门槛。状态进入 `IN_PROGRESS_IMPLEMENTATION`，阈值不得事后放宽。
- 2026-07-27：首个尝试样本=`5 passed / 542.51s`，JUnit SHA-256=
  `92135429ff5ff51da75d7447dbd9d4f421f0f7d6da30491721709d0c12d35213`，但运行期间
  TRADING-2461 natural Full `.../7b88_full` 于 `2026-07-27 00:08:25 +09:00` 启动并占用
  16 workers。该样本固定为 `CONTAMINATED_EXTERNAL_FULL_NOT_ACCEPTANCE_SAMPLE`，不参与收益
  比较，也不被描述为候选回退。
- 2026-07-27：共享 Full 结束后取得两个无重型并发的有效 after。After-A=
  `5 passed / 268.69s`，相对 baseline 只改善 `30.65s / 10.24%`，JUnit SHA-256=
  `a4cea28c5ad9ea9f11a8f78e91b57c306eeb4446b95816728e65e201a0b8a61a`；它通过绝对
  30 秒门槛但未通过 20%。After-B=`5 passed / 372.41s`，相对 baseline 回退
  `73.07s / 24.41%`，JUnit SHA-256=
  `aa3ebf1175ebbf5e5c4e62ec7a20d7fd2bc91476a883939af6eaca37161a8ba6`。按较慢 after
  判定，`372.41s > 239.472s`，结论=`REJECTED_THRESHOLD_MISS`。
- 2026-07-27：module-scoped test implementation 已撤回。目标文件 current/base Git blob
  均为 `6092152071797758d7413cc3d19bd5ebeac4126b`，证明 byte-exact restore；production
  source 从未修改。结果说明跨 test 延长 session 既不能稳定跨越收益门槛，又可能积累昂贵的
  fingerprint/cache lookup 生命周期。不得重开同类 scope-lifetime 微优化；后续只接受能直接减少
  producer DAG 构建或形成有界 immutable authority 的新证据。
