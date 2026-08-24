# DEVX-010 Full parent publication binding 规范化与回归验证

## 状态

- 当前状态：`VALIDATING`
- 下一责任方：`integration-coordinator`
- 来源：DEVX-009 v5 在 Full 实际派发前触发 `PUBLICATION_FULL_PARENT_MISMATCH: missing-side`
- 生产影响：`none`
- Broker 行为：`none`

## 问题定义

`run_validation_tier.py` 会先把 `--parent-run` 校验并规范化为结构化
`parent_run` binding。Full publication 适配层随后只在该值仍为字符串时构造
`Path`，导致合法的结构化 binding 被转换为 `None`，与事务中已冻结的
`full_parent` 文件 binding 比较时错误地 fail closed。

同一比较规则也会使不需要消费父运行的事务校验入口在事务包含
`full_parent` 时被错误阻断。修复必须保持 exact-byte 文件绑定与 fail-closed
语义，不能放宽父运行的 hash、size、路径或状态校验。

## 实施步骤

1. 在 validation runner 的 publication 适配层，从已规范化 binding 中恢复并校验
   `summary_path`，再把它传给 publication fence。
2. 为结构化 binding 的成功路径和缺失/不一致路径补充回归测试，确认测试进程在
   围栏失败时仍不会被派发。
3. 更新 DEVX-010 canonical task 状态，重建受影响的 generated authority 与 Atlas
   exact-commit 输出。
4. 在最终候选上重新执行 Architecture、Contract、Integration、Reproducibility，
   并以 v3 失败 Full 作为 `failure_fix_rerun` parent 完成唯一 Full。
5. 通过事务 CAS 后 fast-forward 本地 `main`、普通推送并清理任务分支。

## 验收标准

- 合法的 `failure_fix_rerun` parent summary 经规范化后仍与事务
  `full_parent` 的 path、SHA-256、size 完全一致。
- 非法、缺失、被篡改或不一致的 parent 继续在 pytest 派发前 fail closed。
- Full runtime artifact 记录 parent、task、boundary、transaction 和最终 PASS。
- 四个前置正式层级与 Full 均在同一最终候选提交上通过。
- local `main`、remote `main` 与最终 candidate SHA 一致，且受保护的已知无关文件
  未被读取、hash、diff、stage 或修改。

## 进度记录

- 2026-08-24：登记任务；确认 v5 拒绝发生在 pytest 派发前，四个前置层级已
  分别通过 876、277、995、24 个测试，但这些结果不替代修复后最终候选重跑。
- 2026-08-24：完成结构化 `parent_run.summary_path` 适配和非 Full 延迟消费规则；
  focused regression `83 passed`，Ruff PASS，SINGLE_LANE LANE/INTEGRATION
  preflight PASS。进入绑定真实失败父运行的最终候选验证。
