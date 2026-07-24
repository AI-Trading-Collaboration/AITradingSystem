# ENG-VAL-010 Validation Parent Run Portable Import

最后更新：2026-07-24

状态：`VALIDATING`

稳定任务 ID：`ENG-VAL-010_VALIDATION_PARENT_RUN_PORTABLE_IMPORT`

## 背景与真实 blocker

OPS-067 在 commit `00b41d5c58932bafbe52ec1382706c3e4d4df24a` 的首次 formal Full
得到 `7053 passed / 1 failed / 4 skipped / 1091.87s`。失败 summary 与 profile 分别为：

- `test_runtime_summary.json`：SHA-256
  `f49fe0127bfdc7145d06f4597f65029e2d458bcc95f45483999d8f7ed502e2e7`；
- `test_runtime_profile.json`：`13,231,643` bytes，SHA-256
  `80ac1117dd5eeb2bb24ecc90273b308aaec77a309fabccdd3db0702c9712114b`。

失败 Full 所在 validation clone 后来因测试隔离缺口 materialize 了 owner 明确保护的研究文档，
因此该 clone 不再用于新的 validation 或 source inspection。失败 run 的四份 runtime artifact 已
同字节复制到保护文件缺席的 exact-tree 候选 clone；但现有 `failure_fix_rerun` binder 要求
summary 内 `runtime_profile_path` 和 `output_artifacts[].path` resolve 到当前 run 目录的 fixed
sibling。旧 summary 的两个 locator 仍是生成 clone 的绝对路径，所以复制后的合法 bytes 被拒绝。

这不是 pytest correctness 失败，也不能通过以下方式绕过：

- 改写旧 summary 中的绝对路径并重新计算 SHA；
- 用 junction、symlink、路径替换或重建旧 clone 伪造原目录；
- 把第二次 Full 改写成 `broad_shared_contract_change`、natural boundary 或其他 trigger；
- 只引用 opaque run id、口头说明或 focused PASS 代替 parent binding。

## 目标

1. 建立显式、版本化、同字节的 `validation_parent_run_import.v1`。
2. 允许失败 formal Full artifact 在不同 clean worktree 中被严格导入并继续作为
   `failure_fix_rerun` parent，同时保留原 summary/profile SHA 与失败依据。
3. provenance 必须显示本次使用了 import proof，并绑定 proof 的 repository-relative path 与
   SHA-256；不能把 relocation 静默解释成 direct binding。
4. 后续默认 managed validation runtime artifact 使用 repository-relative locator，避免新的
   Full 再依赖生成 worktree 的绝对路径。
5. 保持 legacy same-worktree direct binding、pytest exit authority、runtime profile formal
   semantics 和所有 fail-closed contract 不变。

## 设计

### S0：import proof builder

新增 canonical builder，输入当前仓库 `outputs/validation_runtime/<run_id>/` 下同字节复制的
`test_runtime_summary.json`，只读取它和 fixed sibling `test_runtime_profile.json`，不访问旧
source clone。

builder 使用 strict JSON loader，拒绝 duplicate key、non-finite constant、非法 UTF-8、非 mapping
root；随后生成 fixed sibling `validation_parent_run_import.json`，schema 为
`validation_parent_run_import.v1`，至少绑定：

- `run_id` 与 source `git_commit`；
- source summary/profile/inventory 的原始绝对 locator；
- current imported summary/profile 的 repository-relative fixed-sibling locator；
- summary/profile SHA-256 与 size；
- `status=PASS`、`production_effect=none`。

source locator 必须共享同一 source repository prefix，并精确以
`outputs/validation_runtime/<run_id>/<canonical filename>` 结尾；不得包含 null、`.`、`..`、
wrong run id、wrong suffix 或 drive-relative/非绝对形状。builder 不重写 source artifact bytes。

### S1：runner import validation

`run_validation_tier.py` 为 `failure_fix_rerun` 增加 `--parent-run-import`，并提供对应环境变量。
它属于 whole-envelope 的 parent binding 输入：只在 `failure_fix_rerun` 且同时提供
`--parent-run` 时合法。

import validator 在 pytest 启动前：

1. 要求 manifest 为 parent run 目录下的 fixed sibling，且 resolve 后仍在当前
   `outputs/validation_runtime`；
2. strict 解析并要求 exact schema/keys/types/status/safety boundary；
3. 重新读取同一份 copied summary/profile bytes，验证 proof 的 SHA/size；
4. 验证 proof 的 source locators 与 source summary 内三个 locator 精确一致；
5. 验证 imported locators 精确映射到当前 repository-relative summary/profile fixed siblings；
6. 验证 run id、source commit、summary schema、Full status/exit/provenance、profile formal
   semantics、inventory SHA/size 与 failure basis；
7. 在 child `parent_run` binding 中追加 exact import variant：
   `locator_mode=portable_import_v1`、repository-relative `import_manifest_path` 与
   `import_manifest_sha256`。

`validation_trigger_provenance.v1` 保留 direct legacy parent 的既有 exact key set，同时显式允许
上述 import variant；其他额外字段继续拒绝。

### S2：future managed locator portability

当 validation runtime artifact 位于当前 repository root 下时，summary 的 managed
`artifact_dir`、`summary_path`、`reader_brief_path`、`runtime_profile_path` 与
`output_artifacts[].path` 使用 repository-relative POSIX locator。显式选择 repository 外
`--artifact-dir` 时仍可保留绝对 locator，但不得形成可跨仓库误解释的 import proof。

已有绝对路径 artifact 保持 immutable，由 S0/S1 proof 兼容；不批量重写历史证据。
默认 `outputs/validation_runtime/` 同时是 tracked `.gitignore` 中的 developer-runtime
边界，使 clean clone 中的 parent、print-only 与 formal gate 产物不会被 architecture
worktree guard 误判为业务或共享路径脏写；其他 untracked/modified 路径仍由 guard fail closed。

## 验收标准

- exact-byte copied OPS-067 parent 可通过 print-only provenance preflight，binding 仍报告原
  summary/profile SHA 与 `failure_basis=PYTEST_FAIL`，并额外绑定 import manifest SHA；
- source summary/profile、import proof 任一 byte tamper、hash/size/commit/run-id/path drift 均
  fail closed；
- duplicate/non-finite JSON、traversal、wrong suffix、source-prefix mismatch、manifest
  symlink/outside-root、imported sibling escape、重复或缺失 inventory mapping均 fail closed；
- 未提供 import proof 的 relocated parent 继续失败；同 worktree legacy direct parent继续 PASS；
- 新 default runtime artifact 的 managed locators 为 repository-relative，旧 consumer 与
  refresh tooling contract测试 PASS；
- tracked `.gitignore` 只排除 `outputs/validation_runtime/` developer evidence root，
  Wave14 worktree guard 仍拒绝该 root 外的任何 unexpected dirty path；
- focused、static、architecture-fitness、contract-validation、integration、reproducibility
  与 OPS-067 formal Full PASS；
- `strategy_logic_changed=false`、`cached_data_mutated=false`、`production_effect=none`，
  不写 production/active-shadow weights，不触发 broker/order/trading。

## 步骤与依赖

1. `COMPLETE`：冻结 manifest schema、task register 与 system-flow 边界。
2. `COMPLETE`：并行实现 builder/strict validator 与 runner/provenance integration，文件 ownership
   保持分离。
3. `COMPLETE`：focused/static、generated authority 与
   architecture/contract/integration/reproducibility 已在 exact candidate PASS。
4. `COMPLETE`：以 `failure_fix_rerun` + parent summary + import proof 完成第二次 exact-tree Full。
5. `IN_PROGRESS`：刷新并验证 compatibility/deprecation/source hashes，执行 post-Full gates；
   由 OPS-067 coordinator 提交、push 后进入 canonical daily acceptance。

## 安全边界

- 任务只处理 developer validation evidence，`production_effect=none`。
- 不访问或重新 materialize owner 保护的研究文档。
- 不修改旧失败 Full bytes，不访问其已废弃 source clone 的其他内容。
- import proof 不是新的测试结果，只是对同一份旧失败 evidence 的可移植 locator 映射。
- 任何无法验证的历史 locator 或 bytes 必须作为真实 blocker 报告，不得自动放宽。

## 进展

- 2026-07-24：builder/strict validator、runner `--parent-run-import`、provenance direct/import
  双形态、future repository-relative managed locator、文档与 tests 已集成。combined focused
  `148 passed / 14.77s`；Ruff、Black、strict mypy（新 module、provenance module、builder）
  均 PASS。
- 2026-07-24：使用真实 OPS-067 copied artifact 生成 proof 后，原 summary
  SHA-256=`f49fe0127bfdc7145d06f4597f65029e2d458bcc95f45483999d8f7ed502e2e7`
  / `26,687` bytes、profile
  SHA-256=`80ac1117dd5eeb2bb24ecc90273b308aaec77a309fabccdd3db0702c9712114b`
  / `13,231,643` bytes 均保持不变；manifest
  SHA-256=`c7ad6fdc5ac90b1b5b498b40145bc6fa3294e53736045de85e55fe7c950a0af4`
  / `1,165` bytes。parent preflight=`PASS`，绑定
  `failure_basis=PYTEST_FAIL`、`locator_mode=portable_import_v1`，未读取 source locator
  指向的废弃 clone。状态转 `VALIDATING`，仍须完成 authority refresh、formal gates 与
  OPS-067 Full。
- 2026-07-24：首个 exact candidate `f0aaa851ce1d4129a57d9c56a32fc2a7830701dd`
  / tree `84ddb7be0114fa27f20f61e387dd1cbb2c09f467` 的 combined focused
  `178 passed / 134.45s`，print-only portable parent provenance `PASS`。随后
  architecture-fitness 为 `574 passed / 2 failed / 94.02s`：一项真实暴露
  `docs/task_register.md` 新行尚未刷新 ARCH-005 task-registry baseline/shadow，另一项是
  validation clone 使用 detached HEAD 而无法满足 Wave14 branch evidence。两项均按原设计直接
  修复：刷新 generated task authority，并让新 exact clone 保持 `main` 分支；不删除失败证据、
  不放宽 gate、不以重跑覆盖。
- 2026-07-24：第二个 exact candidate `fcffae49f5496e5dd06919e8cf1e00cf6f2c299c`
  / tree `c7b40188ea4f6691d6864c2f94caba2bbf77d542` 的 combined focused
  `185 passed / 134.29s`，task authority 与 branch 问题均已关闭；architecture-fitness
  进一步得到 `575 passed / 1 failed / 92.90s`，唯一失败为 clean clone 未继承主工作区
  `.git/info/exclude`，导致合法 `outputs/validation_runtime/` parent/print-only 证据被
  Wave14 guard 视为 unexpected dirty path。由于 Full 也包含该 guard，不能靠重排命令规避；
  直接把 developer validation runtime root 固化进 tracked `.gitignore` 并增加 focused
  contract，其他路径的 guard 语义不变。
- 2026-07-24：最终 Full-tested candidate=`d92eae358cc4b0819d1fa1205e0d3dad8104b57a`
  / tree=`0e3bca65e5d98d96b4ec892f58936e8680f3a034`。combined focused=`186 passed`；
  architecture/contract/integration/reproducibility=`577/274/993/23 passed`；parent-bound
  Full=`7115 passed / 4 skipped / 643 warnings / 1176.47s`。summary
  SHA-256=`33cb2f5fe64d49be1cb8df7934bbf049185e7a70dcb227a826af07d77d974435`
  / `26,749` bytes，profile
  SHA-256=`6e8c076ccaffeca5ce9714ffa80e972b460b39db69c20d5488bd17e64c8c5b87`
  / `13,345,435` bytes；profile/telemetry/performance/provenance binding、scheduler
  applied/no-fallback/order 与 formal selection 全部 PASS。一次执行通道中断未产生新的 formal
  artifact，未计作 Full attempt 或 pytest 结果。当前只剩 evidence-only post-Full gates、提交和
  push；duration seed 保持已验证的 `PARTIAL_SEED v24`，不在 Full 后改动 Full-sensitive 输入。
