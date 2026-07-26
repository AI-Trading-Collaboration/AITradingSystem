# DEVX-005：Target-Bound Worktree Audit

## 状态

- status：BASELINE_DONE
- priority：P0
- owner：developer workflow / checkout guard owner
- last updated：2026-07-27

## 问题

`scripts/architecture_arch005_checkout_guard.py` 当前以脚本文件位置计算固定
`PROJECT_ROOT`。从另一个 worktree 的当前目录调用主工作区脚本，仍会审计主工作区。
DEVX-001 的 2026-07-27 清理因此错误地把主 checkout PASS 当成目标 worktree PASS；
目标 tracked unstaged/staged diff 在删除前未被正确证明。

## 目标设计

1. `worktree-audit` 增加显式、规范化的 target repository 参数。
2. 输出必须同时披露 policy repository、audited repository、toplevel、git common dir 和
   worktree registration identity，禁止调用方误读。
3. target 必须是当前 policy repository 同一 Git common dir 下的已登记 worktree；
   路径不存在、不是toplevel、不是registered worktree或指向policy repo之外的独立clone时
   fail closed。
4. known-unrelated exclusions 至少使用当前 reviewed policy repository 的完整 exact literal
   集合；不得因目标 worktree 的旧配置遗漏当前 exclusion。
5. dirty inventory、unstaged diff check 和 staged diff check 三条 Git 调用必须全部绑定同一
   target，且全部注入相同 exclusion。

## 实施步骤

1. 在核心 guard 中建立 policy repository / audited repository 双身份与 registered
   worktree binding，审计前后均复核 identity；
2. CLI 增加显式 `--target-repository`，默认仍审计 policy checkout，并保留
   `--project-root` 兼容别名；
3. 为真实临时 worktree、独立 clone、非 toplevel、未登记 target 和 identity drift 增加
   fail-closed 测试；
4. 更新 compatibility authority、task shadow 和用户可见流程文档，执行 focused、
   Architecture 与 Contract 验证；
5. 进入 reviewed main 并推送后，才允许 DEVX-001 重新评估后续 worktree 清理。

## 进展

- 2026-07-27：转为 `IN_PROGRESS`；START preflight 通过，开发分支为
  `codex/devx005-target-bound-worktree-audit`。本阶段只修复审计能力，不执行 worktree
  删除。
- 2026-07-27：实现`checkout_worktree_audit.v2`双身份、registered-worktree binding、
  audit前后identity drift检查与`--target-repository`入口；focused guard=`20 passed`，
  guard + documentation=`25 passed`，Ruff/Black通过。真实只读审计
  `D:\Work\AITradingSystem_t2462_tailrisk_v3`为PASS，输出明确区分policy checkout与target
  checkout并证明same Git common dir及registration；未执行worktree删除。任务转为
  `VALIDATING`，等待Architecture、Contract与收口流程。
- 2026-07-27：首次正式Architecture为`684 passed / 8 failed`，artifact=
  `outputs/validation_runtime/architecture-fitness_20260726T172749Z/test_runtime_summary.json`。
  失败原因是新增公共API后generated module/test manifest未完成最终重生成，以及7个历史
  compatibility authority测试尚未纳入DEVX-005 supersession集合；核心guard行为测试无失败。
  直接补齐manifest与append-only authority链后，Architecture=`692 passed`，artifact=
  `outputs/validation_runtime/architecture-fitness_20260726T173348Z/test_runtime_summary.json`；
  Contract=`275 passed`，artifact=
  `outputs/validation_runtime/contract-validation_20260726T173546Z/test_runtime_summary.json`。
  未降低或绕过验证门禁。任务转为`BASELINE_DONE`；进入reviewed main后只解除DEVX-001的工具
  前置阻塞，不自动授权任何worktree删除。

## 验收

- 真实临时测试 worktree 中制造的 tracked unstaged、staged 和 untracked 状态只出现在
  target audit，不污染或误报 policy checkout；
- 默认无参数行为继续审计当前 policy checkout；
- wrong path、independent repo、unregistered directory和target/policy identity drift均
  typed fail closed；
- known-unrelated内容不被打开、hash、复制、暂存或修改；
- focused、Architecture、Contract与兼容性 authority通过；
- 不改变数据、策略、报告、production或broker语义。

## 安全边界

本任务修复开发治理工具，不授权继续删除OPS-070或TRADING-2462 worktree。只有本任务进入
reviewed main且真实target-bound audit通过后，DEVX-001才可重新评估后续删除。
