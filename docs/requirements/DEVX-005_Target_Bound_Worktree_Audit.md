# DEVX-005：Target-Bound Worktree Audit

## 状态

- status：PROPOSED
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
