# DEVX-021 本地发布 ff-merge 钩子对「ORIG_HEAD 已等于旧 HEAD」的健壮性 V1

- 任务：`DEVX-021_PUBLICATION_ORIG_HEAD_UNCHANGED_ROBUSTNESS`
- 优先级：P1；状态：PROPOSED；next owner：Claude Code coordinator
- 来源：2026-10-04 v25 正式 Full 通过后，`local-publish` 在真实主检出上被钩子校验拒绝（GOV-007 P1-C 基线发布，DEVX-018 v25）。
- 影响范围：`src/ai_trading_system/platform/architecture/workflow_coordination.py`（`_validate_publication_git_merge`、`record_publication_hook`）与
  `workflow_integration.py`（合并窗口检查与 ORIG_HEAD 效果分类）。不涉及投资逻辑、数据、回测；`production_effect=none`，`broker_action=none`。

## 1. 现象与证据
- v25 Full：14,706 通过 / 4 跳过 / 0 失败，墙钟 12,500 秒（3 小时 28 分）；`LOCAL_MAIN_FF_PRE` 通过。
- `local-publish`（约 18 分钟后）返回 `RECOVERY_REQUIRED`，worker 阶段：`hooks_created → ready_held → heads_switched → merge_resumed → merge_exit 128`
  （`fatal: ref updates aborted by hook`）；钩子返回 `LEASE_EXECUTION_PUBLICATION_GIT_MERGE_LOCK: execution lifecycle rejected`。
  原事务库只留下一条 `reference-transaction aborted ORIG_HEAD`（`prepared_file=None`）——第一次 `prepared` 被拒绝，所以没有被记录。
  `local-publication-recover` 得到 `STABLE_FAILED_ATTEMPT / ORIGINAL_UNCHANGED`，HEAD 恢复到任务分支，main/origin/main 仍为 `cbc31cdff`；事务按失败释放。
- 证据文件：`outputs/validation_runtime/gov-007-p1c-devx015-full-20261004-v25/publication-*.{stdout,result.json}`、
  `outputs/architecture/integration_revalidation/devx015-v389/claude_v25_local_publish.{json,err}`、`claude_v25_local_publication_recover.json`。

## 2. 根因（已在真实布局副本上复现）
- `_validate_publication_git_merge` 要求 `reference-transaction prepared ORIG_HEAD` 时 `ORIG_HEAD.lock` 的内容恰为 `expected_main_sha + "\n"`。
- 真实主检出里 `.git/ORIG_HEAD` 早已等于当前 main（`cbc31cdff`，2026-09-26 的历史操作遗留）。此时 git 的 `files` 后端发现要写入的值与现有值相同，
  **只创建空的 `ORIG_HEAD.lock`，不写入内容**（size 0），于是校验以 `PUBLICATION_GIT_MERGE_LOCK` 拒绝，git 中止 ref 更新。
- 复现：`D:/Work/devx018-realclone`（真实 `.git` 的整份副本，含 packed-refs/ORIG_HEAD 布局）里执行同一个 `git merge --ff-only`，钩子日志：
  - ORIG_HEAD 预先存在且等于旧 main：`[prepared] ORIG_HEAD.lock size=0`；
  - 不存在 ORIG_HEAD：`[prepared] ORIG_HEAD.lock size=41 content=cbc31cdff…`；
  其余序列（`refs/heads/main.lock` 内容 = 候选、`HEAD.lock`、`post-merge`、`AUTO_MERGE` 的 aborted/prepared/committed 与空的 `packed-refs.lock`）两种情形完全一致，
  与校验器期望相符。
- 为什么 fixture 演练没发现：所有 fixture 都是新建仓库，没有预先存在的 ORIG_HEAD；该缺陷只在「ORIG_HEAD == 旧 main」的真实仓库状态下出现。

## 3. 最佳方案（本任务）与为何不在 v26 里做
- 最佳方案：以计划里已记录的基线 `plan["orig_head"]`（发布前 `ORIG_HEAD` 的元数据与内容）为准——
  当基线内容已等于 `expected_main_sha + "\n"` 时，`prepared` 阶段合法的锁内容为空（size 0）且最终 `ORIG_HEAD` 文件必须保持不变；其余情形保持现有严格检查。
  同步修改 `record_publication_hook` 的锁元数据读取与 `workflow_integration.py` 里合并窗口/效果分类中对 ORIG_HEAD 的「permitted」推导
  （约 2964、3058–3067、4154–4170 行），并补测试：预先存在且等于旧 main、预先存在但不同、不存在 三种基线；加 real-layout 副本演练。
- 阻塞与取舍：这是发布路径的语义改动（锁、效果分类、恢复路径），任何改动都产生新候选并需要一次完整正式 Full（约 3.5 小时），且效果分类的多处联动没有真实仓库演练就容易再出一个只在真实状态下出现的缺陷。
  因此 v26 不改发布代码。

## 4. 临时前置条件（已接受的变通，须 owner 复核）
- 内容：`local-publish` 之前删除主检出 `.git/ORIG_HEAD`（git 的临时便利指针；其内容 `cbc31cdffcfb8cda1cf106f6da2a302f87183255` 已记录在此，且就是当前 main/origin/main），
  使仓库状态与所有已演练的 fixture 一致（计划会记录「ORIG_HEAD 不存在」的基线）。
- 原因：见第 2 节；不删除则必然被拒绝。
- 行为影响：对已跟踪内容、历史、分支、远端无影响；`ORIG_HEAD` 会在合并时被 git 重新写为旧 main。可逆。
- 风险：低。仅在 `local-publish` 前一次性执行，不改任何校验或证据语义；之后的合并/校验仍走完整的原始路径。
- 验证覆盖：真实布局副本上的钩子序列演练（见第 2 节）；v26 的真实 `local-publish`。
- 退出条件：本任务完成并通过「预先存在的 ORIG_HEAD」演练后，不再需要该前置条件。
- 状态：等待 owner 确认（v26 正式 Full 期间提出）。

## 5. 验收标准
1. 校验器与效果分类对三种 ORIG_HEAD 基线（不存在、等于旧 main、不同于旧 main）都按语义正确放行/拒绝，并有负向测试（锁内容被篡改、基线与锁不一致必须拒绝）。
2. fixture 演练覆盖「预先存在且等于旧 main」，真实布局副本演练通过。
3. 不放宽任何既有检查；恢复路径（`local-publication-recover`、`adopt-published`）对新情形仍可重放。
4. 更新 `docs/system_flow.md`（若发布路径的文字说明受影响）、任务登记与本文档。

## 6. 进展记录
- 2026-10-04：登记；根因复现完成；前置条件待 owner 确认；实施排在基线发布之后（P1）。
