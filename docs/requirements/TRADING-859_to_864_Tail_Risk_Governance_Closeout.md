# TRADING-859～864 Tail-Risk Governance Closeout

状态：VALIDATING / OWNER_REVIEW_REQUIRED
最后更新：2026-06-22

## 背景

TRADING-843～858 已完成 tail-risk fallback governance follow-up 的实现与验证，但当前工作树包含本轮开始前已经存在的未提交改动。收口前必须先区分归因，避免把旧改动混入提交，也避免继续扩展新的 governance report 使工作树归因更难。

所有新增 closeout 输出继续固定：

- `production_effect=none`
- `broker_action=none`
- 不修改 promotion、paper-shadow、production weight 或 broker/order
- 不自动提交无法可靠归因的 mixed worktree 文件

## 阶段拆解

- TRADING-859：生成 worktree change attribution，区分本轮 843～858、新增 859～864、pre-existing 和 mixed risk，并给出 safe commit 状态。
- TRADING-860：基于 TRADING-859 结果安全提交或生成人工 handoff；当状态为 `OWNER_REVIEW_REQUIRED` / `BLOCKED_BY_MIXED_WORKTREE` 时不得提交。
- TRADING-861：真实运行 TRADING-843～858 follow-up CLI，汇总真实 status、warnings、blockers、sample_count、baseline dominance 和 readiness。
- TRADING-862：生成 promotion hard-block end-to-end proof，覆盖 TRADING-827/828/829/830、baseline dominated 和 insufficient sample 场景。
- TRADING-863：审查 Reader Brief tail-risk safety 摘要是否安全，不得暗示 active strategy、paper-shadow candidate 或可执行建议。
- TRADING-864：生成 owner review pack，回答实现完整性、未提交文件、mixed worktree risk、真实 CLI run、blocked/readiness/baseline dominance/trigger v2 和 owner 需要批准的事项。

## 验收标准

- `outputs/research_strategies/value_surface_review/tail_risk_followup_change_attribution.json/md` 存在，并包含 `git status --short`、`git diff --name-status`、`git diff --stat` 和逐文件归因表。
- TRADING-860 在 mixed worktree 下不得提交；必须生成人工 review 清单和 patch handoff。
- TRADING-861 真实运行新增 CLI，不只依赖 pytest，并输出 JSON/Markdown summary。
- TRADING-862 所有 hard-block 场景最终 safety fields 必须保持 false/none。
- TRADING-863 Reader Brief 摘要必须显示 promotion/production/broker 禁止状态，且不污染日报主体。
- TRADING-864 owner pack 必须明确当前 fallback 仍 blocked、readiness score、baseline dominated、trigger v2 建议和 owner approval 项。
- 验证使用并行 pytest；工程检查至少包含 Ruff、compileall 和 `git diff --check`。

## 进展记录

- 2026-06-22：新增本需求文档，任务登记进入 IN_PROGRESS；开始执行 closeout 顺序 859 → 860 → 861 → 862 → 863 → 864。
- 2026-06-22：完成 859～864 收口产物。859 输出 `BLOCKED_BY_MIXED_WORKTREE`；860 未提交并生成 handoff patch；861 真实 CLI run 输出 `REAL_RUN_PASS`；862 输出 `HARD_BLOCK_E2E_PASS`；863 输出 `DAILY_BRIEF_SAFE`；864 输出 `OWNER_REVIEW_PACK_READY`。当前 owner 仍需人工 review mixed/pre-existing worktree hunks 后再决定是否提交。
- 2026-06-22：验证通过：`python -m pytest -n 16 --dist loadfile tests/test_tail_risk_independent_validation_governance.py tests/test_task_register_consistency.py tests/trading_engine/test_reader_brief.py` 15 passed；Ruff、compileall 和 `git diff --check` 均通过。
