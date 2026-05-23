# TRADING-027: Daily Operator Brief Scheduler Runbook and Manual Installation Guide

关联任务：`TRADING-027`

状态：`DONE`

## 目标

新增 Daily Trading System Operator Brief scheduler 的人工安装指南和 checklist，覆盖 Windows Task Scheduler、cron、GitHub Actions optional setup、dry-run 决策处理、依赖 artifact 缺失处理、运行后人工验证、暂停/关闭和失败排查。

TRADING-027 只生成文档，不创建真实 scheduler，不运行 operator brief，也不运行任何上游 pipeline。

## Scope

- 新增 `docs/runbooks/daily_operator_brief_scheduler_manual_installation.md`。
- 新增本文需求文档。
- 更新 `docs/task_register.md`，记录 TRADING-027 状态、验收标准和安全边界。
- 更新 `docs/system_flow.md`，把 TRADING-027 标为 TRADING-026 之后的 documentation-only 安装指南。
- 更新 `docs/artifact_catalog.md`，登记 TRADING-027 documentation artifacts。

## Non-goals

1. 不创建真实 scheduler。
2. 不提交 GitHub Actions workflow。
3. 不写 Windows Task Scheduler XML。
4. 不写 cron file。
5. 不运行 operator brief。
6. 不运行 dry-run。
7. 不触发任何 pipeline。
8. 不运行 `TRADING-021`。
9. 不运行 `TRADING-022`。
10. 不运行 `TRADING-023`。
11. 不运行 `TRADING-024`。
12. 不运行 `TRADING-026`。
13. 不下载或刷新数据。
14. 不修改 production profile。
15. 不修改 shadow weights。
16. 不执行 promotion、apply 或 rollback。
17. 不触发 broker、replay runner 或 trading execution。

## Safety Boundaries

所有 TRADING-027 文档必须明确以下固定边界：

```json
{
  "production_effect": "none",
  "manual_review_only": true,
  "runbook_only": true,
  "scheduler_created": false,
  "operator_brief_executed_by_runbook": false,
  "pipelines_executed_by_runbook": false,
  "data_downloaded_by_runbook": false,
  "apply_executed_by_runbook": false,
  "rollback_executed_by_runbook": false,
  "broker_execution": false,
  "replay_execution": false,
  "trading_execution": false
}
```

`production_effect=none` 在本任务中只表示文档变更没有生产运行影响，不授权未来 scheduler 修改任何生产配置或触发交易相关执行。

## Files Added / Updated

新增：

- `docs/runbooks/daily_operator_brief_scheduler_manual_installation.md`
- `docs/requirements/daily_operator_brief_scheduler_manual_installation_2026-05-24.md`

更新：

- `docs/task_register.md`
- `docs/system_flow.md`
- `docs/artifact_catalog.md`

不新增：

- `.github/workflows/*.yml`
- Windows Task Scheduler XML
- cron file
- scheduler script
- runtime data artifact
- 018B-026 smoke artifact

## Acceptance Criteria

- Runbook 明确写出不会自动创建 scheduler。
- Runbook 明确禁止 apply / rollback / broker / replay / trading。
- Windows Task Scheduler 步骤完整，包含 Python 路径、arguments、Start in、日志和 `%DATE%` 风险说明。
- cron 示例完整，并明确只是人工示例。
- GitHub Actions 示例明确标记为 documentation only，不提交 workflow。
- Pre-installation checklist 包含连续 3 日 `READY` 或已复核 `READY_WITH_WARNINGS` 的要求。
- Safety checklist 覆盖 TRADING-018E2 apply、TRADING-018E3 rollback、broker、replay、trading、output directory 和 log separation。
- Dry-run decision 解释覆盖 `READY`、`READY_WITH_WARNINGS`、`NOT_READY`、`SAFETY_BLOCKED`、`ERROR`。
- Manual validation checklist 覆盖 operator brief JSON/Markdown、`brief_status`、broker/replay/trading/apply/rollback safety flags 和 dashboard 只读读取。
- Failure handling 覆盖 Python not found、wrong working directory、missing repository path、missing 021/023/024 artifacts、operator brief output missing、`SAFETY_BLOCKED`、permission denied、stale artifacts 和 git working tree pollution。
- Disable / pause scheduler 步骤覆盖 Windows Task Scheduler、cron 和 GitHub Actions。
- `docs/system_flow.md` 明确 TRADING-027 是 documentation-only，不创建 scheduler。
- `docs/artifact_catalog.md` 明确 TRADING-027 只产出 documentation artifacts，没有 runtime data artifact。
- `git status --short --untracked-files=no` 中本次 tracked diff 只包含 TRADING-027 文档相关文件。

## Validation Commands

按需求执行：

```bash
python -m pytest tests/test_daily_task_dashboard.py -q
python -m pytest tests/trading_engine -q
python -m pytest -q
python -m ruff check scripts src tests
python -m black --check scripts src tests
```

如果 `black` 仍被既有无关 baseline 阻断，例如 `tests/test_market_data.py`，记录原因，不混入无关格式化 diff。

提交前还要检查：

```bash
rg "^\|[^|]+\|[^|]+\|P[0-3]\|(DONE|BASELINE_DONE|DROPPED)\|" docs/task_register.md
git status --short --untracked-files=no
```

## Future Tasks

可考虑：

```text
TRADING-028: Scheduler Configuration Template Generator
```

TRADING-028 可以生成可审阅模板，例如 Windows Task Scheduler XML template、cron line template 和 GitHub Actions workflow template，但不应自动安装 scheduler。

## Progress Notes

- 2026-05-24：新增并进入 `IN_PROGRESS`。Owner 要求在 TRADING-026 scheduler dry-run 之后补齐人工 scheduler 安装 runbook；本任务严格限定为文档 only，不创建 scheduler、不运行 TRADING-021/022/023/024/026、不触发任何 pipeline 或交易执行。
- 2026-05-24：实现完成并进入 `VALIDATING`。已新增 manual installation runbook 和需求文档，并更新 task register、system flow、artifact catalog；验证通过 `python -m pytest tests/test_daily_task_dashboard.py -q`、`python -m pytest tests/trading_engine -q`、`python -m pytest -q` 和 `python -m ruff check scripts src tests`。`python -m black --check scripts src tests` 仍仅被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。本任务未创建 scheduler、未提交 GitHub Actions workflow、未写 cron / Windows Task Scheduler XML、未运行 TRADING-021/022/023/024/026 或任何 market/backtest/scoring/data download/broker/replay/trading pipeline。
- 2026-05-24：最终收尾验证完成并改为 `DONE`。人工检查确认 runbook 包含 Windows Task Scheduler 手动安装步骤、cron 手动安装步骤、GitHub Actions 可选示例、pre-installation checklist、safety checklist、dry-run decision 处理说明、disable / pause scheduler、failure handling，并明确说明 TRADING-027 不创建 scheduler。仓库检查确认本任务未新增 Windows Task Scheduler XML、cron 文件、GitHub Actions workflow 或 scheduler 自动安装脚本；现有 `.github/workflows/ci.yml` 与既有 scheduler dry-run 脚本不是 TRADING-027 新增项。文档明确禁止 scheduler 运行 TRADING-018E2 apply、TRADING-018E3 rollback、broker execution、replay runner、trading execution、data download、market/backtest/scoring pipeline，除非未来任务明确审查。最终验证通过目标 pytest、`tests/trading_engine`、全量 pytest 和 ruff；Black check 仍只被既有无关 `tests/test_market_data.py` baseline 阻断，未混入无关格式化 diff。
