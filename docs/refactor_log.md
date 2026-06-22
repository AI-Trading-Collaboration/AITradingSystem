# Refactor Log

## 2026-06-23 Daily Incremental Refactor

- 检查时间：2026-06-23 08:07 Asia/Tokyo。
- 起始 HEAD：`4ed42716c213eab63c64225d7b85e8ce72418067` (`TRADING-888-893 Add simple baseline candidate validation`)。
- 最近一次合格重构基线提交：`5fe628e5a26701d03e8bf3b4aa5b4d807511b80b` (`refactor: record AITradingSystem refactor log SHA`)。判定依据：提交信息明确标识 refactor，变更范围为重构记录维护，并更新专门的 `docs/refactor_log.md`；前序实现提交为 `428e8534`。
- 评估范围：`5fe628e5..HEAD` 的代码、配置、测试、文档和报告登记变更；重点检查 TRADING-734～893 后新增 data foundation、source qualification、controlled strategy、tail-risk governance 和 simple baseline command/report 契约。主要维护风险是 `src/ai_trading_system/cli_commands/research.py` 在上次拆分后重新增长到 8000 行以上，并集中承载 TRADING-865～893 simple baseline CLI wrapper。
- 本轮变更文件：
  - `docs/task_register_completed.md`
  - `docs/requirements/TRADING-894_Daily_Incremental_Refactor_Simple_Baseline_CLI_Boundary.md`
  - `docs/system_flow.md`
  - `docs/refactor_log.md`
  - `src/ai_trading_system/cli_commands/research.py`
  - `src/ai_trading_system/cli_commands/research_simple_baselines.py`
- 重构理由：simple baseline 底层研究实现已经在 `simple_baseline_portfolio_control.py` 和 `simple_baseline_candidate_validation.py`，但 20 个 `aits research strategies ...` wrapper 仍集中在 `research.py`。拆出 `research_simple_baselines` 可降低 research CLI 主模块继续膨胀的风险，并让 TRADING-865～893 命令族有清晰边界。
- 行为影响：预期无外部行为变化；20 个 simple baseline 命令仍在 `aits research strategies ...` 下注册，命令名、参数、artifact path、输出 safety fields、`typer.BadParameter` 转换和 FAIL 退出语义保持兼容。
- 数据/投资解释影响：无。该改动不接触 cached market/macro data、technical features、scoring、backtest、daily report、threshold、score band、promotion gate、position cap、data quality gate、official weights、paper-shadow state、broker 或 order path；本轮未生成新的 cached-data dependent output，因此未额外运行 `aits validate-data`。底层 simple baseline 数据依赖命令仍按既有实现调用同源 `validate_data_cache`。
- 验证命令与结果：
  - `python -m ai_trading_system.cli research strategies --help`：PASS，20 个 simple baseline command 仍在原路径下可见。
  - `python -m ai_trading_system.cli research strategies simple-baseline-registry-review --help`：PASS，参数 help 与默认 path 可见。
  - `python -m pytest -n 16 --dist loadfile tests/test_simple_baseline_portfolio_control.py tests/test_documentation_contract.py tests/test_task_register_consistency.py`：PASS，13 passed。
  - `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，193 passed；runtime artifact `outputs/validation_runtime/contract-validation_20260622T230834Z/test_runtime_summary.json`。
  - `python -m ruff check src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_simple_baselines.py tests\test_simple_baseline_portfolio_control.py`：PASS。
  - `python -m black --check src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_simple_baselines.py tests\test_simple_baseline_portfolio_control.py`：PASS。
  - `python -m compileall src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_simple_baselines.py`：PASS。
  - `git diff --check`：PASS。
- 遇到的 blocker：无。一次本轮自有机械迁移初稿因旧索引造成 `research.py` 局部错位；已在提交前恢复本轮自有改动并改用原文切片重做，随后 compile、Ruff、CLI help 和 pytest 均通过。
- 后续增量重构参考点：本轮完成后以最终 refactor log 回填提交 SHA 为下一次基线候选。后续可继续评估 `controlled_strategy_batch.py` 超大模块和 `research.py` 中 tail-risk / controlled strategy wrapper 边界，但不得在同一低风险切片中扩大范围。
- 本轮重构实现提交 SHA：`4523ad8c69eacb5c49a2799ed98dedcb605130aa`。

## 2026-06-21 Daily Incremental Refactor

- 检查时间：2026-06-21 10:25 Asia/Tokyo 开始；2026-06-21 10:40 Asia/Tokyo 继续。
- 起始 HEAD：`176141010ff329ab4361f581b623eb2aa9594e63`。
- 本轮先行整理提交：`2e342c6b` (`TRADING-703-733 add research governance data foundation baseline`)。
- 最近一次合格重构基线提交：未找到。`git log` 中存在 `5f17db76 refactor: add workflow and artifact contracts` 和 `87251a99 Archive structured refactor followup task`，但当前仓库没有既有 `docs/refactor_log.md` 可证明这些提交同步更新专门重构记录文档，因此本轮按无合格基线处理。
- 评估范围：当前仓库整体，重点检查最近新增 research governance / data foundation CLI、配置、schema、report registry、system flow 和 validation tier 变更。
- 本轮变更文件：
  - `docs/task_register.md`
  - `docs/requirements/TRADING-734_Daily_Incremental_Refactor_CLI_Boundary.md`
  - `docs/system_flow.md`
  - `docs/refactor_log.md`
  - `src/ai_trading_system/cli_commands/research.py`
  - `src/ai_trading_system/cli_commands/research_foundation.py`
- 重构理由：`src/ai_trading_system/cli_commands/research.py` 在 TRADING-703～733 后同时承载 campaign、indicator、governance、portfolio decision、ops 和 data-foundation research commands，模块边界继续膨胀。拆出 `research_foundation` 子模块可以降低后续 data-foundation CLI 维护成本，同时保持外部 `aits research ...` command surface 不变。
- 行为影响：预期无外部行为变化；`aits research labels|runs|execution|cases` 仍由 `aits research` 注册，命令参数、artifact 写入和退出语义保持兼容。
- 数据/投资解释影响：无。该改动不接触 cached market/macro data、technical features、scoring、backtest、daily report、threshold、score band、promotion gate、position cap、data quality gate、official weights、paper-shadow state、broker 或 order path。
- 验证命令与结果：
  - `python -m pytest -n 16 --dist loadfile tests/test_research_master_roadmap.py tests/test_data_foundation_roadmap.py`：PASS，10 passed。
  - `python -m compileall src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_foundation.py`：PASS。
  - `python -m ruff check src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_foundation.py tests\test_data_foundation_roadmap.py tests\test_research_master_roadmap.py`：PASS。
  - `python -m black --check src\ai_trading_system\cli_commands\research.py src\ai_trading_system\cli_commands\research_foundation.py tests\test_research_master_roadmap.py tests\test_data_foundation_roadmap.py`：PASS。
  - `git diff --check`：PASS。
  - `python scripts/run_validation_tier.py fast-unit --write-runtime-artifact`：PASS，84 passed；runtime artifact `outputs/validation_runtime/fast-unit_20260621T013807Z/test_runtime_summary.json`。
  - `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，83 passed；runtime artifact `outputs/validation_runtime/contract-validation_20260621T013854Z/test_runtime_summary.json`。
  - 归档 TRADING-734 后补充 `python -m pytest -n 16 --dist loadfile tests/test_documentation_contract.py tests/test_validation_tier_script.py tests/test_research_master_roadmap.py tests/test_data_foundation_roadmap.py`：PASS，21 passed。
  - 归档 TRADING-734 后补充 `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_documentation_contract.py`：PASS，10 passed。
  - 归档 TRADING-734 后补充 terminal-state grep：`docs/task_register.md` 中无 `DONE|BASELINE_DONE|DROPPED` 当前任务行。
  - 误执行记录：一次 `python -m black --check ... docs/requirements/TRADING-734_Daily_Incremental_Refactor_CLI_Boundary.md ...` 因 Markdown 不是 Python 文件而 ParseError；随后已用仅 Python 受影响文件的 Black check 通过。
- 遇到的 blocker：初次触发时工作区已有未提交 TRADING-703～733 改动；经 owner 后续授权，已验证并提交为 `2e342c6b` 后继续。
- 后续增量重构参考点：本轮完成后以最终 refactor commit SHA 为下一次基线候选。
- 本轮重构实现提交 SHA：`428e8534`。
