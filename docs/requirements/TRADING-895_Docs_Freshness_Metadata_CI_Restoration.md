# TRADING-895: Docs Freshness Metadata CI Restoration

最后更新：2026-06-23

关联任务：`TRADING-895_DOCS_FRESHNESS_METADATA_CI_RESTORATION`

## 背景

`main` 接收 simple baseline 分支后，GitHub CI 在 `Docs freshness` 步骤失败：

```text
python -m ai_trading_system.cli docs validate-freshness
```

本地复现显示失败原因是 39 个 `docs/requirements/*.md` 缺少
`最后更新：YYYY-MM-DD` 元数据。该问题会阻断 `main` 的 CI，但不涉及代码行为、数据流、
评分、回测、report output、cache schema、market-regime interpretation 或投资解释。

## 范围

- 补齐缺失 `最后更新：2026-06-23` 的 requirements 文档元数据。
- 保持文档正文、任务状态、验收标准和历史记录不变。
- 不更新 `docs/system_flow.md`，因为本次修复不影响 CLI、配置、cache schema、报告契约、
  数据质量门、评分、回测、market-regime interpretation 或主要模块边界。

## 安全边界

- `production_effect=none`
- 不写 production weights 或 active shadow weights。
- 不触发 broker、order、trading action。
- 不修改阈值、score band、promotion gate、backtest acceptance rule、仓位约束或数据质量门。
- 不创建 waiver，不降低文档新鲜度校验规则。

## 验收标准

- `python -m ai_trading_system.cli docs validate-freshness` 通过。
- `python -m pytest -n 16 --dist loadfile tests/test_docs_freshness.py tests/test_task_register_consistency.py tests/test_documentation_contract.py` 通过。
- `python -m ruff check src/ai_trading_system/docs_freshness.py src/ai_trading_system/cli_commands/docs.py tests/test_docs_freshness.py` 通过。
- `git diff --check` 通过。

## 进展记录

- 2026-06-23：任务创建并完成。补齐 39 个 requirements 文档的 `最后更新`
  元数据，保持文档内容和系统行为不变；Docs freshness 本地门禁恢复为 PASS。
