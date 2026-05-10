# DTWEXBGS freshness policy

## 背景

2026-05-09 每日 `aits ops daily-run` 已执行 `download_data`，并重新请求
FRED `DGS2`、`DGS10` 和 `DTWEXBGS`，但 `DTWEXBGS` 最新 observation 仍为
2026-05-01。`score-daily` 在数据质量门禁中触发 `rates_stale`，因为当前
FRED 宏观序列统一使用 7 个日历日 freshness 阈值。

这不是本地任务未下载，也不是 FRED API key 问题。`DTWEXBGS` 来自
Federal Reserve H.10 外汇统计发布，页面说明通常在周一 4:15 p.m. 发布前
一周的每日汇率和美元指数；周末、美国假日、时区和发布窗口会让最新值在
每日运行时自然超过 7 个日历日。

## 目标

- 保留 `aits validate-data` 作为缓存市场/宏观数据的强制质量门禁。
- 放宽 `DTWEXBGS` 的 freshness 规则，使其符合 H.10 周度发布机制。
- 不降低 `DGS2`、`DGS10` 等利率序列的 freshness 要求。
- 在配置、测试和系统流图中明确这是 series 级规则，不是临时绕过。
- 记录可选第二数据源和成本边界；owner 2026-05-10 已决定暂无新增 macro/price qualified source 计划。

## 设计决策

1. 在 `RateSeriesQualityOverrideConfig` 中新增
   `max_stale_calendar_days` override。
2. `rates.max_stale_calendar_days` 继续作为默认值，当前保持 7 天。
3. `DTWEXBGS` 配置为 14 个日历日。
   - 可覆盖 H.10 正常周度发布、周末、假日和亚洲时区运行窗口。
   - 仍会在连续两周左右缺失时 fail closed。
4. 数据质量报告继续使用 `rates_stale` 作为硬错误；只有超过该 series
   实际阈值才触发。

## 数据源成本和接入选项

| 选项 | 现金成本 | 工程成本 | 能否解决当前 freshness 问题 | 备注 |
|---|---:|---:|---|---|
| 继续 FRED `DTWEXBGS` | 0 | 低 | 部分；需 series 级阈值 | 当前实现。FRED 方便、可审计，但跟随 H.10 发布节奏。 |
| 直接接入 Federal Reserve H.10 / DDP | 0 | 低到中 | 可能只减少 FRED 同步延迟，不能改变 H.10 周度发布 | 可作为第二 primary source 或 freshness 诊断源。 |
| FRED/ALFRED vintage API | 0 | 中 | 不解决当日发布滞后 | 主要改善 PIT/vintage 审计，不改善最新值。 |
| 用 FX 现货和 Fed weights 自建 broad USD proxy | 低到中，取决于 FX provider | 中到高 | 可提高频率，但不等同于官方 `DTWEXBGS` | 需明确权重、节假日、缺失值和回测 PIT 口径。 |
| Nasdaq Data Link / 其他宏观数据平台 | 数据集可能免费或付费，premium 需逐项确认 | 中 | 取决于具体 dataset | API 使用本身可能不额外收费，但 premium feed 价格按数据集变化。 |
| Bloomberg / Refinitiv / FactSet / Macrobond | 高，通常 contact sales | 中 | 可能改善 SLA 和授权 | 除非系统整体进入生产级预算，否则不建议只为单个美元 proxy 采购。 |
| Alpha Vantage / 类似低价 FX API | 免费层或低价 premium | 中 | 只能辅助自建 FX proxy，不能直接替代 `DTWEXBGS` | 适合作为实验，不适合作为官方 broad USD index 替代。 |

2026-05-10 owner 决策：当前继续使用 FRED `DTWEXBGS`，不接入额外宏观第二来源或自建 FX proxy。上述选项保留为未来重新打开 `PROD-003` 时的评估材料；现阶段不得用临时数据源伪装为宏观双源 reconciliation。

## 验收标准

- `DTWEXBGS` 最新值距评估日 8 到 14 个日历日时不再触发 `rates_stale`。
- `DGS2` 或 `DGS10` 超过默认 7 个日历日仍触发 `rates_stale`。
- 配置加载测试确认 `DTWEXBGS.max_stale_calendar_days == 14`。
- `docs/system_flow.md` 明确数据质量门禁支持 FRED series 级 freshness。
- 不修改下载逻辑、不补写或伪造缺失日期。

## 状态记录

- 2026-05-09：新增并进入实现，原因：owner 要求先放宽
  `DTWEXBGS` 限制，并评估其他数据源接入成本。
- 2026-05-09：实现完成。新增 FRED rate series 级
  `max_stale_calendar_days` override，`DTWEXBGS` 配置为 14 天，DGS2/DGS10
  继续使用默认 7 天；`aits validate-data --as-of 2026-05-09` 结果为
  `PASS_WITH_WARNINGS`，错误数 0、警告数 11。验证通过
  `ruff check src tests`、`pytest -q tests/test_config.py tests/test_data_quality.py`
  、完整 `pytest -q`、`aits data-sources validate` 和 `git diff --check`。
- 2026-05-10：owner 确认继续使用现有 FMP + Marketstack + FRED，暂无引入第二个更可靠 macro/price qualified source 计划；`DTWEXBGS` freshness policy 继续作为 FRED 单源宏观输入的显式质量规则。
