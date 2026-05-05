# 价格源不一致归因与质量报告标注

## 背景

2026-05-05 切换 FMP 主价格源后，真实缓存的质量门禁仍出现价格类错误和
Marketstack 跨源差异。复核发现需要先区分三类问题：

- 主价格源自身数据错误；
- 第二行情源 Marketstack 自身 schema / OHLC / adjusted close 异常；
- 主源与第二源之间的 ticker alias、share class 或复权口径差异。

当前最明确的问题是 `GOOG`：内部 ticker 表示 Alphabet C share，而 FMP 价格源默认
把 `GOOG` 请求成 `GOOGL`，这会把 A share 与 C share 混在一起。FMP 估值和
forward-only PIT 路径仍可保留 `GOOG -> GOOGL` 的供应商 alias，但价格路径不能复用
该 alias。

## 决策

- FMP 价格主源直接请求内部价格 ticker；`GOOG` 不再默认映射为 `GOOGL`。
- FMP 估值、分析师预期和 forward-only PIT 的 provider symbol alias 继续独立维护。
- 价格一致性默认只覆盖项目主要研究窗口 `ai_after_chatgpt`，即 `2022-12-01`
  以来；更早历史只作为 warm-up、压力测试或跨周期比较，不应在默认日报质量门禁中
  因复权口径或跨源差异阻断当前趋势判断。
- 宏观序列的单日变化异常提示使用同一默认研究窗口；数值范围、新鲜度和覆盖检查不
  因窗口而放松。
- 数据质量报告必须显示问题来源，至少区分：
  - 价格主源；
  - 第二行情源 Marketstack；
  - 跨源核验：主价格源 vs Marketstack；
  - FRED 宏观序列；
  - 下载审计清单。
- Owner 已批准第二源阻断策略调整：Marketstack 作为必需第二行情源时，其自身
  schema/OHLC/非正价格/adjusted close 极端波动等 self-check 异常必须记录到
  数据质量报告，但默认降级为第二源健康告警，不在 FMP 主价格源通过时阻断
  `score-daily`。
- 主价格源自身错误、第二源缺失或读取失败、主源/二源 raw close 未解决差异、重叠覆盖
  不足等跨源核验错误仍可 fail closed；该策略不允许用第二源平滑、覆盖或修正主价格缓存。
- 当 Marketstack 出现明显自身坏行且 FMP 主源对应行通过时，可以使用 Yahoo Finance
  做显式、诊断性的第三来源复查，范围只限异常 ticker/date 附近的 raw OHLC/close
  sanity check；复查结果只进入数据源健康或调查报告，不进入默认评分、主缓存、第二源缓存
  或仓位闸门。
- 不做本地补写、回填、平滑或用另一 ticker 替代。

## Owner 决策与剩余边界

- 已确认：遇到 Marketstack 第二源自身异常时，记录异常详情，以主数据源 FMP 为准，
  不阻塞后续流程。
- 边界：如果主源自身失败、第二源文件缺失且命令要求第二源、或主/二源 raw close
  出现无法归因的真实冲突，仍停止下游评分。
- Yahoo 复查边界：Yahoo 是 `public_convenience` 诊断来源，不能把三源投票结果自动写成
  价格真值；如果 Yahoo 与 FMP 一致、Marketstack 明显坏行，只能增强“Marketstack
  第二源健康告警”的归因；如果 Yahoo 与 FMP 不一致，必须升级为人工调查，不能自动覆盖主源。
- 退出条件：如果后续第二源长期出现研究窗口内关键 ETF/股票坏行，应更换或升级第二源；
  本策略只防止第二源健康问题误杀主源干净的日报，不代表 Marketstack 数据已可作为主源。

## 验收标准

- `FmpPriceProvider` 默认请求 `GOOG`，并继续跳过 `^VIX`，由 Cboe 专用源补入。
- 下载 manifest 不再记录 `GOOG -> GOOGL` 的价格 alias；仍记录 `^VIX: null`。
- `outputs/reports/data_quality_YYYY-MM-DD.md` 的问题表包含“来源”列。
- 报告显示价格一致性和宏观变化检查窗口起点；默认起点为 `2022-12-01`。
- Marketstack 自身价格异常显示来源为“第二行情源 Marketstack”。
- Marketstack 自身价格异常默认显示为“警告”，并保留 row count、样例和来源。
- 主源/二源差异显示来源为“跨源核验：主价格源 vs Marketstack”。
- 主源/二源 raw close 超过错误阈值仍显示为“错误”，并阻断下游流程。
- 后续 Yahoo 诊断复查若实现，必须单独输出 provider、endpoint、request parameters、
  download timestamp、row count、checksum、异常 ticker/date、FMP/Marketstack/Yahoo
  raw OHLC 对比和“diagnostic only / production_effect=none”声明。
- Yahoo 诊断复查失败、空结果或缺历史不能阻断 `score-daily`，也不能降低 FMP 主源权威性。
- FMP 估值/PIT 测试仍确认估值路径的 `GOOG -> GOOGL` alias 未被误删。

## 进展记录

- 2026-05-05：新增任务，原因：owner 将股价数据不一致列为重点问题；初步复核显示
  SPY/INTC/MSFT/SOXX 的硬异常主要落在 Marketstack 第二源缓存，`GOOG` 硬差异来自
  价格路径错误复用估值 alias。
- 2026-05-05：完成基础实现。FMP 价格路径默认直接请求 `GOOG`，数据质量问题表新增
  “来源”列并覆盖第二源自检与跨源核验归因；定向 Ruff、87 项相关 pytest 和
  `aits data-sources validate` 通过。完整生产确认仍需下一次真实
  `download-data` / `validate-data` 观察。
- 2026-05-05：owner 明确数据一致性验证应只考虑项目研究时间跨度，太久之前的
  不一致可以忽略或降低严重等级。实现口径：`config/data_quality.yaml` 显式配置
  `consistency_start_date: 2022-12-01`，用于价格波动/复权比例、跨源 reconciliation
  和宏观序列单日变化提示。
- 2026-05-05：完成 DATA-010 baseline。真实 `aits validate-data` 报告已显示
  价格一致性和宏观变化窗口；2020 年 DTWEXBGS 旧窗口 warning 已移出默认报告，
  但 2024/2026 年 Marketstack 第二源硬错误仍在研究窗口内，继续 fail closed。
  `ruff check .` 和 `pytest -q` 356 项通过。
- 2026-05-05：新增 DATA-011 决策项，原因：窗口口径已排除旧历史噪音，但当前
  阻断来自研究窗口内 Marketstack 第二源自身错误，需要 owner 决定是否保持阻断或在
  明确条件下降级为第二源健康告警。
- 2026-05-05：owner 批准 DATA-011 降级策略。实现口径：第二源 self-check 异常默认
  降级为质量报告告警，主数据源 FMP 通过时不阻断日报；主源错误、第二源缺失/不可读和
  主/二源 raw close 未解决冲突仍 fail closed。
- 2026-05-05：记录 DATA-012 后续方向。对于 Marketstack 明显自身坏行，可增加显式
  Yahoo 诊断复查作为第三来源 sanity check；该能力只能帮助归因和数据源健康判断，
  不得重新启用 Yahoo 作为生产主源或自动改写主价格缓存。
