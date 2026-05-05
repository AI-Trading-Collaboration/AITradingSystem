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
- 不改变 fail-closed 行为：Marketstack 作为必需第二行情源时，自身硬错误仍会阻断
  下游评分，除非 owner 明确批准降级策略。
- 研究窗口内的第二源硬错误仍保持阻断；该规则只降低或忽略研究窗口之前的
  consistency/reconciliation 噪音，不用于绕过 2024/2026 的异常行情行。
- 不做本地补写、回填、平滑或用另一 ticker 替代。

## 待 owner 决策

- Marketstack 第二源在研究窗口内出现自身硬错误，但 FMP 主源对应行通过时，是否继续
  fail closed。
- 可选方向：
  - 保守：继续阻断，直到 Marketstack 修复、升级 plan 或更换第二源。
  - 降级：在 FMP 主源通过、raw close 跨源覆盖足够、异常只落在第二源自身时，把第二源
    self-check hard error 降为数据源健康告警和日报限制说明。
- 该决策会影响 `score-daily` 是否能在主源干净但第二源异常时继续运行，不能作为临时
  workaround 静默实现。

## 验收标准

- `FmpPriceProvider` 默认请求 `GOOG`，并继续跳过 `^VIX`，由 Cboe 专用源补入。
- 下载 manifest 不再记录 `GOOG -> GOOGL` 的价格 alias；仍记录 `^VIX: null`。
- `outputs/reports/data_quality_YYYY-MM-DD.md` 的问题表包含“来源”列。
- 报告显示价格一致性和宏观变化检查窗口起点；默认起点为 `2022-12-01`。
- Marketstack 自身价格异常显示来源为“第二行情源 Marketstack”。
- 主源/二源差异显示来源为“跨源核验：主价格源 vs Marketstack”。
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
