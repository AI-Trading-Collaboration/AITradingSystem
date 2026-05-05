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
- 数据质量报告必须显示问题来源，至少区分：
  - 价格主源；
  - 第二行情源 Marketstack；
  - 跨源核验：主价格源 vs Marketstack；
  - FRED 宏观序列；
  - 下载审计清单。
- 不改变 fail-closed 行为：Marketstack 作为必需第二行情源时，自身硬错误仍会阻断
  下游评分，除非 owner 明确批准降级策略。
- 不做本地补写、回填、平滑或用另一 ticker 替代。

## 验收标准

- `FmpPriceProvider` 默认请求 `GOOG`，并继续跳过 `^VIX`，由 Cboe 专用源补入。
- 下载 manifest 不再记录 `GOOG -> GOOGL` 的价格 alias；仍记录 `^VIX: null`。
- `outputs/reports/data_quality_YYYY-MM-DD.md` 的问题表包含“来源”列。
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
