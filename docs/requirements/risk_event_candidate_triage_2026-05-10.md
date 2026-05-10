# 风险事件官方候选 AI 模块 Triage

## 背景

2026-05-10 运行观察显示，`CONGRESS_API_KEY` 与 `GOVINFO_API_KEY` 可用，官方来源抓取链路本身不是阻塞项。当前复核压力主要来自官方候选抽取规则较宽：sanctions、China、Russia 等主题会被映射到 AI 芯片出口管制风险，导致人工队列中存在较多与 AI 模块没有明显直接联系的候选。

Owner 决策：先做分类。当前系统关注 AI 模块，无明显联系的候选降低复核优先级。

## 范围

本任务只新增官方候选 triage 层：

- 输入：`data/processed/official_policy_source_candidates_YYYY-MM-DD.csv`。
- 输出：`data/processed/official_policy_candidate_triage_YYYY-MM-DD.csv`。
- 报告：`outputs/reports/risk_event_candidate_triage_YYYY-MM-DD.md`。
- 命令：`aits risk-events triage-official-candidates`。

不改变：

- 不写入 `risk_event_occurrence`。
- 不把候选自动标记为 active/watch/resolved/no-risk。
- 不进入评分、仓位闸门或回测标签。
- 不替代 owner 的人工复核声明。

## 分类口径

分类必须以 AI 模块相关性为主，而不是简单继承候选 CSV 中由宽泛主题生成的 `matched_risk_ids` 或 `affected_tickers`。

优先级桶：

- `must_review`：标题、URL、来源名称或明确 metadata 直接命中 AI 模块高相关信号，例如 export control、Entity List、advanced computing、semiconductor、AI chip、GPU、data center、NVDA、AMD、TSM、INTC、ASML 等。
- `review_next`：直接涉及 artificial intelligence / AI policy，但没有明显市场、半导体、出口管制或核心 ticker 信号。
- `sample_review`：只命中宽泛政策/地缘主题，例如 sanctions、China、Russia、Taiwan、trade policy，需要抽样复核以避免漏检，但不应与 AI 芯片直接候选同等优先。
- `auto_low_relevance`：标题/URL 没有明显 AI 模块关键词，且候选主要来自宽泛 sanctions/geopolitics 主题或自动映射。
- `duplicate_or_noise`：重复标题/来源或明显低信息重复项，只保留审计记录。

## 验收标准

- CLI 可按日期读取候选 CSV，并写入 triage CSV 与中文 Markdown 报告。
- 报告显示各 bucket 数量、直接命中的 AI 模块信号、低优先级理由和审计边界。
- OFAC/CSL 中仅因 sanctions/China/Russia 宽泛词命中的银行、个人或无关实体应降为 `auto_low_relevance`，不能因为原始候选存在 `ai_chip_export_control_upgrade` 就进入高优先级。
- Congress/Federal Register 中直接命中 AI/半导体/出口管制的候选应进入 `must_review` 或 `review_next`。
- 输出字段包含原始 `candidate_id`、`source_id`、`source_title`、`matched_topics`、`matched_risk_ids`、`triage_bucket`、`ai_relevance_score`、`triage_reason`、`review_policy`、`production_effect`。
- 测试覆盖确定性分类、重复降级、CLI 写文件和报告边界。
- `docs/system_flow.md`、`README.md`、`docs/task_register.md` 同步更新。

## 进展记录

- 2026-05-10：任务创建并进入实现；先实现确定性规则 triage，不调用外部 LLM，不改变评分或正式风险事件记录。
- 2026-05-10：基础版完成。新增 `aits risk-events triage-official-candidates`，输出 triage CSV 和中文报告；真实 2026-05-10 官方候选 403 条分类为 `must_review=7`、`review_next=1`、`sample_review=94`、`auto_low_relevance=297`、`duplicate_or_noise=4`。验证通过 `python -m ruff check src tests`、目标测试 24 passed 和完整 `python -m pytest -q` 436 passed。后续如 owner 复核发现误判，再调整关键词、bucket 阈值或抽样规则。
