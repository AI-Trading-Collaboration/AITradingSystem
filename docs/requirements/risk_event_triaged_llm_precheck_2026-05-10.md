# 高优先级官方候选 LLM 风险等级预审

## 背景

`RISK-010` 已把 2026-05-10 官方政策/地缘候选按 AI 模块相关性分类。Owner 确认当前高风险源可以用 LLM 判断风险等级，以减少人工复核时对 L1/L2/L3 初判的负担。

现有 `risk_event_prereview` 已支持 OpenAI Structured Outputs，并输出 `status_suggestion`、`level_suggestion`、`matched_risk_ids`、`affected_tickers`、`affected_nodes` 和 `human_review_questions`。本任务复用该链路，只新增从 triage 高优先级 bucket 选择官方候选的入口。

## 范围

新增命令：

- `aits risk-events precheck-triaged-official-candidates`

默认输入：

- `data/processed/official_policy_source_candidates_YYYY-MM-DD.csv`
- `data/processed/official_policy_candidate_triage_YYYY-MM-DD.csv`

默认 bucket：

- `must_review`
- `review_next`

输出：

- `outputs/reports/risk_event_prereview_triaged_openai_YYYY-MM-DD.md`
- `data/processed/risk_event_prereview_queue.json`

## 边界

- 只发送 metadata-only 官方候选内容，不默认发送 raw payload 全文。
- 只处理 triage 选中的高优先级 bucket，不把低优先级 backlog 批量送入 LLM。
- OpenAI 输出只能作为 `llm_extracted / pending_review` 预审建议。
- 不写入 `risk_event_occurrence`。
- 不触发评分、仓位闸门、回测标签或人工复核声明。
- provider 的 `llm_permission.external_llm_allowed` 必须通过，否则 fail closed。

## 验收标准

- 命令能按 as-of 推导官方候选 CSV 和 triage CSV。
- 支持 `--triage-buckets` 指定 bucket 集合，默认 `must_review,review_next`。
- 支持 `--max-candidates` 控制本次 OpenAI 成本上限。
- 缺少 OpenAI API key、候选 CSV、triage CSV 或 provider LLM 授权时停止并输出明确错误。
- 报告和队列保留 model、reasoning effort、request id、input/output checksum、risk level suggestion 和人工复核问题。
- 结果仍强制 pending review，不进入正式 occurrence 或评分。
- README、系统流图和测试同步更新。

## 进展记录

- 2026-05-10：任务创建并进入实现；先实现独立 CLI，不默认改变 `score-daily` 的自动预审行为。
- 2026-05-10：基础版完成。新增 `aits risk-events precheck-triaged-official-candidates`，默认读取 `must_review/review_next`，复用 OpenAI risk event prereview 并保持 metadata-only。真实 2026-05-10 运行送入 8 条高优先级候选，生成 5 条 `llm_extracted / pending_review` 预审记录，其中 L2/L3 候选 4 条、active 候选 0 条；报告为 `outputs/reports/risk_event_prereview_triaged_openai_2026-05-10.md`，队列为 `data/processed/risk_event_prereview_queue.json`。验证通过 `python -m ruff check src tests`、CLI help、目标测试 26 passed 和完整 `python -m pytest -q` 438 passed；后续需 owner 人工确认哪些建议可整理为 reviewed occurrence CSV 或复核声明。
