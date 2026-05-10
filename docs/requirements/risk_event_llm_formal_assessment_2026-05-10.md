# LLM 正式风险评估准入

## 背景

Owner 决策：高优先级官方候选可以参考 LLM 的复核结果作为正式评估结果，短期内不要求每条候选先人工复核。

这会改变政策/地缘模块的正式输入口径。实现必须保留审计边界：LLM formal assessment 是正式评估来源，但不是人工复核，也不能伪装成人工复核。

## 范围

新增命令：

- `aits risk-events apply-llm-formal-assessment`

输入：

- `data/processed/risk_event_prereview_queue.json`

输出：

- `data/external/risk_event_occurrences/*.yaml`
- `outputs/reports/risk_event_llm_formal_assessment_YYYY-MM-DD.md`
- `outputs/reports/risk_event_occurrences_YYYY-MM-DD.md`

## 口径

- LLM formal assessment 可以写入正式 risk occurrence YAML。
- `reviewer` 必须写成 `llm_formal_assessment:<model>`，不得写成人工 reviewer。
- evidence sources 同时保留原始官方来源和 `llm_extracted` 评估来源。
- LLM formal evidence 默认最高按 `B` 级处理，可进入普通评分，但不能单独触发 position gate。
- `status_suggestion=active_candidate` 才转换为 `active`；`watch/candidate` 默认转换为 `watch`。
- 可以写入 LLM formal attestation，表示 LLM 已覆盖本次队列中的高优先级候选；它不是人工全量复核声明。
- 日报来源类型必须显示为 `llm_formal_assessment`，置信度低于人工复核。

## 验收标准

- 命令能读取 prereview queue，生成 occurrence YAML、LLM formal attestation 和报告。
- 生成的 occurrence 保留 model、reasoning effort、request id、input/output checksum、status/level suggestion、confidence、precheck id 和“未人工复核”说明。
- `validate-occurrences` 能读取并校验 LLM formal occurrence。
- 日报政策/地缘模块在只存在 LLM formal attestation 时不再显示 `insufficient_data`，但来源类型为 `llm_formal_assessment`。
- LLM formal assessment 默认不单独触发 position gate。
- README、系统流图、任务登记和测试同步更新。

## 进展记录

- 2026-05-10：任务创建并进入实现。
- 2026-05-10：基础版完成。新增 `aits risk-events apply-llm-formal-assessment`，可把 `risk_event_prereview_queue.json` 写为正式 risk occurrence YAML 和 LLM formal attestation；日报政策/地缘模块新增 `llm_formal_assessment` 来源类型，置信度上限低于人工复核。真实 2026-05-10 队列写入 5 条 `watch` occurrence 和 1 条 LLM formal attestation，`validate-occurrences` 校验 PASS；报告为 `outputs/reports/risk_event_llm_formal_assessment_2026-05-10.md` 与 `outputs/reports/risk_event_occurrences_2026-05-10.md`。验证通过 `python -m ruff check src tests`、CLI help 和完整 `python -m pytest -q` 444 passed。
