# LLM 正式风险评估准入

最后更新：2026-05-12

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

## 2026-05-12 临时运行口径

Owner 决策：短期没有精力每日人工复核政策/地缘候选；如果日报卡在人工复核缺口，可以放宽该限制，把 LLM formal assessment 作为可信结论使用。

该口径是接受的临时绕行方案，不是人工复核完成：

- 存在原因：每日人工复核 SLA 暂不可用，但政策/地缘模块长期停留 `insufficient_data` 会压低 5 月 12 日及后续日报判断置信度。
- 行为影响：`score-daily` 在 OpenAI 官方来源预审成功后，可以自动写入 LLM formal occurrence 和 LLM formal attestation；full coverage 的 `llm_formal_assessment` 模块置信度从 55% 调整为 65%，不再触发 `低置信度模块：policy_geopolitics`。
- 风险：LLM formal 只覆盖本次官方候选和预审队列，不等同人工全量风险消除证明；它仍不得伪装为 `manual_input`，不得单独触发 `position_gate`，L2/L3 或 active_candidate 仍需在报告中保留“未人工复核”边界。
- 验证覆盖：单元测试覆盖 LLM formal 写入、风险事件校验、日报 `policy_geopolitics` 来源类型和置信度；目标 CLI 测试覆盖自动 formal 写入路径。
- 退出条件：建立稳定人工复核责任人、来源范围和每日/交易日复核 SLA 后，重新评估是否把 LLM formal 置信度降回低于 60%，或只保留为人工复核前置队列。

## 验收标准

- 命令能读取 prereview queue，生成 occurrence YAML、LLM formal attestation 和报告。
- 生成的 occurrence 保留 model、reasoning effort、request id、input/output checksum、status/level suggestion、confidence、precheck id 和“未人工复核”说明。
- `validate-occurrences` 能读取并校验 LLM formal occurrence。
- 日报政策/地缘模块在只存在 LLM formal attestation 时不再显示 `insufficient_data`，但来源类型为 `llm_formal_assessment`。
- LLM formal assessment 默认不单独触发 position gate。
- `score-daily` 成功完成 OpenAI 官方来源预审后，可自动写入 LLM formal occurrence/attestation；重复运行同一 as-of 时必须可审计且可覆盖同名 LLM formal 输出。
- README、系统流图、任务登记和测试同步更新。

## 进展记录

- 2026-05-10：任务创建并进入实现。
- 2026-05-10：基础版完成。新增 `aits risk-events apply-llm-formal-assessment`，可把 `risk_event_prereview_queue.json` 写为正式 risk occurrence YAML 和 LLM formal attestation；日报政策/地缘模块新增 `llm_formal_assessment` 来源类型，置信度上限低于人工复核。真实 2026-05-10 队列写入 5 条 `watch` occurrence 和 1 条 LLM formal attestation，`validate-occurrences` 校验 PASS；报告为 `outputs/reports/risk_event_llm_formal_assessment_2026-05-10.md` 与 `outputs/reports/risk_event_occurrences_2026-05-10.md`。验证通过 `python -m ruff check src tests`、CLI help 和完整 `python -m pytest -q` 444 passed。
- 2026-05-12：owner 批准短期放宽人工复核限制。`score-daily` OpenAI 预审成功后会自动写入 LLM formal assessment，并将 full coverage `llm_formal_assessment` 置信度提升到 65%，以解除 `policy_geopolitics` 低置信提示；人工复核声明仍只能由真实 reviewer 显式写入。验证：`python -m ruff check` 目标文件通过，目标 pytest 41 passed，完整 `python -m pytest -q` 467 passed。
