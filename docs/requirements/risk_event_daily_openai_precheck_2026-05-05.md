# RISK-007 日报前风险事件 OpenAI 自动预审

状态：BASELINE_DONE

最后更新：2026-05-17

关联任务：`RISK-004`、`RISK-005`、`RISK-006`、`LLM-001`

## 背景

2026-05-05 日报中，`policy_geopolitics` 仍是低置信度模块，直接原因是没有覆盖评估日的风险事件发生记录或有效复核声明。已有链路支持官方来源抓取、OpenAI live 预审和人工复核声明；本任务把官方来源抓取与 OpenAI 预审纳入 `score-daily` 默认日报前流程。

owner 要求先使用本机 `OPENAI_API_KEY` 做预审，并把这一步加入日报前自动流程。默认情况下，OpenAI 预审不应增加人工确认输入；只有模型识别出候选风险事件时，才写入 `llm_extracted / pending_review` 队列，等待后续人工复核。

## 设计边界

- 自动预审先抓取官方/一手低成本来源，再把候选元数据送入 OpenAI Responses API。
- 默认发送内容级别为 `metadata_only`，不发送未授权付费内容，不输出 API key、token 或付费内容原文。
- 自动预审默认最多处理 20 条官方候选，超过上限时记录 warning，避免日报前流程成本或延迟失控。
- OpenAI Responses API 默认模型为 `gpt-5.5`，`reasoning.effort=high`，请求读超时为 120 秒；`gpt-5.5-pro` 当前只作为显式覆盖测试选项，不作为日报前默认模型。
- OpenAI 输出只能进入 `data/processed/risk_event_prereview_queue.json`，记录为 `llm_extracted / pending_review`。
- `irrelevant` 或无风险候选的默认输出不进入人工复核队列，避免把正常噪音变成人工待办。
- 自动预审不写 `risk_event_occurrence`，不写 `review_attestation`，不直接改变评分、仓位闸门、thesis 状态或回测。
- 默认日报前 OpenAI 预审缺少 `OPENAI_API_KEY`、官方来源抓取失败、provider LLM 权限失败或 OpenAI 请求失败时应停止日报评分，而不是静默跳过。
- 有效“无未记录重大事件”声明仍必须由 `aits risk-events record-review-attestation` 或等价真实人工复核输入生成，不能由 OpenAI 或自动流程伪造。

## 任务清单

- [x] 增加官方候选到 OpenAI 预审输入的转换器，默认只发送来源 metadata/title、匹配 topic、risk_id、ticker/node 和审计字段。
- [x] 增加批量自动预审函数：读取官方候选，逐条调用现有 `run_openai_risk_event_prereview`，合并报告和队列。
- [x] 修改 `score-daily`，默认在风险事件发生记录校验前运行官方来源抓取和 OpenAI 风险事件预审；保留 `--skip-risk-event-openai-precheck` 作为排查/离线开关。
- [x] 将 OpenAI 默认配置设为 `gpt-5.5`、`reasoning.effort=high`、120 秒请求读超时。
- [x] 按 AI 政策/出口管制相关性、来源可信度、ticker/node 覆盖和发布日期为官方候选排序，避免前 20 条被低相关噪音占用。
- [x] 在日报正文输出官方抓取、OpenAI 预审、模型、reasoning effort、超时、候选数、队列数和报告路径。
- [x] 运行默认 20 条候选的真实日报前预审样本，记录耗时、错误、队列数量和噪音质量；2026-05-05 样本在 OpenAI HTTP 502 后按 fail closed 停止，未写入新队列。
- [ ] owner 复核待复核队列，确认是否需要导入 occurrence 或生成有效 review attestation。
- [x] 失败策略：单个 OpenAI 请求失败时重试该请求 2 次；如果仍失败，则整批 fail closed，不写部分队列，不继续日报评分。

## 验收标准

- `score-daily` 默认在日报评分前运行官方来源抓取和 OpenAI 风险事件预审，并保留 `--skip-risk-event-openai-precheck` 作为离线/排查开关。
- 自动预审输出仍为 `llm_extracted / pending_review`，且不改变 `risk_event_occurrences` 或复核声明。
- 没有候选时报告显示 0 条队列记录，不增加人工输入，也不伪造无风险结论。
- 缺少 `OPENAI_API_KEY` 或预审失败时，默认自动预审停止日报评分。
- 测试覆盖 CLI 行为、队列隔离和系统边界。

## 进展

- 2026-05-05：新增需求文档并进入实现，原因：owner 要求把 OpenAI 风险事件预审加入日报前自动流程，同时保持默认不自动加入人工确认输入。
- 2026-05-05：基础版实现完成。`score-daily` 会在风险事件发生记录校验前抓取官方来源并调用 OpenAI metadata-only 风险事件预审；当时输出仅写 `llm_extracted / pending_review` 队列和报告，`irrelevant` 结果不增加人工队列；默认最多预审 20 条官方候选，可通过 `--risk-event-openai-precheck-max-candidates` 调整。验证：`ruff check src tests` 通过，`pytest -q` 362 passed。
- 2026-05-05：按 owner 决策把 OpenAI Responses API 请求读超时从 60 秒提高到 600 秒，并暴露 `--openai-timeout-seconds` 供 `llm precheck-claims`、`risk-events precheck-openai` 和 `score-daily --risk-event-openai-precheck` 调整；当时 `gpt-5.5-pro` + `reasoning.effort=xhigh` 的结构化响应可能超过 60 秒。验证：`ruff check src tests` 通过，`pytest -q tests/test_llm_precheck.py tests/test_risk_event_prereview.py` 18 passed，`pytest -q` 362 passed。真实 smoke：`aits score-daily --as-of 2026-05-05 --risk-event-openai-precheck --risk-event-openai-precheck-max-candidates 1 --openai-timeout-seconds 600` 等待约 408 秒后收到 OpenAI HTTP 502，预审 fail closed，未写队列，未继续日报评分。
- 2026-05-05：`gpt-5.5` 单候选真实 smoke 可收到正常响应并继续日报评分；同时发现模型可能输出 `status_candidate=irrelevant`、`level_candidate=none` 但保留候选 `risk_id`。已收紧过滤逻辑：只要状态为 `irrelevant/none`、等级为 `none`、动作类为 `none`，就不进入人工队列，避免把无关结果变成人工待办。
- 2026-05-05：owner 决策后续默认使用 `gpt-5.5`，原因是 `gpt-5.5-pro` 在 600 秒单候选真实 smoke 中返回 HTTP 502，而 `gpt-5.5` 两次单候选 smoke 均能正常响应。默认模型常量、README、系统流图、数据源目录、架构文档和示例模板已同步。验证：不带 `--openai-model` 的单候选 `score-daily --risk-event-openai-precheck` 约 96 秒完成，预审 `PASS_WITH_WARNINGS`，待复核队列 0，日报 `PASS_WITH_LIMITATIONS`。
- 2026-05-05：owner 决策把 OpenAI 默认配置改为 `gpt-5.5`、`reasoning.effort=high`、120 秒读超时；`score-daily` 默认启用日报前预审，候选排序改为 AI 政策/出口管制优先，日报正文新增预审摘要。
- 2026-05-05：默认 20 条候选真实日报前样本已执行，命令 `score-daily --as-of 2026-05-05` 未显式覆盖 OpenAI 参数。官方来源抓取 `PASS`，8 个 payload，402 条候选；OpenAI 预审处理上限 20 条，报告 `FAIL`，LLM claim 数 23，错误 1、警告 13，错误为 OpenAI Responses API HTTP 502；按 fail closed 未写入新 `risk_event_prereview_queue.json`，日报评分未继续。
- 2026-05-05：owner 决策单个 OpenAI 请求失败时允许重试该请求 2 次；若第 3 次尝试仍失败，则整批 fail closed，不采用断点续跑、部分成功落队列或降级为限制项。
- 2026-05-05：已实现单请求 2 次重试和批量失败即停止。验证：`ruff check src tests` 通过，`pytest -q tests/test_llm_precheck.py tests/test_risk_event_prereview.py tests/test_daily_scoring.py` 42 passed，`pytest -q` 366 passed，`git diff --check` 无 whitespace 错误。真实默认样本 `score-daily --as-of 2026-05-05`：官方来源抓取 `PASS`，8 个 payload、402 条候选；OpenAI 预审报告 `FAIL`，LLM claim 数 16，错误 1、警告 7，最终失败为单请求 3 次尝试后仍出现 `URLError`；按 fail closed 未写入新队列，日报评分未继续。

## 剩余限制

- 真实每日运行仍需要本机配置 `OPENAI_API_KEY`；单个请求最多 3 次尝试，仍失败时会阻断整批日报前预审。
- 2026-05-12：owner 批准短期把 LLM formal assessment 作为可信政策/地缘结论。`score-daily` 的 OpenAI 预审成功后默认会写入 LLM formal occurrence/attestation；该来源能让 `policy_geopolitics` 脱离 `insufficient_data`，但来源类型仍是 `llm_formal_assessment`，不是 `manual_input`，也不能单独触发 position gate。真实人工复核声明仍只能由 owner/reviewer 显式运行 `aits risk-events record-review-attestation` 写入。

