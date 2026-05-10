# OpenAI/Agent 请求短期缓存与审计归档

关联任务：`LLM-003`

最后更新：2026-05-10

## 背景

调试 `score-daily`、风险事件 OpenAI 预审和高优先级官方候选预审时，短时间内可能反复发送完全相同的 OpenAI Responses API 请求。重复请求会增加成本、延迟和外部 API 失败暴露面，也会让已经付费获得的结构化响应缺少可复用归档。

Owner 确认需要建立短期缓存：半天或一天内发送相同请求时优先复用本地缓存，超过时限后才允许重新发送。同时，所有实际发送给 OpenAI 的请求和响应详细信息都应记录下来，作为成本已经发生的数据资产和审计输入。

后续系统可能接入其他 agent 或 LLM provider，因此缓存底座必须使用 provider-neutral 概念抽象。OpenAI Responses API 是当前首个接入方，但缓存 key、TTL、审计归档、secret 脱敏和 report count 不应复制成 OpenAI 专有实现。

## 决策

- 底层实现使用 `agent_request_cache` 通用模块；调用方必须显式传入 `provider`、`api_family`、`endpoint`、request payload、input checksum、client name 和审计 diagnostics。
- 默认本地目录为 `data/processed/agent_request_cache`；每个成功缓存文件仍按 cache key 平铺保存，实际 live 请求归档写入 `archive/{provider}/{api_family}/YYYY-MM-DD/`。
- 第一阶段只把 OpenAI Responses API 接入该通用 cache；CLI 参数继续保留 `--openai-cache-dir` 与 `--openai-cache-ttl-hours`，避免破坏现有命令语义。
- 第一阶段默认 TTL 为 24 小时，并暴露 CLI 参数用于改成 12 小时等更短窗口。
- 缓存只按完全相同请求命中，不做语义相似、URL 相似、标题相似或候选相似复用。
- 缓存 key 至少覆盖 cache schema version、provider、api family、endpoint、input checksum 和完整 request payload checksum；OpenAI 的 prompt version、model、reasoning effort、结构化输出 schema、source permission 和 content sent level 均已包含在 request payload/input checksum 中。
- OpenAI API 仍使用 `store=false`；本地缓存不改变 provider 侧存储策略。
- API key、Authorization header 和 secret 不写入缓存、报告或错误信息。
- 已发送请求的 request payload、sanitized headers、response status/header/body、attempt diagnostics、provider request id、client request id、input/output checksum、cache key、created/expires 时间必须本地归档。
- 如果来源 `cache_allowed=false`，live OpenAI 自动请求应 fail closed。原因是本任务要求实际发送请求和响应必须可本地完整归档；来源不允许缓存时，系统不能同时满足发送和归档边界。
- 缓存命中结果仍保持 `llm_extracted / pending_review`，不得直接写入正式 occurrence、评分、仓位闸门、thesis 状态或交易建议。

## 实施步骤

1. 增加 provider-neutral `agent_request_cache` helper：
   - 生成稳定 cache key；
   - 读取未过期成功响应；
   - 写入成功响应缓存；
   - 写入每次实际发送请求的审计归档。
2. 将 OpenAI Responses 适配到通用 helper，并把 cache dir 和 TTL 接入：
   - `aits llm precheck-claims`
   - `aits risk-events precheck-openai`
   - `aits risk-events precheck-triaged-official-candidates`
   - `aits score-daily` 的日报前 OpenAI 预审
3. 报告输出 cache 状态：
   - LLM claim 预审报告；
   - 风险事件 OpenAI 预审报告；
   - 日报 OpenAI 预审章节。
4. 更新系统流图、README 和测试。

## 验收标准

- 相同 OpenAI request payload 在 TTL 内第二次运行不调用 HTTP client，报告显示 cache HIT。
- TTL 过期后允许重新调用 HTTP client，并刷新成功响应缓存。
- 实际发出的 OpenAI 请求会写入本地归档，包含 sanitized request、response body、attempt diagnostics、cache key 和 checksum，不包含 API key。
- 缓存 payload 记录 `provider=openai`、`api_family=responses`、`provider_request_id` 和 `client_name`，使后续其他 agent 能复用同一 cache schema。
- `cache_allowed=false` 且启用 OpenAI 请求缓存时，命令 fail closed，不发起 live OpenAI 请求。
- 缓存命中不改变 LLM 输出隔离边界，仍只能进入 `llm_extracted / pending_review`。
- `docs/system_flow.md` 说明 agent request cache 的位置、TTL、provider/api family 分层和权限边界。

## 进展记录

- 2026-05-10：新增任务并进入实现。原因：调试中会短时间重复发送相同 OpenAI 请求，owner 要求半天或一天内复用缓存，并完整归档已付费请求/响应详情。
- 2026-05-10：实现完成。新增 `agent_request_cache` 通用模块，`llm_precheck` 作为 OpenAI Responses adapter 接入该模块；`llm precheck-claims`、`risk-events precheck-openai`、`risk-events precheck-triaged-official-candidates` 与 `score-daily` 均接入默认 24 小时 TTL，本地目录为 `data/processed/agent_request_cache`，审计归档目录为 `archive/openai/responses/YYYY-MM-DD/`；报告和队列记录 cache HIT/MISS/EXPIRED/DISABLED；`cache_allowed=false` 时 fail closed。验证：目标测试 53 passed，完整 `pytest -q` 445 passed；Ruff 对本次核心文件通过，`cli.py` 单独用 `--ignore F401` 通过，原因是工作区已有未使用的 `risk_event_llm_formal` 导入。
