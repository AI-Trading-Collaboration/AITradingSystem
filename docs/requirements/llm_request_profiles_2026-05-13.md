# LLM 请求配置分层

关联任务：`LLM-004`

最后更新：2026-05-13

## 背景

当前 OpenAI live 请求已经有统一底座 `run_openai_claim_precheck`，但默认请求策略仍分散在代码常量和 CLI 参数中。实际入口包括通用 claim 预审、风险事件单条预审、高优先级官方候选预审，以及 `score-daily` 默认的日报前官方候选预审。它们共享 `model`、`reasoning.effort`、timeout、HTTP client、缓存 TTL 和重试策略，但业务风险、成本和延迟约束不同。

## 当前请求来源

| 来源 | 命令或模块 | 是否 live OpenAI 请求 | 当前用途 |
|---|---|---:|---|
| 通用 claim 预审 | `aits llm precheck-claims` | 是 | 从 source-permission 输入抽取 claim，写入 `llm_extracted / pending_review` 队列。 |
| 风险事件单条预审 | `aits risk-events precheck-openai` | 是 | 复用 claim 预审底座，只保留风险事件候选并写入风险事件预审队列。 |
| 高优先级官方候选预审 | `aits risk-events precheck-triaged-official-candidates` | 是 | 读取官方候选 CSV 和 triage CSV，只把 `must_review/review_next` 送入 OpenAI。 |
| 日报前官方候选预审 | `aits score-daily --risk-event-openai-precheck` | 是 | 抓取官方政策/地缘来源后，对候选做 metadata-only OpenAI 预审；默认由 `daily-run` 调度。 |
| LLM formal assessment | `aits risk-events apply-llm-formal-assessment` / `score-daily` 自动写入 | 否 | 读取已生成的 OpenAI 预审队列，按 owner 决策写入正式 occurrence/attestation。 |
| 历史 replay | `aits ops replay-day --openai-replay-policy cache-only` | 否 | 只复用可证明在 cutoff 前已存在的 OpenAI 预审队列/cache，不调用 live OpenAI。 |

## 目标

1. 新增 `config/llm_request_profiles.yaml`，把不同请求类型的 OpenAI 请求参数放入可审计配置。
2. CLI 默认从 profile 读取；显式 CLI 参数仍可覆盖 profile，便于临时对照实验。
3. `daily-run` 传递 profile id 给 `score-daily`，避免 direct dispatcher 继续把默认候选上限硬编码成有效配置。
4. 保持安全边界不变：provider LLM 权限、`cache_allowed`、`store=false`、本地 request/response 审计、`llm_extracted / pending_review` 隔离和 fail closed 语义不放松。
5. 更新系统流图、README 和测试，报告/命令输出能看出实际使用的 profile 和关键请求参数。

## 设计

新增 profile 字段：

- `profile_id`
- `description`
- `provider`
- `api_family`
- `endpoint`
- `model`
- `reasoning_effort`
- `timeout_seconds`
- `http_client`
- `cache_ttl_hours`
- `max_retries`
- `max_candidates`
- `official_policy_limit`
- `formal_assessment`

第一版只配置请求级策略和日报候选/LLM formal 运行参数，不把 prompt/schema 版本改成可配置。原因是 prompt/schema 会影响结构化输出契约和下游校验，后续若要开放必须单独建任务并同步 schema 迁移。

## 验收标准

- 默认配置包含 `llm_claim_prereview`、`risk_event_single_prereview`、`risk_event_triaged_official_candidates` 和 `risk_event_daily_official_precheck`。
- `llm precheck-claims`、`risk-events precheck-openai`、`risk-events precheck-triaged-official-candidates` 和 `score-daily` 默认读取对应 profile。
- CLI 显式传入 `--openai-model`、`--openai-reasoning-effort`、`--openai-timeout-seconds`、`--openai-http-client`、`--openai-cache-ttl-hours` 或候选上限时覆盖 profile。
- `daily-plan/daily-run` 的 `score-daily` 命令传递 profile id；未显式候选上限时由 `score-daily` profile 决定。
- 配置 schema 测试、目标 LLM/risk/daily-run 测试、ruff 和 `git diff --check` 通过。
- `docs/system_flow.md` 和 README 说明 profile 配置路径与入口边界。

## 进展记录

- 2026-05-13：新增任务并进入实现；范围限定为请求策略 profile，不改变 LLM 输出权限、source policy 或评分/仓位边界。
- 2026-05-13：实现完成并归档为 DONE；新增 profile 配置、加载器、CLI 覆盖、daily-run profile 传递和报告披露，验证通过全量 `pytest`、`ruff check src tests`、`git diff --check`、CLI help、data-sources validate 和 daily-plan smoke。
