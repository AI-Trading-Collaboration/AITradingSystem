# LLM-002 OpenAI 请求发送链路诊断

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`LLM-001`、`RISK-004`、`RISK-007`

## 背景

2026-05-05 日报前 OpenAI 风险事件预审在默认 20 条官方候选样本中出现
`openai_responses_api_request_failed`，最终错误为 `URLError: <urlopen error [Errno 2] No such file or directory>`。

同一失败候选使用相同 payload builder 单独重跑时，约 20 秒返回 `PASS_WITH_WARNINGS`，且未生成可写入队列的风险事件候选。这说明候选内容和模型判断不是主要 blocker，问题更可能在本机 OpenAI API 发送链路、代理/TUN、TLS、客户端实现或批量调用时的间歇性传输状态。

## 已观察事实

- `api.openai.com:443` 在本机路由经由 `mihomo` 接口，`Test-NetConnection` 显示 TCP 连通。
- Python `urllib`、`curl.exe` 和 PowerShell 对无鉴权 OpenAI 请求均能拿到正常 `401 Unauthorized`，说明 OpenAI 域名不是稳定不可达。
- 一次 Python `urllib` 无鉴权 POST `/v1/responses` 曾返回 `SSLEOFError`，随后多次重复正常返回 `401`，说明问题有间歇性。
- 当前实现不会持久化完整 OpenAI request body、每次 attempt 的 `client_request_id`、payload byte size、TLS/proxy 诊断或逐次异常；失败报告只保留最终错误。
- 失败候选的可重建 request body 为 `metadata_only`，默认 `gpt-5.5`、`reasoning.effort=high`、`store=false`、120 秒读超时。

## 目标

提升 OpenAI 请求发送链路的可观测性和稳定性，同时继续遵守数据与密钥边界：

- 不打印或保存 `OPENAI_API_KEY`、Authorization header、未授权付费内容全文。
- 保留 `store=false`、provider LLM permission fail closed、`llm_extracted / pending_review` 隔离。
- 请求失败仍不能写 occurrence、复核声明、评分、仓位闸门或部分成功队列。

## 待办清单

- [x] 在 OpenAI 请求失败报告中记录安全诊断字段：attempt count、每次 attempt 的 `client_request_id`、candidate/precheck id、endpoint host、payload byte size、input checksum、exception type、errno/reason、HTTP status、OpenAI `x-request-id`（如有）。
- [ ] 增加只读诊断命令或 debug 选项，能够对同一候选运行无写入 dry-run，并输出 sanitized transport diagnostics。
- [x] 对比 `urllib`、`requests` 或 OpenAI SDK/httpx 在本机 `mihomo` TUN 下的 Responses API 行为，记录是否能稳定拿到 HTTP 响应。
- [x] 如果切换客户端，先补 dependency、超时、重试、TLS/代理行为和错误分类测试，再替换生产默认客户端。
- [x] 明确哪些诊断字段可以进入 Markdown 报告：仅 sanitized transport diagnostics；API key、Authorization header 和请求正文不进入报告。

## 验收标准

- 单次请求失败时，报告能区分 DNS/TLS/proxy/socket/HTTP status/structured output 错误。
- 同一批次内可追踪每次 retry 的 sanitized attempt 信息，不需要 API key 也能定位客户端层问题。
- `pytest` 覆盖请求重试、最终失败、诊断字段脱敏和不写部分队列。
- `docs/system_flow.md` 和数据源/LLM 文档同步说明 transport diagnostics 的边界。

## 进展

- 2026-05-05：新增任务，原因：默认 20 条日报前 OpenAI 预审样本出现间歇性 `URLError`；同一失败候选单独重跑成功，说明需要先补发送链路诊断，而不是继续调大 timeout 或改变模型判断策略。
- 2026-05-05：第一阶段实现完成。`llm_precheck` 的 OpenAI 请求重试会为每次 attempt 记录 sanitized diagnostics，包括 `client_request_id`、endpoint host、payload byte size、input checksum、HTTP status、OpenAI `x-request-id` 或异常类型/errno/reason；`llm_claim` 与 `risk_event_prereview` Markdown 报告新增“请求诊断”章节；风险事件批量失败仍不写部分队列。验证：`ruff check src tests` 通过，`pytest -q tests/test_llm_precheck.py tests/test_risk_event_prereview.py` 22 passed。
- 2026-05-05：第二阶段实现完成。默认 OpenAI HTTP client 从 `urllib` 改为 `requests`，并在 `aits llm precheck-claims`、`aits risk-events precheck-openai` 和 `aits score-daily` 增加 `--openai-http-client` 选项，保留 `urllib` 做本机传输对照。`requests` 真实单候选 dry-run 对此前失败的 USTR 候选约 20 秒返回 `PASS`、`record_count=0`。
- 2026-05-05：真实默认日报验证中，第一次完整 `score-daily --as-of 2026-05-05` 的 OpenAI 预审完成后暴露日报章节签名未接收 `http_client` 的集成 bug，已补签名、日报展示和回归测试。第二次默认日报暴露 OpenAI 边缘 HTTP 520 未被重试，已把所有 5xx 归入可重试瞬时错误。最终默认日报通过：OpenAI 预审 `PASS_WITH_WARNINGS`，官方候选 402、候选上限 20、LLM claim 24、待复核队列 7、L2/L3 候选 6、active 候选 0；日报 `PASS_WITH_LIMITATIONS`，执行动作 `wait_manual_review`。验证：`ruff check src tests` 通过，目标测试 45 passed，完整 `pytest -q` 369 passed。
