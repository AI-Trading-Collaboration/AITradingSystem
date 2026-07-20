# OPS-065：External request cache 到期复验 singleflight

最后更新：2026-07-20

状态：`DONE`

## 背景与边界

OPS-064 保证失败 response 到期后可恢复、所有历史证据不可变，并保证并发 writer 最终 pointer/body
一致；但多个进程在同一 cache key 的到期边界仍可能同时观察到 `EXPIRED_REVALIDATE`，从而重复发送
同一 live request。这不会把错误数据解释为成功，却会放大 provider quota、429 和长尾耗时风险。

本任务只治理跨线程/跨进程 revalidation coordination，不改变 HTTP status TTL、provider retry、request
budget、数据质量门禁或业务 refresh window。不得用全局串行锁，也不得在等待超时后静默绕过门禁。

## 设计要求

1. 以 cache key 为粒度建立可审计 lease；owner identity、acquired/expires、policy version、process/host
   context 与 causal event 必须可重放，TTL/等待/轮询参数写入 reviewed policy。
2. winner 获取 lease 后必须再次 lookup；若其他 writer 已发布可复用 generation，直接复用且不联网。
3. waiter 在有界时间内等待并再次 lookup；winner 成功发布后所有 waiter 复用同一 generation。
4. owner 崩溃或 lease 过期时允许确定性接管；stale owner 不能覆盖新 owner 的 lease 状态。
5. 网络异常未产生 response 时仍 fail closed；是否允许下一 waiter 重试必须由 policy 与已有 provider
   retry/request-budget 共同决定，不能形成无限重试。
6. 正缓存、legacy v1 read、immutable body/negative observations、CAS invalidation 与 provider wrapper
   行为保持兼容。

## 验收标准

- 两个独立进程同时访问同一到期 key，injected fake client 只收到一次 live request；
- double-check、waiter reuse、owner crash/stale lease takeover、timeout、invalidation race 和 source tamper
  均有确定性测试；
- 不同 cache key 仍可并行，不引入全局 provider 锁；
- lifecycle/lease artifacts 可审计且不包含 credential；
- FMP/Marketstack/FRED/SEC focused regression、architecture/contract 与风险相称的 full PASS；
- 不运行 periodic operation，不删除 cache，不改变 DQ、投资结论或策略状态，
  `production_effect=none`。

## 状态记录

- 2026-07-20：engineering lane 的独立实现已具备 integration-ready API：
  `ExternalRequestRevalidationCoordinator(request_dir, cache_key).execute(probe, fetch, publish)`；其中
  `probe` 只提交 lookup status、generation id、body SHA-256、稳定 reason code 与可复用 value，
  `fetch` 只由 lease winner 在 arbiter 外调用，`publish` 则在短 arbiter lock 内经过 current
  head/token/generation fencing 后调用。低层 `acquire/complete/publish_if_current_owner/replay` 保留给
  接线与审计测试，不要求 provider wrapper 复制协调逻辑。reviewed pilot policy 位于
  `config/data/external_request_cache_revalidation_coordination_policy.yaml`。
- 2026-07-20：per-key OS file lock 只保护短临界区；live request 在锁外、lease 内运行。每一代
  owner evidence 记录 acquired/expires、policy ref、PID、host SHA-256 fingerprint、causal probe，
  raw owner token 只在进程内存中存在，artifact 仅存 token SHA-256。immutable event hash chain 与
  atomic current pointer 支持 fail-closed replay；同 cause 最多一次 stale takeover，explicit owner
  failure 禁止 waiter 自动重试，避免绕过 provider retry/request budget。
- 2026-07-20：TTL=0/evidence-only response 不会被改写成 cache reusable：winner 返回原始 live
  response 并发布 `NON_REUSABLE_RESPONSE` terminal evidence；已经进入等待的 caller 只在确认新
  causal generation 后于同一总 deadline 内重新竞争，因此两个调用各自获得 live response，但请求
  严格串行而不并发放大。
- 2026-07-20：stale takeover 后旧 owner 即使恢复，也会在执行 cache publish callback 之前被
  `STALE_LEASE_OWNER` 拒绝；测试确认旧 callback 调用数为 0、新 owner callback 为 1，因此不依赖
  “lease TTL 大于 HTTP timeout”的脆弱时间假设。
- 2026-07-20：独立 focused suite 目前 13 PASS（`pytest -n 16 --dist loadfile`，5.18s），包括
  winner double-check、invalidation supersede、新 generation、bounded timeout、stale owner takeover、
  stale owner write rejection、pointer/event tamper，以及真实 `spawn` 多进程同 key 单次 live request/
  不同 key live 区间重叠、TTL=0 两个 caller 各自成功且 live 区间严格串行。shared cache 接线和
  provider/full gates 尚未完成，任务仍为
  `IN_PROGRESS`；`production_effect=none`。
- 2026-07-20：coordinator 完成 requests/urllib shared wrapper 接线，只在初始 lookup 为
  `EXPIRED_REVALIDATE` / `INVALIDATED_REVALIDATE` 时进入 singleflight；HIT、cold MISS、legacy v1、
  cache disabled、cache key/body/lifecycle/CAS/DQ 语义保持。额外 wrapper 并发测试确认两个 caller
  只产生一次 live request，返回一份 live 与一份原 validator 复用结果。严格 fencing 把 network fetch
  与 cache publish 分开，takeover 后旧 owner 的 publish callback 不执行，避免 lease state 正确但 cache
  pointer 被旧 owner 覆盖。
- 2026-07-20：final validation PASS：module/cache/provider focused=`13/38/118 passed`；
  architecture=`446 passed / 35.84s`；contract=`265 passed / 151.39s`；full=`6470 passed / 2 skipped /
  642 warnings / 975.18s`。Full provenance=`natural_integration_boundary`、scheduler applied=true、
  fallback=false、tail idle max=`15.57s`；相对最近 969.84s 基线约 +0.55%，新增协调测试未进入
  slowest 50，无异常性能回归。任务归档 `DONE`，`production_effect=none`、`broker_action=none`。
- 2026-07-20：owner 指示按双线 Wave 1 继续推进，engineering lane 从 base=`8bf2b86c` 启动。
  owned scope 为 coordination policy/module 与 multiprocess/failure tests；shared cache 接线、task register、
  system flow、generated manifests 和 formal gates 由 integration coordinator 单写。策略 lane 同时只读
  恢复 TRADING-2449 evidence，二者不得互改 shared path；`production_effect=none`。
- 2026-07-20：OPS-064 设计审计确认 content-addressed body + atomic pointer 只保证并发完整性，
  不保证到期边界只有一次 live revalidation；登记为独立后续，避免把跨进程协调偷偷塞入缓存
  生命周期切片并扩大风险。
