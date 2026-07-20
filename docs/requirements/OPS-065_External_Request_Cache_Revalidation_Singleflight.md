# OPS-065：External request cache 到期复验 singleflight

最后更新：2026-07-20

状态：`READY`

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

- 2026-07-20：OPS-064 设计审计确认 content-addressed body + atomic pointer 只保证并发完整性，
  不保证到期边界只有一次 live revalidation；登记为独立后续，避免把跨进程协调偷偷塞入缓存
  生命周期切片并扩大风险。
