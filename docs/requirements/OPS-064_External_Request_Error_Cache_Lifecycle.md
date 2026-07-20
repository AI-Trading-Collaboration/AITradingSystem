# OPS-064：External request error cache 生命周期

最后更新：2026-07-20

状态：`DONE`

## 背景与目标

现有 `external_request_cache.v1` 会把 HTTP 4xx/5xx 与成功响应一样长期复用。由于 API key 等
敏感凭据被正确排除出 request identity，同一 identity 在凭据恢复、quota reset 或 provider
恢复后仍可能持续 HIT 旧失败，形成 fail-closed 但无法自行恢复的 poisoned cache。

最佳修复不是静默删除 failure response，也不是完全不记录失败；而是把“审计保留”与“可复用
期限”分开：失败 response 永久保留为不可变 observation，但只在 reviewed pilot TTL 内可复用；
到期后下一请求必须 live revalidate。显式 invalidation 只使目标 generation 不再可复用，不能删除
body、metadata 或 observation。

## Policy 与阈值治理

新增 `external_request_cache_lifecycle_policy.v1`，状态为 `pilot_baseline`，owner 为 data platform
owner + operations owner。TTL 是恢复速度、provider quota 与重复失败流量之间的模型政策，必须写入
manifest 而非散落在代码中：

|HTTP 状态|pilot TTL|理由|
|---|---:|---|
|408 / 425 / 429 / 5xx|300 秒|瞬时、拥塞、限流或服务端失败应尽快重验|
|401 / 403|300 秒|credential 不进入 identity，避免凭据恢复后继续命中旧失败|
|404 / 410|900 秒|资源缺失通常较稳定，但不能永久缓存 provider 暂态路由结果|
|其他 4xx|3600 秒|请求级错误可短期抑制重复流量，但仍需有界恢复|
|<200 或 >599|0 秒，仅证据|非标准状态不作为可复用 HTTP 结论|

如果响应带合法 `Retry-After`，瞬时类 TTL 取 policy TTL 与 Retry-After 的较大值，但上限为
21600 秒；避免违反 provider retry signal，也避免永久冻结。policy review condition 为：30 天运行
观测、provider quota/429 分布或任何 poisoned-cache recurrence；exit condition 为 owner 依据实际
observations 批准稳定 TTL 或替换本 pilot。

2xx/3xx 正缓存沿用当前持久复用语义。本任务只治理 HTTP status；HTTP 200 内嵌 provider semantic
error 是否进入独立 semantic-negative policy 另行登记，不在本任务中偷偷扩大范围。

## 存储与并发契约

### 不可变内容与 current pointer

- response body 写入 `bodies/<body_sha256>.body`，同内容天然去重且不可原地覆盖；
- root `metadata.json` 是当前 generation pointer，记录 generation id、body path/checksum、status、
  created/expires、policy id/version/class；
- 每个负响应另写 `negative_observations/<generation_id>.json`，永久保留该次失败的 status、headers、
  body checksum、request identity、TTL 与 source timestamps；
- legacy v1 `response.body` 与 metadata 必须继续可读；首次新写可升级 current pointer，不批量删除旧 cache。

### 原子与并发

- body、observation、event 与 pointer 使用 unique temp + flush/fsync + atomic replace；
- body/observation 是 content/generation-addressed，多 writer 不会互相覆盖；
- current pointer 采用 last-completed-writer-wins，但 pointer 只有在目标 body checksum 已落盘后才能发布；
- lookup 必须同时校验 schema、cache key、body path containment、size/checksum、generation 与 policy；
  任一异常均 MISS/fail closed，不能解释为成功数据。
- 本切片不实现到期边界的跨进程 singleflight；多个进程可能同时 live revalidate，但所有 immutable
  evidence 与最终 pointer 保持完整。该额度/长尾风险已登记
  `OPS-065_EXTERNAL_REQUEST_CACHE_REVALIDATION_SINGLEFLIGHT`，不得误写为已解决。

### 到期与显式失效

- lookup 返回 `HIT`、`MISS`、`EXPIRED_REVALIDATE` 或 `INVALIDATED_REVALIDATE`；到期/失效不删除证据；
- 显式 invalidation 必须提供 actor、reason、reference、expected generation/body checksum，使用 CAS
  拒绝 stale target；写 `lifecycle_events/<event_id>.json` 与原子 `invalidation.json`；
- 新 live response 产生新 generation，旧 invalidation 不自动作用于新 generation；
- 无 actor/reason/reference、checksum 不匹配或 path 越界必须 fail closed。

## 实施拆解

|步骤|内容|验收|
|---|---|---|
|E1|policy manifest 与 typed evaluator|所有 status/Retry-After/TTL 分支有确定性测试，无硬编码投资/运营启发式|
|E2|v2 content-addressed writer、legacy reader、negative observations|旧 v1 可读；failure 原始 body/metadata 永久保留；tamper/path escape fail closed|
|E3|expiry/revalidation 与 CAS invalidation API|首次 failure、TTL 内 HIT、到期 live revalidate、显式失效、新 generation 解锁全部覆盖|
|E4|requests/urllib 与 FMP/Marketstack/FRED/SEC 契约回归|错误响应不会解释成成功；provider wrapper 保持 fail closed|
|E5|runbook、artifact catalog、system flow、task evidence 与 manifests|运行边界和 operator action 可审计，`production_effect=none`|

## 验收标准

- 4xx/5xx 不再永久 HIT；所有 policy 分支与 Retry-After cap 可复算；
- failure evidence、body checksum、request identity 与 lifecycle event 在 revalidate/invalidate 后仍保留；
- 并发写不产生 metadata/body mismatch，stale CAS invalidation 被拒绝；
- legacy v1 cache、requests 与 urllib wrapper 行为向后兼容；
- FMP/Marketstack/FRED/SEC focused tests、Ruff、architecture/contract 与风险相称的 full PASS；
- 不删除现有 cache、canonical state/ledger，不绕过 data quality gate，不运行 periodic operation，
  `production_effect=none`。

## 状态记录

- 2026-07-20：OPS-062 工作区审计发现 HTTP failure 可永久 HIT，登记 OPS-064。
- 2026-07-20：只读设计审计确认 TTL-only 会丢失审计与并发完整性，任务进入 `IN_PROGRESS`；
  采用 immutable negative observation + content-addressed body + atomic pointer + expiry/CAS invalidation。
- 2026-07-20：明确当前并发退出边界为 storage consistency，不包含跨进程 singleflight；后者登记为
  OPS-065，以免 OPS-064 在实现中无审计地扩大为分布式协调任务。
- 2026-07-20：实现完成并进入 `VALIDATING`。Focused provider/cache/CLI/strategy integration 为
  81 passed；report/docs/deprecation focused 为 26 passed；contract-validation 为
  265 passed / 149.33s。未运行网络或 periodic operation，等待 final architecture/full gate。
- 2026-07-20：formal closeout PASS 并归档 `DONE`。Architecture=446 passed / 35.11s；full=
  6456 passed / 2 skipped / 643 warnings / 969.84s；runtime profile/performance/telemetry/provenance
  均 PASS，scheduler applied、fallback=false、tail idle max=15.33s。Full 相对最近 972.33s 基线约
  -0.26%，新增测试未进入 slowest 50。未运行网络、periodic operation 或 cache deletion；
  `production_effect=none`。
