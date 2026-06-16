# TRADING-368 Data Source Fallback Policy

最后更新：2026-06-16

## 背景

Paper-shadow research 依赖 cached market / macro data、PIT source manifest、data refresh audit、evidence staleness monitor 和 shadow continuation readiness。当前系统已经能披露数据质量、PIT grade 和 refresh audit，但还缺少统一规则说明：当 primary source 不可用时，是否允许 fallback、fallback 是否合格、以及 fallback 是否已经在下游 artifact metadata 中显式暴露。

本任务建立 read-only fallback policy artifact。它只定义和报告 fallback 状态，不下载数据、不改 cache、不运行评分或回测、不写 official target weights、不触发 broker/order，也不放宽 fail-closed policy。

## 范围

### In scope

- 新增 data-source fallback policy config 和 policy document。
- 输出 fallback states：`PRIMARY_OK`、`FALLBACK_USED`、`FALLBACK_UNAVAILABLE`、`BLOCKED_NO_VALID_SOURCE`。
- 定义 source priority、source eligibility 和 fallback metadata requirements。
- 新增 `run` / `report` / `validate` CLI。
- 生成 JSON report、Markdown report、validation JSON/Markdown、Reader Brief section 和 latest pointer。
- 接入 data refresh audit、PIT source manifest、evidence staleness monitor 和 shadow continuation readiness 的只读 summary。
- 登记 report registry、artifact catalog、README、system flow、operations runbook、requirements index 和 task register。
- 补充 focused tests。

### Out of scope

- 不新增或切换真实 data provider。
- 不把 fallback 数据写入 primary cache。
- 不补造任何 source artifact、download manifest、quality report、market panel 或 paper-shadow artifact。
- 不改变 strategy、score、backtest、paper account、production state、broker integration 或 order ticket。
- 不引入 silent waiver；fallback used 必须在 JSON 和 Reader Brief 中显式显示。

## Source Priority And Eligibility

Fallback policy 以 `config/data_source_fallback_policy.yaml` 为 source policy manifest。每个 data domain 定义 primary sources、eligible fallback sources、ineligible sources、metadata requirement 和 fail-closed behavior。

Eligibility baseline:

- source 必须存在于 `config/data_sources.yaml`。
- source status 必须是 `active`。
- source domain 必须覆盖对应 data domain。
- source type 允许 `primary_source` 或 `paid_vendor`。
- `public_convenience` 和 `manual_input` 默认不得作为 paper-shadow research fallback，除非未来 owner 审核后在 policy 中明确记录理由、验证证据和 expiry/review condition。
- Fallback used 必须写出 source priority、primary source unavailable reason、fallback source id、provider、endpoint、request parameters、download timestamp、row count、checksum、metadata fields、source artifact path 或明确说明 artifact 尚未生成。

## State Semantics

|State|含义|下游解释|
|---|---|---|
|`PRIMARY_OK`|Primary source 可用，未使用 fallback。|可继续由其它 data quality / freshness gate 决定是否可解释。|
|`FALLBACK_USED`|Primary source 不可用或被显式标记 unavailable，且合格 fallback 已被显式使用并披露 metadata。|不得静默替换 primary；Reader Brief 和 JSON 必须显示 fallback source、reason 和 metadata status；paper-shadow readiness 至少需要人工复核。|
|`FALLBACK_UNAVAILABLE`|Primary 不可用，policy 中存在 fallback candidate，但没有合格且显式 metadata 的 fallback artifact。|fail closed；不得继续把对应 data domain 当作有效输入。|
|`BLOCKED_NO_VALID_SOURCE`|Primary 不可用，且没有任何合格 fallback candidate。|fail closed；需要恢复 primary 或 owner 审核新增合格 source。|

## Acceptance Criteria

- `aits data fallback-policy run --as-of YYYY-MM-DD` 能生成完整 artifact。
- `aits data fallback-policy report --latest` 能只读 latest artifact。
- `aits data fallback-policy validate --latest` 能 fail closed 校验状态、eligibility、metadata 和 safety boundary。
- `FALLBACK_USED` 时 JSON report 和 Reader Brief section 必须显示 fallback source、primary source、reason、provider、endpoint、request parameters、download timestamp、row count、checksum、metadata status 和 `production_effect=none`。
- `FALLBACK_UNAVAILABLE` / `BLOCKED_NO_VALID_SOURCE` 必须在 evidence staleness 或 readiness 中进入 blocking data path。
- Data refresh audit 和 PIT source manifest 必须显示 fallback policy summary。
- Report registry / documentation contract 不因新增 artifact 缺失而产生未登记项。
- Focused pytest、ruff、compileall 和 git diff check 通过。

## Progress Notes

- 2026-06-16: 新增需求文档并进入实现；当前阶段只建立 fallback governance artifact 和只读集成，不改变数据下载、cache 写入、评分、回测或 paper-shadow strategy behavior。
- 2026-06-16: 实现完成并归档为 DONE；新增 fallback policy config、CLI、artifact writer/validator、Reader Brief summary、report registry/artifact catalog/README/system flow/runbook/requirements 集成，以及 data refresh audit、PIT source manifest、evidence staleness monitor 和 shadow continuation readiness 的只读 fallback summary。真实 artifact `data-source-fallback-policy_2026-06-16_55a3f5b7f6152f52` 为 `PASS` / `PRIMARY_OK`，验证通过 focused pytest、Ruff、compileall、documentation contract、report index、Reader Brief/quality 和 git diff check。
