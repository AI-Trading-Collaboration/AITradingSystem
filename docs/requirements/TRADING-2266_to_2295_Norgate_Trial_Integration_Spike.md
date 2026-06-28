# TRADING-2266 to 2295 Norgate Trial Integration Spike

最后更新：2026-06-28

## Status

- Task id: `TRADING-2266_to_2295_NORGATE_TRIAL_INTEGRATION_SPIKE`
- Status: `VALIDATING`
- Last updated: 2026-06-28
- Owner: project owner

## Scope

本批只做 Norgate US Stocks Platinum Trial 的工程接入和数据能力验证。
用户已开通 3 周 trial，访问方式为 Windows Python。Trial 页面提示 daily price
history limited to 2 years，因此本批不得宣称完成 2021-02-22 primary window 的完整
historical breadth 验证。

目标是验证或 fail-closed：

- Python package import and local database access。
- Nasdaq-100 / NDX historical membership query。
- Delisted securities visibility。
- Date x symbol daily membership snapshot summary。
- Trial 2Y price coverage 内的 breadth prototype。
- PIT / leakage / cache governance。
- 是否值得 owner 人工批准正式 Platinum 订阅。

## Non-Goals

- 不购买正式 Platinum。
- 不自动升级套餐。
- 不提交 vendor raw data、本地 Norgate cache、账号密码或完整 member symbol list。
- 不恢复 first-layer channel research、v4 或 minimal forward diagnostic。
- 不训练模型，不输出 target weights、trade advice 或 allocation。
- 不进入 owner review、paper-shadow、production 或 broker。

## Stages

1. `TRADING-2266_to_2269_SCOPE_ENV_SOURCE_CONTRACT`
   - Scope、environment contract、source contract、safe connector 和 CLI smoke test。
2. `TRADING-2270_to_2273_MEMBERSHIP_DELISTED_PRICE_SNAPSHOT`
   - Membership query probe、delisted visibility probe、price coverage probe、
     daily membership snapshot summary prototype。
3. `TRADING-2274_to_2277_BREADTH_PIT_GOVERNANCE_VOI`
   - 2Y-only breadth prototype、PIT/leakage audit、cache/artifact governance、
     trial value-of-information review。
4. `TRADING-2278_to_2279_DECISION_OWNER_BRIEF`
   - Paid Platinum decision gate and Chinese owner brief。
5. `TRADING-2280_to_2282_REGISTRY_TEST_VALIDATION`
   - Report registry、artifact catalog、system flow、task register、guardrail tests
     and validation。
6. `TRADING-2283_to_2295_CLOSEOUT`
   - Closeout and final matrix with allowed Norgate trial final statuses。

## Acceptance Criteria

- Norgate package/local DB availability can be checked without printing credentials.
- Missing package or missing local DB fails closed and does not fail the full suite.
- Raw data paths are gitignored and no raw vendor data is committed.
- Trial 2Y price limitation blocks primary-window model-ready status.
- Membership/delisted/price/snapshot/prototype outputs are derived summaries only.
- Paid Platinum decision gate requires owner manual approval before purchase.
- Report registry、artifact catalog、system flow and task register are updated.
- Focused pytest、Ruff、compileall、contract-validation and diff checks pass.

## Open Questions And Blockers

- 当前运行环境可能没有安装 Norgate Windows Python package 或本地 Norgate database。
  这是允许的 trial spike blocker，必须 fail-closed 写入 summary。
- Trial daily price history limited to 2 years，不能覆盖 2021 primary window 或 2022
  stress slice 的完整 price join。
- Norgate Python API 的 index naming 需要 adapter 层映射，业务代码不得散落 vendor
  symbol。
- 任何正式订阅、sample download、本地 raw cache 或 primary-window validation 需要 owner
  人工批准。

## Progress Log

- 2026-06-28: Task created from owner attachment. Implementation starts with a
  safe connector, fail-closed CLI, environment/source contracts, derived summary
  artifacts, governance docs, paid Platinum decision gate and guardrail tests.
- 2026-06-28: Implementation completed and moved to `VALIDATING`. Added
  `aits data norgate trial-smoke-test|membership-probe|trial-pack`, safe
  Norgate adapter, membership snapshot and 2Y breadth prototype summaries,
  source/environment contracts, PIT/cache governance, paid Platinum decision
  gate, owner brief, closeout/final matrix, report registry, artifact catalog,
  system-flow update and guardrail tests. Local trial-pack run is fail-closed
  because this machine has no `norgatedata` package/local database:
  final status `NORGATE_TRIAL_INCONCLUSIVE`, package/access
  `NORGATE_ENV_MISSING_PACKAGE`, membership query not validated, delisted
  visibility not confirmed, price coverage and breadth prototype blocked.
  Promotion, paper-shadow, production and broker remain disabled. Validation
  passed: Ruff, compileall, focused Norgate pytest, adjacent paid-data and
  first-layer gate regressions, report/documentation/task-register tests,
  research audit/governance tests and `contract-validation`.
