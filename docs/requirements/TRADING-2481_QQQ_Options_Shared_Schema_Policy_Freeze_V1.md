# TRADING-2481：QQQ Options Shared Schema / Policy Freeze V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2481_QQQ_OPTIONS_SHARED_SCHEMA_POLICY_FREEZE_V1`

优先级：`P0`

状态：`BASELINE_DONE`

Owner 决定：
`owner_decision:TRADING-2481:2026-08-02:freeze_shared_schema_policy_v1_without_thresholds_or_platform_actions`

退出标识：`QQQ_OPTIONS_SHARED_CONTRACT_V1_REVIEWED`

production effect：`none`

broker action：`none`

## 1. 目标

本任务是 QQQ options research capability 的最小串行 contract wave。它在任何 signal exporter、
QuantConnect adapter、合约选择、成交模拟、组合会计或 cloud pilot 实现之前，冻结跨模块共享的记录合同、
序列化规则、单位、时区、lineage、license/export 分类与安全边界。

本波次只定义结构和机械不变量，不替 Project Owner 决定 DTE、moneyness、delta、spread、OI、volume、
premium budget、position cap、fill/slippage、fee、expiry buffer、reconciliation tolerance 或 promotion gate。
这些会影响投资解释的值必须由后续 reviewed policy 独立冻结。

## 2. 依赖与准入边界

- 父规划：`TRADING-2478_QUANTCONNECT_QQQ_DAILY_OPTIONS_BACKTEST_CAPABILITY_TECHNICAL_PLAN_V1`；
- admission predecessor：`TRADING-2480_QC_QQQ_OPTIONS_CAPABILITY_LICENSE_EVIDENCE_SPIKE_V1`；
- `TRADING-2480` offline baseline 已完成，但真实 QuantConnect entitlement/license/evidence probe 仍因
  `owner_authorization:NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS` blocked；
- 未闭合字段必须能够表达 `UNKNOWN_REQUIRES_LICENSE_REVIEW`，不得把 public docs 或 schema presence
  冒充账户级 license confirmation；
- active research default 仍从 `2021-02-22` 开始，但本任务不请求、评估或运行任何日期窗口；
- 后续 `TRADING-2482..2493` 必须消费本合同或通过 reviewed breaking-change wave 升级，不能复制一套
  不兼容 schema。

## 3. 统一 envelope

所有记录共享 `QQQOptionsRecordEnvelope`，至少固定：

- `schema_name`、`schema_version`；
- `run_id`、`record_id`、`created_at_utc`；
- `producer_version`、`repository_code_sha`；
- `policy_id`、`policy_version`、`policy_sha256`、`contract_schema_sha256`；
- `source_ids`、`source_checksums`；
- `requested_start/end` 与 `evaluated_start/end`；
- `storage_timezone=UTC` 与 `exchange_timezone=America/New_York`；
- `dq_status`、`pit_status`；
- `export_classification`、`lineage_id`；
- canonical semantic payload 的 `content_sha256`。

主键、ID、SHA-256、时间、日期、Decimal、枚举和集合顺序必须被 typed validators 约束。canonical JSON
使用 UTF-8、sorted keys、固定缩进、LF 结尾、禁止 NaN/Infinity；内容哈希计算时排除自身
`content_sha256`，receipt/replay 必须 exact-byte 一致。

## 4. V1 共享记录合同

|schema|主键/顺序|冻结语义|
|---|---|---|
|`run_manifest`|`run_id`|research window、initial cash、account currency/type、underlying、resolution、signal/code/policy/engine/evidence identity；不包含策略收益。|
|`daily_signal`|`run_id + signal_session`|`signal_as_of_utc`、generated time、earliest effective session、`LONG_CALL/LONG_PUT/FLAT`、source lineage；不得定义 signal mapping 阈值。|
|`contract_candidate_snapshot`|`run_id + selection_snapshot_utc + option_sid`|right/expiry/strike/multiplier/DTE、moneyness、prior-day model freshness、daily OI freshness、quote validity、eligibility、field export classification；raw market rows可标记 QC-only。|
|`selection_decision`|`run_id + decision_id`|selected SID 或 typed no-contract reason、candidate digest、stable rank components/rejected counts；不冻结 rank 数值阈值。|
|`order_intent`|`run_id + intent_id`|side、quantity、order type/limit、cash reservation、not-before time、selection lineage；V1 仅允许 long-premium intent。|
|`order_event` / `fill_event`|platform order + sequence|submit/update/cancel/reject/partial/fill、UTC 时间、quantity、price per share、multiplier、fees、quote lineage、settlement；不得允许 daily-close fill。|
|`position_lifecycle_event`|`run_id + position_id + sequence`|状态迁移、quantity/cash delta、expiry/exercise/assignment/corporate-action reason；非法迁移 fail closed。|
|`portfolio_snapshot`|`run_id + snapshot_utc`|settled/unsettled/reserved cash、option market value、fees、realized/unrealized P&L，全部为 Decimal USD。|
|`dq_report`|`run_id + scope + report_version`|coverage、missing sessions、quote/OI/Greeks freshness、calendar/mapping/PIT checks及 typed reason codes；本任务不放宽 `aits validate-data`。|
|`platform_evidence_manifest`|`run_id + bundle_id`|platform/backtest/tier/engine identity、artifact checksums、collector、license/export classification、limitations；禁止 secret/account/broker ID 和 raw rows。|
|`reconciliation_report`|`run_id + check_id`|local/platform value、delta、unit、tolerance policy reference、difference class、explanation、status；不冻结 tolerance 数值。|

## 5. 单位、时间与数值不变量

- 存储权威时间一律是 timezone-aware UTC；exchange-local 字段必须显式为
  `America/New_York`，不得使用 naive datetime；
- 交易 session 使用 ISO date，不把周末或假日自行平滑为交易日；
- 金额、价格、strike、quantity、fees、P&L 使用 finite `Decimal`，canonical 输出为普通十进制字符串；
- option premium 是每股价格，cash impact 必须显式乘 `contract_multiplier`；
- `contract_multiplier` 必须为正整数，V1 不把 100 作为不可验证的实际值；
- `repository_code_sha` 使用 lowercase Git object SHA（当前 SHA-1 repository 为 40 characters，未来
  SHA-256 repository 为 64 characters）；policy、schema、source、artifact checksum 使用 lowercase
  64-character SHA-256；
- `requested_start <= evaluated_start <= evaluated_end <= requested_end`；没有 evaluated range 时必须使用
  typed blocked/incomplete 状态，而不是虚构日期；
- signal、selection、intent、submit、fill 的跨记录事件序由 `TRADING-2482` 进一步冻结，本任务先提供
  必需字段，不提前实现 adapter chronology。

## 6. Enums 与安全边界

V1 至少冻结以下稳定 enum：

- signal：`LONG_CALL / LONG_PUT / FLAT`；
- option right：`CALL / PUT`；
- capability/export：`QC_ONLY_NOT_EXPORTED / EXPORT_ALLOWED_DERIVED /
  UNKNOWN_REQUIRES_LICENSE_REVIEW / EXPORT_PROHIBITED`；
- DQ/PIT：`PASS / FAIL / NOT_EVALUATED`；
- order side：`BUY_TO_OPEN / SELL_TO_CLOSE`；
- order lifecycle：`CREATED / SUBMITTED / UPDATED / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED`；
- position lifecycle：`FLAT / INTENT_PENDING / OPEN_PARTIAL / OPEN / EXIT_PENDING / EXIT_BLOCKED /
  CLOSED / SCOPE_VIOLATION / INVALID_RUN`；
- reconciliation：`PASS / EXPLAINED_DIFFERENCE / FAIL / INCOMPLETE`。

所有 policy 和 record 必须固定：

```text
research_only=true
promotion_allowed=false
paper_shadow_allowed=false
production_allowed=false
broker_action=none
production_effect=none
raw_options_data_export_allowed=false
strategy_execution_allowed=false
bounded_cloud_pilot_authorized=false
```

## 7. Governed policy envelope

`qqq_options_shared_contract_v1.yaml` 必须包含：

- `policy_id/version/status/owner`；
- `rationale/intended_effect`；
- `validation_plan`；
- `review_condition/expiry_condition`；
- supported schema names/versions、canonical contract schema SHA-256 和稳定 enum；
- currency、premium unit、timezones、hash/canonicalization contract；
- allowed export classifications 与 raw-field prohibition；
- safety boundary；
- `investment_thresholds_frozen=false` 以及后续 Owner-review exit condition。

policy loader 必须 exact-byte 计算 SHA-256，并拒绝 missing/extra schema、enum drift、错误单位/时区、危险
safety flag、unknown policy status 或任何已填入但没有 reviewed threshold authority 的数值启发式。

## 8. 实施范围

Task-owned：

- 本 supporting requirement；
- `src/ai_trading_system/qqq_options_research/__init__.py`；
- `src/ai_trading_system/qqq_options_research/contracts.py`；
- `src/ai_trading_system/qqq_options_research/policy.py`；
- `config/research/qqq_options_shared_contract_v1.yaml`；
- `tests/test_qqq_options_shared_contract.py`；
- 对应 architecture module/flow fragments。

Coordinator-owned：

- `docs/task_register.md`；
- `docs/system_flow.md`；
- architecture generated manifests、deprecation inventory、compatibility authority；
- task-shadow generated state；
- formal validation artifacts。

明确不实现：

- QuantConnect login/project/API/CLI/cloud run/data download；
- option universe selection、fill engine、portfolio accounting runtime、expiry/exercise处理；
- signal mapping、任何投资阈值或收益计算；
- live/paper/broker/production path。

## 9. 阶段与验收

### S0：合同与 policy freeze

- supporting requirement、policy manifest、统一 envelope、12 类 record models 完成；
- schema name/version、PK、enum、单位、时区、Decimal、SHA、lineage 和 export classification 受验证器约束；
- license 未闭合状态显式保留。

### S1：Canonical replay 与 breaking-change tests

- 每类 record 可 canonical serialization / exact-byte replay；
- content hash、policy hash、record id/PK 可重算；
- naive/future-invalid time、float/NaN、错误单位/币种、duplicate source、bad SHA、extra field、unsafe flag、
  raw export、illegal lifecycle state、schema/enum drift 全部 fail closed；
- policy 不含投资阈值，threshold authority 缺口继续可见。

### S2：Shared wiring 与 formal closeout

- `docs/system_flow.md` 和 architecture fragments 同步；
- task shadow、DevEx、deprecation、append-only compatibility authority 可 deterministic rebuild；
- focused、Architecture、Contract、Integration、Reproducibility、Full 按自然 integration boundary PASS；
- validated task commit ff-only 到 local main，CLOSEOUT PASS 后 ordinary push，最终
  `local main = origin/main = candidate`。

## 10. Governed execution 与生命周期

- mode：`SINGLE_LANE` dedicated serial contract wave；
- `contract_change=true`；
- frozen base：`5bb71658c38da3104f9f198c9156d8f4153abe41`；
- branch：`codex/trading-2481-qqq-options-shared-contract`；
- 复用 `D:/Work/AITradingSystem_ops073_integration` checkout，不创建新 worktree/clone/cache；
- branch exit condition：final commit 已 ff-only/push 到 main、canonical runtime evidence 保留、worktree
  audit clean、无活动进程，随后删除任务分支；
- external platform action：`none`；
- `production_effect=none`、`broker_action=none`。

## 11. 进度记录

- 2026-08-02：TRADING-2480 offline baseline 已普通 push 到 exact main
  `5bb71658c38da3104f9f198c9156d8f4153abe41`；外部 Owner token 未授予，但 shared schema 可以用
  `UNKNOWN_REQUIRES_LICENSE_REVIEW` 保持 fail-closed 继续串行冻结。
- 2026-08-02：Owner 要求“先推进”；跨任务协调确认本 QQQ 任务独占 TRADING-2481，Atlas 线不写本
  task/shared generated paths。SINGLE_LANE START/LANE preflight 均 PASS，active lease=[]，任务进入
  `IN_PROGRESS`；未访问 QuantConnect、未下载数据、未运行回测。
- 2026-08-02：12 个 typed record model、共用 envelope/public enums、canonical `seal()` / exact-byte
  `from_json_bytes()`、contract-schema hash binding、reviewed policy、安全边界、architecture fragments 与
  system flow 已实现。Contract SHA-256=`c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`，
  policy SHA-256=`d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；task focused
  `28 passed`，adjacent governance bundle `98 passed`，DevEx 与 task shadow v1/v2 deterministic rebuild
  PASS。任务转 `BASELINE_DONE`，等待 final-tree formal gates；后继 TRADING-2482 必须消费本合同，
  license/external platform gap 继续由 TRADING-2480 admission receipt fail closed。
- 2026-08-02：首轮 final Architecture 正式暴露 76 个由同一 successor-authority 缺口级联的失败：
  TRADING-2481 尚未接管 task shadow v2 / ARCH-004G inventory 漂移。未绕过门禁；扩展 coordinator claim
  后追加 immutable TRADING-2481 compatibility section，约束 2478/2479/2480 predecessor 到 2481 的
  双向 authority 差集，并以显式 `PYTHONPATH=src;.` 重算当前 worktree inventory。deprecation inventory
  固定为 `1067 modules / 1234 test files / 856 direct writers / 0 violations`，完整 compatibility +
  deprecation 回归 `178 passed`；下一步重跑正式 Architecture。
