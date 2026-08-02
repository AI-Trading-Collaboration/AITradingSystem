# TRADING-2483：QQQ Options Signal / Run Manifest Export V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2483_QQQ_OPTIONS_SIGNAL_RUN_MANIFEST_EXPORT_V1`

优先级：`P1`

状态：`BASELINE_DONE`

Owner 指令：
`owner_instruction:2026-08-02:proceed_with_offline_qqq_options_engineering`

退出标识：`INTERNAL_QQQ_OPTION_SIGNAL_PACKAGE_READY`

production effect：`none`

broker action：`none`

## 1. 目标

本任务把已经规范化为 `LONG_CALL / LONG_PUT / FLAT` 的 QQQ 日级研究信号封装为 immutable、可重放、
可供后继 QuantConnect adapter 消费的离线 run package。package 只包含 2481 冻结的
`DailySignalRecord`、`RunManifestRecord` 和 derived/export-safe receipt，不选 option contract、不读取 raw
option rows、不模拟订单或成交、不计算收益，也不访问 QuantConnect。

本任务同时把 local cached-data quality、signal point-in-time、reviewed exchange calendar、source bytes、
policy/code/contract hashes 和 requested/evaluated range 绑定到同一个 canonical artifact inventory。后继模块
不能用文件名、目录日期或未审计 CSV 的 `date` 字段代替 PIT 证据。

## 2. 精确基线与继承 authority

- frozen base：`4ccdd86641b14ab8b6076934e003e260a7a3bddb`；
- predecessor：`TRADING-2481_QQQ_OPTIONS_SHARED_SCHEMA_POLICY_FREEZE_V1` 与
  `TRADING-2482_QQQ_OPTIONS_DQ_PIT_CACHE_EVIDENCE_IDENTITY_V1`；
- shared contract SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- DQ/PIT policy SHA-256：
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；
- 只从 `qqq_options_research.contracts` 导入 2481 public enums、safety boundary、
  `DailySignalRecord`、`RunManifestRecord` 与 canonical seal/replay；
- 只从 2482 导入 reviewed DQ/PIT policy loader 和 `LocalCachedDataGateDeclaration`，不复制或改写其
  15 checks、reason taxonomy、chronology、cache identity 或 threshold UNKNOWN 语义；
- quote freshness、spread、OI、volume、DTE、moneyness、delta、premium budget、position cap、fill、fee 和
  reconciliation tolerance 继续未冻结，本任务不得填入任何数值。

## 3. Signal 输入边界与未决 mapping

V1 输入必须已经是 typed normalized signal，并显式提供：

- `signal_session`；
- `source_data_cutoff_utc`，作为 2481 `signal_as_of_utc`；
- `generated_at_utc`；
- `signal=LONG_CALL / LONG_PUT / FLAT`；
- source artifact id、locator、exact-byte SHA-256；
- canonical `aits validate-data` execution receipt 的 content-addressed repo path、expected as-of、reviewed
  policy path 与 exact input-role set。builder 必须调用
  `verify_data_quality_execution_receipt()` 同一路径重新读取 receipt、report、policy、validator sources、
  invocation 与所有 bound inputs，随后才可从 verified receipt 事实派生 local DQ declaration；调用者不能
  自报 `LocalCachedDataGateDeclaration(PASS)`。

现有 ETF signal 的 `bullish / bearish / neutral` 到 options signal 的映射仍没有 reviewed Owner token。
因此 V1 policy 固定：

```text
input_mode=PRE_NORMALIZED_ONLY
etf_signal_mapping_status=UNKNOWN_REQUIRES_OWNER_REVIEW
etf_signal_mapping_allowed=false
```

代码必须拒绝 `bullish / bearish / neutral` 和任何调用者注入的 mapping table，不能默认为
`bullish→LONG_CALL`、`bearish→LONG_PUT`、`neutral→FLAT`。Owner 后续若批准映射，必须更新 reviewed policy、
本 requirement、task register 和相应 negative/golden tests；不能仅改 adapter 条件分支。

## 4. Lag=1、calendar 与 PIT

本任务继承任务登记时已冻结的 `lag=1 / no-lookahead` 验收要求，并把它解释为：

1. `signal_session` 必须是 reviewed US equity exchange calendar 的有效 session；
2. `source_data_cutoff_utc` 的 exchange-local 日期必须等于 `signal_session`，且不得早于该 session 的
   reviewed close time；
3. `generated_at_utc >= source_data_cutoff_utc`，且 exchange-local 日期仍必须等于 `signal_session`；
4. `earliest_effective_session` 必须是 `signal_session` 后的首个有效 exchange session；
5. package creation time 可以晚于 signal generation，但不能改变上述 effective session；
6. 周末、regular holiday、reviewed special closure 与 partial session 使用现有
   `ai_trading_system.trading_calendar` authority，不按 calendar day 猜测。

evaluated window 内每个有效 exchange session必须恰有一条规范化 signal。duplicate session、missing
session、non-session、range 外记录、逆序或 source cutoff 缺失全部 fail closed；`FLAT` 必须显式记录，不能
用缺行代表。

## 4.1 Primary research window authority

- 新 primary package 的 `requested_start` 与 `evaluated_start` 默认且必须等于 `2021-02-22`；
- `PRIMARY` role 不允许调用者附带其他 authority，也不允许把 `2022-12-01` 或任意其他日期设为默认；
- 不同 start 只能使用 `PROXY / SENSITIVITY / STRESS` role，并引用 2483 exact policy 中登记的
  `REVIEWED_ACTIVE` authority；该 authority 必须含 exact role、requested/evaluated start、Owner decision、
  rationale、DQ caveat 与 review condition；
- V1 policy 的 `approved_non_primary_window_authorities=[]`，因此当前 baseline 对所有非 primary start
  fail closed。新增例外必须经过 reviewed policy change，不能由调用参数临时构造。

## 5. Immutable package 合同

每个 package 使用调用者提供的 portable `run_id`，输出固定目录结构：

```text
<output_root>/<run_id>/
  daily_signals/<YYYY-MM-DD>.json
  signal_index.json
  run_manifest.json
  package_receipt.json
```

- 每个 daily signal 文件是 2481 `DailySignalRecord.canonical_bytes`；
- `signal_index.json` 是 sorted path + SHA-256 的 canonical inventory；
- `RunManifestRecord.signal_artifact_sha256` 必须等于 exact `signal_index.json` bytes 的 SHA-256；
- `package_receipt.json` 是 task-owned typed receipt，至少绑定 run manifest、signal index、每个 signal、
  source artifact、local DQ report、calendar、2481 contract/shared policy、2482 DQ/PIT policy、2483 export
  policy、repository code SHA 和 safety boundary；
- package receipt 与 index 使用 UTF-8、sorted keys、indent=2、LF、no NaN/Infinity，并提供 exact-byte replay；
- receipt 同时绑定 canonical DQ execution receipt id/path/SHA/size、由其派生的 report locator/SHA/as-of、
  research window role 与可选 reviewed non-primary authority；
- output directory 不存在时可原子创建；已存在且所有 bytes 完全一致时是 idempotent PASS；任何现存 byte
  不同、额外文件、缺失文件或目录 traversal 都 fail closed，禁止覆盖或部分修补；
- artifacts 均为 `EXPORT_ALLOWED_DERIVED`；raw options data 继续只能
  `QC_ONLY_NOT_EXPORTED / EXPORT_PROHIBITED`，且本 package 不包含它们。

`RunManifestRecord` 的 V1 runtime facts 固定为 QQQ、USD CASH、DAILY signal、MINUTE execution；
`initial_cash_usd` 是每次 run 的必填正 Decimal，不在全局 policy 中硬编码。因为外部 Owner token 尚未授予，
本任务生成的 manifest 必须保持 engine identity 未确认、evidence admission blocked、research-only、无
strategy execution authority。

## 6. DQ / PIT 双轴与状态解释

- package 生成前必须调用 canonical receipt verifier；它要求 receipt/content-addressed path、canonical bytes、
  exact policy/validator/calendar/input/invocation/report projection 全部一致并执行 `assert_strict_passed()`。
  `FAIL`、`PASS_WITH_WARNINGS`、unknown schema/status、fake PASS、arbitrary report bytes、scope/as-of/hash/window
  mismatch 全部停止；verified receipt 的 `checked_at/report path/report SHA` 才能派生
  `LocalCachedDataGateDeclaration(status=PASS, scope=CACHED_MARKET_MACRO, ...)`；
- daily signal / run manifest 的 `dq_status=PASS` 只表示 signal source package 的 local-cache gate、coverage、
  schema、identity 和 checksum 通过，不表示 future option-event DQ PASS；
- `pit_status=PASS` 只表示 signal cutoff/generation/effective-session chronology 通过，不表示 selection、
  intent、submit、fill quote 或 fill chronology 已评估；
- package receipt 必须显式写出 `option_event_dq_status=NOT_EVALUATED` 与
  `option_event_pit_status=NOT_EVALUATED`，防止两条轴相互替代；
- 2482 的 `UNKNOWN` 永不变成 PASS，且 source report 缺失、hash mismatch 或 scope 不符不得降级为 warning。

## 7. Governed policy

`config/research/qqq_options_signal_export_v1.yaml` 必须 exact-byte 加载并绑定：

- policy id/version/status/owner、Owner instruction、rationale/intended effect；
- validation plan、review/expiry condition；
- 2481 contract/shared policy 和 2482 DQ/PIT exact hashes；
- input mode、approved signal enum、lag/effective-session/coverage rules；
- primary start=`2021-02-22`、primary role、非 primary role/authority/DQ-caveat contract，以及
  `2022-12-01 is default=false`；
- package layout、canonicalization 和 artifact classifications；
- ETF mapping unresolved 状态；
- no external platform、raw export、selection、execution、accounting、return calculation 或 promotion 的 safety
  boundary。

loader 必须拒绝 extra/missing key、hash drift、enum drift、layout drift、安全 flag 放宽、mapping table、
investment threshold 或 numeric option heuristic。

## 8. 文件权属

Task-owned：

- 本 supporting requirement；
- `config/research/qqq_options_signal_export_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/signal_package.py`；
- `tests/test_qqq_options_signal_package.py`；
- `config/architecture/fragments/modules/qqq_options_signal_package.yaml`；
- `config/architecture/fragments/flows/qqq_options_signal_package.yaml`；
- 必需的 append-only compatibility/deprecation authority tests。

Coordinator-owned：

- `docs/task_register.md`；
- `docs/system_flow.md`；
- `inputs/architecture/**` generated manifests/inventories；
- `registry/development_tasks_shadow/**` 与 v2；
- formal validation artifacts。

明确不修改：

- 2481 `contracts.py`、`policy.py`、`__init__.py` 与 shared policy；
- 2482 `dq_pit_identity.py` 与 DQ/PIT policy；
- existing ETF signal generation/mapping logic；
- QuantConnect project/API/CLI/account、cloud/paper/live/broker、raw options rows；
- contract selection、order/fill、cash accounting、position lifecycle 或收益计算。

## 9. 阶段、依赖与验收

### S0：registration / policy / typed receipt

- task row 与本 supporting requirement 建立；
- governed `SINGLE_LANE` START/LANE preflight 从 exact base PASS；
- exact-byte export policy loader、normalized input、index/receipt model 实现；
- 2481/2482 exact hash 和 safety inheritance tests PASS。

### S1：canonical build / replay / negative coverage

- daily signals、index、run manifest、package receipt deterministic build 与 exact-byte replay PASS；
- duplicate/missing/non-session/range/cutoff/chronology、fake-PASS/arbitrary DQ bytes、semantic
  FAIL/PASS_WITH_WARNINGS/unknown、wrong DQ scope/as-of/hash/window、bad source checksum、float cash、
  mapping injection、unsafe flags、path traversal、pre-existing mismatch、tamper 和 extra field fail closed；
- primary default window、unreviewed pre-window、`2022-12-01` not-default 与 reviewed non-primary authority
  matching tests PASS；
- holiday/special closure/partial session 和 lag=1 golden/property coverage PASS；
- focused tests 使用 `pytest -n 16 --dist loadfile`。

### S2：architecture / formal closeout

- `docs/system_flow.md` 与 architecture fragments 同步；
- DevEx、task shadow v1/v2、compatibility/deprecation 和 generated manifests deterministic fresh；
- Architecture、Contract、Integration、Reproducibility、Full 在 final candidate 串行 PASS，Full 独占；
- commit、ff-only local main、CLOSEOUT、ordinary non-force push 后
  `local main = origin/main = candidate`；
- 任务分支/worktree 按生命周期规则清理，向 TRADING-2484 回传 exact SHA、冻结 API/hashes 与 evidence。

## 10. Governed execution 与生命周期

- mode：`SINGLE_LANE`；
- `contract_change=true`，因为新增 consumer-visible offline package layout/policy，但不修改 2481/2482 contract；
- frozen base：`4ccdd86641b14ab8b6076934e003e260a7a3bddb`；
- branch：`codex/trading-2483-qqq-options-signal-package`；
- 复用 clean primary checkout，不创建新 worktree/clone/cache；
- branch exit：final commit 已验证、ff-only/push 到 main、canonical evidence 已保存、checkout audit clean、无
  active gate，然后删除 task branch；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不读取、不 hash、不 stage；
- external Owner token：`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`；
- `research_only=true`，promotion/paper-shadow/production/raw export/strategy execution/cloud pilot 均 false，
  `production_effect=none`、`broker_action=none`。

## 11. 进度记录

- 2026-08-02：TRADING-2482 已完成 ordinary push 和 cleanup，exact base
  `4ccdd86641b14ab8b6076934e003e260a7a3bddb`，正式五级门禁 PASS，资源释放。2483 现登记并仅获授权
  推进 offline derived/export-safe package；ETF direction mapping 仍未获 Owner 批准，保持 fail closed。
- 2026-08-02：协调审查给出 `COORD-2483-DQ-WINDOW-V1` STOP-before-formal。实现调整为直接消费 canonical
  `DataQualityExecutionReceipt` verifier capability，不再信任调用者 declaration/report bytes；同时把 primary
  `2021-02-22` 与非 primary reviewed-role/DQ-caveat authority 写入 exact policy。修复完成前不运行 formal tiers。
- 2026-08-02：offline package baseline 已实现。canonical DQ execution receipt 会在 builder 内通过与
  `aits validate-data` 相同的 verifier 路径重验 report/policy/validator/calendar/input 的 schema、status、scope、
  as-of 与 checksum，并从已验证事实派生 downstream declaration；伪造 PASS、semantic FAIL/UNKNOWN、scope/
  as-of/hash mismatch 均 fail closed。primary requested/evaluated start 固定为 `2021-02-22`，当前未登记任何
  reviewed non-primary authority，`2022-12-01` 明确不是默认值。focused signal/package tests=`25 passed`，
  与 2481/2482 邻接合同合计=`85 passed`；Ruff、mypy、DevEx writer/freshness 与 task-shadow deterministic
  generation 均 PASS。正式五级门禁仍须在 final tracked tree 串行完成，外部平台、option event 数据、选约、
  交易、收益和 ETF direction mapping 继续不在本 baseline 能力内。
