# TRADING-2511：QQQ Options Primary Window Derived Calibration Evidence Generator V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2511_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_CALIBRATION_EVIDENCE_GENERATOR_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2510 admission/readiness contract 之后，建立一个可实际运行、确定性、可重放的离线
derived calibration evidence generator。它消费数据源侧已经完成去标识与汇总的 primary-window
per-session observations，依据 versioned metric-definition catalog 生成 2510 可直接 admission 的
slot evidence records、evidence references 与 package manifest。

本任务不下载、接收或导出 raw option rows，不执行 QuantConnect/cloud/API/CLI/HTTP/Object Store 动作，
不选择或建议 DTE/moneyness/delta/spread/OI/volume/quote freshness/fee/slippage/latency/partial-fill/
sizing/cash/acceptance 数值，不把 G3 转换为 G2，不激活 selection、engine 或 backtest。

## 2. 冻结基线与继承

- registration base / latest main：`062b21087aaaa398e3ff575fb65348bd8ed31819`；
- 2510 policy file SHA-256：
  `d19b2de2eec00759cb1e5b85ab976858518205ba933e121324e871e78120d7fd`；
- 2510 implementation file SHA-256：
  `343f9ebd9f8afb9cbf00f7b39309c334ea395ecad7a6c6f9563085a73d101c49`；
- 2510 exact scope：18 个 Owner-selected G3 slots；
- primary research start：`2021-02-22`；exchange calendar：`XNYS`；
- 继承 2481 shared records/envelope、2482 DQ/PIT identity、2499 DAILY chronology、2500 reviewed
  capability、2509 v2 migration/attestation 与 2510 catalog/admission/readiness；不得复制或弱化这些
  public contracts。

## 3. 输入合同

generator 只接受 canonical sealed source bundle：

- requested/evaluated start 必须为 `2021-02-22`，end 不得倒置；
- session inventory 必须等于 reviewed XNYS calendar 的完整 evaluated sessions；
- provider、dataset、source checksum、repository code SHA 与 DQ report identity 必须显式绑定；
- 每个 session/slot/statistic observation 必须为 canonical finite decimal、带 unit、sample count 与
  metric-definition identity；
- 输入顺序可以任意，但 canonical package identity 必须与输入排列无关；
- duplicate session/slot/statistic、未知 slot/statistic、unit/aggregation drift、missing session、range/as-of/
  source/hash mismatch 全部 fail closed；
- `derived_export_safe=true`、`contains_raw_option_rows=false`、`raw_options_data_exported=false`、
  `external_action_performed=false`、`investment_interpretation_generated=false` 为硬不变量。

source bundle 中某个 slot 不存在表示 production evidence 尚未提供，不得自动填零或把缺失转换为 PASS。

## 4. Metric-definition catalog

task-owned policy 为 exact 18 个 G3 slots 定义：

- metric definition id/version；
- evidence class 与 canonical group（必须与 2509/2510 mechanical derivation 一致）；
- 每个 statistic 的 id、unit 与 aggregation operator；
- operator 仅允许 `SUM`、`MIN`、`MAX`，不包含阈值、cutoff、rank weight、quantile grid 或推荐区间；
- `SUM` 只聚合可加计数/量，`MIN/MAX` 只描述 observed envelope；
- 输出 statistic 的 `is_policy_value=false`，任何 observed envelope 都不得解释为建议 policy range。

catalog 状态固定为 `OWNER_REVIEW_REQUIRED_ENGINEERING_BASELINE`。若未来需要均值、分位数、候选网格、
scenario weight 或 acceptance rule，必须通过独立 reviewed methodology policy，不得在本任务静默加入。

## 5. DQ/PIT 与 2510 admission

generator 必须：

1. 解析 canonical 2481 `DQReportRecord`，不信任调用者自报 PASS；
2. 重验 2482 scope/policy/contract/repository/source/range 与 exact 15-check PASS；
3. 为每个生成的 slot evidence 写入同一 DQ/source/session lineage；
4. 使用 2510 `build_qqq_options_primary_window_calibration_evaluation` 重放生成结果；
5. 只有 2510 admission 成功后 package manifest 才能为 `GENERATED_AND_ADMITTED`；
6. DQ `FAIL`/`UNKNOWN`/`NOT_EVALUATED`、伪造 PASS、hash/scope/as-of mismatch 均停止整个 package，
   不输出部分成功假象。

## 6. 输出

实现 deterministic sealed：

1. source observation bundle；
2. per-slot generated evidence records；
3. evidence reference index；
4. generator package manifest；
5. 2510 catalog/receipt/readiness/handoff 的 exact cross-binding。

production source inventory 为空时，不写 production evidence，2510 页面状态继续为
`EVIDENCE_NOT_PROVIDED_POLICY_BLOCKED`。测试 fixture 只证明 generator mechanics，不得进入 production
inventory、Owner review 或投资解释。

## 7. 安全不变量

所有输出保持：

- `owner_policy_value_count=0`；
- `executable_policy_authorized=false`；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `selection_authorized=false`；
- `orders=0`、`fills=0`；
- `external_action_authorized=false`；
- `investment_interpretation_allowed=false`；
- `production_effect=none`、`broker_action=none`。

## 8. 实现与验证计划

### S0：registration / authority audit

- canonical task row + supporting requirement；
- governed START/LANE preflight；
- 2510 hashes、18-slot inventory、DQ/PIT 与 no-threshold audit。

### S1：generator contract

- task-owned policy、strict loader、typed observation/source/package models；
- canonical seal/from-json/replay；
- deterministic statistic aggregation 与 2510 admission adapter。

### S2：fail-closed coverage

- unit/property/golden：permutation identity、partial/complete scope、duplicate/unknown、operator/unit drift、
  missing session、pre-window、range/source/repository/hash mismatch、forged/failed/unknown DQ、raw-row flags、
  symlink/path escape、package tamper 与 fixture/production separation；
- generated evidence 的 statistic 全部 `is_policy_value=false`；
- no external action / order / fill assertions。

### S3：共享 wiring 与收口

- system flow、architecture fragments、task registry/generated/compatibility authority；
- Atlas 只披露“generator 已实现但 production evidence inventory 仍为空”，不得把工程绿色解释成策略有效；
- focused/adjacent/compatibility 后，在 final tree 串行完成
  Architecture→Contract→Integration→Reproducibility→exclusive Full；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 9. 验收标准

1. 任意顺序的合法 source observations 生成 byte-identical evidence/package。
2. exact 18-slot metric catalog 与 2509/2510 authority 一致，未知或越权 slot 无法生成。
3. 生成结果必须经 canonical DQ/PIT 与 2510 admission 重验，不能由调用者声明 PASS。
4. production input 缺失时保持 evidence inventory=0，不用 fixture 或默认零伪造完成度。
5. 无新增投资阈值、无 raw export、无 engine/backtest/external/production/broker action。
6. focused、compatibility、generated authority 与 final five-tier gates PASS。

## 10. 当前 blocker / 后继

当前 blocker：`PRIMARY_WINDOW_DERIVED_SESSION_AGGREGATES_NOT_PROVIDED`。

generator 完成后，需要一个独立授权的数据源侧 derived-aggregate collection run 提供真实 primary-window
source bundle；之后 2510 admission 才能形成可交付 Project Owner 的 evidence review pack。只有 Owner 对每个
slot 另行提供 typed G2 value、rationale、evidence refs 与 review/expiry metadata，才可进入 executable-policy
serial contract wave。
