# ARCH-004 Wave15：D0B3 + G4B 首个 Consumer / G3 收口准备

最后更新：2026-07-25

状态：`BASELINE_DONE`

稳定任务 ID：`ARCH-004W15_D0B3_G4B_G3_CLOSE_PARALLEL_READINESS`

current stage=`S3_FORMAL_EXIT_COMPLETE`

Owner 决策：
`owner_decision:ARCH-004-WAVE15:2026-07-25:approve_narrow_d0b3_g4b_g3_close_v1`

## 决策上下文

Wave14 已以 replacement Full=`7007 passed / 4 skipped`和 post-Full evidence-only gates完成
D0B2 + bounded G3 formal closeout。随后
`owner_decision:ARCH-005S4D:2026-07-24:approve_narrow_s0_s1_v1`批准并完成同 checkout
write/operations lease guard，failure-fix Full=`7136 passed / 3 skipped`。

2026-07-25，Owner 明确工程线继续推进。本任务把该指令收敛为既有路线中最窄的 Wave15：

- Data/Operations lane：`D0B3 + G4B first consumer`；
- Reporting lane：关闭 Wave14 的 bounded G3 slice，并形成后续 G5 所需的 contract readiness；
- coordinator：先从授权基线最终 HEAD 生成 exact readiness carrier，再分配实现。

本授权不是 S5、全量 consumer cutover、外部 scheduler、真实 periodic execution、provider refresh、
G5、production 或 broker 授权。

## C / D 精确载体序列

为避免 readiness evidence 自引用或绑定未提交工作树，Wave15 采用两提交载体：

- `C`：承载 owner authorization、任务/需求、C/D 双态合同与 generated task/devex facts；
- `D`：承载 reviewed Wave15 policy 与 exact readiness evidence，且只允许新增下面两个文件：
  - `config/architecture/arch_004_wave15_d0b3_g4b_g3_close_readiness.yaml`；
  - `inputs/architecture/arch_004_wave15_d0b3_g4b_g3_close_parallel_readiness.json`。

`D` 必须从已提交并推送的 `C` HEAD 构建。两文件必须原子同在或同缺；policy/evidence 进入 Git
history 后不得再把缺失态当作合法 C。readiness 必须校验 base/tree、task rows、requirement bytes、
generated state、ownership、change manifests、known-unrelated exact exclusion 和全部 safety false/none
边界。`D` PASS 并提交推送前，domain assignment 与实现写入均为 false。

## Lane 1：D0B3 + G4B `daily_score_daily`

首个且唯一获批的 consumer identity 固定为 `daily_score_daily`。`daily_validate_data` 仍是 receipt
producer observation，不算本次 consumer；weekly/monthly consumer 不在授权内。

### 合同

1. 新的 reviewed per-consumer profile 必须绑定：
   consumer id/version、exact DQ execution receipt、D0B2 immutable publication companion
   attestation、dataset/source lineage、requested/actual window、policy/validator/input/report bytes。
2. 历史 receipt/publication bytes 和其中原有 `consumer_cutover_allowed=false` 不得改写；D0B3
   通过独立、可撤销、consumer-scoped authorization attestation 表达本次窄授权。
3. 默认只接受 strict `PASS`。`PASS_WITH_WARNINGS` 继续 fail closed；未来如需例外，必须另有 reviewed
   typed warning profile 和 owner decision，不能在本波隐藏放宽。
4. missing、FAIL、warning、expired、as-of drift、profile drift、publication/receipt/source/tamper、
   consumer mismatch 时，必须在 runner 调用及下游 artifact 写出前阻断：
   `runner_calls=0`、`downstream_artifacts=0`。
5. `periodic_consumer_migration` 只可对 `daily_score_daily` 暴露受控 native dispatch；其余四个
   representative identities 继续 `dispatch_authorized=false`。automatic non-daily dispatch=false。

### 验收

- fake clock/runner 证明 due/not-due、duplicate、lock、retry/resume、typed preflight 与 lineage parity；
- `ops_daily -> cli_direct -> daily discovery` 具有真实跨层代码路径回归，但本波验证使用隔离 fixture/
  fake runner，不执行真实 daily operation、provider 或 cache mutation；
- legacy path 与新受控 path 在同输入下 command/artifact/status parity，且 authorization removal 可回滚；
- production effect、broker action、external scheduler mutation 均为 none。

## Lane 2：G3 bounded-slice close / G5 readiness

本 lane 的“G3 收口”只关闭 Wave14 已授权的 bounded reporting slice，并冻结后继 contract readiness；
不表示一次迁完剩余 9 个 generic provider，也不启动 G5。

### 固定范围

- 复核 `data_quality_and_pit` native pure projector 的事实来源、19-field legacy parity、JSON/HTML bytes、
  report registry/fragment 和 Reader Brief single-owner contract；
- 证明历史 F3 raw SHA 与当前 `reader_brief.py` exact SHA/LOC/function-count ratchet 持续有效，
  legacy `_data_quality_pit_safety` 不可回生；
- 生成 G3 close/readiness evidence：列出剩余 9 个 generic provider 的 consumer inventory、
  ownership、依赖与 G5 前置 contract，明确 `migration_executed=false`；
- 不重算 score、threshold、策略结论或 DQ/PIT facts，不改变报告投资解释。

### 退出边界

只有 bounded slice 的 P0/P1 findings 关闭、parity/ratchet/readiness evidence 可重建、formal gates PASS，
才可将该 slice 标为 complete。G5 仍需后续独立 owner authorization 和 exact carrier。

## Ownership 与并行边界

Wave15 readiness 通过后最多两个互斥 domain scope：

|lane|主要 owned scope|禁止写入|
|---|---|---|
|D0B3/G4B|consumer authorization contract/profile、DQ publication binding、operations adapter及专属 tests|task register、system flow、central CLI/runbook、generated manifests|
|G3 close/readiness|reporting close/readiness artifact、validator及专属 tests|Reader Brief/Owner Daily central cut-in、task register、central manifests|
|Coordinator|CLI/runbook/system flow/catalog/register、shared contracts/exports、compatibility/generated state|不得夹带新策略、阈值或生产切换|

所有 shared path 保持单写。checkout mutation 必须持有效 S4D lease；known-unrelated 只能按 exact
path 排除，guard 不得读取、hash、复制或暂存其 bytes。

## 分阶段计划

|阶段|内容|退出条件|
|---|---|---|
|S0-C|授权与治理基线|本需求、任务登记、双态 test、generated facts 与 compatibility PASS；C commit/push|
|S0-D|exact carrier|两文件 deterministic build/validate PASS，绑定 C HEAD/tree；D commit/push|
|S1-D0B3/G4B|首个 consumer 实现|reviewed consumer profile/attestation、strict verifier、isolated parity/tamper/rollback tests PASS|
|S1-G3|bounded close/readiness|single-owner/parity/ratchet复核与剩余 inventory/readiness artifact PASS|
|S2|shared integration|CLI/runbook/system flow/catalog/register/generated/compatibility 同步；combined focused PASS|
|S3|formal exit|architecture、contract、integration、report、reproducibility 与唯一 final Full PASS|

## 安全字段

整个 Wave15 固定：

- `dispatch_allowed=false`（generic/machine dispatch）；
- `automatic_command_dispatch=false`；
- `automatic_non_daily_dispatch=false`；
- `lease_acquisition_allowed=false`（readiness carrier 本身不发 domain lease）；
- `task_registry_mutation_allowed=false`（machine mutation）；
- `consumer_cutover_scope=[daily_score_daily]` 且任何其他 consumer=false；
- `real_periodic_operation_executed=false`；
- `provider_refresh_executed=false`；
- `g5_authorized=false`、`s5_authorized=false`；
- `production_effect=none`、`broker_action=none`。

## 进展记录

- 2026-07-25：S3 formal exit完成。最终pre-Full门禁为combined/architecture/contract/integration/
  report/reproducibility=`233/615/275/993/57/23 passed`；首次Full=`7179 passed / 1 failed /
  3 skipped`只捕获test-local精确白名单漏项，带父链failure-fix Full=`7180 passed / 3 skipped /
  643 warnings`。证据收口后的post-Full focused/architecture/contract/integration/
  reproducibility=`243/615/275/993/23 passed`，runtime behavior source未再改变，无需第二个自然
  Full。Wave15转`BASELINE_DONE`；其他consumer、真实periodic/provider、G5、S5、production与
  broker仍未授权，下一波必须由owner另行选择。
- 2026-07-25：S3首次combined run为`175 passed / 9 failed`；9项均指向尚未刷新
  compatibility current authority。收口检查同时发现旧deprecation reference scanner会读取已由
  ARCH-005S4D明确排除的user-owned文档，因此直接修复scanner复用exact exclusion policy并补零读取
  回归；不以跳过inventory/architecture/Full作为替代。generated/compatibility刷新后必须重跑同一
  combined与全部formal tiers。
- 2026-07-25：首次architecture tier为`610 passed / 3 failed`。一项是新consumer contract test
  合法构造verifier capability但未进入reviewed private-factory test whitelist，已只增加该test path；
  另两项是Wave14/Wave15已提交C/D carrier test仍尝试用实施后的dirty worktree重放“实施前guard”。
  新增明确的committed-carrier validator，只验证C/D direct-child、两文件exact Git blob、canonical
  evidence及dependency binding，不重放post-carrier worktree；live evidence validator仍保持原有
  fail-closed语义。三项修复后必须以failure-fix reason重跑architecture tier。
- 2026-07-25：上述三项修复后定向回归`37 passed`；第二次architecture tier为
  `612 passed / 1 failed`。唯一失败来自ARCH-005S4D Windows仲裁owner的瞬时
  `PermissionError`误分类，不涉及D0B3/G4B/G3或策略计算。已在ARCH-005S4D同一任务边界登记
  有界读取稳定化修复；修复后必须继续并行architecture复验，不得以serial PASS替代。
- 2026-07-25：Windows仲裁修复后architecture=`615 passed`、contract=`275 passed`；
  integration首次为`992 passed / 1 failed`。唯一失败是scheduled daily链测试仍逐字断言旧
  `score-daily`命令，遗漏D0B3要求的
  `--consumer-authorization-profile daily_score_daily@1.0.0`。修复范围仅为把该集成断言升级为精确
  要求reviewed profile token；runtime命令不回退，缺少/错误token继续在runner前fail closed。
- 2026-07-25：pre-Full最终门禁为combined/architecture/contract/integration/report/reproducibility
  `233/615/275/993/57/23 passed`。首次Full=`7179 passed / 1 failed / 3 skipped`，唯一失败为
  `test_data_quality_execution_contract.py`内另一份机械私有factory扫描白名单未纳入
  `tests/test_data_quality_consumer_authorization.py`；reviewed runtime policy与Wave12架构白名单均已
  正确。只把同一精确test path加入该Full-only断言，随后以首个Full artifact为parent执行
  failure-fix Full；不得放宽src importer或改用通配符。

- 2026-07-25：S0-C/D正式完成。authorization commit
  `3030114be1c07b71eab5af2d8cbf4f54325cb2ef`与direct-child carrier commit
  `7ec6fd713b0e676607e38522be36b8e4d6c20d55`均已推送；carrier policy/evidence canonical
  SHA=`50ba37193bf3aef67439e16a9cf3dd3183bb2e20df84fc63ea075293923a2e51`，direct validate
  PASS，carrier focused=`40 passed`。
- 2026-07-25：S1 domain与S2 shared integration实现完成。D0B3新增reviewed singleton policy、
  canonical content-addressed consumer attestation与verifier-only capability，绑定exact receipt、
  D0B2 publication、source/window/policy/validator/input/report lineage，24小时reviewed TTL且可撤销；
  G4B仅为`daily_score_daily`开放controlled native dispatch，其他四身份与automatic non-daily保持
  false。真实代码路径为`ops_daily -> cli_direct -> daily discovery -> receipt/publication verify ->
  consumer attestation -> controlled runner`，isolated fixture证明任一missing/warning/FAIL/expiry/
  identity/profile/source/tamper均`runner_calls=0`、`downstream_artifacts=0`。
- 2026-07-25：G3 close/readiness evidence重验历史F3 raw SHA、当前Reader Brief
  SHA=`b2a089e8…`/LOC=`29005`/top-level functions=`366`、legacy builder=`0`、
  native/generic=`1/9`、19字段与fragment/single-owner边界；剩余9项的source keys、owner与前置
  typed contract已登记，全部`migration_executed=false`，`g5_authorized=false`。domain focused
  合计=`73 passed`，shared combined focused=`152 passed`，Ruff/Black/strict scoped mypy PASS；
  未运行真实daily、provider或cache mutation，现进入S3 formal exit。
- 2026-07-25：Owner 批准工程线继续推进；coordinator 将其登记为上述窄版 Wave15，开始 S0-C。
  在 D exact carrier PASS 并推送前，不进行 domain assignment；策略线 TRADING-2458 仍保持独立
  ownership，不共享 threshold、candidate、report conclusion 或 prospective 权限。
