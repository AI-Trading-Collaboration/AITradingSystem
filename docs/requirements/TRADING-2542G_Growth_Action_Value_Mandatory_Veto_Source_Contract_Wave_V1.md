# TRADING-2542G：Growth Action-Value Mandatory Veto Source-Contract Wave V1

## 1. 状态与 Owner 决策

- task id：`TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1`；
- priority：`P0`；
- governed mode：`SINGLE_LANE` serial consumer-contract wave；
- current status：`IN_PROGRESS_NON_EXECUTABLE_SOURCE_CONTRACT_DRAFT`；
- Owner decision：
  `owner_decision:TRADING-2542F-2542G:2026-08-25:approve_exact_architecture_freeze_and_source_contract_followup_v1`；
- 授权边界：只允许本地、result-blind、non-executable `DATA_RESEARCH` 合同与 synthetic
  validation；不授权 veto series、R1 manifest、provider/cache query、真实 DQ、backtest、
  orders、fills、positions、paper、live、production 或 broker action。

Owner 已逐项接纳并精确冻结以下已发布草案，原文件必须保持 immutable：

| authority | file SHA-256 | canonical SHA-256 |
| --- | --- | --- |
| architecture V1 | `9b4856614298d64b2c8b5897980735a9e2a19c46fecb6c2362cb750ae13b136d` | `88e1283b0333bafca24779c9c527d362acef40b65d4cff1a9d081ded07ac70e4` |
| legacy compatibility map V1 | `c5867551aec4f152256219e4fb19b7c52ec5a6b7f8d8c316961d33a75749679d` | `067a6b23daa1bfff22a6d4f4fcb773346a7d866e21cf2adb759acde75d04f524` |

本任务使用独立 freeze-admission artifact 记录上述决策，不修改已批准草案的 bytes、status、
version 或 canonical identity。

## 2. 问题定义

TRADING-2542F 已冻结新 consumer 的分层：DQ、QQQ/SGOV action guard、四个正交 market-state
veto、option alpha、optional option-risk diagnostic 与 next-session join。当前真正阻塞 R1 manifest
的不是 option alpha，而是四个 mandatory veto 仍缺少完整、独立、PIT 可审计的 source contract。

旧链路不能直接晋升为 successor authority：

- `risk_off_veto` 由 `growth_allowed` 反向 alias，违反 alpha-to-veto dependency ban；
- `volatility_veto` 的 legacy role 虽标为 ready，但当前 runtime 中 `0.55/0.25` 仍是未独立冻结的
  threshold authority，不能把“旧代码可调用”误写为“successor exact source contract 完备”；
- macro event feature 当前是 `PIT_WARNING` / `diagnostic_only`，缺 scheduled release 的
  published-at/available-at authority 与 exact threshold；
- `trend_break_veto` 没有 callable、versioned、独立的 QQQ-underlying producer。

因此本波次先机械区分 architecture readiness、candidate evidence 与 successor admission。不得用
constant false、retained-series truncation、cross-date fallback 或 placeholder hash 消除阻塞。

## 3. 目标与非目标

### 3.1 目标

1. 生成 architecture exact-freeze admission，绑定 Owner decision 与四项精确 SHA；
2. 建立四类 mandatory veto 的统一 source-contract schema/readiness artifact；
3. 对每类机械要求：`source_contract_sha256`、`independent_producer_identity`、
   `formula_category`、`decision_as_of`、`available_at`、`missing_terminal` 与
   `exact_1202_session_inventory`；
4. 只承认有证据的当前状态，并为未满足字段返回 typed blocker；
5. 拒绝 selected pair、option alpha、candidate result、`growth_allowed` alias 与结果后 bucket；
6. 在任一 mandatory source 未完整 admitted 时，停止于 R1 manifest 之前；
7. 更新 system flow 与 canonical task state，使阻塞原因可审计。

### 3.2 非目标

- 不生成任何实际 veto boolean/series；
- 不选择、校准、搜索或回填 threshold/formula；
- 不查询本地 market cache、provider、QuantConnect 或其他外部平台；
- 不运行真实 DQ、backtest 或绩效比较；
- 不修改 execution V1/V2、DQ/PIT V3、exact sheet V4、architecture V1 或 compatibility V1；
- 不把期权 selected CALL/PUT activity 变成 mandatory market-state veto。

## 4. 四类 source readiness 合同

| successor veto | architecture evidence | 本波次 admissibility | 最小 blocker |
| --- | --- | --- | --- |
| `broad_market_risk_off_veto` | independent broad-market source required | blocked | independent producer、formula、timing 未冻结；`growth_allowed` alias 禁止 |
| `realized_volatility_veto` | legacy source role compatible | blocked for successor admission | exact threshold/formula authority 与独立 producer binding 未冻结 |
| `scheduled_event_risk_veto` | scheduled event source required | blocked | published-at/available-at PIT contract、event set、window/threshold 未冻结 |
| `underlying_trend_break_veto` | independent QQQ-underlying source required | blocked | callable producer、formula/threshold 与 timing adapter 未冻结 |

每个 row 必须显式包含 readiness 与 required identity fields。只有全部字段为非空、exact identity
一致、dependency scan PASS、1202-session inventory contract admitted 时，才可进入
`SOURCE_CONTRACT_ADMITTED_SERIES_NOT_GENERATED`；本任务不得产生该状态。

`missing_terminal` 固定为 `INSUFFICIENT`，malformed authority 固定为 `INVALID`。两者都不得转换为
market-clear `false`。最终 join 仍要求 DQ=`PASS`、action guard=`PASS` 且四个 veto 全部为 exact
`false`，option alpha 才能在 next valid QQQ session 被消费。

## 5. Artifact 与 validator 设计

新增：

- `config/research/qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1.yaml`：
  Owner exact-freeze admission，只绑定已批准的 architecture/compatibility bytes；
- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_source_contract_wave_v1.yaml`：
  四类 readiness、candidate evidence role、required identity 与 stop policy；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_source_contract.py`：
  strict typed loader、canonical identity、root containment、immutable binding 与 dependency checks；
- `tests/test_growth_action_value_mandatory_veto_source_contract.py`：
  exact-freeze、typed blocker、forbidden dependency、missing-as-clear、series/manifest/external safety
  negative tests。

Validator 必须重放 TRADING-2542F architecture loader，而不是只相信配置中复制的哈希；所有绑定文件
必须位于 project root、不是 symlink、file/canonical SHA 同时匹配。artifact 不包含 series、return、
weight、backtest id 或 result-derived value。

## 6. 分阶段实施与验收

### S0：任务登记与 freeze admission

- 将 TRADING-2542F 更新为 `COMPLETE`，记录 Owner exact-freeze 与精确哈希；
- canonical register 本任务；
- 保留原 architecture/compatibility bytes 不变。

### S1：source-contract readiness artifact

- 四个 successor veto 全部存在且顺序固定；
- architecture evidence 与 successor admission 分离；
- 不完整 row 返回稳定 blocker code；
- 当前 aggregate terminal 固定为
  `BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS`。

### S2：验证与发布

- focused/adjacent、Ruff、strict mypy、py_compile PASS；
- Architecture、Contract、Integration、Reproducibility 与 Full PASS；
- generated task views、Atlas 与 system flow authority fresh；
- local `main` ordinary fast-forward，remote ordinary non-force push；
- task branch 在 evidence canonical 后清理。

### S3：Owner 后续 exact-freeze（本任务之后）

对每个 source 单独审阅 producer、formula category、threshold、decision-as-of/available-at、missing
terminal 与 exact inventory。任何一个不完整，都继续 fail closed；不得因此运行真实数据或 backtest。

## 7. Path、contract 与 evidence claims

task-owned paths：本 requirement、两份新 config、typed loader 与 focused tests。

coordinator-owned paths：TRADING-2542F/2542G canonical fragments、task index/views、
`docs/system_flow.md`、Atlas/architecture/report-flow generated authority及其 exact freshness tests。

evidence roles：

- architecture/compatibility V1=`OWNER_EXACT_FROZEN_IMMUTABLE_AUTHORITY`；
- current runtime/capability graph/free-feature modules=`READINESS_OR_CANDIDATE_EVIDENCE_ONLY`；
- diagnostic/PIT-warning event features=`NOT_ADMITTED_MANDATORY_VETO_SOURCE`；
- new source-contract artifact=`NON_EXECUTABLE_READINESS_AUTHORITY_NO_SERIES`。

## 8. Lifecycle 与安全边界

- frozen base：`1a3c3ef6eb51292de25bcf452aeacf4f0d20f012`；
- branch：`codex/trading-2542g-veto-source-contracts`；
- workspace：复用 `D:\Work\AITradingSystem`，不创建额外 worktree/clone/cache；
- known-unrelated exclusion：`docs/research/growth_tilt_owner_diagnosis_pack.md` 不读、不 hash、不
  diff、不 stage、不修改；
- external action=`none`，production effect=`none`，broker action=`none`；
- recovery：tracked bytes 由 Git/main 恢复；没有 provider、dataset、backtest 或 broker state；
- exit condition：validated non-executable readiness artifact 发布，后续具体 source formula/threshold
  仍以独立 Owner exact-freeze 为前提。

## 9. 进度记录

- 2026-08-25：Owner 同意已解释的后续方案，并精确冻结 architecture/compatibility 上述四项 SHA；
  READ_ONLY audit 确认 main=origin/main=`1a3c3ef6eb51292de25bcf452aeacf4f0d20f012`、active
  lease=0、worktree audit PASS。选择 SINGLE_LANE serial contract wave；未读取 provider/cache，
  未生成 series/manifest，未运行真实 DQ/backtest，orders/fills/positions/production/broker=0。
- 2026-08-25：TRADING-2542F 已通过 canonical task event 转为 `DONE`，并同时保留原 requirement 与
  本 successor requirement 的连续审计链接；TRADING-2542G 已以 P0/`IN_PROGRESS` 登记。三个仅用于
  task-source 写入的 publication transaction 均在完成受控 canonical mutation 后按“非中间发布”释放，
  未生成候选、未运行 Full、未推送；正式候选仍只允许由最终单一 publication transaction 发布。
- 2026-08-25：freeze admission file/canonical SHA-256=
  `414626c92a140a780b602c122adce17d0039ec0f9bcd6fa326b9518ffa35ed83`/
  `6cc4ab50beed6f9b2e89e969099203cf684cb7021600d2fffa5b7c44b46712c6`；source-contract wave
  file/canonical SHA-256=`76e38c969ee0849c77ac4012b72d0e65115f0a3448ecb276c9ca8cfef5faf8b5`/
  `0f8204170b4c8810cf2685e63dd5035801cef79788932b63cdf5691c1ba28e26`。当前 aggregate=
  `0/4 admitted`，terminal=`BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS`。
  focused/adjacent source-contract tests=`57 passed`，单模块复核=`19 passed`，task-source self-hosted=
  `9 passed`；Ruff、strict mypy、py_compile PASS。
- 2026-08-25：Atlas consumer audit 发现 live policy 仍把 2542F 显示为等待 Owner freeze；scope-expansion
  LANE preflight PASS 后已把 2542F 分类为 exact-frozen、把 2542G 设为 current mainline/largest blocker/
  next legal action，并将 coverage 72→73。未执行官方 generator 时 Atlas focused=`46 passed, 1 failed`，
  唯一失败精确指出 ignored canonical page/sidecar 仍是旧 72-task bytes；按 generated-authority fence 规则
  不在 lane 中私自改写，留待最终 publication transaction 的 `atlas-authority` generator 重建后复核。
- 2026-08-25：final publication transaction v1 完成 task-source update 与 architecture-manifests 重建后，
  `atlas-authority` 在写 page 前以 `ATLAS_PAGE_SOURCE_WORKTREE_DRIFT:config/atlas/live_snapshot.yaml`
  fail closed。原因是新 page source 尚未形成 exact lane commit，不能把旧 HEAD 冒充新 source identity；
  v1 已释放、未生成 candidate、未写新 canonical page、未运行 Full。下一事务先绑定包含全部 page source 的
  精确 lane head，再从 `canonical-task-source` 起完整重放五类 generator；不绕过 exact-commit check。
