# TRADING-2542G：Growth Action-Value Mandatory Veto Source-Contract Wave V1

## 1. 状态与 Owner 决策

- task id：`TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1`；
- priority：`P0`；
- governed mode：`SINGLE_LANE` serial consumer-contract wave；
- current status：`IN_PROGRESS_NON_EXECUTABLE_OWNER_FREEZE_DECISION_PACK_DRAFT`；
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

S3 producer-contract draft 另新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_producer_contract_draft_v1.yaml`：
  四类候选 producer 的输入独立性、结构公式、PIT/时钟、待冻结 decision inventory 与 blocker；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_producer_contract_draft.py`：
  strict typed loader，重放 source-wave exact identity 与 candidate evidence hashes；
- `tests/test_growth_action_value_mandatory_veto_producer_contract_draft.py`：
  0/4 admission、pilot threshold、event `published_at`/revision、forbidden dependency、T+1 与
  execution safety 的正负向验证。

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

Owner 于 2026-08-25 指示“继续推进”后，S3 先增加一个不产生 admission 的 producer-contract
draft 子阶段。该指示允许 Codex 整理候选 producer、结构公式、PIT 时钟、输入 schema 和阈值来源，
但不等于 Owner 已精确冻结任何公式或阈值，也不授权 series、R1 manifest、provider/cache、真实 DQ
或 backtest。

#### S3.1：四类 producer contract draft

| veto | candidate producer boundary | 可直接冻结的结构 | 继续保留的 Owner blocker |
| --- | --- | --- | --- |
| `broad_market_risk_off_veto` | 独立 broad-market daily-price producer；候选 universe 只允许 broad-market proxy，不读取 option alpha、`growth_allowed` 或 candidate result | 输入独立性、T 日 close 后可用、T+1 消费、missing=`INSUFFICIENT`、malformed=`INVALID` | exact proxy universe、trend/drawdown 公式、窗口与阈值 |
| `realized_volatility_veto` | 复用 `volatility_compression_free_v1` 作为 candidate evidence；QQQ adjusted-close realized vol 与 VIX percentile 均不得读取 option result | callable candidate identity、price/VIX 字段、T 日 close/发布后可用、pilot policy provenance | successor exact formula、窗口、VIX/realized-vol threshold 与组合逻辑 |
| `scheduled_event_risk_veto` | scheduled-release calendar contract；每条事件必须有 authority、`scheduled_for`、`published_at`、revision/source identity | event taxonomy proposal、PIT 字段、仅使用决策时已发布日历、missing/malformed terminal | exact event set、pre/post window、score/boolean threshold、source precedence |
| `underlying_trend_break_veto` | 专用 QQQ underlying daily-price producer，不复用 broad-market veto 或 option state | QQQ-only input、T 日 close 后计算、T+1 消费、无 cross-date fallback | moving-average/drawdown/trend-score 公式、窗口、阈值与 recovery/hysteresis |

draft artifact 必须绑定本 V1 source-wave file/canonical SHA 和所有 candidate code/policy bytes，区分
`STRUCTURE_PROPOSAL`、`PILOT_THRESHOLD_PROVENANCE_ONLY` 与
`BLOCKED_OWNER_EXACT_FREEZE`。四类 row 均保持 `admitted=false`，aggregate 仍为 `0/4`；validator
必须拒绝：

- option contract、selected activity、`growth_allowed` 或 result-derived input；
- 把 pilot threshold 标成 Owner-frozen；
- 把 `event_date` 自动等同于 `published_at`；
- 把 missing/malformed 解释为 veto clear；
- 生成 boolean series、R1 manifest、真实 inventory hash 或任何交易输出。

#### S3.2：验收与退出条件

1. strict loader 重放 source-wave exact identity 与所有候选 evidence hash；
2. 四个 ordered producer draft rows 具有独立 inputs、structural formula、timing、PIT 与 threshold
   decision inventory；
3. validator 明确输出 `OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED`；
4. focused/adjacent、Ruff、mypy、py_compile、Architecture/Contract/Integration/Reproducibility/Full
   按适用门禁 PASS；
5. Atlas 显示 producer draft 已完成、真实 source admission 仍阻塞；
6. 下一状态仍由 Owner 对 exact producer/formula/threshold/timing 逐项冻结决定，不能由代码或测试
   自动升级。

### S4：Owner exact-freeze decision pack draft

Owner 于 2026-08-26 再次指示“继续”。该指示允许 Codex 把 S3 的空 decision inventory 收敛成一组
可逐项批准或退回修改的推荐对象，但仍不等于 Owner 已批准其中任何值。S4 新增
`growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft.v1`，精确绑定 S3 producer draft
file/canonical SHA，并保持 `0/4 admitted`。

四项 recommendation 固定为 result-blind、compatibility-anchored proposal：

| veto | 推荐 producer / 公式 | recommendation evidence role | 仍阻塞 admission 的条件 |
| --- | --- | --- | --- |
| `broad_market_risk_off_veto` | SPY-only；`close < SMA200 OR drawdown63 <= -10%`；T close 后、T+1 消费 | 复用既有 SPY 200-session 与 63-session/-10% 边界，只作 Owner review proposal | Owner 未冻结；dedicated producer 未实现；未观察 exact-1202 inventory |
| `realized_volatility_veto` | `VIX percentile252 >= 0.75 OR QQQ annualized RV20 > 0.25`；T+1 消费 | free-feature stress percentile 与 legacy RV20 边界的兼容性 proposal | Owner 未冻结；successor adapter/VIX available-at 未冻结；未观察 inventory |
| `scheduled_event_risk_veto` | 仅 Federal Reserve/BLS/BEA official schedule；若 next QQQ session 有任一 admitted event 则 veto | 不使用 event result/weight；只使用 `published_at <= decision_as_of` 的最新 official revision | Owner 未冻结；published-at/revision schema 与 official adapters 未实现；未观察 inventory |
| `underlying_trend_break_veto` | QQQ-only；`close < SMA200 AND drawdown63 <= -12%` 进入；连续 2 日 `close >= SMA200` 恢复 | 复用既有 QQQ 200-session 与 63-session/-12% 边界，新增明确 hysteresis proposal | Owner 未冻结；dedicated stateful producer/initialization 未实现；未观察 inventory |

这些数值是明确记录的 temporary review baseline，不是 active threshold policy：

- calibration status=`UNVALIDATED_NO_REAL_DATA_OR_BACKTEST`；
- recommendation 不得驱动 runtime、DQ、series、manifest、backtest 或投资结论；
- Owner 必须对四项分别 exact-freeze；partial freeze 也不能生成 series；
- review/expiry condition=`BEFORE_ANY_SOURCE_CONTRACT_ADMISSION_OR_SERIES_GENERATION`；
- 如 Owner 退回任一 recommendation，修改必须产生新版本与新 file/canonical identity，不改写本草案；
- 真实 DQ/backtest 即使四项全部冻结，仍需另行授权且必须先通过 manifest replay。

S4 strict loader 必须重放 S3 producer draft loader，而不是只相信复制的 SHA；拒绝 broad producer 读取
QQQ、trend producer 读取 SPY、event source 使用 convenience-provider fill、`event_date` 代替
`published_at`、Owner-freeze/admission flag 变真、observed inventory 伪造或任何外部/交易开关开启。

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

- current S4 frozen base：`138d1fdfa12c9ffc0f5dcf7dbfaaf3e0314254be`；
- current S4 branch：`codex/trading-2542g-veto-owner-freeze-pack`；
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
- 2026-08-25：lane commit `eecba8a53e568f9f5930a44c275e90ef0da41f52` 建立后，publication v2
  的 task-source、architecture-manifests 与 Atlas 17-artifact rebuild 均 PASS；report-flow builder 随后
  以 `RCF_SOURCE_SEAL_DRIFT` 拒绝旧 `docs/system_flow.md` seal（expected 2,275,242 bytes，actual
  2,277,444 bytes）。v2 已失败释放、未生成 candidate、未运行 Full。使用 builder 自身的 exact splitter/
  digest 只读计算新 seal=`2,277,444 bytes / 7ed40a...e33b / git blob cf275f...89a1 /
  1099 entries`，仅同步 reviewed source identity 与对应 frozen regression；不改变策略、DQ/PIT 或授权。
- 2026-08-25：上一候选已以 `16162ad9bbd0c7c0bcc3a79c5fe38f2e8571e77b` 完成 Full
  `9589 passed / 3 skipped` 并 ordinary-push 到 `origin/main`。Owner 随后指示“继续推进”；READ_ONLY
  与 SINGLE_LANE START/LANE preflight 均 PASS，选择复用当前 task 的 S3 producer-contract draft
  子阶段。该阶段只整理结构、候选 provenance 与待冻结 decision inventory，保持 admitted=`0/4`，
  不读取 provider/cache，不生成 series/R1 manifest，不运行真实 DQ/backtest，
  orders/fills/positions/production/broker=`0`。
- 2026-08-25：S3 producer-contract draft 已实现并由 strict loader 重放。draft file/canonical
  SHA-256=`8bd9799b42a0d2f547afbb5bb8708775bef0de35d504197b117ed210e49a6baa`/
  `a6e3ff096d5c5c6df6ec76756581bf0262be4988b696cb2cfb6457dd1b07f063`；四类 row 仍为
  `0/4 admitted`，terminal=`OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED`。其中 realized-volatility
  仅是 callable candidate，scheduled-event 明确为 PIT incomplete，broad-market/trend producer 仍未
  callable；所有 exact threshold 保持 `null`。producer-draft + source-wave focused/adjacent=
  `36 passed`；未读取真实数据，series/R1/provider/cache/真实 DQ/backtest 与交易边界继续关闭。
- 2026-08-25：final publication v1 的 canonical-task、architecture-manifests 与 Atlas rebuild 均已
  通过，report-flow builder 随后以 `RCF_SOURCE_SEAL_DRIFT` 拒绝新增本节数据流后的旧
  `docs/system_flow.md` seal（expected `2,277,444` bytes，actual `2,279,738` bytes）。v1 已失败释放，
  未生成 candidate、未运行 Full。使用 builder 的 `EXACT_BLANK_LINE_BLOCKS_V1` 与 Git blob 算法只读
  计算 reviewed successor seal=`2,279,738 bytes / 75804e...3590 / cf427e...067d / 1,103 entries`；
  仅同步 source identity 与 frozen regression，不改变 veto、DQ/PIT、阈值或授权语义。
- 2026-08-26：上一 S3 candidate 已以 `138d1fdfa12c9ffc0f5dcf7dbfaaf3e0314254be` 完成 Full
  `9606 passed / 3 skipped` 并 ordinary-push 到 `origin/main`。Owner 再次指示“继续”；READ_ONLY 与
  SINGLE_LANE START/LANE preflight PASS，canonical task 已恢复为 `IN_PROGRESS`。S4 decision-pack
  draft file/canonical SHA-256=`4f188c6e10758a32984bb92c3252507636686f97404c4491df014c1d22807479`/
  `c8838a4baef788a6b936e4e098658413e2c563e169f1ec4a5da8ec7318c9e4af`；四个 recommendation object
  ready for Owner review，但 owner-frozen/admitted 仍为 `0/4`。S4 + S3 + source-wave focused=
  `53 passed`；Ruff、strict mypy、py_compile PASS。未读取 provider/cache 或真实数据，未生成 series/R1
  manifest，未运行真实 DQ/backtest，orders/fills/positions/production/broker=`0`。
