# TRADING-2542G：Growth Action-Value Mandatory Veto Source-Contract Wave V1

## 1. 状态与 Owner 决策

- task id：`TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1`；
- priority：`P0`；
- governed mode：`SINGLE_LANE` serial consumer-contract wave；
- current status：`IN_PROGRESS_S11_HISTORICAL_PIT_STATIC_AUTHORITY_RECEIPT_CONTRACT`；
- Owner decision：
  `owner_decision:TRADING-2542F-2542G:2026-08-25:approve_exact_architecture_freeze_and_source_contract_followup_v1`；
- S4A Owner decision：
  `owner_decision:TRADING-2542G:S4A:2026-08-26:authorize_exact_calculation_time_state_contract_v2`；
- S4B Owner decision：
  `owner_decision:TRADING-2542G:S4B:2026-08-26:freeze_s4a_v2_exact_semantics_and_continue_non_executable_admission_v1`；
- S7 Owner decision：
  `owner_decision:TRADING-2542G:S7:2026-08-26:freeze_s6_real_source_adapter_manifest_inventory_contract_v1`；
- S8 continuation：Owner 于 2026-08-26 指示“继续”，按 S7 已冻结的
  `separate_non_executable_adapter_implementation_followup_authorized=true` 边界启动独立 consumer wave；
- S9 Owner decision：
  `owner_decision:TRADING-2542G:S9:2026-08-26:authorize_manifest_replay_source_admission_continue_v1`；
- S11 Owner decision：
  `owner_decision:TRADING-2542G:S11:2026-08-27:adopt_web_pro_static_authority_receipt_contract_sequence_v1`；
- S8 授权边界：只允许本地、result-blind、non-executable `DATA_RESEARCH` 合同与 synthetic
  validation；不授权 veto series、R1 manifest、provider/cache query、真实 DQ、backtest、
  orders、fills、positions、paper、live、production 或 broker action；
- S9 授权边界：允许生成并自动执行 exact manifest replay/source-admission capability gate；只有
  replay=`PASS` 才允许在固定 request maxima 内读取真实 provider payload。若任一 historical
  `available_at`、event schedule revision vintage、endpoint 或 session identity 无法证明，必须在
  provider query 前 typed stop。真实 DQ/backtest 仍不授权，orders/fills/positions/production/broker=`0`。

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

### S4A：四项 exact calculation/time/state contract V2

Owner 于 2026-08-26 指示“参考这个结论推进”，接受网页版 Pro 对 S4 四项均为
`ACCEPT_WITH_EXACT_CONTRACT_EDITS` 的规划意见，并授权先实施最小 serial contract wave。该指示仅授权
生成新的 non-executable V2 draft、typed semantics 与 synthetic validation；它不等于 Owner 已对尚未
生成的 V2 bytes 作 exact-freeze，也不授权 producer implementation、provider/cache、真实数据、veto
series、R1 manifest、真实 DQ、backtest、threshold search、orders/fills/positions、paper/live、production
或 broker action。S4 V1 file/canonical bytes 继续 immutable，V2 使用新的 version、file SHA 与 canonical
SHA。

#### S4A.1 共同 calculation、time 与 terminal 语义

V2 必须把 S4 V1 的自由字符串和通用 window/threshold dict 收敛为逐 veto typed objects，并机械冻结：

- `target_calendar_identity=QQQ_EXCHANGE_SESSIONS`，所有 rolling window 以绑定的 exchange session
  计数，不按自然日计数，不压缩缺失 session；
- price rolling 都包含当前 source session，使用完整 minimum observations；target inventory 与
  pre-target warm-up inventory 分开，warm-up 不得扩入 1202-session target result inventory；
- price 数据禁止 forward fill/interpolation；duplicate/conflicting session 为 `INVALID`；缺 observation、
  warm-up 或无法证明 `available_at <= decision_as_of` 为 `INSUFFICIENT`；
- `decision_as_of`、`available_at`、`scheduled_for` 与 next-action cutoff 必须是带 timezone、可机械比较的
  timestamp；消费只允许 next valid QQQ session，不允许 same-session 或 cross-date fallback；
- adjusted-close 必须绑定 adjustment basis、corporate-action vintage、source identity 与 snapshot SHA；
- 任一 mandatory component 不完整时，`OR`/`AND` 都不得 short-circuit 产出 boolean；source qualification
  terminal 先于 formula evaluation；
- formula 使用 typed operator tree；validator 必须拒绝 ticker、operator、comparison equality、window、
  threshold、clock、PIT、terminal 或 state transition 的未授权漂移；
- `missing_terminal=INSUFFICIENT`、`malformed_authority_terminal=INVALID`；V2 继续保持
  `owner_exact_freeze_granted=false`、`producer_contract_admitted=false`、
  `observed_inventory_lf_sha256=null` 与 `series_generation_allowed=false`。

#### S4A.2 四项精确语义

`broad_market_risk_off_veto`：

- 保留 SPY-only、`close < SMA200 OR drawdown63 <= -0.10`、stateless one-session entry/clear；
- `SMA200=ARITHMETIC_MEAN`，完整 200 个 SPY sessions；drawdown 使用完整 63-session、包含当前 session 的
  rolling max；两个 component 都合格后才计算 OR；
- 明确这是“复用 component anchors 并改变 producer universe/combination semantics”的新 pilot policy，
  不得宣称整体继承旧 `regime.py` 行为。

`realized_volatility_veto`：

- 保留 `VIX percentile252 >= 0.75 OR QQQ annualized RV20 > 0.25`；VIX component 必须标为
  implied-volatility stress proxy，不得把整行误称为纯 realized volatility；
- VIX percentile 要求完整 252 个 bound-session observations，包含当前 observation，tie method 固定为
  average-rank，禁止沿用 candidate code 的 20-row minimum periods；
- RV20 固定为 21 个连续完整 QQQ closes 形成 20 个 simple returns，`fill_method=None` 等价语义、sample
  standard deviation `ddof=1`、annualization=`sqrt(252)`；
- 必须绑定 VIX authority/provider、level definition、observation/session mapping、published/available
  timestamp、revision policy 与 snapshot checksum。

`scheduled_event_risk_veto`：

- 只允许 Federal Reserve/FOMC rate decision、BLS CPI、BLS nonfarm payrolls、BEA PCE price index 与
  BEA GDP advance estimate；只使用 official authority，不允许 convenience-provider fill；
- 对 decision session `T` 的 exact post-close cutoff，action session 为 next valid QQQ session；active
  revision 满足 `decision_as_of < scheduled_for <= next_action_session_close` 时 veto=true，next-session
  pre-market/in-session event 阻断该 action session，after-close event 映射到再下一 action session；
- stable event key、revision id、`published_at`、deterministic ordering、reschedule supersession、cancel
  handling 与 same-published-at conflict terminal 必须机械冻结；decision 后发布的 revision 不得回写历史；
- event=false 必须同时具备 Fed/BLS/BEA 三套 snapshot coverage receipt，且 coverage 至少覆盖 next action
  session close；无 row 或空查询不得替代 coverage proof；unscheduled intervention 不属于本 veto。

`underlying_trend_break_veto`：

- 保留 QQQ-only；entry=`close < SMA200 AND drawdown63 <= -0.12`；recovery 为连续两个有效 QQQ
  sessions `close >= SMA200`；entry drawdown 只控制进入，不控制 persistence/clear；
- typed state 固定为 `UNKNOWN|CLEAR|VETO_ACTIVE`，初始状态不得默认为 clear；从 exact pre-target replay
  inventory 起重放，target start 前必须解析为 known state；
- missing observation 使当前 terminal=`INSUFFICIENT`、中断 recovery streak，并使后续 state 回到
  `UNKNOWN`，直到 entry 或 two-session clear 条件重新建立；malformed authority 使受影响 replay chain
  为 `INVALID`，修复后必须从 checkpoint 重放；
- state checkpoint 必须绑定 producer version、source inventory SHA 与 deterministic replay identity。

#### S4A.3 重叠声明、产物与验收

V2 只允许声明
`orthogonality_claim=SEMANTIC_AND_INPUT_SEPARATION_ONLY_NOT_EMPIRICAL_INDEPENDENCE`；SPY/QQQ、OR/AND 与
hysteresis 差异不能证明低相关。四项 union 可能过严，但本阶段不得读取 observed series。首次观察前只
冻结 descriptive evidence schema：per-veto/exclusive hit、pairwise Jaccard、union blocked sessions、
episode count/duration、trend recovery lag、event-only blocked sessions、alpha-available-but-blocked sessions
与 missing/FAIL/INSUFFICIENT/INVALID inventory；不得包含 candidate weights、returns 或 V4 result。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1.yaml`；
- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2.py`；
- `tests/test_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2.py`。

验收标准：

1. V2 loader 重放 immutable S4 V1 file/canonical identity 与新的 calculation semantics identity；
2. 四项使用独立 typed model，formula operator tree、rolling、time/PIT、revision、coverage、state 与
   terminal 均可机械验证；
3. negative tests 拒绝 operator/window/threshold/ticker drift、missing-as-false、event date 代替
   `published_at`、same-session/cross-date fallback、非法 state transition 及 admission flag 伪造；
4. focused/adjacent、Ruff、strict mypy、py_compile 及适用 Architecture/Contract/Integration/
   Reproducibility/Full PASS；
5. aggregate 仍为 `OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED`，任何 partial approval 不得生成 series；
6. 后续 producer/adapters synthetic implementation 只有在四项 V2 全部 Owner exact-freeze 后另行开始。

### S4B：Owner exact-freeze admission（最小 serial contract wave）

Owner 于 2026-08-26 在收到 S4A 两份 V2 authority 的精确 file/canonical SHA、当前唯一阻塞项与
安全边界后指示“继续吧”。按照 R1 standing owner scope replay，本指示精确冻结以下两份已发布
authority，并授权生成一个独立、non-executable freeze-admission artifact；不得改写原 authority bytes：

| authority | file SHA-256 | canonical SHA-256 |
| --- | --- | --- |
| calculation semantics V1 | `813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0` | `824ef20a66e4eba3c2841489cae8b03ff3a6cad4f73003469c086d8e09237cf1` |
| owner-freeze decision pack V2 | `d08480c07047e636f8b4a8208ec60406acd5debdc60f30541411310e401b789f` | `99ed7dbdac82faf594633ab25be1ffb1417709030af0817fb19c4ace332dc389` |

本波必须把两种状态机械分开：

- `owner_exact_frozen=4/4` 只表示四项 operator/window/threshold/time/PIT/revision/coverage/state
  语义已作为 immutable policy authority 接纳；
- `producer_contract_admitted=0/4` 表示尚无任何 producer/adaptor 通过独立 callable identity、synthetic
  conformance、PIT/DQ contract 与 exact inventory admission；
- `exact_1202_session_inventory_admitted=0/4`、`series_generation_allowed=false`，不得把 Owner freeze
  推断成 observed data admission、DQ PASS、backtest authority 或 investment conclusion；
- producer/adapters synthetic implementation 必须在本 serial freeze wave 发布后的新 exact main base
  另行开始，不能和 freeze authority 在同一 stale-base consumer wave 内混合；
- 真实数据、provider/cache query、真实 DQ/backtest 与 exact series 仍需 manifest replay PASS 后另行授权，
  orders/fills/positions/paper/live/production/broker 始终为 0。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_exact_semantics_freeze_admission_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_exact_semantics_freeze_admission.py`；
- `tests/test_growth_action_value_mandatory_veto_exact_semantics_freeze_admission.py`。

验收标准：

1. strict loader 必须重放 S4A V2 loader，并同时校验两份 immutable authority 的 file/canonical SHA；
2. 四项 veto 按固定顺序一次性冻结；缺一项、partial freeze、recommendation identity 漂移或原文件 bytes
   漂移均 fail closed；
3. aggregate 精确为 `OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4`，四项 producer、observed
   inventory 与 series flag 继续为 false/null；
4. negative tests 拒绝 owner/admission 状态混淆、伪造 observed inventory、开启 provider/DQ/backtest/
   execution flag，以及 threshold/formula authority 漂移；
5. focused/adjacent、Ruff、strict mypy、py_compile 与适用 formal tiers PASS；本波发布后才允许从新 exact
   main base 启动 separate synthetic producer/adapters consumer wave。

### S5：non-executable synthetic producer/adapters conformance

S4B 已以 candidate/main/origin SHA `be2492b53f53fc6d89145cb1ccbdec5e3296e15e` 独立发布，满足
“consumer 只能从新的 exact main base 开始”的 serial contract fence。S4B Owner decision 已明确授权
separate synthetic producer follow-up；本阶段据此实现纯内存、无 provider/cache IO 的四类 callable
conformance，不需要也不推断新的 Owner threshold/formula decision。

S5 只允许实现以下机械行为：

- shared clock：所有 timestamp 必须 timezone-aware，先验证 `available_at <= decision_as_of < action_cutoff`，
  same-session 与 cross-date fallback 均不存在；
- broad SPY：完整 200-session arithmetic SMA 与完整 63-session inclusive-current drawdown，严格执行
  `close < SMA200 OR drawdown63 <= -0.10`；任一 window 不完整不得 short-circuit；
- volatility：完整 252-observation VIX average-rank percentile，与 21 QQQ closes 形成 20 simple returns、
  `ddof=1`、`sqrt(252)` annualized RV；严格执行 `VIX percentile >= 0.75 OR RV20 > 0.25`；
- scheduled event：只接受 frozen Fed/BLS/BEA taxonomy；按 `published_at <= decision_as_of` 选 active
  revision，处理 reschedule/cancel/same-published-at conflict，并要求三 authority coverage receipts 覆盖
  next action close 后才允许返回 clear；
- QQQ trend：entry=`close < SMA200 AND drawdown63 <= -0.12`；active persistence 忽略 entry drawdown，
  连续两个 valid `close >= SMA200` 才 clear；missing 产生 `INSUFFICIENT`、中断 streak 并令 next state
  `UNKNOWN`，malformed 产生 `INVALID`；checkpoint identity 必须绑定 producer/source inventory/state SHA。

新增 contract artifact 必须精确绑定 S4B freeze-admission file/canonical SHA=
`ef075527750efd24433eafbd8a2e586104562868f4ce2b666043069fe5368765`/
`97f3678417b5dcb0a4965a308953552d17d17e4cb947532316dceca2506df147`，并把四项状态写为
`SYNTHETIC_CALLABLE_CONFORMANCE_IMPLEMENTED_NOT_SOURCE_ADMITTED`。这只把 callable/synthetic conformance
推进到 `4/4`；`producer_contract_admitted`、real-source PIT/DQ identity 与 exact-1202 observed inventory
仍保持 `0/4`，不得生成 series/R1 manifest。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_synthetic_producer_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_synthetic_producer_contract.py`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_synthetic_producers.py`；
- `tests/test_growth_action_value_mandatory_veto_synthetic_producers.py`。

验收标准：

1. contract loader 重放 S4B strict loader 与 exact file/canonical identity，四个 callable id 顺序和 frozen
   recommendation/producer id 完全一致；
2. synthetic tests 覆盖所有 equality 边界、完整 window、OR/AND no-short-circuit、VIX tie、RV estimator、
   event revision/coverage 以及 trend UNKNOWN/recovery/checkpoint transition；
3. wrong ticker、wrong window、naive/late timestamp、NaN/invalid price、event conflict、missing coverage、
   fabricated source admission/inventory/series 或 execution flag 均 fail closed；
4. aggregate=`SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4`，下一合法动作仅为
   reviewed real-source adapter contract/inventory admission planning，不是 provider query、DQ、series 或 backtest；
5. focused/adjacent、Ruff、strict mypy、py_compile、generated freshness 与 formal tiers PASS。

### S6：真实来源 adapter 与 exact-1202 inventory 接纳评审包（当前阶段）

Owner 于 2026-08-26 同意 Codex 独立准备后续评审材料。本授权只覆盖可静态重放的
non-executable `DATA_RESEARCH` 合同、纯内存 receipt validator、manifest/inventory 规划和 Owner
决策矩阵；不覆盖 provider query、cache/真实数据读取、source/inventory admission、veto series/R1、
真实 DQ/backtest 或任何交易动作。

新增 artifact 必须精确绑定 S5 synthetic producer contract file/canonical SHA=
`14a8995e0bcb5cdc1a5fccb67d6389c5e72fb65ce1efdb926d1f9520e1d4d314`/
`c064ec2418f43184e89fdecdf1ced60c932b15e5de6b6548fa01dc6af99ac95c`，并保持 S5 四项
exact semantics/callable surface 不变。评审包使用以下 candidate，candidate 不等于 admitted：

1. broad-market 与 trend 的 SPY/QQQ EOD 候选为 `FmpPriceProvider`/
   `fmp_eod_daily_prices`；必须固定 endpoint/params、raw/adjusted close、corporate-action adjustment
   basis/vintage、`available_at`、download time、row count、snapshot checksum 和 symbol identity。
   `MarketstackPriceProvider` 仅可做独立 second-source reconciliation；Yahoo 不能成为 fallback；
2. volatility 的 VIX 候选为 official `CboeVixPriceProvider`/
   `cboe_vix_historical`；必须固定 official endpoint、VIX level 定义、session/timezone 映射、
   `available_at`、snapshot checksum 和与 QQQ session 的 join 规则。FRED `VIXCLS` 只可诊断交叉核对，
   不能补值或覆盖 official Cboe；
3. event 候选必须由 Federal Reserve FOMC calendar、BLS release schedule、BEA release metadata 三个
   official capture-only adapter 共同组成；必须保存 stable event key、exact taxonomy、revision/action、
   scheduled time、official `published_at`/capture time、`available_at`、coverage-through、snapshot checksum、
   cancellation/reschedule/conflict 语义。任一 authority coverage receipt 缺失时不得输出 clear；
4. trend 与 volatility QQQ 复用同一份已接纳 QQQ snapshot identity，但各自拥有独立 consumer binding；
   trend 还必须保存 replay start、initial checkpoint、state transition lineage 和 target-start checkpoint。

exact-1202 inventory 规划固定 project default target start=`2021-02-22`、target session count=`1202`、
calendar=`QQQ exchange sessions`。实际 target end、session-list LF SHA、四类 source snapshot SHA、row count
和 observed coverage 在未来受权 manifest replay 前必须为 `null`；不得根据墙钟日期或本地 cache 推测。
warmup 与 1202 个 target sessions 必须分开计数：SPY/QQQ SMA200 至少 199 个先行 session、VIX percentile252
至少 251 个、QQQ RV20 至少 19 个；stateful trend 还需从可证明的 initial checkpoint 连续重放到 target
start，不得把“199 个最小公式 warmup”冒充完整 state lineage。

未来 admission manifest replay 必须同时验证：exact code/policy/calendar/source identities、request params、
schema、ticker/universe、timezone/availability、duplicates/gaps/conflicts、corporate-action vintage、warmup 与
target 分离、1202 exact session equality、cross-source reconciliation、event 三 authority coverage、trend
checkpoint lineage、artifact checksum、actual counters，以及 orders/fills/positions/production/broker=`0`。
任一 gate 失败或字段缺失均 fail closed；评审包自身不能执行 replay。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_real_source_adapter_admission_review_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_real_source_adapter_admission_review.py`；
- `tests/test_growth_action_value_mandatory_veto_real_source_adapter_admission_review.py`。

Owner 一次性决策面必须逐项显示 candidate provider/adapter、source class、endpoint identity、required
receipt/inventory fields、禁止替代来源、未满足 blocker、推荐决定与其精确影响。默认且唯一合法的当前结果为
`OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4`；`candidate_ready_for_review=true` 不能被
解释为 `real_source_identity_admitted=true` 或 `exact_1202_session_inventory_admitted=true`。

验收标准：

1. strict loader 重放 S5 exact file/canonical identity，并校验四项 veto/callable/producer 顺序不漂移；
2. config 精确包含四类 source candidate、三个 official event authority、exact-1202 inventory 模板、
   manifest replay gates、Owner decision rows 与全关闭 authorization/safety surface；
3. 纯内存 validator 只接收显式 mapping，不包含 network/filesystem/cache/provider code path；完整 planning
   receipt 可得到 `REVIEW_READY_NOT_ADMITTED`，缺字段、未知字段、hash/timestamp/session/count/coverage/
   checkpoint/reconciliation/admission/execution 伪造均拒绝；
4. observed inventory/source admission 均保持 `0/4`，所有 observed hash/end/row count 为 `null`，不生成
   series/R1，不运行真实 DQ/backtest；
5. focused/adjacent、Ruff、strict mypy、py_compile、generated freshness 与 formal tiers PASS。

### S7：S6 adapter、manifest 与 inventory 合同 exact-freeze admission

Owner 于 2026-08-26 在收到 S6 精确 file/canonical SHA 与“合同冻结不等于真实来源接纳”的边界后
指示“继续推进”。本指示精确冻结 S6 review pack 的 adapter candidate、manifest replay gate、
exact-1202 inventory plan 与四项 Owner decision surface；S6 原 bytes 保持 immutable：

| authority | file SHA-256 | canonical SHA-256 |
| --- | --- | --- |
| S6 real-source adapter admission review V1 | `d0adae89a1faf7c160cf82edc9d51ede74fa2ea279fcc2526c009752a9a5b57e` | `be705f1b46431e432169b186db6d336bb68d51cf296ca08ca5d6cca465ffc6e3` |

本最小 serial freeze-admission wave 只记录 immutable Owner decision，不实现或执行 adapter，不读取
provider/cache/真实数据，也不生成 observed manifest、inventory、series、DQ 或 backtest。四项状态必须
机械分离：

- `owner_adapter_manifest_contract_frozen=4/4`：只表示 S6 的 candidate/source class/endpoint identity、
  receipt fields、exact-1202 plan、warmup/state lineage 与 14 项 replay gate 已作为后续实现合同冻结；
- `adapter_implementation_admitted=0/4`：既有 FMP/Cboe candidate 未绑定本合同的 PIT/DQ receipt，
  Fed/BLS/BEA capture adapter 仍为 planned；
- `real_source_identity_admitted=0/4`、`exact_1202_session_inventory_admitted=0/4`：所有 observed
  target end/session SHA/source SHA/row count/coverage/checkpoint 继续为 `null`；
- `manifest_replay_allowed=false`、`series_generation_allowed=false`、`real_dq_allowed=false`、
  `backtest_allowed=false`；真实 DQ/backtest 仍只能在未来独立实现、exact manifest replay PASS 与
  separate admission 后另行授权；
- 下一合法动作只允许从本波发布后的新 exact `main` 启动 separate non-executable adapter
  implementation，不得在本波 stale base 内混合 consumer implementation。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission.py`；
- `tests/test_growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission.py`。

验收标准：

1. strict loader 递归重放 S6 loader，并同时校验 immutable S6 file/canonical SHA；
2. 四项 veto decision row 按 S6 固定顺序原子冻结；缺一项、partial freeze、candidate/inventory/gate
   surface drift 或原文件 bytes 漂移均 fail closed；
3. aggregate 精确为
   `OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4`；
4. negative tests 拒绝 adapter/source/inventory admission、observed evidence、manifest replay、provider/cache/
   real-data/DQ/backtest/series/execution flag 或 threshold/formula/callable identity 漂移；
5. focused/adjacent、Ruff、strict mypy、py_compile 与适用 formal tiers PASS；发布后才允许从新 exact main
   开始 separate adapter implementation wave。

### S8：pure in-memory PIT receipt-bound adapter implementation

S7 已以 candidate/main/origin SHA `107c4b53b5e68d384cafafac79939c4cbdd7654a` 独立发布，满足
“adapter implementation 只能从新的 exact main base 开始”的 serial contract fence。Owner 于
2026-08-26 再次指示“继续”，本阶段据此实现 S7 已授权的 non-executable adapter consumer wave；该指示
不授权 provider/network/cache/filesystem market-data IO、manifest replay、真实 source/inventory
admission、veto series/R1、真实 DQ、backtest 或任何交易动作。

本阶段把“真实来源 adapter 的数据获取”与“已获取 receipt/payload 的机械验证和规范化”分离。S8 只实现
后者：adapter 必须以调用方显式注入的 mapping/rows 为输入，不得导入或调用现有 provider client，不得打开
market cache 或 source file。synthetic fixture 的 checksum 只证明 parser/conformance，不得写入 S6/S7
保留为 `null` 的 observed source、target inventory 或 checkpoint 字段。

四项实现边界固定如下：

1. `broad_market_risk_off_veto`：接收 frozen FMP `SPY` daily-price receipt；验证 provider/source/
   endpoint/request params、symbol、timezone-aware `downloaded_at`/`available_at`、raw/adjusted close、
   adjustment basis/vintage、row count、payload checksum、schema、duplicate/gap/conflict 与 session order；
2. `realized_volatility_veto`：接收同一 frozen FMP `QQQ` receipt 与 official Cboe `VIX` receipt；Cboe
   adapter 额外验证 official endpoint、VIX level identity、America/Chicago observation mapping、revision
   policy、snapshot checksum 与 QQQ-session join identity；FRED 仍只可 diagnostic，不得补值；
3. `scheduled_event_risk_veto`：接收 Federal Reserve、BLS、BEA 三个 official capture receipt；验证
   stable event key、frozen taxonomy、revision id/action、`scheduled_for`、official `published_at`、
   `captured_at`、`available_at`、`coverage_through`、snapshot checksum、cancel/reschedule/supersession、
   same-published-at conflict 与 deterministic order；三 authority coverage 缺一即 fail closed；
4. `underlying_trend_break_veto`：复用已验证的 frozen FMP `QQQ` receipt bytes，但建立独立 consumer
   binding；只验证 adapter/input identity，不生成 trend state、checkpoint、target inventory 或 veto series。

新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_pit_receipt_adapter_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_pit_receipt_adapter_contract.py`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_pit_receipt_adapters.py`；
- `tests/test_growth_action_value_mandatory_veto_pit_receipt_adapters.py`。

验收标准：

1. S8 strict contract loader 递归重放 S7 loader 与 S7 exact file/canonical SHA，并校验四项 frozen
   decision row、candidate/endpoint/receipt-field identity 不漂移；
2. pure adapters 只接收显式 mapping/rows，完整 synthetic FMP SPY/QQQ、official Cboe VIX 与 official
   Fed/BLS/BEA receipts 可得到 typed normalized receipt；未知字段、错误 ticker/provider/endpoint/params、
   naive/late timestamp、checksum/row-count/schema drift、NaN、duplicate、gap/conflict 或 event coverage/
   revision drift均 fail closed；
3. adapter conformance 精确为 `4/4`，aggregate=
   `SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4`；
4. `adapter_implementation_admitted`、`real_source_identity_admitted`、
   `exact_1202_session_inventory_admitted` 继续为 `0/4`，所有 observed evidence 继续为 `null`，
   manifest replay/series/real DQ/backtest/execution flag 全部为 false；
5. focused/adjacent、Ruff、strict mypy、py_compile、generated freshness 与 formal tiers PASS；下一合法动作
   仍需 Project Owner 对 exact manifest replay/source/inventory admission 另行授权。

### S9：exact manifest replay capability gate

Owner 于 2026-08-26 在收到 S8 exact hashes、`4/4` synthetic adapter conformance 与剩余
source/inventory admission blocker 后指示“好的，你继续”。本阶段把该指示解释为对 exact manifest
replay/source-admission gate 的 standing owner scope，不把它扩大为真实 DQ、backtest、series、交易或
生产授权。

S9 必须先解决两个 predecessor contract 冲突，不能直接 replay 旧 execution V2：

1. immutable execution V1/V2 仍把 historical `tqqq_veto` 列为第五个必须 exact-false 的 market-state
   veto；TRADING-2542F architecture V1 已把它正确迁移为独立
   `NO_LEVERAGE_ETF_ACTION_GUARD`，新的 mandatory market-state gate 只有四项；
2. S8 pure adapter 可以验证调用方注入的 `available_at`/`published_at`，但当前真实 provider/capture
   surface 尚未证明能够产生这些历史 PIT 字段。把当前下载时间、session 次日或页面 last-modified
   推断成 2021–2025 的历史可用时间属于伪造 evidence，禁止使用。

#### S9.1 successor bridge 与 exact identity

新增 manifest replay gate 必须同时绑定且递归重放：

- execution V2 file/canonical SHA-256=
  `f02df23a4bd36069f5fe09354a3ce8480583fc451b71ec511bc3ba2da27780f2`/
  `9b39a1cf6d1ad48c427755f07c592610ae2ad94055af4aab79d3327bf4e82456`；
- architecture V1 file/canonical SHA-256=
  `9b4856614298d64b2c8b5897980735a9e2a19c46fecb6c2362cb750ae13b136d`/
  `88e1283b0333bafca24779c9c527d362acef40b65d4cff1a9d081ded07ac70e4`；
- S8 adapter contract file/canonical SHA-256=
  `00cda4f20b10729a085967085b000497344c540dc87d4ed9b7cd8f5a360672e9`/
  `1eddecaa2a7809c98cbdd7d2418826ec99a470861b064116b2d4cf828403efe7`；
- exact session source
  `inputs/research/qqq_options/trading_2537_exact_date_provider_catalog_attribution_correction_v2/run_scope.json`
  file SHA-256=`04376415dd9f6310aa796a930465f875d51effd6f681cfb2ac9da21bbde7191a`，
  ordered count=`1202`、LF SHA-256=
  `d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0`。

successor bridge 只允许把旧 `tqqq_veto` 移出 market-state conjunction 并绑定为 action guard；不得修改
V1/V2 的 contributor、growth-state、QQQ/SGOV weights、PIT、threshold、requested/evaluated range 或
zero-execution rules。

#### S9.2 pre-query capability gates

manifest replay 在任何 provider/cache/market-file 读取前必须机械检查：

| source surface | 当前 capability blocker | query disposition |
| --- | --- | --- |
| FMP SPY/QQQ | `FmpPriceProvider` 返回当前 normalized price frame，但没有逐历史 row 的 source-proven `available_at` receipt | `BLOCKED_PRE_QUERY` |
| official Cboe VIX | full-history CSV 没有逐历史 row 的 publication/availability vintage | `BLOCKED_PRE_QUERY` |
| Federal Reserve FOMC | consolidated calendar 可显示历史 meeting/statement，但当前 adapter 没有 schedule revision publication ledger | `BLOCKED_PRE_QUERY` |
| BLS CPI/NFP | current/year schedule pages 未绑定可重放的 revision `published_at` vintage | `BLOCKED_PRE_QUERY` |
| BEA PCE/GDP advance | S8 冻结 endpoint=`https://apps.bea.gov/api/data` 是 data API，不是 release-schedule revision authority | `BLOCKED_PRE_QUERY` |

当前 S9 预期 terminal 固定为
`MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE`；这表示授权已重放但
技术 gate 未通过，不得伪报为 authorization failure。actual provider/cache/file query count、orders、fills、
positions、production、broker 必须全部为 `0`。只有后续 versioned PIT archive/adapter contract 能证明
上述字段后，才允许生成新的 manifest version 并消费真实 query maxima。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_manifest_replay_gate_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_manifest_replay_gate.py`；
- `tests/test_growth_action_value_mandatory_veto_manifest_replay_gate.py`。

验收标准：

1. strict loader 递归重放 execution V2、architecture V1、S8 与 exact-1202 session source 的 file/canonical/
   ordered inventory identity；
2. successor bridge 机械证明 four-veto conjunction 与独立 action guard，不改写任何 immutable predecessor；
3. replay report 分轴记录 `authorization_state=STANDING_OWNER_SCOPE` 与 technical blocker，并在 capability
   gate 未全 PASS 时保持所有真实读取 counters 为 0；
4. negative tests 拒绝 predecessor/session/code SHA drift、重新引入 tqqq market veto、伪造
   available-at/published-at、endpoint 替换、query counter 非零、real DQ/backtest/series/execution flag；
5. focused/adjacent、Ruff、strict mypy、py_compile、generated freshness 与 formal tiers PASS；发布后的
   exact candidate 自动 replay，预期在 provider query 前 typed stop，并把下一 owner/engineering blocker
   收敛到 historical PIT receipt capability，而不是继续要求重复授权。

### S10：historical PIT receipt authority 决策包草案

Owner 于 2026-08-27 指示“好的，你继续推进”。本阶段把该指示解释为允许 Codex 在现有
non-executable `DATA_RESEARCH` 边界内，把 S9 的五个 source capability blocker 收敛为可逐项审阅的
historical PIT authority 决策对象；该指示不等于 Owner 已选定、提供或冻结任何 archive/provider，
也不改变 S9 的 `BLOCKED_PRE_QUERY`、四项 veto exact semantics 或 exact-1202 session inventory。

S10 必须机械区分四类 evidence：

1. `PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE`：provider 能按历史 as-of 重放原始 payload、schema、
   revision/adjustment vintage 与 availability receipt；可作为 primary-window authority candidate；
2. `IMMUTABLE_TIMESTAMPED_CAPTURE_OF_FROZEN_SOURCE`：保存 frozen source 原始 bytes、可信 capture
   timestamp、request/endpoint identity、checksum 与 supersession ledger；可作为 candidate，但 exact
   archive identity 与覆盖率仍需 Owner 单独冻结；
3. `FORWARD_ONLY_CAPTURE_LEDGER`：只能支持首次可靠 capture 之后的 forward research，不能回填
   2021-02-22 起的 primary window，也不能让 S9 historical replay PASS；
4. `INFERRED_OR_CURRENT_STATE_SUBSTITUTE`：以当前下载时间、页面 `Last-Modified`、session+1、文件
   日期或今日 endpoint 内容反推历史 `available_at`/`published_at`；一律 rejected，不得用于 PIT、DQ、
   series、backtest 或投资结论。

五项决策面固定如下，recommendation 只表示设计建议，不表示 authority admission：

| blocker | primary-window 可接受 authority candidate | forward-only 价值 | 明确拒绝项 |
| --- | --- | --- | --- |
| FMP SPY/QQQ | provider-native historical as-of archive，或 Owner 提供且可验证的逐 snapshot immutable ledger；必须含 per-row `available_at`、adjustment/corporate-action vintage、request、schema 与 checksum | 从首次 capture 起建立新 ledger | 当前 normalized frame、download time 或 session+1 冒充 historical PIT |
| Cboe VIX | versioned official CSV snapshot archive，或可信时间戳的 frozen official-source capture ledger；必须含 publication/capture vintage、level/session mapping 与 checksum | 从首次 capture 起支持 forward VIX receipt | 当前 full-history CSV 的页面/文件时间回填到每个历史 row |
| Federal Reserve FOMC | official versioned schedule archive，或可信时间戳的 official-page capture ledger；必须含 stable event key、revision/action、`published_at`/`captured_at`、coverage 与 supersession | 从首次 capture 起建立 schedule revision ledger | 用 meeting/result date、当前 consolidated calendar 或声明发布时间代替 schedule revision history |
| BLS CPI/NFP | official versioned schedule archive，或可信时间戳的 official-page capture ledger；字段与 Fed 同级 | 从首次 capture 起建立 schedule revision ledger | 用 release result、当前年度 schedule 或 event date 反推 historical publication |
| BEA PCE/GDP advance | 先把 frozen data API 修正为 official release-schedule authority，再绑定 versioned archive/capture ledger；必须含 taxonomy、revision、coverage 与 checksum | 从新 schedule authority 首次 capture 起可积累 | 继续把 `https://apps.bea.gov/api/data` 当作 release-schedule revision authority |

组合推荐固定为
`ACQUIRE_OR_PROVIDE_EXACT_HISTORICAL_PIT_AUTHORITY_FOR_ALL_FIVE_THEN_CREATE_SEPARATE_FREEZE_WAVE`。
它是保持已冻结四-veto semantics、primary research window 与 historical/live 可比性的唯一直接路径。
如果五项中任一只能得到 forward capture，S10 必须继续显示 primary-window replay blocked；是否把
`scheduled_event_risk_veto` 改为 forward-only diagnostic、是否缩短窗口或是否建立 governed sensitivity
角色，均属于新的 architecture/research-window Owner 决策，不能由本决策包默认采用。

本阶段新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack.py`；
- `tests/test_growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack.py`。

验收标准：

1. strict loader 递归重放 S9 policy 的 exact file/canonical identity 与五个 blocker row；
2. 五个 ordered decision row 必须包含 acceptable authority classes、minimum receipt/coverage contract、
   forward-only impact、rejected substitutes 与 Owner decision state；
3. aggregate 固定为 `OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED`，任何 row 都不得
   标记 selected/admitted/remediated，S9 继续为 technical `BLOCKED`；
4. negative tests 拒绝删除 blocker、把 forward capture 计为 historical coverage、接受 inferred
   timestamp、把 BEA data API 保留为 schedule authority、开启 provider/query/source admission/series/
   real DQ/backtest/execution；
5. focused/adjacent、Ruff、strict mypy、py_compile、generated freshness 与 formal tiers PASS；下一合法动作
   是 Owner 提供或精确批准五项 archive/source identity 后建立独立 freeze-admission wave，不是直接 query。

### S11：historical PIT static authority/receipt contract（最小 serial contract wave）

Owner 于 2026-08-27 在只读来源发现和 Web Pro exact-commit 复核后指示“采纳 Pro 的建议继续推进”。
本阶段据此冻结共享的 authority、receipt、revision、coverage proof 和状态分离合同，并固定五项候选
处置状态；不把候选 family、公开产品范围或最终日历升级为 exact authority、historical coverage、
source admission 或真实数据授权。

S10 compatibility-only failure-fix 已先在 exact commit
`5635010dc2bf8e2fa2f68fc78723b5aff380c85d` 上完成。Architecture/Contract/Integration/
Reproducibility=`878/278/995/24 passed`；parent-bound Full=
`9787 passed / 3 skipped`，artifact=`outputs/validation_runtime/full_20260827T123031Z/`
`test_runtime_summary.json`，SHA-256=
`253cf9d4b8437232650cf03011b4122c64742fe00336f3f641b38ff36c59fa3d`。失败 parent 仍为
`outputs/validation_runtime/full_20260827T025319Z/test_runtime_summary.json`。该 closeout 只证明当前
compatibility authority 与 Atlas/test-manifest freshness，不改变 S10 五项 blocker。

#### S11.1 五项候选处置

| source | frozen candidate disposition | 本阶段允许的最窄边界 | 仍未满足 |
| --- | --- | --- | --- |
| FMP | `VENDOR_EVIDENCE_REQUIRED` | 冻结 capability/license/fee evidence contract；准备但不发送询价 packet | versioned/as-of payload、row `available_at`、corporate-action vintage、reissue/supersession、许可与报价 |
| Cboe | `VENDOR_EVIDENCE_REQUIRED` | 冻结 Main Channel EOD candidate family 与 evidence contract；准备但不发送询价 packet | 原始逐日交付、correction/reissue、delivery receipt、digest/supersession、许可与报价 |
| Fed | `FREEZE_CANDIDATE` | 冻结 2021--2025 official annual tentative FOMC schedule press-release family、source role 与 precedence | exact bytes/digest、完整 amendment inventory、逐 cutoff coverage；不得计为 exact authority 或 coverage PASS |
| BLS | `INVENTORY_ONLY` | 盘点 annual schedule、official notices 与可信 capture 候选 | initial/revision/final 完整链、digest、trusted-capture policy 与逐 cutoff coverage |
| BEA | `INVENTORY_ONLY` | 把 data API 降为 rejected schedule authority，盘点 SCB/update/archive 候选 | 可复取 official schedule identity、更新前版本、revision diff、digest 与逐 cutoff coverage |

以上五行只冻结 `candidate_disposition`；只有 Fed 的 authority family/role/precedence 可以标为
`candidate_source_approved=true`。五项 `exact_authority_identity_frozen`、
`historical_coverage_proven`、`source_contract_admitted`、`runtime_authorized` 和
`blocker_remediated` 均保持 false。`FREEZE_CANDIDATE` 不得被计入 S10 的 exact authority 计数。

#### S11.2 共享静态合同

1. `authority_class` 沿用 S10 三种可接纳 historical class，不增加 inferred/current-state class；
2. `authority_role` 显式区分 initial schedule、revision notice、immutable capture、terminal
   reconciliation 与 result-release-not-schedule-authority；price/VIX 另有独立 publication role；
3. receipt identity 必须绑定 source/document id、exact URL、immutable payload SHA-256、version/revision、
   `available_at`、capture authority 与 supersession lineage；`downloaded_at` 只允许 audit，不得作为 PIT；
4. schedule ledger append-only，至少支持 `ADD|MOVE|TIME_CHANGE|CANCEL|RESTORE|METADATA_CORRECTION`；
   stable event key 不得只用可变的 `scheduled_for`，被 supersede 的 revision 不得删除；
5. current/final calendar 只能承担 `TERMINAL_RECONCILIATION_ONLY`，不能证明历史 cutoff 当时可见状态；
6. `available_at_precision=DATE_ONLY` 在没有 Owner-frozen conservative cutoff policy 时不得跨越 intraday
   cutoff；event date、scheduled time、page Last-Modified、download time 或 session+1 均不得推断
   `available_at`；
7. inventory 可以记录 `REGULAR_SCHEDULED|SPECIAL_OR_EMERGENCY|NOTATION_VOTE|RESCHEDULE_NOTICE|`
   `CANCELLATION|RESTORATION`，但只有既有 frozen event type 精确映射成功的 row 才能影响 veto；未映射
   event 返回 `INSUFFICIENT_OWNER_EVENT_TAXONOMY_REQUIRED`，不得自动扩张 event universe；
8. 对每个 exact-1202 decision cutoff，只能选择 `available_at <= cutoff` 的最新 admitted revision，并证明
   coverage 到 next action close、无 later-revision leakage、无 unresolved conflict、receipt lineage 完整；
9. FMP/Cboe 仍需分别证明 frozen warm-up、adjustment/reissue lineage 与 license；1202 个 target row
   存在不能替代 warm-up 或 historical version coverage；
10. `candidate_source_approved`、`exact_authority_identity_frozen`、`source_contract_admitted`、
    `historical_coverage_proven` 与 `runtime_authorized` 为独立状态，任何前态都不得自动升级后态。

#### S11.3 产物、验证与退出条件

新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract.py`；
- `tests/test_growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract.py`。

验收标准：

1. strict loader 递归重放 S10 exact file/canonical identity、五个 blocker row 与 exact-1202 window；
2. 五项 ordered source row 精确为 FMP/Cboe=`VENDOR_EVIDENCE_REQUIRED`、Fed=`FREEZE_CANDIDATE`、
   BLS/BEA=`INVENTORY_ONLY`，但 exact authority/coverage/admission/runtime/remediation 仍为 0/5；
3. receipt、schedule revision、event taxonomy、cutoff coverage、falsification 与状态机均为 typed contract；
4. negative tests 拒绝 current/final source 冒充历史 ledger、推断 timestamp、date-only intraday crossing、
   deletion of superseded rows、partial source freeze 生成 series、capability evidence 自动授权 query/DQ/backtest；
5. aggregate terminal 固定为
   `S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN`；S9/S10 仍保持技术阻塞；
6. focused/adjacent、Ruff、strict mypy、py_compile、Architecture/Contract/Integration/Reproducibility/Full
   与 generated freshness PASS；所有 provider/network/cache/market-file/source-admission/series/real-DQ/
   backtest/orders/fills/positions/production/broker counters 保持 0。

S11 发布后才允许从同一 exact main base 拆分 official-schedule inventory 与 vendor evidence packet 两条
只读 evidence lane。实际 vendor contact、付费、真实 payload、DQ、series 或 backtest 都需要新的独立
Owner 授权。

### S12：五来源公开证据复核与 Owner 决策包

Owner 于 2026-08-28 授权对 FMP、Cboe、Fed、BLS、BEA 进行只读来源发现和候选评估，要求优先
official 或 vendor-native versioned archive，并先提交候选来源、覆盖证明、费用和缺口；未授权购买、
真实市场 payload、DQ 或 backtest。S12 只固化公开页面观察结果和下一步审批请求，不下载或 hash
远端 schedule 文件，不联系供应商，也不改变 S11 的 source state。

#### S12.1 复核结论

| source | 公开证据能证明 | 公开证据不能证明 | 建议（仍需 Owner 批准） |
| --- | --- | --- | --- |
| FMP | dividend-adjusted EOD 产品定义、名义历史范围与公开 self-serve 价格参考 | per-row historical `available_at`、versioned/as-of archive、adjustment/reissue lineage、required-scope license/quote | 保持 `VENDOR_EVIDENCE_REQUIRED`，另行批准后再发送询证 |
| Cboe | Main Channel EOD 包含 VIX、名义历史范围与 next-trading-day delivery 描述 | 原始 daily receipt/digest、correction/reissue/supersession、required product quote/license | 保持 `VENDOR_EVIDENCE_REQUIRED`，All Access 价格仅作旁证，不替代产品报价 |
| Fed | 2021--2025 timestamped annual tentative-schedule press-release family 与 official calendar | 完整 amendment/reschedule/cancel ledger、exact bytes/digest、逐 cutoff coverage | 保留既有 `FREEZE_CANDIDATE` family，不升级 exact authority |
| BLS | official archived annual schedule index、annual pages、CPI/NFP result archive | initial/revision/final 的完整版本链、可信 `available_at`、exact bytes/digest | exact-document inventory 后建议 Owner 决定是否升级 candidate family |
| BEA | 2021--2025 official annual PDF identity、SCB schedule、update notice example 与 result archive | superseded schedule bytes、完整 revision notice set、逐 revision lineage 与 cutoff coverage | 修正 S11 的 locator 缺口；exact-document inventory 后建议 Owner 决定是否升级 candidate family |

`nominal_2021_2025_scope_located=true` 只说明公开产品或官方 document family 的标称日期范围覆盖研究窗，
不等价于 exact remote bytes、historical `available_at`、revision/reissue lineage 或 exact-1202 cutoff
coverage。FMP 的 USD 0/22/59/149 和 Cboe All Access 的 trial/2499/4599 等值只能作为公开价格参考，
不得解释成 required-scope 报价、许可或购买建议。

#### S12.2 状态、审批面与退出条件

1. S12 递归绑定 S11 exact file SHA-256=`32071acfde9aa4c432f26964a839fe288de69f1e1a6fce99a2c07431e2a4caa7`
   与 canonical SHA-256=`f8aa502366a9e952810c25510e758bd8f1d0a2492535056f46cda730cf05ba10`；
2. 五项 `candidate_source_approved` 状态精确保持 1/5（仅 Fed），所有推荐均不得自动改变该状态；
3. `exact_authority_identity_frozen`、`historical_coverage_proven`、`source_contract_admitted`、
   `runtime_authorized` 与 `blocker_remediated` 保持 0/5；
4. 待 Owner 分别决定：是否允许向 FMP/Cboe 发送窄范围 capability/license/quote 询证；是否允许对
   Fed/BLS/BEA official schedule metadata documents 做 bounded download、exact digest inventory 和
   revision-gap analysis；两项批准都不自动授权 provider query、admission、DQ、series 或 backtest；
5. 浏览器执行过公开 documentation discovery，但浏览器工具未提供可审计的 exact HTTP request count；
   S12 显式记录 `count_recorded=false`，不伪造零计数。provider API query、vendor contact、purchase、
   official document file download、real payload、cache/market-file read、admission、series、DQ、backtest、
   orders/fills/positions/production/broker 实际计数均为 0；
6. terminal 固定为
   `S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5`。

新增：

- `config/research/qc_qqq_options_growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review.py`；
- `tests/test_growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review.py`。

## 7. Path、contract 与 evidence claims

task-owned paths：本 requirement、逐阶段 immutable config、typed loader/pure adapter 与 focused tests。

coordinator-owned paths：TRADING-2542F/2542G canonical fragments、task index/views、
`docs/system_flow.md`、Atlas/architecture/report-flow generated authority及其 exact freshness tests。

evidence roles：

- architecture/compatibility V1=`OWNER_EXACT_FROZEN_IMMUTABLE_AUTHORITY`；
- current runtime/capability graph/free-feature modules=`READINESS_OR_CANDIDATE_EVIDENCE_ONLY`；
- diagnostic/PIT-warning event features=`NOT_ADMITTED_MANDATORY_VETO_SOURCE`；
- new source-contract artifact=`NON_EXECUTABLE_READINESS_AUTHORITY_NO_SERIES`。

## 8. Lifecycle 与安全边界

- current S9 frozen base：`cfb3c8559cbbc38aecf0b0a291456bc3411f9162`；
- current S9 branch：`codex/trading-2542g-s9-manifest-replay-gate`；
- workspace：复用 `D:\Work\AITradingSystem`，不创建额外 worktree/clone/cache；
- known-unrelated exclusion：`docs/research/growth_tilt_owner_diagnosis_pack.md` 不读、不 hash、不
  diff、不 stage、不修改；
- S9 pre-query external action maxima：provider/network/cache/market-file reads=`0` until replay PASS；
  production effect=`none`，broker action=`none`；
- recovery：tracked bytes 由 Git/main 恢复；没有 provider、dataset、backtest 或 broker state；
- exit condition：validated non-executable readiness artifact 发布，后续具体 source formula/threshold
  仍以独立 Owner exact-freeze 为前提。

## 9. 进度记录

- 2026-08-28：S12 只读公开来源复核已形成 strict owner-review draft。新增五项 ordered evidence rows、
  公开 locator、nominal coverage/PIT proof 分离、fee reference/required-scope quote 分离及两项独立 Owner
  decision request；递归绑定 S11 exact identity，保持 candidate approved=1/5，exact authority/coverage/
  admission/runtime/remediation=0/5。S12+S11 focused parallel=`61 passed`，Ruff、strict mypy、py_compile
  与 strict load PASS；未下载 official schedule 文件、未查询 provider、未联系 vendor、未购买，也未运行
  DQ/series/backtest。待完成 generated authority rebuild 与 formal validation 后发布评审包。
- 2026-08-28：failure-fix candidate=`de11ef4453d326195f05886bff7059ae99f4cf86` 的 parent-bound
  Architecture/Contract/Integration/Reproducibility=`878/278/995/24 passed`；Full=
  `9830 passed / 3 skipped / 1 failed`，上一轮两个 report-flow aggregate failure 已消失。唯一剩余失败为
  local ignored Atlas canonical page 的 `repository_commit=58f0f4c9...`，而 compatibility-authority rebuild
  后最终 tracked candidate=`de11ef445...`；`test_local_canonical_page_uses_current_successor_identity_when_available`
  因 exact-commit mismatch 正确 fail-closed。失败 Full=
  `outputs/validation_runtime/full_20260827T154449Z/test_runtime_summary.json`，SHA-256=
  `a26536b1382cb080a5486590dd60b312ef2613dbe3970beeacea79f658e98790`。v11 已失败释放；下一事务必须先
  完成全部 tracked generated commit，再以最终 candidate SHA 重新 hydrate Atlas ignored artifact，然后才
  进入 formal 与 parent-bound Full。不得把 generator 前 page identity 视为 current，也不得改变 S11 语义。
- 2026-08-28：candidate=`a137bbcc818d6479a18592d6d72eecf07a67d468` 的五类 generated authority 与
  Architecture/Contract/Integration/Reproducibility=`878/278/995/24 passed`；首次实际 Full=
  `9829 passed / 3 skipped / 2 failed`。两个失败均来自
  `tests/test_devx_006d_report_catalog_flow_authority.py` 的 aggregate entry count 仍固定为 `3068`，而
  S11 在 `docs/system_flow.md` 新增 4 个 lossless block 后官方 report-flow authority 已正确产出 `3072`；
  单 target 的 `1143` regression、source seal、fragments 与 compatibility authority 均已一致。失败 Full=
  `outputs/validation_runtime/full_20260827T145645Z/test_runtime_summary.json`，SHA-256=
  `ae2149d9bc216231d7130f6de3c7e3eb9bd7d11cb5678f01c438a0f350f516a8`。v9 已 fail-closed 释放；
  failure-fix 只同步两个 aggregate 常量，并以该 Full 为 immutable parent 完整重建、重跑 formal 与
  `failure_fix_rerun`，不改变 report-flow source、策略、DQ/PIT 或授权边界。
- 2026-08-27：failure-fix candidate=`9f3e4b8d920f50383c5acb7180df29363577b891` 的
  Architecture/Contract/Integration/Reproducibility=`878/278/995/24 passed`。在启动 Full 前，coordinator
  先手动 checkpoint `FULL_DISPATCHED`，随后 `run_validation_tier.py full` 因其必须从
  `FORMAL_VALIDATION_PRE` 自行原子完成 dispatch 而以 `PUBLICATION_PHASE_MISMATCH` 拒绝；pytest 未启动，
  没有 Full result 或数据/执行动作。v8 已以 immutable dispatch claim
  `0336dcc6de95eae3e7120f83c18930da8ad1285884504df8891474d9fb08e98a` fail-closed 释放。下一事务
  不手工写 `FULL_DISPATCHED`，由 Full runner 完整执行 validate → dispatch → pytest → result；不得把 v8
  的无测试 dispatch claim 当作 Full evidence。
- 2026-08-27：S11 publication v4 的五类 generated authority 全部重建 PASS，candidate=
  `491988d256ddc95745e90b6e84934fbcf2daf4eb`；首次 Architecture formal=`877 passed / 1 failed`。
  唯一失败是 `tests/test_arch_004g_deprecation.py` 仍冻结 S10 的 `1166 modules / 1326 tests`，而官方
  generator 已确定性产出 S11 的 `1167 / 1327` 与 inventory id
  `arch_004g_deprecation_inventory_65f81781bf916306f464`。失败 artifact=
  `outputs/validation_runtime/architecture-fitness_20260827T134856Z/test_runtime_summary.json`，SHA-256=
  `2d6ba37afa25017c42946f4d3eb9efa03746950d7af79e4ba520485b2e9abd8c`；v4 已 fail-closed 释放，
  未运行后续 formal/Full、未集成或推送。failure-fix 只同步 repository-count ratchet 后完整重放生成与
  formal tiers，不改变 deprecation lifecycle、五项 blocker、veto、DQ/PIT 或授权边界。
- 2026-08-27：S11 source-preparation commit=`139c3a86d24b307d4613bd52b3a5a65014df5a25`；
  config file/canonical SHA-256=`32071acfde9aa4c432f26964a839fe288de69f1e1a6fce99a2c07431e2a4caa7`/
  `f8aa502366a9e952810c25510e758bd8f1d0a2492535056f46cda730cf05ba10`，S11+S10 focused=
  `66 passed`，Ruff、strict mypy、py_compile 与 strict load PASS。publication v3 的 canonical task、
  architecture manifests 与 Atlas 17-artifact rebuild PASS；report-flow builder 随后以
  `RCF_SOURCE_SEAL_DRIFT` 拒绝 S11 新增流程后的旧 `docs/system_flow.md` seal（expected
  `2,297,456` bytes，actual `2,299,528` bytes）。v3 已 fail-closed 释放、未生成 candidate、未运行
  formal/Full、未集成或推送。通过 builder 的 `EXACT_BLANK_LINE_BLOCKS_V1` 与 Git blob 算法只读计算
  reviewed successor seal=`2,299,528 bytes / a86a05...1d20 / 7b49ee...1f2c / 1,143 entries`；
  只同步 source identity 与对应 frozen regression，不改变五项 blocker、veto、DQ/PIT 或授权语义。
- 2026-08-27：Owner 指示“好的，你继续推进”；READ_ONLY 与 SINGLE_LANE START/LANE preflight PASS，
  main=origin/main=`45618688e05fa67531950da93a81861a410dadff`、active lease=0。选择 S10
  non-executable historical PIT receipt authority decision-pack draft：只把 S9 五个 blocker 收敛成
  authority class、minimum receipt/coverage、forward-only impact 与 rejected substitute，不选择或接纳
  source/archive，不运行 provider/cache/market-file、真实 DQ/backtest/series，
  orders/fills/positions/production/broker=`0`。
- 2026-08-27：S10 decision pack file/canonical SHA-256=
  `110830c6c14cf7112db2b0a9cefcd516650db66e0f3f7317ea4d6f266356d1fb`/
  `f13ec082d07f5fd33bfb2c2dab40d999d2a146afb35bfbf4cf92ed7e8a9cd1d3`；strict loader 递归重放
  S9 exact identity、五个 blocker/endpoint 与 exact primary window，aggregate=
  `OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED`。S10 + 完整 mandatory-veto
  adjacent=`235 passed`，Ruff、strict mypy、py_compile PASS。task-activation publication v1 在
  `TASK_SOURCE_PRE_WRITE` 后因其只声明 task-source paths、不能作为最终 publication transaction 而按
  fail-closed 释放；没有 generated/candidate/Full/main/remote mutation。最终发布使用包含全部 S10、shared、
  generator 与 formal resource claims 的新 transaction。provider/network/cache/market-file、source
  admission、series、真实 DQ/backtest 与 orders/fills/positions/production/broker 继续为 `0`。
- 2026-08-27：S10 candidate `8c54a54fa77d0291508b5fe7967081725c5c742d` 的
  Architecture/Contract/Integration/Reproducibility=`878/278/995/24 passed`；首次 Full=
  `9784 passed / 3 skipped / 3 failed`。三个失败全部由 Atlas S10 expectation 修复后再次生成
  `arch_004e_test_manifest.yaml`，但 compatibility authority 尚绑定前一 manifest hash 所致：两项历史
  compatibility source-current 检查发现 current SHA 滞后，一项 authority freshness 指向已被新 index
  取代的 fragment path。S10 loader、五项 blocker、Atlas renderer、DQ/PIT 与交易边界没有失败。
  failure-fix 只刷新 task progress 与 current compatibility authority；最终 Full 必须使用
  `outputs/validation_runtime/full_20260827T025319Z/test_runtime_summary.json`（SHA-256=
  `8d80a5752a7ff5003841b295d890d3d2cd05d4679831ad3ffb81860597524c7f`）作为 immutable parent，
  不允许用 focused/serial PASS 替代。

- 2026-08-27：S9 candidate=`b63bd87270926bfbd361cb7ac32682eb485fe3e0` 已完成
  Architecture/Contract/Integration/Reproducibility=`878/278/995/24 passed`，最终 Full=
  `9756 passed / 3 skipped`，并 ordinary-push 后验证 `main=origin/main=candidate`。发布后的真实仓库
  static manifest replay 在任何 provider query 前暴露运行时校验缺陷：`RepositoryReplayContext` 将
  `candidate_sha/local_main_sha/origin_main_sha` 错用 64 位 SHA-256 validator，而本仓库 Git object id 为
  40 位 SHA-1，导致 Pydantic `ValidationError`，未能产出预期 typed BLOCKED report。修复波次必须接受
  lowercase 40/64 位 Git object id、拒绝其他长度/字符，补真实 CLI 40 位回归测试；完成前保持
  provider/network/cache/market-file、source admission、series、真实 DQ/backtest 与
  orders/fills/positions/production/broker 全部为 `0`，且不改变五项 PIT source blocker 或 Owner blocker。
  修复后 policy file/canonical SHA-256=`54458cc019fd0bdfa67d4a4d2f6836777afdd86b3709932ee70a68cb0fd89364`/
  `7bfed1414ce85b777acdc807f0e425d1752dd7331de347b84e08b154b03aed10`，executor SHA-256=
  `a564908f1d48cb69fb9cbf343a7e3f7f52643ecd333eb9be57ee294ab8846ccf`；focused=`22 passed`，
  full mandatory-veto adjacent=`233 passed`，Ruff、strict mypy、py_compile PASS。真实 main CLI 只允许在
  final candidate 发布并满足 `HEAD=main=origin/main` 后重放。

- 2026-08-26：S9 strict gate 已实现并完成本地 focused/adjacent validation。policy file/canonical
  初版 policy file/canonical SHA-256=`46fca457b25b40b409568cb75e080e51104c79963e6097683c53747a8a9038c8`/
  `f5cee930aaa9817088895e6fc8ef1bd65ce2f4418b8da8c0ebecd184993b25f6`。首次 formal Architecture
  tier=`876 passed / 2 failed`，精确发现新增 module manifest freshness 与 CLI direct writer ratchet；没有
  降级为 serial PASS 或绕过。CLI 已改用 canonical `write_bytes_atomic`，更新后 policy file/canonical
  SHA-256=`4516b74310e446954fcac0fd1988852e613b0a5647fb7286210d82d605304787`/
  `4833df465f3aae498ca9bbefcbd2764a21c5dab810e5dec720d37352f77abc4e`，绑定的 replay executor
  SHA-256=`139936d26522e629e90bae5d850f55a2871318b83153be9fdcff5948daccbc8f`；architecture manifest
  由官方 generator 在 failure-fix candidate 重建。第二轮 Architecture=`877 passed / 1 failed`，仅剩
  `arch_004g_deprecation_inventory` frozen regression 仍绑定 S8 的 `1164/1324` module/test counts；已按
  generator 输出同步到 S9 `1165/1325` 与 inventory id
  `arch_004g_deprecation_inventory_64e7ee83696d68abb801`，不改变任何 deprecation lifecycle 或 direct-writer
  allowance。第三轮 Architecture=`878 passed`，Contract/Integration/Reproducibility=
  `278/995/24 passed`；首次 Full=`9749 passed / 3 skipped / 7 failed`。7 项均为 `docs/system_flow.md`、
  deprecation regression 与 Atlas page bytes 尚未进入 compatibility/report-flow/Atlas generated authority；
  S9 loader、source gate、DQ、策略与交易边界没有失败。新 system-flow reviewed source seal=
  `2295512 bytes / c019cf452293a1cba3fa6626c3282a6c1f6f5860a0ac4b13fd0c4d40619401eb /
  git blob 6e6c0376844986397ce7e8cd00f7ccfdf1a0bde8 / 1135 entries`，后续 failure-fix transaction
  只运行 official compatibility/report-flow/Atlas generators，并以失败 Full artifact 为 parent 重跑。
  首次 official report-flow + compatibility rebuild 后，原 7 项中 5 项已通过；focused 仅剩 2 个 total
  entry-count regression 仍为 `3060`，已按 `1371 + 558 + 1135 = 3064` 同步并重新生成 compatibility
  authority。Atlas 留到 clean exact candidate 上单独重建，避免把 dirty task/source bytes 冒充 exact commit。
  首次 Atlas render 又因最新 task event notes 未携带 `Supporting requirement:` 而 fail closed，canonical
  projection 的 `requirement_refs` 变为空；这是 task-source binding 问题。后续 event 必须重新携带本
  requirement exact path，并在 clean commit 后再运行 Atlas generator。
  authority、exact-1202 session 与 four-veto + action-guard compatibility replay 全部 `PASS`；五个
  source capability row 均在 query 前给出 typed blocker，整体 terminal=
  `MANIFEST_REPLAY_BLOCKED_PRE_PROVIDER_QUERY_SOURCE_RECEIPT_CAPABILITY_INCOMPLETE`。focused + Atlas=
  `19 passed`，完整 mandatory-veto adjacent=`224 passed`，Ruff/strict mypy PASS。task 状态转为
  `BLOCKED_OWNER_INPUT`，但阻塞内容是需要可评审的 versioned historical PIT receipt authority 或
  source-contract correction，不是再次授权 replay。provider/network/cache/market-file、source admission、
  series、真实 DQ/backtest 与 orders/fills/positions/production/broker 全为 `0`；formal publication validation
  绑定最终 candidate 运行。

- 2026-08-26：S8 candidate=`cfb3c8559cbbc38aecf0b0a291456bc3411f9162` 已完成 Architecture/
  Contract/Integration/Reproducibility=`878/278/995/24 passed`，Full=`9743 passed / 3 skipped`，并
  ordinary-push 后验证 `main=origin/main=candidate`。Owner 在收到下一步需 separately authorize exact
  manifest replay/source admission 的说明后指示“好的，你继续”；S9 SINGLE_LANE START/LANE preflight
  PASS，选择 pre-query manifest replay capability gate。静态能力审计确认旧 execution V2 的 five-veto
  surface 已被 architecture V1 的 four-veto + action-guard successor 取代，同时 FMP/Cboe historical
  `available_at`、Fed/BLS schedule revision vintage 与 BEA schedule endpoint 尚不能被当前真实 adapter
  证明；S9 在这些 gate PASS 前保持 provider/network/cache/market-file query=0，真实 DQ/backtest/series
  与 orders/fills/positions/production/broker=0。

- 2026-08-26：S7 candidate=`107c4b53b5e68d384cafafac79939c4cbdd7654a` 已完成 Architecture/
  Contract/Integration/Reproducibility=`878/278/995/24 passed`，Full=`9726 passed / 3 skipped`，并
  ordinary-push 后验证 `main=origin/main=candidate`。Owner 指示“继续”；S8 READ_ONLY 与
  SINGLE_LANE START/LANE preflight PASS，选择纯内存 PIT receipt-bound adapter wave。S8 不访问
  provider/network/cache/filesystem market data，不运行 manifest replay/真实 DQ/backtest，不生成
  series/R1；orders/fills/positions/production/broker=0。
- 2026-08-26：S8 PIT receipt adapter contract file/canonical SHA-256=
  `00cda4f20b10729a085967085b000497344c540dc87d4ed9b7cd8f5a360672e9`/
  `1eddecaa2a7809c98cbdd7d2418826ec99a470861b064116b2d4cf828403efe7`。FMP SPY/QQQ、official
  Cboe VIX 与 official Fed/BLS/BEA capture receipt 已实现为纯 caller-injected mapping/rows adapter；
  checksum、PIT timestamp、exact session、adjustment、three-authority coverage、revision conflict 与 trend
  supplemental binding 均 fail closed。S8 focused=`17 passed`，完整 mandatory-veto authority adjacent=
  `173 passed`，Ruff、strict mypy、py_compile PASS；terminal=
  `SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4`。没有
  provider/network/cache/filesystem market-data IO、observed manifest/inventory、series/R1、真实 DQ/backtest
  或交易动作；正式 generated freshness 与 formal tiers 绑定最终 publication candidate 运行。

- 2026-08-26：Owner 在收到 S6 精确 file/canonical SHA 与“合同冻结不等于 source/inventory admission”
  的说明后指示“继续推进”。READ_ONLY audit 与 SINGLE_LANE START/LANE preflight PASS，
  main=origin/main=`36ec9fb3c9e534b2d910a46e4e216b169da9e046`、active lease=0。选择最小 S7 serial
  freeze-admission wave；本波只绑定 S6 immutable identity 并冻结四项 adapter/manifest/inventory 合同，
  source/inventory admission 继续为 0/4；不访问 provider/cache/真实数据，不执行 manifest、series、真实
  DQ/backtest，orders/fills/positions/production/broker=0。
- 2026-08-26：S7 freeze-admission file/canonical SHA-256=
  `d4e431350c0220934d48482e1cfd02287b06f291f8903f58901d75735d8b1636`/
  `3344d14fd7b94b6951a8f676e77674c50b1dbe38820f83b6c45f96d4727a8405`；strict loader 递归重放 S6
  exact identity、7 个 candidate、4 个 review/decision row、exact-1202 warmup/state surface 与 14 项
  manifest gate，aggregate=
  `OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4`。S7+S6 focused=
  `46 passed`，Ruff、strict mypy、py_compile PASS；adapter implementation、provider/cache/真实数据、
  observed inventory/manifest、series/R1、真实 DQ/backtest 与交易动作仍全部关闭。

- 2026-08-26：Owner 在收到 S4A V2 两份精确 file/canonical SHA 与“唯一阻塞为 exact-freeze”的说明后
  指示“继续吧”；按 standing owner scope replay，S4B 只接纳四项 exact semantics 为 immutable policy，
  不接纳 producer、observed inventory 或 series。READ_ONLY 与 SINGLE_LANE START/LANE preflight PASS，
  main=origin/main=`79427bb32c1e57a818d901f154ebdcdecb07add9`、active lease=0。选择最小 serial
  freeze-admission wave；synthetic producer/adapters 必须从本波发布后的新 exact base 另行开始，真实数据、
  provider/cache、DQ/backtest/series 与 orders/fills/positions/production/broker 仍未授权。
- 2026-08-26：S4B freeze-admission file/canonical SHA-256=
  `ef075527750efd24433eafbd8a2e586104562868f4ce2b666043069fe5368765`/
  `97f3678417b5dcb0a4965a308953552d17d17e4cb947532316dceca2506df147`；strict loader 递归重放
  S4A V2 与 calculation-semantics exact identities，aggregate=`OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4`。
  freeze-admission + S4A V2 + source-wave adjacent=`62 passed`，Ruff、strict mypy、py_compile PASS；
  Owner freeze 与 producer/inventory admission 已机械分离，provider/cache/真实数据/series/R1/DQ/backtest/
  orders/fills/positions/production/broker 仍为 0。正式 formal tiers 与 generated authority freshness 绑定
  最终 publication candidate 运行。
- 2026-08-26：S4B candidate=`be2492b53f53fc6d89145cb1ccbdec5e3296e15e` 已完成 Architecture/
  Contract/Integration/Reproducibility=`878/278/995/24 passed`，Full=`9666 passed / 3 skipped`，并
  ordinary-push 后验证 `main=origin/main=candidate`。publication transaction v3 completed、lease released，
  临时 task branch 已删除。S5 从该新 exact base 以独立 SINGLE_LANE 开始；仍无 provider/cache/真实数据/
  series/R1/DQ/backtest，orders/fills/positions/production/broker=0。
- 2026-08-26：S5 synthetic producer contract file/canonical SHA-256=
  `14a8995e0bcb5cdc1a5fccb67d6389c5e72fb65ce1efdb926d1f9520e1d4d314`/
  `c064ec2418f43184e89fdecdf1ced60c932b15e5de6b6548fa01dc6af99ac95c`；四个纯内存
  evaluator 已实现 exact windows/operators/clock/revision/coverage/state transition，terminal=
  `SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4`。首轮 S5+S4B adjacent=
  `33 passed`，扩展 S5/S4B/S4A/source/Atlas adjacent=`99 passed`，Ruff、strict mypy、py_compile PASS；
  未读取 filesystem market data、cache、network 或
  provider，未生成 series/R1，未运行真实 DQ/backtest，orders/fills/positions/production/broker=0。
- 2026-08-26：Owner 指示 Codex 先独立准备后续内容；S6 real-source admission review
  file/canonical SHA-256=`d0adae89a1faf7c160cf82edc9d51ede74fa2ea279fcc2526c009752a9a5b57e`/
  `be705f1b46431e432169b186db6d336bb68d51cf296ca08ca5d6cca465ffc6e3`。评审包固定 FMP
  SPY/QQQ、official Cboe VIX、Marketstack/FRED diagnostic-only 以及 planned Fed/BLS/BEA official capture
  contracts，定义 exact-1202 target/warmup/state lineage、14 项 manifest replay gate 和 Owner 决策矩阵；
  terminal=`OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4`。focused=`15 passed`，S6+S5+
  prior authority adjacent=`125 passed`，Ruff、strict mypy、py_compile PASS；没有 provider query、cache/
  真实数据读取、adapter execution、observed inventory/manifest、series/R1、真实 DQ/backtest 或交易动作。

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
- 2026-08-26：Owner 指示“参考这个结论推进”，授权 S4A non-executable serial V2 contract wave；
  canonical task 已恢复为 `IN_PROGRESS`。calculation-semantics file/canonical SHA-256=
  `813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0`/
  `824ef20a66e4eba3c2841489cae8b03ff3a6cad4f73003469c086d8e09237cf1`；V2 decision-pack
  file/canonical SHA-256=`d08480c07047e636f8b4a8208ec60406acd5debdc60f30541411310e401b789f`/
  `99ed7dbdac82faf594633ab25be1ffb1417709030af0817fb19c4ace332dc389`。四项 operator、full-window、
  time/PIT、event revision/coverage 与 trend state transition 已改为 typed contract；V2+V1+S3+source-wave+
  Atlas adjacent=`97 passed`，Ruff、strict mypy、py_compile PASS。V2 bytes 尚未 Owner exact-freeze，
  owner-frozen/admitted 仍为 `0/4`；未实现 producer，未读取 provider/cache 或真实数据，未生成
  series/R1 manifest，未运行真实 DQ/backtest，orders/fills/positions/production/broker=`0`；正式
   Architecture/Contract/Integration/Reproducibility/Full 绑定最终 publication candidate 运行。
- 2026-08-26：candidate `5cfe9df194ce62ccb32d218c8610500fe832de16` 的 formal
  Architecture Fitness=`877 passed / 1 failed`；唯一失败为新增一个 typed loader module 与一个
  synthetic test file 后，ARCH-004G repository inventory 从 `1156/1318` 漂移到 `1157/1319`，
  inventory id 从 `arch_004g_deprecation_inventory_6688790249e4cce3a8c1` 变为
  `arch_004g_deprecation_inventory_f3e9b45084cb2290f70b`。publication transaction
  `trading-2542g-s4a-publication-20260826-v1` 已按 FAIL 释放，证据保留在
  `outputs/validation_runtime/architecture-fitness_20260825T182521Z/test_runtime_summary.json`；
  本波次只刷新与实际仓库计数一致的 deterministic removal-blocking ratchet，不改变任何弃用 lifecycle、
  veto 语义、DQ/PIT、阈值或授权边界。修正后必须以新 candidate 与新 publication transaction 重跑全部
  formal tiers。
- 2026-08-26：publication v2 acquire 命令误用了 PowerShell 保留变量 `$args`，导致 immutable
  transaction 只登记了末端 resource paths、未登记 owned paths 与 generator order；
  `GENERATED_REBUILD_PRE` 以 `PUBLICATION_GENERATOR_ORDER_MISMATCH` 正确拒绝。由于同一 shell 行仍继续
  启动了 architecture generator，产生的唯一 tracked byte 是
  `inputs/architecture/arch_004e_test_manifest.yaml` 中上述测试文件 SHA 的确定性刷新；该 byte 已以
  incident-preservation commit `cffce3b0d` 保存，但不作为合格 generated evidence。v2 在 worktree 恢复
  clean 后按 FAIL 释放；后续 v3 必须使用非保留参数变量、先重放 transaction claims，再从五项 generator
  顺序的第一项完整重建。未读取 known-unrelated 文件、provider/cache 或真实数据，未触发 DQ/backtest/
  execution/broker。
- 2026-08-26：v3 candidate `53e80e7d12c64ae8336f49c04746799ce3a64194` 的 Architecture/
  Contract/Integration/Reproducibility 分别 `878/278/995/24 passed`；Full=`9646 passed / 3 skipped /
  1 failed`。唯一失败为 local ignored Atlas canonical page 仍绑定 generator 前 source commit
  `9d50bf25703e63c8c8539ab9c41736304b8452a1`，而最终 candidate 已因 compatibility-authority rebuild
  前进到 `53e80e7d...`；exact-commit freshness test 正确 fail closed。v3 已用
  `outputs/validation_runtime/full_20260825T185227Z/test_runtime_summary.json` 作为 immutable parent evidence
  按 FAIL 释放，未集成、未推送。下一 v4 从新的精确 lane head 完整回放 generator，并在最终 candidate
  identity 不再变化后重新 hydrate Atlas；Full 必须使用 `failure_fix_rerun` 且绑定上述 failed Full parent。
