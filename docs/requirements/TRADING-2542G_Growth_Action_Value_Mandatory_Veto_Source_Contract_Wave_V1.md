# TRADING-2542G：Growth Action-Value Mandatory Veto Source-Contract Wave V1

## 1. 状态与 Owner 决策

- task id：`TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1`；
- priority：`P0`；
- governed mode：`SINGLE_LANE` serial consumer-contract wave；
- current status：`IN_PROGRESS_S5_SYNTHETIC_PRODUCER_CONFORMANCE`；
- Owner decision：
  `owner_decision:TRADING-2542F-2542G:2026-08-25:approve_exact_architecture_freeze_and_source_contract_followup_v1`；
- S4A Owner decision：
  `owner_decision:TRADING-2542G:S4A:2026-08-26:authorize_exact_calculation_time_state_contract_v2`；
- S4B Owner decision：
  `owner_decision:TRADING-2542G:S4B:2026-08-26:freeze_s4a_v2_exact_semantics_and_continue_non_executable_admission_v1`；
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
