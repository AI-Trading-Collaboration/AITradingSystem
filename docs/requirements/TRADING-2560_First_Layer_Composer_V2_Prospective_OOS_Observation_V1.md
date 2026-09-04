# TRADING-2560：First-layer composer v2 前瞻 OOS 观察 V1

最后更新：2026-09-04

稳定任务 ID：`TRADING-2560_FIRST_LAYER_COMPOSER_V2_PROSPECTIVE_OOS_OBSERVATION_V1`

Owner 指令：2026-09-04，在确认当前候选进入冻结观察、同时继续其他策略研究后要求
“好的，那就这样继续推进两条线吧”。

状态：`BASELINE_DONE`

## 1. 目标

为 `first_layer_composer_v2` 建立与 TRADING-2555～2559 历史开发窗口严格分离的
prospective OOS 观察链。观察链从合同冻结后的首个可用 XNYS session 开始，只保存当时可见的
feature/signal/definition identity 与 next-session decision；1/5/20-session outcome 只能在自然成熟后
追加，禁止回填冻结日前历史结果。

本任务不把 TRADING-2557 的 `INSUFFICIENT/HOLD`、TRADING-2558 的
`TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO` 或 TRADING-2559 的
`SINGLE_EPISODE_DEPENDENT` / `ANTICIPATORY_ALIGNMENT_DOMINATES` 改写为正面结论。

## 2. 冻结边界

- signal/model/threshold/state mapping：保持已接纳 `first_layer_composer_v2` 定义；
- action mapping：`constructive/risk_on -> LONG QQQ`，其他状态 `-> FLAT/cash`；
- decision/execution：after-close decision，one-session lag；
- primary cost：单向 traded notional `5 bps`；
- primary comparator：与实际 prospective exposure 匹配的静态 QQQ/zero-return-cash；
- outcome horizons：1/5/20 XNYS sessions，20-session 为主要成熟 outcome；
- 每条 observation 必须绑定 feature、signal、model、policy、DQ 与 source bytes identity；
- 任一策略定义变化必须使用新的 strategy/policy version，旧 observation 不得重写。

## 3. 阶段

### S0：结果盲合同

- 建立 versioned preregistration、strict loader/schema 与 deterministic tests；
- 定义 append-only observation、maturity update、idempotency 和 identity-drift fail-closed 语义；
- 定义 `OBSERVATION_READY`、`OBSERVATION_WRITTEN`、`OBSERVATION_ALREADY_EXISTS`、
  `MATURITY_PENDING`、`EVIDENCE_INSUFFICIENT` 和 `INVALID` 状态；
- 不读取冻结日后的 outcome，不运行市场数据、DQ 或回测。

### S1：启动门禁

- 验证 frozen producer 能在当前 session 只使用当时可见输入生成信号；
- 运行 canonical DQ 并显式保存 PASS receipt；
- 预先登记首次 capture 的 decision date、输入 identity、动作上限和输出位置；
- 如 producer 只能重放旧包而不能生成当前信号，任务保持 blocked，不使用历史复制或规则替代。

### S2：append-only 观察

- 每个获准 session 最多写一条 observation；相同 decision date 重复运行必须幂等；
- outcome 只在相应 1/5/20-session window 完整成熟后追加；
- 每次 maturity update 保留原 observation identity，不修改当时 signal；
- scoreboard 在预登记的 sample/episode gate 完成前只能输出 `EVIDENCE_INSUFFICIENT`。

### S3：预登记复核

- 复核 exposure-matched net excess、block uncertainty、LONG episode 分散度、placebo/timing 与成本；
- sample floor、独立 episode floor 和裁决边界必须在首次正式 scoreboard 前另行形成 owner-reviewed
  pilot policy，不在见到 prospective outcome 后补阈值；
- 正面结果只支持继续研究，不能自动授权 Options、paper/live 或 production。

## 4. 验收标准

- strict contract 拒绝回填、未成熟 outcome、identity drift、同日不同 signal、DQ 非 PASS 与越权结论；
- append-only/idempotency/maturity 边界有并行 focused tests；
- requested/evaluated dates、成熟 horizon 与 LONG episode inventory 可审计；
- 首次真实 observation 前完成独立 manifest replay 和 exact bounded authorization；
- `production_effect=none`、`broker_action=none`，data download、cache mutation、provider、
  QuantConnect、option backtest、orders、fills、positions 全部为 0；
- 更新 canonical task、`docs/system_flow.md` 与适用生成权威；正式验证、local-main、普通 push 和
  cleanup 门禁通过。

## 5. 当前阻塞与下一责任人

- S0 已完成；producer readiness audit 明确输出 `PRODUCER_NOT_READY`。既有 frozen operational
  producer 固定在 `2021-02-22..2025-12-02`、1202 sessions，不能以同一合同生成冻结日后的当前
  session signal；reason codes 为
  `FROZEN_OPERATIONAL_PRODUCER_WINDOW_ENDS_AT_HISTORICAL_CUTOFF` 与
  `PROSPECTIVE_START_NOT_FROZEN`；
- 不允许把旧 prediction 复制成新 observation，也不允许临时改变信号、窗口或标签规则；
- durable next step 是新增 versioned、single-session、只使用 mature labels 的 prospective producer，
  并在首次 capture 前冻结 prospective start、运行 canonical DQ、重放 exact manifest；
- 首次正式 scoreboard 前仍需 owner-reviewed sample/episode gate；
- next owner：Codex 在新的受治理实现波次中完成上述 producer；Project Owner 只在新增阈值、真实
  capture 授权或扩大运行范围时复核。

## 6. 工作区生命周期

- governed mode：`DUAL_LANE` 的 strategy-evidence lane；
- frozen base：任务登记后由 preflight 固定；
- planned branch：`codex/trading-2560-composer-v2-prospective-oos`；
- planned worktree：`D:\Work\AITradingSystem_trading2560_forward_oos`；
- purpose：只实现 prospective contract/observer 和 lane-focused tests；
- exit condition：lane commit 已进入经正式验证的 coordinator candidate，canonical evidence 完整，
  无进程依赖且 tracked/untracked/ignored audit 无唯一内容后清理；
- 当前不创建 observation runtime、scheduler 或 automation。

## 7. 进度记录

- 2026-09-04：Owner 批准双线继续。本任务先进入 result-blind S0；授权仅覆盖 tracked contract、
  implementation、tests、docs 与本地离线验证，不覆盖真实 prospective capture、数据下载、cache
  mutation、外部 provider 或任何交易行为。
- 2026-09-04：完成 versioned preregistration、strict loader、append-only observation/maturity/
  idempotency 合同及 10 个 focused tests；Ruff、Black 与 strict mypy 均 PASS。lane commits 为
  `1983da6fd`（S0）与 `9c78e2512`（producer readiness audit），coordinator 对应 commits 为
  `8029f9c77` 与 `984a046a1`。
- 2026-09-04：readiness audit 在不读取市场数据、不运行 DQ、不写 observation 的条件下确认旧
  producer 无法合法产出当前 session signal，因此本任务以 `BASELINE_DONE` 保留 S1 阻塞；真实
  prospective observation count=`0`，所有禁止动作计数仍为 0。
