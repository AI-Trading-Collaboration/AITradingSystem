# TRADING-2561：其他策略正交候选 retained-evidence 筛选 V1

最后更新：2026-09-04

稳定任务 ID：`TRADING-2561_ORTHOGONAL_STRATEGY_RETAINED_EVIDENCE_SCREEN_V1`

Owner 指令：2026-09-04，在确认当前候选进入冻结观察、同时继续其他策略研究后要求
“好的，那就这样继续推进两条线吧”。

状态：`IN_PROGRESS`

## 1. 目标

在不新增市场数据、不运行新回测、不读取 prospective holdout outcome 的前提下，对当前已治理的
strategy registries 与 retained decision evidence 做一次确定性筛选，选出与
`first_layer_composer_v2` 机制不同、工程上可执行、PIT/DQ 边界清楚且值得进入下一次独立预注册实验的
研究线。

本任务不是根据历史收益再次排名，也不把“输入不同”冒充经验独立性。它只回答：哪些既有候选值得
继续观察，哪些候选与当前 composer 共享 trend/beta/path-dependency 风险，哪些路线因数据、PIT、
leverage、Options 或生产边界继续关闭。

## 2. 冻结输入

- `config/research/simple_baseline_strategy_registry.yaml`；
- `config/research/layer2_strategy_component_pool_v1.yaml`；
- `config/research/qqq_plus_growth_candidate_registry.yaml`；
- `config/research/evidence_first_research_portfolio_v1.yaml`；
- TRADING-2557～2559 canonical terminal evidence；
- 只读取 tracked configuration 与 aggregate decision facts，不读取逐日 prospective outcome。

所有输入必须绑定 exact commit/SHA-256；缺失、schema drift 或 task verdict 不一致时输出 `INVALID`。

## 3. 筛选轴

每个候选至少披露：

1. `mechanism_family`：static allocation、volatility allocation、trend timing、drawdown gate、
   leveraged beta、options 或其他；
2. `input_overlap`：与 composer 的 QQQ price/trend/state 输入重叠；
3. `action_overlap`：二元 QQQ/cash timing、连续 QQQ/SGOV allocation 或 leverage overlay；
4. `evidence_state`：retained owner decision、forward-aging readiness、PIT/DQ readiness；
5. `structural_orthogonality`：只允许 `DISTINCT`、`PARTIAL`、`OVERLAPPING`；
6. `empirical_independence_claim`：本任务固定为 `NOT_ESTABLISHED`；
7. `route`：`CONTINUE_EXISTING_FORWARD_AGING`、`PREREGISTER_NEW_EXPERIMENT`、`HOLD_REFERENCE`、
   `EXCLUDE`。

筛选优先保护简单、透明、非 Options、非高 leverage 且已有 owner-reviewed forward-aging 合同的候选。
不得见结果新增权重、阈值或候选 family。

## 4. 初始研究假设与解释边界

- `equal_risk_qqq_sgov` 是当前最有条件继续的简单防御候选：连续 volatility-based allocation 与
  composer 的五态二元 timing 在 action/mechanism 上不同，但共享 QQQ price inputs，因此最多标记
  `PARTIAL`，不能声称经验独立；
- static QQQ/SGOV 只作为 comparator/reference，不伪装成新 alpha；
- trend-gated TQQQ 与 composer 共享 trend/timing 暴露且增加 leverage path dependency，默认保持
  `HOLD_REFERENCE`；
- Options、tail-risk quarantine 与生产路线保持 `EXCLUDE`；
- 若 retained evidence 中没有 `DISTINCT` 且 PIT-ready 的候选，输出“继续 equal-risk 观察，同时另立
  新机制数据合同”，而不是勉强选一个候选。

## 5. 阶段与验收

### S0：retained-evidence screen

- 建立 versioned policy、strict loader、deterministic classifier 与 golden/negative tests；
- 输出 aggregate-only routing result，不运行回测、DQ、provider 或 cache mutation；
- 输入顺序变化不得改变结果；未知 strategy、缺安全字段或未治理阈值 fail closed。

### S1：下一实验合同

- 只有 route 为 `PREREGISTER_NEW_EXPERIMENT` 或既有 `CONTINUE_EXISTING_FORWARD_AGING` 的候选可进入；
- 新实验必须有自己的 freeze date，冻结前数据只能算 development evidence；
- 与当前 composer 的 prospective ledger 完全分离；
- 正面 retained screen 不能授权 empirical run、paper/live 或 production。

### 验收标准

- 每个 route 都有可解释 reason codes 与 source bindings；
- 明确区分 structural orthogonality 和 empirical independence；
- 默认输出不包含交易建议、weights 变更或 promotion；
- `production_effect=none`、`broker_action=none`，data download、cache mutation、provider、
  QuantConnect、option backtest、orders、fills、positions 全部为 0；
- canonical task、system flow、正式验证、local-main、普通 push 和 cleanup 门禁通过。

## 6. 工作区生命周期

- governed mode：`DUAL_LANE` 的 engineering lane；
- planned branch：`codex/trading-2561-orthogonal-strategy-screen`；
- planned worktree：`D:\Work\AITradingSystem_trading2561_orthogonal_screen`；
- purpose：只实现 retained-evidence policy/loader/classifier 与 lane-focused tests；
- exit condition：lane commit 已进入经正式验证的 coordinator candidate，canonical evidence 完整，
  无进程依赖且 tracked/untracked/ignored audit 无唯一内容后清理。

## 7. 进度记录

- 2026-09-04：Owner 批准双线继续。本任务先执行不消费市场数据的 retained-evidence screen；
  authorization_state=`STANDING_OWNER_SCOPE` 仅用于本地 R0/static evidence work，不授权新的经验回测。
