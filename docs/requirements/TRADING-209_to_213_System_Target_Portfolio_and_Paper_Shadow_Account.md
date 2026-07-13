# TRADING-209 to TRADING-213 System Target Portfolio and Paper Shadow Account

最后更新：2026-07-13

## 背景

TRADING-204 到 TRADING-208 已经完成 owner-maintained manual portfolio snapshot 的 dry-run review、owner decision recording、paper/no-action tracking 和 weekly real snapshot review。该链路面向 owner 提供的脱敏真实快照，但仍固定 `broker_action_allowed=false`、`broker_action_taken=false`、`order_ticket_generated=false` 和 `production_effect=none`。

本阶段新增一条完全分离的 research-only 链路：系统不依赖 owner 当前真实持仓，而是基于 shadow shortlist、candidate consensus、position advisory rules 和 guardrails 生成自己的 `research_model_target_weights`，再用独立 `paper_shadow_account` 长期模拟验证。

## 范围

本阶段包含五个任务：

- TRADING-209 Research Model Target Weight Generator
- TRADING-210 Paper Shadow Account Initialization
- TRADING-211 Model Target Rebalance Simulator
- TRADING-212 Paper Shadow Performance vs Baselines
- TRADING-213 System Target Portfolio Review Pack

本阶段不做：

- broker API、broker import 或真实账户连接
- 自动下单、真实交易、order ticket 生成
- official target weights 写入
- 自动 production candidate、自动 owner approval 或真实持仓 mutation

所有输出必须显式声明：

- `research_target_only=true`
- `paper_shadow_only=true`
- `not_official_target_weights=true`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `order_ticket_generated=false`
- `production_effect=none`

## 设计决策

`system_target_portfolio` 不是 owner real portfolio。它只代表系统当前研究规则下的 target methods，用于模拟、比较和人工复核，不得作为真实账户目标仓位。

`research_model_target_weights` 不是 official target weights。即使某个 method 在 paper shadow 中表现最好，也只能进入 owner research review 或继续 forward confirmation；不能自动写入 baseline、production config 或 broker 路径。

`consensus_target` 只能作为聚合候选和 upper-bound reference。它可能提高风险资产暴露或集中度，不能因为总收益较高就自动成为默认执行方法。

`defensive_limited_adjustment` 继续 research-only。当前 defensive pressure evidence 仍以 simulation/research 样本为主，不能自动批准防守规则或改写 advisory policy。

## 实施步骤

1. 新增 `config/etf_portfolio/dynamic_v3_rescue/model_target_portfolio_v1.yaml`，治理 target methods、static baseline、constraints、source configs 和 safety fields。
2. 新增 `config/etf_portfolio/dynamic_v3_rescue/paper_shadow_account_v1.yaml`，治理 base currency、initial equity、AI regime start date、tracked methods、pricing/cost settings 和 safety fields。
3. 由 canonical `dynamic_v3_system_target_portfolio` 模块生成 model target artifacts、paper shadow artifacts、rebalance artifacts、performance artifacts 和 system target review pack；旧 `dynamic_v3_system_target` 只保留兼容入口及后续任务的尚未迁移实现。
4. 由 canonical `interfaces/cli/etf_portfolio/dynamic_v3_system_target_portfolio.py` 接入 `aits etf dynamic-v3-rescue` 的17个 generate/init/simulate/run/pack/report/validate callback，legacy CLI root 不保留这些定义。
5. 更新 report registry、artifact catalog、system flow、operations runbook、README 和 Reader Brief。
6. 新增 focused tests 覆盖 config validation、safety labels、target method generation、constraint checks、paper shadow initialization、rebalance simulation、performance comparison、review decision 和 Reader Brief integration。

## 验收标准

- `model-target config-validate` PASS。
- `model-target generate` 至少生成 `static_baseline`、`consensus_target`、`limited_adjustment`、`defensive_limited_adjustment`。
- 所有 model target rows 标记 `research_target_only=true` 和 `not_official_target_weights=true`。
- `paper-shadow init` 为每个 target method 生成独立 paper state，且 `broker_action_taken=false`。
- `model-rebalance simulate` 不覆写 source paper state，只在 rebalance artifact 追加 `paper_shadow_state_after.json`；缺失/失效 target、时间回退、重复 Paper+Target 或 hard constraints 失败均在正式写件前 fail closed。
- `paper-shadow-performance run` 生成 method summary、pairwise comparison、regime breakdown 和 Reader Brief section。
- `system-target-review pack` 生成 recommended research method、owner checklist 和 Reader Brief section。
- 所有 validate CLI PASS。
- `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue` PASS。
- Focused tests、ruff、compileall 和 `git diff --check` PASS；尽量完成 full pytest。

## 当前状态

2026-07-13：ARCH-004 G2.4CH hardened migration已`COMPLETE`，G2.4继续。17 callback与五环节实现分别归属canonical CLI/domain模块，legacy CLI定义清零，legacy domain删除19个旧public实现并仅保留lazy compatibility wrappers。225个focused、11个core positive/negative、269个architecture-fitness和203个contract-validation测试通过；generated manifests为901 modules / 1,114 tests / 858 direct writers / 0 violations。当前source-backed fixture结果仍仅用于reproducibility：10 methods、turnover=`0.4961400001`、3个共同return observations、DQ=`PASS_WITH_WARNINGS`，Review选择`limited_adjustment/CONTINUE_OBSERVATION`且不声称performance winner。所有路径保持research/paper-shadow/manual-only、`production_effect=none`；不触发ARCH-005 handoff。

2026-07-13：ARCH-004 G2.4CH进入`IN_PROGRESS_HARDENING`。17 callback迁canonical interface；五环节新增timezone/cutoff、pre-output source validation、semantic source selection、bounded source/config/cache/DQ snapshots和content-derived byte validators。Model Target禁止缺candidate/consensus时用baseline补造；Paper Init要求validated Target及source-exact policy；Rebalance要求forward chronology、exact methods/constraints、duplicate target gate与atomic pre/post state；Performance按共同可用finite交易日及versioned costs计算，missing/null不填0并冻结DQ/cache；Review要求Target→Paper→Performance exact lineage，`REVIEWED_OBSERVATION_PRIORITY`明确不是performance winner、official target或自动批准。固定research/paper-shadow/manual-only、`production_effect=none`，不触发phase handoff。

五类权威input snapshot分别为`model_target_input_snapshot.v2`、`paper_shadow_account_input_snapshot.v2`、`model_rebalance_input_snapshot.v2`、`paper_shadow_performance_input_snapshot.v2`和`system_target_review_input_snapshot.v2`。Snapshot冻结source/config/cache/DQ的path、size、SHA-256、validation及计算views；validator重验live source与semantic selection，并逐字节重建全部输出。当前source-backed fixture为10个method、Rebalance turnover=`0.4961400001`、Performance共同return observations=`3`且DQ=`PASS_WITH_WARNINGS`；best return/risk-adjusted=`consensus_target`，但Review仍按reviewed observation priority选择`limited_adjustment`、`decision_status=CONTINUE_OBSERVATION`、`performance_winner_claimed=false`。这只证明链路可复算，不构成收益或promotion结论。

2026-06-12：P0 baseline 已实现并进入 `VALIDATING`。

已新增：

- `config/etf_portfolio/dynamic_v3_rescue/model_target_portfolio_v1.yaml`
- `config/etf_portfolio/dynamic_v3_rescue/paper_shadow_account_v1.yaml`
- `src/ai_trading_system/etf_portfolio/dynamic_v3_system_target.py`
- `aits etf dynamic-v3-rescue model-target config-validate/generate/report`
- `aits etf dynamic-v3-rescue paper-shadow init/state/report`
- `aits etf dynamic-v3-rescue model-rebalance simulate/report`
- `aits etf dynamic-v3-rescue paper-shadow-performance run/report`
- `aits etf dynamic-v3-rescue system-target-review pack/report`
- 对应 `validate-*` CLI、report registry entries、latest pointer repair specs、Reader Brief `Dynamic Rescue System Target Portfolio` 摘要和 focused tests。

验收链路使用当前本地缓存可审计窗口：

- `target_id`: `model-target_8bb78129f2a68d0b`
- `paper_shadow_id`: `paper-shadow_9b6984c19ecdb4ce`
- `rebalance_id`: `model-rebalance_1db70778e22ce9ac`
- `performance_id`: `paper-shadow-performance_14e18d3e0b3a1a38`
- `system_target_review_id`: `system-target-review_84b81667ea1e932d`
- performance start: `2026-06-09`
- evaluation as-of: `2026-06-10`
- data quality status: `PASS_WITH_WARNINGS`
- focused pytest: `5 passed`
- full pytest: `2367 passed, 640 warnings`
- `ruff check src tests`: PASS
- `compileall -q src tests`: PASS
- `git diff --check`: PASS，伴随 `docs/task_register.md` CRLF/LF normalization warning

附件示例中的 `2026-06-21` 相对当前工作区日期和本地价格缓存是未来日期；`paper-shadow-performance run` 不能用未来 as-of 绕过 `aits validate-data` 等价门禁。本轮使用 `2026-06-10` 作为缓存可验证 evaluation date，并在 artifact 中显式写出 `performance_start_date` 和 `evaluation_as_of`。

本轮不纳入的 P1/P2 后续仍包括图表化、更细风险解释、多 shadow account、交易成本/slippage sensitivity 和真实账户映射。
