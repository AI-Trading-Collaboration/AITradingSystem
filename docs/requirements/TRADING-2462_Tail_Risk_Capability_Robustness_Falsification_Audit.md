# TRADING-2462：Tail-Risk Capability Robustness / Falsification Audit

最后更新：2026-07-27

状态：`EVIDENCE_COMPLETE_FORMAL_VALIDATION_PENDING`

稳定任务 ID：`TRADING-2462_TAIL_RISK_CAPABILITY_ROBUSTNESS_FALSIFICATION_AUDIT`

Owner 决策：
`owner_decision:TRADING-2462:2026-07-27:approve_tail_risk_capability_robustness_falsification_audit_v1`

## 1. 目标

本任务承接 TRADING-2461 的 `TAIL_RISK_ONLY_SKILL`，主动证伪当前系统对
`QQQ_FUTURE_WORST_1D_RETURN` 的判断能力。它必须回答该能力是否：

1. 在原 7 个 purged walk-forward folds、不同已知时 regime 和事件基准率下稳定；
2. 不依赖单一 fold、单一 feature/source role、同日 feature timing 或原 20-session embargo；
3. 对 train-only tail decile/quintile event 具有可校准的风险排序价值；
4. 显著优于保持时序相关性的 deterministic placebo；
5. 足够稳健到允许 Owner 另立 Decision Value Audit，或应被判定 fragile/falsified 并关闭。

本任务不是 risk overlay、candidate family、策略回测、权重生成或收益优化。

## 2. 权威输入与隔离

唯一能力输入是已归档 TRADING-2461 的真实 Batch 2 package：

- selected research window=`2021-02-22..2026-07-24`；
- actual evaluated range=`2021-08-11..2026-06-25`；
- outer folds=`7`；
- prediction rows=`118300`；
- evaluation commitment=
  `9c538f23d3e6d463feb5faab484132ca4635c043c09cafee960d1aac9095dc02`；
- primary model=`M1_RIDGE_LINEAR`；
- primary feature prefix=`CROSS_ASSET_STATE`；
- supported target=`QQQ_FUTURE_WORST_1D_RETURN`，supported horizons=`1d/5d/10d`；
- full canonical DQ=`FAIL`，QQQ/SPY/SGOV scoped DQ=`PASS`，
  `global_cache_pass_claimed=false`。

实现必须把 Batch 2 result、summary、envelope、run ledger、policy、source snapshot 与四份
Batch 1 source artifacts逐文件绑定 path/size/SHA-256。任何 source、schema、commitment、
fold、prediction、DQ 或 safety drift 必须在审计计算前 fail closed。

历史 `TRADING-816~825` controlled tail-risk fallback artifacts不属于本任务输入。它们来自已退役
candidate/fallback 语义、不同 denominator 和旧结论链，不得用于支持、校准或放宽本审计。

## 3. 预注册证伪轴

详细 numeric policy 必须在读取 Batch 2 detailed metric/prediction rows前写入 reviewed
`decision_target_tail_risk_robustness_audit_policy.v1`。V1 至少冻结：

1. **Exact reconstruction**：重算 primary target 三个原通过 horizons及原 capability gate；
2. **Timing stress**：所有 feature 额外 lag 1 session；
3. **Purge/embargo stress**：embargo 从 20 增至 40 sessions；
4. **Feature/source ablation**：family-prefix、SPY-derived、SGOV-derived与cross-asset roles；
5. **Fold influence**：逐 fold jackknife、worst fold与单 fold贡献集中度；
6. **Regime concentration**：只用 decision-time feature形成 train-only trend/volatility/drawdown
   strata，不读取未来 regime label；
7. **Event prevalence/calibration**：每 fold 只用 train labels确定 tail decile与quintile，
   对 test rows评估top-risk lift、bottom/top separation与fold consistency；
8. **Placebo**：保持20-session重叠结构的deterministic block permutation null，不用 iid row
   shuffle；
9. **Multiple-axis decision**：不按结果删除 stress，不挑选最好 horizon/variant。

所有阈值必须是 temporary pilot falsification baseline，记录 owner、rationale、planned
evidence与 review condition。结果只允许：

- `TAIL_RISK_CAPABILITY_ROBUST`
- `TAIL_RISK_CAPABILITY_FRAGILE`
- `TAIL_RISK_CAPABILITY_FALSIFIED`
- `INSUFFICIENT_ROBUSTNESS_EVIDENCE`

## 4. 实施阶段

|阶段|内容|依赖|退出条件|
|---|---|---|---|
|S0|任务登记、checkout/lease审计、输入盘点|TRADING-2461 DONE|SINGLE_LANE preflight PASS|
|S1|reviewed v1 policy与experiment spec冻结|只读schema/code，不读取detailed result rows|policy hash与所有stress/gate固定|
|S2|deterministic builder、validator与focused tests|S1|fixture覆盖lag/embargo/ablation/regime/calibration/placebo/tamper|
|S3|真实historical-seen audit双构建|S2、exact Batch 2 bytes|byte-identical、content-derived validator 0 errors|
|S4|中文报告、registry/catalog/system flow与formal gates|S3|focused/static/report/reproducibility/architecture/contract/Full PASS|
|S5|结论归档|S4|只决定是否允许另立Decision Value Audit|

## 5. 验收标准

- policy在读取详细结果前冻结，且没有结果驱动新增/删除variant、horizon、metric或gate；
- exact input commitment、fold/test row identity与Batch 2 reconstruction一致；
- lag/embargo/ablation/jackknife/regime/calibration/placebo全部输出，不得静默跳过失败轴；
- regime与event threshold只从各fold train partition拟合；
- placebo保留overlapping-horizon时序块，随机种子、block size、replicate count固定；
- aggregate decision由机械规则生成，任何mandatory axis失败不得输出`ROBUST`；
- builder双构建byte-identical，validator从source重建并拒绝result/summary/report/safety tamper；
- 报告披露 selected/requested/evaluated ranges、full/scoped DQ、事件数与不足证据；
- required focused、Ruff、Black、strict mypy、report/reproducibility、
  architecture/contract/Full与post-Full治理门禁通过；
- 不创建risk overlay/candidate/backtest/weights/QLD signal/paper-shadow/promotion/production/broker。

## 6. 安全边界

- `research_only=true`
- `historical_seen_only=true`
- `prospective_accessed=false`
- `candidate_family_created=false`
- `risk_overlay_created=false`
- `strategy_backtest_executed=false`
- `transaction_cost_model_applied=false`
- `target_weights_generated=false`
- `qld_used_as_signal=false`
- `paper_shadow_changed=false`
- `promotion_allowed=false`
- `production_effect=none`
- `broker_action=none`

## 7. 工作区与并发边界

- original exact base=`e4e262bba3fa35083cb88aaaaae88f6067dc74d2`；
- current exact base=`bc8496b11039f3d6a8d2bc837e821c298e04c9cf`；
- mode=`SINGLE_LANE`；
- current branch=`codex/trading-2462-tail-risk-robustness-audit-v5`；
- current workspace=`D:\Work\AITradingSystem_t2462_tailrisk_v5`；
- purpose：隔离共享根目录并从exact local main实现本审计；
- known-unrelated exclusion=`docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、
  hash、复制、stage、修改或删除；
- coordinator owns task register、system flow、artifact catalog、report registry、
  architecture/generated state、formal evidence与最终集成；
- 若local/remote main或TRADING-2461公共输入合同在提交前变化，按新exact main重新评估，
  不自动merge/rebase；
- exit condition：final candidate通过required gates、ff-only进入local main并ordinary push后，
  确认canonical outputs已迁移且无unique content/process dependency，再审计并清理本worktree；
- DEVX-005 target-bound audit已进入reviewed main，但这只解除工具前置阻塞，不自动授权删除；
  v1-v4只读保留，待v5 final candidate集成且canonical evidence迁移后，再逐target完成
  tracked/untracked/ignored/process/evidence/恢复边界审计与清理；
- v1 base=`e4e262b`，v2 base=`0e4d5a8`，v3 base=`f8b32b2`。三次正式tier前main分别前进，
  且每次都触及compatibility authority、test manifest、task registry或共享测试，因此旧
  workspace转为只读迁移源，没有把旧基线PASS嫁接至新基线。

## 8. 进展记录

- 2026-07-27：Owner同意按“先稳健性证伪、再决策价值、最后才考虑overlay”的顺序继续策略线；
  任务建立并进入protocol freeze。
- 2026-07-27：在未读取Batch 2 detailed prediction/metric rows的前提下冻结v1 policy：
  mandatory variants=`EXACT_PRIMARY/FEATURE_LAG_1/EMBARGO_40/DROP_SPY_DERIVED/
  DROP_SGOV_DERIVED`；diagnostic family-only variants=3；同时冻结7-fold jackknife、三类
  train-only regime terciles、train-only tail decile/quintile calibration、199次20-session
  block permutation placebo及ROBUST/FRAGILE/FALSIFIED/INSUFFICIENT机械映射。后续不得按结果
  修改本policy。
- 2026-07-27：deterministic builder与content-derived validator完成。Exact source
  reconstruction逐行一致，evaluation commitment与TRADING-2461相同；统一experiment runner
  已生成primary、summary、中文Markdown、envelope与run ledger；`investment_facing=false`、
  `manual_review_required=true`、`production_effect=none`、`broker_action=none`。
- 2026-07-27：真实审计机械结论为`INSUFFICIENT_ROBUSTNESS_EVIDENCE`。Exact、fold influence、
  placebo gates通过；mandatory variants 4/5 target-supported，其中`DROP_SPY_DERIVED`只有5d
  通过，1d/10d未保留capability。Regime audit有6个pooled strata低于80-row预注册地板：
  DRAWDOWN/LOW在1d/5d/10d分别为65/64/63 rows，VOLATILITY/HIGH三个horizon均为69 rows。
  Event calibration各horizon的tail decile/quintile只有1或2个eligible folds，低于每格6-fold
  预注册地板。不得因实际Spearman为正、placebo通过或4/5 mandatory通过而把不可评估轴改判为
  ROBUST/FRAGILE。
- 2026-07-27：当前机械route为`CLOSE_TAIL_RISK_PATH_OR_REDESIGN_DECISION_TARGET`。
  Decision Value Audit未获授权；risk overlay、candidate family、strategy backtest、weights、
  QLD signal、paper-shadow、promotion、production和broker均未创建或改变。
- 2026-07-27：v1、v2、v3均完成各自exact-base迁移与内容重建，但在正式tiers前main连续前进至
  `0e4d5a8`、`f8b32b2`、`0cb8bf9`；新增提交都触及消费者可见治理边界，因此每次均fail
  closed。v3最终验证了1,780,838-byte compatibility历史前缀、26项live source hash、
  refactor-policy `78 passed`，并完成canonical重发与10个核心字段零差异的独立重建；
  这些只作为v4迁移对照，不充当v4最终formal证据。
- 2026-07-27：v4以`0cb8bf9be3e1f91044e6cb950f6b401b5e230fb3`为exact base完成迁移；
  1,785,830-byte compatibility历史前缀逐字节一致，27项current authority source hash
  一致；canonical runner与独立双重重建再次得到同一结论和commitment。Focused=`111`、
  Architecture=`688`、Contract=`275`、Report=`57`、Reproducibility=`23`、
  Integration=`995`全部PASS。唯一Full启动前main前进至
  `bc8496b11039f3d6a8d2bc837e821c298e04c9cf`；该提交正式实现DEVX-005 target-bound audit，
  并修改checkout contract、compatibility authority、task registry、system flow、generated
  manifests与共享测试，属于消费者可见串行合同波。因此v4不运行Full，转为只读迁移源；v5从
  新exact main重放本任务变更并完成formal/Full/归档。v1-v4只在v5证明无unique
  content/evidence/process dependency后清理。
