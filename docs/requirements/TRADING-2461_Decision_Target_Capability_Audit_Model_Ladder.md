# TRADING-2461：Decision Target Capability Audit Batch 2 Model Ladder

最后更新：2026-07-26

状态：`COMPLETE_BATCH2_CAPABILITY_AUDIT`

稳定任务 ID：`TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER`

Owner 决策：
`owner_decision:TRADING-2461:2026-07-26:approve_decision_target_capability_audit_batch2_v1`

## 1. 目标

本批承接 TRADING-2460 已完成的真实 label foundation，回答当前系统的受治理输入对哪些
decision target 具有可重复、跨 fold 稳定、相对简单基线有增量的判断能力，再由该能力约束
后续 Strategy Style Discovery。它不是新 candidate family、收益最大化回测或自动策略选择。

本批必须先冻结 split、model ladder、feature-family order、metrics、classification 和安全边界，
再读取结果。不能根据首轮结果增加模型、删除 horizon、改变指标或重新划分 fold。

## 2. 冻结输入与目标

唯一 label authority 是 TRADING-2460 v2 的
`decision_target_capability_audit_label_foundation.v1` 真实 package：

- active primary research start=`2021-02-22`；
- source end 与实际 evaluated range 由 package exact 记录；
- common sessions=`1362`、label rows=`5412`；
- horizons=`1d/5d/10d/20d`；
- primary target=`QQQ_MINUS_SGOV`；
- diagnostic targets=`SPY_MINUS_SGOV`、`QQQ_MINUS_SPY`；
- tail labels=QQQ/SPY/SGOV future max drawdown 与 worst 1d return；
- capability receipt=
  `dq_capability_e7f233ca6e0c41ce9506df46f067e56348004a19f953cf74626d5c9936ccb059`；
- full canonical DQ=`FAIL`，唯一 global error 影响 `^VIX`；QQQ/SPY/SGOV exact scope=`PASS`，
  `global_pass_claim=false`。

实现前必须把实际 package、source panel、receipt、policy、requirement 和 implementation
绑定为 size/SHA-256 commitment。任何 path、hash、schema、row count、target、horizon、
DQ status、receipt 或 safety drift 必须在 split/model/output 之前 fail closed。

`QQQ_MINUS_SGOV` 只表示 gross opportunity difference，不得称为 alpha。
`QQQ_MINUS_SGOV == SPY_MINUS_SGOV + QQQ_MINUS_SPY` 仍为逐行必须满足的分解合同。
QLD 不进入 label、feature、model、style classification 或 target；SPY 保持 reference /
regime-control，不自动成为 action-weight asset。

## 3. Purged walk-forward 合同

Batch 2 使用 session-indexed expanding purged walk-forward：

1. outer test fold 必须按连续共同交易 session 形成，fold schedule 在结果读取前进入 reviewed
   policy；
2. 任一 train row 的 `label_available_on_session` 必须不晚于该 fold train cutoff；
3. 任一 train label interval 与 test interval 相交时必须 purge；
4. feature lookback 必须全部位于 decision session 当日或之前；
5. embargo 规则与 session count 必须进入 reviewed policy，并由最大 label horizon、feature
   lookback 和相邻 fold 泄漏边界给出 rationale；不得把结果优化成 embargo；
6. test labels 只用于 outer evaluation，模型选择或标准化只能使用当时 train partition；
7. fold coverage 不满足 policy floor 时必须输出 `INSUFFICIENT_FOLD_COVERAGE`，不得缩短 horizon
   或合并 test 结果来伪造可评估性。

本批只使用 `HISTORICAL_SEEN` 数据，不访问 TRADING-2452/2458 的 prospective untouched
区间，也不声称 unbiased prospective/OOS。

## 4. Feature family 与 model ladder

Feature 必须来自 TRADING-2460 exact QQQ/SPY/SGOV adjusted-close panel，逐 decision session
只使用 known-at trailing values。V1 family 顺序固定为：

1. `PRICE_TREND`：QQQ/SPY/SGOV trailing total return 与 QQQ-SPY / QQQ-SGOV relative return；
2. `VOLATILITY_DRAWDOWN`：trailing realized volatility、current drawdown；
3. `CROSS_ASSET_STATE`：QQQ/SPY/SGOV 相对强弱与防御资产相对状态。

不得使用 `^VIX`、rate、breadth、fundamental、valuation、event/news、TRADING-2316 regime
label、旧 candidate output 或 strategy score。新增 family 必须新 policy version 与 Owner review。

每个 target/horizon/family-prefix 使用同一冻结 ladder：

- `M0_TRAIN_MEAN`：仅用当前 train label mean 的无 feature 基线；
- `M1_RIDGE_LINEAR`：train-only standardization 的 reviewed ridge linear model；
- `M2_RIDGE_INTERACTION`：只增加 policy 明列的有限二阶 interaction，不做自动 feature search。

分类 target 的概率模型、tree/boosting、神经网络、自动超参搜索、ensemble 与结果驱动 model
addition 不在本批范围。所有 numeric lookback、ridge penalty、interaction、fold、sample floor、
classification threshold 和稳定性规则必须位于 reviewed policy，不得散落为未解释 literal。

## 5. Metrics 与能力分类

每个 outer fold 必须输出：

- continuous：MAE、RMSE、Pearson correlation、Spearman rank correlation；
- directional：sign accuracy 与相对 train-only base-rate 的增量；
- economic diagnostic：prediction quintile 的 realized target spread，仅作能力解释，不计交易成本、
  不形成策略 PnL；
- tail diagnostic：预测分位与 future max drawdown / worst 1d 的关联，保持与 return score 分离；
- coverage：eligible、matured、purged、embargoed、train/test row count 和实际日期。

汇总不能只看 pooled average，必须披露 fold direction consistency、worst fold、跨 fold dispersion、
相对 M0 的增量和 family-prefix marginal contribution。能力结论只能取：

- `NO_MEASURABLE_SKILL`
- `BROAD_EQUITY_RISK_PREMIUM_SKILL`
- `NASDAQ_LEADERSHIP_SKILL`
- `COMBINED_QQQ_DEFENSIVE_ALLOCATION_SKILL`
- `TAIL_RISK_ONLY_SKILL`
- `MIXED_OR_UNSTABLE_SKILL`
- `INSUFFICIENT_FOLD_COVERAGE`

classification gate 必须由 reviewed policy 给出 owner、版本、rationale、planned evidence 与 review
condition。本批的阈值是 pilot baseline，不得据此 promotion；若 evidence 不足，结论必须保持
`MIXED_OR_UNSTABLE_SKILL` 或 `INSUFFICIENT_FOLD_COVERAGE`。

## 6. 产物与独立验证

正式输出至少包括：

- immutable input snapshot/commitment；
- split ledger；
- fold-level predictions 与 metrics；
- target/horizon/model/family capability matrix；
- style classification JSON；
- artifact envelope / run ledger；
-中文 report；
- content-derived independent validation result。

Validator 必须从冻结 input 与 policy 重建 features、splits、models、predictions、metrics、
classification、summary 和 Markdown，并拒绝 source、availability、purge、standardization、
model、metric、classification、safety 或 artifact tamper。置换 source row order必须 fail closed；
相同输入双构建必须 byte-identical。

## 7. 分阶段计划

|阶段|内容|退出条件|
|---|---|---|
|S0|任务、需求、Owner 决策与输入盘点|范围、禁止项、workspace 与依赖完整登记|
|S1|policy、split feasibility 与 source commitment|fold/embargo/model/metric/classification 在结果读取前冻结|
|S2|feature、purged walk-forward 与 model ladder|fixture 上 train-only、no-lookahead、deterministic tests PASS|
|S3|真实 historical-seen build 与 validator|exact TRADING-2460 package生成完整矩阵，content-derived validation 0 errors|
|S4|报告、registry/catalog/system flow 与 formal validation|focused、report/reproducibility、architecture/contract/required Full PASS|
|S5|Owner style decision|只决定下一批是否预注册新 family；不自动进入 candidate/backtest/weights|

## 8. 安全边界

- `research_only=true`
- `historical_seen_only=true`
- `prospective_accessed=false`
- `candidate_family_created=false`
- `candidate_search_executed=false`
- `parameter_search_executed=false`
- `strategy_backtest_executed=false`
- `transaction_cost_model_applied=false`
- `target_weights_generated=false`
- `action_universe_changed=false`
- `qld_used_as_signal=false`
- `paper_shadow_changed=false`
- `promotion_allowed=false`
- `production_effect=none`
- `broker_action=none`

## 9. 临时工作区与并发边界

- owning task：`TRADING-2461_DECISION_TARGET_CAPABILITY_AUDIT_MODEL_LADDER`；
- exact base：启动时固定为
  `281c8236b3b4da103b3ccb665e19d5d51e1bba42`；
- branch：`codex/trading-2461-decision-target-capability-audit-b2`；
- workspace：复用已审计干净的
  `D:\Work\AITradingSystem_trading2458_retirement_rebase3_20260726`，不新增 clone/worktree；
- purpose：与共享根目录在途 DATA-GOV-002 B2 consumer migration 隔离；
- DATA-GOV-002 B2 不属于本任务输入，本任务不得读取其未提交实现或生成状态；
- strategy-owned code/policy/test 与 DATA-GOV B2 模块保持不重叠；task register、system flow、
  catalog/registry、architecture manifests 和 formal evidence 由本任务 coordinator 收口；
- 若 main 的 shared contract、capability receipt schema 或 consumer semantics 先行变化，按
  serial contract wave 规则从新 exact main 重算，不自动 rebase/merge；
- 本 workspace 含已登记 known-unrelated exclusion
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，本任务不得读取、hash、复制、stage、修改或删除；
- exit condition：候选经 required validation 后进入 local/remote main；任务 branch 在安全审计后
  清理。该复用 workspace 是否删除继续受 known-unrelated exclusion 独立约束，不由本任务强删。
- 2026-07-26 base-drift重建：`origin/main`在Full期间前进到
  `a309fa2f5bf5ef2205041c2ef7416c3e03487aab`，且改动shared report/catalog/system-flow/task/
  compatibility/manifest边界；按serial contract wave规则禁止在旧base自动rebase/merge。新建
  `D:\Work\AITradingSystem_t2461_a309`，
  branch=`codex/trading-2461-decision-target-capability-audit-b2-a309`，purpose=从新exact main
  重放本任务可归属patch并重算所有generated/compatibility evidence；exit condition=新候选通过
  required validation并安全进入main后，审计tracked/untracked/ignored与process dependency，
  再用`git worktree remove`清理。首次使用较长workspace路径的创建尝试被仓库既有deep-lineage
  文件触发Windows filename-too-long并在checkout前退出；审计确认该路径不存在、worktree未登记、
  新branch仅指向`a309fa2f5`且无unique content。仓库启用`core.longpaths=true`后改用上述短路径；
  旧workspace在确认patch等价且无unique evidence前保留。
- 2026-07-26 second base-drift重建：`a309fa2f5`候选已通过required Full，但归档后的
  post-Full architecture gate发现`origin/main`又前进到
  `8a319c2fe26088d414fe034478727d777ca54b84`。新增提交只实施DEVX-003远端收口治理，未改
  Batch 2策略模块或真实输出，但改动task registry、compatibility baseline、test manifest与
  architecture authority，导致Wave14/15按设计报`CARRIER_PUSH_DRIFT`，且旧候选无法从新main
  `ff-only`集成。任务因此从DONE重开为`VALIDATING`；新建
  `D:\Work\AITradingSystem_t2461_8a31`，
  branch=`codex/trading-2461-decision-target-capability-audit-b2-8a31`，purpose=从新exact
  main重放可归属patch、保留9/9同hash真实审计证据并重算共享/generated/final validation；
  exit condition=最终候选通过required validation并安全进入local/remote main后，逐一审计并清理
  两个重建worktree。旧worktree及其输出在等价性与canonical evidence确认前保留。

## 10. 进展记录

- 2026-07-26：任务登记并从exact
  `main=281c8236b3b4da103b3ccb665e19d5d51e1bba42`启动；`SINGLE_LANE` preflight与write
  lease通过。DATA-GOV-002 B2继续留在共享根目录独立分支，本任务未读取其未提交实现或生成状态。
- 2026-07-26：reviewed v1 policy已冻结input authority、共同horizon decision universe、
  120-session feature warmup、378/126-session expanding split、20-session embargo、final partial
  floor、252-row train floor、三个feature-family prefix、M0/M1/M2 ladder及pilot classification
  thresholds。Classification只消费`M1_RIDGE_LINEAR + CROSS_ASSET_STATE`；M2与较窄prefix不得
  事后挑选。
- 2026-07-26：真实Batch 1四项输入逐文件exact匹配policy：
  label=`3b41ca325d7f1110173593b25d35e1fab7e9409e3ace61d9e959f117b7553b80`、
  source package=`eb86292a470a1ab13c54a52b3b9c21ee98eeb9c547460057903d6d18e53d554e`、
  panel=`155093a99e0c258e89268745a4579c6a0e8838fa7aa363466044a5a722137150`、
  receipt=`624623557c2552b9090469f070cb177cadf6bc8b71940f97772a13c629d85afa`。
  Full canonical DQ=`FAIL`、scoped QQQ/SPY/SGOV=`PASS`、global claim=false保持可见。
- 2026-07-26：真实run=`CAPABILITY_AUDIT_READY`，selected window=
  `2021-02-22..2026-07-24`，actual evaluated range=`2021-08-11..2026-06-25`，
  outer folds=`7`、predictions=`118300`、evaluation commitment=
  `9c538f23d3e6d463feb5faab484132ca4635c043c09cafee960d1aac9095dc02`，
  double-build byte-equivalent且content-derived validator=`0 errors`。
- 2026-07-26：机械style=`TAIL_RISK_ONLY_SKILL`。`QQQ_MINUS_SGOV`、
  `SPY_MINUS_SGOV`、`QQQ_MINUS_SPY`均0 horizon通过；QQQ future max drawdown只有5d通过，
  未达到target gate；QQQ future worst-1d在1d/5d/10d通过。该结论只允许Owner考虑后续
  tail-risk robustness audit，不批准risk overlay、candidate family、策略回测、权重、QLD signal、
  paper-shadow、promotion、production或broker。
- 2026-07-26：当前focused=`10 passed`（Batch 1+2），Ruff/Black/strict mypy/py_compile均PASS；
  任务进入formal validation，不把focused结果替代architecture/contract/Full。
- 2026-07-26：formal分层结果：reporting architecture=`17 passed`、deprecation=`9 passed`；
  architecture首次因最后一次test manifest未刷新而`669 passed / 1 failed`，刷新后
  `670 passed`；contract=`275 passed`、report=`57 passed / 62 warnings`、
  reproducibility=`23 passed`。首个Full因外层调用窗口超时被终止且未形成结果；第二个Full完成
  `7349 passed / 4 skipped / 642 warnings / 2 failed`，两项失败均为运行期间
  `origin/main`从冻结base前进到`a309fa2f5`后触发的Wave14/15
  `CARRIER_PUSH_DRIFT`，不是本任务策略实现断言。由于新main同时修改shared contract与兼容性
  权威，任务保持`VALIDATING`并从新exact main重建，不接受stale-base Full。
- 2026-07-26：从新exact `main=a309fa2f5bf5ef2205041c2ef7416c3e03487aab`
  完成serial contract wave重建；六个task-owned文件逐字节等价，48,950,532 bytes真实输出迁移后
  9/9文件SHA-256一致，content-derived validator保持`0 errors`。新base focused/authority=
  `94 passed`，architecture=`672 passed`、contract=`275 passed`、
  report=`57 passed / 62 warnings`、reproducibility=`23 passed`，required Full=
  `7355 passed / 4 skipped / 643 warnings`。Batch 2任务完成并归档；机械结论仍只允许Owner决定
  是否另立tail-risk robustness audit，不批准risk overlay、candidate family、策略回测、权重、
  QLD signal、paper-shadow、promotion、production或broker。
- 2026-07-26：归档后的post-Full architecture=`670 passed / 2 failed`；两项仅为DEVX-003
  推进`origin/main`后触发的Wave14/15 `CARRIER_PUSH_DRIFT`。任务重开并从exact
  `8a319c2fe26088d414fe034478727d777ca54b84`执行第二次serial contract wave重建；不把
  `a309fa2f5` Full冒充最终树验证，也不重跑或改变真实能力审计结论。
- 2026-07-26：新worktree首次task-shadow generate按设计因缺少ARCH-005 bootstrap handoff
  引用的四份untracked immutable validation summary而fail closed。按tracked
  `inputs/architecture/arch_005_bootstrap_handoff.yaml`的exact path/SHA-256清单，从已审计
  `D:\Work\AITradingSystem_t2461_a309`只迁移这四份summary；不复制旧workspace其余outputs，
  不改变artifact bytes或tracked handoff。四份临时安装物只服务本worktree registry验证，
  随worktree在任务收口后一起删除；canonical tracked bundle与历史hash继续作为恢复边界。
- 2026-07-26：exact `8a319c2f`重建已保持task-owned 5文件逐hash等价、真实输出
  9/9文件共48,950,532 bytes逐hash等价；新base active task registry=`910 total / 420 active /
  490 completed / byte-identical`。pre-Full focused/authority=`22 passed`、reporting
  architecture=`17 passed`、deprecation=`9 passed`、architecture=`674 passed`、
  contract=`275 passed`、report=`57 passed / 62 warnings`、reproducibility=`23 passed`；
  Full与post-Full仍为PENDING，任务保持`VALIDATING_FINAL_INTEGRATION`。
- 2026-07-26：exact `8a319c2f` Full=`7370 passed / 4 skipped / 643 warnings`，
  runner elapsed=`1393.60s`，artifact=
  `outputs/validation_runtime/trading2461_decision_target_capability_audit_b2_20260726/
  8a31_full/test_runtime_summary.json`。任务进入completed archive与post-Full evidence gate；
  task-owned code/test、真实预测、评估承诺与9份审计输出在Full后不再修改。
- 2026-07-26：最终归档态post-Full architecture=`674 passed`、contract=`275 passed`，
  artifacts分别为`8a31_post_full_architecture_final`与`8a31_post_full_contract_final`；
  completed task shadow、兼容性权威和任务归档状态在最终树上一致，Full后仅发生治理证据更新。
- 2026-07-26：提交前冻结检查发现local/remote main第三次前进到`fb18463e5`，新增
  `DATA-GOV-002C1`共享report/catalog/system-flow/architecture authority。旧`8a319c2f`
  候选继续作为已验证carrier保留，不做merge/rebase；新建
  `D:\Work\AITradingSystem_t2461_fb18`，从exact main重放并重新计算共享权威与最终门禁。
- 2026-07-26：exact `fb18463e5`重建保持五个task-owned policy/spec/doc/module/test文件
  逐SHA-256等价，真实输出9/9文件共48,950,532 bytes逐hash等价；兼容性权威以完整
  DATA-GOV-002C1 blob为不可变前缀。focused/authority=`12 passed`、reporting/deprecation=
  `17/9 passed`、architecture=`676 passed`、contract=`275 passed`、report=
  `57 passed / 62 warnings`、reproducibility=`23 passed`；required Full仍为PENDING。
- 2026-07-26：exact `fb18463e5` required Full=`7381 passed / 4 skipped / 643 warnings`，
  elapsed=`1236.63s`，artifact=`outputs/validation_runtime/
  trading2461_decision_target_capability_audit_b2_20260726/fb18_full/
  test_runtime_summary.json`。Full期间local/remote main保持冻结base；任务转completed archive，
  Full后仅允许任务状态、task shadow、兼容性验证字段与post-Full证据更新。
- 2026-07-26：completed archive的post-Full architecture=`676 passed`、contract=`275 passed`，
  artifacts分别为`fb18_post_full_architecture`与`fb18_post_full_contract`；完成态task shadow、
  compatibility authority、Full结果和安全边界一致。写入本证据后再对exact final bytes复验。
- 2026-07-26：本地main在`953e289ac`提交后由并发DATA-GOV-002C2 sibling先行
  ff-only/push到`7b883b840`；旧候选无法继续ff-only。新建
  `D:\Work\AITradingSystem_t2461_7b88`，以exact new main重放已验证task commit，保留
  C2 rate attribution authority并重新生成所有重叠共享证据。
- 2026-07-26：7b88组合树保持五个task-owned文件与真实输出9/9逐hash等价，C1/C2
  compatibility authority均作为不可变历史前缀保留；focused/authority=`15 passed`、
  reporting/deprecation=`17/9 passed`、architecture=`678 passed`、contract=`275 passed`、
  report=`57 passed / 62 warnings`、reproducibility=`23 passed`。
- 2026-07-26：exact `7b883b840`组合树required Full=
  `7394 passed / 4 skipped / 643 warnings`，runner elapsed=`1183.11s`，artifact=
  `outputs/validation_runtime/trading2461_decision_target_capability_audit_b2_20260726/
  7b88_full/test_runtime_summary.json`。Full期间local/remote main保持冻结base；任务转completed
  archive，Full后仅允许任务状态、task shadow、compatibility验证字段与post-Full证据更新。
- 2026-07-26：completed archive的post-Full architecture=`678 passed`、contract=`275 passed`，
  artifacts分别为`7b88_post_full_architecture`与`7b88_post_full_contract`；完成态task shadow、
  C1/C2继承权威、TRADING-2461当前hash authority、Full结果和安全边界一致。写入本证据并刷新
  requirement source hash后，对exact final bytes再次执行同两道门禁。
