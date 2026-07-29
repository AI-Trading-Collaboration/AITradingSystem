# TRADING-2464：O1 Model / Feature Family Owner 决策包

最后更新：2026-07-30

状态：`OWNER_REVIEW_REQUIRED_NOT_ACTIVE`

所属任务：
`TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT`

## 1. 为什么必须单独决策

Owner 于 2026-07-30 要求继续推进 TRADING-2464。该指令足以授权审计当前主线、准备
serial contract wave 与决策材料，但没有给出 exact model/feature family。S4 已冻结 target、
5-session horizon、split、coverage floors、primary gate 与 mandatory falsification，却没有
冻结可执行的 model/feature identity。根据既有 requirement，实施者不能自行补选。

本决策包因此保持 inactive；在 Owner 明确选择前：

- 不读取本轮真实 eligibility/coverage count；
- 不训练模型，不生成 prediction/metric/falsification；
- 不访问 prospective；
- 不启动 Decision Value Audit、risk overlay、candidate/backtest/weights。

## 2. Exact authority 与 no-result-read

- execution base：
  `c6a88ecb337d2cd5ea231bd3c56f2f2bb8269d53`
- S4 policy：
  `TRADING_2463_O1_S4_PILOT_V1`
- S4 文件 SHA-256：
  `3f44a9e0c404661e9abeed20c8141884794f440cc4822beaa2f0a155bd719766`
- historical model policy SHA-256：
  `9b84ac832cced4823367c75e2670e22652c7dbeba8dd0aa5bbc30942287b9b19`
- `new_o1_result_read=false`
- `prospective_accessed=false`
- `model_training_executed=false`
- `coverage_audit_executed=false`

本轮只复核既有政策、历史 contamination 与 DQ authority；TRADING-2461 已归档的负面结果
属于已知设计历史，不能把复用的 family 声称为独立新发现。

## 3. 新发现的 DQ evidence-byte blocker

需求文档和 D0B2B authority 记录：

- receipt ID：
  `dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4`
- expected SHA-256：
  `6a4319f15f65a06345f08965c04cada01083d00a478e06febfdfd21f5ef56a58`
- documented result：`PASS / 0 errors / 0 warnings`
- requested：`2021-02-22..2026-07-27`
- evaluated：`2021-02-22..2026-07-24`

但当前主工作区及三个已登记 worktree 均不存在该 `receipt.json` 的实际 bytes。文本中的 ID、
状态和 hash 不能替代执行输入。正确处理只有两种：

1. 从受治理 canonical evidence store 恢复 exact bytes，并验证 SHA 完全一致；或
2. 在 Owner 选择 model/feature family 后，运行新的 canonical strict DQ，若得到新的 receipt，
   先发布并审阅 policy v2 exact binding，再读取任何真实 coverage。

不得复制不明来源文件、伪造 receipt，或用 TRADING-2460 的 scoped exception 替代。

### 3.1 只读恢复审计结论

2026-07-30 继续进行精确路径审计后，确认 OPS-070 已治理的 permanent runtime clone
`D:\Work\AITradingSystem_ops_runtime` 保留完整历史链；该目录的生命周期权威是
`docs/requirements/OPS-070_Objective_Blocker_and_Consumer_Dependency_DAG.md`，本轮没有
修改其任何字节。

只读验证结果：

- receipt 实际 SHA-256 与 expected SHA 完全一致；
- consumer authorization
  `dq_consumer_authorization_fe8360fab72bb976f3b799dba3a7bb933561cc34fa47b3ad7040a9e5fe5fcc02`
  的 SHA-256 完全一致；
- discovery pointer SHA-256=
  `65f652f79ed07c0cc074dc1cc09fe444fa912fa21580df6ebb2eee340926199f`；
- transaction=`download_txn_80b403268d6023acaf33b0608630b908`，snapshot SHA-256=
  `9ed6e7ec705633bec21e032a25f48ca93fd7ef0ead899bbe857b0f30591d7778`；
- prices、rates、secondary_prices 三个 immutable member 的实际 SHA 与 receipt
  逐项一致；
- 现有 `foundation_consumer_migration` historical-acceptance、pointer、transaction
  contract validator 全链 `PASS`。

因此状态从“没有可恢复来源”修正为
`PASS_EXACT_CHAIN_RECOVERABLE_NOT_MATERIALIZED`。但 runtime clone 与主 checkout 当前 live
`data/raw` 已向后推进，三份 live projection 均不等于 receipt 当时的 input bytes；仅复制
`receipt.json` 仍然无效，也不得覆盖 live `data/raw`。

安全恢复路径是：Owner 选择 A（或批准完整 B）后，使用既有
`materialize_isolated_candidate` 路径从 immutable transaction 生成 isolated candidate，
逐对象校验并在隔离目录重验 strict DQ。选择前没有 materialize、没有 DQ rerun、没有
coverage/model run。

## 4. 推荐选项 A

推荐：
`ADOPT_M1_RIDGE_CROSS_ASSET_STATE_EXACT_REUSE`

建议 Owner 决策：

```text
owner_decision:TRADING-2464:2026-07-30:approve_o1_m1_ridge_cross_asset_state_single_family_v1
```

冻结内容：

- model：`M1_RIDGE_LINEAR`
- penalty：`1.0`
- intercept：train target mean
- preprocessing：仅 train partition z-score
- zero-scale epsilon：`1e-12`
- feature prefix：cumulative `CROSS_ASSET_STATE`
- exact feature IDs：proposal YAML 中列出的 28 项
- interaction：禁止
- automatic search / subset selection / tree / boosting / ensemble：禁止

推荐理由不是历史收益较好，而是该 family 已经 reviewed、deterministic、PIT price-only、实现
和 validator 路径成熟，并且显式复用比在已知旧负面结果后新造 feature subset 更少研究者自由度。
TRADING-2461 的旧 O1 结果必须进入 append-only attempt ledger；本轮只能回答新 S4
split/metric/gate 下的受限 capability，不得声称独立 novel discovery。

## 5. 其他 Owner 选项

### 选项 B：要求新 family

```text
owner_decision:TRADING-2464:2026-07-30:require_new_o1_model_feature_family_pack_v1
```

保持当前 proposal inactive，另发新版本，逐项冻结 feature ID、lookback、transform、model、
penalty、preprocessing、经济 rationale 与 multiple-testing treatment。新 family 仍必须在
任何真实 count/result 前批准。

### 选项 C：继续暂停

```text
owner_decision:TRADING-2464:2026-07-30:hold_o1_capability_audit_v1
```

不恢复/重跑 DQ，不实现 builder，不读取 coverage，不训练模型。

## 6. Owner 选择后的固定顺序

仅当选项 A 或完整的新 family 获批，且 receipt bytes gate 关闭后：

1. 激活 serial contract policy，不改变 S4 其他 slot；
2. 使用 synthetic fixtures 实现 builder/validator 与负例；
3. 通过 deterministic double-build 与 source reconstruction；
4. 执行 coverage-only audit；
5. coverage 任一 floor 不足则机械关闭；
6. 只有 coverage PASS 才运行一次 canonical model；
7. 执行全部 mandatory falsification；
8. 机械输出四类之一。

正面 class 仍只允许 Owner 决定是否另立 Decision Value Audit。

## 7. 安全边界

- `activation_allowed=false`
- `new_o1_result_read=false`
- `prospective_accessed=false`
- `model_training_executed=false`
- `decision_value_audit_started=false`
- `risk_overlay_created=false`
- `candidate_backtest_weights_created=false`
- `qld_automatic_selection_enabled=false`
- `paper_shadow_changed=false`
- `production_effect=none`
- `broker_action=none`
