# TRADING-141_to_145 Historical Advisory Replay and Backfilled Outcome Evaluation

最后更新：2026-07-12

## 状态

- 任务登记：`docs/task_register.md` 中 `TRADING-141_to_145`
- 当前状态：VALIDATING
- 下一责任方：项目 owner 人工复核
- 安全边界：no broker API、no automatic order、no production candidate、no owner auto approval、no official target weight mutation、no production config auto calibration

## 背景

`TRADING-136_to_140` 已完成 paper portfolio、forward advisory outcome、owner attribution、shadow aging 和 weekly advisory review。当前 forward outcome 必须等 1/5/10/20 trading day 窗口自然到期；这是正确的 point-in-time 行为，但会让 advisory 规则验证节奏很慢。

本阶段新增 historical replay 链路：只使用历史上已经存在的 shadow/advisory/owner/paper artifacts 作为 decision input，在 PIT safety audit 通过后重放 advisory variants，并用已经发生的价格计算 backfilled outcome。

## 结果类型

`FORWARD_OUTCOME` 表示真实 forward tracking：某天系统真实生成 advisory，未来窗口到期后再计算结果，可信度最高但速度慢。

`HISTORICAL_REPLAY` 表示基于历史已存在 artifact 的重放：decision input 必须来自当时已经存在的 advisory/target/portfolio/owner artifacts；as_of 之后的价格只能用于 outcome calculation，不能进入 replay decision input。

`BACKTEST_SIMULATION` 表示用当前策略或当前参数回推历史：可用于研究，但可信度低于 historical replay，必须明确标记，不能伪装成历史真实 advisory replay。

所有新增 replay/outcome/simulation/review artifacts 必须写入 `outcome_mode` 或等价字段，并保留 `production_effect=none`、`broker_action_allowed=false`、`broker_action_taken=false`。

## 阶段拆分

1. `TRADING-141` Historical Advisory Artifact Inventory & PIT Safety Audit
   - 新增 `replay-inventory build/report` 和 `validate-replay-inventory`
   - 扫描 `shadow_monitor`、`position_advisory_daily`、`consensus_drift`、`owner_review`、`paper_portfolio` 等 existing artifacts
   - 输出 PIT_SAFE / PIT_WARNING / PIT_UNSAFE、replay eligibility 和 limitations

2. `TRADING-142` Historical Advisory Replay Engine
   - 新增 `historical-replay run/report` 和 `validate-historical-replay`
   - 默认跳过 PIT_UNSAFE，按需要允许显式包含 PIT_WARNING
   - 为每个 replay event 生成 `no_trade`、`consensus_target`、`limited_adjustment`、`owner_decision`、`paper_action` variants

3. `TRADING-143` Backfilled Outcome Evaluation
   - 新增 `backfill-outcome run/report` 和 `validate-backfill-outcome`
   - 对 1/5/10/20 trading day 窗口输出 AVAILABLE / PENDING / INSUFFICIENT_DATA
   - 价格数据只用于 outcome，不能改变 replay decision input

4. `TRADING-144` Historical Paper Portfolio Simulation
   - 新增 `historical-paper-sim run/report` 和 `validate-historical-paper-sim`
   - 基于 selected replay variant 重建 simulated paper state history 和 simulated trade ledger
   - `broker_action_taken=false` 必须可验证

5. `TRADING-145` Replay Performance Review & Advisory Rule Calibration
   - 新增 `replay-performance-review run/report` 和 `validate-replay-performance-review`
   - 汇总 advisory rule effectiveness、variant effectiveness、calibration recommendations 和 Reader Brief section
   - full-PASS backfill/simulation必须绑定同一replay并冻结source/policy；missing metrics保持null
   - relative return正负不能冒充false alarm/missed opportunity；方向性建议受versioned sample floors约束
   - 只输出建议，不自动修改 `position_advisory_v1.yaml` 或任何 production config

## PIT Safety 规则

`PIT_SAFE`：
- source advisory / target weights existed at `as_of`
- decision input 不使用 later candidate selection
- decision input 不使用 future price
- `as_of` 之后价格只用于 outcome calculation

`PIT_WARNING`：
- optional metadata reconstructed
- owner decision missing
- portfolio snapshot approximated
- source artifact existed but missing optional fields

`PIT_UNSAFE`：
- target weights generated after replay date using future data
- candidate selected after replay date
- missing source artifact and using current config to infer past decision
- cannot prove what was known at `as_of`
- advisory artifact generated after replay `as_of`
- missing price coverage needed to evaluate outcome windows

`PIT_UNSAFE` 不允许进入默认 historical replay，也不能被
`--include-pit-warning` 纳入；该参数只适用于 owner decision missing、snapshot
approximated、optional metadata reconstructed 等 non-hard warning。

## 验收标准

- `replay-inventory build/report` 可运行，`validate-replay-inventory` PASS
- `historical-replay run/report` 可运行，variants 生成完整，`validate-historical-replay` PASS
- `backfill-outcome run/report` 可运行，AVAILABLE/PENDING/INSUFFICIENT_DATA 区分正确，`validate-backfill-outcome` PASS
- `historical-paper-sim run/report` 可运行，ledger 可重建 state，`validate-historical-paper-sim` PASS
- `replay-performance-review run/report` 可运行，recommendations 可读且不自动修改 config，`validate-replay-performance-review` PASS
- Reader Brief 展示 `Dynamic Rescue Historical Replay Performance`
- README、operations runbook、system flow、report registry、artifact catalog、task register 和本需求文档同步
- focused tests、ruff、compileall、git diff check、dynamic-v3 root validation 和 dynamic-v3 family artifact validation 通过

## 进展记录

- 2026-07-12：G2.4AV完成TRADING-145 hardening与CLI迁移；focused 91、architecture 231、contract 203全部PASS。当前fixture只有1个可比distinct event，低于3-event/10-window pilot gate，因此明确输出`directional_evidence_ready=false/continue_forward_tracking`；这不是失败，而是防止稀疏历史结果被误解释为规则校准依据。G2.4继续，尚未触发ARCH-005 handoff。
- 2026-07-12：ARCH-004G2.4AV对TRADING-145执行source-derived hardening。新增reviewed `replay_performance_review_v1.yaml`、source/time/lineage preflight与`replay_performance_review_source_snapshot.json`；effectiveness只输出证据可支持的positive/nonpositive rate，unsupported false-alarm/missed-opportunity和missing metrics保持null。Pilot evidence gate要求至少3个distinct replay events、10个available 5日variant windows且simulation AVAILABLE；当前fixture只有1个可比distinct event，因此诚实输出`directional_evidence_ready=false/continue_forward_tracking`。Validator重验两个live source与policy并重算全部JSON/Markdown/Reader Brief；任何建议仍为manual-only，不修改config、promotion、weights、portfolio、production或broker。
- 2026-06-09：新增任务登记和需求文档，进入实现阶段。
- 2026-06-09：实现完成并转入 VALIDATING。真实链路输出 replay
  inventory `4cd60c43a04b2288`、historical replay `0f407af36295acf9`、
  backfilled outcome `b9bc15e81c38dade`、historical paper simulation
  `daf5c9deef4601a9`、replay performance review `9d0ee2c74043c904`。
  当前 backfilled outcome / performance review 为 `PENDING`，原因是真实样本的
  1/5/10/20 trading day outcome windows 尚未全部到期；系统未伪造 AVAILABLE 结论。
  Reader Brief `2026-06-05` 已展示 replay-only `PARTIAL/PENDING` 摘要，
  `production_candidate_generated=false`、`automatic_candidate_promotion=false`、
  `shadow_enrollment_allowed=false`。验证通过五个新增 artifact validate、
  dynamic-v3 root validation、dynamic-v3 family artifact validation、
  documentation contract、report index、Reader Brief quality、focused pytest、
  ruff 和 compileall。
- 2026-06-09：TRADING-146 hardening 补充 PIT safety 解释：advisory artifact
  generated after replay `as_of`、missing outcome price coverage 和 missing target
  weights 都属于 hard PIT limitations，必须归为 `PIT_UNSAFE` / `INELIGIBLE`；
  `--include-pit-warning` 不得纳入这些 rows。
- 2026-06-09：TRADING-146 hardening 后重建 latest replay chain。新 inventory
  `358d353576f86a47` 把原 2 个 warning events 改为 `PIT_UNSAFE`；
  historical replay `86b56e67308cdc50` 输出 0 events / 2 skipped；backfilled
  outcome `38f3b24513fc8229`、historical paper sim `278aaea770c9d106` 和
  replay performance review `f5a0d6edae45994f` 均保持 `INSUFFICIENT_DATA` 或
  `continue_forward_tracking`，不生成生产候选、不自动 promotion、不触发 broker。
- 2026-06-09：full pytest 复验发现
  `tests/test_backfill_outcome.py` 的 `INSUFFICIENT_DATA` fixture 仍使用
  missing symbol 触发缺价；TRADING-146 hardening 后该 row 会先被标为
  `PIT_UNSAFE` 并在 historical replay 阶段排除，不能再证明 backfill outcome
  对已进入 replay 的样本正确区分 `INSUFFICIENT_DATA`。本轮已将 fixture 改为
  inventory/replay 阶段有未来价格、backfill 窗口计算时缺少部分价格的样本，并
  补齐缺失 window price fail-closed 行为，避免把 `NaN` return 误记为
  `AVAILABLE`。验证通过 `tests/test_backfill_outcome.py`、scoped Black/Ruff
  和全量 pytest（2270 passed, 330 warnings）。
