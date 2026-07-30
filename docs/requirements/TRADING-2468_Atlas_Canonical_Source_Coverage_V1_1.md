# TRADING-2468：Atlas Canonical Source Coverage V1.1

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2468_ATLAS_CANONICAL_SOURCE_COVERAGE_V1_1`

优先级：`P1`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2468:2026-07-30:advance_atlas_canonical_source_coverage_v1_1
```

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2466 的只读 Atlas MVP 上扩展真实 canonical research sources，使金融知识较少的
读者能够在同一页面区分：

- 历史研究重启为什么没有进入新候选扩展；
- QLD 为什么只获得 role-limited implementation 身份；
- decision-target label foundation 为什么是“标签合同可用”而不是“模型可用”；
- O1 为什么在模型训练前因 coverage 不足停止；
- 未来 O1 re-entry 为什么仍未到期且未获运行授权。

V1.1 只扩展 Git-authoritative source coverage、研究路径、结果卡和归因卡，不重新执行或重算
DQ、coverage、model、backtest、metric、threshold、candidate、weight、promotion 或 Owner
decision。它不是全仓历史研究的完成态；未纳入的 research campaigns 必须在后续 adapter
任务中继续显式登记，页面不得声称“已经覆盖所有历史结果”。

## 2. 与 V1 的关系

复用并保持不变：

- `strategy_research_explorer_snapshot.v1`；
- `strategy_research_explorer_source_ref.v1`；
- `strategy_research_path_node.v1`；
- `strategy_research_path_edge.v1`；
- `strategy_research_result_card.v1`；
- `strategy_research_attribution.v1`；
- 五种互斥 assertion kinds；
- raw/display status 分离；
- read-only、no-recompute、no-write、no-command-dispatch 边界。

本任务不改变 public contract、DQ/PIT semantics、cache identity、research window policy 或
investment-facing threshold，因此使用 `SINGLE_LANE`，不触发 serial contract wave。

## 3. V1.1 exact source coverage

V1.1 新增以下 canonical source refs：

1. `docs/requirements/TRADING-2446_to_2448_Research_Restart_R0_R2.md`；
2. `docs/research/strategy_research_restart_r0_r2_closeout_2026-07-20.md`；
3. `docs/requirements/TRADING-2459_Strategy_Style_Discovery_SPY_QLD_Universe_Evaluation.md`；
4. `docs/requirements/TRADING-2460_Decision_Target_Capability_Audit_Label_Foundation.md`；
5. `docs/requirements/TRADING-2463_Decision_Target_Redesign_Preregistration.md`。

继续保留 V1 的：

6. `config/atlas/source_registry.yaml`；
7. `config/research/o1_relative_opportunity_capability_audit_v1.yaml`；
8. `config/research/o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml`。

每个 source ref 必须绑定 exact commit、repository-relative path、content SHA-256、
artifact identity、requested/evaluated window（适用时）、DQ/context readiness 与 limitation。
不得读取 ignored runtime result、cache、外部数据、浏览器历史或任何 secret。

## 4. 窗口与证据角色

项目当前 primary research window 仍从 `2021-02-22` 开始。

TRADING-2446～2448 的历史 closeout 必须设置：

```text
legacy_history_partial=true
research_context_complete=false
data_quality_ready=false
```

理由：

- 其 `2022-12-01` 结果仅能作为 `legacy_comparison`；
- source top-N 与 locked holdout 有重叠，不能声称 unbiased OOS；
- event-risk 与 forward maturity 证据没有闭合。

Atlas 可以展示该历史链路和负面/不足证据，但不得让它成为新 run 的 active default、primary
conclusion boundary 或现行策略结果。

QLD source 必须同时展示：

- scoped DQ=`PASS_WITH_WARNINGS`；
- canonical full-cache DQ=`FAIL`；
- role-limited implementation approved；
- automatic selection、official weights、paper-shadow、production 与 broker 仍关闭。

Label foundation source 必须同时展示：

- scoped `QQQ/SPY/SGOV` receipt=`PASS`；
- global canonical DQ=`FAIL`；
- `LABEL_FOUNDATION_READY` 不等于 model/capability/strategy ready；
- 未训练模型、未执行 candidate/parameter search。

## 5. Research Path Map

V1.1 至少形成以下相互连接的 campaign paths：

### A. 历史研究重启

```text
restart contract
  -> walk-forward / robustness / forward maturity evidence
  -> CONTINUE_EVIDENCE_CLOSURE
  -> candidate expansion paused
```

必须展示：

- R0 13/13 hard checks PASS；
- 40 个 test folds 为 20 reject + 20 review-required；
- `event_risk_high=15<20`；
- 20d/60d forward maturity 为 0；
- 结论不是 strategy PASS，而是继续闭合证据。

### B. QLD implementation role

```text
SPY reference + QLD instrument evaluation
  -> scoped research result
  -> Owner role-limited implementation decision
  -> canonical DQ strict PASS blocker
```

必须展示 QLD 是 2x implementation instrument，不是独立 alpha、signal、style 或自由 candidate。

### C. Decision target foundation

```text
label foundation
  -> target redesign
  -> O1 preregistration
  -> O1 coverage-only audit
```

必须展示 label contract、Owner target selection 与 empirical result 是不同阶段，不能用工程
validation PASS 替代 capability conclusion。

### D. O1 current and future

```text
O1 coverage insufficient
  -> model remains NOT_EVALUATED
  -> Owner A+D route
  -> inactive calendar re-entry
```

未来 trigger 仍为 `2027-02-01` not-before，仅开放重新向 Owner 申请，不自动执行。

## 6. 实施步骤

### S0：任务登记

- 登记本任务、Owner token、优先级、状态、owner、blocker 与 acceptance；
- 不修改 Atlas product code；
- 运行 governed preflight。

### S1：Source inventory freeze

- 验证全部 exact paths 存在且不命中 known-unrelated exclusion；
- 冻结 source IDs、artifact identities、window roles、DQ/context flags 与 limitations；
- 明确 V1.1 coverage boundary 和尚未覆盖的 campaigns。

### S2：Registry 与投影扩展

- 更新 `config/atlas/source_registry.yaml` 的 registry identity、sources、campaign nodes、
  edges、results、attributions 与 glossary；
- 不修改公共 read-model contract；
- 所有新增 result 均保持 `investment_facing=false`。

### S3：Validation 与 reader presentation

- 增加 exact coverage regression；
- 验证 source drift、missing source、duplicate ID/path、closed references、graph lineage、
  DQ limitation、legacy-window downgrade 与 no-investment-pass；
- 验证静态 HTML 在多 campaign 数据下仍无 script/form/write API。

### S4：Preview 与 formal validation

- 生成 deterministic local preview：
  `outputs/atlas/strategy_research_explorer/trading_2468_v1_1/`；
- 运行 focused、task-shadow/generated freshness、Architecture、Contract、Integration、
  Reproducibility 与风险相称的 Full；
- 将最终 evidence 迁移到 canonical output root。

### S5：Closeout

- 更新本 requirement 与 task register；
- MVP 扩展完成后转 `BASELINE_DONE`；
- cross-snapshot diff、interactive API、带引用问答及剩余 campaign adapters 继续另立任务。

## 7. Claims

task-owned paths：

```text
config/atlas/source_registry.yaml
src/ai_trading_system/atlas/html_renderer.py
tests/atlas/test_source_registry_coverage.py
tests/atlas/test_source_projection.py
tests/atlas/test_snapshot_builder.py
tests/atlas/test_html_renderer.py
```

coordinator-owned paths：

```text
docs/requirements/TRADING-2468_Atlas_Canonical_Source_Coverage_V1_1.md
docs/task_register.md
docs/system_flow.md
docs/artifact_catalog.md
inputs/architecture/**
registry/development_tasks_shadow/**
tests/test_arch_004_refactor_policy.py
```

module claim：

```text
AtlasSourceRegistry content expansion only
```

resource claim：

```text
read-only tracked Git files only
no external network
no cache
no DQ execution
no runtime research result read
no production or broker resource
```

## 8. 验收标准

1. V1.1 source refs 精确覆盖第3节的8个 source paths；
2. 所有 source refs 绑定 exact commit 与 content SHA-256；
3. 新增四条 campaign paths 全部连接到 program root；
4. legacy restart 明确降级，不把 `2022-12-01` 作为 active default；
5. QLD 同时显示 scoped warning/global FAIL/role-limited/no-auto 边界；
6. label foundation 显示 scoped readiness/global FAIL/no-model 边界；
7. O1 显示 coverage insufficient、model NOT_EVALUATED、future inactive；
8. source、node、edge、result 与 attribution IDs 唯一且 closed；
9. 所有新增 result `investment_facing=false`；
10. validation PASS 不显示为 strategy PASS 或 production ready；
11. deterministic double build byte-identical；
12. source tamper/path traversal/missing/duplicate/lineage drift fail closed；
13. HTML 无 script、form、external resource、write API 或 command dispatch；
14. task shadow、compatibility/current authority 与 applicable formal tiers PASS；
15. `production_effect=none`、`broker_action=none`。

## 9. Stop conditions

出现以下任一情况立即停止：

- 需要修改 public read-model contract 或 status semantics；
- 需要读取 runtime/cache/external data 才能解释 source；
- 无法同时保留 full/scoped DQ 边界；
- 旧窗口可能被误显示为 active default；
- 任一 source claim 不能由 exact Git bytes 支持；
- 需要新增 investment threshold、模型计算或策略推断。

## 10. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- frozen base：`9eb3909cb7d50e48f54d4d38ca20e2e1fc8ce65a`；
- branch：`codex/trading-2468-atlas-coverage`；
- workspace：`D:/Work/AITradingSystem`，不创建额外 worktree；
- known-unrelated exclusion：
  `docs/research/growth_tilt_owner_diagnosis_pack.md`，不得读取、hash、复制、stage、修改或删除；
- exit condition：task branch 已形成 validated final candidate、ff-only 进入 local `main`、
  ordinary push 完成、canonical preview/evidence 校验完成；
- cleanup：切回 `main` 后删除 merged task branch并prune；tracked实现由 final commit 恢复，
  ignored preview保留在 canonical output root。

## 11. 进度记录

- 2026-07-30：Owner确认可参考“先扩 canonical source coverage，再做 diff/问答”的方案持续
  推进；登记本任务并进入 `IN_PROGRESS`。当前只授权 Git-authoritative read-only coverage
  expansion，不授权任何 empirical action、production 或 broker action。
- 2026-07-30：`SINGLE_LANE` START / LANE preflight 均 PASS；完成8个 source、4条 campaign、
  21个 node、22条 edge、8个 result、12条 attribution 的 registry 与页面实现。focused
  Atlas / contract tests=`28 passed`，Ruff PASS；task-shadow S0/S1/V2 generate + validate PASS，
  task count=`931`、active=`426`、completed=`505`、byte-identical=true。
