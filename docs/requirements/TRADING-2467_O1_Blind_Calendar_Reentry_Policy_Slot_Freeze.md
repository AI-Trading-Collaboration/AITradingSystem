# TRADING-2467：O1 Blind Calendar Re-entry Policy Slot Freeze

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2467_O1_BLIND_CALENDAR_REENTRY_POLICY_SLOT_FREEZE`

优先级：`P0`

状态：`IN_PROGRESS`

Owner 决定：

```text
owner_decision:TRADING-2465:2026-07-30:select_o1_blind_calendar_reentry_with_generic_evidence_infrastructure_v1
owner_decision:TRADING-2467:2026-07-30:authorize_inactive_o1_blind_calendar_reentry_policy_slot_freeze_v1
```

production effect：`none`

broker action：`none`

## 1. 目标与授权边界

本任务把 Owner 采纳的 A+D 路线、blind calendar trigger、data-vintage rule、single-look
budget 和 stopping rule 冻结为 inactive reviewed policy，并实现 independent validator 与
controlled/static tests。

本任务不授权：

- data acquisition 或 `aits validate-data` 执行；
- 任何真实 eligibility、fold、ESS、regime、event 或 coverage read；
- model training、prediction、metric、canonical run 或 falsification；
- Decision Value Audit、risk overlay、candidate/backtest/weights、paper-shadow；
- production 或 broker action。

policy/validator/tests PASS 后必须停止；真实 coverage run 仍需要新的独立 Owner token 与任务。

## 2. 权威输入

冻结 source base：

```text
repository=https://github.com/AI-Trading-Collaboration/AITradingSystem
exact_commit=26e76d25b425957926a37ce8be5e55c58d356f37
TRADING-2465 requirement blob=563e1e778c8277b981784b1b0332d97888f3425e
TRADING-2465 proposal blob=43960eeab15344a7b8ec697bcbf517c555bd0401
TRADING-2464 frozen policy blob=7d90d304cd4b9d3b2a69e534413837b5daf19e05
```

Web Pro advisory：

```text
conversation_url=https://chatgpt.com/c/6a6ac917-8d38-83ee-9161-6e6fab018828
reviewed_commit=26e76d25b425957926a37ce8be5e55c58d356f37
classification=UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED
CANNOT_VERIFY_EXACT_BACKEND_ROUTE=true
```

网页回答不是 Owner approval；上述 Owner tokens 才是项目授权。

## 3. Frozen policy values

### 3.1 Route 与 calendar trigger

```text
route_id=A_PLUS_D_O1_BLIND_CALENDAR_REENTRY_WITH_GENERIC_EVIDENCE_INFRASTRUCTURE
trigger_type=BLIND_CALENDAR_NOT_BEFORE
not_before_date=2027-02-01
not_before_at=2027-02-01T00:00:00-05:00
timezone=America/New_York
count_based_trigger_allowed=false
dynamic_trigger_allowed=false
automatic_execution_on_trigger=false
```

理由：在读取任何新的 O1 eligibility/coverage 前固定日期；日期只表示可以重新向 Owner
申请运行，不表示预计 coverage 会通过。选择基于六个完整 calendar-month publication
cycles，不基于四个已知 failed cells 的距离。

### 3.2 Data-vintage rule

```text
source_publication_cutoff_inclusive=2027-01-31T23:59:59-05:00
primary_research_start=2021-02-22
pre_2021_02_22_primary_rows_allowed=false
requested_end=2027-01-29
evaluated_end=2027-01-22
dq_required_status=PASS
dq_required_error_count=0
dq_required_warning_count=0
```

`evaluated_end` 是其后第五个 common session 不晚于 `requested_end` 的最后 frozen anchor。
项目 US equity calendar 已核对 2027-01-25 至 2027-01-29 为五个 future sessions。未来 source
无法满足该 exact boundary 时停止，不得静默缩短、延长、平移或替换。

publication/PIT 条件：

- `source_published_at <= cutoff`；
- `known_at <= cutoff`；
- `available_at <= cutoff`；
- unknown historical timestamp、current-view substitution、silent imputation 均禁止；
- manifest/source/window drift 视为 `INVALID`，不得隐式创建新 vintage。

在首次真实 coverage read 前，future runner 必须 deterministic 构建 canonical manifest，
至少绑定 execution commit、policy path/blob/content SHA、Owner decision ID、provider/
endpoint/parameters、publication/download timestamps、每个 immutable member SHA-256/size/
rows/range、DQ receipt、calendar/event ledger、requested/evaluated range、PIT contract、
runtime/package lock。稳定排序和 UTF-8 deterministic serialization 后：

```text
exact_vintage_identity =
  O1_V2_VINTAGE_SHA256_ + SHA256(canonical_manifest_bytes)
```

本任务只冻结该规则，不构建真实 manifest。

### 3.3 Future attempt 与 look budget

```text
attempt_id=O1_M1_RIDGE_CROSS_ASSET_STATE_V2_CALENDAR_REENTRY
pristine_independent_first_attempt_claim_allowed=false
append_only_attempt_ledger_required=true
maximum_reentry_coverage_looks=1
budget_window_start_inclusive=2027-02-01T00:00:00-05:00
budget_window_end_exclusive=2028-02-01T00:00:00-05:00
rolling_window=false
automatic_rollover=false
unused_budget_after_expiry=EXPIRES
```

任何暴露或 materialize 新真实 eligibility、fold、ESS、regime cell、event family 或 coverage
gate 的动作都消耗 look。future runner 必须在首次读取前原子 append `LOOK_CONSUMED`；
之后 crash/exception/partial output 仍消耗唯一 look。

```text
automatic_retry_allowed=false
resume_allowed=false
overwrite_allowed=false
second_candidate_allowed=false
result_driven_rerun_allowed=false
```

## 4. V1 immutable boundary

TRADING-2464 V1 的以下内容必须 exact 复用，不能根据 `98/100`、
`23.71930136737/24`、volatility HIGH `2/3` 或 current_drawdown LOW `13/15` 修改：

- target、continuous form、5 common sessions horizon；
- split、purge、embargo、maturity；
- coverage floors 与 ESS formula；
- regime axes/bins/train-only fit；
- FOMC/CPI/NFP event definitions/window/ledger rule；
- `M1_RIDGE_LINEAR + CROSS_ASSET_STATE`；
- feature IDs、preprocessing、seed；
- metric、bootstrap 与 all mandatory falsification；
- mechanical classification mapping。

V1 capability 仍是 `NOT_EVALUATED`，不是 `NO_MEASURABLE_SKILL`。

## 5. Stop matrix

### PASS

未来只有 strict DQ `PASS/0/0`、exact vintage/lineage valid 且每个 frozen mandatory coverage
floor PASS 时，才能输出 `COVERAGE_ELIGIBLE_PASS_ONLY`。随后停止，不训练模型；下一步必须是
新的 Owner canonical-run decision。

### FAIL

Owner 拒绝路线、static policy/validator/engineering validation 失败或 exact policy 无法构建，
停止且不产生研究 capability conclusion。

### INSUFFICIENT

future run valid，但任一 mandatory DQ 或 coverage requirement 不满足，输出既有
`INSUFFICIENT_COVERAGE_OR_DQ`，关闭 V2，不重试。

### INVALID

任一 PIT breach、wrong vintage、lineage tamper、exact reconstruction mismatch、
unauthorized result read、post-result contract change、resume/overwrite、attempt-ledger
violation 或 independent validator error，quarantine artifacts，不产生 capability class。

## 6. Implementation claims

serial contract wave 已冻结：

- `config/research/o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml`；

策略 lane leaf paths：

- `scripts/trading2467_validate_o1_blind_calendar_reentry_policy.py`；
- `tests/research_strategies/test_o1_relative_opportunity_blind_calendar_reentry_policy.py`。

module claim：

```text
inactive policy serialization and independent validator only
```

contract claim：

```text
o1_relative_opportunity_blind_calendar_reentry_policy.v1
```

resource claim：

```text
no network
no cached market data
no DQ execution
no runtime output root
no retained evidence bytes
```

evidence-lineage claim：只读取 Git-authoritative V1 IDs、SHA 和 immutable section hashes；
禁止打开、复制、修改或清理 TRADING-2464 retained runtime evidence。

## 7. Coordinator-only boundary

策略 lane 不得修改：

- `docs/task_register.md`；
- 本 supporting requirement；
- `docs/system_flow.md`；
- `inputs/architecture/**`；
- compatibility/deprecation authority；
- `registry/development_tasks_shadow/**`；
- module/test manifests 与 formal validation artifacts。

纯 inactive policy 不改变实际 data flow，因此本 lane 不要求修改 `docs/system_flow.md`。若
scope 扩大到 CLI、runtime、report schema 或数据流，必须停止、更新 requirement/claims，
并由 coordinator 在同一 change 更新 system flow。

## 8. Temporary workspace lifecycle

计划 worktree：

```text
D:/Work/AITradingSystem_t2467_o1_reentry_policy
```

- owner task：本任务；
- purpose：serial contract wave 后实现 independent validator 与 controlled/static tests；
- creation condition：两任务已登记、共享合同进入 exact new main、DUAL_LANE preflight PASS；
- exit condition：lane commit 已进入 validated final candidate、无 active process、无 unique
  tracked/untracked/ignored content、Git history 可恢复；
- cleanup：exact absolute path audit 后用 `git worktree remove` 删除。

## 9. Acceptance criteria

1. policy 所有 Owner-selected 字段完整，不含 `NOT_SELECTED`；
2. V1 path/file/blob/section hashes exact 绑定；
3. `activation_allowed=false`；
4. data acquisition、DQ、coverage、model、canonical、falsification、downstream 全为 false；
5. retry/resume/overwrite/second candidate 全为 false；
6. validator independent 重建 route/date/vintage/look/stopping/V1 identities；
7. wrong date、rolling budget、second look、wrong vintage、threshold drift、PIT missing、
   unauthorized activation 全部 fail closed；
8. deterministic double serialization byte-identical；
9. 不读取 retained runtime evidence；
10. focused、authority、Architecture、Contract 与 final formal validation PASS；
11. `production_effect=none`、`broker_action=none`；
12. first stop 为 policy/validator/tests PASS 后、任何真实 data/DQ/coverage/result read 前。

## 10. 进度记录

- 2026-07-30：Owner 采纳 Web Pro 建议，选择 A+D、`2027-02-01` blind trigger、固定
  12-month single-look budget 与 exact vintage rule，并授权本 inactive policy-freeze task。
- 2026-07-30：任务登记为 `IN_PROGRESS`；当前仍不授权任何 empirical action。
- 2026-07-30：serial contract wave 已写入
  `o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml`；历史 TRADING-2465
  proposal 保持 immutable inactive，旧 validator 已改为同时验证历史 proposal 和
  `BASELINE_DONE -> TRADING-2467` exact Owner handoff。策略 lane 只剩独立 validator/tests，
  等待串行候选进入 exact `main`。
