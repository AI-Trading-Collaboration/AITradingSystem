# TRADING-2542C：Growth Action Value 独立复核整改与冻结就绪 V1

## 1. 状态与目标

- 状态：`BLOCKED_OWNER_INPUT_ENGINEERING_CANDIDATE_READY`；
- 优先级：`P0`；
- 上游：`TRADING-2542A_GROWTH_ACTION_VALUE_EXACT_MEASUREMENT_AND_JOINT_DECISION_CONTRACT_V1`、`TRADING-2542B_GROWTH_ACTION_VALUE_CANONICAL_DQ_PIT_SERIAL_CONTRACT_V1`；
- 目标：把 exact-commit 独立复核发现转成一个新的、可机械回放的冻结就绪合同版本；保留 V1/V2 与 2542B V1 字节不变，不用修改旧版本掩盖缺口；
- 安全边界：本任务不运行 provider、cache、真实 DQ、Cloud、backtest 或 empirical evaluation，不产生投资结论，不改变 production/broker/order/fill 状态。

## 2. Owner 指令与独立复核

Project Owner 已要求 Codex 继续剩余工作，并在此前明确要求 Web Pro 复核结果不再逐次人工转述确认。该指令授权 Codex登记和实现独立复核所要求的新版本工程整改，但不能被解释为：

- 当前 V2 八轴已逐项批准；
- 四项 DQ numeric 已经获得可执行 authority；
- 任何真实数据读取、DQ、回测、production 或 broker action 已获授权。

本轮 advisory review 固定在：

- repository commit：`1ca8ccf95c2a93a1b50164345d3e101a59b50838`；
- conversation：`https://chatgpt.com/c/6a8ae448-a5b4-83e8-8d88-d7e6b22e0fc2`；
- UI evidence：账号与 composer 均显示 `Pro`；
- backend route：`CANNOT_VERIFY_EXACT_BACKEND_ROUTE`，模型自述或 UI 标签不是权威路由证明；
- 最终响应：已自然完成，UI 显示 `GPT-5.6 Pro`，思考时长 `26m 50s`；
- overall disposition：`REQUEST_NEW_VERSION_BEFORE_FREEZE`。

## 3. 独立复核结论与本地 reconciliation

以下事实已由 Web Pro 的逐文件取证和本地 exact tree 交叉核验：

1. V2 `common_series_contract` 没有直接绑定 `expected_session_count=1202` 与 exact session inventory SHA-256；`missing_required_session_outcome=INVALID` 因此缺少合同内可重放的“required session set”身份。
2. `NON_BETA_ACTION_VALUE` 的 `insufficient_rule` 引用了 axis/global sample floor，但 V2 没有给出该轴的明确 `minimum_common_sessions`，存在静默缩窗解释空间。
3. 复核提示中指定的 `tests/test_strategy_growth_action_value_threshold_exact_value_sheet_v2.py` 在 exact commit 不存在；真实 V2 测量合同测试位于 `tests/test_strategy_growth_action_value_measurement_contract.py`。这是审阅入口可发现性缺口，不能用不存在的路径冒充测试证据。
4. 提示中指定的 `strategy_growth_action_value_threshold_exact_value_sheet.py` 是保留的 V1 loader；真实 V2 loader/纯测量函数位于 `strategy_growth_action_value_measurement_contract.py`。V1 loader 本身不是错误，但新版本必须提供无歧义的版本身份与测试入口。
5. 2542B 已定义 exact 1202-session inventory、六类 DQ/PIT serial semantics、UNKNOWN/INVALID 与 window terminal；四项 numeric 仍为 `OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY`，不得把工程 PASS 写成 DQ PASS。

第 3、4 项是本轮提示入口与仓库真实文件布局之间的可发现性问题，不表示 V2 没有 loader/tests。真实 V2 authority 是：

- `src/ai_trading_system/strategy_growth_action_value_measurement_contract.py`；
- `tests/test_strategy_growth_action_value_measurement_contract.py`。

新 successor 仍须使用明确的版本化文件名，避免未来审阅再次把 V1 loader 当成 V2/V3 authority。

### 3.1 八轴 disposition

| axis | disposition | successor 动作 |
|---|---|---|
| `NON_BETA_ACTION_VALUE` | `APPROVE_EXACTLY_AS_DRAFTED` | 保留 `0.0100`、paired circular block bootstrap、20/10000/0.95/seed 2542；新增 global 1202 inventory binding 和明确 `minimum_common_sessions=252`。 |
| `NET_OF_COST_RETURN` | `REJECT_AND_REQUEST_NEW_VERSION` | 保留 `0.0075` 与 `0.0001`，冻结 candidate/comparator gross-net-cost reconciliation operands、逐 session 频率、`MAX_ABSOLUTE_SESSION_RESIDUAL` aggregation、decimal-return unit、exact comparison 与 missing/invalid mapping。 |
| `ACTUAL_PATH_DRAWDOWN_REGRESSION` | `APPROVE_EXACTLY_AS_DRAFTED` | 原样保留 `0.0200`、actual compounded NAV、每 slice NAV reset 与六 slice 全称门禁。 |
| `FALSE_RISK_OFF_COST` | `REJECT_AND_REQUEST_NEW_VERSION` | 保留 `0.0025`/10/20/`0.0300`/`-0.0500`；定义 first-session left censor、transitive adjacent-anchor merge、带括号 missed-return 公式和 `EX_POST_ATTRIBUTION_ONLY_NOT_DECISION_INPUT`。 |
| `CANONICAL_DQ_PIT` | `REJECT_AND_REQUEST_NEW_VERSION` | 新建 2542B V2 successor；四 numeric 仍不得仅凭旧 intent 获得 executable authority。 |
| `SAMPLE_AND_WINDOW_DEPENDENCE` | `REJECT_AND_REQUEST_NEW_VERSION` | 保留 30/5/20/0.50；冻结 active episode start/end、left/right censor、transitive merge、跨 slice inclusion 与 single-assignment。 |
| `ACTUAL_PATH_TURNOVER` | `APPROVE_EXACTLY_AS_DRAFTED` | 原样保留 1.00/0.25、one-way fill notional、no half multiplier、no same-session netting。 |
| `LEVERAGE_BETA_ATTRIBUTION` | `APPROVE_EXACTLY_AS_DRAFTED` | 原样保留 0.0200/0.0100、OLS with intercept、252-session floor 与 no-leverage。 |

### 3.2 DQ numeric disposition

`max_quote_age_seconds=120`、`max_relative_spread=0.20`、`min_open_interest=10`、`min_volume=1` 均为 `INSUFFICIENT_EVIDENCE_TO_APPROVE`。这不表示数值已证伪，而是当前 V1 没有逐字段 source、rationale、stage、risk、review/expiry authority。

V2 successor 必须在不读取新 primary-window result 的前提下补齐：

- `120`：quote cadence、`quote_end_utc` 语义、decision clock、latency budget、适用 stage 与 expiry；
- `0.20`：mark uncertainty、option universe、transaction-cost boundary、仅 DATA_RESEARCH 而非 execution liquidity；
- `10`：exact OI source、prior-session availability、revision/backfill policy、仅 derived contributor evidence；
- `1`：decision-as-of cumulative volume、field-specific `available_at`、禁止日终/修订值 lookahead、仅 research evidence presence。

在后续再次 independent review 与 Owner exact approval 前，这些值保持 `NON_EXECUTABLE_PILOT_POLICY_PENDING_REVIEW`。

### 3.3 2542B serial disposition

- `GLOBAL_DQ_TERMINAL_ORDER`：`APPROVE_EXACTLY_AS_DRAFTED`；
- quote timestamp、spread、contract-to-session aggregation、missing/UNKNOWN mapping、exact-source-date/PIT：`REJECT_AND_REQUEST_NEW_VERSION`；
- 2542B V1 overall：`REJECT_AND_REQUEST_NEW_VERSION`，但可作为 non-executable draft substrate 保留。

V2 successor 必须解决：

- requirement/config/code/test 对 missing timestamp 与 single-sided quote 的冲突；
- quote timestamp 与 source date、trusted prior-session、field-specific `available_at`、volume as-of；
- expected contributor manifest、unique contract ID、duplicate/unexpected/missing contributor enforcement；
- excluded-row invalid propagation；
- frozen real authority 与 synthetic authority 类型隔离；
- numeric check order、pre-run hard stop、identity/PIT in-run hard stop、numeric FAIL/UNKNOWN collect-all；
- derived-only run artifacts、abort receipt、replay report 和 checksum catalog。

## 4. 分阶段实施

### C1：完成复核归档与本地 reconciliation

- 既有 Web Pro turn 已自然完成且未重复提交；
- 记录 11 个请求文件的 retrieval 状态、模型/路由证据、八轴 disposition、四项 DQ numeric disposition、六类 serial semantics 和 overall disposition；
- 将 reviewer advisory 与本地 authority 对照；错误路径、漏检文件或事实冲突必须显式记录，不得直接照抄。

验收：已满足；最终 disposition 与最小整改集合在本需求中完整、可追溯，且不把 advisory 当作仓库 authority。

### C2：创建版本化冻结就绪合同

- 旧 V1/V2/2542B V1 保持 immutable；
- 新建 V3 exact sheet 与 2542B V2 DQ/PIT successor；两者绑定 `2021-02-22..2025-12-02`、`expected_session_count=1202` 和 exact session inventory identity；
- `NON_BETA_ACTION_VALUE` 明确 common-sample floor 及其 PASS/FAIL/INSUFFICIENT/INVALID 机械语义；
- 新版本提供明确的 typed loader、canonical replay、predecessor/DQ binding 和按版本命名的测试入口；
- 四项 DQ numeric 逐项记录 owner、pilot rationale、intended effect、known risk、一次 primary-window review/expiry condition；在再次复核和 Owner exact approval 前保持 non-executable，不得仅通过改状态字段获得 PASS；
- 2542B V2 同时冻结 contributor manifest、field as-of、typed authority、run artifact 和 stop/recovery semantics，但仍不授权真实 DQ。

验收：任何缺失、重复、越界、hash drift、样本缩窗、错误版本 loader、DQ authority 越权或 terminal drift 均 fail closed。

### C3：验证与冻结边界

- focused tests 使用 `python -m pytest -n 16 --dist loadfile ...`；
- formal validation 使用 `python scripts/run_validation_tier.py <suite> --write-runtime-artifact`；
- 根据影响至少运行 Architecture、Contract、Integration、Reproducibility，并在最终树运行 Full；
- validation PASS 只证明工程合同，不等于 DQ、策略价值或投资结论 PASS；
- 未达到完整 Owner/independent review authority 时，新版本保持 unfrozen，真实 DQ run 仍关闭。

验收：最终树验证通过，报告实际 requested/evaluated scope 和 terminal boundary；任何真实 DQ 或 empirical successor 均由后续单独、范围固定的 R1 manifest 触发。

## 5. 依赖与顺序

1. 既有 Web Pro turn 完成；
2. C1 reconciliation；
3. `SINGLE_LANE` START/LANE preflight；
4. C2 版本化实现；
5. focused + formal validation；
6. 任务状态与生成视图同步；
7. task commit、local-main fast-forward、fetch、CLOSEOUT、ordinary non-force push、SHA 等值验证与分支清理。

## 6. 开放问题与 fail-closed 条件

- 最终 reviewer 若未逐项给出八轴 disposition，冻结保持阻塞；
- 四项 DQ numeric 任一为 `INSUFFICIENT_EVIDENCE_TO_APPROVE` 时，不得建立 executable DQ authority；
- 2542B serial semantics 即使通过，也不能替代 numeric authority；
- 任何样本 inventory 无法与 1202-session hash 一致时为 `INVALID`，不得静默 intersection 后继续；
- identity/PIT `INVALID` 在真实 successor run 中必须 hard stop；numeric `FAIL`/`UNKNOWN` 记录后继续完成固定 inventory，以保留完整审计证据；
- 本任务不授权真实 DQ、策略评估或交易动作。

## 7. 进度记录

- 2026-08-23：登记任务；Web Pro exact-commit review 自然完成，overall=`REQUEST_NEW_VERSION_BEFORE_FREEZE`。四轴原样保留、四轴新版本修订；四个 DQ numeric 均 `INSUFFICIENT_EVIDENCE_TO_APPROVE`；2542B V1 作为 non-executable substrate 保留，新建 V3 sheet 与 DQ/PIT V2 successor 后再独立复核，不运行真实 DQ。
- 2026-08-23：完成 V3 exact sheet 与 DQ/PIT V2 successor 工程草案。V3 file/canonical SHA-256 分别为 `f563f6499c86853c791589e40cf9d1dbac04b53b0728310e3b0e08376653a3d9` / `7b7a0d19d04f52de2de4dc813cc29de4dc62e0a624e93cc486256b3071a2d8bd`；DQ/PIT V2 分别为 `762f00395963ea32033334ff4ea1d26231bd1b0d0b9f22a05bd505dd572276e6` / `c9f762d4419aa7a8d9be77cce83d32026735bc61d1589b699f4e7db34489c672`。Ruff、strict mypy 与 37 项并行聚焦测试通过；正式兼容与 final-tree validation 待执行。当前 terminal 仍为 `NEW_VERSION_DRAFT_COMPLETE_PENDING_SECOND_REVIEW_AND_OWNER_APPROVAL`，真实 DQ/provider/cache/backtest 继续关闭。
- 2026-08-23：旧 V1/V2、2542B V1 与 successor 联合并行回归 `122 passed`。frozen-lane Architecture 门禁为 `861 passed / 4 failed`：两项是 `HEAD=1ca8...` 落后 `origin/main=4eef...` 的 carrier ancestry fail-closed，一项是 canonical task count 从 1018 增至 1019 后 frozen-base 测试常量待 final tree 刷新，一项是新 source/test 引起 ownership manifest freshness fail。该结果按 DEVX-006 保留为 base-drift evidence，不在旧 base 伪造 formal PASS；所有 formal tiers 将绑定唯一 latest-main integration candidate。
- 2026-08-23：第一版 final candidate 的 Architecture/Contract/Integration/Reproducibility 分别为 `865/276/995/24 passed`；Full 保留在 `outputs/validation_runtime/full_20260823T135255Z/test_runtime_summary.json`，结果为 `9462 passed / 21 failed / 3 skipped`。失败来自 main 再次前进后的 carrier binding、Atlas successor 分类、compatibility/report-flow current authority 与 generated freshness，不是 V3/DQ V2 策略机械断言失败。后续 Full 必须以该 artifact 为 parent，使用 `failure_fix_rerun`，不得覆盖或伪装第一次失败。
- 2026-08-24：main 连续前进后，在 frozen lane `bb56f868...` 对最新 `main=58b8c681...` 生成并复核第三轮有效 reconciliation 计划 `integration-revalidation-3896dbcd4bedda8be9a9`，decision=`RECONCILIATION_REQUIRED`，INTEGRATION preflight=`PASS`。唯一 latest-main candidate 从 `58b8c681...` 重建；保留先到达 main 的 `TRADING-2544` task/requirement，并把其 Atlas 状态显式分类为 `PROPOSED -> NOT_DUE`，不改变当前 `TRADING-2542C` 主线或授权边界。
- 2026-08-24：新增 append-only compatibility successor section `phase_trading_2542c_growth_action_value_independent_review_remediation_and_freeze_readiness_v1`，把当前 task、V3/DQ V2、Atlas、`docs/system_flow.md`、task registry、report-flow、generated 与 tests 的 live hashes 绑定到新 section；DEVX-006D 历史 fragment 保持 `3000` entries 不改写，当前 report-flow authority 独立更新为 `3004` entries。Atlas focused=`41 passed`；正式 final-tree validation 仍待 exact candidate commit 后执行。
- 2026-08-24：工程候选的 reconciliation、版本化合同、current authority 与 focused gate 已就绪；任务状态收敛为 `BLOCKED_OWNER_INPUT`。Codex 继续完成 exact candidate commit 与 parent-bound 正式验证，但 V3/DQ V2 仍保持 unfrozen；解除冻结仍需要第二次独立复核、四项 DQ numeric authority 处置和 Owner exact approval。
- 2026-08-24：首次状态收敛事件的 `notes` 未重复 supporting-requirement 短语，canonical compatibility projection 因而把 `requirement_refs` 清空，Atlas 聚焦回归按设计 fail closed（`25 failed / 104 passed`）。已用追加式 task event 恢复 exact requirement binding；未手改 fragment、生成视图或 Atlas 运行时来绕过该错误。
- 2026-08-24：首轮 final-tree Architecture 正式门禁保留在 `outputs/validation_runtime/architecture-fitness_20260823T153447Z/test_runtime_summary.json`，结果为 `805 passed / 61 failed`。61 项均来自 `tests/test_arch_004_refactor_policy.py` 的单一陈旧全局常量：append-only authority 已新增 2542C successor，但 `LATEST_COMPATIBILITY_SECTION` 仍指向 DEVX-007。该修复仅更新 current-authority consumer 并重建其绑定；不改写任何历史 prefix/hash，不改变 V3/DQ V2 数值或执行边界。
- 临时集成证据生命周期：owner=`TRADING-2542C`，purpose=保存 frozen-base/lane-head/latest-main 的 `change_manifest.v1` 与 successive `integration_revalidation_plan.v1`；当前 exact allowlist 为 `D:\Work\AITradingSystem\outputs\architecture\trading_2542c_change_manifest.v1.json`、`trading_2542c_integration_revalidation_plan.v1.json`、`trading_2542c_integration_revalidation_plan_v2.v1.json`、`trading_2542c_integration_revalidation_plan_v3.v1.json`、`trading_2542c_integration_revalidation_plan_v4.v1.json`。final candidate 通过正式门禁并完成 closeout 后删除；若计划为 `BLOCKED`、需要 serial contract wave 或仍是唯一失败证据，则保留到阻塞解决且在任务记录中继续登记。
