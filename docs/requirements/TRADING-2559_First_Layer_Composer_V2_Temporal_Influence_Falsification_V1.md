# TRADING-2559：First-layer composer v2 时间位移与 episode 影响度证伪 V1

最后更新：2026-09-03

稳定任务 ID：`TRADING-2559_FIRST_LAYER_COMPOSER_V2_TEMPORAL_INFLUENCE_FALSIFICATION_V1`

Owner 指令：2026-09-03，在看到 matched-placebo 结果与后续低成本验证建议后要求“好的，你再验证下”。

授权状态：`STANDING_OWNER_SCOPE`

Failure-fix 授权状态：`EXACT_PREAUTHORIZED`

状态：`DONE`

## 1. 目标与解释边界

TRADING-2557 的冻结结果为 `INSUFFICIENT / HOLD`；TRADING-2558 进一步得到
`TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO`，单侧 randomization
`p=0.20007999200079993`。已知 retained evidence 还显示收益集中在 2022 防御期及少数
LONG episode，因此本任务不重复年度归因或 matched-placebo，也不增加 draws。

本任务只回答两个更窄的问题：

1. 在原信号、模型、阈值、窗口、成本和 comparator 逻辑完全冻结时，将既有可执行 exposure
   相对收益 interval 前后平移，真实对齐是否表现出延迟脆弱或非因果提前对齐占优；
2. 逐一删除 41 个既有 LONG episode，并按剩余 LONG interval 重新 exposure-match 静态
   QQQ/cash comparator 后，正超额是否依赖单个 episode。

本任务是 reused-development-window diagnostic，不是 pristine OOS，不重新训练、不搜索参数，
不改变 TRADING-2557/2558 的结果、reducer 或优先级。任何正面结果最多只能表示本轮低成本证伪
未击穿；它不能覆盖 matched-placebo `p=0.20`，不能提高 prospective OOS、Options 或生产优先级。
负面结果可进一步降低候选研究优先级。

## 2. 冻结输入与可见性

- prior visibility：`PARTIAL_PRIOR_VISIBILITY`；已知全窗收益、年度/episode attribution、
  bootstrap、leave-one-year-out、成本敏感性和 matched-placebo 结果；
- 尚未计算：本文定义的九个 temporal displacement 结果和 41 个 rematched
  leave-one-episode-out 结果；
- 主研究窗口及完整影响度窗口：XNYS `2021-02-22..2025-12-02`，1202 个 signal session、
  1201 个 return interval；
- 冻结既有可执行 exposure：one-session lag 后 385 个 LONG interval、41 个 LONG episode；
- 主成本：单向 traded notional `5 bps`；现金收益为 0；
- comparator：对每条被检验 exposure path 使用其 LONG interval 占比构造静态
  exposure-matched QQQ/zero-return-cash comparator；
- exact-bind：TRADING-2558 preregistration、authorization、manifest、aggregate result、
  canonical DQ identity、TRADING-2557 terminal aggregate、operational predictions、canonical
  QQQ adjusted-close bytes，以及本任务实现、authorization 和 manifest。

任一 hash、session、DQ/PIT、lag、状态映射、价格、成本、accounting 或输入身份漂移均为
`INVALID`，不得进入诊断计算。

## 3. Temporal displacement 算法

1. 以 TRADING-2558 已复核的 1201 个二元可执行 exposure interval 为 `target[i]`；`shift=0`
   表示原始可执行对齐。
2. shift 集合一次冻结为 `[-10, -5, -2, -1, 0, 1, 2, 5, 10]`，不得见结果增删：
   - `shift < 0`：非因果 anticipatory control；
   - `shift > 0`：在原 one-session lag 基础上的额外执行延迟；
   - 对共同原始 interval 索引 `i=10..1190`，使用 `target[i-shift]`，因此九条路径都在
     完全相同的 1181 个 QQQ return interval 上比较。
3. 每条 shift path 单独按其 LONG interval 比例重建静态 exposure-matched comparator，
   candidate 和 comparator 均使用相同 5 bps 成本函数。
4. 报告每条路径的 LONG count、trade-event count、candidate/comparator net return、paired
   excess、max drawdown，以及 deterministic best shift；完全相等时选择数值最小的 shift；不得
   基于结果新增 shift 或指标。
5. `shift=+1` 是主执行脆弱性检查；anticipatory set 的最大 excess 与 `shift=0` 比较仅作为
   反应型/对齐诊断，不作为可交易结果。

## 4. Leave-one-episode influence 算法

1. 在完整 1201 interval exposure path 上按既有连续定义提取恰好 41 个 LONG episode。
2. 每次只把一个 episode 的所有 target 置 0，其他 interval 不变；共形成 41 条确定性路径。
3. 对每条删除路径，按剩余 LONG interval 比例重新构造静态 exposure-matched comparator，
   candidate/comparator 均重新应用相同 5 bps 成本。
4. 报告每个 episode 的日期、长度、剩余 LONG count、rematched paired excess，以及相对原始
   `+13.745976956735628pp` 的变化；主集中度检查为是否存在任一删除后 paired excess `<= 0`。
5. 原始路径必须与 TRADING-2558 的 candidate、comparator、paired excess 和 exposure accounting
   在 `1e-10` 内复算一致，否则 `INVALID`。

## 5. 临时 pilot reducer

以下门槛均在读取新结果前冻结，只影响研究解释：

1. `INVALID`：输入、DQ/PIT、身份、完整路径对账、shift common-window、episode inventory、成本、
   manifest、运行计数或 independent replay 任一失败；
2. `SINGLE_EPISODE_DEPENDENT`：至少一个 rematched leave-one-episode paired excess `<= 0`；
3. `ONE_SESSION_DELAY_FRAGILE`：未触发上项，但 `shift=+1` paired excess `<= 0`；
4. `ANTICIPATORY_ALIGNMENT_DOMINATES`：未触发前两项，且九个 shift 的最大 paired excess 在
   `shift < 0`，并以 `1e-12` 以上严格超过 `shift=0`；
5. `LOW_COST_ROBUSTNESS_NOT_DISCONFIRMED_DIAGNOSTIC_ONLY`：以上均未触发。

同时输出所有适用 reason codes，precedence 只决定主 reducer。零值是“正超额是否被击穿”的自然
边界；`1e-12` 仅用于浮点相等判断，不是投资阈值。无论 reducer 为何，TRADING-2557 继续
`INSUFFICIENT/HOLD`，TRADING-2558 继续 `TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO`。

## 6. 有界运行与安全边界

本任务最多允许：

- execution manifest replay：1；
- canonical DQ：1；
- local temporal/influence run：1；
- independent deterministic replay：1；
- data download、cache mutation、external provider、QuantConnect、option backtest、paper/live、
  production、broker、orders、fills、positions：0。

仅允许读取已经存在并由 manifest/hash 绑定的本地输入。输出 aggregate-only，不保存逐日价格、
逐日信号、shift 后逐日 target 或账户/交易 payload。`production_effect=none`、
`broker_action=none`。

## 7. 实施与验收

1. 登记任务并通过 `SINGLE_LANE` preflight；
2. 写入 versioned preregistration、strict loader、deterministic implementation 和
   golden/negative tests；
3. 在读取新结果前提交实现 identity，随后生成 exact authorization/manifest；
4. dispatch 前自动 replay manifest，执行一次 canonical DQ、一次本地诊断和一次独立重放；
5. independent replay 必须从冻结输入重新计算全部九个 shift 和 41 个 episode，所有数值在
   `1e-10` 内一致；
6. actual counters 必须为 `1/1/1/1`，所有禁止动作计数为 0；
7. 报告 requested/full evaluated range 与 temporal common evaluated range；
8. 更新 result admission、canonical task、system flow 和适用生成权威；完成 focused、
   Architecture、Contract、Integration、Reproducibility、Full、local-main ff-only、普通 push、
   SHA 复核与 governed cleanup。

## 8. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/trading-2559-temporal-influence-falsification`；
- exact frozen base：`eab7971d3a41f4802f110200d70620df443341be`；
- 复用当前 checkout，不创建额外 worktree/clone；
- failed immutable runtime output：`outputs/research/first_layer_composer_v2_temporal_influence_v1/`；
- admitted failure-fix runtime output：
  `outputs/research/first_layer_composer_v2_temporal_influence_failure_fix_v1/`；
- output write-once，额度消费后不得重跑或覆盖；
- 分支仅在 main/origin 包含同一候选、证据完整、无进程依赖且审计无独有内容后清理。

## 9. 进度记录

- 2026-09-03：Owner 在看到上一轮 matched-placebo 与低成本后续建议后要求继续验证。本任务采用
  `STANDING_OWNER_SCOPE`，只允许本文固定的本地 R1 bounded research；收益集中度的既有年度结果
  只作冻结上下文，不重复运行。尚未计算本文九个 shift 或 41 个 leave-one-episode 结果。
- 2026-09-03：未见结果实现 identity 已冻结为 commit
  `e297242f56ad0b8077eabeec89e6c61a3e1c125d`、module SHA-256
  `265a4b2cbf487c70ebfdc94a274b143e5d1b58d5c8ade641d1511104521478ff`；合成 focused
  parallel pytest `13 passed`，Ruff PASS。Authorization 与 manifest 已绑定 23 个既有本地输入，
  正式 dispatch 前所有计数为 0。
- 2026-09-03：唯一正式 attempt 在 manifest replay、canonical DQ 与 primary diagnostic 之后
  fail closed，`technical_validation_state=INVALID`、reason=`TIF_UNEXPECTED_FAILURE`。原因是对账代码
  读取不存在的顶层 `observed_max_drawdown_magnitude_pct`；TRADING-2558 的权威字段实际位于
  `matched_placebo.observed_max_drawdown_magnitude_pct`。失败 evidence 为
  `outputs/research/first_layer_composer_v2_temporal_influence_v1/aggregate_result.json`，SHA-256
  `4d4516737551ed7328d2925482c39c09ed299e1db7da46220d8541f87e2f6ef0`。实际计数为
  manifest/DQ/local/independent=`1/1/1/0`，所有禁止动作计数为 0；未形成 temporal 或 episode
  结论。最佳修复仅是把该对账字段投影到正确的既有嵌套路径，不改变本文任何窗口、shift、episode、
  模型、阈值、成本、comparator、reducer 或输入。原 write-once 上限已消费，须由 Owner 明确授权
  一次 failure-fix manifest replay、canonical DQ、local run 与 independent replay 后才能另建
  immutable 输出目录重跑。
- 2026-09-03：Owner 回复“批准”，接受上一条所列精确 failure-fix 范围。v2 只允许：把最大回撤
  对账字段投影改为 `matched_placebo.observed_max_drawdown_magnitude_pct`；补充实际 TRADING-2558
  schema 回归测试；为新 authorization/manifest 提供严格路径绑定；在
  `outputs/research/first_layer_composer_v2_temporal_influence_failure_fix_v1/` 执行新的
  manifest/DQ/local/independent=`1/1/1/1`。窗口、九个 shift、41-episode 算法、信号、模型、
  阈值、成本、comparator、reducer 与输入保持不变，所有禁止动作仍为 0。authorization_state
  必须为 `EXACT_PREAUTHORIZED`，原失败输出与 v1 authorization/manifest 保持 immutable。
- 2026-09-03：failure-fix 已在新 immutable output 以精确 `1/1/1/1` 完成；canonical DQ 为 PASS
  （0 error / 0 warning），independent replay 最大差异为 `6.821210263296962e-13`，小于
  `1e-10` tolerance，所有下载、cache mutation、provider、QuantConnect、option backtest、order、
  fill、position 计数为 0。完整窗口原始 paired excess 仍复算为
  `+13.745976956735628pp`；统一 1181-interval temporal window 的 `shift=0/+1/+2` 分别为
  `+10.410605362323075/+5.537673124193127/-1.6675372437030163pp`。非因果 `shift=-2`
  为 `+142.10847322324653pp` 并占优，但只作为 temporal-alignment 反证，不能解释为可交易收益。
  删除 episode 12（`2023-03-16..2023-08-10`，101 intervals）后 rematched paired excess 从
  `+13.745976956735628pp` 降至 `-2.56913050870304pp`，下降 `16.31510746543867pp`；冻结
  reducer 因此为 `SINGLE_EPISODE_DEPENDENT`，并附 reason
  `ANTICIPATORY_ALIGNMENT_DOMINATES`。结果 aggregate SHA-256 为
  `db8f2b9ffd33b08132c8c3798248351883cffdc39bedacb12378d45b77ecaea9`。该结果进一步削弱而非
  增强当前有效性判断：2557 仍为 `INSUFFICIENT/HOLD`，2558 仍为
  `TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO`，不提高 prospective OOS、Options 或生产优先级。
- 2026-09-03：v4 candidate staging 时，一个手工列出的 compatibility fragment path 含哈希字符
  转录错误，`git add` 因 pathspec 不存在而失败；紧随其后的 `git commit` 自动输出了未过滤的工作区
  摘要，因此仅暴露已登记 `known_unrelated_exclusions` 文件的路径名。这记为 checkout inspection audit
  incident：未读取、hash、diff、复制、暂存或修改该排除文件的内容，且没有 commit 形成。v4 transaction
  终止为失败；补救为重新使用 `worktree-audit`、仅对明确 task-owned generated roots 执行 staging，并因本
  requirement 字节变化从新的 exact source commit 重跑全部五个官方 generator。
- 2026-09-04：TRADING-2559 v5 candidate `93b09ccd128bad59b08abd19dd8ddf2d83d8b8f2`
  已完成 focused `82 passed` 并停在 `FORMAL_VALIDATION_PRE` 时，同一 repository 的 OPS-078
  先行占用唯一 Full/共享 publication 资源；其验证与普通 push 使 local/remote `main` 从冻结 base
  前进。v5 的研究 evidence、aggregate SHA-256 与精确 `1/1/1/1` counters 保持 immutable，不重跑研究，
  但旧 transaction 的 expected-main 已 stale，不能继续发布。按 governed base-drift 流程，将生成
  `integration_revalidation_plan.v1`，在最新稳定 `main` 上创建唯一 coordinator candidate，并只重建
  coordinator-refreshable task/Atlas/report/compatibility authority 后重新跑正式验证。若需要隔离 checkout，
  临时 worktree 固定为 `D:\Work\AITradingSystem_trading2559_integration`，owner 为 TRADING-2559
  integration coordinator，purpose 为 latest-main final candidate；exit condition 为 candidate 已进入
  `main`/`origin/main`、所有 canonical evidence hash 复核、无运行进程依赖且 tracked/untracked/ignored
  audit 无唯一内容后用 `git worktree remove` 清理。production、broker、provider、download、cache mutation、
  QuantConnect、option backtest、orders、fills、positions 仍全部为 0。
- 2026-09-04：base-drift publication v6 在 exact latest-main `3048a2178a383c7f240cb1e9c8aafa53a796913c`
  上重放四个原始 canonical task events，并完成 task-owned source、Atlas source 与 `docs/system_flow.md`
  的 reviewed reconciliation；`integration-revalidation-400b2424fe550dda5ca4` 要求人工复核的 overlap
  均选择“保留 latest-main 业务字节、加入 TRADING-2559 task source、随后重新生成共享 authority”。在真正运行
  generator 前复核发现 Atlas 只接受已提交的 exact-source commit；v6 的 source bytes 尚未提交，因此不冒充
  source identity，不运行 Atlas/generator/正式验证，将 v6 以 failed 关闭。下一 v7 从包含这些 source bytes 的
  clean commit 重新 acquisition，并从 `canonical-task-source` 起完整重放五个 generator。研究运行与 counters
  不变，未新增 DQ、manifest replay、local run 或 independent replay。
- 2026-09-04：v7 已在 source commit `ec3b82bcdd361030e30e9de87ab34ae998b4ae00` 上完成五个
  generator 及其校验，但随后 focused parallel pytest 得到 `303 passed / 6 failed`。六个失败均为把
  TRADING-2559 叠加到 latest-main 后的静态 authority 基线未同步：canonical task count 应为 `1059`，
  report-catalog-flow entry count 应为 `3140`，`docs/system_flow.md` 应为 SHA-256
  `da95f82971243115b2adc34a5acf918677c939d268b9854f1c07c63447c09368` / `1207` entries，
  deprecation inventory 应为 `arch_004g_deprecation_inventory_c4bad4dec03beef1e00c`、modules/tests
  `1193/1351`。这不是研究逻辑或 evidence 失败；只允许机械更新这些派生断言并从新 transaction 重建
  共享 authority。v7 在任何 candidate commit、正式验证或 Full 前以 failed 关闭；研究运行与
  `1/1/1/1` counters 保持不变，所有禁止动作仍为 0。
- 2026-09-04：v8 在机械修正上述 authority 基线后完成五个 generator、Ruff、focused parallel
  pytest `309 passed`，并形成 clean candidate `71d6134ad7ed109647d8d41b6373d338d9773ebb`；但在
  `FORMAL_VALIDATION_PRE` 之前，已占用唯一 Full 资源的 OPS-078 完成 `10195 passed / 6 skipped`
  并把 local/remote main 推进到 `73a1015ccc7567d2c88f15ba953c136cac12a890`。因此 v8 不在旧
  expected-main 上继续，按 failed 释放；新一轮 drift 以 `3048a2178a383c7f240cb1e9c8aafa53a796913c`
  为 frozen base、`71d6134ad7ed109647d8d41b6373d338d9773ebb`（加本记录后的 exact lane head）为 lane、
  `73a1015ccc7567d2c88f15ba953c136cac12a890` 为 latest main，重新生成独立 v2 manifest/plan。
  只允许重整合、共享 authority 重建和正式验证；不重跑研究，`1/1/1/1` 与所有禁止项计数不变。
- 2026-09-04：v9 在 latest-main coordinator tree 上通过 canonical writer 精确重放四个既有 task
  events；四个 event id 与 fragment checksum 均未变化，task-source validate PASS（1059 tasks，
  518 active，541 completed）。由于 Atlas generator 必须绑定包含最新 task index/view 的已提交 exact
  source，v9 在 generator 前以 failed 关闭；先提交 task-source 生成视图，再由 v10 从该 clean source
  commit 完整运行五个 generator。未运行研究、DQ、provider 或任何交易动作。
- 2026-09-04：v10 final candidate `68e7e86c3fc16934b86beef815abee226c77fcb4` 已通过
  Architecture `885 passed`、Contract `278 passed`、Integration `995 passed` 和
  Reproducibility `24 passed`，但首次 Full 在 `10194 passed / 5 skipped / 19 failed` 后 fail
  closed。父级失败证据为
  `outputs/validation_runtime/full_20260903T205706Z/test_runtime_summary.json`，SHA-256
  `6608dbb37bf7250fb920905b0296857897e9c476c6a1f781bb5914c49297cafc`。其中 17 个失败源于
  coordinator 隔离 worktree 未带入测试已由配置/hash 固定引用的本地 ignored retained evidence；另
  1 个失败是 Atlas task coverage 随 TRADING-2559 从 88 增至 89 后静态断言未更新，1 个失败是
  ignored Atlas manifest 仍绑定 generator source commit 而非生成权威提交后的 final HEAD。均不涉及
  研究算法、数据内容或 TRADING-2559 的 `1/1/1/1` 运行结果。
- 2026-09-04：受限 Full failure-fix 只允许：（1）从原 checkout 按既有 SHA 复制 Full 测试实际引用
  的 1,215 个 retained evidence 文件到 coordinator worktree，不覆盖任何已有文件；（2）把 Atlas 静态
  task coverage 断言机械更新为 89；（3）从新的 exact source commit 重建五个共享 authority，并在
  final candidate commit 后重新渲染 ignored Atlas output 以绑定 exact HEAD；（4）以
  `failure_fix_rerun` 和上述父级 Full artifact 执行一次正式重验。已复制文件包括 O1 DQ gate
  `ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`、由 receipt
  `7cb8807c5938be5453e49c392e3173aca38e10643c643c28b335914196eda494` 固定且逐文件复核无差异的
  1,205-file QQQ options signal package，以及 foundational
  与 matched-placebo admission 已冻结 evidence bindings；目标 bytes 均与原 checkout SHA-256
  相同。该 hydration 不执行 O1/Options/研究、不下载数据、不改 cache；所有 provider、QuantConnect、
  option backtest、order、fill、position 动作继续为 0，TRADING-2559 immutable research output 与
  `1/1/1/1` counters 不变。
