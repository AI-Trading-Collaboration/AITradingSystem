# TRADING-2557：First-layer composer v2 基础可证伪运行失败修复 V1

最后更新：2026-09-03

稳定任务 ID：`TRADING-2557_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_FAILURE_FIX_V1`

Owner 指令：`owner_instruction:TRADING-2557:2026-09-03:exact_schema_only_failure_fix_1_1_1_1`

授权状态：`EXACT_PREAUTHORIZED`

状态：`VALIDATING`

## 1. 目标与解释边界

本任务承接 TRADING-2556 的 terminal `INVALID`，在独立、版本化的 V2 路径中修复唯一已定位的
schema adaptation 缺陷：V1 把 bootstrap 审计字段 `replicates` 和 `random_seed` 一并传给严格的
`BootstrapInterval` reducer model，而该 model 只接受五个统计字段。

本任务只允许显式投影以下 reducer 字段：`block_length_sessions`、`percentile_2_5`、
`percentile_50`、`percentile_97_5`、`probability_excess_less_than_or_equal_to_zero`。审计输出仍必须保留
`replicates=10000` 与 `random_seed=2555`。不得改变窗口、输入、信号、lag、成本、comparator、现金
处理、bootstrap draws、诊断定义或 reducer。V1 模块、authorization、manifest 与 output 保持不可变。

任何 V2 结果仍是 reused-development-window research evidence，不是 pristine out-of-sample 证据；
无论结果为何，均不授权 Wave C、paper/live、production、broker、orders、fills 或 positions。

## 2. 冻结权威与失败绑定

- exact base/main：`0925dda66cb74c8ceaaf47a39a6540859ba5ae6e`；
- TRADING-2556 V1 implementation commit：`0c57f57ce743528e23083551aef3ab7eab51ca70`；
- V1 execution manifest：
  `inputs/research/first_layer_composer_v2_foundational_falsification_v1/execution_manifest.json`；
- V1 terminal result/failure SHA-256：
  `1f0de3193b4807ed091636d4808847e9529679085d207690b2b588a8d6baaebc`；
- V1 result admission：
  `config/research/first_layer_composer_v2_foundational_falsification_result_admission_v1.yaml`；
- V1 canonical DQ：`PASS`，requested/evaluated window 均为
  `2021-02-22..2025-12-02`，0 error / 0 warning；
- V1 independent accounting replay：`PASS`，candidate/comparator final-value diff 分别为
  `1.4551915228366852e-10` 与 `1.1641532182693481e-10`，容差 `1e-8`。

V2 authorization、manifest 和 result admission 必须同时绑定上述 V1 failure/result admission 身份。
V2 不是 V1 同-manifest重跑；必须使用新模块路径、新 authorization、新 manifest 与新 write-once output。

## 3. 冻结研究合同

- 主窗口：XNYS `2021-02-22..2025-12-02`，1202 个信号 session、1201 个收益 interval；
- 信号 lag：1 session；`constructive/risk_on -> QQQ weight 1`，其余状态为 0；
- 主 comparator：`long_interval_count / 1201` 的静态 QQQ 权重，其余为零收益现金；
- 主成本：candidate 与 comparator 同为单边 5 bps；成本敏感性仍为 5/10/15/20 bps；
- bootstrap：paired circular moving-block，block length 21/63，seed 2555，每个长度 10,000 次；
- 年度、episode、leave-one-year-out、SGOV carry、状态迁移、policy consumption、source revision、
  reconciliation 和 F0 reducer 定义全部复用 TRADING-2556 requirement，不得重估。

主路径必须先执行 canonical data-quality gate 并显式停止于非 PASS。请求和实际评估日期必须同时写入
terminal artifact；不得以历史 DQ 文字代替本次运行的可重放 gate evidence。

## 4. 有界运行与安全边界

本任务最多允许：

- V2 manifest replay：1；
- canonical DQ：1；
- local schema-corrected foundational run：1；
- independent replay：1；
- data download、cache mutation、QuantConnect、option/provider action、orders、fills、positions：0。

输出只允许 aggregate diagnostics、输入身份、授权计数、DQ/PIT/窗口和安全边界；不得输出逐日价格、
逐日信号或账户/交易原始 payload。`production_effect=none`、`broker_action=none`。

## 5. 实施与验证阶段

1. 登记本任务和 requirement，完成 `SINGLE_LANE` preflight；
2. 新建 V2 module/test，不修改 V1 模块；用 golden/negative tests 证明只过滤两个 audit-only 字段；
3. 提交实现身份后生成 exact V2 authorization/manifest，并在 dispatch 前自动 replay；
4. 运行一次 canonical DQ、一次 V2 aggregate diagnostics 和一次独立重放；
5. 写入 V2 result admission，更新 task/Atlas/system flow；
6. 刷新 canonical task、architecture、Atlas、report-flow 和 compatibility authority；
7. 完成 focused、Architecture、Contract、Integration、Reproducibility、Full、local-main fast-forward、
   ordinary push、SHA 复核和 governed cleanup。

## 6. 验收标准

- V1 所有实现与运行证据 byte-identical，且 V2 显式绑定 V1 terminal invalid admission；
- V2 只有 schema projection 差异，五个 reducer 字段显式 allowlist，两个审计字段仍保留在诊断输出；
- 新 authorization/manifest/output 独立版本化且 write-once，同 manifest 不得重复运行；
- canonical DQ PASS 并披露 requested/evaluated `2021-02-22..2025-12-02`；
- independent replay 在 `1e-8` 内对账，全部必需 aggregate diagnostics 完整；
- reducer 只按冻结 F0 规则给出 `INVALID/FAIL/INSUFFICIENT/PASS`，不接受手工覆盖；
- 结论明确披露 reused-development-window、partial-year、非 pristine-OOS 和无部署授权；
- 所有外部、缓存、生产、broker 与交易计数保持 0；正式验证与默认发布门禁 PASS。

## 7. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/trading-2557-foundational-falsification-f1-fix`；
- 复用当前 checkout，不创建额外 worktree/clone；
- runtime output：
  `outputs/research/first_layer_composer_v2_foundational_falsification_failure_fix_v1/`；
- output 在 admission 和正式验证完成前不得覆盖或删除；唯一 V2 attempt 消耗后不得重跑；
- 分支只在 main/remote 已含同一候选、证据完整、进程无依赖且 audit 无独有内容后清理。

## 8. 进度记录

- 2026-09-03：TRADING-2556 已以 terminal `INVALID` 发布；该状态只说明结果组装失败，不说明策略有效或
  无效。Owner 随后明确精确授权本任务：只修正传给 `BootstrapInterval` 的字段过滤；允许 manifest replay、
  canonical DQ、本地 bounded run 和 independent replay 各 1 次；窗口、信号、模型、阈值、成本、bootstrap
  draws 与 reducer 均不得改变。授权状态据此为 `EXACT_PREAUTHORIZED`；数据下载、cache mutation、
  QuantConnect、期权回测、provider 及任何交易行为均未获授权。
- 2026-09-03：先前登记的 `STANDING_OWNER_SCOPE` 属于授权归类错误；canonical task source 通过追加事件
  纠正为 `EXACT_PREAUTHORIZED`，不改写既有事件历史。
- 2026-09-03：只读代码审查确认唯一修复点位于 V1 `FoundationalDiagnosticSummary` 构造前的
  `BootstrapInterval.model_validate(row)`；正确修复是 V2 显式字段投影，不是放宽 strict schema，也不是
  修改 bootstrap 输出、统计参数或 V1 历史文件。
- 2026-09-03：schema-only V2 executor 已绑定实现 commit
  `a1b345b14bfc7bbf5f2f3068613c4405df37ed68`；V1 module SHA-256 仍为
  `8feb16c9328eac48c8751b2a664d21b0bcb495889653e8db352f28836444730f`。唯一受权运行已消费完毕，
  manifest replay/canonical DQ/local bounded run/independent replay 为 `1/1/1/1`，其余计数均为 0。
- 2026-09-03：canonical DQ 为 PASS（0 error/0 warning），requested/evaluated range 均为
  `2021-02-22..2025-12-02`；独立重放 PASS，candidate/comparator final-value diff 分别为
  `1.4551915228366852e-10` 与 `1.1641532182693481e-10`，小于 `1e-8`。
- 2026-09-03：冻结 reducer 输出 `INSUFFICIENT / HOLD`。5 bps 下 candidate 净收益为
  `45.279358871873754%`，exposure-matched comparator 为 `31.533381915138126%`，paired excess 为
  `+13.745976956735628` 个百分点；但 21/63-session bootstrap 的 2.5% 下界分别为
  `-34.943648104514985` 与 `-31.97461050658123` 个百分点，且排除 2022 后 excess 为
  `-5.048443883389609` 个百分点，触发两个预注册稳健性门槛。该结果不支持推进 options Wave B/C，
  不构成 pristine OOS 或生产资格；若继续，只能另行预注册新的独立证据阶段，不能事后调整阈值救援。
