# TRADING-2558：First-layer composer v2 matched-placebo 时点证伪 V1

最后更新：2026-09-03

稳定任务 ID：`TRADING-2558_FIRST_LAYER_COMPOSER_V2_MATCHED_PLACEBO_FALSIFICATION_V1`

Owner 指令：2026-09-03，在已完成低成本 retained-result review 后“继续推进验证”。

授权状态：`STANDING_OWNER_SCOPE`

状态：`DONE`

## 1. 目标与解释边界

TRADING-2557 的冻结结果为 `INSUFFICIENT / HOLD`：主窗口 `2021-02-22..2025-12-02`
内，5 bps exposure-matched excess 为 `+13.745976956735628` 个百分点，但 21/63-session
moving-block bootstrap 下界跨 0，且 leave-2022-out excess 为负。低成本 retained-result
review 进一步显示收益集中在 2022 防御和少数 LONG episode。

本任务只检验一个更窄的问题：在保持真实策略 LONG/FLAT 暴露总量、LONG episode 长度多重集、
内部 FLAT gap 长度多重集、边界状态、换手次数和交易成本完全相同时，真实持仓时点是否优于随机
排列的同形态 placebo 时点。

本任务不重新训练模型，不改变五状态信号、二元映射、窗口、lag、成本、comparator、现金处理、
TRADING-2557 reducer 或任何既有结果。它不是 pristine OOS，也不授权 Options Wave B/C、paper/live、
production、broker、orders、fills 或 positions。

## 2. 非盲边界与冻结输入

- prior visibility：`PARTIAL_PRIOR_VISIBILITY`；TRADING-2557 的真实策略收益、年度/episode 归因、
  bootstrap、leave-one-year-out 和成本敏感性均已知；
- 尚未计算：本任务定义的 10,000 个 matched-placebo 排列及其统计分布；
- 主窗口：XNYS `2021-02-22..2025-12-02`，1202 个信号 session、1201 个收益 interval；
- one-session lag、`constructive/risk_on -> 100% QQQ`、其他状态 `-> zero-return cash`；
- 主成本：单向 traded notional `5 bps`；
- comparator：TRADING-2557 的静态 exposure-matched QQQ/cash comparator；
- 冻结期望：LONG interval=`385`，LONG episode=`41`，return interval=`1201`；
- 输入必须 exact-bind TRADING-2557 implementation、authorization、manifest、aggregate result、
  canonical DQ identity、operational predictions 和 canonical QQQ adjusted-close bytes。

任一 hash、session、DQ/PIT、lag、exposure、episode count、turnover、cost 或 comparator 漂移均为
`INVALID`，不得进入 placebo 计算。

## 3. Matched-placebo 算法

1. 从冻结、lag 后的 1201 个二元 exposure interval 提取：
   - 41 个连续 LONG run length；
   - leading/trailing FLAT boundary gap；
   - 40 个内部 FLAT gap length。
2. leading/trailing boundary gap 保持原位；每个 draw 独立随机排列 LONG run-length 多重集和内部
   FLAT gap-length 多重集，再按 `boundary-flat -> long -> interior-flat ... -> long -> boundary-flat`
   重建 1201 interval exposure path。
3. 每个 placebo 必须保持 interval count、LONG count、LONG episode count、边界状态、换手次数及
   5 bps 成本完全一致；任一不一致立即 fail closed。
4. seed 固定为 `2558`，draws 固定为 `10000`；seed/draws 不是可搜索参数。
5. 每个 placebo 使用与真实策略相同的 QQQ interval return、成本函数和静态 comparator，计算
   net total return、paired excess 与 max drawdown。

主统计量为真实 `paired excess` 相对 placebo excess 分布的单侧 randomization p-value：

`p = (1 + count(placebo_excess >= observed_excess)) / (10000 + 1)`。

同时报告 observed percentile、placebo 2.5%/50%/97.5% 分位数、超过真实结果的 draw 数，以及
max-drawdown 的描述性分布；不得新增见结果后统计量。

## 4. 临时 pilot reducer

`alpha=0.05` 是本任务预注册的临时 pilot baseline，依据常用单侧 randomization-test 口径，仅用于
研究优先级，不是生产、仓位或 Options gate。Owner/reviewer=`Project Owner / strategy research`；
review condition=本次 write-once 结果形成后；exit condition=由 prospective OOS policy 取代，或候选归档。

precedence：

- `INVALID`：任一输入、DQ/PIT、结构匹配、成本、manifest、运行计数或 independent replay 失败；
- `TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO`：有效 `p > 0.05`；
- `TIMING_DISTINGUISHED_DIAGNOSTIC_ONLY`：有效 `p <= 0.05`。

无论 reducer 输出为何，TRADING-2557 保持 `INSUFFICIENT/HOLD`，Options Wave B/C 和所有生产/交易
权限保持关闭。正面结果只支持继续 F2 prospective OOS；负面结果支持降低本候选研究优先级。

## 5. 有界运行与安全边界

本任务最多允许：

- execution manifest replay：1；
- canonical DQ：1；
- local matched-placebo run：1；
- independent deterministic replay：1；
- data download、cache mutation、external provider、QuantConnect、option backtest、paper/live、
  production、broker、orders、fills、positions：0。

仅允许读取已经存在且由 manifest/hash 绑定的本地研究输入。输出为 aggregate-only，不保存逐日价格、
逐日信号、10,000 条完整路径或账户/交易 payload。`production_effect=none`、`broker_action=none`。

## 6. 实施步骤

1. 登记本任务与 requirement，完成 `SINGLE_LANE` preflight；
2. 建立 versioned preregistration、strict loader、deterministic implementation 和 golden/negative tests；
3. 提交实现身份后生成 exact authorization/manifest，并在 dispatch 前自动 replay；
4. 运行一次 canonical DQ、一次 placebo run 和一次独立重放；
5. 写入 result admission，并更新 canonical task、system flow 和适用生成权威；
6. 完成 focused、Architecture、Contract、Integration、Reproducibility、Full、local-main ff-only、
   ordinary push、SHA 复核与 governed cleanup。

## 7. 验收标准

- TRADING-2557 所有实现、manifest 和结果保持 byte-identical；
- 实际 exposure/accounting 与 TRADING-2557 在 `1e-8` 内复算一致；
- 每个 placebo 精确保持 LONG count、run/gap 多重集、边界状态、换手和成本；
- seed=`2558`、draws=`10000`，同 manifest write-once；
- canonical DQ PASS 并披露 requested/evaluated range；
- independent replay 对 aggregate 与 p-value 完全一致；
- reducer 机械执行本 requirement，不接受手工覆盖；
- 明确披露 historical reused-development、partial prior visibility 和非 pristine-OOS；
- 所有下载、缓存、外部、期权、生产、broker 与交易计数保持 0。

## 8. 工作区生命周期

- governed mode：`SINGLE_LANE`；
- branch：`codex/trading-2558-matched-placebo-falsification`；
- exact frozen base：`ea8937b2a07f5c4fc52ba1c437566017be137baa`；
- 复用当前 checkout，不创建额外 worktree/clone；
- runtime output：`outputs/research/first_layer_composer_v2_matched_placebo_v1/`；
- output write-once；发布前不得覆盖或删除；运行额度消费后不得重跑；
- 分支仅在 main/origin 包含同一候选、证据完整、进程无依赖且 audit 无独有内容后清理。

## 9. 进度记录

- 2026-09-03：Owner 在看到 retained-result 低成本检查和 matched-placebo 建议后要求继续推进验证。
  本任务据此采用 `STANDING_OWNER_SCOPE`，但只允许本 requirement 固定的本地 R1 bounded research；
  不扩大到数据下载、cache mutation、Options、provider、production、broker 或交易行为。
- 2026-09-03：已冻结 V1 preregistration，完成 deterministic SHA-256 permutation、逐 draw
  shape/turnover invariant 检查、主账户计算与独立账户重放实现；focused parallel pytest
  `9 passed`，Ruff PASS。尚未读取 matched-placebo 结果，正式 manifest replay、DQ、研究 run 与
  independent replay 计数均仍为 `0/1`。
- 2026-09-03：实现身份固定为 commit `bd00069d0cd56dc2135ab61e70663f7e38b7d037`、
  module SHA-256 `d72f7b9294cc7b8030a9eb8a3a04559bb64b433c0179810a12f83d6807e3e20d`；
  standing-scope authorization 与 execution manifest 已逐字节绑定 preregistration、TRADING-2557
  终态证据及全部既有本地输入。loader/core focused parallel pytest `10 passed`，正式 1/1/1/1
  计数仍未消费。
- 2026-09-03：唯一正式 attempt 已完成。Manifest replay 与 canonical DQ 均 PASS，DQ 为 0 error /
  0 warning；实际计数 `1/1/1/1`，独立重放的 placebo excess 与 drawdown 最大绝对差均为 `0.0`。
  真实 paired excess `+13.745976956735628pp` 位于 matched-placebo 第 `80.0` 百分位；10,000 draws
  中有 `2,000` 个不低于真实值，单侧 `p=0.20007999200079993`，故冻结 reducer 输出
  `TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO`。所有禁止动作计数为 0；TRADING-2557 仍为
  `INSUFFICIENT/HOLD`，不支持提高 prospective OOS、Options 或生产优先级。
- 2026-09-03：publication transaction v1 在 `GENERATED_REBUILD_PRE` 因最初声明的 lane head 仍是
  frozen base 而 fail closed；v2 从已提交结果 identity 重启后，Atlas generator 以
  `ATLAS_LIVE_UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED` 拒绝旧 2557 页面。两次失败均未覆盖 Atlas
  canonical page、未运行 Full、未改变研究结果，也未再次消费研究/DQ/replay 额度。已显式新增 2558
  Atlas 分类与读者摘要；同时发现 canonical task terminal update 的 notes 未保留 `Supporting requirement:`
  前缀，导致派生 `requirement_refs` 为空。后续事务必须通过 canonical writer 追加纠正事件，恢复同一
  requirement ref，再从 generator order 起完整重放。
