# TRADING-2550：Frozen Signal Value Confirmation V1

最后更新：2026-08-31

- stable task id：`TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1`
- priority：`P0`
- status：`PROPOSED`
- task class / evidence type：`EMPIRICAL_EVIDENCE`
- research question id：`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`
- production effect：`none`
- broker action：`none`

## 1. 任务目的

这是 evidence-first portfolio 选定的下一项 umbrella research task。它只回答一个问题：冻结的第一层五态
组合信号，在固定资本、固定时钟、固定成本和结果不可见时预先登记的比较基准下，是否提供值得保留的增量
价值。它不得在同一任务中修改信号、搜索参数、删除不利时期或事后添加 benchmark。

本记录只建立后续经验研究的任务边界，不授权本轮执行数据下载、回测、QuantConnect、外部提供方、
paper/live/production/broker，也不生成投资结论。

## 2. P0 admission fields

- `research_question_id`：`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`；
- `decision_enabled`：在同资本、同成本的预注册比较下形成一次 `RETAIN / REJECT / INSUFFICIENT` signal-value verdict；
- `evidence_type`：`EMPIRICAL_EVIDENCE`；
- `blocked_experiment`：`none`；本任务就是当前 portfolio 的 primary experiment，不得再以页面、投影或便利性
  successor 代替；
- `stop_condition`：`RETAIN` 才进入期权实现 paired comparison；`REJECT` 关闭该实现路线的 P0；
  `INSUFFICIENT` 只补 verdict 明确指出的 prospective evidence；
- `successor_condition`：只允许上述三个 verdict 机械选择 conditional successor，不得自动创建新的治理任务。

## 3. 启动前必须冻结

1. exact 1,202-session signal package 与输入身份；
2. primary comparator、primary metric、成本、资本、时钟、缺失值和失败传播规则；
3. 结果不可见的 `RETAIN / REJECT / INSUFFICIENT` reducer；
4. historical development/confirmation 与 prospective evidence 的用途边界；
5. 一次性研究运行 manifest、资源上限、零生产/零 broker 边界和 terminal artifact；
6. 结果准入与独立重放路径。

这些内容应留在同一个 supporting requirement 和 umbrella task 的阶段记录中；除非出现真正的 shared contract
wave 或独立外部授权边界，不再为 contract、DQ、execution、result admission、reader projection 和 closeout
自动拆分 successor。

## 4. Acceptance criteria

1. 预注册发生在任何 outcome 可见之前；
2. primary window 起点保持 `2021-02-22`，requested/evaluated range 显式记录；
3. signal package、comparator、capital、clock、cost、metric 与 reducer exact-bind；
4. 输出严格为 `RETAIN / REJECT / INSUFFICIENT` 之一，并解释允许的下一动作；
5. 工程 PASS、数据 PASS、页面 PASS 或既有期权 `+4.48%` 均不能代签 signal-value verdict；
6. 任一 DQ/PIT、identity、manifest、resource 或 replay gate 失败时 fail closed；
7. 运行需在未来单独通过 governed preflight 和适用的 R1/R2/R3 授权边界；
8. `production_effect=none`、`broker_action=none`。

## 5. 当前 blocker 与 next owner

- blocker：primary comparator、metric、成本、资本和三态 reducer 尚未在 outcome-blind 状态冻结；
- next owner：strategy research coordinator 起草单一预注册阶段，Project Owner 复核会影响投资解释的 metric、
  threshold 与 stop rule；
- exit condition：预注册合同与运行 manifest 通过独立验证且取得适用授权后，才可进入 bounded empirical run。

## 6. 进度记录

- 2026-08-31：由 TRADING-2549 S3 建立为 `PROPOSED` empirical handoff。未运行实验，未读取新市场数据，
  未调用外部平台，未创建订单或仓位。
