# TRADING-2550：Frozen Signal Value Confirmation V1

最后更新：2026-08-31

- stable task id：`TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1`
- priority：`P0`
- status：`BLOCKED_OWNER_INPUT`（`draft.1` 已形成，等待 exact file/canonical SHA 复核）
- task class / evidence type：`EMPIRICAL_EVIDENCE`
- research question id：`SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2`
- production effect：`none`
- broker action：`none`

## 1. 任务目的

这是 evidence-first portfolio 选定的下一项 umbrella research task。它只回答一个问题：冻结的第一层五态
组合信号，在固定资本、固定时钟、固定成本和结果不可见时预先登记的比较基准下，是否提供值得保留的增量
价值。它不得在同一任务中修改信号、搜索参数、删除不利时期或事后添加 benchmark。

本轮 Owner 已授权继续推进 outcome-blind 预注册设计，但没有授权读取市场结果、DQ、收益计算、回测、
QuantConnect、外部提供方、paper/live/production/broker，也不生成投资结论。预注册草案必须先形成 exact
file/canonical SHA，再由 Owner 对会影响投资解释的比较方法、成本、阈值与 stop rule 作独立复核。

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

### 3.1 `draft.1` 的研究对象

本草案不重建趋势判断，也不使用 option data。唯一信号仍是 exact 1,202-session
`first_layer_composer_v2:trend_state` package：

- `constructive / risk_on -> 100% QQQ`；
- `defensive / neutral / risk_off -> ZERO_RETURN_CASH`；
- 不允许 leverage、short、options、参数搜索或结果出现后修改映射；
- 使用 package 已冻结的 `NEXT_VALID_US_EQUITY_SESSION` 一日 lag，不从本地重新解释 signal date。

这是一条信号价值 measurement implementation，不是新的 allocation authority，更不是面向期权的第二条预测链。

### 3.2 primary comparator 与共同会计

唯一 primary comparator 提议为 `EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH`。在加载价格结果前，仅从冻结
signal plan 计算：

`target_qqq_weight = LONG exposure return intervals / 1201`。

Comparator 在首个可评估 close 按该固定比例买入 QQQ，剩余为零收益现金，持有固定 shares 到末日 close 清算；
不根据价格结果拟合，也不允许运行后追加 comparator。Candidate 与 comparator 均使用：

- initial capital=`USD 100,000`；
- adjusted-close、effective-session close 到下一 XNYS session close；
- fractional shares、无负现金、无 fill-forward、无缺失价格插值；
- 相同 `FIXED_TRADED_NOTIONAL_BPS` cost formula；
- initial allocation、target change 与 terminal liquidation 均按 one-way `5 bps` 计费。

QQQ buy-and-hold 只作 market context；calendar-year 与 pre-2023/post-2022 slice 只披露 concentration，不参与
primary verdict。这样 primary test 比较的是同计划平均 QQQ exposure 下的 timing increment，而不是把更少持仓
误写成 signal alpha。

### 3.3 primary metric、falsification 与三态 reducer

- primary metric：`candidate net total return - exposure-matched comparator net total return`，单位为 percentage
  points；
- RETAIN return threshold：严格 `> 0`；
- mandatory falsification guard：`candidate max-drawdown magnitude - comparator max-drawdown magnitude <= 0`；
- 任一 identity、DQ/PIT、manifest replay、1202/1201 coverage、missing/duplicate/unknown/imputation、accounting/
  cost reconciliation、metric、independent replay 不满足，先输出 `INSUFFICIENT`，不得落入性能比较；
- 两个结果条件同时满足才 `RETAIN`；任一 return `<= 0` 或 drawdown regression `> 0` 则 `REJECT`；
- diagnostics 不得覆盖 primary verdict。

Stop action 仍保持 portfolio 已冻结的语义：`RETAIN` 仅开启期权 paired-comparison 的 Owner review；`REJECT`
关闭该 options implementation P0 路线且不参数救援；`INSUFFICIENT` 只补 verdict 明确指出的 prospective evidence。

### 3.4 historical / prospective 与未来运行 envelope

`2021-02-22..2025-12-02` 明确标记为 `REUSED_DEVELOPMENT_CONFIRMATION`，不得称为 researcher-pristine OOS。
Prospective 起点只能是 final policy approval 后首个 XNYS session，且只能确认冻结规则。

草案包含 `SPECIFICATION_ONLY_NOT_AUTHORIZED` 的一次性 future envelope：未来若另获 exact Owner 授权，最多
允许 manifest replay=1、canonical DQ=1、local signal-value confirmation=1、independent replay=1；download、
cache mutation、QuantConnect、option backtest、provider、orders/fills/positions 均为 0。该 proposed maxima 不构成
当前授权；当前 safety 中 data read、DQ、confirmation、backtest 与所有 external action 均为 false/0。

### 3.5 exact draft identity

- policy：`frozen_signal_value_confirmation_preregistration_v1@1.0.0-draft.1`；
- file SHA-256：`507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa`；
- canonical SHA-256：`7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c`；
- authority-set SHA-256：`45d508d563b46b0929d80687155213d265399a4f105da69f31810780a34c754f`；
- current decision state：`OWNER_REVIEW_REQUIRED`；
- execution activation：`false`。

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

- blocker：`draft.1` 已在 outcome-blind 状态形成并通过 focused mechanical validation，但 comparator、`5 bps`
  cost、zero return threshold、zero drawdown-regression guard 与 reducer 仍是 proposed policy，尚无 Owner exact
  file/canonical SHA approval；
- next owner：Project Owner 复核并选择接纳、要求修订或拒绝 `draft.1`；strategy research coordinator 不得在
  Owner decision 前读取 outcome 或执行 manifest/DQ/confirmation；
- exit condition：预注册合同与运行 manifest 通过独立验证且取得适用授权后，才可进入 bounded empirical run。

## 6. 进度记录

- 2026-08-31：由 TRADING-2549 S3 建立为 `PROPOSED` empirical handoff。未运行实验，未读取新市场数据，
  未调用外部平台，未创建订单或仓位。
- 2026-08-31：Owner 授权继续推进预注册设计。`SINGLE_LANE` START/LANE preflight 从 exact
  `main=ab478cd5fe8dcb811780e91e0b221a914252af71` PASS；形成 strict `1.0.0-draft.1` policy、authority
  byte/semantic replay、non-executable action rejector 和 focused negative/golden tests。草案把同一冻结五态信号
  映射为 fully-funded QQQ/zero-return cash，并以 result-blind exposure-matched static QQQ/cash 作为 primary；
  没有读取 signal payload、市场结果或 option data，没有运行 DQ、confirmation、backtest、QuantConnect/provider，
  orders/fills/positions=`0/0/0`。focused pytest-xdist=`9 passed`；当前转为 `BLOCKED_OWNER_INPUT`，等待 exact
  draft identity 复核，不自动触发经验运行。
