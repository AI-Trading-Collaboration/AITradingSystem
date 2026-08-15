# TRADING-2522 — QQQ Options 主窗口 daily Slice 再验证执行与证据 V1

- status: `IN_PROGRESS`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- registration base: `5485794ec1aa8b89b8e1d7d8d683d0dcc43b27bb`
- predecessor contracts: `TRADING-2520` / `TRADING-2521`
- production effect: `none`
- broker action: `none`

## 目标

在 Project Owner 已签署的 v4 单次授权下，严格执行一次 QuantConnect QQQ Options 主窗口
daily Slice 零订单 Cloud revalidation，收集 export-safe Results artifact，并通过 2521 admission、
2512 strict Results parser 与 2482 DQ/PIT canonical path 形成可审计结论。

本任务只验证数据交付、属性 transport、1202-session coverage 和 derived aggregate 完整性；不执行
策略选择、真实订单、投资解释、paper/live/broker/production，也不授权日级 engine 自动解锁。

## Owner 授权事实

- token：`owner_decision:TRADING-2520:2026-08-15:authorize_single_zero_order_primary_window_daily_slice_revalidation_v4`；
- owner-decision file SHA-256：
  `f37e778a8f8c71e126efe622ef7d3f659af944164f7c97d82269125fa663e197`；
- owner-decision content SHA-256：
  `d62b681d2fafdea939f30278ae2dca39ab28048973868faa0301c650ea00fcd0`；
- 2521 owner candidate content SHA-256：
  `ef5eb9ea3ea8c1b73d6ca6dda6be31fede13b1cfd0f051541508f7d0789c0e9f`；
- collector authorization content SHA-256：
  `87c3360797d8ae913e4e37a3683460eec14ed960c993714e1ede8cbdf713e33b`；
- admitted-unused receipt content SHA-256：
  `818c6edf7811234c621775c31a13525ba26ecdafbfdd3b15edc7ac77c07f0a49`；
- expiry：`2026-08-21T00:00:00Z`；
- current state：`OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED`、`authorization_consumed=false`、
  `external_action_performed=false`。

上述 admission 仅为纯内存 strict validation，不构成 Cloud run 或证据收集。第一次实际 Cloud run
attempt 无论 completed/failed 都消费授权，之后禁止第二次运行。

## 冻结执行边界

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- exchange calendar：`XNYS`；expected sessions：`1202`；
- project code LF SHA-256：
  `88a60874737c1e210f5a2f5ac990d14d0f4de3024a1db8f41edaddf3db6226aa`；
- maximum project mutations / Cloud backtests / orders / fills：`1 / 1 / 0 / 0`；
- result carrier：manual browser download of export-safe Results JSON；
- allowed action order：login → one existing-project mutation → one Cloud backtest → one manual
  Results collection；
- prohibited：API、CLI、HTTP、Object Store、raw option rows/log/export、second project/backtest、
  purchase/subscription、range expansion、investment interpretation、paper/live/broker/production。

## 执行步骤

1. 从 exact registration main 运行 START/LANE preflight，并复核 token 仍 admitted-unused。
2. 使用已登录浏览器打开 project `34808569`，读取当前 project code identity；不读取 cookie、local
   storage、secrets 或其他项目。
3. 仅当 target project、code mutation cap 和项目身份一致时，用 2520 canonical `main.py` 覆盖一次。
4. 启动一次、且仅一次 Cloud backtest；该点击/提交即 first run attempt，立即生成 consumed receipt。
5. 等待 terminal，记录 build/engine/backtest id、requested/evaluated range、orders/fills 和错误事实。
6. 仅通过页面的 Results 下载收集 export-safe JSON；禁止 raw option rows、logs download 和其他载体。
7. 运行 2521 strict parser 与 2482 DQ/PIT evaluator；FAIL/UNKNOWN/NOT_EVALUATED 均保持 cash preservation。
8. 更新本 requirement/task projection、system flow/Atlas disclosure 和 canonical evidence；final gates 后
   ordinary push/cleanup。

## 验收标准

1. action ledger ordinal、timestamp、project/code/backtest/result identity 完整且 canonical。
2. project mutation ≤1、Cloud backtest =1、orders=0、fills=0；第二次 run fail closed。
3. token 在 first run attempt 后标记 consumed，即使 Cloud run 失败或 Results 不完整。
4. Results artifact file/content checksum、backtest id、1202 session identity和所有 required derived series
   由 2512 parser 验证，不接受调用者自报 PASS。
5. DQ/PIT 仅由 2482 canonical 15-check path 派生；UNKNOWN 永不产生 PASS。
6. 成功也只可支持 `GO_FOR_DAILY_ENGINEERING_ONLY` 候选评审，不自动激活 selection/engine。
7. 任意越权、订单/成交、raw transport、range/code/scope mismatch 输出 typed failure 并停止。
8. tracked evidence、task registry/shadow、system flow、Atlas 和正式验证保持可重放。

## Path claims

Task-owned：

- `docs/requirements/TRADING-2522_QC_QQQ_Options_Primary_Window_Daily_Slice_Revalidation_Execution_Evidence_V1.md`；
- `inputs/research/qqq_options/trading_2522_primary_window_daily_slice_revalidation_execution_v1/**`；
- `tests/test_qqq_options_daily_slice_revalidation_execution_evidence.py`（如需新增）。

Coordinator-owned：task registry/index/shadow、`docs/system_flow.md`、architecture/DevEx/compatibility
authority 与 Atlas page-effectiveness consumer。

## 当前状态

`OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED`。registration boundary 发布前不执行浏览器外部动作；
registration push 后由本任务单一 coordinator 消费授权。
