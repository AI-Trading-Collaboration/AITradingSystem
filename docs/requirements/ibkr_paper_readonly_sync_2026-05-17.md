# TRADING-009：IBKR Paper Read-only Sync

最后更新：2026-05-17

关联任务：`TRADING-009`

## 背景

`TRADING-008A` post-merge sanity review 和对应 CI 已通过。当前阶段只验证
IB Gateway / TWS Paper 的只读账户同步能力，不提交订单，不改变 production
评分、仓位建议、paper runner 或参数晋级。

## 范围

1. 新增 `IBKRPaperReadOnlyAdapter`：
   - 允许 `connect`、`disconnect`、`get_account_summary`、`list_positions`、
     `list_open_orders`、`list_executions`、`get_contract_details`。
   - `submit_order` 必须抛出明确错误。
   - 默认通过注入 mock / client 对象测试；真实连接仅在显式启用配置时发生。
2. 新增 `config/ibkr_paper_readonly.yaml`：
   - `enabled=false`
   - `trading_mode=paper`
   - `readonly=true`
   - `production_effect=none`
   - 不保存密码、token、API key 或其他敏感凭证。
3. 新增 `scripts/run_ibkr_paper_readonly_snapshot.py`：
   - 输出 `outputs/reports/ibkr_paper_account_snapshot_YYYY-MM-DD.json`
     和 `.md`。
   - 报告连接状态、脱敏 account id、账户摘要、持仓、open orders、近期
     executions、contract details sample、`production_effect=none`、
     `readonly=true`。
4. 新增 `IBKRPaperReadOnlyReconciliation` scaffold：
   - 第一版只比较 symbol、quantity、cash summary 是否存在和 unknown
     positions。
   - 输出 `reconciliation_status`：`PASS`、`LIMITED` 或 `BLOCK`。

## 安全边界

- 默认 `enabled=false`，脚本不会连接真实 IBKR。
- `trading_mode != paper` 时 fail closed。
- `readonly != true` 时 fail closed。
- `production_effect` 必须为 `none`。
- account id 只允许 masked 输出；完整 account id 不得写入 JSON、Markdown 或日志。
- account id 不像 Paper / `DUP` 账号时，账户同步状态至少为 `LIMITED` 或 `BLOCK`。
- 不读取 broker API key、密码、token 或真实交易密钥。
- 不实现真实下单路径；调用 `submit_order` 必须抛出明确错误。

## 不在本轮范围

- 不提交、取消或修改 IBKR 订单。
- 不接入真实 production account。
- 不把 IBKR snapshot 结果写入正式仓位建议、prediction ledger、daily score、
  paper replay 或 parameter promotion。
- 不精确计算 IBKR avg cost、realized/unrealized PnL、税费或融资成本。

## 验收标准

- `python -m pytest tests/trading_engine/test_ibkr_paper_readonly.py`
- `python -m pytest tests/trading_engine`
- `python -m pytest`
- `python -m ruff check scripts src tests`
- `python -m black --check scripts src tests`
- 提交并 push 后确认 GitHub Actions 本次 commit 对应 CI run 通过。

## 实施步骤

1. 增加任务登记、需求文档和系统流图边界。
2. 实现只读 adapter、配置 loader、安全校验和 account id masking。
3. 实现 snapshot 脚本和 Markdown/JSON 报告。
4. 实现 PaperPortfolio 对账 scaffold。
5. 用 mock client 补测试，不连接真实 broker，不读取任何密钥环境变量。
6. 跑本地验收，提交、push 并检查 CI。

## 状态记录

- 2026-05-17：新增并进入实现。原因：owner 要求在 `TRADING-008A`
  sanity review 和 CI 通过后，推进 IBKR Paper Account 只读同步 spike，
  用于验证连接、账户状态读取和本地 PaperPortfolio 对账能力；本轮严格限制为
  read-only、paper-only、`production_effect=none`。
- 2026-05-17：实现完成并进入验证。已新增只读 adapter、默认 disabled 配置、
  snapshot JSON/Markdown 脚本、account id masking、非 paper/read-only
  fail closed、非 Paper / DUP account 阻断、`submit_order` 明确报错和
  PaperPortfolio 对账 scaffold；测试使用 mock client，不连接真实 IBKR，
  不读取 broker credentials。本地验证通过 `python -m pytest
  tests/trading_engine/test_ibkr_paper_readonly.py`、`python -m pytest
  tests/trading_engine`、`python -m pytest`、`python -m ruff check scripts src
  tests` 和 `python -m black --check scripts src tests`。
