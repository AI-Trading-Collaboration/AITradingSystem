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

## TRADING-009A：本机验证固化

`TRADING-009A` 不新增下单能力，也不提交真实本机配置。目标是把本机
IBKR Paper 只读连接验证流程固化为可复用 runbook，并给测试提供脱敏样例。

范围：

1. 新增 `docs/runbooks/ibkr_paper_readonly_local_validation.md`，记录 TWS /
   IB Gateway Paper 启动、API socket 开启、推荐 port/client_id、本机 local
   YAML 准备、snapshot 命令、常见错误和验证 checklist。
2. `.gitignore` 覆盖 `config/*.local.yaml` 和
   `outputs/reports/ibkr_paper_account_snapshot_*.json/md`，避免本机账号和只读
   账户状态报告被误提交。
3. 新增 `tests/fixtures/ibkr_paper_account_snapshot_sanitized.json`，只保留
   masked account id 和结构样例，不包含完整账号、真实姓名、邮箱、现金细节或
   敏感标识。
4. 测试确认 `.local.yaml` 不被测试依赖、sanitized fixture 不含完整 `DUP`
   账号格式、snapshot writer 默认隐藏完整 account id。

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
- 2026-05-17：真实 TWS Paper validation 通过。已在本机检测到 TWS 进程和
  `127.0.0.1:7497` API socket，修复 Python 3.14 下 `ib_insync` import 前
  缺少 asyncio event loop 的初始化问题，并补齐 `ib_insync` AccountValue /
  Position 序列行解析。使用 `config/ibkr_paper_readonly.local.yaml` 运行
  `python scripts/run_ibkr_paper_readonly_snapshot.py --date 2026-05-17 --config
  config/ibkr_paper_readonly.local.yaml`，生成
  `outputs/reports/ibkr_paper_account_snapshot_2026-05-17.json/md`，
  `connection_status=CONNECTED`、`snapshot_status=PASS`、
  `reconciliation_status=PASS`、account id masked、`production_effect=none`、
  `readonly=true`，未出现 `submit_order` / `cancel_order` / `placeOrder`。
- 2026-05-17：`TRADING-009A` 新增并进入实现。原因：owner 要求在
  `TRADING-009` 完成且 CI 通过后，固化本机 IBKR Paper 只读验证流程和安全
  checklist；本阶段只记录/验证 local workflow，不提交本机敏感配置或真实账户
  snapshot。
- 2026-05-17：`TRADING-009A` 实现完成并进入验证。已新增本机验证 runbook、
  `.gitignore` local config / snapshot 规则、sanitized snapshot fixture、
  `--config` snapshot CLI alias 和测试覆盖；本地验证通过 `python -m pytest
  tests/trading_engine/test_ibkr_paper_readonly.py`、`python -m pytest
  tests/trading_engine`、`python -m pytest`、`python -m ruff check scripts src
  tests` 和 `python -m black --check scripts src tests`。
