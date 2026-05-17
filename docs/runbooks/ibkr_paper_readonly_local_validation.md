# IBKR Paper Read-only 本机验证 Runbook

最后更新：2026-05-17

适用范围：`TRADING-009A` 本机 IBKR Paper 只读连接验证。该流程只读取
TWS / IB Gateway Paper 账户状态，不提交订单、不取消订单、不修改账户，不改变
production 评分、仓位建议、paper runner、replay 或参数晋级。

## 前置条件

- 已安装并能登录 TWS Paper Trading 或 IB Gateway Paper Trading。
- 项目本地环境已按 README 使用 Python 3.11 `.venv` 安装
  `.[dev,data,dashboard,brokers]`，其中 `brokers` 包含 `ib-insync`。
- 只使用 Paper / `DUP` 账户做验证；production account 不进入本流程。
- 不把密码、token、session cookie、API key、完整账号、姓名、邮箱或现金细节写入
  repo、issue、PR、日志摘录或测试 fixture。

## 启动 TWS / IB Gateway Paper

1. 打开 TWS 或 IB Gateway。
2. 在登录界面选择 `Paper Trading`，不要选择 live / production session。
3. 登录后保持应用运行，确认本机没有 sleep、网络断开或会话锁定导致 API 断线。
4. 如果同时运行多个 IBKR 客户端，只保留一个 Paper API endpoint，避免端口冲突。

## 开启 API Socket

TWS 常见路径：

1. `File` -> `Global Configuration` -> `API` -> `Settings`。
2. 勾选 `Enable ActiveX and Socket Clients`。
3. 建议勾选 `Read-Only API`。
4. `Socket port` 推荐使用 Paper TWS 默认 `7497`。
5. `Trusted IPs` 保持只允许本机，或显式添加 `127.0.0.1`。
6. 保存设置后重启 TWS，确保 socket 重新监听。

IB Gateway 常见路径：

1. `Configure` -> `Settings` -> `API` -> `Settings`。
2. 勾选 `Enable ActiveX and Socket Clients`。
3. 建议勾选 `Read-Only API`。
4. Paper Gateway 常见端口是 `4002`；如果使用 Gateway，请同步修改本地 YAML。
5. 保存后重启 Gateway。

本项目推荐：

- `host: 127.0.0.1`
- `port: 7497`（TWS Paper）或 `4002`（IB Gateway Paper）
- `client_id: 19009`
- `trading_mode: paper`
- `readonly: true`
- `production_effect: none`

端口检查示例：

```powershell
Test-NetConnection 127.0.0.1 -Port 7497
```

`TcpTestSucceeded=True` 只能说明 socket 在监听，不代表账号、readonly 或 API
权限一定正确；最终以 snapshot 脚本输出为准。

## 准备本地配置

复制默认配置到本机 local 文件：

```powershell
Copy-Item config\ibkr_paper_readonly.yaml config\ibkr_paper_readonly.local.yaml
```

编辑 `config/ibkr_paper_readonly.local.yaml`：

```yaml
enabled: true
host: 127.0.0.1
port: 7497
client_id: 19009
account_id: "DUP_REPLACE_WITH_PAPER_ACCOUNT_ID"
trading_mode: paper
readonly: true
production_effect: none
```

安全要求：

- `config/ibkr_paper_readonly.local.yaml` 只保存在本机，`.gitignore` 已覆盖
  `config/*.local.yaml`，不得提交。
- 该 YAML 不需要、也不得包含 password、token、API key、session cookie 或
  任何真实交易密钥。
- `account_id` 在本机文件里需要填真实 Paper 账号用于连接；脚本输出必须只显示
  masked 结果，例如 `DUP***1234`。
- 如果账号不是 Paper / `DUP` 格式，脚本会 fail closed，并输出 `BLOCK`。

## 运行 Snapshot

使用 Python 3.11 `.venv` 运行：

```powershell
.\.venv\Scripts\python.exe scripts\run_ibkr_paper_readonly_snapshot.py --date 2026-05-17 --config config\ibkr_paper_readonly.local.yaml
```

`--config` 与 `--config-path` 等价。默认输出：

- `outputs/reports/ibkr_paper_account_snapshot_2026-05-17.json`
- `outputs/reports/ibkr_paper_account_snapshot_2026-05-17.md`

这些本机 snapshot 输出可能包含真实账户状态的只读摘要，`.gitignore` 已覆盖
`outputs/reports/ibkr_paper_account_snapshot_*.json` 和 `.md`。需要测试样例时，
只使用 `tests/fixtures/ibkr_paper_account_snapshot_sanitized.json` 这类已脱敏 fixture。

## 本机验证 Checklist

- `connection_status.status` 为 `CONNECTED`，或在无持仓、无订单、无执行时以
  `LIMITED` / empty list 正常表达，而不是异常退出。
- `account_id_masked` 只显示 masked account id，不显示完整账号。
- JSON 和 Markdown 中没有完整 `DUP` / `U` account id。
- `readonly=true`。
- `production_effect=none`。
- `reconciliation_status` 为 `PASS`、`LIMITED` 或 `BLOCK`，且原因可读。
- `positions`、cash/account summary、`open_orders`、`recent_executions` 能读到；
  没有数据时输出空列表或 `LIMITED`，不补造内容。
- `submit_order` 不可用；本流程不得出现 `submit_order`、`cancel_order`、
  `placeOrder` 等提交或取消行为。
- 代码路径未读取 broker API key、password、token 或 secret。
- 未把完整账号、姓名、邮箱、现金细节或敏感标识写入 repo。

可选本机检查：

```powershell
rg "\bDUP[0-9]{5,}\b|\bU[0-9]{5,}\b" outputs\reports\ibkr_paper_account_snapshot_2026-05-17.*
rg "submit_order|cancel_order|placeOrder|api_key|password|token|secret" outputs\reports\ibkr_paper_account_snapshot_2026-05-17.*
```

第一条命令应无匹配。第二条命令若只命中安全边界说明文字，需要人工确认没有实际
order mutation、credential 或完整账号内容。

## 常见错误

|现象|常见原因|处理|
|---|---|---|
|`ConnectionRefusedError` / `ERROR`|TWS / Gateway 未启动、API socket 未开启、端口未监听|启动 Paper session，开启 API socket，重启 TWS / Gateway 后重试|
|连接超时或端口检查失败|TWS Paper 与 Gateway Paper 端口混用|TWS Paper 用 `7497`，Gateway Paper 常用 `4002`，同步修改 local YAML|
|`readonly must be true`|local YAML 被改成 `readonly: false`|恢复 `readonly: true`；本流程不允许写操作|
|`trading_mode must be paper`|local YAML 不是 `trading_mode: paper`|停止验证，切回 Paper session 和 `trading_mode: paper`|
|`BLOCKED_ACCOUNT_ID`|`account_id` 缺失或不像 Paper / `DUP` 账号|确认登录的是 Paper Trading，并只在 local YAML 填 Paper account id|
|`ib_insync is required`|本地环境未安装 broker optional dependency|运行 `.\.venv\Scripts\python.exe -m pip install -e ".[dev,data,dashboard,brokers]"`|
|client id 冲突|同一 TWS / Gateway 上已有相同 client id 连接|换一个未占用的 `client_id`，并记录在 local YAML|
|account summary 为空但连接成功|Paper 账户无可见 cash summary、权限不足或 IBKR 返回字段差异|保留输出为 `LIMITED`，不要手工补造现金数据；必要时截图本机 TWS 设置供人工复核|
