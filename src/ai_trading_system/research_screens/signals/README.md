# 信号模块（TRADING-2565 S4b）

每个信号有两个文件，id 只用小写字母、数字和下划线：

- `<id>.py`：`signal(returns: pd.Series, sources: Mapping[str, pd.DataFrame], params: Mapping[str, Any]) -> pd.Series`。
- `<id>.yaml`：`signal_id`（等于文件名）、`mechanism`（非空机制说明）、`expected_sign`（1 或 -1）、`target`（return 或 risk）、`params`（参数字典）、`sources`（来源 id 列表）、`markets`（all 或 qqq）、`literature`（非空，可写 none）。未知字段拒绝。

收益 r_t 是 t−1 收盘到 t 收盘的收益；来源已按发布滞后对齐。函数返回同索引的实数 Series，可有 NaN，不可有无穷。每个 t **只能用 t 为止的数据**，不能用未来值、全样本排名或未来拟合的参数。模块不得自行读文件、联网或使用外部状态；评估台的黑盒前视检查防误用，不是执行沙箱。数值和布尔参数必须实际改变评估窗口内的输出，其他类型会列为 unchecked。

新增模块在 B 区，但每次通过检查的筛选都进 `research/_screens/ledger/` 全局账；模块和规格各以 CRLF 规范成 LF 后的 SHA-256 钉住。参数和预期方向写在规格内，更改规格就是新家族成员。经典信号（均线、动量、实际波动）是固定基准，不另筛。

命令：`python -m ai_trading_system.research_bench screen <id> --signal-snapshot <ID> [--bench-snapshot <ID>]`；`screen-ledger` 看当前全局 BH 和裁决。所有市场截到配置的 `screen_end`（最晚 2010-12-31），没有读更晚数据的选项。检查失败不记账；重复的数据、配置和模块组合在计算前拒绝。

只有 candidate 才能另开 C 区预注册题；candidate 不是研究正面结论、paper-shadow 准入或 production/broker 批准。

预注册用 `screens`、`screen_family_size` 引用筛选记录；通用“信号 → 仓位”策略是 `research_bench/signal_strategy.py`（S4d）。

第一批真实信号（TRADING-2565-10）：`credit_spread_change_63`、`credit_spread_level`、`term_spread_level`、`high_distance_252`。规格文字在任何筛选之前定好并按哈希钉住，改规格就是新家族成员。信号模块不要用 `@dataclass`：模块加载时不进 `sys.modules`，dataclass 会因此加载失败（报错文字是 `'NoneType' object has no attribute '__dict__'`），用普通函数和元组。
