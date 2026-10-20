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

经典趋势信号（TRADING-2565-11）：`tsmom_21`、`tsmom_63`、`tsmom_126`、`tsmom_252`（过去 21、63、126、252 个交易日的复利收益）和 `ma_distance_210`（价格相对 210 日均线的距离）。它们是第二题（QQQ-Q2）预注册里按哈希钉住的固定基准，不进筛选账；`ma_distance_210` 和 `research_bench/screen.py` 里的基准信号 `ma_10m_distance` 是同一个量。四个 `tsmom` 模块是同一份文件的四个名字，内容逐字节相同。

研究自动循环第一批不用新数据的信号（TRADING-2565-12）：`industry_breadth_210`（49 个行业里低于自己 210 日均线的比例，目标是风险）、`industry_corr_63`（49 个行业过去 63 日两两相关系数的平均值）、`dip_in_uptrend_10`（过去 252 日收益为正时的过去 10 日收益，否则没有值）。两个行业信号只读来源 `ff_49_industries_daily`，不用 `returns`；筛选时 49 个行业本身也是被筛的市场，读复现广度时要知道信号是由它们算出来的。`dip_in_uptrend_10` 的想法来自第一、二题验证窗口的结果，验证窗口对它不是干净的检验。

研究自动循环第一批用白名单 FRED 序列的信号（TRADING-2565-13）：`vrp_21`（VIX 减美股大盘过去 21 日的实际波动，所有市场共用一个值）、`rate10_change_126`（10 年期国债收益率 126 日的变化）、`bill3m_change_126`（3 个月国库券利率 126 日的变化）。来源 `vixcls`、`vxncls`、`dgs10`、`dtb3` 按 `owner_decision:TRADING-2565:2026-10-15:research_loop_p1` 加进 `config/research/research_screen.yaml`；用到它们的筛选要用包含它们的新信号快照，旧快照照样能读。这一批没有信号用 `vxncls`，它留给试跑阶段的 VXN 对照。

研究自动循环第二批信号（TRADING-2565-25，第二批假设卡 H07、H08）：`bad_vol_share_63`（过去 63 日下跌日收益平方和减上涨日收益平方和、除以两者之和，看波动的方向构成，不看水平）、`reversal_profit_21`（49 个行业按前 5 日复利收益做的短期反转组合，做多和做空各 1，过去 21 日日收益之和，所有市场共用一个值）。两个的目标都是风险。它们只用 1926-07 起的 Ken French 日度数据，加上筛选的 252 天预热，在试跑门第一段（美股大盘 1929-01-02 起）开始之前就有值。`bad_vol_share_63` 选它的理由引用了第一题 q1a 在验证窗口里的失败原因，验证窗口对它不是干净的检验；`reversal_profit_21` 由 49 个行业算出，读复现广度的提醒和 `industry_corr_63` 相同。

研究自动循环第三批信号（TRADING-2565-28，第三批假设卡 H09、H10、H11，Baker 2026-10-19 去掉产量上限以后的第一批）：`absorption_shift_63`（49 个行业过去 63 日协方差的吸收比，即最大的五分之一特征值之和占全部特征值之和的比例；信号是它过去 15 日均值减过去 252 日均值、除以过去 252 日标准差）、`industry_dispersion_21`（49 个行业每天收益的截面标准差，过去 21 日的均值）、`defensive_lead_63`（Food、Beer、Smoke、Hshld、Drugs、Util 六个防御行业等权篮子过去 63 日的复利收益减大盘的复利收益，大盘是三因子文件的 Mkt-RF 加 RF；行业表缺其中任何一列就报错）。三个的目标都是风险，所有市场共用一个值，都只用 1926-07 起的 Ken French 日度数据，在试跑门第一段开始之前就有值；读复现广度的提醒和 `industry_corr_63` 相同。

研究自动循环第四批信号（TRADING-2565-30，第四批假设卡 H12、H13，两个新族「日历季节」「风格情绪」）：`may_oct_share_30`（之后 30 个日历日里月份是五月到十月的天数占比，只用日期、不用数据源，各市场用自己的交易日；目标是收益，方向 −1）、`growth_over_value_252`（1 减 Ken French 三因子文件 HML 过去 252 个交易日的复利增长，即成长股过去一年跑赢价值股的幅度，所有市场共用一个值；目标是风险）。两个都在试跑门第一段开始之前就有值。

第四题的对照信号（TRADING-2567-31）：`realized_vol_21`（资产自己过去 21 个交易日日收益的标准差乘 √252，目标是风险）。它和 `research_bench/screen.py` 里的基准信号 `rv_21` 是同一个量，是固定基准，不进筛选账；做成模块是为了在 `research/QQQ-Q4/prereg.yaml` 里按哈希钉住，作 `industry_dispersion_21` 的参照规则（两者在 S0013 里的秩相关是 0.878）。
