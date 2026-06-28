# Norgate Trial Integration Scope

状态：`NORGATE_TRIAL_INTEGRATION_SCOPE_READY`

本批只做 Norgate US Stocks Platinum Trial 的工程 / 数据能力 spike。Trial 长度为
3 周，访问方式为 Windows Python。Trial daily price history limited to 2 years，
因此只能验证最近 2 年 prototype，不允许声明覆盖 2021 primary window。

允许工作：

- Python package / local DB smoke test。
- Nasdaq-100 / NDX membership query probe。
- Delisted visibility probe。
- Price coverage probe。
- Daily membership snapshot summary。
- 2Y breadth prototype schema / summary。
- PIT / leakage / raw data governance audit。
- Paid Platinum owner decision evidence。

禁止工作：

- 不购买正式订阅或自动升级。
- 不提交 vendor raw data、local Norgate cache、credentials 或完整 member list。
- 不恢复 first-layer、v4、minimal forward diagnostic、promotion、paper-shadow、production 或 broker。
