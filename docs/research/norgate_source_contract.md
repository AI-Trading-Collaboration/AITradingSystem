# Norgate Source Contract

配置：`config/data/norgate_source_contract.yaml`

`source_id`: `norgate_us_stocks_platinum_trial`

本 source contract 只允许提交派生 summary artifact。Norgate raw data、local cache、
账号密码和完整 membership symbol list 均不得提交。

Trial limitation：

- Daily price history limited to 2 years。
- 可验证 Python access、membership query、snapshot prototype 和 2Y prototype。
- 不可完成 2021-02-22 primary window 的完整 price join。

Index alias 由 adapter 统一处理：`nasdaq100` / `ndx` -> `$NDX`。
