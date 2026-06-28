# Paid Breadth Data Vendor Registry Review

配置：`config/data/paid_breadth_data_vendor_registry.yaml`

## 分层结论

|Vendor|Tier|当前结论|主要 blocker|
|---|---|---|---|
|Norgate US Stocks Platinum|A|`NORGATE_TRIAL_RECOMMENDED_OWNER_APPROVAL_REQUIRED`|license / local cache / Python membership query 需要 trial 验证；成本需 owner 批准|
|FMP ETF holdings / historical holdings|B|`FMP_NOT_SUITABLE`|当前 `FMP_API_KEY` 对 holdings endpoint 返回 HTTP 402；known-at 和 delisted membership 未验证|
|EODHD historical constituents / delisted data|B|`CONDITIONAL_TRIAL_IF_COST_ACCEPTABLE`|Nasdaq-100 daily membership、known-at、revision policy 和 plan entitlement 未验证|
|QuantConnect / AlgoSeek US equities|C|`QUANTCONNECT_DEFERRED`|membership availability 未确认；local export / LEAN integration cost 不清楚|
|Tiingo / Marketstack / Yahoo / FMP price / EODHD price|D|`PRICE_ONLY_NOT_TRUE_BREADTH`|只能做 price cross-check，不能解决 historical constituents|

## 解释边界

公开页面和 vendor 文档只能作为 due diligence input。进入 model-ready breadth 之前，
仍需要 trial sample、license review、daily snapshot export proof、schema validation、
PIT audit 和 local cache evidence。

本 registry 不允许自动购买、自动升级、恢复 first-layer 或产生 promotion evidence。
