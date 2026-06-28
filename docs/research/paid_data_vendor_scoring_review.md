# Paid Data Vendor Scoring Review

CSV：`outputs/research_trends/paid_data_due_diligence/vendor_scoring_matrix.csv`

## 评分口径

每个 category 归一到 0～20 分，总分 0～100：

- coverage
- PIT safety
- engineering
- cost
- research value

推荐解释：

- `>=80`: `TRIAL_RECOMMENDED`
- `60-79`: `CONDITIONAL_TRIAL_IF_COST_ACCEPTABLE`
- `40-59`: `DIAGNOSTIC_ONLY_OR_DEFERRED`
- `<40`: `NOT_RECOMMENDED`

## 当前排序

|Vendor|Total|Recommendation|
|---|---:|---|
|Norgate US Stocks Platinum|84|`TRIAL_RECOMMENDED`|
|EODHD historical constituents / delisted data|67|`CONDITIONAL_TRIAL_IF_COST_ACCEPTABLE`|
|QuantConnect / AlgoSeek US equities|53|`DIAGNOSTIC_ONLY_OR_DEFERRED`|
|FMP ETF holdings / historical holdings|47|`DIAGNOSTIC_ONLY_OR_DEFERRED`|
|Price-only sources|45|`DIAGNOSTIC_ONLY_OR_DEFERRED`|

## 解释

Norgate 得分最高，但 recommendation 只等于 owner review 的 trial 候选，不等于购买。
FMP 当前 key 在 holdings probe 上返回 402，且 PIT fields 未验证。EODHD 可作为成本敏感
替代方案继续确认。QuantConnect / AlgoSeek 的 membership 和 local export 成本未明。
Price-only sources 不能解决 true breadth。
