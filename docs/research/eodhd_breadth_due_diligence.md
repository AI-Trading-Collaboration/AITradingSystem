# EODHD Breadth Due Diligence

YAML：`inputs/research_reviews/eodhd_breadth_due_diligence.yaml`

## 当前判定

`EODHD_CONDITIONAL_TRIAL_CANDIDATE`

EODHD 作为低成本替代源值得保留在候选池中，但当前还没有足够 evidence 证明它能满足
Nasdaq-100 / QQQ-like daily membership、delisted membership 和 PIT known-at 契约。

## Trial 前必须确认

- historical constituents 是否覆盖 Nasdaq-100 / S&P / Dow / 其他 universe。
- 是否包含 delisted data 且可与 membership 关联。
- membership snapshot 是否能按日期查询。
- 是否能区分 effective date、reported date、known-at / revision policy。
- API plan、成本、速率限制和 local cache 权限。

## 当前用途

在 sample 和 license 未验证前，EODHD 只能作为 conditional trial candidate，不得进入
model-ready breadth、promotion evidence 或 first-layer reopen gate。
