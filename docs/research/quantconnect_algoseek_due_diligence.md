# QuantConnect / AlgoSeek Due Diligence

YAML：`inputs/research_reviews/quantconnect_algoseek_due_diligence.yaml`

## 当前判定

`QUANTCONNECT_DEFERRED`

QuantConnect / AlgoSeek 可能提供 survivorship-bias-free equity data 和 delisted
handling，但当前关键问题是 Nasdaq-100 historical membership 是否自然可用、是否能本地
导出、是否必须依赖 LEAN 生态，以及接入当前 repo 的成本是否过高。

## 必须确认

- 是否能本地导出 security master / delisted evidence。
- 是否包含 Nasdaq-100 historical membership；若没有，是否仍需额外 vendor。
- 是否允许把数据缓存到本地并生成派生 feature。
- 是否可用 Python / CLI 自动跑 daily membership snapshot。
- 是否需要 LEAN cloud/local runtime，集成成本是否可控。

## 当前用途

本批不优先选择 QuantConnect / AlgoSeek trial。除非 Norgate / EODHD 不可用且 owner
愿意承担 ecosystem spike，否则保持 deferred。
