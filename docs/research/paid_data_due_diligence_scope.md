# Paid Data Due Diligence Scope

本批状态：`PAID_DATA_DUE_DILIGENCE_SCOPE_READY`

## 范围

本批只做 paid data due diligence，不购买数据、不升级 provider plan、不恢复
first-layer channel research、不训练模型、不进入 promotion、paper-shadow、production
或 broker。

当前依据是 post-2085 结果：free feature re-ablation 只有 diagnostic evidence，
participation proxy 不是 true PIT breadth，first-layer reopen gate 仍为
`FIRST_LAYER_REOPEN_DENIED`。因此下一步不是继续调模型，而是确认是否存在可审计、
成本可控、可本地自动化的 historical constituents / daily membership 数据源。

## 必答问题

1. 是否能提供 Nasdaq-100 / QQQ-like historical constituents。
2. 是否包含 delisted securities，避免 survivorship bias。
3. 是否能构建 daily membership snapshot。
4. 是否允许 Python / API / 本地数据库自动化接入。
5. 是否覆盖 `2021-02-22` 起的 primary research window。
6. 是否能生成 model-ready breadth / participation / concentration features。
7. 是否明显优于当前 participation proxy。
8. 是否值得进入 owner-approved trial。

## 明确禁止

- 不把 participation proxy 当作 true breadth。
- 不把 vendor marketing statement 当作 PIT evidence。
- 不用 current constituents backfill 构造历史 membership。
- 不输出 target weights、trade advice、allocation 或 broker action。
- 不允许 due diligence 直接解锁 first-layer、v4、minimal forward diagnostic 或 promotion。
