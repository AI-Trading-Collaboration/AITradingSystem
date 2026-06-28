# Norgate Due Diligence Checklist

YAML：`inputs/research_reviews/norgate_due_diligence_checklist.yaml`

## 当前判定

`NORGATE_TRIAL_RECOMMENDED_OWNER_APPROVAL_REQUIRED`

Norgate 是本批唯一 Tier A true breadth 候选。公开资料显示它最可能同时覆盖
historical constituents、delisted securities 和本地研究工作流所需的数据形态，但
这些仍必须通过 owner-approved trial 亲自验证，不能直接视为 PIT proof。

## Trial 前必须检查

- 是否包含 Nasdaq-100 historical constituents。
- 是否包含 delisted securities。
- 是否能查询某股票在某日是否属于某指数。
- Python API 是否能自动访问 membership。
- 是否能导出 daily membership snapshot。
- 是否允许 local cache 和派生 feature。
- 覆盖是否早于 `2021-02-22`。
- 授权是否允许个人研究、本地存储、校验和、派生特征。
- 成本、取消条款和 trial 可得性是否可接受。

## 不自动购买

本批只建议 owner review Norgate trial，不批准购买。试用、订阅、下载样本、缓存样本
都需要 owner manual approval。
