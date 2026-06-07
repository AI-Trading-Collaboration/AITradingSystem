# TRADING-126 to TRADING-130 Shadow Shortlist and Position Advisory Readiness

## 背景

TRADING-121_to_125 已基于 medium_real sweep `sweep_20260607T102300Z_ae5ae1d8`
完成 evidence gate diagnosis、gate impact simulation、policy calibration、candidate recovery
和 observe pool rebuild。当前可审计结果为：

- `observe_pool_id=01f0f0056e78f293`
- 300 个 `observe_only` / `manual_review_required` candidates
- 0 个 hard blocked candidates
- `GO_WITH_LIMITS`
- 不存在 production candidate，不存在 broker action

剩余问题是 300 个候选太多，全部需要人工 review，无法直接进入长期 shadow monitoring，
更不能直接解释成实际仓位控制建议。本阶段需要把候选压缩为少量 shadow shortlist，
并建立“目标权重 -> 实际仓位建议”的人工复核前置层。

## 子任务拆分

|Task|Title|Status|Acceptance summary|
|---|---|---|---|
|TRADING-126|Observe Pool Pruning and Shadow Shortlist Selection|VALIDATING|从 observe pool 生成 5-20 个 shortlist candidates，hard fail candidate 不进入 shortlist，selected rows 必须有 selection reasons。|
|TRADING-127|Candidate Diversity Clustering and Representative Selection|VALIDATING|输出 parameter / weight path / metric similarity matrices、candidate clusters 和 cluster representatives。|
|TRADING-128|Shadow Shortlist Monitoring Pack|VALIDATING|从 shortlist + cluster representatives 生成 shadow shortlist，每个候选包含 monitoring requirements，不写 production。|
|TRADING-129|Target Weight to Practical Position Advisory|VALIDATING|支持 TARGET_ONLY 和 manual snapshot delta 两种 advisory；固定 `broker_action_allowed=false`、`owner_approval_required=true`。|
|TRADING-130|Position Advisory Review Pack and Go/No-Go Decision|VALIDATING|整合 shortlist、cluster、shadow shortlist 和 advisory，输出 owner review pack 与 go/no-go decision，`production_readiness=NOT_READY`。|

## 设计边界

本阶段不做自动下单、不生成 broker action、不生成 production candidate、不执行 owner approval、
不写 official target weights、不修改 baseline config 或 production state。所有新增输出仍是
research / observe-only / manual-review artifacts。

## 关键策略

1. shortlist 不按单一 score 选择，而综合 performance、risk、evidence、regime、stability 和 diversity。
2. hard fail candidate fail closed：`data_quality=FAIL`、`date_range_status` 为 `FAIL` 或
   `INSUFFICIENT_DATA`、缺 real evaluation artifact、缺 daily weights、`overfit_status=HIGH_RISK`、
   `tech_semiconductor_relevance=LOW` 或已生成 production candidate 的候选不得进入 shortlist。
3. `PASS_WITH_WARNINGS`、`RECONSTRUCTED_MANIFEST`、`PARTIAL`、`REVIEW_REQUIRED` 和
   regime warning 可进入 shortlist，但必须保留 `manual_review_required=true`。
4. clustering 第一版使用可解释的参数距离、daily weight path average distance 和 metric 距离；
   daily weights 缺失时标记 `INCOMPLETE`，不得伪造 path similarity。
5. shadow shortlist 只选择 cluster representatives，用于后续 daily / weekly monitoring，不写 shadow
   registry，不触发已有 shadow enrollment。
6. position advisory 只把 candidate target weights 转换为 owner-review 建议；没有当前持仓 snapshot 时
   输出 `TARGET_ONLY`，有 snapshot 时输出 delta 与限制检查。
7. candidate disagreement 必须触发 manual review；position review 最终仍保持
   `production_readiness=NOT_READY`。

## 新增 CLI

- `aits etf dynamic-v3-rescue shortlist build`
- `aits etf dynamic-v3-rescue shortlist report --latest`
- `aits etf dynamic-v3-rescue validate-shortlist --shortlist-id <shortlist_id>`
- `aits etf dynamic-v3-rescue candidate-cluster run`
- `aits etf dynamic-v3-rescue candidate-cluster report --latest`
- `aits etf dynamic-v3-rescue validate-candidate-cluster --cluster-id <cluster_id>`
- `aits etf dynamic-v3-rescue shadow-shortlist build`
- `aits etf dynamic-v3-rescue shadow-shortlist report --latest`
- `aits etf dynamic-v3-rescue validate-shadow-shortlist --shadow-shortlist-id <shadow_shortlist_id>`
- `aits etf dynamic-v3-rescue position-advisory run`
- `aits etf dynamic-v3-rescue position-advisory report --latest`
- `aits etf dynamic-v3-rescue validate-position-advisory --advisory-id <advisory_id>`
- `aits etf dynamic-v3-rescue position-review pack`
- `aits etf dynamic-v3-rescue position-review report --latest`
- `aits etf dynamic-v3-rescue validate-position-review --review-id <review_id>`

## 新增配置

- `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`
- `config/etf_portfolio/dynamic_v3_rescue/current_portfolio_snapshot.example.yaml`

`position_advisory_v1.yaml` 中的 adjustment limits、agreement threshold 和 review rules 是
pilot baseline，用于防止本阶段把 target weights 误读为可执行交易指令。完成一轮 owner review 后，
这些阈值需要迁移或复核为 owner-reviewed policy。

## 新增 artifacts

- `reports/etf_portfolio/dynamic_v3_rescue/shortlist/<shortlist_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/candidate_cluster/<cluster_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/shadow_shortlist/<shadow_shortlist_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/position_advisory/<advisory_id>/`
- `reports/etf_portfolio/dynamic_v3_rescue/position_review/<review_id>/`

## 验收命令

```bash
aits etf dynamic-v3-rescue shortlist build --observe-pool-id 01f0f0056e78f293 --target-size 10 --max-size 20
aits etf dynamic-v3-rescue candidate-cluster run --shortlist-id <shortlist_id>
aits etf dynamic-v3-rescue shadow-shortlist build --shortlist-id <shortlist_id> --cluster-id <cluster_id>
aits etf dynamic-v3-rescue position-advisory run --shadow-shortlist-id <shadow_shortlist_id> --config config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml
aits etf dynamic-v3-rescue position-review pack --shortlist-id <shortlist_id> --cluster-id <cluster_id> --shadow-shortlist-id <shadow_shortlist_id> --advisory-id <advisory_id>
aits etf dynamic-v3-rescue validate-shortlist --shortlist-id <shortlist_id>
aits etf dynamic-v3-rescue validate-candidate-cluster --cluster-id <cluster_id>
aits etf dynamic-v3-rescue validate-shadow-shortlist --shadow-shortlist-id <shadow_shortlist_id>
aits etf dynamic-v3-rescue validate-position-advisory --advisory-id <advisory_id>
aits etf dynamic-v3-rescue validate-position-review --review-id <review_id>
aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue
```

Focused tests must cover shortlist hard exclusions, score breakdown, clustering representatives,
weight path incomplete handling, shadow shortlist build, TARGET_ONLY advisory, snapshot delta advisory,
candidate disagreement manual review, no broker action, position review go/no-go, and Reader Brief
integration.

## 进展记录

- 2026-06-08：任务创建并进入 IN_PROGRESS。首版实现目标为 P0：完整 CLI/artifact/validate
  contract、TARGET_ONLY 和 example snapshot delta 均可运行，focused tests / ruff / compileall /
  git diff check PASS；聚类算法和 advisory explanation 先保持可解释基础版。
- 2026-06-08：实现完成并转入 VALIDATING。真实链路基于 rebuilt observe pool
  `01f0f0056e78f293` 输出 shortlist `b2adc39c5a098b3a`、candidate cluster
  `28f42fe8a8530c93`、shadow shortlist `4378b3ed3fc1be41`、TARGET_ONLY advisory
  `240a09fa911aac38`、snapshot advisory `db4ba22c3aad20fa` 和 position review
  `d884c2bcd7436afd`；position review 仍为 `production_readiness=NOT_READY`，
  advisory 仍为 `broker_action_allowed=false` / `owner_approval_required=true`。
  验证通过 focused pytest、existing dynamic-v3 tests、documentation contract、全量 pytest
  （2234 passed）、ruff、compileall、git diff check、dynamic-v3 family artifact validation
  和 root validation。下一步是 owner 人工复核 shortlist / advisory / review artifacts，
  不是自动 approval 或 broker action。
