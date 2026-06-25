# TRADING-1024～1030 Layer-1 Archive And Equal-Risk Forward-Aging Stabilization

最后更新：2026-06-25

## 背景

TRADING-1015～1023 已完成 Layer-1 low-turnover selector final gate。真实结果为
`KEEP_SELECTOR_DRY_RUN_ONLY_AND_CONTINUE_EQUAL_RISK_FORWARD_AGING`：最佳低换手候选
`min_holding_soft_blend` 只优于 `always_equal_risk`，不优于 `always_100_qqq`，且
`switch_count_controlled` 未通过。

本阶段目标是正式归档 Layer-1 simple-rule selector 研究线，并稳定当前主线
`equal_risk_qqq_sgov` forward-aging。所有输出继续保持：

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`

## 范围

本批任务覆盖 7 个 CLI/report：

|任务|命令|主要输出|
|---|---|---|
|TRADING-1024|`aits research strategies layer1-selector-dry-run-archive-report`|Layer-1 selector dry-run-only 归档报告|
|TRADING-1025|`aits research strategies layer1-selector-restart-condition-contract`|未来重启 Layer-1 selector 的条件合约|
|TRADING-1026|`aits research strategies equal-risk-forward-aging-daily-run-health-check`|equal-risk forward-aging observation 链路健康检查|
|TRADING-1027|`aits research strategies equal-risk-forward-aging-maturity-update-check`|maturity updater 包装检查与原始 observation 不变性审计|
|TRADING-1028|`aits research strategies equal-risk-forward-aging-scoreboard-first-window-review`|scoreboard 首窗 pending/insufficient 审查|
|TRADING-1029|`aits research strategies layer2-growth-component-gap-review`|Layer-2 growth component 缺口审查|
|TRADING-1030|`aits research strategies research-roadmap-master-review`|研究路线 master review|

## 非目标

- 不新增 Layer-1 selector。
- 不启动 ML selector。
- 不扩大 200DMA 变体搜索。
- 不把 selector 加入 forward-aging watchlist。
- 不恢复 QQQ-plus growth selectable。
- 不恢复 tail-risk fallback、TQQQ-heavy、LEAPS、Wheel 或 Options。
- 不进入 paper-shadow、production、broker 或真实交易建议。

## 实施顺序

1. 登记本任务并创建本需求文档。
2. 新增统一 stabilization report builder，复用既有 Layer-1 final gate、simple baseline forward-aging、Layer-2 component pool 和 report artifact 写入约定。
3. 注册 7 个 CLI。
4. 更新 report registry、artifact catalog、system flow 和 focused tests。
5. 运行指定验证；验证通过后按本地提交规则提交/推送。

## 验收标准

- 7 个 CLI 均可生成 JSON/Markdown artifacts，1024 与 1030 同步写入 `docs/research/`。
- Report registry 新增 7 个 report entries，均为 `artifact_selection_policy=latest_available`、`required_for_daily_reading=false`、`production_effect=none`、`broker_action=none`。
- Artifact catalog 明确 producer command、schema contract、status enum、research-only safety note 和 owner next action。
- `docs/system_flow.md` 记录 1024～1030 的数据流和安全边界。
- Focused tests 覆盖新增 CLI、registry 条目、safety flags、scoreboard insufficient/pending 行为和 Layer-2 growth gap 结论。
- 通过附件指定验证命令。

## 进展记录

- 2026-06-25：新增任务文档；状态 `IN_PROGRESS`。实现不得改变 production config，不得写 broker/order/paper-shadow 状态。
- 2026-06-25：实现完成并转入 `VALIDATING`。新增 7 个 CLI/report、report registry entries、artifact catalog、system flow 和 focused tests；1025 的 mature observation floor 读取既有 `simple_baseline_forward_aging_contract_v1` policy，不在新报告中硬编码解释性阈值；真实 `research-roadmap-master-review` 生成 `docs/research/layer1_selector_dry_run_archive_report.md` 与 `docs/research/research_roadmap_master_review.md`，结论为继续 `equal_risk_qqq_sgov` forward-aging、暂停 Layer-1 selector、回到 Layer-2 growth research；所有 safety fields 仍为 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`、`manual_review_required=true`。
- 2026-06-25：验收验证通过：`python -m ruff check src tests`、`python -m compileall -q src tests`、Layer-1 focused pytest、Layer-2 focused pytest、task/report/documentation pytest 和 `git diff --check`。
