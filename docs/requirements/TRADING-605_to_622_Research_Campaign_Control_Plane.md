# TRADING-605 to 622 Research Campaign Control Plane

最后更新：2026-06-19

## 背景

B2/B3/B4 多轮研究已经证明，继续为每个 case 编写 task-specific runner 会让研究状态、任务编号、证据记录和决策日志耦合。TRADING-605 to 622 的目标是把研究推进方式收敛为配置驱动、状态机驱动、证据预算约束的通用 Research Campaign 控制面。

默认市场 regime 为 `ai_after_chatgpt`，anchor event 为 ChatGPT public launch on 2022-11-30，默认研究结论窗口从 2022-12-01 开始。所有 Campaign 输出必须披露实际请求日期范围、数据质量状态、research-only safety boundary 和 source artifacts。

## 阶段拆解

|阶段|任务|状态|验收重点|
|---|---|---|---|
|Phase A|TRADING-605 to 608|VALIDATING|Campaign spec/schema、module capability registry、Evidence Record、window/holdout policy 均可验证。|
|Phase B|TRADING-609 to 612|VALIDATING|消融/交互矩阵、状态机、evidence budget/stop rule、gate policy 均配置驱动。|
|Phase C|TRADING-613 to 616|VALIDATING|标准 stage runner、evidence store/lineage、next-action planner、reproducibility manifest 可用；计算型 stage adapter 未配置时 fail-closed。|
|Phase D|TRADING-617 to 619|VALIDATING|统一 CLI、canonical status、Campaign Reader Brief / owner packet 可用；周期调度仅记录边界，未接入 daily scheduler 自动执行。|
|Phase E|TRADING-620 to 622|VALIDATING|B2/B3 历史状态迁移、旧流程兼容/弃用说明、control-plane smoke/release note 已完成。|

## 设计决策

- 新增 `Research Campaign` 作为研究工作流单元，和工程 task 分离。
- 状态拆分为 `stage + outcome + reason_codes`，避免继续创建 B2/B3 case-specific 主状态。
- 每个 Campaign 固定 `evidence_budget`，`NEEDS_MORE_EVIDENCE` 达到预算后必须转为受限决策集。
- 通用 runner 可以执行 scope/precheck/gate/packet/status 等控制面阶段；需要真实计算的 diagnostic/backfill 阶段必须通过显式 adapter 或导入 evidence，不能静默伪造计算结果。
- B2/B3 迁移只导入既有 evidence 和状态，不重跑、不调参、不访问 untouched holdout。

## 安全边界

- `research_only=true`
- `manual_review_only=true`
- `official_target_weights=false`
- `paper_shadow_allowed=false`
- `broker_effect=none`
- `order_effect=none`
- `production_effect=none`
- owner packet 不自动 append owner decision
- holdout 只允许 `FINAL_GATE + OWNER_AUTHORIZATION`

## 进行中验收清单

- `aits research campaign validate --spec ...` 能验证 B2/B3 sample specs。
- `aits research campaign init --spec ...` 能创建 campaign state、evidence store、transition audit 和 reproducibility manifest。
- `aits research campaign plan/status/diagnose/gate/packet/archive` 能从同一入口读取状态和输出固定结构。
- Module registry 能阻断 P0 mixed allocator 冒充单模块、Signal 模块输出权重、Evaluation 模块修改策略。
- Window/holdout policy 能 fail-closed 阻断未授权 holdout 访问。
- Experiment design planner 能生成 E/R/T 主效应与两两交互，未批准前不生成 C/G 高阶组合。
- B2 迁移状态可表示为 `TARGETED_EVIDENCE + NEEDS_MORE_EVIDENCE + reason_codes`，并输出受预算约束的 next actions。
- B3 迁移保持 signal-precheck mixed，不生成权重或 backfill。
- 旧 B2/B3 task-specific CLI 暂保留，但新功能只进入 Campaign 控制面。

## 进展记录

- 2026-06-19: 新增任务登记和本需求文档，开始实现 v1 control plane。
- 2026-06-19: v1 control plane 实现完成并转入 VALIDATING；新增 `aits research campaign ...` CLI、schema/validator、module registry、window/holdout policy、gate policy、state machine、evidence budget、experiment matrix、evidence store、transition audit、reproducibility manifest、next-action planner、owner packet、B2/B3 config-driven migration、compatibility/deprecation policy、system flow、artifact catalog 和 release note。验证通过 focused pytest 17 passed、scoped Ruff、compileall、CLI validate smoke、`git diff --check` 和 task-register terminal-status check。已知限制：diagnostic/backfill 类 stage 尚未接入真实 adapter，当前按 no-silent-workaround 要求 fail-closed 为 `STAGE_ADAPTER_NOT_CONFIGURED`。
