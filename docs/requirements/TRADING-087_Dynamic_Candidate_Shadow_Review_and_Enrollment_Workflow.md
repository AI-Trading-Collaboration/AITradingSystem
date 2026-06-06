# TRADING-087 Dynamic Candidate Shadow Review and Enrollment Workflow

最后更新：2026-06-05

- 父任务：TRADING-087
- 优先级：P0
- 状态：VALIDATING
- owner：system + project owner
- 创建日期：2026-06-05
- 来源计划：`G:/Download/TRADING-083_to_TRADING-087_Two_Layer_Dynamic_ETF_Allocation_Strategy_Roadmap.md`

## 背景

TRADING-083 到 TRADING-085 建立 two-layer dynamic ETF allocation 的 signal、
allocation 和 candidate batch/cache workflow；TRADING-086 用真实 ETF price returns
生成 dynamic robustness evidence。TRADING-087 负责把 dynamic robustness 证据转成
owner-reviewed forward shadow observation workflow，回答哪些 dynamic candidates
可以进入前向 shadow tracking。

本任务不是 promotion 或 production baseline replacement。只有 owner 明确批准、且
dynamic calibration validation、dynamic robustness validation、data quality gate 和
operations validation 均可审计通过的 dynamic candidate，才允许生成 forward shadow
enrollment / tracking records。任何缺失前置条件都必须 fail closed 或标记为
`needs_more_data` / `review_required`，不得自动 enroll。

## 安全边界

所有 TRADING-087 输出必须固定：

```text
observe_only = true
candidate_only = true
production_effect = none
broker_action = none
manual_review_required = true
production_state_mutated = false
baseline_config_mutated = false
official_target_weights_mutated = false
automatic_candidate_promotion = false
auto_enrollment_without_owner_approval = false
```

禁止：

```text
production_weight_update
baseline_config_mutation
official_target_weights_write
broker_order
automatic_candidate_promotion
auto_enrollment_without_owner_approval
```

## 子任务拆解

|子任务|状态|验收摘要|
|---|---|---|
|TRADING-087A Dynamic Shadow Review Policy|BASELINE_DONE|新增 dynamic shadow policy config，显式定义前置 gate、owner approval、enrollment limit、tracking metrics、weekly review threshold 和 safety fields。|
|TRADING-087B Dynamic Candidate Review Package|BASELINE_DONE|从 latest dynamic robustness/calibration/validation evidence 构建 owner review package，展示 candidate readiness、blocking evidence、source links 和 safety boundary。|
|TRADING-087C Owner Approval Capture|BASELINE_DONE|记录 owner decision、rationale、confidence、decision journal link 和 conditions；不允许 production/broker decisions。|
|TRADING-087D Approved Dynamic Candidate Enrollment|BASELINE_DONE|仅 `approved_for_dynamic_shadow` 且前置 gate 通过的 candidate 可生成 enrollment record；未批准 candidate fail closed。|
|TRADING-087E Dynamic Forward Tracking Records|BASELINE_DONE|为已 enroll candidate 生成 forward tracking records，包含 dynamic/static/current/benchmark return、drawdown、turnover、regime switches、false signal 和 constraint hits。|
|TRADING-087F Daily Dynamic Forward Update|BASELINE_DONE|新增 daily update command，从 cached prices 生成只读 forward update；数据依赖命令必须先执行 `aits validate-data` 等价门禁。|
|TRADING-087G Weekly Dynamic Shadow Review|BASELINE_DONE|新增 weekly review report，按 active_shadow / needs_more_data / watch / reject_pending_review / rejected / archived 汇总。|
|TRADING-087H Decision Journal Integration|BASELINE_DONE|approval、enrollment 和 weekly review 都保留 decision journal reference 或 proposed journal entry，不自动写 production decision。|
|TRADING-087I Evidence Dashboard Integration|BASELINE_DONE|dynamic shadow package/enrollment/update/weekly review 必须在 evidence dashboard 或 report registry evidence sources 中可见。|
|TRADING-087J Reader Brief Dynamic Shadow Section|BASELINE_DONE|Reader Brief 只读 latest dynamic shadow artifacts，展示 approval/enrollment/tracking/weekly status 和 safety。|
|TRADING-087K Dynamic Shadow Validation Gate|BASELINE_DONE|新增 `aits etf dynamic-shadow validate`，fail closed 校验 A-J workflow 和 safety boundary。|

## 设计约束

1. Enrollment 的必要条件是 owner approval captured；任何自动 approval、auto-enrollment 或 production mutation 都必须被 validation gate 阻断。
2. Review package 可以展示 `review_required` candidate，但 enrollment 必须检查 required gates、owner decision、safety fields 和 source artifact links。
3. Forward update 使用 cached price data 时必须运行 `aits validate-data` 等价质量门禁并在输出披露 status / report path。
4. Forward tracking 只用于 observation，不能写 `data/etf_portfolio/target_weights.csv`、production baseline config、broker state 或 shared production registry。
5. Weekly review 动作只允许 continue observation、needs more data、watch、reject pending owner review、reject 或 archive；promotion 和 broker action 不属于本任务。
6. 所有 thresholds、minimum observations、watch/reject triggers、approval decisions 和 tracking metrics 必须来自 `config/etf_portfolio/dynamic_shadow.yaml` 或作为 schema invariant 明确记录。
7. Runtime artifacts 写入 ignored `reports/` / `data/simulation/` 路径；源码变更只包含 policy/config、module、CLI、Reader Brief、dashboard/registry、docs 和 tests。

## 验收命令

完成后至少运行：

```bash
python -m pytest tests/test_etf_dynamic_shadow.py tests/test_etf_dynamic_robustness.py tests/test_reader_brief.py tests/test_report_index.py -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf dynamic-shadow validate
```

如最终 CLI 名称不同，必须同步更新本文件、`docs/task_register.md` 和
`docs/system_flow.md`。

## 进展记录

- 2026-06-05: 新增并进入 IN_PROGRESS。基于 TRADING-083_to_TRADING-087 roadmap、TRADING-086 robustness handoff 和 owner-approved-only safety boundary，开始实现 dynamic shadow review package、owner approval capture、approved-only enrollment、forward tracking records、daily update、weekly review、decision journal/evidence dashboard/Reader Brief integration 和 validation gate。本阶段不允许 production mutation、broker action、automatic promotion 或 auto-enrollment without owner approval。
- 2026-06-05: A-K baseline 实现完成并转入 VALIDATING。新增 dynamic shadow policy、`aits etf dynamic-shadow package/approve/enroll-approved/update/weekly-review/validate`、owner-approved-only enrollment gate、dynamic shadow registry、forward tracking metrics、weekly review statuses、decision journal reference/proposed entries、Reader Brief `Dynamic Shadow Review` section、Strategy Evidence Dashboard `dynamic_shadow` optional source、report registry/artifact catalog/system flow/runbook/README integration 和 focused tests。验证：`tests/test_etf_dynamic_shadow.py tests/test_etf_dynamic_robustness.py tests/test_reader_brief.py tests/test_report_index.py -q` 共 19 passed；`aits etf dynamic-shadow validate` 为 PASS；`aits etf evidence-dashboard validate --as-of 2026-06-05` 为 PASS；ruff、compileall、diff check 通过；真实 latest package smoke 生成 `dynamic-shadow-package_4f8d09abd136.json/md`，`status=OWNER_REVIEW_REQUIRED`、`top_candidate=dynamic-candidate-pack_8904048b4dbd`、`ready_after_owner_approval_count=1`、`blocked_count=0`、`automatic_enrollment_allowed=false`；全量 `python -m pytest tests -q` 通过，结果为 2157 passed、330 warnings、耗时 645.98s。剩余条件是真实 owner approval 后的 enrollment/update/weekly observation 运行复核。
