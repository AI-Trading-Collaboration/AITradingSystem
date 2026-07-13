# TRADING-174 to TRADING-178 Forward Confirmation Execution and Rule Review Cycle

最后更新：2026-06-10

## 1. 背景

TRADING-169 to TRADING-173 已生成 simulation advisory review 和 forward confirmation plan。
最新真实链路包括：

- interpretation_id：`a629c036f1ea3129`
- risk_return_id：`c61b1b9ca357cba1`
- defensive_validation_id：`b79486b62042b702`
- proposal_review_id：`f5dc442131f3740c`
- confirmation_plan_id：`808e55a74ca6951f`

当前Plan在G2.4BX后只允许真实proposal解锁且存在于validated Forward Bridge的targets；
fixture当前只有`limited_adjustment_vs_no_trade`，不得继续假设固定三target集合。Plan仍是静态
artifact。系统还缺少target registry、progress tracking、success/failure evaluation、
rule review cycle和owner decision journal。

## 2. 编号说明

附件原始标题使用 `TRADING-174` 到 `TRADING-178`。当前任务登记表中 `TRADING-174` 已被
`Full Pytest Runtime and Validation Tier Optimization` 使用并处于 VALIDATING。为避免 task id
复用，本轮登记和文档使用唯一汇总 ID：

`TRADING-174_to_178_FORWARD_CONFIRMATION_CYCLE`

本文仍保留附件原始编号范围，便于追溯 owner 提供的开发计划。

## 3. 阶段目标

把 `forward_confirmation_plan` 从静态计划升级为持续跟踪闭环：

1. 注册 active/watch-only confirmation targets。
2. 每次 forward outcome、outcome dashboard 或 weekly review 更新后聚合 target progress。
3. 在样本达到要求时评估 success criteria / failure conditions；样本不足时保持 NOT_READY。
4. 输出 rule review cycle report，作为每周人工复核材料。
5. 记录 owner 对 rule proposal 的结构化 decision，但不自动改配置。

## 4. 阶段拆解

|原始编号|名称|状态|验收重点|
|---|---|---|---|
|TRADING-174|Forward Confirmation Target Registry|VALIDATING|从 `confirmation_plan_id` 注册 targets；`target_id` 唯一；`auto_apply=false`；`owner_approval_required=true`。|
|TRADING-175|Confirmation Progress Tracker|VALIDATING|聚合 limited-vs-notrade、consensus-risk 和 available outcome evidence；样本不足不得 READY。|
|TRADING-176|Success / Failure Condition Evaluator|VALIDATING|只在 progress 达到要求后判断 SUCCESS/FAILURE；单指标 pass 不得判定 SUCCESS。|
|TRADING-177|Rule Review Cycle Report|VALIDATING|汇总 registry/progress/evaluation；默认 `policy_change_allowed=false`；输出 Reader Brief section。|
|TRADING-178|Owner Decision Integration|VALIDATING|创建和记录 owner decision journal；`auto_apply=false`、`broker_action_allowed=false`、`production_effect=none`。|

## 4.1 ARCH-004G2.4BY Registry contract

- Register必须先调用同一Plan validator并要求Plan manifest status=`AVAILABLE`，再检查timezone-aware cutoff；
- snapshot冻结Plan full bundle/validation/lineage、materialized registry path与写入前preimage；
- 只逐值注册Plan真实targets，不补造固定三target、priority、events、windows、criteria或failure；
- source `TRACKING_REQUIRED`严格映射`active`，其他未知status阻断；同一Plan重复注册阻断；
- artifact与materialized YAML必须使用canonical atomic writer，全部views可从snapshot逐字节重算；
- 本slice不运行TRADING-175及后续链，不修改投资policy/portfolio/production/broker。

2026-07-13完成：4 callback已迁canonical；validated/AVAILABLE/cutoff Plan full bundle、registry preimage/postimage、source-exact target projection、duplicate Plan gate、atomic materialized registry及全view/live-source validator通过540 focused、260 architecture、203 contract。当前fixture只注册1个limited target；这只表示manual tracking registry已建立，不是forward progress或success。

## 4.2 ARCH-004G2.4BZ Progress contract

- Update必须先要求指定Registry content-derived validation=`PASS`且其generated time不晚于本次cutoff；
- 只按Registry真实target选择所需evidence source；source按`generated_at/id`确定性选择，不使用mtime，且必须通过其content-derived validator；
- `confirmation_progress_input_snapshot.v2`冻结Registry与selected evidence的full bundles、validation payload、path/checksum和selection inventory；
- events/windows/criteria不设fallback，逐值继承Registry；target或source adapter未知、重复event identity、future/invalid evidence均在写件前阻断；
- available count使用unique event unit，不能把同一event的多个window相加；每个window独立披露AVAILABLE finite覆盖；
- defensive pressure evidence只有真实pressure tag时才计数；缺source、缺window、缺pressure tag或指标不可定义必须保持null/`INSUFFICIENT_EVENTS`，不得用0冒充观察结果；
- validator重验live Registry/evidence并重算snapshot、progress rows、summary、manifest和Markdown bytes；
- 本slice不运行TRADING-176及后续链，不修改投资policy/portfolio/production/broker。

2026-07-13完成：3 callback已迁canonical；Registry与selected evidence content-derived validation、cutoff-bound deterministic selection、full bundle/validation snapshot、source-exact windows/events、cross-window conservative unique count、missing-null/pressure-tag insufficiency和全部JSON/JSONL/Markdown byte validation通过。旧mutable latest、10/5 default和80% near-ready路径已删除。当前Registry只有1个limited target且fixture没有AVAILABLE event，因此输出0 ready和`INSUFFICIENT_EVENTS`；这不是failure或rule-change信号。

## 4.3 ARCH-004G2.4CA Evaluation contract

- Run必须先要求指定Progress content-derived validation=`PASS`且Progress generated time不晚于本次timezone-aware cutoff；
- `confirmation_evaluation_input_snapshot.v2`冻结Progress full JSON/JSONL/Markdown bundle、validation、path/id和registry lineage；
- Progress非`READY_FOR_EVALUATION`时不做criteria/failure判定：criteria逐项`NOT_EVALUATED`、actual=null，failure trigger为空，evaluation=`NOT_READY`；
- READY时success criteria必须非空唯一且仅使用source `_min/_max` operator；actual只接受finite metric；全部criteria PASS且无failure trigger才SUCCESS，任一FAIL或trigger才FAILURE，missing/unknown为REVIEW_REQUIRED；
- Failure conditions按Registry/Progress冻结的list逐条读取，condition/action不得丢失；condition adapter的boundary引用对应source criterion required值，不设置独立0或default，unknown condition写件前阻断；
- validator重验live Progress并重算snapshot、target evaluations、summary、manifest和Markdown bytes；
- 本slice不运行TRADING-177及后续链，不修改投资policy/portfolio/production/broker。

2026-07-13完成：3 callback已迁canonical；Progress full validation/cutoff snapshot、NOT_READY零partial evaluation、READY criteria/failure严格判定、unknown failure阻断和全部JSON/JSONL/Markdown byte validation通过。当前source-backed fixture只有1个NOT_READY limited target，输出0 success/0 failure；这是样本未成熟，不是规则失败。首次并发验证的parallel worker因内存竞争失败，保留artifact后仍用16-worker parallel、按重型文件资源隔离重跑通过。

## 4.4 ARCH-004G2.4CB Rule Review contract

- Run必须先要求Registry、Progress、Evaluation三个content-derived validator=`PASS`，并校验timezone-aware generated cutoff、Registry→Progress→Evaluation id lineage、生成时间单调与三层target exact coverage；
- `rule_review_cycle_input_snapshot.v2`冻结三层bounded full-byte commitment bundles、计算必需的JSON/YAML/JSONL views、validation evidence、path/id与lineage；commitment必须覆盖每个canonical source file的path、size与SHA-256，validator逐文件重算，不得把上游input snapshot解析后递归嵌套；
- watch-only target固定`KEEP_REFERENCE_ONLY`；active NOT_READY=`CONTINUE_TRACKING`；MIXED/REVIEW_REQUIRED=`DEFER`；active SUCCESS/FAILURE只表示`READY_FOR_OWNER_REVIEW`，不得把review readiness误写成规则已批准；
- Matrix完整携带Progress状态、Evaluation criteria results、source failure conditions/triggers/actions；不得按target id另造RENAME/TIGHTEN结论或新threshold；
- report/Reader Brief分别披露progress-ready、success、failure、review-required，不再把SUCCESS数量误标为ready-for-evaluation；
- validator重验live三source并重算snapshot、decision matrix、manifest、Markdown与Reader Brief bytes；
- 本slice不创建/记录TRADING-178 Owner Decision，不修改投资policy/portfolio/production/broker。

2026-07-13实现审计发现：既有Registry/Progress/Evaluation“full bundle”把上游input snapshot同时作为parsed JSON和raw text递归嵌入，fixture分别膨胀到约0.89GB/2.37GB/6.50GB，CB再嵌套时并行测试在JSON序列化处`MemoryError`。这不是可接受的资源偶发或测试绕路。CB将BY/BZ/CA与本slice统一改为bounded source bundle：完整canonical文件集合只保存path/size/SHA-256 commitment，计算所需视图单独解析；validator仍重验上游content-derived PASS并逐文件比较live commitment，因此保留全字节漂移检测，同时把artifact大小从递归指数增长改为随文件数线性增长。首次失败的16-worker parallel证据保留，修复后仍以相同parallel配置验证。

2026-07-13完成：3 callback已迁canonical，三源validation/cutoff、lineage/chronology/exact coverage、bounded commitment snapshot、generic owner-review decision、source action保留、progress/evaluation分离统计和全部输出byte validation通过。四层snapshot约18KB/28KB/14KB/37KB，资源增长恢复为线性；累计focused 570、architecture 263、contract 203均以16-worker parallel通过。当前fixture仍为1个active limited target、Progress `INSUFFICIENT_EVENTS`、Evaluation `NOT_READY`，所以Cycle正确输出`CONTINUE_TRACKING`和0 owner action；这表示样本尚未成熟，不是自动否定规则。本slice未创建TRADING-178 Owner Decision，未修改policy/config/portfolio/production/broker。

## 5. 安全边界

- 不修改 `config/etf_portfolio/dynamic_v3_rescue/position_advisory_v1.yaml`。
- 不修改 policy、official target weights、baseline/production state、paper/real portfolio。
- 不生成 production candidate。
- 不触发 broker API 或自动下单。
- 不自动执行 owner approval。
- SUCCESS 只表示可以进入人工 rule review，不表示自动修改规则。
- `approve_manual_policy_review` 只表示允许人工配置 review，不表示自动应用配置。

## 6. Artifact 计划

新增 runtime artifact roots：

```text
reports/etf_portfolio/dynamic_v3_rescue/forward_confirmation_registry/<registry_id>/
reports/etf_portfolio/dynamic_v3_rescue/confirmation_progress/<progress_id>/
reports/etf_portfolio/dynamic_v3_rescue/confirmation_evaluation/<evaluation_id>/
reports/etf_portfolio/dynamic_v3_rescue/rule_review_cycle/<cycle_id>/
reports/etf_portfolio/dynamic_v3_rescue/rule_owner_decision/
```

Registry 也会写入 reviewable YAML：

```text
registry/etf_portfolio/dynamic_v3_rescue_forward_confirmation_targets.yaml
```

## 7. CLI 计划

```bash
aits etf dynamic-v3-rescue confirmation-targets register --confirmation-plan-id 808e55a74ca6951f
aits etf dynamic-v3-rescue confirmation-targets list
aits etf dynamic-v3-rescue confirmation-targets report --latest
aits etf dynamic-v3-rescue validate-confirmation-targets --registry-id <registry_id>

aits etf dynamic-v3-rescue confirmation-progress update --registry-id <registry_id>
aits etf dynamic-v3-rescue confirmation-progress report --latest
aits etf dynamic-v3-rescue validate-confirmation-progress --progress-id <progress_id>

aits etf dynamic-v3-rescue confirmation-evaluate run --progress-id <progress_id>
aits etf dynamic-v3-rescue confirmation-evaluate report --latest
aits etf dynamic-v3-rescue validate-confirmation-evaluate --evaluation-id <evaluation_id>

aits etf dynamic-v3-rescue rule-review-cycle run --registry-id <registry_id> --progress-id <progress_id> --evaluation-id <evaluation_id>
aits etf dynamic-v3-rescue rule-review-cycle report --latest
aits etf dynamic-v3-rescue validate-rule-review-cycle --cycle-id <cycle_id>

aits etf dynamic-v3-rescue rule-owner-decision create --cycle-id <cycle_id>
aits etf dynamic-v3-rescue rule-owner-decision list
aits etf dynamic-v3-rescue rule-owner-decision record --decision-id <decision_id> --decision continue_tracking
aits etf dynamic-v3-rescue rule-owner-decision report --latest
aits etf dynamic-v3-rescue validate-rule-owner-decision --decision-id <decision_id>
```

## 8. 验收标准

- 五段 CLI run/report/list/validate 可运行。
- Focused tests 覆盖 registry、progress、evaluation、rule review、owner decision。
- Reports 用中文说明 market regime、actual date range/source artifact、data quality/status 限制和安全边界。
- `policy_change_allowed=false` 为 rule review 默认值。
- `auto_apply=false`、`broker_action_allowed=false`、`production_effect=none` 在所有相关 artifact 中可见。
- `position_advisory_v1.yaml` 不被修改。
- README、operations runbook、system flow、report registry、artifact catalog、Reader Brief 相关文档同步。

## 9. 进展记录

- 2026-06-10：新增需求文档和 task register 入口，状态为 IN_PROGRESS；记录附件原始编号与现有 `TRADING-174` 冲突，采用唯一汇总 ID 继续实现。
- 2026-06-10：baseline 实现完成并转入 VALIDATING；真实链路从 confirmation plan `808e55a74ca6951f` 生成 registry `ad9f5724b143a76d`、progress `8e5c03f0284aab0b`、evaluation `bce0745ee33cea84`、rule review cycle `c80e7855c31eeee5` 和 owner decision `b4de77feff8cd189`。当前 progress 为 `ready_for_evaluation_count=0`、`insufficient_events_count=2`，evaluation 为 `success_count=0`、`failure_count=0`、`not_ready_count=3`，cycle recommendation 为 `continue_tracking`，owner decision 记录为 `continue_tracking`；缺少足够 forward samples 和 pressure-regime tagged outcomes 时保持 NOT_READY/INSUFFICIENT_EVENTS，不允许自动规则变更。验证通过五个新增 validate CLI、五个 report CLI、dynamic-v3 root validation、dynamic-v3 family artifact validation、report index、documentation contract、Reader Brief latest、Reader Brief quality、focused pytest 6 passed、ruff、compileall、git diff check 和全量 pytest 2320 passed / 640 warnings / 14:31。
