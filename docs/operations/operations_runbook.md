# AITradingSystem Operations Runbook

最后更新：2026-10-10（GOV-008 P5 重写）

执行任何 daily、weekly、biweekly、monthly、调度或产物目录任务前先读本文：确认 cadence、触发路径、质量门禁、
预期产物和"不影响 production"的边界。数据怎么流动见 `docs/system_flow.md` 第 1～6 节；产物怎么理解见
`docs/artifact_catalog.md`。2026-10-10 之前的完整旧版（含已删除的发布、调度发布与 ETF 机制）在
`docs/operations/operations_runbook_legacy_2026-10-10.md`，只作历史查阅。

## 1. 唯一入口

```powershell
aits ops daily-run                      # 外部调度唯一入口；评估最近一个已完成的美股交易日
aits ops daily-run --as-of YYYY-MM-DD   # 指定评估日
aits ops daily-plan --fail-on-missing-env   # 只出计划、检查环境变量，不执行
aits ops replay-day --mode cache-only --as-of YYYY-MM-DD   # 历史严格复现，不用 daily-run 补造历史 PIT
```

- 只允许一个外部调度入口。weekly / biweekly / monthly / ad hoc 任务不得登记为独立的系统计划任务（Windows Task Scheduler、
  cron、GitHub Actions 或云调度器都一样）。
- 未给 `--as-of` 时，`daily-run` 与 `validate-data` 使用 America/New_York 最近已完成交易日加收盘后 3 小时
  provider-ready 缓冲，不用主机本地日期。
- **调度现状**：此前的外部触发是 Codex automation `aitradingsystem-pit`，在独立运行副本里调用 `aits ops daily-run`。
  为它服务的 release promotion、scheduler checkout preflight、Codex scheduler observation 与 deployment acceptance
  命令已在 GOV-008 删除，`daily-run` 也不再持有 checkout guard（`--manual-execution` 选项保留但已无作用）。
  确定性调度（Windows 计划任务调用一次 `aits ops daily-run`，并停用 Codex automation）是 OPS-082，尚未实施；
  在此之前 Codex automation 的启停由 owner 决定。

## 2. 每日链路

步骤与依赖以 `config/scheduled_tasks.yaml`（`scheduled_tasks_v7`，`daily_trading_day` 共 32 个任务）为准，顺序如下：

| 阶段 | 步骤（命令） | 条件与阻断 |
|---|---|---|
| 输入 | `ops capture-daily-inputs`（交易日）：market_macro、fmp_forward_pit、sec_companyfacts、fmp_valuation、official_policy_sources | 每个组件独立尝试，原始字节与 checksum manifest、XNYS 缺口台账全部保留；`CAPTURED` 只表示字节已保存，不等于 DQ 通过 |
| 输入（休市日） | `download-data`、`pit-snapshots fetch-fmp-forward`、`fundamentals download-sec-companyfacts`、`valuation fetch-fmp`、`risk-events fetch-official-sources` | 只在休市日运行 |
| 数据质量 | `validate-data --execution-profile daily_default.v1` | 写 DQ receipt 与按 profile/as-of 的发现指针；**只有 strict PASS** 才能放行评分 |
| PIT | `pit-snapshots project-fmp-forward-capture` → `build-manifest` → `validate` | 失败快照不得作为 PIT 输入 |
| SEC | `fundamentals extract-sec-metrics` → `merge-tsm-ir-sec-metrics` → `validate-sec-metrics` | |
| 评分 | `score-daily --consumer-authorization-profile daily_score_daily@1.0.0` | 评分前严格复核 DQ receipt、capture 证据与来源；任一缺失或漂移则零运行 |
| 评分之后 | `forward-evidence capture-dry-run-daily`、`reports dashboard`、`sec-pit shadow-observe/shadow-monitor`、`reports score-change-attribution`、`reports market-panel`、`data freshness`、`data recover-freshness` | 只读或只写研究存档；不写权重、不触发 broker |
| 报告 | `reports artifact-lineage` → `validate-artifact-lineage` → `reports index` → `docs report-contract` → `reports research-governance-summary` → `reports reader-brief` → `reports quality-gate` → `reports validate-reader-brief` | 质量失败必须在输出中可见 |
| 收尾 | `ops health`、`security scan-secrets`（always-run） | 它们的 PASS 不覆盖投资门禁 |

之后 `daily-run` 在同一运行控制下做 canonical finalization：生成本次运行的 decision summary 与 task dashboard，
重新生成最终 Reader Brief 并让质量检查绑定最终字节；`report_quality_status=FAIL` 或 Reader Brief 质量 `FAILED` 时运行失败，
`PASS_WITH_WARNINGS`、`LIMITED_READER_CONTEXT` 可以完成但必须披露并进入人工复核。

**休市日**：不生成新的评分、决策快照、Reader Brief 评分产物或 forward evidence；数据刷新与 PIT/SEC 检查照常。

**缺失与失败**：某个 capture 组件缺失只阻断依赖它的步骤，其他步骤继续并保留证据；整体状态保持
`BLOCKED_DEPENDENCY` 或 `FAIL`，finalization 不运行。

## 3. 运行控制与恢复

- 同一 workflow/规格/as-of 只能有一个活动运行；已完成的重复触发不重跑；中断的运行只恢复明确幂等且未 PASS 的步骤。
  状态与台账在 `outputs/run_control/daily/states/`。**不得手工删除状态或锁来强行重跑**，过期锁由控制层回收。
- 步骤列表变化会产生新的规格 id 与新的幂等键（旧状态与台账保留），所以修改 `config/scheduled_tasks.yaml` 后同一 as-of
  会作为新运行开始。
- 终止后恢复（OPS-071）只能用同一入口：`aits ops daily-run --recovery-parent-run-id <id> --recovery-from-step <step>
  --recovery-reason-code <code>`，三项必须同时给出；只允许从 `artifact_lineage` 及其后的报告/收尾步骤恢复，已 PASS 的
  capture、DQ、PIT、评分必须复用。**限制**：恢复目前仍要求环境变量 `AITS_OPS_DEPLOYMENT_RECEIPT` 指向一个 deployment
  receipt，而生成它的命令已删除，所以只能使用现存 receipt；OPS-082 会去掉这一依赖。
- 同一 as-of 的终止运行没有合法恢复边界时，等待下一个 provider-ready 交易日的普通运行，不得伪装成恢复。
- 历史缺口：`aits ops recover-historical-gap` / `validate-historical-gap` 只由 owner 人工触发单个已审查的队列项，只生成
  隔离证据，不改旧运行、不补造 strict PIT。

## 4. 数据质量门禁

`aits validate-data` 是缓存行情与宏观数据的必经门禁。任何从缓存数据产生特征、评分、回测或日报的命令都先运行它
（或调用同一代码路径），失败即停止；下游报告写明 DQ 状态或链接质量报告。

- 一次执行只调用一次 canonical validator，并把策略、输入字节、报告与 `DataQualityEvidence` 冻结到
  `outputs/data_quality/executions/{receipt_id}/receipt.json`；报告正文按 SHA-256 存在 `outputs/data_quality/reports/`。
- 显式 `--as-of` 默认解析为 `manual.v1`，不会覆盖 daily 指针；只有满足全部默认输入约束的 `daily_default.v1` 才发布
  `outputs/data_quality/executions/discovery/daily_default/{as_of}/current.json`。指针只用于发现，不是质量证明。
- `download-data` 先复用已有缓存，只请求缺失的尾部窗口；Marketstack 第二源做额度预检，额度不足默认 fail closed，
  只有 `config/data_source_request_budget_policy.yaml` 登记的小额例外可以继续，并在下载清单中披露。

## 5. 非每日任务

`config/scheduled_tasks.yaml` 另登记 27 个任务：weekly（回测 `unified_primary_2021` 与稳健性、参数回放/候选/治理、
权重候选评估与晋升门禁、研究治理摘要复核）、biweekly（投资复核、反馈闭环、shadow lane、SEC PIT observe-only、
人工 thesis 与风险复核）、monthly（文档契约、产物目录、报告登记、数据源覆盖、PIT 覆盖、长窗口回测复核）、
ad hoc（SEC PIT 回填/评估/基线对比/诊断/候选复核、大参数搜索、缓存回放窗口）。

- `daily-run` 只为它们写不执行的 `periodic_operations_plan_YYYY-MM-DD.json`：是否到期、缺哪些证据。自动派发关闭
  （`config/operations/periodic_control.yaml`：`automatic_command_dispatch_enabled: false`）。
- 人工运行：

  ```powershell
  aits ops periodic-dispatch --task-id <task> --daily-status PASS --data-quality-status PASS `
    --data-quality-evidence-id <receipt-id> --source-artifact-id <artifact> --owner-decision-id <id> `
    --confirm-manual-dispatch            # ad hoc 任务另加 --explicit-trigger
  ```

  未解析的占位符或不在允许前缀里的命令在执行前 BLOCKED。
- 每个任务的输出写明 cadence、as_of、日期范围、来源产物、数据质量状态与 `production_effect`。缺少上游时输出
  `SKIPPED`/`LIMITED`/`INSUFFICIENT_DATA` 或 fail closed，不补造结论。

## 6. 操作前检查

1. 确认 cadence、`as_of`、日期范围、研究窗口（默认起点 2021-02-22）与是否交易日。
2. 确认 `aits validate-data` 或同一路径门禁已通过，并记下质量报告。
3. 确认 `production_effect`、权重写入、broker 与交易动作的边界（默认全部为 none）。
4. 会改变 CLI、配置、产物、报告 schema、数据流、评分、回测或解释的改动，同一次改动更新 `docs/system_flow.md`、
   `docs/artifact_catalog.md`、任务记录与相关需求文档或本文。
5. 遇到阻塞不静默绕过：说明最佳方案、阻塞原因、是否先修阻塞；临时绕过需 owner 同意并记录原因、影响、风险、验证与退出条件。

## 7. 前瞻采集（手工研究入口）

前瞻采集不是调度任务，不新增 scheduler entry；每次运行在 capture hold 下进行（第一小节）。

### TRADING-2564 S3b 手工前瞻采集边界

S3b `python -I -B scripts/run_named_data_quality.py --operation activate|capture` 为有限手工研究
入口，固定 68-module/11-dependency profile（`named_prospective_five_candidate_sources_v2.json`）与
`prospective_capture_execution_v3.yaml`（采集协议 v3，GOV-008），不登记第二个 scheduler。执行前
显式复核 exact manifest/owner review、source commit、capture hold、根/输出归属、允许的 feature
sessions 和 expiry。真实范围尚待独立 review；真实 activation/capture/DQ/provider/order/fill 均为零。

**采集 hold（协议 v3，替代 S4D lease）**：hold 绑定精确候选 commit（HEAD 相同且 `src/config/scripts/tools`
无未提交修改）、独占写入路径和有界有效期；它是本机自证，不是独立权威。操作顺序：

1. `python scripts/capture_hold.py acquire --candidate-commit <HEAD> --path <manifest 输出目录> --actor <名字> --ttl-minutes <分钟>`，记下输出里的 `hold_id`；
2. 以 `--source-hold-id <hold_id>` 启动上面的采集入口（子进程只恢复并复核该 hold，不新建）；
3. 结束后 `python scripts/capture_hold.py release --hold-id <hold_id>`；进程崩溃留下的 hold 同样用 release 清除，
   `python scripts/capture_hold.py status` 列出全部记录。hold 状态在 `outputs/runtime/capture_holds/`（git 忽略）。

activation 完整 ACK 保守时间上界的纽约日期之后首 XNYS 为最早 F；capture 仅 F close 后、next_XNYS(F) close
前执行。父固定零 DQ，child 最多一次 canonical DQ，严格 PASS + 原始成功终态/guards 后，同父
context 零 DQ verify、原五候选 preview、完整闭包保全及 S3a signal witness 才能进入 ACK。
ACK v2 保留固定 raw UTC ns/QPC anchor、每个原 recorder return 与 postguard；全部已验证
bound 必须严格小于 D close。第一段收益为 Close(D)→Close(next(D))。
result v3 单次提交前重验完整闭包及原 live capture hold，并保留同 anchor 最终时间证据；
最终 bound 仅约束 manifest/hold expiry，自己落盘完成时间不作承诺。外层 source terminal
失败属于交付未确认；失败 sampler 的完整/部分原始读数必须保留，不得换 anchor 重试。
本入口不读取收益或运行 maturity/scoreboard。首次 outcome 访问须先冻结 S4 实验/会计合同。

固定 key 原 attempt/部分证据必须保留；重复调用包括到期后的旧 key 只读复核完整 result 或返回
INCOMPLETE，不能重试 DQ、补签时间或换 snapshot。输出路径和复核链见 artifact catalog 的
S3b 条目。PASS admission 仅代表该范围本地源码/严格 DQ/信号/记录时间核验，不建立 provider
available_at、PIT/OOS、调度启用、production 或 broker 权限。

### TRADING-2560 Composer 手工前瞻研究入口

`python -I -B scripts/run_named_data_quality.py --operation
composer-activate|composer-readiness|composer-capture --request <request.json>
--request-sha256 <sha256> --source-hold-id <hold-id>` 使用独立 84-module/21-dependency
`named_composer_prospective_sources_v2.json` 与 `composer_prospective_capture_v1.yaml`；hold 的获取与
释放见上一节。
这是 Owner 已选择的有限手工研究，沿用当前已知 revision 输入规则；不新增 scheduler entry。
详细边界见 `docs/requirements/TRADING-2560_Composer_Known_Snapshot_Prospective_Capture_V1.md`。

每次派发前自动 replay exact manifest、owner review、代码/政策、原 capture hold、根路径、唯一 F
和 expiry。readiness 仅对显式现有快照运行 training/exact_cash/primary 三段 canonical DQ，
完整保留 requested/evaluated 窗口与原 consistency 起点，零拟合、零观察；不能替代未来 F 的 DQ。
activate 零 DQ/拟合，以实际完整 ACK 确定纽约日期之后首 XNYS F。
capture 在 F close 后、D=next_XNYS(F) close 前完成三段同快照严格 PASS、完整输入封存、原
504/20 Composer fit 与信号记录；价格完整至 F，rates 可早于 F，但原 DQ freshness 门禁不变。
逐 rates series 披露 raw/effective 日期，R 来自原 input recorder 的保守返回上界。

同 key 只读重放，部分尝试不得重试或补签；异常保留实际 DQ 计数，缺失计数保持 UNKNOWN。
输出在 `outputs/research/composer_prospective/<manifest-id>/`，包括 control、attempt、分段
DQ、segmented_dq_identity、timing、ACK 和 observation。记录 current revision 历史训练访问，
不建立历史 provider available_at/PIT；未来 outcome、maturity、scoreboard、下载、缓存修改、
production/broker/order/fill 均关闭。首次收益查看另须既有 S4 研究/会计合同。

## 8. 已删除的机制

下列内容在 GOV-008 删除，旧版手册里的相关段落不再适用：发布与验证机制（fence、lease、Full 验证、validation tier）、
release candidate/canary/promote、runtime git exclusions、scheduler checkout preflight、Codex scheduler observation、
deployment acceptance、workflow health 遥测与周任务、Atlas、DEVX-015 源码保全检查点、ETF 候选链（`aits etf ...`、
dynamic-v3 rescue、候选跟踪每日步骤）。发布改由 `python tools/gov008/ship.py` 完成（见 AGENTS.md）。
