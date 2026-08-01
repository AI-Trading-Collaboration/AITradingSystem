# OPS-072：Daily Official Policy Transport Resilience

最后更新：2026-08-01

稳定任务 ID：`OPS-072_DAILY_OFFICIAL_POLICY_TRANSPORT_RESILIENCE`

优先级：`P0`

状态：`VALIDATING`

Owner / 下一责任人：Codex（正式验证、release promotion）；operations owner（新 release
部署后下一合法 automation invocation 的 ordinary operational acceptance）。

## 1. 触发证据

2026-08-01 唯一外部 trigger 从 permanent runtime
`D:\Work\AITradingSystem_ops_runtime`、exact release
`556f93a421f76ea09a61f6f791b973e0b749bf54` 执行：

- `as_of=2026-07-31`；
- parent run id：`daily_ops_run:2026-07-31:20260801T014510Z`；
- canonical state / ledger：`FAILED`，14 steps `PASS`、19 steps dependency-blocked、
  `pipeline_health` `FAIL`；
- `market_macro`、FMP forward PIT、SEC companyfacts、FMP valuation 均成功；
- `official_policy_sources` 为 validated `PARTIAL_CAPTURE`；
- 真实 source failure 是 `official_govinfo_federal_register` 在收到 HTTP response 前发生
  `URLError: [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol`；
- strict DQ、PIT build/validation、SEC metrics/validation 均 `PASS`；
- `score_daily` 因 `CAPTURE_COMPONENT_NOT_PASS:official_policy_sources:FAIL` 未执行；
- `pipeline_health` 只因当日 `daily_score_2026-07-31.md` 不存在而正确 `FAIL`；
- Reader Brief、dashboard、lineage 与报告尾链均未生成，不得从旧日期补造。

Immutable runtime evidence 保留在：

- `outputs/run_control/daily/states/operations_run_66162bb4f69d48ee56aa73a4.json`；
- `outputs/run_control/daily/states/operations_run_66162bb4f69d48ee56aa73a4.run_ledger.json`；
- `outputs/runs/daily/20260801T014510Z/as_of_2026-07-31__daily_ops_run_2026-07-31_20260801T014510Z/`；
- `outputs/daily_input_capture/2026-07-31/`。

原始 parent bytes、source state、ledger、manifest、provider payload 与 diagnostic artifacts
不可修改或删除。

## 2. 根因

本次是可复现的工程韧性缺口叠加一次外部 provider/CDN transient failure：

1. `UrllibOfficialPolicyHttpClient` 对一次 pre-response TLS/timeout/connection failure 没有
   request-level bounded retry；单次 TLS EOF 会直接让整个 composite source component 失败。
2. CLI 失败摘要只输出 payload/candidate/error 数量，没有输出 stable failure category；capture
   只能从自由文本猜测 blocker。
3. `_classify_source_blocker` 对任意裸字符串 `401` / `403` 都判为
   `PROVIDER_PERMISSION_DENIED`。本次 stdout 的 `待复核候选：401` 被误当成 HTTP 401，
   遮蔽了真实 TLS transport failure。
4. component-level historical recapture 仍必须禁止；不能通过第二个 daily trigger、删除 state、
   放宽 score gate 或补抓旧 PIT 来恢复本 parent。

## 3. 最佳耐久修复

采用 OPS-062 已验证的 provider-adapter retry 纪律，并保持 official-source 特有边界：

1. 只对 `cached_urllib_get` 在收到 response 前抛出的 `TimeoutError`、non-HTTP
   `urllib.error.URLError` / connection-level `OSError` 做最多 3 次有界尝试，退避 1 秒、2 秒；
2. `urllib.error.HTTPError` 已由 cache layer 转为带 status 的 response，HTTP 4xx/5xx 不进入
   transport retry；JSON/parser/schema/provider error 也不重试；
3. 最终 transport exhaustion 使用 typed exception / stable issue code，记录 source、attempt、
   max attempts、timeout、exception type 和脱敏 message，不记录 secret value；
4. CLI stdout 输出 stable issue codes / transport exhaustion marker，使 capture 可审计分类；
5. capture permission 分类只接受带 HTTP/status/auth 上下文的 401/403，不再匹配任意裸数字；
6. TLS EOF / typed transport exhaustion 稳定分类为 `PROVIDER_UNAVAILABLE`；
7. external request cache、immutable raw payload、download manifest、DQ/PIT、score dependency、
   historical recapture prohibition、production/broker boundary 均不变。

这不是 temporary workaround。退出条件是代码、测试、文档、formal validation、普通 main
push、exact release promotion 与 deployment acceptance 全部完成，并由下一合法 automation
invocation 对 terminal parent 做 contract 允许的处理。若 replay boundary 仍不允许从 capture/
score 恢复，则必须明确记录该 parent 不可恢复，并由下一 provider-ready session ordinary run
完成运营验收；不得扩大 recovery allowlist 来迁就本次故障。

## 4. 实施步骤与依赖

1. `S0`：登记 task / requirement，运行 `SINGLE_LANE` governed preflight。
2. `S1`：实现 official-policy request-level bounded transport retry 与 typed diagnostics。
3. `S2`：修复 CLI/capture stable classification，覆盖裸候选数 `401` 的负例。
4. `S3`：补 unit/integration tests；确认 HTTP status、parser/schema/provider failure 不重试。
5. `S4`：更新 operations runbook、daily runbook 与 `docs/system_flow.md`。
6. `S5`：运行 focused、Fast、Architecture、Contract、Integration、Reproducibility、Full；
   commit、latest-main integration、普通 push。
7. `S6`：生成 exact release candidate，promotion、deployment acceptance；不运行第二次 daily。
8. `S7`：记录 `READY_FOR_RECOVERY` 或不可恢复边界，并清理已合并 task worktree。

依赖：

- exact local-main base；
- 无 active checkout lease；
- runtime parent immutable；
- 不与当前 `TRADING-2473` dirty lane 混合；
- promotion 六类 validation artifact 必须绑定同一 exact candidate commit。

## 5. 验收标准

- transient TLS EOF 可在同一 request call 内受控成功，不重新抓取已成功来源；
- exhaustion 明确为 `PROVIDER_UNAVAILABLE`，不会因 candidate count `401` 误报 permission；
- HTTP 401/403 仍为 `PROVIDER_PERMISSION_DENIED`，且不进行 transport retry；
- HTTP 5xx、parser/schema/provider error 保持 fail closed，除非另有 reviewed policy；
- diagnostic 不泄露 `CONGRESS_API_KEY` / `GOVINFO_API_KEY`；
- daily dependency DAG 与 strict DQ/PIT/score 门禁不变；
- `production_effect=none`，不写 production / active-shadow weights，不触发 broker/trading；
- runtime checkout 不被直接修改；promotion 后 active receipt 精确绑定新 release；
- 当前 invocation trigger count 保持 1。

## 6. 临时工作区生命周期

- owning task：`OPS-072_DAILY_OFFICIAL_POLICY_TRANSPORT_RESILIENCE`；
- purpose：隔离当前 shared checkout 中 `TRADING-2473` 的 dirty implementation；
- absolute path：`D:\Work\AITradingSystem_ops072_official_policy_transport`；
- branch：`codex/ops-072-official-policy-transport-resilience`；
- frozen base：`eea2d61d5123220c98adf3448600357de6065f2a`；
- exit condition：candidate 已进入 reviewed main、required validation / deployment evidence 已迁入
  canonical location、无 unique tracked/untracked/ignored bytes、无 process/lease 依赖后，使用
  `git worktree remove` 并 `git worktree prune`；Git 历史可恢复实现，runtime immutable evidence
  保留。

## 7. 进度记录

- 2026-08-01：任务登记并进入 `IN_PROGRESS`。当前 invocation 已使用唯一 daily trigger；
  terminal parent 为 `FAILED`。已确认外部 GovInfo TLS EOF、缺少 adapter retry 与裸 `401`
  误分类的复合根因；未修改 runtime checkout，未第二次调用 trigger。
- 2026-08-01：实现完成并进入 `VALIDATING`。新增 request-local 三次有界 transport retry、typed
  exhaustion、stable CLI blocker marker、contextual HTTP 401/403 分类、manifest/report attempt
  evidence 与相应文档；并行 focused / docs regression `50 passed`，scoped Ruff 与新增文件 Black
  check PASS。Scoped mypy 仍显示仓库既有 87 个 strategy/CLI 类型错误，未作为 PASS 证据，
  本次改动的 focused runtime/contract tests 均通过。
- 2026-08-01：复核 `operations_recovery_request.v1` allowlist 后确认本 parent 不满足合法
  recovery：它在 `score_daily` 前失败，而当前 reviewed replay boundary 只从
  `artifact_lineage` 开始并要求 capture/DQ/PIT/score 已 PASS。不得把 parent 标成
  `READY_FOR_RECOVERY`，也不得为其扩大 allowlist 或重复 provider/capture。新 release 完成
  promotion / deployment acceptance 后，下一可用 session 应执行 ordinary daily acceptance；
  parent 继续 immutable `FAILED`，reason code=`RECOVERY_BOUNDARY_NOT_ALLOWLISTED_SCORE_NOT_PASS`。
