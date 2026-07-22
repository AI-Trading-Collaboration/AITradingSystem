# OPS-062 / OPS-063 本周每日结果缺口恢复

最后更新：2026-07-22

## 背景与安全边界

本周最新已满足 provider-ready 条件的 U.S. equity trading day 为
`2026-07-16`。当前完整结果如下：

- `2026-07-15`：canonical `aits ops daily-run` 36/36 PASS；
- `2026-07-13`、`2026-07-14`：cache-only replay inventory 为
  `INCOMPLETE_REPLAY / INSUFFICIENT_DATA`；
- `2026-07-16`：canonical `aits ops daily-run` 在唯一允许重试的
  `download_data` 步骤连续两次失败，attempt budget 已耗尽。

恢复过程必须遵守 `docs/operations/operations_runbook.md` 与
`docs/runbook_daily_ops.md`：不得删除或改写 canonical state/ledger，不得把
历史日改为 live refetch，不得直接运行下游步骤，不得写 production weights、
active shadow weights、broker state 或任何 trading action。

## OPS-062：2026-07-16 provider 恢复

状态：`BLOCKED_OWNER_INPUT`（provider 与请求级耐久修复已验证，owner 已批准新的可审计 recovery workflow/spec；preflight 证明严格 2026-07-16 canonical inputs 不完整，等待 exact archive 或受限处置决策）

### 已确认 blocker

两次 `aits ops daily-run` 均在 FMP `AMAT` 价格请求处收到
`SSLError / SSLEOFError: EOF occurred in violation of protocol`：

- attempt 1 run id：`daily_ops_run:2026-07-16:20260717T010215Z`；
- attempt 2 run id：`daily_ops_run:2026-07-16:20260717T010332Z`；
- canonical state：
  `outputs/run_control/daily/states/operations_run_c0229ca89bf11414deb018d6.json`；
- workflow spec：`workflow_spec_db7194b783bb9b502f76`；
- state：`FAILED`，`download_data` attempt=`2`，后续 35 steps 未启动；
- 两次失败前后市场/宏观 cache checksum 未变化，`production_effect=none`。

### 解除条件与顺序

1. provider/network owner 确认 FMP TLS 连接恢复，并保留故障证据；
2. 由于原 workflow spec 的 attempt budget 已耗尽，由项目 owner 审批新的可审计
   recovery workflow/spec 或其他 runbook 允许的恢复路径；不得删除旧 state 后重跑；
3. 仍以 `aits ops daily-run` 作为唯一外部 trigger；
4. 验证 36/36 steps、Data Quality Gate、daily PIT、SEC、`score-daily`、
   dashboard/latest checks、Reader Brief 与 `validate-reader-brief` 全部闭合；
5. 确认所有产物绑定同一 canonical run id，且 `production_effect=none`。

### 2026-07-22 Owner 批准与执行前置审计

- Owner 决策：批准为 `2026-07-16` 建立新的可审计 recovery workflow/spec；决策记录 id 为
  `owner_decision:OPS-062:2026-07-22:approve_auditable_recovery_v1`。
- 批准不授权删除/改写旧 state/ledger、提高原 spec attempt budget、修改原
  `workflow_spec_db7194b783bb9b502f76`、直跑下游步骤、把当前 live provider 响应冒充
  2026-07-16 contemporaneous input，或绕过 PIT / data-quality / visibility gate。
- 当前生产 `aits ops daily-run` 对历史 `as_of` 固定返回
  `daily_run_historical_as_of_requires_replay`；仅改变 `run_id` 不会产生新 spec，且仍命中
  原 idempotency key。执行前必须先完成两项 preflight：
  1. 证明 2026-07-16 所需 market/macro、FMP forward PIT、SEC fundamentals、valuation、
     risk-event prereview 等输入存在 cutoff-compatible contemporaneous evidence；
  2. 设计并验证显式读取 owner decision、绑定 predecessor state/spec/checksum、生成独立
     recovery spec/idempotency key、只允许 unified `daily-run` 全链执行且保留旧证据的
     fail-closed recovery 入口。
- 若 contemporaneous evidence 不完整，必须停止 canonical recovery 并记录
  `INSUFFICIENT_DATA` / blocker；不得用 2026-07-22 live refetch 或后续日期 snapshot
  回填。若 evidence 完整，先以 focused parallel pytest 和 scheduler parity 验证新入口，
  再执行一次 owner-approved recovery；结果仍须满足原 36/36 与同一 canonical run id
  验收条件。

### 2026-07-22 Contemporaneous input preflight 结果

- 只读执行：
  `aits ops replay-day --as-of 2026-07-16 --mode cache-only --visible-at 2026-07-17T01:02:15Z --inventory-only --allow-incomplete --openai-replay-policy disabled`。
  输出位于
  `outputs/replays/2026-07-16/ops062-owner-approved-preflight-20260722/`，状态为
  `INCOMPLETE_REPLAY`；未运行 score、未调用 live OpenAI、未修改 production/state。
- Preflight 在原 attempt 1 开始时刻 cutoff 下确认 market/macro、PIT manifest、valuation、
  risk-event occurrences、trade theses 与历史 feature/score seed 可冻结，但缺少四个 required
  exact-as-of artifact：
  1. `data/processed/pit_snapshots/fmp_forward_pit_2026-07-16.csv`；
  2. `outputs/reports/pit_snapshots_validation_2026-07-16.md`；
  3. `outputs/reports/fmp_forward_pit_fetch_2026-07-16.md`；
  4. `data/processed/sec_fundamentals_2026-07-16.csv`。
- Cutoff 前存在 17 个 ticker 的 2026-07-15 FMP forward raw snapshot（captured at
  `2026-07-16T02:21:40Z`）及 2026-07-15 normalized/report/SEC artifacts，但 payload
  `as_of` 仍为 2026-07-15。它们是 PIT-safe prior-day evidence，不是 exact 2026-07-16
  daily-chain artifact；静默 retarget/copy 会改变 lineage。今天 live refetch 也晚于 cutoff，
  不能补成 contemporaneous evidence。
- 因此，owner 对“新 recovery workflow/spec”的批准已满足治理前置，但严格 canonical
  recovery 的数据前置仍不满足。下一步只能二选一：
  1. owner 提供或定位 cutoff 前已归档的四个 exact 2026-07-16 artifacts，再继续实现并执行
     canonical recovery spec；或
  2. owner 明确批准单独的 `LIMITED_PRIOR_SNAPSHOT_RECONSTRUCTION`，使用冻结的 2026-07-15
     prior-day evidence，保持 2026-07-16 canonical status=`FAILED`，不得生成或传播 canonical
     daily PASS、promotion、backtest、weight 或 production 结论。
  在收到其中一项输入前不得继续实现历史 visibility bypass 或 recovery executor。

### 2026-07-17 根因复核与修复决策

- 历史证据显示同类 TLS EOF 并非 FMP/AMAT 独有：2026-05-15 的 OpenAI
  请求曾在前两次分别出现 `RemoteDisconnected` 与 `SSLEOFError`，第三次请求成功；
  2026-06-22 的 Marketstack 请求也曾出现 `UNEXPECTED_EOF_WHILE_READING`。
- 当前网络探测中，FMP 域名可解析、TCP 443 可达、TLS certificate verify=0；不带
  key 的同一 AMAT endpoint 完成 TLS 并返回预期 401。使用现有环境凭据只读探测
  non-split-adjusted 与 dividend-adjusted 两个 endpoint，均返回 HTTP 200、1 行
  `2026-07-16` 数据。因此排除 AMAT symbol、请求日期和订阅权限为本次根因；故障归类
  为客户端到 provider/CDN 链路被瞬时提前关闭。
- 代码缺口是 FMP EOD 路径没有请求级 transient retry，且 pre-response exception 未包装
  为 `ProviderDownloadError`，导致单次 TLS reset 直接失败并消耗整个 canonical
  `download_data` step attempt。最佳修复是在 FMP adapter 内仅对 requests 的
  `Timeout` / `ConnectionError`（含 `SSLError`）实施有界重试，并在最终失败时写入
  脱敏、带 cache identity、attempt count 和 timeout 的结构化诊断。
- 不重试 HTTP 4xx/5xx、JSON/schema/provider error，不删除 canonical state/ledger，
  不把代码修复视为 2026-07-16 已恢复。原 workflow spec attempt budget 仍保持耗尽；
  完成代码验证后仍须按上节顺序审批新的 recovery workflow/spec。

### 修复与验证结果

- `FmpPriceProvider` 默认 timeout=30 秒、max attempts=3、retry backoff=1/2 秒；
  只识别 requests `Timeout`、`ConnectionError`、`SSLError`、
  `ChunkedEncodingError` 为 retryable transport error。
- 最终 pre-response failure 统一包装为 `ProviderDownloadError`，诊断包含 provider、
  api family、endpoint、stage、脱敏 request parameters、cache status/key/path、
  row count、attempt/max attempts、timeout、exception type/message；API key 不落盘。
- HTTP status、invalid JSON、schema/provider error 直接 fail closed，不进入 transport
  retry；已有 external request cache 语义和 downstream DQ gate 不变。
- 真实隔离 adapter probe：AMAT / 2026-07-16 返回 1 行，non-split-adjusted 与
  dividend-adjusted 两个 cache metadata 均生成，临时目录随后清理。
- 验证：ruff PASS；并行定向测试 `42 passed`；正式 `fast-unit` `204 passed`，
  runtime artifact `outputs/validation_runtime/fast-unit_20260717T014012Z/test_runtime_summary.json`；
  正式 `contract-validation` `203 passed`，runtime artifact
  `outputs/validation_runtime/contract-validation_20260717T014417Z/test_runtime_summary.json`。
- 代码修复已完成，但 OPS-062 的 canonical daily result recovery 尚未完成。原 state/ledger
  与 attempt budget 保持不变；本轮未调用 `aits ops daily-run`，未修改 production 或
  active shadow weights，未触发 broker/trading action，`production_effect=none`。
- 2026-07-20 工作区归属审计将该代码 slice 重放到最新 `main`，并补齐精确错误分类：
  非 JSON HTTP failure 仍归入 `http_status`，HTTP 200 provider error 归入
  `provider_error`；两类都不进行 transport retry。通用 external request cache 对 HTTP
  failure response 的既有保留语义未在本 slice 中静默改变，后续 lifecycle policy 已登记为
  `OPS-064_EXTERNAL_REQUEST_ERROR_CACHE_LIFECYCLE`。
- 2026-07-20 最新 `main` 集成复验：定向 data/provider 测试 `44 passed`，ruff PASS；
  `fast-unit` `317 passed`（artifact SHA256=`1be6558567707191828a471af5886ec0655d4556fc9cca525bf033e2337b5a9e`）；
  `architecture-fitness` `446 passed`（SHA256=`e317b46e513f3c8c637f1e7ba0f52a47654d4d45e262d51bf6936e6b8003b57d`）；
  `contract-validation` `265 passed`（SHA256=`ba3040462269f7f4b1d663e3812ccebd76c3c8e3b97e9df149b8f004c836d42a`）；
  带 `natural_integration_boundary` provenance 的 full 为 `6435 passed / 2 skipped / 643 warnings`，
  elapsed=`972.33s`，artifact SHA256=`e7da0664e85f0e6b8989921facf4f47252f91d8ed9140fe610d366d6e7f3eed1`，
  profile SHA256=`8898c9ad15df4198608a65dace2f75fb0466dbae64f07a2be315b49e00b6a759`。
  相对最近 `970.42s` full 基线约 `+0.20%`，未发现本 slice 引入的异常性能退化；slowest 50
  仍由既有 smoothed/weekly/research 集成测试构成。上述验证没有调用 periodic operation，
  `production_effect=none`。

## OPS-063：2026-07-13 / 2026-07-14 strict PIT 缺口处置

状态：`DONE`（按 owner 批准的受限非 PIT 处置完成；strict PIT 仍不可用）

### 已确认 blocker

两日 inventory-only replay 都缺少当时可见的五类硬输入：

1. FMP forward PIT normalized CSV；
2. PIT validation report；
3. FMP forward PIT fetch report；
4. SEC fundamentals CSV；
5. OpenAI risk-event prereview report。

现有 inventory：

- `outputs/replays/2026-07-13/weekly_gap_audit_20260716_20260713/replay_run.md`；
- `outputs/replays/2026-07-14/weekly_gap_audit_20260716_20260714/replay_run.md`。

### Owner 决策与验收

项目 owner 可选择：

1. 提供两日原始 contemporaneous archived inputs，由 cache-only replay 校验并生成
   严格 PIT 结果；或
2. 批准生成隔离的 `LIMITED_NON_PIT_RECONSTRUCTION`：只重建已有日期行和
   checksum 的 market/macro 事实；FMP forward PIT、SEC fundamentals 和 OpenAI
   prereview 保持 null / `INSUFFICIENT_DATA`，不生成 canonical daily score、仓位、
   Decision Snapshot 或 Reader Brief，也不得进入 weekly、governance、promotion、
   backtest 或 production 证据；或
3. 接受两日永久标记为 `INSUFFICIENT_DATA`，不得补造投资结论。

任何现在的 live provider refetch 都不能代表 `2026-07-13` / `2026-07-14`
当时可见信息，因此不属于合规恢复路径。

现有 `2026-07-15` FMP forward PIT snapshot 实际采集于 `2026-07-16T02:22:48Z`
附近，晚于两日 replay cutoff。不得将该 snapshot 回填为两日当时可见输入，也不得
据此生成延迟近似综合分；否则会形成 look-ahead contamination。

### 一次性 artifact 治理

本次 `limited_non_pit_reconstruction.v1` 是 owner 明确批准的一次性 manual evidence，
不是可重复运行的 canonical producer、report registry 类型或 scheduler task。它只保存在
隔离 `outputs/replays/limited_non_pit_reconstruction/...` 目录，不能被 daily、weekly、
governance、Reader Brief、评分、回测或 promotion 链消费，也不能升级两日 canonical status。
`docs/artifact_catalog.md` 只登记其阅读和禁止误用边界。如果未来需要复跑或形成正式下游，
必须新建任务，补齐 reviewed schema、可重复 producer、content-derived validator、source
snapshot/checksum 与 tamper tests；不得把本次人工 bundle 直接泛化成 reusable workflow。

## 状态记录

- 2026-07-17：登记。`2026-07-16` 两次 canonical daily-run 均被 FMP TLS
  外部故障阻断，attempt budget 耗尽；`2026-07-13`、`2026-07-14` 保持
  `INCOMPLETE_REPLAY / INSUFFICIENT_DATA`。未实施 workaround，未生成伪结果，
  `production_effect=none`。
- 2026-07-17：owner 询问是否可特殊处理。提出受控
  `LIMITED_NON_PIT_RECONSTRUCTION` 方案并等待明确批准；未使用 7/15 snapshot
  回填历史日，未生成近似分数或投资结论。
- 2026-07-17：owner 明确批准受限重建。两日均以 cache-only、OpenAI disabled
  方式生成隔离 input freeze、replay inventory、market/macro DQ 与结构化/Markdown
  reconstruction report：
  `outputs/replays/limited_non_pit_reconstruction/2026-07-13/limited_non_pit_reconstruction_2026-07-13_20260717/`
  和
  `outputs/replays/limited_non_pit_reconstruction/2026-07-14/limited_non_pit_reconstruction_2026-07-14_20260717/`。
  两日 DQ 均为 `PASS_WITH_WARNINGS`（0 errors / 1 warning），wrapper status 均为
  `LIMITED_NON_PIT_RECONSTRUCTION`，canonical daily status 保持
  `INSUFFICIENT_DATA`。逐项 source SHA、行情值、null contract、无 conclusion artifact、
  canonical cache checksum 与无全局 validation sidecar 残留均验证 PASS；未生成 score、
  position、Decision Snapshot 或 Reader Brief，`production_effect=none`。OPS-063 验收
  完成并归档；未来若找到 contemporaneous archived inputs，应新开或重开 strict PIT
  recovery 任务，不得静默升级本受限结果。
- 2026-07-17：OPS-062 provider 已恢复，FMP EOD transient transport retry 与结构化
  诊断修复通过定向/正式门禁；原 canonical workflow spec budget 仍耗尽，7/16 recovery
  继续等待 operations owner 审批新的可审计 workflow/spec，`production_effect=none`。
- 2026-07-20：完成旧工作区归属审计；保留最新 `main` 的 ARCH-004/005 状态，只恢复
  OPS-062/063 的有效独特增量。OPS-062 代码已通过定向、fast-unit、architecture、contract
  与 full 正式门禁，OPS-063 继续保持一次性、无 canonical downstream 的归档边界；未执行
  任何 periodic operation。OPS-062 canonical recovery 仍等待 owner 审批新 workflow/spec。
- 2026-07-22：owner 明确批准新的可审计 recovery workflow/spec。登记决策后进入
  contemporaneous input 与 historical visibility preflight；批准本身不等于授权 historical
  live refetch、不等于 canonical recovery 已完成，也不允许绕过原 state/ledger 或质量门禁。
- 2026-07-22：cutoff-bound inventory preflight 输出 `INCOMPLETE_REPLAY`，缺 4 个 exact
  2026-07-16 required artifacts。任务转为 `BLOCKED_OWNER_INPUT`；旧 state/ledger 未变，
  未实现 visibility bypass，未运行 score 或 live provider recovery，等待 exact archive 或
  `LIMITED_PRIOR_SNAPSHOT_RECONSTRUCTION` 明确决策。
