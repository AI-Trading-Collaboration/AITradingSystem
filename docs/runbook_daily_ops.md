# Daily Ops Runbook

最后更新：2026-07-25

本文是 `aits ops daily-run` 的人工可交接运行手册。它不替代数据质量门禁、日报、pipeline health、secret hygiene 或 evidence bundle；它只规定什么时候跑、失败时看什么、哪些输出是正式结论、哪些只是审计附录。

## 运行节奏

|频率|命令|目的|
|---|---|---|
|交易日前/盘前|`aits ops daily-plan --as-of YYYY-MM-DD --fail-on-missing-env`|确认计划级环境、缓存路径、预期 artifact 和当日是否交易日。FMP/SEC/OpenAI 等逐 provider 缺失不会在 capture 前全局阻断；真实缺失由 capture/score component 脱敏记录并 fail closed。未传 `--as-of` 时默认使用最新已完成美股交易日。|
|交易日盘后|`aits ops daily-run` 或 `aits ops daily-run --as-of YYYY-MM-DD`|先逐源保全市场/宏观、FMP PIT、SEC、估值和官方来源，再执行 strict DQ、PIT/SEC/score、只读 dashboard、Reader Brief、pipeline health 和 secret scan。未传 `--as-of` 时默认使用最新已完成美股交易日。|
|历史时点复现|`aits ops replay-day --as-of YYYY-MM-DD --mode cache-only --openai-replay-policy cache-only`|只读归档输入，生成隔离 replay bundle；不调用 live provider 或 OpenAI。|
|每周|`aits reports investment-review --period weekly --as-of YYYY-MM-DD`、`aits feedback loop-review --as-of YYYY-MM-DD`、`aits feedback optimize-market-feedback --as-of YYYY-MM-DD`|复核结论变化、outcome、learning queue、shadow maturity、blocked tasks，并判断市场反馈优化 readiness。|
|每月|`aits backtest --robustness-report --to YYYY-MM-DD`、`aits feedback build-parameter-replay --as-of YYYY-MM-DD`、`aits feedback build-parameter-candidates --as-of YYYY-MM-DD`、`aits reports investment-review --period monthly --as-of YYYY-MM-DD`、`aits feedback optimize-market-feedback --as-of YYYY-MM-DD --replay-start 2022-12-01 --replay-end YYYY-MM-DD`、必要时运行覆盖诊断|复核规则、数据源、gate 松紧、样本成熟度、参数复测收益变化、参数候选台账、as-if 回放窗口和 owner action。|

建议盘后运行时间放在美股收盘且数据供应商 EOD 数据稳定后。未显式传入 `--as-of` 时，系统按 `America/New_York` 判断 U.S. equity market 最新已完成交易日：常规交易日美东 16:30 之后使用当日，16:30 前、周末或 NYSE 常规整日休市日使用上一交易日。具体云 VM 时区和时刻由 owner 后续决定；当前不在 GitHub Actions 中配置生产 cron。

市场反馈优化的样本门槛由 `config/feedback_sample_policy.yaml` 控制。当前为 pilot 阶段：少量样本达到 pilot floor 后即可启动因果链、学习队列和候选规则整理；低于 diagnostic / promotion floor 时不得把结果写成正式调权或 production 晋级。若连续周度报告显示候选生成过快或噪声过高，优先调高 `pilot_floor` 或 `review_after_reports`，而不是在代码里改阈值。

TRADING-089 Dynamic v0.2 review 是 owner-requested ad-hoc 复核，不是 daily / weekly / monthly scheduler entry。执行前先确认 latest TRADING-088 dynamic rescue evaluation 和 v0.4 candidate robustness report 存在；若 latest 指向的不是 `dynamic_regime_overlay_v0_4_lower_turnover`，必须显式传入 `--candidate-robustness-report <path>`。命令为 `aits etf dynamic-v2-review package --latest-rescue-report` 或显式 `--rescue-report <path> --candidate-robustness-report <path>`，随后运行 `aits etf dynamic-v2-review validate`。输出只允许作为 review-only owner package；`CONSTRAINT_HIT_WORSENED` 和 `DRAWDOWN_PRESERVATION_FAILED` 未解除时必须保持 `not_shadow_ready`，不得生成 owner approval、shadow enrollment、official target weights、baseline mutation、production mutation 或 broker action。

TRADING-090 Dynamic v0.3 rescue 是 owner-requested ad-hoc 复核，不是 daily / weekly / monthly scheduler entry。执行前先确认 latest TRADING-089 Dynamic v0.2 review package 存在且仍指向 `dynamic_regime_overlay_v0_4_lower_turnover`，命令为 `aits etf dynamic-v3-rescue run --latest-v2-review` 或显式 `--v2-review-package <path>`，随后运行 `aits etf dynamic-v3-rescue validate`。输出只允许作为 candidate-only v0.3 rescue evaluation；即使 status 为 `v0_3_rescue_success_candidate_found`，也只表示可进入 owner review / TRADING-091 handoff，不得生成 owner approval、shadow enrollment、official target weights、baseline mutation、production mutation 或 broker action。

TRADING-091 Dynamic v0.3 real evaluation 是 owner-requested ad-hoc 复核，不是 daily / weekly / monthly scheduler entry。执行前先确认 TRADING-090 v0.3 rescue policy 已通过 `aits etf dynamic-v3-rescue validate`，再运行 `aits etf dynamic-v3-rescue real-evaluate`；该命令会先执行 `aits validate-data` 等价 cached market / macro data quality gate，失败即停止。完成后运行 `aits etf dynamic-v3-rescue validate-real`，并用 `real-report --latest` 或 Reader Brief 的 `Dynamic v0.3 Real Evaluation` 区块复核结果。`promote_candidate` / `observe_only` / `reject` 只是人工复核候选资格标签，不得生成 owner approval、shadow enrollment、official target weights、baseline mutation、production mutation 或 broker action。

## 正式输出

`daily-run` 的正式运行归档为：

```text
outputs/runs/daily/<executed_at_utc>/
  as_of_<YYYY-MM-DD>__<run_id>/
    manifest.json
    reports/
    traces/
    metadata/
```

`<executed_at_utc>` 使用 `YYYYMMDDTHHMMSSZ`，表示本轮实际执行时间；`as_of_<YYYY-MM-DD>` 表示市场评估日期。目录名使用 filesystem-safe run id；原始 run id、执行时间戳、评估日期和 run root 会写入 `manifest.json` 和 daily ops metadata。

`data/raw/` 与 `data/processed/` 是可校验状态缓存和输入引用来源，不是每次运行的完整归档副本。正式 run bundle 归档本轮报告、trace、metadata、manifest 和 checksum 引用；需要严格历史复现时使用 `outputs/replays/` 下的隔离 replay bundle。

OPS-069 起，交易日 `daily-run` 的第一项业务步骤是内部
`aits ops capture-daily-inputs --as-of YYYY-MM-DD`。它不是第二个 scheduler entry，而是由统一
trigger 调用的 umbrella step。输出包括：

```text
data/raw/daily_input_capture/YYYY-MM-DD/
data/processed/daily_input_capture/YYYY-MM-DD/
data/external/daily_input_capture/YYYY-MM-DD/
outputs/daily_input_capture/YYYY-MM-DD/
  daily_input_capture_manifest_YYYY-MM-DD.json
  daily_input_capture_validation_YYYY-MM-DD.json
outputs/daily_input_capture/
  daily_input_capture_gap_ledger.json
  daily_input_capture_recovery_queue.json
  daily_input_capture_recovery_queue_validation.json
  source_control/YYYY-MM-DD/<component>/state.json
```

一个 provider 失败时仍继续尝试其余来源；成功来源的 bytes 和 checksum 会保留。OPS-070 S1
起，validated `PARTIAL_CAPTURE` 表示 capture closure 完成，内部 command 返回成功但 executor
将该 step 记为 `LIMITED`；它不等于 required inputs 完整，也不等于数据质量通过。manifest 固定
`data_quality_status=NOT_EVALUATED`、
`consumer_cutover_allowed=false`、`production_effect=none`。人工复核可运行
`aits ops validate-daily-input-capture --as-of YYYY-MM-DD`；checksum、policy 或 safety drift
均为 `FAIL`。gap ledger 自 reviewed tracking start 按 XNYS session 显示
`CAPTURED/PARTIAL_CAPTURE/MISSED/INSUFFICIENT_DATA`，不得为修饰连续性而补抓历史 strict PIT。
交易日的 canonical market/macro `download-data` 已是 umbrella 的第一个 required component，
保留最多两次受控尝试；成功后将 `prices_daily.csv`、`prices_marketstack_daily.csv`、
`rates_daily.csv` 和 `download_manifest.csv` 复制到同日 `market_macro/` capture 目录。
后续 strict DQ 仍读取 canonical cache；同日副本只用于留存与审计，不授权消费。

OPS-070 S2 后，每个 component 使用独立 source/session idempotency key、attempt history 与
active lease。manifest 的 `blocker_code` 区分 credential、permission、quota、provider
unavailable、schema、filesystem/integrity、lease conflict、state invalid 和 attempt budget
exhausted；只有 policy 明确列出的 transient code 才能在该 source 自己的 budget 内重试。
不要删除 `source_control` state/lock 强行重试；stale lease 会先归档到 `lease_history`。

OPS-070 S3 的 recovery queue 只把 market/macro 标记为可人工准备 immutable raw backfill；
SEC 需要 owner 复核，FMP PIT/valuation 与 official sources 保持
`HISTORICAL_RECAPTURE_FORBIDDEN`。queue validator PASS 也不授权自动请求 provider、strict PIT、
DQ、score 或 consumer cutover。

Daily executor 按 `scheduled_tasks_v4` 的显式 dependency DAG 运行。`market_macro` 只控制
`validate_data` branch，`fmp_forward_pit` 只控制 PIT branch，`sec_companyfacts` 只控制 SEC branch；
`score_daily` 必须同时等到 strict DQ/PIT/SEC、`fmp_valuation` 和
`official_policy_sources`。缺失 component 或 upstream failure 会把相应 consumer 记为
`BLOCKED`，但无关 sibling 继续形成同日证据。`pipeline_health` 与 `secret_hygiene` 始终执行
operator closure。任一 required branch 不完整时 overall 仍为 `BLOCKED_DEPENDENCY` 或 `FAIL`，
不得生成最终 Reader Brief/finalization PASS。

外部 scheduler 部署前先运行：

```powershell
aits ops scheduler-checkout-preflight
```

外部 scheduler 必须在同一个 `aits ops daily-run` 入口设置 reviewed env contract，指向与开发
工作区不同的 clean ops checkout 和 exact release commit。daily-run 会在 provider/cache mutation
前再次执行 runtime preflight。当前 policy 固定 `activation_authorized=false`，不会安装或启用
Windows Task Scheduler/cron/GitHub Actions；该部署需要 owner 独立完成。

外部供应商调用前还有一层请求级缓存：`data/raw/external_request_cache/`。FMP、Marketstack、Cboe VIX、FRED、SEC、TSMC IR、官方政策源、EODHD 和 yfinance 路径的相同请求命中 cache 时不得再次请求供应商；只有 MISS 才发送请求。200～399 继续持久复用；4xx/5xx 按 `config/data/external_request_cache_lifecycle_policy.yaml` 的 reviewed pilot TTL 短期复用，到期后下一次业务调用 live revalidate。新记录使用 `metadata.json` atomic current pointer、`bodies/<sha256>.body` 和 `negative_observations/<generation_id>.json`；旧 v1 `response.body` 保持可读，过期、复验或显式失效都不能删除原始失败证据。这个缓存保护供应商额度，不替代业务 raw cache、download manifest、PIT manifest 或数据质量门禁。Cboe VIX 的 `VIX_History.csv` 是固定 URL 的可变静态文件，cache identity 额外包含 ticker/start/end/interval 业务窗口；命中缓存时还会校验 CSV 最大日期覆盖请求 `end`，避免新 as-of 或同窗口旧缓存复用过期 CSV 响应。排查供应商额度或重复请求问题时，先看该目录的 `metadata.json`、`generation_id`、`body_sha256`、`expires_at` 和 negative observation，不要重新跑 live 命令试探。若确需显式失效，必须从当前 pointer 取得 expected generation/body checksum，并用 `aits invalidate-external-request-cache` 提供 actor、reason、reference；CAS 失败时停止，不能手工删文件或覆盖证据。失效本身 `production_effect=none`，但下一次业务调用可能产生外部请求。若 `download-data` 失败，先看 `download_data_diagnostics_YYYY-MM-DD.md`；它记录 provider、失败阶段、cache status、cache key、脱敏请求参数，以及 Marketstack quota preflight 的 budget profile / `violation_reasons`，但不保存 stdout/stderr 原文或供应商响应正文。

FMP EOD 价格请求对 requests `Timeout` / `ConnectionError`（含 `SSLError`）采用最多三次的请求级有界重试，默认以 1 秒、2 秒递增退避；只有未收到响应的 transient transport error 可重试。HTTP status、invalid JSON、schema/provider error 仍立即 fail closed。重试耗尽时，`download_data_diagnostics_YYYY-MM-DD.md` 应记录脱敏请求参数、cache identity、attempt count、timeout 和异常类型；不得因重试删除 canonical state/ledger、跳过 `aits validate-data` 或提高 scheduler step attempt budget。

OPS-072 后，official policy/geopolitical adapter 对每个来源应用同样窄化的 pre-response
transport retry：TLS/timeout/connection 最多 3 次，退避 1 秒、2 秒；HTTP status、parser/schema/
provider error 不重试。成功来源报告与 download manifest 记录
`transport_attempt_count`；最终 exhaustion 在官方来源报告中记录
`official_policy_source_transport_exhausted`、`PROVIDER_UNAVAILABLE`、attempt/max-attempt、
timeout、exception type 与脱敏 message。CLI 另输出 stable `blocker_code=`，capture 只接受带
HTTP/status/auth 上下文的 401/403，不把候选数量等裸数字误判为 permission。若该 component
仍失败，`score_daily` 及其 Reader Brief/dashboard/lineage descendants 必须 BLOCKED；不要删除
source state、手工失效成功 cache、用第二个 trigger 重跑同 parent，或补造当日评分。

OPS-074 后，trading-day capture-active plan 会把同 `as_of` canonical
`daily_input_capture_manifest` 显式传给 `score_daily`。评分必须先用同一 capture
validator 复核 policy/status/component 与 artifact path/size/SHA，然后校验 download
manifest、candidate `as_of`/review/production boundary 和 raw payload path/SHA lineage。
验证通过后只读取留存 candidate CSV，不发起第二次 official-policy provider
请求；canonical report 必须披露 `evidence_mode=verified_retained_daily_capture`、
`provider_request_performed=false` 和 exact evidence SHA。任一 missing/tamper/drift/as-of
mismatch 都 fail closed，不回退至 live fetch。只有未传 capture manifest 的 manual/non-daily
`score-daily` 保留原 live-fetch contract。

OPS-063 的 `limited_non_pit_reconstruction.v1` 仅是 2026-07-13/14 缺 contemporaneous hard inputs 时经 owner 批准的一次性 manual evidence。它不属于 `aits ops daily-run`、periodic-dispatch、Reader Brief 或 governance 下游，canonical daily status 仍为 `INSUFFICIENT_DATA`；operator 不得用 live refetch、7/15 snapshot 或该 bundle 补造 PIT、score、position 或投资结论。未来若需复跑，必须先新建受治理 producer/validator 任务。

OPS-068 已建立受治理的 `limited_non_pit_reconstruction.v2` producer/validator，仅用于 owner 批准后的 2026-07-21 隔离历史事实证据。运行前必须先完成 explicit cache-only inventory，并显式传入 inventory bundle、owner decision id、bundle id 和需要保持 byte-identical 的 canonical cache/state/ledger guard paths：

```text
aits ops reconstruct-limited-non-pit
  --inventory-bundle <explicit-inventory-bundle>
  --owner-decision-id <owner-decision-id>
  --bundle-id <new-bundle-id>
  --guard-path data/raw/prices_daily.csv
  --guard-path data/raw/prices_marketstack_daily.csv
  --guard-path data/raw/rates_daily.csv
  --guard-path data/raw/download_manifest.csv
  --guard-path <explicit-2026-07-22-state>
  --guard-path <explicit-2026-07-22-ledger>
  --guard-path <explicit-2026-07-23-state>
  --guard-path <explicit-2026-07-23-ledger>
```

Producer 禁止 `latest`/glob 发现、live provider/OpenAI、canonical/latest pointer 或下游结论写入。输出中的 DQ 只验证隔离 market/macro facts，不能解释为 canonical requested-window receipt。生成后必须使用 `aits ops validate-limited-non-pit` 显式绑定同一 as-of、owner decision 和 inventory bundle；validator PASS 仍只表示 frozen bytes、DQ、market/macro facts、null contract 和无下游污染可复核，canonical daily evidence 保持 `MISSING`，结论保持 `INSUFFICIENT_DATA`。

过渡期仍可在 `outputs/reports/` 看到 legacy mirror。投资阅读入口优先级：

1. `evidence_dashboard_YYYY-MM-DD.html`：只读每日决策展示入口，不替代审计源。
2. `daily_score_YYYY-MM-DD.md`：趋势判断日报和 Decision Card。
3. `data_quality_YYYY-MM-DD.md`：市场和宏观缓存质量门禁。
4. `pipeline_health_YYYY-MM-DD.md`：关键 artifact 健康，不等于投资结论有效。
5. `daily_ops_run_YYYY-MM-DD.md` 与 `daily_ops_run_metadata_YYYY-MM-DD.json`：运行步骤和脱敏元数据。
6. `manifest.json`：本次 run 的输入、输出、checksum、legacy mirror 和 visibility cutoff。

## 阻断规则

必须停止并先排查：

- `aits validate-data` 或 `score-daily` 内部同一路径数据质量门禁失败。
- SEC metrics、估值快照、风险事件发生记录、execution policy 或 rule card 校验失败。
- 非 capture 的必需环境变量缺失只阻断对应 step 和 dependents；capture-managed provider key
  缺失由 component 记录后形成 `PARTIAL_CAPTURE`，不得在第一步前抹掉其他来源的抓取机会。
- 显式未来 `as_of` 或历史 `as_of` 被 `daily-run` 输入可见性预检查识别为 `BLOCKED_VISIBILITY`；不得用生产调度入口补跑 strict PIT 复现。
- `score-daily`、`pipeline health` 或 secret scan 报告状态为 `FAIL`。
- OpenAI 风险事件预审在启用状态下 fail closed。

可降级但必须披露：

- 显式 `--skip-risk-event-openai-precheck`，日报必须显示未执行自动预审。
- capture 中某个来源失败但其他来源已保全；缺失来源的 consumer 必须 `BLOCKED`，整条
  daily-run 仍非 PASS；成功 sibling bytes 只有在自己的 strict gate 通过后才能被对应 consumer
  使用，不得跨 branch 绕过门禁。
- 休市日模式跳过 `score-daily`，只保留官方政策/地缘来源抓取和健康检查。
- 第二数据源覆盖不足，报告必须保留 source limitation，不能写成跨源核验完成。

## 排查入口

|现象|优先检查|
|---|---|
|数据质量失败|`outputs/reports/data_quality_YYYY-MM-DD.md`、download manifest、provider health。|
|daily input capture partial/失败|`outputs/daily_input_capture/YYYY-MM-DD/daily_input_capture_manifest_YYYY-MM-DD.json` 与 validation，按 component 查看 blocker code、attempt history、source lease/idempotency、missing expected paths、retained checksum 和 daily report 的 `CAPTURE_COMPONENT_NOT_PASS`；不要删除旧 terminal key/source state。|
|连续交易日疑似漏跑|`outputs/daily_input_capture/daily_input_capture_gap_ledger.json`、`daily_input_capture_recovery_queue.json` 及 validation；`MISSED/PARTIAL_CAPTURE` 不补造 strict PIT，queue 不自动执行。|
|外部 scheduler 被 checkout 阻断|读取 `ops_scheduler_checkout_preflight_*.json/md`，核对 marker、独立绝对路径、exact release commit、origin remote、clean status 和 current-process checkout；不得改用 dirty development checkout。|
|`download-data` 失败|`outputs/reports/download_data_diagnostics_YYYY-MM-DD.md`，确认 provider、失败阶段、cache status、cache key 和脱敏请求参数。|
|疑似重复供应商请求|`data/raw/external_request_cache/<provider>/<api_family>/<cache_key>/metadata.json`，确认 cache key、status code 和 body checksum。|
|PIT checksum mismatch|`pit_snapshots_validation_YYYY-MM-DD.md`、`fmp_forward_pit_fetch_YYYY-MM-DD.md`、raw payload 路径。|
|日报没有生成|`daily_ops_run_YYYY-MM-DD.md` 的阻断步骤和对应子命令报告。|
|历史复现被 daily-run 阻断|改用 `outputs/replays/` 下的 `ops replay-day --mode cache-only` bundle，检查 `input_freeze_manifest.csv` 和 replay report。|
|报告存在但结论不可用|日报“结论使用等级”、Decision Card 的 Data Gate、人工复核摘要。|
|OpenAI 预审失败|`risk_event_prereview_openai_YYYY-MM-DD.md` 和本地 request cache，不保存 API key。|
|SEC PIT feature 需要认知评估|先确认 TRADING-039 `data/processed/sec_edgar/sec_pit_feature_panel.csv` 和数据质量门禁可用，再运行 `aits sec-pit evaluate --start 2023-01-01 --end YYYY-MM-DD --quality-as-of CACHE_AS_OF --feature-panel data/processed/sec_edgar/sec_pit_feature_panel.csv --universe config/sec_companies.yaml --benchmark QQQ --output-dir outputs/sec_pit_evaluation`；历史 evaluation 的 `--end` 是 feature/decision window，`--quality-as-of` 是 market/macro cache 质量门禁日期，不得因当前 cache 含 `--end` 之后 outcome label 数据而绕过质量门禁；只读取 evaluation artifacts，shadow weight 输出不修改 production。|
|Dynamic v0.2 review package 需要复核|先确认 latest TRADING-088 rescue evaluation 和 v0.4 candidate robustness report 指向同一 candidate；运行 `aits etf dynamic-v2-review package --latest-rescue-report` 或显式输入路径，再运行 `aits etf dynamic-v2-review validate`。该流程只生成 review-only package，不运行 enrollment、approval、production 或 broker。|
|Dynamic v0.3 rescue report 需要复核|先确认 latest TRADING-089 v0.4 review package 存在且 blocker 仍为 constraint/drawdown；运行 `aits etf dynamic-v3-rescue run --latest-v2-review` 或显式 `--v2-review-package <path>`，再运行 `aits etf dynamic-v3-rescue validate`。该流程只生成 candidate-only evaluation，不运行 enrollment、approval、production 或 broker。|
|Dynamic v0.3 real evaluation 需要复核|先确认 TRADING-090 validate PASS，再运行 `aits etf dynamic-v3-rescue real-evaluate`，该命令先过 `aits validate-data` 等价质量门禁；随后运行 `aits etf dynamic-v3-rescue validate-real` 和 `aits etf dynamic-v3-rescue real-report --latest`。`promote_candidate` 也只是人工复核资格标签，不运行 enrollment、approval、production 或 broker。|
|通知投递审计失败需要分类|先看 `data/derived/operator_briefs/notifications/delivery_audit/notification_delivery_audit_summary_YYYY-MM-DD.json`，再运行或读取 `outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`；TRADING-036 只分类和报告，不发送通知、不自动 retry。|
|通知投递失败需要 retry 候选队列|先看 `outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`，再运行 `python scripts/run_retry_candidate_queue.py`；如需指定源报告，运行 `python scripts/run_retry_candidate_queue.py --classification-report outputs/notification_delivery_failure_classification/notification_delivery_failure_classification_YYYY-MM-DD.json`。TRADING-037 只生成只读 retry candidate queue / manual approval gate 报告，不执行 retry、不发送 notification、不修改 delivery state 或 approval state。|
|retry candidate 需要人工 approval dry-run|先由人工创建或编辑 `inputs/manual_retry_approvals/manual_retry_approval_YYYY-MM-DD.json`，再运行 `python scripts/run_retry_execution_dry_run.py`；如需指定输入，运行 `python scripts/run_retry_execution_dry_run.py --queue-report outputs/retry_candidate_queue/retry_candidate_queue_YYYY-MM-DD.json --approval-record inputs/manual_retry_approvals/manual_retry_approval_YYYY-MM-DD.json`。TRADING-038 只读取 approval record 并生成 retry execution dry-run 报告，不执行 retry、不发送 notification、不修改 approval record、delivery state 或 production 参数。|
|疑似 secret|`secret_hygiene_YYYY-MM-DD.md`，只输出脱敏片段。|

## 调度示例

systemd timer 示例只作为部署参考，真实路径、用户、环境文件和时间由 owner 在云 VM 决策后确认：

```ini
[Unit]
Description=AITradingSystem daily ops

[Service]
Type=oneshot
WorkingDirectory=/opt/AITradingSystem
EnvironmentFile=/opt/AITradingSystem/.env
ExecStart=/opt/AITradingSystem/.venv/bin/aits ops daily-run
```

```cron
# 美股盘后示例；具体时区和数据稳定窗口需 owner 确认
30 22 * * 1-5 cd /opt/AITradingSystem && .venv/bin/aits ops daily-run
```

凭据不得写入仓库；stdout/stderr 可由系统日志保存，但正式审计以 `outputs/runs/daily/<executed_at_utc>/.../manifest.json`、daily ops metadata 和各质量报告为准。
