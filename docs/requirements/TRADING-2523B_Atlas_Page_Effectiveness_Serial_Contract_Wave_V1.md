# TRADING-2523B：Atlas Page Effectiveness 串行合同波 V1

最后更新：2026-08-15

- stable task id：`TRADING-2523B_ATLAS_PAGE_EFFECTIVENESS_SERIAL_CONTRACT_WAVE_V1`
- priority：`P1`
- status：`DONE`
- governed mode：`SINGLE_LANE`
- contract change：`true`
- exact registration base：`b20757326430152a1f3340cf5871773595194a8b`
- triggering plan：`integration-revalidation-6dcc47dbf6e7454bc540`
- triggering plan SHA-256：`6dcc47dbf6e7454bc5405eae89897f45b513bcd32a96ad2531d9ff36cd6be330`
- production effect：`none`
- broker action：`none`

## 1. 触发原因

TRADING-2523A 的真实 base-drift plan 对以下身份完成了重放：

- frozen base：`f876ec853c1431e760bc4cf5b89123265a32080f`；
- frozen terminology lane：`b8c71e0610c42e3fcb8fb8ba6872876e28bcc45a`；
- latest main：`b20757326430152a1f3340cf5871773595194a8b`。

plan validator 为 `PASS`，但 typed decision 是 `SERIAL_CONTRACT_WAVE_REQUIRED`。lane 与
mainline 均以 `WRITE` 修改 `strategy-research-page-effectiveness@1.0.0`；不得通过省略
mainline claim、直接三方合并或把冲突降级为普通 generated refresh 来绕过。

## 2. 冻结目标

本波只冻结 page-effectiveness 的最小 consumer-visible 合同，不吸收 2523 的术语表、renderer
呈现、HTML、后续 2524--2527 页面改造，也不实现或修改独立的 2528 transport successor。

合同目标：

1. 将 manifest schema 明确推进为 `strategy_research_page_effectiveness.v2`；
2. human review 为 `PASS` 时必须绑定 exact canonical page SHA-256；非 PASS 不得携带该 SHA，
   engineering track 不得冒充 human review；
3. task coverage 接受 canonical `TRADING-<number><optional suffix>_...` identity，例如
   `TRADING-2523A_...`，并按 `(number, suffix)` 确定性升序；空集合、重复、非法 identity 或乱序
   fail closed；
4. completeness 不再由硬编码 task count、连续数字范围或特殊尾号维护，而由 policy exact set、
   canonical task registry successor replay 与 manifest equality 共同证明；
5. 保留 latest main 已发布的 2521 admitted-unused、2522 invalid run 与 2528 offline diagnostic
   disclosure，不回退 Cloud、DQ/PIT、engine 或授权事实；
6. human review PASS 必须与 manifest 中 canonical `index.html` identity 相等，否则 validation
   返回 typed failure。

## 3. 最小实施顺序

### S0：合同与负例

- 冻结 v2 schema、task identity parser/order 和 review-page binding；
- 覆盖 suffix identity、乱序、重复、非法 task id、human PASS 缺 SHA、错误 SHA、非 PASS 带 SHA、
  engineering 携带 human 字段等负例。

### S1：consumer 与 policy

- `load_page_effectiveness_policy` 与 manifest validation 只消费上述 typed helpers；
- policy 新增本合同任务的 disclosure，但保留 2522/2528 exact current facts；
- 不生成新的策略结论、订单权限、Cloud run、生产或 broker 动作。

### S2：authority、验证与发布

- 更新本 requirement、canonical task projection、system flow 与适用 architecture/test/compatibility
  authority；
- focused tests 后，在 final tree 运行 Architecture、Contract、Integration、Reproducibility 与
  exclusive Full；
- PASS 后按 SINGLE_LANE 规则 fast-forward local main、普通 non-force push并复核 SHA。

## 4. Path claims

Task-owned：

- `docs/requirements/TRADING-2523B_Atlas_Page_Effectiveness_Serial_Contract_Wave_V1.md`；
- `src/ai_trading_system/contracts/strategy_research_page_effectiveness.py`；
- `tests/atlas/test_page_effectiveness.py`；
- `tests/atlas/test_cited_query_renderer.py`（human PASS consumer compatibility）；
- `tests/atlas/test_historical_projection_review.py`（旧 ignored sidecar 明确 stale/skip，不降级解析）；
- `tests/test_arch_005_s5_task_source_cutover.py`；
- `tests/test_devx_006d_report_catalog_flow_authority.py`。

Coordinator-owned：

- `config/atlas/page_effectiveness.yaml`；
- `src/ai_trading_system/atlas/page_effectiveness.py`；
- `src/ai_trading_system/atlas/cited_query_renderer.py`（仅移除重复的 fixed-count guard，不改呈现）；
- canonical task registry/index 与 generated task views；
- `docs/system_flow.md`；
- architecture module/test/deprecation manifests 与 append-only compatibility authority。

明确不归属：

- 2523 terminology authority、projection、inventory、renderer 与 HTML/sidecars；
- 2524--2527 follow-on implementation；
- 2528 QQQ transport code、requirement、evidence 或 task authority；
- `docs/research/growth_tilt_owner_diagnosis_pack.md` known-unrelated exclusion。

## 5. 验收标准

1. v2 manifest exact replay、canonical JSON 与 content hash 可重放；旧 v1 payload 不会被静默解释为
   v2。
2. `TRADING-2523A_...` 与无 suffix task id 可确定排序；非法、重复、乱序均 typed fail closed。
3. human PASS 缺失或错绑 canonical page SHA 均失败；pending/fail review 不复用旧页面 identity。
4. 2522/2528 reader summary、coverage 与安全边界保持 latest-main 事实，不从 frozen lane 回退。
5. 本波不包含 terminology renderer/HTML 或 QQQ transport mutation。
6. focused、五级正式门、governed audit、local-main integration 与 ordinary push 全部通过。

## 6. 后续边界

本波发布后的 exact main 才是 2523A rebuild base。2523A 必须从该新 base 重建 terminology lane，
丢弃 frozen lane 中已由本波取代的 page-effectiveness contract bytes；再次生成/验证真实
`integration_revalidation_plan.v1` 后，才可构造唯一 latest-main candidate。

## 7. 进展记录

- 2026-08-15：v2 contract、suffix-aware task identity/order、human PASS exact page SHA-256
  binding、policy successor replay 与 renderer consumer 已实现；未修改 terminology 呈现或 2528。
- 2026-08-15：ARCH-004E、ARCH-005 task source 与 DEVX-006D report/catalog/flow authority
  deterministic rebuild/validate 均为 `PASS`。
- 2026-08-15：final focused validation：`70 passed in 104.46s`；Atlas projection refresh 后的
  focused validation：`24 passed in 82.38s`；首轮 Full 的 4 个精确失败点修复复核为
  `3 passed / 1 skipped in 15.26s`，其中 skip 只声明本机 ignored v1 sidecar 早于 v2 合同，未静默
  解析或改写该历史证据。
- 2026-08-15：首轮 Full `full_20260815T110413Z` 为
  `9024 passed / 4 failed / 3 skipped / 644 warnings`，失败证据锁定为 compatibility authority
  未随最终 ARCH-004E manifests 重放，以及本机 ignored v1 sidecar 的显式版本不匹配。
- 2026-08-15：最终树串行验证全部通过：Architecture
  `865 passed in 317.33s`（`architecture-fitness_20260815T114202Z`）、Contract
  `276 passed in 147.19s`（`contract-validation_20260815T114729Z`）、Integration
  `995 passed / 642 warnings in 46.56s`（`integration_20260815T115006Z`）、Reproducibility
  `24 passed in 20.29s`（`reproducibility_20260815T115102Z`）。
- 2026-08-15：failure-fix Full 以首轮失败 artifact 为 exact parent，最终
  `9027 passed / 4 skipped / 644 warnings in 1309.73s`，runtime artifact 为
  `full_20260815T115139Z`；production effect 与 broker action 均为 `none`。
