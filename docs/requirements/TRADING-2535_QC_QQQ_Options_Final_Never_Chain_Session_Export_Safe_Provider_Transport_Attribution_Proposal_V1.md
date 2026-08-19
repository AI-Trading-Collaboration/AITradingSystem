# TRADING-2535 — QQQ Options final never-chain session export-safe provider/transport attribution proposal V1

- priority: `P0`
- status: `DONE`（terminal proposal candidate；以 final-tree validation 与 ordinary push 为 RELEASE 条件）
- owner: Codex capability coordinator（offline proposal）；Project Owner（任何 external action）
- governed mode: `SINGLE_LANE`
- contract change: `false`
- registration base: `c290f1244bb81df789d3b95d29d894b657943ca8`
- production effect: `none`
- broker action: `none`

## 1. 问题与目标

TRADING-2532 的唯一 zero-order external validation 在 `2021-02-22..2025-12-02`
精确覆盖 1202 个 session：1201 个 session 最终收到 option chain，1 个 session
从始至终没有 chain。TRADING-2533 因而保持 `chain_presence=FAIL`；TRADING-2534
修复了 staged readiness 与 export-safe evidence authority 的结构性循环，但明确不把该事实
改写成 PASS。

现有 export-safe aggregate 只有窗口级计数，没有给出唯一缺失 session 的日期，也没有同时绑定
provider catalog 与 subscribed `Slice.option_chains` 的交付状态。仅凭 `1 never-chain` 无法判断：

1. provider/universe 在该日没有可用 QQQ option contract；
2. provider/universe 有可用 contract，但已配置的 option subscription 没有把 chain 交付到 Slice；
3. provider 查询本身失败；
4. 现有证据仍不足以归因。

本任务只生成并验证一个独立、sealed、zero-order、export-safe 的 attribution proposal；不访问
QuantConnect，不修改 Cloud project，不启动 backtest，不读取或导出 raw option rows。

## 2. 冻结输入

- released predecessor main: `c290f1244bb81df789d3b95d29d894b657943ca8`；
- TRADING-2532 backtest id: `acf111f24d09a41870f9a23e93fcbe3b`；
- requested range: `2021-02-22..2025-12-02`；
- expected/observed sessions: `1202 / 1202`；
- 2532 export-safe evidence file SHA-256:
  `c25029918785ccd9321cb9087620f989b3f6b951e2c242c279bf42cc99b25671`；
- 2532 export-safe evidence content seal:
  `ffa9faafd1d480282bcfe1c07c896f538f26d2b23d7d7d8356460bc881e0bc49`；
- 2533 admission report file SHA-256:
  `d04107b82e2afbf5edb2bcb5b937d8f343eef285e6114752c259815f95c9ef18`；
- 2533 admission report content seal:
  `58a80cf8c0c7678dd1eab0cc8b3297fc1c27a6aace45f46d6789efc2446d7c0a`；
- 2534 staged-readiness policy/evaluator and final release main identities must be bound by the
  generated proposal package.

Any hash、count、window、task identity 或 canonical serialization drift 必须 fail closed。

## 3. 候选诊断的最小可导出字段

候选 Cloud code 只能通过 bounded runtime statistics 导出：

- unique final never-chain session count；
- unique final never-chain session date（仅在 exact count 为 1 时）；
- 该 session 的 canonical QQQ equity Slice 是否出现；
- 该 session 的 subscribed option-chain event count；
- 同日 provider/universe probe 的状态 `AVAILABLE / EMPTY / ERROR / NOT_EVALUATED`；
- provider contract count（只允许非负 count，不允许 symbol 列表）；
- exact classification；
- expected/observed session count、orders、fills、portfolio、raw/log/Object Store safety counters；
- engine/platform/bundle/evidence identity。

明确禁止导出：option symbol、contract identifier、strike、expiry、right、bid、ask、last、quote、
Greeks、IV、OI、volume、raw row、异常消息正文、日志载荷或 Object Store 内容。

## 4. Typed attribution contract

仅允许以下结论：

- `PROVIDER_CATALOG_EMPTY_FOR_TARGET_SESSION`：unique never-chain=1，provider probe 成功且 count=0；
- `PROVIDER_CATALOG_AVAILABLE_BUT_SUBSCRIBED_SLICE_NEVER_DELIVERED`：unique never-chain=1，
  provider probe 成功且 count>0，同时 subscribed chain event count=0；
- `PROVIDER_PROBE_ERROR`：unique never-chain=1 且 provider probe 为 ERROR；
- `ATTRIBUTION_INDETERMINATE`：任何 identity/count/window 不完整或不满足上述 exact 条件。

这些状态只归因 data availability/transport，不代表 DQ/PIT PASS，不授权研究、selection、engine、
策略结论、交易或 broker action。异常正文不得作为数据导出；ERROR 只输出稳定 reason code。

## 5. 外部动作边界

proposal 完成前以及 Project Owner 对 final exact hashes 给出新的单次 token 前：

- `maximum_project_mutations=0`；
- `maximum_cloud_backtests=0`；
- `maximum_orders=0`；
- `maximum_fills=0`；
- `external_action=none`。

proposal package 可以提出未来上限 `1 project mutation / 1 zero-order Cloud backtest / 0 orders /
0 fills`，但该上限本身不是授权。TRADING-2532 token 已 single-use consumed 且过期，不得复用。

## 6. 实施范围

Task-owned：

- 本 supporting requirement；
- `config/research/qc_qqq_options_final_never_chain_provider_transport_attribution_proposal_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/final_never_chain_provider_transport_attribution_proposal.py`；
- `tests/test_qqq_options_final_never_chain_provider_transport_attribution_proposal.py`；
- `inputs/research/qqq_options/trading_2535_final_never_chain_provider_transport_attribution_proposal_v1/**`；
- task-specific architecture fragments。

Coordinator-owned：canonical task registry/index、generated task views、`docs/system_flow.md`、
generated architecture/compatibility/deprecation authority 与 formal validation artifacts。

明确不修改 2532/2533 immutable evidence、2481/2482 contracts、2534 policy/evaluator、QuantConnect
project/API/browser、raw Results、Cloud、Object Store、selection/engine/order/fill/broker 行为。

## 7. 阶段与验收

### S0 — Registration

- canonical task row 与本 requirement 建立；
- governed `SINGLE_LANE` preflight PASS。

### S1 — Offline sealed proposal

- strict typed policy、proposal、run scope、manifest 与 candidate `main.py` deterministic build；
- exact predecessor hashes、session counts、classification contract 与 safety boundary 绑定；
- candidate code compile，静态拒绝 raw fields/log/Object Store/orders/network；
- package load/replay/tamper tests PASS；
- 生成 exact content/canonical/project-code/package hashes 与 unsigned owner-decision request。

### S2 — Publication

- system flow 与 architecture fragments 同步；
- focused 与 applicable formal validation 在 final tree PASS；
- task terminal、local-main ff-only、ordinary non-force push、SHA verify 与 cleanup 完成；
- publication 后停在 `OWNER_FINAL_TOKEN_REQUIRED`，不得自动访问 QuantConnect。

## 8. 实施与预关闭证据

- strict policy、sealed run scope/proposal/manifest、candidate `main.py`、unsigned owner request、
  replay 与 tamper rejection 已实现；
- policy file/canonical SHA-256：
  `3b101cdca7c85c01b9d4a5a5fe8a51b80ab0cc4d1e768bf8a9d8a31d830d01e1` /
  `1c075a5a7cc153e730d03a138f863f5ed3736b1424a1027c2f060eb59bb443bf`；
- run scope content/canonical SHA-256：
  `98606ee39114622ba8e1d1f14fc06119f7829bac2c326feb98076b39324f4e8c` /
  `ef67fefb3313a9881861150779ddcc1eca809f5b032c98e4eb0aff7e32469748`；
- proposal content/canonical SHA-256：
  `83f19609f617d8a2ec1ec68b935a7b54558f4e2ee6ff6884a430323d111612de` /
  `aff23a1fc9c49dfd3a8d14a6b8cf2940d9749eceb76d060a0e895a421e06fca3`；
- project code：`22533 bytes` / SHA-256
  `9307d438da6ba0b46f42c590db683d383d3b272e973bdede2819166ebbf18ebe`；
- package manifest content/canonical SHA-256：
  `3978c94ad4a5fa00ef77ae9325bec727bc20df0bc722e123916f22e821b927c1` /
  `a076912219f948d18112cc5df59658af410084c17fa521bd3a184a12eb480e45`；
- focused proposal/replay/tamper：`13 passed`；2529→2534 邻接链：`76 passed`；
  architecture/generated authority focused：`60 passed`；Ruff PASS；
- 首轮 focused 的 `4 failed / 9 passed` 与第二轮 `2 failed / 11 passed` 均来自 sandbox
  package 测试预期没有遵循 repository-bound loader 与 canonical-record typed error；修正测试夹具和
  exact error expectation 后第三轮 `13 passed`，未放宽 production loader；
- 当前与本任务相关的 external counters 保持
  `project_mutations/cloud_backtests/orders/fills = 0/0/0/0`；没有访问 Cloud、browser、API、raw rows、
  Object Store、selection、engine 或 broker；
- terminal publication 后 next state 固定为 `OWNER_FINAL_TOKEN_REQUIRED`。任何 external attempt
  必须另做 exact token admission，且首次 attempt 无论成功或失败都消费授权。
