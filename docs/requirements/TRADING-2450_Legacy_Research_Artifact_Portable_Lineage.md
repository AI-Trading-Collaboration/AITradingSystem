# TRADING-2450：Legacy research artifact portable lineage

最后更新：2026-07-20

状态：`READY`

## 问题与目标

TRADING-2449 exact recovery 证明旧 R0/R1/R2 bundle 的 bytes、commitments 和当前结论均有效，但部分
artifact 把生成时的绝对 worktree 路径纳入 immutable lineage。当前 validator 依赖历史 worktree
`D:\Work\AITradingSystem-eb0-candidate` 存在；删除或移动它会使已恢复证据 fail closed。

本任务建立 portable、可审计且不改写 legacy bytes 的 lineage resolution contract，使 clean clone 或
受控 archive 能通过 content identity 找到同一 source，同时保留原 absolute path 作为历史 provenance。
它不重新计算研究结果，也不改变 R2 或 TRADING-2449 gate。

## 设计边界

- legacy JSON/Markdown/manifest 保持 byte-identical，不替换内部 path、ID 或 checksum；
- 新 contract 必须 versioned，并绑定 legacy artifact SHA-256、原 path、portable locator、source SHA-256、
  resolver policy/version 与 resolution evidence；
- portable locator 优先采用 project-relative path 或 content-addressed archive；不得依赖“latest”或仅同名匹配；
- validator 必须区分 legacy path 仍可用、portable locator 可用、两者冲突、两者缺失和 bytes tampered；
- path 与 portable source 同时存在但内容不一致时 fail closed，不能静默选择任一版本；
- 不运行 backtest、evaluator、candidate generation、parameter search 或 provider refresh，不改变 DQ/PIT、
  window、holdout、threshold、策略结论、production 或 broker 状态。

## 实施拆解

1. 冻结 locator/resolution schema、source priority、conflict reason codes 与 migration boundary；
2. 实现只读 resolver 与 sidecar builder，不接触 immutable legacy artifact；
3. 为 R0/R1/R2 validator 增加显式 opt-in portable resolution adapter，保持旧直接路径行为兼容；
4. 覆盖 clean clone、missing legacy worktree、hash conflict、path traversal、sidecar tamper 和 exact replay；
5. 更新 artifact catalog、system flow、module/test manifests 与 reproducibility/formal gates。

## 验收标准

- 历史 worktree 不存在时，同一 content-addressed source 可使 R0/R1/R2 validator PASS；
- 原 legacy artifacts 的 path/length/SHA-256 与 run IDs 全部不变；
- legacy path 与 portable locator 任一漂移或冲突均给出稳定 fail-closed reason；
- clean clone/replay evidence 绑定 resolver policy 与全部 source hashes；
- TRADING-2449 仍为 `BLOCKED_CONTAMINATED_LEGACY_SOURCE`，R2 仍为
  `CONTINUE_EVIDENCE_CLOSURE`，不得由 portability 修复推断 clean eligibility；
- focused、reproducibility、architecture/contract 与风险相称的 full PASS，
  `production_effect=none`、`broker_action=none`。

## 状态记录

- 2026-07-20：由 TRADING-2449 exact recovery audit 登记。当前无数据缺失，主要依赖为 versioned
  resolver contract 设计与 immutable-artifact compatibility review；未授权在当前 Wave 1 内顺带实现。
