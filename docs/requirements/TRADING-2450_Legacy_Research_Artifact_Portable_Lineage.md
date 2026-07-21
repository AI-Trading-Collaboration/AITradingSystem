# TRADING-2450：Legacy research artifact portable lineage

最后更新：2026-07-20

状态：`DONE`

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

## 执行链路与输入输出

1. reviewed policy 输入为
   `config/research/legacy_research_artifact_portable_lineage_policy.yaml`。loader 对 v1 的 sidecar、resolution、
   distribution、consumer、reason-code 与 safety contract 做 exact 校验；缺文件、YAML 损坏或任一 policy
   drift 统一以 `POLICY_SCHEMA_INVALID` fail closed。
2. canonical sidecar 输入为
   `inputs/research/legacy_lineage/trading2449_r0_r1_r2_portable_lineage.v1.json`。builder 只读扫描恢复后的
   R0、walk-forward、robustness、R2 manifest 和其原有 source path graph，输出 4 个 immutable artifact
   bindings 与 108 个 source bindings；每条 binding 固定 original legacy path、project-relative locator、
   consumer、size 与 SHA-256，sidecar 自身以 canonical content ID 和 policy SHA 绑定。
3. portable mode 只有调用 validator 时显式传入 `portable_lineage_sidecar_path` 才启用；未传入时沿用原直接
   path 行为。resolver 先验证 policy、sidecar ID、policy hash、subject artifact binding 和该 consumer 的
   全部 source binding，再返回 project root 内的 locator。locator 绝对路径、`..`、symlink escape、缺失或
   checksum/size drift 均失败。
4. historical source 不存在时允许按 portable source 的 exact bytes replay；若 historical 与 portable 同时
   存在，则二者必须和 binding 完全一致，任一冲突不得选择优先级绕过。R0/R1/R2 validator 随后继续执行
   原 checksum、lineage、fold/comparator、forward/DQ、Markdown 与 decision 重算逻辑，不接受 resolver
   自身 PASS 替代业务 validator PASS。
5. 输出是在原 validation payload 上追加的 `portable_lineage_resolution` evidence，包含 mode、consumer、
   sidecar/policy ID 与 SHA、legacy artifact binding、resolved sources 及稳定 reason code；不改写任何 legacy
   artifact。archive 安装必须把原 artifact/source bytes 放回 sidecar 指定的 project-relative locator；只有
   tracked sidecar、尚未安装 archive 的 clean clone 明确报 `PORTABLE_SOURCE_MISSING`。

当前 canonical sidecar ID=`portable-lineage_dfa5dfc7208e5913fc75`、SHA-256=
`031428a97a3e123486142bc603a7df52f65d1b32f987cef32630b13e296c4f9b`。policy 或 source 变化不会静默
重映射，必须经 review 重建 sidecar 并重新通过全部 validator。

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
- 2026-07-20：project owner 要求继续双线推进，Wave 2 从 base=`ca9dea5e` 启动。本 lane 持有
  locator policy/module、sidecar builder、R0/R1/R2 opt-in resolution adapters 与 focused tests；不得编辑
  task register、system flow、artifact catalog、compatibility/deprecation/manifests/generated registry，
  这些共享路径由 integration coordinator 单写。不得改写 recovered legacy bytes、运行 backtest、候选、
  搜索或 provider refresh；`production_effect=none`、`broker_action=none`。
- 2026-07-20：实现完成并转 `VALIDATING`。versioned policy、tracked canonical sidecar、只读 builder/resolver
  及 R0/WF/robustness/R2 explicit opt-in adapters 已实现；默认 direct-path 行为兼容。真实 recovered bundle
  replay 为 R0/WF/robustness/R2=`PASS/PASS/PASS/PASS`，source counts=`13/87/9/16`，R2 decision 保持
  `CONTINUE_EVIDENCE_CLOSURE`，TRADING-2449 gate 回归 PASS 且仍为
  `BLOCKED_CONTAMINATED_LEGACY_SOURCE`；四份 canonical artifacts 前后 size/SHA 不变。focused=`33 passed /
  20.05s`，Ruff/Black/mypy/diff-check PASS；shared manifests 与 formal tiers 由 coordinator 收口。
- 2026-07-20：Wave 2 integration 完成并转 `DONE`。architecture/contract/reproducibility=
  `446/265/23 passed`，正式 Full=`6487 passed / 2 skipped / 642 warnings`，runner wall=`1169.47s`；
  collection=`6489 nodes / 1084 files`，duration scheduler applied=true、fallback=false，profile/telemetry/
  performance/provenance 全部 PASS。Full 前后 legacy artifacts、R2 decision 与 TRADING-2449 gate 均未变化；
  `production_effect=none`、`broker_action=none`。后续 archive operator 只可依据 tracked sidecar 安装 exact
  bytes；任何 policy、locator、size 或 SHA drift 必须重新 review，不重开本任务来改写旧证据。
- 2026-07-21：TRADING-2452 将 active primary window 迁移到 `2021-02-22` 后，sidecar 绑定的
  `strategy_research_restart_policy.yaml` 与 `controlled_strategy_next_stage_research.yaml` 历史 bytes 不再
  位于 active locator。canonical sidecar 与四份 immutable artifacts 均保持不变；当前重放按设计返回
  `HISTORICAL_PORTABLE_CONFLICT`。精确历史 source archive 与分离 locator 由 TRADING-2454 承接，
  不用当前 source 重建 sidecar，也不回滚 active window。
