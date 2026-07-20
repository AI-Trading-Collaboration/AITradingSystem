# TRADING-2449 canonical artifact recovery audit

最后更新：2026-07-20

状态：`PASS_WITH_PORTABILITY_FOLLOW_UP`

## 审计结论

Wave 1 strategy-evidence lane 从可信 Git worktree
`D:\Work\AITradingSystem-eb0-candidate`（HEAD
`1a33122448521abfc49eaf044ffef21fd715f0c2`）恢复了 TRADING-2446～2448 的 exact
R0/R1/forward/R2 canonical bytes。复制前后逐文件相对路径、长度与 SHA-256 全部一致；未运行
backtest、candidate generation、parameter search、evaluator 或 locked-holdout access。

Git 历史只跟踪实现与文档，两个现存 stash 不包含这些 ignored runtime artifacts；其他历史和
supervised worktree 只包含文档引用，不包含目标 bundle。因此本次来源不是 synthetic fixture、重新计算
或同名替代物。

## 恢复清单

|Evidence|Identity / 路径|恢复规模与关键 SHA-256|
|---|---|---|
|R0 preflight|`outputs/research_ops/strategy_restart/strategy_research_restart_preflight.{json,md}`|JSON=`b7272a44f8f9eba845e47a103895de7ce207da0913d5518d08fdd5be2ad4faf9`；Markdown=`a9cc63c98855290fce441f21676126bddf1f123918d69a577a03fd758619ffa0`|
|R1 walk-forward|`r1-wf_6447beb5464bad37`|84 files / 156,823,314 bytes；report=`de581cede1c6a8c5454bbc07b760843b0f030010a376bce39a34761828a7e234`；manifest=`50a60f7aca02cd4e11b4a6fd982c7faf2cf46a9933b5eaa57f3dc1f4f89888bc`|
|R1 robustness|`r1-robustness_8c93b0e2615d0ace`|6 files / 10,802,741 bytes；report=`dc5964a43dd096301058e6039e52ba08bb22586a75de8e5d87bdabe47ade1243`；manifest=`876b9d81ed98b8e0f38b696f51e253c5ad15e29e75e631ce725231b1c95e8692`|
|Forward maturity|`outputs/forward_evidence/maturity_tracker_r1/`|4 files / 52,976 bytes|
|R2 decision|`r2-decision_c761da11538fc58c`|decision JSON=`2b5055e2cba784df757974062e76c6081d860a500683f8136abf45878212d423`；manifest=`0fc4ded3cbde2aac73d6d6f7dee50a0946a7a9d5a7632a1b78dac3f677968743`|

所有 live source commitments 仍与当前文件匹配，包括 restart policy、prices、secondary prices、rates、
download manifest、candidate results、leaderboard、normalized config、sweep manifest、real evaluation
与 forward ledger。

## 验证结果

- R0 validator：`PASS / 0 failed`；
- R1 walk-forward validator：`PASS / 0 failed`；
- R1 robustness validator：`PASS / 0 failed`；
- R2 validator：`PASS / 0 failed`，decision 保持 `CONTINUE_EVIDENCE_CLOSURE`，
  `candidate_expansion_allowed=false`；
- TRADING-2449 gate：`clean-selection-gate_caed06d5b6175e9f`，
  `BLOCKED_CONTAMINATED_LEGACY_SOURCE`；validator=`PASS / 0 failed`；
- gate JSON/Markdown/manifest SHA-256 分别为
  `ea64280e238964ce32ca817f1188a7d4c064596a8a693019011ae720c15d90ce`、
  `42e7006cd4d90f7b5fe1b8d7bfa359cf7bb399943cd1ec4415b81e92cf4b1d31`、
  `7f75a2e80a6a8635b7bcd658cb13d717bfce8f3ed7c0e50138035047a428a2aa`；
- contamination evidence：full-period source leaderboard top-N=`20`，locked-holdout overlap=`4`；
- `clean_run_unblocked=false`、candidate expansion/new search/evaluator/holdout access 全部为 false，
  `production_effect=none`、`broker_action=none`。

## Portability follow-up

为保持 exact bytes，R0、walk-forward 与 R2 内部的历史绝对路径 commitment 仍指向生成时的
`AITradingSystem-eb0-candidate` worktree。该 worktree 当前存在，因此 live-path validators PASS；如果
未来删除或移动它，旧 artifact 会按设计 fail closed。

不得直接改写旧 JSON、manifest 或 Markdown 来替换路径，因为这会破坏既有 ID、checksum 和 exact
recovery 事实。portable lineage 需要独立 versioned contract，以 content-addressed 或 project-relative
locator 加 sidecar binding 连接 immutable legacy bytes，并保留原始路径 provenance。后续工作登记为
`TRADING-2450_LEGACY_RESEARCH_ARTIFACT_PORTABLE_LINEAGE`。
