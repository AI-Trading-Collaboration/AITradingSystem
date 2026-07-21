# TRADING-2454：统一窗口迁移后的历史 Portable Source Archive

最后更新：2026-07-21

状态：`READY`

稳定任务 ID：`TRADING-2454_HISTORICAL_PORTABLE_SOURCE_ARCHIVE_AFTER_WINDOW_MIGRATION`

## 背景

TRADING-2450 的 canonical portable-lineage sidecar
`portable-lineage_dfa5dfc7208e5913fc75` 冻结了 TRADING-2449 R0/R1/R2 当时使用的 108 个
source bindings。TRADING-2452 按 owner 决策把 active primary window 迁移到 `2021-02-22` 后，
其中两份仍被 active runtime 使用的配置已经合法变化：

- `config/research/strategy_research_restart_policy.yaml`；
- `config/research/controlled_strategy_next_stage_research.yaml`。

四份 immutable R0/R1/R2 artifacts 仍保持原 byte/hash；canonical sidecar 也必须保持冻结。因为当前
project-relative locator 上的 source bytes 不再等于历史 binding，portable validators 现在正确返回
`HISTORICAL_PORTABLE_CONFLICT`。用当前 source 重建 sidecar 会破坏“原结果由原 source 重放”的语义，
不得作为修复。

## 目标与阶段

|阶段|内容|验收|
|---|---|---|
|S0|恢复精确历史 source bytes|从可信历史 commit/archive 恢复上述两份配置在 TRADING-2450 sidecar 冻结时的 exact bytes，并记录来源 commit、size、SHA-256|
|S1|设计 versioned archive locator|历史 archive 与 active config 路径分离；resolver 明确选择 sidecar 绑定的历史 source，不覆盖 active `2021-02-22` 配置|
|S2|重放与 tamper 验证|R0/WF/robustness/R2 四级 validator 全部 PASS；任一 archive missing/hash/size/path drift 继续 fail closed|
|S3|共享收口|artifact catalog、system flow、compatibility/source hashes 与 focused/reproducibility/architecture/contract/full 按风险更新|

## 安全边界

- 不改写 canonical TRADING-2450 sidecar、四份 legacy artifacts 或原 decision；
- 不把当前 2021 active config 复制并标记为历史 source；
- 不回滚 active window，不运行 backtest、candidate generation、parameter search 或 prospective holdout；
- archive 未完成前保持 `HISTORICAL_PORTABLE_CONFLICT`，不得将 validator PASS 人工替代；
- 全部工作 `production_effect=none`、`broker_action=none`。

## 依赖与下一责任方

- 依赖可信历史 commit/archive 能提供两份配置的 exact bytes；
- strategy evidence platform 负责 source provenance 与 replay，integration coordinator 负责共享文档和
  manifests；
- TRADING-2452 和 TRADING-2453 不依赖历史 replay 恢复，不能用本任务阻塞当前 active-window closeout。
