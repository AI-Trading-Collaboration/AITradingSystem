# TRADING-2277 Gate Policy v2 Challenger Rerun

最后更新：2026-06-28

## 背景

TRADING-2273 challenger matrix 目前只给出 offline validation-ready rows，不能直接解释为
promotion candidates。TRADING-2275/2276 完成后，需要用 gate policy v2 和 active selection
audit 结果重跑 challenger 状态分类，目标是把候选从 binary dead state 转成可审计研究状态。

## 范围

- 使用 gate policy v2 semantics 重新解释 TRADING-2273 challenger rows。
- 区分 `BLOCKED`、`OWNER_REVIEW_REQUIRED`、`OFFLINE_VALIDATION_READY` 和
  `PROMOTION_READY=false`。
- 不训练模型、不直接生成 promotion candidate。

## 边界

Promotion、paper-shadow、production 和 broker action 继续固定 false/none。

## 进展

- 2026-06-28：新增为 `READY`，依赖 TRADING-2275 和 TRADING-2276 完成。
