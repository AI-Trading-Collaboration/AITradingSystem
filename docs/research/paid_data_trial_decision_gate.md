# Paid Data Trial Decision Gate

Policy：`config/research/paid_data_trial_gate_policy.yaml`

## 当前 gate 结果

`NORGATE_TRIAL_RECOMMENDED`

该结果只表示：如果 owner 愿意批准成本和 license review，Norgate 是当前最值得先试用的
true breadth 候选。它不允许自动购买、自动升级、下载数据、接入 pipeline 或恢复策略。

## Owner approval requirement

以下操作都必须 owner manual approval：

- trial registration；
- subscription / purchase；
- provider plan upgrade；
- local cache；
- sample download；
- paid data 派生 feature；
- 将 trial sample 接入任何 model-ready breadth builder。

## Fail-closed 边界

Due diligence 和 trial decision 不能解锁：

- first-layer reopen；
- channel-specific v4；
- minimal forward diagnostic；
- promotion；
- paper-shadow；
- production；
- broker。

即使 trial 推荐成立，所有安全字段继续是 `false / none / BLOCKED`。
