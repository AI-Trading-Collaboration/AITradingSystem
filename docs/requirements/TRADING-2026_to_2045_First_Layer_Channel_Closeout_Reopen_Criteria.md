# TRADING-2026 to 2045 First-Layer Channel Closeout and Reopen Criteria

## 背景

TRADING-2006～2025 已确认：

- `do_not_de_risk v3` 归档为 `DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT`。
- `risk_on_veto v3` 虽有 observe-only diagnostic 工程能力，但 `net_veto_benefit_total=-2.343111`，compatibility=`VETO_TOO_STRICT_FOR_RETURN_SEEKING_DIAGNOSTIC`。
- add-risk channel 没有 selected family。
- return-seeking diagnostic 有 drawdown、beta/TQQQ、2023+ dependence blocker。
- breadth/event_risk family 仍是 PIT blocked，不能进入模型。

因此当前 first-layer channel 没有可进入策略候选、owner review、paper-shadow 或 promotion 的对象。

## 非目标

- 不继续调 `do_not_de_risk v3` 或 `risk_on_veto v3`。
- 不训练 add-risk 或 universal first-layer 模型。
- 不修改 second-layer probe weights。
- 不进入 gated overlay、owner review、paper-shadow、production 或 broker。
- 不输出 portfolio weights、trade advice、target allocation 或 recommended allocation。
- 不启动新的历史搜索来让当前 channel 变好。

## 实施范围

1. 新增 `config/research/first_layer_channel_archive_policy.yaml`。
2. 新增 closeout runner / CLI `aits research trends first-layer-channel-closeout`。
3. 生成 master closeout、channel status matrix、diagnostic evidence labels、reopen criteria、PIT data gap roadmap、minimal forward diagnostic plan 和 owner brief。
4. 更新 report registry、artifact catalog、system flow、task register 和 research audit metadata schema。
5. 新增 archive guardrail tests。

## Reopen Criteria

未来重开 first-layer channel research 至少需要以下条件之一作为触发：

- 新 PIT-approved breadth / participation 数据。
- 新 PIT-approved event risk / macro shock 数据。
- 更稳健的 relative strength / sector participation 数据。
- Forward diagnostic evidence 证明现有 diagnostic signal 在未来样本中有效。
- 新 feature family 能在 primary 2021 window 与 2022 slice 同时通过。

重开仍必须满足：

- selection rule 预注册。
- primary window 优先，不能以 2022-12 legacy 为主。
- same-risk static frontier comparison 通过。
- 不是 TQQQ / beta-only。
- 不是 2023+ only。
- defensive probe regression 为 0，或 owner 明确接受 caveat。
- net-of-cost 后仍有效。
- owner 手动批准 reopen。

## 验收标准

- Channel status matrix 覆盖 `do_not_de_risk_v3`、`risk_on_veto_v3`、`add_risk`、`return_seeking_diagnostic`、`defensive_channel`、`risk_veto_channel`、breadth/event PIT-blocked families。
- Reopen criteria 明确允许触发、禁止触发和 owner approval required。
- Minimal forward diagnostic plan 默认 `enabled=false`。
- Guardrail tests 证明 archived / unsupported / PIT-blocked / 2023+ only evidence 不能进入 candidate、model 或 forward watch。
- 所有 tracked YAML 保持 candidate_count=0，promotion/paper-shadow/production/broker disabled。
- Focused parallel pytest、Ruff、compileall、documentation/audit governance 和 diff check 通过。

## 进展记录

- 2026-06-28：任务登记为 `IN_PROGRESS`；开始实现 first-layer channel closeout / reopen criteria。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增 `aits research trends first-layer-channel-closeout`、archive policy、disabled minimal forward diagnostic plan、master closeout、status matrix、diagnostic evidence labels、reopen criteria、PIT data gap roadmap、owner brief、registry/catalog/system flow 和 archive contract tests。
- 2026-06-28：真实 run 生成 `FIRST_LAYER_CHANNEL_RESEARCH_CLOSED_NO_CANDIDATE`：candidate_count=0；`do_not_de_risk_v3`=`ARCHIVED` / `NO_MATERIAL_IMPROVEMENT`；`risk_on_veto_v3`=`HISTORICAL_DIAGNOSTIC_ARCHIVE`，net_veto_benefit_total=`-2.343111`；add-risk=`NOT_SUPPORTED`，selected families=[]；return-seeking diagnostic=`HISTORICAL_DIAGNOSTIC_ONLY`；breadth/event families=`PIT_BLOCKED`；owner review、forward watch、promotion、paper-shadow、production、broker 均保持 blocked/false/none。
- 2026-06-28：验证通过 `python -m pytest -n 16 --dist loadfile tests/test_first_layer_channel_archive_contract.py`（10 passed）、关联治理测试组（63 passed）、`python -m ruff check`、`python -m compileall -q src tests/test_first_layer_channel_archive_contract.py`、`git diff --check`；task-register DONE/BASELINE/DROPPED regex 无匹配。
