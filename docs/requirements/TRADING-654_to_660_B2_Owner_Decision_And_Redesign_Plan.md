# TRADING-654 to 660 B2 Owner Decision and Redesign Plan
最后更新：2026-06-23

版本：2026-06-20

## 背景

TRADING-649~653 已完成 B2 Campaign finalization：final repeatability run 使用
Campaign `run --stage next` 和 B2 compute adapter，final gate 输出
`OWNER_OVERRIDE_REQUIRED`，owner packet ready，branch finalization 为
`OWNER_REVIEW_REQUIRED`。当前 B2 current-form 不能继续以 generic
`NEEDS_MORE_EVIDENCE` 推进，需要记录 owner decision、归档 current campaign，并把可复用
evidence 转入更窄的 redesign planning。

## 范围

- TRADING-654：记录 B2 owner decision，推荐 `return_to_design`。
- TRADING-655：归档 current B2 campaign，保留 evidence 和 lineage。
- TRADING-656：抽取 reusable evidence、weak informative evidence、invalidated evidence、
  role-specific-only evidence 和 not reusable evidence。
- TRADING-657：生成 Slow-Drawdown Defensive Overlay RFC。
- TRADING-658：生成 Re-entry Policy Design Contract。
- TRADING-659：生成 Fast-Shock Trigger Feasibility RFC。
- TRADING-660：生成 Post-B2 Campaign Program Snapshot。

## 非目标和安全边界

- 不批准 paper-shadow、extended shadow 或 live trading。
- 不批准 B4 retest、B5、B6 或 v3。
- 不生成 official target weights。
- 不生成 broker/order artifacts。
- 不修改 production state。
- 不访问 untouched holdout。
- 不实现 slow-drawdown overlay、re-entry policy 或 fast-shock module 行为；本批次只做
  owner decision、archive 和 redesign planning artifacts。

## 实施顺序

1. 新增 B2 owner decision audit entry 与 Reader Brief，并将 Campaign state 更新为
   returned-to-design archived status。
2. 新增 current B2 campaign archive summary，保留 evidence/source lineage，并继续标记
   B4/B5/B6/v3/paper-shadow blocked。
3. 新增 reusable evidence extraction report。
4. 新增 slow-drawdown defensive overlay RFC。
5. 新增 re-entry policy design contract。
6. 新增 fast-shock trigger feasibility RFC。
7. 新增 post-B2 Campaign program snapshot。
8. 接入 validation pack、system flow、artifact catalog、release note、task register 和
   focused tests。

## 验收标准

- Owner decision audit entry 记录 `owner_action=return_to_design`、reason summary 和
  next action；disallowed actions 全部 false。
- Campaign state 反映 owner decision 和 archived/returned-to-design status；evidence 不删除。
- Archive summary 输出 `b2_current_form_campaign_archive.json/md`，保留 lineage 和安全边界。
- Reusable evidence report 分类 control/slow-drawdown/fast-risk/re-entry/utility evidence。
- Slow-drawdown overlay RFC、re-entry policy contract、fast-shock trigger RFC 输出 JSON/Markdown
  与 Reader Brief。
- Program snapshot 输出 B2 current-form status、owner decision、redesign artifact statuses、
  B3 status、B4/B5/B6/v3/paper-shadow allowed flags 和 next recommended research action。
- focused tests、Ruff、compileall、B2/B3 campaign validate、validation-pack、task-register
  consistency 和 `git diff --check` 通过。

## 当前状态

2026-06-20：进入 `IN_PROGRESS`。本批次只记录 B2 owner decision、archive current
campaign 并生成 redesign planning artifacts，不启用 paper-shadow/live/official weights、
broker/order、B4/B5/B6/v3 或 production mutation，不访问 untouched holdout。

2026-06-20：实现进入验证阶段。新增 Campaign B2 owner decision record、current-form
archive、reusable evidence report、slow-drawdown defensive overlay RFC、re-entry policy
design contract、fast-shock trigger feasibility RFC 和 post-B2 program snapshot builder；
`validation-pack` 升级为 rc5，并在临时 Campaign root 中验证 owner decision append 与
archive state mutation。归档后的 B2 `campaign plan` 不再给出 next run actions，
`campaign run --stage next` 返回 archived no-op，不消耗 evidence budget、不写新
evidence、不触发 production effect。真实 B2 owner decision 已 append，canonical B2 state
已更新为 `ARCHIVED + RETURNED_TO_DESIGN`。验证通过 focused
`tests/test_research_campaign.py` 20 项、Ruff、compileall、B2/B3 campaign validate、
adapter validation、rc5 validation-pack、task-register consistency run/validate 和
`git diff --check`；当前进入 `VALIDATING`，等待项目 owner 后续复核。
