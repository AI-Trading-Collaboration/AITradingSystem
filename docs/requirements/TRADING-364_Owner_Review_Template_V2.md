# TRADING-364 Owner Review Template V2

最后更新：2026-06-16

状态：DONE

## 背景

现有 owner review artifacts 分散在多个 ETF / paper-shadow workflow 中，字段和 decision language
不完全一致。TRADING-364 建立 governance-wide owner review template v2，使 manual reviews 可以按同一
字段、同一 owner action enum 和同一 safety boundary 进行比较。

## 范围

- 定义 owner review template v2 required fields：
  - candidate id；
  - evidence interpretation；
  - main reason to continue；
  - main reason to reject；
  - uncertainty；
  - required follow-up；
  - final owner action；
  - linked input artifacts；
  - safety status；
- 固定 owner action enum：
  - `continue_shadow`；
  - `enter_extended_shadow`；
  - `needs_more_evidence`；
  - `return_to_research`；
  - `reject_candidate`；
  - `hold`；
- 输出 template/report JSON + Markdown；
- 支持 validation CLI 校验 template contract，并可选校验 filled owner review JSON；
- 接入 Reader Brief、report registry、artifact catalog、README、system flow、operations runbook 和 focused tests。

## 安全边界

- governance-only / manual-review-only；
- 不记录 owner decision log；
- 不 append 或修改历史 review artifacts；
- 不运行 upstream evidence collection；
- 不刷新数据；
- 不修改 candidate、paper-shadow、score、backtest、policy 或 production state；
- 不生成 official target weights、order ticket 或 broker action；
- 所有输出固定 `production_effect=none`。

## CLI

- `aits reports owner-review-template-v2 --as-of YYYY-MM-DD`
- `aits reports validate-owner-review-template-v2 --latest`
- `aits reports validate-owner-review-template-v2 --review-json-path path/to/filled_review.json`

## 验收标准

- template report 暴露 required field contract、owner action enum、blank template、filled review validation
  guidance、safety boundary 和 next action；
- validation CLI 对缺 required field、非法 owner action、缺 linked input artifact、缺 safety status 和 unsafe
  production effect fail closed；
- Reader Brief 展示 latest template readiness、required field count、owner action count、validation status 和
  next action；
- report registry、artifact catalog、README、system flow、operations runbook 和 task register 同步；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、compileall 和 git diff check
  通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-364。本阶段只建立可复用
  owner review template v2 和校验路径，不创建 owner decision audit log（TRADING-378 范围），不改变
  candidate/paper-shadow/production state。
- 2026-06-16：实现完成并归档；新增 template/validation report CLI、optional filled review JSON
  validation、Reader Brief section、report registry、artifact catalog、README、system flow、operations
  runbook 和 focused tests。真实 2026-06-16 template 输出 `TEMPLATE_READY`、required fields=9、
  owner actions=6；validation 输出 `PASS`、checks=9、failed=0。latest Reader Brief 使用本机可用的
  2026-06-15 snapshot 验证 owner review template section；精确 2026-06-16 Reader Brief 仍受缺失
  `data/processed/decision_snapshots/decision_snapshot_2026-06-16.json` 限制，本任务未补造 snapshot。
