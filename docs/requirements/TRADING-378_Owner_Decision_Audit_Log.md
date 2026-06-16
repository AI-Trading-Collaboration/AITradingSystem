# TRADING-378 Owner Decision Audit Log

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-364 已建立 owner review template v2，但 template 本身不记录 owner decision。
TRADING-378 建立 append-only owner decision audit log，让 owner 的人工决定可被 monthly review
pack、promotion board、extended shadow protocol 和 governance end-to-end pack 复用，同时不改变任何
strategy output 或 candidate state。

## 范围

- 定义 owner decision audit record schema：
  - decision id；
  - timestamp；
  - candidate id；
  - input artifacts；
  - owner action；
  - reason summary；
  - safety status；
  - next action；
- 提供 append-only JSONL 写入入口；
- 提供 report CLI 和 validate CLI；
- Reader Brief 展示 latest audit log status、latest decision、validation status 和 downstream input status；
- report registry、artifact catalog、README、system flow、operations runbook、task register 和 focused tests 同步；
- monthly review pack 和 promotion board 只能读取该 log 作为治理输入，不能把它解释为 live trading approval。

## 安全边界

- governance-only / manual-review-only；
- append 命令只追加一条 JSONL，不重写既有 log；
- report / validation 只读读取 append-only log；
- 不运行 upstream evidence collection；
- 不刷新数据；
- 不修改 strategy outputs、candidate state、paper-shadow state 或 production state；
- 不生成 official target weights；
- 不生成 order ticket；
- 不触发 broker action；
- 所有输出固定 `production_effect=none`。

## CLI

- `aits reports owner-decision-audit-log append --decision-json-path <owner_decision.json>`
- `aits reports owner-decision-audit-log report --as-of YYYY-MM-DD`
- `aits reports owner-decision-audit-log validate --latest`

## 验收标准

- append CLI 追加一条 JSONL，拒绝 malformed source record、duplicate decision id、缺 required field、
  非法 owner action / safety status 和 `SAFETY_BLOCKED` 搭配 continuation action；
- report CLI 输出 JSON / Markdown，披露 schema、record counts、latest decision、owner action counts、
  monthly review pack input、promotion board input、blocking issues、safety boundary 和 next action；
- validation CLI 对 malformed JSONL、duplicate decision id、invalid record 和 production mutation fields
  fail closed，empty log 可作为 no-owner-decision-yet 状态通过；
- Reader Brief 只读展示 latest audit log status、validation status、record count、latest decision 和 downstream
  input status；
- report registry、artifact catalog、README、system flow、operations runbook、requirements、task register 和
  focused tests 同步；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、compileall 和 git diff check
  通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-378。本阶段建立 append-only
  owner decision audit log 和 report/validation visibility，不改变 strategy outputs、candidate state、
  paper-shadow state、production state、official target weights、order ticket 或 broker workflow。
- 2026-06-16：实现完成并归档；新增 append-only JSONL schema、`owner-decision-audit-log`
  append/report/validate CLI、Reader Brief section、report registry、artifact catalog、README、system
  flow、operations runbook 和 focused tests。当前没有真实 owner decision record，未补造或 append 示例
  decision；真实 2026-06-16 report 输出 `AUDIT_LOG_EMPTY`、records=0、
  monthly_review_pack_input=`NO_OWNER_DECISIONS_RECORDED`、promotion_board_input=`NO_OWNER_DECISIONS_RECORDED`；
  validation 输出 `PASS`、checks=12、failed=0、empty_log_allowed=true。Reader Brief latest 使用本机可用的
  2026-06-15 snapshot 和 2026-06-16 report index 验证，section 展示 audit log `AUDIT_LOG_EMPTY`、
  validation=`PASS`、next action=`append_owner_decision_after_next_manual_owner_review`。
