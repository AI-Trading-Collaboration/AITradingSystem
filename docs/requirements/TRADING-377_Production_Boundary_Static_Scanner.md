# TRADING-377 Production Boundary Static Scanner

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-363 已建立 runtime/report artifact safety boundary audit，但仍需要一个 source/config/docs
level 的静态扫描器，提前发现危险 production-facing terms、accidental broker/order integration
和 secret-like identifier。该扫描器应作为 recurring governance check，在 future promotion milestone
前与 safety boundary audit 一起运行。

## 范围

- 只读扫描 `src/`、`config/`、`docs/`、`scripts/` 和关键 project Markdown；
- 检查以下 production-facing term family：
  - broker；
  - order ticket / live order；
  - live allocation；
  - official target weight；
  - production mutation；
  - auto execute；
  - account id；
  - API key / secret-like key；
- 支持 documentation-only / safety-boundary allowlist，避免把明确的禁止、只读、manual-review-only
  语境误判为 blocking；
- 输出 suspicious locations、severity、matched term、allowlist reason 和 recommended action；
- 新增 report CLI、validation CLI、Reader Brief 摘要、report registry、artifact catalog、README、
  system flow、operations runbook 和 focused tests。

## 安全边界

- 只读扫描本地文本文件；
- 不修改命中 source/config/docs；
- 不隐藏或自动 waive suspicious location；
- 不读取或输出真实 secret value；
- 不调用 broker、order、portfolio、paper account 或 production workflow；
- 不刷新数据、不运行 scoring/backtest/paper-shadow 上游；
- 所有输出固定 `production_effect=none`。

## CLI

- `aits reports production-boundary-static-scan --as-of YYYY-MM-DD`
- `aits reports validate-production-boundary-static-scan --latest`

## 状态

- `OK`
- `WARNING`
- `BLOCKING`

## 验收标准

- report 输出 JSON/Markdown，包含 scanned path counts、finding counts、per-finding severity、
  allowlist status、source location 和 recommended action；
- validation CLI 对 schema、production_effect、status enum、blocking finding、required term family
  coverage fail closed；
- documentation-only safety mentions 可被 allowlist 为 warning/allowed，不得静默删除；
- suspicious production-like source/config addition 必须 `BLOCKING`；
- Reader Brief 展示 latest static scanner status、blocking/warning counts 和 next action；
- report registry、artifact catalog、README、system flow、operations runbook 和 task register 同步；
- focused tests、documentation contract、report index、Reader Brief quality、ruff、compileall 和 git diff
  check 通过。

## 进展记录

- 2026-06-16：进入 IN_PROGRESS；owner 要求继续附件中的 TRADING-377，目标是建立只读
  production boundary static scanner，先覆盖 source/config/docs 中 dangerous production-facing
  terms 与 documentation-only allowlist。
- 2026-06-16：实现完成并归档；新增 report/validation CLI、allowlist policy、Reader Brief section、
  report registry、artifact catalog、README、system flow、operations runbook 和 focused tests。真实
  2026-06-16 scan 输出 `scan_status=WARNING`、blocking findings=0，validation 输出
  `validation_status=WARNING`、failed checks=0；warning findings 保持可见，用于后续 owner review。
  latest Reader Brief 使用本机可用的 2026-06-15 snapshot 验证 static scan section；精确 2026-06-16
  Reader Brief 仍受缺失 `data/processed/decision_snapshots/decision_snapshot_2026-06-16.json` 限制，
  本任务未补造 snapshot。
