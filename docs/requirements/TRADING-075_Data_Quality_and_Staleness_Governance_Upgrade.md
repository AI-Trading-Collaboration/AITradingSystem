# TRADING-075 Data Quality and Staleness Governance Upgrade

最后更新：2026-06-03

## 状态

- 父任务：TRADING-075
- 当前状态：VALIDATING
- 优先级：P0
- 下一责任方：系统验证 + 项目 owner
- 安全边界：`observe_only=true`、`candidate_only=true`、`production_effect=none`、`broker_action=none`、`manual_review_required=true`

## 背景

TRADING-074 已把 ETF portfolio research 的 daily / weekly / biweekly / monthly operations workflow 组织成只读规划、freshness、failure policy、owner checklist、dry-run、operations health report、Reader Brief operations section 和 validation gate。TRADING-075 在此基础上把数据质量与 staleness 治理提升为显式层，避免 daily report、weekly review、attribution、parameter review proposal 和 Reader Brief 链接依赖过期或不完整证据。

本阶段回答的问题：

```text
Is the data and evidence base fresh, complete, internally consistent, and safe enough
for daily reports, weekly reviews, attribution reports, and parameter review proposals?
```

## 非目标

- 不实现 broker execution。
- 不写 production weights。
- 不自动 baseline replacement。
- 不自动 candidate promotion。
- 不要求 paid vendor integration。
- 不实现完整 corporate action engine。
- 不迁移数据仓库。

## 阶段拆解

|子任务|状态|验收标准|
|---|---|---|
|TRADING-075A Data Quality Policy Config|DONE|新增 `config/etf_portfolio/data_quality.yaml`，配置可加载、可校验，必需 section 和 safety fields 缺失时 fail fast。|
|TRADING-075B Price Data Freshness Checks|DONE|检查 required / optional asset 的 latest date、expected latest trading date、trading day lag、freshness status 和 blocking status。|
|TRADING-075C Missing Bars and Market Calendar Coverage Checks|DONE|按显式 U.S. trading calendar 近似规则检查 expected trading days、available bars、missing dates、coverage ratio 和 coverage status。|
|TRADING-075D Return Outlier / Split-Dividend Sanity Checks|DONE|按 policy 阈值标记 warning / critical outlier、adjacent-day reversal 和 possible adjustment issue。|
|TRADING-075E Config Hash and Model Version Drift Checks|DONE|比较 artifact metadata 中的 config hash / model version 与当前配置，unknown metadata 不静默忽略。|
|TRADING-075F Evidence Completeness Checks|DONE|检查 forward dashboard、weekly review、decision journal、parameter review、weight calibration、AI attribution 和 satellite attribution evidence 的 required fields、sample count、coverage 和 completeness status。|
|TRADING-075G Validation Gate Freshness Checks|DONE|检查 credibility、experiments、forward、AI confirmation / attribution、satellite / attribution、weekly review、decision journal、parameter review、weight calibration、ops 和 data quality gates。|
|TRADING-075H Report Staleness and Reader Brief Link Checks|DONE|通过 report registry 和 Reader Brief artifact 检查 required report stale/missing 与 broken/outdated links。|
|TRADING-075I Data Quality Report Generator|DONE|生成 JSON / Markdown data quality governance report，包含 safety banner、run metadata、各检查区块、blockers、warnings、manual review items 和 source links。|
|TRADING-075J Reader Brief Data Quality Section|DONE|Reader Brief 只读展示 latest ETF data quality governance report status、blockers、warnings 和详细报告链接。|
|TRADING-075K Data Quality Validation Gate|DONE|新增 `aits etf data-quality validate`，fail-closed 校验 policy、checkers、report generator、Reader Brief integration 和 safety boundary。|

## 设计决策

1. Data quality governance 采用独立 ETF policy config，不改变既有 `aits validate-data` 运行时要求。
2. Checker 和 report generator 保持只读，默认扫描已有 price cache、report registry 与 artifacts，不运行上游命令。
3. Critical required failures 阻断 dependent research interpretation；optional artifact 缺失只 warning。
4. Calendar coverage 使用显式的工作日 + policy holiday list baseline。完整交易所日历或 corporate action engine 属于后续数据源增强，不在本阶段伪造。
5. Config/model drift 第一版读取 artifact 顶层或 metadata 中常见键；缺失 metadata 作为 unknown warning 或 required blocker，不静默忽略。
6. Reader Brief 只读 report index 指向的 latest `etf_data_quality_governance_report`，缺失时显示 section-level `MISSING`，不补跑 data-quality CLI。

## 验收命令

最终运行：

```powershell
python -m pytest tests -q
python -m ruff check config src tests scripts docs
python -m compileall -q src tests scripts
git diff --check
python -m ai_trading_system.cli etf data-quality validate
python -m ai_trading_system.cli etf ops validate --as-of 2026-06-03
```

验证结果：

- `python -m pytest tests -q`：2046 passed，330 warnings。
- `python -m ruff check config src tests scripts docs`：PASS。
- `python -m compileall -q src tests scripts`：PASS。
- `git diff --check`：PASS。
- `python -m ai_trading_system.cli etf data-quality validate --output-dir reports\etf_portfolio\data_quality\validation`：PASS，failed_check_count=0。
- `python -m ai_trading_system.cli etf ops validate --as-of 2026-06-03 --output-dir reports\etf_portfolio\operations\validation`：PASS，failed_check_count=0。

## 进展记录

- 2026-06-03: 新增任务文档并进入 IN_PROGRESS，原因：owner 提供 TRADING-075 开发计划，要求在 TRADING-074 operations workflow 后新增数据质量与 staleness governance baseline。
- 2026-06-03: 从 IN_PROGRESS 改为 VALIDATING，原因：TRADING-075A~K baseline 已完成；新增 policy config、checkers、report/validation CLI、Reader Brief data-quality section、operations daily graph step、report registry/catalog/system-flow/runbook/README integration 和专项测试。最终验证通过全量 pytest、ruff、compileall、diff check、`aits etf data-quality validate` 和 `aits etf ops validate`；下一步观察真实 daily runs、artifact freshness、Reader Brief links 和 owner manual review。
