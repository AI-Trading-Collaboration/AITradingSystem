# TRADING-360 Report Quality Gate

最后更新：2026-06-16

## 状态

- 当前状态：DONE
- 优先级：P2
- 下一责任方：系统实现
- Production boundary：只读报告治理；不修改 source report artifact、不刷新数据、不运行上游、不触发 broker/order/production。

## 背景

附件要求为 generated research reports 和 Reader Brief 增加统一质量门禁。当前 `reader_brief_quality` 已能检查 Reader Brief 的部分结构，但缺少一个面向全部 report artifact 的通用检查层，也没有把普通报告的目的、输入、输出决策、安全边界、限制和下一步作为可审计结论暴露。

## 目标

新增 `aits reports quality-gate`，读取 report index、latest report artifacts 和 Reader Brief JSON，生成只读 JSON / Markdown 质量报告。该报告必须显式输出：

- `report_quality_status`
- `missing_sections`
- `blocking_quality_issues`
- `warning_quality_issues`

## 范围

1. 普通 report quality gate 检查每个可读 report artifact 是否包含：
   - purpose
   - input artifacts
   - output decision
   - safety boundary
   - limitations
   - next action
2. Reader Brief quality gate 检查 Reader Brief 是否包含：
   - human-readable summary
   - key result
   - blocking issues
   - warnings
   - recommended next step
3. CLI 写出 JSON / Markdown report。
4. report registry / report index 能追踪质量门禁 artifact。
5. Reader Brief 只读展示 latest report quality gate 摘要。
6. focused tests 覆盖 pass、missing sections、blocking production-effect risk、Reader Brief 检查、CLI 输出和 registry/index 可见性。

## 非目标

- 不自动修复缺失 section。
- 不回写或重排既有 report 内容。
- 不刷新数据、不运行 `aits validate-data`、不补造 upstream artifact。
- 不把 warning 升级为投资结论阻断，除非存在 production safety boundary 风险、不可读 artifact 或 Reader Brief 缺少核心摘要/结果。

## 实施步骤

1. 新增 `src/ai_trading_system/reports/report_quality_gate.py`，提供 payload builder、JSON/Markdown writer 和默认输出路径。
2. 在 `src/ai_trading_system/cli_commands/reports.py` 增加 `aits reports quality-gate`。
3. 在 `config/report_registry.yaml` 登记 `report_quality_gate`，让 `aits reports index` 能扫描。
4. 在 Reader Brief 加入 `report_quality_gate` 摘要字段和 source path。
5. 更新 README、operations runbook、artifact catalog 和 system flow。
6. 增加 focused tests 并运行验证。

## 验收标准

- `aits reports quality-gate --date YYYY-MM-DD` 可写出 JSON / Markdown。
- JSON 包含顶层 `report_quality_status`、`missing_sections`、`blocking_quality_issues`、`warning_quality_issues`。
- 普通 report 缺 section 会进入 `missing_sections` 和 warning；production-effect 非 `none` 或 artifact 不可读会进入 blocking issue。
- Reader Brief 缺 human-readable summary、key result 或 recommended next step 会进入 blocking issue；缺 warnings/blocking issue disclosure 会进入 warning。
- `aits reports index --as-of YYYY-MM-DD` 能扫描 quality gate artifact。
- Reader Brief JSON 暴露 latest `report_quality_*` summary。
- focused pytest、Ruff、compileall、git diff check、documentation contract、report index 和 Reader Brief/quality 通过或给出显式有限上下文原因。

## 进展记录

- 2026-06-16: 新增需求文档并登记任务，准备实现通用只读 report quality gate。
- 2026-06-16: 实现完成并归档。新增 `report_quality_gate` 模块、`aits reports quality-gate` CLI、daily-run / scheduled task / direct dispatcher integration、report registry entry、Reader Brief summary、artifact catalog、README、operations runbook、system flow 和 focused tests。真实 2026-06-16 artifact 为 `outputs/reports/report_quality_gate_2026-06-16.json/md`，`report_quality_status=PASS_WITH_WARNINGS`、checked reports=378、checked Reader Brief=1、missing sections=1312、blocking=0、warnings=1320；warnings 主要反映 legacy report templates 尚未显式暴露全部 required sections，不是 production blocker。Report index 为 `PASS_WITH_EXPLICIT_WAIVERS` / unwaived=0；Reader Brief 显示 latest quality gate summary，Reader Brief quality `LIMITED_READER_CONTEXT` / failed=0，限制原因是显式复用 latest 2026-06-15 decision snapshot。
