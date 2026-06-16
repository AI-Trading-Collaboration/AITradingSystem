# TRADING-380 Candidate Rejection Postmortem Template

最后更新：2026-06-16

状态：DONE

## 背景

TRADING-379 已建立 paper-shadow promotion board。附件要求在候选被拒绝时有统一 postmortem template，保留拒绝原因、失败 evidence gate、stress failure、数据质量和 safety boundary 问题，以及该想法未来是否可重新进入研究。当前没有真实 rejection，本任务不得补造 rejection record。

## 范围

- 新增 `aits reports candidate-rejection-postmortem-template --as-of YYYY-MM-DD`。
- 新增 `aits reports validate-candidate-rejection-postmortem-template --latest`。
- 生成空白 postmortem template，并可选用 `--postmortem-json-path <filled.json>` 只读校验已填写 record。
- 只读读取 report index 中的 latest promotion board / owner decision / monthly pack / safety audit 作为上下文，不运行上游、不刷新数据、不修改 candidate state。
- 输出 JSON / Markdown template report、validation artifact 和 Reader Brief section。

## 必需 Sections

- candidate summary
- reason for rejection
- failed evidence gates
- failed stress scenarios
- data quality issues
- safety boundary issues
- whether idea can be revisited
- lessons learned

## 安全边界

- `production_effect=none`。
- Template 和 validation 只用于 manual research governance。
- 不拒绝任何候选，不修改 candidate / paper-shadow / production state。
- 不生成 official target weights、order ticket 或 broker action。
- 没有 filled postmortem 时报告只能显示 template ready / no rejection record provided。
- 已填写 postmortem 缺 required section、缺 rejection reason、缺 revisit decision、缺 lessons、或带 production mutation flag 时必须 fail closed。

## 验收标准

- CLI 可生成 `outputs/reports/candidate_rejection_postmortem_template_YYYY-MM-DD.json/md`。
- Validation CLI 可生成 `outputs/reports/candidate_rejection_postmortem_template_validation_YYYY-MM-DD.json/md`。
- JSON schema 显式包含 8 个 required sections。
- Optional filled postmortem validation 对缺 section、非法 safety boundary、非法 `can_revisit` 和缺 lessons fail closed。
- Reader Brief 展示 template status、validation status、candidate id、postmortem record 是否提供、failed gate / stress / data quality / safety issue counts、revisit status 和 detail link。
- `config/report_registry.yaml`、`docs/artifact_catalog.md`、`docs/system_flow.md`、`docs/operations/operations_runbook.md`、`README.md` 和 task register 同步。
- Focused tests 覆盖 template generation、filled postmortem validation fail closed、CLI output 和 Reader Brief summary。

## 进展记录

- 2026-06-16：任务新增并进入 `IN_PROGRESS`。当前没有真实 rejection 或 owner-filled postmortem，本阶段只交付 template/validator，不补造 rejection conclusion。
- 2026-06-16：实现完成并归档 `DONE`。新增 candidate rejection postmortem template report/validation、CLI、optional filled record validation、Reader Brief section、registry/catalog/runbook/system flow/README/tests。真实 template `outputs/reports/candidate_rejection_postmortem_template_2026-06-16.json/md` 输出 `TEMPLATE_READY`、required sections=8、postmortem_record_provided=false、filled status=`NO_POSTMORTEM_RECORD_PROVIDED`；validation `PASS`、failed=0。当前没有真实 owner-filled rejection postmortem，未补造 record 或 candidate rejection，保持 read-only / no official target / no broker / no production mutation。
