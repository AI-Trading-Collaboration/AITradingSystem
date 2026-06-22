# TRADING-843～858 Tail-Risk Fallback Governance Follow-Up

状态：VALIDATING
最后更新：2026-06-22

## 背景

TRADING-827～842 已把 tail-risk fallback 从旧的 label-coupled 证明链路改为只读 governance 链路。当前后续任务需要确认这些 artifacts 是真实可审计输出，而不是结构性跑通；同时补齐 artifact snapshot、状态矩阵、真实数据审计、forward outcome 复核、baseline 竞争力、determinism、registry integrity、Reader Brief 安全摘要和最终人工决策文档。

所有新增输出继续固定：

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- 不修改 production weights、paper-shadow state、promotion gate 或 broker/order

## 阶段拆解

第一批 P0/P1：

- TRADING-843：运行或读取 TRADING-827～842 tail-risk governance artifacts，生成统一 snapshot JSON/Markdown。
- TRADING-844：聚合 827～842 状态矩阵，暴露 promotion/paper-shadow/production 可用性和 owner next action。
- TRADING-845：审计这些 CLI 是否基于真实 artifact，检查 fixture fallback、missing input PASS、placeholder、zero-sample positive conclusion。
- TRADING-846：专门复核 TRADING-828 independent forward outcome 的样本、字段、禁用依赖和 baseline context。
- TRADING-847：专门复核 TRADING-832 baseline 对比，要求 fallback 与更强简单 baseline 比较。
- TRADING-848：检查核心治理 artifact 在相同输入下的 deterministic 输出稳定性。
- TRADING-849：清理 CRLF/LF 噪音并保持新增 Python/YAML/MD/JSON 为 LF。
- TRADING-850：补任务级 provenance and coverage map。

第二批 P1/P2：

- TRADING-851：构造 mutation/bad-input checks，确认 hard blockers fail closed。
- TRADING-852：检查 report registry entries 的唯一性、命令、artifact policy、daily reading flags、JSON/MD pairing。
- TRADING-853：Reader Brief 显示极简 tail-risk fallback safety summary，不展开全部治理报告。
- TRADING-854：在 trigger v2 之前审查可用 input feature 质量。
- TRADING-855：扩展简单 baseline 集合，避免 fallback 只打赢弱基准。
- TRADING-856：如果简单 baseline 支配 fallback，自动输出 baseline-dominated blocker。

第三批 P2：

- TRADING-857：把 TRADING-827～856 汇总为 0～100 research readiness score。
- TRADING-858：输出最终人工决策文档，回答是否继续 tail-risk fallback 研究、哪些指标污染、哪些 research-only 可用、是否值得 trigger v2。

## 验收标准

- 新增 CLI 均可通过 `aits research strategies ...` 执行并写出 JSON/Markdown artifacts。
- 所有新增 artifacts 披露 market regime、date range、status、warnings、blockers 和安全字段。
- TRADING-843 snapshot 和 TRADING-844 status matrix 必须显示 TRADING-827/830/838/839 hard blockers，且不允许 promotion/paper-shadow/production。
- TRADING-845/846/847 不能把 missing input、fixture-only、placeholder 或 sample_count=0 输出为 positive research conclusion。
- TRADING-855/856 的简单 baseline 结果进入 downstream readiness/decision 输出。
- TRADING-853 只展示极简 safety summary，不把 15 个治理报告塞入日报正文。
- `docs/system_flow.md`、`docs/artifact_catalog.md`、`config/report_registry.yaml`、任务登记和测试同步更新。
- 验证使用并行 pytest；若只做 focused one-off，使用 `python -m pytest -n 16 --dist loadfile ...`。

## 进展记录

- 2026-06-22：新增本需求文档，任务登记进入 IN_PROGRESS；开始实现 843～858 follow-up governance artifacts。
- 2026-06-22：实现完成并转入 VALIDATING；新增 TRADING-843～858 follow-up CLI/artifacts、report registry/catalog/system flow、Reader Brief safety summary、focused governance tests 和 baseline dominance/readiness/next-decision 输出。验证通过 focused parallel pytest、registry/documentation/task-register tests、Ruff、compileall 和 git diff check；所有新增输出继续禁止 promotion、paper-shadow、production 和 broker/order。
