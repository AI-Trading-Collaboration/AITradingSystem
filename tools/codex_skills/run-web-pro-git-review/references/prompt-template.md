# Web Pro Exact-Git Review Prompt

Fill all angle-bracket placeholders. Remove unused optional blocks before submission.

```text
请以“受治理的规划审阅者”身份，只基于下面指定的公开或已授权 Git exact commit 审阅当前状态，
并给出下一阶段的具体安排。不要读取 moving main；如果某个链接无法访问，必须明确列出，不能用
猜测补齐。

第一部分必须命名为 MODEL_IDENTITY_AND_ROUTING_RISK，并先回答：
1. 只根据你当前实际可见的 system/UI/response metadata，能否验证本次回答由
   <EXPECTED_PRO_PRODUCT_LABEL> 生成？
2. 能否验证本次没有发生 fallback 或路由到其他模型？
3. 如果拿不到 authoritative backend model identifier/route，请明确写
   CANNOT_VERIFY_EXACT_BACKEND_ROUTE。
4. 即使你自述为 Pro，也必须说明“模型自述不是权威路由证据”；同时记录你实际可见的模型标签，
   不要根据回答质量、订阅等级、延迟或提示词猜测。

Repository exact snapshot：
- repository：<REPOSITORY_URL>
- exact commit：<EXACT_COMMIT_SHA>
- exact tree：<EXACT_TREE_URL>

请逐项打开并引用以下 exact-commit 文件：
<NUMBERED_EXACT_BLOB_URLS>

任务目标：
<PLANNING_QUESTION>

必须被仓库证据核对的当前事实：
<LOCALLY_VERIFIED_FACTS>

仍未解决、不得由提示词替代证据的事项：
<OPEN_QUESTIONS_OR_POLICY_GAPS>

安全与授权边界：
<PROHIBITED_ACTIONS_AND_DOWNSTREAM_GATES>

输出要求：
A. REPOSITORY_RETRIEVAL
   - 列出成功、部分成功和失败的文件；
   - 记录 exact commit；
   - 引用关键证据；
   - 不得假装读取成功或使用 moving main。
B. CURRENT_STATE
   - 区分已完成、刚解除的 blocker、仍需 Owner 授权和仍禁止的下游阶段。
C. RECOMMENDED_SEQUENCE
   - 按顺序给出任务波次；
   - 每波说明目的、输入、Owner decision、允许动作、禁止动作、产物、验证、退出条件和失败结论。
D. FIRST_TASK_SPEC
   - 提出稳定 task ID、状态、优先级、scope、依赖、path/module/contract/evidence-lineage
     claims 与 acceptance criteria。
E. EXECUTION_TOPOLOGY
   - 判断 SINGLE_LANE、DUAL_LANE 或先 serial contract wave；
   - 说明哪些共享 schema、policy、DQ/PIT/cache/publication identity 必须先冻结。
F. FALSIFICATION_AND_STOP_MATRIX
   - 对每个 mandatory axis 给出 PASS、FAIL、INSUFFICIENT 或 INVALID 的机械处置。
G. DOWNSTREAM_GATES
   - 明确本阶段通过后仍需哪些独立 evidence 和 Owner authorization；
   - 不得把 capability、数据资格或工具选择自动解释为投资价值或生产授权。
H. 给出未来 <TIME_HORIZON> 的可执行排序和最重要的三个风险。

请使用 <OUTPUT_LANGUAGE>，保留标准标识符。不要实施任何建议，不要生成投资建议、官方仓位、
production 变更或 broker action，不要建议放宽冻结阈值来追求通过。
```

## Submission checklist

- Repository is public or the user explicitly authorized outbound use.
- Exact commit and every blob URL contain the immutable SHA.
- Local facts were read from the same snapshot or are clearly marked newer.
- Secrets, private paths, exclusions, and unscoped content were removed.
- The user explicitly asked to submit the packet to ChatGPT Web Pro.
