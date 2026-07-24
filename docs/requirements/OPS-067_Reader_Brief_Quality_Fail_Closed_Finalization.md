# OPS-067 Reader Brief / Report Quality Fail-Closed Finalization

最后更新：2026-07-24

状态：`IN_PROGRESS`

稳定任务 ID：`OPS-067_READER_BRIEF_QUALITY_FAIL_CLOSED_FINALIZATION`

## 背景

2026-07-24 在 Wave14 S2 后置 canonical daily ops 修复验收的最终树预检中发现：

- `aits reports quality-gate` 即使生成 `report_quality_status=FAIL` 仍返回 0；
- `aits reports validate-reader-brief` 即使生成 `status=FAILED` 仍返回 0；
- daily runner 只看进程返回码，且 `_post_step_artifact_status_error` 尚未读取这两个
  quality artifact，因此可能把真实质量失败登记为 step PASS；
- controlled daily 在 step ledger 已完成后才生成 daily task dashboard、decision summary，
  并重写 canonical Reader Brief。重写后的最终 Reader Brief bytes 未被原
  `report_quality_gate` 覆盖，最终 `reader_brief_quality` 也未参与 run-control terminal
  decision。

这会使 canonical state、ledger、bundle 与最终读者报告质量结论不一致，不能通过事后人工
查看 JSON 来替代 fail-closed 控制。

## 状态语义

本任务只收紧明确失败，不改变既有受限状态的业务含义：

- Report quality：`PASS`、`PASS_WITH_WARNINGS` 可继续；`FAIL` 必须失败。
- Reader Brief quality：`OK`、`PASS_WITH_WARNINGS`、`LIMITED_READER_CONTEXT`
  可继续并在最终交付中明确披露；`FAILED` 必须失败。
- `LIMITED_READER_CONTEXT` 表示执行成功但证据上下文受限，不得被泛化成 CLI 失败。
- 所有输出保持 `production_effect=none`，不得写 production weights、active shadow
  weights，不得触发 broker/order/trading。

## 实现步骤

### S0：CLI fail-closed

1. `reports quality-gate` 在写出 JSON/Markdown 后遇到 `FAIL` 返回非零。
2. `reports validate-reader-brief` 在写出 JSON/Markdown 后遇到 `FAILED` 返回非零。
3. `cli_direct` 保持原参数传播，并把上述非零语义传回 daily runner。

### S1：daily step artifact gate

1. `report_quality_gate` 读取其 canonical JSON，严格允许
   `PASS` / `PASS_WITH_WARNINGS`。
2. `validate_reader_brief` 读取其 canonical JSON，严格允许
   `OK` / `PASS_WITH_WARNINGS` / `LIMITED_READER_CONTEXT`。
3. 两类 JSON 使用同一 strict loader：拒绝 duplicate key、`NaN`、`Infinity`、
   `-Infinity`、非法 UTF-8 与非法 JSON；根对象必须是 mapping。
4. `schema_version` 必须是 exact integer `1`，不能用 `true`、`1.0` 或字符串 `"1"`
   冒充；`report_type`、由文件名解析出的 `as_of`、`production_effect=none` 和状态字段
   必须相互一致。Report quality 的 `status` 还必须与
   `report_quality_status` 完全一致。
5. 缺文件、路径/日期不匹配、契约不完整、未知状态、bytes tamper 或明确失败均生成
   typed diagnostic，并阻断下游。

### S2：canonical finalization gate

1. controlled run 在 active run-control lease 内完成 executor steps 后进入
   finalization；释放 lease、写 terminal PASS 之前必须完成 current-run daily report、
   periodic plan、dashboard、decision summary、blocked-only order intent、最终
   Reader Brief refresh、两类最终质量校验及 finalization evidence。
2. executor metadata 在 finalization 前冻结。会被 finalization 重写的
   Reader Brief、Owner Daily Brief、Report quality 与 Reader Brief quality legacy
   paths 不再列入 `metadata.produced_artifacts`；metadata 此后不重写，decision summary
   绑定这份稳定 metadata 的 SHA256。上述最终可变产物的 exact identity 由
   finalization evidence 与 bundle manifest 接管，避免 metadata checksum 自循环或
   decision summary 引用 stale metadata。
3. 最终 Reader Brief JSON/HTML 的 path、SHA256、size 形成同一个
   `final_reader_brief_bytes.v1` binding，并同时写入 Report quality、Reader Brief
   quality 和 finalization evidence；四份 quality JSON/Markdown 及 canonical/legacy
   mirror 也必须逐文件 same-byte 绑定。旧的 pre-refresh gate、相同文件名或子命令
   rc=0 都不得冒充最终 bytes 的证明。
4. bundle manifest 使用两阶段发布：先原子写 `status=FINALIZING`，对磁盘上的
   evidence、最终 Reader Brief、quality artifacts、metadata checksum、
   canonical/legacy mirrors 和 manifest artifact identities 执行 closure validation；
   再对拟发布的最终 `PASS` / `PASS_WITH_SKIPS` manifest 执行同一 closure validation，
   通过后才原子替换 manifest，并允许 run-control terminal PASS。
   `FINALIZING` 只是中间发布状态，不能被消费者解释为成功。
5. finalization、closure、manifest 写入或 terminal PASS state 写入出现可捕获异常时，
   evidence 与 manifest 必须执行补偿降级，state/CLI 必须失败并记录 typed blocker。
   closure 前只允许发布 `FINALIZING`，因此即使失败记录本身再次写失败，也不能遗留
   closure 未通过的 PASS bundle。
6. `run_ledger.v1` 增加可向后兼容的顶层 `run_status` 与
   `run_blocker_codes`。历史 payload 可继续缺省这两个字段，但新运行的消费者必须读取
   顶层 run outcome；finalization 发生在既有 workflow entries 之后，因此即使所有
   entries 都是 `PASS` / `SKIPPED`，顶层 `run_status=FAILED` 仍代表整次运行失败。
   消费者不得只聚合 `entries` 推断 whole-run PASS；旧 ledger 缺少顶层 outcome 时必须
   与 canonical state 对照。
7. 成功路径保持现有 artifact 路径、Reader Brief schema、受限状态语义和只读边界；
   全链固定 `production_effect=none`，不写 production weights、active shadow
   weights，不允许 broker/order/trading。
8. 本 slice 不把多个独立文件声明为 power-loss atomic：terminal state 与最终 manifest
   之间的进程 kill、断电、磁盘级联写失败及恢复演练仍归现有 DATA-GOV D0C
   crash-durability/backup-restore 工作。当前验收证明正常执行与可捕获异常的补偿语义，
   不替代 D0C。

### S3：文档、兼容与验收

1. 更新 operations runbook 与 `docs/system_flow.md`。
2. 刷新 module/test manifests、compatibility baseline 与相关 source hashes。
3. 在最终提交树运行 focused、architecture、contract、integration、
   reproducibility 和任务风险相称的 Full validation。
4. 正式运营验收只能通过 `aits ops daily-run`；不得用 standalone 命令拼装伪 PASS。

#### S3 Full blocker：Windows atomic replace contention

首次 Full 在 `7053 passed / 1 failed` 后暴露 canonical atomic writer 的真实平台缺口：
Windows 目标文件被另一个进程短暂以 read handle 打开时，`os.replace` 会以
`WinError 5` 拒绝替换。external-request singleflight 的 winner 正在发布新 cache
generation、contender 同时 probe 旧 generation 时可命中该窗口；writer 立即失败，
随后 coordination 按既有 policy 正确记录 `OWNER_FAILURE_RETRY_DISABLED`。focused
单跑通过不能证明该并发缺口不存在。

本任务不通过直接重跑 Full 或放宽 singleflight oracle 绕过。durable fix 固定在唯一
canonical writer：

- 只对 `os.replace` 阶段且只对 Windows transient sharing/access winerror 做短时、
  固定上限的 retry；写入、flush 与 fsync 不重复；
- 非 transient 错误立即失败；retry 耗尽仍抛 `ATOMIC_ARTIFACT_WRITE_FAILED`，保留旧
  target 并清理 unique temp；
- retry budget 使用具名 platform 常量，目的只是在短暂 reader handle 释放后完成原子
  replace，不改变 artifact bytes/schema/path 或 fail-closed 语义；
- external-request cache 现有同预算外层 retry 下沉后删除，避免 platform 与 domain
  双层退避；coordination lease/fencing 与 owner-failure policy 不变；
- 增加 transient-then-success、persistent-transient exhaustion 与既有 non-transient
  cleanup 回归，并重复运行跨进程 same-key fixture；修复后才允许第二次 Full。

同次 Full 还发现测试隔离问题：focused-diagnosis master CLI fixture 只把 master doc
重定向到 `tmp_path`，其嵌套 owner doc 仍使用项目默认路径，导致 sparse validation
checkout 在 Full 中重新 materialize owner 明确保护的研究文档。修复只允许在测试中把
嵌套 builder 的 `docs_path` 显式重定向到同一 `tmp_path`；不得读取、哈希、diff 或更新
该项目文档，也不得借此改变生产 CLI 默认路径。

## 验收标准

- CLI 对 Report quality `FAIL`、Reader Brief quality `FAILED` 返回非零，且产物仍可审计。
- `LIMITED_READER_CONTEXT` 仍返回 0，并在 dashboard/Reader Brief/最终交付中披露。
- daily step 对缺失、tamper、日期不匹配、未知状态和明确失败 fail closed。
- strict JSON 对 duplicate key、non-finite constant 和非 exact-integer
  `schema_version=1` fail closed。
- post-run canonical Reader Brief 的最终 bytes 由同一次 finalization quality evidence
  覆盖；run-control 只有在 finalization 成功后才可 terminal PASS。
- `FINALIZING` manifest 和拟发布最终 manifest 均通过 closure validation，最终
  manifest、finalization evidence、两类 quality artifacts、metadata checksum 与
  canonical/legacy mirrors 的 path/SHA256/size 一致。
- metadata 不再声称拥有 finalization 后会重写的产物；这些产物由 finalization
  evidence 与 manifest 给出最终身份。
- 失败注入证明 state、ledger 顶层 outcome、CLI 与 bundle 不会出现 false PASS；
  任意消费者都不能只凭 ledger entries 判定整次运行成功。
- 对进程 kill、断电与级联存储故障不声称 power-loss durable；该非本 slice 退出门禁的
  长期缺口由 DATA-GOV D0C 继续跟踪。
- 旧 2026-07-22 FAILED state/ledger 保持 byte-identical。
- `production_effect=none`；无 production/active-shadow weight、broker/order/trading
  mutation。

## 进展

- 2026-07-24：任务新增并进入 `IN_PROGRESS`。根因来自 Wave14 S2 后置 daily ops
  验收的 final-tree 只读审计；未通过人工查看 JSON 绕过，先直接修复再执行新的
  provider-ready canonical daily run。
- 2026-07-24：设计收紧为 strict JSON + exact-integer schema、稳定 executor metadata
  与 finalization-owned identities 分离、`FINALIZING -> closure -> final manifest`
  两阶段发布，并为 `run_ledger.v1` 增加顶层 whole-run outcome；仍等待最终树的正式
  validation 与 canonical daily-run 验收，状态保持 `IN_PROGRESS`。
- 2026-07-24：审查明确区分“可捕获异常补偿”与“跨文件 power-loss durability”；
  后者继续归 DATA-GOV D0C，不在 OPS-067 中以临时 workaround 或口头说明冒充闭环。
- 2026-07-24：实现候选已落在 `8fb33e5c`，scheduler strict-artifact fixture 与
  compatibility authority 闭合修正在 `ada1f7bc`；候选 tree
  `65af3adf7bb1d60e6b8f53704362a212f1a6737b` 上的 architecture
  `569 passed / 107.75s`、contract `268 passed / 119.99s`、integration
  `993 passed / 50.59s`、reproducibility `23 passed / 17.31s` 均为 PASS，
  delta focused `36 passed`，DevEx/Ruff/Black 均为 PASS。状态仍为
  `IN_PROGRESS`：还必须在证据写回后的最终候选上通过 Full 与 post-Full gates，
  推送稳定后再只用 canonical `aits ops daily-run` 完成运营验收。
- 2026-07-24：commit `00b41d5c` / tree `100268ee` 的首次 Full 运行
  `7053 passed / 1 failed / 4 skipped / 1091.87s`。唯一失败为 Windows
  `os.replace` 对短暂 reader handle 的 `WinError 5` contention；16 个 xdist worker
  活性采样约占用 15.5 cores，证明不是挂死。已按 no-silent-workaround 登记 S3
  blocker；在 canonical writer 完成 bounded transient retry 和定向重复验证前，不把
  本次结果改写成 PASS，也不直接启动第二次 Full。
- 2026-07-24：Windows canonical writer 修复已在 `b0f4c82c` 完成；combined focused
  `101 passed`、architecture `571 passed`、contract `268 passed`、integration
  `993 passed`、reproducibility `23 passed`。第二次 Full 的 parent preflight 随后暴露
  旧 summary 内绝对 worktree locator 不可移植，已按 no-silent-workaround 登记并实现
  ENG-VAL-010。真实 copied parent 的 summary/profile SHA 与 bytes 不变，新增
  `validation_parent_run_import.v1` SHA-256=`c7ad6fdc...0af4`；新 binding preflight
  `PASS` 并保留原 `PYTEST_FAIL`。现等待 generated authority refresh、exact commit
  formal gates 与第二次 Full，不以 focused 或 print-only 冒充 Full PASS。
- 2026-07-24：exact candidate=`d92eae358cc4b0819d1fa1205e0d3dad8104b57a`
  / tree=`0e3bca65e5d98d96b4ec892f58936e8680f3a034` 的 focused/static 与
  architecture/contract/integration/reproducibility=`186/577/274/993/23 passed`。
  第二次 Full 通过 `failure_fix_rerun` + `portable_import_v1` 精确绑定首次失败，
  结果=`7115 passed / 4 skipped / 643 warnings / 1176.47s`；summary/profile
  SHA-256=`33cb2f5f...4435`/`6e8c076c...5b87`，profile、telemetry、performance、
  provenance、scheduler applied/no-fallback/order 与 formal selection 全部 PASS。
  首次 FAIL 保留且未被覆盖；一次执行通道中断未产生 formal artifact，未冒充额外 attempt。
  当前只剩 evidence-only post-Full gates、commit/push 与随后唯一 canonical
  `aits ops daily-run` 运营验收，任务状态继续 `IN_PROGRESS`。
- 2026-07-24：Full evidence writeback commit=`633d04a11896642462cc551fb335137c4db31223`
  / tree=`49c856d92840317833fcc18b31c1bf84d9ca7c72` 的 focused/static、task authority、
  compatibility/deprecation freshness 与
  architecture/contract/integration/reproducibility=`577/274/993/23 passed`。
  工程状态进入 `ENGINEERING_COMPLETE_AWAITING_CANONICAL_DAILY_ACCEPTANCE`；完成最终
  evidence commit 的 exact-tree repeat gates 与正常 push 后，才接管唯一 scheduler trigger
  `aits ops daily-run`。
