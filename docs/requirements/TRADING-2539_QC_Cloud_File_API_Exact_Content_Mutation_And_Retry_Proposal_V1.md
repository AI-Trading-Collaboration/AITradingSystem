# TRADING-2539 — QuantConnect Cloud file API exact-content mutation and retry proposal V1

- priority: `P0`
- status: `BLOCKED_EXTERNAL`
- governed mode: `SINGLE_LANE`
- predecessor: `TRADING-2538`
- production effect: `none`
- broker action: `none`
- external boundary: Owner-authorized Free Web IDE canary only; one clone of project `34808569` and one `main.py` canary save in that clone are allowed, while the original project, compile, backtest, provider query, order, fill, public sharing, and formal candidate write remain prohibited

## 1. 问题与结论

TRADING-2538 唯一一次已授权 Monaco mutation attempt 使用 `Control+A`、浏览器 clipboard、
`Control+V`、`Control+S`，但保存并构建的仍是原代码。该 token 已消费，terminal counters 为
project mutation attempts / verified mutations / Cloud backtests / provider queries / orders / fills =
`1 / 0 / 0 / 0 / 0 / 0`，不得用当前 token 重试。

2026-08-21 的后续只读诊断确认：

1. Monaco 的实际输入面是 `.native-edit-context`；`textarea.ime-text-area` 为内部只读 IME 面，
   不能据此判断项目永久只读；
2. 项目加载早期标签短暂出现 `Read-only`，加载完成后该标记消失，编辑器、Cloud Build 与
   Backtest 操作恢复，故永久权限只读不是已证明根因；
3. `Control+A` 能形成 Monaco selection，但 synthetic `Control+C` 未更新预置 clipboard，
   因而 clipboard bridge 不能作为 exact content carrier 或 copy-back authority；
4. Explorer 只提供 New File、New Folder、Refresh Explorer 与 Collapse Folders，没有可见的
   upload / replace existing file 入口；
5. QuantConnect 官方 REST API 提供 `/files/update` 更新文件内容和 `/files/read` 回读文件内容，
   可以用 exact request bytes 更新 `main.py`，随后回读 UTF-8 内容并验证 byte count 与 SHA-256。

因此 intended best solution 是使用官方 authenticated file API 完成一次原子内容更新和回读校验，
不再使用 clipboard paste、Monaco direct typing、Explorer upload、page-internal fetch 或其他 UI carrier。

官方依据：

- [QuantConnect API authentication](https://www.quantconnect.com/docs/v2/cloud-platform/api-reference/authentication)
- [QuantConnect update file](https://www.quantconnect.com/docs/v2/cloud-platform/api-reference/file-management/update-file)
- [QuantConnect read file](https://www.quantconnect.com/docs/v2/cloud-platform/api-reference/file-management/read-file)

## 2. 当前直接阻塞

本机只读 readiness probe 显示：

- `lean` CLI 未安装；
- `QC_USER_ID` 未设置；
- `QC_API_TOKEN` 未设置；
- `$USERPROFILE/.lean` 不存在。

因此当前不能对官方 API 做 authenticated read/update proof。Project Owner 需要先通过安全的
process-local secret 输入提供 `QC_USER_ID` 与 `QC_API_TOKEN`，或者完成 reviewed Lean CLI
authentication。凭据不得写入 tracked/untracked repository 文件、命令参数、shell history、日志、
task artifact、Codex 回复或屏幕截图；所有 readiness 输出只允许记录 present/absent，不允许记录值、
前后缀、hash 或可关联片段。

官方 API 仍是 exact-content mutation 的 intended best solution。Project Owner 为保持 Free 套餐，已于
2026-08-21 单独批准第 9 节定义的隔离 clone canary，用来验证 Web IDE direct-input 与人工 copy-back
是否能形成可核验的免费 carrier；该例外不授权正式候选写入，也不把 canary 结果等同于 API read-back。

## 3. 分阶段门禁

### S0 — 本地 proposal closeout

1. 固化本诊断、API carrier、secret boundary、exact counters 与 fail-closed 规则；
2. 完成 canonical task registration、focused governance validation、local commit、local-main
   fast-forward；
3. 使用单独的新 SSH-over-443 授权完成普通 push（若默认端口仍不可达），并验证
   `local main = origin/main = proposal candidate`；
4. 本阶段不触发任何 QuantConnect API、browser mutation、compile 或 backtest。

### S1 — credential readiness 与 read-only remote precondition

仅在 Owner 提供安全认证方式并发出新的 exact authorization 后：

1. 最多一次 `/authenticate`；
2. 最多一次 `/files/read` 读取 project `34808569` 的 `main.py`；
3. 记录 response status、文件名、UTF-8/LF byte count 与 SHA-256，不提交 remote source 内容；
4. 将 remote pre-mutation SHA-256 与允许的 predecessor identity 精确匹配；不匹配则在 mutation 前
   fail closed；
5. authentication/read 失败不得改用浏览器或 page-internal request，也不得自动重复请求。

S1 是 read-only admission，不授权 project mutation、compile、backtest 或 provider query。

### S2 — exact single-use mutation 与 zero-order run

仅在 S1 PASS 且 Owner 返回绑定 ordinary-pushed proposal SHA、remote pre-mutation SHA、candidate
byte count/hash、endpoint/counter limits 与 expiry 的新 exact token 后：

1. 恰好一次向 `/files/update` 提交 project `34808569`、name=`main.py`、2537 sealed candidate
   exact content 与 `codeSourceId=Codex Governed TRADING-2539`；HTTP request dispatch 即计为首次
   project mutation attempt 并不可逆消费 token；
2. update response 成功后恰好一次 `/files/read` 回读 `main.py`，要求 UTF-8/LF byte count=`26223`
   且 SHA-256=`86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4`；
3. 回读不匹配、缺失、非 canonical LF、API error 或 response ambiguity 均 fail closed；不得第二次
   update，也不得退回 UI carrier；
4. 只有 exact read-back PASS 才允许最多一次 compile 和最多一次 zero-order Cloud backtest；
5. provider query / orders / fills 最大值保持 `1 / 0 / 0`；结果继续受 TRADING-2537/2538 的
   export-safe attribution、execution/attribution terminal separation、DQ/PIT/selection/engine/
   cash-preservation boundary 约束。

## 4. Future authorization fields

新的 mutation token 至少必须 exact 绑定：

- `ordinary_pushed_main_sha`；
- TRADING-2539 requirement file SHA-256 与 canonical proposal identity；
- TRADING-2538 blocked manifest/incident/ledger identity；
- TRADING-2537 package manifest content SHA-256、candidate LF byte count 与 SHA-256；
- target project id=`34808569`、file name=`main.py`；
- pre-mutation remote file SHA-256（来自 S1）；
- API base=`https://www.quantconnect.com/api/v2`；
- update/read/compile/backtest exact endpoints 与 maximum attempts；
- maximum project mutations / Cloud backtests / provider queries / orders / fills=`1 / 1 / 1 / 0 / 0`；
- authorization expiry、single-use 和 invalidation-on-first-update-or-run-attempt；
- `production_effect=none`、`broker_action=none`。

占位符、不完整 token、字段重排、duplicate/extra key、过期、future-dated、wrong project/file、
wrong source、wrong precondition hash 或 trailing newline 均拒绝 admission。

## 5. Continually prohibited

- 当前 TRADING-2538 token 的任何 retry；
- 在未取得新 exact token 前调用 QuantConnect authenticate/read/update/compile/backtest API；
- page-internal fetch、raw CDP、DevTools、clipboard paste、Monaco direct typing、Explorer upload、
  alternate browser 或未登记 CLI carrier；
- 第二次 update、第二次 post-update read-back、第二次 compile/backtest 或自动恢复；
- API token/user id 的持久化、回显、hash、截图、提交或日志记录；
- project creation、range expansion、purchase/subscription、paper/live/broker/portfolio action；
- raw option rows、contract identifiers、strike/expiry/right/quote/Greeks/IV/OI/volume、Logs-as-data、
  Object Store 或从日志重建 raw value；
- DQ/PIT admission、selection/engine activation、strategy validity、return/risk、deployability 或
  investment conclusion。

## 6. Path claims and validation

Task-owned：

- `docs/requirements/TRADING-2539_QC_Cloud_File_API_Exact_Content_Mutation_And_Retry_Proposal_V1.md`。

Coordinator-owned：canonical task registry/index/generated compatibility views，以及新增 task 后的
`tests/test_arch_005_s5_task_source_cutover.py` deterministic task-count expectation 与对应生成的
`inputs/architecture/arch_004e_test_manifest.yaml` freshness。当前 proposal 不改变 CLI、cache schema、
report output、data-quality gate、scoring、backtest behavior 或数据流，所以不更新 `docs/system_flow.md`。

Known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 始终不读取、hash、stage 或修改。

验证至少包括 canonical task-source validate、generated compatibility freshness、governed worktree audit 与
适用 focused architecture checks。只有本地 proposal candidate 完整验证后才可提交和集成。

## 7. Temporary workspace and Git lifecycle

本任务分支：`codex/trading-2539-qc-cloud-file-api-proposal`；不创建额外 Git worktree、clone、download
目录或 credential 文件。任务仅在 proposal 本地集成和可授权 publication boundary 建立后进入
`BLOCKED_EXTERNAL`，等待 Owner 的 credential readiness 与 exact external token。

默认 remote push 若因端口 22 不可达而需要 SSH-over-443，必须取得新的、明确绑定 proposal candidate
SHA 的单次授权；TRADING-2538 closeout 使用过的 SSH-over-443 授权已消费，不能复用。

## 8. Acceptance criteria

1. 失败根因与 transient read-only、clipboard bridge、Explorer upload 三条诊断事实可审计；
2. 官方 `/files/update` + `/files/read` 被记录为唯一拟议 exact-content carrier；
3. missing credential/CLI blocker、secret handling 与两阶段 Owner authorization 边界明确；
4. 未触发 QuantConnect API、mutation、compile、backtest、provider query、order 或 fill；
5. future S2 必须 update-once、read-back-once、exact byte/hash gate、no UI fallback、no retry；
6. TRADING-2537/2538 的 source/date/attribution/DQ-PIT/production/broker 边界不变；
7. canonical registry、generated views、focused validation 与 governed audit PASS；
8. local commit/local-main integration完成；remote ordinary push 仅在新的 SSH-over-443 授权和 closeout gate
   通过后执行。

## 9. Owner-approved Free Web IDE clone canary

Owner 决定：

`授权使用 QuantConnect Free Web IDE 克隆 project 34808569 一次作为 TRADING-2539 传输沙箱；仅在克隆项目 main.py 执行一次短 canary 写入和保存，用于验证人工复制回读及本地 SHA-256 校验。禁止修改原项目、编译、回测、公开分享、迁移、实盘、下单或成交。验证完成后不得写入正式候选代码，须另行授权。`

这是 Owner 明确接受的临时 workaround，边界如下：

- reason：Free organization 不提供 API Access，Owner 选择保持免费；Web IDE 当前没有官方 upload、
  download 或 source hash surface；
- behavioral impact：恰好创建一个 project `34808569` 的云端 clone，并只在该 clone 的 `main.py`
  完成一次 canary replace/save；原 project `34808569` 不修改；
- canary exact LF content：

  ```python
  # TRADING-2539_FREE_WEB_CANARY_V1
  def canary():
      return "ASCII|quotes:'|brackets:[]{}|equals=|colon:|comma:,"
  ```

  exact LF byte count=`113`，SHA-256=
  `23e0492a1e2e5f4627820aecde6881fc772520c28c41f38531e24da8e007de2d`，并要求文件末尾保留一个 LF；
- risk：Monaco direct input 可能丢字节、改变换行或保存失败；浏览器自动化不能把内部 editor model、
  clipboard 或 page-internal request 作为 authority；clone 在人工 copy-back 前会暂时保留在 Owner 的
  Free organization 中；
- validation coverage：记录 clone project id/name，保存后仅用可见 editor state 确认 canary marker；
  随后由 Owner 从 clone `main.py` 人工 `Ctrl+A` / `Ctrl+C` 并粘贴回本地，由 Codex 对 UTF-8 LF bytes
  计算 byte count 与 SHA-256；未完成 copy-back 或 hash 不匹配均为 FAIL；
- exit condition：人工 copy-back exact byte/hash PASS 或首次 canary save/read-back 不确定即停止；无论
  PASS/FAIL 均不得写入 sealed candidate。clone 不含独有策略实现，但在获得单独删除授权并确认无
  唯一证据前不得删除；下一 Owner 动作为完成 copy-back，并决定是否授权 clone cleanup 或正式候选步骤；
- prohibited：不得对原项目写入，不得第二次 clone、第二次 canary save、candidate write、build、
  backtest、provider query、公开分享、迁移、live/broker/portfolio/order/fill、page-internal fetch、raw CDP
  或 clipboard 自动化读取。

## 10. Progress

- 2026-08-21：完成 logged-in QuantConnect project `34808569` 的只读诊断。加载完成后 Monaco 可编辑；
  Explorer 无 upload/replace；clipboard copy-back 仍不可信。未进行 code input、save、build、run 或 API call。
- 2026-08-21：核对官方 file API 与 authentication 文档；本机 readiness probe 仅确认 `lean`、
  `QC_USER_ID`、`QC_API_TOKEN` 和 `.lean` 均不存在，未读取或生成任何 secret。
- 2026-08-21：intended best solution 确定为 official file API exact update + read-back hash gate；任务保持
  `BLOCKED_EXTERNAL`，next owner 为 Project Owner 提供安全 credential readiness 并审阅 fresh-token scope。
- 2026-08-21：首次 focused parallel pytest 为 `29 passed / 1 failed`，唯一失败是新增 canonical task 后
  repository task count 从 `1011` 增至 `1012`；将该 deterministic expectation 纳入 coordinator claim
  后按实际 canonical fragment count 更新，未改动 runtime、policy 或投资语义。
- 2026-08-21：首次 Architecture formal tier 为 `864 passed / 1 failed`，失败 artifact 为
  `outputs/validation_runtime/architecture-fitness_20260821T082853Z/test_runtime_summary.json`；唯一 mismatch
  是修改 deterministic test 后 `arch_004e_test_manifest.yaml` stale。经 coordinator claim 后使用官方
  generator 刷新，architecture fitness 重新计算为 `PASS`、module/test orphan=`0/0`、violation=`0`。
- 2026-08-21：Owner 选择保持 Free 套餐并授权一次隔离 Web IDE clone canary。执行前固化 113-byte
  canary、人工 copy-back SHA gate、原项目零 mutation、零 build/backtest/order/fill 和 clone 保留/清理
  边界；该 workaround 不改变 official API 仍是正式 exact-content carrier 的结论。
- 2026-08-21：唯一一次 Clone 已完成，沙箱为 `Clone of Sleepy Yellow-Green Shark`、project id=
  `35444189`；Free organization 的唯一 coding session 先从原项目停止，再由 clone 使用。原 project
  `34808569` 未写入；clone `main.py` 经 Monaco 原生键盘 carrier 完成一次 select-all、113-byte canary
  输入和一次 `Control+S`，可见 editor 显示三条预期代码行和第 4 行空行，tab 无 dirty marker；exact
  copy-back SHA 尚未验证。
- 2026-08-21：保存后 Cloud Terminal 自动出现 `Built project 'Clone of Sleepy Yellow-Green Shark'
  in Cloud for Lean Engine 2.5.0.0.18016, with Id 'e972ea-ce1a69'`。Codex 未点击 Cloud Build，但该自动
  compilation side effect 仍超出 Owner 的 zero-compile 边界，按 incident 处理并立即停止；不得重试、
  不得 backtest。终端计数为 clone / canary mutation / save / Cloud build / backtest / provider query /
  orders / fills = `1 / 1 / 1 / 1 / 0 / 0 / 0 / 0`。任务转回 `BLOCKED_EXTERNAL`，等待 Owner 人工
  copy-back、SHA 核验、incident review 和 clone cleanup 决定。
- 2026-08-21：Owner 将人工 copy-back 保存为 `D:\Work\TRADING-2539-canary-copyback.py`。只读 byte
  audit 结果为 UTF-8 无 BOM、raw byte count=`116`、raw SHA-256=
  `2b7e94a647459cc6aa94dd91a3e07b9752d86b97ea3daaee2aedc11f4a7e5461`、LF=`3`、CRLF=`3`、
  lone CR=`0`、末尾 LF=`true`；因此预先声明的 raw 113-byte gate 为 FAIL，差异类型仅为本地保存器
  将三处 LF 转为 CRLF。
- 2026-08-21：同一文件经只读 CRLF-to-LF canonicalization 后 byte count=`113`、SHA-256=
  `23e0492a1e2e5f4627820aecde6881fc772520c28c41f38531e24da8e007de2d`，与 sealed canary 完全匹配；
  分类为 `CANONICAL_LF_CONTENT_PASS / RAW_COPYBACK_FILE_FAIL_LINE_ENDING_ONLY`，不得静默升级为 raw
  exact PASS。该外部本地文件由 Owner 保留到 raw LF 复核或明确放弃；Codex 不修改或删除它。任务继续
  `BLOCKED_EXTERNAL`，等待 Owner 将 copy-back 文件转换为 LF、审阅 auto-build incident，并决定 clone
  cleanup；QuantConnect 端不得再执行任何动作。
