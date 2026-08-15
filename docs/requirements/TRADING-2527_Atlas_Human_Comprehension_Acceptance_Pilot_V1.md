# TRADING-2527：Atlas 人类理解验收 Pilot V1

最后更新：2026-08-16

- stable task id：`TRADING-2527_ATLAS_HUMAN_COMPREHENSION_ACCEPTANCE_PILOT_V1`
- priority：`P1`
- status：`IN_PROGRESS`（仅 2527-A protocol preparation）
- proposed governed mode：2524 `DUAL_LANE` strategy-evidence preparation + final serial human gate
- contract change：`false`（验证既有 frozen reader contract；若 pilot 需要改合同则另开 wave）
- predecessor gates：2527-A 依赖 2524-S0；2527-B 依赖 2526-B 的 exact-HTML PASS 与 Owner policy
- production effect：`none`
- broker action：`none`

## 1. 问题与目标

自动化可以证明元素存在、可到达、无溢出、ARIA state 变化和 artifact 可重放，但不能证明读者没有把
engineering/page PASS 当成 strategy PASS，也不能证明读者理解 `LIMITED`、数据/证据/页面日期、停止原因
与下一合法动作。

本任务对冻结的 exact candidate HTML 执行受治理的人类理解验收 pilot，建立可重复、可审计的事实基线。
本任务不发明样本数量、通过比例、critical-error 或投资解释阈值；这些必须由 Owner policy 冻结。

## 2. 冻结测试对象

每轮记录 exact HTML SHA-256、repository/source commit、manifest/sidecar identity、viewport、browser、
OS、assistive technology、默认展开状态与 protocol 版本。HTML identity 在测试中变化时，旧结果不得与新
identity 合并，必须重新测试。

## 3. 无引导任务与事实记录

参与者需用自己的语言回答：

- 当前研究主线和最大 blocker 是什么；
- 哪些只是工程能力，哪些是真实研究证据；
- 当前证据不能推出什么；
- 下一责任方与下一合法动作是什么；
- 现在是否允许形成策略有效性结论、启动 engine 或下单；
- 数据截至何时、证据何时形成、页面是否 current；
- 相对上一 snapshot 发生了什么变化。

记录原始回答、首次点击/滚动路径、首次误解、不认识术语、PASS 误读、证据查找、判断改变、桌面/移动
差异与键盘/screen-reader 阻塞点。至少由两名 Owner 指定 reviewer 独立映射到 canonical truth；可先记录
`正确 / 部分正确 / 错误 / 无法回答 / 页面诱导禁止推断`，但何种组合构成 PASS 由 Owner policy 决定。

## 4. Owner 决策、允许动作与禁止动作

Owner 必须批准 participant profiles、sample、scenario、critical error、通过阈值、复测规则、独立 reviewer、
签署人和 accessibility coverage。

允许：approved protocol、招募、观察、verbatim logging、独立编码、disagreement resolution、问题修复后
绑定新 exact bytes 的 retest。

禁止：自动化代签、临时发明通过线、HTML 变化后合并旧结果、以无投诉/无 overflow/DOM PASS 宣称理解
通过、运行新研究或触发外部/交易系统。

## 5. 两阶段执行与 path claims

### 2527-A：并行 protocol preparation

2524-S0 后可与 2525、2526-A 并行；可与 2526-A 共用 strategy-evidence worktree，但 task-owned paths 与
evidence lineage 必须独立。预先冻结：

- `config/atlas/reader_comprehension_protocol.yaml`（仅保存 reviewed schema、PENDING policy slots 与
  provenance，不得替 Owner 填入 sample/threshold/critical-error 决策）；
- `src/ai_trading_system/atlas/reader_comprehension_protocol.py`；
- `tests/atlas/test_reader_comprehension_protocol.py`；
- `docs/research/atlas_reader_comprehension_protocol_v1.md`。

2527-A 可准备 schema validator、participant profile template、scenario sheet、canonical truth rubric、
identity binding 与 negative-case/retest procedure，但不得招募参与者、执行 pilot 或签署 acceptance。
supporting requirements、task registry、canonical observation/attestation artifacts 和 final generated outputs
由 coordinator 管理。

### 2527-B：exact-HTML 人类验收

只有以下条件同时满足才可串行启动：

1. 2524 coordinator 已生成唯一 final HTML/manifest/sidecars；
2. 2526-B 对同一 HTML SHA-256 完成 browser/AT/mobile PASS；
3. Project Owner 已批准 participant/sample/scenario/critical-error/threshold/retest/reviewer/signature policy；
4. canonical truth、两名独立 reviewer 与 observation/attestation 记录路径已冻结。

2527-B 运行期间不得修改 HTML。若发现问题需要修复，当前结果按 exact identity 保留并停止；回到
coordinator remediation、重新生成 bytes、重跑 2526-B，再为新 identity 启动新的 2527-B round。

## 6. 制品、验收与退出条件

预期制品：approved protocol、participant profile/matrix、scenario sheet、exact HTML identities、verbatim
observation log、independent answer coding、disagreement log、issue-to-source mapping、retest identity 与
Owner attestation。

验证：canonical truth replay、两名独立 reviewer、desktop/mobile/assistive-technology scenario coverage、
identity immutability 与 negative-case review。

Exit criteria：Owner 对 exact HTML 明确签署 `PASS / FAIL / PENDING_REVIEW` 并记录理由、证据与 follow-on；
不得由统计脚本自动升级 acceptance。

STOP CONDITION：sample/threshold/scenario policy 未批准，测试对象 identity 中途变化，或 reviewer 无法绑定
canonical truth 时立即停止验收。

只有 Owner 完成人类理解 attestation 后，才允许评估是否把该 reader contract 推广到 Reader Brief 或其他
研究页面；推广本身必须另有受治理任务。

## 7. 进度记录

- 2026-08-15：根据 Project Owner 要求登记为后续计划；状态为 `PROPOSED`。尚未批准 protocol、招募
  participant、运行 pilot 或签署任何人工 acceptance。
- 2026-08-15：Project Owner 确认 staged topology。2527-A 可在 strategy-evidence lane 提前准备 protocol/
  schema/scenario/truth rubric；2527-B 仍是硬串行门，必须等待唯一 exact HTML、2526-B PASS 与 Owner
  policy。状态仍为 `PROPOSED`，未启动 participant 或 acceptance。
- 2026-08-15：canonical update 的首个批量进程在工具 30 秒 yield 后继续完成，随后人工补跑导致同一
  `trading-2527-staged-comprehension-topology-20260815-v1` payload 形成两个连续 no-op events。两者的
  status、owner、blocker、acceptance 与 notes 完全一致，canonical validator 为 `PASS`，projection 未
  分叉。按 append-only 纪律保留原事件，并以唯一 audit change id 记录本执行事件；不手工改写历史。
- 2026-08-16：2524-S0 exact main=`ece8d97373c1a8a70949aa0ae445b79593ee09b3`，DUAL_LANE START
  claims `PASS`。2527-A 与 2526-A 共用的临时 Git worktree 计划为
  `D:\Work\AITradingSystem_trading2526_2527_evidence`，但只写第 5 节四个 task-owned protocol paths；
  exit condition 与 2526 requirement 记录一致。当前不招募、不执行 pilot、不生成 participant data，
  `2527-B` 继续等待唯一 exact HTML、2526-B PASS 与 Owner policy。
