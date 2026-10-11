# GOV-008：研究需求驱动的治理重构（Research-First Governance Refactor）

最后更新：2026-10-10（登记；P0 开始，P1 并行）

稳定任务 ID：`GOV-008_RESEARCH_FIRST_GOVERNANCE_REFACTOR`

状态：`IN_PROGRESS`；优先级：`P0`（工程任务最高优先级，owner 2026-10-10 决定）

模式：`SINGLE_LANE`；production effect：`none`；broker action：`none`；`agent_harness=claude_code`

## 1. 背景与 Owner 决定

过去三周的工作（GOV-007 及 DEVX-015/016/018/021/022/023）持续在优化旧发布流程：12 次发布、每次验证链
3.6～4.6 小时，10 月 1～10 日约 180 个提交中没有一个是投资系统本身的改动。2026-10-10 的系统性梳理
得出的结论是：问题不是检查太少或太多，而是流程设计让每个问题只能靠"再加一层检查"解决。

Owner 决定（`owner_decision:GOV-008:2026-10-10:research_first_refactor_v1`）：

1. 同意"从研究需求推导保证、重做流程、停止旧发布链优化"的方向（D1）；
2. 重构是工程任务的最高优先级；重构可能影响的既有任务，待重构完成后重新评估必要性（挂起，不删除）；
3. 不使用 PR，也不开启 GitHub 分支保护（项目由 owner 独立开发，2026-10-10 补充决定）。新流程是"任务分支 -> 本机
   新门 -> 同一 SHA 快进 main 并普通推送"；门禁由 `ship` 在本机运行，不依赖 GitHub 强制（见第 4 节第 4 条与第 10 节）；
4. 验证平台为本机 Windows；Linux 可移植性与其它 GitHub 侧问题本阶段不考虑；GitHub Actions（Windows）至多作为可选的
   夜间复核，不是门禁；
5. 真实链路测试按"默认删除、举证保留"处理（见第 5 节）；P1 文档评论（2026-10-10）确认接受收窄后的范围：只删依赖
   租约/发布事务环境的真实链路变体，五条研究线的行为契约测试保留，例外名单在 P2 前由脚本生成并交 owner 裁决；
6. 产品代码中的工程设计代码在重构后应下降（见第 5 节），具体由 P1 的使用分析确认；
7. 开始重构后，任务排程必须以最小化总开发时间为目标（见第 8 节）；
8. 五条研究线（Composer 前瞻观察、equal-risk、simple baseline、layer1/2 meta policy、QQQ options）全部保留，
   本次重构不改动实际研究线需求（P1 文档评论，2026-10-10）；日报链路中的 ETF dynamic v3 步骤退役（同日评论）。
   这两项收窄了第 5 节的删除范围：研究线代码与行为契约测试保留，只删依赖租约/发布事务环境的变体。

## 2. 问题证据（摘要）

| 症状 | 旧补丁 | 根因（流程设计） | 重构方向 |
|---|---|---|---|
| 租约库回放 25～30 s 且越用越慢 | 外置/v4/memo/复用/并行回放/seal（约 8 次发布） | 发布状态存成只增不减的事件史并整体回放 | 发布状态用小的当前态记录，审计靠 git |
| Full 7 h -> 2h13m | 调度清单 v1～v7、独占组、K 上限、爬坡 | Full 内约 99 个节点在测试里再跑一遍 Full（约 25 节点小时） | 机制薄到不需要此类测试，机制测试独立 |
| 固定超时被负载打爆 | 逐个标定 180/900/1800 s（PROVISIONAL） | 每次调用哈希约 16.5k 个 venv 文件做运行时身份 | 取消每调用身份哈希 |
| 发布阶段缺陷需重跑 3～7 h Full | 预演、冒烟、轻量层必跑 | PASS 绑定最终候选，发布代码又由该 Full 验证 | 发布 = 推送本机新门已验证的 SHA |
| 本地合并 63 min、残留 HEAD.lock | 恢复命令、stale-lock | `git merge --ff-only` 挂了回放租约库的 hook | 不挂这类 hook |
| 每候选至少一次 reseal | reseal 命令、pin 改 floor | 派生数据入库再用哈希测试 | 派生数据不入库，需要时按需生成比较 |
| 文档/任务行只能"搭下一班车" | 攒在 lane 上 | 发布成本与改动大小无关 | 按改动区域分级 |

另有耦合：13 个 src 文件（composer 前瞻采集、命名 DQ 执行、每日输入采集、研究 outcome 访问、release
promotion、scheduler checkout 等）引用 lease/checkout 机制，研究执行与日常运行因此依赖开发发布机制。

量化基线（2026-10-10，p-20261010-v2 Full profile）：15,276 个测试 / 34.9 节点小时 / 2h13m；发布与工作流机制测试
占 62.6%（3,794 节点），命名 DQ 与 composer 类研究采集测试占 25.2%（1,759 节点），其余产品测试 12.3%
（9,723 节点，约 4.3 节点小时，其中 ≥30 s 的 110 个节点占 2.2 节点小时）。分类按文件名正则，近似值。

## 3. 目标：研究真正需要的 7 条保证

| # | 保证 | 实现位置 |
|---|---|---|
| G1 | 数据可信：来源、校验和、DQ 门禁，报告中可见 | 运行时（`aits validate-data`，保留） |
| G2 | 无前视、可复现：窗口角色、PIT、运行绑定 commit/配置/数据快照 | 统一 `RunContext` |
| G3 | 防事后选择：预注册、试验计数、holdout 不回流、事前时间戳 | 实验台账 + push 时间见证 |
| G4 | 结论边界：PASS 不等于投资结论；`production_effect=none`/`broker_action=none`；阈值有出处 | 运行时 + 区域 C 不变量测试 |
| G5 | 改代码不破坏以上各条 | 本机新门（产品套件 + 不变量套件）|
| G6 | 工作不丢：任务与决策可追溯 | 简化任务库 + git 历史 |
| G7 | 人能读懂：结论/限制/下一步在第一屏 | Reader Brief（精简） |

其余机制都只是手段，必须能追溯到某条保证，否则不保留。

## 4. 新流程设计（目标形态）

1. **按改动区域分级**：A 记录区（文档/实验记录/任务）只做 lint 与链接检查；B 产品代码区走本机 PR 套件；
   C 语义关键区（DQ 门禁、PIT、窗口策略、阈值配置、安全边界、不变量测试）在 B 之上加不变量套件与 owner
   决策记录；D 开发机制（fence/租约库/DEVX-015/seal/DUAL_LANE/compat 钉）冻结后删除。
2. **研究运行器与实验台账**：所有研究命令经同一 `RunContext`（干净树与 commit 固定、`validate-data`、窗口
   requested/evaluated 标注、`manifest.json`）。每个研究问题一个目录：`prereg.yaml`、`runs/`（自动登记试验）、
   `decision.md`。预注册提交并 push 之后才允许首次读取结果，GitHub push 时间为外部时间见证。前瞻采集保留只增
   哈希链 JSONL（只增不减在这里才是真需求）。
3. **验证金字塔**：提交前（lint/format/类型/配置 schema，约 1 min）-> 快测（受影响模块，约 5 min）-> PR 套件
   （保留的产品测试排除 ≥30 s 节点，8,684 个节点，16 核三次实测 7m40s～8m46s）-> 不变量套件（含在 PR 套件内）-> 夜间
   （全部产品测试加慢节点、真实数据复现）。分层靠 pytest marker 与路径，不再手写文件清单。
4. **发布 `ship`**：任务分支在本机运行新门（PR 套件加不变量套件，PR 套件 16 核实测 7m40s～8m46s（约 8.5 min），含不变量测试），通过后同一 SHA 快进
   main 并普通推送。SHA 绑定由 git 保证，没有租约库、12 阶段状态机与 reseal。门禁结果（套件、节点数、耗时、干净树）
   写进提交信息的 `Gate:` 行；区域 C 改动必须带 `Owner-Decision:` 行，缺失则 `ship` 拒绝。并发靠每任务一个
   worktree 与 git 的快进冲突检测。不开启分支保护（单人项目），所以这是 agent 自己运行的门，不是独立验证者；
   GitHub Actions（Windows）至多作为可选的夜间复核。
5. **任务、决策、文档**：任务库合并为简单 YAML（id/状态/owner/优先级/下一步/验收/文档链接），不要事件溯源、
   shadow 副本与生成视图；DONE 归档为单文件。`system_flow.md` 还原为真正的数据流图（约 400 行）；
   AGENTS.md 压到 150 行内并保持 harness 中立（同时满足 DEVX-017 的目标）。
6. **日常运行**：调度器直接运行 `aits daily-run`，不经过 checkout lease，产品路径不依赖开发机制。

目标工作量：改一个评分/报告模块并发布，从约 25 min 准备加约 3.6 h 验证降到快测 5 min 加本机新门约 8.5 min（PR 套件三次实测 7m40s～8m46s，含不变量测试）；
仅改文档/任务/实验记录从"等下一次发布"降到 5 min 内；新开一个研究实验降到一个 prereg 文件加一条命令。

## 5. 删除与简化原则

- **默认删除、举证保留**。一个测试要保留，必须回答：对应 G1～G7 的哪一条；它抓的是哪类真实缺陷；小的契约测试为什么
  替代不了。答不全即删。发布/工作流机制的 3,794 个节点随机制一起删；命名 DQ/composer 类 1,759 个节点
  （约 8.8 节点小时，其中 81 个 ≥30 s 的节点占 7.7 节点小时）逐类评估，不强烈的删除。
- **删除的安全证据**：沿用 DEVX-016 C3b 的变异等价法，抽样证明被删检查所抓的缺陷类，新门要么也抓到，要么与投资无关；
  不做逐批 owner 审批，owner 抽查（`owner_decision:DEVX-016:2026-10-07`）。
- **产品代码**：按目录粗分，核心投资管线约 16 万行（scoring 约 0.4 万、backtest 约 0.7 万、data 约 3.1 万、
  trading_engine 约 7.9 万）、研究候选族约 53 万行（其中 `etf_portfolio` 约 26 万行，candidate 宇宙已于
  2026-07-25 正式退役）、平台/契约/Atlas/CLI 接线约 23 万行、按文件名判断的治理/验证/就绪度约 19 万行、其它
  顶层约 22 万行（`reader_brief.py` 单文件 2.9 万行）。
  **P1 调用图实测（2026-10-10，静态 import 闭包，过度近似）**：1,242 个模块、132.3 万行中，日常链路可达 640 个模块
  / 72.1 万行，其他定期任务额外 1.8 万行，研究线种子额外 4.2 万行，**不被任何调度到达 558 个模块 / 54.1 万行（40.9%）**。
  日常链路内含 30 个开发机制模块 / 4.7 万行（`cli_commands/ops.py` 导入 `checkout_guard` 所致）；整个 `etf_portfolio`
  包 26.2 万行在日常闭包内（两个 ETF forward 入口加 Reader Brief 引入，候选宇宙已于 2026-07-25 退役）。
  **研究线全部保留后的重算（owner 2026-10-10 决定，见第 1 节第 8 条）**：五条线按模块名关键字取种子，静态闭包
  45.5 万行，全部保留；其中 4.7 万行开发机制模块是被它们引用的（13 个 src 文件引用 lease/checkout）。确认可移除约
  32.4 万行（24.5%）= `platform/architecture` 6.1 万 + `etf_portfolio` 26.2 万（五条线不引用 ETF 包），**前提是先完成
  两处解耦**：五条线相关文件改绑 `RunContext` 以脱离 lease；退役 ETF 日常步骤并让 Reader Brief 脱离 ETF 包。另有约
  22.4 万行（215 个模块，主要是 trading_engine 3.6 万、atlas 1.6 万、research_campaign 与若干 dynamic_* 复盘模块）既不
  属于五条线也不被调度到达，待逐模块归线后决定，乐观上限约 41%。此前"59 万行/剩 47 万行"的估计在五条线保留后作废。
  "不可达"只是候选清单：owner 手动运行的 `aits research ...` 命令不在调度里，11 个文件用 `import_module` 动态导入未计，
  五条线的种子是按文件名关键字取的近似（P1 后续用 owner 确认的线归属重做）。
- 历史不丢：重构前给 main 打 tag `legacy-governance-final`；不可变证据保留文件与哈希清单，读取器随代码删除。

## 6. 受影响既有任务（待重构后重新评估）

挂起方式：状态改为 `DEFERRED`，blocker 指向本任务，原"下一步"文字完整保留在新文字之后和事件历史中。不删除任何记录。
2026-10-10 在同一个 `TASK_SOURCE_ONLY` 事务内处理下列 20 个处于 IN_PROGRESS/VALIDATING 的任务：

- 被新框架替换的机制：ARCH-004、ARCH-004G2、DEVX-001、DEVX-014、DEVX-015、DEVX-016、DEVX-018、DEVX-023、
  GOV-006、GOV-007。
- 与日常运行/数据治理耦合：OPS-070、OPS-072、OPS-073、OPS-074、OPS-077、OPS-078、OPS-079、OPS-081、
  DATA-GOV-001、DATA-GOV-002。

不改状态但需在重构中重评估（PROPOSED 或研究线）：ARCH-004G5、ARCH-004H、DEVX-017（并入 P6 pi 就绪清单）、
DEVX-019、OPS-082（范围改为"无 checkout lease 的确定性调度"，是恢复日常运行的入口，见第 8 节并行轨道）、
TRADING-2560（composer 前瞻观察，时间敏感，重构期间需决定走最简路径继续采集或接受缺口）、TRADING-2564、
Atlas 相关任务、KNOWLEDGE-001、PLATFORM-UX-001。

## 7. 阶段计划

时间为粗估日历日（专注工作），P2 实测后校准。

| 阶段 | 内容 | 退出条件 | 粗估 |
|---|---|---|---|
| P0 登记与挂起 | 本文档、任务行、20 个任务挂起；一个 TASK_SOURCE_ONLY 事务 | 事务 COMPLETED，已提交，preflight PASS | 0.5 d |
| P1 保证映射与分诊（只读） | 旧机制 -> G1～G7 映射表；测试"留/删"名单（profile 加依赖图自动生成，仅例外人工裁决）；代码 LIVE/FROZEN/DEAD 名单；区域 C 路径清单 | owner 一次集中评审通过（DP1/DP2） | 1～2 d |
| P2 建新门并影子运行 | 本机 PR 套件与不变量套件、marker/路径分层、`ship`、`RunContext` 与台账最小版；3 个真实改动新旧对照；变异等价抽样 | PR 套件实测时长达标；影子对照一致；无需 owner 外部操作 | 3～5 d |
| P3 切换 | 重写 AGENTS.md（含重构期间旧 fence 不再强制）；旧发布链停用 | 新流程发布 1 个真实改动成功 | 1 d |
| P4 分批删除 | 见第 8 节第 5 条的删除块 | 每块新门绿加变异等价抽样通过 | 3～5 d |
| P5 重整 | 任务库、system_flow、runbook | 文档与行为一致 | 与 P4 并行 |
| P6 接入 pi | 移植 3～4 个 skill、权限配置、3 个真实任务试跑 | pi 就绪清单全部满足 | 2～3 d，P3 后可并行起步 |

pi 就绪清单：AGENTS.md 中性且 ≤150 行；有一条 `ship` 命令；存在统一的发布门（`ship` 本机新门，区域 C 要求 owner 决策记录）；agent 路径上没有 Windows 注册表或
租约依赖；任务库简单；日常调度确定。

## 8. 排程：最小化总开发时间

关键路径：P0 -> P1 -> P2 -> P3 -> max(P4, P5, P6)，粗估 8～12 个工作日。以下做法的目标是不让任何等待落在关键路径上。

1. **旧发布链次数最小化**：重构期间禁止启动新的旧链发布和 Full（也避免 CPU 争用）。目标旧链发布次数为 0（引导路线 B）
   或 1（引导路线 A，见第 9 节）。任务行/文档变更合并为少量 TASK_SOURCE_ONLY 事务：P0 一个事务，切换前最多再一个，
   切换后走新门。
2. **owner 决策集中、提前**：DP1（G1～G7 映射、真实链路测试留删、LIVE/FROZEN/DEAD 名单、区域 C 清单）与 DP2（引导路线）
   在 P1 结束时一次评审问完，避免在 P3 才发现阻塞。不开启分支保护后，新流程不需要
   任何 owner 外部操作（`ship` 只用已有的推送 main 权限）；切换提交也不要求 owner 审阅 diff（第 9 节）。删除批次不做逐批审批。
3. **并行流（写范围互不相交）**：S-A 新门（`.github/workflows`、`ship`、pytest 分层）；S-B 测试分诊（只读 profile
   与依赖图，产出清单）；S-C 代码使用分析（只读，产出清单）；S-D `RunContext` 与台账最小版；S-E 文档草拟
   （AGENTS.md、system_flow、runbook 草稿）。S-B/S-C 只读，可与 S-A 同时进行；是否启用并行会话或子 agent 由 owner 决定
   （成本较高，默认不启用）。
4. **先做 spike，早失败**：P2 第一天先验证四个不确定项：本机 16 核上 PR 套件的实测时长；marker/路径分层是否可行；
   "任务分支 -> 本机新门 -> 推送同一 SHA 到 main"在本仓库可行（含提交信息 `Gate:` 与 `Owner-Decision:` 行的检查）；
   `RunContext` 套入一个真实日常步骤。任一失败立即调整路线，
   不等到切换才发现。
5. **删除按自成一体的块大批量做**，每块一次提交、新门验证（约 10～30 min）：块 1 发布机制（`platform/architecture`、
   `scripts/architecture_arch005_*`、`config/architecture`、`registry` 授权与 shadow 目录及其测试）；块 2 退役候选族；
   块 3 过期测试、文档、生成视图；块 4 重复的兼容别名与死 CLI。块数少于旧流程的批次数，且不依赖 4 小时的 Full。
6. **能自动化的不手工**：测试分诊用 Full profile 与调用图脚本生成名单；变异等价复用 C3b harness；任务批量更新用
   S2 事务；删除用脚本按名单执行并跑新门。
7. **时间盒与停止条件**：每阶段有时间盒与退出条件，超时先缩范围而不是延期；P2 末做一次校准检查点，更新后续估计。
8. **并行轨道：恢复日常运行**：不挂在关键路径上。查明日报停在 7 月的原因，用最简调度恢复；OPS-082 的范围随新设计重定义。
9. **宿主约束**：长任务期间屏幕不休眠（owner 设置）；大批量文件操作错开 Defender 扫描峰值；删除块执行时不并行跑重测试。

## 9. 引导（Bootstrap）问题与路线

问题：旧规则要求任何候选走旧 fence 加 Full（约 4 小时），而旧链会随删除一并消失；S3 发布命令的 scope policy 还把
AGENTS.md 列为禁止路径。现在不阻塞（P0～P2 在分支上完成，新门在分支上即可运行），P3 切换前需要选路线：

- **路线 A（旧链一次）**：把"新门基础 + AGENTS.md 修订"通过旧链发布一次（因 AGENTS.md 是禁止路径需走手工链，约 4～5 小时），
  之后所有改动走新门。
- **路线 B（一次性引导例外）**：owner 书面决定，切换提交以"新门通过"代替旧 fence（owner 后来也去掉了 diff 审阅，见下），普通推送到 main。

**已决定（2026-10-10，P1 文档评论，`owner_decision:GOV-008:2026-10-10:bootstrap_route_b_v1`）：路线 B。**
理由：路线 A 是花数小时满足一个已决定拆除的流程，且手工链最少演练。适用范围与条件（本文对该决定的操作化定义，
owner 可调整）：

- 仅适用于 GOV-008 的切换提交（含 AGENTS.md 改写与伴随的未发布任务行/文档提交），不构成对其它任务的先例；
- 替代条件：该提交的准确 SHA 已在任务分支上通过新门（本机新门运行并留证据；不要求 GitHub CI）；
- 动作仍是普通非强制推送到 main，不使用 PR；不放宽 AGENTS.md 中 force-push、历史改写、远端分叉修复的限制；
- **不再要求 owner 审阅 diff 摘要，也不保留原始 diff**（owner 2026-10-10 聊天回复：去掉这个检查，
  `owner_decision:GOV-008:2026-10-10:route_b_no_diff_review_v1`）。切换前我仍会发一条简短的变更说明作为通知，不作为门槛。
  回退办法：切换前给当时的 main 打 tag `legacy-governance-final`；切换若有问题，用普通提交 `git revert` 回退，不改写历史；
- 例外随 P3 完成而结束，之后所有改动走新门。

## 10. 风险与未验证项

- **不开启分支保护（owner 2026-10-10，单人独立开发）**：没有任何技术手段阻止一次未过门禁的推送，`ship` 是 agent 自己
  运行的门，不是独立验证者。接受理由：单人项目；旧机制同样由 agent 自己运行，DEVX-015 防的是本机进程篡改验证器，
  不是研究保证。剩余防线：`ship` 对区域 C 改动要求 `Owner-Decision:` 行、提交信息的 `Gate:` 记录便于事后抽查、
  可选的夜间复核。agent 能在同一提交中修改测试这一点，旧机制同样防不住（测试在候选内）。pi 核心只有
  read/write/edit/bash 四个工具，权限控制来自第三方扩展，且 bash 通用，因此 pi 迁移本身不会缓解该风险，
  它只提供便利层。
- 本机 PR 套件时长已实测（第 13 节）；GitHub 侧问题本阶段不考虑，如夜间复核要启用再回头。
- 约 8.8 节点小时的命名 DQ/composer 测试带真实链路性质，保留与否由第 5 节规则裁决。
- 本任务推翻 owner 既有决定：2026-09-23"DEVX-015 做完全部 106 项"、2026-09-26"Full 后不做局部重跑/DUAL_LANE 仅
  opt-in"、2026-10-05"不改正式 Full 合同"。已由 2026-10-10 决定取代；相关任务重构后重评估。
- 估算来自 Full profile 的文件名分类与目录粗分，P1 用真实依赖图校正。

## 11. 开放问题

1. 引导路线 A 或 B（DP2）：**已决定路线 B**（第 9 节）。
2. 区域 C 的路径清单（DP1）：**已决定（2026-10-10，P1 文档评论）接受**第 6 节提议清单（数据与 PIT、窗口与研究语义、
   评分/阈值/仓位、安全边界、验证者自身；`universe`/`watchlist`/`industry_chain` 等认知层输入不列入）。P2 的 `ship`
   据此检查区域 C 改动是否带 owner 决策记录。
3. TRADING-2560 前瞻采集在重构期间的处理（最简路径继续，或接受缺口）。
4. 已决定（2026-10-10，owner 在 P1 文档评论中回答）：退役日报链路中的 ETF dynamic v3 步骤
   （`etf_forward_*`、`dynamic_v3_rescue_schedule_observe`、`portfolio_candidate_tracking`）；
   现在不删除，P4 删除块 2 随代码一并移除；删除前须确认日报链路缩为"数据 -> DQ -> 评分 -> 报告"后仍通过新门。
5. 是否启用并行会话/子 agent 加速 P1/P2（成本换时间）：owner 未要求，默认不启用。
6. **Atlas 已决定退役**（2026-10-10，owner 回复：退役）。Atlas 的页面测试依赖本机未跟踪且绑定 commit 的页面，曾使 PR 套件
   带 1 个已知失败（第 13.1 节）。退役后：`atlas` 包 28 个模块 / 1.7 万行、33 个测试文件、`scripts/render_atlas_strategy_research_page.py`、
   35 个 config 与 31 个 docs 文件进入 P4 删除块（现在只从 PR 套件排除）；Atlas 相关任务（TRADING-2466～2525、PLATFORM-UX-001）
   随下一个任务行事务标记终止。src 里只有将被删除的 `validation_readiness` 引用 Atlas，其余无依赖。门禁已全绿（第 13.2 节）。

## 12. 进度记录

- 2026-10-10：登记。owner 同意方向并要求最小化开发时间；本文档、任务行与 20 个任务的挂起在同一
  `TASK_SOURCE_ONLY` 事务 `gov-008-register-task-20261010-v1` 内完成（acquire 119 s，每个任务更新约 50 s，
  与 P1 并行执行）。
- 2026-10-10：owner 决定不开启 GitHub 分支保护（项目为单人独立开发）。设计随之调整：`ship` 在本机运行新门并在提交信息
  记录 `Gate:` 行，区域 C 改动要求 `Owner-Decision:` 行；GitHub CI 至多作可选夜间复核；P2 无需 owner 外部操作；发布耗时
  估算从"CI 约 30 min"改为"本机约 10 min"。任务行 next_owner 文字里的"GitHub 分支保护与推送权限设置"已过时，随下一个
  任务行事务更正。
- 2026-10-10：P1 草稿完成（仓库外，Claude Docs，供 owner 逐条批注）：
  https://claude.ai/code/artifact/ef0c8312-2709-4b2a-8f03-fa5b7822dd69 。内容：G1～G7、18 个机制的映射表（含"你的裁决"
  下拉）、测试分诊（Full 节点时间：发布机制 62.7%、研究采集 25.2%、其余产品测试 12.1%）、代码使用分析（上文）、
  区域 C 路径清单提议、5 个待裁决问题。等待 owner 评审（DP1/DP2）。分析脚本在会话临时目录，结果已固化在上述文档，
  P2 前以仓库内脚本重新生成名单。

## 13. P2 spike 结果（2026-10-10，代码在 `tools/gov008/`、`config/gov008_ship.yaml`、`core/provenance.py`）

**S1 本机 PR 套件时长（关键数字）**：保留的产品测试 971 个文件、8,882 个节点（≥30 s 的 28 个节点划入夜间），
16 个 worker、`--dist loadfile`，实测 **冷缓存 7m40s、热缓存 8m24s**（两次的差异在运行噪声内），8,877 通过、
3 失败、2 跳过。理论下界 6.5 min（16 核理想并行），最长单文件 4.1 min。

- 第一次测量跑了 25 分钟以上仍未结束、CPU 利用率只有约 19%。用 `faulthandler` 定时栈转储定位到根因：worker 停在
  收集阶段的 `_pytest/main.py` `samefile_nofollow` -> `lstat`。**我给 pytest 传了 972 个文件参数**，pytest 9 在
  Windows 上对每个参数逐个比较目录项，复杂度是"参数数 × 目录项数"，不是测试慢，也不是系统限速（单线程基准
  与 WMI 脱离启动速度一致，16 路并行基准全速运行，worker 无网络连接，40 文件单进程抽样与历史耗时相符）。
  修正为只传 `tests` 目录加按文件集合过滤的插件 `tools/gov008/pr_filter.py`（线性）。
- 教训：`-p` 写在 `@argsfile` 里不生效，必须直接放命令行；pytest 输出重定向到文件时需 `PYTHONUNBUFFERED=1` 才能看到进度；
  xdist 起 2 个 worker 的固定开销约 12 s。
- 3 个失败全是旧治理的哈希钉被本任务 P0 的任务登记改动连带打破：2 个断言 `docs/task_register.md` 的权威哈希与现状不符
  （`test_trading2452_architecture_contract.py`，分诊规则已补进 M5 随机制删除），1 个是 Atlas 页面相对任务索引过期。
  另有 128 个测试文件引用 `docs/task_register.md` 或任务索引。这是 DEVX-016 记录过的"无关改动让测试失败"的新鲜实例，
  支持删除生成视图与哈希钉。

**S2 分层**：不需要分层清单。P4 删除后，PR 套件 = `pytest tests -m "not slow"`，夜间 = 全部；`slow` 标记只需打在
28 个 ≥30 s 的节点上（0.52 节点小时），区域 C 不变量套件放 `tests/invariants/`。迁移期用 `pr_filter.py`。

**S3 `ship`**：`--dry-run` 在真实仓库跑通（识别 2 个区域 C 路径、找到 `Owner-Decision` 行、尊重未关联脏文件豁免、选出
门禁命令）；17 个单元测试用临时 git 仓库覆盖区域分类、`Owner-Decision` 缺失被拒、脏树被拒、main 非祖先被拒、
在 main 上被拒、`Gate:` 行记录被测 tree、compare-and-swap 快进在 main 被并发移动时失败。**未对真实 main 执行任何移动或推送。**

**S4 运行溯源**：现有运行清单（`report_traceability._run_manifest`）只记配置路径。新增 `core/provenance.py`，在清单加
`provenance` 块：git commit、分支、`code_modified`（src/config/scripts/tools 是否被改）、每个配置文件的内容 sha256，
每次约 0.12 s；4 个单元测试加 242 个相关现有测试通过。G2 其余部分（窗口、DQ 报告引用）清单里已有。研究线各自的清单
（QQQ options、权重校准等）后续按同样方式接入。

### 13.1 P2 剩余项完成情况（2026-10-10 晚）

- **`slow` 标记（S2 收尾）**：28 个 ≥30 s 的节点已在代码里打上 `@pytest.mark.slow`（24 个测试文件，其中 6 个是从
  `tests/layer1_meta_policy_readiness_cases.py` 重导出的，标记打在定义处），`pyproject.toml` 已注册 `slow`。`-m slow`
  收集 28 个节点，`-m "not slow"` 收集 8,654 个节点，单进程收集 24 s。PR/夜间的边界从此是代码里的标记，不再是名单。
- **过滤插件改为"排除名单"**：`tools/gov008/pr_filter.py` 先前按"保留名单"收集，会把分诊之后**新增**的测试文件（包括刚写的
  `tests/invariants`）静默漏掉，这是门禁里的洞。现改为 `docs/requirements/GOV-008_lists/pr_exclude.txt`（388 个将被删除的
  文件），新测试默认被收集。P4 删除后该名单与插件一起消失，PR 套件就是 `pytest tests -m "not slow"`。
- **不变量套件 `tests/invariants/`（区域 C）**，3 个文件 9 个测试，每个都有反例对照：
  - `test_dq_gate_blocks_downstream.py`（G1）：有效缓存通过（对照）；重复主键、非正收盘价、缺必需 ticker 三种缺陷下
    `build-features` 必须失败、不写特征文件、质量报告写出且含 FAIL。
  - `test_features_do_not_look_ahead.py`（G2）：把 as-of 之后的数据全部替换成极端值，特征不得变化；一个故意偷看最后一行的
    构建器必须被同一检查抓住（检查本身有效力的对照）。
  - `test_run_manifest_records_provenance.py`（G2）：清单必须含 commit、`code_modified`、配置内容哈希，配置改动哈希必须变。
  - 变异证据（见 `tests/invariants/README.md`）：拆掉数据质量门禁使 `missing_required_ticker`、`non_positive_close` 变红，
    `duplicate_price_key` 不变红（另有独立检查也拦重复主键，所以该用例不是门禁本身的证据）；让特征忽略 as-of 使
    look-ahead 测试变红；丢弃 provenance 使三个清单测试变红。
  - 已有的同类守门测试（数据质量、PIT、窗口、阈值治理、生产边界静态扫描、调度安全）不重复，列在 README 的索引里，
    因为它们本来就在 PR 套件内。
- **研究线运行清单接入 provenance：本轮不做。** 各研究线有自己的清单且部分带冻结或外部契约的 schema（例如 QQQ options 的
  QC 适配器清单是对平台的导出契约），加字段可能破坏哈希绑定的历史证据。daily 与 backtest 的追踪包已覆盖；研究线逐条
  评估后再接入，不在"本次重构不动研究线需求"的范围内擅自改。

- **最终配置的干净计时**：`pytest tests -n 16 --dist loadfile -m "not slow" -p tools.gov008.pr_filter`，8,684 个节点（含 9 个
  不变量测试、17 个 `ship` 测试、4 个 provenance 测试）**8m46s**（526 s），8,681 通过、1 失败、2 跳过。三次计时
  7m40s、8m24s、8m46s，PR 套件按约 8.5 分钟计。
- **唯一的失败，以及为什么不能悄悄绕过**：`tests/atlas/test_historical_projection_review.py::
  test_local_canonical_page_uses_current_successor_identity_when_available`。它读取本机**未跟踪**的 Atlas 页面
  （`outputs/atlas/...`），要求页面与当前任务索引一致且绑定当前 commit。页面一旦存在就会在任何新提交后过期，包括
  `ship` 追加 `Gate:` 行的那次修改本身，所以这个测试在本机永远稳定不了；干净克隆里页面不存在则跳过。这是"测试依赖本机
  未跟踪状态"的又一个实例。处理属于 Atlas 去留（P1 文档第 3 节最后一行）：若 Atlas 退役，测试随之删除；若保留，需要把
  "页面过期"改成跳过并提示重新生成。**在 owner 决定前不改测试、不加豁免**，门禁带 1 个已知失败。（已解决：owner 决定 Atlas 退役，见第 11 节第 6 条与第 13.2 节。）

### 13.2 Atlas 退役后的门禁与未归属代码（2026-10-10 晚）

- **门禁全绿**：Atlas 的 30 个测试文件加入 `pr_exclude.txt`（共 418 个文件）后，PR 套件 8,453 个节点 8m54s（534 s），
  8,451 通过、2 跳过、0 失败。四次计时 7m40s～8m54s，PR 套件按约 8.5 分钟计。
- **确认可移除的代码**：开发机制 6.1 万 + ETF 30.3 万 + Atlas 1.7 万 = **38.2 万行（28.9%）**，前提仍是两处解耦
  （第 5 节）。分类脚本中 Atlas 不再参与五条线的关键字种子（`atlas.qqq_options_projection` 因名字含 `qqq_options`
  曾被误判为五条线依赖，实际没有任何 Atlas 之外的模块导入它）。
- **未归属 185 个模块 / 20.4 万行，分为四类**（"未归属"只表示不在调度、不在五条线、不在 ETF/Atlas/机制里，不表示无用；
  例如 `cli_commands.market_features` 就是 `build-features` 命令）：

| 类 | 模块 | 代码行 | 直接引用它们的测试 | 最后修改 | 建议 |
|---|---|---|---|---|---|
| A 休眠的一次性研究/复盘模块（`high_intensity_risk_cap_*` 31 个、`dynamic_target_*`、`scope_narrowed_*`、`refined_*`、`liquidity_rates_*`、`ai_leadership_*`、`exposure_cap_*`、`regenerated_*` 等） | 114 | 13.4 万 | 539 个文件 / 3,269 节点 / 0.89 节点小时 | 2026-06～07，其中 `high_intensity_risk_cap_*` 的 31 个文件全部改于 7 月 4～5 日 | 冻结：代码移出 main（tag 可恢复），证据文件保留 |
| B `trading_engine.*`（paper trading 引擎与 broker 适配器） | 35 | 3.4 万 | 32 个文件 / 541 节点 / 0.01 节点小时 | 2026-05～07 | 待 owner：是否计划使用 paper trading；区域 C 含其 broker 路径 |
| C `data.*`（数据基础治理：访问控制、持久性、消费者迁移等） | 9 | 1.0 万 | 9 个文件 / 128 节点 | 2026-07～09 | 默认保留，随 DATA-GOV-001/002 重估 |
| D `cli_commands.*` 与 `research_framework.*`（`build-features`、`system`、`watchlist`、`industry-chain`、`parameters`、`trade_review`、`trace` 等命令；研究插件） | 27 | 2.6 万 | 92 个文件 / 426 节点 / 0.14 节点小时 | 2026-06～09 | 默认保留（属认知层与手动命令）；请 owner 圈出不再运行的命令 |

  A 类的价格标签：它们的测试约占 0.89 节点小时，冻结后 PR 套件粗估缩短 2～3 分钟（估算，删除后实测）。B、C、D 的测试几乎
  不占时间，保留的代价主要是维护复杂度而不是验证时间。
- **AGENTS.md 重写草稿**：`docs/requirements/GOV-008_AGENTS_md_v2_DRAFT.md`，130 行（现行 516 行），harness 中立。保留研究窗口、
  数据源纪律、数据质量门禁、阈值治理、中文输出、外部动作分级、no silent workarounds；新增 `ship` 发布流程、区域与
  `Owner-Decision:`/`Gate:` 行、运行溯源；删除治理开发模式与 preflight、fence 纪律、DUAL_LANE/base-drift、未关联脏文件审计命令、
  token 级证据准入记录。过渡条款标注"(until P4)/(until P5)"：任务行仍用现有 `TASK_SOURCE_ONLY` 事务直到 P5。草稿**不生效**，
  现行 AGENTS.md 未改动。

### 13.3 owner 对未归属代码与切换流程的决定（2026-10-10 晚，聊天回复）

- **B 类 `trading_engine` 保留**（owner：也是研究基础设施，用于回测）。35 个模块 / 3.4 万行，分类 `KEEP_TRADING_ENGINE`，其 broker
  与 execution 路径仍在区域 C。**C 类 `data.*` 保留**（默认，随 DATA-GOV-001/002 重估）。
- **D 类冻结，判据是"没有被实际研究模块逻辑引用"**（owner 从未手动调用这些命令）。核查结果：27 个模块没有任何被日常链路、
  五条线或其他调度的逻辑导入，只被 `cli.py`、`cli_direct.py` 注册引用；6 个研究插件只出现在各自的实验 YAML 与
  `config/report_registry.yaml`（目录），没有被研究逻辑加载。结论：26 个模块 / 2.55 万行冻结。**唯一例外**
  `cli_commands.market_features`（`build-features`，263 行）保留，因为 `tests/invariants` 用它证明数据质量门禁会拦截下游。
  点名一项供 owner 单独撤回：`research_framework.plugins.leveraged_exposure_instrument_evaluation`（杠杆标的评估，
  主题离 TQQQ 主窗口研究最近，1,184 行），目前随 D 类冻结。
- **A 类冻结**（owner 无异议）：92 个模块 / 12.9 万行（`FREEZE_DORMANT`）。另有 18 个不足 400 行的不可达小模块（0.28 万行，含
  `watchlist`、`industry_chain`、`explain`、`error_taxonomy`、`features.technical`）归 `REVIEW_SMALL_UNREACHABLE`，默认保留，
  P4 逐个复核（静态分析看不到动态导入，且收益可忽略）。
- **合计**：冻结或退役 **537,378 行 = 40.6%**，保留 785,308 行（59.4%）。分项：开发机制 6.2 万（含两个验证调度模块）、
  ETF 30.3 万、Atlas 1.7 万、休眠研究 12.9 万、CLI 与框架 2.6 万。
- **解耦工作量**（`frozen_boundary_worklist.csv`）：保留代码里仍导入被冻结模块的文件 18 个 / 4.7 万行 = 此前的 16 个（机制 14、
  ETF 2）加 2 个小残留（`high_intensity_risk_cap_guardrail_closure_common`、`portfolio_decision` 包）。**没有任何保留文件导入
  A、D 类冻结模块**，所以这两类只需删除并去掉命令注册。`cli.py` 与 `cli_direct.py` 需去掉 25 处被冻结命令的注册
  （`frozen_registration_edits.csv`），`config/report_registry.yaml` 需修剪对应报告条目。
- **A 类与测试**：直接导入 A 类模块的测试有 539 个文件（约 0.89 节点小时）。P4 的做法是先删模块再收集，凡因 `ImportError` 无法导入
  的测试文件随之删除，运行期才导入的由跑套件时的 `ModuleNotFoundError` 暴露；不靠名字猜。
- **切换流程**：owner 决定路线 B 去掉 diff 审阅检查，也不需要保留原始 diff（第 9 节已改）。

## 14. 解耦（P3 前置）：设计、结果与剩余项（2026-10-10 晚）

目标：保留的代码不再导入将被删除的模块。依据是 `tools/gov008/code_usage.py` 的"边界清单"
（`docs/requirements/GOV-008_lists/frozen_boundary_worklist.csv`）。起点 18 个保留文件（4.7 万行）仍导入被冻结的模块。

**解耦分三层，难度差别很大：**

| 层 | 内容 | 状态 |
|---|---|---|
| L1 路径函数 | 4 个报告与 `feedback` CLI 只用 `canonical_task_register_view_path`（返回任务登记视图的文件路径，但原实现要校验整个规范登记库）。新建中立模块 `core/task_register_paths.py`，5 个调用点改过去；P5 替换任务登记时只改这一处 | 完成，28 个相关测试通过 |
| L2 功能移除 | 移除只为 Codex 自动化与发布晋升机制服务的功能，以及耦合已退役 ETF 候选链的报告命令 | 完成，见下 |
| L3 采集协议里的 lease | 预期采集（prospective capture）的 5 个文件把 S4D lease 写进了证据格式 | **待 owner 决定** |

**L2 的具体改动与可见的行为变化：**

- `cli_commands/ops.py` −537 行：移除 7 个命令（`scheduler-checkout-preflight`、`release-candidate`、`release-canary`、
  `scheduler-observe-codex`、`runtime-git-exclusions-install`、`release-promote`、`deployment-acceptance`）和 `daily-run`
  外面的 checkout guard 装饰器。**`daily-run` 从此不持有 checkout guard、不做调度器 checkout 预检、不再强制声明执行模式**；
  `--manual-execution` 选项保留（旧调用不报错）但已无作用。`ops_release_promotion.py`（3.3k 行）与
  `ops_scheduler_checkout.py`（1.1k 行）因此不再被引用，随冻结代码删除。
- `cli_commands/reports.py` −3,743 行：移除 62 个命令，均属那条已退役 ETF 候选链的生命周期报告——`next-candidate-*` 及其
  `validate-`、`candidate-v2-*` 及其 `validate-`、`next-research-cycle-intake`、`return-to-research-reset`、
  `executable-research-evidence-gap-ledger`、`executable-binding-safety-audit`、各类 `*-weakness-attribution`、
  `*-blocker-drilldown`、`backfill-partial-root-cause-repair-plan`、`candidate-redesign-hypothesis-v2`；
  以及开发流程遥测 `ensure-workflow-health` / `validate-workflow-health` 的注册。用 AST 精确定位，ruff 未定义名检查兜底，
  只清理出一轮悬空辅助函数（3 个）。`aits` 与 `cli_direct` 导入正常。
- Reader Brief 通过**报告索引**读取这些报告而不导入模块，缺失时显示"artifact missing"，不会报错；对应栏目与
  `config/report_registry.yaml` 条目、`config/scheduled_tasks.yaml` 中的周任务（`weekly_workflow_health_review`、
  `weekly_etf_forward_review`、`weekly_dynamic_v3_rescue_*`）、`docs/artifact_catalog.md` 留到 P4/P5 统一修剪，避免现在改一半。
- 两个保留的 `ops` 测试原来只是把 guard 打桩绕过，打桩行已删除；`test_ops_scheduler_checkout.py` 测的是被移除功能，随冻结代码删除。

**分类器的修正**（均为暴露出的误判）：根命令注册模块 `cli.py`、`cli_direct.py` 归 `KEEP_CLI_ROOT`；`platform.validation_scheduling`
与 `validation_trigger_provenance` 归开发机制；`high_intensity_risk_cap_*` 的小辅助随家族冻结；`portfolio_decision` 小包整体保留复核；
测试分诊新增 `DELETE_WITH_FROZEN_CODE`（315 个文件 / 1,437 节点 / 0.26 节点小时，导入了休眠或冻结 CLI 模块的测试）。

**结果**：边界从 18 个文件降到 **5 个**（正好是 L3 的采集文件）。冻结或退役 56.5 万行（42.8%），保留 75.4 万行。
解耦后 PR 套件 **7,019 通过、2 跳过、0 失败，7m20s**，排除名单 733 个文件。

**L3 的事实**（供 owner 决定）：5 个文件——`composer_prospective_capture`、`prospective_capture_execution`、
`prospective_event_time_evidence`、`research_outcome_access`、`data/named_quality_dispatch`（共约 6.4k 行，约 350 处 lease 引用）。
lease 在这里做三件事：(1) 采集前后重新核验运行的是精确的候选 commit；(2) 采集用到的路径被独占持有；(3) 租约保持 ACTIVE
（没过期没释放）。这三项被写进证据：尝试记录与确认记录含 `source_lease_id`，并校验"记录者的原始 S4D lease 处于 ACTIVE"。
**现有采集证据全部在 `synthetic/` 下，真实采集为零**（`operations_runbook.md` 同样声明），所以没有真实证据需要保持兼容。
相关测试：R1 依赖租约的 12 个文件 / 1,063 节点 / 8.17 节点小时，其中的真实链路变体已按 owner 决定排除。
选项与建议见 owner 决策记录。

### 14.1 L3 决定：采集协议 v3（owner 选 A，2026-10-10 深夜）

**决定** `owner_decision:GOV-008:2026-10-10:capture_protocol_v3`：用"运行来源（provenance）+ 本机单次持有锁（capture hold）"
替换预期采集 5 个文件里的 S4D lease，使发布机制整体可删。放弃的方案：B（为采集保留约 2 万行精简 lease 核心）、
以及"保留 lease 字段却没有真实 lease"的空壳（证据会说谎）。理由：真实采集为零，没有需要保持兼容的真实证据；
新协议不再依赖开发期的发布机制。

**v3 保证了什么、没保证什么（须在证据格式里如实表达）：**

| 项 | S4D lease（旧） | capture hold（新） |
|---|---|---|
| 精确候选 commit | 重放租约库核对 intent/base commit | 直接读 git：HEAD == 候选 commit，且 `src/config/scripts/tools` 无未提交修改（`core/provenance.py` 同一实现） |
| 写入路径独占 | 租约库里的路径资源声明 | 本机 `active/` 目录下每个 hold 一份记录，获取时在短互斥（O_EXCL）内检查路径重叠，存活的重叠 hold → 拒绝 |
| 持有期有效 | 租约 ACTIVE 且未过期 | hold 记录含 `acquired_at/expires_at`，每次 recheck 校验未过期、未释放、持有进程仍是本进程 |
| 独立权威 | 租约库是第二份权威 | **没有**：hold 是本机自证，证据是"本机父进程自述"，不是签名；这一点与旧 `verify_retained_named_capture_proof` 文档里"本地父证明"的措辞一致 |

**步骤（每步结束跑 ruff + 相关测试，S5 才跑整套 PR 门禁）：**

| 步骤 | 内容 | 验收 |
|---|---|---|
| S1 | 新模块 `src/ai_trading_system/data/capture_hold.py`：`acquire/restore/recheck/release/verify_retained` + 单元测试（获取、重叠、过期、commit 漂移、代码被改、进程已死的陈旧 hold、保留证明篡改） | 新测试全过；不导入 `platform.*` |
| S2 | 端口 `data/named_quality_dispatch.py`、`data/named_quality_execution.py` 与 3 个 contracts：`source_lease_id`→`source_hold_id`（`hold-<20hex>`），`active_lease`→`active_hold`，schema 版本号 +1 | 这两个文件与 contracts 不再引用 lease |
| S3 | 端口 `prospective_event_time_evidence.py`（`lease_event/lease_intent`→`hold_record/hold_check`）、`prospective_capture_execution.py`、`composer_prospective_capture.py`、`research_outcome_access.py`（`hold_lease_arbiter`→hold 互斥）；新策略文件 v3 并更新 sha256 钉住值 | 5 个文件 `rg "lease|platform\.architecture"` 为空 |
| S4 | 重写测试夹具 `tests/named_data_quality_support.py` 用 tmp git 仓库 + 真 hold；删除只测 lease 绑定的测试（`test_devx022_lease_event_externalization.py` 等）；行为契约测试保留并出 R1 排除名单 | 这批测试回到 PR 门禁 |
| S5 | 重算边界清单（目标：保留代码对冻结模块的导入 = 0）；PR 门禁；提交 | 门禁绿；`frozen_boundary_worklist.csv` 为空 |

**旧证据**：`synthetic/` 下的 v1/v2 证据按旧协议生成，v3 验证器**不**验证它们；它们保留原样，仅作历史。

### 14.2 L3 结果（2026-10-10 深夜，S1–S5 完成）

| 步骤 | 结果 |
|---|---|
| S1 | `data/capture_hold.py`（获取/恢复/复核/释放/按 id 释放/保留证明验证，hold 记录含 `git_common_dir`）+ `scripts/capture_hold.py`（acquire/release/status）；`tests/test_capture_hold.py` 26 个测试 |
| S2 | `named_quality_dispatch`（删掉 lease 的 5 个函数，−453 行）、`named_quality_execution`、contracts、启动脚本改用 hold；`--source-lease-id` → `--source-hold-id`；相关 schema 版本 +1 |
| S3 | 事件时间证据（槽内 `hold_record.json`/`hold_check.json`，只认 v3 策略，删 v1 分支）、五候选采集（只留 v3 ACK）、Composer 采集、结果访问网关（检查点移到 `outputs/research/experiment_outcome_access_checkpoints_v1`，串行化用 hold 存储的 OS 文件锁）；新策略 `prospective_event_time_evidence_v3.yaml`、`prospective_capture_execution_v3.yaml`；源清单 v2：五候选 94→68 模块、Composer 110→84 模块（去掉 28 个发布机制模块，加入 `core.provenance`、`data.capture_hold`）；旧 v1/v2 策略与 v1 源清单删除 |
| S4 | 测试改用真 hold：dispatch、执行、bootstrap、事件时间、五候选、Composer、结果访问。按 owner 对 R1 的决定**删除**依赖发布事务/lease 环境的真实链路变体：`test_named_data_quality_actual_candidate.py`、`test_named_data_quality_candidate.py`、`test_named_simple_baseline_preview_candidate.py`、`named_data_quality_support.py` 的真实候选父进程、Composer 合约测试末尾的真实候选段；只测旧 V1 ACK 内部时钟算法与 v1 策略的测试删除；lease 恢复/保留证明测试由 `test_capture_hold.py` 覆盖 |
| S5 | 边界清单为空（保留代码对冻结模块的导入 = 0）；8 个采集测试文件回到 PR 门禁（排除名单 −11）；一个 40 s 节点标 `slow`；**PR 门禁 7,720 通过、3 跳过、0 失败，8m35s** |

**实现中发现并修正的问题：**结果访问网关在持锁期间要写仓库根下的账本，而 `exclusive_store_maintenance` 会把"根权限"
绑定到 hold 目录，导致 `ARTIFACT_ROOT_AUTHORITY_CONFLICT`。改为 hold 存储使用不绑定根权限的 OS 文件锁
（`hold_store_lock`），hold 的获取/释放与网关共用同一把锁。

**行为与覆盖的变化（需要知道的）：**

- 自动化测试里**不再有用真实仓库 + 真实数据跑命名 DQ 子进程的端到端测试**（被删的真实链路变体）。合成端到端测试
  仍覆盖父/子协议、时间证据、ACK 与重放。真实链路的第一次检验将是 TRADING-2560 的第一次真实采集
  （按运维手册：先 `capture_hold.py acquire`，再以 `--source-hold-id` 启动，最后 release）。
- hold 是本机自证：保留证明携带 hold 记录原文与检查时的 git 状态，但没有第二份权威可以交叉核对。
- 崩溃进程留下的 hold 在有效期内会挡住重叠路径的采集，用 `capture_hold.py release --hold-id` 清除；
  有效期上限 24 小时（`MAX_HOLD_SECONDS`，代码内注明理由）。
- `daily_input_capture.py` 自带的本地 "source lease" 与 S4D 无关，未改动。

**留给后续：**任务登记的状态更新并入下一次 TASK_SOURCE_ONLY 批量事务（与 Atlas/B/D 决定一起）；
P4 删除发布机制时同时删除 `config/architecture/arch_005_*` 与机制测试。

## 15. P3 切换（2026-10-10）

**切换内容**：新 AGENTS.md 生效（137 行，取代 516 行旧版；草稿文件删除）；`config/gov008_ship.yaml` 状态改为
`OWNER_APPROVED_ENFORCED`；`ship` 推送前 fetch 远端 main 并要求它是候选的祖先，推送后核对远端 SHA；切换前给当时的
main（`2d29e4e35`）打 tag `legacy-governance-final`，回退用普通 `git revert`。按路线 B，切换提交在本机新门通过后普通推送，不走旧 fence/Full。

**切换时发现的阻塞与决定**：TRADING-2540（QQQ options 研究线）的预注册 `strategy_growth_action_value_preregistration_v1.yaml`
与阈值决策包 `strategy_growth_action_value_threshold_decision_pack_v1.yaml` 把 AGENTS.md 的精确字节哈希钉为"工程规则权威"，
加载时与当前文件逐字节比对；改写 AGENTS.md 会让这条研究线的加载器失败（8 月 24 日旧流程改 AGENTS.md 时就重钉过一次）。

决定 `owner_decision:GOV-008:2026-10-10:historical_rules_authority`（owner 选项 1，聊天）：研究记录一个字节都不改；
新模块 `core/rules_authority.py` 只对 `AGENTS.md` 这一个权威文件放宽为"当前字节等于钉住值，或该记录最后一次提交时的
AGENTS.md 字节等于钉住值"；预注册加载器仍对**当前** AGENTS.md 检查要求的规则原文（新 AGENTS.md 保留了
`default research and backtest start: 2021-02-22;` 等原句）。其余所有权威文件保持逐字节严格比对。加载结果里的观测哈希
仍是钉住值，下游哈希不变。测试：`tests/test_rules_authority.py`（5 个，含"记录与规则同时改则必须重钉"），三个研究测试的
无 git 夹具改为写入登记时的 AGENTS.md 字节。

**风险分级测试**：`test_risk_tiered_external_action_evidence_admission.py` 原来断言旧 AGENTS.md 的 DEVX-008 章节标题；
新 AGENTS.md 保留 R0–R3 分级但不再有该标题，测试改为断言 R0–R3 仍在。

## 16. P5a 任务库（2026-10-10）

P3 已发布：`af6b76519`（门禁 7,727 通过、0 失败；`Gate:` 记录在提交信息里），main = origin/main。

删除发布机制会连带删掉更新任务登记的唯一工具（`scripts/architecture_arch005_task_source.py` 需要发布事务），
所以任务库先于 P4 删除块 1 替换。

**新任务库**：每个任务一个文件 `tasks/<ID>.yaml`（schema `task.v1`，字段即视图的 8 列：id、area、priority、status、
next_owner、next_step、acceptance、notes）。`tools/tasks.py` 提供 list / show / new / set / render / check；
`docs/task_register.md`（未完成）与 `docs/task_register_completed.md`（DONE/DROPPED）由 render 生成，格式与报告读取的
8 列表格相同，表格后的说明文字移到 `tasks/view_templates/`。未完成视图按优先级再按 ID 排序，完成视图按 ID 排序。
没有事件链；审计记录就是 git 历史。`tests/test_tasks_register.py` 校验每个任务文件并在视图过期时失败。

**迁移**：从两个视图的任务表逐行转换（1081 个任务：316 未完成、765 完成）；多出的 `|` 留在 notes 里，渲染结果与原行
逐字节相同。重新渲染的视图与迁移前相比，只有首行横幅与行序不同（行集合完全一致，已在 scratchpad 模拟验证）。
删除：`registry/development_tasks` 的 1080 个事件碎片、两个 shadow 目录、索引与两个视图模板。**保留**
`registry/development_tasks/2f/2f96dc…yaml`：QQQ options 研究线 TRADING-2548 的配对比较合约把它的精确字节钉为
"终态结果准入事件"证据（钉住值核对仍成立），按"不改研究线"原则原样保留，并在该目录 README 说明。

**首批任务更新（在新库里完成，不再需要 TASK_SOURCE_ONLY 事务）**：26 个 Atlas 相关的未完成任务（TRADING-2466～2536
中的 Atlas 页面任务与 PLATFORM-UX-001）标为 DROPPED，依据 Atlas 退役决定（第 11 节第 6 条；引用名
`owner_decision:GOV-008:2026-10-10:atlas_retired`）；GOV-008 任务行更新下一步与责任方（去掉已不需要的 GitHub 分支保护事项）。

## 17. P4 删除块 1：发布机制（2026-10-10，与 P5a 同一次发布）

**为什么和 P5a 合并**：P5a 单独 ship 时门禁两次失败在同一个测试（`test_validation_failure_diagnostics.py` 的真实
16 进程运行，第二次在主机空闲时仍超时）。原因是旧验证运行器 `scripts/run_validation_tier.py` 的部分路径读取刚删除的
旧任务登记库。旧登记库与读取它的旧机制必须在同一次发布里一起删除，main 才保持一致；`ship` 两次都正确拒绝，main 未动。

**删除**：`src/ai_trading_system/platform/architecture/`（整个目录）与 `platform/validation_scheduling.py`、
`platform/validation_trigger_provenance.py`；31 个机制脚本（`scripts/architecture_*`、`run_validation_tier.py`、
`pytest_runtime_profile.py`、`refresh_partial_duration_profile.py`、`build_validation_parent_run_import.py`、
`governance_task_portfolio_normalization.py` 等）；`config/architecture/`；`registry/architecture_compatibility_authority`、
`registry/report_catalog_flow_authority`；`inputs/architecture/` 除 `arch_004e_*` 三个文件（日报的 engineering_closeout
仍读取）外的全部；`tools/codex_skills/`（两个绑定旧机制的 Codex 技能，P6 为 pi 重建）。导入了已删机制的冻结/退役代码同批删除：
Atlas 包、`scripts/render_atlas_strategy_research_page.py`、`config/atlas`、`ops_release_promotion.py`、
`ops_scheduler_checkout.py`、workflow health 遥测（`reports/workflow_health.py`、`reports/workflow_improvement.py`、
`cli_commands/workflow_health_reports.py`）。测试：机制测试 75 个文件，以及上述模块各自的测试与依赖方向门禁测试。

**修正的分诊误判**：`tools/gov008/test_triage.py` 原来只凭文件名（如 `wave`、`dependency`）把测试归入"机制"组，
有 21 个既不导入机制也不含机制标记的测试被排除在 PR 门禁外，其中包括 QQQ options 研究线的
`test_qqq_options_paired_comparison_wave_a.py` 与测保留平台模块的 `test_arch_004c_*`、`test_arch_004f*`、`test_arch_004g*`。
规则改为：文件名命中且确有机制导入或标记，才判为机制测试；其余按导入关系正常分类，因此这些测试回到 PR 门禁。

**产品测试的解耦**：8 个产品测试（数据源资格、数据基础、研究路线图、controlled strategy 辅助模块等）原来断言"自己登记在
旧验证分层 `TIER_SPECS` 里"，这只是旧机制的登记要求，删去这些断言与导入；测试本身的行为断言不变。

**GitHub CI**：原 `ci.yml` 在每次推送 main 时于 Ubuntu 调用已删的 `run_validation_tier.py`。按 owner 决定（GitHub Actions
至多可选检查、Linux 不考虑），改为手动触发的 ruff 检查；仓库已有的 lint 存量（`inputs/research` 下的 QuantConnect 算法文件、
src 里两处超长行）不作为门禁失败。

**首轮 PR 套件（删除后）发现的 17 个失败与处理**：都是刚回到门禁的 ARCH-004 架构棘轮测试（wave14/15 就绪度、F3 报告
清单、G4 架构碎片），读取已删除的 `config/architecture`。日报 owner 简报运行时只读 `config/reporting/reporting_architecture.yaml`，
不调用这些棘轮，日报链路不受影响。删除 `platform/reporting/g3_close_readiness.py` 与两个 wave 测试文件、F3/G4 测试中的棘轮函数；
"owner 简报固定 10 个板块（1 个原生、9 个来自旧载荷）"这条产品行为保留为测试内的明确常量。风险分级测试中对已退役 Atlas
页面的断言删除，其余文档断言不变。`platform/reporting/inventory.py` 里不再被调用的扫描/冻结比对函数留到后续清理。

## 18. P4 删除块 2：ETF 候选链与休眠研究代码（2026-10-10）

依据：ETF 每日步骤退役（第 11 节第 4 条）、A 类休眠代码冻结（13.3）、D 类 CLI/框架冻结（13.3，见下方修正）。

**删除**：`src/` 371 个文件 / 约 45.3 万行——`etf_portfolio/` 包（127 个文件，保留 `regime.py`）、`interfaces/`（144 个，ETF CLI
分层）、`cli_commands/etf_compat.py` 与 `etf_portfolio.py`、100 个休眠研究模块（`dynamic_target_baseline_*`、
`high_intensity_risk_cap_*`、`dynamic_v3_clean_selection_*`、`defensive_preservation_lane`、`return_seeking_diagnostic_lane`、
`equal_weight_proxy_data_fix` 等）。测试 643 个文件 / 约 11.2 万行：测已删模块的测试、45 个只被它们使用的测试辅助文件。
`tools/gov008/pr_filter.py`（PR 套件过渡插件）随排除名单清空一起删除，门禁命令不再带 `-p tools.gov008.pr_filter`。

**每日计划与调度**（行为变化）：`ops_daily.py` 去掉 7 步（`portfolio_candidate_tracking`、`portfolio_tracking_review`、
`portfolio_tracking_review_report`、`etf_forward_update/dashboard/watchlist`、`dynamic_v3_rescue_schedule_observe`）；
`config/scheduled_tasks.yaml` 去掉这 7 个每日任务与 14 个非每日任务（`weekly_etf_forward_review`、`weekly_dynamic_v3_rescue_*`、
`ad_hoc_dynamic_v3_rescue_*`）及对它们的依赖，登记任务 81 → 60（非每日 42 → 28）。其余每日步骤的顺序不变。Reader Brief 的
ETF forward 栏目通过报告索引读取，从此显示缺失、不报错；栏目本身与 `config/report_registry.yaml` 条目留到 P5 修剪。
`aits etf`、`regime`、`features`、`simulation`、`report`、`run`、`experiments` 命令组移除。

**作为证据保留（不是运行代码）**：

- `config/etf_portfolio/`（96 个文件）：被研究登记与阈值登记按路径引用，删除会让研究线加载失败。
- `src/ai_trading_system/etf_portfolio/regime.py`：QQQ options growth-action-value 生产者合约与 owner 冻结包按精确字节钉住它，
  同目录 README 注明"不得编辑或移动"。没有保留代码导入它。
- `cli_commands/portfolio.py`：属于 `trading_engine` 的 CLI（owner 决定 B 保留），其 track-candidate 等命令仍可手动运行，
  只是不再在每日计划里。

**D 类决定的前提修正（需要 owner 知道）**：13.3 记录 D 类"只被 `cli.py`/`cli_direct.py` 注册引用、没有被研究逻辑加载"，因此
整体冻结。删除后首轮 PR 套件 361 个失败，暴露这个前提不成立：静态导入分析只看"谁 import 谁"，而 `aits research ...` 等
命令组本身就是保留研究模块的调用入口，测试通过这些命令运行研究代码（另一部分失败来自上面两项证据文件）。处理：恢复 24 个非 ETF 的 D 类模块（18 个 `cli_commands` 模块与 6 个
`research_framework` 插件）及其 50 个测试；只把实现模块已被删除的命令从中剪掉（AST 定位 + ruff 未定义名检查兜底），
`research trends` 106 → 29 个命令、`research` 195 → 179 个、`scenarios` 2 → 0 个，测这些命令的 29 个 `research_trends` CLI 测试与
3 个注册测试随之删除。只有 ETF 的两个 CLI 模块真正删除。实际冻结的 D 类因此远小于 13.3 的 2.55 万行。
另有 4 个被分类器误判为可删、实际在每日链路上的模块恢复：`research_restart`（`register_research_restart_commands` 只保留
preflight 命令）、`historical_portable_source_archive`、`legacy_research_artifact_portable_lineage`、
`platform/artifacts/validation_session`。

**测试更新**：每日计划/调度的预期测试（`test_ops_daily`、`test_scheduled_tasks`、`test_arch_004f1_operations_control_plane`、
`test_cli_direct`）改为新的步骤序列与任务数；新增断言"调度里不再有 ETF/dynamic-v3 任务、每日命令不含 `etf`/`dynamic-v3-rescue`"。
占位符拦截测试原来借用已退役的 dynamic-v3 ad hoc 任务（其命令含 `{max_candidates}`、`<sweep_id>`），改为在保留的 SEC PIT ad hoc
任务上注入含占位符的模板，覆盖不变。`test_prospective_capture_execution_contract` 的"旧协议"用例只改 `schema_version`。

**分析清单**：`docs/requirements/GOV-008_lists/` 重新生成，排除名单为空，机制与 ETF 工作清单为 0。剩余分类：
`FREEZE_CLI_FRAMEWORK` 1 个（`cli_commands/portfolio`，按上文保留）、`REVIEW_SMALL_UNREACHABLE` 19 个小模块（默认保留，P5/P6 期间复核）。

**任务登记**：48 个仍未关闭的 ETF 候选链任务（area 以 `ETF` 开头：TRADING-062～350 中的 BASELINE_DONE/VALIDATING/DEFERRED 行）
标为 DROPPED，notes 追加"ETF 候选链退役、代码已删除、可从 git 历史取回"，下一步改为"无"。依据同上；与 Atlas 任务的处理方式
一致（第 16 节）。`OPS-078` 等名字里含 rescue 但不属于 ETF 链的任务不动。

**验证**：PR 套件（`pytest tests -n 16 --dist loadfile -m "not slow"`）7,926 通过、3 跳过、0 失败，11m05s（任务登记更新前的同一树；
ship 在最终树上重跑）。变异等价：被删测试只覆盖被删代码；对保留模块的测试只做了步骤列表与任务数的预期更新，未删除行为断言。

块 2 已发布：`2eb7579e8`（ship 门禁 7,926 通过、0 失败，10m42s），main = origin/main。

## 19. P5 文档重整（2026-10-10）

**约束（实测）**：多个保留的研究模块按文本核对 `docs/system_flow.md`（`REQUIRED_SYSTEM_FLOW_REFERENCES` 等）与
`docs/artifact_catalog.md`（`REQUIRED_CATALOG_REFERENCES`）中的条目，并读取 `config/report_registry.yaml` 中自己的报告条目；
运维手册只被检查"文件存在"。所以前两个文件不能删旧文字，只能在前面加当前说明、把旧文字保留为只读附录；
报告登记不删条目，用已有的 `visibility_policy: archived_optional` 标记退役（报告索引把 MISSING/STALE 显示为
`ARCHIVED_MISSING`/`ARCHIVED_STALE`，只做审计，不进警告）。

| 步骤 | 内容 | 验收 |
|---|---|---|
| D1 | `system_flow.md`：新增第 1～6 节当前流程（每日链路 mermaid 图、门禁与阻断、非每日任务、研究链路、开发与发布、已退役清单），原文整体降为第 7 节只读附录 | 研究模块的文本核对测试仍通过 |
| D2 | 运维手册重写为当前步骤（入口、每日链路、非每日任务人工派发、DQ 门禁、恢复、前瞻采集 hold 步骤、已删除机制）；旧全文移到 `docs/operations/operations_runbook_legacy_2026-10-10.md` | 新手册不再描述已删除的命令；旧文可查 |
| D3 | `artifact_catalog.md` 顶部加当前状态与退役 artifact 族说明，正文不动 | 同 D1 |
| D4 | 报告登记：命令已不存在的 507 个条目（ETF 链、已删的休眠研究命令、workflow health、退役 ETF 报告命令）标 `archived_optional`；均非每日必读 | 报告索引测试通过 |
| D5 | 调度：退役 `weekly_workflow_health_review`（调用块 1 已删除的 `aits reports ensure-workflow-health`）；登记 60 → 59，非每日 28 → 27 | 调度测试更新后通过；所有登记任务命令都能解析 |
| D6 | 删除代码已在解耦 L2 删除的 `config/operations/ops_release_promotion.yaml`、`ops_scheduler_checkout.yaml` | 无代码引用 |
| D7 | 任务：OPS-082 下一步去掉已删除机制的描述；新登记 Reader Brief ETF 栏目清理（约 1,700 行，独立代码改动，不放进 P5） | `tools/tasks.py check` |

**已知但不在 P5 处理**：`ops daily-run` 的终止恢复（OPS-071）仍要求 `AITS_OPS_DEPLOYMENT_RECEIPT`，而生成该 receipt 的
`deployment-acceptance` 已在 L2 删除——在 OPS-082 重新设计调度之前，恢复只能使用现存 receipt。运维手册如实写明。
其余未被代码引用的研究配置（约 50 个）被研究记录或文档引用，属于证据，不清理。

**P5 结果（D1～D7 完成）**：

- D1 `system_flow.md` 新增第 1～6 节（约 100 行，含每日链路与研究链路两张 mermaid 图），原 11,770 行降为第 7 节只读附录，
  一行未删。D3 `artifact_catalog.md` 顶部加"当前状态"一节与不再生成的产物族表（路径按本机实际目录核对）。
- D2 新运维手册 174 行（旧 579 行）：入口与调度现状、每日链路表、运行控制与恢复（含 deployment receipt 限制）、DQ 门禁、
  非每日任务人工派发、操作前检查、前瞻采集两节（原文）、已删除机制清单；旧全文移到
  `docs/operations/operations_runbook_legacy_2026-10-10.md`。`docs/runbook_daily_ops.md` 与
  `docs/runbooks/scheduled_task_orchestration.md` 顶部加说明，指向新手册。
- D4 报告登记 507 个条目标 `archived_optional`（482 个新增字段、25 个由 legacy/deprecated 改为 archived），文件头注释说明原因。
- D5 调度退役 `weekly_workflow_health_review`，策略版本升为 `scheduled_tasks_v7`，并在 `policy_metadata` 记录块 2 与本次退役
  （块 2 删除每日步骤时漏了版本号；运行控制的规格 id 对步骤列表做哈希，所以块 2 已自动产生新幂等键，没有误复用旧状态）。
  所有登记任务的命令现在都能解析到现存 CLI 命令或脚本。
- D6 删除 `ops_release_promotion.yaml`、`ops_scheduler_checkout.yaml`。D7 OPS-082 下一步与验收改写；新任务
  `GOV-008A_READER_BRIEF_ETF_SECTION_RETIREMENT`（P2）。
- 验证：调度/每日计划/控制面/CLI 测试 138 通过；研究策略目录与文档契约等 913 通过；docs freshness 与 research trends 41 通过；
  整套 PR 门禁由 ship 运行。

P5 已发布：`6dc531d22`（ship 门禁 7,926 通过、0 失败，10m21s），main = origin/main。

## 20. P6 接入 pi（2026-10-10）

**pi 的事实（官方文档 2026-10-10 查阅：github.com/badlogic/pi-mono `packages/coding-agent` 的 README、docs/skills.md、
docs/extensions.md、docs/configuration.md、docs/security.md）**：

- 包名 `@earendil-works/pi-coding-agent`；官方安装脚本（Windows：`powershell -c "irm https://pi.dev/install.ps1 | iex"`）或
  `npm install -g --ignore-scripts @earendil-works/pi-coding-agent`；需要 Node ≥ 22.19（本机 v24.15.0）。
- 登录：在 pi 里 `/login` 连接订阅或 API key，凭据在 `~/.pi/agent/auth.json`。
- 上下文：从工作目录及其上级读取 `AGENTS.md`（不需要信任项目）。项目级 `.pi/`（settings、extensions、skills）与
  `.agents/skills/` 只在**信任项目**后加载（`/trust`，记录在 `~/.pi/agent/trust.json`）。
- **pi 没有内置沙箱或权限系统**：工具按 pi 进程的操作系统权限运行；扩展可以在 `tool_call` 事件里拦截工具调用。

**仓库侧已完成**：

| 项 | 内容 |
|---|---|
| Skills | `.agents/skills/` 4 个 Agent Skills（pi、Codex、Claude Code 通用格式）：`ship-change`（改动到发布的步骤）、`task-records`（任务库与需求文档、owner 决定的记法）、`periodic-operations`（运维手册、唯一入口、DQ 门禁、恢复与派发）、`research-run`（研究窗口、预注册、运行溯源、结果准入、capture hold、证据文件不改）。取代 P4 块 1 删除的两个 Codex 技能 |
| 权限 | `.pi/extensions/aits-guard/`：拦截强推、直接推 main、`update-ref` main、rebase、`reset --hard`、历史改写、`git clean`、整体丢弃改动、`--no-verify`；拦截写入 `tree_clean_exclusions`、渲染出的任务视图、被字节钉住的研究证据；编辑 C 区路径前弹确认（无交互界面时直接拦截）。它是防误操作的护栏，不是安全边界 |
| 测试 | `guard.test.ts`（7 个 node 测试）；`tests/test_agent_harness_files.py` 在 PR 套件里运行它（本机无 Node ≥ 23.6 时跳过并说明），并校验每个 skill 的名称规则、描述长度、名称与目录一致 |
| 文档 | AGENTS.md 引言加一句指向 skills 与守卫（137 行）；`system_flow.md` 第 5 节同步 |

**pi 就绪清单（第 7 节）**：

| 条件 | 状态 |
|---|---|
| AGENTS.md 中性且 ≤150 行 | 满足（137 行，不点名任何 harness） |
| 一条 `ship` 命令、统一发布门、C 区要求 owner 决定 | 满足 |
| agent 路径上没有 Windows 注册表或租约依赖 | 满足（src/scripts/tools 无 `winreg` 引用；lease 字样只出现在分析工具 `tools/gov008/test_triage.py` 的分类关键词里） |
| 任务库简单 | 满足（`tasks/<ID>.yaml`） |
| skills 移植、权限配置 | 满足（上表） |
| 日常调度确定 | **未满足**：OPS-082（PROPOSED），需 owner 确认调度规则与停用 Codex automation |
| 3 个真实任务试跑 | **未开始**：需要 owner 安装 pi 并 `/login`、信任本项目 |

建议的 3 个试跑任务（由易到难）：(1) 只读：用 `periodic-operations` skill 跑 `aits ops daily-plan --fail-on-missing-env`
并写中文摘要；(2) A 区：按第 6 节为挂起任务写重评表（只改任务记录与文档，ship 不跑测试）；(3) B 区：GOV-008A（Reader Brief
ETF 栏目清理）走完整 ship 流程。每个试跑记录：是否按 skill 步骤执行、守卫是否误拦或漏拦、门禁结果、耗时。

P6 仓库侧已发布：`ba1746b63`（ship 门禁 7,932 通过、0 失败，10m04s），main = origin/main。

## 21. 收尾：第 6 节挂起任务的重评（2026-10-10）

**已关闭**（代码在 GOV-008 中删除，任务无法再推进；依据 `owner_decision:GOV-008:2026-10-10:research_first_refactor_v1`，
记录可随时改回）：DEVX-014、DEVX-015（含 GOV-007 时"做完 106 项验收"的要求）、DEVX-015A、DEVX-016、DEVX-018、
DEVX-019、DEVX-020、DEVX-021、DEVX-022、DEVX-023、GOV-007、OPS-077、OPS-081 → DROPPED；DEVX-017 → DONE
（AGENTS.md 中性、skills 在 `.agents/skills`；证据 harness 字段与 token 遥测随旧机制删除不再适用）。

**留给 owner 决定**（代码仍在，但原下一步依赖已删除的正式验证/release 流程，需重新定范围）：

| 任务 | 现状 | 建议 |
|---|---|---|
| OPS-070、OPS-072、OPS-073、OPS-074、OPS-079 | 产品行为已实现（capture DAG、传输重试、终止处置、留存政策证据消费、历史缺口恢复），等真实 daily 运营验收 | 改为 VALIDATING，验收条件改成"OPS-082 新调度下第一次 ordinary daily 全链"；OPS-073/079 的恢复路径需去掉 deployment receipt 依赖 |
| OPS-078 | Codex automation 的隔离 carrier 与同日 rescue 窗口 | 并入 OPS-082（新调度是否保留第二个 rescue 窗口由 owner 定） |
| OPS-082 | 确定性调度，PROPOSED | 下一个要做的运营任务；需 owner 确认用 Windows 计划任务并停用 Codex automation |
| DATA-GOV-002 | 按 consumer 能力划分的 strict DQ receipt | 保留 DEFERRED，排在 OPS-082 之后 |
| ARCH-004、ARCH-004G5、ARCH-004H | 架构 v2 迁移计划 | 需 owner 判断还要不要做；GOV-008 已删去其中与发布机制相关的部分 |
| GOV-006 | 未完成任务的优先级整理 | 新任务库下可简化为一次表格复核；建议保留，待 pi 试跑后用作 A 区试跑任务 |
| DEVX-001 | 临时工作区生命周期工具 | 规则已在 AGENTS.md；建议 DROPPED 或降为 P3 |
| KNOWLEDGE-001、TRADING-2560、TRADING-2564 | 研究/产品线 | 不受 GOV-008 影响，按各自需求文档推进 |

收尾已发布：`ba41ae637`（ship 门禁 7,932 通过、0 失败；第一次 ship 在推进 main 前 `git fetch` 遇到网络瞬断被拒绝，main 未动，重试成功）。

### 20.1 owner 决定：pi 由 owner 安装并登录（2026-10-10，聊天）

`owner_decision:GOV-008:2026-10-10:pi_install_by_owner`：owner 自己用官方脚本安装 pi，并在 pi 里完成 `/login` 与 `/trust`；
完成后由 agent 安排第 20 节的 3 个试跑并记录结果（试跑消耗 owner 的模型额度，owner 已知悉）。放弃的选项：agent 用 npm 安装；
暂不试跑、先做 OPS-082。

### 20.2 pi 试跑记录（2026-10-11，pi 1.1.0，模型 gpt-5.5，`pi -p --mode json` 非交互运行，事件流保存在会话临时目录）

| 试跑 | 内容 | 结果 | 工具调用 / 花费 / 耗时 |
|---|---|---|---|
| 冒烟 | 列出 skills；执行 `git push --dry-run -f origin HEAD:refs/heads/aits-guard-probe` | 4 个 skill 均可见；强推被守卫拦下（`blocked by AGENTS.md: force-push`） | 1 次 / $0.03 / <1 min |
| 1 只读 | 按 `periodic-operations` 做每日计划检查 | 先读 skill 与运维手册，自己找到 venv 的 `aits`，只运行 `ops daily-plan --fail-on-missing-env`；中文汇报（2026-10-09 交易日、27 步、环境变量齐全、无阻断），未改仓库、未下载数据 | 6 次 / $0.19 / 39 s |
| 2 A/B 区记录 | 按 `ship-change` + `task-records` 更新 OPS-082 需求文档与任务记录并发布 | 从 main 建分支、改文档、`tasks.py set/render/check`、只提交 3 个相关文件、dry-run 后按要求停下；审阅通过后执行 `ship`：门禁 7,932 通过、0 失败（12m22s），main = origin/main = `d79f88291`，删除任务分支。文档内容准确，没有替 owner 做决定 | 34 次 / $0.88 / 3.4 + 13.1 min |

**试跑发现的问题（已修）**：守卫原来只要命令里出现 `tree_clean_exclusions` 的路径就拦截，连 AGENTS.md 要求的排除写法
`git status -- . ':(exclude,literal)<路径>'` 也被拦，pi 只能改用 `git ls-files -m -o --exclude-standard` 绕开（行为本身无害，但违背
"用规定的写法"）。修正：先去掉 `:(exclude,literal)`、`:(exclude)`、`:!`、`:^` 这类排除 pathspec 里的该路径，剩下仍出现该路径的命令照样拦截；
新增 1 组允许用例（6 种写法）与 3 个仍须拦截的用例（含"排除写法 + 另一处直接引用"）。

### 20.3 试跑 3：GOV-008A Reader Brief ETF 栏目清理（B 区，pi 完成，2026-10-11）

| 轮次 | 内容 | 工具调用 / 花费 / 耗时 |
|---|---|---|
| 实施 | 读 skill 与任务记录，先把 GOV-008A 设为 IN_PROGRESS，从 main 建分支；用 AST 分析 `reader_brief.py`（2.9 万行）后删除栏目，跑相关测试与完整 PR 套件（7,930 通过），ruff 通过，dry-run 后停下 | 75 次（6 次报错均为自我修正中的语法错误、测试失败、未用导入）/ $4.99 / 23.3 min |
| 修正 1 | 审阅发现误删非 ETF 栏目 "Backtest / Shadow / Governance"；pi 按 main 原样恢复并加回归断言 | 14 次 / $1.96 |
| 修正 2 | 任务记录：pi 把状态设为 DONE（应为 BASELINE_DONE，`etf_operations_health` 仍保留），且整段替换了原 notes；改正 | 5 次 / $0.58 |
| 发布 | `ship`：门禁 7,930 通过、0 失败（613 s），main = origin/main = `ea560aa89` | $0.31 |

**审阅方法（以后同类改动照此做）**：用同一份真实输入（as_of 2026-07-20 的日报产物）分别在 main 与分支上生成 Reader Brief，
逐项比较：payload（删掉的 32 个字段全为 `etf_*`，其余 72 个逐字相同）、HTML 栏目（删 41 个、保留栏目顺序与内容不变）、
Reader Brief 质量 payload（相同）、owner 简报视图模型（固定时间戳后相同）。PR 套件全绿也没有发现误删的栏目——这类删除必须做
行为等价比较，不能只看测试。

**结果**：`reader_brief.py` 29,005 → 约 12,900 行；GOV-008A 为 BASELINE_DONE，遗留 `etf_operations_health`（owner 简报
operations_health 板块的来源键，改板块构成需 owner 审阅）。

### 20.4 P6 试跑结论

三个试跑全部按 skill 步骤走完"任务记录 → 分支 → 改动 → 测试 → dry-run → 审阅 → ship"，没有越权的 git 操作；守卫拦下了强推，
修正后的排除写法可正常使用。需要人工把关的是**判断类错误**（误删一个栏目、任务状态与备注处理），由审阅与行为等价比较抓到。
另一处小偏差：pi 一次用系统 Python 而不是项目 venv 运行临时脚本。花费合计约 $9（试跑 3 占 $7.8，主要是 2.9 万行文件的上下文）。
pi 就绪清单除"日常调度确定（OPS-082）"外全部满足。

### 20.5 pi 接入本地模型（owner 要求，2026-10-11，进行中）

owner 要求先试 pi + 本地模型以降低成本（聊天：可以先尝试将 pi 接入我们本地模型来跑），并给出接入步骤。本机服务
`LocalModelService`（`~/.local-model-service`，OpenAI 兼容接口 `http://127.0.0.1:8080/v1`，模型 Qwen3.8-27B Q4_K_M + DFlash2
推测解码，RTX 4090 24 GB，空闲 60 s 卸载）。已做：`service.json` 上下文 4096 → 16384（备份 `service.before-pi-16k.json`）；
pi `models.json`（provider `local-model-service`，`contextWindow` 16384，`maxTokens` 2048，temperature 0）与 `settings.json`
压缩参数（reserve 2048 / keepRecent 4096，备份 `settings.before-local-model.json`）。实测：服务只在启动时读配置，修改后加载仍为
`n_ctx_slot = 4096`，需要在 dashboard 重启服务。下一步：重启后验证 16k 生效，再用本地模型重跑试跑 1（只读）与一个小的记录类任务，
记录成功率、工具调用错误、耗时；16k 上下文装不下大文件（如 2.9 万行的 `reader_brief.py`），本地模型先只承担小范围任务。

### 20.6 本地模型试跑结果（2026-10-11，owner 在 dashboard 重启服务后）

重启后后端日志 `n_ctx_slot = 16384`，显存 23.5 / 24.5 GB（含 DFlash2 草稿模型）。仓库外冒烟测试：pi 调用 `read` 读文件并正确回答（3 s）。

| 试跑 | 本地 Qwen3.8-27B（16k） | gpt-5.5（同一提示词） |
|---|---|---|
| 1 只读：每日计划检查 | 成功，32 s，5 次工具调用，0 报错，$0；结论一致且更详细（引用了计划报告原文） | 成功，39 s，6 次，$0.19 |
| 2 记录：向 DEVX-024 追加结果并 ship | 成功：建分支、`show` 后追加 notes、render/check、只提交 2 个文件，dry-run 19 s；审阅后 ship 门禁 7,930 通过、0 失败（614 s），main = origin/main = `a01ea1eb4`，删分支；$0 | （试跑 2 为另一个记录任务：34 次调用，$0.88） |

**偏差与限制**：

- 试跑 1 跳过了 skill 要求的"先读运维手册"，并跑了一次不带排除 pathspec 的 `git status`（本次没有暴露内容；守卫只拦提到该路径的命令，拦不到这种情况）。试跑 2 在提示里写明后全程用了排除写法。
- 试跑 2 汇报里把 A/B 区说反了（`docs/**` 是 A 区、任务 yaml 是 B 区），实际改动无误。
- **上下文溢出**：`ship` 的门禁输出约 1.1 万 token，加上已有对话超过 16k，模型那一轮被截断（输出 1 token），pi 自动压缩（reason=overflow）后正确完成收尾。说明 16k 对长输出偏紧；压缩会丢细节。
- 2.9 万行级别的文件或需要大量阅读的任务装不下 16k，不适合本地模型。

**结论**：本地模型在 pi 下能可靠完成"指令明确、上下文小"的任务（只读检查、记录类改动、按 skill 走 ship），零花费、速度不慢于云端；
判断类与大范围代码改动仍用云端模型，改动由审阅（必要时做行为等价比较）把关。可选的后续：KV cache 量化或去掉草稿模型以换取 32k 上下文
（需实测显存与速度）；让 `ship` 的门禁输出更短，减少溢出。

### 20.7 本地模型换成 Strata + Qwen3.8-Flash-Next（owner 要求，2026-10-11）

owner 要求在本机模型服务里加入 Strata 模型供本项目使用，选官方 Qwen3.8-Flash-Next（不用去拒答版），用途为 pi 开发任务，并在跑通后
设为服务默认模型。配置都在仓库外：

- 引擎 [Strata](https://github.com/Niko1221/Strata)（`D:\Work\Strata`，提交 `61b3fb5`），权重 ISTA-DASLab GSQ-RCO IQ3_S（84 GB，
  `D:\Work\Strata-data`）+ MTP 草稿层；128k 上下文，INT8 KV，默认 `reasoning_effort=none`（请求可自行开启思考）。
  选 IQ3_S 的依据：Strata 仓库里 4090 的社区测试（143k 上下文读提示 2,000–3,000 tok/s、生成 77–97 tok/s、长文检索 6/6）。
- 本机模型服务（`D:\Work\LocalModelService`）新增 Strata 后端，仍由服务统一排队、校验身份和空闲卸载，接口不变
  （`http://127.0.0.1:8080/v1`），模型 id `qwen3.8-flash-next-iq3_s`。空闲卸载 60 s → 900 s，单次推理超时 90 s → 600 s。
  原 Qwen3.8-27B 仍可在 dashboard 切回。
- pi `models.json` 改为该模型（`contextWindow` 131072，`maxTokens` 8192，支持 reasoning effort），压缩参数
  reserve 16384 / keepRecent 20000；旧配置备份为 `models.before-strata.json`、`settings.before-strata.json`。

实测（RTX 4090 24 GB，内存 94 GB）：冷启动 44–50 s；约 5 万 token 提示读 2,145 tok/s、生成 78 tok/s，并正确回答了埋在其中的问题；
工具调用格式正确；显存约 23.7 GB。

| 试跑 | Strata IQ3_S（128k） | 对照 |
|---|---|---|
| 1 只读：每日计划检查（与 20.2 试跑 1 同类提示） | 成功，72 s，12 次工具调用（1 次报错后自行修正），$0；先读 skill 与运维手册（Qwen 27B 当时跳过了），运行 `ops daily-plan --fail-on-missing-env`，未改仓库 | gpt-5.5：39 s，6 次，$0.19；Qwen 27B：32 s，5 次 |

偏差：临时脚本用了系统 `python` 而不是项目 venv（与 20.4 记录的偏差相同）。

**试跑中发现的运维问题（已核对产物，需 owner 处理）**：计划本身 READY（2026-10-09 交易日、27 步、环境变量齐全），但 2026-10-09 的
`daily_default.v1` DQ 回执为 FAIL：评估窗口只到 2026-07-23，请求到 2026-10-09，阻断项为 `prices_stale`、`rates_stale`、
`prices_requested_window_coverage_missing`、`prices_non_market_session_date`、`download_manifest_requested_window_mismatch`，
`consumer_cutover_allowed=false`。也就是说缓存行情自 2026-07-23 后未刷新。gpt-5.5 与 Qwen 27B 的试跑只看了计划，没有检查 DQ 回执。

**结论**：128k 上下文解决了 20.6 记录的溢出与大文件限制，本地模型可以承担更大范围的阅读任务。判断类改动仍需审阅把关。
