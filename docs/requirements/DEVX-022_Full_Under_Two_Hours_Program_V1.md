# DEVX-022 Full 压缩到 2 小时内的优化程序 V1

- 任务：`DEVX-022_FULL_UNDER_TWO_HOURS_PROGRAM`
- 优先级：P0（owner 2026-10-04：最高优先级，目标把一次正式 Full 压到 2 小时内）；状态：IN_PROGRESS；next owner：Claude Code coordinator
- 授权：owner 2026-10-04 对 `DEVX-020` 第 9 节的评估回复「好的，你按照这个顺序依次验证优化吧」，即批准按下列顺序实施并逐步验证。
  其中 S4（复合组拆分、变体共享不可变前缀）已在评估里标注「须 owner 复核测试结构」，本批准视为同意实施；实施时仍须给出覆盖等价性论证，并在发现语义风险时停下报告。
- 关联：`DEVX-018`（调度清单、O1–O3、K 实验）、`DEVX-020`（成本剖面与评估）、`DEVX-021`（发布钩子/worker 墙钟/残留锁）。
- 影响范围：验证调度（`scripts/pytest_runtime_profile.py`、`config/architecture/devx_018_validation_scheduling.yaml`）、发布租约库（`FileExecutionLeaseStore`、`workflow_coordination.py`）、
  测试结构（`tests/test_arch_005_integration_publication_fence.py` 等重型 fixture）。不涉及投资逻辑、数据、回测；`production_effect=none`，`broker_action=none`。

## 1. 起点与目标
- 起点（基线已发布后，v26 真实 Full）：墙钟 3.59 小时；总工作量 43.0 节点小时（重型 24.6、轻量 18.4）；16 个 worker；K=8。
- 目标：一次正式 Full 墙钟 < 2.0 小时，同时不放宽任何验收语义、不减少 collected nodeids、不引入跨调用身份缓存（O3 跨调用缓存仍未获批）。
- 评估结果（DEVX-020 第 9 节）：中等假设约 2.2 小时、乐观假设约 1.9 小时、再加 20 个 worker 约 1.6–1.8 小时；L4（发布阶段操作）与 L6（worker 数）是最大不确定项。

## 2. 步骤分解、依赖与验收（每步必须有实测，不以模型为准）
| 步骤 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| S1 | 取证：4 个代表节点的函数级剖面（发布阶段约 7 分钟的内部分解）+ 计数器型遥测框架（只用性能计数器，禁用 WMI 进程轮询） | 无（不改代码） | 报告：每个发布阶段操作的次数/耗时/CPU 占比；L4/L6 上限的实测估计；遥测可用于后续各步的 Full |
| S2 | L0：重型起跑间隔只对前 K 个起跑限速（仅爬坡）；调度清单 v6；调度器单元/性质测试 | S1 可并行 | 调度测试通过；公式性质测试证明「爬坡后不限速、首批 K 个仍按间隔、上限 K 不变、无死锁」；一次正式 Full 的 profile 显示重型并发与 v26 相同（该步本身不减少工作量） |
| S3 | P4：租约库重放成本（单次完整重放 16–17 秒/402 MB，每个租约事件约 6 次重放）+ DEVX-021（ORIG_HEAD 基线、`local-publish` worker 墙钟纳入受审配置、残留锁恢复） | S1 的剖面确认重放占比 | 7 个大发布节点各 ≤ 约 20 分钟（v26 为 50–68 分钟）；租约库重放语义不变（篡改/截断/重排负向测试仍拒绝）；真实 `.git` 副本演练 `git merge --ff-only` 钩子序列；一次正式 Full |
| S4 | L2 复合组拆分（`host_registry_view` / `fixed_publication_binding_job`）+ L3 变体共享不可变前缀（每族一次构建，各变体克隆） | S2、S3 | 覆盖等价性论证（每个变体的断言与分叉点不变）；collected nodeids 与 v26 完全一致；互斥语义由测试证明；一次正式 Full |
| S5 | L4/L5/L6：发布阶段只读检查在**同一事务内**共享、轻量测试审计、worker 数 | S1 实测上限 | 按 S1 剖面逐项取舍，每项独立验收；最终一次正式 Full 墙钟 < 2.0 小时（或明确报告差距） |

## 3. 验证与发布策略
- 每步结束运行一次**正式 Full**（完整 fence 事务，parent 沿用 v20 失败摘要），**不发布**，事务按失败释放，以得到无环境噪声的 PASS/FAIL 与 profile；
  最终候选（S5 之后）的 Full 同时用于发布。理由：当前 `local-publish` 的 worker 被写死 3600 秒终止（DEVX-021 新项 A），在 S3 修复之前再次发布会再次触发恢复路径与人工清理。
- 每步 Full 前必须先在安静主机上对被改模块跑非重型层测试（`-p scripts.pytest_runtime_profile -m "not real_full_chain"`），这是 v24 的教训。
- 遥测只用性能计数器（WMI 进程轮询曾让同一节点慢 2.6 倍）。
- 临时 worktree/目录遵守生命周期规则：记录用途、路径、退出条件，步骤结束后审计并清理。

## 4. 开放问题（需要实测或 owner 复核）
1. 发布阶段约 7 分钟的内部分解（S1）——决定 L4 的可行性与做法。
2. 租约库重放优化的做法（同一原子窗口内共享重放 vs 增量重放 vs 事件外置）取决于 S1/S3 的实测；任何缓存都只能在持有仲裁锁的同一次原子操作内，
   且首次完整验证不可省略（同 O3-c 的「首尾完整、中间元数据」原则）；若必须跨操作缓存，先停下报告。
3. 变体共享前缀的覆盖等价性（S4）：每个变体的分叉点必须晚于共享前缀；若某变体在前缀内就分叉，则不共享。
4. worker 数提升的 CPU 余量（S5/L6）。

## 5. 进展记录
- 2026-10-04：登记任务与本计划；owner 批准顺序；开始 S1。


## 6. S1 取证结果与已落地的两项低风险优化（2026-10-04）

### 6.1 方法
- 在 `.venv/Lib/site-packages/sitecustomize.py` 放了一个**临时、仅在环境变量 `AITS_TRACE_DIR`/`AITS_CPROFILE_DIR` 存在时才生效**的探针，记录每次选定脚本的 CLI 子进程（含 `-I` 隔离的 profile 检查器）的墙钟与 CPU 时间，
  可选再写 cProfile；探针不进仓库，S1 结束后移除并验证。进程内部分用串行 `-m cProfile -m pytest -p no:xdist` 的诊断运行（规程允许的、已注明的串行诊断例外）。
- 遥测只用性能计数器。教训：WMI 进程轮询采样器（0.7 秒间隔）使同一节点从 705 秒变成 1,806 秒，不能用。
- 全部在安静主机上，一次一个测试；节点：`replays_candidate_publish[normal]`（中型）、`completed_admission_full_entry[LANE]`、`source_generation_and_commit_crash_recovers`、`ff_only_and_independent_recovery`（大节点）。

### 6.2 发现 1（已修复，O3-d）：`bounded_regular_bytes` 每次读取都按 256 MiB 预算分配缓冲区
- Full profile 检查器（`inspect_full_publication_profile`，`-I` 子进程，每个发布类节点跑 ≥2 次）捕获 1,326 个文件并整体复核一遍（共 ~2,650 次 `bounded_regular_bytes`），
  调用方预算是 256 MiB；`reader.read(budget + 1)` 先分配整个预算：每次约 33 毫秒，哪怕文件只有几 KB。**单次检查器 155 秒里有 87 秒是这个。**围栏进程内的 `_recheck_full_profile`
  也一样（2 次，各约 46 秒）。整节点 655 秒调用里约 264 秒（40%）是这个分配。
- 修复（commit `828f756bc`）：读取量改为 `min(budget, 已冻结文件大小) + 1`（对象 deny-write 且大小刚核对过），预算检查与「读到的长度必须等于冻结大小」的新检查保留语义。
  实测：检查器 155/144 秒 → 62/60 秒；`replays_candidate[normal]` 调用 655 秒 → 368 秒（**−44%**）；小文件 256 MiB 预算读取 33 毫秒 → 0.25 毫秒。
- 同一修复让所有走检查器/重新核对的节点受益，不限于单一节点族（这是 K 实验里没发现的，因为那里测的是平均节点而非单次调用的函数级剖面）。

### 6.3 发现 2（已修复，S2/L0）：起跑限速对每一次重型起跑生效
`heavy_start_interval_seconds` 在仍有轻量单元时限制每一次重型起跑，节点变短后把重型吞吐封顶在每小时 ~30 个单元；模型显示不改它，工作量降到 27 节点小时墙钟仍约 2.8 小时。
改为只对前 K 个起跑限速（commit `db3b5f6cb`，清单 v6），新测试在旧调度器上失败、新调度器上通过。

### 6.4 发现 3：大发布节点（ff_only 等）由租约库重放主导，真实租约库已累积到 687 MB
- 带 O3-d 修复、安静主机的 `ff_only`：调用 1,750 秒（v26 负载下约 4,000 秒）。其中 8 次 `local-publication-hook` 子进程共 611 秒墙钟（每次约 76 秒、CPU 81 秒，几乎纯 CPU）、
  `local-publication-worker` 1,054 秒、检查器 3 次共 193 秒。**钩子与 worker 的时间是租约库重放**：保留的 396 MB/45 事件租约库单次重放 13.0 秒。
- cProfile：重放的 88% 在 `parse_lease_event → validate_execution → _validate_hook_capsule → _validate_hook_ready`（18 次，20.5 秒）：每个事件携带同一份约 16.5k 行的 `read_file_custodies`，
  逐行做 pathlib 校验（约 120 万次 `Path` 解析）、逐行 `json.dumps`，同一内容在一个租约里被重复校验 18 次。JSON 解码只占 1.6 秒。
- **真实主检出的租约库**：817 个租约、5,382 个事件、687 MB（v25 与 v26 两个租约各约 300 MB），`replay()` 每次读取并校验**全部**租约：22–24 秒/次，
  这就是 v26 `local-publish` 超过 3600 秒的原因，而且会随每次发布累积变慢（历史租约不会被排除）。
- 修复 S3a（commit `06f5c415d`）：`FileExecutionLeaseStore.replay()` 在 `replay_validation_scope()` 内运行，只在这一次调用内按深度相等对四类大子结构做一次校验
  （hook-ready 行校验及分发摘要、git-launch 的两个 canonical 摘要、profile 捕获行检查）；调用结束后一切失效，没有磁盘标记、没有跨调用缓存，内容有任何差异则完整校验。
  与既有设计声明（「No validation survives this invocation」）一致，并有测试证明作用域只在单次调用内、被篡改的拷贝仍被拒绝。
  实测：396 MB 租约库重放 13.0 → **5.5 秒（−58%）**。剩余：JSON 解码 2.9 秒（体积决定）、事件摘要 0.9 秒、其余。

### 6.5 其余热点（未修复，供 S3b/S4/S5 取舍）
- `check_full_readiness`（每个中型节点 4–5 次，每次安静主机约 38 秒、纯 CPU）：`build_architecture_fitness` 对约 1,230 个源文件做 AST 解析与访问（解析约 16 秒 + 访问约 12 秒）、规范任务库校验约 10 秒（400 次 `safe_dump`）、Atlas 绑定约 7 秒。
- 每个发布类节点的 fixture 搭建约 87 秒（`_seed_source_generator_architecture` 约 77 秒含基线抓取与 `build_architecture_fitness`、`_seed_readiness_atlas_inputs` 约 22 秒）。
- 内层 Full（真实 `run_validation_tier full`，安静主机约 190 秒；其中 pytest 仅 109 秒，主要是 16 个 worker 启动）。
- 重放仍是 JSON 体积主导：把 16.5k 行列表外置为内容寻址的旁文件（只存摘要引用，读取时一次性解析并校验）可再降一个数量级，属 S3b，涉及租约事件存储格式（事件 ID 语义不变），风险更高，待决定。

### 6.5a S3a 之后的大节点复测（安静主机、同一节点、带探针）
`ff_only_and_independent_recovery[full-profile-publish]`：调用 **1,176 秒**（O3-d 后 1,750 秒；v26 满载约 4,000 秒）。各阶段墙钟（S3a 前 → 后）：
`local-publish` 1,363 → 862 s；`local-publication-worker` 1,054 → 687 s；8 次钩子 611 → 339 s；检查器 ×3 193 → 167 s；`recover` 84 → 41 s；readiness 37 → 39 s；Atlas 25 → 26 s。
租约库未变（45 事件 / 396 MB）。结论：S3a 之后钩子单次仍约 42 秒（JSON 解码 + 摘要 + Python 启动与模块导入），已接近体积决定的下限；继续下降需要 S3b（事件外置）。

### 6.5b S1 收尾与临时资源记录
- 临时探针 `.venv/Lib/site-packages/sitecustomize.py` 已删除并复核不存在（它从未进入仓库，仅在环境变量存在时生效）。
- 保留（用途：S3b 重放基准与证据；退出条件：S3b 决定后或本任务关闭时清理）：`D:/Work/devx022-s1/bt_n4b`（`ff_only` 节点 basetemp，含 396 MB/45 事件租约库，486 MB）、
  `D:/Work/devx022-s1/` 下的 trace/profile/log 证据、`D:/Work/devx020-k`（K 实验原始数据）。其余 `D:/Work/devx021-prof` 为日志。均无活动进程依赖。

### 6.6 对计划的影响
- 一个已经提交的低风险优化（O3-d）就让中型发布节点 −37~44%；S3a 又让大节点重放 −58%。预计 Full 工作量由 43 节点小时降到约 33–35 节点小时（待 S2 之后的正式 Full 实测）。
- 下一步：对被改模块跑非重型层测试 + 重型样本，然后做 S2 之后的第一次正式 Full（带计数器型遥测）以得到真实 profile，再决定 S3b / S4 / S5 的取舍。

## 7. M1：S1–S3a 之后的第一次正式 Full 实测（2026-10-04，未发布）

- 候选 `f8a01460f`（`1328ea59b` + 兼容性授权重封），事务 `gov-007-devx022-m1-formal-20261004-v1`（`failure_fix_rerun`，parent = v20 失败摘要），冻结 driver `run_claude_gov007_m1.py`；
  事务按失败释放，未发布、未改 main（`main = origin/main = 1e46e6ac7`）。
- 结果：**14,711 passed / 4 skipped / 0 failed**；pytest `8,460.66 s`（2 小时 21 分），Full 阶段 `8,809 s`；v26 为 `12,911 s` / `13,247 s`，**墙钟 −34.5%**。
  整条正式链六个阶段合计 `3.01 小时`（v26 `4.44 小时`，其中 v26 的 named-parent-positive 1,087 s 受 Defender 文件扫描干扰，本次 428 s）。
- 工作量与利用率：全部节点合计 **37.25 节点小时**（v26 43.0；重型家族 17.3、其余 19.9）；16 个 worker 在 140 分钟里全程满载，**尾部空闲总计 610 s（0.7%）、最大 44 s**——
  关键路径（复合组串行链）不再是瓶颈，墙钟 ≈ 总工作量 / 16。S2（只对前 K 个起跑限速）与 O3-d/S3a 的收益在这里兑现。
- 主机计数器（性能计数器，4 秒间隔，`D:/Work/devx022-m1-counters.csv`）：32 个逻辑处理器，Full 窗口 CPU 平均 **78.5%**（p90 100%），可用内存最低 23 GB。
  **CPU 是约束资源**：16 worker 加上每个重型节点内部的 16 worker 内层 Full 已使 CPU 接近饱和，因此提高 worker 数（S5/L6）预计没有收益，暂不做。
- 重型大发布节点在满载下仍为 2,200–2,400 s（静机同一节点 1,176 s，约 1.9× 膨胀）。
- 距 2.0 小时目标的差距：需要总 CPU 工作量由 37.25 降到约 31–32 节点小时（约 −15%）；这个量级仍然靠「减少工作量」而不是「调度」。

## 8. 新发现 F4（S6）：被归为「轻量」的慢节点，92%/87% 的时间在 `validation_session` 的路径指纹（2026-10-04）

### 8.1 现象
M1 profile 里，调度清单未列为重型的节点中，≥60 s 的有 **236 个（12.3 节点小时）**，≥200 s 的有 73 个（8.1 节点小时），集中在
`test_smoothed_*`、`test_paper_shadow_*`、`test_composer_prospective_*`、`test_named_*` 等文件。串行 cProfile（规程允许的、已注明的诊断例外，静机、一次一个节点、`D:/Work/devx022-t1/`）：

| 节点 | 满载 Full 中 | 静机串行 | 指纹计算占比 | 指纹调用 | `nt.stat` | `nt._getfinalpathname` |
|---|---|---|---|---|---|---|
| `test_smoothed_forward_weekly_run_handles_no_due_windows` | 1,428 s | 402 s | 92%（367 s） | 872 | 3.06 M / 197 s | 1.24 M / 105 s |
| `test_paper_shadow_weekly_validation_rejects_illegal_decision` | 533 s | 221 s | 87%（193 s） | 3,169 | 1.61 M / 105 s | 0.70 M / 61 s |

两个无关家族的热点相同：`platform/artifacts/validation_session.py::_compatibility_artifact_fingerprint`（旧式、未声明范围的校验调用走的指纹路径），
系统调用合计占 75% 以上（每次 `stat` 约 64 µs、`_getfinalpathname` 约 85 µs，Windows + 实时扫描）。

### 8.2 根因（读代码 + 调用计数）
每次指纹计算对每个「绑定路径」（被工件 JSON 里的校验和引用的路径，单次约 300 个）调用 `_observe_path`：
1. `_is_link_or_junction(path)`：对**每个祖先目录**各做一次 `is_symlink()`（平均每个路径 7.7 次 lstat）；同一次计算里数百个路径共享同一组祖先，却每个路径重新检查；
2. `_resolved(path)`（`Path.resolve`，每次 2 个 `_getfinalpathname`）：`_observe_path` 内部一次，调用方 `observed.get(_resolved(bound))` 再对同一个路径做一次（平均 2.35 次/路径）；
3. 再 1 次 `stat`。
`cached_artifact_validation` 每次校验会重复计算指纹 2–3 次（前/确认/后），嵌套校验使一个测试里累计到 872–3,169 次指纹计算。

### 8.3 方案（S6）与语义边界
- 在**一次指纹计算**（`_compatibility_artifact_fingerprint` 与 `artifact_fingerprint`）的范围内：
  a. 记住「某祖先目录是否为链接/联接」的结果（发现链接即抛 `_UncacheableFingerprintScope`，与现行为一致）；
  b. 记住同一个输入路径的 `resolve` 结果。
  计算返回或抛出时整份记忆失效；**不跨调用、不跨会话、不落盘**；这与 owner 在 O3 已批准的「同一次计算内共享祖先检查」是同一原则。
- 对静止的文件系统，指纹输出逐字节不变；不可缓存的判定条件不变；`cached_artifact_validation` 的前/确认/后三次指纹仍各自完整计算（每次一份新记忆），TOCTOU 防护不被削弱。
- 残余风险（须披露）：一次指纹计算内（毫秒到秒级）若某祖先目录被换成链接，后续路径的检查不再重新观察到；但被换链接后 `resolve` 后的路径字符串进入指纹，下一次指纹计算（前/确认/后之一）会因路径或摘要不同而不一致，退化为重新校验。
- 同时纯重构 `_platform_change_token`：ctypes 结构类与 `kernel32` 绑定改为每进程一次（原先每次调用重建，修复后占节点约 9–11%），观测值不变（与逐次重建的参考实现比对）。
- 预期：该节点系统调用 4.3 M → 约 2.0 M，节点 402 s → 约 220 s（−45%）；对全量 Full 约 **4–5.5 节点小时（−11%~−15%）**，与 S3b/S4/S5 独立、风险低；若达成，则 M1 的差距基本被它填上。

### 8.4 验收标准
1. 指纹输出对静止文件系统与改动前逐字节相同（对嵌套目录、绑定路径、缺失路径、目录型绑定、不可读路径各一个等价测试，对照旧实现的参考逻辑）。
2. 祖先链接/联接仍被拒绝（含：记忆命中之后换成链接的下一次计算必须拒绝）；记忆只在一次计算内有效（计算结束后再次计算会重新检查祖先）。
3. 计数型测试：同目录 N 个绑定文件的祖先检查次数 = 唯一祖先数，而不是 N × 深度。
4. 既有 `validation_session` 相关测试全部通过；对上述两个节点静机串行 cProfile 复测并记录前后对比；`production_effect=none`（只改变速度，不改变校验结论）。
5. 随 S6 做一次带计数器遥测的正式 Full（M2，不发布）以实测总工作量与墙钟。

### 8.5 对顺序的影响（须告知 owner）
S6 不在 owner 2026-10-04 批准的 S1–S5 序列内，是 M1 profile 暴露的新项；它风险低、独立、收益大，因此排在 S3b/S4/S5 之前做，并作为 S3 之后的补充项记录。S3b、S4、S5 仍按原批准保留，是否还需要，取决于 M2 实测与 2.0 小时目标的剩余差距。

### 8.6 S6 实现与静机复测（2026-10-04，commit `300fdaf57`）
- 实现：`validation_session._FingerprintComputationMemo`（已验证无链接的路径字符串集合 + 精确输入字符串到 `resolve` 结果的字典）由装饰器 `_one_fingerprint_computation`
  绑定在 `_compatibility_artifact_fingerprint` 与 `artifact_fingerprint` 的**单次调用**上（`ContextVar`，调用返回或抛出时复位）；`_is_link_or_junction`/`_resolved` 只在该范围内使用记忆。
  `stat` 结果**不**记忆，所以「文件在读取期间变化」的检查不变；`cached_artifact_validation` 的前/确认/后三次指纹各自是独立计算、各有新记忆。
- 静机串行 cProfile 复测（同一节点、同一机器、前后各一次）：

| 节点 | 前 | 后 | 变化 | 指纹计算 | `nt.stat` | `nt._getfinalpathname` |
|---|---|---|---|---|---|---|
| `test_paper_shadow_weekly_validation_rejects_illegal_decision` | 221 s | 94 s | **−57%** | 192.7 → 66.9 s | 1.61 M → 0.53 M | 0.70 M → 0.24 M |
| `test_smoothed_forward_weekly_run_handles_no_due_windows` | 402 s | 77 s | **−81%** | 366.8 → 45.9 s | 3.06 M → 0.58 M | 1.24 M → 0.17 M |

  收益远大于 8.3 节的 −45% 预估：除了祖先检查与重复 `resolve`，同一个绑定路径在 `_observe_path` 与绑定拓扑校验里被重复检查最终组件，也被一并共享。
- 测试（`tests/test_artifact_validation_session.py`，新增 6 个）：指纹字节与未记忆的参考实现逐字节相同（含缺失绑定）；`is_symlink` 观测次数在一次计算内每个条目至多一次、每次新计算重新观测；
  `resolve` 记忆只在计算范围内；在一次干净计算之后换成链接的下一次计算仍然拒绝，且失败的计算也释放记忆；变更令牌与逐次重建的参考实现相等且只绑定一次。
  回归：`validation_session` 相关 73 个测试文件的非重型层 513 通过 / 126 失败，**126 个失败全部在 `test_arch_004_refactor_policy.py` 的哈希权威家族**（源文件哈希变化，需要候选时的兼容性授权重封；与 M1 候选前的同类现象一致），其余文件全部通过。
- 残余风险（再次披露）：一次指纹计算内（毫秒~秒）若某祖先目录被换成链接，该次计算内后续路径不再重新观察；换链接会改变 `resolve` 后的路径字符串，使前/确认/后三次指纹之一不一致，退化为重新校验。
- 下一步：重封候选 → M2 正式 Full（不发布，带计数器遥测）实测总工作量与墙钟；按剩余差距决定 S3b/S4/S5/DEVX-021。
