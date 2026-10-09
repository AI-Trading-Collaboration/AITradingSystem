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

### 8.7 兼容性授权链的处理（须披露）
- `validation_session.py` 与 `tests/test_artifact_validation_session.py` 是兼容性基线里**按历史哈希固定**的源文件；它们的实时内容变化后，`test_arch_004_refactor_policy` 等哈希权威家族（M2 候选重封后仍有 118 个失败）要求变化的路径由最新授权段拥有。
- 处理方式：S6 的新测试放进新文件 `tests/test_devx022_fingerprint_computation_memo.py`（未被固定，因此原测试文件**不修改**，已从 S6 提交里还原）；只有 `validation_session.py` 一个路径需要授权，
  已加入最新的 `phase_devx_015_workflow_contract_v3` 段的受审来源列表（`compatibility_authority.py`），并同步独立钉死的预期集合 `DEVX_015_WORKFLOW_ADDED_SOURCE_PATHS`（`tests/test_devx_006c_compatibility_authority.py`）。
  兼容性授权链重建后，`test_arch_004_refactor_policy` / `test_devx_006c_compatibility_authority` / `test_trading2452_architecture_contract` / `test_devx_006d_report_catalog_flow_authority` 共 596 个测试全部通过，片段数仍为 28。
- 取舍与理由：另一种做法是**追加一个新的 DEVX-022 授权段**（更符合「只追加」的表述）。但这会让 arch_004 策略测试里约 100 处 `LATEST_COMPATIBILITY_SECTION` 的「最新段」假设整体重新参数化（以及 006c/2452 里约 25 处「最后一段 == v3」的断言），
  是一次独立的、需要完整回归的治理改动。本次选择在既有的、当前哈希会随每个候选重新封存的最新段里扩展一个受审路径，范围最小、可逆、并有测试钉死。
  如果 owner 更希望使用专用的新授权段，这是一个独立的后续任务，不阻塞 S6 的收益。

## 9. M2c：S1–S3a + S6 之后的正式 Full 实测（2026-10-05，未发布）

### 9.1 过程记录（含一次主机卡死和一次围栏拒绝，须披露）
- 候选 `c6a6b9d8d`（含 S6 与兼容性授权重封），M2 正式事务 `gov-007-devx022-m2-formal-20261005-v1`；前 5 个阶段全部通过（named-parent-positive 1,177 s、contract 296 s、integration 75 s、reproducibility 49 s、architecture-fitness 1,408 s）。
  named-parent-positive 本次 1,177 s、M1 为 428 s、v26 为 1,087 s，波动与代码无关（该阶段不计入 Full 的 pytest 墙钟）。
- **2026-10-05 02:03 主机硬卡死**（Event 41 + 6008，无 1001 蓝屏、无新转储；与记忆里的显示唤醒卡死同类，非测试负载或代码问题），M2 的 Full 在约 37 分钟（10%）时被打断，2026-10-05 02:04 重启。
  对被打断的 Full 做 `--recover-full --recover-full-action observe`：原 Job 已不存在（`DEAD_LAUNCHER_JOB_EMPTY`），事务按 `RECOVERED_FAILED_ATTEMPT` 记为失败、租约释放，生成 `full_incomplete_recovery.json`；无发布效果，主检出无残留锁，`main` 未动。
- **M2b（02:18）被围栏拒绝**：以上述恢复文件作为 parent 重开事务，Full 派发前被拒，`PUBLICATION_FULL_PARENT_REUSED: new candidate required`（同一候选不能以自己的失败尝试作 parent；围栏的本意是「失败后重跑必须是修复过的新候选」）。无产物、无副作用，事务已释放。
- **M2c（02:28）**：parent 改回 v20 失败摘要（本任务不发布的测量运行一贯采用的约定，见第 3 节），前 5 个阶段作为「带入记录」写入 driver（来自被打断那次的同一候选），只重跑 Full。
  变通说明：系统没有「主机中断后重试同一候选」的路径，我没有为通过围栏而制造无意义的新提交；如果 owner 希望把它做成正式恢复路径，应另开任务。
- 事务 `gov-007-devx022-m2c-formal-20261005-v1` 按失败释放（不发布）。

### 9.2 结果
- **14,717 passed / 4 skipped / 0 failed**（比 M1 多 6 个，是 S6 的新测试）；pytest `8,022.86 s`（2 小时 13 分 42 秒），Full 阶段 `8,348 s`；M1 为 `8,461 s`，v26 为 `12,911 s`（累计 **−37.9%**）。
- 总工作量 **30.93 节点小时**（M1 37.25，v26 43.0）：重型 15.95（M1 17.32）、其余 14.98（M1 19.93）。
- S6 兑现：≥200 s 的慢「轻量」节点 73 个，8.06 → 4.25 节点小时；`smoothed_*`/`paper_shadow_*` 等指纹主导节点普遍降到原来的 10–15%（如 1,428 → 189 s、1,065 → 137 s、761 → 82 s）；≥60 s 的轻量节点 236 → 179 个、12.3 → 7.0 节点小时。
- 但墙钟只比 M1 降 5%：**尾部空闲重新出现**（最长 1,235 s、合计 12,295 s；M1 为 44 s/610 s）。原因是**重型通道成为瓶颈**：重型工作量 15.95 节点小时 / 并发上限 K=8 ≈ 1.99 小时，与总工作量 / 16 ≈ 1.93 小时同量级；
  轻量单元在约 112 分钟耗尽后，8 个 worker 空闲，只剩 8 个 worker 在顺序跑最后几个 10 分钟以上的重型节点（最后一个 worker 133 分钟才结束）。

### 9.3 调度模型（用 M2c 的真实单元时长，`scratchpad/tail_model.py`；模型基线 7,145 s 对实测 7,982 s，低估约 11%，与此前校准一致）
| 方案 | 模型墙钟 | 折算实测（×1.117） |
|---|---|---|
| 现状 K=8 | 7,145 s | 7,982 s |
| 轻量耗尽后解除重型上限 | 7,009 s | 7,830 s（−2%，且尾部并发升高会放大负载/超时风险，暂不做） |
| K=6 / 10 / 12 | 8,724 / 7,249 / 7,589 s | 都不优于 K=8 |
| 复合组拆开（S4 的复合组部分） | 7,276 s | 无收益（不再是关键路径） |
| **重型工作量 −10%** | 6,597 s | 7,370 s（2.05 h） |
| **重型工作量 −20%** | 6,262 s | **6,995 s（1.94 h）** |
| **重型工作量 −30%** | 5,927 s | 6,620 s（1.84 h） |
结论：**调度已无大头可挖；要进 2 小时，需要把重型通道的工作量降 ≥ 20%（约 3.2 节点小时），同时总工作量保持 ≲ 31 节点小时。** S4 里「复合组拆分」可以取消，「变体共享不可变前缀」仍有效（减重型工作量）。

### 9.4 下一批候选（按风险从低到高，预估节省为重型通道节点小时，待测量确认）
1. **轻量侧剩余热点**：`test_actual_composer_*`（1,287 s、695 s、492 s，不依赖 validation_session，S6 未触及）等约 4.25 节点小时的慢节点；先用同样的静机 cProfile 找共享热点（S1 方法）。
2. **就绪度/架构适应度评估的纯加速**（`check_full_readiness` 每个重型节点 4–5 次、每次约 38 s，其中 AST 解析约 16 s、遍历约 12 s）：单次评估内共享遍历与解析，不改输出（生成器零差异即等价性证明）；预估 −1~2 节点小时。
3. **S3b 租约事件外置**（约 6.2 节点小时的大发布节点，重放与钩子占约一半）：预估 −2~2.5 节点小时；改变租约事件存储格式（事件 ID 语义不变），风险最高，需要完整回归与真实 `.git` 副本演练。
4. **S4 变体共享不可变前缀**：预估 −2~3 节点小时；须给出覆盖等价性论证（分叉点必须晚于共享前缀），owner 已在 S4 批准时标注需复核。
5. 取消：S4 复合组拆分（模型无收益）、S5 worker 数（CPU 已饱和）、轻量耗尽后解除上限（收益约 2%、风险上升）。

## 10. M2c 之后的剖析：重型节点的 CPU 构成，以及「内层 Full 改 4 个 worker」被搁置的原因（2026-10-05）

### 10.1 测量方法的教训（须记住）
- 对含内层 Full 的节点，**不能用父进程 cProfile 的耗时估算**：`completed_admission_full_entry[LANE]` 在 cProfile 下是 1,205 s，不带 cProfile 只有 326 s（M2c 满载 Full 里 508 s），
  差值是父进程里 Python 层的哈希/校验循环被 cProfile 放大。含子进程的节点用**进程 CPU 计数器**（`Get-Process` 每 15 s，`scratchpad/py_poll.ps1`），函数级细节只在隔离调用里取。

### 10.2 重型节点的 CPU 构成（静机、一次一个节点，`D:/Work/devx022-t1/poll_f.csv`）
- 同一节点（316 s 墙钟）python 进程累计 **628 CPU 秒**（平均并行度 2.0）：内层 Full 的 16 个 worker 各约 **24 CPU 秒、共 384 CPU 秒（61%）**，但只跑 74 个桩测试（测试窗口 0.03 s）；
  其余是就绪度评估（内层 runner 一次、最终准入一次，各约 65 s 的单核 CPU）、夹具搭建与 git。
- worker 在 `pytest_configure` 与 `sessionfinish` 各做一遍「运行时清单校验 + 实现绑定」：运行时清单校验 4.0 s（热缓存）、实现绑定 2.5–3.1 s、验收绑定 0.8 s，合计约 8 s/遍，两遍约 16 s，再加启动与导入约 24 CPU 秒。
- Full 本身是 CPU 受限（32 个逻辑核，Full 窗口平均 78.5%），所以**降低 CPU 工作量**才会反映到墙钟上；每个重型节点都带一次 16 worker 的内层 Full，是 CPU 的大头。

### 10.3 被搁置的方案：夹具内层 Full 改 4 个 worker（owner 2026-10-05 选择后，实施前发现须停下报告）
- owner 选择了「夹具内层 Full 改用 4 个 worker（真实 Full 仍 -n 16）」。读代码后发现 `-n 16` 不只是夹具设置：正式 Full 的合同写在**受保护的发布检查器**里
  （`scripts/run_validation_tier.py`：`inspect_full_publication_profile` 要求 `-n` 恰为 `"16"`、`expected_worker_count=16`；验收结果 `expected_collections=16`；时长剖析来源 `source_workers == 16`）。
  夹具里的「真实 Full」就是走这套检查器，所以要让夹具用 4，必须把发布门槛里的常量变成可配置，等于修改正式 Full 的合同，并让夹具比真实发布更宽松。
- 风险：这正是 v25/v26 暴露过的类型（夹具通过几百次、真实仓库才出问题）；16 worker 的闭合（witness、collections、Job 包含）只会在真实发布第一次被端到端执行。
- 处理：**不实施**，向 owner 报告并改走不动合同的路径（10.4）。如果 owner 仍希望这样做，需要显式批准「修改正式 Full 合同的 worker 数策略」，并同时保留 1–2 个专项节点继续使用 16，另行立项。

### 10.4 S7：不改验证合同的 CPU 削减（本节登记后实施）
| 项 | 内容 | 语义边界 | 验收 |
|---|---|---|---|
| S7a | worker 清单校验去掉 pathlib 往返：`acceptance_runtime_identity_from_inventory`、`_InventoryBackedInputs.__init__`、`_verify_runtime_ancestors` 改用字符串路径与 `os.lstat` | 检查项不变（路径集、size、mtime_ns、文件 ID、非链接/重解析点、祖先无重解析点）；产出的 identity 与旧实现逐字节相同 | 真实运行时 identity 逐字节相等；篡改 size/mtime/链接/祖先重解析点的负向测试仍拒绝；静机 worker 校验 4.0 s → 目标 ≤ 2.5 s |
| S7b | 任务登记 `_load_generated_mapping`：「这段字节等于它解析结果的规范序列化」是字节的纯函数，用字节 SHA-256 记忆；**文件每次仍重新读取并解析**，只跳过重复的序列化比对 | 不缓存任何文件系统状态，键是内容摘要；非规范内容永不缓存；改一个字节必然重新校验 | 同输入同结果；非规范仍以同一错误码拒绝；一次就绪度评估内 3 次登记校验的序列化比对只做 1 次；就绪度 canonical 相关耗时下降 |
| S4（评估中） | 变体共享前缀：admission 家族 11 个变体都会改写共享状态（提交/改文件/篡改事务/释放租约/等待过期），且夹具状态绑定绝对路径，不能克隆 | 需要逐家族的覆盖等价性论证与逐变体状态还原；不可还原的变体（terminal、expired）必须独立前缀 | 另文给出每个家族的分叉点分析后再决定 |
- 放弃：AST 解析/遍历优化（`ast.parse` 每文件一次，19 s 是固有成本；遍历仅 4.8 s）；整对象共享登记校验结果（比 S7b 多省约 6 s，但扩大「同一次评估内状态被换掉后不再重新观察」的窗口）。
- 预估：S7a+S7b 合计约 −5~8% 的 CPU；仍不足以单独进 2 小时（M2c 为 2 h 13 m），需与 S3b/S4 的结果合并评估后再做 M3 正式 Full。

## 11. 收口（2026-10-05）：S7 结果、named-DQ/composer 剖析、种子顺序模型，以及待 owner 决策的事项

### 11.1 S7 实测
- **S7a 已撤回（未提交）**：真实 16.5k 文件运行时上 worker 清单校验 `2.62 → 2.39 s`（−9%），折算每个内层 Full 约 7 CPU 秒（节点 CPU 的 <1%）。改动落在安全相关的校验路径上，收益不抵风险，故撤回；`tests/test_devx022_inventory_verification.py` 一并删除。
- **S7b 已提交**（`b252e94dd`，重封 `449c5144c`、`b59803159`）：同一进程连续 3 次 `validate_canonical_registry`，`21.6 → 11.5 s`（−47%）；一次就绪度评估里同批任务片段被校验 3 次，每次评估约省 10 s（65 s 的约 15%）。10 个测试
  （`tests/test_devx022_canonical_yaml_memo.py`）：与旧的两次读取实现在规范/非规范/CRLF/孤立 CR/非映射 6 种输入上结论一致；相同字节只序列化一次但文件每次仍读取解析；改一个字节必然完整重验；非规范内容永不缓存；每次返回新对象。**尚未经正式 Full 验证。**

### 11.2 named-DQ / composer 类慢节点（存活事务下的 cProfile，`D:/Work/devx022-t1/prof_h.prof`）
- `test_actual_composer_activation_and_readiness_routes_use_real_guards_and_replay`（满载 Full 中 1,287 s）：profile 834 s 里 461 s 是 604 次 `Thread.join`（`subprocess.communicate` 在等约 300 次 CLI 子进程，是节点要测的行为，不可砍）；
  `_live_parent_proof` 9 次，共 **18 次对真实主检出租约库的重放**（每次静机 10.8 s，约 195 s）；`named_data_quality_support._json_bytes` 32 次约 90 s。
- 真实租约库：5,502 个文件、**689 MB**，其中 29 个 >5 MB 的事件文件占 653 MB（v25/v26 发布遗留的 hook capsule）。我这一轮的测量事务几乎没增加体积（+约 2 MB），所以库没有因测量膨胀；但**每次真实发布会追加约 300 MB，之后每条围栏命令的重放成本随之线性增长**。
- 结论：这是 S3b/P4 的同一个根因（事件内嵌 16.5k 行自定义清单）。治本是租约事件外置或历史租约压缩；它同时会降低真实发布流水线里每条命令的重放成本。

### 11.3 种子顺序模型（`scratchpad/order_model.py`）
- 用 M2c 的真实单元时长，按**当前过期种子（v7，2026-09-26）的顺序**与按**真实时长最长优先**调度：`7,440 s` 对 `7,145 s`（−4%，约 5 分钟）。≥8 分钟的重型单元晚于第 60 分钟起跑的数量两种顺序相同（32 个），因为重型通道在末尾仍是满的。
- 刷新种子按 TRADING-2564 S5 的规则需要严格回放、独立复核，并随一个发布候选走四个正式 tier 与一次自然 Full；最多约 5 分钟，暂不做。

### 11.4 距 2 小时的差距与待 owner 决策
- 现状：M2c `2 小时 13 分`；目标 `< 2 小时`，差约 13 分钟（约 10%）。各项估算（待验证）：S7b 约 −3 分钟、种子刷新约 −5 分钟、S4 变体共享约 −6 分钟（高投入、须逐家族论证）、**S3b 租约事件外置约 −9~13 分钟（高风险）**。
- 把 `-n 16` 改成内层 4 个 worker（10.3）会修改正式 Full 合同，已搁置。
- **需要 owner 决定**：
  1. 是否批准 S3b（改变租约事件存储格式，旧事件保持可读；需要完整回归、真实 `.git` 与真实租约库演练、一次真实发布来验证）。这是稳进 2 小时的主要途径，也是长期必须解决的可扩展性问题。
  2. 是否批准修改正式 Full 的 worker 合同策略（10.3）；若批准，需同时保留 1–2 个专项节点继续用 16。
  3. 若两者都不做：以 S6 + S7b 候选为终版（预计约 2 小时 10 分），转入 DEVX-021 与发布。
  4. 种子刷新是否随发布候选一并做。
- 临时资源：本段的 basetemp 已按白名单删除（释放约 14 GB）；保留 `D:/Work/devx022-t1` 下的 profile/日志/计数器（约 12 MB）、`D:/Work/devx022-s1/bt_n4b`（S3b 基准，486 MB）、`D:/Work/devx020-k`；无活动进程依赖。

## 12. S3b：租约事件的行表外置（owner 2026-10-05 批准；实施前登记）

### 12.1 事实（真实租约库与真实大事件，2026-10-05 实测）
- 真实租约库 5,502 个文件 / **689 MB**，其中 29 个大事件（22.4–22.9 MB 文件）占 653 MB；一个大事件的紧凑 JSON 为 10.3 MB，其中 **`read_file_custodies`（17,936 行）占 9.65 MB（94%）**，其余是 `profile_inspection.captures`（0.32 MB）等。
  文件是带缩进写入的，55% 的磁盘体积是空白。该行表在每个后续事件里原样重复一份（每个事件内嵌整个 execution 快照）。
- 一个大事件的重放成本（隔离、热缓存）：读文件 9 ms + `json.loads` 43 ms + 规范序列化 43 ms + SHA-256 4 ms ≈ **100 ms/事件**；51 个事件 ≈ 5 s，与 S3a 之后实测的重放（396 MB 库 5.5 s）一致。
  结论：S3a 之后重放的剩余成本几乎全是「每个事件重新解码并规范序列化那 10 MB 行表」；去掉行表后同一事件解码 1.1 ms、规范序列化 2.1 ms。
- 每次真实发布向库里追加约 300 MB，之后每条围栏命令（钩子 ×8、worker、恢复、准入、live proof）的重放成本随之线性增长。

### 12.2 设计
1. **存储形式（stored form）**：满足阈值的 `read_file_custodies`（行数 ≥ `EXTERNALIZED_ROWS_MINIMUM = 1024`，工程不变量：低于它内嵌更省，且小夹具保持原样）只在两个固定位置被替换：
   `execution.hook_capsule.ready.inputs.read_file_custodies` 与 `execution.publication_attempts[i].hook_capsule.ready.inputs.read_file_custodies`，
   换成标记 `{"externalized_rows.v1": {"sha256": <规范字节的 SHA-256>, "row_count": N}}`。
2. **行表存为内容寻址 blob**：`<store>/blobs/<sha[:2]>/<sha>.json`，内容是行表的规范 JSON 字节（`sort_keys`、紧凑分隔符、`ensure_ascii=False`），blob 的 SHA-256 即标记里的 `sha256`，因此校验只需对原始字节做一次哈希。
3. **事件 ID 始终是「存储形式」规范体的哈希**：旧事件（内嵌）ID 公式不变；新事件的存储形式是紧凑体，所以验证 ID 不再序列化 10 MB。schema：含标记 → `execution_lease_event.v3` / `execution_lease.v3`；不含 → v2；二者严格互斥（含标记却写 v2、或 v3 却无标记，一律无效）。
4. **内存形式不变**：解析时把标记展开成与旧版逐值相同的 `list`（子类 `ExternalizedRows` 携带 `sha256`，使 `_body()` 重新压缩时无需再序列化行表）。所有下游校验器（`validate_execution`、热点校验、过渡规则、S3a 的 replay 内记忆）看到的数据与旧版相同。
5. **写入顺序与持久性**：先原子写 blob（已存在且字节摘要匹配则复用，不匹配则失败，不覆盖），再原子写事件；崩溃只会留下无害的孤儿 blob。写入在仲裁锁内，沿用现有 `write_json_atomic` 类写入器。
6. **读取与失败闭合**：blob 缺失、摘要不符、行数不符、非列表、标记出现在固定位置之外、无 blob 读取器却遇到标记，一律使该事件无效，`replay` 以 `LEASE_EVENT_INVALID` fail closed（与损坏事件同级）。
7. **replay 内共享**：同一次 `replay()` 调用内，相同摘要的 blob 只读取、哈希、解码一次，并在各事件间共享同一个 `ExternalizedRows` 对象（沿用 S3a「只在单次调用内、调用结束即失效、不落盘」的原则）。
8. **兼容**：旧事件（内嵌 v2）原样可读、ID 公式不变；v3 事件被不认识它的旧代码拒绝（`LEASE_EVENT_SCHEMA`），是显式失败而不是误读。
9. **不改变**：仲裁与过渡规则、`validate_execution` 语义、阈值以下的列表、`profile_inspection.captures` 等其余字段、历史事件字节。

### 12.3 风险与缓解
- 租约库是仲裁权威：任何格式改动都要有完整的负向测试（见验收 3）和对真实库的逐项一致性证明（验收 2）。
- 「夹具全部通过、真实仓库才出问题」的风险：新旧事件混合只在真实库出现，所以必须在真实库副本上做新旧代码的 replay 对比，并构造混合库（旧内嵌 + 新 v3）测试；并且需要一次真实发布来最终验证（发布前由 owner 触发）。
- 直接解析事件的其它调用点（`named_quality_dispatch`、`checkout_telemetry`、`task_checkpoint`、`workflow_integration`、`prospective_event_time_evidence`、`workflow_coordination` 的退役回放）读取的是 checkout/checkpoint/dispatch 租约事件，不含行表；若遇到标记则显式失败，并有测试覆盖。
- 历史 653 MB 保持原样（历史租约的压缩/归档涉及审计证据，需另行设计），所以真实库的 live proof 重放不会立刻变快；收益主要在新写入的库（夹具、之后的每次真实发布）。

### 12.4 验收标准
1. 往返：对每个真实大事件，`compact(expand(stored)) == stored`，且展开结果与旧版内存形式逐值相等。
2. 真实租约库（689 MB）：新代码 `replay()` 与旧代码结论逐项一致（事件数、heads、active、issues）；旧事件继续可读。
3. 篡改反例全部拒绝：blob 字节被改、标记的 `sha256` 或 `row_count` 被改、blob 缺失、标记出现在非固定路径、schema 与标记不匹配、事件 ID 与标记不一致、无读取器遇到标记。
4. 混合库（旧内嵌事件 + 新 v3 事件）`replay` PASS；`_append_event` 的不可变性检查对 v3 事件仍有效。
5. 事件文件体积：22.9 MB → < 1.5 MB；夹具租约库（396 MB / 45 事件）单次重放 5.5 s → 目标 ≤ 0.8 s。
6. 大发布节点（`ff_only`、`replays_candidate_publish`、`lifecycle_binds`、`closeout_admits`、`recovers_independent_main_advance`、`interrupted_after_main_commit`）与 `large-full-profile-publish` 端到端通过（钩子 ×8、worker、恢复、`adopt-published`）。
7. `arch_005`、`devx015` 相关既有测试全部通过。
8. M3 正式 Full 实测墙钟（不发布），并更新本文件。

### 12.5 不做
- 历史租约的压缩/归档；`profile_inspection.captures` 的外置；改变事件缩进写法以外的写入语义。

### 12.6 实现与验证结果（2026-10-05）
- 提交：`fda8239c9`（存储形式、blob、`parse_lease_event` 展开、replay 内共享）、`d3a90710a`（修复：由 replay 得到的 head 派生的事件已持有 `ExternalizedRows`，原判据「适配是否替换了表」使终态事件仍按内嵌 v2 写入，22 MB；
  现判据为「是否存在满足条件的表」，并加回归测试，在上一版内核上该测试失败）、`664dea047`（`system_flow.md`、006d 封印、两处钉死的条目数 3449 → 3450）。
- **验收 1/3/4**：新增 13 个测试全过（往返逐值相等；ID 绑定标记、标记绑定 blob；blob 被改/缺失/标记错位/无读取器/schema 与标记不匹配/行数不符/摘要路径穿越全部拒绝；
  混合库（旧内嵌 + 新 v3）重放 PASS 且同摘要 blob 一次调用只读一次、下一次调用重新读；小于阈值的表仍内嵌；blob 先于事件写入、已存在但被篡改的 blob 不被信任也不被覆盖；共享表不可变、拷贝为普通 list）。
  对真实的 22.9 MB 历史事件：转成新形式后体积 < 1/10，展开后与原内存形式逐值相等，`to_dict()` 与存储形式一致。
- **验收 2**：真实租约库（689 MB、5,521 个事件、835 个 heads）新旧内核重放结论逐项相同（PASS、heads 摘要与 head 事件 ID 摘要相同、无 issue），旧格式事件在新内核下读取结果不变（同一个 396 MB 夹具库：旧内核 5.63 s、新内核 5.65 s）。
- **验收 5（部分达成）**：事件文件 22.9 MB → 夹具里没有 >1.5 MB 的事件；6 个大发布节点的夹具租约库由约 396 MB 降到 4–20 MB（事件 3–11 MB + 一个 9 MB blob）；单次重放 `5.63 s → 1.36 s（−76%）`，**未达 ≤ 0.8 s 的目标**。
  剩余成本（cProfile 占比）：17.9k 行表在每次 replay 里的第一次校验约 0.8 s（S3a 记忆命中其余 17 次，这次校验本身不能省）、每个事件为 `profile_inspection.captures` 重建 `Path` 集合约 0.35 s（replay 内可按内容相等共享，约 −11%，暂未做）、
  事件 ID 与子结构规范序列化约 0.5 s。
- **验收 6**：6 个大发布节点（`ff_only`、`replays_candidate_publish`、`lifecycle_binds`、`closeout_admits`（`large-full-profile-publish`）、`recovers_independent_main_advance`、`interrupted_after_main_commit`）端到端通过，含钩子 ×8、worker、恢复；
  另取 15 个不同家族的重型节点（`workflow_coordination` 的 public_recovery/remote_admission/readiness_input_replaced 等、`workflow_integration`、`workflow_execution` 的 x01/source_generation/mandatory 链、`publication_fence` 的 x02/terminal_confirmation、`governed_development_skill` 的 completed_admission）冒烟，15/15 通过，各节点 220–380 s（M2c 满载 520–840 s）。
  `ff_only` 单节点静机 `814 s`（S3a 1,176 s、O3-d 1,750 s、v26 满载约 4,000 s），S3b 单独贡献 −31%。
- **验收 7**：受影响的 31 个测试文件 + 三个新测试文件的非重型层 `2,680 passed / 1 skipped / 0 failed`（35 分 57 秒）。
- **仍未覆盖**：历史 653 MB 事件不压缩，真实租约库的 live-proof 重放（named-DQ/composer 节点，每次约 10.8 s）不会变快；需要一次真实发布最终验证（发布前由 owner 触发）；验收 8（M3 正式 Full）进行中。


## 13. M3：S1–S3a + S6 + S7b + S3b 之后的正式 Full 实测（2026-10-05，未发布）

### 13.1 过程与结果
- 候选 `2a024eda6`，正式事务 `gov-007-devx022-m3-formal-20261005-v1`（租约 `lease-6f1042882fd8ece9f23c`，`failure_fix_rerun`，parent 为 v20 失败摘要，不发布）。前 5 个阶段通过：named-parent-positive 502 s、contract 267 s、integration 68 s、reproducibility 47 s、architecture-fitness 1,308 s。
- Full：**`1 failed, 14,739 passed, 4 skipped`**，pytest `8,187.33 s`（2 小时 16 分 27 秒），summary 记录 `8,311.37 s`，driver 的 Full 阶段 `8,539 s`；M2c 为 `8,022.86 s`。collected 14,744（M2c 14,721，多出的 23 个是 S3b 13 个、S7b 10 个新测试）。
- 事务按失败释放（证据 `outputs/validation_runtime/gov-007-devx022-m3-full-20261005/test_runtime_summary.json`，sha256 `3e966870cf422692a14d52a12f65b837a2df3389c6a4c6213ecda9cceb162804`；释放回执 `outputs/architecture/integration_revalidation/devx015-v389/claude_m3_release.json`）。
  主检出无残留，`main`/`origin/main` 未动（`1e46e6ac7`）。计数器遥测 `D:/Work/devx022-m3-counters.csv` 保留。

### 13.2 与 M2c 的对比：没有可测的墙钟收益
| 指标 | M2c | M3 |
|---|---|---|
| pytest 墙钟 | 8,022.86 s | 8,187.33 s（+2.0%） |
| 总节点时长 | 30.93 节点小时 | 31.40（把提前被杀的 composer 节点按 M2c 时长补回约 31.63） |
| 重型 / 轻量（节点小时） | 15.95 / 14.98 | 15.80 / 15.60 |
| 窗口内总 CPU（`\Processor(_Total)` 积分，32 逻辑核） | 51.8 CPU 小时（均值 69.7%） | 54.0 CPU 小时（均值 70.8%，+4.2%） |
| 轻量单元耗尽时刻 / 最后一个 worker 结束 | 112.5 / 133.0 分钟 | 118.3 / 135.6 分钟 |
| 尾部空闲（worker 时间） | 3.42 节点小时（9.6%） | 3.98 节点小时（11.0%） |
- **轻量节点各档位普遍慢 4–6%**（≥1 s 的各档 ×1.04–1.06；轻量未触及 S3b 的路径），重型节点合计 ×0.991。总 CPU 同步 +4.2%。这是**主机背景负载差异**（M2c 在夜间、M3 在白天，计数器是系统级），不是代码退化；
  单次运行的噪声约 ±4%，所以 S3b/S7b 的真实收益（见下）在单次 Full 里无法分辨。
- S3b 命中的 7 个大发布节点（秒，M2c → M3）：`closeout_admits[large]` 2,587 → 1,603；`recovers_independent_main_advance` 2,125 → 1,709；`ff_only[full-profile-publish]` 2,118 → 1,625；`ff_only[native-linked]` 2,089 → 1,472；
  `interrupted_after_main_commit` 1,968 → 1,728；`lifecycle_binds[unchanged]` 1,433 → 1,629；`lifecycle_binds[index-replaced]` 1,344 → 1,363。合计 13,664 → 11,129 s（**−0.70 节点小时**）；`lifecycle_binds` 两个不用大 hook capsule，没有受益。
  同一重型通道里其余节点（`replays_candidate_publish` ×9、`completed_admission_full_entry_rejects_real_invalid_context` ×11 等）涨约 10%，抵消了这 0.70 小时。
- 结论：**S3b 的实际价值是体积与可扩展性**（新事件 22 MB → 不到 1.5 MB，真实库不再随每次发布增长约 300 MB），不是这一轮的墙钟。S7b 同理，收益在噪声内。

### 13.3 唯一的失败：composer 激活子进程的 300 秒预算（根因已查明）
- 节点 `tests/test_composer_prospective_capture_contract.py::test_actual_composer_activation_and_readiness_routes_use_real_guards_and_replay`：生产 CLI 子进程（`activate_0`）被测试侧 `communicate(timeout=300)` 杀掉，`TIMED_OUT_AND_CHILD_REAPED`、returncode 1、stdout 0 字节
  （保留的审计 `outputs/architecture/trading_2560_composer_known_snapshot/synthetic/synthetic_composer_routes_8ff6903cf55f408f8c5b5101a0029c5d_v1/test_parent/activate_0/parent.json`）。
- 预算来源：`_ACTUAL_COMPOSER_ROUTE_OVERHEAD_SECONDS = 300`（激活没有 DQ 子进程，预算只有这 300 s；就绪度路径为 300 + 3×120 s）。
- **子进程耗时随真实租约库增长**（`activate_0`，保留审计全量统计）：租约库 ≤ 31 MB 时 33–77 s；真实发布后（2026-10-03 22:49 UTC 起，库 325 → 689 MB）234–262 s（v26、M1、M2c 满载）；M3 超过 300 s；
  安静主机（2026-10-05 02:36 UTC 的 S3b 冒烟）152.6 s。就绪度路径 `readiness_0`：安静 275.5 s，满载 541 s（预算 660 s，已用 82%）。
- 真实租约库的体积时间线（文件 mtime 统计）：2026-10-02 前合计 30.7 MB；10-03 +294.6 MB（13 个 >5 MB 事件），10-04 +363.1 MB（16 个）；现为 **689.2 MB / 5,555 个文件 / 29 个大事件（652.9 MB）**。每次真实重放约 10.8 s（安静、热缓存），
  就绪度路径里 DQ 子进程本身只要 18–60 s（上限 120 s，没有风险），两个 DQ 子进程之间的 67–172 s 是父侧 `_live_parent_proof` 等对真实库的重放与守卫校验。
- 因此这是 DEVX-018 v15–v23 同一类「按空闲主机校准的固定等待在满载下被超过」，且**漏校准的原因是它是模块级命名常量而不是 `timeout=<字面量>`**：`test_host_side_loaded_timeouts_use_calibrated_constants` 只扫描关键字参数与默认值里的字面量，看不到它。
  全量扫描 `tests/*.py` 的模块级 `*_SECONDS`/`*TIMEOUT*` 数值常量，落在 300–1799 s 且不是 `LOADED_HOST_*` 的只有这一个。

### 13.4 修复方案（登记；PROVISIONAL_PENDING_OWNER_REVIEW）
- 最佳方案是让子进程预算不随主机负载漂移（按进度或按子进程 CPU 时间计预算），而不是改墙钟常量；受阻原因：需要 Windows 子进程 CPU 计时与「无进展」判据，属于测试基础设施的新设计，且 CPU 预算对阻塞型挂起无效，仍须墙钟上限。
  因此沿用 DEVX-018 已记录的暂行变通类别（按负载校准的命名常量）：
  - 在该测试文件内定义 `LOADED_HOST_CLI_TIMEOUT_SECONDS = 1800`（与其他测试文件同值同含义），`_ACTUAL_COMPOSER_ROUTE_OVERHEAD_SECONDS` 取该常量，旁注测量依据；生产常量 `CHILD_TIMEOUT_SECONDS = 120` 不动。
  - 行为影响：成功路径不变，只延后「子进程卡死」的判定（最长 1,800 s 而不是 300 s，就绪度路径 2,160 s）；风险：真正的挂起要多等 25–30 分钟才失败；验证：不变量测试 + 最终候选的 Full；
    退出条件：真实库重放成本降低（历史大事件归档/压缩另行立项）或改为进度式预算后撤销。
  - 把「模块级 `*_SECONDS`/`*TIMEOUT*` 数值常量不得落在 300–1799 s 之外的 `LOADED_HOST_*` 命名」加入既有不变量测试（变异检查：临时写回 300 必须失败），避免同类漏校准再在 2 小时的 Full 里才暴露。
- 其余测试侧预算检查：本次 Full 其他节点无超时失败；readiness 路径 82% 占用属于同类风险，由同一常量覆盖。

### 13.5 距 2 小时：结构下界与待 owner 决策
- 结构：总工作量 31.4 节点小时 ÷ 16 worker = 118 分钟，是工作量守恒下界；实测 135.6 分钟 = 118 + 爬坡约 8（前两个 10 分钟桶重型并发 2.8 / 7.0）+ 尾部排空约 9（最后 8 个 540–750 s 的重型节点在 124–136 分钟结束，期间 CPU 降到 36% → 16%）。
- 模型（M3 真实单元时长，composer 节点按 1,286.55 s 补回；基线 7,146 s 对实测窗口 8,134 s，低估 12%，与此前校准一致，下列折算已乘 ×1.138）：
  | 方案 | 模型 | 折算墙钟 |
  |---|---|---|
  | 现状 K=8、间隔 120 s | 7,146 s | 8,134 s |
  | 起跑间隔 120 → 30 s（仍只对前 K 个） | 6,963 s | 7,925 s（−3.5 分钟） |
  | 解除重型上限 / K=6、10、12 | 7,146 / 8,687、7,448、7,797 s | 无收益或更差 |
  | 重型工作量 −10% / −20% / −30% | 6,786 / 6,451 / 6,116 s | 7,723 / 7,341 / 6,960 s |
  | 间隔 30 s + 重型 −20% | 6,293 s | 7,162 s（约 1 小时 59 分） |
- 种子刷新（≈ −5 分钟）须来自已发布候选的 PASS profile，当前没有可用来源；现阶段可做的零合同改动只有起跑间隔（≈ −3.5 分钟，须实测验证起跑突发是否仍安全，O3-c 之后启动校验已大幅变轻）。
- **要进入 2 小时，需要把重型工作量再降约 20–25%，没有不动合同的大头**：
  - A. 夹具内层 Full 用 4 个 worker：每个重型节点 CPU 628 → 约 340 CPU 秒（−46%），重型 −25~−35%，预计 1 小时 50 分上下；须把 `-n 16`/`expected_worker_count`/`expected_collections` 参数化（受保护的发布检查器），并保留 1–2 个专项节点继续用 16；风险：夹具比真实发布更宽松，16 worker 闭合只在真实发布端到端执行（v25/v26 类缺陷）。
  - B. 取消 worker 在 `pytest_sessionfinish` 的第二遍清单校验（控制器在结束时仍完整字节重算）：每个内层 Full 约 −128 CPU 秒（节点 CPU 的 20%，总 CPU 约 −12%），重型 −15~−20%；改变强制验收的 worker 行合同与检查器；失去「运行期间被触碰又还原」的 mtime 检测。
  - C. 不动合同：停在约 2 小时 10 分（起跑间隔调整后），转入 DEVX-021 与发布；A/B 留给后续单独决策。
- **owner 决策点**：A / B / C；以及 §13.4 的暂行校准是否接受为长期做法。我的建议是 C，并行完成 §13.4 与 DEVX-021，随最终候选的 Full 一起发布（owner 触发）。


## 14. M4 最终候选：正式 Full、真实发布与 owner 决策（2026-10-05/06，已发布）

### 14.1 候选与过程
- 候选 `0cbdd9a45`（M3 之上的 composer 预算校准 `c737b2268`、DEVX-021 (A) `207ce5551`、两次重封与弃用清单钉死值 `ae8aecc87`/`0cbdd9a45`），正式事务 `gov-007-devx022-m4-formal-20261005-v1`（`failure_fix_rerun`，parent 为 v20 失败摘要），**这一次不释放、不当作测量**，Full 通过后继续走真实发布。
- 发布前先做了两个重型发布节点的冒烟（`ff_only[full-profile-publish]` 819 s、`interrupted_after_main_commit[full-profile-publish]` 709 s，静机，均通过，与 S3b 之前的静机基线一致），再走重封链（弃用清单钉死值 `…7ac1e70619f26757e73f`、`python_test_file_count = 1400`，两次重封后生成器零差异）。
- 前 5 个阶段：named-parent-positive 440 s、contract 281 s、integration 75 s、reproducibility 47 s、architecture-fitness 1,365 s；Full 阶段 9,058.81 s。

### 14.2 Full 结果与三次同级别实测的对比
- **`14,745 passed, 4 skipped, 0 failed`**（比 M3 多 5 个：DEVX-021 的 4 个预算测试和 1 个模块级常量不变量测试）；pytest `8,697.96 s`（2 小时 24 分 57 秒），summary `8,824.31 s`，composer 路由节点通过（1,388 s）。
| 指标 | M2c | M3 | M4 |
|---|---|---|---|
| pytest 墙钟 | 8,023 s（2:13:42） | 8,187 s（2:16:27，1 失败） | 8,698 s（2:24:57，全过） |
| profile 窗口 | 133.0 分钟 | 135.6 分钟 | 144.0 分钟 |
| 总节点时长（重型 / 轻量） | 30.93（15.95 / 14.98） | 31.40（15.80 / 15.60） | 33.84（16.80 / 17.04） |
| 窗口内总 CPU（均值） | 51.8 CPU 小时（69.7%） | 54.0（70.8%） | 60.6（75.1%） |
| 轻量单元耗尽 / 尾部空闲 | 112.5 分钟 / 3.42 节点小时 | 118.3 / 3.98 | 128.7 / 4.01 |
- M4 相对 M3：轻量 ×1.093、重型 ×1.063，**整机同步变慢**，每节点小时的 CPU 从 1.67 → 1.72 → 1.79 逐次上升；M4 相对 M3 只增加 5 个小测试和 composer 节点跑满（+0.23 节点小时），解释不了 +12% 的 CPU，
  因此无法归因于候选代码，更像宿主侧的背景负载（计数器是系统级，没有逐进程拆分；下次测量应同时记录逐进程 CPU）。**单次 Full 墙钟的噪声约 ±8%**，三次同级别实测给出 2 小时 14–25 分的区间。
- S3b 命中的 7 个大发布节点（秒，M2c / M3 / M4）：`closeout_admits[large]` 2,817 / 1,825 / 1,978；`ff_only[full-profile-publish]` 2,357 / 1,849 / 1,840；`ff_only[native-linked]` 2,289 / 1,695 / 1,921；
  `interrupted_after_main_commit` 2,213 / 1,969 / 2,044；`recovers_independent_main_advance` 2,334 / 1,923 / 2,099；`lifecycle_binds[index-replaced]` 1,575 / 1,587 / 1,621；`lifecycle_binds[unchanged]` 1,655 / 1,842 / 1,821。前五个在 M3、M4 都比 M2c 快 8–30%（profile 节点时长，含 setup 与 teardown），`lifecycle_binds` 不用大 hook capsule，没有受益。
- 结论：DEVX-022 把一次正式 Full 从 v26 的 **3 小时 35 分压到 2 小时 14–25 分（−33%～−38%）**，**没有达到 < 2 小时**。

### 14.3 真实发布（owner 预授权，2026-10-06）
- owner 2026-10-05 的决定（AskUserQuestion）：(1) Full 全过且发布前检查清单全绿，就由我直接执行真实发布（本地 main 快进 + 对 origin/main 的普通 push，不开 PR、不 force-push，任何一项不绿就停下）；(2) 后续提速选 **C：不动正式 Full 合同**。
- 发布前检查清单（DEVX-021 §8.3）6 项全绿：main = 事务 expected_main `1e46e6ac7`；`.git/ORIG_HEAD` = `1144ce36…` ≠ main；无残留 `.lock`；无活动 git/python 进程；真实租约库重放 PASS（5,592 个事件、唯一活动租约为本事务）；距 Full 结束 0.04 小时。
- 步骤与时间：worktree 审计 PASS → `LOCAL_MAIN_FF_PRE` → `local-publish`（WMI 脱离，01:32:45 启动）→ `LOCAL_PUBLISHED`、`recovered: false`（02:44:00，共 **71 分 15 秒**）→ `git fetch origin main`（远端 = `1e46e6ac7`，是候选的祖先）→ `REMOTE_PUSH_PRE` → 治理 CLOSEOUT 预检 PASS（无 blocker/warning）→ **普通 push `1e46e6ac7..0cbdd9a45 main -> main`** → push 后 fetch：
  本地 main = FETCH_HEAD = origin/main = 候选 `0cbdd9a4549987364d5fd2d450d3905083ef1775` → `CLEANUP_PRE` → 事务 `RELEASED/COMPLETED`。**没有任何人工干预**：不需要删除 ORIG_HEAD、不需要清理锁、不需要恢复路径。发布后工作树审计 PASS，已切回任务分支 `claude/gov007-post-baseline-followups`（与 main 同一提交）。
- worker 阶段时间线（相对 worker 启动 01:35:29）：`hooks_created` 01:54:55（+19.5 分钟，v26 约 14 分钟）→ `ready_held` 02:03:57（+9.0）→ `heads_switched` 02:14:46（+10.8）→ `merge_resumed` 02:19:10（+4.4）→ 合并与全部钩子 → `merge_exit 0` 02:38:35（**git 子进程 19.4 分钟**，旧的 1,800 s 上限刚好够）→ 协调端采纳（adopt）02:44:00（+5.4）。
  **worker 共约 63 分钟：旧的 3,600 s 墙钟会在 02:35:29 终止它，早于 `merge_exit`（02:38:35），与 v26 被杀的位置相同**；DEVX-021 (A) 的新常量（10,800 s / 7,200 s）正是这次通过的原因，实测余量约 2.8 倍（worker）和 6 倍（git 子进程）。

### 14.4 S3b 的真实发布验证
- 真实租约库最近 3 小时（M4 的事务与发布）写入 35 个事件，合计 12.79 MB，**最大 0.97 MB**（此前每个大事件 22 MB）；其中 20 个是 `execution_lease_event.v3`（带 `externalized_rows.v1` 标记），15 个是不含大表的 v2；新增 blob 1 个（9.65 MB，内容寻址，被所有 v3 事件共享）。
- 库体积 689.2 MB → 712.2 MB（M3 + M4 的全部事务与发布合计 **+23 MB**；旧法每次真实发布约 +300 MB）。发布之后真实库重放：PASS、5,622 个事件、842 个 head、无活动租约、无 issue，12.6 s（静机；含 20 个 v3 事件的 blob 读取）。历史 29 个大事件（653 MB）按设计不压缩，真实库重放成本约 11–13 s 保持不变。

### 14.5 决策记录与剩余项
- **owner 选择 C**：停在约 2 小时 14–25 分，不为进入 2 小时去改正式 Full 合同。A（夹具内层 4 个 worker，重型 −25~−35%，须参数化受保护的检查器并保留 16 worker 专项节点）与 B（取消 worker 在 `pytest_sessionfinish` 的第二遍清单校验，总 CPU 约 −12%）搁置；
  复审条件：Full 耗时重新成为瓶颈，或 owner 主动要求。任务状态 `IN_PROGRESS` → `BASELINE_DONE`。
- 剩余的不动合同手段（均未做）：(1) 起跑间隔 120 → 30 s（模型约 −3.5 分钟，须先实测起跑突发是否仍安全）；(2) 种子刷新（模型约 −5 分钟）：M4 现在是已发布的 PASS profile，可作为来源，按 TRADING-2564 S5 的严格回放与独立复核规则单独立项；
  (3) 变体共享不可变前缀（S4，模型约 −6 分钟，须逐家族的覆盖等价性论证）；(4) 下次测量同时记录逐进程 CPU，区分测试负载与宿主背景负载。
- 须向 owner 披露（汇总，细节在各自章节）：超时与等待校准仍为 `PROVISIONAL_PENDING_OWNER_REVIEW`（本批新增 composer 子进程预算 1,800 s、发布 worker 10,800 s / git 子进程 7,200 s）；O3 设计偏离与残余风险、O3 跨调用缓存未获批；S6 与 composer 测试路径都以「向 V3 来源清单加一条路径」承接哈希权威；
  同一候选重试曾使用 v20 parent；2026-10-05 02:03 宿主卡死（显示唤醒类，非代码）；上次发布期间一次未预批的空锁删除；pytest 在退出时的 atexit 清理会删除旧的 `%TEMP%\pytest-of-JACK` 编号目录（本轮一次聚焦重跑因此在退出时多花了约 10 分钟）。

### 14.6 临时资源（生命周期记录）
- 已按精确绝对路径白名单删除（释放约 53 GB，脚本 `scratchpad/cleanup_closeout.py`，日志 `D:/Work/devx022-closeout-cleanup.log`）：`%TEMP%\pytest-of-JACK\pytest-21707/-21763/-22449/-22776`（本程序各次验证与冒烟的 basetemp，含 M4 Full 的 27.9 GB）、
  `D:/Work/devx022-s3b`、`-s3b-l`、`-s3b-sm`、`-s3b-h`、`-s3b2`、`-s3b5`、`-s3b6`（S3b 的 basetemp）、`D:/Work/devx022-old`（旧代码导出）、`D:/Work/devx022-s1/bt_n4b`（旧格式 396 MB 租约库基准，数据已记入 §6.4、§12）。已确认无活动进程依赖，均可再生。
- 保留：`D:/Work/devx022-t1`（profile/日志/计数器，12 MB）、各次计数器 CSV（`D:/Work/devx022-m{1,2,2b,2c,3,4}-counters.csv`）、`D:/Work/devx021-prof`、`D:/Work/devx021-smoke-logs`；`D:/Work/devx020-k`（K 实验原始数据 6.3 GB，随 DEVX-020 关闭时清理）；
  `D:/Work/devx015-*`（已登记的证据根，不动）；`outputs/validation_runtime/gov-007-devx022-m*-*` 与 `outputs/architecture/integration_revalidation/devx015-v389/claude_m*`（证据，git 忽略）。
- 仍待 owner 操作：5 个 HKCU `AITS-DEVX015-Test-*` 遗留注册表根（实测仍在）：运行 `D:/Work/Remove-AitsDevx015TestRegistryRoots.ps1`（默认 dry-run，`-Execute` 需键入 DELETE；有 python.exe 运行时会拒绝）；agent 不执行删除。


## 15. 后续流程耗时基线、告警线与租约重放成本模型（2026-10-06，owner 要求持续关注）

owner 2026-10-06：「继续，但也要关注后续的流程耗时是否超出目前的预期，如果发现出现过长耗时仍需要继续分析。」本节固定当前预期、告警线和分析方法；之后每一次受治理的运行都与本节对照，超出就先分析再继续，并把结论写回本节。

### 15.1 基线与告警线（暂行的工程启发式，不是投资解释；每次真实运行后复核）
| 环节 | 预期（实测区间） | 告警线 | 依据 |
|---|---|---|---|
| 任务行围栏事务（acquire → TASK_SOURCE_PRE_WRITE → update → release） | 1–2 分钟 / 个 | > 5 分钟 | 2026-10-06 五个事务；acquire 15 s、checkpoint 28 s、release 56 s、update 数十秒 |
| 重封链单轮（5 个生成器 + 检查） | 约 90 s / 轮，共 3 轮 + 2 次提交 | 单轮 > 3 分钟 | M3/M4 准备链 |
| 正式事务 acquire → `FORMAL_VALIDATION_PRE` + 预检 + 就绪度 | 约 8 分钟 | > 20 分钟 | M4 |
| named-parent-positive | 440–502 s（M2c 1,177 s 受 Defender 影响） | > 900 s | M3/M4 |
| contract / integration / reproducibility | 267–281 s / 68–75 s / 47 s | > 600 s / 200 s / 150 s | M3/M4 |
| architecture-fitness | 1,308–1,408 s | > 2,400 s | M2c/M3/M4 |
| 正式 Full（pytest 墙钟） | 8,023–8,698 s（2:14–2:25） | > 9,500 s（约 2:38） | M2c/M3/M4，单次噪声约 ±8% |
| Full 之后的真实 `local-publish` | 71 分钟（worker 约 63 分钟） | > 100 分钟 | M4 |
| 发布收尾（fetch → push → CLEANUP_PRE → release） | 约 10 分钟 | > 30 分钟 | M4 |
| 非重型层聚焦回归（受影响的 30 个左右测试文件） | 约 36 分钟 | > 60 分钟 | S3b |
- 告警线取「已观测上限 × 1.1」（Full、发布）或「典型值 × 2」（短阶段）。**超过告警线时的处理**：先判断是否宿主噪声（对照同时段的逐进程 CPU 与磁盘），再按环节下钻（重放次数、节点时长对比、尾部），并把原因与结论写回本节；不要先加大超时再说。

### 15.2 租约重放主导受治理流程的耗时（2026-10-06 实测）
- 单次真实库重放 **13.6–14.7 s**（712 MB、5,642 个事件，静机）。围栏命令的墙钟几乎全是重放：`acquire` 1 次重放（15.2 s）、`checkpoint` 2 次（28.3 s）、`release` 4 次（56.0 s），其余耗时不到 1 s（`scratchpad/fence_instrumented.py` 只计数与计时，不改行为）。
- 真实发布的 34 个租约事件按文件时间排列：起步阶段每个事件间隔 3–5 分钟（01:35 → 01:54，6 个事件）；`ready_held` 前一个事件间隔 544 s；`heads_switched`/`merge_resumed` 前后 221–354 s；**git 钩子阶段 10 个事件各约 127–139 s（约 9 次重放/事件）**；
  采纳与收尾 254 s、213 s、115 s、36 s。71 分钟 ≈ 34 个事件 × 约 2 分钟，也就是 **发布耗时 ≈ 重放次数 × 14 s**。
- 一次重放的构成（cProfile，17.3 s）：`parse_lease_event` 10.7 s（JSON 解码 4.5 s、规范序列化与哈希 3.3 s）、`validate_execution` 7.3 s（hook-ready 校验 5.4 s，其中自定义清单路径解析 3.0 s、`pathlib` 1.4 s）。29 个历史大事件（653 MB）约占一半。
- 钩子里的重放来源（读 `record_publication_hook`、`ExecutionLifecycle._head/_append`）：持锁之前的乐观读取（`fence.replay`、`_head`、`_require_original_publication`）一轮，持锁之后的复核（`_head`、`_require_original_publication`、`_append` 里为取前一个事件 ID 再重放一次）一轮；
  `_append_event` 本身不重放。持锁复核是 TOCTOU 防护，不能省；`_append` 里那一次是同一把锁内的重复读取。
- **可选方向（均未实施，待 owner 决定；O3 跨调用缓存仍未获批）**：
  - W 同一原子操作内共享：只在持有仲裁锁的同一次 `atomic` 内复用上一次重放，写入之后立即作废；预计每个钩子 9 → 约 6 次重放，发布约 71 → 约 50 分钟，风险低。
  - S 历史前缀封印：每次仍逐字节哈希全部历史事件文件（约 1–2 s），只跳过已封印前缀的语义重验；重放约 14 s → 约 2 s，发布约 71 → 约 15 分钟，每条围栏命令快约 5 倍，Full 里的 live-proof 节点也受益；但这是一种带完整性绑定的持久化检查点，须 owner 评审信任模型并随内核版本失效。
  - S 的设计要点（读 `_replay_lease_events` 得到）：因果重放是**按租约链独立**进行的（同一 `lease_id` 的事件成链，逐条校验转移与执行转移），跨链只有对 ACTIVE 头的资源冲突检查；所以终态链（RELEASED/REASSIGNED/BLOCKED）的校验结果只取决于它自己的事件字节与内核代码，不依赖其他链。封印可以按租约链记录「事件文件清单 + 各自 sha256 + 头事件 ID + 内核指纹」，重放时对每条已封印的链只做逐字节哈希核对（任何文件缺失、多出、哈希不符都回退到该链的全量校验），内核指纹（校验相关源码的哈希）变化则整体失效。难点在于 `lease_heads` 里每个头都带完整 execution（hook-ready 清单约 10 MB），封印后需要按需延迟加载（只在访问 execution 时解码）。实测逐字节读取并哈希全部 713 MB（5,652 个文件）只要 0.75–1.8 s，对比全量重放 13.6–14.7 s。
  - **封印原型实测（2026-10-06）（READ_ONLY，真实库 5,667 个事件 / 852 条租约链 / 713 MB，`scratchpad/seal_proto.py`，不写库）**：
    全量重放 17.35 s（此时正式 Full 在并行，静机 13.6–14.7 s）；逐字节读取并哈希全部 5,667 个事件文件 1.13 s；只解码 852 个头事件 0.37 s（头事件几乎都很小，大 hook capsule 在中间事件里）；
    从一次全量重放构建封印（每条链的文件清单 + sha256 + 头事件 ID + 头状态）1.47 s；
    **封印重放 4.41 s（只对 1 条非终态链做全量校验，其余终态链只做哈希核对并完整解析校验各自的头事件），`LeaseReplay` 的 status / lease_heads / active_leases / head_event_ids / event_count / issues 与全量重放逐项相同**；
    把某条终态链的一个文件摘要置为不符（内存模拟篡改）后，该链回退为全量校验，结论仍相同（5.55 s，重验 2 条链）。
    即现负载下约 3.9 倍；若头事件也按封印信任（不再逐个解析校验），预计约 2 s。推算：围栏命令（1–4 次重放）15–56 s → 5–18 s，真实发布约 71 → 约 25 分钟。
  - M 路径解析微优化（`_validated_hook_ready_paths` 去掉 `Path` 往返）：约 −2 s/次（−14%），语义不变，收益有限。
- 结论：当前没有环节超出预期（发布 71 分钟在预估的 30–90 分钟内）；但重放成本随库体积线性增长，S3b 之后库每次发布只涨约 +23 MB，增长已受控。
- **2026-10-08 补充（DEVX-023 P）**：重放是 stage 1 与发布的主要成本（17.7）。已实现一条默认关闭的链级并行重放（`AITS_LEASE_PARALLEL_REPLAY`；串行仍是权威，任何异常回退串行；真实库只读对账结果逐项相同，18.3 s → 8.3–9.2 s）。详见 DEVX-023 第 10.8.1、12 节。P4 已发布并实测（17.8）：stage 1 718.6 s（关）→ 568.7 s（开）（−21%，只加速测试进程；受限子进程保持串行）；启用范围的完整链收益由 P6 实测后写回本文。
- **2026-10-08 夜补充（DEVX-023 P6）**：第八次发布的实测（17.10）显示重放成本随每次发布尝试增长；并行重放的开启范围改为由已评审的 `config/architecture/devx_023_parallel_replay_scope.v1.yaml`（`PILOT_BASELINE`，4 个工作进程）给出：开发布命令及其子进程、stage 1 与 `local-publish` worker / 钩子，不开 4 个 xdist tier、正式 Full 与命名 DQ 受限子进程。钩子里的隔离演练：每次重放串行 26.0–26.3 s → 并行 10.2–10.8 s（无回退）。验收与回滚见 DEVX-023 第 12.6 节。
- **2026-10-09 补充（DEVX-023 封印 S，候选 A 默认关闭）**：owner 批准信任模型后实现了封印感知的重放（`AITS_LEASE_SEAL`，`lease_replay_seal.py`，设计与三处遗漏见 DEVX-023 第 10.9 节）。真实库副本上：封印重放与串行全量重放逐项相同，一次重放 28.6–32.7 s → 3.4–4.2 s（约 8–9 倍），构建一次约 31–35 s。它是唯一能停止重放成本随发布增长的方案；在发布链里启用（候选 B）和命名 DQ 受限子进程能否用它（Full 关键路径所在）待 owner 看过候选 A 的实测后决定。

### 15.3 监测方法
- 各阶段起止时间来自 driver 日志、租约事件文件时间戳与 `claude_*` 证据文件，与 15.1 对照。
- 计数器遥测改用 `scratchpad/k_sampler2.ps1`：在系统总量之外增加逐进程名 CPU（python、git、Defender、svchost、System、其他），仍只用性能计数器（不做 WMI 进程轮询），用来把测试负载与宿主背景负载分开；M4 总 CPU 比 M3 高 12% 而无法解释，正是缺少这一拆分。
- 本节的预期与告警线随每次真实运行更新；若某环节反复触线，登记为任务，而不是调高告警线。


## 16. S3b 回归：公开 `to_dict()` 泄漏共享表类型，以及不带 blob 读取器的事件解析（2026-10-06 发现并修复；**影响已发布的 main**）

### 16.1 现象与发现过程
- DEVX-021 P1 候选（`67bfe0ae1`）的正式验证在 stage 1（named-parent-positive）失败，耗时 276 s、2 个节点：
  `test_exact_candidate_production_parent_mints_new_profile_seal_and_complete_closure` 与 `test_exact_candidate_real_clock_synthetic_activation_and_read_only_duplicate`；
  异常 `ValueError: only exact JSON scalar, object and array types are supported`，出处 `named_quality_dispatch.py` 对 `canonical_json_bytes(before)` 的调用。静机重跑同样失败（确定性，不是负载问题）。
- 逐层定位：命名 DQ 的父证明（`_read_lease`）把 `replay.to_dict()` 整体写进证明；真实库的重放结果里，**M4 发布期间写入的第一批 v3 事件所属租约的 head** 带着 `ExternalizedRows`（`list` 的子类，带摘要槽），
  它从 `ExecutionLease.to_dict()` 的浅拷贝里原样漏出；严格规范 JSON 编码器只接受精确的 `list/dict/str/int/float/bool/None`，于是拒绝。
- 同类排查发现第二处：`checkout_telemetry`（遥测快照 CLI）按 `leases/events/*/*.json` 全量解析事件时不带 blob 读取器，对 v3 事件按设计 fail-closed。
  真实库实测：`telemetry snapshot FAILED ... LEASE_EXTERNALIZED_ROWS_UNAVAILABLE: no reader`（1.6 s）。

### 16.2 为什么 M3/M4 的 Full 与发布没有发现
- S3b 之前真实库里**没有** v3 事件；第一批 v3 头是 M4 发布过程中写入的（35 个事件里的 20 个 v3）。M3/M4 的 named-DQ 活体证明都在发布**之前**运行，当时重放里没有携带共享表的 head。
- S3b 的 13 个测试覆盖了存储形式、blob 校验、混合库重放与共享/不可变语义，但**没有一个测试把重放结果交给严格规范 JSON 的消费方**，也没有测试枚举「外部解析点是否带读取器」。这是 S3b 验收标准里缺的一类用例，而不是测试被绕过。
- 因此：这是已发布 main（`0cbdd9a45`）里的缺陷。表现为：真实库里已有 v3 头之后，任何把整库重放交给严格规范 JSON 的流程都会失败（命名 DQ 活体证明，即正式 Full 的 stage 1 与 Full 内对应节点），遥测快照 CLI 也失败。
  它**不**改变任何已存事件的字节、事件 ID、blob 内容或租约语义，也没有产生任何错误的租约/发布结论（失败都是 fail-closed）。

### 16.3 根因（一类问题的两个入口）
1. `ExecutionLease.to_dict()` 对 `execution` 做浅拷贝，重放得到的 head 里共享的 `ExternalizedRows` 作为叶子原样进入「公开字典」。S3b 设计里用 list 子类携带摘要，以便存储形式不必重新序列化大表；
   设计时只检查了「`json.dumps` 能编码」，没有检查「严格编码器拒绝 list 子类」这一类消费方。
2. 外部 `parse_lease_event(payload)` 调用点不带 blob 读取器。设计上这是 fail-closed 的正确行为（事件引用了 blob 却没有读取器就必须失败，而不是误读），但调用点清单没有随「真实库开始出现 v3」而核对。

### 16.4 修复（本批次，无合同变更、存储形式不变）
- `ExecutionLease.to_dict(*, shared_rows=False)`：公开字典用 `_rewrite_rows(execution, _plain_rows)` 把共享表换成精确的 `list`（写时复制；没有共享表时是同一个对象，零开销；head 自己持有的共享表不被改动）。
  `LeaseEvent._body()` 用 `shared_rows=True`，保留共享表与摘要槽，所以存储形式仍由摘要压缩、不重新哈希；`LeaseEvent.to_dict()` 在压缩之后再做一次转换，保证事件字典同样只含精确类型。
- 新增 `ExternalizedRowsReader.beside_event(path)`：由 `<store>/events/<lease>/<event>.json` 推出 `<store>/blobs`；路径不符合该布局时返回 None，此时引用了 blob 的事件仍然 fail-closed，不会从猜测的位置读取。
- 解析点：遥测加载器与校验器、命名 DQ 的两处（`_read_lease`、`verify_retained_named_capture_proof`，后者把守卫的构造提前）、source-handoff 释放事件，都传入所在 store 的读取器。
- 哈希授权：`checkout_telemetry.py` 加入 V3 来源清单（`compatibility_authority.py` 的 `_devx_015_workflow_contract_section`）与 `tests/test_devx_006c_compatibility_authority.py` 的固定集合，由生成器链重封；仅此一条路径，`named_quality_dispatch.py`、`workflow_integration.py`、`parallel_control_kernel.py` 此前已在授权范围内。
- 静态守卫：新增测试枚举 `src/` 与 `scripts/` 下所有 `parse_lease_event(` 调用，要求要么带 `blobs=`，要么在带理由的白名单里（5 处：3 处检查点租约、已退役旧控制根的终态验证、证据捕获的 ACTIVE 源租约事件；都不会携带发布托管表）；白名单条目失效（调用消失或已带读取器）也会失败，防止白名单变成死清单。
- 新测试放在既有的 `tests/test_devx022_lease_event_externalization.py`（该文件不在哈希授权的固定清单里）：
  `test_public_dicts_of_a_replay_hold_only_exact_json_types`（两种位置参数化：顶层胶囊与 `publication_attempts` 内；断言 replay/head/active/event 的所有字典只含精确类型、被严格编码器接受、head 自己的共享表不受影响、事件重新压缩后与存储字节相同）、
  `test_a_derived_event_keeps_its_stored_form_after_the_public_conversion`（先取公开字典再派生事件，仍写成 v3 标记）、`test_checkout_telemetry_reads_externalized_events_of_the_lease_store`（含 blob 缺失仍 fail-closed）、`test_every_stored_event_parse_names_a_blob_reader_or_is_justified`。
  修复前这 4 组（5 个用例中的 4 个）按预期失败，失败原因与现象一致；`test_a_derived_event_keeps_its_stored_form...` 在修复前后都通过，作为「不破坏摘要复用」的保护。

### 16.5 验证（修复提交前的结果；正式 Full 与发布结果在后续章节记录）
- 修复前：4 个新用例按预期失败（`to_dict` 泄漏、遥测不带读取器、解析点枚举），失败原因与现象一致。
- 聚焦回归（项目 venv 的 Python 3.11.9，`-n 16 --dist loadfile`）：第一批 9 个模块（S3b、遥测、内核、租约仲裁、named-DQ dispatch、检查点、事件时间证据、checkout guard）**546 通过 / 0 失败，32:43**（§15.1 基线 27–36 分钟）。
  第二批 16 个模块（发布围栏、dispatch、source preservation 等，排除 `real_full_chain`）1,515 通过、126 失败，全部可解释：
  (1) 118 个 `tests/test_arch_004_refactor_policy.py` 哈希授权测试：`checkout_telemetry.py` 是历史上受哈希固定的源，改动后必须由最新授权段接管，做法同 S6（见 16.4 的授权项），由生成器重封后复跑；
  (2) 2 个 `test_arch_005_source_preservation.py` 的「已提交实现」测试：工作树存在未提交修改时按设计必败（`SOURCE_PRESERVATION_IDENTITY: trusted implementation is not exact committed source`），提交后复跑；
  (3) 6 个 composer 活体节点：需要 `AITS_NAMED_DQ_PUBLICATION_TRANSACTION` / `AITS_NAMED_DQ_SOURCE_LEASE_ID`（只在正式验证里运行，已知前置条件）。
- 真实库探针（只读）：`canonical_json_bytes(replay.to_dict())` 通过（修复前失败）；遥测快照构建 PASS，6,531 个来源、852 条租约，166.8 s（每个事件解析 3 次，修复前对 v2 事件同样如此；只是手动 CLI，不在周期流程里）。
- 观察（须记录）：命名 DQ 父证明把整库重放内嵌进证明，规范字节现在是 **32.4 MiB**（整库重放 19.8 s、`to_dict` + 规范编码 0.70 s；3 个已释放的发布 head 各带 9.5–9.9 MiB 的托管表，其中至少一个来自 v3 事件的展开）。每个活体证明因此写入更大的文件（pre/post 各一份）；
  读写路径没有字节上限，类型修复后可通过；对时长的影响在正式 stage 1 对照 §15.1（named-parent-positive 440–502 s，告警线 900 s）。长期做法是不再把整库重放内嵌进每个证明（例如 DEVX-023 的封印之后只绑定重放摘要），须 owner 评审，本批不做。

### 16.6 教训与流程改动
- 真实库是「会演化的输入」：存储格式一旦进入真实库，所有读真实库的路径都必须有一次「用真实库」的冒烟，而不只是夹具；S3b 当时的真实库验证只覆盖了写入与重放（§14.4），没有覆盖对重放结果的下游消费。已把「公开字典精确类型」与「解析点枚举」作为静态/单元保护固化，后续任何新存储形式都要在验收里加这两类用例。
- 本次排查中有一次误用系统 Python 3.14 运行聚焦回归（得到大量租约仲裁 `STATE_INVALID` 失败），属于已记录的环境陷阱（只能用 `.venv\Scripts\python.exe` 3.11）；这批结果已废弃并用 3.11 重跑，不作为验证证据。

### 16.7 重建候选 `25d026bf6` 的正式验证与发布（2026-10-06；owner 预授权：通过后直接发布，普通推送）
- 候选与过程：先释放失败的 D21 正式事务（`CHECKOUT_RELEASE_DIRTY_UNATTRIBUTED` 要求干净树，故先暂存修复）→ DEVX-022 任务行事务登记缺陷（`3b4cf2bdc`）→ 提交修复（`d45c08f26`）→ 准备事务（一轮重封，提交 `25d026bf6`，第二轮生成器零差异）→ 正式事务（`failure_fix_rerun`，parent = v20 失败汇总）→ 预检与就绪度 PASS → 驱动（WMI 脱离）。
- 阶段耗时（对照 §15.1）：`named-parent-positive` **505 s**（修复前该阶段失败；基线 440–502 s，告警线 900 s；32.4 MiB 的整库重放证明未造成可见影响）、`contract-validation` 258 s、`integration` 56 s、`reproducibility` 36 s、`architecture-fitness` 1,216 s（基线 1,308–1,408 s）。
- **正式 Full：PASS，14,779 通过 / 4 跳过 / 0 失败，pytest 8,495 s（2:21:34）**，runner 8,667 s、驱动 8,934 s；与 M2c/M3/M4（2:14–2:25）同带，未触 9,500 s 告警线；较 v26 的 3:35 为 −34%；仍未达 <2 小时（owner 2026-10-05 选择 C，不改合同）。
- 链路耗时：任务行事务 2:46；准备链约 7 分钟；正式事务到 `FORMAL_VALIDATION_PRE` 4:19；预检 + 就绪度 1:26；`LOCAL_MAIN_FF_PRE` 3:08。
- **真实发布（owner 预授权，普通推送）**：`main = origin/main = 25d026bf6b1031bd65fd90c87b1e6418c82525e9`（`0cbdd9a45..25d026bf6`），事务 `gov-007-d21b-formal-20261006-v1` RELEASED/COMPLETED，**无恢复、无人工干预、无 PR/强推**。发布前清单 6 项全绿；`local-publish` **86 分 13 秒**（19:08:42 → 20:34:55；阶段：`hooks_created` +25 分、`ready_held` +36 分、`heads_switched` +50 分、`merge_resumed` +55 分、`merge_exit 0` +79 分），M4 为 71 分钟（+21%，仍低于 100 分钟告警线，原因与趋势见第 17 节）；`REMOTE_PUSH_PRE` 3:43、CLOSEOUT 预检 40 s、推送 8 s、`CLEANUP_PRE` 52 s、释放 70 s；`LOCAL_PUBLISHED` 之后到释放共 7:45（基线约 10 分钟）。
- 发布后的真实库探针：重放 PASS（5,730 个事件、855 个 head、无活动租约）；`canonical_json_bytes(replay.to_dict())` 通过（新的 v3 事件也无泄漏）；遥测与命名 DQ 路径此前已验证。
- 总 CPU（逐进程计数器，`D:/Work/devx022-d21b-counters.csv`）：Full 窗口 147.2 分钟、平均 68.6%（32 个逻辑核）≈ **53.8 CPU 小时**（M3 54.0、M4 60.6、M2c 51.8）；profile：14,783 个节点、32.52 节点小时。

### 16.8 临时资源（生命周期记录）
- 已按精确绝对路径白名单删除（脚本 `scratchpad/cleanup_d21b.py`，日志 `D:/Work/devx022-d21b-cleanup.log`，释放约 33 GB，用时 5.5 分钟）：`%TEMP%\pytest-of-JACK\pytest-22760/-22922/-23024/-23025`（含本次 Full 的 28 GB basetemp）、`D:/Work/devx022-fx`（批次 2 的 basetemp）、`D:/Work/devx021-light`（4.3 GB）、`devx021-named`、`devx021-orig-head-repro`（P1 的隔离仓库实验，事实已记入 DEVX-021 第 9.1 节）。已确认无活动进程依赖；正式证据在 `outputs/validation_runtime/gov-007-d21b-full-20261006/` 与 `outputs/architecture/integration_revalidation/devx015-v389/claude_d21b_*`（git 忽略）。
- 保留：`D:/Work/devx022-d21b-counters.csv`（逐进程 CPU 计数器）、`devx022-t1`、`devx021-prof`、`devx021-smoke-logs`、`devx020-k`（随 DEVX-020 关闭时清理）、`devx015-*`（已登记的证据根）。仍待 owner 操作：5 个 HKCU `AITS-DEVX015-Test-*` 遗留注册表根（`D:/Work/Remove-AitsDevx015TestRegistryRoots.ps1`，agent 不执行）。
## 17. 发布耗时随每次发布增长：第三次发布的量化分析与选项（2026-10-06，owner 要求持续关注流程耗时）

### 17.1 事实
- 本次 `local-publish` **86 分 13 秒**，M4 为 71 分钟；按租约事件时间戳，发布部分的跨度 74.0 → 89.4 分钟（+15.4 分钟，+21%）。仍低于 100 分钟告警线（§15.1），但趋势明确。
- 两次发布**结构相同**：发布部分各 28 个事件，其中 20 个 >100 KB 的 v3 事件，合计 11.8 MiB。变长的不是事件数或字节，而是每次重放的成本：大事件之间的间隔中位数 129 → 162 s（+26%），均值 170 → 208 s。
- 真实库单次重放：13.6–14.7 s（M4 之前，5,642 个事件）→ **15.9–17.1 s**（现在，5,730 个事件，静机，三次实测）。cProfile（22.4 s，含探针开销）：`parse_lease_event` 14.2 s（其中 `json.loads` 5.7 s、规范序列化与哈希 4.4 s）、`_validate_hook_ready` 69 次共 7.3 s（每次 106 ms，其中路径解析 3.7 s、`pathlib` 构造 48 万次）。
- 每次发布留给库的约 20 个 0.5–0.97 MB 事件，大头**不是**已外置的 `read_file_custodies`，而是两处 S3b 没有外置的大表，在每个晚期事件里各重复一份：`publication_attempts[].hook_capsule.ready.inputs`（约 315 KB）与 `publication_stable_observation.profile_inspection.captures`（1,376 行，约 313 KB）。
- 增长估算：每多一次发布，单次重放约 +3 s（+20%），发布耗时约 +15 分钟；按此趋势，**下一次发布约 105 分钟，会越过 100 分钟告警线**，再下一次约 120 分钟。`local-publish` 的 worker 墙钟（10,800 s）与 git 子进程等待（7,200 s）还够用，但「远大于」的余量会在 5–6 次发布内用完。

### 17.2 命名 DQ 父证明同样随发布增长
- 证明内嵌整库重放（`lease_replay`）：规范字节 32.4 MiB → **42.3 MiB**（本次发布新增一个已释放 head，带 9.86 MiB 的托管表）；每个活体证明写 pre/post 各一份。读写路径没有字节上限，不会失败（stage 1 为 505 s，在 440–502 s 基线的边缘），
  但每次发布 +约 10 MiB × 全部活体证明。长期做法是证明只绑定重放摘要与来源租约，不再内嵌每个历史 head 的 execution（证明合同变更，须评审）。

### 17.3 选项（登记到 DEVX-023，等 owner 决定）
- **S3c（新增）**：把上面两处未外置的大表也按 S3b 的内容寻址机制外置（沿用 owner 2026-10-05 批准的同一机制，不是新的信任模型；旧事件原样可读；新增 marker 位置在旧代码下 fail-closed）。每个晚期事件 ~0.95 MB 预计降到 ~20 KB，每次发布留下的字节 11.8 MiB → <1 MiB，单次重放的逐发布增长大部分消失（目标 +3 s → <+0.5 s）。
- **W**：同一原子操作内共享重放（约 −15% 的重放次数，低风险）。**S**：按租约链封印历史（原型：重放 17.35 s → 4.41 s，结论逐项相同；发布约 86 → 25–30 分钟；须 owner 评审信任模型）。**M**：路径解析微优化（约 −2 s/次）。
- 我的建议顺序：**S3c + W 先做**（保持现有信任模型，一次候选），观察一次发布后再决定 S；若 owner 希望一次性解决量级问题，S 才是答案。

### 17.4 耗时监测的结论
- 本次候选所有环节对照 §15.1 都在预期内，唯一的趋势性偏离是 `local-publish`（71 → 86 分钟）与命名 DQ 证明体积。按本节规则「反复触线 → 登记为任务，而不是调高告警线」：趋势已登记在 DEVX-023（评估为越早做越划算：后面还有 3–4 次发布，按当前趋势合计多花约 4 小时）。

### 17.5 后续实测（2026-10-07，第四次发布，DEVX-023 的 S3c + S3d + W）
- §17.1 预测「下一次发布约 105 分钟，会越过 100 分钟告警线」被改变了：第四次发布的 `local-publish` **54 分 8 秒**（−37%），发布部分租约事件字节 11.85 → 1.13 MiB，每个钩子事件间隔中位数 162 → 107 s。详见 DEVX-023 第 9 节。
- 没有消除的部分：每多一次发布单次重放仍 +1.5–1.8 s（原 +3 s），命名 DQ 父证明 42.3 → 52.2 MiB，`named-parent-positive` 505 → 579 s；两者都在 §15.1 告警线内（发布 54 分 < 100，stage 1 579 s < 900）。按「反复触线 → 登记任务」规则，这两项继续挂在 DEVX-023（封印 S 与证明不再内嵌整库重放），等 owner 决定。
- 本次 Full：14,791 通过 / 0 失败，pytest 2:25:37（基线带 2:14–2:25，边缘），总 CPU 约 57.2 CPU 小时（M2c 51.8、M3 54.0、M4 60.6、d21b 53.8）；阶段 `contract` 264 s、`integration` 78 s、`reproducibility` 48 s、`architecture-fitness` 1,412 s 均在基线带内或边缘。

### 17.6 第五次发布（2026-10-07，DEVX-016 C1）：耗时对照与 stage 1 的原因分析
- **更正（2026-10-07 晚）**：本节「结论」与「处理与登记」里「证明体积决定 stage 1 耗时、约 12 s / MB、外推 860–890 s」的归因，已被 17.7 的逐调用计时否定，以 17.7 为准；耗时对照表、宿主侧与逐组件时间线仍然有效。
- 对照 §15.1（`claude_d24_*` 证据文件的时间戳与 `D:/Work/devx022-d24-counters.csv`；告警线沿用，不调高）：

| 环节 | d24 实测 | §15.1 预期 | 告警线 | 判断 |
|---|---|---|---|---|
| 任务行围栏事务（`TASK_SOURCE_ONLY`，每个事务 2 个任务） | 2:41、2:21 | 1–2 分钟 / 个 | > 5 分钟 | 内（约 1.2–1.3 分钟 / 个） |
| 重封链单轮（5 个生成器 + 前后处理，共 6 轮） | 119–150 s | 约 90 s | > 180 s | 高于典型值 32–67%，未过告警线 |
| 正式事务 acquire → 就绪度结束 | 约 5.5 分钟 | 约 8 分钟 | > 20 分钟 | 内 |
| named-parent-positive（stage 1） | **751 s** | 440–502 s | > 900 s | **高于预期上限 50%，未过告警线**（505 → 579 → 751） |
| contract / integration / reproducibility | 292 / 74 / 48 s | 267–281 / 68–75 / 47 s | > 600 / 200 / 150 s | contract 略高 4%，其余在内 |
| architecture-fitness | 1,370 s | 1,308–1,408 s | > 2,400 s | 内 |
| 正式 Full（pytest 墙钟） | 8,820 s（2:27:00） | 8,023–8,698 s | > 9,500 s | 高于带上限 1.4%，未过告警线；总 CPU 54.4 CPU 小时（d23 57.1、d21b 53.6） |
| 真实 `local-publish` | 57 分 3 秒 | 约 71 分钟 | > 100 分钟 | 内（d23 54 分 8 秒） |
| 发布收尾（REMOTE_PUSH_PRE 3:32 + CLOSEOUT 预检 0:48 + CLEANUP_PRE 0:41 + release 0:43；另有 LOCAL_MAIN_FF_PRE 3:44） | 5:44（不含 owner 手工 push 的等待） | 约 10 分钟 | > 30 分钟 | 内 |

- 超出预期的两项（重封链、stage 1）按本节规则先分析。**重封链**：单轮 119 s（正式窗口，静机：canonical-task-source 8 s、architecture-manifests 38 s、atlas-authority 39 s、report-flow-authority 3 s、compatibility-authority 12 s，前后处理 19 s）到 150 s（准备阶段，与我同时在跑的聚焦回归重叠）；
  绝对值小，六轮合计约 14 分钟；原因未下钻（生成器输入随仓库增长是候选解释，未验证），登记为观察项。
- **stage 1 的结构**：该阶段只有 2 个测试（同一文件 `tests/test_named_data_quality_actual_candidate.py`，日志 `2 passed in 750.66s`），`--dist loadfile` 让它们在同一个 worker 上串行。测试 1 做 2 次活体证明 + 1 个生产 CLI 子进程，测试 2 做 5 次活体证明 + 2 个子进程；
  每次活体证明 = `fence.validate` + `audit_worktree` + `guard.replay()` + `fence.replay(transaction)`，并把整库重放结果（`lease_replay`）内嵌进证明。
- **宿主侧**：stage 1 窗口总 CPU 17.8%、python 平均约 1.25 个核（d21b 1.13、d23 1.25；采样峰值 3.5–4.1 个核），磁盘 1.2 MB/s，可用内存 ≥ 56 GB，Defender 0.3%：单核串行负载，没有饱和或 I/O 压力。python 核·秒 554 → 706 → 923（d21b → d23 → d24，+31%）。
  d24 窗口里 python 平均核数与 d23 相同，没有可见的额外 python 负载，所以与我误起的杂散 pytest 重叠不是原因。（DEVX-016 文档此前写的「python 约 3 个核、1,737→2,313 核·秒」是把采样峰值当成了平均值，已更正；结论不变。）
- **逐组件的时间线**（用两个测试写出的文件的修改时间重建，目录 `outputs/architecture/trading_2564_s3b_prospective_capture/synthetic/`，脚本 `scratchpad/stage1_timeline.py`；单位 s）：

| 组件 | d21b | d23 | d24 | d23 → d24 |
|---|---|---|---|---|
| 测试 1：生产 CLI 子进程 | 79 | 90 | 118 | +31% |
| 测试 1：第二次活体证明（含写出） | 30 | 37 | 49 | +32% |
| 测试 2：第一次活体证明 | 31 | 34 | 47 | +38% |
| 测试 2：第一个 CLI（证明 + 子进程） | 210 | 240 | 308 | +28% |
| 测试 2：第一个 CLI 之后的证明 | 33 | 37 | 45 | +22% |
| 测试 2：第二个 CLI（证明 + 幂等重放子进程 + 证明） | 78 | 88 | 116 | +32% |
| 首个证明完成 → 最后一次写出 | 468 | 535 | 698 | +30% |
| 单个证明文件 `test_parent_pre_guard.json` | 35.70 MB | 46.60 MB | 57.52 MB | +23% |
| `activation_cli_*_parent.json`（每次运行 2 份，`indent=2`） | 170.7 MB | 224.4 MB | 278.2 MB | +24% |

- **结论**：每个组件都随「活体证明的体积」同比例增长，而不是随事件数（5,813 → 5,860，+0.8%）或重放次数（不变）：单次证明约 0.8 s / MB（30 / 35.7、37 / 46.6、49 / 57.5），整个阶段约 12 s / MB，三次完整通过的拟合为 86 s + 11.3 s / MB（35.70 MB → 505 s、46.60 → 579、57.52 → 751）。
  证明体积每次发布 +约 10.9 MB（已释放 head 的托管表被整库重放内嵌，见 17.2、DEVX-023 第 8 节第 2 条），所以 stage 1 每次发布 +约 120 s。**外推**：下一次发布约 860–890 s（告警线 900 s），再下一次约 980–1,030 s（越线）。
  正式 Full 里同一个文件的两个测试同样随之增长（同一 worker 上串行）。
- **没有证明的部分**：证明体积如何变成耗时（`to_dict` 构造、规范序列化与哈希、子进程里的重放校验、57.6 MB 的子进程 stdout 解析）尚未逐调用拆分；两份 278 MB 的 `indent=2` 写出各只要 7 s，不是主要成本。
- **处理与登记**：按「反复触线 → 登记任务」不调高告警线；趋势挂在 DEVX-023（第 8 节第 2 条，本数据提高其优先级）。根治路径须 owner 评审：证明只绑定重放摘要与来源租约、不再内嵌整库重放
  （证明合同变更：`named_quality_dispatch.py`、`named_quality_execution.py`、`prospective_capture_execution.py` 同样定义并消费 `named_dq_existing_parent_proof.v1`，不是只改测试）；封印 S 另外降低重放本身的耗时。
  无合同变更的缓解（仅测试层，可选，待 owner 决定是否先做）：把上述文件拆成两个文件，使 `--dist loadfile` 把两个测试放到两个 worker（阶段约 751 → 约 550 s，测试 2 约 530 s 是瓶颈）。
- **监测计划**：C2 的正式窗口里把 stage 1 用 `stage1_timing_plugin.py`（包住测试进程内的 `FileExecutionLeaseStore.replay` 与 `_live_parent_proof`）单独测一次，把耗时拆成重放、证明构造与序列化、子进程三部分，结果写入本节下一小节；该窗口没有我自己的并发杂活，是干净的对照。

### 17.7 C2 窗口的 stage 1 逐调用计时：更正 17.6 的归因，以及第六次发布（DEVX-016 C2）的耗时对照
- **更正**：17.6 把 stage 1 的增长归因于命名 DQ 证明体积（约 12 s / MB）。C2 窗口里用 `scratchpad/stage1_timing_plugin.py` 对 stage 1 的两个测试做了逐调用计时（在活的 `FORMAL_VALIDATION_PRE` 事务上、冻结驱动启动之前单独跑一次，诊断运行 761.1 s），**该归因被否定**：
  (1) 证明体积按预测长到了 68.40 MB（+10.9 MB，`activation_cli_0_parent.json` 331.9 MB），stage 1 却只有 761 s（冻结驱动里的 stage 1 为 785.5 s，期间我并发跑了一次 32 s 的 cProfile 重放），比 d24 的 751 s 只多 10 s，旧模型预测 860–890 s；
  (2) 测试进程内：租约库重放 14 次（每次活体证明 2 次：`fence.validate` 内 1 次、证明里 `guard.replay()` 1 次），每次 21.43 s，共 300.0 s；`guard.audit_worktree` 7 次共 5.5 s；`support._json_bytes` 21 次共 24.4 s（3%）；`_sha` 43 次共 0.4 s。每次活体证明 ≈ 47 s = 2 次重放 42.9 s + 序列化 3.5 s + 审计 0.8 s；
  (3) 两个测试分别 215.5 s 与 537.4 s；两个生产 CLI 子进程没有被插桩，但它们的耗时与「一次重放」的比值在四次运行里几乎不变（测试 1 的子进程 5.2、5.2、5.2、5.6 倍；测试 2 的第一个 CLI 子进程 11.7、11.8、11.4、11.8 倍；d21b、d23、d24、C2 诊断运行），
  说明它们也由重放主导（约 5 次与 11–12 次，第二个幂等 CLI 约 1 次）。全阶段约 32 次重放 × 21.4 s ≈ 685 s，约占 90%。
- **结论**：**stage 1 ≈ 重放次数 × 单次重放**，证明体积不是主因（证明构造与序列化约 3%）。单次重放（由活体证明耗时反推，扣除序列化与审计）：d21b 约 13.4 s、d23 约 15.4 s、d24 约 21.9 s、C2 21.4 s（直接测量）；d23 → d24 之间有一次台阶，d24 → C2 持平，
  所以 d24 的 751 s 不是我并发杂活造成的异常，而是新水平。空闲机上直接测量：20.4–21.0 s（5,926 个事件、876 条链、721 MB；d23 发布后是 15.4–15.7 s）。
- **cProfile**（一次真实库重放，含约 ×1.5 的探针开销，30.9 s；`scratchpad/profile_replay_now.py`）：`parse_lease_event` 5,926 次共 19.9 s（JSON 解码 7.3 s、规范序列化与哈希 7.0 s）、`validate_execution` 14.7 s（hook-ready 完整校验 33 次共 9.8 s，其中 `_validated_hook_ready_paths` 6.3 s）、
  跨事件转移校验 3.5 s、文件读取与哈希合计约 1.9 s。终态链占 875 / 876。
- **重新外推**：每次发布单次重放约 +1.7 s × 约 32 次 ≈ +55 s；下一次 stage 1 约 840 s，再下一次约 895 s（碰 900 s 告警线）。17.6 里「860–890 s / 980–1,030 s」作废。
- **处理与登记**：封印 S（DEVX-023 第 10 节设计）是杠杆，预计重放 21 s → 约 5 s、stage 1 → 约 280 s、`local-publish` → 约 25 分钟；但封印是一种落盘的「已校验」结论，与 DEVX-018 O3 的决定和 `_replay_uncached` 的不变量直接冲突，所以先要 owner 决定是否改这条边界；
  不改边界的替代是减少重放次数与按租约链并行重放（第 10.8 节，未实测）。W-DQ（证明只留源租约视图）降为可选的证据体积卫生（第 11 节）。owner 2026-10-07 把路线改为 C2 → 封印 S → C3 → C4。
- **重封链单轮的补充**：静机上的一轮是 89 s 生成器时间（canonical-task-source 6.4 s、architecture-manifests 33.5 s、atlas-authority 35.8 s、report-flow-authority 2.1 s、compatibility-authority 11.7 s），含两次检查点共 129 s；17.6 里的 119–150 s 是我并发工作造成的，§15.1 的约 90 s 预期仍成立。
- **第六次发布（C2）的耗时对照**（§15.1；本次没有启动逐进程计数器采样，所以没有 CPU 小时数据，是监测上的缺口）：

| 环节 | C2 实测 | §15.1 预期 | 告警线 | 判断 |
|---|---|---|---|---|
| 任务行围栏事务（`TASK_SOURCE_ONLY`，2 个任务） | 2:09 | 1–2 分钟 / 个 | > 5 分钟 | 内（约 1.1 分钟 / 个） |
| 重封链单轮（5 个生成器，共 3 轮；第 2 轮无检查点） | 129 s、108 s、129 s | 约 90 s（生成器） | > 180 s | 内（生成器 89 s） |
| 正式事务 acquire → 就绪度结束 | 约 5.5 分钟 | 约 8 分钟 | > 20 分钟 | 内 |
| named-parent-positive（stage 1） | **785.5 s**（诊断 761.1 s） | 440–502 s | > 900 s | **高于预期上限 56%，未过告警线** |
| contract / integration / reproducibility | 282 / 69 / 48 s | 267–281 / 68–75 / 47 s | > 600 / 200 / 150 s | 内（contract 高 0.4%） |
| architecture-fitness | 1,348.6 s | 1,308–1,408 s | > 2,400 s | 内 |
| 正式 Full（pytest 墙钟） | 8,883.6 s（2:28:03；runner 8,989.5 s） | 8,023–8,698 s | > 9,500 s | 高于带上限 2.1%，未过告警线；14,974 通过 / 4 跳过 / 0 失败；最慢节点 `test_actual_composer_activation_and_readiness_routes_use_real_guards_and_replay` 1,857.7 s（重放主导） |
| 真实 `local-publish` | 61 分 9 秒 | 约 71 分钟 | > 100 分钟 | 内（预期约 71 分钟；比 d24 的 57 分 3 秒长 7%，随单次重放变长） |
| 发布收尾（本机命令） | 6:05（REMOTE_PUSH_PRE 3:38 + CLOSEOUT 预检 0:47 + CLEANUP_PRE 0:52 + release 0:48；另有 LOCAL_MAIN_FF_PRE 3:25） | 约 10 分钟 | > 30 分钟 | 内（不含 owner 手工 push 的等待） |

### 17.8 第七次发布（DEVX-023 P：链级并行重放）：耗时对照、逐进程核数，以及 Full 的关键路径（2026-10-08）
- **过程**：S3 命令（`scripts/architecture_arch005_publish.py`）第一次完整跑通 A–E（run `p-20261008-v3`，候选 `5150efbac`；`v1` 因我用 `tail -F` 占着 journal 而中止，催生 C2.1；`v2` 因开关开启的诊断发现受限子进程不能开进程池而中止并修复；`v3` 一次通过）。
  最后的 `git push` 由 owner 在终端执行，命令用远端 tip 证明。本次启动了逐进程计数器采样（`D:/Work/devx026-p4-counters.csv`，10 s 间隔，01:48–07:18 JST），补上 C2 的监测缺口。
- **耗时对照**（§15.1；开关默认关闭，所以这些数字是「默认关闭的候选」，不含并行重放的收益）：

| 环节 | 实测 | §15.1 预期 | 告警线 | 判断 |
|---|---|---|---|---|
| A–C：依赖门 → 就绪度（S3 命令，无人值守） | 699 s（prep 链 334 s：依赖门另计 32 s、生成器两轮 142 s / 101 s；正式事务 acquire → 就绪度 331 s） | prep 链约 7 分钟；正式 acquire → 就绪度约 8 分钟 | > 20 分钟 | 内 |
| named-parent-positive（stage 1） | **718.6 s** | 440–502 s | > 900 s | **高于预期上限 43%，未过告警线**（C2 为 785.5 s，其中含我并发的 cProfile） |
| contract / integration / reproducibility | 219.1 / 62.0 / 37.0 s | 267–281 / 68–75 / 47 s | > 600 / 200 / 150 s | 内（低于带下限） |
| architecture-fitness | 1,243.7 s | 1,308–1,408 s | > 2,400 s | 内（略低于带下限） |
| 正式 Full（pytest 墙钟） | **9,078.6 s**（2:31:18；runner 9,177.6 s；驱动阶段 9,378.9 s） | 8,023–8,698 s | > 9,500 s | **高于带上限 4.4%，距告警线 4.4%**；15,027 通过 / 4 跳过 / 0 失败；最慢节点仍是 composer 激活测试 1,985.9 s（重放主导） |
| 真实 `local-publish` | 62 分 57 秒（3,777 s） | 约 71 分钟（S3c/W 之后实测 54–61 分钟） | > 100 分钟 | 内 |
| 发布收尾（本机命令，不含 owner 推送等待） | 560 s：FF_PRE 189.8 + fetch 2.9 + REMOTE_PUSH_PRE 217.0 + CLOSEOUT 预检 50.3 + CLEANUP_PRE 50.4 + release 49.9 | 约 10 分钟 | > 30 分钟 | 内 |
| 整条链 A–E58（到停在等 owner 推送为止，扣除中间的诊断暂停） | 16,752 s（4 小时 39 分）：验证驱动 11,795 s（70.4%）、`local-publish` 3,777 s（22.5%）、A–C 699 s（4.2%）、其余 E 步骤约 481 s | — | — | — |

  S3 命令把 E52（`LOCAL_MAIN_FF_PRE`）和 E56（`REMOTE_PUSH_PRE`）标成 SLOW：它们的基线常量是 60 s，实测一直是 190–220 s（C2 链为 205 / 218 s），这是基线设低了，不是变慢（DEVX-016 的 S3 后续小项 (b)）。
- **开关开启的 stage 1 诊断运行**（活的 `FORMAL_VALIDATION_PRE` 事务上、冻结驱动之前，4 个工作进程）：**568.7 s**，对照同一候选冻结驱动里（关）的 718.6 s 是 −21%（对照 C2 窗口的关闭诊断运行 761.1 s 是 −25%）。
  测试进程内 14 次重放均值 **8.20 s**（串行 21.43 s），共 114.8 s（串行 300.0 s）；`support._json_bytes` 27.7 s、`guard.audit_worktree` 6.9 s。其余约 419 s 在命名 DQ 的受限子进程里（按设计保持串行，约占开启后 stage 1 的 74%，约 19–20 次串行重放）。
- **逐进程核数**（计数器 CSV，32 个逻辑核，取窗口内平均值；「总核数」含宿主背景与采样器自身约 1 个核）：

| 窗口 | 时长 | 总核数 | python 核数 | 说明 |
|---|---|---|---|---|
| stage 1 | 719 s | 3.9 | 1.2 | 几乎单线程：整段是串行重放（约 32 次 × 21 s），机器约 88% 空闲 |
| contract-validation | 219 s | 6.6 | 4.4 | |
| integration | 62 s | 2.6 | 1.0 | |
| architecture-fitness | 1,244 s | 9.4 | 2.8 | |
| Full（驱动阶段） | 9,379 s | 18.5 | 11.3 | 前 10 分钟 7.2 核；10–60 分钟 25.2；60–120 分钟 25.5；最后 36 分钟 8.4 |
| `LOCAL_MAIN_FF_PRE` | 190 s | 2.9 | 1.0 | |
| `local-publish` | 3,777 s | 2.6 | 1.0 | **63 分钟里 python 平均只有约 1 个核**：git 钩子里的串行重放 |
| `REMOTE_PUSH_PRE` | 217 s | 2.4 | 1.0 | |
| 等 owner 推送（采样结束前的 61 分钟） | — | 1.2 | 0.0 | 只有宿主背景与采样器 |

  结论：整条链里有两个大块是单核串行重放——stage 1（719 s）和 `local-publish`（3,777 s，22.5% 的链耗时）；这就是 P6 要评估开启范围的依据（(a) 驱动/围栏命令的子进程环境，(b) local-publish worker 与 git 钩子）。
- **Full 的关键路径**（`test_runtime_profile.json` 的节点起止与 worker_id，逐 worker 时间线）：
  - 10 分钟桶的平均并发：16.0 一直到 +110 分钟，随后 14.2 → 5.4 → 2.2 → 1.0 → 0.05（+110 / +120 / +130 / +140 / +150 分钟）。`tail_idle_total_seconds` 24,609 s（**6.84 节点小时，占 17.0%**），最大空闲 1,896 s。
  - 最后结束的文件是 `tests/test_named_data_quality_candidate.py`：26 个节点、单个 loadfile 单元，在 gw7 上从 +2,098 s 串行跑到 +9,031 s（合计 6,933 s）；另有 gw2（acceptance，到 +8,165 s）与 gw1（integration，到 +8,138 s）是重型通道的尾巴；其余 13 个 worker 在 +7,136 到 +7,694 s 之间结束（轻量积压在 +7,136 s 耗尽）。
  - 六次 Full 的对照：

| Full | 窗口 | 尾部空闲 | 最后结束的文件 | 命名 DQ 候选文件：起跑 / 合计 / 结束 | 重型通道尾巴（acceptance 文件结束） |
|---|---|---|---|---|---|
| M4（10-05） | 8,643 s | 14,426 s（10.4%） | acceptance 8,643 s | 1,960 / 5,353 / 7,450 s | 8,643 s |
| d21b（10-06） | 8,439 s | 15,159 s（11.2%） | acceptance 8,439 s | 1,949 / 5,643 / 7,591 s | 8,439 s |
| d23（10-06） | 8,682 s | 14,067 s（10.1%） | acceptance 8,682 s | 2,090 / 6,346 / 8,436 s | 8,682 s |
| d24（10-07） | 8,657 s | 15,587 s（11.3%） | **命名 DQ 8,657 s** | 2,011 / 6,646 / 8,657 s | 8,474 s |
| d25（10-07，C2） | 8,829 s | 19,484 s（13.8%） | **命名 DQ 8,829 s** | 1,875 / 6,953 / 8,829 s | 8,441 s |
| v3（10-08，本次） | 9,031 s | 24,609 s（17.0%） | **命名 DQ 9,031 s** | 2,098 / 6,933 / 9,031 s | 8,165 s |

  - **原因**：该文件是单个 loadfile 单元（26 个节点在一个 worker 上顺序跑），合计从 M4 的 5,353 s 涨到 6,933 s（+30%，与租约库变大、单次重放变慢同方向），而时长种子（`inputs/architecture/arch_004g2_full_duration_profile.yaml`，PARTIAL_SEED v26，来自 2026-09-26 的 v7 Full）里它只记着 1,000.6 s，
    按「种子时长降序」排在第 15 位，要等前面的文件释放 worker 才在约 +1,900～+2,100 s 起跑；Full 墙钟 = 起跑 + 合计。自 d24 起它就是最后结束的文件，而重型通道的尾巴（acceptance 文件）在 8,165～8,682 s 结束，所以 Full 的墙钟被这个晚起跑的单元多拉长了 180～870 s。
  - **估计**：该文件若在 ≲ +1,200 s 起跑，Full 将由重型通道的尾巴决定：v3 约 8,165 s（−866 s，−9.6%）、d25 约 8,441 s（−388 s）、d24 约 8,474 s（−183 s）。这是用实测的各文件起止时间算出来的上界，不是实测收益。
  - **调度模拟器的不可信**：`scratchpad/sim_sched.py`（真实调度类 + 实测节点时长）对 v3 给出「刷新种子无收益」（7,745 s 对 7,811 s），但它把这个文件在 +364 s 就起跑（实测 +2,098 s），重型文件的起跑时刻也与实测不符，对 v3 低估 14%——前 30 分钟的模拟并不忠实，所以这条预测不能用来否定上面的估计；决定性的实验是用刷新后的种子跑一次 Full。
  - **处理与登记**：登记为 DEVX-022 的候选杠杆（M5：用 `scripts/refresh_partial_duration_profile.py` 机械刷新种子，默认 dry-run；种子是 Full 敏感输入，因此须单独归因——用本节的逐文件起止时间对照，而不是只看总墙钟）。另一个未评估的选项是把该文件加入 split-scope 列表使 26 个节点分散到多个 worker（须先证明节点间无共享状态）。本次发布后不单独为它起一个候选（一次完整候选约 2.5 小时 Full + 1 小时发布），随下一个候选做。（2026-10-08 补记：owner 回复「做」后已执行，见 17.9。）
- **对 §15.1 的影响**：Full 的预期带上限（8,698 s）已被最近四次 Full 连续超过（pytest 墙钟 8,737 → 8,820 → 8,884 → 9,079 s，每次约 +1%～2%），都未过 9,500 s 告警线；原因已定位：命名 DQ 候选文件随租约库变慢（5,353 → 6,933 s），而调度种子过期使它在约 +2,000 s 才起跑，自 d24 起它超过重型通道成为最后结束的文件，之后它每次的增量都直接加到 Full 墙钟上；不是总工作量失控，所以不调告警线，登记杠杆。`LOCAL_MAIN_FF_PRE` / `REMOTE_PUSH_PRE` 各约 3–3.7 分钟是常态，补进监测对照。

### 17.9 M5：时长种子刷新（owner 2026-10-08 批准「做」；并入 C3a 候选）
- **决定**：我在发布后的汇报里提出刷新时长种子（预计 Full 少 180–870 s），并说明没有放进 C3a 是因为受治理清单的写入被自动模式分类器拒绝、且尚未获 owner 确认；owner 回复「做」。分类器随后放行了刷新工具的 `--write`。
- **做法**：`scripts/refresh_partial_duration_profile.py --write`（机械刷新，校验来源 profile、summary 与 inventory 摘要），来源 `outputs/validation_runtime/p-20261008-v3-full/test_runtime_profile.json`（sha256 `517f143f884e7b8de2c821840ff0be87135639ae10f3a7eed9c04bde619816a2`，15,031 nodes / 1,353 files，PASS）；
  写入结果的 sha256 `e3c8545f77dfad878d14767ce616d17db5bdb22f000b5afe0f8317bbf07d8e35`，与此前 dry-run 打印的 `output_sha256` 一致。新种子 `devx_022_m5_full_duration_partial_seed` v27（旧 `devx_018_s1_full_duration_partial_seed` v26 来自 2026-09-26 的 v7 Full，1,334 files）。
  这份来源 profile 是种子的证据，必须随种子保留（git 忽略的 `outputs/validation_runtime/p-20261008-v3-full/` 不得清理）。
- **新旧排序**（观测秒数，按种子降序的前 12 位）：`test_arch_005_integration_publication_fence` 23,207、`test_devx015_workflow_coordination` 14,493、`test_devx015_workflow_execution` 13,092、`test_governed_development_skill` 9,872、
  **`test_named_data_quality_candidate` 6,933（第 5 位；旧种子只记 1,000.6 s，排第 15 位）**、`test_arch_005_task_checkpoint` 5,096、`test_devx015_workflow_integration` 4,942、`test_named_simple_baseline_preview_candidate` 4,261（10 个节点的单元）、`test_composer_prospective_capture_contract` 4,217、
  `test_arch_005_source_preservation` 2,734、`test_research_outcome_access` 2,125、`test_devx015_workflow_acceptance` 1,800。
- **打包决定（披露）**：我原先说「作为独立步骤做」。C3a 的 S3 链当时刚好停在 `C26.readiness`，尚未派发任何验证阶段，所以我没有再等一轮 4.5 小时，而是把 v4 run（候选 `2ff096e5c`，正式事务 `p-20261008-v4-formal`）以 FAILED 释放（没有验证阶段、没有 Full 被派发；证据 `claude_c3a_v4_abandoned.txt`），
  把种子刷新作为独立提交并入候选，用新 run `p-20261008-v5` 重跑整条链。C3a 的其余内容对 Full 时长几乎没有影响（60 个新账本测试约 11 s 且在单个 worker 上，S3 测试几秒），所以归因仍然干净；若 owner 更想要单独的候选，种子提交可以单独还原。
- **预期与验收**（以逐文件起止时间为准，不只看总墙钟；对照 17.8 的六次 Full）：(1) 命名 DQ 候选文件起跑 ≤ +1,200 s（此前 +1,875～+2,098 s）；(2) Full pytest 墙钟 ≤ 8,698 s（§15.1 带上限），预期 −200～−800 s（v3 的重型通道尾巴在 8,165 s，但新顺序会让原来排第 5 的 integration 文件晚几百秒起跑，所以不再宣称 −870 s）；
  (3) 尾部空闲占比 ≤ 12%（v3 为 17.0%）；(4) 0 失败、无新的 hang/超时。**回滚**：任一失败或 Full 变慢超过 5% 且原因指向顺序，则还原种子提交（git revert），并把原因写回本节。
- **风险**：命名 DQ 候选文件更早起跑，会与启动期的重型节点同时占用 CPU（它在旧顺序里也运行在 25 核满载期内，所以负载形态相近）；种子只改文件顺序，不改任何 nodeid、文件内节点顺序或验收语义（`verify_duration_order` 与调度器的重放校验不变）。
- **结果（Full v5，2026-10-08，候选 `5480507f1`；发布阶段被宿主卡死中断，见 DEVX-021 第 10 节）**：Full **通过**（15,095 通过 / 4 跳过 / 0 失败，+68 项是 C3a 的新测试）；profile 总时长 9,343 s、观测窗口 9,289 s、runner 9,473 s、驱动阶段 9,723 s。
  逐文件起止时间对照（秒，自首个节点起跑）：

| 指标 | v3（2026-10-08 02:29–05:04 JST，种子 v26） | v5（12:12–14:53 JST，种子 v27） |
|---|---|---|
| 命名 DQ 候选文件 起跑 / 合计 / 结束 | 2,098 / 6,933 / 9,031 | **433** / **8,856** / 9,289（仍是最后结束的文件） |
| 轻量积压耗尽（8 个 worker 收工） | 7,136 | 7,822 |
| 重型通道的尾巴（acceptance / integration 结束） | 8,165 / 8,138 | 8,300 / 8,208 |
| `test_governed_development_skill` 结束 | 7,540 | 7,989 |
| `test_arch_005_integration_publication_fence` 结束 | 4,561 | 4,848 |
| 观测窗口 | 9,031 | 9,289（+2.9%） |
| 尾部空闲 | 24,609 s（17.0%） | 19,322 s（13.0%），最大空闲 1,467 s（v3 为 1,896 s） |

  **归因（逐节点对照；更正我最初「不是种子造成的」的说法）**：种子按设计生效——命名 DQ 候选文件的起跑提前了 1,665 s；但它自己长到 8,856 s（+1,923 s，+28%），起跑 433 + 8,856 = 9,289，仍然决定墙钟。该文件的 26 个节点在两次 Full 里一一对应，把这 +1,923 s 拆成两部分：
  (1) **共同的变慢约 +19.5%（约 +1,350 s）**：两次都落在繁忙期的前 14 个节点合计 4,735 s → 5,659 s（×1.195）。这与同一次链的 stage 1（909.6 s，+27%；stage 1 与 Full 不重叠）、轻量积压（+9.6%）、Full 窗口里「其他」进程的核数（+22%，见下）方向一致，属于宿主/负载层面，不是种子造成的。（**第二次更正见 17.10**：这个共同变慢不是宿主负载，而是租约库重放成本随发布增长；「不是种子造成的」这一半仍然成立。）
  (2) **约 +570 s 是更早起跑带来的争用（种子的副作用）**：后 12 个节点在 v3 里恰好跑在轻量积压已耗尽的安静期（合计 2,198 s），在 v5 里提前落入 16 个 worker 全忙的窗口（合计 3,197 s，×1.45；`equal_risk_actual_profile_does_not_weaken_canonical_guard` 的 5 个参数节点 217–241 s → 299–485 s，×1.4–2.1）。
  所以早起跑的账是：起跑 −1,665 s，该文件自身 +1,923 s（其中约 +570 s 与排序有关）；窗口净增 +2.9%，低于 5% 的回滚线，**保留种子**。即使没有争用惩罚，该文件（6,933 × 1.195 ≈ 8,285 s，结束于 ≈ 8,718 s）仍是关键路径，只比重型通道的尾巴（8,300 s）晚约 420 s——要让 Full 再明显缩短，必须缩短这个文件本身（见下）。
  计数器（`D:/Work/devx028-c3a-counters.csv` 与 `D:/Work/devx026-p4-counters.csv`，只用性能计数器，约 28 秒一个样本，窗口取 Full runner 的起止时间，32 个逻辑核的均值）：v5 平均 19.56 核（python 11.30、其他 3.96、Defender 1.60），v3 为 18.63 核（11.36 / 3.25 / 1.58）——测试进程的核数几乎相同，多出的约 0.7 核在「其他」进程里。分段（v5 / v3）：前 10 分钟 10.4 / 9.8 核，10–60 分钟 27.0 / 25.7，60–120 分钟 25.3 / 25.5，最后 40 分钟 9.7 / 8.8（只剩命名 DQ 候选文件的一条链，机器约 30% 利用率）。
  **验收对照**：(1) 起跑 ≤ +1,200 s：达成（433 s）；(2) pytest ≤ 8,698 s：未达成（9,343 s，未过 9,500 s 告警线）；(3) 尾部空闲 ≤ 12%：未达成（13.0%，较 17.0% 改善）；(4) 0 失败：达成。**不回滚种子**（回滚条件「变慢超过 5% 且原因指向顺序」不满足）。
- **stage 1 为 909.6 s（略超 §15.1 的 900 s 告警线，v3 为 718.6 s）**：python CPU 时间只多 11%（991 对 890 CPU 秒），平均并行度 1.09（v3 为 1.24），Defender 与磁盘没有争用；该阶段期间用 `store._replay_serial()` 测到单次重放 32.4 s，但当时合同验证阶段正在并行，不能当结论（空闲时 20.1–21.0 s）。
  处理：不调告警线；空闲时再测一次单次重放（待做）。对策方向不变：并行重放（DEVX-023 P6a）。
- **下一个杠杆（M6，登记）**：缩短命名 DQ 候选文件本身——(i) 加入 split-scope 清单让 26 个节点分散到多个 worker（须先证明节点间无共享状态：它们共用同一个活的正式事务，输出目录按 uuid 隔离）；(ii) 让其测试进程内的重放并行（受限子进程仍串行，上限约 −25%）；(iii) 减少每个节点的重放次数。
  预期 Full 落到重型通道的尾巴（8,165–8,300 s，即 −1,000 s 左右）；但这些节点拆开后会与其他 worker 同时争核（上面的争用数据：同一节点在全忙窗口里慢 1.4–2.1 倍），实际收益会小于 1:1，须实测后再下结论。
- **阶段耗时对照**（v5，§15.1）：A–C 12:57（无人值守）；stage 1 909.6 s；contract 284.1 s（预期 267–281）；integration 68.0 s；reproducibility 47.0 s；architecture-fitness 1,374.1 s（预期 1,308–1,408）；Full 阶段 9,723.3 s（pytest 9,343 s）；D31 合计 3 小时 29 分 40 秒；
  E50 24.3 s；E52 `LOCAL_MAIN_FF_PRE` 234.4 s（v3 为 189.8 s，+23%，同样偏慢）；E54 `local-publish` 在 58 分钟处被宿主卡死中断。

### 17.10 第八次发布（DEVX-016 C3a，S3 run `p-20261008-v6`，候选 `e30c62a97`）：耗时对照、第二次更正归因，以及重放成本的趋势（2026-10-08）

**结果**：Full 15,096 通过 / 4 跳过 / 0 失败（+1 项是隐藏窗口启动器的新测试），pytest 9,249.9 s（2:34:09），观测窗口 9,202.5 s。S3 命令 A–E 全部 DONE，68 条 journal，2026-10-08 22:41 由 Claude 在 owner 的「仅本候选」授权下普通推送（`5150efbac..e30c62a97`），本地 main = origin/main = ls-remote = 候选；证据 `claude_c3a_v6_push.log`。

**三次链的耗时对照**（秒；v3 = 第七次发布，v5 = 被宿主卡死中断的那次，v6 = 本次；来源 S3 journal 与 `validation_progress.json`）：

| 项 | v3 | v5 | v6 | 说明 |
|---|---|---|---|---|
| A–C（准备与冻结） | 699 | 780 | 782 | |
| stage 1 named-parent-positive | 718.6 | 909.6 | 873.6 | 重放主导 |
| contract-validation | 219.1 | 284.1 | **206.1** | 未见随库增长 |
| integration | 62.0 | 68.0 | **57.0** | |
| reproducibility | 37.0 | 47.0 | **37.0** | |
| architecture-fitness | 1,243.7 | 1,374.1 | 1,282.0 | |
| Full 阶段（驱动） | 9,378.9 | 9,723.3 | 9,613.5 | |
| pytest（`pytest_output.log`） | 9,078.6 | 9,344.3 | 9,249.9 | |
| D31 合计 | 11,794.7 | 12,580.0 | 12,246.4 | 基线 11,059 |
| E52 `LOCAL_MAIN_FF_PRE` | 189.8 | 234.4 | 234.7 | 基线 210 |
| E54 `local-publish` | 3,777.2 | （被杀） | **4,926.5（82 分）** | 基线 3,240；SLOW |
| E56 `REMOTE_PUSH_PRE` | 217.0 | — | 273.4 | 基线 210；SLOW |
| E57 / E61 / E62 | 50.3 / 50.4 / 49.9 | — | 62.5 / 58.5 / 54.2 | |

**Full 内部**（`test_runtime_profile.json`，自首个节点起跑的秒数）：命名 DQ 候选文件 起跑 / 合计 / 结束 = v3 2,098 / 6,933 / 9,031，v5 433 / 8,856 / 9,289，**v6 411 / 8,792 / 9,203**；轻量积压耗尽 7,136 / 7,822 / **7,645**；其余 15 个 worker 最晚收工 8,165 / 8,300 / **7,989**；观测窗口 9,031 / 9,289 / 9,203；尾部空闲 17.0% / 13.0% / **14.7%**（最大空闲 1,896 / 1,467 / 1,557 s）。
v6 里重型通道已经提前到 7,989 s 收工，命名 DQ 候选文件仍在 9,203 s 才结束，两者相差 **1,214 s**：这条链是 Full 墙钟的唯一决定因素；它拆开或变快多少，Full 就缩短多少（上限约 −20 分钟）。
**§17.9 的验收对照（v6）**：(1) 起跑 ≤ +1,200 s：达成（411）；(2) pytest ≤ 8,698 s：未达成（9,250 s，未过 9,500 s 告警线）；(3) 尾部空闲 ≤ 12%：未达成（14.7%，较 17.0% 改善）；(4) 0 失败：达成。种子保留。

**第二次更正归因（更正 17.9 中「宿主整体偏慢」的说法）**
- 17.9 把该文件 +27% 里约 +20% 的「共同变慢」归因于宿主/负载。v6 否定了这一点：v6 的宿主是安静的（链运行期间我没有做别的重活），**stage 1 之外的各阶段（对重放的依赖小得多）都回到或优于 v3**（contract 206 对 219，integration 57 对 62，reproducibility 37 对 37，architecture-fitness 1,282 对 1,244）；v5 里它们偏慢的那部分（284 / 68 / 47 / 1,374）更可能是链运行期间的并发负载（包括我当时做的测量）造成的。
- 变慢的恰好是**重放主导**的工作：stage 1 +21.6%（873.6 对 718.6 s）、`local-publish` +30%（82 对 63 分钟）、命名 DQ 候选文件的前 14 个节点 ×1.216（v5 为 ×1.195，两次一致，说明是状态而不是噪声）、Full 里最慢的节点（composer 激活测试，重放主导）1,985.9 → 2,410.6 → **2,542.8 s（v3 → v5 → v6，+28%）**。
- 逐节点分解（26 个节点一一对应）：前 14 个节点（三次都在繁忙期）4,735 → 5,759 s（×1.216）；后 12 个节点（v3 里恰好跑在轻量积压耗尽后的安静期）2,198 → 3,033 s（×1.380）；按共同因子外推后仍多出 **359 s**（v5 为 570 s）——这部分是更早起跑落入 16 个 worker 全忙窗口的争用，是种子的副作用（净账：起跑 −1,687 s，窗口 +172 s）。
- 租约库的变化（只读测量）：真实库 6,210 个事件 / 893 条链 / 728 MB；逐链串行重放的合计 26.0 s，其中两条历史大链（361.6 MB 与 292.5 MB）5.77 + 4.41 = 10.2 s（与 10 月 8 日凌晨独立测的 5.8 / 4.7 s 一致，说明机器没有变慢），每条发布链（47–63 个事件、1–13 MB）各 1.2–2.8 s，**永久计入之后的每一次重放**；自 10:00 的第七次发布测量（串行重放 20.14 s、6,076 个事件）以来新增 9 条链 / 138 个事件，合计 +2.7 s（v5 的链 1.22 s、v6 的链 1.42 s，被放弃的 v4 与各准备链接近 0）。
  stage 1 ≈ 32 次重放，+155 s 对应每次重放约 +4.5 s（20.9 → 25.4 s），与「新增链 +2.7 s 加上跨链汇总」同量级；**每一次发布尝试（包括被放弃的）都让之后的每次重放永久变慢约 1.2–2.8 s**。
- **未解决的测量噪声**：晚上 22:50–23:05（owner 的交易软件与 Defender 在后台活动，总 CPU 约 11%，Defender 空闲采样就占一个核的 35%）重测 `store._replay_serial()` 为 25.2–32.0 s（9 次里 3 次在 25–26 s，其余 29–32 s），`guard.replay()` 为 28–35 s。所以「安静时约 25–26 s，晚间后台活动时到 31 s」是目前最好的描述；需要在夜里/清晨安静时再测一次来校准，并检查 Defender 对租约库与 `outputs/` 是否有排除项（读 6,210 个小文件会触发逐文件扫描）。
- **趋势与预测**：按每次尝试 +2–4 s 估算，下一次发布的 stage 1 ≈ 950–1,000 s（越过 900 s 告警线），`local-publish` ≈ 90–95 分钟，Full 的命名 DQ 候选文件 ≈ 9,500–9,700 s。**这不是调参能解决的**：要么并行（DEVX-023 P6，常数 ÷2.3 左右、增长率不变），要么封印（DEVX-023 第 10 节，停止增长，需要 owner 对信任模型的决定）。

**`local-publish` 的 82 分钟时间线**（22:08:29 起为引用事务；J = 启动 21:12:54）：+21.2 min 钩子目录创建；+25 min `hooks_created`；+50 min `merge_resumed`（22:02:59）；+55.6 min 引用事务 `prepared`（22:08:29，锁文件 `HEAD.lock` + `refs/heads/main.lock` 出现）；+58.4 min `committed`（22:11:15，main 前进，锁释放）；+75.2 min worker 结果（22:28:06）；+82.1 min E54 DONE（22:34:59，编排器收养与校验）。持锁窗口只有 **2 分 46 秒**，但被杀/卡死落在这 2 分 46 秒里就会像 v5 那样留下需要人工处理的锁（DEVX-021 第 11 节）。重放主导的各段（钩子目录创建前的 21 分钟、`merge_resumed` 前的 25 分钟、worker 结束后的 7 分钟收养）都是 DEVX-023 P6(b) 的目标。

**隐藏窗口启动器（S3 小项 (e)）的真实验证**：驱动（D30）与 `local-publish` worker 都经新的 `WmiDetachedLauncher` 启动，进程 `MainWindowHandle` = 0，没有 `OpenConsole` / `WindowsTerminal` 进程；启动前用 `ping.exe` 做了一次真实冒烟（有输出、无窗口）。

**结论与排序**：(1) 种子保留；(2) 优先级：**DEVX-023 P6（并行重放的开启范围，覆盖 stage 1、`local-publish` worker 与钩子；预期每次发布 −30 至 −45 分钟，并缩短 `local-publish` 里的重放，包括持锁窗口里的钩子）排在 M6 之前**；M6（缩短命名 DQ 候选文件，最多 −20 分钟 Full）仍然有效，但收益更小；(3) 封印 S 是唯一能停止增长的方案，等待 owner 对信任模型的决定。

### 17.11 第九次发布（DEVX-023 P6，S3 run `p-20261008-v7`，候选 `a63827b28`）：并行重放按角色开启的实测（2026-10-09）

**结果**：Full 15,131 通过 / 4 跳过 / 0 失败（+35 项是 P6 的新测试），pytest 9,676.1 s（2:41:16）；S3 命令 A–E 全部 DONE，68 条 journal，**`slow_steps` 为空**（第一次没有任何 SLOW 步骤）；2026-10-09 04:16 由 Claude 在 owner 的「仅本候选」授权下普通推送（`e30c62a97..a63827b28`），本地 main = origin/main = ls-remote = 候选，证据 `claude_p6_v7_push_authorization.json` 与 `claude_p6_v7_push.log`。整条链 23:48 → 04:18，共 4 小时 30 分（v6 为 5 小时 20 分，**−16%**）；隐藏窗口启动器照常工作，`OpenConsole` / `WindowsTerminal` 进程数为 0，`local-publish` worker 起来后可以看到它派生了 4 个重放工作进程。

**对照表**（秒；v3 = 第七次发布，v6 = 第八次发布，v7 = 本次；「角色」是 DEVX-023 12.6 的开启范围）：

| 项 | v3 | v6 | v7 | v7/v6 | 角色 |
|---|---|---|---|---|---|
| A–C（准备与冻结） | 699 | 782 | **607.8** | 0.78 | `s3_command` 开 |
| C22 formal_generators | 144.1 | 162.9 | 134.5 | 0.83 | `s3_command` |
| C25 governed_preflight | 47.2 | 62.4 | 35.5 | 0.57 | `s3_command` |
| stage 1 named-parent-positive | 718.6 | 873.6 | **744.8** | 0.85 | `validation_stage_1` 开 |
| contract-validation | 219.1 | 206.1 | 244.1 | 1.18 | 关 |
| integration | 62.0 | 57.0 | 63.0 | 1.11 | 关 |
| reproducibility | 37.0 | 37.0 | 39.0 | 1.05 | 关 |
| architecture-fitness | 1,243.7 | 1,282.0 | 1,342.3 | 1.05 | 关 |
| Full 阶段（驱动） | 9,378.9 | 9,613.5 | 10,071.7 | 1.05 | 关（`never_enabled`） |
| pytest | 9,078.6 | 9,249.9 | 9,676.1 | 1.05 | 关 |
| E50 pre_publish_checks | 19.0 | 25.2 | 11.4 | 0.45 | `s3_command` |
| E52 `LOCAL_MAIN_FF_PRE` | 189.8 | 234.7 | **148.0** | 0.63 | `s3_command` |
| E54 `local-publish` | 3,777.2（62:57） | 4,926.5（82:06） | **2,328.6（38:49）** | **0.47** | `local_publish_worker` 开 |
| E56 `REMOTE_PUSH_PRE` | 217.0 | 273.4 | **168.1** | 0.61 | `s3_command` |
| E57 / E61 / E62 | 50.3 / 50.4 / 49.9 | 62.5 / 58.5 / 54.2 | 34.3 / 26.6 / 23.5 | 0.55 / 0.45 / 0.43 | `s3_command` |

**验收（DEVX-023 12.6）**：
- E52 / E56 各降 ≥ 25%：**达成**（−37% / −39%）；
- E54 ≤ 60 分钟：**达成**（38.8 分钟，−52.7%；`local-publish` 里的各阶段几乎都快了一倍：钩子目录创建 +9.2 分钟（v6 为 +21.2）、`merge_resumed` +23 分钟（+50）、引用事务 `committed` +27 分钟（+58.4））；
- journal 无 FAILED：**达成**，且 `slow_steps` 为空；
- stage 1 ≤ 650 s：**未达成**（744.8 s，−14.7%）。我把目标估高了：stage 1 的重放里约七成发生在按设计保持串行的命名 DQ 受限子进程里（P4 诊断时就是这样：测试进程内的重放 21.4 → 8.2 s，约 419 s 留在子进程里），并行只能作用在剩下的约三成；要让 stage 1 再降，要么封印 S，要么另行设计一条在受限子进程里也成立的、经评审的并行路径；
- Full 的 pytest 不变（±3%）：**+4.6%**（9,676 对 9,250）。这不是 P6 造成的：Full 的角色是 `never_enabled`，驱动只给 stage 1 附加环境（stage 结果里的 `environment_additions` 只出现在 stage 1）；变慢的是租约库继续增长带来的重放成本——Full 里最慢的节点 composer 激活测试 2,542.8 → 2,674.2 s（+5.2%），命名 DQ 候选文件 8,792 → 9,207 s（+4.7%），与 v3 → v6 的趋势一致，而且在加速（v3 → v6 的 pytest 为 +1.9%，v6 → v7 为 +4.6%）。

**P6 之后链的构成**（v7）：A–C 608 s（4%）、D31 12,678 s（**78%**）、E50–E63 约 2,900 s（18%）。**Full 现在占链的约八成**，P6 只压了另外两成。

**Full 内部**（`test_runtime_profile.json`）：命名 DQ 候选文件 起跑 +418、合计 9,207、结束 +9,630；其余 15 个 worker 最晚 +8,007 收工，两者相差 **1,623 s**（v6 为 1,214 s，v3 为 866 s）；尾部空闲 18.2%（v6 14.7%，v3 17.0%），最大空闲 1,985 s。也就是说：M6（把命名 DQ 候选文件拆开）现在最多能让 Full 少约 27 分钟（扣掉更多 worker 同时争核之后会小一些），比 17.10 估计的 20 分钟更值。Full 窗口里的 CPU（同一窗口定义）：python 9.73 核（v6 11.77），总 17.80 核（v6 19.63）——尾部那条链越来越长，机器越来越空。

**租约库重放（清理刚结束、Defender 仍在后台时测，6,274 个事件）**：串行 34.2 / 36.6 s，4 个工作进程 12.6 / 13.2 s，8 个 12.0 / 10.3 s，全部与串行逐项相同、无回退（安静时的串行约 26 s，晚间 29–33 s，噪声约 ±20%，见 17.10）。4 与 8 个进程差别在噪声之内，所以 `workers: 4` 保持不变。

**结论与排序**：(1) 保留 P6（`PILOT_BASELINE`，4 个工作进程），把它记为「发布命令 + `local-publish`」这两段的有效杠杆（链约两成，压了一半）；(2) 对占八成的 Full，唯一能停止增长的是封印 S（DEVX-023 第 10 节，待 owner 对信任模型的决定）；备选是 M6 拆开命名 DQ 候选文件；(3) `validation_pre_full_tiers`（四个 xdist tier 合计约 1,690 s，需要先测它们的重放占比）暂不开。

### 17.12 封印 S 候选 A 的发布尝试（S3 run `p-20261009-v1`，候选 `6f77aeae0`）：Full 因与封印无关的负载超时失败，以及对写死的 5 s git 探测超时的加固（2026-10-09）

**结果**：run 在 D31 停止，正式事务 `p-20261009-v1-formal` 以 FAILED 释放（证据 `claude_seal_s1_full_failure.txt` 与 `claude_seal_s1_formal_release.json`），**没有任何东西被推送**（main = origin/main = `a63827b28`）。stage 1 749.3 s（P6 之后的水平，说明封印默认关闭时没有改变行为）、contract 234.1 s、integration 70.0 s、reproducibility 37.0 s、architecture-fitness 1,260.4 s 全部通过；Full：15,212 通过 / 4 跳过 / **1 失败 + 1 错误**（同一个节点的调用与 teardown），pytest 10,026 s（2:47:05，比 v7 的 9,676 s 慢 3.6%）。

**失败的节点**：`tests/test_governed_development_skill.py::test_completed_admission_full_entry_rejects_real_invalid_context[terminal-full-profile-publish]`。该测试在临时 git 夹具里跑一次内层的「强制验收 Full」（`-n 16` 的内层 pytest）。内层 pytest 本身通过（74 passed in 130.54 s），但随后 `record_summary` 抛出 `WORKFLOW_EXECUTION_FULL_RESULT_SUMMARY_BINDING`。

**原因（读夹具留下的产物得出，不是猜的）**：夹具里写出的 `test_runtime_summary.json` 的 `git_commit` 是 `"unknown"`，而执行请求里的 `candidate_sha` 是真实的提交；绑定检查要求两者相等。`scripts/run_validation_tier.py::_git_commit()` 用 `subprocess.run(("git", "rev-parse", "HEAD"), timeout=5)`，超时或失败就返回 `None`，调用处写成 `"unknown"`。这个摘要在约 15:22:30 写出，计数器显示 15:21:29 整机 CPU 100%、约 600 个进程（外层 Full 的 16 个 xdist worker 加内层 Full 的 16 个）——一次普通的 `git rev-parse HEAD` 在饱和的主机上超过了 5 s。teardown 错误是它的后果：夹具的租约留下了未终止的 execution（`LEASE_EXECUTION_NOT_TERMINAL`）。

**与封印无关**：封印默认关闭（`AITS_LEASE_SEAL` 未设时钩子不导入任何东西），前两个候选（`e30c62a97`、`a63827b28`）通过了同一个测试，其余 15,212 个节点通过。这是 Full 运行器里一个写死的、对负载敏感的超时，与 `LOADED_HOST_*` 系列校准同类，只是它在生产脚本里而不是测试里。外层 Full 写自己的摘要时也经过同一个函数，只是那时机器已经空闲，所以一直没出事。

**处理（durable fix，不是重跑碰运气）**：把 5 s 字面量换成具名常量 `GIT_COMMIT_PROBE_TIMEOUT_SECONDS`（挂起保护，不是语义阈值；取 120 s，远高于任何观测到的延迟、远低于 Full 自己的预算），保持「超时或失败 → `None`」的语义不变，并加测试：探测使用该常量、超时 / 非零 / 空输出返回 `None`、真实 git 夹具返回 `HEAD`、函数源码里不再有一位数的字面量超时。随后以 `failure_fix_rerun` 重发（新 run，父 = 本次失败的 Full `outputs/validation_runtime/p-20261009-v1-full/test_runtime_summary.json`）；推送授权只对旧候选 `6f77aeae0` 有效，新候选到 E59 前再问。

**加固已实现（2026-10-09）**：`scripts/run_validation_tier.py` 新增具名常量 `GIT_COMMIT_PROBE_TIMEOUT_SECONDS = 120`（紧邻其他运行器常量，注释写明这是挂起保护而不是语义阈值），`_git_commit()` 用它替换字面量 `5`；超时或失败仍返回 `None`，绑定检查照旧 fail-closed。新增 `tests/test_devx022_git_commit_probe.py`（10 个测试）：探测使用该常量且 argv 恰为 `git rev-parse HEAD`、常量落在 30–600 s 之间、函数源码里没有一位数的字面量超时、超时 / 非零 / 空输出 / git 缺失都返回 `None`、真实临时 git 仓库返回它的 `HEAD`。运行器里剩下的唯一小字面量是 `process.wait(timeout=0.25)`，那是轮询间隔，不是挂起保护。

**验证记录**：新测试 10 passed。与运行器相邻的 `test_devx018_validation_scheduling.py`、`test_validation_parent_run_import.py`、`test_devx015_protected_full_entry.py` 在 `-n 16 --dist loadfile` 下第一次聚合运行是 104 passed / 3 failed（385 s）：失败的是 `test_devx018_validation_scheduling.py` 里三个 real-xdist 用例，它们在临时夹具里起嵌套的 `pytest -n 3`，嵌套进程打印 `13 passed` 之后不退出，外层 120 s 超时。单独重跑与手工复现同样挂住（手工复现里嵌套主进程 50 s 后仍存活，没有 worker 也没有 git 子进程）；清掉遗留的嵌套 pytest 进程之后，同一批用例 3–8 s 通过（该文件加新测试共 68 passed，32 s），随后前台 / 管道 / 后台三种方式启动嵌套命令都在 6 s 内退出。**没有复现，根因未确定**：系统与应用事件日志在该时段没有错误、挂起或显卡重置记录，采样器显示主机当时空闲（CPU 4–12%）。嵌套夹具加载的插件链不导入运行器（已核对），所以与本次改动无关。这是一次未解释的瞬时现象，这里只记录事实、不下结论；如果它在正式 Full 里再次出现，按独立事项调查（先用 `sitecustomize` 里的 `faulthandler.dump_traceback_later` 在挂住时抓线程栈）。
