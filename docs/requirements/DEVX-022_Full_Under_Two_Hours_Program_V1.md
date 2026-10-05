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

