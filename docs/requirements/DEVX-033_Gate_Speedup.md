# DEVX-033 门禁提速：排队顺序、临时目录、影子回测夹具、git 模板仓库、耗时汇总

## 1. 背景

2026-10-15 量了车道门禁（项目文件 `knowledge/claude/ops/gate-timing-2026-10-15.md`）：全量门禁中位 3 分 56 秒，34 次里 12 次有 37–52 秒的停顿不在 pytest 自己的计时里。瓶颈分析（`knowledge/claude/ops/gate-bottleneck-2026-10-15.md`，云端 junit 加 xdist 分配重放）找到五处：

1. xdist `--dist loadfile` 按文件里的测试个数排队，只有 2–5 个测试的慢文件排到最后，16 个进程四成时间在空等（重放 130 秒，理想 78 秒）。
2. 22 个测试各跑一遍影子参数回测，夹具用 32 个候选、约 17 个窗口，占全部测试时间的 29%。
3. 流程工具测试每次门禁启动约 9,900 次 git，Windows 上起进程比 Linux 贵一倍左右。
4. pytest 主进程退出时删除三次之前那次运行的临时目录（一次全量门禁约 4.5 万个文件、314 个临时 git 仓库），一个一个删，不在 pytest 报的耗时里（推测，是那段停顿最可能的原因）。
5. 门禁只留 pytest 最后一行，要分析耗时只能在云端重放。

## 2. owner 决定

owner_decision:DEVX-033:2026-10-15:gate_speedup：Baker 在项目线程「报告和门禁耗时」于 2026-10-15 11:14Z（20:14 JST）说"都改下吧，改完继续验证是否符合预期以及是否有新的瓶颈出现？"，指瓶颈分析里的五条建议。

## 3. 改动

门禁命令和 `config/gov008_ship.yaml` 不变；改动全在 `tests/`（B 区）和文档。不带 `-n` 的 pytest 运行行为不变。

1. **影子回测夹具缩小网格**：`tests/trading_engine/test_shadow_parameter_backtest.py` 的 `_shadow_config_payload` 把 `max_candidates` 从 32 改成 4（18 个文件用它）。没有测试断言网格大小；4 个候选仍然走完逐窗口挑选的路径。
2. **按文件耗时排队**：新的 `tests/conftest.py` 在 `--dist loadfile` 下让每个 xdist 进程按文件耗时从长到短收集测试（同一文件内顺序不变），并关掉 xdist 按测试个数的重排（等于 `--no-loadscope-reorder`）。耗时先取这个工作区 `.pytest_cache` 里的 `aits/file_durations`（每次 1000 个以上测试的 xdist 运行更新其中跑过的文件，没跑的文件保留原值），没有就取 `tests/file_durations.json`（云端测的初值），再没有按每个测试 0.3 秒估。主进程把同一份耗时发给所有进程，所以各进程收集的顺序一致。
3. **临时目录由各进程自己删**：xdist 进程自己的测试全部通过时，结束前删掉自己的临时目录 `popen-gwN`（只读文件先改权限再删，删不掉的跳过）；有失败就全部保留，方便查。主进程退出时再删三次之前的目录，那时里面已经基本是空的。主进程用了 `--basetemp` 时不删。
4. **git 模板仓库**：`test_research_lane_ship.py`、`test_gov008_ship.py`、`test_parallel_lanes_ship.py`、`test_ship_push_recovery.py` 的 `repo` 夹具改为每个进程只建一次仓库（`tests/conftest.py` 的 `git_repo_template`），每个测试复制一份，复制时去掉 git 的示例钩子。只省夹具里的 git 调用（每次门禁约 830 次）；其余 git 调用来自被测试的 `ship.py`、`lane.py`、`accept_checks.py`，不改。`test_agent_lane.py`、`test_agent_publish.py` 的仓库带绝对路径（远端、工作树、提交进仓库的配置），`test_agent_accept_checks.py` 在测试体里建仓库，这三个不改。
5. **耗时汇总**：xdist 运行有 200 个以上测试时，主进程在 pytest 最后一行之前打印 `gate timing:` 三行（进程数、测试数、启动到收集完、测试、测试后、总耗时；各进程跑完的时间；最慢的 5 个文件），同样内容写进 `.pytest_cache` 的 `aits/last_gate_timing`（含最慢 10 个文件和各进程删临时目录的最长耗时）。`ship.py` 的门禁摘要仍取最后一行；门禁日志（`.git` 下的 `gov008-ship/`）保留全部输出。

## 4. 验收

- 新测试 `tests/test_gate_timing.py`：排序（最长的文件在前、未知文件按测试数估、同耗时按收集顺序、文件内顺序不变）；记录的耗时覆盖初值；初值文件可读、列的多数文件存在；只读文件能删；起一个真实的 `-n 1 --dist loadfile` 子运行：执行顺序按耗时，`gate timing` 行在、不是最后一行，缓存里有两份记录，通过时 `popen-gw0` 被删，失败时保留，1000 个以上测试的运行只更新跑过的文件，不到 1000 个测试的运行不改记录的耗时。
- 改动的五个测试文件全部通过；全套 PR 测试在云端通过（只缺电脑上才有的依赖的那些除外，与 main 相同）。
- 发布后在电脑上量：门禁总秒数和 pytest 报的秒数之差（原来 12/34 次有 37–52 秒停顿），全量门禁中位（原来 236 秒），`gate timing` 里各进程跑完的时间是否接近，最慢的文件是哪些；和 2026-10-15 的数字比，找出新的瓶颈。

## 5. 进展

- 2026-10-15：DEVX-033-01 改动和测试在云端完成，交 pi 队列。
