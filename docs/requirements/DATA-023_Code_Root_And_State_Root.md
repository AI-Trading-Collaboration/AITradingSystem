# DATA-023：代码根和状态根分开

## Owner decision

- `owner_decision:DATA-023:2026-10-13:state_root_separation`
- 原话（Baker，2026-10-13 02:03 JST，线程"日报最小上下文"的两张决定卡片）：
  - 日报运行目录按哪种做法收窄：「直接做发布包」（运行目录不带 git，只放清单内容）；
  - 数据隔离先做到哪一步：「连根目录一起改」（数据路径改成可配置，所有研究目录共用一处数据）。
- 含义：代码目录和数据目录分开。日报以后从不带 git 的发布包运行，数据放在所有 checkout 之外。本任务做第一步：数据路径可配置，默认行为不变。发布包另立任务。

## 背景

- 代码一直按 `PROJECT_ROOT`（源码目录往上两级）拼 `data/`、`outputs/`、`artifacts/`。src 里共 1271 处，分布在 203 个文件，没有任何可配置的数据根。
- 后果：
  - 开发仓、运行副本、pi 克隆各有一份数据。2026-10-12 核对，开发仓价格停在 2026-07-24。
  - 研究产物留在运行副本里。
  - 运行副本只能是完整的 git 克隆，不能只放日报需要的文件。

## 设计

新模块 `ai_trading_system.paths`：

- `CODE_ROOT`：checkout 或发布包目录。
- `state_root()`：存放 `data/`、`outputs/`、`artifacts/` 的目录。
  - 设置了环境变量 `AITS_STATE_ROOT` 时用它，必须是绝对路径。
  - 没设置时等于 `CODE_ROOT`，也就是原来的布局。
- 函数：`state_dir`、`join_root`、`relative_to_root`、`glob_root`、`rglob_root`、`contained_root`、`cli_path`、`state_args`。

规则：

- 只有代码根下的状态目录会改到状态根。别的根保持原样，例如测试的临时目录、回放的镜像目录。
- 没设置 `AITS_STATE_ROOT` 时，每个函数的返回值与它替换掉的 pathlib 表达式相同。
- 相对路径字符串的写法不变。`outputs/...` 在状态根下解析，`config/...`、`docs/...` 在代码根下解析。登记表、报告、manifest 里记的路径都不用改。

改法：

- 机械改写（按语法树）：
  - `X / "data|outputs|artifacts..."` 改为 `state_dir(X, ...)`；
  - `<root> / <表达式>` 改为 `join_root`；
  - `.relative_to(<root>)` 改为 `relative_to_root`；
  - `<root>.glob/rglob` 改为 `glob_root/rglob_root`。
- 手改 18 处：
  - `cli_direct` 入口用 `state_args` 把相对状态路径参数改成状态根下的绝对路径，只在状态根单独设置时生效。
  - `security scan-secrets` 的扫描入口改用 `cli_path`。
  - `immutable_publish` 的根约束读、写、删，对代码根下的状态路径改用状态根（`contained_root`）。
  - 按工作目录解析的 `Path.cwd() / value` 改为 `join_root(Path.cwd(), value)`。
- ruff：`extend-immutable-calls` 加上 `state_dir`、`join_root`。它们出现在参数默认值里，和原来的 pathlib 表达式一样，都在导入时求值。
- 测试 `tests/test_paths.py`：
  - 各函数在两种设置下的行为；
  - 棘轮：src 里除 `paths.py` 外不得再自己拼状态路径。C 区还没改的文件列在 `PENDING_ZONE_C`，这个清单只减不增。

## 步骤

1. **DATA-023-01（B 区）：**
   - 路径模块；
   - B 区 195 个文件的改写；
   - 测试和 ruff 配置。
2. **DATA-023-02（C 区，需要 owner 按 tree 批准）：**
   - 8 个 C 区文件同样改写：`data/quality_capability.py`、`quality_capability_discovery.py`、`quality_consumer_authorization.py`、`quality_execution.py`、`quality_execution_discovery.py`、`quality_issue_attribution_inventory.py`，以及 `trading_engine/brokers/` 下 2 个；
   - `quality_execution.py` 的行号会变，DQ 归因就绪清单和利率归因复核包按 DATA-022 的做法重新生成并重新绑定；
   - 清空 `PENDING_ZONE_C`。
3. **之后另立任务：**
   - 发布包：`RELEASE.json` 取代 git 识别版本（共 7 处）、构建和部署脚本、计划任务入口；
   - `scripts/ops` 的包装脚本和摘要脚本接受状态根；
   - 跟踪在 `data/`、`outputs/` 下的 43 个文件移到代码侧；
   - 数据登记表和价格快照。

## 不改的

- `scripts/` 下 45 处手拼路径，其中 `scripts/ops/daily_scheduler_summary.py` 有意只用标准库。由发布包任务处理。
- 计划任务和运行副本都不变。不设置 `AITS_STATE_ROOT` 时行为与之前完全一致。

## 验收

1. **云端 PR 套件：** 与改动前 main 的失败集合相同，只有环境原因的 22 个，没有新增失败。
2. **云端对照运行：** 同一份初始状态，分别用两种方式跑 daily-plan 和 27 步：
   - 旧布局；
   - 代码目录加单独的状态根，并且代码目录里没有 `data/`、`outputs/`、`artifacts/`。

   两种方式应满足：
   - 每步退出码相同；
   - 产出文件集合相同；
   - 去掉时间戳、运行 id 和绝对路径后，内容和输出相同；
   - 代码目录下没有新写入。
3. **本机：** ship 门禁通过。
4. **DATA-023-02 发布后：** `PENDING_ZONE_C` 为空。

## 进展

- **2026-10-13：** DATA-023-01 在云端完成并测试。
  - PR 套件 2935 passed，22 failed。失败与改动前 main 相同，都是缺 broker 依赖等环境原因。
  - 对照运行在合入 DATA-023-02 改写的完整版本上做：28 条命令的退出码全部相同；两边各 101 个产出文件；stdout 一致。
  - 内容差异只有三类：时间戳、运行 id；报告里绝对路径变长后的字节数；一处真实漏改，`scan-secrets` 的扫描入口按工作目录解析，已用 `cli_path` 修复，复跑后一致。
  - production_effect=none；broker_action=none。
