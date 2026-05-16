# OPS-012 daily-run 子命令本地 Python 执行器

任务 ID：`OPS-012`

最后更新：2026-05-17

## 背景

2026-05-12 修复 daily-run 默认 as-of 后，`aits ops daily-run` 已正确选择
`2026-05-11`，且 FMP PIT 独立命令和 SEC companyfacts 独立命令在项目
`.venv\Scripts\python.exe -m ai_trading_system.cli ...` 下可通过。

但本机 PATH 上的 `aits` 指向全局 Python：

```text
C:\Users\32739\AppData\Local\Programs\Python\Python311\Scripts\aits.exe
```

`daily-run` 内部再用父进程 `sys.executable -m ai_trading_system.cli` 执行子命令，
导致子命令继承全局解释器而不是项目本地虚拟环境；PIT/SEC 等大数据步骤出现
Windows access violation 或解释器级异常，无法稳定完成日报链路。

## 目标

- `daily-run` 执行 `aits ...` 子命令时，若项目根目录存在 `.venv` Python，必须优先
  使用项目本地解释器调用 daily-run direct dispatcher。
- 找不到项目 `.venv` 时才回退当前 `sys.executable`，保留非 venv 环境可运行性。
- direct dispatcher 只解析 daily-run 使用的子命令并调用同一批 CLI command function，
  避免 Typer 为单个子命令构造整棵命令树和解析全部 annotations。
- 子命令环境显式设置 `PYTHONMALLOC=malloc`、`PYTHONFAULTHANDLER=1`、
  `PYTHONDONTWRITEBYTECODE=1`，并为每次 `daily-run` 创建独立
  `PYTHONPYCACHEPREFIX`，降低 Windows 本机长流程子进程在 CPython
  allocator/stdlib/字节码缓存路径上的原生崩溃风险，并保留崩溃堆栈行数供诊断。
- 启动子命令前清理项目源码目录下已有 `__pycache__`，避免历史运行留下的损坏
  bytecode cache 被后续子进程读取。
- 执行报告继续展示用户理解的逻辑命令 `aits ...`，metadata 不记录 secret 或 stdout/stderr
  原文。
- 继续避免 Windows 上从 `aits.exe` 父进程递归启动 `aits.exe`。

## 非目标

- 不改变 daily-plan 步骤顺序、质量门禁、PIT/SEC/valuation/score 语义。
- 不修改用户 PATH 或全局 Python 安装。

## 验收标准

- 单元测试覆盖存在 `.venv\Scripts\python.exe` 时 `_execution_command` 使用该解释器；
  不存在时回退 `sys.executable`。
- 单元测试覆盖 daily-run 传给子命令的环境包含 `PYTHONMALLOC=malloc`、
  `PYTHONFAULTHANDLER=1`、`PYTHONDONTWRITEBYTECODE=1`，且
  `PYTHONPYCACHEPREFIX` 位于每次运行独立目录。
- `aits ops daily-run` 默认最新完整交易日能越过 PIT 和 SEC companyfacts；若后续步骤失败，
  报告新的真实门禁原因。

## 进展记录

- 2026-05-12：新增并进入 `IN_PROGRESS`。原因：本机 PATH `aits` 为全局 Python 入口，
  daily-run 子命令未绑定项目本地 `.venv`，导致生产每日链路在 PIT/SEC 子进程出现
  解释器级崩溃。
- 2026-05-12：补充 Windows 子进程运行时稳定化。`PYTHONMALLOC=malloc` 下，PIT 全量
  抓取和 SEC companyfacts 捕获方式均通过；后续又观察到 `unknown opcode`，清理
  `__pycache__` 并设置 `PYTHONDONTWRITEBYTECODE=1` 后 PIT 捕获方式通过。daily-run 将对
  所有子命令注入这些环境变量。
- 2026-05-12：真实 PIT 子流程再次出现 `unknown opcode`；验证设置
  `PYTHONPYCACHEPREFIX=outputs/tmp/pycache/daily_run` 后，PIT fetch/normalized/manifest/
  validation 全部 PASS。daily-run 子命令环境同步加入隔离 pycache 前缀。
- 2026-05-12：将 daily-run 子命令的 `PYTHONPYCACHEPREFIX` 从继承式 `setdefault`
  改为强制覆盖到 run 专用隔离目录，避免父进程已有前缀把损坏 bytecode cache 带入
  PIT/SEC 子流程。
- 2026-05-12：完整 `daily-run` 第二轮已越过 download/PIT/SEC/valuation，但
  `score-daily` 子进程在固定 `outputs/tmp/pycache/daily_run` 前缀下以
  Windows 原生退出码 3221226505 崩溃；同一 `score-daily` 改用新的手工
  pycache 前缀可通过。将 daily-run pycache 前缀细化为每次运行的唯一目录，避免
  前一次子进程崩溃或异常 opcode 缓存污染后续步骤。
- 2026-05-12：真实 PIT direct command 随后暴露 `unknown opcode`，确认
  `PYTHONDONTWRITEBYTECODE` 不能阻止读取源码目录既有 `.pyc`。daily-run 在启动子命令前
  清理 `src/**/__pycache__`，配合 run 专用 `PYTHONPYCACHEPREFIX` 避免读取损坏字节码。
- 2026-05-12：清理 bytecode 后，PIT 子命令又在 CLI 顶层导入链
  `alerts -> scoring.daily -> position_gates -> valuation -> pydantic` 的
  `ValuationSnapshot` class construction 阶段访问冲突。估值快照 schema 改为项目内轻量
  dataclass 校验对象，保留 `model_validate()` / `model_dump(mode="json")` 兼容接口，
  避免无关 PIT 子命令在 import 阶段触发 pydantic 模型构造。
- 2026-05-12：PIT 子命令随后在 Typer 解析整棵 CLI command tree annotations 时访问冲突。
  新增 `ai_trading_system.cli_direct`，daily-run 子命令改为通过该 dispatcher 调用同一批
  CLI command function，报告仍展示逻辑命令 `aits ...`。
- 2026-05-12：direct dispatcher 下 PIT 单独运行可 PASS，但完整 `daily-run` 在
  `download_data` 后启动 PIT 子进程仍以 Windows 原生退出码 3221225477 随机访问冲突。
  当前已确认不是供应商空结果或 PIT 校验失败，而是本机 Python 3.11.4/Windows 子进程运行时
  不稳定；继续下游会违反 fail-closed 编排要求。
