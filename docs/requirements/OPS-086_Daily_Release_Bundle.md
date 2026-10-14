# OPS-086：日报从不带 git 的发布包运行

最后更新：2026-10-14

稳定任务 ID：`OPS-086_DAILY_RELEASE_BUNDLE`

状态：`IN_PROGRESS`（步骤 1～4 由 pi 写代码；步骤 5～7 之后另定）

关联：DATA-023（代码根和状态根分开，本任务是它需求文档步骤 3 的发布包部分）、OPS-082（计划任务、包装脚本、终止恢复的运行副本校验）、
DEVX-029（C 区决定记录）；任务记录 `tasks/OPS-086_DAILY_RELEASE_BUNDLE.yaml`。

## 1. Owner decision

- `owner_decision:DATA-023:2026-10-13:state_root_separation`：Baker 2026-10-13 02:03 JST 选「直接做发布包」（运行目录不带 git，
  只放清单内容）和「连根目录一起改」。本任务做发布包。
- 2026-10-14 07:45 JST，Baker 在 DATA-023 线程的卡片上选「新线程开始」：搬家（计划 P2）和发布包（计划 P3）按新分工做，
  Claude 写需求和验收测试，pi 写代码，Claude 审，Baker 批。
- 步骤 1 要改 3 个 C 区文件里的路径文字（`config/data_sources.yaml`、`src/ai_trading_system/scoring/daily.py`、
  `src/ai_trading_system/scoring/position_gates.py`），只改路径，不改规则和数值。Baker 2026-10-14 08:06 JST 在本任务线程的卡片上
  选「批准」（卡片问题：批准 P2 搬家时一并改 3 个 C 区文件里的 thesis 路径文字吗？）。它属于上面 DATA-023 决定的范围，按 DEVX-029 写成该决定的决定记录，`c_paths` 就是这 3 个文件；
  提交的 `Owner-Decision:` 也写这个决定。

计划全文在项目文件 `knowledge/claude/daily-release-and-data-root-plan-2026-10-13.md`；本文是其中 P2、P3 的需求，两者不一致时以本文为准。

## 2. 背景

- DATA-023 之后，`AITS_STATE_ROOT` 设为别处时，`data/`、`outputs/`、`artifacts/` 都从状态根读写。但 git 还跟踪着这三个目录下的
  43 个文件，其中 35 个不是 `.gitkeep`：
  - `data/external/trade_theses/*.yaml` 6 个：手写的交易 thesis，`score-daily` 读；
  - `data/governance/owner_decision_audit_log.jsonl`：owner 决定的追加式记录，人工 `append` 后提交；
  - `outputs/research_trends/` 下 28 个研究结果：`indicator_family_ablation`（10）、`norgate_trial`（17）、
    `paid_data_due_diligence`（1）。前 27 个由 `config/report_registry.yaml` 的 `artifact_globs` 点名，最后一个由
    `inputs/research_reviews/paid_data_due_diligence_final_matrix.yaml` 和它的测试点名。

  状态根单独设置时，这些文件不在状态根里，`score-daily` 就读不到 thesis。它们是随代码一起审阅的输入和记录，应放在代码侧。
- 运行副本靠 git 识别"跑的是哪份代码、有没有被改过"，共 7 处（第 4 节）。发布包不带 git，要换一个来源。
- 运行副本是完整的 git 克隆，运维手册 9.1 用 `git checkout` 更新。发布包要有自己的构建、核对、部署和回退。

## 3. 步骤 1：跟踪文件搬到代码侧（OPS-086-01，C 区，决定记录）

### 3.1 搬家

| 原位置 | 新位置 |
|---|---|
| `data/external/trade_theses/<名>.yaml`（6 个） | `inputs/trade_theses/<名>.yaml` |
| `data/governance/owner_decision_audit_log.jsonl` | `inputs/governance/owner_decision_audit_log.jsonl` |
| `outputs/research_trends/<族>/<路径>`（28 个，族为 `indicator_family_ablation`、`norgate_trial`、`paid_data_due_diligence`） | `research/_results/<族>/<路径>` |

- 用 `git mv`，内容一个字节都不改。
- `.gitkeep` 留在原处（不设状态根时目录结构照旧）。
- 审计日志放 `inputs/governance/`（已有的治理记录目录），不新建计划里写的 `registry/`；研究结果放 `research/_results/`：
  下划线开头的目录不是研究题目（`research_bench.ledger.question_directories` 跳过它），也不进日报发布包。

### 3.2 读取方跟着改

- 新常量 `ai_trading_system.thesis.DEFAULT_TRADE_THESES_DIR = PROJECT_ROOT / "inputs" / "trade_theses"`。`score-daily --thesis-path`、
  `thesis list|validate|review` 的默认值、`ops_daily` 运行前输入清单（`_pre_run_input_paths`）都用它或同样从代码根拼出的路径。
- `historical_replay`：thesis 的来源改为 `<project_root>/inputs/trade_theses`；回放镜像里的位置
  （`<input_root>/data/external/trade_theses`，以及传给回放 `score-daily` 的 `--thesis-path`）不变。
- `reports/owner_decision_audit_log.py` 的 `DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH` 改为代码根下的新位置。
- `vendor_adapters/norgate_connector.py` 的 `DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT` 改为 `PROJECT_ROOT / "research" / "_results" / "norgate_trial"`
  （它写的是可提交的汇总，与 docs、inputs 一起提交）。
- `config/report_registry.yaml`：28 个研究结果的 `artifact_globs` 改到新位置；`config/rule_cards.yaml` 的
  `source_config_paths`、`inputs/research_reviews/paid_data_due_diligence_final_matrix.yaml` 里的路径同样改。
- C 区（决定记录）：`config/data_sources.yaml` 中 `local_trade_theses` 的 `endpoint` 和 `cache_paths`；`scoring/daily.py` 输入表里
  "交易 thesis"一行；`scoring/position_gates.py` 中 thesis gate 的 `source`。只改路径文字。
- 测试和现行文档（`docs/system_flow.md`、`docs/artifact_catalog.md` 等）里的旧路径一并改。历史记录不改：`docs/archive/`、
  `docs/requirements/`、`docs/owner_reviews/`、两份任务总表、`docs/implementation_backlog.md`、旧版运维手册、`tasks/`。

### 3.3 运行副本更新时的影响

运行副本切到这次提交时，git 会从运行副本的 `data/`、`outputs/` 删除这 35 个原来跟踪的文件，同时在 `inputs/`、`research/_results/`
放出同样内容。这是切换本身的结果，但运维手册 9.1 第 4 条规定更新时不改 `data/`、`outputs/`，所以这次更新要 owner 事先同意，
并先只读核对运行副本 `data/external/trade_theses/` 里没有未跟踪的 thesis（`data/external/*` 被 `.gitignore` 忽略，git status 看不出来）。

### 3.4 验收

- `tests/test_ops086_tracked_state.py`（Claude 写，pi 不改）全过：
  - git 在 `data/`、`outputs/`、`artifacts/` 下只跟踪 `.gitkeep`；
  - 新位置的文件齐全，默认路径指向代码根，设了状态根也不变；
  - 登记表仍点名那 27 个研究结果，尽调矩阵仍点名评分表，旧路径不再出现在现行文件里。
- PR 套件与改动前 main 的失败集合相同。
- `ship.py --dry-run` 为 C 区；发布前按 DEVX-029 写好决定记录（`decisions/DATA-023.2026-10-13.state_root_separation.json`）。

## 4. 步骤 2：发布身份（OPS-086-02，B 区）

### 4.1 `RELEASE.json`

发布包根目录的 `RELEASE.json`（构建时写，第 5 节）：

```json
{
  "schema_version": "aits_release.v1",
  "release_id": "20261014-62502d10c",
  "source_commit": "<40 位小写十六进制>",
  "source_tree": "<40 位>",
  "origin_main_commit": "<构建时本地 origin/main 的提交>",
  "built_at": "<UTC ISO 8601>",
  "manifest_rules_sha256": "<config/release/daily_release.yaml 在该提交的 sha256>",
  "dependencies_sha256": "<包内 pyproject.toml 的 sha256>",
  "files": [{"path": "src/ai_trading_system/paths.py", "sha256": "<64 位>", "size": 1234}],
  "files_sha256": "<按 files 顺序把每行 '<sha256>  <path>\n' 连起来的 sha256>"
}
```

- `release_id` = 源提交的提交时间（UTC）的 `YYYYMMDD` + `-` + 源提交前 9 位。同一提交重建得到同一个编号。
- `files` 按 `path` 排序，`path` 是相对发布包根目录的 POSIX 路径，不含 `RELEASE.json` 本身；不得有绝对路径、`..`、重复。
- 发布包里没有 `.git`，也没有 `data/`、`outputs/`、`artifacts/`。

### 4.2 `ai_trading_system.release_identity`

只用标准库和 `ai_trading_system.paths`（包导入失败时包装脚本也要能核对）。提供：

- `is_release_root(root) -> bool`：`root/RELEASE.json` 存在且 `root/.git` 不存在。有 `.git` 的目录一律按 git checkout 处理。
- `read_release_manifest(root) -> ReleaseManifest`（字段 `release_id`、`source_commit`、`source_tree`、`origin_main_commit`、`built_at`、
  `files`，`files` 是 `ReleaseFile(path, sha256, size)` 的元组）：读并校验，不合格抛 `ReleaseManifestError`（`ValueError` 的子类）。校验：
  - 文件可读、是 JSON 对象，`schema_version` 为 `aits_release.v1`；
  - `source_commit`、`source_tree`、`origin_main_commit` 都是 40 位小写十六进制；
  - `release_id` 形如 `YYYYMMDD-<9 位小写十六进制>`，后 9 位是 `source_commit` 的前 9 位；
  - `files` 是非空列表；每项 `path` 安全（非空、POSIX、不含 `\`、`:`、`..`、`.` 段，不是绝对路径，不是 `RELEASE.json`，第一段不是 `data`、`outputs`、`artifacts`），`sha256` 为
    64 位小写十六进制，`size` 为非负整数（不接受布尔值）；按 `path` 排序且不重复；
  - `files_sha256` 与 `files` 一致（4.1）。
- `release_mismatches(root, manifest=None, *, prefixes=()) -> tuple[str, ...]`：逐个文件重算 sha256，返回排好序的
  `"missing <path>"`、`"changed <path> <实际 sha256>"`；给了 `prefixes` 时只看这些目录（如 `("src", "config", "scripts", "tools")`）。
  清单外多出的文件不算（发布包部署后只读）。
- `find_release_root(path) -> Path | None`：从 `path`（或它所在目录）往上找第一个 `is_release_root` 的目录。
- 命令行 `python -I -m ai_trading_system.release_identity verify [--root <目录>]`：
  - 打印 `release_id=…`、`source_commit=…`、`mismatches=<个数>`，有不一致时再打印前 20 条；
  - 退出码：0 一致；1 有不一致；2 没有 `RELEASE.json`、根目录有 `.git` 或清单不合格。

### 4.3 7 处改为先看发布身份

每处的规则：根目录是发布包（`is_release_root`，或第 1 处的 `find_release_root`）时用 `RELEASE.json`；否则**原来的 git 代码一字不改**，
输出与现在完全相同。

| # | 位置 | 发布包里的值 |
|---|---|---|
| 1 | `core/provenance.git_provenance(anchor)` | `commit`=源提交，`branch`=`release:<release_id>`，`code_modified`=`CODE_PATHS` 下有不一致 |
| 2 | `run_artifacts._git_commit(project_root)` | 源提交 |
| 3 | `ops_daily._git_metadata(project_root)` | `available`=True，`commit`=源提交，`dirty`=有不一致，`status_sha256` 与 `dirty_diff_sha256` = 不一致各行用 `\n` 连接后的 sha256，另加 `release_id` |
| 4 | `shadow/lineage.git_commit_sha()`、`git_worktree_dirty()` | 源提交；有不一致 |
| 5 | `trading_engine/portfolio_candidate_review._git_path_modified(path)` | 文件存在时：不在清单里或 sha256 不同为 True |
| 6 | `platform/operations/runtime_checkout.inspect_runtime_checkout(root)` | `head_commit`=源提交，`origin_main_commit`=`RELEASE.json` 的 `origin_main_commit`，`dirty_entries`=不一致，`head_on_origin_main`=True，新字段 `release_id`；清单不合格抛 `RuntimeCheckoutError("RUNTIME_RELEASE_INVALID", …)` |
| 7 | `scripts/ops/daily_scheduler_run.ps1` 前置检查 | 见 4.4 |

清单不合格时：第 1 处三项都是 `unavailable`；第 2、4 处返回 `None`；第 3 处 `available`=False、`commit`=None、`dirty`=True；
第 5 处为 True；第 6 处抛错（见表）。不会报出清单里的提交号。第 4、5 处没有根目录参数，看模块里的 `PROJECT_ROOT`（测试会替换它）。

终止恢复"必须换一份代码才能重跑"的规则不变：比较的是源提交和父运行 manifest 的 `git_commit`（第 2 处在发布包里也写源提交）。
`engineering_release_candidate.py` 不在日报路径上，不改。

### 4.4 包装脚本和摘要接受状态根

- `daily_scheduler_run.ps1`：
  - 状态根：进程环境的 `AITS_STATE_ROOT` 有值时用它，否则等于运行目录。日志目录改为 `<状态根>\outputs\run_control\scheduler`。
    值不是绝对路径（盘符加 `\` 或 `\\` 开头）时状态根仍取运行目录写日志，前置检查为 `STATE_ROOT_INVALID`。
  - 前置检查：运行目录有 `.git` 时照旧做 git 检查；否则有 `RELEASE.json` 时，要求设了 `AITS_STATE_ROOT` 且不等于运行目录（否则
    `STATE_ROOT_NOT_SET`；`.venv\Scripts\python.exe` 不存在为 `AITS_EXECUTABLE_MISSING`），再用 `.venv\Scripts\python.exe -I -m ai_trading_system.release_identity verify --root <运行目录>` 核对：
    退出码 1 为 `RUNTIME_RELEASE_MODIFIED`，其他非 0 为 `RUNTIME_RELEASE_INVALID`；两样都没有为 `RUNTIME_IDENTITY_MISSING`。
    核对通过时 `runtime_head` 取输出里的 `source_commit`，日志另记 `release_id`。
  - 调用摘要时加 `--state-root <状态根>`，发布包里再加 `--release-id <编号>`。
  - 发布包分支不调用 git；git 检查只在有 `.git` 时做，调用和现在一样。
- `daily_scheduler_summary.py`（仍只用标准库）：新参数 `--state-root`（默认等于 `--runtime-root`）和 `--release-id`；
  所有 `outputs/...` 从状态根读；DQ 指针里的 `receipt_path` 以状态目录开头时从状态根解析；摘要 JSON 加 `state_root`、`release_id`。
- 不设 `AITS_STATE_ROOT` 的 git 运行副本，行为和日志内容与现在相同（只多出摘要 JSON 的两个键）。

### 4.5 验收

- `tests/test_ops086_release_identity.py`（Claude 写，pi 不改）全过：在临时目录造发布包和 git checkout 两种根，逐处核对上表；
  git checkout 下输出与改动前相同；`verify` 命令的输出和退出码；摘要从状态根读；包装脚本的文本检查。
- PR 套件无新增失败。

## 5. 步骤 3：构建、部署、入口（OPS-086-03，B 区）

### 5.1 清单规则 `config/release/daily_release.yaml`

```yaml
schema_version: daily_release_manifest.v1
include:                 # 匹配源提交里跟踪的文件；** 跨目录
  - src/**
  - config/**
  - scripts/ops/**
  - inputs/**
  - pyproject.toml
  - README.md
  - .python-version
  - docs/artifact_catalog.md
  - docs/schema/historical_gap_recovery.v1.schema.json
include_registry_globs: config/report_registry.yaml   # 另加登记表 artifact_globs 匹配到的跟踪文件
exclude:
  - data/**
  - outputs/**
  - artifacts/**
  - "**/__pycache__/**"
  - "**/*.pyc"
production_effect: none
broker_action: none
```

清单由脚本生成，不手写（2026-10-13 手写的第一版漏了 7 个登记项点名的文件）。步骤 5 给登记表分范围后，`include_registry_globs`
只取日报范围的登记项。

### 5.2 构建 `tools/release/build_daily_release.py`

`python tools/release/build_daily_release.py --commit <SHA> --out-dir <目录> [--repo <开发仓>]`，在开发仓运行，只读仓库，成功退出码 0，
拒绝或出错退出码 2（`error=…` 写到 stderr，不留下半个 zip）：

1. 该提交必须可从本地 `refs/remotes/origin/main` 到达，否则拒绝。
2. 用 `git archive <提交>` 取内容（与 checkout 一样按 `.gitattributes` 转换换行，`*.ps1` 为 CRLF），不读工作区。
3. 按 5.1 的规则选文件，规则文件和登记表都读该提交里的版本（`git show <提交>:<路径>`）：
   - `include`、`exclude` 的匹配：`**` 跨目录，`*`、`?` 只在一段路径内；选中 = 匹配某条 `include` 或某个登记表 glob，且不匹配任何 `exclude`；
   - 登记表 glob 按 `glob.glob(pattern)`（不带 `recursive`）的语义匹配跟踪文件：逐段 fnmatch、段数相同，含通配符的段不匹配以 `.` 开头的名字；
   - 文件按路径排序。
4. 写 `RELEASE.json`（4.1）：`release_id` 用提交时间（UTC），`origin_main_commit` 是本地 `origin/main` 的提交，
   `manifest_rules_sha256` 是规则文件在该提交的 sha256，`dependencies_sha256` 是包内 `pyproject.toml` 的 sha256，`built_at` 为
   `YYYY-MM-DDTHH:MM:SSZ`。
5. 输出 `<out-dir>/<release_id>.zip`（条目正好是选中的文件加 `RELEASE.json`，没有目录条目）和 `<release_id>.zip.sha256`
   （内容一行 `<sha256>  <zip 文件名>\n`）。同名 zip 已存在时拒绝，已有文件不动。

另外提供 `release_file_list(repo, commit) -> list[str]`（选中的文件，排好序），供测试在不要求 origin/main 的情况下核对清单。
构建可以用 PyYAML 和 `ai_trading_system.release_identity`（开发仓的 `.venv`）。

### 5.3 部署 `tools/release/deploy_daily_release.py`

只用标准库，由 Baker 在开发仓运行：

```
python tools/release/deploy_daily_release.py --bundle <zip> --daily-root <目录>
    [--state-root <绝对路径>] [--constraints-from <python.exe>] [--unpinned]
    [--python <建 venv 用的解释器，默认当前解释器>] [--task-name "AITradingSystem Daily Run"]
python tools/release/deploy_daily_release.py --activate <release_id> --daily-root <目录> [--task-name ...]
```

接口（测试用）：`main(argv=None, *, run=<执行函数>, now=<返回 UTC datetime 的函数>) -> int`；所有外部命令都经
`run(argv: list[str], *, env=None, cwd=None)` 执行，返回 `subprocess.CompletedProcess`（文本 stdout/stderr）；`venv_python(release_dir)`、
`venv_aits(release_dir)` 返回 venv 里的 `python`、`aits`（Windows 为 `.venv\Scripts\python.exe`、`aits.exe`，其他系统为 `.venv/bin/python`、`aits`）。
成功退出码 0；任何拒绝或失败退出码 2，`current.json` 和 `<daily_root>\run_daily.ps1` 不变。每次（含拒绝）都在
`<daily_root>/deploy_log.jsonl` 追加一行 JSON：`at`、`action`（`deploy`/`activate`）、`outcome`（`ACTIVATED`/`REFUSED`）、`code`（成功为 null）、
`release_id`、`detail`；stdout 打印 `outcome=…` 和 `release_id=…` 或 `code=…`。

部署顺序：

1. 状态根：第一次部署（没有 `current.json`）必须给 `--state-root`，否则 `STATE_ROOT_REQUIRED`；给的值不是绝对路径为
   `STATE_ROOT_INVALID`；以后不给时沿用 `current.json` 的值，给了但与它不同为 `STATE_ROOT_CHANGED`。
2. 没有活动运行：`run(["schtasks", "/query", "/tn", <任务名>, "/fo", "LIST"])` 退出码 0 且有一行 `Status: Running` 为 `TASK_RUNNING`
   （任务不存在时 schtasks 非 0，不算运行）；`<状态根>/outputs/run_control/daily/locks/` 下有文件为 `RUN_LOCK_PRESENT`。
3. 约束来源：给了 `--constraints-from`，或有当前 release 的 `.venv_freeze.txt`，或给了 `--unpinned`；都没有为 `CONSTRAINTS_REQUIRED`。
4. 核对 zip：旁边的 `.sha256` 存在、内容是 `<zip 的 sha256>  <zip 文件名>`，否则 `BUNDLE_SHA256_MISMATCH`；zip 能打开，`RELEASE.json`
   的 `schema_version`、`release_id` 格式、`files`（路径规则同 4.2，含不以状态目录开头）、`files_sha256` 都合格，zip 里除 `RELEASE.json`
   外正好是 `files` 里的文件，每个的 sha256 一致；否则 `BUNDLE_INVALID`。
5. `<daily_root>/releases/<release_id>/` 已存在为 `RELEASE_EXISTS`。以上都在解包前，拒绝时什么都不解、不调用 venv 和 pip。
   然后解包到这个目录（editable 安装会记下绝对路径，所以不先解到临时目录再改名）。
6. 约束：`--constraints-from <py>` 时 `run([py, "-m", "pip", "freeze"])`；否则读当前 release 的 `.venv_freeze.txt`。去掉空行、`#` 行、
   `-e` 行和名字为 `ai-trading-system`（或 `ai_trading_system`）的行，写成约束文件。
7. `run([<--python>, "-m", "venv", <release>/.venv])`（失败 `VENV_FAILED`）；`run([<venv python>, "-m", "pip", "install", "-e", <release>,
   "-c", <约束文件>])`，`--unpinned` 时不带 `-c`（失败 `PIP_INSTALL_FAILED`）；`run([<venv python>, "-m", "pip", "freeze"])` 的输出写到
   `<release>/.venv_freeze.txt`（失败 `PIP_FREEZE_FAILED`）。
8. `run([<venv python>, "-I", "-m", "ai_trading_system.release_identity", "verify", "--root", <release>])` 退出码 0（否则 `VERIFY_FAILED`）。
9. 清单里的文件和 `RELEASE.json` 去掉写权限（`.venv` 不动）。
10. `run([<venv aits>, "ops", "daily-plan", "--fail-on-missing-env", "--output-path", <临时目录>/daily_ops_plan.md], env=<当前环境加
    AITS_STATE_ROOT=<状态根>、PYTHONDONTWRITEBYTECODE=1>, cwd=<release>)` 退出码 0（否则 `DAILY_PLAN_FAILED`）。临时目录不在状态根和
    release 目录里，用完删除；计划报告不写进生产状态。
11. 原子替换 `<daily_root>/current.json`：`schema_version: aits_daily_current.v1`、`release_id`、`previous_release_id`（原来的
    `release_id`，第一次为 null）、`state_root`、`source_commit`、`activated_at`（`now()`，`YYYY-MM-DDTHH:MM:SSZ`）；再把该 release 的
    `scripts/ops/run_daily.ps1` 原子复制到 `<daily_root>/run_daily.ps1`。

- 第 6～10 步失败时，release 目录改名为 `<release_id>.failed-<now() 的 YYYYMMDDTHHMMSSZ>`，不删除。
- `--activate <release_id>`（回退或切换）：要有 `current.json`；第 2 步照做；`releases/<release_id>/RELEASE.json` 存在（否则
  `RELEASE_MISSING`）且第 8 步通过；再做第 11 步。不再安装。
- 部署从不删除任何 release 目录。成功后 `releases/` 下的 release 超过 3 个时，除最新 3 个、当前和上一个以外的逐个打印
  `cleanup_candidate=<release_id>`，由 owner 决定。

### 5.4 入口 `scripts/ops/run_daily.ps1`

部署后在 `<daily_root>/run_daily.ps1`，计划任务直接运行它：读 `current.json`；release 目录不存在或文件不合格时写
`<daily_root>/run_daily_error.log` 并退出 2；设 `AITS_STATE_ROOT`=`state_root`、`PYTHONDONTWRITEBYTECODE=1`；调用
`releases\<release_id>\scripts\ops\daily_scheduler_run.ps1 -Window <同一窗口>`，以它的退出码退出。ASCII 文件。

### 5.5 计划任务注册

`register_daily_scheduler_task.ps1` 加参数 `-DailyRoot`：给了时任务运行 `<DailyRoot>\run_daily.ps1`（要求该文件和
`current.json` 存在），不给时照旧指向运行副本。"只允许一个调度来源"的检查同时认 `daily_scheduler_run.ps1`、`run_daily.ps1`、
`ops daily-run`。

### 5.6 运维手册

第 9 节加 9.3"发布包"：构建、部署、回退、切换计划任务的命令和前提；9.1 保留给现在的运行副本，直到切换（步骤 7）完成。

### 5.7 验收

- `tests/test_ops086_release_bundle.py`（Claude 写，pi 不改）全过：在临时 git 仓库构建（文件集合、`RELEASE.json`、CRLF、
  拒绝不在 origin/main 上的提交、不覆盖）；部署的正常路径、回退和各种拒绝（用假的命令执行函数，不真建 venv、不碰计划任务）；
  `run_daily.ps1`、注册脚本和运维手册 9.3 的文本检查；真实仓库的清单包含日报要的文件和登记表点名的跟踪文件、不含 `tests/`、
  `tools/`、状态目录。
- 标了 `slow` 的端到端用例：用当前提交的文件构建发布包、解到临时目录，在单独的状态根下跑 `ops daily-plan`，
  并用 audit hook 确认没有打开开发仓里的文件。
- PR 套件无新增失败。

## 6. 步骤 4：`scripts/` 其余手拼路径（OPS-086-04，B 区，机械改写）

`scripts/` 下另有 22 个旧脚本（paper trading、IBKR paper、shadow promotion 等，不在日报路径上）共 41 处自己拼 `data/`、`outputs/`。
改用 `ai_trading_system.paths`；`tests/test_paths.py` 的棘轮扩到 `scripts/`，`scripts/ops/daily_scheduler_summary.py` 因只用标准库
列为例外（它从参数拿状态根）。

## 7. 之后（另定）

- 步骤 5：登记表分范围。日报收尾（artifact-lineage、reports index、docs report-contract、quality-gate）只处理日报范围；
  研究登记项改在开发仓按需检查。届时请 Baker 定 reader brief 是否保留"研究治理摘要"一节（默认不保留）。要在步骤 6 之前做，
  否则新的生产状态里没有研究遗留产物，研究登记项会多出缺失警告。
- 步骤 6：电脑上试运行（计划 P5），只写新目录。
- 步骤 7：Baker 切换计划任务（计划 P6）。之后再单独问旧运行副本和研究数据搬进 archive。

## 8. 不改的

- 投资逻辑、评分规则、DQ 规则、阈值、回测、broker 边界。production_effect=none；broker_action=none。
- 现在的运行副本和计划任务：步骤 1～4 只改仓库。

## 9. 进展

- 2026-10-14：需求和步骤 1～3 的验收测试由 Claude 写好，pi 按 OPS-086-01 起依次实现。
- 2026-10-14：步骤 1（OPS-086-01）：35 个文件搬到 inputs/ 和 research/_results/，读取方、登记表和 3 个 C 区文件的路径文字（决定记录）一并改。production_effect=none；broker_action=none。
- 2026-10-15：步骤 2（OPS-086-02）：release_identity 模块、7 处发布身份、包装脚本和摘要接受状态根。production_effect=none；broker_action=none。
- 2026-10-15：步骤 3（OPS-086-03）：清单规则、构建和部署脚本、run_daily.ps1、注册脚本 -DailyRoot、运维手册 9.3。production_effect=none；broker_action=none。
