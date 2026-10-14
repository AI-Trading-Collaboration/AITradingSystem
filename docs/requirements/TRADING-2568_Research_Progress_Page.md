# TRADING-2568 研究进展页面

## 1. 背景

- 旧的研究展示页 Atlas（TRADING-2466）在 2026-10-10 按 `owner_decision:GOV-008:2026-10-10:atlas_retired` 退役，代码当天删除。它的问题：自带 source registry、projection、contract 和术语表，约 1.7 万行；页面是本机未跟踪文件并绑定 exact commit，一有新提交就过期，还让 PR 测试挂了一个已知失败；靠人手动重跑，最后一版停在 2026-10-10，用的数据到 2026-09-24。
- 2026-10-12 的 GOV-009 之后，研究记录都在仓库里：`research/ledger.md`（台账）、`research/negative_results.md`、`research/<题号>/prereg.yaml`、`runs/NNNN.json`、`window_reads.jsonl`、`decision.md`，S4b 之后还有 `research/_screens/ledger/NNNN.json`。研究记录走快车道（DEVX-030）发布到 main。
- 方案全文见项目文件 `knowledge/claude/research-progress-page/design-2026-10-14.md`。

## 2. owner 决定

owner_decision:TRADING-2568:2026-10-14:research_progress_page —— owner（Baker）2026-10-14 08:44 JST（2026-10-13 23:44:05Z）在研究进展页面线程的决定卡上作答：

- 卡片问题："研究进展页面放在哪里看？"
- 选择："GitHub Pages"
- 选项后果："手机也能看，发布到 main 就自动重建；页面公开，要你批一次 workflow 并打开 Pages。"
- 卡片说明："选完后我写需求和验收测试，交给 pi 写代码，排在 S4 和 Q1 开发期之后。"

范围：一个只读的页面生成器（B 区）和一个 GitHub Pages workflow（`.github/**`，C 区，在 `decision_record_excluded` 里，按 tree 批）。页面公开；仓库本来是公开的，页面不读仓库以外的任何东西。

## 3. 原则

1. 页面只把仓库里已有的研究记录排版出来：不解释、不重算结论、不另存一份事实，也不定义新的记录格式。
2. 生成器只读文件和 git 元数据，不联网，不读 `data/`、`outputs/`、`data/raw/research/`，不写仓库。
3. 同样的仓库内容生成逐字节相同的页面：页面上的时间只用提交时间，不用生成时间。
4. 页面坏了不影响研究和发布：生成器不进 `ship.py` 门禁，workflow 不是 main 的检查。某个研究题的文件有问题时，页面照常生成，把问题显示出来。
5. 只用标准库和仓库已有依赖（`research_bench`、`yaml_loader`）；页面是一个自带 CSS 的静态 HTML，没有脚本，没有外部资源。

## 4. 页面回答的问题

1. 研究路线整体是什么样：想法从哪几条入口进来，汇进哪条流程，出口是什么；哪些入口还没上线。
2. 现在在研究什么，每个题走到研究流程的哪一步，下一步是什么。
3. 已有哪些结论（保留、拒绝、证据不足、未测成），评级多少。
4. 一共试了多少次（失败的也算），每个题的试验额度用了多少，验证期读过没有。
5. 信号筛选筛掉了什么、留下了什么（S4b 之后）。
6. 最近发生了什么。

三条入口（2026-10-14 第二版）：信号筛选（TRADING-2565 S4）、思路快筛和判断对账（TRADING-2569 S3、S1/S2，人工研究验证模块）。快筛和判断对账的面板在 TRADING-2568-03 里接，本批只留占位；owner 2026-10-14 选了"判断对账只放状态和进度"，命中率、Brier 和纸面组合不上公开页。

研究流程的 8 步（2026-10-13 告诉 owner 的标准流程）：①想法和前人研究 ②数据 ③信号筛选 ④开题（预注册）⑤开发期 1999–2010 ⑥验证期读一次 ⑦一致性期 ⑧期权层。研究题目录从第 ④ 步开始存在。

## 5. 设计

### 5.1 位置和接口

包 `src/ai_trading_system/research_site/`（B 区），公开名字：

| 名字 | 说明 |
|---|---|
| `SCHEMA_VERSION = "research_site.v1"` | 数据模型版本 |
| `TASK_PREFIXES = ("TRADING-2565", "TRADING-2566", "TRADING-2567", "TRADING-2568", "TRADING-2569", "DEVX-030")` | 研究工程任务 |
| `collect(repo: Path, *, repo_url: str \| None = None) -> dict` | 读仓库，返回数据模型（5.2） |
| `render(model: Mapping) -> str` | 数据模型到 HTML（5.4） |
| `markdown_to_html(text: str) -> str` | 极简 Markdown 转换（5.5） |
| `build_site(repo: Path, out: Path, *, repo_url: str \| None = None) -> Path` | 建 `out`，写 `index.html` 和 `site.json`，返回 `index.html` 的路径 |
| `main(argv: Sequence[str] \| None = None) -> int` | 命令行 |

命令：`python -m ai_trading_system.research_site --out <目录> [--repo <仓库，默认 .>] [--repo-url <https://github.com/owner/repo>]`。打印一行中文汇总（题数、进行中、累计试验、问题条数和 `index.html` 路径）；页面有问题也返回 0。

`site.json` = `json.dumps(model, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False)` 加换行。

### 5.2 数据模型

仓库里的文本文件一律用 `Path.read_text(encoding="utf-8")` 读（Windows 上换行统一成 `\n`），模型里的原文就是这样读到的字符串。

管道表格的"格"：以 `|` 开头的行，去掉两端空白，再去掉两端的 `|`，按 `|` 切开，每格去掉两端空白。连续的以 `|` 开头的行是一张表，第一行是表头。

模型只含 JSON 值（`str`、`int`、`float`、`bool`、`None`、列表、字典），没有 NaN 和无穷（写 `None`），`json.dumps(..., allow_nan=False)` 不报错。

```
{
  "schema_version": "research_site.v1",
  "source": {"commit": str|None, "commit_time": str|None, "repo_url": str|None},
  "summary": {"active": int, "closed": int, "global_trials": int, "failed_runs": int,
              "validation_reads": int, "chain_ok": bool, "problems": int},
  "questions": [题, ...],              # 按 question_id 排序
  "ledger": {"markdown": str|None, "parse_ok": bool, "rows": [行, ...],
             "counts": {"保留": int, "拒绝": int, "证据不足": int, "未测成": int}},
  "negative_results": str|None,        # research/negative_results.md 原文
  "negative_sections": [{"title": str, "markdown": str}, ...],
  "screens": {"present": bool, "records": int, "family_size": int,
              "counts": {"candidate": int, "eliminated": int, "inconclusive": int},
              "members": [成员, ...]},
  "tasks": [{"id", "status", "next_step", "area"}, ...],   # 按 id 排序
  "activity": [{"commit", "time", "subject"}, ...],        # 新的在前，最多 20 条
  "problems": [str, ...]
}
```

**来源** `source`：`repo` 本身是 git 工作区的顶层（`git -C <repo> rev-parse --show-prefix` 成功且输出为空；`repo` 只是某个外层仓库里的子目录时不算，这样不用比较 Windows 路径的写法）时，`commit` = `git rev-parse HEAD`（40 位），`commit_time` = `git log -1 --format=%cI`；不是 git 仓库或没有 git 时两项都是 `None`、`activity` 为空列表，不算问题。`repo_url` 原样（去掉末尾的 `/`）。

**题** —— `research/` 下每个含 `prereg.yaml` 的子目录，名字以 `_` 开头的除外（和 `research_bench.ledger.question_directories` 一致）：

```
{"question_id": 目录名, "question": str|None, "status": str, "step": int|None, "next": str,
 "approved": bool, "owner_decision": str|None, "trial_cap": int|None, "trials": int,
 "runs_total": int, "runs_failed": int, "strategies": [策略 id, ...],
 "runs": [运行, ...], "reads": [读取, ...],
 "decision": {"words": [{"label": str|None, "word": str}, ...], "markdown": str}|None,
 "last_activity": {"commit", "time", "subject"}|None,
 "links": {"prereg": str|None, "decision": str|None}, "problems": [str, ...]}
```

- `prereg.yaml` 用 `research_bench.prereg.load_prereg` 读。读不出来（记一条问题；pydantic 的 `ValidationError` 按 `字段: 原因` 写，多个错误用 `；` 连接，不带 pydantic 的文档网址），或 `question_id` 不等于目录名，或运行记录、窗口读取的链有问题（`ledger.verify_runs`、`ledger.verify_reads`），这个题的 `status` 为 `error`、`step` 为 `None`、`runs` 和 `reads` 为空列表，原因写进 `problems`；其余照常。
- `approved`：`owner_decision` 不为空，且仓库 `docs/requirements/` 下某个 `*.md` 文件的文本里含这个字符串（直接读文件，不用 git）。
- 状态，按顺序取第一个成立的：

| 条件 | `status` | `step` | `next`（照抄） |
|---|---|---|---|
| 有 `decision.md` | `closed` | 不看 `decision.md` 时会得到的 step | `已结题。` |
| 窗口读取里有 `consistency` | `consistency_read` | 7 | `一致性期已读，等写结论。` |
| 窗口读取里有 `validation` | `validation_read` | 6 | `验证期已读，等写结论。` |
| 有 `window.role` 为 `development` 的运行记录 | `developing` | 5 | `开发期已跑 {trials} 次试验；下一步 Claude 审实现，再读验证期。` |
| `approved` | `awaiting_dev_runs` | 5 | `已批准开题，等 pi 跑开发期。` |
| 其余 | `awaiting_approval` | 4 | `等你批准开题。` |
| （`error`） | `error` | `None` | `记录有问题，先修：见下面的问题。` |

- `trials` = `len(research_bench.ledger.development_trials(运行记录))`（和 `ledger_summary` 的算法相同）；`trial_cap` = 预注册的 `trial_cap`。`runs_total` = 运行记录条数；`runs_failed` = `status` 不是 `ok` 的条数。
- `research_bench.ledger.ledger_summary(repo / "research")` 只调用一次。它抛异常时（例如某个 `prereg.yaml` 不是合法 YAML）页面照常生成：所有 `deflated_sharpe` 为 `None`，`summary.global_trials` 用各题 `trials` 之和，顶层 `problems` 加一条，含 `ledger_summary`。
- 运行（按记录顺序）：

```
{"run_id", "strategy", "role", "status", "trial",
 "geometric_annual_return", "max_drawdown", "sharpe_vs_cash", "deflated_sharpe",
 "vs_matched_return", "vs_matched_drawdown", "rules_held": int|None, "rules_total": int|None}
```

`run_id`、`strategy`（`strategy.id`）、`role`（`window.role`）、`status`、`trial`（`trial.number`）取自记录；三项指标取 `result.strategy` 的同名字段；`vs_matched_return`、`vs_matched_drawdown` 取 `result.comparison` 的 `geometric_annual_return`、`max_drawdown`；`deflated_sharpe` 取 `ledger_summary` 里同一 `run_id` 的那一行；`rules` 不为 `None` 时 `rules_total` = 条数、`rules_held` = `holds` 为真的条数。缺的一律 `None`。

- 读取：`{"read_id", "role", "window_id", "recorded_at"}`，取自 `window_reads.jsonl`。
- 结论：有 `decision.md` 时 `markdown` 为原文，`words` 按在原文里的位置先后列出下面两种写法找到的结论词（一题可以有几个子问题，例如 QQQ-Q1 的 Q1a、Q1b 在一张表里各一行）：
  1. 原文里每一处匹配 `结论\s*[:：]\s*\**\s*(保留|拒绝|证据不足|未测成)`：`{"label": None, "word": 匹配到的词}`，位置是匹配的开头。
  2. 表头有一格去掉两端的 `*` 和空白后正好是 `结论` 的表（取第一个这样的列）：表头以下每一行，这一列的格去掉两端的 `*` 和空白后以四个结论词之一开头的，记 `{"label": 这一行第一格去掉两端的 * 和空白, "word": 那个词}`；`结论` 就是第一列时 `label` 为 `None`。位置是这一行的开头。分隔行的格是 `---`，自然不算。没有 `结论` 列的表不看。
  一个都没找到时 `words` 为空列表，这个题的 `problems` 加一条，含 `decision.md`（状态仍是 `closed`）。
- `last_activity`：`git log -1 --format=%H%x1f%cI%x1f%s -- research/<题号>`，没有为 `None`。
- `links`：`repo_url` 和 `source.commit` 都有时，`prereg` = `{repo_url}/blob/{commit}/research/{题号}/prereg.yaml`，有 `decision.md` 时 `decision` 同理；否则为 `None`。
- `strategies`：预注册里的策略 id；`question`：预注册的 `question`。

**汇总** `summary`：`active` = `status` 不是 `closed` 的题数（含 `error`）；`closed`；`global_trials` = `ledger_summary` 的 `global_trials`（它失败时见上）；`failed_runs` = 各题 `runs_failed` 之和；`validation_reads` = 各题角色为 `validation` 的读取条数；`chain_ok` = 没有任何题的运行记录或窗口读取链有问题；`problems` = 顶层 `problems` 的条数。

**台账** `ledger`：`markdown` 为 `research/ledger.md` 原文（文件不存在为 `None`）。从 `## 总表` 那一行之后的表格里取数据行：以 `|` 开头、按 `|` 切开去空白后正好 5 格、第一格是数字的行；行为 `{"number": int, "line": 第 2 格, "verdict": 第 3 格, "words": 结论词, "rating": 第 4 格, "fate": 第 5 格}`；`words` 是 `verdict` 里正则 `保留|拒绝|证据不足|未测成` 的全部匹配，按出现顺序（例如 `Q1a 证据不足；Q1b 拒绝` 为两个词）。没有匹配的行 `words` 为空列表，顶层 `problems` 加一条，含 `research/ledger.md` 和 `第 <编号> 行`。`counts` 四个键都在，数所有行 `words` 里的词，按子问题计（一行两个子问题就计两次）。`parse_ok` = 至少取到一行。文件不存在或一行都没取到，顶层 `problems` 加一条，含 `research/ledger.md`。

**筛选** `screens`：目录 `research/_screens/ledger/`。`present` = 目录存在。文件名匹配 `^[0-9]{4,}\.json$` 的按编号读；读不出 JSON 的跳过，顶层 `problems` 加一条（含文件名）。`records` = 读到的条数；成员按 `module_sha256:spec_sha256` 分组，取编号最大的一条：`{"signal_id", "number", "verdict", "primary_p_value", "records": 这组的条数}`，按 `number` 排序；`family_size` = 成员数；`counts` 三个键都在，数成员的 `verdict`。

**负结果分节** `negative_sections`：`research/negative_results.md` 里每个以 `## `（两个 `#` 加一个空格）开头的行是一节的标题；`title` 是标题行 `## ` 之后的文字去掉两端空白，`markdown` 是这一行之后、下一个 `## ` 行之前的各行用 `\n` 连接、再去掉两端的换行。第一个 `## ` 之前的文字不属于任何一节（仍在 `negative_results` 里）。文件不存在或没有 `## ` 行时为空列表。

**任务** `tasks`：`tasks/*.yaml` 里 `id` 等于 `TASK_PREFIXES` 之一、或以"前缀 + `_`"开头的任务卡，取 `id`、`status`、`next_step`、`area`（缺的为 `None`）。

**动态** `activity`：`git log -n 20 --format=%H%x1f%cI%x1f%s -- research` 加上每个前缀的 `tasks/<前缀>*`。

**问题** `problems`：每个题的问题前面加 `"<题号>: "` 收进来，再加台账和筛选的问题。

### 5.3 不做（v1）

收益曲线（运行记录按设计只存汇总数字）、交互比较、问答、术语解释、编辑、任何写回仓库的动作。以后要曲线，需要评估台在记录里加按月净值，那是 C 区改动，另立。

### 5.4 页面

一个 `index.html`，中文。版式见 pi 运行目录里的 `design/example-index.html`（参考实现在 main 上生成的页面，浅深色、手机宽度都看过；只作对照，不进仓库）；样式见同目录的 `design/research_site.css`，**逐字**作为模块里的 `CSS` 字符串常量写进 `<style>`，不改。生成的 HTML 用示例页里同样的标签结构和 class 名，下面写的 id、class 和文字是验收测试要查的。

- 以 `<!doctype html>` 开头；`<html lang="zh-CN">`；有 `<meta name="viewport" content="width=device-width, initial-scale=1">` 和 `<meta charset="utf-8">`；`<title>研究进展</title>`；内容在一个 `<main>` 里。
- 样式里颜色用变量，有 `@media (prefers-color-scheme: dark)`；手机宽度下没有横向滚动，宽表格包在 `<div class="wrap">` 里；表格在窄屏变成"标签：值"叠放（`<td data-label>`）。
- 没有 `<script>`、`<link>`、`<img>`、`<iframe>`、`@import`、`url(`。没有 `repo_url` 时页面里没有指向 `http` 的链接（`href="http`）。
- 所有来自仓库的文本都经过 HTML 转义。
- 数字格式：收益、回撤用百分比一位小数；差值写成 `+8.1 点`、`-0.4 点`（乘 100、带正负号、一位小数、加" 点"）；Sharpe、DSR 两位小数；`None` 显示 `—`。
- 自上而下：
  1. 头部 `<header class="top">`：`<h1>研究进展</h1>`；`<p class="meta">`，内容为 `数据截至 main <code>前 9 位</code> · YYYY-MM-DD HH:MM +09:00`（提交时间去掉秒，保留提交者的时区偏移，不换算；没有 git 时写"不在 git 仓库里"）；一行汇总 `<p class="sum">`（进行中、已结题、累计试验、失败运行、验证期读取、记录链）；`<p class="legend">`，里面按顺序是四个结论词的徽章。
  2. 有问题时：`<section id="problems">`，逐条列出。没有问题时不出现这个 id。
  3. 研究路线 `<section id="route">`：一个 `<div class="map">`，里面六个 `<a class="node live|idle" href="#锚点">`，live 表示有记录或已上线，idle 表示还没上线（虚线）：`#screens` 信号筛选（`present` 为真时 live，显示已筛个数和淘汰、证据不足、候选各几个；否则 idle，写"S4b 上线后启用"）、`#quick` 思路快筛（idle，"J4 上线后启用"）、`#judgment` 判断对账（idle，"J3 上线后启用"）、`#questions` 研究问题（live，进行中和已结题题数、累计试验、验证期读取次数、记录链）、`#board` 结论板（live，台账的行数和四个结论词的计数）、`#negative` 负结果清单（live，节数）。三个入口节点在左列，研究问题在中间，结论板和负结果在右列，节点之间有箭头；窄屏上下叠放。
  4. 研究题 `<section id="questions">`：没有研究题时写"还没有研究题。"；否则每题一张 `<article class="card" id="q-<题号>">`，进行中的题（`status` 不是 `closed`）排在前面、已结题的在后，各自保持按题号的顺序。卡片的可见部分：
     - 卡头：题号和题目（题目超过 80 个字符时只显示前 80 个字符加 `…`，并在卡头之后放 `<details><summary>题目全文</summary><p>全文</p></details>`；题目为 `None` 时显示 `—`）；右侧 `<p class="state">`，`closed` 写"已结题"，`error` 写"记录有问题"，其余写"进行中"。
     - `decision.words` 不为空时，卡头里有 `<p class="verdicts">`，每个词一个徽章 `<span class="b 类">…</span>`，按词在 `decision.md` 里的先后；徽章文字是 `<label> <word>`（`label` 为 `None` 时只写 `<word>`）；类：保留 `keep`、拒绝 `rej`、证据不足 `weak`、未测成 `fail`。没有结论词时不出现 `class="verdicts"`。
     - 8 步进度 `<ol class="rail">`：每步一个 `<li class="done|now|todo">`；`step` 为 `None` 时全是 `todo`；`step` 之前的是 `done`，`step` 本身是 `now`，已结题时 `step` 本身算 `done`。
     - `<p class="next">`：`next` 原文。
     - 三个指标：`试验额度`（`trials / trial_cap`，带进度条）、`验证期`（有 `role` 为 `validation` 的读取时写 `读过 N 次`，否则 `未读`）、`失败运行`（`runs_failed`），格式为 `<span class="m">名</span>…<span class="n">值</span>`。
     - 每个 `role` 为 `validation` 或 `consistency` 的运行一个 `<div class="sub-card">`：运行号、策略、`规则成立`（`规则成立 held / total`，没有规则写 `—`）、`最大回撤相对同敞口固定仓位（正为更浅）`（`vs_matched_drawdown` 的差值格式）、`年化相对同敞口固定仓位`（`vs_matched_return`）、`年化 / 最大回撤`。开发期运行不出这个块。
     - 折叠：有开发期运行时 `<details><summary>开发期 N 次运行</summary>` 里是运行表（运行号、策略、状态、年化、最大回撤、折算夏普，`<td data-label="列名">`）；有 `decision` 时 `<details><summary>结论全文</summary>` 里是 `markdown_to_html(decision.markdown)`。
     - 有 `links.prereg` 或 `links.decision` 时各一个链接；每条 `problems` 一个 `<p class="m">`。
  5. 结论板 `<section id="board">`：`<p class="lead">` 写"按子问题计："加四个词的计数；`parse_ok` 为假时再显示 `markdown_to_html(原文)`（没有原文就什么都不加）。`parse_ok` 为真时按四个词各一个 `<details class="grp" id="grp-keep|grp-rej|grp-weak|grp-fail">`（顺序：保留、拒绝、证据不足、未测成），只有拒绝一组带 `open`；`<summary>` 里是徽章和 `<strong>条数</strong> 条`；每个 `words` 里含这个词的台账行是一个 `<li>`（一行有两个词时在两组里各出现一次，一行里同一个词出现几次都只出现一次），`<li>` 里有编号、`line`、每个词的徽章、`verdict` 原文（`verdict` 恰好等于一个结论词时不重复写）、`评级 <rating>`、`fate`。一组没有行时写"还没有。"。`words` 为空的行另放一组 `<details class="grp" id="grp-none">`（徽章文字"无结论词"），没有这样的行时这一组不出现。
  6. 负结果 `<section id="negative">`：有 `negative_sections` 时每节一个 `<details class="grp"><summary><strong>标题</strong></summary>` 加 `markdown_to_html(markdown)`；没有节但有 `negative_results` 时显示整个 `markdown_to_html(negative_results)`；文件不存在时写"没有找到 research/negative_results.md。"
  7. 信号筛选 `<section id="screens">`：`present` 为假时写"S4b 上线后启用"；否则一句"已筛 N 个信号（共 M 条记录）：淘汰 a · 证据不足 b · 候选 c"（N 是 `family_size`，M 是 `records`），再一张成员表：信号、裁决（徽章，`candidate` 为 `<span class="b cand">候选</span>`，`eliminated` 为 `b out` 的"淘汰"，`inconclusive` 为 `b unk` 的"证据不足"）、主市场 p（三位小数，`None` 为 `—`）、记录条数。更多的列（方向一致的市场数、调整后 p、最小可检出效应）等 TRADING-2568-03 按 S4b 实际记录加。
  8. 占位：`<section id="quick" class="sec-idle">`（"TRADING-2569 J4 上线后启用"）和 `<section id="judgment" class="sec-idle">`（"TRADING-2569 J3 上线后启用"）。
  9. 研究工程进度 `<section id="work">`：每个任务一个 `<li>`：`<strong>` 里是任务号的前缀（`id` 里第一个 `_` 之前的部分），后面 `<span class="m">area</span>`、状态徽章 `<span class="b st-<状态小写>">状态</span>`，再一个 `<details><summary>next_step 的第一句</summary><p>next_step 全文</p></details>`；第一句是第一个 `。` 之前的文字，超过 96 个字符时取前 96 个字符加 `…`；`next_step` 为空时 summary 写 `—`。其后一个 `<details><summary>最近动态</summary>`，里面是提交时间、前 9 位、标题；有 `repo_url` 和提交时链接到 `{repo_url}/commit/{commit}`。

### 5.5 Markdown

`markdown_to_html` 只支持仓库研究文档用到的：`#` 到 `####` 标题（`<h1>` 到 `<h4>`，不带属性）、`- ` 列表（`<ul><li>`）、管道表格（整张表放在 `<div class="wrap"><table class="stack">` 里；第一行 `<th>`，分隔行跳过，其余 `<td data-label="表头文字">`，`data-label` 取同一列表头的原文并转义引号，单元格比表头多时多出的格子 `data-label` 为空；其余标签不带属性）、段落（`<p>`）、行内 `` `代码` ``（`<code>`）和 `**粗体**`（`<strong>`）。先转义再加标签；不认识的语法原样转义输出。不渲染链接和图片。

### 5.6 GitHub Pages（运行 02，C 区）

`.github/workflows/research-site.yml`：push 到 main 且改动涉及 `research/**`、`tasks/**`、`docs/requirements/**`、`src/ai_trading_system/research_bench/**`、`src/ai_trading_system/research_site/**` 时，或手动触发，在 ubuntu 上装 Python 3.11 和本仓库（`pip install -e .`），运行生成器（`--repo-url https://github.com/${{ github.repository }}`），用 `actions/upload-pages-artifact` 和 `actions/deploy-pages` 发布。权限只给 `contents: read`、`pages: write`、`id-token: write`。owner 需要在仓库 Settings → Pages 把 Source 设为 GitHub Actions。这个 workflow 不是 main 的检查，失败不挡任何发布（GOV-008：门禁在本机）。

## 6. 步骤

| 运行 | 区 | 内容 | 验收 |
|---|---|---|---|
| TRADING-2568-01 | A、B | 本需求文档、任务卡、`research_site` 包、验收测试 `tests/test_research_site.py`（Claude 写，原样放入）、`research/README.md` 加一行 | 验收测试全过；全量 PR 套件不新增失败；`ship.py --dry-run` 为 zone B |
| TRADING-2568-02 | C | `.github/workflows/research-site.yml`，trailer 引本决定 | owner 按 tree 批准；发布后 workflow 成功，页面地址可打开 |
| TRADING-2568-03（待写） | B | 信号筛选面板按 S4b 的实际记录加深（方向一致的市场数、调整后 p、最小可检出效应），接 TRADING-2569 的快筛面板和判断对账面板（公开页只放状态、样本进度、还差几个事件和最小可检出效应） | 等 S4b 和 TRADING-2569 J3、J4 的记录出现再写需求和测试 |

## 7. 进展

- 2026-10-14：登记，owner 选 GitHub Pages（第 2 节）。production_effect=none；broker_action=none。
- 2026-10-14：第二版版式（owner 要一个读得懂、覆盖当前工作的展示）：研究路线图、先说结论的问题卡、按结论词分组的结论板、负结果分节、手机上表格叠放；`TASK_PREFIXES` 加 `TRADING-2569`；owner 选判断对账公开页只放状态和进度。
- 2026-10-14：运行 01 之前补上一题多个子问题的结论词（`decision.md` 的结论表、台账一格两个词），因为 QQQ-Q1 的 Q1a、Q1b 是这样写的。
