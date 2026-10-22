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

owner_decision:TRADING-2568:2026-10-19:page_rules_and_zh —— owner（Baker）2026-10-19 21:49 JST（12:49:45Z）在项目聊天里写：

> 目前研究页面的描述是否可以更具体一些，尤其是这个策略具体是什么规则执行买入和卖出以及调仓等行为，现在的页面可读性感觉不太强，还有我们依据了哪些标准淘汰部分信号可以更具体一些？而且最好除了专业名词外用中文会好一点

范围：只改页面怎么写，不改任何研究记录、门槛、判定和设置。每张题卡写清每个策略怎么买、怎么卖、多久调一次仓和通过门槛；信号筛选写清按什么标准淘汰、为什么淘汰了这一个；试跑写清门槛和结果；除专业名词和代码名外用中文。涉及 B 区（`research_site` 包和验收测试）和 A 区（本文档、说明文件 `research/signal_names.yaml`、`research/<题号>/zh.yaml`、`docs/`）；不碰 `prereg.yaml`、`strategy.py`、`config/` 和 `.github/`（C 区）。期望结果见第 5.7 节和 `tests/test_research_site_zh.py`；符合它的改动不再问 owner。

owner_decision:TRADING-2568:2026-10-22:page_relayout —— owner（Baker）2026-10-22 15:12 JST（06:12Z）在「研究流程评审与结果分析」线程的决定卡上作答：

- 卡片问题：「按新主次重排研究进展页吗?」
- 选择：「按提案重排」
- 选项后果：「顶部加现状、保险账、前瞻记录、当期题、家族台账五块，路线图改成真实漏斗，信号表加去向列并按候选优先排序，题卡和试跑折叠；Claude 写需求和测试，pi 写代码。」
- 他 15:10 JST 的原话：「我们现在研究进展的页面上，对于信号展示目前来看主次不太清晰，是否应该把有价值的信息做好排版梳理？」

范围：只改页面读什么、怎么排，不改任何研究记录、读数、门槛和设置。新读的文件都是 main 上已有的 A 区记录：`research/_loop/forward_records.md`、`research/_forward/weekly.md`、`research/families.md`、`research/_loop/charter_p<N>.md`、`research/<题号>/prereg.yaml` 的 `screens`、`research/_trials/T*.yaml` 的 `candidate.screens`。`research/_forward/ledger.jsonl` 不读。

## 3. 原则

1. 页面只把仓库里已有的研究记录排版出来：不重算结论、不另存一份事实，也不定义新的记录格式。规则、门槛和淘汰理由可以翻译成中文：译文由 `prereg.yaml`、信号规格和设置文件按固定句式生成，或来自作者写在 `research/signal_names.yaml`、`research/<题号>/zh.yaml` 里的说明文字（2026-10-19 起，5.7）；这些说明文件只是展示文字，不进任何运行，不影响结论，也不进 `ship.py` 的检查。
2. 生成器只读文件和 git 元数据，不联网，不读 `data/`、`outputs/`、`data/raw/research/`，不写仓库，`--out` 在仓库里面时拒绝（`ValueError`，含"仓库之外"）。运行 03 起还读仓库里的配置和规格文件（`config/research/research_screen.yaml`、`research_bench.yaml`、`research/_trials/`、`src/ai_trading_system/research_screens/signals/<信号>.yaml`），只当 YAML 读，不导入信号模块，不读市场数据。
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
7. 每个策略具体按什么规则买入、卖出、多久调一次仓，仓位是多少（2026-10-19，5.7）。
8. 一个信号凭什么被淘汰、候选要过哪几条，每条的数字是多少；通过门槛的每一条用中文怎么说、实测是多少（2026-10-19，5.7）。
9. 试跑了哪些候选，主配置过没过门（2026-10-19，5.7）。

三条入口（2026-10-14 第二版）：信号筛选（TRADING-2565 S4）、思路快筛和判断对账（TRADING-2569 S3、S1/S2，人工研究验证模块）。快筛和判断对账的面板在 TRADING-2568-04 里接，目前只留占位；owner 2026-10-14 选了"判断对账只放状态和进度"，命中率、Brier 和纸面组合不上公开页。

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
| `research_site.zh`（模块，运行 03） | 中文措辞：`clean`、`number`、`percent`、`load_signal_names`、`load_question_text`、`signal_view`、`strategy_card`、`rule_text`、`observed_text`、`rule_rows`、`screen_reasons`，常量 `SCHEDULES`、`EPISODE_NAMES`（5.7） |

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
              "validation_reads": int, "chain_ok": bool, "problems": int,
              "trial_configs": int, "question_trials": int},   # 后两项运行 03 加，见 5.7
  "questions": [题, ...],              # 按 question_id 排序
  "ledger": {"markdown": str|None, "parse_ok": bool, "rows": [行, ...],
             "counts": {"保留": int, "拒绝": int, "证据不足": int, "未测成": int}},
  "negative_results": str|None,        # research/negative_results.md 原文
  "negative_sections": [{"title": str, "markdown": str}, ...],
  "screens": {"present": bool, "records": int, "family_size": int,
              "counts": {"candidate": int, "eliminated": int, "inconclusive": int},
              "members": [成员, ...]},
  "screen_details": {...}, "screen_criteria": {...}|None,    # 运行 03，5.7
  "trials": {...}, "bench": {...}|None,                      # 运行 03，5.7
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

**汇总** `summary`：`active` = `status` 不是 `closed` 的题数（含 `error`）；`closed`；`global_trials` = `ledger_summary` 的 `global_trials`（它失败时见上；运行 03 起有 `research/_trials/` 时改为题目开发试验加试跑配置，见 5.7）；`failed_runs` = 各题 `runs_failed` 之和；`validation_reads` = 各题角色为 `validation` 的读取条数；`chain_ok` = 没有任何题的运行记录或窗口读取链有问题；`problems` = 顶层 `problems` 的条数。

**台账** `ledger`：`markdown` 为 `research/ledger.md` 原文（文件不存在为 `None`）。从 `## 总表` 那一行之后的表格里取数据行：以 `|` 开头、按 `|` 切开去空白后正好 5 格、第一格是数字的行；行为 `{"number": int, "line": 第 2 格, "verdict": 第 3 格, "words": 结论词, "rating": 第 4 格, "fate": 第 5 格}`；`words` 是 `verdict` 里正则 `保留|拒绝|证据不足|未测成` 的全部匹配，按出现顺序（例如 `Q1a 证据不足；Q1b 拒绝` 为两个词）。没有匹配的行 `words` 为空列表，顶层 `problems` 加一条，含 `research/ledger.md` 和 `第 <编号> 行`。`counts` 四个键都在，数所有行 `words` 里的词，按子问题计（一行两个子问题就计两次）。`parse_ok` = 至少取到一行。文件不存在或一行都没取到，顶层 `problems` 加一条，含 `research/ledger.md`。

**筛选** `screens`：目录 `research/_screens/ledger/`。`present` = 目录存在。文件名匹配 `^[0-9]{4,}\.json$` 的按编号读；读不出 JSON 的跳过，顶层 `problems` 加一条（含文件名）。`records` = 读到的条数；成员按 `module_sha256:spec_sha256` 分组，取编号最大的一条：`{"signal_id", "number", "verdict", "primary_p_value", "records": 这组的条数}`，按 `number` 排序；`family_size` = 成员数；`counts` 三个键都在，数成员的裁决：读得到筛选设置、记录链完整时数当前裁决（`screen_ledger.screen_ledger_summary` 里同一成员的 `verdict`，它按整个筛选过的家族做 Benjamini-Hochberg 校正，家族变大后会和记录当时的 `verdict` 不同），否则数记录里的 `verdict`（运行 03；成员里的 `verdict` 仍是记录当时的）。

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

运行 03 对 5.4 的改动见 5.7；5.4 写过的 id、class 和文字仍然有效，5.7 说了替换的除外。

### 5.5 Markdown

`markdown_to_html` 只支持仓库研究文档用到的：`#` 到 `####` 标题（`<h1>` 到 `<h4>`，不带属性）、`- ` 列表（`<ul><li>`）、管道表格（整张表放在 `<div class="wrap"><table class="stack">` 里；第一行 `<th>`，分隔行跳过，其余 `<td data-label="表头文字">`，`data-label` 取同一列表头的原文并转义引号，单元格比表头多时多出的格子 `data-label` 为空；其余标签不带属性）、段落（`<p>`）、行内 `` `代码` ``（`<code>`）和 `**粗体**`（`<strong>`）。先转义再加标签；不认识的语法原样转义输出。不渲染链接和图片。

### 5.6 GitHub Pages（运行 02，C 区）

`.github/workflows/research-site.yml`：push 到 main 且改动涉及 `research/**`、`tasks/**`、`docs/requirements/**`、`src/ai_trading_system/research_bench/**`、`src/ai_trading_system/research_site/**` 时，或手动触发，在 ubuntu 上装 Python 3.11 和本仓库（`pip install -e .`），运行生成器（`--repo-url https://github.com/${{ github.repository }}`），用 `actions/upload-pages-artifact` 和 `actions/deploy-pages` 发布。权限只给 `contents: read`、`pages: write`、`id-token: write`。owner 需要在仓库 Settings → Pages 把 Source 设为 GitHub Actions。这个 workflow 不是 main 的检查，失败不挡任何发布（GOV-008：门禁在本机）。

### 5.7 运行 03：中文规则、淘汰标准、试跑（`research_site.v1` 只加不改）

owner 2026-10-19 的要求（第 2 节）。`SCHEMA_VERSION` 不变；5.2 的键和含义不变，只加键（旧验收测试 `tests/test_research_site.py` 一字不改，全过）。措辞由 `tests/test_research_site_zh.py` 冻结：这里写规则，测试里的句子是页面的句子，改一个字就是改测试。新代码在 `research_site/zh.py`，`collect.py` 和 `render.py` 调它；样式换成 `design/research_site.css`（逐字），版式见 `design/example-index.html`。

**两个说明文件**（A 区，只是展示文字）。缺文件不算问题；格式不对时那个文件不生效，顶层 `problems` 加一条，含文件路径（`research/signal_names.yaml` 或 `<题号>: zh.yaml`）：

- `research/signal_names.yaml`：`schema_version: research_signal_names.v1`，`signals: {信号 id: {name: 非空文字, meaning: 文字（可选）}}`，不许有别的键。`load_signal_names(research, problems) -> {id: {"name", "meaning"|None}}`，`meaning` 用 `clean`。
- `research/<题号>/zh.yaml`：`schema_version: research_zh.v1`，`title`、`summary` 非空文字，`verdict` 可选，`strategies: {策略 id: {name, role?, rule?}}`；`rule` 是非空列表，每项恰有 `label` 和 `text`；`strategies` 里的 id 必须是这个题预注册的策略；不许有别的键。`load_question_text(directory, strategy_ids, problems) -> {"title","summary","verdict"|None,"strategies":{id:{"name","role"|None,"rule"|None}}}|None`（没有文件或不合格为 `None`）。模板在 `research/_template/zh.yaml`；新题的说明文件和 `prereg.yaml` 一起写，没有也能生成页面。
- `zh.clean`：折叠空白成一个空格，并去掉两个中文字符之间的空格（YAML 折行会留下）；`zh.number(value, digits=1)`：去掉小数末尾的 0（没有小数点时不去），`-0` 写 `0`；`zh.percent(v)` = `number(v*100)` 加 `%`。

**信号** `zh.signal_view(repo, id, names)`：`{"id","name","meaning","favourable","expectation"}`。名字：说明文件里的；否则信号规格（`src/ai_trading_system/research_screens/signals/<id>.yaml`，只读 YAML）的 `mechanism` 里第一个 `：` 之前的话（`：` 在前 20 个字符内）；再否则 `id`。`meaning`：说明文件里的；否则 `mechanism` 里 `：` 之后第一个 `。` 之前的话；规格读不到为 `None`。`favourable`（走高有利为 1，走低有利为 -1）= `expected_sign` 乘（`target` 为 `return` 取 1，为 `risk` 取 -1），规格不全为 `None`；`expectation` 为"信号越高，之后收益越高/低"或"信号越高，之后波动越大/小"。

**策略卡** `zh.strategy_card(entry, *, repo, names, text, validated, warmup_days=None)`，`entry` 是预注册里的策略条目加 `rebalance`（试跑配置还可以有 `reserve`）：

- 返回 `{"id","name","role","validated","kind","signals","lines","levels","source"}`。`name` 取说明文件，否则 `id`；`role` 取说明文件，否则"验证期读取"（`validated` 为真）或"开发期变体"。`lines` 是 `[{"label","text"}]`，`levels` 是 `[{"when","exposure"}]` 或 `None`。
- `function` 为 `signal_exposure` 且 `params.signals` 是列表：`kind` 为 `signal_exposure`，`signals` 是各信号的 `signal_view`。任何一个信号的 `favourable` 是 `None` 时不猜方向：`source` 为 `unavailable`，`lines` 只有"信号"和"规则"两行（"读不到 … 的信号规格，不知道哪个方向有利，页面写不出买卖规则"）。否则 `source` 为 `generated`，`lines` 依次是：信号、打分（符号打分或百分位打分，多个信号取平均，有百分位打分且给了 `warmup_days` 时说明先要攒够多少个有值的交易日）、仓位、买入（加仓）、卖出（减仓）、调仓、其他。三种映射（`threshold`、`linear`、`steps`）各有句式；单个符号信号和阈值 0.5 时直接说"信号为正/负"；单个百分位信号说"信号在自己历史的 X% 分位以上/以下（分数 ≥/< 阈值）"，方向相反的信号分位数取补；2 到 4 个符号打分的信号按"有利信号的个数"给出 `levels`；`threshold` 和 `steps` 也给 `levels`；仓位上限超过 100% 时在"其他"里写"仓位最高 N 倍（用 2 倍、3 倍杠杆产品凑出来）"，否则写"不加杠杆，仓位在 a 到 b 之间，其余是现金"；"调仓"写调仓日（每个交易日／每周最后一个交易日／每月最后一个交易日）、"下一个交易日收盘成交"、`delay_days`、其余日子不动、带宽（"偏离目标超过目标的 X% 才真的交易"）。
- `function` 为 `fixed_exposure` 且有 `params.exposure`：`kind` 为 `fixed_exposure`，`lines` 是"仓位"（固定持有 X% 的 QQQ，其余是现金或 10 年期国债，不看信号）和"调仓"；`reserve` 为 `treasury_10y` 时写 10 年期国债（用 FRED DGS10 合成）。
- 其余：`kind` 为 `custom`，`source` 为 `unavailable`，`lines` 一行"规则"：由代码里的 `<function>` 定义，页面暂时没有中文说明。
- 说明文件里这个策略有 `rule` 时，`lines` 换成它，`source` 为 `zh.yaml`（`kind` 不变）。

**门槛的话** `zh.rule_text(metric, op, value, confidence=None)`：`<段>.<字段>`，段是 `comparison`、`ci_low`、`ci_high`、`leave_year_min`、`leave_episode_min`、`placebo_rank`、`cost_<bps>`、`delay_<天>`；字段是 `cvar_5pct_relative_improvement`（左尾，用百分比）、`max_drawdown`（最大回撤，用"个点"）、`geometric_annual_return`、`rolling_geometric_return_low`（用"个点"）。`>=`、`>` 译成"比对照至少小/浅/多 …"、"比对照小/浅/高"、"不比对照深/低"、"最多大/深/少 …"等，`ci_*` 与 0 比较时用"改善的 95% 区间下限 > 0（排除「没有改善」）"一类的话，`placebo_rank` 译成"胜过 95% 的随机择时"。认不得的指标或符号退回 `<指标> <符号> <数>`，不抛异常。`zh.observed_text(metric, observed)`：左尾写 `+24.8%`，其余写 `+8.1 点`，随机择时写"胜过 97.3%"，`None` 写 `—`。`zh.rule_rows(rules, confidence=None)`：每条规则保留 `metric`、`op`、`value`，加 `text`，有 `observed` 时再加 `observed`、`observed_text`、`holds`。`confidence` 为 `None` 时区间写"置信区间"，不写数字。

**淘汰的话** `zh.screen_reasons(inputs, results, adjusted_p, criteria)`，`inputs` 是筛选记录的 `results.verdict_inputs`，`results` 提供 `direction_matches`、`direction_tested`，`criteria` 是 `screen_criteria`：返回 `[{"kind","text"}]`。淘汰（`kind` 为 `eliminate`，满足几条列几条，顺序：区间、方向一致比例、复制基准）：主检验（只在 QQQ 上筛的信号用 QQQ）的区间整体在预期方向的反面；方向一致的市场比例低于淘汰线；和某个基准信号的最大秩相关达到重复线。有任何淘汰理由就只返回它们；否则逐条列出候选的四个条件，各带 `ok` 或 `fail`（QQQ 方向、方向一致比例不低于候选线减 1e-6、调整后 p 不高于 `bh_q`、去掉任意一个年代后方向不变；`leave_out_stable` 不是 `True` 算 `fail`）。只在 QQQ 一个市场上筛的信号不能成为候选，返回一条 `fail`。

**新增的模型键**：

- 题加 `"zh": {"title","summary","verdict"|None}|None`、`"strategy_cards": [策略卡]`（预注册顺序，`role` 里的"验证期读取"是 `validation.strategies` 里的）、`"gates": [{"strategy","name","rules": rule_rows(该策略的预注册规则, validation.confidence)}]`（`validation.strategies` 的顺序）。说明文件的 `strategies` 里写了不存在的策略 id 时整个说明文件作废，题照常，`zh` 为 `None`。
- 运行加 `"rules": rule_rows(记录里的 rules, 记录的 falsification_confidence)|None`。
- 顶层 `screen_criteria`：`config/research/research_screen.yaml` 的 `screen_end`（`isoformat`）、`primary_market`、`primary_horizon`、`eliminate_breadth`、`candidate_breadth`、`copy_corr`、`bh_q`、`confidence`、`warmup_days`；文件不存在为 `None`，读不出来为 `None` 并加问题 `config/research/research_screen.yaml: …`。
- 顶层 `screen_details`：`{str(编号): {"name","meaning","expectation","adjusted_p","verdict_now","direction_matches","direction_tested","reasons"}}`，对应 `screens.members` 里的每个成员；`adjusted_p`、`verdict_now` 取 `screen_ledger.screen_ledger_summary(repo, 设置)` 里的同一成员，读不到设置或链坏时为 `None`（链坏加问题，含"筛选记录的 BH 校正"）；`reasons` 是 `screen_reasons` 的结果，没有 `verdict_inputs` 或没有设置时为空列表。
- 顶层 `bench`：`config/research/research_bench.yaml` 的 `cvar_levels`、`rolling_years`、`rolling_percentile`、`cost_bps`、`delay_days`、`placebo_block_days`、`placebo_draws`、`bootstrap_block_days`、`bootstrap_draws`、`confidence`、`episodes: [{id, name, start, end}]`（`name` 取 `zh.EPISODE_NAMES`，没有就用 id）；文件不存在或读不出来为 `None`（读不出来加问题）。
- 顶层 `trials`：`{"present", "trial_configs", "question_trials", "global_trials", "gate", "plans"}`。`present` = `research/_trials/` 存在；不存在时其余为 0、`None`、空列表。`trial_configs`、`question_trials`、`global_trials` 取 `research_bench.trial.trial_ledger_summary(research)`（题目开发试验加试跑配置）；它的问题加进顶层 `problems`，前缀"试跑: "。`gate` = `{"confidence", "markets": [{"market","name","rules"}]}`，市场顺序 `us_market`、`qqq`，名字"美股大盘长历史（第一段，主检验）"、"QQQ 开发期（第二段，只查方向）"，`rules` 用 `rule_rows`。`plans`：每个 `research/_trials/T*.yaml` 一项 `{"trial_id","signals": [signal_view],"owner_decision","hypothesis_card","from_validation_window","primary","configs": [id],"verdict","verdict_name","card": 主配置的策略卡,"markets": [{"market","name","holds","current_gate","rules"}]}`；`verdict`、`verdict_name`（过门／没过／未完成）取 `trial_ledger_summary`；每个市场取主配置在这个市场第一条 `status` 为 `ok` 的试跑记录：`holds` 是它的 `gate_holds`，`rules` 是它的 `gate` 的 `rule_rows`，`current_gate` 表示这条记录当时判的规则（metric、op、value）和现行门完全一样；没有记录时 `holds` 为 `None`、`rules` 为空。
- `summary` 加 `trial_configs`、`question_trials`（没有 `research/_trials/` 时为 0 和题目开发试验数）；`global_trials` 有 `research/_trials/` 时 = `trials.global_trials`。2026-10-19 核对：页面原来写的累计试验只数了研究题，漏了试跑配置，真实值是 50（题目 27 + 试跑 23），不是 27。
- 说明文件、规格、设置文件缺失或坏掉都不让生成器失败：对应部分按"没有"处理，坏的加问题。

**页面**（5.4 的基础上；全部文字经 HTML 转义，不加脚本和外部资源）：

- 头部：一行汇总里"累计试验"后面有试跑时写 `6（题目 3 + 试跑 3）`；`<p class="guide">` 说明怎么读（含"怎么读"、"怎么买、怎么卖、多久调一次仓"、"信号筛选"、"名词解释"）；`<nav class="nav">` 按顺序链到 `#route`、`#questions`、`#board`、`#screens`、`#trials`、`#glossary`；页面上所有 `href="#…"` 都有对应的 `id`。
- 研究问题的 `<p class="lead">` 写"先看一句话问题和「怎么判」，再看策略怎么买卖、通过门槛和验证期结果"。
- 题卡：有 `zh` 时卡头里的题目用中文标题，下面依次是 `<p class="zhsum">` 中文摘要、`<p class="verdict-rule"><strong>怎么判：</strong>…</p>`（有 `verdict` 时）、`<details><summary>英文原题</summary>`；没有 `zh` 时保持 5.4 的样子（长题目用"题目全文"），再加一行 `<p class="m">还没有中文说明：research/<题号>/zh.yaml</p>`（`error` 的题不加）。
- 题卡里"验证期结果"和"开发期运行"之前是 `<h4 class="sec-h">策略怎么买卖</h4>`：每个 `validated` 的策略一个 `<div class="strat">`（名字、`<span class="id">`、`<span class="b role">角色</span>`；生成的卡先是信号列表 `<ul class="sigs">`（名字、"走高有利/走低有利"、一句含义），再是 `<dl class="rule">`（生成卡不重复"信号"行），有 `levels` 时是 `情形 / 目标仓位` 表；然后一个 `<details>` 通过门槛：没有这个策略的验证期运行时标题"通过门槛：共 N 条（还没有运行结果）"、每条 `·` 加规则的话；有时标题"通过门槛：成立 / 总数 条成立（实测是相对对照的数，正为更好）"，每条 `class="pass|miss"` 带 ✓ 或 ✗、规则的话和实测）。其余策略放进一个折叠 `开发期变体 N 个（只在开发期看，不决定结论）`，里面是 `变体 / 说明` 表：默认角色的变体写它的仓位、规则、调仓，有自己角色的写角色。
- 信号筛选：已筛一句话之后是 `<div class="criteria">` 说明怎么筛、凭什么淘汰（截止日、主检验市场和 horizon、淘汰三条、候选四条、证据不足，数字取 `screen_criteria`；"共 N 个市场"取各成员 `direction_tested` 的最大值），再是成员表 `信号 / 裁决 / 主市场 p（未校正） / 记录条数 / 依据`：信号格 `<span class="sigcell">` 里是名字、id、"预期：…"；裁决徽章按当前裁决（没有当前裁决时按记录的）；依据是 `<ul class="reasons">`，每条 `<li class="ok|fail|eliminate"><span class="mk">✓|✗</span>文字</li>`。没有 `screen_criteria` 时不出现 `class="criteria"`。
- 试跑 `<section id="trials">`：没有 `research/_trials/` 时写"还没有试跑记录。"；否则一段说明、"累计试验 N = 研究题的开发试验 a + 试跑配置 b"、试跑门槛（两个市场各一份规则列表，"对照：平均仓位相同的固定仓位。"），再每个计划一张 `<article class="card plan" id="trial-<编号>">`：编号、信号名（没有信号时"配置 <主配置>"）、裁决徽章（`pass`、`miss`、`unk`）、"主配置怎么买卖"、两个市场的门槛列表；`current_gate` 为假时市场标题后写"（按当时的门判，和现行门不同）"。
- 页尾 `<section id="glossary" class="glossary">` 折叠的名词解释：对照、左尾、最大回撤、几何年化、预注册、秩相关、Benjamini-Hochberg 校正、折算夏普、开发期／验证期；有 `bench` 时再加滚动 N 年几何年化的低位、区间（置信区间）、随机择时、单边成本 bps、晚 N 天执行、去掉一年／去掉一个固定行情段，里面的数字（块长、次数、成本、天数、行情段）都取 `bench`。
- 变化的措辞：运行 01 的"题目全文"、结论板、负结果、研究工程进度不变。

**不在本步**：思路快筛和判断对账面板（TRADING-2568-04）。`research/README.md` 说明新文件的位置和写法；`docs/system_flow.md` 的"展示"一条加上新读的文件。

### 5.8 运行 06：按主次重排（`research_site.v1` 只加不改）

owner 2026-10-22 的决定（第 2 节）。`SCHEMA_VERSION` 不变；5.2 和 5.7 的键和含义不变，只加键；`screens.members` 的每个成员仍然只有 5.2 的五个键（`test_screens_keep_their_shape_and_follow_the_current_verdict` 断言了键的集合），去向放在顶层 `screen_fates`。原则照第 3 节：页面只排版仓库里已有的记录，新块的每个数都读自某个记录文件；读不到的文件按「没有」处理，不算问题；坏掉的表加一条问题，页面照常生成；同样的仓库内容逐字节相同。

#### 5.8.1 新读的文件和新增的模型键

**前瞻记录** `forward_records`，来源 `research/_loop/forward_records.md`：

```
{"present": bool, "markdown": str|None,
 "sections": [{"title": str, "markdown": str, "tables": [str, ...], "prose": str}, ...],
 "rows": [{"id": str, "written": str, "config": str, "expected": str, "check": str, "section": str}, ...],
 "count": int, "pending": int}
```

- `present` = 文件存在；不存在时 `markdown` 为 `None`、`sections` 和 `rows` 为空、`count` 和 `pending` 为 0，不算问题。
- `sections`：按 `## ` 标题切分，和 `negative_sections` 的切法相同（`title` 是标题文字，`markdown` 是标题下的原文，首尾空行去掉）。`tables` 是这一节里每张管道表的原文（连续的以 `|` 开头的行，按出现顺序，各自首尾不留空行）；`prose` 是这一节去掉所有表格行以后的原文（首尾空行去掉）。
- `rows`：文件里每一张表头第一格是 `编号` 的表（任何一节都算），表头以下每一行，去掉分隔行（每格都是 `---` 一类）和第一格为空的行：`id` = 第 1 格，`written` = 第 2 格，`config` = 第 3 格，`expected` = 倒数第 2 格，`check` = 最后一格，`section` = 所在节的标题（表在第一个 `## ` 之前时为空字符串）。格少于 5 个的行跳过。两张表的列数不同（慢熊型 7 列、日报模型 7 列但中间几列含义不同），所以只取首尾。
- `count` = `rows` 的条数；`pending` = `check` 等于 `待核` 的条数。文件存在而 `rows` 为空时顶层 `problems` 加一条，含 `research/_loop/forward_records.md`。

**前瞻账本周汇总** `forward_ledger`，来源 `research/_forward/weekly.md`（`ledger.jsonl` 从不读，页面上没有日收益行）：

```
{"present": bool, "markdown": str|None, "lead": str,
 "sections": [{"title": str, "markdown": str}, ...],
 "days": int|None, "start": str|None, "end": str|None}
```

- `lead` 是第一个 `## ` 之前的原文，去掉以 `# ` 开头的一级标题行，首尾空行去掉；`sections` 同上面的切法（不要 `tables`、`prose`）。
- `days`、`start`、`end` 取原文里第一处匹配 `账本 (\d+) 行，(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})` 的三个组（`days` 转成 `int`）；没有匹配时三项都是 `None`，不算问题。
- 文件不存在时 `lead` 为空字符串，其余为 `None`、空列表，不算问题。

**家族台账** `families`，来源 `research/families.md`：

```
{"present": bool, "markdown": str|None, "counter": str|None, "notes": str, "table": str|None,
 "rows": [{"family","variants","best","price","tags","status","reopen","line"}, ...],
 "status_counts": {"开放": int, "停车": int, "关闭": int},
 "by_signal": {信号 id: 家族名, ...}}
```

- `counter`：原文里第一行以 `距下次横读` 开头的整行（去掉两端空白）；没有为 `None`。
- `## 家族` 这一节：第一张表头第一格是 `家族` 的表是家族表，`table` 是它的原文；`notes` 是这一节在家族表之前的原文（首尾空行去掉）。没有 `## 家族` 节或节里没有家族表时 `table` 为 `None`、`notes` 为空字符串、`rows` 为空，顶层 `problems` 加一条，含 `research/families.md`（文件不存在时不加）。
- `rows`：家族表表头以下每一行，去掉分隔行，恰有 8 格的行按顺序取 `family`、`variants`、`best`、`price`、`tags`、`status`、`reopen`、`line`（每格去掉两端空白）；格数不是 8 的行跳过。
- `status_counts`：各行 `status` 等于三个词之一的计数；别的写法不计。
- `by_signal`：对每一行，用 `[a-z][a-z0-9_]*` 在 `variants` 格里找出所有记号，和 `screens.members` 的 `signal_id` 相同的记进 `by_signal`，值是 `family`；同一个信号在几行出现时取第一行。

**当期章程** `charter`，来源 `research/_loop/charter_p<N>.md`，取 `N` 最大的那个（`N` 是 `charter_p(\d+)\.md` 的整数）：

```
{"present": bool, "path": str|None, "period": int|None, "markdown": str|None,
 "term": str|None, "direction": str|None,
 "questions": [{"title": str, "rule": str|None, "table": str|None, "markdown": str}, ...]}
```

- `path` 是仓库内路径（`research/_loop/charter_p4.md`），`period` = `N`。没有这样的文件时 `present` 为假，其余 `None`、空列表，不算问题。
- `direction`：标题匹配 `^## 第\s*\d+\s*期的方向` 的节里第一段（标题之后第一个非空行起，到下一个空行或下一个标题为止，行按 `\n` 连接后用 `zh.clean`）；没有为 `None`。`term`：标题匹配 `^## 1\.\s*期限` 的节里第一段，同样处理；没有为 `None`。
- `questions`：第一个标题含 `预注册` 的 `## ` 节里，每个 `### ` 子节一项，按出现顺序。`title` 是 `### ` 之后的文字，去掉开头的编号（`^(\d+(\.\d+)*\s+)?`）；`markdown` 是子节标题行之后到下一个 `### ` 或 `## ` 标题之前的原文（首尾空行去掉）；`rule` 是子节里第一组连续的以 `> ` 开头的行，去掉 `> ` 后按 `\n` 连接再 `zh.clean`，没有为 `None`；`table` 是子节里第一张管道表的原文，没有为 `None`。没有含 `预注册` 的节时 `questions` 为空列表。

**漏斗** `funnel`：

```
{"screened": int, "candidates": int, "trials": int, "passed": int, "questions": int, "kept": int, "forward": int}
```

- `screened` = `screens.family_size`；`candidates` = `screens.counts.candidate`（按当前裁决，和信号筛选一节的句子一致）。
- `trials` = `trials.plans` 里 `screens` 非空的计划数；`passed` = 其中 `verdict` 为 `pass` 的计划数。按 owner 决定写的计划（T0002、T0008 一类）不算，它们不是筛选候选。
- `questions` = `questions` 里 `screens` 非空的题数（不论状态）；`kept` = 其中 `decision.words` 里有 `保留` 的题数。
- `forward` = `forward_records.count`。
- 2026-10-22 main fcdee14a 上应得：17 → 7 → 6 → 1 → 2 → 0，前瞻 5。开题是 2 不是复盘线程提案里写的 1：QQQ-Q2 的预注册写了 `screens: [S0002]`（试跑模式上线以前直接开的题），QQQ-Q4 写了 `[S0013]`。

**去向** `screen_fates`：`{str(编号): {...}}`，对应 `screens.members` 的每个成员（键和 `screen_details` 一样是 `str(number)`）：

```
{"screen_id": str, "trials": [{"trial_id": str, "verdict": str, "verdict_name": str}, ...],
 "questions": [{"question_id": str, "state": str}, ...],
 "family": {"name": str, "status": str}|None, "text": str}
```

- `screen_id` = `f"S{number:04d}"`。
- `trials`：`trials.plans` 里 `screens` 含这个 `screen_id` 的计划，按计划顺序；`verdict`、`verdict_name` 照计划的。
- `questions`：`questions` 里 `screens` 含这个 `screen_id` 的题，按 `question_id` 顺序；`state` 和题卡一样：`closed` 为 `已结题`，`error` 为 `记录有问题`，其余 `进行中`。
- `family`：`families.by_signal` 里有这个成员的 `signal_id` 时，`name` 是家族名、`status` 是那一行的 `status`；否则 `None`。
- `text`，按顺序拼：每个试跑一段 `试跑 {trial_id}：{verdict_name}`，每个题一段 `转 {question_id}（{state}）`，用 `；` 连接。一段都没有时：成员当前裁决是 `candidate`（`screen_details` 的 `verdict_now`，没有就用记录的 `verdict`）写 `没试跑`，有家族时再接 `；家族「{name}」{status}`；不是候选时有家族写 `家族「{name}」{status}`，没有写 `—`。

**计划和题各加一个键**：`trials.plans[*]` 加 `"screens": [str, ...]`（`candidate.screens`，没有候选时空列表）；`questions[*]` 加 `"screens": [str, ...]`（预注册的 `screens`，读不出预注册时空列表）。

**Markdown**：`markdown_to_html` 加一种块：连续的以 `> ` 开头的行是引文，输出 `<blockquote><p>…</p></blockquote>`，每行去掉 `> ` 后按 `\n` 连接，行内格式同段落。其余不变。

#### 5.8.2 页面

从上到下的顺序和 `id`（全部文字经 HTML 转义；不加脚本和外部资源；页面上所有 `href="#…"` 都有对应的 `id`）：

1. 头部：标题、`meta`、`sum` 一行、四个结论词的 `legend`、`guide` 都不变。`<nav class="nav">` 改为按顺序链到：`now` 现状、`account` 保险账、`forward` 前瞻记录、`charter` 当期题、`families` 家族台账、`funnel` 漏斗、`screens` 信号筛选、`trials` 试跑、`questions` 研究问题、`board` 结论板、`route` 研究路线、`glossary` 名词解释。
2. `<section id="problems">` 不变，只在有问题时出现。
3. `<section id="now"><h2>现状</h2>` 三行 `<p>`，每行以 `<strong>` 开头：
   - `<strong>结论：</strong>保留 a · 拒绝 b · 证据不足 c · 未测成 d（按子问题计）；筛选 s 个信号 → 候选 c → 试跑 t → 过门 p → 开题 q → 保留 k。` 四个词的数取 `ledger.counts`，后半句取 `funnel`。
   - `<strong>前瞻：</strong>` 有前瞻记录时 `前瞻记录 n 条，待核 m 条；` 再接 `账本 d 个交易日（start 到 end）；`（`days` 为 `None` 时略去这一句）再接 `下一次核对要等 QQQ 划出一个完整的慢熊段或长牛段。`；没有前瞻记录（`present` 为假或 `count` 为 0）时整行写 `还没有前瞻记录。`
   - `<strong>这期：</strong>` 有章程时 `第 N 期章程：{term 的第一句}{direction}`，第一句是 `term` 到第一个 `。` 为止（含句号；`term` 为 `None` 时略去）；`direction` 为 `None` 时略去；没有章程时写 `还没有章程。`
4. `<section id="account"><h2>保险账（前瞻账本）</h2>`：`<p class="lead">每个交易日记三条线的敞口和收益，和同平均敞口的固定敞口比，每周五追加；只描述，不判，核对要等完整的慢熊段或长牛段。</p>`；有文件时先 `markdown_to_html(lead)`，再每一节：前两节 `<h3>标题</h3>` 加 `markdown_to_html(markdown)`，第三节起放进 `<details class="grp"><summary><strong>标题</strong></summary>…</details>`；没有文件时 `<p>还没有前瞻账本：research/_forward/weekly.md</p>`。
5. `<section id="forward"><h2>前瞻记录</h2>`：`<p class="lead">写在核对之前、写下以后不改的预期；到期核对时在「核对」列追加结果。不计数、不进验证、不进组合。</p>`；有文件时 `<p>共 n 条，待核 m 条。</p>`，再每一节 `<h3>标题</h3>`、这一节的每张表 `markdown_to_html(表)`、有 `prose` 时 `<details class="grp"><summary>说明</summary>markdown_to_html(prose)</details>`；没有文件时 `<p>还没有前瞻记录：research/_loop/forward_records.md</p>`。
6. `<section id="charter"><h2>当期题</h2>`：有章程时 `<p class="lead">第 N 期章程预注册的题：规则、预期和判法都在跑之前写下，读完填实际和对错。</p>`，每题一张 `<article class="card">`：`<h3>title</h3>`，有 `rule` 时 `<p class="verdict-rule"><strong>规则：</strong>rule</p>`，有 `table` 时 `markdown_to_html(table)`，再 `<details><summary>全文（预期、判法、失败淘汰什么）</summary>markdown_to_html(markdown)</details>`；`questions` 为空时 `<p>这期章程没有预注册的题。</p>`；`repo_url` 和 `source.commit` 都有时最后一行 `<p><a href="{repo_url}/blob/{commit}/{path}">charter_pN.md</a></p>`；没有章程时 `<p>还没有章程：research/_loop/charter_pN.md</p>`。
7. `<section id="families"><h2>家族台账</h2>`：`<p class="lead">按机制家族累计失败：家族价和状态按台账里写下的规则机械算出，关闭的家族再开题必须写新机制。</p>`；有文件时：有 `counter` 时 `<p>counter</p>`；有 `rows` 时 `<p>开放 a · 停车 b · 关闭 c</p>` 和 `markdown_to_html(table)`；有 `notes` 时 `<details class="grp"><summary>家族价和状态怎么算</summary>markdown_to_html(notes)</details>`；没有文件时 `<p>还没有家族台账：research/families.md</p>`。
8. `<section id="funnel"><h2>漏斗</h2>`：`<p class="lead">从筛选到可信的结论，每一格都读自仓库里的记录。</p>`，`<div class="funnel">` 里六格，格之间 `<span class="fa">→</span>`，每格 `<a href="#目标"><span class="big">数</span><span class="m">名</span></a>`，依次是 已筛信号（`#screens`）、候选（`#screens`）、试跑（`#trials`）、过门（`#trials`）、开题（`#questions`）、保留（`#board`）；下面 `<p class="m">另一条线：前瞻记录 n 条，不计数、只核对，见「前瞻记录」。</p>`。样式表里已有 `.funnel`、`.funnel .big`、`.funnel .fa`。
9. `<section id="screens">` 信号筛选：已筛一句话不变；`<div class="criteria">` 的内容不变，整个放进 `<details class="grp"><summary><strong>筛选标准</strong></summary>…</details>`；成员表表头改为 `信号 / 裁决 / 去向 / 主市场 p（未校正） / 记录条数 / 依据`，去向格按 `screen_fates[编号].text` 的拼法，但试跑编号写成 `<a href="#trial-T0004">T0004</a>`、题号写成 `<a href="#q-QQQ-Q4">QQQ-Q4</a>`（有对应 `id` 时才加链接；没有时写纯文字），家族那段纯文字；行按 当前裁决 `candidate` → `inconclusive` → `eliminated` → 其他，同裁决按编号升序。
10. `<section id="trials">` 试跑：说明、累计试验一行不变；试跑门槛的 `<div class="criteria">` 内容不变（含 `<h3>试跑门槛</h3>`），整个放进 `<details class="grp"><summary><strong>门槛</strong></summary>…</details>`；每份计划的 `<article class="card plan" id="trial-…">` 内容不变，整个放进 `<details class="grp"><summary>编号 · 信号名（或 配置 主配置）· 裁决徽章</summary>…</details>`。
11. `<section id="questions">` 研究问题：`lead` 不变；排序不变（进行中在前）；每张 `<article class="card" id="q-…">` 内容不变，整个放进 `<details class="grp"><summary>题号 · 中文标题（没有时英文题目，超过 80 字截断加 …）· 状态</summary>…</details>`，进行中和记录有问题的题 `<details … open>`，已结题的不 `open`。
12. `<section id="board">`、`<section id="negative">` 不变。
13. `<section id="route">` 研究路线：标记不变（三条入口、研究问题、结论板、负结果清单，含 `quick`、`judgment` 的 idle 节点和「J4 上线后启用」「J3 上线后启用」），只是位置移到这里。
14. `<section id="quick">`、`<section id="judgment">`、`<section id="work">`、`<section id="glossary">` 不变。

样式：只用已有的类；新块不需要新规则。`.now p` 一类的小补充可以加在样式表末尾，不改已有规则。

#### 5.8.3 对已有测试的改动（只这两处）

`tests/test_research_site_zh.py`：

- `test_every_in_page_link_has_a_target`：`nav` 的期望列表改为 `[("now", "现状"), ("account", "保险账"), ("forward", "前瞻记录"), ("charter", "当期题"), ("families", "家族台账"), ("funnel", "漏斗"), ("screens", "信号筛选"), ("trials", "试跑"), ("questions", "研究问题"), ("board", "结论板"), ("route", "研究路线"), ("glossary", "名词解释")]`。
- `test_screens_table_names_signals_and_gives_the_reasons`：`headers` 的期望改为 `["信号", "裁决", "去向", "主市场 p（未校正）", "记录条数", "依据"]`。

其余断言原样全过（题卡、试跑卡的 `_block` 按 `id` 取，折叠不影响；`<div class="criteria">` 里的文字不变）。`tests/test_research_site.py` 一字不改，全过（路线图的节点和占位节都还在）。

#### 5.8.4 不在本步

思路快筛和判断对账面板（运行 04）照旧；前瞻账本的图（续期卡后定）；记分表第 1、3 节的逐行读数不上页（文件太长、还在变），只在家族台账和当期题里引。

#### 5.8.5 任务卡和说明文件

`tasks/TRADING-2568_RESEARCH_PROGRESS_PAGE.yaml`：`status` 仍是 `BASELINE_DONE`，`next_step` 开头加「运行 06（按主次重排）随本提交发布到 main；」（提交里写不进它自己发布后的 sha，真实 sha 由下一批研究记录补），其余不动。`research/README.md` 在研究进展页那一行加上新读的四个文件；`docs/system_flow.md` 的「展示」一条同样加上。

### 5.9 运行 07：家族表和前瞻表的排版修正（只改 render.py、markdown.py 的一处）

模型不变：`SCHEMA_VERSION` 不变，`collect()` 一字不改，`site.json` 逐字节不变。只改页面。

#### 5.9.1 家族台账块（`<section id="families">`）

块内顺序：lead 段（不变）→ 横读计数首句 → 「横读怎么计」折叠 → 状态计数行（不变）→ 7 列表 → 每个家族一条「重开条件」折叠 → 「家族价和状态怎么算」折叠（不变）。

- **横读计数**：`families.counter` 按第一个 `。` 切成首句和其余。首句（含那个 `。`）印成 `<p>…</p>`，行内 markdown 照 5.9.3 的 `inline` 处理（反引号成 `<code>`）；其余非空时印成 `<details class="grp"><summary>横读怎么计</summary><p>…</p></details>`，其余为空时不印这个折叠。`counter` 为空时两者都不印。
- **7 列表**：用 render.py 已有的 `_table`（`table.stack`，`data-label` 照列名），列和顺序固定为 家族、状态、家族价、已试变体（筛选 / 正式）、最好的验证结果、标签分布、线，取自 `families.rows` 的 `family`、`status`、`price`、`variants`、`best`、`tags`、`line`；每格经 `inline`。不印「重开条件」列。只在 `rows` 非空时印表（和状态计数行同一个条件，不变）。
- **重开条件折叠**：表后按 `rows` 顺序，每个 `reopen` 不是空串也不是 `—` 的家族一条：`<details class="grp"><summary>重开条件：{家族}（{状态}）</summary><p>{reopen 经 inline}</p></details>`。`reopen` 为 `—` 或空的家族不印。
- 原 5.8.2 里「家族表原样 `markdown_to_html(families["table"])`」作废；`families.table` 键保留在模型里不动。

#### 5.9.2 前瞻记录表：表头可折行，编号、日期和核对不折行

页面 CSS 在 `table{border-collapse:collapse;width:100%;font-size:14px}` 之后加两行，原文：

```
#forward th{white-space:normal}
#forward td:nth-child(-n+2),#forward td:last-child{white-space:nowrap}
```

只作用于前瞻记录块里的表。全局 `th` 是 `white-space:nowrap`，这个块的表头「慢熊段汇总（美股大盘：……）」太长，不让它折行时整张表撑出容器；所以这里表头可折行，编号、写下两列和最后的核对列不折行。在 1500 像素宽的浏览器里核过：表不出容器，`F0001`、`2026-10-21`、`待核` 各一行。

#### 5.9.3 markdown.py 加一个公开函数

```python
def inline(text: str) -> str:
    """One cell or line of markdown: code and bold rendered, everything else escaped."""
    return _inline(text)
```

放在 `markdown_to_html` 之前；`_inline` 本身不动。render.py 改为 `from .markdown import inline, markdown_to_html`。

#### 5.9.4 验收

`tests/test_research_site_layout.py` 43 条、`tests/test_research_site_zh.py`、`tests/test_research_site.py` 全过；ruff check、ruff format --check、mypy 干净。真实仓库上生成的页面：`site.json` 和运行 06 逐字节相同；`index.html` 的家族台账块表头恰好 7 个 `<th>`，没有 `<th>重开条件</th>`，有 `重开条件：经典趋势（关闭）` 等折叠；CSS 含 5.9.2 的两行。

#### 5.9.5 任务卡和说明文件

`tasks/TRADING-2568_RESEARCH_PROGRESS_PAGE.yaml` 的 `next_step` 把「运行 06（按主次重排）随本提交发布到 main；」改成「运行 07（家族表和前瞻表排版修正）随本提交发布到 main；运行 06 已发布（main 98b9d860）；」；`docs/task_register.md` 同句同改。`research/README.md`、`docs/system_flow.md` 不改（读的文件没变）。

## 6. 步骤

| 运行 | 区 | 内容 | 验收 |
|---|---|---|---|
| TRADING-2568-01 | A、B | 本需求文档、任务卡、`research_site` 包、验收测试 `tests/test_research_site.py`（Claude 写，原样放入）、`research/README.md` 加一行 | 验收测试全过；全量 PR 套件不新增失败；`ship.py --dry-run` 为 zone B |
| TRADING-2568-02 | C | `.github/workflows/research-site.yml`，trailer 引本决定 | owner 按 tree 批准；发布后 workflow 成功，页面地址可打开 |
| TRADING-2568-03 | A、B | 2026-10-19 owner 的要求（第 2 节、5.7）：`research_site/zh.py`、`collect.py` 和 `render.py` 的改动、样式、`tests/test_research_site_zh.py`（Claude 写）、说明文件 `research/signal_names.yaml` 和 `research/QQQ-Q1..Q4/zh.yaml`（Claude 写）、`research/_template/zh.yaml`、`research/README.md`、`docs/system_flow.md`、本文档 | 验收测试 `test_research_site_zh.py` 和 `test_research_site.py` 全过；`ruff`、`mypy --strict` 干净；`ship.py --dry-run` 为 zone B；发布后 Pages 重建，真实页面里能看到每个策略的买卖规则、淘汰标准和试跑 |
| TRADING-2568-04（待写） | B | 接 TRADING-2569 的快筛面板和判断对账面板（公开页只放状态、样本进度、还差几个事件和最小可检出效应） | 等 TRADING-2569 J3、J4 的记录出现再写需求和测试 |
| TRADING-2568-06 | A、B | 2026-10-22 owner 的决定（第 2 节、5.8）：`collect.py`、`render.py`、`markdown.py` 的改动、`tests/test_research_site_layout.py`（Claude 写）、`tests/test_research_site_zh.py` 的两处断言（5.8.3）、`research/README.md` 和 `docs/system_flow.md` 加上新读的文件、本文档 | `test_research_site_layout.py`、`test_research_site_zh.py`、`test_research_site.py` 全过；`ruff`、`mypy --strict` 干净；`ship.py --dry-run` 为 zone B；发布后 Pages 重建，真实页面顶部是现状、保险账、前瞻记录、当期题、家族台账，信号表有去向列 |
| TRADING-2568-07 | A、B | 家族台账改 7 短列、重开条件按家族折叠、横读计数只露首句，前瞻记录表表头可折行、编号、日期和核对不折行（5.9）。只改 `render.py` 和 `markdown.py`，模型不变。文件：`render.py`、`markdown.py`、`tests/test_research_site_layout.py`（Claude 写）、本文档、任务卡 | `test_research_site_layout.py`（43 条）、`test_research_site_zh.py`、`test_research_site.py` 全过；`ruff`、`mypy --strict` 干净；`ship.py --dry-run` 为 zone B；`site.json` 和运行 06 逐字节相同；发布后 Pages 重建，真实页面的家族台账是 7 列表加每个家族一条重开条件折叠 |

## 7. 进展

- 2026-10-14：登记，owner 选 GitHub Pages（第 2 节）。production_effect=none；broker_action=none。
- 2026-10-14：第二版版式（owner 要一个读得懂、覆盖当前工作的展示）：研究路线图、先说结论的问题卡、按结论词分组的结论板、负结果分节、手机上表格叠放；`TASK_PREFIXES` 加 `TRADING-2569`；owner 选判断对账公开页只放状态和进度。
- 2026-10-14：运行 01 之前补上一题多个子问题的结论词（`decision.md` 的结论表、台账一格两个词），因为 QQQ-Q1 的 Q1a、Q1b 是这样写的。
- 2026-10-15：运行 01（页面生成器）已发布；运行 02 加 GitHub Pages workflow，按 tree 批准。
- 2026-10-19：owner 要页面把策略的买入、卖出、调仓规则和淘汰标准写具体、用中文（第 2 节）。运行 03 写成需求（5.7）和验收测试；原来的运行 03 内容（快筛、判断对账面板）顺延为运行 04。核对时发现页面写的累计试验漏了试跑配置（27 → 50），一并改正。
- 2026-10-20：运行 03 发布到 main 12af29a9（pi 写代码，门禁整套 5142 条过），线上页面核对过：页面含「策略怎么买卖」「怎么筛、凭什么淘汰」，累计试验 55（题目 32 + 试跑 23）。pi 的实现和 Claude 的参考实现在同一份仓库内容上逐项对照，只有不拦发布的差别：多信号符号打分的情形表写「3/4 个信号有利」（需求写的是“有利信号的个数”）；秩相关写三位小数；情形表没有用 `narrow` 样式类（样式表里有这条规则，页面上两列小表在桌面宽度下占满行宽）。以后动这些句子要连测试一起改。任务卡改为 BASELINE_DONE，剩运行 04。
- 2026-10-22：owner 看了页面说信号展示主次不清（第 2 节），在卡上选「按提案重排」。运行 06 写成需求（5.8）和验收测试 `tests/test_research_site_layout.py`；现有两份测试只改 5.8.3 的两处断言。核对时发现提案里的「开题 1」应是 2（QQQ-Q2 也由筛选候选 S0002 开题）。
- 2026-10-22：owner 18:07 JST 看线上页面的家族台账，问「这个排版是预期内的吗」；不是，是运行 06 把 8 列表原样印出造成的。运行 07 写成需求（5.9）和 `test_research_site_layout.py` 的三处改动；整页在浏览器里渲染核对过，其余块正常。
