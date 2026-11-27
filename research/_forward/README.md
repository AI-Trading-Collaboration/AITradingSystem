# 前瞻记录台

研究第一次验证日报本身（第 3 期章程第 3 节第 3 条）。从 2026-10-12 起，每个美股交易日一行，记三条线的敞口和收益：日报现在的模型、固定敞口 0.8、F0001（QQQ-Q3 的 fast_slow 覆盖）。脚本 `src/ai_trading_system/research_probes/forward_ledger.py`（TRADING-2565-45），账本 `ledger.jsonl`，周汇总 `weekly.md`；第 6 期章程第 3.3 节（`owner_decision:TRADING-2565:2026-10-23:research_loop_p6`）给周汇总加的几样由 TRADING-2565-60 做。

依据：

- Baker 2026-10-22 00:06 JST（2026-10-21T15:06:51Z）在「研究流程评审与结果分析」线程的卡「定日报仓位方案:固定敞口,还是固定加趋势覆盖?」上选「fast_slow 覆盖」（`owner_decision:TRADING-2565:2026-10-22:daily_policy`），选项原话：「方案记为 F0001(满仓为底,快慢两个趋势信号各管一半,平均敞口 0.66);账本记日报模型、固定敞口、它三条线,每周五追加。」固定线的档位他没另说，按复盘线程卡后的说明（选 A 或 B 时固定线默认记 0.8）和下一张卡上写明的「固定 0.8」记 0.8。
- 2026-10-22 00:07 JST（15:07:30Z）在同一线程的卡「可以让前瞻记录台只读日报的输出吗?」上选「可以」（`owner_decision:TRADING-2565:2026-10-22:read_daily_outputs`），选项原话：「前瞻记录台打包交 pi 队列(A 区记录加 B 区脚本),发布后每周五追加,F0005 的预期写在第一行之前。」卡上点名的两份文件见下面「读什么」；之后不再问，直到第一次慢熊段核对。

需求：复盘线程的项目文件 `knowledge/claude/research-review/forward-ledger-requirements-2026-10-21.md`（含第 7 节修订；第 6–8 条是循环线程打包前提出、复盘线程同意的价格来源、现金和时点）。

## 读什么（只读）

都在日报的状态根目录下（命令的 `--daily-root`）。只读，不写这些目录，不碰生产、日报、券商，不改日报的模型。

- `data/processed/decision_snapshots/decision_snapshot_<日期>.json`：`positions.final_risk_asset_ai_band`（日报发布的「AI 风险资产仓位区间」：min_position、max_position、label），`positions.model_risk_asset_ai_band` 只取中点、只印。
- `data/raw/daily_input_capture/<日期>/market_macro/prices_daily.csv`：QQQ 的 adj_close。捕获文件是日报的累计缓存（2018 年起），后面的文件含前面的日子。行按文件里的价格日期记：一天的 QQQ 收益（那天和它前一行）取自从那天起第一份含那天价格的捕获文件，通常就是当天那份；当天的文件缺了或者里面还没有当天的价格，就取之后最近的一份，三条线都不断。一次追加只写到最后那份文件里最后的价格日。F0001 的信号要 252 个交易日的回看，取这次追加的最后一天那份文件里的 QQQ 历史；`append` 的输出写明是哪份文件。
- `weekly --daily-root` 时再读账本最后一行的 `price_file`（同一份价格捕获文件）里 QQQ 和 SGOV 的 adj_close：QQQ 印离运行最高的回撤，SGOV 印现金按 SGOV 的一组（第 6 期章程第 3.3 节）。记录起点以前的价格只在这里用来定运行最高和划段，不印，也不算别的指标；以前的段（含 2022）不印；这不是一致性窗口或验证窗口的读取，不计数。

## 每行记什么

| 字段 | 内容 |
|---|---|
| `date`、`seq`、`backfill` | 交易日（价格日期）、行号；2026-10-20 及以前的行是回填 |
| `qqq_tr`、`price_file`、`price_last_date` | QQQ 当日总收益（adj_close）、取自哪份价格捕获文件、那份文件里最后的价格日；文件里没有那天的价就记空、注明 |
| `exp_daily`、`daily_min`、`daily_max`、`daily_label`、`snapshot_file` | 当天发布的区间中点、区间和标签、文件名；当天没有快照就记空、注明 |
| `daily_model_mid` | 模型区间（未经置信度和门调整）的中点，只印 |
| `exp_daily_held` | 日报模型那条线当天持有的敞口：两个交易日前发布的区间中点；那天没发布就沿用更早的最近一份（最早找到 2026-10-08） |
| `exp_fixed` | 0.8，每天回到 0.8 |
| `overlay`、`exp_overlay` | F0001；当天持有的敞口 |
| `ret_daily`、`ret_fixed`、`ret_overlay` | 各线当天收益 = 持有敞口 × `qqq_tr`；任一项空就记空 |
| `notes` | 空缺的原因 |
| `code_commit`、`code_modified` | 写这一行的代码提交；有改动时脚本拒绝追加 |
| `prev_sha256` | 上一行字节的 sha256（CRLF 按 LF 算），第一行为空 |

## 规则

- 时点和评估台相同：t 收盘定的敞口在 t+1 收盘交易，t+2 那天持有。这是评估台的口径，日报自己的意图可能是 t+1 持有；行里存了 as_of 日期和区间，t+1 的变体以后可以从行重算，现在不做。
- F0001 照 QQQ-Q3 预注册的 fast_slow 原样（21 日和 252 日收益的符号各管一半），用评估台的 `signal_exposure` 和 `simulate`：月底调仓，两次调仓之间持仓随价格漂移，不加杠杆，成本 0；2026-10-09 收盘按 9 月底的目标建仓。
- 现金收益记 0：三条线的收益都是「敞口 × QQQ 当日收益」，敞口低的线少记了现金利息，每年大约是敞口差乘短期利率，方向是让覆盖线和日报模型线的核对更难过；F0005 的预期按现金 0 的口径写。第 6 期章程第 3.3 节（`owner_decision:TRADING-2565:2026-10-23:research_loop_p6`）定：周汇总另印一组现金按同一个价格文件里的 SGOV 算的累计，只印；账本的行和三条线不改，不从起点重算，现金 0 仍是判的口径（F0005、F0008 的预期按现金 0 写，重算等于改已写预期的口径）。收益不扣成本。
- 把日报的「AI 风险资产仓位」当成 QQQ 敞口是推断：日报的区间是 AI 风险资产占的仓位，不是 QQQ 的权重。
- 前瞻起点按快照生成的时间：Baker 点卡（2026-10-21T15:06:51Z）以前生成的快照是回填，按 as_of 是 2026-10-12 到 10-20；as_of 10-21 起是前瞻。`backfill` 按行的日期标；日报模型那条线 t+2 持有，10-21、10-22 两行的收益用的还是回填的快照（as_of 10-19、10-20），用前瞻快照的收益从 10-23 起。
- F0001 的回看落在一致性窗口（2021-02-22 起）里，只用来算记录日当天的信号；记录起点以前的收益和指标一概不算、不印。这不是一致性窗口或验证窗口的读取，不计数。
- 只追加：写下的行不改。`check` 核链和行号、日期是连续的美股交易日、`backfill` 标记、敞口在 0 到 1、日报中点等于区间两端的平均、各线收益等于敞口乘 QQQ 收益；`check --day <日期>` 再读那天的快照和那一行用的价格捕获文件，核对那一行。`check` 和周汇总还印一句价格捕获文件的文件夹日期和它里面最后的价格日相差几个交易日、是不是恒定（只印，不拦）。

## 加的线（第 7 期 5.4a，TRADING-2565-135）

第 7 期章程（`owner_decision:TRADING-2565:2026-11-01:research_loop_p7`）第 3.3 题「F0001 × 2，加，只记账」、第 3.5 题「周汇总另印一列总资产口径，只印」和第 5.4 节。新文件 `added_lines.jsonl`，主账本 `ledger.jsonl` 和它的模式一字不动。模块 `src/ai_trading_system/research_probes/forward_added.py`；`forward_ledger` 的 `append`、`check`、`weekly` 三个命令各在末尾调它一次。

- 起点 2026-11-19（`ADDED_START`）：第一行就是那一天，以前的日子不记、不回填。
- 每行挂主账本同一天那一行：`ledger_seq`、`ledger_sha256`（那一行字节的 sha256，就是主账本下一行的 `prev_sha256`），`qqq_tr` 和 `price_file` 照抄。价格文件就是主账本那一行记的；快照主账本没记文件名，按主账本日报那条线的同一规则找（前两个交易日或更早、最近一份），`check --day` 核它最终区间的中点等于主账本的 `exp_daily_held`。
- **F0001 × 2**：目标是账本 F0001 那条线的目标（0、0.5、1）乘 2。调仓行是主账本 F0001 回到目标的那一行：月底收盘定、下一个收盘调、再下一个交易日起按新敞口，也就是每个月最后一个交易日之后的第 2 个交易日（2026-11 是 11-03）；调仓行的 `target_overlay2` = 2 × 主账本那一行的 `exp_overlay`，别的行等于上一行的；第一行取起点当天或以前最近一个调仓行（2026-11-19 时是 11-03 的 0.5，所以是 1）。
  - 当天持有的敞口 w：第一行和调仓行是 `target_overlay2`，别的行 w = 上一行的 w × (1 + q) ÷ (1 + r)（q 是 `qqq_tr`，r 是那一行的收益），接着上一行算、不从头重算，价格文件以后改了以前的价也不动已写的行。
  - 借的部分 b = max(w − 1, 0)，付同一份价格文件里 SGOV 当天的收益 s 加 1%/年（按 252 天摊）：r = w × q − b × (s + 0.01 / 252)。w ≤ 1 时余下的现金记 0（不拿 SGOV），r = w × q。不扣调仓成本。
  - 缺：q 空，或 b > 0 而 s 空，那一行 r 记空、注明，之后的 w 也空，直到下一个调仓行；w 是 0 或 1 时下一行的 w 不用 q。
  - 1 + r ≤ 0 时 r 记 −1、注明「净值归零」，之后的行 w、b、r 都空（调仓行也不再回到目标）。
- **总资产口径一列**：日报快照里 `positions.total_asset_ai_band` 的中点，t+2 持有，用的快照和主账本日报那条线同一份（前两个交易日或更早、最近一份，最早找到 2026-10-08）。只印，判的口径照旧是 `final_risk_asset_ai_band`；读不到或不是合法区间，这一列空、注明，不挡别的列和主账本。
- 字段：

| 字段 | 含义 |
|---|---|
| `schema_version` | `forward_added.v1` |
| `seq`、`date` | 行号（1 起）和日子 |
| `ledger_seq`、`ledger_sha256`、`qqq_tr`、`price_file` | 主账本同一天那一行的行号、字节摘要、QQQ 收益和价格文件 |
| `target_overlay2`、`exp_overlay2`、`borrow_overlay2` | F0001 × 2 的目标（0、1、2）、当天的敞口 w（不设上界）、借的部分 b |
| `sgov_tr`、`ret_overlay2` | 同一份价格文件里 SGOV 当天的收益（b = 0 也记，没有就空）和 F0001 × 2 当天的收益 |
| `total_min`、`total_max`、`total_snapshot_file`、`exp_total_held`、`ret_total` | 持有的那份快照的总资产区间、文件、中点和中点 × `qqq_tr` |
| `notes` | 空缺的原因 |
| `code_commit`、`code_modified`、`prev_sha256` | 同主账本 |

- 读坏不挡主账本：`append` 先照旧写主账本；加的线算不出或新文件已有的行核不过，主账本照写、命令退出 1、新文件不写，下一次 `append` 自动补齐。新文件允许落后主账本。退出 1 时，`forward_ledger:` 那几行都正常、只有 `forward_added:` 报错，就是主账本已经写好：照常提交 `ledger.jsonl` 和 `weekly.md`，把 `forward_added:` 那几行报给循环线程。
- 调仓行主账本的 `exp_overlay` 不是正好 0、0.5 或 1（包括空的）时，加的线停在那一行，要另一批定怎么接；这是「下一次 `append` 自动补齐」唯一不成立的情况。
- `check` 另核新文件的链、行号、从起点起和主账本逐行对上、中间没有缺、行内等式（容差 1e-12）；`check --day <日期>` 再读那天的价格文件和快照，还核持有的那份快照里 `final_risk_asset_ai_band` 的中点等于主账本那一行的 `exp_daily_held`。
- `weekly` 在原来各节后面加「加的线」一节：F0001 × 2、同期买入持有、同期 F0001、日报总资产口径、同期日报模型的交易日、缺收益、累计、最大回撤、平均敞口；平均借的比例、最大的 w、累计利息、对买入持有和对 2 × 同期 F0001 的累计对数差、期间 SGOV 年化、空仓拿 SGOV 时的累计（只印）、借钱的日子里 SGOV 收益小于 0 或大于 0.1% 的日子，和「加的线落后主账本 N 行」。
- 前瞻记录 F0009（`../_loop/forward_records.md`「账本加的线」）。两条按财报日程降仓的线（5.4b）等 11-27 续期卡。
- 净值归零以后，周汇总那一节不印两个累计对数差（对数在 −1 没有定义），只印归零的日子。
- 2026-11-19 以后的周五追加批同时提交 `added_lines.jsonl`，研究会话把它原样打印给循环线程，同主账本。

## 冻结的线（第 7 期 5.7，TRADING-2565-137）

第 7 期章程第 5.7 节：同回撤总账表（TRADING-2565-134，`owner_decision:TRADING-2565:2026-11-10:same_drawdown_table`）照跑前写死的挑法挑出 `x2_s5_under_ladder`（TRADING-2565-126 的规则：X2 出场、分四档买回、结构转变带 L 止损），Baker 2026-11-16 15:20 JST 在冻结卡上选「冻，只记账」。规则原样冻进账本，只记账，不进日报、不动仓位。新文件 `frozen_lines.jsonl`（`forward_frozen.v1`），主账本 `ledger.jsonl`、`added_lines.jsonl` 和它们的模式一字不动。模块 `src/ai_trading_system/research_probes/forward_frozen.py`；`forward_ledger` 的 `append`、`check`、`weekly` 三个命令末尾在加的线之后各调它一次。

- 起点 2026-11-23（`FROZEN_START`）：第一行就是那一天，以前的日子不记、不回填。每行挂主账本同一天那一行：`ledger_seq`、`ledger_sha256`、`qqq_tr`、`price_file` 照抄。
- 用哪份收盘：第 t 行用主账本第 t 行记的价格文件里 QQQ 的 `adj_close`，从 2024-01-02（`WALK_FROM`）到 t，一个交易日都不能缺；t 以后的收盘不用。那份撑不起（文件不在、读不了、缺一个交易日）时，用主账本 t 以后第一个撑得起的那一行的价格文件，同样截到 t，`rule_price_file` 记实际用的那份、`notes` 注明；t 以后还没有撑得起的，就停在 t，下一次 `append` 有了再补。
- 规则照原模块调用（`structure_shift_ladder.walk_ladder`，`exit_under` 的 X2、`v0_entries`、`shifts`），不另写；从锚点 2025-05-12（表在那天回到 X 买满）满仓起走。最后一个收盘响的出场（X2 或 L）当天就记；X2 和 L 同一个收盘响时记 X2。
- 时序：收盘 t 的决定在 t+1 收盘调。卖出和表同一个收盘（表也是出场响的下一个收盘卖），买进比表晚一个收盘（表在决定的那个收盘买）。目标：出场响是 0，买满是 1，只有分档成交时是规则不扣成本的 QQQ 份额 `rule_frac`。两次调仓之间 w 按 w(1 + q) ÷ (1 + w q) 漂，0 和 1 不漂；q 空时那一行收益空、之后 w 空到下一次调仓。收益 = w × q，空仓记 0（不拿 SGOV），不扣成本。第一行从锚点按同一规则模拟到起点；以后的行接着写过的行，不重算。
- 两道核对，过了才写：A，截到表的快照末 2026-10-12 走出来的买卖要和表印的那一笔一模一样（2026-03-20 X2、03-23 卖、04-13 结构转变买满，没有分档，L 在 03-30），锚点那一档的比值和回到 X 的那天也要对上，锚点以前不能有信号；B，已写的行（和这一次前面刚算出的行）的决定重走以后不变，第一行的调仓也不变。任何一道不过，冻结的线这一次一行都不写、退出 1，主账本和加的线照写。核对 A 不过不在包里预先放宽，停下交人定。
- 字段：

| 字段 | 含义 |
|---|---|
| `schema_version` | `forward_frozen.v1` |
| `seq`、`date` | 行号（1 起）和日子 |
| `ledger_seq`、`ledger_sha256`、`qqq_tr`、`price_file` | 主账本同一天那一行的行号、字节摘要、QQQ 收益和价格文件 |
| `rule_price_file` | 规则实际走的那份价格文件：一般就是 `price_file`，撑不起时是主账本 t 以后第一个撑得起的那一行的 |
| `sgov_tr` | 同一份价格文件（`price_file`）里 SGOV 当天的收益（没有就空），只给周汇总印 |
| `line` | `x2_s5_under_ladder` |
| `rule_actions` | 这个收盘规则的决定：`fire`（X2 或 L）、`fill`（档位和份额）、`full`（怎么买满，结构转变时带 L 那天） |
| `rule_frac` | 这个收盘以后规则不扣成本的 QQQ 份额 |
| `trade_frozen`、`exp_frozen`、`ret_frozen` | 这个收盘调到多少（没有调仓是空）、当天持有的敞口 w、当天的收益 |
| `phase`、`sale_date`、`filled_levels`、`close_vs_x` | `in`（满仓）、`fired`（出场响了，下一个收盘卖）、`out`（卖了，等买回）；`out` 时卖出那天、已成交的档、离 X 多少 |
| `high_date`、`close_vs_high`、`floor_date`、`close_vs_floor`、`close_vs_line` | 满仓时 X2 最高那天和离最高多少、L 那天和离 L 多少；离 200 日线多少 |
| `notes` | 空缺的原因、用了别的价格文件 |
| `code_commit`、`code_modified`、`prev_sha256` | 同主账本 |

- 读坏不挡主账本和加的线；新文件允许落后主账本。退出 1 时，`forward_ledger:` 那几行都正常、只有 `forward_added:` 或 `forward_frozen:` 报错，就是主账本已经写好：照常提交，把报错的那几行报给循环线程。
- `check` 另核新文件的链、行号、从起点起和主账本逐行对上、中间没有缺、键、`rule_price_file`、行内等式和状态的前后衔接（容差 1e-12），坏行只印行号和原因、不抛；`check --daily-root` 再用最后一行的 `rule_price_file` 跑核对 A、B；`check --day <日期> --daily-root` 读那一行记的 `rule_price_file`，核它是那一行或主账本更晚一行的价格文件、撑得起、重走出来的决定和状态和写着的一样，`sgov_tr` 和重读自己那份的一样；那一行自己那份现在也撑得起、或写着空的 `sgov_tr` 现在读得出数时，只印一句提示。
- 起点以前 `append` 和 `check --daily-root` 不读价格、不核 A，返回成功。
- `python -m ai_trading_system.research_probes.forward_frozen guard --daily-root <日报状态根>`：只读，用主账本最后一行的价格文件（或 `--price-file`）跑核对 A，印锚点以后规则的每个决定和现在的状态；过了退出 0，不过退出 1。
- `weekly` 在原来各节后面加「冻结的线」一节：冻结的线（账本口径）、同期买入持有、同期 F0001、同期固定 0.8 的交易日、缺收益、累计、最大回撤、平均敞口；现在的状态；起点以来规则的每个决定和账本的每次调仓；空仓那部分拿 SGOV 时的累计、调仓次数、换手和按表的 0.05% 算的成本（只印）；表里印过的最深回撤两行；「冻结线落后主账本 N 行」。还没有行时只印「冻结的线自 2026-11-23 起记，还没有行。」
- 前瞻记录 F0010（`../_loop/forward_records.md`「冻结的线」）。
- 2026-11-23 以后的周五追加批同时提交 `frozen_lines.jsonl`，研究会话把它原样打印给循环线程，同主账本。

## 日报换成满仓为底以后（TRADING-2565-139）

Baker 2026-11-19 选「满仓为底，闸门只提示」（`owner_decision:TRADING-2572:2026-11-19:full_exposure_default`，TRADING-2572）：生产日报的 `final_risk_asset_ai_band` 默认 1.0–1.0（「满仓（默认）」），按原规则的区间另记在 `gated_risk_asset_ai_band`。主账本 `ledger.jsonl` 的模式和已写的行一个字节不动，日报那条线照旧读 `final_risk_asset_ai_band`；`added_lines.jsonl`、`frozen_lines.jsonl` 不碰；固定 0.8 照旧。只改周汇总（`forward_ledger.py`）。

- 两张主表（自 2026-10-12、自 2026-10-21）各加最后一行「满仓 1.0（= QQQ 持有，自 <起点> 起）」：敞口每天 1.0，收益就是账本的 `qqq_tr`，缺收益同 `qqq_tr` 空的行；现金 0、不扣成本。标签写明起点，和「冻结的线」一节自 2026-11-23 起的「买入持有」那行区分。
- 日报线分段的判据：日报线持有的那份快照里 `positions.position_policy.mode` 等于 `full_exposure_default`。不是看有没有这一块：按原规则写的 `gated_min` 不算，换包以前的快照没有这一块，也不算。日报线持有两个交易日以前的区间（找快照的规则同上面 `exp_daily_held`：前两个交易日或更早、最近一份），所以「换包后」第一行是持有第一份满仓快照的那一行，比那份快照的 as_of 晚两个交易日；从那一行起都算换包后。新发布包第一次正式运行是 2026-11-21 12:00 JST（as_of 2026-11-20），按计划第一份满仓快照 as_of 2026-11-20，换包后第一行是 **2026-11-24**；实际的日子以周汇总印的为准。
- `weekly --daily-root` 时在「按周」后面加「日报线换包前后（自 2026-10-12）」一节：判据、第一份满仓快照和换包后第一行的日子，一张表三行（交易日、缺收益、累计、最大回撤、平均敞口）：日报模型换包前；「同平均敞口的固定（换包前日报模型的对照）」，敞口取换包前那段日报模型有收益的行的平均持有敞口，算法同主表那条对照；日报模型换包后。还没有满仓快照时换包后那行是 0 个交易日。没给 `--daily-root` 时只印一句「不分段」。只读快照；快照读不了或不是 JSON 时报错、不写周汇总（同 `append` 读快照）。
- F0005 写的是换包前的日报模型（按原规则压仓），前后分开读：核对时只读换包前那段，对照用上一条里换包前那段自己的同平均敞口固定，不改 F0005 的数；换包后的日报线就是满仓 1.0。两张主表里日报模型那行和它的同平均敞口对照跨了两段，平均敞口混了两种规则，只印。
- 「加的线」里的「日报总资产口径」读快照的 `total_asset_ai_band`，满仓快照里它也是 1.0，所以从同一天（按计划 2026-11-24）起那一列也读到 1.0。
- 按原规则的仓位从 2026-11-24 起由下一节「闸门线（提示）」记（TRADING-2565-142）。

## 闸门线（提示）（第 8 期 3.4，TRADING-2565-142）

第 8 期章程（`owner_decision:TRADING-2565:2026-11-27:research_loop_p8`，Baker 2026-11-27 17:06 JST 照推荐签）第 3.4 题、第 5.1 节：日报自 as_of 2026-11-20 起满仓为底（上一节），日报线从 2026-11-24 起读到 1.0；按原规则算出的区间记在快照的 `gated_risk_asset_ai_band`，这条线把它记下来，换包以后它是日报信号唯一的前向记录。只记账，不进日报、不动仓位。新文件 `gate_lines.jsonl`（`forward_gate.v1`），主账本 `ledger.jsonl`、`added_lines.jsonl`、`frozen_lines.jsonl` 和它们的模式一字不动。模块 `src/ai_trading_system/research_probes/forward_gate.py`；`forward_ledger` 的 `append`、`check`、`weekly` 三个命令末尾在冻结的线之后各调它一次。

- 起点 2026-11-24（`GATE_START`）：主账本里持有 as_of 2026-11-20 那份快照的第一行（t+2），以前的日子不记、不回填。每行挂主账本同一天那一行：`ledger_seq`、`ledger_sha256`、`qqq_tr`、`price_file` 照抄。
- 持有：第 d 行持有 d 前两个交易日或更早、最近一份存在的快照（找法同 `exp_daily_held`），最早只找到 2026-11-20（`GATE_HELD_FROM`：换包以前的快照没有 gated 键，不读）；一份都没有时快照各列空、注明「2026-11-20 起没有可持有的快照」。和主账本日报线持有的是同一份。
- 读：那份快照 `positions.gated_risk_asset_ai_band` 的 `min_position`、`max_position`（0 到 1 的有限数、min ≤ max），中点就是当天的敞口，收益 = 中点 × `qqq_tr`；现金 0，不扣成本。0–0 也是合法的敞口 0，照记（成绩单那条要做除法的线才记缺）。`label` 是字符串就照记。不按 `position_policy.mode` 分支：`gated_min` 模式下 gated 区间等于最终区间，照读 gated 键。同一份快照的 `gated_total_asset_ai_band`（闸门总资产口径）、`position_policy.mode`，和同一价格文件里 SGOV 当天的收益只印。
- 字段：

| 字段 | 含义 |
|---|---|
| `schema_version` | `forward_gate.v1` |
| `seq`、`date` | 行号（1 起）和日子 |
| `ledger_seq`、`ledger_sha256`、`qqq_tr`、`price_file` | 主账本同一天那一行的行号、字节摘要、QQQ 收益和价格文件 |
| `gate_snapshot_file` | 持有的那份快照（相对路径）；一份都没有时空 |
| `policy_mode` | 那份快照的 `position_policy.mode`（只印） |
| `gate_min`、`gate_max`、`gate_label` | 闸门区间和标签 |
| `exp_gate_held`、`ret_gate` | 区间中点（当天持有的敞口）、中点 × `qqq_tr` |
| `gate_total_min`、`gate_total_max`、`exp_gate_total_held`、`ret_gate_total` | 闸门总资产区间、中点、中点 × `qqq_tr`（只印） |
| `sgov_tr` | 同一份价格文件里 SGOV 当天的收益（只印） |
| `notes` | 空缺的原因 |
| `code_commit`、`code_modified`、`prev_sha256` | 同主账本 |

- 缺：快照读不了、不是 JSON 或 `positions` 不是对象时文件名照记、其余空；闸门区间缺或不合法时闸门几列空、总资产几列照读，总资产缺时只空它自己那几列；`qqq_tr` 空时敞口照记、收益空。闸门区间空的行 `notes` 一定不空。行写下以后不改，没有跨行的状态。
- 读坏不挡主账本、加的线和冻结的线；新文件允许落后主账本，下一次 `append` 补齐；一次追加和分几次追加写出来的字节一样；主账本还没有 2026-11-24 以后的行时不建文件，印「闸门线自 2026-11-24 起记，还没有行」。退出 1 时只有 `forward_gate:` 报错，就是别的都写好了：照常提交，把报错的那几行报给循环线程。
- `check` 另核新文件的链、行号、从起点起和主账本逐行对上、中间没有缺、键、快照路径是 2026-11-20 到前两个交易日之间的一份、行内等式（中点、收益，容差 1e-12），坏行只印行号和原因、不抛；`check --day <日期> --daily-root` 重找那天持有的快照，重读两组区间、标签、仓位方式和 SGOV，再核那份快照最终区间的中点等于主账本那一行的 `exp_daily_held`。
- `weekly` 在冻结的线后面加「闸门线（提示）」一节：闸门线、同平均敞口的固定（闸门线的对照）、买入持有、日报线、固定 0.8、F0001、闸门总资产口径（只印）七行（交易日、缺收益、累计、最大回撤、平均敞口）；闸门线对同平均敞口固定和对买入持有的累计对数差（满 252 个有收益的交易日另印几何年化差）；现金按 SGOV（只印）；持有快照的仓位方式；闸门区间变了的日子（次数和最近 10 次）；「闸门线落后主账本 N 行」。还没有行时只印「闸门线自 2026-11-24 起记，还没有行。」和落后行数。这一节印的是自 2026-11-24 的全程，只作描述。
- 前瞻记录 F0011（`../_loop/forward_records.md`「闸门线」）。F0005 照旧只读换包前那段；它的两行和 F0011 第 1、2 行是同一个预期，核对时只算一次，记在 F0011 名下，段完整以后从 `gate_lines.jsonl` 截那一段另算。
- 日报仓位成绩单（`research_probes/daily_band_scorecard.py`）同一批改成快照有 `gated_risk_asset_ai_band` 键就读 gated 区间（`docs/system_flow.md`）。

## 什么时候、谁做

- 每周五研究会话在电脑上跑（和周五摘要同一次，不新增定时任务）：`append --through <最近一次跑完的日报的 as_of 日期> --daily-root <日报状态根>`，`weekly --daily-root <日报状态根>`，`check`，`pytest tests/test_research_forward_ledger.py tests/test_research_forward_added.py tests/test_research_forward_frozen.py tests/test_research_forward_gate.py -k committed`；提交 `ledger.jsonl`、`weekly.md`、（2026-11-19 以后）`added_lines.jsonl`、（2026-11-23 以后）`frozen_lines.jsonl` 和（2026-11-24 以后）`gate_lines.jsonl`（A 区）并发布。退出 1 时照上面「冻结的线」「闸门线（提示）」两节读。追加批是 A 区，门不跑测试，所以那两条测试（核入库的账本、周汇总盖住每一行）在提交前跑；循环线程在云端核行和指纹时，在套好的 tree 上再跑同一条，过了才回「可以发布」（2026-10-23 起，第 6 期第 3.3 节）。第一次从 2026-10-12 回填。要不要改成每天自动，跑两周以后再定。
- 周汇总 `weekly.md`：自 2026-10-12（含回填）和自 2026-10-21（前瞻）两段，各线的累计收益、最大回撤、平均敞口，日报模型和 F0001 各自的对照（同平均敞口的固定敞口），F0001 对它的累计对数差（前瞻那段是「研究的实际收益」，即 F0008 第 2 行的读数；满 252 个交易日另印几何年化差），和按周的收益；给了 `--daily-root` 再印 QQQ 月末总收益离运行最高（`research_bench.segments` 的划段规则，只印现在的状态：开段或未开段）和现金按 SGOV 的一组（只印）；TRADING-2565-139 起两张主表各加满仓 1.0 一行，给了 `--daily-root` 再分日报线换包前后（上一节）。周五摘要引一行。描述，不判。
- 核对：前瞻记录 F0001 和 F0005（`../_loop/forward_records.md`）在 QQQ 按同一规则划出下一个完整慢熊段或长牛段时逐条核，记对错和记分表第 3 节的四列；中间每季度在周五摘要印一行描述，不判。同一配置连续两次慢熊段核对为对之前，不拿它开题或改日报。

production_effect=none；broker_action=none。
