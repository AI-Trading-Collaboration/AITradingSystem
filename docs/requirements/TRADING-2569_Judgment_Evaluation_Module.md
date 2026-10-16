# TRADING-2569：人工研究验证模块（研究漏斗 + 按市场结果对账）

- 状态：开发中（J1 导入、J2 结算已发布；J3a 报告开发中）。
- 需求提出：Baker，2026-10-14（学习会话里确认了方向和顺序）。
- 边界：`production_effect=none`，`broker_action=none`。只读评估，不进评分、仓位或任何生产路径。

## Owner decision

- `owner_decision:TRADING-2569:2026-10-14:judgment_eval_module`。
- 依据：Baker 2026-10-14 16:36 JST（07:36Z）提出需求；16:46 JST 问研究路线和人工介入点；16:48 JST（07:48Z）回复"好的，那就先按照这个思路尝试开发并且走一遍流程，我记得 tsm 有一些台账"。
- 范围：同意本文的方向和默认值（第 8 节），开始开发并用 TSM 台账走一遍流程。每一批 C 区改动仍按决定记录或结果 tree 批准，批准人是 Baker 本人。
- J2（结算）的 C 区文件是 `config/research/research_judgment_eval.yaml`（结算阈值），单独一个决定号 `owner_decision:TRADING-2569:2026-10-14:judgment_settlement`：范围只含这一个文件，等 pi 实现完成、Claude 审过后，由 Baker 在 TRADING-2569 线程打一行批准，电脑会话写决定记录。决定号写在这里只是让 pi 的 J2 批次能在 main 的 `docs/requirements` 里找到它，不代表已批准。
- 记分口径：Baker 2026-10-14 08:19Z 要求记分用补记里的最新概率；10:08Z 决定 `34fl2bjh…` 也按 70% 计分（见 §3.3）。
- J3a（报告）的 C 区文件是同一个 `config/research/research_judgment_eval.yaml` 的 `report` 部分（报告阈值），单独一个决定号 `owner_decision:TRADING-2569:2026-10-16:judgment_report`：范围只含这一个文件，等 pi 实现完成、Claude 审过后，由 Baker 在自己的 PowerShell 里带决定记录草稿的 SHA-256 运行 `approve.ps1` 批准。决定号写在这里只是让 pi 的 J3a 批次能在 main 的 `docs/requirements` 里找到它，不代表已批准。

## 1. 要解决什么

Owner 在训练手册里对 AI 产业链公司做财报预测台账，很多判断有 AI 协助。现在从提出假设到验证有效性，流程分散；有些假设研究完才发现对股价几乎没有影响。

两个目标：

1. **先快筛**：一个假设值不值得花时间研究（S3，研究漏斗）。
2. **再对账**：把人工（含 AI 协助）的判断按市场结果自动结算，按研究思路统计是否有效，把时间集中到有价值的方向（S1、S2）。

## 2. 先说一个会改变预期的数字

Baker 给的量级：20 个交易日相对 SMH 的波动，TSM 5.2%、NVDA 8.8%、MU 14.2%；要确认每次约 1% 的优势，t=2 需要 n = (2σ/δ)²，即 TSM 约 108 条、NVDA 约 310 条、MU 约 810 条独立判断。这和需求里的"约 100 / 300"一致。

问题在于**独立**：每家公司每季度只有一次财报，一只票一年最多 4 条独立的财报判断，TSM 攒到 100 条要 25 年。所以按标的分组评估永远是"样本不足"。设计上要做三件事：

- 评估单位是**研究思路**（跨标的合并），不是标的。结果先按标的自己的波动标准化（相对 SMH 的收益除以该标的同期期限的历史波动），合并才有意义。按标的分组的表只作描述，不给状态。
- 报告每个分组的**最小可检出效应**（MDE），让"样本不足"成为机械结论：MDE 大于 owner 关心的优势（默认 1% 标准化前、或标准化后 0.15σ）就是样本不足，不看直觉。
- 同一事件的多条判断（同一标的同一财报的多个思路、同一季的多只相关标的）要合并成有效样本，见 §4.3。

这也意味着：很多思路在 1–2 年内都不会到"可放弃"。这是诚实的结果，不是模块失灵。

## 3. S1 判断数据导入

### 3.1 来源和落点

- 来源：训练手册台账（v19 字段见下）。导出为 JSON，格式版本 `judgments_export.v1`。
- 落点：数据目录 `data/external/judgments/`（`.gitignore` 已忽略 `data/external/*`，不进 git），`exports/judgments_<导出时间>_<sha前8位>.json` 原样保存每次导出，旧文件不改。判断是个人研究内容，不提交，不发给外部模型。
- 登记：`config/data_sources.yaml` 加 `local_judgment_ledger`（`source_type: manual_input`，`status: active`，`cadence: manual`，`requires_credentials: false`）。`audit_fields` 含 provider、file_path、captured_at、row_count、checksum；`limitations` 写明手工输入、证据质量取决于记账纪律、AI 协助条目不能回测。`config/data_sources.yaml` 在 C 区，这一条要按 tree 批准。
- 导入记录：同目录 `imports.jsonl`，追加式、哈希链（每行带上一行的 sha256，首行为 64 个 0），每行含序号、导入时间（UTC）、导出文件名与 sha256、导出时间、行数、研究/学习行数、新增 id、状态有变化的 id。记录只含 id 和计数，不含判断内容。

### 3.2 字段

**2026-10-14 读 artifact 数据库核对**（`ledger` 集合，4 条，都是 TSM、都未结算、截止 2026-10-15）：

- 记录没有 `id` 字段，文档 id（如 `34fl2bjhknwk3zydftc4`）就是 id；导出时写进 `id`。
- `addenda` 里有的补记写了新的概率，起初只是文字；2026-10-15 起台账的补记可以带结构化的新概率 `prob`（(0,1) 的小数，只能在揭晓日之前追加，必须同时写理由）和可选的 `probNote`。导出方把它写进 `probRevisions`，模块交叉检查两者一致（§3.3 第 3 条）。
- 实际还有：`qs`（Q1–Q4）、`event`、`baseRate`、`consensus`、`implied`、`invalidation`、`rationale`、`weighing`、`fromDraft`、`metaNote`、`resolvedAt`。模块只认下面列的字段，其余原样保存、不检查。
- `addenda` 的每条带 `at`、`rationale`、`baseRate`、`invalidation`，有的带 `note`。
- 4 条都没有 `aiProb`、`sawSystem`；`purpose`、`aiMode`、`methods` 是 2026-10-14 事后补标的（`metaNote` 写明）。这些条目导入后保持原样，评估 AI 参与时要注明是事后补标。
- 4 条里没有价格或波动类，没有 `market`。S2 的价格结算现在没有可结算的条目。

台账 v19 已有：`id`、`ticker`、`statement`、`prob`、`deadline`、`kind`（基本面/价格/波动/其他）、`createdAt`、`addenda`、`status`/`review`/`errorType`、`purpose`（research|learning）、`aiMode`（human|assisted|ai）、`aiProb`（可空）、`sawSystem`、`methods`（标签数组）、`market`（价格/波动类必填：`benchmark` 默认 SMH、`horizon` 交易日数、`anchor` 为 event_prev_close|entry_close、`direction`）。

模块再要两个可选字段，建议在台账 v20 里加，导出里没有就当缺失：

- `eventId`：同一次财报的所有判断共用，没有就由 `ticker + deadline` 推出；用于有效样本合并。
- `eventTiming`（bmo|amc|unknown）：不填由 §6 的数据源推出。

### 3.3 导入检查（任一失败整批不入）

1. schema：必填字段、类型、`prob` 在 (0,1)、枚举值合法。
2. `id` 唯一；跨导入不重复。
3. **不可变**：对上一次导入里已有的 `id`，`ticker`、`statement`、`prob`、`createdAt`、`deadline`、`kind`、`market`、`methods`、`aiMode`、`purpose`、`aiProb`、`sawSystem` 不得改动；`aiProb`、`sawSystem` 上次为空的，这次也不能补（它们必须在写判断时记，事后补等于事后看过结果）；`addenda` 只能追加；`status`、`review`、`errorType`、`resolvedAt` 和其他字段允许变化。上次有、这次没有的判断也拒绝。改动 = 拒绝并列出差异。`prob` 是写判断时的原始概率，固定不变。后来改了概率，要记在可选字段 `probRevisions`（只能追加的列表，每项 `at`、`prob`，可带 `note`）：记分用最新一项的 `prob`，没有就用 `prob`；原始 `prob` 保留，报告里两个口径的 Brier 都给，用来看修订有没有让判断变好。每项 `at` 必须晚于 `createdAt`、按 UTC 日期早于截止日，并且严格递增。补记正文仍是文字，模块不解析；补记带结构化的 `prob` 时，`probRevisions` 里必须有 `at`（同一时刻）和 `prob` 都相同的一项，否则拒绝 `revisions_mismatch`，补记里的 `prob` 不是 (0,1) 的小数或 `at` 读不出时间按 `schema` 拒绝；`probRevisions` 里多出的项不拒绝（它们来自 owner 的口头说明）。已导入的补记必须原样，所以 2026-10-15 台账给旧补记补录 `prob`、`probNote` 之后，导出方对已经导入的补记去掉这两个键，值已经在 `probRevisions` 里，`at` 和 `prob` 与基线一致；新追加的补记保留这两个键（现有 `893mhl1q…` 一条：补记写了 55%、35%，owner 2026-10-14 说明记分取最新概率 35%；`34fl2bjh…` 补记里的 70% 原写"只做记录，不计分"；owner 2026-10-14 10:08Z 改为按 70% 计分，所以导出里它有一项 `probRevisions`，`at` 为 2026-10-01T06:26:24Z，`prob` 为 0.7）。
4. 时间先后：`createdAt` 早于事件。价格/波动类，事件时间 = 结算锚点日；基本面类，事件时间 = `deadline`。晚于或等于事件的条目拒绝，不降级。
5. `ticker` 在 `config/universe.yaml` / `watchlist.yaml` 登记过。
6. `methods` 标签都在方法登记表内（§5.1）。
7. `kind` 为价格/波动时 `market` 必填且完整。
8. `purpose=learning` 的条目导入但打标记，不进入任何评估分母。

### 3.4 导入检查的问题码

`format`（导出文件格式）、`schema`（字段和取值）、`duplicate_id`、`market_missing`（价格/波动类缺市场块）、`time_order`（写入日期按 UTC 必须早于截止日，同一天也拒绝）、`unknown_ticker`（必须在自选股、基准或产业链清单里，大小写精确）、`unknown_method`、`immutable`、`addenda_rewritten`、`revisions_rewritten`（`probRevisions` 只能追加）、`revisions_mismatch`（补记带的 `prob` 在 `probRevisions` 里找不到同一时刻、同一数值的项）、`disappeared`、`stale_export`（导出时间不晚于上次导入）、`baseline_tampered`（上次导入的原样副本被改过或丢了）、`chain_broken`（导入记录的链断了）。同一次检查报全部问题，任何一个问题都不写任何文件。

## 4. S2 按市场对账与评估报告

### 4.1 结算

命令 `python -m ai_trading_system.research_judgments settle --root <判断数据根> [--asof YYYY-MM-DD] [--prices PATH] [--dry-run]`。判断和结算都在 `--root` 下的 `data/external/judgments/`（真实数据放在仓库外，见 J1 的导入）；价格默认读系统的价格缓存 `state_dir(PROJECT_ROOT, "data", "raw", "prices_daily.csv")`，列 `date`、`ticker`、`adj_close`，不用 Yahoo。

**范围**：只结算 `purpose=research` 的条目，`learning` 不结算。每条得到一个状态：

| 状态 | 含义 |
|---|---|
| settled | 有结果：`outcome` 1 = 陈述成真（hit），0 = 没成真（miss） |
| pending | 还没到：台账里还是 open，或价格窗口还没走完（不估算） |
| void | 作废：台账 void、或价格上相对收益恰好为 0（平手） |
| data_gap | 数据不够好，留给人：价格缺失、日历空档、疑似拆股价格、状态未知等，原因写在 `reason`，永远不估算 |

**人工结算**（`kind` 为基本面、其他、波动）：沿用台账的 `status`：hit→settled/1，miss→settled/0，void→void，open→pending（截止日之后仍 open 的标 `overdue`），其他值→data_gap `unknown_status`。波动类在 J2 只走人工结算，已实现波动的自动结算以后再说；隐含波动类一直人工。

**价格结算**（`kind=价格`）：

- 交易日历 = ticker 和基准两条序列都有 `adj_close` 的日期。基准默认 SMH；ticker 等于基准是 data_gap `ticker_is_benchmark`。
- 锚点：`event_prev_close`：事件日 = 日历里第一个不早于 `deadline` 的日期（截止日不是交易日就取下一个交易日），锚点 = 事件日的前一个交易日（事件日是日历第一天则 data_gap `no_prior_close`）；`entry_close`：锚点 = 第一个收盘（美东 16:00，时区和时间在配置里）严格晚于 `createdAt` 的交易日（`createdAt` 恰好在收盘时刻则取下一个）。
- 终点 = 锚点后第 `horizon` 个交易日；价格还没走到终点是 pending `window_not_complete`；事件日或入场收盘还没出现是 pending `no_event_day_yet` / `no_entry_close_yet`。
- 结果 = 窗口内 ticker 收益减基准收益（用 `adj_close` 比值），四舍五入到 10 位小数后再判符号。`outperform`：相对收益 > 0 为 hit；`underperform`：< 0 为 hit。
- 窗口检查（阈值都在受审配置，代码里不许写死）：相邻两个价格日相隔超过 `max_calendar_gap_days` 个自然日是 data_gap `calendar_gap`；任一价格不大于 0 是 `bad_price`；任一条序列单日涨跌幅绝对值超过 `max_abs_daily_move` 是 `suspect_jump`（多半是没复权的拆股，不是真收益）。
- 台账状态和价格结果的关系：台账 void 优先（void，原因 `ledger_void`）；台账是 hit/miss 但和价格结果不一致，**记价格结果**，同时标 `conflict=true`（报告里单独列，不自动改台账）；台账 open 时只看价格。
- 没有 `check` 字段：台账 v20 的结构化核对（TSM IR 数值核对）放在 J2 之后，真实结算（2026-10-15）以后再决定。

**输出**：`settle` 只往同一数据目录写，不进 git：`settlements/settlement_<asof>_<sha前8位>.json`（格式 `judgment_settlements.v1`，只有 id、数字、状态和哈希，没有判断原文）和哈希链日志 `settlements.jsonl`（序号、结算时间、asof、文件名与 sha256、所用导出和价格的 sha256、各状态计数、新结算的 id、台账结果变化的 id、上一行 sha256）。价格哈希只覆盖用到的序列，其他股票价格变化不影响。同一输入再跑不写东西，输出 `unchanged`。

**冻结**：已经 settled 的价格结果不许变。再算时如果状态、outcome、锚点日期、终点日期有变，或相对收益相差超过 1e-9，整次结算被拒（`settlement_changed`），什么都不写，逼人去看是哪条价格被改了。台账来源的结果变化（例如 hit 改 miss）不拒，记进 `changed_ids`。拒绝原因还有 `no_import`、`baseline_tampered`（导入的原样副本被改过）、`chain_broken`、`asof_backwards`、`prices_missing`、`prices_schema`。

**DQ**：J2 不调用 `aits validate-data`；价格异常靠上面的窗口检查兜底（只会变成 data_gap，不会估算）。要不要把"最新 DQ receipt 通过"变成结算的前置条件，等有了第一次真实结算再定。

### 4.2 报告（J3a：只用结果和概率）

台账里的判断多数是基本面类，只有"应验 / 未应验"和概率，没有收益，所以报告分两批：J3a 只用结果和概率，对所有已结算的 research 判断（价格类也一样，用它的结算结果）都适用；J3b 才用相对收益和纸面组合（§4.6），等有价格类判断再做。

命令：`python -m ai_trading_system.research_judgments report --root R [--asof YYYY-MM-DD] [--dry-run]`。读 `--root` 下判断数据目录里最新一次导入的原样副本和最新一次结算文件，写 `reports/judgment_report_<asof>_<sha前8位>.json` 和同名 `.md`（给人读）。内容是 id、标的、思路 id 和数字，不含陈述和理由。报告是输入和配置的纯函数，随时可重算，所以不写哈希链日志；同样内容再跑是 `unchanged`；`--dry-run` 什么都不写。拒绝：`no_import`、`no_settlement`、`settlement_stale`（最新结算对应的导出不是最新一次导入）、`baseline_tampered`、`chain_broken`、`asof_backwards`（`asof` 早于最新结算的 `asof`），拒绝时什么都不写。

**哪些判断进统计**：结算状态为 `settled` 的 research 判断；pending、void、data_gap 只计数。每条判断取记分概率 `p`（最后一次事前修订，没有就用 `prob`）、原始概率 `q`（`prob`）和结果 `y`（1 应验，0 未应验）；Brier 为 `(p−y)²` 和 `(q−y)²`。`purpose=learning` 的条目单独一节（结果取台账状态 hit / miss），不进任何思路组，不给状态；另给一行"全部已结算"（研究加练习）的合计。

**分组**：研究思路（`methods`；一条判断属于它的每个思路，思路组之间有重叠，报告写明）给状态；标的、`aiMode`、`sawSystem`（`yes` / `no` / `unknown`）只描述，不给状态。

**每组的数字**（以事件为单位，见 §4.3）：

- `n_judgments`、`n_events`、`n_clusters`；
- 命中率：事件内 `y` 的平均，再对事件平均；Wilson 区间（双侧，置信水平来自配置），把事件数当样本数、各事件 `y` 之和当成功数（事件内部有分歧时是小数，这是近似）；
- Brier：记分口径 `brier_scored`、原始口径 `brier_original`、全报 50% 的 0.25、基准率口径 `brier_baseline`；基准率用**留一事件**：对事件 e，恒报其他事件命中率的平均，所以基准率不会偷看自己；只有一个事件时不可算；
- **提升** `d_e = brier_baseline_e − brier_scored_e`（相对基准率；另给相对 50% 的平均提升 `lift_coin_mean`）。提升的平均值 `lift_mean`、聚类稳健标准误 `lift_se`：把事件按财报季簇分组，`u_c = Σ_{e∈c}(d_e − d̄)`，`V = G/(G−1) · Σ_c u_c² / N²`（G 为簇数，N 为事件数；G 小于 2 时不可算）。单侧置信下界 `lift_lower`、上界 `lift_upper` 用正态分位数（水平来自配置），`lift_p` 为单侧 p 值。不用自助法：结果确定，不需要随机种子；簇很少时两者都不准，所以状态还要靠样本门槛；
- 前后两半的平均提升 `lift_first_half`、`lift_second_half`：事件按事件日期、再按事件键排序，前 `⌊N/2⌋` 个为前半；
- **最小可检出提升** `mde = (z_{置信} + z_{功效}) · lift_se`，事件数不少于 `min_events_for_mde` 且簇数不少于 `min_clusters` 才算，否则为空；
- **修订增益** `update_gain` = 事件平均的（原始 Brier − 记分 Brier）：正数表示事前更新让判断变好；`n_updated`（有更新的事件数）、`n_flipped`（原始概率和记分概率在 0.5 两侧的判断数）。只描述，不给状态；
- **校准表**：概率低于 0.5 的判断先折叠成相反的陈述（`p' = 1−p`，`y' = 1−y`），`p = 0.5` 不折叠；按配置的分箱（左闭右开，最后一箱含上界）统计，以判断为单位（不合并事件）：判断数、平均 `p'`、命中率、Wilson。整体一张，每个思路组一张。

所有小数四舍五入到 10 位再写出。报告顶部固定写样本规模（事件数、簇数、"同一事件的多条判断只算 1 个样本"）和多重比较个数，让"样本不足"带数字。

### 4.3 有效样本

三层，报告都列：

1. 条数（`n_judgments`）；
2. 事件数（`n_events`）：按 `eventId` 合并，没有 `eventId` 时同一 `ticker` 和 `deadline` 视为同一事件；有 `eventId` 的和没有的互不合并。同一事件的多条判断先在事件内取 `y`、`p`、`q` 和两个 Brier 的平均，一个事件算一个样本；事件日期取其判断里最早的 `deadline`；
3. 财报季簇（`n_clusters`）：事件日期所在的日历季度（如 2026Q4）。聚类稳健标准误按它分组，防止 AI 产业链公司同季度一起涨跌造成假精度。

同一事件、同一指标上门槛不同的判断（现有的毛利率 66% 和 66.5% 两条）高度相关，所以合并。

### 4.4 状态判定（门槛在受审配置里，下列为建议默认）

只对研究思路组判定，每次重算，不手改。门槛都在 `config/research/research_judgment_eval.yaml` 的 `report` 部分，代码里不许写死：`confidence 0.90`、`power 0.80`、`min_events 30`、`abandon_events 60`、`edge_brier 0.02`、`min_events_for_mde 5`、`min_clusters 3`、`bh_q 0.10`、`calibration_bins [0.5,0.6,0.7,0.8,0.9,1.0]`。

按下面的顺序判，第一条成立就停：

| 顺序 | 条件 | 状态 / 原因 |
|---|---|---|
| 1 | 事件数 < `min_events` | 样本不足 / `events_below_minimum` |
| 2 | `mde` 为空 | 样本不足 / `mde_unavailable` |
| 3 | `mde` > `edge_brier` | 样本不足 / `mde_above_edge` |
| 4 | 事件数 ≥ `abandon_events` 且 `lift_upper` < `edge_brier` | 可放弃 / `edge_excluded` |
| 5 | `lift_mean` ≥ `edge_brier`，`lift_lower` > 0，前后两半的平均提升都 > 0，并通过 BH | 有希望 / `edge_reached` |
| 6 | 其余 | 样本不足 / `keep_collecting` |

说明：

- `edge_brier` 是"值得追的优势"：平均每个事件比留一事件基准率好 0.02 的 Brier。这是 owner 审定的数，不是推出来的。"有希望"要求提升平均值至少达到它，防止一个没有技巧、只靠基准率估计噪声的思路（恒报 50%）被当成有希望。
- "可放弃"定义成"能排除关注的优势"，而不是"没显著"，这样不会把检验力不足误判为无效。
- "有希望"只表示值得优先研究，不是结论；正式结论走 research bench 的预注册流程。
- BH（Benjamini–Hochberg）：m = 本期有 settled research 判断的思路组数；事件数达到 `min_events` 且 `lift_p` 可算的组用它的 `lift_p`，其余按 1 参与排序；排序后最大的满足 `p_(k) ≤ k/m · q` 的 k，前 k 个通过（逐步上升）。
- 改门槛 = 改受审配置，C 区，Baker 批。

### 4.5 AI 协助的限制

- 模型训练截止日之前的事件不得回测：`aiMode` 为 assisted/ai 的条目只能走事后逐条对账，且 `createdAt` 必须早于事件。S2 本来就只处理 `createdAt` 在事件前的条目，这里再加一条检查：任何要求用历史事件重放 AI 判断的功能一律不做。
- `aiMode`、`aiProb`、`sawSystem` 作为分组维度，用来回答"AI 参与有没有改善判断"和"看过系统日报有没有影响判断"，样本不够时只列不下结论。

### 4.6 J3b（收益口径，暂不实现）

等有价格类、波动类判断之后再做，范围保留如下：

- 每组的平均相对收益（标准化前后各一份）及置信区间；标准化 = 相对 SMH 的收益除以该标的同期期限的历史波动，合并才有意义；
- 纸面组合：`prob` 高于阈值做多、低于做空（默认 0.5±0.1，写进受审配置），等权、持有一个 `horizon`，扣 `config/research/transaction_cost_model.yaml` 的单边成本，对比同期 SMH；这是评估口径，不是交易建议；
- 收益口径的状态判定：标准化平均相对收益的单侧置信下界、MDE 与关注的优势（默认标准化前 1%、标准化后 0.15σ）比较；
- 需求和门槛在 J3b 开工时再审，J3a 的 `report` 配置不预留这些键。

## 5. S3 研究漏斗（思路快筛）

### 5.1 研究思路登记表

`config/research/research_judgment_methods.yaml`。**注意文件名**：Baker 草案里的 `config/research/judgment_methods.yaml` 不在 `config/research/research_*.yaml` 通配下，会落到 B 区，pi 就能改门槛；改成 `research_judgment_methods.yaml` 才自动进 C 区。同理评估阈值放 `config/research/research_judgment_eval.yaml`。

每个思路：id、说明、能否机械化（可机械化 / 需要人工输入）、对应的研究问题、快筛模板（事件研究 / 先行指标 / 横截面 / 无）、状态。初始 id 即台账 v1 标签：guidance_base_rate、hf_leading、op_leverage、capex_cycle、chain_readthrough、expectation_crowding、valuation_implied、vol_pricing、accounting_detail、other。`other` 不进入评估，只用来发现新标签。

### 5.2 三种快筛模板

- **事件研究**：事件前后 [a, b] 个交易日相对 SMH 的异常收益，按事件对齐平均，聚类自助法给区间。
- **先行指标**：先行指标变化 vs 后续财报指标/反应，序列相关或排序相关。
- **横截面**：同期多个标的，指标高低分位的后续收益差。

每个模板的输出都含：样本数、有效样本、效应及区间、MDE、跨标的方向一致比例、判定（排除 / 候选 / 不确定，和 TRADING-2565 的 S4 同一套措辞）。

### 5.3 规矩

- 每次快筛先把**规格**（窗口、期限、基准、事件定义）固定在配置里再跑，跑完不改规格重跑。
- **每次快筛都记**，不论结果：`research/_judgment_screens/ledger.jsonl`，哈希链，含规格哈希、数据快照、结果、判定。全局计数，供多重检验使用。
- 通过 = "优先研究"；正式结论走 research bench 的预注册。
- 确认无影响的，提议写入 `research/negative_results.md`。负结果清单只由 owner 审后更新，模块只产出提议文本，不直接改。
- 开始前按研究流程第 1 步查 `research/negative_results.md` 和 `research/ledger.md`：落在已证伪范围内要先说明区别。

### 5.4 第一个候选：财报前抢跑 / 拥挤度

- 假设：财报前 21 个交易日相对 SMH 的涨幅越大，财报反应（相对 SMH）越差（拥挤度已被定价）。方向事先写定为负，同时给双边结果，防止事后挑方向。
- 财报反应 = 事件首个可交易日收盘对其前一收盘的相对 SMH 收益。盘后发布取下一交易日；盘前发布取当日。用 `acceptanceDateTime` 判定（美东 16:00 之后算盘后）。
- 预期看的量：排序相关系数、前后三分位反应差、跨标的方向一致比例，聚类自助法按财报季。
- 样本量估计：本地 SEC 提交记录里 NVDA 的 8-K Item 2.02 从 2020-03 起；价格缓存最早 2018-01，所以事件约 2020 起，每只票约 25 个，十几只票约 300 个事件，合并为约 25 个财报季，聚类后的有效样本远小于 300。MDE 要如实写在报告第一屏。
- 竞争解释要预先写：财报前涨幅可能只是 beta、动量或同板块行情；要同时报告对 SMH 的相对收益（已部分去 beta）和与同期动量的偏相关。

## 6. 历史财报日期

当前情况：FMP 财报日历只有采集日之后的时点数据，不能回填。

本地已有且可用：`data/raw/sec_edgar/submissions/*.json`（SEC 提交记录），NVDA 的最近提交列表里 8-K 的 Item 2.02（业绩公告）带 `filingDate` 和 `acceptanceDateTime`，起点约 2020-03。其他 AI 产业链美股同理。这是一手来源，时点干净，时间戳还能判盘前盘后。

缺口：TSM、ASML 等外国发行人不提交 8-K Item 2.02。TSM 已有 `tsm_investor_relations_quarterly_results`（`filed_date` 为披露日期）；ASML 等需要手工整理一张带来源链接的表。更早的历史（SEC `recent` 列表之外）要读 `filings.files` 的分页文件，需要新增抓取，细节由 pi 按 brief 做。

## 7. 开发拆分

按"Claude 写需求和验收，pi 写代码"。建议分批，每批一个任务记录子项：

| 批 | 内容 | 区 |
|---|---|---|
| J1 | 导入器 + 检查 + 方法登记表 + `data_sources.yaml` 登记 + 导入记录（代码在 `src/ai_trading_system/research_judgments/`） | 配置 C（`data_sources.yaml`、`research_judgment_methods.yaml`，都可走决定记录）；代码 B |
| J2 | 结算器（人工结果 + 价格结果）+ 受审阈值配置的结算部分 | 配置 C；代码 B |
| J3a | 报告（结果和概率口径）：分组、事件合并、Wilson、Brier（记分/原始/基准率）、聚类稳健标准误、MDE、校准表、修订增益、§4.4 状态判定 + 配置的 `report` 部分；同批把补记概率的交叉检查（§3.3 第 3 条）加进导入检查 | 配置 C；代码 B |
| J3b | 收益口径（§4.6）：相对收益、标准化、纸面组合、收益口径的状态 | 配置 C；代码 B；等有价格类判断 |
| J4 | 方法登记表 + 财报日期表 + 快筛模板 + 快筛账 + 抢跑快筛 | 配置 C；代码 B；账 A |

验收见任务记录。每批都要有带反向对照的测试（改动 `prob` 被拒、`createdAt` 晚于事件被拒、已知序列手算的 Wilson/Brier、单事件多条判断合并）。

研究记录（A 区）走研究记录快车道；配置（C 区）可走决定记录（`data_sources.yaml` 不在决定记录排除清单里）。代码放在 B 区，判定门槛放在 C 区配置里，这样改门槛要 owner 批，改代码要过验收测试；J2 的结算阈值和 J3 的状态判定如果实现里出现写死的门槛，验收测试要抓住。

## 8. 待定问题与建议默认值

1. **台账导出方式**。默认：先手动导出，由 owner 在训练手册会话里让 Claude 读 artifact 数据库后写 `judgments_export.v1` 文件；等导出连续 3 次没出过格式问题、且每周有新增判断时，再考虑自动化（那时要给 artifact 数据库一个只读出口，另议）。
2. **历史财报日期**。默认：美股用 SEC 8-K Item 2.02（本地已有，一手、有盘前盘后时间戳）；TSM 用现有 TSM IR 源；ASML 等外国发行人手工表带来源。FMP 财报日历只用于采集日之后的前瞻日期。
3. **门槛数值**。默认见 §4.4：有效样本 30/60、置信 90%、关注的优势 1%（标准化后 0.15σ）、BH q=0.10、纸面组合阈值 0.5±0.1。写进 `research_judgment_eval.yaml`，owner 审定。
4. **AI 独立判断对照组**。默认：S1–S2 不做。台账里已有 `aiProb`（AI 独立概率，可空），先用它。要扩充样本再做时必须：只针对未来事件、提示词和模型 id 冻结并记录、先按 `data_sources.yaml` 的 `llm_permission` 评估外部 LLM 的使用范围、给出调用成本上限；带 `aiProb` 的已结算判断累计到 30 条之后再回头决定。

另外几个我查仓库时发现、需要 owner 一并定的点：

5. 判断文件是否进 git。默认：不进 git，放在 `data/external/judgments/`（已被 `.gitignore` 忽略；OPS-086 搬数据根后跟着 `state_dir` 走）。判断含个人研究内容，而仓库有远端。
6. `config/research/judgment_methods.yaml` 改名为 `research_judgment_methods.yaml`（见 §5.1）。
7. 波动类判断没有期权数据时只做已实现波动结算，隐含波动类保留人工结算。
8. 价格缓存：我看到的 `data/raw/prices_daily.csv` 日期范围是 2018-01-02 至 2026-07-24，是否就是现行缓存、在 OPS-086 搬迁后路径是否变化，开发时由 pi 先核实，不假设。

## 9. 用 TSM 台账走一遍（2026-10-14 起）

当前台账里 4 条 TSM 判断，都在 2026-10-15 财报日结算，是第一批真实样本。流程：

1. J1 发布后，导出这 4 条（已有 `judgments_export.v1` 文件）并导入，得到 `imports.jsonl` 第 1 行。
2. 10-15 财报公布后，owner 在台账里结算 4 条；毛利率两条和折旧摊销一条用 TSM IR 管理报告核对（TSM IR 源的 `filed_date` 是披露日）。
3. 重新导出并导入：只有 `status`、`review`、`errorType`、`resolvedAt` 和 `addenda` 变化才通过；这一步同时验证不可变检查。
4. J2 发布后，`settle` 在重新导入后的数据上算出结果（3 条 research 都是基本面类，走人工结算；没有价格结算）；J3a 发布后同样的数据产出第一份报告：4 条里 3 条 research、1 条 learning；3 条 research 属于同一次财报，有效样本是 1 个事件；状态必然是"样本不足"，报告要写出最小可检出效应，让这个结论有数字。
5. 样本不足是预期结果。这一遍的目的是验证流程，不是评价思路。

## 进展

- 2026-10-14：J1（TRADING-2569-02）判断导入器、检查、方法登记表和数据源登记；production_effect=none；broker_action=none。
- 2026-10-15：J2（TRADING-2569-04）判断结算器、受审结算阈值配置；production_effect=none；broker_action=none。
