# DATA-024：可变数据源的外部请求缓存加有效期

最后更新：2026-10-16

稳定任务 ID：`DATA-024`

状态：`IN_PROGRESS`（DATA-024-01 由 pi 写代码，Claude 审，Baker 批）

关联：OPS-064（请求缓存对失败响应的期限）、OPS-065（过期后的跨进程 singleflight）、OPS-086（日报发布包；2026-10-15 的电脑试运行发现了本问题）、
DATA-022（价格只用 FMP）；任务记录 `tasks/DATA-024.yaml`。

## 1. Owner decision

- `owner_decision:DATA-024:2026-10-16:cache_positive_ttl`：Baker 2026-10-16 14:17 JST 在 OPS-086 线程的卡片上选「先修再切换」
  （卡片问题：切换生产前，先修好联网数据缓存永不过期的问题吗？）。先发布本任务，再重建发布包、在电脑上再试运行一次，之后才切换生产。

## 2. 背景

- 外部请求缓存（`data/raw/external_request_cache/`）把 HTTP 200～399 的响应当作 `positive_persistent`：没有期限，请求键相同就永远先用存下的那份，不再联网。
  请求键里带日期的请求（价格的 `from`/`to`、FRED 的 `cosd`/`coed`）每天自然换新键；键里没有日期的请求，永远停在第一次存下的响应。
- 2026-10-15 的电脑试运行（评审 `knowledge/claude/runs/OPS-086-06/REVIEW.md`）用空缓存联网跑同一个 as_of，和运行副本比：
  SEC companyfacts 17 家的文件在运行副本里连续几天逐字节相同，其中 15 家比试运行取到的旧（例如 AMAT 的季度特征：运行副本 Q2，试运行 Q3），
  基本面分 75.1 对 73.0；FMP 估值快照的数值 17 家全部不同；运行副本各联网组件的单次耗时只有试运行的 1/3 到 1/6（命中缓存）。
- 代码核对：SEC companyfacts 的请求键只有 CIK 和请求头；`FmpHttpValuationProvider._get` 的各端点参数只有股票代码（和 period、limit），键里没有日期；
  官方政策源按 URL 取页面，同一个 URL 永远命中。生产状态的缓存从空开始，不修就会重新冻住。
- 不修的后果：基本面特征、估值快照和政策事件的输入停在缓存第一次存下的那天，日报每天照常通过，没有任何告警。

## 3. 范围

有效期 12 小时（43200 秒）：主窗口（12:00 JST）和同日 17:30 的补跑共用一次联网，次日重取。只覆盖请求键里没有日期、数据源会变的来源：

| provider | api_family | 为什么 |
|---|---|---|
| `SEC EDGAR` | `companyfacts` | 键只有 CIK；每次新申报后内容才变 |
| `Financial Modeling Prep` | `quote-short`、`key-metrics-ttm`、`ratios-ttm`、`analyst-estimates`、`key-metrics`、`ratios`、`price-target-summary`、`price-target-consensus`、`grades`、`grades-consensus`、`ratings-snapshot`、`earnings-calendar` | 估值端点的参数只有股票代码（`earnings-calendar` 有日期窗口，一并列入，窗口不变时同样会冻住） |
| `Official policy source` | `source_fetch` | 按 URL 取页面，同一页面会更新 |

其余保持不变：价格（`eod_daily_prices`、Marketstack）和 FRED（`fredgraph_csv`）键里有日期；`filing_archive`（按申报号，内容不变）；
`sec_pit_submissions`、`sec_pit_companyfacts`（历史回填，要可复现）；`quarterly_text`（TSMC IR 按季度）；4xx/5xx 的规则不变。
以后新增的、键里没有日期而来源会变的请求，必须在 `positive_ttl_rules` 里登记。

## 4. 机制

1. `config/data/external_request_cache_lifecycle_policy.yaml` 新增可选键 `positive_ttl_rules`（列表，每项 `lifecycle_class`（取 `positive_bounded_mutable_source`）、`provider`、`api_families`、`ttl_seconds`），
   `policy_version` 不变。没有这个键的策略文件照旧能加载，行为同以前。同一个 provider 加 api_family 不得在两条规则里出现，`ttl_seconds` 必须是正整数。
2. `evaluate_external_request_cache_lifecycle` 新增可选参数 `provider`、`api_family`。状态码在 `positive_status` 范围内、且 (provider, api_family) 命中一条规则时，
   返回规则的 `lifecycle_class`、`ttl_seconds` 和 `expires_at = observed_at + ttl`；否则同以前（持久）。4xx/5xx 的规则不受影响。
3. `write_external_request_cache_response` 写入时和 `_validated_cache_generation` 校验存下的条目时都传 provider 和 api_family（校验用元数据里的值）。
   到期的条目走已有的 `EXPIRED_REVALIDATE` 路径：下一次调用联网，写新 generation；旧 body 和观察记录不删除（证据保留）。
4. 规则生效之前存下的条目（元数据里 `ttl_seconds` 是空）对被点名的来源不再被信任：校验失败按没有缓存处理，联网一次后换成带期限的新条目。
   没被点名的来源的条目完全不受影响，所以不需要升 `policy_version`，也不会触发整个缓存重取。

## 5. 失败行为

到期后的联网失败照常抛出，不退回使用过期的旧响应（不做 stale-if-error）：用旧数据悄悄通过正是本任务要消除的问题。联网失败由已有的数据源重试、日报补跑和数据质量门禁处理。

## 6. 对日报的影响

- 这一批发布本身不改变运行副本；生效在发布包切换之后（运行副本在 DATA-024 之前冻住的旧数据不会被修复，运行副本退役）。
- 切换后第一次日报：SEC、FMP 估值、前瞻 PIT、官方政策源的联网都是真实抓取（试运行的耗时是上限：约 SEC 45 秒、前瞻 PIT 135 秒、估值 79 秒、官方政策 39 秒），
  之后每天联网一次（12 小时内的补跑命中缓存）。基本面分、估值快照和总分在第一天会有一次台阶，原因是数据更新，不是规则变化。
- `production_effect=none`、`broker_action=none`：不改评分规则、权重、门禁或任何交易动作，只让输入变新。

## 7. 验收

`tests/test_data024_request_cache_ttl.py`：策略对被点名的每个 provider/api_family 给 12 小时、对其余来源和缺少 provider 或 api_family 时保持持久；4xx/5xx 规则不变；
策略文件的非法规则（ttl 非正、重叠、空 api_families、缺 provider 等）被拒绝，缺这个键的旧文件照常加载；条目在 TTL 内命中、到期返回 `EXPIRED_REVALIDATE` 且旧 body 仍在；
到期后联网一次并写入新条目；规则生效前存下的条目不被使用；联网失败抛出而不是返回旧响应；SEC companyfacts 提供者、FMP 估值提供者的 12 个端点和官方政策客户端实际用到的名字都在规则里
（经 `external_request_cache_trace` 和客户端调用核对）；本文和 `docs/system_flow.md` 有对应记录。PR 套件通过。

## 8. 进展

- 2026-10-16：需求和验收测试写好，参考实现和测试在云端通过。
