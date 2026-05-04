# SEC accession-level filing archive 设计评估

状态：BASELINE_DONE

最后更新：2026-05-05

关联任务：`DATA-004`、`DATA-003`

## 背景

当前 SEC 基本面链路已经下载 `companyfacts` 聚合 JSON，并在指标抽取后保留
`accession_number`、`filed_date`、`form` 和聚合 JSON `source_path`。这能防止
历史日报或回测读取 `filed_date > signal_date` 的事实，但还不能把每条指标追溯到
原始 filing 目录、primary document、XBRL exhibit、accepted time 和 raw payload
checksum。

SEC 官方文档说明：

- `data.sec.gov/submissions/CIK##########.json` 提供 filer filing history；
- `companyfacts` 和 XBRL API 是跨 submission 的抽取/聚合数据；
- API JSON 会随披露实时更新，bulk ZIP 会夜间重建；
- accession-number directory 允许目录浏览，并提供 `index.html`、`index.xml`、
  `index.json` 便于自动化抓取；
- SEC fair access 当前要求自动化请求控制在不超过每秒 10 次。

参考来源：

- <https://www.sec.gov/edgar/sec-api-documentation>
- <https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm>
- <https://www.sec.gov/about/developer-resources>

## 当前能力

- `aits fundamentals download-sec-companyfacts` 下载每个 active CIK 的
  `companyfacts` 原始 JSON，写入 `sec_companyfacts_manifest.csv`，记录 endpoint、
  request parameters、row/fact count 和 checksum。
- `aits fundamentals validate-sec-companyfacts` 校验 JSON、CIK、taxonomy 和
  manifest checksum。
- `aits fundamentals extract-sec-metrics` 从已校验 `companyfacts` 抽取收入、毛利、
  营业利润、净利润、研发、CapEx 等指标，保留 `accession_number` 和 `filed_date`。
- 派生指标要求组件事实的周期、单位、截止日、财年、财期和 accession number 一致。
- `aits fundamentals validate-sec-metrics` 会把 `filed_date > as_of` 视为错误。

## 缺口

- 没有下载或校验 `submissions` filing history，因此无法记录 accession 的
  `acceptanceDateTime`、primary document、report date、document list 和历史分页文件。
- `source_path` 只指向聚合后的 `companyfacts` JSON，不指向 accession 原始目录或
  filing document。
- 当前无法为每条 SEC 指标提供原始 filing index/document checksum，也无法证明
  companyfacts 聚合事实与原始 filing archive 的对应关系。
- 对日频趋势判断，`filed_date <= signal_date` 是可接受 baseline；对 intraday、
  严格 PIT 审计、重大 filing 事件复核或供应商/SEC 聚合差异排查，缺少 accepted time
  和 raw filing archive 会限制结论强度。

## 设计结论

结论：应新增独立任务 `DATA-004`，建设 SEC accession-level filing archive baseline。
这不是 DATA-003 阶段 1-4 的阻塞项；DATA-003 的 forward-only PIT 供应商快照层可以
标记完成。SEC accession archive 影响 SEC 基本面下载、指标抽取、验证、报告追溯和
回测审计，应单独实施并单独测试。

第一版不应下载全部 SEC 历史 filing，也不应抓取所有 exhibit。范围应限制为当前
`config/sec_companies.yaml` active 公司，以及本地 SEC 指标 CSV 中已使用的
accession number。这样能满足审计追溯，同时遵守 fair access 和本地存储约束。

## DATA-004 建议范围

1. 新增 `aits fundamentals download-sec-submissions`：
   - 读取 active CIK；
   - 下载 `data.sec.gov/submissions/CIK##########.json`；
   - 写入 `data/raw/sec_submissions/*.json` 和 manifest；
   - 记录 endpoint、request parameters、downloaded_at、row count、checksum。

2. 新增 accession selector：
   - 从 `sec_fundamentals_YYYY-MM-DD.csv` 读取已使用 accession；
   - 与 submissions recent/files 元数据匹配；
   - 输出 accession manifest，包含 ticker、CIK、accession number、form、filing date、
     report date、accepted time、primary document、archive URL 和来源 submissions path。

3. 新增 `aits fundamentals download-sec-filing-archive`：
   - 下载每个 accession directory 的 `index.json`；
   - 第一版至少下载 primary document、complete submission text 或明确选择只下载 index；
   - 每个 payload 写 checksum、bytes、content type、downloaded_at 和本地路径；
   - 请求节流默认低于 SEC fair access 限制。

4. 扩展校验：
   - accession manifest 必须覆盖 SEC 指标 CSV 中的所有非空 `accession_number`；
   - `accepted_time <= as_of` 或 `filing_date <= as_of` 规则必须显式；
   - raw filing index/document checksum 必须可复核；
   - accession 与 CIK、form、filed date 不一致时 fail。

5. 扩展报告和追溯：
   - SEC 指标报告显示 accession archive 覆盖率；
   - 回测审计报告能区分 `companyfacts_only` 与 `accession_archived`；
   - `docs/system_flow.md` 在实现时新增 SEC submissions/archive 节点。

## Baseline 实施结果

- 已新增 `sec_filings` 模块。
- 已新增 `aits fundamentals download-sec-submissions`，下载 active CIK 的 submissions
  JSON，写入 `data/raw/sec_submissions/` 和 `sec_submissions_manifest.csv`。
- 已新增 `aits fundamentals download-sec-filing-archive`，按当日
  `sec_fundamentals_YYYY-MM-DD.csv` 已使用的 accession 下载 directory `index.json`，
  写入 `data/raw/sec_filings/<ticker>/<accession>/index.json` 和
  `sec_filing_archive_manifest.csv`。
- 已新增 `aits fundamentals sec-accession-coverage`，检查 SEC 指标 CSV 中已使用
  accession 的 submissions metadata、accepted time 和 archive index checksum 覆盖，
  输出 `outputs/reports/sec_accession_coverage_YYYY-MM-DD.md`。
- 已更新数据源目录、README、系统流图和测试。

Baseline 仍不默认下载全部 exhibit、primary document 或 complete submission text；这些属于
owner 决策点和后续增强。

## Owner 决策点

- 第一版是否只保存 `index.json`，还是同时保存 primary HTML/XML 和 complete submission
  text file。
- 本地保留周期和备份策略。
- 是否允许为历史回测区间批量补齐 accession archive，还是只对 forward/live 样本和近期
  回测窗口补齐。
- 是否需要将 SEC archive 与通用 PIT manifest 合并，还是保持 SEC 专用 manifest 并在
  数据源健康报告中汇总。

## 验收标准

- `DATA-004` baseline 能从当前 active CIK 生成 submissions manifest。
- 对 SEC 指标 CSV 已使用的 accession，能生成 accession coverage report。
- 至少 `index.json` 的 raw payload 路径、sha256、bytes 和下载时间可复核。
- 报告明确 accepted time / filed date 的可见性规则和剩余限制。
- 实现时同步更新 `docs/system_flow.md`、README、测试和任务状态。

## 状态记录

- 2026-05-05：从 READY 改为 IN_PROGRESS，原因：开始实现 baseline submissions 下载、accession coverage 和 accession directory index 归档。
- 2026-05-05：从 IN_PROGRESS 改为 BASELINE_DONE，原因：已实现 `download-sec-submissions`、`download-sec-filing-archive`、`sec-accession-coverage`、数据源目录、README、系统流图和测试；完整 DONE 仍取决于 owner 是否要求归档 primary document、complete submission text 或全部 exhibit。
