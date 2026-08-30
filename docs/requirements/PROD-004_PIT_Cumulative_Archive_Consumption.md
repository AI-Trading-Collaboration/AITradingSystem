# PROD-004 PIT 累计归档消费与估值历史接通

状态：VALIDATING

最后更新：2026-08-31

关联任务：`PROD-004`、`DATA-003`、`BTINPUT-001`、`VALUATION-002`

## 背景与问题

项目已经持续保存 forward-only PIT 原始快照，但两个默认消费路径仍停留在单日视图：

- `aits backtest-pit-coverage` 默认读取 `data/raw/pit_snapshots/manifest.csv`，该文件由
  每日链路按当日 raw 目录重建，不能代表跨日期累计覆盖；
- 交易日 `daily_input_capture` 调用 `aits valuation fetch-fmp` 时，把
  `--pit-normalized-path` 和 valuation history 都指向当日隔离目录，导致已经留存的历史
  PIT estimates 和 valuation snapshots 没有进入 90 日 revision 与本地估值分位计算。

2026-08-31 对独立 operations runtime 做只读盘点，精确扫描 legacy raw 目录与
`data/raw/daily_input_capture/YYYY-MM-DD/` 后观察到 4,808 个可识别快照、2,069,507 条
原始记录、87 个 UTC 可见日期、17 个 ticker，日期范围为 2026-05-03 至 2026-08-29；
其中没有重复 `snapshot_id`。物理积累已经超过当前 policy 的 60 日期 B 级门槛，主要缺口
是默认消费者没有读取累计归档，而不是缺少 raw bytes。

## 目标

1. 建立可重复生成、可校验、可检查的累计 PIT manifest，覆盖 legacy raw 与每日不可变
   capture 目录。
2. 让未显式传入 `--manifest-path` 的 `aits backtest-pit-coverage` 默认重建并校验累计
   manifest；显式 manifest 继续保持历史兼容语义。
3. 让交易日 valuation capture 继续只写当日隔离目录，但读取 canonical 累计 normalized
   PIT estimates 与累计 valuation history。
4. 对累计索引中的 `snapshot_id` 冲突、valuation history 重复 key、payload/available-time
   校验问题保持 fail closed 或显式失败，不静默平滑。
5. 更新系统流图、artifact catalog 和 operations runbook，使累计读取边界可审计。

## 非目标与安全边界

- 不补造 2026-05-03 之前的历史 PIT，也不把当前供应商视图回填到历史 signal date。
- 不把自建 captured snapshot 升格为 `strict_point_in_time`；A 级仍需 strict PIT vendor
  archive 或等价一手可见时间证明。
- 不改变 `min_forward_days=60`、`max_staleness_days=3`、90 日 revision lookback、估值
  分位最低样本数等 reviewed policy/常量。
- 不新增 provider 请求，不执行历史 recapture，不修改 provider budget、daily retry 或
  recovery allowlist。
- 不写 official target weights，不触发 production、broker、order 或 fill。
- 本任务只修改开发 checkout；不直接 promotion 到 operations runtime。

## 设计

### 累计 PIT manifest

累计发现器只扫描显式允许的目录，不对整个 `data/raw` 做模糊分类：

- legacy：`fmp_analyst_estimates`、`fmp_historical_valuation`、
  `fmp_forward_pit`、`eodhd_earnings_trends`；
- daily capture：每个合法 ISO 日期目录下同名的四类子目录。

结果按 `snapshot_id` 确定性排序并写入
`data/raw/pit_snapshots/cumulative_manifest.csv`。同一 `snapshot_id` 若映射到不同记录或
不同 payload 路径，生成过程必须失败；下游仍使用既有 manifest validator 校验必填字段、
source catalog、payload 存在性、checksum、字节数、row count、日期约束和重复 key。

### Coverage 默认消费

`aits backtest-pit-coverage` 的选择规则为：

1. 显式 `--manifest-path`：只校验并消费该文件，不重建累计索引；
2. 未显式提供：从 `--raw-root` 发现累计 raw，写入
   `--cumulative-manifest-path`，随后执行同一 validator 和 readiness 计算。

现有 monthly scheduler 命令无需改变；其无显式 manifest 调用会自动进入累计模式。

### Valuation 累计历史

交易日 capture 的 FMP valuation component 保持以下写入边界：

- 当前 valuation YAML 写入
  `data/external/daily_input_capture/YYYY-MM-DD/valuation_snapshots/`；
- 当前 analyst raw 写入
  `data/raw/daily_input_capture/YYYY-MM-DD/fmp_analyst_estimates/`。

历史读取改为：

- `--pit-normalized-path data/processed/pit_snapshots`，并继续执行
  `available_time <= decision_time`；
- `--valuation-history-dir data/external`，只递归读取文件名匹配
  `fmp_*_valuation_*.yaml` 的快照，忽略无关 YAML；feature 计算继续排除
  `snapshot.as_of >= as_of`。

## 阶段、依赖与验收

|阶段|依赖|实施内容|验收标准|
|---|---|---|---|
|1|现有 PIT manifest schema 与 validator|累计 raw 发现、冲突检查、manifest CLI/默认路径|legacy + 多日 capture 均被纳入；顺序确定；冲突 fail closed|
|2|阶段 1|`backtest-pit-coverage` 默认累计消费与显式旧路径兼容|默认写累计 manifest 并输出 B/A readiness；显式路径不改变|
|3|现有 valuation as-of loader|daily capture 接入 canonical PIT/valuation history|当前日仍隔离写入；90 日 revision 与分位可读取历史；未来/同日快照不进入历史|
|4|阶段 1-3|系统流图、catalog、runbook、generated fragment shadow|monolith 与 generated authority 一致，月度/每日边界清楚|
|5|阶段 1-4|focused、architecture/contract、integration、reproducibility、Full 与真实数据只读 smoke|测试通过；真实归档报告为 B 或按 freshness 明确降级；无 active runtime 写入|

## 工作区与发布生命周期

- governed mode：`SINGLE_LANE`；
- frozen base：`ab2c7077ec38d92d40d2b9143a595b7508885949`；
- task branch：`codex/prod-004-pit-cumulative-consumption`；
- 本任务复用主 checkout，不创建临时 worktree、clone 或外部 cache；
- publication transaction：
  `prod-004-pit-cumulative-consumption-20260831-v4`；
- 完成条件：candidate 通过正式验证，local `main` fast-forward，普通 push 后
  `local main = origin/main = candidate`，publication lease 正常释放；若 remote diverge、
  非 fast-forward 或出现未归属变更则停止。

## 状态记录

- 2026-08-31：owner 要求继续推进。只读盘点确认 physical PIT archive 已达到 B 级日期
  数量，但 monthly coverage 和 daily valuation 仍消费单日目录，因此任务从
  `BLOCKED_EXTERNAL` 转为 `IN_PROGRESS`，先修复累计消费合同；不补历史、不改变 A 级定义。
- 2026-08-31：累计发现、coverage 默认消费、valuation history 与 daily capture 合同已实现；
  focused Ruff PASS，focused parallel pytest `116 passed`。对独立 operations runtime 的
  只读 smoke 得到 4,808 snapshots、2,069,507 rows、87 日期、17 ticker，manifest/coverage
  均 PASS，`valuation_expectations` 为 B（首个 B 日期 2026-07-18，最新 2026-08-29，A
  仍为空）；累计 valuation history 1,211 条、PIT analyst history 1,169 条且无 history
  issue。任务转为 `VALIDATING`，等待 generated authority 和正式验证；active runtime 未写入。
- 2026-08-31：v1 transaction 在 generated-authority focused test 阶段发现固定 source-seal
  hash 测试也必须随 monolith 更新，但该 coordinator test 未列入 v1 声明路径；未修改未声明
  文件，v1 以 FAILED 释放 lease。v2 增加
  `tests/test_devx_006d_report_catalog_flow_authority.py` 后重新 acquire，作为本次唯一继续发布
  transaction；没有复用失败测试作为 PASS evidence。
- 2026-08-31：v2 的 `architecture-fitness` 发现 3 个既有 PIT 接口文件发生了受控变更，
  但 compatibility authority 尚未追加 PROD-004 接管节，导致 116 个历史 current-hash
  权威断言连锁失败；业务测试没有失败。v2 连同该正式失败 artifact 以 FAILED 释放 lease，
  未把失败结果重解释为 PASS。v3 增加 compatibility generator、index/fragment 与 contract
  test 声明范围，追加 `phase_prod_004_pit_cumulative_archive_consumption_v1` 后从新候选重跑。
- 2026-08-31：v3 在 source-seal 检查与 compatibility build 并行调度时，build 先于
  `GENERATED_REBUILD_PRE` checkpoint 写入已声明 generated 路径。不存在路径越权，但阶段
  顺序不合规，因此 v3 以 FAILED 释放 lease，并把生成索引作为事故证据；该次输出不作为
  发布 PASS evidence。v4 复用同一候选与已声明范围，从 `TASK_SOURCE_PRE_WRITE` 开始严格串行
  执行 checkpoint、三类 generated authority rebuild 与正式验证。
