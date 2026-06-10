# TRADING-174 Full Pytest Runtime and Validation Tier Optimization

最后更新：2026-06-10

## 1. 背景

近期多个任务记录显示 full pytest 本地验证出现 15 分钟超时：

- TRADING-156 to TRADING-160：full pytest 15 分钟超时，未返回失败明细。
- TRADING-169 to TRADING-173：full pytest 15 分钟超时，未返回失败明细。
- TRADING-101：曾记录 full pytest 15 分钟超时，后续补跑通过。

同时，多个历史任务显示 full pytest 曾在约 10-11 分钟内完成，且当前测试规模已超过
2300 tests。问题更像验证套件 runtime / 分层策略问题，而不是当前某个业务模块的确定性
失败。

## 2. 目标

1. 找出 full pytest runtime 的主要耗时来源：慢测试、慢目录、重复集成路径或外部依赖。
2. 建立开发期验证分层，让常规任务可以快速跑到与改动相关的高信号 suite。
3. 保留 full pytest 作为完整门禁，不把 timeout 当作 PASS。
4. 让 validation summary 可审计：明确跑了哪些 suite、覆盖哪些风险、未跑 full 时的限制。

## 3. 非目标

- 不降低 P0 投资解释、数据质量、scoring、backtest、report 或 broker safety 的验证要求。
- 不因为 runtime 长而删除关键测试。
- 不把慢测试隐藏为默认跳过，除非有明确 marker / 文档 / CI 策略。
- 不改变业务逻辑、production state、runtime artifacts 或 broker 行为。

## 4. 初始假设

1. 当前超时可能来自全量测试规模增长，而不是单个 hang。
2. 某些 integration / CLI / Reader Brief / Dynamic v3 / trading_engine tests 可能重复构建较重 fixtures。
3. 本地 15 分钟工具超时低于 full suite 的稳定上界，导致验证结果不稳定。
4. 建立 fast/domain/full tiers 比盲目延长 timeout 更能提升开发效率。

## 5. 验收标准

- 记录 pytest collection 规模和 runtime profiling 方法。
- 输出 top slow tests / slow directories / slow markers 的测量结果。
- 提供一个可重复的 fast validation entry，用于常规代码改动前快速反馈。
- 提供 domain scoped validation guidance，用于 P0 ETF / Dynamic v3 / Reader Brief 等任务。
- full pytest 仍保留为完整 gate；若仍超时，记录明确慢点和下一步，不写成 PASS。
- 更新 task register 和本文状态。
- 通过 ruff、compileall、git diff check 和新增/修改工具测试。

## 6. 进展记录

- 2026-06-10：新增任务并进入 IN_PROGRESS；开始检查 pytest 配置、collection 规模、历史 runtime 和 slow test profiling。
- 2026-06-10：collection profiling 结果为 2311 tests，collect-only 约 4 秒，说明主要问题不是
  collection hang。
- 2026-06-10：本机执行 `python -m pytest tests -q --durations=50 --durations-min=1`
  完成时间约 1167 秒（19 分 27 秒），结果为 2310 passed / 1 failed；失败来自
  documentation contract 对 TRADING-169 到 TRADING-173 新增 catalog 行的 schema/status
  术语警告，已通过补充 artifact catalog 行修复，针对性回归通过。
- 2026-06-10：top slow tests 集中在 Dynamic v3 simulation/research 和 trading_engine
  integration/report 类用例，最慢单测约 74 秒；因此新增 `scripts/run_validation_tier.py`
  作为 auditable pytest tier runner，提供 `fast`、`reader-brief`、`dynamic-v3`、
  `trading-engine` 和 `full` 分层入口。CI full pytest 保持完整执行，但增加
  `--durations=50 --durations-min=1` 以便后续失败/超时时定位慢点。
- 2026-06-10：`python scripts/run_validation_tier.py fast` 实测 37 passed，pytest runtime
  约 13 秒，script elapsed 约 15 秒。该 tier 适合 CLI wiring、report registry 和 docs
  contract 快速反馈，但不能替代涉及投资解释、data quality、scoring、backtest、Reader
  Brief 或 broker safety 的领域 gate / full gate。
- 2026-06-10：`python scripts/run_validation_tier.py full` 在 15 分钟后仍正常运行并最终
  PASS：2313 passed、330 warnings，pytest runtime 1076.41 秒（17 分 56 秒），script
  elapsed 1078.43 秒。结论：此前多次 15 分钟 timeout 主要是本地命令超时预算低于当前
  suite 真实成本，不应被解释为 pytest hang 或 PASS。
- 2026-06-10：最新 full tier top slow tests 仍集中在
  `tests/test_etf_dynamic_v3_parameter_research.py`（约 60 秒/测试）、
  `tests/test_etf_dynamic_v3_failure_attribution.py`（约 44 秒）、
  `tests/test_etf_dynamic_v3_real_evaluation.py`（约 35 秒）、
  `tests/test_etf_dynamic_rescue.py`（约 25 秒）以及
  `tests/trading_engine/test_portfolio_*` / tracking window / candidate tracking
  integration tests（约 19-24 秒）。后续若仍要缩短 full runtime，应优先调查这些
  fixture/artifact 生成路径是否可缓存或拆分。
- 2026-06-10：owner 要求继续压缩 full runtime。第二阶段从 top slow tests 入手，优先
  处理 Dynamic v3 simulation/research 和 trading_engine integration/report 中的重复真实评估、
  重复 synthetic price generation、重复 CLI validation/report 生成路径。约束不变：不跳过关键
  投资解释测试，不把 slow tests 默认隐藏，不降低 full gate。
- 2026-06-10：第二阶段完成首轮 runtime reduction：
  - `test_dynamic_v3_real_validation_report_and_cli_pass` 和
    `test_dynamic_v3_failure_attribution_validation_report_and_cli_pass` 改为由 CLI 完整生成
    validation artifact，再读取 JSON 断言 status/safety，去掉同一测试内 direct builder + CLI 的
    重复 heavy sample build；两个用例合计从约 80 秒级降到约 42 秒。
  - `test_etf_dynamic_v3_parameter_research.py` 的 real smoke cache 从 520 个 trading days
    收窄到 360 个 trading days，并把 injection audit contract 测试从 2 个真实候选收窄为
    1 个真实候选；该文件 17 tests 通过，总耗时约 52 秒，两个原 60 秒级测试合计约 48 秒。
  - portfolio candidate / review / tracking shared fixture 从 80 days / 20 min history 收窄到
    40 days / 12 min history；相关 40 tests 通过，总耗时约 2 分 28 秒，原 23 秒级
    shadow-backtest reference 用例降到约 11-12 秒。
  - `python scripts/run_validation_tier.py dynamic-v3` 结果为 47 passed，elapsed 227.26 秒
    （约 3 分 46 秒）。
  - `python scripts/run_validation_tier.py full` 结果为 2313 passed、640 warnings，
    pytest runtime 864.67 秒（14 分 24 秒），script elapsed 866.78 秒；相对第一阶段
    full baseline 17 分 56 秒，减少约 3 分 32 秒。
- 2026-06-10：剩余 top slow tests 已从 60-74 秒级下降到约 30 秒级；后续继续压缩应优先
  调查 `test_etf_dynamic_rescue.py`、Dynamic v3 failure/real report builders，以及
  trading_engine portfolio sensitivity / shadow-backtest reference 测试是否能复用已生成的
  diagnostics/summary artifacts，或拆分为 contract-only 与 full integration 两层。

## 7. 当前使用规则

本阶段不降低 full pytest gate，只改变日常反馈入口：

- 普通 CLI wiring、report registry、documentation contract 改动：先跑
  `python scripts/run_validation_tier.py fast`。
- Reader Brief / report navigation 改动：跑
  `python scripts/run_validation_tier.py reader-brief`，必要时叠加 `fast`。
- Dynamic v3 rescue、backtest simulation、simulation advisory review 改动：跑
  `python scripts/run_validation_tier.py dynamic-v3`。
- paper trading engine、scheduler、portfolio tooling 改动：跑
  `python scripts/run_validation_tier.py trading-engine`。
- 跨模块、P0 投资解释、data quality、scoring、backtest、broker safety 或交付前最终验证：
  跑 `python scripts/run_validation_tier.py full`，本地命令 timeout 应按 25-30 分钟级设置。

如果没有运行 full gate，交付说明必须明确已运行的 scoped tiers 和限制；如果 full gate
被环境超时中断，不能写成 PASS，必须记录 timeout 时长、已完成进度和可见 slow-test 线索。
