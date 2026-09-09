# TRADING-2564 S4：前瞻收益会计算术核验 V1

日期：2026-09-09。状态：IN_PROGRESS；owner / next owner：integration-coordinator / Codex。
所属任务：TRADING-2564_LONG_TERM_RESEARCH_CAPABILITY_IMPROVEMENT_V1。
Owner 在批准既定研究顺序后补充「后续其他任务也要推进下」。本波使用既有 S4 会计范围，
不新建 umbrella 或改选主策略；exact local-main base 为
`4ac10a668f203486f4f22921f7e2548adfc9ef42`。

## 阶段决定 SDP-20260909-002

- 上一决定：阶段试行中的 SDP-20260909-001。Composer 输入连接已发布，实际 readiness /
  activation 已完成；首个 F=2026-09-10，capture 尚未派发，未来时间和输入依赖不变。
- 主问题：在合法执行与相同资本、时间、成本下取得可信策略证据。Owner 另要求推进其他任务；
  本波选 equal-risk 首次成熟结果消费所必需的 S4 会计核验，完成后返回该具名消费的准入工作。
- 最新旧 DQ 事实：原开发根的 corrected DQ 已于 2026-09-04 实际执行并 FAIL；不是仍待第一次
  corrected retry。已只读核对 `D:/Work/AITradingSystem/outputs/data_quality/executions/`
  `dq_execution_d4229d2a50e008715a99b46e1f3d17077b1d6936e6bbb034130a0f43c48ffecb/receipt.json`：
  requested 主窗 2021-02-22..2026-09-03，evaluated 到 2026-07-23，five blocking issues。
  该回执只能说明该输入 FAIL，不替代新命名快照 DQ，也不重写旧 2563 manifest / task event。
- 必要依赖：旧 `simple_baseline_forward_aging._forward_window_metrics` 把 SGOV 平均仓位写成
  `cash_drag`，且回撤路径没有纳入期初净值。将旧报表原样解释成现金收益贡献或完整持有期
  drawdown 会误导随后 equal-risk 的收益评估。旧 frozen catch-up 源不在本波修改范围。
- 唯一动作：R4，有界会计算术基线；不读取真实账本或收益，不重试旧 DQ，不扩展所有 legacy
  reader、S5、scheduler 或新策略。独立 reviewer 做只读合同/实现审查，coordinator 拥有全部写入。
- 返回条件：纯内存核验、合成正负例、独立审查和正式发布通过后结束本波；后继仍须明确
  equal-risk first-access adapter、历史暴露、会计输入来源、DQ/PIT、协议与真实执行范围。

## 合同与解释边界

新增独立 `research_forward_accounting.py`，不接旧 CLI / writer，不修改旧 observation、
冻结策略、成本政策或 sample/episode 阈值。旧模块的数值和历史 artifact 原样保留。

输入为显式的期初估值日期、完整预期 session 序列、固定资产集合和逐期自融资账本：
beginning NAV、每个资产扣本字段交易费用前的货币 PnL、单列非负 trading cost、ending NAV、
显式 external flow。V1 只允许 external flow=0、正期初/期末 NAV；不支持清算到零或负资本。
金额只接受有限 Decimal；内部转 Fraction 做精确算术，不依赖调用者 Decimal context。

每期和全期必须分别满足：

1. ending NAV = beginning NAV + sum(asset PnL) - trading cost；
2. next beginning NAV = previous ending NAV；
3. sum(all asset PnL) - sum(all costs) = final NAV - initial NAV。

资产累计贡献为累计货币 PnL / initial NAV；费用贡献为负累计费用 / initial NAV；净收益为
final NAV / initial NAV - 1。比值保持精确 Fraction，贡献总和必须与净收益完全相等，
不把舍入残差塞入 SGOV 或最后一个资产。此定义是初始资本归一化的累计货币归因，不是各资产
收益率简单相加，也不是逐资产独立复合收益。PnL 已扣本项费用时不得再以 cost 字段重复扣费；
不平衡输入 fail closed，但语义上把成本伪装成损益不能由纯数学层单独识别。

最大回撤从包含 initial NAV 的完整净值路径计算，结果以非负损失比例表示；
`100 -> 80 -> 90` 必须为 1/5。SGOV 若在显式资产集合中，只输出它实际输入的 PnL 贡献，
不把仓位当作 cash drag、机会成本或再加一层无风险收益。

预期日期与账本日期必须非空、唯一、递增且逐项一致；拒绝缺失、额外、重复、乱序与 datetime，
不排序、不填零。资产集合也必须非空、唯一且逐期 exact，不静默遗漏零收益资产。
纯内存接口只核验 caller 声明的日历序列，不能证明 XNYS 完整性、真实持仓、价格、费用政策、
provider available_at、PIT、无外部暴露或真实 DQ。真实 adapter 必须在首次读取输入前通过
既有 S4 gateway，独立证明 source / calendar / DQ / policy；本波没有该 adapter 或真实访问授权。

主研究默认仍为 2021-02-22；本波只使用明确标识的合成日期和金额，不产生实际 requested /
evaluated 市场研究窗口，不得把合成例子升级为回测或投资结果。

## 分步与验收

1. 本支持需求、canonical task 更新和写入门禁先于实现；合同独立审查后进入纯算术实现。
2. 合成并行测试覆盖非终止小数、改变 Decimal context、大金额加微小损益、资产排列、费用、
   多期复合、首日亏损回撤、连贯性及逐期/全期平衡、日期/资产全集与非有限/类型/现金流负例。
3. 模块不导入数据读取、DQ、writer、gateway 或 provider；无调用即无文件输出。
   不新增阈值、评分、策略结论、自动 promotion 或生产路径。
4. 独立审查和 focused / static 检查通过后，更新 system flow / architecture authority；
   在最终精确 candidate 完成必需 formal tiers 与 Full，再普通 local-main / origin-main 发布。
5. baseline 的技术 PASS 仅表示会计算术层完成；首次真实结果准入及两份旧 observation 的
   maturity / scoreboard / continuity 仍未执行。返回既有 S4 后继，禁止无限扩展会计平台。

## 工作区、证据与进度

SINGLE_LANE，复用 `D:/Work/AITradingSystem_devx014_source_preservation`，分支
`codex/trading-2564-s4-forward-accounting-v1`；不创建新 checkout。canonical 协调证据保留在
`outputs/architecture/trading_2564_s4_forward_accounting/`。正式测试可能创建的既有 S3b / Composer
合成目录由本 transaction 声明；收口前核验 unique 内容与进程依赖、保存必要 evidence 后清理。
退出时删除已发布分支；canonical root / evidence 留存，不在清理允许列表内。
Composer 的原 activation / capture manifest 仍绑定原 candidate `4ac10a668`；131 条显式
依赖路径相对该 base 均逐字未变。会计新提交不替换原 capture 身份；后续仍须在原 manifest 的
精确 candidate / root 上按新 lease、新输入和原时间条件执行，不把推进 main 等同于重签冻结。

- 初次 START 缺少 scope 被拒；补充实际 path claims 后 START/LANE PASS，没有提前实现。
- 首次 acquire 使用系统 Python 3.14.4，Windows stat / fstat 的 st_ctime_ns 不同而失败；
  migration owner.json SHA-256 为 1481ce996900f09ae072e4c1d7c2075502a3d7c40baa7453277b7c1d9578c5f8，
  前后 path stat 相同、handle stat 相同，未观察到内容改变。v1 transaction 未创建。
  改用既有 `D:/Work/AITradingSystem/.venv/Scripts/python.exe` (3.11.9)，同证据只读 replay PASS，
  source-v2 acquire / TASK_SOURCE_PRE_WRITE PASS。未改证据、时间字段、guard 或锁。
- 合同独立审查采纳 Fraction 精确比值、期初回撤、完整日期/资产与非真实 authority 边界。
  随后纯算术实现及首轮 54 项合成测试通过；独立 reviewer 无 must-fix，并补验极端 Decimal
  context 下微小漏计和四期跨峰值回撤。两反例纳入长期回归后共 56 项通过，Black / Ruff /
  strict mypy 通过。原始源码 SHA 为
  `92248d1bee72b517e1204948e41850f33b63c240caf70842d1aea9ee2974113b`。
  正式 source / candidate 验证和发布尚待完成，不预先宣称本波已交付。
  真实 DQ、data / outcome、capture、maturity、scoreboard、provider、
  download、cache mutation、order / fill / position 全部为 0；production_effect=none。
- 共享兼容性首轮 500 PASS / 1 FAIL：新增 catalog / system-flow 五条后，一个 current successor
  count 仍为 3195，官方生成/验证为 3200。仅修正该 current count 和解释注释；DEVX-011
  历史合同、fragment count=192、hash 检查、safety 与全部投资规则不变。原失败 XML、源字节和
  精确修正复验单独保留；最终 candidate 的正式 Full 仍需覆盖原测试。
- 本波最终技术/发布状态以同一 canonical evidence 目录中的
  `final_validation_acceptance_v1.json`、`main_publication_v1.json` 和 cleanup / release receipt
  为准；未出现的回执不是 PASS。发布通过后下一动作是既有 equal-risk first-access adapter
  与结果盲协议的准入，不重复已完成会计基线或旧 DQ retry。
