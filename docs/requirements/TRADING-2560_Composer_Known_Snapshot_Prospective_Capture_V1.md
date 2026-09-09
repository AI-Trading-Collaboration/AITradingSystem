# TRADING-2560：Composer 当前已知快照与首次真实前瞻记录

日期：2026-09-09（Asia/Tokyo）。状态：IN_PROGRESS；P0。
所属任务：TRADING-2560_FIRST_LAYER_COMPOSER_V2_PROSPECTIVE_OOS_OBSERVATION_V1。
Owner：Project Owner；执行及下一责任人：integration-coordinator / Codex。
冻结 local main：a5b82f14f97a898d8c25951560f6e77d15aca295。

## 1. 研究目的与 Owner 决定

Owner 要求「赶快回到研究任务上吧」。本波直接承接
SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2：冻结的 Composer 信号在真实可执行的时点，能否在同资本、
时间和成本下提供相对预登记 benchmark 的增量价值。既有历史证据仍为 INSUFFICIENT/HOLD，
不因工程完成而改变。主研究窗口的项目默认起点仍为 2021-02-22；2018 年开始的输入只承担
既有训练初始化角色，不升级为主评价窗口。

Owner 在收到“生成信号时已取得的数据快照、明确披露宏观滞后、保留模型/阈值规则及下一交易日
收盘生效”的具体建议后回复「继续把，打错了」，明确继续该方案。本决定只新增前瞻输入信息集
政策版本和最小采集接线，不重选模型、策略、特征、训练规则、成本或 benchmark，也不提前制定
见结果后的 sample/episode/停止阈值。适用新的 standing owner research scope；旧一次性
manifest、implementation-only policy 和历史精确授权字节均原样保留，不复用或伪写其状态。

## 2. 已知快照的信息集合同

- F 是已收盘的 canonical XNYS feature session；价格覆盖必须完整至 F。
- R 是真实输入闭包已取得并完成保存的时点，不能由 caller 任填一个历史时间。
- 输入为 R 时实际已取得的当前 revision 快照；只消费观察日期不晚于 F 的原始行。
- rates 允许其实际最后一行早于 F；不添加空 F 行、不伪造当日值、不重写原输入。
- 保留原 `reindex(XNYS_index).ffill()` 特征算法。逐 series 分别披露原始最后有效日期与实际
  effective carry_source_date；非 XNYS 原始观测可能不会进入原算法，不能把两者混为一谈。
- 每个有效输入值必须来自此次捕获的同一不可变闭包；记录 source snapshot/revision、原始 bytes
  identity、R 和实际使用来源日期。DQ 的 freshness/缺失规则保持现行 reviewed policy，不能用
  “已知”豁免无限陈旧、缺失或质量失败。
- R 只证明这些本地 bytes 在 R 已知，不证明 provider 历史 available_at。历史训练使用当前
  revision 的声明必须可见；禁止把这条未来观察升级为历史 PIT 或 pristine historical OOS。
- 原 v1 preview 入口保持 rates.max=F 及 implementation-only 合同原义；新命名入口使用单独
  policy。公共入口不得增加一个可绕过旧门禁的无政策 allow_stale 开关。

## 3. 训练、DQ 与未来结果隔离

继承 current-session producer 的模型/feature/state mapping、每次请求 refit、最新 504 个完整
成熟训练样本、20 XNYS-session label、SHY→SGOV 拼接，以及固定 threshold-selection 算法。
滚窗产生的不同 fitted threshold snapshot 是原训练规则的结果，不是事后参数调整。

完整输入质量按三个既有角色覆盖：

1. training_proxy_history：2018-01-02..F，QQQ/TQQQ/SHY 与 DGS2/DGS10/DTWEXBGS；
2. exact_sgov_history：2020-05-28..F，SGOV；
3. primary_exact_three：2021-02-22..F，QQQ/SGOV/TQQQ。

三段都带 DGS2/DGS10/DTWEXBGS；training 和 primary 固定 require_secondary_prices=true、
input_roles=(prices,rates,secondary_prices)，exact_cash 固定 require_secondary_prices=false、
input_roles=(prices,rates)，与既有分段物化规则一致，caller 不能自行减少。
不能把运营主窗 PASS 当作 2018 训练范围 PASS。复用
canonical DQ 及其 Named immutable publication/receipt/verifier；实际 requested/evaluated
和各价格/rates 覆盖单独列出。共同 evaluated 末日不被改写为 F；Composer 专属 accessor
对完整价格范围和 rates feature 使用作明确的新消费者证明，不能借用 FIVE_CANDIDATE 或
equal-risk 的 DQ_GUARD_ONLY rates 权限。

DQ 保持现有 `config/data_quality.yaml` 数值及 consistency_start_date：2018 训练的 requested
coverage/缺失检查不等于所有跨来源 consistency 检查也从 2018 开始。收据必须原样暴露该政策
边界，不宣称获得未执行的 pre-2021 strict cross-source 或 historical PIT 证明。

历史标签构造会读取历史收益，最后 fit 只选成熟的 504 样本。因此分开记录
historical_training_access 与 future_observation_outcome_access=false，不能把整次 fit
称为零 outcome 访问。固定 prequential 更新会在未来标签成熟后消费其历史训练信息；原始
observation identity 不随 refit 改写，相应历史也不得重新标为 untouched。

## 4. 首次运行边界

本地 source root 为只读的 D:/Work/AITradingSystem_ops_runtime；实际执行代码和 evidence
留在 D:/Work/AITradingSystem_devx014_source_preservation。不修改运营缓存、当前下载指针、
release 或 scheduler，不新增 provider/download 调用，不运行 daily pipeline。

先做一次显式快照的完整分段 DQ readiness 检查：最多三个 canonical DQ dispatch，每段一次；
保存原失败/成功收据和实际计数，不重试同一已尝试 key。readiness 与 capture 分阶段独立记账，
整份有限 manifest 的 canonical DQ 总上限为 6；readiness 不运行 model fit，capture 最多一次
四模型的既定 fit pipeline。现有 2026-09-04 快照只能承担训练
输入检查，不能回填真实前瞻观察，也不证明未来 F 的 freshness。

实际 activation/capture manifest 由 coordinator 在派发前从精确代码、政策、source 和
canonical 日历自动生成并独立 replay。Owner 无需把机器 SHA 粘贴回聊天。有限范围为一次
activation、一个明确的未来 feature session、每 session 最多一次 capture、最多三段 DQ；
有效期限不晚于该 session 的 decision close。异常保留原尝试和 counters；不换名重试或
把失败擦成没有发生。技术 validation_state 与 authorization_state 分别记录。

最早 F 为实际 activation 完整确认时刻的纽约日期之后首个 XNYS session。D=next_XNYS(F)，
真实 capture 在 F close 之后完成，完整信号记录确认必须严格早于 D close；待测收益从
Close(D) 起。复用 S3a v2 / S3b corrected HostClockEvidence，禁止倒填时刻或沿用 v1
错误的跨时钟不等式。缺少当前输入时记录 typed blocker；时间未到时如实保留等待状态。

首条完整 observation 绑定输入、fit audit、signal、policy、DQ 和真实时间闭包。maturity、
scoreboard 和本次前瞻未来 outcome 访问保持关闭；正式访问前仍须完成既有 S4 的研究/会计、
sample/episode、停止和重复查看规则。不得以实现本入口顺便开展更多历史 empirical runs。
production_effect=none、broker_action=none；provider/download/cache mutation/QuantConnect/
Options/paper/live/order/fill/position 动作均为零。

## 5. 顺序与验收

1. SINGLE_LANE 预检、publication fence、canonical task 更新和本需求先于实现。
2. 最小串行输入合同先经独立静态审查，再接共享计算核心及 Composer 专属 verified accessor。
3. 接三段 DQ、同一进程代码/输入身份、activation/recording、原始终态和 retained replay。
4. 16-worker focused tests 证明 v1 不变、允许真实较早 rates、不补造行、逐 series carry 真实、
   非 XNYS/raw/effective 区分、未来行/非有限值/缺系列/DQ失败/错误scope与源码身份拒绝；
   同 key 幂等、部分失败不重试、freeze/输入/signal 次序与 next-close 截止、训练和未来结果分离。
5. 更新 system flow/运行入口与生成权威，最终精确 candidate 完成所需正式验证，普通 local-main
   fast-forward/push 和 cleanup。只把实际完成阶段写为完成，不把能力 PASS 写成真实观察 PASS。
6. 在已验证代码上执行受限真实 readiness，建立真实 activation；仅在合法 F/D 时间及严格 DQ
   满足时写第一条 observation。未到时点或数据缺失时保留具体下一日期/输入条件，研究任务继续。

## 6. 工作区与证据生命周期

复用 canonical root，不创建新 worktree/clone。分支
codex/trading-2560-composer-known-snapshot-v1 从上述 exact main 开始。
协调证据：outputs/architecture/trading_2560_composer_known_snapshot；真实研究 evidence：
outputs/research/composer_prospective。二者为保留的 canonical evidence，合成样本单独标识。
分支在已验证 ordinary push、确认无唯一实现后删除；临时验证目录仅在逐项内容/ignored/进程
审计及 required evidence 留存后按绝对 allowlist 清理。既有 canonical root 不在删除范围。

## 7. 进度与审计

- 失败计数的终态语义：原 Named child 调度器已观察到计数，但 terminal 文件发布失败时，
  保留实际次数。若已有 parent binding，则独立验证它；若没有，则明确记录
  `ORIGINAL_PARENT_OBSERVATION_ONLY`，仅由原父进程 source/lease/clock 终态保存这个观察。
  后者可形成 BLOCKED result 以保留审计事实，不能获得 input seal、CAPTURED 或技术成功。
  原时钟已失败或 source terminal 无法完成时仍不能发布 result，仅保留异常及 INCOMPLETE。

- 2026-09-09：START/LANE（SINGLE_LANE、contract_change=true）PASS；main/origin 同 SHA，
  clean 且无旧 active lease。source fence 采用
  trading-2560-composer-known-snapshot-source-20260909-v1。
- 前置只读调查的一次范围超读：审查员为读取运营 DQ metadata，对既有
  outputs/data_quality/reports/561a04d4cf8e2d02fbc308a8c44c154ee1c9d6b3782c63de454c7c559134c0be/
  data_quality_report.md 使用 Get-Content -TotalCount 58，超出第 27 行 metadata 段，返回无关
  非 Composer 个股数值样例。没有打开 CSV、运行 DQ/model/preview 或访问 Composer signal/
  outcome；样例未用于判断。记录为 READ_SCOPE_EXCESS，不声称该次全程 metadata-only；
  保留原工具输出，不重新复制数值。后续 metadata 查询限字段/行，真实输入访问另有上述完整
  研究用途和动作记录。这不改变历史研究结论或创建任何前瞻样本。

- 最小实现已接通：新输入policy、三段独立Composer DQ scope、不可变same-bytes seal、真实输入R先于fit、activation/capture及retained replay。旧producer计算AST不变；103 source modules/24 policy dependencies闭包经复核。
- 聚焦验证：producer 39 PASS、DTO 186 PASS、Named dispatch 79 PASS；共享Named初轮293 PASS/1 fixture FAIL，修正fixture后精确重验1 PASS。runtime分轮失败和修正及身份限制见canonical outputs下runtime_validation_composition_v1.json；不改写原FAIL。6个实际committed-candidate synthetic测试及最终正式验证尚待执行。真实DQ、fit和observation均为0，研究结论仍为INSUFFICIENT/HOLD。

- 共享 source 验证原轮为 500 PASS / 1 FAIL：新增八个 Composer catalog/system-flow 条目后，
  一处 live successor count 断言仍为 3187。仅将当前 count 更新为经官方生成器核实的 3195，
  历史 workflow contract、hash 和安全断言保留；原 FAIL 与精确重验分开留存。
- 本节计数描述各工程检查当时的状态，不能替代后续真实运行证据。首次真实运行固定在
  `outputs/research/composer_prospective/composer_known_snapshot_first_real_v1/`；实际
  readiness/activation 的原始 result、外层进程终态与 coordinator result 是该次运行状态来源。
  未创建结果、部分 attempt、BLOCKED 和成功必须分别解释，不能用本需求中的工程 PASS 推断。
- source commit `d34610c9b` 后的 Atlas exact-commit 检查识别旧页面需求绑定；页面仍指向
  原 producer-not-ready 文档，而 canonical task 已绑定本需求。直接同步当前任务链接、
  coverage 与 reader summary，不改历史页面快照或投资结论。原 final transaction 未派发
  Full，作为失败准备证据释放；小范围 source 修正提交后重新绑定最终候选并验证。

- committed candidate d2099dbe 的 6 项真实 bootstrap 合成验证为 2 PASS / 4 FAIL。
  三段 same-bytes seal、旧 profile 限制通过，activation 及其 replay 完成；三项坏输入
  均按原 DQ policy 返回 FAIL 并禁止 dispatch/seal。失败来自新增测试误把正常 CLI
  执行退出与质量 PASS 混为一谈，并误认为关联证明等于输入访问授权。仅修正测试断言，
  显式保留 FAIL、issue codes、ready=false、dispatch=false 和 no seal。另一个失败为
  readiness 父层没有初始化已声明的分段 evidence root，child 返回 ARTIFACT_ROOT_INVALID。
  修复采用现有 contained directory 创建，在实际子派发前建立受 lease 管辖的目录；
  真实路由测试不加 mkdir，通用 dispatcher、DQ 门槛及策略不变。原 synthetic DQ 总数
  为 8，未执行后两段，不改写为 10。原 FAIL 永久保留，新候选另行复验；此前未派发 Full。
- 分段目录创建前逐次重验原 source、manifest expiry 与 S4D lease；前一段长时间 DQ
  完成后若 lease 已失效，下一段不能创建目录或 dispatch。聚焦回归覆盖第一/第二段完成后
  撤销 lease，保留已实际观察到的次数并维持 INCOMPLETE，不能发布成功终态。

- 2026-09-09 路由验证运行预算修正：c063d347 的 5 项 seal 检查通过；原第六项
  在 publication lease 到期后失败。以新 lease 仅补验该项后，pre/post guard 均 PASS，
  三段 synthetic DQ 均完成，但父测试 300 秒上限在后续结果验证前终止 readiness。
  原两次 FAIL 及合计 13 次 synthetic DQ 完整保留，真实 DQ/fit/observation 均为 0。
  intended solution 为仅将测试外层通信预算设为既有 300 秒启动/审计预算加声明的
  named DQ 子请求数量乘生产 CHILD_TIMEOUT_SECONDS（120 秒）；三段为 660 秒，
  activation 仍为 300 秒。嵌套子进程上限、runtime、clock、lease、DQ/PIT 和投资
  policy 均不变。这是匹配嵌套工作的测试资源边界，不是临时绕过失败。
  验收：Black/Ruff/mypy、相关 source/generated 检查及最终候选上的实际路由通过，
  全部 6 项 actual-candidate 测试在正式 Full 中必须通过，并保留原失败和实际计数。
  final-v3/v4 和各测试临时目录按同一任务生命周期在证据入库和无依赖后清理。

- 2026-09-09 最终集成：冻结 source 4f6c0485 的实际 Composer route 已通过；等待中的另一任务已正式发布 main 7477d7b3。按 integration-revalidation-c9c29ce7a24f3cd4db0f 逐项复核，24 项交集均为 task index/Markdown/compatibility 生成状态，无运行时或研究契约冲突。使用现有 canonical checkout 的单一 codex/trading-2560-composer-integration-v1 candidate；保留新 main 的 2564 task/events/requirements，通过官方 task writer 原样重放 2560 原事件并记录 cycle 映射，然后重建共享生成物。旧 source/review/失败证据保留；最终候选四个必需 tier 和 Full 重新验证，Full 必须覆盖全部 6 项实际 Composer 测试。通过后 ordinary push、证据归档和分支/fixture 生命周期清理，再执行固定真实 readiness/activation；当前真实 DQ/fit/observation 仍为零。冻结 source 分支在证据/可恢复提交保全后按 reviewed patch equivalence 清理；无新增临时 checkout。
