# ARCH-004G2 Interfaces 与 ETF CLI 迁移

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004G2_INTERFACES_AND_ETF_CLI_MIGRATION`
- parent：`ARCH-004G_DOMAIN_MIGRATION_AND_SUBTRACTION`
- priority：`P0`
- status：`IN_PROGRESS`（G2.3 `COMPLETE`，G2.4 `IN_PROGRESS`）
- owner：interface platform / architecture coordinator
- dependency：ARCH-004G1 `COMPLETE`
- production effect：`none`

## 为什么先冻结 contract

`src/ai_trading_system/cli_commands/etf_portfolio.py` 当前包含41个一级command group、291个group、993个叶子命令。它同时承担import composition、Typer app构造、group注册、参数声明和command callback，任何直接切文件操作都可能静默改变command path、option alias、required/default、help、exit code或重复注册状态。

G2因此先把当前Click/Typer解析后的真实command tree冻结为可复算contract，而不是只数decorator。Baseline必须忽略callback模块位置，使函数迁移不会天然造成漂移；但必须覆盖用户可见的path、参数、类型、默认值、flag语义、help、group行为和duplicate状态。

## 分阶段计划

### G2.1 Command registry 与 golden contract

输入：

- runtime `etf_app`；
- Click/Typer解析后的command tree；
- 当前`etf_portfolio.py` source hash与deprecation inventory；
- 两组待选真实CLI fixture及其现有测试。

计算：

1. 递归遍历Click group，生成稳定command path；
2. 对每个group/leaf规范化help、short help、epilog、invoke/no-args行为；
3. 对每个parameter规范化argument/option类型、name、opts、required、nargs、multiple、default、type、flag/count/hidden/help；
4. 对每条command contract计算canonical JSON SHA-256，再计算全树SHA-256；
5. 同级command name重复、path重复、非确定default或baseline drift一律fail closed。

输出：

- canonical CLI contract scanner；
- `inputs/architecture/arch_004g2_etf_cli_contract.yaml`；
- command/group/count/hash drift tests；
- 两组真实CLI fixture选择与迁移前结果。

退出：`41 root / 291 group / 993 leaf / 0 duplicate`可复算；tracked baseline hash一致；architecture-fitness纳入该test。

### G2.2 Registration shell 与 shared parameter contract

- 建立`interfaces.cli.etf_portfolio` package与明确group ownership；
- root module先只保留app composition和兼容export；
- 共享callback helper/parameter alias只在至少两个真实caller且语义一致时抽取；
- 禁止在拆分中修改threshold、date/window、DQ/PIT或artifact语义。

退出：root registration与旧command tree golden parity；无duplicate；fixture 1 PASS。

### G2.3 Data、operations 与 reporting groups

- 先迁data/features/data-quality、ops/run、report/evidence-dashboard等较低投资解释风险group；
- callback连同其专属imports移动，禁止保留双实现；
- root只注册迁移后的Typer app。

退出：选定group旧callback reachability=0；golden tree不变；fixture 1/2 PASS。

### G2.4 Research、shadow 与 portfolio groups

- 分别迁research/experiments/calibration、shadow/paper-shadow、portfolio/allocation/backtest；
- 每个slice单独证明DQ/PIT、窗口、阈值、artifact和exit parity；
- dynamic-v3大组继续按内部语义边界拆分，不形成另一个god module。

退出：全部993 leaf由明确模块owner承载；root无计算和artifact write；两组真实fixture及相关domain tests PASS。

### G2.5 Freeze、deprecation 与 closeout

- 将旧root置为thin registration façade或按gate删除；
- 更新deprecation inventory、compatibility baseline、system flow和task register；
- 通过focused、CLI contract、architecture、integration、reproducibility和full parallel。

退出：command registry全覆盖、0 duplicate、root thin、无永久dual track；G3才可开始。

## 不变量

- command name/path/options/default/help/exit不变；
- AI regime与research window不混用；
- required DQ gate不得旁路；
- 不在结构迁移中调策略、阈值、权重或promotion；
- 不产生paper-shadow、production或broker effect；
- baseline drift不得用更新snapshot直接掩盖，必须先解释差异来源。

## 当前状态

- 2026-07-11：G2.3 closeout正式通过：selected-group test=`15 passed`、contract/policy=`27 passed`、architecture-fitness=`183 passed`、standalone contract/Ruff PASS；G2.4保持`IN_PROGRESS`，Baseline Review实现尚未开始。G2.3的DQ/regime/strategy/threshold/production/broker不变量全部保留。
- 2026-07-11：G2.3正式`COMPLETE`，G2.4 research/shadow/portfolio进入`IN_PROGRESS`。退出审计固定8 slices / 9 canonical modules / 26 callbacks / 13 helpers；旧root selected callback/helper/domain imports均为0。G2.3累计root=`36,045 -> 34,440 lines / 1,049 -> 1,010 functions / 993 -> 967 decorators`，direct writer=`861 -> 860`；closeout focused=`15 passed`，CLI仍为41 root/291 group/993 leaf/0 duplicate。G2.4首个slice选择7个Baseline Review callback，本closeout未开始实现。
- 2026-07-11：G2.3H trend-calibration slice `COMPLETE`，G2.3继续。最终trend/DQ/direct/contract=`54 passed`、contract/policy=`26 passed`、architecture-fitness=`182 passed`、Ruff/standalone contract PASS；generated=`811 modules / 1,114 tests / 860 direct writers / 0 violations`。Legacy root净减3 callback、4 DQ helper及254行，DQ-before-feature fail closed，strategy/regime/threshold不变，runtime 993 leaf不变。
- 2026-07-11：G2.3H trend-calibration slice实现完成并进入`VALIDATING`。三个callback迁canonical trend module，四个cached-DQ helper迁canonical data-quality并供legacy callers alias复用；旧root定义/import=0。Root=`34,694 -> 34,440 lines / 1,017 -> 1,010 functions / 970 -> 967 decorators`；tree不变，trend/DQ/direct/contract=`54 passed`，DQ-before-feature fail-closed fixture PASS，等待compatibility/generated/architecture门禁。
- 2026-07-11：G2.3H选择`trend-calibration run/report/validate`三个DQ/feature/research-sensitive callback，并把download-manifest、Marketstack path/requirement与完整cached-DQ gate四个helper迁入canonical data-quality module。旧root callers直接alias复用，trend module禁止反向依赖legacy root。退出要求DQ先于feature/search且失败exit 1、standard-price failure保持BadParameter、policy market regime与evaluation-only/candidate-only、dataset/report/registry path/schema、report read-only、validation fail-closed及CLI tree parity。
- 2026-07-11：G2.3G satellite-attribution slice `COMPLETE`，G2.3继续。最终satellite/AI/shared-helper/contract=`78 passed`、contract/policy=`25 passed`、architecture-fitness=`181 passed`、Ruff/standalone contract PASS；generated=`810 modules / 1,114 tests / 860 direct writers / 0 violations`。Legacy root净减3 callback、2 shared helper及243行，invalid-price fail closed，AI regime default保持2022-12-01，runtime 993 leaf不变。
- 2026-07-11：G2.3G satellite-attribution slice实现完成并进入`VALIDATING`。三个callback与prepare helper进入canonical module，optional-JSON/quality-metadata进入common并由legacy 60/15 caller复用；旧root定义/import=0。Root=`34,937 -> 34,694 lines / 1,022 -> 1,017 functions / 973 -> 970 decorators`；tree不变，satellite/AI/shared-helper/contract=`78 passed`，invalid-price fail closed，等待compatibility/generated/architecture门禁。
- 2026-07-11：G2.3G选择`satellite-attribution build/report/validate`三个DQ/regime-sensitive callback，并把`quality_metadata`与`load_optional_json_payload`两个高复用helper迁入canonical common。旧root 15/60个caller通过直接alias复用，禁止wrapper/复制；satellite module复用canonical parse/resolve-date。退出要求价格质量FAIL仍exit 1、default start仍为AI regime `2022-12-01`、requested range/market regime/DQ report lineage、build/report dataset parity、validation fail-closed、artifact path/bytes与CLI tree不变。
- 2026-07-11：G2.3F parameter-review slice `COMPLETE`，G2.3继续。最终parameter/weekly/contract=`65 passed`、contract/policy=`24 passed`、architecture-fitness=`180 passed`、Ruff/standalone contract PASS；generated=`809 modules / 1,114 tests / 860 direct writers / 0 violations`。Legacy root净减4 callback、1 helper及188行，runtime 993 leaf不变。
- 2026-07-11：G2.3F parameter-review slice实现完成并进入`VALIDATING`。四个callback与共享report helper迁canonical `parameter_review.py`，直接复用weekly-review date helper，旧root无callback/helper/domain imports。Root=`35,125 -> 34,937 lines / 1,027 -> 1,022 functions / 977 -> 973 decorators`；tree不变，parameter/weekly/contract=`65 passed`，等待compatibility/generated/architecture门禁。
- 2026-07-11：G2.3F选择`parameter-review aggregate/report/run/validate`四个只读reporting callback及共享report helper。Canonical `parameter_review.py`复用G2.3E `weekly_review_date`，禁止日期helper复制；旧root不得保留callback/helper/domain imports。退出要求report/run同源、validation fail-closed、artifact path/bytes、observe-only/candidate-only/no-production与CLI tree parity。Satellite Attribution因跨价格DQ与AI-regime日期解释留待独立高风险切片。
- 2026-07-11：G2.3E weekly-review slice `COMPLETE`，G2.3继续。最终weekly/downstream/contract=`84 passed`、contract/policy=`23 passed`、architecture-fitness=`179 passed`、Ruff/standalone contract PASS；generated=`808 modules / 1,114 tests / 860 direct writers / 0 violations`。Legacy root净减4 callback、2 helper及239行，runtime 993 leaf不变。
- 2026-07-11：G2.3E weekly-review slice实现完成并进入`VALIDATING`。四个callback、generate helper与shared date helper迁至canonical `weekly_review.py`，legacy callers直接复用date helper，旧root无callback/helper/domain import wrapper。Root=`35,364 -> 35,125 lines / 1,033 -> 1,027 functions / 981 -> 977 decorators`；tree不变，weekly/downstream/contract=`84 passed`，等待compatibility/generated/architecture门禁。
- 2026-07-11：G2.3E选择`weekly-review aggregate/generate/run/validate`四个只读reporting callback及专属generate helper。Canonical `weekly_review.py`承载命令和计算编排，shared weekly-review date parser移入该模块并由尚未迁移的legacy callers直接复用；禁止wrapper/复制。退出要求旧root四callback、generate helper和weekly domain imports=0，aggregation FAIL、validation exit、generate/run alias、artifact bytes/path、observe-only/no-production与CLI tree parity。
- 2026-07-11：G2.3D evidence-dashboard slice `COMPLETE`，G2.3继续。最终domain/direct/contract=`44 passed`、contract/policy=`22 passed`、architecture-fitness=`178 passed`、Ruff/standalone contract PASS；generated=`807 modules / 1,114 tests / 860 direct writers / 0 violations`。Legacy root净减3 callback和190行，aggregation direct write同时迁canonical atomic writer且bytes parity保持，runtime 993 leaf不变。
- 2026-07-11：G2.3D evidence-dashboard slice实现完成并进入`VALIDATING`。三个callback及专属strategy-evidence imports迁至canonical `reporting.py`，date parser/report registry复用既有contract，`cli_direct`直接转canonical，旧root无wrapper。Root=`35,554 -> 35,364 lines / 1,036 -> 1,033 functions / 984 -> 981 decorators`；tree不变，focused=`44 passed`，等待compatibility/generated/architecture门禁。
- 2026-07-11：G2.3D选择`evidence-dashboard aggregate/report/validate`三个reporting callback及其专属strategy-evidence imports。Canonical `reporting.py`直接复用common date parser与report registry contract，`cli_direct`同步转canonical，禁止legacy wrapper。退出要求旧root三callback及专属imports=0，aggregation/dashboard/validation schema、PASS/exit、artifact path、observe-only/no-production与完整CLI tree parity。
- 2026-07-11：G2.3C operations slice `COMPLETE`，G2.3继续。最终operations/direct/contract=`111 passed`、contract/policy=`21 passed`、architecture-fitness=`177 passed`、Ruff/standalone contract PASS；generated=`806 modules / 1,114 tests / 861 direct writers / 0 violations`。Legacy root净减3 callback、1 parser、3目录常量及209行，runtime 993 leaf不变。
- 2026-07-11：G2.3C operations slice实现完成并进入`VALIDATING`。三个callback、一个cadence parser和三项目录常量已迁至canonical `operations.py`，`cli_direct`直接调用canonical实现，legacy root无callback/parser/constant wrapper。Root=`35,763 -> 35,554 lines / 1,040 -> 1,036 functions / 987 -> 984 decorators`；完整tree hash不变，operations/direct/contract=`111 passed`，等待compatibility、generated inventory与architecture门禁。
- 2026-07-11：G2.3C选择`ops dry-run/report/validate`三个callback、cadence parser与三项输出目录常量。Canonical operations module成为常量/CLI唯一实现；dynamic-shadow legacy caller直接导入validation目录常量，`cli_direct`全部转canonical。退出要求旧root callback/parser/constant定义=0，调度图、observe-only/no-production、validation exit、artifact path和tree parity。
- 2026-07-11：G2.3B data-quality slice `COMPLETE`，G2.3继续。最终domain/direct=`44 passed`、contract/policy=`20 passed`、architecture-fitness=`176 passed`、Ruff/standalone contract PASS；generated=`805 modules / 1,114 tests / 861 direct writers / 0 violations`。Legacy root再净减3 callback与176行，runtime 993 leaf不变。
- 2026-07-11：G2.3B data-quality slice实现完成并进入`VALIDATING`。三个callback迁到canonical `data_quality.py`，未新增helper；`cli_direct` report/validate已直接调用canonical callback，旧root无wrapper。Root=`35,939 -> 35,763 lines / 1,043 -> 1,040 functions / 990 -> 987 decorators`；完整tree/node hash不变，DQ/direct-dispatch/contract=`44 passed`、Ruff/standalone contract PASS，等待compatibility与architecture门禁。
- 2026-07-11：G2.3B选择`data-quality price-freshness/report/validate`三个callback。三者只依赖canonical `parse_date/resolve_date`，无需新增helper；`cli_direct`中的report/validate调用必须同步转向canonical callback，禁止旧root wrapper。退出要求旧root三callback/decorator=0、DQ BLOCKED/FAIL仍exit 1、tree/report path/schema/help及相关tests parity。
- 2026-07-11：G2.3A data/features slice `COMPLETE`，G2.3继续。最终domain focused=`72 passed`、contract/policy=`19 passed`、architecture-fitness=`175 passed`、Ruff/standalone contract PASS；generated=`804 modules / 1,114 tests / 861 direct writers / 0 violations`。Legacy root已净减3 callback、3 helper及106行，完整runtime leaf仍为993。下一slice继续从operations/reporting边界选择可独立迁移group。
- 2026-07-11：G2.3首个data/features callback slice实现完成并进入`VALIDATING`。三个callback迁到canonical `data.py`，三个共享helper迁到`common.py`并由legacy root别名复用；旧root对应定义均为0，compatibility alias已直接改用canonical callback而非保留wrapper。Root=`36,045 -> 35,939 lines / 1,049 -> 1,043 functions / 993 -> 990 decorators`；tree/node hash不变，data/feature/alias相关=`72 passed`、Ruff/standalone contract PASS，DQ与exit语义未改。
- 2026-07-11：G2.3首个callback slice选择`data`与`features`。范围固定为`data ingest/validate`、`features build`三个callback，以及它们共用的date/latest/satellite解析helper；helper将进入canonical `common.py`并由legacy root别名复用，禁止复制。迁移目标是legacy root中这三个callback/decorator和三个helper定义reachability=0，同时保持DQ调用、path/default/help/exit、tree hash与ETF data/domain tests。
- 2026-07-11：G2.2 `COMPLETE`，G2.3 data/operations/reporting group迁移进入`IN_PROGRESS`。最终ETF consumer=`341 passed`、contract/policy focused=`18 passed`、architecture-fitness=`174 passed`、Ruff与standalone contract PASS；generated=`802 modules / 1,114 tests / 861 direct writers / 0 violations`。Registration shell未移动callback或改变tree；下一slice从低投资解释风险group开始，每个callback slice均须旧root reachability=0且tree/help/domain parity。
- 2026-07-11：G2.2 registration shell实现完成并进入`VALIDATING`。291个Typer app定义与290条`add_typer`关系完整迁到`interfaces.cli.etf_portfolio.registration`；legacy root中两类语句均为0，所有1,049个callback与993个decorator原地保留。Root=`37,604 -> 36,045 lines / 1,579,334 -> 1,520,029 bytes`；tree/counts/1,284 node contracts逐项相同，tree hash保持`afa0760c...1cec2`。`data --help`与`portfolio --help`固定宽度输出hash已冻结；25个现有ETF CLI consumer文件=`341 passed`、contract characterization=`6 passed`、Ruff/standalone baseline PASS，等待compatibility与architecture正式门禁。
- 2026-07-11：G2.1 `COMPLETE`，G2.2 registration shell进入`IN_PROGRESS`。最终focused=`15 passed`（contract characterization自身3项）、architecture-fitness=`171 passed`、Ruff与standalone baseline validate PASS；generated inventory=`798 modules / 1,114 tests / 861 direct writers / 0 violations`。Golden contract冻结`41 root / 291 group / 993 leaf / 1,284 nodes / 0 duplicate`，ETF CLI runtime与source bytes未改。G2.2先分离app composition/registration，不迁策略计算。
- 2026-07-11：G2.1实现完成并进入`VALIDATING`。Canonical scanner按解析后的Click tree冻结1284个node（291 group + 993 leaf），同时核对Typer raw registration count=993，duplicate=0；每个node hash覆盖path、group行为、help与parameter contract，但有意排除callback module/location以允许纯结构迁移。Tracked tree hash=`afa0760c...1cec2`；duplicate、option/default/help drift characterization与baseline parity=`3 passed`，Ruff PASS。等待generated manifests、compatibility和architecture正式门禁。
- 2026-07-11：G2.1进入`IN_PROGRESS`。只读runtime审计得到`41 root groups / 291 groups / 993 leaf commands / 0 duplicate / 993 unique paths`；下一步实现deterministic contract scanner与tracked golden baseline，尚未改动ETF CLI runtime。
