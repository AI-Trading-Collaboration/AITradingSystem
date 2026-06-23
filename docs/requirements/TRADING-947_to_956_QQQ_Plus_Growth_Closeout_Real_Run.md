# TRADING-947 to 956 QQQ-Plus Growth Closeout and Real-Run Validation

## 背景

TRADING-933 to 946 已新增 QQQ-plus growth challenger research 线，用来评估是否存在
历史收益超过 `100_qqq`、且回撤、波动、换手、TQQQ 路径依赖仍可接受的增长候选。
本批任务做工程收口、真实 CLI suite、数据 warning 影响复核、候选结果解释、beta/exposure
归因、period/drawdown 稳定性复核、forward-aging watchlist gate 和 owner 决策包。

本批不改变 growth challenger 的安全定位：

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`

`equal_risk_qqq_sgov` 继续作为 defensive primary；QQQ-plus growth challenger 只作为
research-only 增长候选研究线，不替代 defensive primary，不进入 paper-shadow，不接 broker，
不恢复 tail-risk fallback，不启动 LEAPS / Wheel。

## 阶段拆解

|任务|阶段|当前状态|验收标准|
|---|---|---|---|
|TRADING-947|worktree attribution|VALIDATING|输出 `qqq_plus_growth_worktree_attribution.json/md`，列出 modified/untracked、是否属于 TRADING-933～946、是否本轮前 dirty、是否 mixed hunk、safe commit candidate list；无法归因时保持 owner review。|
|TRADING-948|safe commit and push|VALIDATING|基于 TRADING-947 结论，只提交可明确归因的 TRADING-933～946 文件；若已在 `origin` Git 历史中，记录 commit/push 证据；不得混入 pre-existing dirty changes。|
|TRADING-949|real CLI suite|VALIDATING|真实运行 14 个 QQQ-plus growth CLI，输出 `qqq_plus_growth_real_cli_suite_summary.json/md`，汇总 status、warnings、blockers、candidate_count、top_candidate、data_quality_status 和 safety fields。|
|TRADING-950|data warning impact review|VALIDATING|检查 QQQ/TQQQ/SGOV/Marketstack/fallback/sample_count/cache reproducibility warning，输出 warning list、affected/unaffected strategy ids、result confidence 和是否需要数据修复。|
|TRADING-951|candidate result summary|VALIDATING|按 return、QQQ edge、Calmar、Sharpe、drawdown constraint、lowest drawdown、non-dominated、rejected 分组输出候选总结。|
|TRADING-952|growth edge materiality review|VALIDATING|评估 annual return edge、Calmar/Sharpe edge、drawdown/turnover/TQQQ path dependency/complexity penalty 和 net growth edge score。|
|TRADING-953|beta and exposure attribution|VALIDATING|拆解 effective QQQ beta、leverage、TQQQ/SGOV weight、QQQ beta return、TQQQ overlay、SGOV carry、rebalance、timing、cash/leverage drag。|
|TRADING-954|period and drawdown validation|VALIDATING|合并 period split 与 drawdown replay，回答是否多数 period 跑赢 QQQ、是否 AI rally concentrated、2022 risk、risk-on/off speed。|
|TRADING-955|forward-aging watchlist gate|VALIDATING|最多允许 1 个 growth challenger 进入 research-only forward-aging watchlist；若条件不足输出 blockers；safety fields 必须保持 false/none。|
|TRADING-956|owner decision pack|VALIDATING|生成 `docs/research/qqq_plus_growth_owner_decision_pack.md` 和 JSON owner pack，回答是否继续增长研究、是否加入 watchlist、是否保持全部安全阻断。|

## 归因约束

本批开始时工作区已有未提交 TRADING-923～932 forward-aging launch 变更。TRADING-947/948
必须把这些变更标为 pre-existing dirty changes，不能把它们混入 TRADING-933～946 safe
commit 结论，也不能通过 `git add .`、`git commit -am` 或 unrelated untracked 文件绕过归因。

若本批需要修改共享文档或 registry，后续提交必须可审计地区分本批增量与 pre-existing dirty
changes；无法安全拆分时保持 owner review，不强行提交。

## 进展记录

- 2026-06-24: 新增需求拆解并进入 `IN_PROGRESS`。初始只读检查显示
  `HEAD=358f3ddd` / `origin/main` 已包含 QQQ outperformance growth challenger research；
  当前未提交工作区变更集中在 TRADING-923～932 forward-aging launch 线和一个 untracked
  923 requirement doc，需在 TRADING-947 attribution 中明确隔离。
- 2026-06-24: 947～956 实现并转入 `VALIDATING`。归因结论：TRADING-933～946
  已在 `358f3ddd` / `origin/main`，本轮无需再提交；当前 dirty worktree 属于
  pre-existing 923～932 与本批 closeout docs/registry/catalog/system flow。真实运行 14 个
  QQQ-plus growth CLI 全部 exit 0，suite status=`QQQ_PLUS_REAL_RUN_WARN`，data quality
  为 `PASS_WITH_WARNINGS`（主源 adjustment ratio warning 与 Marketstack overlap warning）。
  候选总结显示存在历史收益超过 QQQ 的 vol-targeted growth 候选，但
  edge materiality=`GROWTH_EDGE_WEAK`、period/drawdown=`PERIOD_DRAWDOWN_INCONCLUSIVE`、
  watchlist gate=`NO_GROWTH_WATCHLIST_CANDIDATE`，owner recommendation=
  `KEEP_GROWTH_RESEARCH_ONLY`。新增 closeout generator、report registry entries、
  artifact catalog row、system flow paragraph、owner docs；安全边界仍为
  `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
