# Paid Data Due Diligence Closeout

Final status：`NORGATE_TRIAL_RECOMMENDED`

## 完成项

- 建立 `true_breadth_data_contract_v1`。
- 建立 paid breadth vendor registry。
- 完成 Norgate、FMP、EODHD、QuantConnect / AlgoSeek 和 price-only sources 分层评估。
- 生成 vendor scoring matrix。
- 生成 value-of-information estimate。
- 生成 prototype design without purchase。
- 生成 paid data trial decision gate。
- 生成 owner decision packet。
- 新增 guardrail tests。

## Vendor scoring matrix

`outputs/research_trends/paid_data_due_diligence/vendor_scoring_matrix.csv`

当前最高分是 Norgate：84 / 100，recommendation=`TRIAL_RECOMMENDED`。
该 recommendation 只允许进入 owner review，不允许购买。

## Trial decision

`NORGATE_TRIAL_RECOMMENDED`

Owner manual approval 是 trial、purchase、provider upgrade、sample download、
local cache 和任何 derived feature 的硬前置条件。

## True breadth contract status

`TRUE_BREADTH_CONTRACT_ACTIVE`

当前没有任何 vendor output 已通过 contract，因此 `model_ready_breadth_available_now=false`，
first-layer 仍不得 reopen。

## Promotion status

继续固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `dynamic_promotion_status=BLOCKED`

## Remaining blockers

- Norgate license / local cache / Python membership query 仍需 owner-approved trial 验证。
- FMP holdings 当前 key 返回 HTTP 402，且 PIT fields 未验证。
- EODHD membership / known-at / revision policy 未验证。
- QuantConnect / AlgoSeek local export 和 membership availability 未验证。
- Price-only sources 不能解决 historical constituents。

## Validation

- `python -m ruff check src tests`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests/test_paid_data_due_diligence.py`：7 passed
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_reopen_gate.py tests/test_free_feature_pit_contract.py tests/test_first_layer_channel_archive_contract.py tests/test_research_audit_metadata.py tests/test_research_artifact_governance.py`：46 passed
- `python -m pytest -n 16 --dist loadfile tests/test_task_register_consistency.py tests/test_report_index.py tests/test_documentation_contract.py`：27 passed
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：193 passed
- Runtime artifact：`outputs/validation_runtime/contract-validation_20260628T090805Z/test_runtime_summary.json`
