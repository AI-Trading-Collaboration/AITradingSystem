# Confirmation-Only Actual-Path Validation

最后更新：2026-06-30

|candidate|eligible|alignment|delta|false_confirmation|false_warning|status|
|---|---:|---:|---:|---:|---:|---|
|`baseline_plus_trend_structure_scope_narrowed_confirmation_v1`|2820|0.497163|-0.010745|812|319|`CONFIRMATION_SCOPE_VALIDATED_REJECT_RECOMMENDED`|

`confirmation_only` candidate 不是 primary directional signal；本报告只验证 active confirmation records 是否比 inactive reference 更能确认趋势方向。
当前 active scope worse than inactive reference，false confirmation / false warning cost 不支持继续按当前 confirmation scope 推进。
