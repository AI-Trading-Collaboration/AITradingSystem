# Growth Tilt Owner Mapping Inventory

本报告是只读 baseline contract inventory，不运行 PIT replay、backtest 或 scoring，也不代表 owner 已完成 preregistration。

```json
{
  "as_of": "2026-07-10",
  "baseline_binding_status": "BLOCKED_NO_GROWTH_TILT_BASELINE_RUNTIME_BINDING",
  "baseline_config_id": "base_overlay_veto_policy_schema_v1",
  "do_not_de_risk_pass": false,
  "m2_eligible_candidate_count": 0,
  "m2_mapping_status": "BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING",
  "mapping_blocker_codes": [
    "A_BASELINE_RECOVERY_PERSISTENCE_CONTRACT_UNRESOLVED",
    "A_CALLABLE_PIT_APPROVED_RECOVERY_SIGNAL_UNRESOLVED",
    "A_COMPLETE_HARD_VETO_SET_UNRESOLVED",
    "A_GOVERNED_TRANSITION_SCOPE_UNRESOLVED",
    "A_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED",
    "B_COMPLETE_HARD_VETO_SET_UNRESOLVED",
    "B_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION_UNRESOLVED",
    "B_GOVERNED_TRANSITION_SCOPE_UNRESOLVED",
    "B_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED"
  ],
  "market_regime": "ai_after_chatgpt",
  "next_route": "TRADING-2438M1C_GROWTH_TILT_BASELINE_RUNTIME_MAPPING_INVENTORY_AND_OWNER_PREREGISTRATION",
  "owner_mapping_ready_count": 0,
  "owner_mapping_required_count": 2,
  "required_hard_veto_ids": [
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto"
  ],
  "risk_on_veto_pass": true,
  "status": "GROWTH_TILT_OWNER_MAPPING_INVENTORY_READY_OWNER_REVIEW_REQUIRED",
  "strict_validation_error_count": 0,
  "unresolved_hard_veto_ids": [
    "event_risk_veto",
    "risk_off_veto",
    "trend_break_veto"
  ]
}
```

## Candidate Mapping Readiness

```json
[
  {
    "blocker_codes": [
      "A_CALLABLE_PIT_APPROVED_RECOVERY_SIGNAL_UNRESOLVED",
      "A_BASELINE_RECOVERY_PERSISTENCE_CONTRACT_UNRESOLVED",
      "A_COMPLETE_HARD_VETO_SET_UNRESOLVED",
      "A_GOVERNED_TRANSITION_SCOPE_UNRESOLVED",
      "A_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED"
    ],
    "candidate_id": "recovery_reentry_speedup_guard",
    "eligible_signal_ids": [],
    "mapping_ready": false,
    "required_hard_veto_ids": [
      "risk_off_veto",
      "volatility_veto",
      "event_risk_veto",
      "trend_break_veto",
      "tqqq_veto"
    ],
    "unresolved_hard_veto_ids": [
      "event_risk_veto",
      "risk_off_veto",
      "trend_break_veto"
    ]
  },
  {
    "blocker_codes": [
      "B_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION_UNRESOLVED",
      "B_COMPLETE_HARD_VETO_SET_UNRESOLVED",
      "B_GOVERNED_TRANSITION_SCOPE_UNRESOLVED",
      "B_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED"
    ],
    "candidate_id": "false_risk_off_confirmation_relaxation",
    "eligible_soft_confirmation_ids": [],
    "mapping_ready": false,
    "required_hard_veto_ids": [
      "risk_off_veto",
      "volatility_veto",
      "event_risk_veto",
      "trend_break_veto",
      "tqqq_veto"
    ],
    "unresolved_hard_veto_ids": [
      "event_risk_veto",
      "risk_off_veto",
      "trend_break_veto"
    ]
  }
]
```

## 结论

`re_risk_allowed_probability` 有实际生成路径，但 do-not-de-risk channel 未通过最终 selection，且没有 growth-tilt-bound persistence contract。仓库也没有可证明为 neutral/constructive -> defensive 唯一触发原因的 callable PIT soft confirmation。因此 A/B 仍不得进入 M2。

完整 signal、confirmation、veto、regime、exposure 和 transition sample 见同目录 JSON。
