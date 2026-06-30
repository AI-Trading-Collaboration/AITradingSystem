# Risk-Cap Forward Observe Evidence Collection Spec

- observe_mode: `forward_observe_only`
- minimum_observe_days: `60`
- minimum_active_trigger_count: `10`
- minimum_review_windows: `4`

Daily evidence 记录 risk-cap trigger、triggered assets/horizons、risk-cap intensity、source signal records、market context、data quality 和 trigger interpretation。Allowed action 固定为 `observe_only`。

Weekly review 汇总 trigger count、post-trigger forward path、drawdown、realized volatility、false risk-cap cases、missed stress cases 和 evidence accumulation status。

若 60 天内 active trigger 过少，不直接判定无效，应进入 evidence accumulation extension。
