# First-Layer Calibration Probe Dependency Update

- 状态：`FIRST_LAYER_CALIBRATION_DEPENDENCY_UPDATED_PROMOTION_BLOCKED`
- frozen probe registry：`dynamic_second_layer_probe_registry_v2`
- probe count：`8`
- 后续 first-layer label / feature / model / threshold calibration 必须使用本 registry。
- 除非开启新的 second-layer-only research round，不得在 first-layer 校准过程中修改 probe weights 或 entry rules。
- promotion_allowed：`False`；paper_shadow_allowed：`False`；production_allowed：`False`；broker_action：`none`。
