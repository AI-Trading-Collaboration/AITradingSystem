# TRADING-2531 v2 external validation request (not authorization)

当前任务只修复离线合同，不授权 QuantConnect 动作。未来若 Owner 决定验证，必须新建 governed execution task，并绑定以下 exact hashes：

- policy file SHA-256: `cea137e0cb17b1c9594c359926015189f6fcfc2f472c4b6db72357d67a5d0cf5`
- policy canonical SHA-256: `adc2e9cc0c889b814a97a5b8c4841c0890ef73c27dc07eddddc98ed2bed26f22`
- contract content SHA-256: `f3c3918dd5dfd6fc1c6e84b63471c652d34090c9d50fab25d77dc58f9190b378`
- contract canonical SHA-256: `97557122d50f6a82fe68f57286f7008bbe8bbdb511886f62f936d9fc1b6bb7e4`
- project code LF SHA-256: `0665a759a9db9bcae100133da9dd950e7f66597d4f19d00f01b26afb6a478f45`
- predecessor evidence: `d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792`
- predecessor Results SHA-256: `2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7`

边界：最多一次 project mutation、一次 zero-order Cloud backtest、0 orders、0 fills；仍禁止 raw rows、logs-as-data、Object Store、DQ/PIT/strategy/trading conclusion。
