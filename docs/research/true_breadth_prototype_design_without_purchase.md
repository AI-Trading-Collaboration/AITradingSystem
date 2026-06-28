# True Breadth Prototype Design Without Purchase

本设计只描述数据源通过后如何实现，不购买、不下载 paid data、不恢复研究。

## 模块设计

1. Daily membership snapshot builder
   - 输入 vendor membership API / export。
   - 输出 `date, universe_id, symbol, member, weight, effective_date, known_at, source_id`。
   - 每批记录 provider、endpoint、request parameters、download timestamp、row count 和 checksum。

2. Constituent price joiner
   - 用已通过 `aits validate-data` 的价格缓存或 vendor-approved constituent prices。
   - 校验 duplicate keys、missing prices、split/adjustment policy 和 delisted rows。

3. Breadth feature builder
   - `nasdaq100_breadth_v1`: 20d/60d positive return ratio、above MA ratio、new high ratio、outperform QQQ ratio。
   - `qqq_like_participation_v1`: equal-weight return、cap-weight return、median return、participation expansion/contraction。

4. Concentration feature builder
   - top 5 / top 10 contribution share。
   - median vs index return spread。
   - top-heavy rally / narrow rally flags。

5. Semiconductor participation
   - 需要 vendor sector / industry metadata 或独立 semi universe。
   - 输出 semi breadth、equal-weight return、SMH/SOXX confirmation。

6. PIT audit and coverage matrix
   - 检查 `known_at <= decision_at`。
   - 禁止 current constituents backfill。
   - 标记 PIT warning 为 diagnostic-only。

7. Family ablation
   - 只有 contract pass 后才允许进入 family-level diagnostic ablation。
   - 仍不直接进入 promotion、paper-shadow、production 或 broker。
