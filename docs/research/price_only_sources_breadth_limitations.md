# Price-Only Sources Breadth Limitations

YAML：`inputs/research_reviews/price_only_sources_breadth_limitations.yaml`

## 结论

Tiingo、Marketstack、Yahoo、FMP price 和 EODHD price 可以支持价格交叉验证、ETF proxy
price、constituent price quality check，但不能解决 historical constituents。

因此 price-only source 不得标记为：

- true breadth source；
- model-ready breadth；
- Nasdaq-100 daily membership；
- survivorship-free constituent universe；
- promotion evidence。

## 可接受用途

- QQQ / QQQE / RSP / SMH / SOXX / XLK 等 ETF proxy price cross-check。
- 已有 membership source 之后的 constituent price join cross-check。
- 数据质量、调整价格、缺口、重复键和异常值验证。

Price-only source 可以提高后续 feature builder 的价格质量，但不能替代 membership
source。
