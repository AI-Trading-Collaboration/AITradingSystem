# Paid Data Due Diligence Owner Packet

## 1. 为什么现在考虑 paid data due diligence？

Post-2085 结果显示 free features 和 participation proxy 有诊断价值，但不能成为
model-ready breadth。当前 reopen gate 的核心缺口不是模型参数，而是真实历史成分、
daily membership、delisted securities 和 survivorship-free universe。

## 2. 为什么不是直接买 Norgate？

Norgate 是最强候选，但公开资料不是 PIT proof。购买前必须确认 license、trial、
Python access、daily membership query、local cache、coverage start、delisted handling
和成本。没有 owner approval 前不得购买或下载 paid data。

## 3. 免费数据和 proxy 已经证明了什么？

免费数据证明 rates/liquidity、volatility 和 proxy breadth 方向值得诊断；ETF ratio proxy
说明 participation 可能有增量。但 proxy 不能回答历史某日真实成分是谁，不能证明退出
成分，也不能解决 survivorship bias。

## 4. True breadth 可能解决什么？

True breadth 能支持 Nasdaq-100 breadth、QQQ-like participation、concentration score
和 semiconductor participation。它可能直接补足 current first-layer blocker：narrow rally、
false risk-off、2023+ dependency 和 beta/TQQQ dependency 诊断。

## 5. 各 vendor 优缺点

- Norgate：最接近 true breadth 需求；缺点是成本、license 和 Python/member query 需要 trial。
- FMP：已有 key，但 holdings endpoint 当前返回 402；即使升级也需确认 known-at 和 delisted membership。
- EODHD：可作为条件替代；membership 和 PIT semantics 尚未验证。
- QuantConnect / AlgoSeek：生态能力强但本地导出和 repo 集成成本不清楚。
- Tiingo / Marketstack / Yahoo / price-only：适合价格交叉验证，不能解决 membership。

## 6. 成本是否可控？

当前只能说 Norgate trial 值得 owner review。是否可控取决于 trial 条款、个人研究授权、
local cache 权利和取消灵活性。任何试用或购买必须人工批准。

## 7. 推荐 trial / 不 trial 的理由

推荐：`NORGATE_TRIAL_RECOMMENDED`。理由是它最可能一次性满足 historical constituents、
delisted、daily membership 和 Python/local workflow。FMP 当前 plan blocker 明确，EODHD
保留条件备选，QuantConnect deferred。

## 8. 不购买是否会阻塞后续 first-layer research？

会阻塞 model-ready true breadth 路径，但不阻塞继续保持 diagnostic-only research。
在没有 true breadth 前，不应恢复 first-layer、v4、promotion 或 forward diagnostic。
