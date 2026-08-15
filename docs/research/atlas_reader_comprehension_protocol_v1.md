# Atlas 读者理解验收协议 V1（准备态）

本协议只定义可重放的记录结构，不授权招募、执行或签署人类理解验收。当前状态固定为
`PENDING_OWNER_POLICY`；participant profile、sample、scenario assignment、critical error、pass threshold、
retest、reviewer roster、signature authority 与 accessibility coverage 均由 Project Owner 另行批准。

## 测试对象

每轮只绑定一份 exact HTML。记录 HTML SHA-256、source commit、manifest SHA-256、reader projection
contract SHA-256、viewport、browser、OS 与 assistive technology。任一 identity field 变化即结束当前轮；
旧回答不能并入新页面结果。

## 无引导任务

参与者需用自己的话回答：当前研究问题和最大阻塞是什么；为什么沿当前路径推进；哪些只是工程能力、
哪些是研究证据；目前不能推出什么；下一责任方和下一合法动作是什么；是否允许形成策略结论、启动
engine 或下单；数据、证据和页面分别截至何时；相对上一 snapshot 改变了什么。

观察员逐字记录回答、首次点击/滚动路径、首次误解、不认识的术语、对 PASS 的误读、证据查找过程、
判断改变，以及 desktop/mobile/keyboard/screen-reader 阻塞点。不得在任务过程中解释页面内容。

## 独立编码与停止条件

至少两名 Owner 指定 reviewer 独立按 canonical truth 把回答标为 `CORRECT / PARTIALLY_CORRECT /
INCORRECT / UNANSWERED / PROHIBITED_INFERENCE_INDUCED`，并保留 disagreement log。类别只记录事实；
何种组合构成 PASS 仍是待批准政策，任何脚本都不得自动升级。

缺少 Owner policy、两名 reviewer、canonical truth、exact identity，或页面 bytes 中途变化时，立即停止。
本协议不执行研究、回测、外部平台、production 或 broker action。
