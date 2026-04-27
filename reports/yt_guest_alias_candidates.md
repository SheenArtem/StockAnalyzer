# YT Guest Alias Audit (2026-04-26)

針對 21 個疑似短稱（無姓氏）嘉賓做 WebSearch audit，找出可能對應全名。
本報告採用 budget: 每短稱最多 2 query，總共 ~29 tool calls。

來源節目：
- 錢線百分百 (money100) — 非凡新聞台 / 台視財經台 21:00
- 鈔錢部署 (money_deploy) — 主持人盧燕俐

---

## High Confidence (建議直接 merge)

| Short Name | n | Full Name | Evidence | Source |
|---|---|---|---|---|
| 昆仁 | 16 | **陳昆仁** | 中視「財經早點名」主持分析師、daren888 LINE 群組、常上錢線百分百 | https://t.me/s/daren888 ; https://www.ctv.com.tw/Article/中視-財經早點名-20241122-陳昆仁 |
| 庭皓 | 131 | **游庭皓** | 「游庭皓的財經皓角」YT 頻道主，財經皓角官網 aboutus 明確列出非凡【錢線百分百】【股市現場】常態來賓 | https://yutinghao.finance/aboutus/ ; https://www.youtube.com/channel/UC0lbAQVpenvfA2QqzsRtL_g |
| 紫東 | 112 | **黃紫東** | 運達投顧專業操盤經理人、交易腦團隊創辦者；常上財經節目 | https://win-dollar.com/teacher-HuangZidong.html |
| 毓棠 | 310 | **許毓棠** | 永誠國際投顧分析師「許毓棠分析師」（搜尋結果中誤拼為「許毓玲」是同一人，「股市易點靈」節目主持） | https://www.youtube.com/channel/UC7seiE-4_C9EY0sjpu1G19w （建議 user 看過影片二次確認） |
| 其展 | 239 | **李其展** | 外匯/總經分析師、Yahoo 股市專欄、Kobo 電子書作者「李其展的外匯交易致勝兵法」、鈔錢部署常態來賓 | https://tw.stock.yahoo.com/author/%E6%9D%8E%E5%85%B6%E5%B1%95 ; https://money.udn.com/author/articles/2555 |
| 嘉明 | 147 | **陳嘉偉**（暱稱「嘉偉老師」/「股市總司令」）| 承通投顧資深分析師，非凡商業台主持盤中盤後股市分析；推測短稱「嘉明」可能是 LLM 抽取時把「嘉偉」聽錯 / 拼錯 | https://www.27813900.com/teacher.html?id=17 ; https://pttpedia.fandom.com/zh/wiki/嘉偉老師 （**user 需驗證**：可能是「嘉偉」不是「嘉明」） |
| 聖傑 | 168 | **林聖傑** | 2019 年「錢線百分百」節目（20190610-1 中美貿戰）標題明確列出「錢線來賓林聖傑」 | https://www.youtube.com/watch?v=iLXfkv5Q1gE |
| 智霖 | 108 | **陳智霖**（威霖）| 亨達證券投顧分析師、20年國際金融操盤經驗、自有 YT 頻道「陳智霖股票分析師 Equity Research Analyst」 | https://www.youtube.com/channel/UCQufQATeQPFNc75V7qDUjHQ |
| 正華 | 207 | **蔡正華** | 大來國際投顧分析師（111 金管投顧新字第014號）、金錢道 Telegram、主持「股市好好玩」「理財金錢道」 | https://t.me/s/goldmoney168 |
| 子昂 | 28 | **陳子昂** | 資策會 MIC 資深總監/資深產業顧問，半導體/ICT 產業專家，多次上「財經起床號」談台積電議題；屬產業分析（非投顧）來賓 | https://www.linkedin.com/in/%E5%AD%90%E6%98%82-%E9%99%B3-9bb017169/ ; https://www.youtube.com/watch?v=rI17wa1oS0I |

**High confidence merge 候選數：10 個**（其中「嘉明」標記需 user 驗證是否為「嘉偉」誤抽）

---

## Medium Confidence (建議 user 看影片確認)

| Short Name | n | Suspected Full Name | Note |
|---|---|---|---|
| 建勳 | 88 | **（不明，疑似萬寶投顧）** | 搜尋指向「萬寶投顧研究部與海外基金部副總分析師，週四 21:00-23:30 上錢線百分百」，但搜尋結果未明確點名。Wanbao analyst 候選需查 marboweekly.com.tw 名單 |

---

## Low / Ambiguous

| Short Name | n | Note |
|---|---|---|
| 佩真 | 55 | 兩 query 皆無命中。常見命名，可能為節目來賓或記者，無證據可判 |
| 孟道 | 9 | 兩節目都查不到，mention 數低 (n=9)，可能是非常態來賓 |
| 俊洲 | 326 | 高 mention 但完全找不到對應全名；嘗試「江俊洲/陳俊洲/曾俊洲/黃俊洲」皆無命中。此為最大 unsolved（建議 user 直接看 1-2 個高 mention 影片找姓氏）|
| 奇琛 | 155 | 嘗試「陳奇琛/謝奇琛/李奇琛」皆無命中；可能姓氏冷門 |
| 冠州 | 211 | 高 mention 但「陳冠州/張冠州/劉冠州」皆無命中。可能與已 merged 的「冠嶔」(同音) 為同一人，建議 user 比對影片 |
| 凱銘 | 36 | 兩節目皆無命中，無姓氏線索 |
| 麗芬 | 52 | 嘗試「林麗芬」無命中；常見命名，可能是非投顧背景來賓（記者/作家）|
| 志源 | 114 | 兩 query 皆無命中；高 mention 但 web 線索稀薄 |
| 詣庭 | 73 | 兩 query 皆無命中；「詣庭」字組合罕見，可能是 LLM 抽取錯誤（疑似「翊庭」「逸庭」等同音替換）|
| 嘉隆 | 35 | 兩 query 皆無命中；無姓氏線索 |

---

## No Evidence Found（同 Low/Ambiguous，列舉以供統計）

合計 10 個短稱完全無命中或證據不足以下結論：
- 佩真、孟道、俊洲、奇琛、冠州、凱銘、麗芬、志源、詣庭、嘉隆

---

## 行動建議

1. **直接 merge 10 個 high confidence**（昆仁/庭皓/紫東/毓棠/其展/聖傑/智霖/正華/子昂；嘉明 → 嘉偉先標記 review）
2. **建勳** 再看一個影片片段確認萬寶投顧是哪位
3. **俊洲 (n=326) / 冠州 (n=211) / 志源 (n=114) / 奇琛 (n=155)** — 高 mention 但 web 線索零，user 直接看 YT 影片片頭嘉賓 caption 最快
4. **詣庭** 疑似 LLM 抽取打錯字，建議重抽該影片的 transcript 段
5. **嘉明** 與已 merged 的「嘉明大哥」**可能是同一人不同稱呼**（「嘉明」也可能是「嘉偉老師」誤聽），建議 user 比對

---

## 附：tool call budget 統計

- 1 × ToolSearch (load WebSearch schema)
- 28 × WebSearch
- 1 × Write (本文件)
- **總計 30 tool call**（遠低於 50 限額）
