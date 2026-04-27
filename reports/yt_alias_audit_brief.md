# YT Guest Alias A-方案 Audit Brief

> 給 19:30 quota reset 後的 agent 看的 task brief。

## 背景
- 兩個台灣財經 YouTube 節目：「錢線百分百」（money100）+「鈔錢部署」（money_deploy）
- 已從 194 集 VTT 字幕 extract guest 姓名，但 LLM 抽取不一致（全名 vs 短稱 vs 藝名混用）
- 目前 65 個 guest（已 alias merged: 連乾文/志誠/明翰/奇芬/冠嶔/建承/昱衡/俊敏/慶龍/嘉明大哥/蕙慈老師/蜀芳老師/林忠哥/奎國老師/博傑/明哲）
- 完整 list: `reports/yt_guest_credibility.md` 第 7 行起

## 任務
對 65 個 guest 中**尚未確認 alias 的**做 WebSearch，找：
1. 短稱（無姓 2 字）對應的全名（昆仁/子昂/佩真/正華/庭皓/紫東/孟道/毓棠/俊洲/奇琛/冠州/凱銘/麗芬/其展/嘉明/聖傑/智霖/志源/詣庭/嘉隆/建勳）
2. 藝名 → 本名（如「OO 老師」「OO 大哥」等）
3. 主持人（不算 guest，例：祝華已排除）

## Search query 建議
- `"{guest}" 錢線百分百 嘉賓`
- `"{guest}" 投顧` 或 `"{guest}" 老師`
- 短稱：`"OO{短稱}" 投顧` 找姓氏

## 輸出
寫到 `reports/yt_guest_alias_candidates.md`：

```markdown
# YT Guest Alias Audit (2026-04-26)

## Confirmed aliases (high confidence)
| Short Name | Suspected Full Name | Evidence | Confidence |
|---|---|---|---|

## Ambiguous (multiple candidates)

## Likely hosts (排除)

## No evidence found

## Already-merged (skip)
```

不要動程式碼。只寫候選清單。完成後給 user review。
