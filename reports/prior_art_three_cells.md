# Prior-Art 文獻校準 — 三格價量訊號 (2026-06-08)

> Phase B 產出。配 `reports/validation_audit_checklist.md` 一起當 Phase C 對抗證偽的輸入。
> 三份獨立文獻校準 (格1 λ / 格2 量條件化 / 格3 流動性 regime) 的彙整。

## 0. 收斂的元結論 (最重要)

三格各自的文獻都指向**同一個故事**,而且這故事就是本專案自己的 RVOL/liq_50m 教訓:

> **「訊號在 ranking 上可能存在,但在小型/低流動股扣成本後蒸發」** — 這是 Avramov-Chordia-Goyal (2006) 對個股報酬自相關的大樣本判決,逐字:*"not possible to profit from this short-run predictability"*。

因此**三格共用同一條 make-or-break 檢定**:
1. **增量 rank-IC**：訊號對 `{已實現波動、|報酬|、短期反轉、RVOL、size}` 正交化後是否還有 IC。死掉 = artifact。
2. **淨成本存活**：在 `liq_50m` 可交易池扣成本後是否還正。
3. **多重檢定**：Harvey-Liu-Zhu t>3.0 (非 2.0)；Bailey-López de Prado deflated Sharpe；實務常把 backtest Sharpe 直接砍 50%。
4. **leave-one-crisis-out**：剔 2008-09 / 2020 / 2022 後是否崩。

---

## 1. 格1 — Kyle λ / Amihud 量價彈性「背離」

**Verdict：無學術先例 + 最像的同類已死。**

- 單檔時序 λ timing 學界幾乎沒做、沒成功。主流全橫截面溢酬;Amihud 唯一時序結果是**大盤級**。
- 「背離=吸籌/力竭」= OBV/A-D 民間 TA,**零學術驗證**。signed building block (CLV) 是 TA 構件不是學術因子。
- **殺手**：VPIN (唯一被嚴謹檢驗的 signed-flow-from-bars) 死了 — 預測力「**控制已實現波動率後完全消失**」(Andersen-Bondarenko)。CLV×量 結構同型 = 可能只是把要預測的價格路徑重新編碼。
- **量級天花板**：合法效應 ~0.5-1.2%/月、集中微型股、橫截面。單檔月 IC > ~0.05 或淨 Sharpe > ~1.0 = **可疑**。
- **方向陷阱**：嚴謹低流動效應是**反轉**不是續勢。若「續勢有效」查是不是微型股動量(會反轉)。
- **建構**：Amihud `|ret|/成交額` + Abdi-Ranaldo CHL 交叉檢核;**within-stock z-score** 殺非平穩;winsorize 1/99;**V=0 顯式剔除 + Amihud day-count scaling**(別讓 pandas 靜默丟);penny/漲跌停/停牌日剔除。
- 文獻：Amihud 2002 / Andersen-Bondarenko 2013 (VPIN) / Pastor-Stambaugh 2003 / Abdi-Ranaldo 2017 / Corwin-Schultz 2012。

## 2. 格2 — 量條件化反轉 vs 續勢

**Verdict：機制真實,但符號不穩 + 扣成本後死;台股若活只在大型股+法人流。**

- 正典 LMSW (2002)：`R_{t+1}=C0+C1·R_t+C2·(V_t·R_t)`,V=去趨勢 log turnover(減 200d MA)。**C2>0 高量續勢(informed) / C2<0 高量反轉(liquidity)**,是橫截面、個股特性。
- C2 量級：高 info-asym 股 +0.0355,低 +0.0028(反轉);橫截面 b=+0.0557 t=15.9 R²=10.2% — 但**剔 bid-ask bounce 後 R² 塌到 ~3-4%**(小型股一大塊是微結構假象)。
- **符號不穩定是核心**:CGW/ACG 報反轉、Cooper 報續勢、ACG 明說「符號取決於看哪個 size 段 + 週頻 vs 月頻會翻」。**raw pooled「高量續勢」不是穩定 stylized fact**。
- **台股 (Hsieh-Hu 2010)**：續勢出現在**大型股**(法人集中)、由**外資+投信流量**載動,不是小型股 — 與 LMSW 反轉。Korea (Kang 2025)：要**按投資人身份拆 + 用市值normalize conviction**,法人 Q4 +12% CAR/50d、散戶 ~0 noise。
- **決定性 (ACG 2006)**：可預測性集中在高週轉**低流動小型股**(=台股 mid/small-cap),**扣成本後死**。最大型股 0.37%/週 vs 0.52% round-trip 仍淨負。Lewellen 2000：個股月頻無可靠 own-autocorr。
- **baseline**：若台股為真,gross rank-IC 個位數 %、mid/small-cap 淨 Sharpe ≈ 0。唯一可能存活=**大型股+法人流的 ranking-tilt**。
- **建構**(最乾淨首版)：`score = z(prior_return) · z(detrended_log_turnover)` 橫截面;formation lag ≥1 日避 bounce。
- 文獻：LMSW 2002 / Avramov-Chordia-Goyal 2006 / Lee-Swaminathan 2000 / Hsieh-Hu 2010 (TW) / Kang 2025 (KR) / Lewellen 2000。

## 3. 格3 — 流動性當 regime 狀態變數

**Verdict：擱置 lead「太好以致可疑」且可能符號錯;定位只能 informational / risk-off。**

- **符號可能反**：橫截面高 turnover → 未來報酬**低**(Lee-Swaminathan glamour;流動性溢酬同向)。lead 是「volatile 下買高 turnover(D10>D1)加分」= **疑似與文獻雙重相反**。
- **被推翻的 puzzle**：「turnover 波動度預測報酬」= CSA (2001),後證**對 aggregation period/觀察頻率不穩、volume 偏態有估計偏誤、clean 高頻法符號翻正**(Flora 2025) = RVOL/ATR「9.50→4.63 反轉」同型。
- **內部矛盾(本專案自己的)**：`vf_turnover_summary.md` 已標「注意矛盾」— 線性 IC 負(IR -0.48)但 volatile decile D10>D1 正。prior-art 解釋這正是 turnover 雙重身份/CSA artifact 特徵。**Part A 幾近預判:正向 volatile spread 是脆弱側,clean panel 會塌或翻號。**
- **量級**：128%/yr spread = 已發表合法 premium 的 **15-60 倍**(Pastor-Stambaugh 靜態 7.5%/yr;Cao et al. 對沖基金流動性擇時 4-5.5%/yr;因子擇時 ~2% p.a.)。
- **有效 N = 獨立 regime episode 數(25 年約 5-8 段)**,不是 stock-month → SOP-14「N<30 informational」直接咬死。
- **建構**：別用 raw turnover 當 regime(雙重身份+偏態);用 Amihud 市場聚合 + P-S reversal。gate **不對稱** — 只在極端低流動/高波動時 **risk-off 減碼**,不順境加碼選股(Cao et al. 不對稱發現 + 對齊本專案 macro informational 定位)。用**連續 state interaction** 不用 regime dummy(episode 太少)。turnover 預處理 log+winsorize+去趨勢。
- 文獻：Chordia-Subrahmanyam-Anshuman 2001 / Flora 2025 / Lee-Swaminathan 2000 / Cao-Chen-Liang-Lo 2013 / Pastor-Stambaugh 2003 / Hong-Stein 2003 / Asness 2017。

---

## 4. 對 Phase C 的指令

backtest 出數字後,**每格按上面 verdict 的 pre-registered 預期對照**:
- 格1：先跑增量 IC(正交化 vol/ret/reversal/size);若月 IC>0.05 或淨 Sharpe>1.0 → 加倍懷疑非慶祝。
- 格2：先拆 large vs small cap 看符號翻不翻;必須在 liq_50m 池贏純 RVOL,且查是否被 短期反轉+RVOL 吃掉。
- 格3：先查 lead 符號(買高 turnover?);turnover 波動度算法換 aggregation period 看翻不翻;leave-one-crisis-out;regime gate 必跑 portfolio daily-allocation sim(SOP-10~14)。
