# tools/_archive — 已歸檔工具腳本

本目錄收納**已驗收完成、暫不在 active path 上**的腳本，移過來避免 `tools/`
根目錄持續膨脹（清理前 187 檔，惡化中）。**全部腳本仍在 git 內，需要重跑時
直接執行即可**。

歸檔不等於廢棄。每個子目錄的 README 條列腳本用途與 verdict 出處，方便
未來 cold pickup。

## 子目錄

| 目錄 | 內容 | 數量 | 何時用 |
|---|---|---|---|
| `vf/` | Validation framework studies (因子 IC / walk-forward / portfolio backtest) | 52 | 重新驗某個 factor 或想看 study 完整 code |
| `manual_tests/` | 手寫一次性 test 腳本（非 pytest） | 11 | debug 個案 / smoke check 特定功能 |
| `verify/` | 一次性驗證腳本（非 active 排程用） | 7 | 驗某次重構結果（已有結論者勿重跑） |
| `ui_tests/` | Streamlit UI 早期 phase test（已被新 UI 取代） | 4 | UI regression debug |

`tools/verify_scan_stages.py` **未** 歸檔（scanner.bat 排程實際使用）。

## Path 修正

歸檔後檔案位於 `tools/_archive/{subdir}/X.py` (depth 3 from REPO_ROOT)，
原本 `Path(__file__).resolve().parent.parent` 會指錯。已批次改成
`parents[3]`。如新增歸檔檔案，記得手動改。

## 不要做的事

1. **不要刪除**：archive 用意是「git history + 檔案系統都查得到」，git rm
   雖然可救但繞一圈。
2. **不要再 import**：active code 若需要 archived script 的邏輯，請把該邏輯
   重構提取到正式模組（如 `tools/lib/`），不要 `from tools._archive.X import Y`。
3. **不要在這裡寫新 study**：新驗證寫在 `tools/` 根目錄，commit 後 verdict
   寫到 `reports/`，**驗收滿一個月**才搬進 archive。
