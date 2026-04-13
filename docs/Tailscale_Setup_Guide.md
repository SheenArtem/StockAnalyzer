# Tailscale 遠端存取完整設定教學

> 目標：從手機遠端使用 Claude Code CLI + 瀏覽 StockAnalyzer 網頁（Streamlit）

---

## 目錄

1. [架構總覽](#架構總覽)
2. [Step 1：Windows 安裝 Tailscale](#step-1windows-安裝-tailscale)
3. [Step 2：手機安裝 Tailscale](#step-2手機安裝-tailscale)
4. [Step 3：暴露 Streamlit 到 Tailnet](#step-3暴露-streamlit-到-tailnet)
5. [Step 4：設定 OpenSSH Server（遠端 Claude Code）](#step-4設定-openssh-server遠端-claude-code)
6. [Step 5：手機 SSH 連線](#step-5手機-ssh-連線)
7. [Step 6：防火牆安全加固](#step-6防火牆安全加固)
8. [日常使用流程](#日常使用流程)
9. [疑難排解](#疑難排解)

---

## 架構總覽

```
手機 (Tailscale VPN)
  │
  ├─ 瀏覽器 → https://your-pc.tailnet.ts.net → Streamlit (port 8501)
  │
  └─ SSH Client (Termius) → your-pc:22 → bash → claude (Claude Code CLI)
  
Windows 電腦 (Tailscale VPN)
  ├─ Tailscale serve → 代理 localhost:8501
  ├─ OpenSSH Server → port 22
  └─ Streamlit → localhost:8501
```

所有流量走 Tailscale 加密隧道（WireGuard），不對公網開放任何端口。

---

## Step 1：Windows 安裝 Tailscale

### 1.1 下載安裝

**方法 A — 官網下載：**
- 前往 https://tailscale.com/download/windows
- 下載 MSI 安裝檔，執行安裝

**方法 B — winget：**
```powershell
winget install Tailscale.Tailscale
```

### 1.2 登入

1. 安裝完成後，系統匣（右下角）出現 Tailscale 圖示
2. 點擊圖示 → **Log in**
3. 瀏覽器開啟登入頁面，選擇帳號登入（Google / Microsoft / GitHub）
4. 授權後裝置自動加入你的 tailnet
5. 系統匣圖示變為已連線（藍色）

### 1.3 確認連線

```powershell
# 查看本機 Tailscale IP
tailscale ip

# 查看 tailnet 內所有裝置
tailscale status
```

### 1.4 啟用 MagicDNS（通常預設已開）

1. 前往 https://login.tailscale.com/admin/dns
2. 確認 **MagicDNS** 為 Enabled
3. 記下你的 tailnet 名稱（例如 `tail1234b.ts.net`）
4. 可在 Machines 頁面重新命名你的電腦（例如改成 `home-pc`）

> 之後你的電腦 URL 就是 `home-pc.tail1234b.ts.net`

---

## Step 2：手機安裝 Tailscale

### iOS

1. App Store 搜尋 **Tailscale** → 安裝
2. 開啟 App → **Sign in** → 用與電腦**相同帳號**登入
3. 系統提示安裝 VPN 設定檔 → **允許**
4. 連線成功，手機加入 tailnet

### Android

1. Google Play 搜尋 **Tailscale** → 安裝
2. 開啟 App → 同帳號登入
3. 允許 VPN 權限
4. 連線成功

### 確認

手機上開啟 Tailscale App，應該能看到你的 Windows 電腦在裝置列表中。

> **Note：** Tailscale 使用 split tunnel，只有 tailnet 流量走 VPN，一般上網不受影響。

---

## Step 3：暴露 Streamlit 到 Tailnet

### 3.1 啟動 Streamlit

```bash
cd C:\GIT\StockAnalyzer
streamlit run app.py
# 預設在 localhost:8501
```

### 3.2 用 tailscale serve 暴露

開啟**系統管理員權限**的終端（PowerShell 或 Git Bash）：

```bash
tailscale serve --bg --https 443 http://localhost:8501
```

- `--bg`：背景執行
- `--https 443`：透過 HTTPS 443 port 對外服務
- Tailscale 自動簽發 Let's Encrypt 憑證

### 3.3 確認狀態

```bash
tailscale serve status
```

### 3.4 手機瀏覽

在手機瀏覽器輸入：

```
https://home-pc.tail1234b.ts.net
```

（替換成你的實際機器名稱和 tailnet 名稱）

即可看到 StockAnalyzer 介面。

### 3.5 停止/重設

```bash
# 停止 serve
tailscale serve reset
```

### 3.6 開機自動 serve（選用）

建立 `start_serve.bat`：

```bat
@echo off
REM Wait for Tailscale to connect
timeout /t 15 /nobreak >nul
"C:\Program Files\Tailscale\tailscale.exe" serve --bg --https 443 http://localhost:8501
```

放入 Windows 啟動資料夾：
- `Win+R` → `shell:startup` → 把 `start_serve.bat` 的捷徑放進去

---

## Step 4：設定 OpenSSH Server（遠端 Claude Code）

Windows 不支援 Tailscale SSH server，需使用 Windows 原生 OpenSSH Server。

### 4.1 安裝 OpenSSH Server

**方法 A — GUI：**
1. 設定 → 系統 → 選用功能 → 新增功能
2. 搜尋 **OpenSSH Server** → 安裝

**方法 B — PowerShell（管理員）：**
```powershell
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
```

### 4.2 啟動服務

```powershell
# 啟動 SSH 服務
Start-Service sshd

# 設定開機自動啟動
Set-Service -Name sshd -StartupType Automatic
```

### 4.3 將預設 Shell 改為 Git Bash

SSH 連入 Windows 預設是 CMD，改成 bash 才能順暢使用 Claude Code：

```powershell
# PowerShell（管理員）
New-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell -Value "C:\Program Files\Git\bin\bash.exe" -PropertyType String -Force
```

> 如果偏好 PowerShell 7：
> ```powershell
> New-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell -Value "C:\Program Files\PowerShell\7\pwsh.exe" -PropertyType String -Force
> ```

### 4.4 測試本機 SSH

```bash
ssh localhost
# 輸入 Windows 帳號密碼，確認能登入
```

---

## Step 5：手機 SSH 連線

### 5.1 推薦 SSH Client

| 平台 | App | 說明 |
|------|-----|------|
| iOS | **Termius** | 免費，介面好，支援 snippet 和多裝置同步 |
| iOS | **Blink Shell** | 付費（~$16），專業級鍵盤體驗，支援 Mosh |
| Android | **Termius** | 同 iOS 版 |
| Android | **JuiceSSH** | 免費，Android 老牌 SSH client |

### 5.2 設定連線

在 SSH Client 中新增連線：

| 欄位 | 值 |
|------|-----|
| Host | `home-pc.tail1234b.ts.net`（你的 Tailscale 機器名稱） |
| Port | `22` |
| Username | 你的 Windows 使用者名稱 |
| Auth | 密碼 或 SSH Key |

### 5.3 設定 SSH Key（推薦，免輸密碼）

**在手機 SSH Client 中產生 Key Pair，取得公鑰後：**

在 Windows 電腦上：

```powershell
# 建立 authorized_keys（一般使用者）
mkdir $env:USERPROFILE\.ssh -Force
notepad $env:USERPROFILE\.ssh\authorized_keys
# 貼上手機產生的公鑰，存檔

# 如果你的帳號是 Administrators 群組，要用這個檔案：
notepad C:\ProgramData\ssh\administrators_authorized_keys
```

> **重要：** Windows OpenSSH 對 Administrators 群組的使用者，不讀 `~/.ssh/authorized_keys`，而是讀 `C:\ProgramData\ssh\administrators_authorized_keys`。

設定權限：
```powershell
# 確保 administrators_authorized_keys 權限正確
icacls "C:\ProgramData\ssh\administrators_authorized_keys" /inheritance:r /grant "SYSTEM:F" /grant "BUILTIN\Administrators:F"
```

### 5.4 連線使用 Claude Code

SSH 連入後：

```bash
cd /c/GIT/StockAnalyzer
claude
```

即可在手機上使用 Claude Code CLI。

---

## Step 6：防火牆安全加固

### 6.1 SSH 只允許 Tailscale 網段

不要讓 SSH port 22 對所有網路開放，限制只有 Tailscale 內網（100.64.0.0/10）可連入：

```powershell
# PowerShell（管理員）

# 移除預設的 SSH 全開規則
Remove-NetFirewallRule -Name *OpenSSH* -ErrorAction SilentlyContinue

# 建立只允許 Tailscale 網段的規則
New-NetFirewallRule -Name "SSH-Tailscale-Only" `
    -DisplayName "OpenSSH Server (Tailscale Only)" `
    -Enabled True `
    -Direction Inbound `
    -Protocol TCP `
    -Action Allow `
    -LocalPort 22 `
    -RemoteAddress 100.64.0.0/10
```

### 6.2 確認防火牆規則

```powershell
Get-NetFirewallRule -Name "SSH-Tailscale-Only" | Format-List
```

### 6.3 不需要的事

- **不需要** port forwarding（路由器不用設定）
- **不需要** DDNS
- **不需要** 對公網開放任何 port
- Tailscale 透過 DERP relay 穿透 NAT，大部分情況直連（direct connection）

---

## 日常使用流程

### 出門前（電腦端，一次性設定完就不用管）

確認以下服務在運行：
- [x] Tailscale 已連線（系統匣藍色圖示）
- [x] Streamlit 在跑（或設定開機自啟）
- [x] `tailscale serve` 在跑（或放在 startup 資料夾）
- [x] OpenSSH Server 已啟動（已設定自動啟動）

### 在外面（手機端）

**看盤 / 看掃描結果：**
1. 確認手機 Tailscale 已連線
2. 瀏覽器開啟 `https://home-pc.tail1234b.ts.net`
3. 使用 StockAnalyzer 所有功能

**使用 Claude Code：**
1. 開啟 Termius（或其他 SSH Client）
2. 連線到 `home-pc.tail1234b.ts.net`
3. `cd /c/GIT/StockAnalyzer && claude`

---

## 疑難排解

### tailscale serve 失敗

```
error: certificate not available
```
→ 前往 Admin Console → DNS → 啟用 **HTTPS Certificates**

### SSH 連不上

1. 確認 sshd 服務在跑：
   ```powershell
   Get-Service sshd
   ```
2. 確認防火牆規則：
   ```powershell
   Get-NetFirewallRule -Name "*SSH*" | Format-Table Name, Enabled, Action
   ```
3. 確認兩端 Tailscale 都已連線：
   ```bash
   tailscale status
   ```

### 手機 Tailscale 常斷線（iOS）

- iOS 設定 → VPN → Tailscale → 保持連線
- 或每次使用前先開啟 Tailscale App 確認連線

### SSH 進去是 CMD 不是 bash

確認已設定 DefaultShell：
```powershell
Get-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell
```

### Claude Code 畫面跑版

手機螢幕窄，建議：
- 手機橫放使用
- 在 Termius 中調小字體
- 使用 `/compact` 模式（如果 Claude Code 支援）

---

## 附錄：`tailscale serve` vs `tailscale funnel`

| | `tailscale serve` | `tailscale funnel` |
|---|---|---|
| 誰能存取 | 只有你的 tailnet 裝置 | 任何人（公開網路） |
| 安全性 | 高 | 低（公開暴露） |
| 適用場景 | 個人遠端存取 | Demo 分享給外人 |
| 本教學使用 | **是** | 否 |

> **建議：** 個人使用一律用 `tailscale serve`，不需要 funnel。
