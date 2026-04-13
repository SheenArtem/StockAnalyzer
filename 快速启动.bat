@echo off
chcp 65001 >nul
title StockAnalyzer 股票分析系統

echo.
echo ============================================
echo    StockAnalyzer 股票分析系統
echo ============================================
echo.

:: 检查 Python 是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ 錯誤：未找到 Python！
    echo.
    echo 請先安裝 Python 3.9 或更高版本
    echo 下載地址：https://www.python.org/downloads/
    pause
    exit /b 1
)

echo ✅ Python 版本：
python --version
echo.

:: 检查依赖是否安装
echo 📦 檢查依賴包...
python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo ⚠️  首次運行，需要安裝依賴包（約需1-2分鐘）
    echo.
    echo 正在安裝依賴包...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo.
        echo ❌ 依賴包安裝失敗！
        echo 請檢查網絡連接或手動運行：pip install -r requirements.txt
        pause
        exit /b 1
    )
    echo ✅ 依賴包安裝完成！
) else (
    echo ✅ 依賴包已安裝
)

echo.
echo ============================================
echo    正在啟動應用...
echo ============================================
echo.
echo 🚀 應用將在瀏覽器中自動打開
echo 📍 本地地址：http://localhost:8501
echo.
echo 💡 提示：
echo    - 按 Ctrl+C 可停止應用
echo    - 關閉此視窗也會停止應用
echo.
echo ============================================
echo.

:: 启动 Streamlit
streamlit run app.py --server.headless=true

:: 如果 Streamlit 异常退出
if errorlevel 1 (
    echo.
    echo ❌ 應用啟動失敗！
    echo.
    echo 可能的原因：
    echo   1. 端口 8501 已被占用
    echo   2. 缺少某些依賴包
    echo.
    echo 解決方案：
    echo   1. 關閉其他可能占用 8501 端口的程序
    echo   2. 或運行：streamlit run app.py --server.port 8502
    echo.
)

pause
