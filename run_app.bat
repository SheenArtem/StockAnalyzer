@echo off
echo Installing dependencies...
pip install -r requirements.txt

REM Rotate previous log: app.log -> app_prev.log (overwrite older _prev)
if exist app.log (
    if exist app_prev.log del app_prev.log
    ren app.log app_prev.log
)

echo.
echo Starting StockAnalyzer... (log: app.log)
echo Open http://localhost:8501 in your browser manually.
set PYTHONUNBUFFERED=1
streamlit run app.py >> app.log 2>&1
pause
