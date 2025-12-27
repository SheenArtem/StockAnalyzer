@echo off
echo Installing dependencies...
pip install -r requirements.txt

echo.
echo Starting StockAnalyzer...
streamlit run app.py
pause
