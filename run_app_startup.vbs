' ============================================================
'  StockAnalyzer App Autostart - hidden launcher
'
'  Runs run_app_startup.bat with a hidden window so logon does
'  not flash a black console. The Streamlit server then lives
'  in that hidden window for the whole session.
'
'  Run() args:  0     = hidden window (no console shown)
'               False = do not wait; this script exits at once,
'                       so the scheduled task completes instantly
'                       while the server keeps running detached.
'
'  Called by the scheduled task "StockAnalyzer App Autostart".
' ============================================================
CreateObject("WScript.Shell").Run "cmd /c ""C:\GIT\StockAnalyzer\run_app_startup.bat""", 0, False
