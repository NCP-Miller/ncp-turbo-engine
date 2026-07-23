@echo off
REM Double-click this file to start the Zombie Fund Screener on Windows.
cd /d "%~dp0"
pip install -r requirements.txt
streamlit run Today.py
pause
