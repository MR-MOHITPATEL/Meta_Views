@echo off
echo Starting Meta Ads Updater Streamlit App...
cd /d "%~dp0"
call ..\.venv\Scripts\activate.bat
streamlit run meta_updater_app.py
pause
