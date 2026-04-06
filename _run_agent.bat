@echo off
set USE_A2A_MULTISERVER=true
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
cd /d "%~1"
call "%~1\.venv\Scripts\activate.bat"
python agent_servers/server_base.py --agent %~2 --port %~3
