@echo off
set "P=%~dp0"
if "%P:~-1%"=="\" set "P=%P:~0,-1%"

set USE_A2A_MULTISERVER=true
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

call "%P%\.venv\Scripts\activate.bat"

if not exist "%P%\logs" mkdir "%P%\logs"
if not exist "%P%\output" mkdir "%P%\output"
if not exist "%P%\uploads" mkdir "%P%\uploads"

start "profiler"    cmd /k ""%P%\_run_agent.bat" "%P%" profiler    8001"
start "discovery"   cmd /k ""%P%\_run_agent.bat" "%P%" discovery   8002"
start "coder"       cmd /k ""%P%\_run_agent.bat" "%P%" coder       8003"
start "synthesis"   cmd /k ""%P%\_run_agent.bat" "%P%" synthesis   8004"
start "critic"      cmd /k ""%P%\_run_agent.bat" "%P%" critic      8005"
start "dag_builder" cmd /k ""%P%\_run_agent.bat" "%P%" dag_builder 8006"

timeout /t 10 /nobreak >nul

start "frontend" cmd /k "cd /d "%P%\frontend" && npm run dev"

timeout /t 3 /nobreak >nul

echo Backend  : http://localhost:8000
echo Frontend : http://localhost:6173
echo Agents   : ports 8001-8006
echo API Docs : http://localhost:8000/docs

cd /d "%P%"
python -X utf8 main.py
