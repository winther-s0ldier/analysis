@echo off
REM Start all agent A2A servers + main app in A2A multi-server mode
REM Run from Z:\ADK directory

set USE_A2A_MULTISERVER=true
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
call Z:\ADK\.venv\Scripts\activate.bat

echo Starting agent servers...
start "profiler"    cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent profiler    --port 8001"
start "discovery"   cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent discovery   --port 8002"
start "coder"       cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent coder       --port 8003"
start "synthesis"   cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent synthesis   --port 8004"
start "critic"      cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent critic      --port 8005"
start "dag_builder" cmd /k "cd /d Z:\ADK && call .venv\Scripts\activate.bat && set USE_A2A_MULTISERVER=true && set PYTHONIOENCODING=utf-8 && python agent_servers/server_base.py --agent dag_builder --port 8006"

echo Waiting 8 seconds for agent servers to start...
timeout /t 8 /nobreak >nul

echo Starting main app (port 8000)...
python -X utf8 main.py
