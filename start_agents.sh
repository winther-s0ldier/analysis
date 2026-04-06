#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export USE_A2A_MULTISERVER=true
export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1

cd "$PROJECT_DIR"

# Ensure runtime directories exist
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/output"
mkdir -p "$PROJECT_DIR/uploads"
mkdir -p "$PROJECT_DIR/data"

if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
    source "$PROJECT_DIR/.venv/bin/activate"
elif [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
fi

PYTHON=$(command -v python3 || command -v python)

# Build frontend if dist is missing
if [ ! -d "$PROJECT_DIR/frontend/dist" ]; then
    echo "Building frontend..."
    cd "$PROJECT_DIR/frontend" && npm install && npm run build
    cd "$PROJECT_DIR"
fi

# Start all agent servers in background
$PYTHON agent_servers/server_base.py --agent profiler    --port 8001 >> "$PROJECT_DIR/logs/profiler.log"    2>&1 &
$PYTHON agent_servers/server_base.py --agent discovery   --port 8002 >> "$PROJECT_DIR/logs/discovery.log"   2>&1 &
$PYTHON agent_servers/server_base.py --agent coder       --port 8003 >> "$PROJECT_DIR/logs/coder.log"       2>&1 &
$PYTHON agent_servers/server_base.py --agent synthesis   --port 8004 >> "$PROJECT_DIR/logs/synthesis.log"   2>&1 &
$PYTHON agent_servers/server_base.py --agent critic      --port 8005 >> "$PROJECT_DIR/logs/critic.log"      2>&1 &
$PYTHON agent_servers/server_base.py --agent dag_builder --port 8006 >> "$PROJECT_DIR/logs/dag_builder.log" 2>&1 &

echo "Waiting 10s for agents to initialize..."
sleep 10

echo ""
echo "Backend  : http://localhost:8000"
echo "Agents   : ports 8001-8006"
echo "API Docs : http://localhost:8000/docs"
echo ""

$PYTHON -X utf8 main.py
