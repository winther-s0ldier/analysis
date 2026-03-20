#!/bin/bash
# Start all agent A2A servers + main app in A2A multi-server mode
# Run from ADK directory: ./start_agents.sh

export USE_A2A_MULTISERVER=true

echo "Starting agent servers..."
python agent_servers/server_base.py --agent profiler    --port 8001 &
python agent_servers/server_base.py --agent discovery   --port 8002 &
python agent_servers/server_base.py --agent coder       --port 8003 &
python agent_servers/server_base.py --agent synthesis   --port 8004 &
python agent_servers/server_base.py --agent critic      --port 8005 &
python agent_servers/server_base.py --agent dag_builder --port 8006 &

echo "Waiting 5 seconds for agent servers to start..."
sleep 5

echo "Starting main app (port 8000)..."
python main.py
