#!/bin/bash
set -e

echo "[*] Building project..."
cmake -B build -S .
cmake --build build -j$(nproc)

echo "[*] Starting manager..."
./build/src/manager &
sleep 2

echo "[*] Starting scheduler..."
./build/src/scheduler &
sleep 1

echo "[*] Starting agents..."
python3 agents/system_monitor.py &
sleep 1

echo "[*] Running create_agent..."
python3 agents/create_agent.py 128 &
python3 agents/create_agent.py 120000