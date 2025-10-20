#!/bin/bash
set -e

echo "[*] Building manager..."
cmake -B build -S .
cmake --build build -j$(nproc)

echo "[*] Starting agents..."
python3 agents/system_monitor.py &
python3 agents/scheduler.py &
sleep 1

echo "[*] Starting manager..."
./build/manager &
sleep 2

echo "[*] Running create_agent..."
python3 agents/create_agent.py
