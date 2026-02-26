#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
LOG_DIR="${ROOT_DIR}/logs"
NATS_HOST="127.0.0.1"
NATS_PORT="4222"
NATS_URL="nats://${NATS_HOST}:${NATS_PORT}"
EXECUTOR_COUNT="${EXECUTOR_COUNT:-2}"
AUTO_BUILD="${AUTO_BUILD:-1}"

mkdir -p "${LOG_DIR}"

if [[ "${AUTO_BUILD}" == "1" ]]; then
  cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" >/dev/null
  cmake --build "${BUILD_DIR}" -j >/dev/null
fi

is_nats_up() {
  if command -v nc >/dev/null 2>&1; then
    nc -z "${NATS_HOST}" "${NATS_PORT}" >/dev/null 2>&1
  else
    timeout 1 bash -c "</dev/tcp/${NATS_HOST}/${NATS_PORT}" >/dev/null 2>&1
  fi
}

NATS_PID=""
if ! is_nats_up; then
  echo "[run_all] nats-server is not running; starting local instance"
  nats-server -a "${NATS_HOST}" -p "${NATS_PORT}" >"${LOG_DIR}/nats-server.log" 2>&1 &
  NATS_PID=$!
  sleep 0.5
fi

if ! is_nats_up; then
  echo "[run_all] unable to connect to nats-server at ${NATS_URL}"
  exit 1
fi

declare -a PIDS=()

start_agent() {
  local name="$1"
  shift
  local bin="${BUILD_DIR}/${name}"
  if [[ ! -x "${bin}" ]]; then
    echo "[run_all] missing binary: ${bin}"
    exit 1
  fi
  echo "[run_all] starting ${name}"
  "${bin}" "$@" >"${LOG_DIR}/${name}.log" 2>&1 &
  PIDS+=("$!")
}

cleanup() {
  set +e
  echo "[run_all] stopping agents..."
  for pid in "${PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
  if [[ -n "${NATS_PID}" ]]; then
    kill "${NATS_PID}" 2>/dev/null || true
  fi
  wait 2>/dev/null || true
}

trap cleanup INT TERM EXIT

start_agent directory_agent --nats "${NATS_URL}"
start_agent system_monitor --nats "${NATS_URL}" --interval-ms 500
start_agent lease_authority --nats "${NATS_URL}" --mem-factor 0.8

for i in $(seq 1 "${EXECUTOR_COUNT}"); do
  start_agent executor_agent --nats "${NATS_URL}" --id "${i}"
done

start_agent planner_agent --nats "${NATS_URL}"
start_agent task_manager --nats "${NATS_URL}"
start_agent metrics_collector --nats "${NATS_URL}" --report-file "${ROOT_DIR}/derived/metrics_report.txt"

echo "[run_all] all agents are running (logs in ${LOG_DIR})"
wait
