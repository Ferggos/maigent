#!/usr/bin/env bash
#
# run_graph_b_cpu.sh — проверка графика Б на РАСТУЩЕЙ CPU-нагрузке.
#
# CPU выгоднее памяти: загрузка растёт сразу (не нужен swap/PSI-страдание),
# а SET_CPU_MAX (cpu.max) реально режет загрузку цели → CPU-риск падает.
#
# Растущая нагрузка: cpu_ramp.py добавляет молотилки по одной до target_workers.
# Две фазы (baseline без planner / overlay с planner) в зарегистрированной внешней cgroup.
#
# ТРЕБУЕТ ROOT. Запуск: sudo ./run_graph_b_cpu.sh
# Параметры:
#   CPU_RAMP        путь к cpu_ramp.py (по умолчанию рядом со скриптом или /home/$USER/cpu_ramp.py)
#   TARGET_WORKERS  до скольки молотилок расти (по умолчанию 7; у тебя 8 ядер)
#   STEP_DELAY      пауза между добавлением молотилки, сек (по умолчанию 5)
#   HOLD_SEC        удержание на пике, сек (по умолчанию 60)
#   RUN_SECONDS     авто = рост + hold + запас
#
set -uo pipefail

REAL_USER="${SUDO_USER:-$(id -un)}"
REAL_UID="$(id -u "${REAL_USER}")"
REAL_GID="$(id -g "${REAL_USER}")"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}"
[[ ! -d "${ROOT_DIR}/build" && -d "${ROOT_DIR}/../build" ]] && ROOT_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

BUILD_DIR="${ROOT_DIR}/build"
LOG_DIR="${ROOT_DIR}/logs"
RESULT_DIR="${ROOT_DIR}/experiment_results"
NATS_URL="nats://127.0.0.1:4222"
CG_ROOT="/sys/fs/cgroup"

CG_NAME="${CG_NAME:-maigent_test}"
CG_PARENT="${CG_ROOT}/${CG_NAME}"
CG_LEAF="${CG_PARENT}/load"
CG_REL="${CG_NAME}/load"

CPU_RAMP="${CPU_RAMP:-${SCRIPT_DIR}/cpu_ramp.py}"

TARGET_WORKERS="${TARGET_WORKERS:-14}"
STEP_DELAY="${STEP_DELAY:-5}"
HOLD_SEC="${HOLD_SEC:-60}"
RAMP_TIME=$(( TARGET_WORKERS * STEP_DELAY ))
RUN_SECONDS="${RUN_SECONDS:-$(( RAMP_TIME + HOLD_SEC + 15 ))}"

mkdir -p "${RESULT_DIR}" "${LOG_DIR}" "${ROOT_DIR}/reports"
if [[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]]; then
  chown -R "${REAL_UID}:${REAL_GID}" "${LOG_DIR}" "${RESULT_DIR}" "${ROOT_DIR}/reports" 2>/dev/null || true
fi

AGENT_NAMES=(task_manager_agent task_executor_agent planner_agent system_monitor_agent
             actuator_agent lease_authority_agent directory_agent metrics_collector_agent)

say()  { echo -e "\n\033[1;36m[exp]\033[0m $*"; }
err()  { echo -e "\033[1;31m[exp ERROR]\033[0m $*" >&2; }
warn() { echo -e "\033[1;33m[exp WARN]\033[0m $*" >&2; }
ok()   { echo -e "\033[1;32m[exp OK]\033[0m $*"; }

as_user() {
  if [[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]]; then sudo -u "${REAL_USER}" "$@"; else "$@"; fi
}

prepare_cgroup() {
  echo "+memory +cpu" > "${CG_ROOT}/cgroup.subtree_control" 2>/dev/null || true
  mkdir -p "${CG_PARENT}" 2>/dev/null || true
  echo "+memory +cpu" > "${CG_PARENT}/cgroup.subtree_control" 2>/dev/null || true
  mkdir -p "${CG_LEAF}" 2>/dev/null || true
  chown -R "${REAL_UID}:${REAL_GID}" "${CG_PARENT}" 2>/dev/null || true
  [[ -f "${CG_LEAF}/cpu.max" ]] || { err "лист без cpu.max — нужен sudo и +cpu контроллер"; return 1; }
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null || true
  ok "cgroup готова: ${CG_LEAF} (cpu.max=$(cat "${CG_LEAF}/cpu.max" 2>/dev/null))"
}

kill_load() {
  [[ -f "${CG_LEAF}/cgroup.procs" ]] && {
    while read -r p; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null || true; done < "${CG_LEAF}/cgroup.procs"
  }
  pkill -9 -f "cpu_ramp.py" 2>/dev/null || true
  sleep 1
}

cleanup_procs() {
  for pid in $(pgrep -f "task_executor_agent" 2>/dev/null); do pkill -9 -P "${pid}" 2>/dev/null || true; done
  for n in "${AGENT_NAMES[@]}"; do pkill -9 -f "${n}" 2>/dev/null || true; done
  pkill -9 -f "external_cgroup_client" 2>/dev/null || true
  kill_load
}

cleanup_cgroup() {
  kill_load
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null || true
  rmdir "${CG_LEAF}" 2>/dev/null || true
  rmdir "${CG_PARENT}" 2>/dev/null || true
}

ensure_nats() {
  if ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/4222") 2>/dev/null; then
    as_user nats-server -a 127.0.0.1 -p 4222 </dev/null >"${LOG_DIR}/nats-server.log" 2>&1 &
    sleep 1
  fi
}

start_overlay() {
  local mode="$1"
  ensure_nats
  : > "${LOG_DIR}/runtime_system_monitor_agent.log" 2>/dev/null || true
  : > "${LOG_DIR}/runtime_actuator_agent.log" 2>/dev/null || true
  : > "${LOG_DIR}/runtime_planner_agent.log" 2>/dev/null || true

  say "старт надстройки (${mode})"
  as_user "${BUILD_DIR}/directory_agent"        --nats "${NATS_URL}" </dev/null >"${LOG_DIR}/runtime_directory_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/lease_authority_agent"  --nats "${NATS_URL}" </dev/null >"${LOG_DIR}/runtime_lease_authority_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/system_monitor_agent"   --nats "${NATS_URL}" --interval-ms 500 --cgroup-root "${CG_ROOT}" \
       </dev/null >"${LOG_DIR}/runtime_system_monitor_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/actuator_agent"         --nats "${NATS_URL}" --cgroup-root "${CG_ROOT}" \
       </dev/null >"${LOG_DIR}/runtime_actuator_agent.log" 2>&1 &
  if [[ "${mode}" == "with_planner" ]]; then
    as_user "${BUILD_DIR}/planner_agent"        --nats "${NATS_URL}" </dev/null >"${LOG_DIR}/runtime_planner_agent.log" 2>&1 &
  fi
  as_user "${BUILD_DIR}/metrics_collector_agent" --nats "${NATS_URL}" --reports-root "${ROOT_DIR}/reports" \
       </dev/null >"${LOG_DIR}/runtime_metrics_collector_agent.log" 2>&1 &
  sleep 4
}

start_ramp() {
  [[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py: ${CPU_RAMP}"; return 1; }
  say "растущая CPU-нагрузка: workers→${TARGET_WORKERS} delay=${STEP_DELAY}s hold=${HOLD_SEC}s (рост ~${RAMP_TIME}s)"
  rm -f "${LOG_DIR}/cpu_ramp.log" 2>/dev/null || true
  setsid bash -c "
    echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null
    exec python3 '${CPU_RAMP}' --target-workers ${TARGET_WORKERS} --step-delay ${STEP_DELAY} --hold-sec ${HOLD_SEC}
  " </dev/null >"${LOG_DIR}/cpu_ramp.log" 2>&1 &
  sleep 3
  if ! pgrep -f "cpu_ramp.py" >/dev/null 2>&1; then
    err "cpu_ramp не запустился:"; cat "${LOG_DIR}/cpu_ramp.log" 2>/dev/null | head; return 1
  fi
  ok "нагрузка пошла (процессов в cgroup: $(cat "${CG_LEAF}/cgroup.procs" 2>/dev/null | wc -l))"
}

register_cgroup() {
  say "регистрирую cgroup '${CG_REL}' (allow-control)"
  as_user "${BUILD_DIR}/external_cgroup_client" --nats "${NATS_URL}" \
    --register-cgroup "${CG_REL}" --label maigent_cpu --allow-control \
    2>&1 | tee "${RESULT_DIR}/register_${1}.txt" >/dev/null
}

run_phase() {
  local mode="$1" label="$2"
  cleanup_procs
  prepare_cgroup || return 1
  start_overlay "${mode}"
  register_cgroup "${label}"
  start_ramp || { cleanup_procs; return 1; }
  say "[${label}] наблюдаю ${RUN_SECONDS}с..."
  sleep "${RUN_SECONDS}"
  cp "${LOG_DIR}/runtime_system_monitor_agent.log" "${RESULT_DIR}/risk_${label}.log" 2>/dev/null || true
  if [[ "${mode}" == "with_planner" ]]; then
    cp "${LOG_DIR}/runtime_planner_agent.log"  "${RESULT_DIR}/planner_${label}.log"  2>/dev/null || true
    cp "${LOG_DIR}/runtime_actuator_agent.log" "${RESULT_DIR}/actuator_${label}.log" 2>/dev/null || true
  fi
  cleanup_procs
}

analyze() {
  local f="$1" title="$2"
  echo ""; echo "=== ${title} ==="
  [[ -s "$f" ]] || { echo "  (пусто)"; return; }
  grep "pressure_risk=" "$f" | grep -oP 'pressure_risk=\K[0-9]+' \
    | awk '{c[$1]++;t++} END{
        if(!t){print "  нет данных"; exit}
        printf "  LOW  (1): %5.1f%%\n", 100*(c["1"]+0)/t;
        printf "  MED  (2): %5.1f%%\n", 100*(c["2"]+0)/t;
        printf "  HIGH (3): %5.1f%%\n", 100*(c["3"]+0)/t;
        printf "  точек: %d\n", t; }'
  local cmin cmax pc
  cmin=$(grep -oP 'cpu_pct=\K[0-9]+' "$f"|sort -n|head -1)
  cmax=$(grep -oP 'cpu_pct=\K[0-9]+' "$f"|sort -n|tail -1)
  pc=$(grep -oP 'psi_cpu=\K[0-9.]+' "$f"|sort -g|tail -1)
  [[ -n "$cmax" ]] && echo "  CPU%: min=${cmin} max=${cmax} | PSI cpu макс=${pc}"
}

# ---------- main ----------
say "корень: ${ROOT_DIR} | пользователь: ${REAL_USER} | cpu_ramp: ${CPU_RAMP}"
say "CPU-нагрузка: workers→${TARGET_WORKERS} delay=${STEP_DELAY} hold=${HOLD_SEC} | сбор=${RUN_SECONDS}с"
[[ -x "${BUILD_DIR}/actuator_agent" ]] || { err "нет бинарников"; exit 1; }
[[ -x "${BUILD_DIR}/external_cgroup_client" ]] || { err "нет external_cgroup_client"; exit 1; }
[[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py: ${CPU_RAMP} (положи рядом или задай CPU_RAMP=путь)"; exit 1; }
command -v python3 >/dev/null 2>&1 || { err "нет python3"; exit 1; }

trap 'echo; say "прерывание — чищу"; cleanup_procs; cleanup_cgroup; exit 130' INT TERM

say "############ ФАЗА A: BASELINE (без planner) ############"
run_phase "no_planner" "baseline" || { err "baseline не удался"; cleanup_cgroup; exit 1; }

say "пауза между фазами"; sleep 5; cleanup_procs

say "############ ФАЗА B: OVERLAY (с planner) ############"
run_phase "with_planner" "overlay" || { err "overlay не удался"; cleanup_cgroup; exit 1; }

stty sane 2>/dev/null || true
echo ""
echo "############################################################"
echo "#          РИСК (растущая CPU-нагрузка)                    #"
echo "############################################################"
analyze "${RESULT_DIR}/risk_baseline.log" "BASELINE (без надстройки)"
analyze "${RESULT_DIR}/risk_overlay.log"  "OVERLAY (с надстройкой)"

echo ""
say "Planner overlay — воздействия"
grep -ioE "SET_CPU_MAX|SET_CPU_WEIGHT|FREEZE|sent actuator" "${RESULT_DIR}/planner_overlay.log" 2>/dev/null | sort | uniq -c
say "Actuator overlay"
grep -ioE "cpu.max|cpu.weight|updated|applied|failed" "${RESULT_DIR}/actuator_overlay.log" 2>/dev/null | sort | uniq -c

echo ""
echo "ИНТЕРПРЕТАЦИЯ:"
echo "  - baseline HIGH высокий + overlay HIGH НИЖЕ → надстройка снизила риск."
echo "  - baseline CPU% max < 85 → нагрузка слаба, подними TARGET_WORKERS (до 8)."
echo "  - Planner SET_CPU_MAX>0 и Actuator 'cpu.max updated' → надстройка реально режет CPU."
echo "  - overlay CPU% max ниже baseline → cpu.max ограничил загрузку."

cleanup_cgroup
say "готово. Логи: ${RESULT_DIR}/risk_baseline.log, risk_overlay.log"
