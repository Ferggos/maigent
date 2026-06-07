#!/usr/bin/env bash
#
# run_graph_b_ramp.sh — проверка графика Б на РАСТУЩЕЙ нагрузке (как и задумано в графике).
#
# Отличие от прошлых попыток: нагрузка НАРАСТАЕТ ступенями (mem_ramp.py), а не занимает
# всё сразу. Это даёт надстройке шанс ПРЕДОТВРАТИТЬ перегрузку (превентивный сценарий) —
# именно то, что должно снижать долю HIGH.
#
# Две фазы с одинаковой растущей нагрузкой в зарегистрированной внешней cgroup:
#   A) baseline — без planner (нагрузка дорастает до пика, держит HIGH).
#   B) overlay  — с planner (SET_MEM_HIGH душит рост на подходе → HIGH ниже/короче).
#
# Подгонки НЕТ — что получится, то и получится.
#
# ТРЕБУЕТ ROOT. Запуск: sudo ./run_graph_b_ramp.sh
# Параметры:
#   RAMP_SCRIPT     путь к mem_ramp.py (по умолчанию /home/$REAL_USER/mem_ramp.py)
#   TARGET_MB       до скольки растёт нагрузка (по умолчанию 2400)
#   STEP_MB         шаг роста (по умолчанию 100)
#   STEP_DELAY      пауза между ступенями, сек (по умолчанию 5)
#   HOLD_SEC        удержание на пике, сек (по умолчанию 60)
#   RUN_SECONDS     общий сбор риска на фазу (по умолчанию авто = рост + hold + запас)
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

RAMP_SCRIPT="${RAMP_SCRIPT:-${SCRIPT_DIR}/mem_ramp.py}"
TARGET_MB="${TARGET_MB:-2400}"
STEP_MB="${STEP_MB:-100}"
STEP_DELAY="${STEP_DELAY:-5}"
HOLD_SEC="${HOLD_SEC:-60}"
# авто-длительность сбора: время роста + hold + запас
RAMP_TIME=$(( (TARGET_MB / STEP_MB) * STEP_DELAY ))
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
  [[ -f "${CG_LEAF}/memory.high" ]] || { err "лист без memory.high — нужен sudo"; return 1; }
  echo max > "${CG_LEAF}/memory.high" 2>/dev/null || true
  echo max > "${CG_LEAF}/memory.max"  2>/dev/null || true
  ok "cgroup готова: ${CG_LEAF}"
}

kill_load() {
  [[ -f "${CG_LEAF}/cgroup.procs" ]] && {
    while read -r p; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null || true; done < "${CG_LEAF}/cgroup.procs"
  }
  pkill -9 -f "mem_ramp.py" 2>/dev/null || true
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
  echo max > "${CG_LEAF}/memory.high" 2>/dev/null || true
  echo max > "${CG_LEAF}/memory.max"  2>/dev/null || true
  rmdir "${CG_LEAF}" 2>/dev/null || true
  rmdir "${CG_PARENT}" 2>/dev/null || true
}

ensure_nats() {
  if ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/4222") 2>/dev/null; then
    as_user nats-server -a 127.0.0.1 -p 4222 >"${LOG_DIR}/nats-server.log" 2>&1 &
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
  as_user "${BUILD_DIR}/directory_agent"        --nats "${NATS_URL}" >"${LOG_DIR}/runtime_directory_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/lease_authority_agent"  --nats "${NATS_URL}" >"${LOG_DIR}/runtime_lease_authority_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/system_monitor_agent"   --nats "${NATS_URL}" --interval-ms 500 --cgroup-root "${CG_ROOT}" \
       >"${LOG_DIR}/runtime_system_monitor_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/actuator_agent"         --nats "${NATS_URL}" --cgroup-root "${CG_ROOT}" \
       >"${LOG_DIR}/runtime_actuator_agent.log" 2>&1 &
  if [[ "${mode}" == "with_planner" ]]; then
    as_user "${BUILD_DIR}/planner_agent"        --nats "${NATS_URL}" >"${LOG_DIR}/runtime_planner_agent.log" 2>&1 &
  fi
  as_user "${BUILD_DIR}/metrics_collector_agent" --nats "${NATS_URL}" --reports-root "${ROOT_DIR}/reports" \
       >"${LOG_DIR}/runtime_metrics_collector_agent.log" 2>&1 &
  sleep 4
}

start_ramp() {
  [[ -f "${RAMP_SCRIPT}" ]] || { err "нет mem_ramp.py по пути ${RAMP_SCRIPT}"; return 1; }
  say "растущая нагрузка: target=${TARGET_MB}MB step=${STEP_MB}MB delay=${STEP_DELAY}s hold=${HOLD_SEC}s (рост ~${RAMP_TIME}s)"
  rm -f "${LOG_DIR}/ramp.log" 2>/dev/null || true
  # Шелл входит в cgroup и exec python — все аллокации идут внутри cgroup.
  setsid bash -c "
    echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null
    exec python3 '${RAMP_SCRIPT}' --target-mb ${TARGET_MB} --step-mb ${STEP_MB} --step-delay ${STEP_DELAY} --hold-sec ${HOLD_SEC}
  " >"${LOG_DIR}/ramp.log" 2>&1 &
  sleep 2
  if ! pgrep -f "mem_ramp.py" >/dev/null 2>&1; then
    err "mem_ramp не запустился:"; cat "${LOG_DIR}/ramp.log" 2>/dev/null | head; return 1
  fi
  ok "нагрузка пошла (PID в cgroup: $(cat "${CG_LEAF}/cgroup.procs" 2>/dev/null | wc -l))"
}

register_cgroup() {
  say "регистрирую cgroup '${CG_REL}' (allow-control)"
  as_user "${BUILD_DIR}/external_cgroup_client" --nats "${NATS_URL}" \
    --register-cgroup "${CG_REL}" --label maigent_load --allow-control \
    2>&1 | tee "${RESULT_DIR}/register_${1}.txt" >/dev/null
}

run_phase() {
  local mode="$1" label="$2"
  cleanup_procs
  prepare_cgroup || return 1
  start_overlay "${mode}"
  register_cgroup "${label}"   # регистрируем ДО нагрузки, чтобы рост был виден сразу
  start_ramp || { cleanup_procs; return 1; }
  say "[${label}] наблюдаю ${RUN_SECONDS}с (весь цикл роста + hold)..."
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
  local mn mx pm
  mn=$(grep -oP 'mem_avail_mb=\K[0-9]+' "$f"|sort -n|head -1)
  mx=$(grep -oP 'mem_avail_mb=\K[0-9]+' "$f"|sort -n|tail -1)
  pm=$(grep -oP 'psi_mem=\K[0-9.]+' "$f"|sort -g|tail -1)
  [[ -n "$mn" ]] && echo "  MemAvailable: min=${mn}MB max=${mx}MB | PSI mem макс=${pm}"
}

# ---------- main ----------
say "корень: ${ROOT_DIR} | пользователь: ${REAL_USER}"
say "нагрузка: ${RAMP_SCRIPT} | target=${TARGET_MB} step=${STEP_MB} delay=${STEP_DELAY} hold=${HOLD_SEC} | сбор=${RUN_SECONDS}с"
[[ -x "${BUILD_DIR}/actuator_agent" ]] || { err "нет бинарников"; exit 1; }
[[ -x "${BUILD_DIR}/external_cgroup_client" ]] || { err "нет external_cgroup_client"; exit 1; }
[[ -f "${RAMP_SCRIPT}" ]] || { err "нет mem_ramp.py: ${RAMP_SCRIPT} (задай RAMP_SCRIPT=путь)"; exit 1; }
command -v python3 >/dev/null 2>&1 || { err "нет python3"; exit 1; }

trap 'echo; say "прерывание — чищу"; cleanup_procs; cleanup_cgroup; exit 130' INT TERM

say "############ ФАЗА A: BASELINE (без planner) ############"
run_phase "no_planner" "baseline" || { err "baseline не удался"; cleanup_cgroup; exit 1; }

say "пауза между фазами"; sleep 5; cleanup_procs

say "############ ФАЗА B: OVERLAY (с planner) ############"
run_phase "with_planner" "overlay" || { err "overlay не удался"; cleanup_cgroup; exit 1; }

echo ""
echo "############################################################"
echo "#       ГРАФИК Б (растущая нагрузка): РАСПРЕДЕЛЕНИЕ РИСКА    #"
echo "############################################################"
analyze "${RESULT_DIR}/risk_baseline.log" "BASELINE (без надстройки)"
analyze "${RESULT_DIR}/risk_overlay.log"  "OVERLAY (с надстройкой)"

echo ""
say "Planner overlay — воздействия"
grep -ioE "SET_MEM_HIGH|FREEZE|THAW|sent actuator" "${RESULT_DIR}/planner_overlay.log" 2>/dev/null | sort | uniq -c
say "Actuator overlay"
grep -ioE "memory.high|updated|applied|failed" "${RESULT_DIR}/actuator_overlay.log" 2>/dev/null | sort | uniq -c

echo ""
echo "ИНТЕРПРЕТАЦИЯ:"
echo "  - baseline HIGH высокий + overlay HIGH ниже → ГРАФИК ПОДТВЕРЖДЁН (превентивный эффект)."
echo "  - overlay HIGH >= baseline → на zram reclaim создаёт PSI; эффект искажён (см. ниже)."
echo "  - baseline HIGH=0 → нагрузка не создала давления, подними TARGET_MB."
echo "  - Сравни MemAvailable min: если в overlay min ВЫШЕ → надстройка удержала память (это и есть польза)."

cleanup_cgroup
say "готово. Логи: ${RESULT_DIR}/risk_baseline.log, risk_overlay.log"
