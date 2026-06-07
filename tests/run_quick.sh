#!/usr/bin/env bash
#
# run_quick.sh — БЫСТРЫЙ запуск ТОЛЬКО надстройки с planner + нагрузка в cgroup.
# Для итеративной отладки (DIAG-логи). Без двух фаз, без долгого сбора.
#
# Поднимает надстройку, создаёт cgroup, запускает растущую CPU-нагрузку, регистрирует,
# и ОСТАЁТСЯ работать. Логи смотришь в реальном времени. Ctrl+C — чистит всё.
#
# sudo ./run_quick.sh
# Параметры: TARGET_WORKERS (16), STEP_DELAY (3), HOLD_SEC (600 — долго, для отладки)
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
NATS_URL="nats://127.0.0.1:4222"
CG_ROOT="/sys/fs/cgroup"
CG_NAME="${CG_NAME:-maigent_test}"
CG_PARENT="${CG_ROOT}/${CG_NAME}"
CG_LEAF="${CG_PARENT}/load"
CG_REL="${CG_NAME}/load"

CPU_RAMP="${CPU_RAMP:-${SCRIPT_DIR}/cpu_ramp.py}"
TARGET_WORKERS="${TARGET_WORKERS:-16}"
STEP_DELAY="${STEP_DELAY:-3}"
HOLD_SEC="${HOLD_SEC:-600}"

mkdir -p "${LOG_DIR}" "${ROOT_DIR}/reports"
[[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]] && chown -R "${REAL_UID}:${REAL_GID}" "${LOG_DIR}" "${ROOT_DIR}/reports" 2>/dev/null || true

AGENTS=(task_manager_agent task_executor_agent planner_agent system_monitor_agent
        actuator_agent lease_authority_agent directory_agent metrics_collector_agent)

say(){ echo -e "\n\033[1;36m[quick]\033[0m $*"; }
err(){ echo -e "\033[1;31m[quick ERR]\033[0m $*" >&2; }
ok(){ echo -e "\033[1;32m[quick OK]\033[0m $*"; }

as_user(){ if [[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]]; then sudo -u "${REAL_USER}" "$@"; else "$@"; fi; }

cleanup(){
  echo
  say "чищу..."
  for pid in $(pgrep -f task_executor_agent 2>/dev/null); do pkill -9 -P "$pid" 2>/dev/null||true; done
  for a in "${AGENTS[@]}"; do pkill -9 -f "$a" 2>/dev/null||true; done
  pkill -9 -f external_cgroup_client 2>/dev/null||true
  pkill -9 -f cpu_ramp.py 2>/dev/null||true
  [[ -f "${CG_LEAF}/cgroup.procs" ]] && while read -r p; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null||true; done < "${CG_LEAF}/cgroup.procs"
  sleep 1
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true
  rmdir "${CG_LEAF}" 2>/dev/null||true
  rmdir "${CG_PARENT}" 2>/dev/null||true
  ok "готово"
  exit 0
}
trap cleanup INT TERM

# --- проверки ---
[[ -x "${BUILD_DIR}/planner_agent" ]] || { err "нет бинарников, собери: cmake --build build -j"; exit 1; }
[[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py: ${CPU_RAMP}"; exit 1; }

# --- cgroup ---
say "готовлю cgroup ${CG_LEAF}"
echo "+memory +cpu" > "${CG_ROOT}/cgroup.subtree_control" 2>/dev/null||true
mkdir -p "${CG_PARENT}" 2>/dev/null||true
echo "+memory +cpu" > "${CG_PARENT}/cgroup.subtree_control" 2>/dev/null||true
mkdir -p "${CG_LEAF}" 2>/dev/null||true
chown -R "${REAL_UID}:${REAL_GID}" "${CG_PARENT}" 2>/dev/null||true
echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true
[[ -f "${CG_LEAF}/cpu.max" ]] || { err "лист без cpu.max — нужен sudo"; exit 1; }
ok "cgroup готова"

# --- nats ---
if ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/4222") 2>/dev/null; then
  say "поднимаю nats"; as_user nats-server -a 127.0.0.1 -p 4222 >"${LOG_DIR}/nats-server.log" 2>&1 & sleep 1
fi

# --- чистим старые логи для свежего прогона ---
: > "${LOG_DIR}/runtime_planner_agent.log"
: > "${LOG_DIR}/runtime_actuator_agent.log"
: > "${LOG_DIR}/runtime_system_monitor_agent.log"

# --- надстройка С planner ---
say "старт надстройки (с planner)"
as_user "${BUILD_DIR}/directory_agent"        --nats "${NATS_URL}" >"${LOG_DIR}/runtime_directory_agent.log" 2>&1 &
as_user "${BUILD_DIR}/lease_authority_agent"  --nats "${NATS_URL}" >"${LOG_DIR}/runtime_lease_authority_agent.log" 2>&1 &
as_user "${BUILD_DIR}/system_monitor_agent"   --nats "${NATS_URL}" --interval-ms 500 --cgroup-root "${CG_ROOT}" >"${LOG_DIR}/runtime_system_monitor_agent.log" 2>&1 &
as_user "${BUILD_DIR}/actuator_agent"         --nats "${NATS_URL}" --cgroup-root "${CG_ROOT}" >"${LOG_DIR}/runtime_actuator_agent.log" 2>&1 &
as_user "${BUILD_DIR}/planner_agent"          --nats "${NATS_URL}" >"${LOG_DIR}/runtime_planner_agent.log" 2>&1 &
as_user "${BUILD_DIR}/metrics_collector_agent" --nats "${NATS_URL}" --reports-root "${ROOT_DIR}/reports" >"${LOG_DIR}/runtime_metrics_collector_agent.log" 2>&1 &
sleep 4
ok "агенты подняты"

# --- регистрация cgroup ---
say "регистрирую cgroup ${CG_REL}"
as_user "${BUILD_DIR}/external_cgroup_client" --nats "${NATS_URL}" --register-cgroup "${CG_REL}" --label maigent_cpu --allow-control 2>&1 | grep -o "success=[01].*reason=\"[^\"]*\"" || true

# --- нагрузка внутри cgroup ---
say "нагрузка cpu_ramp →${TARGET_WORKERS} воркеров"
rm -f "${LOG_DIR}/cpu_ramp.log" 2>/dev/null||true
setsid bash -c "echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null; exec python3 '${CPU_RAMP}' --target-workers ${TARGET_WORKERS} --step-delay ${STEP_DELAY} --hold-sec ${HOLD_SEC}" >"${LOG_DIR}/cpu_ramp.log" 2>&1 &
sleep 3
pgrep -x stress-ng >/dev/null 2>&1 || pgrep -f cpu_ramp.py >/dev/null 2>&1 || { err "нагрузка не пошла"; cat "${LOG_DIR}/cpu_ramp.log"|head; }
ok "нагрузка пошла (процессов в cgroup: $(cat "${CG_LEAF}/cgroup.procs" 2>/dev/null|wc -l))"

echo ""
echo "=================================================================="
echo " Надстройка РАБОТАЕТ. Смотри логи в реальном времени, например:"
echo "   tail -f logs/runtime_planner_agent.log | grep -E 'DIAG|sent actuator'"
echo "   tail -f logs/runtime_actuator_agent.log | grep -iE 'cpu.max|applied|failed'"
echo " Текущий риск:"
echo "   grep -oP 'pressure_risk=\\K[0-9]+' logs/runtime_system_monitor_agent.log | tail -5"
echo ""
echo " Ctrl+C — остановить и почистить."
echo "=================================================================="
echo ""

# Живой минимониторинг каждые 5с
while true; do
  sleep 5
  risk=$(grep -oP 'pressure_risk=\K[0-9]+' "${LOG_DIR}/runtime_system_monitor_agent.log" 2>/dev/null | tail -1)
  cpu=$(grep -oP 'cpu_pct=\K[0-9]+' "${LOG_DIR}/runtime_system_monitor_agent.log" 2>/dev/null | tail -1)
  acts=$(grep -c "sent actuator" "${LOG_DIR}/runtime_planner_agent.log" 2>/dev/null||echo 0)
  cgmax=$(cat "${CG_LEAF}/cpu.max" 2>/dev/null)
  diag4=$(grep "DIAG4" "${LOG_DIR}/runtime_planner_agent.log" 2>/dev/null | tail -1 | grep -oP 'actions=\[[^]]*\]')
  echo "[$(date +%H:%M:%S)] risk=${risk:-?} cpu%=${cpu:-?} sent_actions=${acts} cpu.max='${cgmax}' ${diag4}"
done
