#!/usr/bin/env bash
#
# run_graph_b_mixed.sh — СМЕШАННАЯ нагрузка (CPU + память + задачи) для графика Б.
#
# Воспроизводит сценарий из текста ВКР: одновременно
#   - растущая CPU-нагрузка (cpu_ramp.py) в общей cgroup,
#   - растущая memory-нагрузка (mem_ramp.py) в той же cgroup,
#   - поток задач надстройки (bench_submitter, ~40 задач).
#
# ЧЕСТНОЕ ПРЕДУПРЕЖДЕНИЕ: на стенде без свопа memory-риск почти не растёт,
# и CPU доминирует в выборе стратегии. Поэтому overlay по риску, вероятно,
# поведёт себя близко к чистому CPU. Скрипт нужен прежде всего как ДЕМОНСТРАЦИЯ
# заявленного смешанного сценария (для воспроизведения по запросу комиссии).
#
# ТРЕБУЕТ ROOT. Запуск: sudo ./run_graph_b_mixed.sh
# Параметры:
#   CPU_WORKERS    воркеров CPU (по умолч. 12)
#   CPU_QUOTA      не используется здесь (значение cpu.max задаётся в коде Planner)
#   MEM_TARGET_MB  до скольки растёт память (по умолч. 1500)
#   MEM_STEP_MB    шаг памяти (по умолч. 100)
#   STEP_DELAY     пауза между ступенями обеих нагрузок (по умолч. 4)
#   HOLD_SEC       удержание на пике (по умолч. 90)
#   TASK_N         сколько задач подать (по умолч. 40)
#   RUN_SECONDS    авто
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
MEM_RAMP="${MEM_RAMP:-${SCRIPT_DIR}/mem_ramp.py}"

CPU_WORKERS="${CPU_WORKERS:-12}"
MEM_TARGET_MB="${MEM_TARGET_MB:-1500}"
MEM_STEP_MB="${MEM_STEP_MB:-100}"
STEP_DELAY="${STEP_DELAY:-4}"
HOLD_SEC="${HOLD_SEC:-90}"
TASK_N="${TASK_N:-40}"
RAMP_TIME=$(( CPU_WORKERS * STEP_DELAY ))
RUN_SECONDS="${RUN_SECONDS:-$(( RAMP_TIME + HOLD_SEC + 20 ))}"

mkdir -p "${RESULT_DIR}" "${LOG_DIR}" "${ROOT_DIR}/reports"
[[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]] && chown -R "${REAL_UID}:${REAL_GID}" "${LOG_DIR}" "${RESULT_DIR}" "${ROOT_DIR}/reports" 2>/dev/null || true

AGENTS=(task_manager_agent task_executor_agent planner_agent system_monitor_agent
        actuator_agent lease_authority_agent directory_agent metrics_collector_agent)

say(){ echo -e "\n\033[1;36m[exp]\033[0m $*"; }
err(){ echo -e "\033[1;31m[exp ERR]\033[0m $*" >&2; }
warn(){ echo -e "\033[1;33m[exp WARN]\033[0m $*" >&2; }
ok(){ echo -e "\033[1;32m[exp OK]\033[0m $*"; }
as_user(){ if [[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]]; then sudo -u "${REAL_USER}" "$@"; else "$@"; fi; }

prepare_cgroup(){
  echo "+memory +cpu" > "${CG_ROOT}/cgroup.subtree_control" 2>/dev/null||true
  mkdir -p "${CG_PARENT}" 2>/dev/null||true
  echo "+memory +cpu" > "${CG_PARENT}/cgroup.subtree_control" 2>/dev/null||true
  mkdir -p "${CG_LEAF}" 2>/dev/null||true
  chown -R "${REAL_UID}:${REAL_GID}" "${CG_PARENT}" 2>/dev/null||true
  [[ -f "${CG_LEAF}/cpu.max" ]] || { err "лист без cpu.max — нужен sudo"; return 1; }
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true
  echo max > "${CG_LEAF}/memory.high" 2>/dev/null||true
  echo max > "${CG_LEAF}/memory.max" 2>/dev/null||true
  ok "cgroup готова: ${CG_LEAF}"
}

kill_load(){
  [[ -f "${CG_LEAF}/cgroup.procs" ]] && while read -r p; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null||true; done < "${CG_LEAF}/cgroup.procs"
  pkill -9 -f "cpu_ramp.py" 2>/dev/null||true
  pkill -9 -f "mem_ramp.py" 2>/dev/null||true
  pkill -9 -f "bench_submitter" 2>/dev/null||true
  sleep 1
}
cleanup_procs(){
  for pid in $(pgrep -f task_executor_agent 2>/dev/null); do pkill -9 -P "$pid" 2>/dev/null||true; done
  for a in "${AGENTS[@]}"; do pkill -9 -f "$a" 2>/dev/null||true; done
  pkill -9 -f external_cgroup_client 2>/dev/null||true
  kill_load
}
cleanup_cgroup(){
  kill_load
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true
  echo max > "${CG_LEAF}/memory.high" 2>/dev/null||true
  echo max > "${CG_LEAF}/memory.max" 2>/dev/null||true
  rmdir "${CG_LEAF}" 2>/dev/null||true
  rmdir "${CG_PARENT}" 2>/dev/null||true
}
ensure_nats(){
  if ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/4222") 2>/dev/null; then
    as_user nats-server -a 127.0.0.1 -p 4222 >"${LOG_DIR}/nats-server.log" 2>&1 & sleep 1
  fi
}

start_overlay(){
  local mode="$1"
  ensure_nats
  : > "${LOG_DIR}/runtime_system_monitor_agent.log"
  : > "${LOG_DIR}/runtime_planner_agent.log"
  : > "${LOG_DIR}/runtime_actuator_agent.log"
  say "старт надстройки (${mode})"
  as_user "${BUILD_DIR}/directory_agent"        --nats "${NATS_URL}" >"${LOG_DIR}/runtime_directory_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/lease_authority_agent"  --nats "${NATS_URL}" >"${LOG_DIR}/runtime_lease_authority_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/task_manager_agent"     --nats "${NATS_URL}" >"${LOG_DIR}/runtime_task_manager_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/task_executor_agent"    --nats "${NATS_URL}" >"${LOG_DIR}/runtime_task_executor_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/system_monitor_agent"   --nats "${NATS_URL}" --interval-ms 500 --cgroup-root "${CG_ROOT}" >"${LOG_DIR}/runtime_system_monitor_agent.log" 2>&1 &
  as_user "${BUILD_DIR}/actuator_agent"         --nats "${NATS_URL}" --cgroup-root "${CG_ROOT}" >"${LOG_DIR}/runtime_actuator_agent.log" 2>&1 &
  if [[ "${mode}" == "with_planner" ]]; then
    as_user "${BUILD_DIR}/planner_agent"        --nats "${NATS_URL}" >"${LOG_DIR}/runtime_planner_agent.log" 2>&1 &
  fi
  as_user "${BUILD_DIR}/metrics_collector_agent" --nats "${NATS_URL}" --reports-root "${ROOT_DIR}/reports" >"${LOG_DIR}/runtime_metrics_collector_agent.log" 2>&1 &
  sleep 4
}

register_cgroup(){
  say "регистрирую cgroup ${CG_REL}"
  as_user "${BUILD_DIR}/external_cgroup_client" --nats "${NATS_URL}" \
    --register-cgroup "${CG_REL}" --label maigent_mixed --allow-control 2>&1 | tee "${RESULT_DIR}/register_${1}.txt" >/dev/null
}

# Запуск CPU+memory нагрузок ВНУТРИ cgroup (форки наследуют cgroup)
start_mixed_load(){
  [[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py: ${CPU_RAMP}"; return 1; }
  [[ -f "${MEM_RAMP}" ]] || { err "нет mem_ramp.py: ${MEM_RAMP}"; return 1; }
  rm -f "${LOG_DIR}/cpu_ramp.log" "${LOG_DIR}/mem_ramp.log" 2>/dev/null||true

  say "CPU-нагрузка →${CPU_WORKERS} воркеров в cgroup"
  setsid bash -c "echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null; exec python3 '${CPU_RAMP}' --target-workers ${CPU_WORKERS} --step-delay ${STEP_DELAY} --hold-sec ${HOLD_SEC}" >"${LOG_DIR}/cpu_ramp.log" 2>&1 &
  sleep 1
  say "memory-нагрузка →${MEM_TARGET_MB}MB в cgroup"
  setsid bash -c "echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null; exec python3 '${MEM_RAMP}' --target-mb ${MEM_TARGET_MB} --step-mb ${MEM_STEP_MB} --step-delay ${STEP_DELAY} --hold-sec ${HOLD_SEC}" >"${LOG_DIR}/mem_ramp.log" 2>&1 &
  sleep 3
  pgrep -f cpu_ramp.py >/dev/null 2>&1 || { err "cpu_ramp не пошёл"; cat "${LOG_DIR}/cpu_ramp.log"|head; }
  pgrep -f mem_ramp.py >/dev/null 2>&1 || { err "mem_ramp не пошёл"; cat "${LOG_DIR}/mem_ramp.log"|head; }
  ok "нагрузка в cgroup (процессов: $(cat "${CG_LEAF}/cgroup.procs" 2>/dev/null|wc -l)), MemAvail сист: $(awk '/MemAvailable/{printf "%.0f MB",$2/1024}' /proc/meminfo)"
}

# Поток задач надстройки (только в overlay/baseline — обе фазы, для сравнения 5.8 тоже)
start_tasks(){
  [[ -x "${BUILD_DIR}/bench_submitter" ]] || { warn "нет bench_submitter — задачи пропущены"; return 0; }
  say "подаю ${TASK_N} задач (no_wait, короткие)"
  as_user "${BUILD_DIR}/bench_submitter" --nats "${NATS_URL}" \
    --n "${TASK_N}" --concurrency 8 --cmd /bin/sh --args "-c,sleep 0.3" \
    --wait-mode no_wait --submit-timeout-ms 5000 >"${LOG_DIR}/bench_${1}.log" 2>&1 &
}

run_phase(){
  local mode="$1" label="$2"
  cleanup_procs
  prepare_cgroup || return 1
  start_overlay "${mode}"
  register_cgroup "${label}"
  start_mixed_load || { cleanup_procs; return 1; }
  start_tasks "${label}"
  say "[${label}] наблюдаю ${RUN_SECONDS}с (CPU+память+задачи)..."
  sleep "${RUN_SECONDS}"
  cp "${LOG_DIR}/runtime_system_monitor_agent.log" "${RESULT_DIR}/risk_${label}.log" 2>/dev/null||true
  if [[ "${mode}" == "with_planner" ]]; then
    cp "${LOG_DIR}/runtime_planner_agent.log"  "${RESULT_DIR}/planner_${label}.log"  2>/dev/null||true
    cp "${LOG_DIR}/runtime_actuator_agent.log" "${RESULT_DIR}/actuator_${label}.log" 2>/dev/null||true
  fi
  cleanup_procs
}

analyze(){
  local f="$1" title="$2"
  echo ""; echo "=== ${title} ==="
  [[ -s "$f" ]] || { echo "  (пусто)"; return; }
  grep "pressure_risk=" "$f" | grep -oP 'pressure_risk=\K[0-9]+' | awk '{c[$1]++;t++} END{
    if(!t){print "  нет данных"; exit}
    printf "  LOW  (1): %5.1f%%\n",100*(c["1"]+0)/t;
    printf "  MED  (2): %5.1f%%\n",100*(c["2"]+0)/t;
    printf "  HIGH (3): %5.1f%%\n",100*(c["3"]+0)/t;
    printf "  точек: %d\n",t; }'
  local cmx mmn pc pm
  cmx=$(grep -oP 'cpu_pct=\K[0-9]+' "$f"|sort -n|tail -1)
  mmn=$(grep -oP 'mem_avail_mb=\K[0-9]+' "$f"|sort -n|head -1)
  pc=$(grep -oP 'psi_cpu=\K[0-9.]+' "$f"|sort -g|tail -1)
  pm=$(grep -oP 'psi_mem=\K[0-9.]+' "$f"|sort -g|tail -1)
  echo "  CPU% max=${cmx} | MemAvail min=${mmn}MB | PSI cpu max=${pc} | PSI mem max=${pm}"
}

# ---------- main ----------
say "СМЕШАННАЯ нагрузка | CPU=${CPU_WORKERS}w mem=${MEM_TARGET_MB}MB задач=${TASK_N} | сбор=${RUN_SECONDS}с"
say "cpu_ramp=${CPU_RAMP} | mem_ramp=${MEM_RAMP}"
[[ -x "${BUILD_DIR}/actuator_agent" ]] || { err "нет бинарников"; exit 1; }
[[ -x "${BUILD_DIR}/external_cgroup_client" ]] || { err "нет external_cgroup_client"; exit 1; }
[[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py"; exit 1; }
[[ -f "${MEM_RAMP}" ]] || { err "нет mem_ramp.py (${MEM_RAMP})"; exit 1; }

trap 'echo; say "прерывание — чищу"; cleanup_procs; cleanup_cgroup; exit 130' INT TERM

say "############ ФАЗА A: BASELINE (без planner) ############"
run_phase "no_planner" "baseline" || { err "baseline не удался"; cleanup_cgroup; exit 1; }
say "пауза"; sleep 5; cleanup_procs
say "############ ФАЗА B: OVERLAY (с planner) ############"
run_phase "with_planner" "overlay" || { err "overlay не удался"; cleanup_cgroup; exit 1; }

echo ""
echo "############################################################"
echo "#    ГРАФИК Б (СМЕШАННАЯ нагрузка): РИСК                    #"
echo "############################################################"
analyze "${RESULT_DIR}/risk_baseline.log" "BASELINE (без надстройки)"
analyze "${RESULT_DIR}/risk_overlay.log"  "OVERLAY (с надстройкой)"

echo ""
say "Planner overlay — воздействия (типы)"
grep -oP 'action=\K[0-9]+' "${RESULT_DIR}/planner_overlay.log" 2>/dev/null | sort | uniq -c | sed 's/$/ (2=cpuW 3=cpuMax 4=memHigh 5=memMax)/'
say "Actuator overlay"
grep -ioE "cpu.max|memory.high|memory.max|applied|failed" "${RESULT_DIR}/actuator_overlay.log" 2>/dev/null | sort | uniq -c

echo ""
echo "ЧЕСТНАЯ ИНТЕРПРЕТАЦИЯ:"
echo "  - Это смешанный сценарий из текста ВКР (CPU+память+задачи в общей cgroup)."
echo "  - Если CPU доминирует и память не дала PSI (PSI mem max ~0) — overlay близок к CPU-only."
echo "  - Смотри типы action: если есть только 3 (cpu.max) — CPU выиграл стратегию,"
echo "    память не вмешивалась (ожидаемо на стенде без свопа)."
echo "  - Ценность: воспроизводит ЗАЯВЛЕННЫЙ сценарий для демонстрации комиссии."

cleanup_cgroup
say "готово. Логи: ${RESULT_DIR}/risk_baseline.log, risk_overlay.log"
