#!/usr/bin/env bash
#
# run_latency.sh — измерение ЛАТЕНТНОСТИ задач (график 5.5) на реальных данных.
#
# Подаёт поток задач под фоновой CPU-нагрузкой, в две фазы (baseline/overlay).
# metrics_collector штатно пишет latencies.csv и summary.txt с p50/p95/p99.
# Скрипт сравнивает submit_to_start_ms (латентность от подачи до старта) между фазами.
#
# Механизм снижения латентности: глобальный контур разгружает узел (cpu.max на фоновой
# нагрузке) → задачи быстрее проходят очередь и стартуют (queue_wait_ms падает).
#
# ТРЕБУЕТ ROOT. Запуск: sudo ./run_latency.sh
# Параметры:
#   BG_WORKERS   фоновых CPU-воркеров (по умолч. 12) — создают нагрузку, мешающую задачам
#   TASK_N       сколько задач подать (по умолч. 60)
#   TASK_CONC    параллельность подачи (по умолч. 6)
#   TASK_SLEEP   длительность каждой задачи, сек (по умолч. 0.5)
#   LOAD_HOLD    сколько держать фоновую нагрузку (по умолч. 70)
#
set -uo pipefail

REAL_USER="${SUDO_USER:-$(id -un)}"
REAL_UID="$(id -u "${REAL_USER}")"
REAL_GID="$(id -g "${REAL_USER}")"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ ! -d "${ROOT_DIR}/build" && -d "${ROOT_DIR}/../build" ]] && ROOT_DIR="$(cd "${ROOT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

BUILD_DIR="${ROOT_DIR}/build"
LOG_DIR="${ROOT_DIR}/logs"
RESULT_DIR="${ROOT_DIR}/experiment_results"
REPORTS_DIR="${ROOT_DIR}/reports"
NATS_URL="nats://127.0.0.1:4222"
CG_ROOT="/sys/fs/cgroup"
CG_NAME="${CG_NAME:-maigent_test}"
CG_PARENT="${CG_ROOT}/${CG_NAME}"
CG_LEAF="${CG_PARENT}/load"
CG_REL="${CG_NAME}/load"

CPU_RAMP="${CPU_RAMP:-${ROOT_DIR}/cpu_ramp.py}"
[[ -f "${CPU_RAMP}" ]] || CPU_RAMP="/home/${REAL_USER}/cpu_ramp.py"
# Фоновая нагрузка должна МЕШАТЬ задачам, но НЕ убивать систему (иначе OOM → все failed).
# 6 воркеров на 8 ядер: узел занят (~75%), но есть запас и по CPU, и по памяти.
BG_WORKERS="${BG_WORKERS:-6}"
TASK_N="${TASK_N:-40}"
TASK_CONC="${TASK_CONC:-4}"
TASK_SLEEP="${TASK_SLEEP:-0.3}"
LOAD_HOLD="${LOAD_HOLD:-90}"

mkdir -p "${RESULT_DIR}" "${LOG_DIR}" "${REPORTS_DIR}"
[[ "$(id -u)" -eq 0 && "${REAL_USER}" != "root" ]] && chown -R "${REAL_UID}:${REAL_GID}" "${LOG_DIR}" "${RESULT_DIR}" "${REPORTS_DIR}" 2>/dev/null || true

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
  echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true
  ok "cgroup готова"
}
kill_load(){
  [[ -f "${CG_LEAF}/cgroup.procs" ]] && while read -r p; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null||true; done < "${CG_LEAF}/cgroup.procs"
  pkill -9 -f "cpu_ramp.py" 2>/dev/null||true
  pkill -9 -f "bench_submitter" 2>/dev/null||true
  sleep 1
}
cleanup_procs(){
  for pid in $(pgrep -f task_executor_agent 2>/dev/null); do pkill -9 -P "$pid" 2>/dev/null||true; done
  for a in "${AGENTS[@]}"; do pkill -9 -f "$a" 2>/dev/null||true; done
  pkill -9 -f external_cgroup_client 2>/dev/null||true
  kill_load
}
cleanup_cgroup(){ kill_load; echo max > "${CG_LEAF}/cpu.max" 2>/dev/null||true; rmdir "${CG_LEAF}" 2>/dev/null||true; rmdir "${CG_PARENT}" 2>/dev/null||true; }
ensure_nats(){ if ! (timeout 1 bash -c "</dev/tcp/127.0.0.1/4222") 2>/dev/null; then as_user nats-server -a 127.0.0.1 -p 4222 >"${LOG_DIR}/nats-server.log" 2>&1 & sleep 1; fi; }

start_overlay(){
  local mode="$1"
  ensure_nats
  : > "${LOG_DIR}/runtime_planner_agent.log"
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
  as_user "${BUILD_DIR}/metrics_collector_agent" --nats "${NATS_URL}" --reports-root "${REPORTS_DIR}" >"${LOG_DIR}/runtime_metrics_collector_agent.log" 2>&1 &
  sleep 4
}

register_cgroup(){ as_user "${BUILD_DIR}/external_cgroup_client" --nats "${NATS_URL}" --register-cgroup "${CG_REL}" --label maigent_bg --allow-control >/dev/null 2>&1; }

start_bg_load(){
  [[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py"; return 1; }
  say "фоновая CPU-нагрузка →${BG_WORKERS} воркеров (мешает задачам)"
  setsid bash -c "echo \$\$ > '${CG_LEAF}/cgroup.procs' 2>/dev/null; exec python3 '${CPU_RAMP}' --target-workers ${BG_WORKERS} --step-delay 2 --hold-sec ${LOAD_HOLD}" >"${LOG_DIR}/bg_load.log" 2>&1 &
  sleep 8  # дать нагрузке вырасти, чтобы узел был занят к подаче задач
  ok "фоновая нагрузка идёт (CPU занят)"
}

# Подать задачи и дождаться, bench_id уникальный на фазу
run_tasks(){
  local label="$1"
  local bench_id="lat-${label}-$(date +%s)"
  say "[${label}] подаю ${TASK_N} задач (bench_id=${bench_id}, ждём финиш каждой)"
  # /bin/sleep — самая устойчивая команда (не падает под нагрузкой, в отличие от sh -c).
  # wait_finish: ждём завершения, чтобы collector записал submit_to_start/finish.
  as_user "${BUILD_DIR}/bench_submitter" --nats "${NATS_URL}" \
    --n "${TASK_N}" --concurrency "${TASK_CONC}" --cmd /bin/sleep --args "${TASK_SLEEP}" \
    --bench-id "${bench_id}" --wait-mode wait_finish --submit-timeout-ms 30000 --wait-timeout-ms 30000 \
    2>&1 | tee "${RESULT_DIR}/bench_${label}.txt"
  echo "${bench_id}" > "${RESULT_DIR}/bench_id_${label}.txt"
  sleep 3  # дать collector дописать отчёт
}

run_phase(){
  local mode="$1" label="$2"
  cleanup_procs
  prepare_cgroup || return 1
  start_overlay "${mode}"
  register_cgroup
  start_bg_load || { cleanup_procs; return 1; }
  run_tasks "${label}"
  cleanup_procs
}

# Достать p50/p95/p99 submit_to_start из latencies.csv
analyze_latency(){
  local label="$1" title="$2"
  local bench_id; bench_id=$(cat "${RESULT_DIR}/bench_id_${label}.txt" 2>/dev/null)
  local csv="${REPORTS_DIR}/${bench_id}/latencies.csv"
  local summary="${REPORTS_DIR}/${bench_id}/summary.txt"
  echo ""; echo "=== ${title} (bench_id=${bench_id}) ==="
  if [[ ! -f "${csv}" ]]; then echo "  нет latencies.csv — задачи не завершились?"; return; fi
  # колонка submit_to_start_ms = 4-я; queue_wait_ms = 9-я
  echo "  submit_to_start_ms (латентность подача→старт):"
  awk -F, 'NR>1 && $4!="" && $4>=0 {print $4}' "${csv}" | sort -n | awk '{a[NR]=$1} END{
    n=NR; if(n==0){print "    нет данных"; exit}
    p50=a[int(n*0.50)+0>0?int(n*0.50):1]; p95=a[int(n*0.95)>0?int(n*0.95):1]; p99=a[int(n*0.99)>0?int(n*0.99):1];
    printf "    p50=%.0f мс  p95=%.0f мс  p99=%.0f мс  max=%.0f мс  n=%d\n", p50,p95,p99,a[n],n }'
  echo "  queue_wait_ms (ожидание в очереди):"
  awk -F, 'NR>1 && $9!="" && $9>=0 {print $9}' "${csv}" | sort -n | awk '{a[NR]=$1} END{
    n=NR; if(n==0){print "    нет данных"; exit}
    p50=a[int(n*0.50)+0>0?int(n*0.50):1]; p95=a[int(n*0.95)>0?int(n*0.95):1];
    printf "    p50=%.0f мс  p95=%.0f мс  max=%.0f мс\n", p50,p95,a[n] }'
  echo "  --- summary.txt коллектора (если есть) ---"
  [[ -f "${summary}" ]] && head -20 "${summary}" | sed 's/^/    /' || echo "    нет summary"
}

# ---------- main ----------
say "ЛАТЕНТНОСТЬ | фон=${BG_WORKERS}w задач=${TASK_N} sleep=${TASK_SLEEP}s | сбор метрик через collector"
[[ -x "${BUILD_DIR}/bench_submitter" ]] || { err "нет bench_submitter"; exit 1; }
[[ -x "${BUILD_DIR}/metrics_collector_agent" ]] || { err "нет metrics_collector"; exit 1; }
[[ -f "${CPU_RAMP}" ]] || { err "нет cpu_ramp.py"; exit 1; }

trap 'echo; say "прерывание"; cleanup_procs; cleanup_cgroup; exit 130' INT TERM

say "############ ФАЗА A: BASELINE (без planner) ############"
run_phase "no_planner" "baseline" || { err "baseline не удался"; cleanup_cgroup; exit 1; }
say "пауза"; sleep 5; cleanup_procs
say "############ ФАЗА B: OVERLAY (с planner) ############"
run_phase "with_planner" "overlay" || { err "overlay не удался"; cleanup_cgroup; exit 1; }

echo ""
echo "############################################################"
echo "#    ГРАФИК 5.5: ЛАТЕНТНОСТЬ ЗАДАЧ                          #"
echo "############################################################"
analyze_latency "baseline" "BASELINE (без надстройки)"
analyze_latency "overlay"  "OVERLAY (с надстройкой)"

echo ""
echo "ЧЕСТНАЯ ИНТЕРПРЕТАЦИЯ:"
echo "  - submit_to_start ниже в overlay → надстройка ускоряет старт задач."
echo "  - Главный вклад — queue_wait_ms: разгрузка узла (cpu.max на фоне) ускоряет очередь."
echo "  - Если разница мала → renice сам по себе латентность почти не снижает (ожидаемо),"
echo "    и тогда честный вывод: латентность улучшается через стабилизацию узла, а не приоритеты."
echo "  - bench_submitter (denied/timeout) и summary.txt → данные для графика 5.8 (успешность) заодно."

cleanup_cgroup
say "готово. Отчёты: ${REPORTS_DIR}/lat-baseline-*/ и lat-overlay-*/"
