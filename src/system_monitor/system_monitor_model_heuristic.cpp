#include "maigent/system_monitor/system_monitor_model.h"

#include <algorithm>
#include <thread>
#include <vector>

#include "maigent/common/time_utils.h"

namespace maigent {

namespace {

SystemMonitorForecastOutput PredictForecastHeuristic(
    const SystemMonitorPressureOutput& latest,
    const std::vector<SystemMonitorPressureHistorySample>& pressure_history) {
  std::vector<SystemMonitorPressureHistorySample> history_with_latest =
      pressure_history;
  history_with_latest.push_back(ToPressureHistorySampleFromOutput(latest));

  double cpu_sum = latest.cpu_usage_pct;
  double mem_sum = static_cast<double>(latest.mem_available_mb);
  int n = 1;
  for (auto it = history_with_latest.rbegin();
       it != history_with_latest.rend() && n < 6; ++it) {
    cpu_sum += it->cpu_usage_pct;
    mem_sum += static_cast<double>(it->mem_available_mb);
    ++n;
  }

  const double cpu_avg = cpu_sum / static_cast<double>(n);
  const int64_t mem_avg = static_cast<int64_t>(mem_sum / static_cast<double>(n));

  SystemMonitorForecastOutput out;
  out.ts_ms = NowMs();
  out.predicted_cpu_usage_pct = std::min(100.0, cpu_avg * 1.08);
  out.predicted_mem_available_mb = std::max<int64_t>(0, mem_avg - 64);
  out.predictor = "heuristic_v1";

  if (out.predicted_cpu_usage_pct >= 85.0 || out.predicted_mem_available_mb < 768) {
    out.risk_level = RISK_HIGH;
    out.overload_probability = 0.85;
  } else if (out.predicted_cpu_usage_pct >= 70.0 ||
             out.predicted_mem_available_mb < 1536) {
    out.risk_level = RISK_MED;
    out.overload_probability = 0.45;
  } else {
    out.risk_level = RISK_LOW;
    out.overload_probability = 0.15;
  }

  return out;
}

void ClassifyTargetHeuristic(SystemMonitorTargetOutput* target) {
  if (target == nullptr) {
    return;
  }

  target->allowed_actions.clear();
  const auto source = target->source_type;
  if (source == MANAGED_TASK) {
    target->is_protected = false;
    target->allowed_actions.push_back(RENICE);
    target->allowed_actions.push_back(SET_CPU_WEIGHT);
    target->allowed_actions.push_back(SET_CPU_MAX);
    target->allowed_actions.push_back(SET_MEM_HIGH);
    target->allowed_actions.push_back(FREEZE);
    target->allowed_actions.push_back(THAW);
    target->allowed_actions.push_back(KILL);
    return;
  }

  if (source == EXTERNAL_PROCESS || source == EXTERNAL_GROUP) {
    target->is_protected = false;
    target->allowed_actions.push_back(RENICE);
    target->allowed_actions.push_back(SET_CPU_WEIGHT);
    target->allowed_actions.push_back(SET_CPU_MAX);
    target->allowed_actions.push_back(SET_MEM_HIGH);
    target->allowed_actions.push_back(SET_MEM_MAX);
    target->allowed_actions.push_back(FREEZE);
    target->allowed_actions.push_back(THAW);
    target->allowed_actions.push_back(KILL);
    return;
  }

  target->is_protected = true;
  target->allowed_actions.push_back(RENICE);
  target->allowed_actions.push_back(SET_CPU_WEIGHT);
}

}  // namespace

SystemMonitorModelOutput HeuristicSystemMonitorModel::Evaluate(
    const SystemMonitorModelInput& input) {
  SystemMonitorModelOutput out;

  out.pressure.ts_ms = input.host.ts_ms;
  out.pressure.cpu_usage_pct = input.host.cpu_usage_pct;
  out.pressure.mem_available_mb = input.host.mem_available_mb;
  out.pressure.load1 = input.host.load1;
  out.pressure.cpu_pressure_some = input.host.psi_cpu_some;
  out.pressure.memory_pressure_some = input.host.psi_mem_some;
  out.pressure.io_pressure_some = input.host.psi_io_some;
  if (input.host.cpu_usage_pct >= 85.0 || input.host.mem_available_mb < 768 ||
      input.host.psi_mem_some > 1.0) {
    out.pressure.risk_level = RISK_HIGH;
  } else if (input.host.cpu_usage_pct >= 70.0 || input.host.mem_available_mb < 1536 ||
             input.host.psi_mem_some > 0.3) {
    out.pressure.risk_level = RISK_MED;
  } else {
    out.pressure.risk_level = RISK_LOW;
  }

  out.forecast = PredictForecastHeuristic(out.pressure, input.pressure_history);

  out.capacity.ts_ms = input.host.ts_ms;
  const int cpu_total = static_cast<int>(std::thread::hardware_concurrency() * 1000);
  out.capacity.cpu_millis_total = cpu_total > 0 ? cpu_total : 4000;
  out.capacity.cpu_millis_allocatable =
      static_cast<int>(out.capacity.cpu_millis_total * 0.85);
  out.capacity.mem_total_mb = input.host.mem_total_mb;
  out.capacity.mem_available_mb = input.host.mem_available_mb;
  out.capacity.mem_allocatable_mb =
      static_cast<int64_t>(static_cast<double>(input.host.mem_total_mb) * 0.8);
  out.capacity.max_managed_tasks = std::max(4, out.capacity.cpu_millis_total / 750);

  out.targets_ts_ms = input.host.ts_ms;
  out.targets.reserve(input.targets.size());
  for (const auto& target_in : input.targets) {
    SystemMonitorTargetOutput target_out;
    target_out.target_id = target_in.target_id;
    target_out.source_type = target_in.source_type;
    target_out.owner_executor_id = target_in.owner_executor_id;
    target_out.task_id = target_in.task_id;
    target_out.pid = target_in.pid;
    target_out.cgroup_path = target_in.cgroup_path;
    target_out.task_class = target_in.task_class;
    target_out.priority = target_in.priority;
    target_out.cpu_usage = target_in.cpu_usage;
    target_out.memory_current_mb = target_in.memory_current_mb;
    target_out.cpu_pressure = target_in.cpu_pressure;
    target_out.memory_pressure = target_in.memory_pressure;
    target_out.io_pressure = target_in.io_pressure;
    ClassifyTargetHeuristic(&target_out);
    out.targets.push_back(std::move(target_out));
  }

  return out;
}

}  // namespace maigent
