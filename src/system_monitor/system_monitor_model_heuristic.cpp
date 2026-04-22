#include "maigent/system_monitor/system_monitor_model.h"

#include <algorithm>
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

  const double cpu_fast_signal =
      std::max(input.host.cpu_usage_pct, input.host.cpu_usage_ema_short);
  const double cpu_slow_signal =
      std::max(input.host.cpu_usage_pct, input.host.cpu_usage_ema_long);
  if (cpu_fast_signal >= 85.0 || input.host.mem_available_mb < 768 ||
      input.host.mem_available_ratio < 0.08 || input.host.psi_mem_some > 1.0 ||
      input.host.mem_some_avg60 > 0.8) {
    out.pressure.risk_level = RISK_HIGH;
  } else if (cpu_slow_signal >= 70.0 || input.host.mem_available_mb < 1536 ||
             input.host.mem_available_ratio < 0.16 ||
             input.host.psi_mem_some > 0.3 || input.host.mem_some_avg60 > 0.3) {
    out.pressure.risk_level = RISK_MED;
  } else {
    out.pressure.risk_level = RISK_LOW;
  }

  out.forecast = PredictForecastHeuristic(out.pressure, input.pressure_history);

  out.capacity.ts_ms = input.host.ts_ms;
  const int cpu_total = std::max(1, input.host.logical_cpu_count) * 1000;
  out.capacity.cpu_millis_total = cpu_total > 0 ? cpu_total : 4000;
  out.capacity.cpu_millis_allocatable =
      static_cast<int>(out.capacity.cpu_millis_total * 0.85);
  out.capacity.mem_total_mb = input.host.mem_total_mb;
  out.capacity.mem_available_mb = input.host.mem_available_mb;
  out.capacity.mem_allocatable_mb =
      static_cast<int64_t>(static_cast<double>(input.host.mem_total_mb) * 0.8);
  out.capacity.max_managed_tasks = std::max(4, out.capacity.cpu_millis_total / 750);

  out.targets_ts_ms = input.host.ts_ms;
  out.targets = input.targets;

  return out;
}

}  // namespace maigent
