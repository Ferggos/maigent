#include "maigent/system_monitor/system_monitor_model.h"

#include <algorithm>
#include <vector>

#include "maigent/common/time_utils.h"

namespace maigent {

namespace {

struct TargetFeatureSummary {
  double max_cpu_intensity = 0.0;
  double total_positive_memory_delta_mb = 0.0;
  double max_cpu_throttled_ratio = 0.0;
  int64_t memory_events_high_delta = 0;
  int64_t memory_events_oom_delta = 0;
};

TargetFeatureSummary SummarizeTargets(const std::vector<UnifiedTarget>& targets) {
  TargetFeatureSummary out;
  for (const auto& target : targets) {
    out.max_cpu_intensity = std::max(out.max_cpu_intensity, target.cpu_usage_delta);
    if (target.memory_delta_mb > 0.0) {
      out.total_positive_memory_delta_mb += target.memory_delta_mb;
    }
    out.max_cpu_throttled_ratio =
        std::max(out.max_cpu_throttled_ratio, target.cpu_throttled_ratio);
    out.memory_events_high_delta += target.memory_events_high_delta;
    out.memory_events_oom_delta += target.memory_events_oom_delta;
  }
  return out;
}

SystemMonitorForecastOutput PredictForecastHeuristic(
    const SystemMonitorHostInput& host,
    const SystemMonitorPressureOutput& latest,
    const std::vector<SystemMonitorPressureHistorySample>& pressure_history,
    const TargetFeatureSummary& target_summary) {
  // latest is provided explicitly; history contains previous samples only.
  double cpu_sum = latest.cpu_usage_pct;
  double mem_sum = static_cast<double>(latest.mem_available_mb);
  int n = 1;
  for (auto it = pressure_history.rbegin(); it != pressure_history.rend() && n < 6;
       ++it) {
    cpu_sum += it->cpu_usage_pct;
    mem_sum += static_cast<double>(it->mem_available_mb);
    ++n;
  }

  const double cpu_avg = cpu_sum / static_cast<double>(n);
  const int64_t mem_avg = static_cast<int64_t>(mem_sum / static_cast<double>(n));

  SystemMonitorForecastOutput out;
  out.ts_ms = NowMs();
  const double cpu_ema_gap =
      std::max(0.0, host.cpu_usage_ema_short - host.cpu_usage_ema_long);
  const double cpu_delta_up = std::max(0.0, host.cpu_usage_delta);
  const double psi_cpu_boost =
      host.cpu_some_avg60 * 2.0 + host.io_some_avg60 * 1.0;
  const double target_cpu_boost = target_summary.max_cpu_intensity * 4.0;
  out.predicted_cpu_usage_pct = std::min(
      100.0, cpu_avg * 1.05 + cpu_ema_gap * 0.45 + cpu_delta_up * 0.2 +
                 psi_cpu_boost + target_cpu_boost);

  const double mem_drop_trend =
      static_cast<double>(std::max<int64_t>(0, -host.mem_available_delta_mb));
  const double target_mem_growth_penalty =
      target_summary.total_positive_memory_delta_mb * 0.4;
  const double mem_pressure_penalty = host.mem_some_avg60 * 128.0 +
                                      host.mem_full_avg60 * 192.0 +
                                      host.swap_used_ratio * 96.0;
  const double mem_events_penalty =
      static_cast<double>(target_summary.memory_events_high_delta) * 4.0 +
      static_cast<double>(target_summary.memory_events_oom_delta) * 48.0;
  out.predicted_mem_available_mb = std::max<int64_t>(
      0, static_cast<int64_t>(static_cast<double>(mem_avg) - 64.0 -
                              mem_drop_trend * 0.5 - target_mem_growth_penalty -
                              mem_pressure_penalty - mem_events_penalty));
  out.predictor = "heuristic_v1";

  if (out.predicted_cpu_usage_pct >= 85.0 || out.predicted_mem_available_mb < 768 ||
      target_summary.memory_events_oom_delta > 0 || host.mem_full_avg60 > 0.1) {
    out.risk_level = RISK_HIGH;
    out.overload_probability = 0.85;
  } else if (out.predicted_cpu_usage_pct >= 70.0 ||
             out.predicted_mem_available_mb < 1536 || host.mem_some_avg60 > 0.3 ||
             target_summary.memory_events_high_delta > 0) {
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
  const TargetFeatureSummary target_summary = SummarizeTargets(input.targets);

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
  const bool severe_target_pressure =
      target_summary.memory_events_oom_delta > 0 ||
      target_summary.max_cpu_throttled_ratio > 0.7 ||
      (target_summary.total_positive_memory_delta_mb > 512.0 &&
       input.host.mem_available_ratio < 0.18);
  const bool moderate_target_pressure =
      target_summary.memory_events_high_delta > 0 ||
      target_summary.max_cpu_intensity > 1.0 ||
      target_summary.max_cpu_throttled_ratio > 0.35 ||
      target_summary.total_positive_memory_delta_mb > 256.0;

  if (cpu_fast_signal >= 85.0 || input.host.mem_available_mb < 768 ||
      input.host.mem_available_ratio < 0.08 || input.host.psi_mem_some > 1.0 ||
      input.host.mem_some_avg60 > 0.8 || input.host.mem_full_avg60 > 0.1 ||
      input.host.swap_used_ratio > 0.7 || severe_target_pressure) {
    out.pressure.risk_level = RISK_HIGH;
  } else if (cpu_slow_signal >= 70.0 || input.host.mem_available_mb < 1536 ||
             input.host.mem_available_ratio < 0.16 ||
             input.host.psi_mem_some > 0.3 || input.host.mem_some_avg60 > 0.3 ||
             input.host.mem_full_avg10 > 0.05 || input.host.swap_used_ratio > 0.4 ||
             moderate_target_pressure) {
    out.pressure.risk_level = RISK_MED;
  } else {
    out.pressure.risk_level = RISK_LOW;
  }

  out.forecast = PredictForecastHeuristic(input.host, out.pressure,
                                          input.pressure_history,
                                          target_summary);

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
