#include "maigent/system_monitor/system_monitor_model.h"

#include <algorithm>
#include <array>
#include <string>
#include <vector>

#include "maigent/common/time_utils.h"

namespace maigent {

namespace {

enum class ResourceBottleneck {
  kNone = 0,
  kCpu = 1,
  kMemory = 2,
  kIo = 3,
};

struct TargetFeatureSummary {
  double max_cpu_intensity = 0.0;
  double total_positive_memory_delta_mb = 0.0;
  double max_cpu_throttled_ratio = 0.0;
  int64_t memory_events_high_delta = 0;
  int64_t memory_events_oom_delta = 0;
};

struct ResourceScores {
  double cpu = 0.0;
  double memory = 0.0;
  double io = 0.0;
  double host_risk = 0.0;
  ResourceBottleneck primary = ResourceBottleneck::kNone;
  ResourceBottleneck secondary = ResourceBottleneck::kNone;
};

bool IncludeTargetInBaselineSummary(const UnifiedTarget& target) {
  return target.source == TargetSource::kManagedTask && !target.is_protected;
}

double Clamp01(double value) { return std::clamp(value, 0.0, 1.0); }

double ScoreFromThresholds(double value, double moderate, double high) {
  if (high <= moderate || moderate <= 0.0) {
    return Clamp01(value > 0.0 ? value / std::max(1e-9, high) : 0.0);
  }
  if (value <= 0.0) {
    return 0.0;
  }
  if (value < moderate) {
    return 0.5 * Clamp01(value / moderate);
  }
  if (value >= high) {
    return 1.0;
  }
  return 0.5 + 0.5 * Clamp01((value - moderate) / (high - moderate));
}

double LowValueScore(double value, double moderate_floor, double high_floor) {
  if (high_floor >= moderate_floor || moderate_floor <= 0.0) {
    return Clamp01(1.0 - value / std::max(1e-9, moderate_floor));
  }
  if (value <= high_floor) {
    return 1.0;
  }
  if (value < moderate_floor) {
    return 0.5 +
           0.5 * Clamp01((moderate_floor - value) / (moderate_floor - high_floor));
  }
  const double relaxed_floor = moderate_floor * 2.0;
  if (value >= relaxed_floor) {
    return 0.0;
  }
  return 0.5 * Clamp01((relaxed_floor - value) / moderate_floor);
}

const char* BottleneckTag(ResourceBottleneck bottleneck) {
  switch (bottleneck) {
    case ResourceBottleneck::kCpu:
      return "cpu";
    case ResourceBottleneck::kMemory:
      return "memory";
    case ResourceBottleneck::kIo:
      return "io";
    case ResourceBottleneck::kNone:
    default:
      return "none";
  }
}

ResourceScores ComputeHostResourceScores(const SystemMonitorHostInput& host) {
  ResourceScores out;

  // CPU score: current load, short-term trend, per-core pressure and CPU PSI.
  const double cpu_usage_signal = ScoreFromThresholds(
      std::max(host.cpu_usage_pct, host.cpu_usage_ema_short), 70.0, 85.0);
  const double cpu_trend_signal = std::max(
      ScoreFromThresholds(std::max(0.0, host.cpu_usage_delta), 8.0, 20.0),
      ScoreFromThresholds(std::max(0.0, host.cpu_usage_ema_short - host.cpu_usage_ema_long),
                          4.0, 12.0));
  const double load_signal = ScoreFromThresholds(host.load_per_cpu, 0.90, 1.40);
  const double cpu_psi_signal = std::max(
      ScoreFromThresholds(host.psi_cpu_some, 0.4, 1.2),
      ScoreFromThresholds(host.cpu_some_avg60, 0.25, 0.9));
  out.cpu = Clamp01(0.42 * cpu_usage_signal + 0.20 * cpu_trend_signal +
                    0.23 * load_signal + 0.15 * cpu_psi_signal);

  // Memory score: availability/headroom, depletion trend, swap and memory PSI.
  const double mem_available_ratio_signal =
      ScoreFromThresholds(1.0 - host.mem_available_ratio, 0.84, 0.92);
  const double mem_available_abs_signal =
      LowValueScore(static_cast<double>(host.mem_available_mb), 1536.0, 768.0);
  const double mem_drop_signal = ScoreFromThresholds(
      static_cast<double>(std::max<int64_t>(0, -host.mem_available_delta_mb)), 128.0,
      512.0);
  const double swap_signal = ScoreFromThresholds(host.swap_used_ratio, 0.40, 0.70);
  const double mem_some_signal = std::max(
      ScoreFromThresholds(host.psi_mem_some, 0.3, 1.0),
      ScoreFromThresholds(host.mem_some_avg60, 0.3, 0.8));
  const double mem_full_signal = std::max(
      ScoreFromThresholds(host.mem_full_avg10, 0.05, 0.10),
      ScoreFromThresholds(host.mem_full_avg60, 0.05, 0.10));
  out.memory =
      Clamp01(0.22 * mem_available_ratio_signal + 0.18 * mem_available_abs_signal +
              0.16 * mem_drop_signal + 0.14 * swap_signal +
              0.12 * mem_some_signal + 0.18 * mem_full_signal);

  // IO score: sustained IO PSI with stronger weight for full pressure.
  const double io_some_signal = std::max(
      ScoreFromThresholds(host.psi_io_some, 0.4, 1.3),
      ScoreFromThresholds(host.io_some_avg60, 0.3, 1.0));
  const double io_full_signal = std::max(
      ScoreFromThresholds(host.io_full_avg10, 0.05, 0.12),
      ScoreFromThresholds(host.io_full_avg60, 0.05, 0.12));
  out.io = Clamp01(0.38 * io_some_signal + 0.62 * io_full_signal);

  std::array<std::pair<ResourceBottleneck, double>, 3> ranked = {
      std::pair<ResourceBottleneck, double>{ResourceBottleneck::kCpu, out.cpu},
      std::pair<ResourceBottleneck, double>{ResourceBottleneck::kMemory, out.memory},
      std::pair<ResourceBottleneck, double>{ResourceBottleneck::kIo, out.io},
  };
  std::sort(ranked.begin(), ranked.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });
  out.primary = ranked[0].second < 0.20 ? ResourceBottleneck::kNone : ranked[0].first;
  out.secondary =
      ranked[1].second < 0.15 ? ResourceBottleneck::kNone : ranked[1].first;
  out.host_risk = Clamp01(0.60 * ranked[0].second + 0.30 * ranked[1].second +
                          0.10 * ranked[2].second);
  return out;
}

double ComputeTargetPressureScore(const TargetFeatureSummary& summary) {
  const double cpu_signal =
      ScoreFromThresholds(summary.max_cpu_intensity, 0.8, 1.8);
  const double mem_growth_signal =
      ScoreFromThresholds(summary.total_positive_memory_delta_mb, 256.0, 768.0);
  const double throttle_signal =
      ScoreFromThresholds(summary.max_cpu_throttled_ratio, 0.35, 0.70);
  const double mem_events_signal = std::max(
      summary.memory_events_oom_delta > 0 ? 1.0 : 0.0,
      ScoreFromThresholds(static_cast<double>(summary.memory_events_high_delta), 1.0,
                          4.0));
  double score = Clamp01(0.24 * cpu_signal + 0.34 * mem_growth_signal +
                         0.20 * throttle_signal + 0.22 * mem_events_signal);
  if (summary.memory_events_oom_delta > 0) {
    score = std::max(score, 0.85);
  }
  return score;
}

RiskLevel RiskLevelFromScore(double risk_score) {
  if (risk_score >= 0.82) {
    return RISK_HIGH;
  }
  if (risk_score >= 0.50) {
    return RISK_MED;
  }
  return RISK_LOW;
}

TargetFeatureSummary SummarizeTargets(const std::vector<UnifiedTarget>& targets) {
  TargetFeatureSummary out;
  for (const auto& target : targets) {
    if (!IncludeTargetInBaselineSummary(target)) {
      continue;
    }
    out.max_cpu_intensity = std::max(out.max_cpu_intensity, target.cpu_intensity);
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
    const TargetFeatureSummary& target_summary,
    const ResourceScores& resource_scores,
    double current_risk_score,
    double target_pressure_score) {
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
  const double cpu_delta_up = std::max(0.0, host.cpu_usage_delta);
  const double cpu_ema_gap =
      std::max(0.0, host.cpu_usage_ema_short - host.cpu_usage_ema_long);
  const double cpu_trend_score = std::max(
      ScoreFromThresholds(cpu_delta_up, 8.0, 20.0),
      ScoreFromThresholds(cpu_ema_gap, 4.0, 12.0));
  const double memory_drop_score = ScoreFromThresholds(
      static_cast<double>(std::max<int64_t>(0, -host.mem_available_delta_mb)), 128.0,
      512.0);
  const double memory_sustained_score = std::max(
      ScoreFromThresholds(host.mem_some_avg60, 0.3, 0.8),
      ScoreFromThresholds(host.mem_full_avg60, 0.05, 0.10));
  const double io_sustained_score = std::max(
      ScoreFromThresholds(host.io_some_avg60, 0.3, 1.0),
      ScoreFromThresholds(host.io_full_avg60, 0.05, 0.12));

  // t+3 reacts stronger to short trend, t+10 stronger to sustained conditions.
  const double risk_t_plus_3 = Clamp01(
      0.62 * current_risk_score + 0.18 * cpu_trend_score +
      0.10 * std::max(memory_drop_score, io_sustained_score) +
      0.10 * target_pressure_score);
  const double risk_t_plus_10 = Clamp01(
      0.45 * current_risk_score + 0.18 * memory_sustained_score +
      0.14 * io_sustained_score +
      0.13 * ScoreFromThresholds(host.cpu_some_avg60, 0.25, 0.9) +
      0.10 * target_pressure_score);
  const double future_risk_score =
      Clamp01(0.55 * risk_t_plus_3 + 0.45 * risk_t_plus_10);

  out.predicted_cpu_usage_pct = std::clamp(
      cpu_avg * 0.70 + (100.0 * risk_t_plus_3) * 0.30 +
          cpu_delta_up * 0.10 + cpu_ema_gap * 0.15 +
          target_summary.max_cpu_intensity * 4.0,
      0.0, 100.0);

  const double mem_drop_trend =
      static_cast<double>(std::max<int64_t>(0, -host.mem_available_delta_mb));
  const double target_mem_growth_penalty =
      target_summary.total_positive_memory_delta_mb * 0.35;
  const double mem_pressure_penalty =
      (memory_sustained_score * 192.0) + (host.swap_used_ratio * 96.0);
  const double mem_events_penalty =
      static_cast<double>(target_summary.memory_events_high_delta) * 4.0 +
      static_cast<double>(target_summary.memory_events_oom_delta) * 64.0;
  out.predicted_mem_available_mb = std::max<int64_t>(
      0, static_cast<int64_t>(static_cast<double>(mem_avg) - 64.0 -
                              mem_drop_trend * 0.5 - target_mem_growth_penalty -
                              mem_pressure_penalty - mem_events_penalty));
  out.predictor =
      std::string("heuristic_v2_") + BottleneckTag(resource_scores.primary);

  double adjusted_future_risk = future_risk_score;
  if (resource_scores.host_risk < 0.35) {
    adjusted_future_risk = std::min(adjusted_future_risk, 0.72);
  }
  if (out.predicted_cpu_usage_pct >= 88.0 || out.predicted_mem_available_mb < 768) {
    adjusted_future_risk = std::max(adjusted_future_risk, 0.85);
  } else if (out.predicted_cpu_usage_pct >= 72.0 ||
             out.predicted_mem_available_mb < 1536) {
    adjusted_future_risk = std::max(adjusted_future_risk, 0.55);
  }

  out.risk_level = RiskLevelFromScore(adjusted_future_risk);
  out.overload_probability =
      std::clamp(0.05 + adjusted_future_risk * 0.90, 0.05, 0.98);

  return out;
}

}  // namespace

SystemMonitorModelOutput HeuristicSystemMonitorModel::Evaluate(
    const SystemMonitorModelInput& input) {
  SystemMonitorModelOutput out;
  const TargetFeatureSummary target_summary = SummarizeTargets(input.targets);
  const ResourceScores resource_scores = ComputeHostResourceScores(input.host);
  const double target_pressure_score = ComputeTargetPressureScore(target_summary);

  out.pressure.ts_ms = input.host.ts_ms;
  out.pressure.cpu_usage_pct = input.host.cpu_usage_pct;
  out.pressure.mem_available_mb = input.host.mem_available_mb;
  out.pressure.load1 = input.host.load1;
  out.pressure.cpu_pressure_some = input.host.psi_cpu_some;
  out.pressure.memory_pressure_some = input.host.psi_mem_some;
  out.pressure.io_pressure_some = input.host.psi_io_some;

  const double target_weight = 0.10 + 0.35 * resource_scores.host_risk;
  double current_risk_score =
      Clamp01(resource_scores.host_risk + target_pressure_score * target_weight);
  if (resource_scores.host_risk < 0.35) {
    current_risk_score = std::min(current_risk_score, 0.68);
  }
  out.pressure.risk_level = RiskLevelFromScore(current_risk_score);

  out.forecast = PredictForecastHeuristic(input.host, out.pressure,
                                          input.pressure_history, target_summary,
                                          resource_scores, current_risk_score,
                                          target_pressure_score);

  out.capacity.ts_ms = input.host.ts_ms;
  const int cpu_total = std::max(1, input.host.logical_cpu_count) * 1000;
  out.capacity.cpu_millis_total = cpu_total > 0 ? cpu_total : 4000;
  double cpu_alloc_factor = std::clamp(0.90 - 0.55 * current_risk_score, 0.35, 0.90);
  if (resource_scores.primary == ResourceBottleneck::kCpu) {
    cpu_alloc_factor *= 0.90;
  } else if (resource_scores.secondary == ResourceBottleneck::kCpu) {
    cpu_alloc_factor *= 0.95;
  }
  out.capacity.cpu_millis_allocatable = static_cast<int>(
      static_cast<double>(out.capacity.cpu_millis_total) * cpu_alloc_factor);
  out.capacity.mem_total_mb = input.host.mem_total_mb;
  out.capacity.mem_available_mb = input.host.mem_available_mb;
  double mem_alloc_factor = std::clamp(0.88 - 0.60 * current_risk_score, 0.30, 0.88);
  if (resource_scores.primary == ResourceBottleneck::kMemory) {
    mem_alloc_factor *= 0.85;
  } else if (resource_scores.secondary == ResourceBottleneck::kMemory) {
    mem_alloc_factor *= 0.92;
  }
  out.capacity.mem_allocatable_mb = std::min<int64_t>(
      input.host.mem_available_mb,
      static_cast<int64_t>(static_cast<double>(input.host.mem_total_mb) *
                           mem_alloc_factor));

  const int base_tasks = std::max(2, out.capacity.cpu_millis_total / 750);
  double task_budget_factor =
      std::clamp(1.0 - 0.70 * current_risk_score, 0.20, 1.0);
  if (out.forecast.risk_level == RISK_HIGH) {
    task_budget_factor *= 0.75;
  } else if (out.forecast.risk_level == RISK_MED) {
    task_budget_factor *= 0.90;
  }
  if (resource_scores.primary == ResourceBottleneck::kMemory &&
      input.host.mem_available_mb > 0) {
    const int64_t memory_bound = std::max<int64_t>(1, input.host.mem_available_mb / 512);
    out.capacity.max_managed_tasks = std::max<int>(
        1, std::min<int>(static_cast<int>(base_tasks * task_budget_factor),
                         static_cast<int>(memory_bound)));
  } else {
    out.capacity.max_managed_tasks =
        std::max(1, static_cast<int>(base_tasks * task_budget_factor));
  }

  out.targets_ts_ms = input.host.ts_ms;
  out.targets = input.targets;

  return out;
}

}  // namespace maigent
