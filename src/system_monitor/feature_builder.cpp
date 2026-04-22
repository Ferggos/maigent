#include "feature_builder.h"

#include <algorithm>
#include <thread>
#include <unordered_set>
#include <utility>

namespace maigent {

namespace {

constexpr double kCpuEmaShortAlpha = 0.35;
constexpr double kCpuEmaLongAlpha = 0.10;

}  // namespace

SystemMonitorFeatureBuilder::SystemMonitorFeatureBuilder(
    TargetClassifier& target_classifier)
    : target_classifier_(target_classifier) {}

double SystemMonitorFeatureBuilder::ClampRatio(double value) {
  return std::clamp(value, 0.0, 1.0);
}

SystemMonitorModelInput SystemMonitorFeatureBuilder::BuildModelInput(
    const SystemMonitorRawSnapshot& raw_snapshot,
    const std::vector<SystemMonitorPressureHistorySample>& pressure_history) {
  SystemMonitorModelInput out;
  out.host.ts_ms = raw_snapshot.host.ts_ms;
  out.host.cpu_usage_pct = raw_snapshot.host.cpu_usage_pct;
  if (host_state_.initialized) {
    out.host.cpu_usage_delta =
        raw_snapshot.host.cpu_usage_pct - host_state_.prev_cpu_usage_pct;
  } else {
    out.host.cpu_usage_delta = 0.0;
  }

  if (!host_state_.initialized) {
    host_state_.cpu_usage_ema_short = raw_snapshot.host.cpu_usage_pct;
    host_state_.cpu_usage_ema_long = raw_snapshot.host.cpu_usage_pct;
  } else {
    host_state_.cpu_usage_ema_short =
        kCpuEmaShortAlpha * raw_snapshot.host.cpu_usage_pct +
        (1.0 - kCpuEmaShortAlpha) * host_state_.cpu_usage_ema_short;
    host_state_.cpu_usage_ema_long =
        kCpuEmaLongAlpha * raw_snapshot.host.cpu_usage_pct +
        (1.0 - kCpuEmaLongAlpha) * host_state_.cpu_usage_ema_long;
  }
  out.host.cpu_usage_ema_short = host_state_.cpu_usage_ema_short;
  out.host.cpu_usage_ema_long = host_state_.cpu_usage_ema_long;

  const int logical_cpu_count =
      std::max(1, static_cast<int>(std::thread::hardware_concurrency()));
  out.host.logical_cpu_count = logical_cpu_count;
  out.host.mem_total_mb = raw_snapshot.host.mem_total_mb;
  out.host.mem_available_mb = raw_snapshot.host.mem_available_mb;
  if (host_state_.initialized) {
    out.host.mem_available_delta_mb =
        raw_snapshot.host.mem_available_mb - host_state_.prev_mem_available_mb;
  } else {
    out.host.mem_available_delta_mb = 0;
  }
  out.host.swap_total_mb = raw_snapshot.host.swap_total_mb;
  out.host.swap_free_mb = raw_snapshot.host.swap_free_mb;
  out.host.load1 = raw_snapshot.host.load1;
  out.host.load_per_cpu =
      raw_snapshot.host.load1 / static_cast<double>(logical_cpu_count);

  if (raw_snapshot.host.mem_total_mb > 0) {
    out.host.mem_available_ratio = ClampRatio(
        static_cast<double>(raw_snapshot.host.mem_available_mb) /
        static_cast<double>(raw_snapshot.host.mem_total_mb));
    out.host.mem_used_ratio = ClampRatio(1.0 - out.host.mem_available_ratio);
  } else {
    out.host.mem_available_ratio = 1.0;
    out.host.mem_used_ratio = 0.0;
  }

  if (raw_snapshot.host.swap_total_mb > 0) {
    const double swap_free_ratio = ClampRatio(
        static_cast<double>(raw_snapshot.host.swap_free_mb) /
        static_cast<double>(raw_snapshot.host.swap_total_mb));
    out.host.swap_used_ratio = ClampRatio(1.0 - swap_free_ratio);
  }

  out.host.psi_cpu_some = raw_snapshot.host.psi_cpu_some;
  out.host.psi_mem_some = raw_snapshot.host.psi_mem_some;
  out.host.psi_io_some = raw_snapshot.host.psi_io_some;
  out.host.cpu_some_avg60 = raw_snapshot.host.psi_cpu_some_avg60;
  out.host.mem_some_avg60 = raw_snapshot.host.psi_mem_some_avg60;
  out.host.io_some_avg60 = raw_snapshot.host.psi_io_some_avg60;
  out.host.mem_full_avg10 = raw_snapshot.host.psi_mem_full_avg10;
  out.host.mem_full_avg60 = raw_snapshot.host.psi_mem_full_avg60;
  out.host.io_full_avg10 = raw_snapshot.host.psi_io_full_avg10;
  out.host.io_full_avg60 = raw_snapshot.host.psi_io_full_avg60;

  host_state_.initialized = true;
  host_state_.prev_cpu_usage_pct = raw_snapshot.host.cpu_usage_pct;
  host_state_.prev_mem_available_mb = raw_snapshot.host.mem_available_mb;

  out.targets.reserve(raw_snapshot.targets.size());
  const int64_t snapshot_ts_ms = raw_snapshot.host.ts_ms;
  std::unordered_set<std::string> seen_target_ids;
  seen_target_ids.reserve(raw_snapshot.targets.size());
  for (const auto& target : raw_snapshot.targets) {
    seen_target_ids.insert(target.target_id);
    UnifiedTarget model_target;
    model_target.target_id = target.target_id;
    model_target.kind = target.kind;
    model_target.source = target.source;
    model_target.owner_executor_id = target.owner_executor_id;
    model_target.task_id = target.task_id;
    model_target.pid = target.pid;
    model_target.cgroup_path = target.cgroup_path;
    model_target.task_class = target.task_class;
    model_target.priority = target.priority;
    model_target.cpu_usage = target.cpu_usage;
    model_target.memory_current_mb = target.memory_current_mb;
    model_target.cpu_pressure = target.cpu_pressure;
    model_target.memory_pressure = target.memory_pressure;
    model_target.io_pressure = target.io_pressure;

    TargetFeatureState& state = target_state_[target.target_id];
    if (state.first_seen_ts_ms <= 0) {
      state.first_seen_ts_ms =
          target.started_ms > 0 ? target.started_ms : raw_snapshot.host.ts_ms;
    } else if (target.started_ms > 0) {
      state.first_seen_ts_ms = target.started_ms;
    }

    if (state.initialized) {
      const double raw_cpu_delta_seconds =
          target.cpu_usage - state.prev_cpu_usage;
      if (state.prev_ts_ms > 0 && snapshot_ts_ms > state.prev_ts_ms) {
        const double interval_sec =
            static_cast<double>(snapshot_ts_ms - state.prev_ts_ms) / 1000.0;
        if (interval_sec > 0.0) {
          model_target.cpu_intensity =
              std::max(0.0, raw_cpu_delta_seconds) / interval_sec;
        }
      }

      model_target.memory_delta_mb =
          target.memory_current_mb - state.prev_memory_current_mb;
      model_target.memory_events_high_delta = std::max<int64_t>(
          0, target.memory_events_high - state.prev_memory_events_high);
      model_target.memory_events_oom_delta = std::max<int64_t>(
          0, target.memory_events_oom - state.prev_memory_events_oom);

      const int64_t delta_nr_periods =
          target.cpu_nr_periods - state.prev_cpu_nr_periods;
      const int64_t delta_nr_throttled =
          target.cpu_nr_throttled - state.prev_cpu_nr_throttled;
      if (delta_nr_periods > 0 && delta_nr_throttled >= 0) {
        model_target.cpu_throttled_ratio = ClampRatio(
            static_cast<double>(delta_nr_throttled) /
            static_cast<double>(delta_nr_periods));
      } else {
        model_target.cpu_throttled_ratio = ClampRatio(target.cpu_throttled_ratio);
      }
    } else {
      model_target.cpu_throttled_ratio = ClampRatio(target.cpu_throttled_ratio);
    }

    if (raw_snapshot.host.mem_total_mb > 0) {
      model_target.memory_ratio_of_host = ClampRatio(
          target.memory_current_mb /
          static_cast<double>(raw_snapshot.host.mem_total_mb));
    }
    model_target.age_sec = std::max<double>(
        0.0, static_cast<double>(raw_snapshot.host.ts_ms - state.first_seen_ts_ms) /
                 1000.0);

    target_classifier_.Classify(&model_target);
    out.targets.push_back(std::move(model_target));

    state.initialized = true;
    state.prev_ts_ms = snapshot_ts_ms;
    state.prev_cpu_usage = target.cpu_usage;
    state.prev_cpu_nr_periods = target.cpu_nr_periods;
    state.prev_cpu_nr_throttled = target.cpu_nr_throttled;
    state.prev_memory_current_mb = target.memory_current_mb;
    state.prev_memory_events_high = target.memory_events_high;
    state.prev_memory_events_oom = target.memory_events_oom;
  }

  for (auto it = target_state_.begin(); it != target_state_.end();) {
    if (seen_target_ids.find(it->first) == seen_target_ids.end()) {
      it = target_state_.erase(it);
    } else {
      ++it;
    }
  }

  out.pressure_history = pressure_history;
  return out;
}

}  // namespace maigent
