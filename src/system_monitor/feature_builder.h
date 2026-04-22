#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "maigent/system_monitor/model_contract.h"
#include "maigent/system_monitor/target_classifier.h"
#include "raw_collector.h"

namespace maigent {

class SystemMonitorFeatureBuilder {
 public:
  explicit SystemMonitorFeatureBuilder(TargetClassifier& target_classifier);

  SystemMonitorModelInput BuildModelInput(
      const SystemMonitorRawSnapshot& raw_snapshot,
      const std::vector<SystemMonitorPressureHistorySample>& pressure_history);

 private:
  struct HostFeatureState {
    bool initialized = false;
    double prev_cpu_usage_pct = 0.0;
    double cpu_usage_ema_short = 0.0;
    double cpu_usage_ema_long = 0.0;
    int64_t prev_mem_available_mb = 0;
  };

  struct TargetFeatureState {
    int64_t first_seen_ts_ms = 0;
    bool initialized = false;
    double prev_cpu_usage = 0.0;
    double prev_memory_current_mb = 0.0;
    int64_t prev_memory_events_high = 0;
    int64_t prev_memory_events_oom = 0;
  };

  static double ClampRatio(double value);

  HostFeatureState host_state_;
  std::unordered_map<std::string, TargetFeatureState> target_state_;
  TargetClassifier& target_classifier_;
};

}  // namespace maigent
