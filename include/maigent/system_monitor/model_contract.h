#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "maigent/common/target_model.h"
#include "maigent.pb.h"

namespace maigent {

struct SystemMonitorHostInput {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_total_mb = 0;
  int64_t mem_available_mb = 0;
  double load1 = 0.0;
  double psi_cpu_some = 0.0;
  double psi_mem_some = 0.0;
  double psi_io_some = 0.0;
};

struct SystemMonitorPressureHistorySample {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_available_mb = 0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
};

struct SystemMonitorModelInput {
  SystemMonitorHostInput host;
  std::vector<UnifiedTarget> targets;
  std::vector<SystemMonitorPressureHistorySample> pressure_history;
};

struct SystemMonitorPressureOutput {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_available_mb = 0;
  double load1 = 0.0;
  double cpu_pressure_some = 0.0;
  double memory_pressure_some = 0.0;
  double io_pressure_some = 0.0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
};

struct SystemMonitorForecastOutput {
  int64_t ts_ms = 0;
  double predicted_cpu_usage_pct = 0.0;
  int64_t predicted_mem_available_mb = 0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
  std::string predictor;
  double overload_probability = 0.0;
};

struct SystemMonitorCapacityOutput {
  int64_t ts_ms = 0;
  int cpu_millis_total = 0;
  int cpu_millis_allocatable = 0;
  int64_t mem_total_mb = 0;
  int64_t mem_available_mb = 0;
  int64_t mem_allocatable_mb = 0;
  int max_managed_tasks = 0;
};

struct SystemMonitorModelOutput {
  SystemMonitorPressureOutput pressure;
  SystemMonitorForecastOutput forecast;
  SystemMonitorCapacityOutput capacity;
  int64_t targets_ts_ms = 0;
  std::vector<UnifiedTarget> targets;
};

PressureState ToProtoPressureState(const SystemMonitorPressureOutput& pressure);
ForecastState ToProtoForecastState(const SystemMonitorForecastOutput& forecast);
CapacityState ToProtoCapacityState(const SystemMonitorCapacityOutput& capacity);
TargetsState ToProtoTargetsState(const SystemMonitorModelOutput& output);
SystemMonitorPressureHistorySample ToPressureHistorySampleFromOutput(
    const SystemMonitorPressureOutput& pressure);

}  // namespace maigent
