#pragma once

#include <cstdint>
#include <string>
#include <vector>

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

struct SystemMonitorTargetInput {
  std::string target_id;
  TargetSourceType source_type = TARGET_SOURCE_UNSPECIFIED;
  std::string owner_executor_id;
  std::string task_id;
  int pid = 0;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  double cpu_usage = 0.0;
  double memory_current_mb = 0.0;
  double cpu_pressure = 0.0;
  double memory_pressure = 0.0;
  double io_pressure = 0.0;
};

struct SystemMonitorPressureHistorySample {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_available_mb = 0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
};

struct SystemMonitorModelInput {
  SystemMonitorHostInput host;
  std::vector<SystemMonitorTargetInput> targets;
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

struct SystemMonitorTargetOutput {
  std::string target_id;
  TargetSourceType source_type = TARGET_SOURCE_UNSPECIFIED;
  std::string owner_executor_id;
  std::string task_id;
  int pid = 0;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  bool is_protected = false;
  std::vector<ControlActionType> allowed_actions;
  double cpu_usage = 0.0;
  double memory_current_mb = 0.0;
  double cpu_pressure = 0.0;
  double memory_pressure = 0.0;
  double io_pressure = 0.0;
};

struct SystemMonitorModelOutput {
  SystemMonitorPressureOutput pressure;
  SystemMonitorForecastOutput forecast;
  SystemMonitorCapacityOutput capacity;
  int64_t targets_ts_ms = 0;
  std::vector<SystemMonitorTargetOutput> targets;
};

PressureState ToProtoPressureState(const SystemMonitorPressureOutput& pressure);
ForecastState ToProtoForecastState(const SystemMonitorForecastOutput& forecast);
CapacityState ToProtoCapacityState(const SystemMonitorCapacityOutput& capacity);
TargetsState ToProtoTargetsState(const SystemMonitorModelOutput& output);
SystemMonitorPressureHistorySample ToPressureHistorySample(
    const SystemMonitorPressureOutput& pressure);

}  // namespace maigent
