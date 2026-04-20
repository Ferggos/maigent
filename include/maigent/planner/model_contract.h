#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "maigent.pb.h"

namespace maigent {

struct PlannerPressureInput {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_available_mb = 0;
  double load1 = 0.0;
  double cpu_pressure_some = 0.0;
  double memory_pressure_some = 0.0;
  double io_pressure_some = 0.0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
};

struct PlannerForecastInput {
  int64_t ts_ms = 0;
  double predicted_cpu_usage_pct = 0.0;
  int64_t predicted_mem_available_mb = 0;
  RiskLevel risk_level = RISK_LEVEL_UNSPECIFIED;
  std::string predictor;
  double overload_probability = 0.0;
};

struct PlannerCapacityInput {
  int64_t ts_ms = 0;
  int cpu_millis_total = 0;
  int cpu_millis_allocatable = 0;
  int64_t mem_total_mb = 0;
  int64_t mem_available_mb = 0;
  int64_t mem_allocatable_mb = 0;
  int max_managed_tasks = 0;
};

struct PlannerTargetInput {
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

struct PlannerModelInput {
  int64_t snapshot_ts_ms = 0;
  PlannerPressureInput pressure;
  PlannerForecastInput forecast;
  PlannerCapacityInput capacity;
  std::vector<PlannerTargetInput> targets;
  int active_tasks = 0;
};

struct PlannerDecisionAction {
  TargetType target_type = TARGET_TYPE_UNSPECIFIED;
  std::string target_id;
  std::string task_id;
  std::string executor_id;
  int pid = 0;
  std::string cgroup_path;
  ControlActionType action_type = CONTROL_ACTION_UNSPECIFIED;
  std::unordered_map<std::string, double> numeric_params;
  std::unordered_map<std::string, std::string> string_params;
  std::string reason;
  std::string policy_id;
  int64_t ts_ms = 0;
};

struct PlannerModelOutput {
  int64_t decision_ts_ms = 0;
  std::string strategy_id;
  std::vector<PlannerDecisionAction> actions;
};

PlannerModelInput ToPlannerModelInput(const PressureState& pressure,
                                      const ForecastState& forecast,
                                      const CapacityState& capacity,
                                      const TargetsState& targets,
                                      int active_tasks);

ControlAction ToProtoControlAction(const PlannerDecisionAction& action);

}  // namespace maigent
