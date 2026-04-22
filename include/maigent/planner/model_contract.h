#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "maigent/common/target_model.h"
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

struct PlannerModelInput {
  int64_t snapshot_ts_ms = 0;
  PlannerPressureInput pressure;
  PlannerForecastInput forecast;
  PlannerCapacityInput capacity;
  std::vector<UnifiedTarget> targets;
  int active_tasks = 0;
};

enum class PlannerInterventionType {
  kUnspecified = 0,
  kDeprioritize = 1,
  kLimitCpuShare = 2,
  kLimitCpuQuota = 3,
  kLimitMemorySoft = 4,
  kLimitMemoryHard = 5,
  kPause = 6,
  kResume = 7,
  kTerminate = 8,
};

struct PlannerDecisionTarget {
  std::string target_id;
};

struct PlannerIntervention {
  PlannerDecisionTarget target;
  PlannerInterventionType intervention_type = PlannerInterventionType::kUnspecified;
  std::unordered_map<std::string, double> numeric_params;
  std::unordered_map<std::string, std::string> string_params;
  std::string rationale;
  int apply_order = 0;
};

struct PlannerModelOutput {
  int64_t decision_ts_ms = 0;
  std::string strategy_id;
  std::vector<PlannerIntervention> interventions;
};

PlannerModelInput ToPlannerModelInput(const PressureState& pressure,
                                      const ForecastState& forecast,
                                      const CapacityState& capacity,
                                      const TargetsState& targets,
                                      int active_tasks);

}  // namespace maigent
