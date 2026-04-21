#include "maigent/planner/model_contract.h"

#include "maigent/common/target_model_proto.h"

namespace maigent {

PlannerModelInput ToPlannerModelInput(const PressureState& pressure,
                                      const ForecastState& forecast,
                                      const CapacityState& capacity,
                                      const TargetsState& targets,
                                      int active_tasks) {
  PlannerModelInput out;
  out.snapshot_ts_ms = pressure.ts_ms();
  out.active_tasks = active_tasks;

  out.pressure.ts_ms = pressure.ts_ms();
  out.pressure.cpu_usage_pct = pressure.cpu_usage_pct();
  out.pressure.mem_available_mb = pressure.mem_available_mb();
  out.pressure.load1 = pressure.load1();
  out.pressure.cpu_pressure_some = pressure.cpu_pressure_some();
  out.pressure.memory_pressure_some = pressure.memory_pressure_some();
  out.pressure.io_pressure_some = pressure.io_pressure_some();
  out.pressure.risk_level = pressure.risk_level();

  out.forecast.ts_ms = forecast.ts_ms();
  out.forecast.predicted_cpu_usage_pct = forecast.predicted_cpu_usage_pct();
  out.forecast.predicted_mem_available_mb = forecast.predicted_mem_available_mb();
  out.forecast.risk_level = forecast.risk_level();
  out.forecast.predictor = forecast.predictor();
  out.forecast.overload_probability = forecast.overload_probability();

  out.capacity.ts_ms = capacity.ts_ms();
  out.capacity.cpu_millis_total = capacity.cpu_millis_total();
  out.capacity.cpu_millis_allocatable = capacity.cpu_millis_allocatable();
  out.capacity.mem_total_mb = capacity.mem_total_mb();
  out.capacity.mem_available_mb = capacity.mem_available_mb();
  out.capacity.mem_allocatable_mb = capacity.mem_allocatable_mb();
  out.capacity.max_managed_tasks = capacity.max_managed_tasks();

  out.targets.reserve(targets.targets_size());
  for (const auto& target : targets.targets()) {
    out.targets.push_back(TargetFromProto(target));
  }

  return out;
}

}  // namespace maigent
