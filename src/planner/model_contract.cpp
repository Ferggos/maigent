#include "maigent/planner/model_contract.h"

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
    PlannerTargetInput target_out;
    target_out.target_id = target.target_id();
    target_out.source_type = target.source_type();
    target_out.owner_executor_id = target.owner_executor_id();
    target_out.task_id = target.task_id();
    target_out.pid = target.pid();
    target_out.cgroup_path = target.cgroup_path();
    target_out.task_class = target.task_class();
    target_out.priority = target.priority();
    target_out.is_protected = target.is_protected();
    target_out.allowed_actions.reserve(target.allowed_actions_size());
    for (int i = 0; i < target.allowed_actions_size(); ++i) {
      target_out.allowed_actions.push_back(target.allowed_actions(i));
    }
    target_out.cpu_usage = target.cpu_usage();
    target_out.memory_current_mb = target.memory_current_mb();
    target_out.cpu_pressure = target.cpu_pressure();
    target_out.memory_pressure = target.memory_pressure();
    target_out.io_pressure = target.io_pressure();
    out.targets.push_back(std::move(target_out));
  }

  return out;
}

ControlAction ToProtoControlAction(const PlannerDecisionAction& action) {
  ControlAction out;
  out.set_target_type(action.target_type);
  out.set_target_id(action.target_id);
  out.set_task_id(action.task_id);
  out.set_executor_id(action.executor_id);
  out.set_pid(action.pid);
  out.set_cgroup_path(action.cgroup_path);
  out.set_action_type(action.action_type);
  for (const auto& [key, value] : action.numeric_params) {
    (*out.mutable_numeric_params())[key] = value;
  }
  for (const auto& [key, value] : action.string_params) {
    (*out.mutable_string_params())[key] = value;
  }
  out.set_reason(action.reason);
  out.set_policy_id(action.policy_id);
  out.set_ts_ms(action.ts_ms);
  return out;
}

}  // namespace maigent
