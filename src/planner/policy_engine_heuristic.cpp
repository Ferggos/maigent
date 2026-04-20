#include "maigent/planner/policy_engine.h"

#include <algorithm>

namespace maigent {

HeuristicPolicyEngine::HeuristicPolicyEngine(int max_actions_per_tick)
    : max_actions_per_tick_(std::max(1, max_actions_per_tick)) {}

std::vector<ControlAction> HeuristicPolicyEngine::Evaluate(
    const PressureState& pressure, const ForecastState& forecast,
    const CapacityState& capacity, const TargetsState& targets) {
  std::vector<ControlAction> actions;

  const bool high_pressure = pressure.risk_level() == RISK_HIGH ||
                             forecast.risk_level() == RISK_HIGH ||
                             pressure.memory_pressure_some() > 0.8 ||
                             pressure.cpu_usage_pct() > 90.0;
  if (!high_pressure) {
    return actions;
  }

  std::vector<const TargetInfo*> managed;
  std::vector<const TargetInfo*> external;
  managed.reserve(targets.targets_size());
  external.reserve(targets.targets_size());

  for (const auto& target : targets.targets()) {
    if (target.is_protected()) {
      continue;
    }
    if (target.source_type() == MANAGED_TASK) {
      managed.push_back(&target);
    } else {
      external.push_back(&target);
    }
  }

  std::sort(managed.begin(), managed.end(), [](const TargetInfo* a, const TargetInfo* b) {
    return a->memory_current_mb() > b->memory_current_mb();
  });
  std::sort(external.begin(), external.end(), [](const TargetInfo* a, const TargetInfo* b) {
    return a->memory_current_mb() > b->memory_current_mb();
  });

  for (const TargetInfo* target : managed) {
    if (static_cast<int>(actions.size()) >= max_actions_per_tick_) {
      break;
    }

    ControlAction action;
    action.set_target_type(TARGET_TASK);
    action.set_target_id(target->target_id());
    action.set_task_id(target->task_id());
    action.set_executor_id(target->owner_executor_id());
    action.set_pid(target->pid());
    action.set_cgroup_path(target->cgroup_path());
    action.set_action_type(RENICE);
    (*action.mutable_numeric_params())["nice"] = 15.0;
    action.set_reason("heuristic policy: high pressure on managed task");
    action.set_policy_id("heuristic_policy_v1");
    action.set_ts_ms(pressure.ts_ms());
    actions.push_back(std::move(action));
  }

  for (const TargetInfo* target : external) {
    if (static_cast<int>(actions.size()) >= max_actions_per_tick_) {
      break;
    }

    ControlAction action;
    action.set_target_type(target->cgroup_path().empty() ? TARGET_PROCESS : TARGET_CGROUP);
    action.set_target_id(target->target_id());
    action.set_task_id(target->task_id());
    action.set_executor_id(target->owner_executor_id());
    action.set_pid(target->pid());
    action.set_cgroup_path(target->cgroup_path());

    if (!target->cgroup_path().empty()) {
      action.set_action_type(SET_CPU_WEIGHT);
      (*action.mutable_numeric_params())["cpu_weight"] = 50.0;
    } else {
      action.set_action_type(RENICE);
      (*action.mutable_numeric_params())["nice"] = 12.0;
    }

    action.set_reason("heuristic policy: high pressure on external target");
    action.set_policy_id("heuristic_policy_v1");
    action.set_ts_ms(pressure.ts_ms());
    actions.push_back(std::move(action));
  }

  (void)capacity;
  return actions;
}

}  // namespace maigent
