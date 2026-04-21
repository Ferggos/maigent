#include "maigent/planner/planner_model.h"

#include <algorithm>

#include "maigent/common/target_model_proto.h"

namespace maigent {

HeuristicPlannerModel::HeuristicPlannerModel(int max_actions_per_tick)
    : max_actions_per_tick_(std::max(1, max_actions_per_tick)) {}

PlannerModelOutput HeuristicPlannerModel::Evaluate(const PlannerModelInput& input) {
  PlannerModelOutput out;
  out.decision_ts_ms = input.snapshot_ts_ms;
  out.strategy_id = "heuristic_policy_v1";

  const bool high_pressure = input.pressure.risk_level == RISK_HIGH ||
                             input.forecast.risk_level == RISK_HIGH ||
                             input.pressure.memory_pressure_some > 0.8 ||
                             input.pressure.cpu_usage_pct > 90.0;
  if (!high_pressure) {
    return out;
  }

  std::vector<const UnifiedTarget*> managed;
  std::vector<const UnifiedTarget*> external;
  managed.reserve(input.targets.size());
  external.reserve(input.targets.size());

  for (const auto& target : input.targets) {
    if (target.is_protected) {
      continue;
    }
    if (target.source == TargetSource::kManagedTask) {
      managed.push_back(&target);
    } else {
      external.push_back(&target);
    }
  }

  std::sort(managed.begin(), managed.end(),
            [](const UnifiedTarget* a, const UnifiedTarget* b) {
              return a->memory_current_mb > b->memory_current_mb;
            });
  std::sort(external.begin(), external.end(),
            [](const UnifiedTarget* a, const UnifiedTarget* b) {
              return a->memory_current_mb > b->memory_current_mb;
            });

  out.actions.reserve(static_cast<size_t>(max_actions_per_tick_));
  for (const UnifiedTarget* target : managed) {
    if (static_cast<int>(out.actions.size()) >= max_actions_per_tick_) {
      break;
    }

    PlannerDecisionAction action;
    action.target_type = ToProtoTargetType(target->kind);
    if (action.target_type == TARGET_TYPE_UNSPECIFIED) {
      action.target_type = TARGET_TASK;
    }
    action.target_id = target->target_id;
    action.task_id = target->task_id;
    action.executor_id = target->owner_executor_id;
    action.pid = target->pid;
    action.cgroup_path = target->cgroup_path;
    action.action_type = RENICE;
    action.numeric_params["nice"] = 15.0;
    action.reason = "heuristic policy: high pressure on managed task";
    action.policy_id = "heuristic_policy_v1";
    action.ts_ms = input.pressure.ts_ms;
    out.actions.push_back(std::move(action));
  }

  for (const UnifiedTarget* target : external) {
    if (static_cast<int>(out.actions.size()) >= max_actions_per_tick_) {
      break;
    }

    PlannerDecisionAction action;
    action.target_type = ToProtoTargetType(target->kind);
    if (action.target_type == TARGET_TYPE_UNSPECIFIED) {
      action.target_type = target->cgroup_path.empty() ? TARGET_PROCESS
                                                       : TARGET_CGROUP;
    }
    action.target_id = target->target_id;
    action.task_id = target->task_id;
    action.executor_id = target->owner_executor_id;
    action.pid = target->pid;
    action.cgroup_path = target->cgroup_path;

    if (!target->cgroup_path.empty()) {
      action.action_type = SET_CPU_WEIGHT;
      action.numeric_params["cpu_weight"] = 50.0;
    } else {
      action.action_type = RENICE;
      action.numeric_params["nice"] = 12.0;
    }

    action.reason = "heuristic policy: high pressure on external target";
    action.policy_id = "heuristic_policy_v1";
    action.ts_ms = input.pressure.ts_ms;
    out.actions.push_back(std::move(action));
  }

  (void)input.capacity;
  (void)input.active_tasks;
  return out;
}

}  // namespace maigent
