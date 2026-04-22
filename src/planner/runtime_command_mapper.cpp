#include "maigent/planner/runtime_command_mapper.h"

#include <algorithm>

#include "maigent/common/target_model_proto.h"

namespace maigent {

namespace {

TargetType ResolveRuntimeTargetType(const UnifiedTarget& target) {
  const TargetType from_kind = ToProtoTargetType(target.kind);
  if (from_kind != TARGET_TYPE_UNSPECIFIED) {
    return from_kind;
  }
  if (!target.task_id.empty()) {
    return TARGET_TASK;
  }
  if (!target.cgroup_path.empty()) {
    return TARGET_CGROUP;
  }
  if (target.pid > 0) {
    return TARGET_PROCESS;
  }
  return TARGET_TYPE_UNSPECIFIED;
}

}  // namespace

ControlActionType ToRuntimeControlActionType(PlannerInterventionType type) {
  switch (type) {
    case PlannerInterventionType::kDeprioritize:
      return RENICE;
    case PlannerInterventionType::kLimitCpuShare:
      return SET_CPU_WEIGHT;
    case PlannerInterventionType::kLimitCpuQuota:
      return SET_CPU_MAX;
    case PlannerInterventionType::kLimitMemorySoft:
      return SET_MEM_HIGH;
    case PlannerInterventionType::kLimitMemoryHard:
      return SET_MEM_MAX;
    case PlannerInterventionType::kPause:
      return FREEZE;
    case PlannerInterventionType::kResume:
      return THAW;
    case PlannerInterventionType::kTerminate:
      return KILL;
    case PlannerInterventionType::kUnspecified:
    default:
      return CONTROL_ACTION_UNSPECIFIED;
  }
}

std::optional<PlannerDispatchMetadata> ResolveDispatchMetadata(
    const PlannerIntervention& intervention,
    const std::vector<UnifiedTarget>& known_targets) {
  const auto it = std::find_if(
      known_targets.begin(), known_targets.end(),
      [&](const UnifiedTarget& target) {
        return target.target_id == intervention.target.target_id;
      });
  if (it == known_targets.end()) {
    return std::nullopt;
  }

  PlannerDispatchMetadata dispatch;
  dispatch.runtime_target_type = ResolveRuntimeTargetType(*it);
  dispatch.task_id = it->task_id;
  dispatch.executor_id = it->owner_executor_id;
  dispatch.pid = it->pid;
  dispatch.cgroup_path = it->cgroup_path;
  return dispatch;
}

ControlAction ToRuntimeControlAction(const PlannerIntervention& intervention,
                                     const PlannerModelOutput& decision,
                                     const PlannerDispatchMetadata& dispatch) {
  ControlAction out;
  out.set_target_type(dispatch.runtime_target_type);
  out.set_target_id(intervention.target.target_id);
  out.set_task_id(dispatch.task_id);
  out.set_executor_id(dispatch.executor_id);
  out.set_pid(dispatch.pid);
  out.set_cgroup_path(dispatch.cgroup_path);
  out.set_action_type(ToRuntimeControlActionType(intervention.intervention_type));

  for (const auto& [key, value] : intervention.numeric_params) {
    (*out.mutable_numeric_params())[key] = value;
  }
  for (const auto& [key, value] : intervention.string_params) {
    (*out.mutable_string_params())[key] = value;
  }

  out.set_reason(intervention.rationale);
  out.set_policy_id(decision.strategy_id);
  out.set_ts_ms(decision.decision_ts_ms);
  return out;
}

std::vector<ControlAction> ToRuntimeControlActions(
    const PlannerModelOutput& decision,
    const std::vector<UnifiedTarget>& known_targets) {
  std::vector<const PlannerIntervention*> ordered;
  ordered.reserve(decision.interventions.size());
  for (const auto& intervention : decision.interventions) {
    ordered.push_back(&intervention);
  }

  std::stable_sort(ordered.begin(), ordered.end(),
                   [](const PlannerIntervention* lhs,
                      const PlannerIntervention* rhs) {
                     return lhs->apply_order < rhs->apply_order;
                   });

  std::vector<ControlAction> out;
  out.reserve(ordered.size());
  for (const PlannerIntervention* intervention : ordered) {
    const auto dispatch =
        ResolveDispatchMetadata(*intervention, known_targets);
    if (!dispatch.has_value()) {
      continue;
    }
    out.push_back(ToRuntimeControlAction(*intervention, decision, *dispatch));
  }
  return out;
}

}  // namespace maigent
