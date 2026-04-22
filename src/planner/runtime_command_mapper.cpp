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

std::optional<TargetAction> ToTargetAction(PlannerInterventionType type) {
  switch (type) {
    case PlannerInterventionType::kDeprioritize:
      return TargetAction::kRenice;
    case PlannerInterventionType::kLimitCpuShare:
      return TargetAction::kSetCpuWeight;
    case PlannerInterventionType::kLimitCpuQuota:
      return TargetAction::kSetCpuMax;
    case PlannerInterventionType::kLimitMemorySoft:
      return TargetAction::kSetMemHigh;
    case PlannerInterventionType::kLimitMemoryHard:
      return TargetAction::kSetMemMax;
    case PlannerInterventionType::kPause:
      return TargetAction::kFreeze;
    case PlannerInterventionType::kResume:
      return TargetAction::kThaw;
    case PlannerInterventionType::kTerminate:
      return TargetAction::kKill;
    case PlannerInterventionType::kUnspecified:
    default:
      return std::nullopt;
  }
}

bool HasAllowedAction(const UnifiedTarget& target, TargetAction action) {
  return std::find(target.allowed_actions.begin(), target.allowed_actions.end(),
                   action) != target.allowed_actions.end();
}

bool IsCgroupOnlyAction(PlannerInterventionType type) {
  return type == PlannerInterventionType::kLimitCpuShare ||
         type == PlannerInterventionType::kLimitCpuQuota ||
         type == PlannerInterventionType::kLimitMemorySoft ||
         type == PlannerInterventionType::kLimitMemoryHard;
}

bool ValidateDispatchMetadata(const PlannerIntervention& intervention,
                              const UnifiedTarget& target,
                              const PlannerDispatchMetadata& dispatch) {
  if (ToRuntimeControlActionType(intervention.intervention_type) ==
      CONTROL_ACTION_UNSPECIFIED) {
    return false;
  }
  const auto target_action = ToTargetAction(intervention.intervention_type);
  if (!target_action.has_value() || !HasAllowedAction(target, *target_action)) {
    return false;
  }
  if (dispatch.runtime_target_type == TARGET_TYPE_UNSPECIFIED) {
    return false;
  }

  const bool task_route = dispatch.runtime_target_type == TARGET_TASK;
  if (task_route) {
    if (dispatch.executor_id.empty() || dispatch.task_id.empty()) {
      return false;
    }
    // TaskExecutor currently supports only process-style actions.
    if (IsCgroupOnlyAction(intervention.intervention_type)) {
      return false;
    }
    return true;
  }

  switch (intervention.intervention_type) {
    case PlannerInterventionType::kDeprioritize:
    case PlannerInterventionType::kTerminate:
      return dispatch.pid > 0;
    case PlannerInterventionType::kLimitCpuShare:
    case PlannerInterventionType::kLimitCpuQuota:
    case PlannerInterventionType::kLimitMemorySoft:
    case PlannerInterventionType::kLimitMemoryHard:
      return !dispatch.cgroup_path.empty();
    case PlannerInterventionType::kPause:
    case PlannerInterventionType::kResume:
      return dispatch.pid > 0 || !dispatch.cgroup_path.empty();
    case PlannerInterventionType::kUnspecified:
    default:
      return false;
  }
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
  if (!ValidateDispatchMetadata(intervention, *it, dispatch)) {
    return std::nullopt;
  }
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
