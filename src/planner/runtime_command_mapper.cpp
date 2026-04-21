#include "maigent/planner/runtime_command_mapper.h"

#include <algorithm>

#include "maigent/common/target_model_proto.h"

namespace maigent {

namespace {

TargetType ResolveRuntimeTargetType(const PlannerDecisionTarget& target) {
  const TargetType from_kind = ToProtoTargetType(target.target_kind);
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

ControlAction ToRuntimeControlAction(const PlannerIntervention& intervention,
                                     const PlannerModelOutput& decision) {
  ControlAction out;
  out.set_target_type(ResolveRuntimeTargetType(intervention.target));
  out.set_target_id(intervention.target.target_id);
  out.set_task_id(intervention.target.task_id);
  out.set_executor_id(intervention.target.owner_executor_id);
  out.set_pid(intervention.target.pid);
  out.set_cgroup_path(intervention.target.cgroup_path);
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
    const PlannerModelOutput& decision) {
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
    out.push_back(ToRuntimeControlAction(*intervention, decision));
  }
  return out;
}

}  // namespace maigent
