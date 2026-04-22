#pragma once

#include <optional>
#include <vector>

#include "maigent/planner/model_contract.h"

namespace maigent {

struct PlannerDispatchMetadata {
  TargetType runtime_target_type = TARGET_TYPE_UNSPECIFIED;
  std::string task_id;
  std::string executor_id;
  int pid = 0;
  std::string cgroup_path;
};

ControlActionType ToRuntimeControlActionType(PlannerInterventionType type);
std::optional<PlannerDispatchMetadata> ResolveDispatchMetadata(
    const PlannerIntervention& intervention,
    const std::vector<UnifiedTarget>& known_targets);
ControlAction ToRuntimeControlAction(const PlannerIntervention& intervention,
                                     const PlannerModelOutput& decision,
                                     const PlannerDispatchMetadata& dispatch);
std::vector<ControlAction> ToRuntimeControlActions(
    const PlannerModelOutput& decision,
    const std::vector<UnifiedTarget>& known_targets);

}  // namespace maigent
