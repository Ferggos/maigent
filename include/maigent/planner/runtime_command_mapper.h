#pragma once

#include <vector>

#include "maigent/planner/model_contract.h"

namespace maigent {

ControlActionType ToRuntimeControlActionType(PlannerInterventionType type);
ControlAction ToRuntimeControlAction(const PlannerIntervention& intervention,
                                     const PlannerModelOutput& decision);
std::vector<ControlAction> ToRuntimeControlActions(
    const PlannerModelOutput& decision);

}  // namespace maigent
