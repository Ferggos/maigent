#include "maigent/planner/planner_model.h"

#include <algorithm>

namespace maigent {

namespace {

bool SupportsAction(const UnifiedTarget& target, TargetAction action) {
  return std::find(target.allowed_actions.begin(), target.allowed_actions.end(),
                   action) != target.allowed_actions.end();
}

}  // namespace

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

  out.interventions.reserve(static_cast<size_t>(max_actions_per_tick_));
  for (const UnifiedTarget* target : managed) {
    if (static_cast<int>(out.interventions.size()) >= max_actions_per_tick_) {
      break;
    }
    if (!SupportsAction(*target, TargetAction::kRenice)) {
      continue;
    }

    PlannerIntervention intervention;
    intervention.target.target_id = target->target_id;
    intervention.intervention_type = PlannerInterventionType::kDeprioritize;
    intervention.numeric_params["nice"] = 15.0;
    intervention.rationale = "heuristic policy: high pressure on managed task";
    intervention.apply_order = static_cast<int>(out.interventions.size());
    out.interventions.push_back(std::move(intervention));
  }

  for (const UnifiedTarget* target : external) {
    if (static_cast<int>(out.interventions.size()) >= max_actions_per_tick_) {
      break;
    }

    PlannerIntervention intervention;
    intervention.target.target_id = target->target_id;

    if (SupportsAction(*target, TargetAction::kSetCpuWeight)) {
      intervention.intervention_type = PlannerInterventionType::kLimitCpuShare;
      intervention.numeric_params["cpu_weight"] = 50.0;
    } else if (SupportsAction(*target, TargetAction::kRenice)) {
      intervention.intervention_type = PlannerInterventionType::kDeprioritize;
      intervention.numeric_params["nice"] = 12.0;
    } else {
      continue;
    }

    intervention.rationale =
        "heuristic policy: high pressure on external target";
    intervention.apply_order = static_cast<int>(out.interventions.size());
    out.interventions.push_back(std::move(intervention));
  }

  (void)input.capacity;
  (void)input.active_tasks;
  return out;
}

}  // namespace maigent
