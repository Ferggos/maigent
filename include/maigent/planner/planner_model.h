#pragma once

#include "maigent/planner/model_contract.h"

namespace maigent {

class PlannerModel {
 public:
  virtual ~PlannerModel() = default;
  virtual PlannerModelOutput Evaluate(const PlannerModelInput& input) = 0;
};

class HeuristicPlannerModel final : public PlannerModel {
 public:
  explicit HeuristicPlannerModel(int max_actions_per_tick);

  PlannerModelOutput Evaluate(const PlannerModelInput& input) override;

 private:
  int max_actions_per_tick_;
};

}  // namespace maigent
