#pragma once

#include <vector>

#include "maigent.pb.h"

namespace maigent {

class PolicyEngine {
 public:
  virtual ~PolicyEngine() = default;

  virtual std::vector<ControlAction> Evaluate(const PressureState& pressure,
                                              const ForecastState& forecast,
                                              const CapacityState& capacity,
                                              const TargetsState& targets) = 0;
};

class HeuristicPolicyEngine final : public PolicyEngine {
 public:
  explicit HeuristicPolicyEngine(int max_actions_per_tick);

  std::vector<ControlAction> Evaluate(const PressureState& pressure,
                                      const ForecastState& forecast,
                                      const CapacityState& capacity,
                                      const TargetsState& targets) override;

 private:
  int max_actions_per_tick_;
};

}  // namespace maigent
