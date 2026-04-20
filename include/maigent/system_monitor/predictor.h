#pragma once

#include <vector>

#include "maigent.pb.h"

namespace maigent {

class Predictor {
 public:
  virtual ~Predictor() = default;
  virtual ForecastState Predict(const PressureState& latest,
                                const std::vector<PressureState>& history) = 0;
};

class HeuristicPredictor final : public Predictor {
 public:
  ForecastState Predict(const PressureState& latest,
                        const std::vector<PressureState>& history) override;
};

}  // namespace maigent
