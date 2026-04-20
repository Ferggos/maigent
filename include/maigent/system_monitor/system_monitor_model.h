#pragma once

#include "maigent/system_monitor/model_contract.h"
#include "maigent/system_monitor/predictor.h"
#include "maigent/system_monitor/target_classifier.h"

namespace maigent {

class SystemMonitorModel {
 public:
  virtual ~SystemMonitorModel() = default;
  virtual SystemMonitorModelOutput Evaluate(const SystemMonitorModelInput& input) = 0;
};

class HeuristicSystemMonitorModel final : public SystemMonitorModel {
 public:
  SystemMonitorModelOutput Evaluate(const SystemMonitorModelInput& input) override;

 private:
  HeuristicPredictor predictor_;
  HeuristicTargetClassifier classifier_;
};

}  // namespace maigent
