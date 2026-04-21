#pragma once

#include <vector>

#include "maigent/system_monitor/model_contract.h"
#include "maigent/system_monitor/target_classifier.h"
#include "raw_collector.h"

namespace maigent {

class SystemMonitorFeatureBuilder {
 public:
  explicit SystemMonitorFeatureBuilder(TargetClassifier& target_classifier);

  SystemMonitorModelInput BuildModelInput(
      const SystemMonitorRawSnapshot& raw_snapshot,
      const std::vector<SystemMonitorPressureHistorySample>& pressure_history) const;

 private:
  TargetClassifier& target_classifier_;
};

}  // namespace maigent
