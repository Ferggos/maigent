#pragma once

#include <vector>

#include "maigent/system_monitor/model_contract.h"
#include "raw_collector.h"

namespace maigent {

class SystemMonitorFeatureBuilder {
 public:
  SystemMonitorModelInput BuildModelInput(
      const SystemMonitorRawSnapshot& raw_snapshot,
      const std::vector<SystemMonitorPressureHistorySample>& pressure_history) const;
};

}  // namespace maigent
