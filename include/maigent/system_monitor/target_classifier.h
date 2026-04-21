#pragma once

#include "maigent/common/target_model.h"

namespace maigent {

class TargetClassifier {
 public:
  virtual ~TargetClassifier() = default;
  virtual void Classify(UnifiedTarget* target) = 0;
};

class HeuristicTargetClassifier final : public TargetClassifier {
 public:
  void Classify(UnifiedTarget* target) override;
};

}  // namespace maigent
