#pragma once

#include "maigent.pb.h"

namespace maigent {

class TargetClassifier {
 public:
  virtual ~TargetClassifier() = default;
  virtual void Classify(TargetInfo* target) = 0;
};

class HeuristicTargetClassifier final : public TargetClassifier {
 public:
  void Classify(TargetInfo* target) override;
};

}  // namespace maigent
