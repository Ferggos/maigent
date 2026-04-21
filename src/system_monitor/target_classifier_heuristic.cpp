#include "maigent/system_monitor/target_classifier.h"

namespace maigent {

void HeuristicTargetClassifier::Classify(UnifiedTarget* target) {
  if (target == nullptr) {
    return;
  }

  target->allowed_actions.clear();

  const auto source = target->source;
  if (source == TargetSource::kManagedTask) {
    target->is_protected = false;
    target->allowed_actions.push_back(TargetAction::kRenice);
    target->allowed_actions.push_back(TargetAction::kSetCpuWeight);
    target->allowed_actions.push_back(TargetAction::kSetCpuMax);
    target->allowed_actions.push_back(TargetAction::kSetMemHigh);
    target->allowed_actions.push_back(TargetAction::kFreeze);
    target->allowed_actions.push_back(TargetAction::kThaw);
    target->allowed_actions.push_back(TargetAction::kKill);
    return;
  }

  if (source == TargetSource::kExternalProcess ||
      source == TargetSource::kExternalGroup) {
    target->is_protected = false;
    target->allowed_actions.push_back(TargetAction::kRenice);
    target->allowed_actions.push_back(TargetAction::kSetCpuWeight);
    target->allowed_actions.push_back(TargetAction::kSetCpuMax);
    target->allowed_actions.push_back(TargetAction::kSetMemHigh);
    target->allowed_actions.push_back(TargetAction::kSetMemMax);
    target->allowed_actions.push_back(TargetAction::kFreeze);
    target->allowed_actions.push_back(TargetAction::kThaw);
    target->allowed_actions.push_back(TargetAction::kKill);
    return;
  }

  target->is_protected = true;
  target->allowed_actions.push_back(TargetAction::kRenice);
  target->allowed_actions.push_back(TargetAction::kSetCpuWeight);
}

}  // namespace maigent
