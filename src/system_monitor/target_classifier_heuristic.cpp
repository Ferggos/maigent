#include "maigent/system_monitor/target_classifier.h"

#include <algorithm>

namespace maigent {

namespace {

bool HasAction(const UnifiedTarget& target, TargetAction action) {
  return std::find(target.allowed_actions.begin(), target.allowed_actions.end(),
                   action) != target.allowed_actions.end();
}

void AddAction(UnifiedTarget* target, TargetAction action) {
  if (target == nullptr) {
    return;
  }
  if (!HasAction(*target, action)) {
    target->allowed_actions.push_back(action);
  }
}

}  // namespace

void HeuristicTargetClassifier::Classify(UnifiedTarget* target) {
  if (target == nullptr) {
    return;
  }

  target->allowed_actions.clear();
  target->is_protected = false;

  const bool has_task_context =
      !target->task_id.empty() && !target->owner_executor_id.empty();
  const bool has_pid = target->pid > 0;
  const bool has_cgroup = !target->cgroup_path.empty();

  const auto source = target->source;
  if (source == TargetSource::kManagedTask) {
    if (has_task_context) {
      // TaskExecutor currently supports only process-style controls.
      AddAction(target, TargetAction::kRenice);
      AddAction(target, TargetAction::kFreeze);
      AddAction(target, TargetAction::kThaw);
      AddAction(target, TargetAction::kKill);
    }
    return;
  }

  if (source == TargetSource::kExternalGroup) {
    if (has_cgroup) {
      AddAction(target, TargetAction::kSetCpuWeight);
      AddAction(target, TargetAction::kSetCpuMax);
      AddAction(target, TargetAction::kSetMemHigh);
      AddAction(target, TargetAction::kSetMemMax);
      AddAction(target, TargetAction::kFreeze);
      AddAction(target, TargetAction::kThaw);
    }
    return;
  }

  if (source == TargetSource::kExternalProcess) {
    if (has_pid) {
      AddAction(target, TargetAction::kRenice);
      AddAction(target, TargetAction::kFreeze);
      AddAction(target, TargetAction::kThaw);
      AddAction(target, TargetAction::kKill);
    }
    if (has_cgroup) {
      AddAction(target, TargetAction::kSetCpuWeight);
      AddAction(target, TargetAction::kSetCpuMax);
      AddAction(target, TargetAction::kSetMemHigh);
      AddAction(target, TargetAction::kSetMemMax);
      AddAction(target, TargetAction::kFreeze);
      AddAction(target, TargetAction::kThaw);
    }
    return;
  }

  // System/unspecified targets are protected in current runtime policy.
  target->is_protected = true;
}

}  // namespace maigent
