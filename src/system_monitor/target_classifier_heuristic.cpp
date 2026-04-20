#include "maigent/system_monitor/target_classifier.h"

namespace maigent {

void HeuristicTargetClassifier::Classify(TargetInfo* target) {
  if (target == nullptr) {
    return;
  }

  target->clear_allowed_actions();

  const auto source = target->source_type();
  if (source == MANAGED_TASK) {
    target->set_is_protected(false);
    target->add_allowed_actions(RENICE);
    target->add_allowed_actions(SET_CPU_WEIGHT);
    target->add_allowed_actions(SET_CPU_MAX);
    target->add_allowed_actions(SET_MEM_HIGH);
    target->add_allowed_actions(FREEZE);
    target->add_allowed_actions(THAW);
    target->add_allowed_actions(KILL);
    return;
  }

  if (source == EXTERNAL_PROCESS || source == EXTERNAL_GROUP) {
    target->set_is_protected(false);
    target->add_allowed_actions(RENICE);
    target->add_allowed_actions(SET_CPU_WEIGHT);
    target->add_allowed_actions(SET_CPU_MAX);
    target->add_allowed_actions(SET_MEM_HIGH);
    target->add_allowed_actions(SET_MEM_MAX);
    target->add_allowed_actions(FREEZE);
    target->add_allowed_actions(THAW);
    target->add_allowed_actions(KILL);
    return;
  }

  target->set_is_protected(true);
  target->add_allowed_actions(RENICE);
  target->add_allowed_actions(SET_CPU_WEIGHT);
}

}  // namespace maigent
