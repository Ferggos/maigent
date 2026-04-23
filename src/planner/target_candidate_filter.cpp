#include "maigent/planner/target_candidate_filter.h"

#include <algorithm>
#include <cmath>
#include <unordered_set>

namespace maigent {

namespace {

constexpr double kScoreEpsilon = 1e-9;

bool IsManagedTask(const UnifiedTarget& target) {
  return target.source == TargetSource::kManagedTask;
}

bool IsExternalTarget(const UnifiedTarget& target) {
  return target.source == TargetSource::kExternalGroup ||
         target.source == TargetSource::kExternalProcess ||
         target.source == TargetSource::kSystemService ||
         target.source == TargetSource::kUnspecified;
}

bool IsActionRoutable(TargetAction action, const UnifiedTarget& target) {
  const bool has_pid = target.pid > 0;
  const bool has_cgroup = !target.cgroup_path.empty();
  switch (action) {
    case TargetAction::kRenice:
    case TargetAction::kKill:
      return has_pid;
    case TargetAction::kSetCpuWeight:
    case TargetAction::kSetCpuMax:
    case TargetAction::kSetMemHigh:
    case TargetAction::kSetMemMax:
      return has_cgroup;
    case TargetAction::kFreeze:
    case TargetAction::kThaw:
      return has_pid || has_cgroup;
    case TargetAction::kUnspecified:
    default:
      return false;
  }
}

bool HasControlRoute(const UnifiedTarget& target) {
  for (const auto action : target.allowed_actions) {
    if (IsActionRoutable(action, target)) {
      return true;
    }
  }
  return false;
}

bool IsExternalEligible(const UnifiedTarget& target) {
  if (target.is_protected || target.allowed_actions.empty()) {
    return false;
  }
  return HasControlRoute(target);
}

bool IsExternalRelevant(const UnifiedTarget& target) {
  return target.cpu_intensity >= 0.20 || target.cpu_pressure >= 0.20 ||
         target.memory_current_mb >= 256.0 || target.memory_delta_mb >= 64.0 ||
         target.memory_ratio_of_host >= 0.03 || target.memory_pressure >= 0.20 ||
         target.io_pressure >= 0.20 || target.cpu_throttled_ratio >= 0.05 ||
         target.memory_events_high_delta > 0 || target.memory_events_oom_delta > 0;
}

double CpuScore(const UnifiedTarget& target) {
  return target.cpu_intensity + target.cpu_pressure +
         target.cpu_throttled_ratio * 2.0;
}

double MemoryScore(const UnifiedTarget& target) {
  return target.memory_current_mb +
         std::max(0.0, target.memory_delta_mb) * 4.0 +
         target.memory_ratio_of_host * 2048.0;
}

double PressureScore(const UnifiedTarget& target) {
  return target.memory_pressure * 100.0 + target.io_pressure * 50.0 +
         target.cpu_throttled_ratio * 100.0 +
         static_cast<double>(target.memory_events_high_delta) * 4.0 +
         static_cast<double>(target.memory_events_oom_delta) * 80.0;
}

double OverallScore(const UnifiedTarget& target) {
  return CpuScore(target) * 2.0 + MemoryScore(target) / 256.0 +
         PressureScore(target) * 0.5;
}

bool ScoreGreater(double lhs, double rhs) { return lhs - rhs > kScoreEpsilon; }

bool IsBetterForScore(const UnifiedTarget* lhs, const UnifiedTarget* rhs,
                      double (*score_fn)(const UnifiedTarget&)) {
  const double lhs_score = score_fn(*lhs);
  const double rhs_score = score_fn(*rhs);
  if (ScoreGreater(lhs_score, rhs_score)) {
    return true;
  }
  if (ScoreGreater(rhs_score, lhs_score)) {
    return false;
  }
  if (lhs->source != rhs->source) {
    return static_cast<int>(lhs->source) < static_cast<int>(rhs->source);
  }
  if (lhs->kind != rhs->kind) {
    return static_cast<int>(lhs->kind) < static_cast<int>(rhs->kind);
  }
  return lhs->target_id < rhs->target_id;
}

template <typename ScoreFn>
std::vector<const UnifiedTarget*> PickTopK(const std::vector<const UnifiedTarget*>& input,
                                           size_t k, ScoreFn score_fn) {
  std::vector<const UnifiedTarget*> ranked = input;
  std::sort(ranked.begin(), ranked.end(),
            [&](const UnifiedTarget* lhs, const UnifiedTarget* rhs) {
              return IsBetterForScore(lhs, rhs, score_fn);
            });
  if (ranked.size() > k) {
    ranked.resize(k);
  }
  return ranked;
}

bool IsManagedLess(const UnifiedTarget& lhs, const UnifiedTarget& rhs) {
  if (lhs.owner_executor_id != rhs.owner_executor_id) {
    return lhs.owner_executor_id < rhs.owner_executor_id;
  }
  if (lhs.task_id != rhs.task_id) {
    return lhs.task_id < rhs.task_id;
  }
  if (lhs.priority != rhs.priority) {
    return lhs.priority < rhs.priority;
  }
  if (lhs.pid != rhs.pid) {
    return lhs.pid < rhs.pid;
  }
  return lhs.target_id < rhs.target_id;
}

bool IsExternalLess(const UnifiedTarget& lhs, const UnifiedTarget& rhs) {
  const double lhs_score = OverallScore(lhs);
  const double rhs_score = OverallScore(rhs);
  if (ScoreGreater(lhs_score, rhs_score)) {
    return true;
  }
  if (ScoreGreater(rhs_score, lhs_score)) {
    return false;
  }
  if (lhs.source != rhs.source) {
    return static_cast<int>(lhs.source) < static_cast<int>(rhs.source);
  }
  if (lhs.kind != rhs.kind) {
    return static_cast<int>(lhs.kind) < static_cast<int>(rhs.kind);
  }
  return lhs.target_id < rhs.target_id;
}

}  // namespace

PlannerTargetCandidateFilter::PlannerTargetCandidateFilter(
    PlannerTargetCandidateFilterConfig config)
    : config_(config) {}

std::vector<UnifiedTarget> PlannerTargetCandidateFilter::BuildCandidates(
    const std::vector<UnifiedTarget>& all_targets) const {
  std::vector<UnifiedTarget> managed_targets;
  managed_targets.reserve(all_targets.size());

  std::vector<const UnifiedTarget*> external_pool;
  external_pool.reserve(all_targets.size());

  for (const auto& target : all_targets) {
    if (IsManagedTask(target)) {
      managed_targets.push_back(target);
      continue;
    }
    if (!config_.include_external_candidates) {
      continue;
    }
    if (!IsExternalTarget(target)) {
      continue;
    }
    if (!IsExternalEligible(target) || !IsExternalRelevant(target)) {
      continue;
    }
    external_pool.push_back(&target);
  }

  std::sort(managed_targets.begin(), managed_targets.end(), IsManagedLess);

  std::unordered_set<std::string> selected_ids;
  selected_ids.reserve(external_pool.size());
  std::vector<const UnifiedTarget*> selected;
  selected.reserve(external_pool.size());

  const auto add_unique = [&](const std::vector<const UnifiedTarget*>& picks) {
    for (const UnifiedTarget* target : picks) {
      if (target == nullptr) {
        continue;
      }
      if (selected_ids.insert(target->target_id).second) {
        selected.push_back(target);
      }
    }
  };

  add_unique(PickTopK(external_pool, config_.external_top_k_cpu, CpuScore));
  add_unique(PickTopK(external_pool, config_.external_top_k_memory, MemoryScore));
  add_unique(PickTopK(external_pool, config_.external_top_k_pressure,
                      PressureScore));

  std::vector<UnifiedTarget> external_targets;
  external_targets.reserve(selected.size());
  for (const UnifiedTarget* target : selected) {
    if (target != nullptr) {
      external_targets.push_back(*target);
    }
  }
  std::sort(external_targets.begin(), external_targets.end(), IsExternalLess);
  if (external_targets.size() > config_.external_max_candidates) {
    external_targets.resize(config_.external_max_candidates);
  }

  std::vector<UnifiedTarget> out;
  out.reserve(managed_targets.size() + external_targets.size());
  out.insert(out.end(), managed_targets.begin(), managed_targets.end());
  out.insert(out.end(), external_targets.begin(), external_targets.end());
  return out;
}

}  // namespace maigent
