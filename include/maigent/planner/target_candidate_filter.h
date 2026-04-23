#pragma once

#include <cstddef>
#include <vector>

#include "maigent/common/target_model.h"

namespace maigent {

struct PlannerTargetCandidateFilterConfig {
  size_t external_top_k_cpu = 3;
  size_t external_top_k_memory = 3;
  size_t external_top_k_pressure = 3;
  size_t external_max_candidates = 8;
  bool include_external_candidates = false;
};

class PlannerTargetCandidateFilter {
 public:
  explicit PlannerTargetCandidateFilter(
      PlannerTargetCandidateFilterConfig config = {});

  std::vector<UnifiedTarget> BuildCandidates(
      const std::vector<UnifiedTarget>& all_targets) const;

 private:
  PlannerTargetCandidateFilterConfig config_;
};

}  // namespace maigent
