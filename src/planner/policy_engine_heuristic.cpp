#include "maigent/planner/planner_model.h"

#include <algorithm>
#include <optional>
#include <vector>

namespace maigent {

namespace {

enum class PlannerStrategy {
  kObserve = 0,
  kRelieveMemory = 1,
  kRelieveCpu = 2,
  kEmergency = 3,
};

bool SupportsAction(const UnifiedTarget& target, TargetAction action) {
  return std::find(target.allowed_actions.begin(), target.allowed_actions.end(),
                   action) != target.allowed_actions.end();
}

double Clamp01(double value) { return std::clamp(value, 0.0, 1.0); }

double ScoreFromThresholds(double value, double moderate, double high) {
  if (high <= moderate || moderate <= 0.0) {
    return Clamp01(value > 0.0 ? value / std::max(1e-9, high) : 0.0);
  }
  if (value <= 0.0) {
    return 0.0;
  }
  if (value < moderate) {
    return 0.5 * Clamp01(value / moderate);
  }
  if (value >= high) {
    return 1.0;
  }
  return 0.5 + 0.5 * Clamp01((value - moderate) / (high - moderate));
}

double LowValueScore(double value, double moderate_floor, double high_floor) {
  if (high_floor >= moderate_floor || moderate_floor <= 0.0) {
    return Clamp01(1.0 - value / std::max(1e-9, moderate_floor));
  }
  if (value <= high_floor) {
    return 1.0;
  }
  if (value < moderate_floor) {
    return 0.5 +
           0.5 * Clamp01((moderate_floor - value) / (moderate_floor - high_floor));
  }
  const double relaxed_floor = moderate_floor * 2.0;
  if (value >= relaxed_floor) {
    return 0.0;
  }
  return 0.5 * Clamp01((relaxed_floor - value) / moderate_floor);
}

bool IsManagedTask(const UnifiedTarget& target) {
  return target.source == TargetSource::kManagedTask;
}

double PriorityPenalty(int priority) {
  if (priority < 0) {
    return 0.02;
  }
  if (priority >= 90) {
    return 0.50;
  }
  if (priority >= 70) {
    return 0.35;
  }
  if (priority >= 50) {
    return 0.22;
  }
  return 0.10;
}

int InterventionSeverity(PlannerInterventionType type) {
  switch (type) {
    case PlannerInterventionType::kDeprioritize:
    case PlannerInterventionType::kLimitCpuShare:
    case PlannerInterventionType::kLimitMemorySoft:
      return 1;
    case PlannerInterventionType::kLimitCpuQuota:
    case PlannerInterventionType::kPause:
      return 2;
    case PlannerInterventionType::kLimitMemoryHard:
      return 3;
    case PlannerInterventionType::kTerminate:
      return 4;
    case PlannerInterventionType::kResume:
    case PlannerInterventionType::kUnspecified:
    default:
      return 5;
  }
}

int MaxSeverityForTarget(const UnifiedTarget& target, PlannerStrategy strategy) {
  if (target.is_protected) {
    return 0;
  }
  if (strategy == PlannerStrategy::kEmergency) {
    if (target.priority >= 70) {
      return 2;
    }
    if (target.priority >= 50) {
      return 3;
    }
    if (target.priority >= 0) {
      return 3;
    }
    return 4;
  }

  if (target.priority >= 70) {
    return 1;
  }
  if (target.priority >= 50) {
    return 2;
  }
  if (target.priority >= 0) {
    return 2;
  }
  return 3;
}

std::optional<TargetAction> ActionForIntervention(PlannerInterventionType type) {
  switch (type) {
    case PlannerInterventionType::kDeprioritize:
      return TargetAction::kRenice;
    case PlannerInterventionType::kLimitCpuShare:
      return TargetAction::kSetCpuWeight;
    case PlannerInterventionType::kLimitCpuQuota:
      return TargetAction::kSetCpuMax;
    case PlannerInterventionType::kLimitMemorySoft:
      return TargetAction::kSetMemHigh;
    case PlannerInterventionType::kLimitMemoryHard:
      return TargetAction::kSetMemMax;
    case PlannerInterventionType::kPause:
      return TargetAction::kFreeze;
    case PlannerInterventionType::kResume:
      return TargetAction::kThaw;
    case PlannerInterventionType::kTerminate:
      return TargetAction::kKill;
    case PlannerInterventionType::kUnspecified:
    default:
      return std::nullopt;
  }
}

bool SupportsIntervention(const UnifiedTarget& target,
                          PlannerInterventionType intervention_type) {
  const auto action = ActionForIntervention(intervention_type);
  return action.has_value() && SupportsAction(target, *action);
}

double ComputeMemoryTargetScore(const UnifiedTarget& target,
                                const PlannerModelInput& input,
                                PlannerStrategy strategy) {
  const double mem_current_signal =
      ScoreFromThresholds(target.memory_current_mb, 256.0, 2048.0);
  const double mem_ratio_signal =
      ScoreFromThresholds(target.memory_ratio_of_host, 0.03, 0.12);
  const double mem_growth_signal =
      ScoreFromThresholds(std::max(0.0, target.memory_delta_mb), 32.0, 256.0);
  const double mem_pressure_signal =
      ScoreFromThresholds(target.memory_pressure, 0.2, 0.8);
  const double mem_events_signal = std::max(
      target.memory_events_oom_delta > 0 ? 1.0 : 0.0,
      ScoreFromThresholds(static_cast<double>(target.memory_events_high_delta), 1.0,
                          4.0));
  const double age_signal = ScoreFromThresholds(target.age_sec, 5.0, 30.0);

  double capability_bonus = 0.0;
  if (SupportsAction(target, TargetAction::kSetMemHigh)) {
    capability_bonus += 0.08;
  } else if (SupportsAction(target, TargetAction::kFreeze)) {
    capability_bonus += 0.04;
  }

  // Memory strategy additionally respects host memory headroom.
  const double host_memory_scarcity = std::max(
      ScoreFromThresholds(1.0 - (input.capacity.mem_total_mb > 0
                                     ? static_cast<double>(input.capacity.mem_available_mb) /
                                           static_cast<double>(input.capacity.mem_total_mb)
                                     : 0.0),
                          0.84, 0.92),
      LowValueScore(static_cast<double>(input.pressure.mem_available_mb), 1536.0, 768.0));
  const double emergency_bias = strategy == PlannerStrategy::kEmergency ? 0.08 : 0.0;

  return Clamp01(0.24 * mem_current_signal + 0.18 * mem_ratio_signal +
                 0.20 * mem_growth_signal + 0.14 * mem_pressure_signal +
                 0.14 * mem_events_signal + 0.06 * age_signal +
                 0.10 * host_memory_scarcity + capability_bonus + emergency_bias -
                 PriorityPenalty(target.priority));
}

double ComputeCpuTargetScore(const UnifiedTarget& target, PlannerStrategy strategy) {
  const double intensity_signal =
      ScoreFromThresholds(target.cpu_intensity, 0.30, 1.20);
  const double cpu_lifetime_share =
      target.age_sec > 1.0 ? target.cpu_usage / target.age_sec : target.cpu_intensity;
  const double cpu_usage_signal =
      ScoreFromThresholds(std::max(0.0, cpu_lifetime_share), 0.30, 1.00);
  const double cpu_pressure_signal =
      ScoreFromThresholds(target.cpu_pressure, 0.20, 0.80);
  const double throttle_signal =
      ScoreFromThresholds(target.cpu_throttled_ratio, 0.05, 0.25);
  const double age_signal = ScoreFromThresholds(target.age_sec, 5.0, 30.0);

  double capability_bonus = 0.0;
  if (SupportsAction(target, TargetAction::kRenice)) {
    capability_bonus += 0.08;
  } else if (SupportsAction(target, TargetAction::kSetCpuWeight)) {
    capability_bonus += 0.06;
  }
  if (strategy == PlannerStrategy::kEmergency) {
    capability_bonus += 0.05;
  }

  return Clamp01(0.30 * intensity_signal + 0.22 * cpu_usage_signal +
                 0.20 * cpu_pressure_signal + 0.18 * throttle_signal +
                 0.10 * age_signal + capability_bonus -
                 PriorityPenalty(target.priority));
}

PlannerStrategy ChooseStrategy(const PlannerModelInput& input, double cpu_pressure_score,
                               double memory_pressure_score,
                               double io_pressure_score) {
  const bool high_pressure = input.pressure.risk_level == RISK_HIGH ||
                             input.forecast.risk_level == RISK_HIGH;
  if (!high_pressure) {
    return PlannerStrategy::kObserve;
  }

  bool severe_memory_indicator =
      input.pressure.mem_available_mb < 768 ||
      input.forecast.predicted_mem_available_mb < 768 ||
      input.forecast.overload_probability >= 0.90;
  for (const auto& target : input.targets) {
    if (target.memory_events_oom_delta > 0) {
      severe_memory_indicator = true;
      break;
    }
  }
  if (severe_memory_indicator && memory_pressure_score >= 0.60) {
    return PlannerStrategy::kEmergency;
  }

  const double cpu_bias = cpu_pressure_score + io_pressure_score * 0.35;
  const double memory_bias = memory_pressure_score + io_pressure_score * 0.15;
  if (memory_bias >= cpu_bias + 0.08 && memory_bias >= 0.45) {
    return PlannerStrategy::kRelieveMemory;
  }
  if (cpu_bias >= 0.45) {
    return PlannerStrategy::kRelieveCpu;
  }
  if (memory_pressure_score >= 0.45) {
    return PlannerStrategy::kRelieveMemory;
  }
  return PlannerStrategy::kObserve;
}

const std::vector<PlannerInterventionType>& ActionLadder(PlannerStrategy strategy,
                                                         bool memory_dominant) {
  static const std::vector<PlannerInterventionType> kMemoryLadder = {
      PlannerInterventionType::kLimitMemorySoft, PlannerInterventionType::kPause,
      PlannerInterventionType::kLimitMemoryHard, PlannerInterventionType::kTerminate};
  static const std::vector<PlannerInterventionType> kCpuLadder = {
      PlannerInterventionType::kDeprioritize, PlannerInterventionType::kLimitCpuShare,
      PlannerInterventionType::kLimitCpuQuota, PlannerInterventionType::kPause,
      PlannerInterventionType::kTerminate};
  static const std::vector<PlannerInterventionType> kEmergencyMemLadder = {
      PlannerInterventionType::kPause, PlannerInterventionType::kLimitMemoryHard,
      PlannerInterventionType::kTerminate};
  static const std::vector<PlannerInterventionType> kEmergencyCpuLadder = {
      PlannerInterventionType::kPause, PlannerInterventionType::kLimitCpuQuota,
      PlannerInterventionType::kTerminate};

  switch (strategy) {
    case PlannerStrategy::kRelieveMemory:
      return kMemoryLadder;
    case PlannerStrategy::kRelieveCpu:
      return kCpuLadder;
    case PlannerStrategy::kEmergency:
      return memory_dominant ? kEmergencyMemLadder : kEmergencyCpuLadder;
    case PlannerStrategy::kObserve:
    default:
      return kCpuLadder;
  }
}

std::optional<PlannerInterventionType> PickIntervention(
    const UnifiedTarget& target, PlannerStrategy strategy, bool memory_dominant) {
  const int max_severity = MaxSeverityForTarget(target, strategy);
  if (max_severity <= 0) {
    return std::nullopt;
  }

  const auto& ladder = ActionLadder(strategy, memory_dominant);
  for (const auto candidate : ladder) {
    if (InterventionSeverity(candidate) > max_severity) {
      continue;
    }
    if (SupportsIntervention(target, candidate)) {
      return candidate;
    }
  }
  return std::nullopt;
}

void FillInterventionParams(const UnifiedTarget& target, PlannerStrategy strategy,
                            PlannerIntervention* intervention) {
  if (intervention == nullptr) {
    return;
  }
  switch (intervention->intervention_type) {
    case PlannerInterventionType::kDeprioritize: {
      double nice_value = 12.0;
      if (target.priority < 0) {
        nice_value = strategy == PlannerStrategy::kEmergency ? 18.0 : 15.0;
      } else if (target.priority >= 70) {
        nice_value = 8.0;
      }
      intervention->numeric_params["nice"] = nice_value;
      break;
    }
    case PlannerInterventionType::kLimitCpuShare: {
      double cpu_weight = 80.0;
      if (strategy == PlannerStrategy::kEmergency) {
        cpu_weight = target.priority < 0 ? 40.0 : 60.0;
      } else if (target.priority < 0) {
        cpu_weight = 60.0;
      }
      intervention->numeric_params["cpu_weight"] = cpu_weight;
      break;
    }
    case PlannerInterventionType::kLimitCpuQuota: {
      intervention->numeric_params["period"] = 100000.0;
      intervention->numeric_params["quota"] =
          strategy == PlannerStrategy::kEmergency ? 50000.0 : 70000.0;
      break;
    }
    case PlannerInterventionType::kLimitMemorySoft: {
      const double mb = std::max(64.0, target.memory_current_mb * 0.85);
      intervention->numeric_params["mem_high_bytes"] = mb * 1024.0 * 1024.0;
      break;
    }
    case PlannerInterventionType::kLimitMemoryHard: {
      const double ratio = (strategy == PlannerStrategy::kEmergency) ? 0.70 : 0.80;
      const double mb = std::max(64.0, target.memory_current_mb * ratio);
      intervention->numeric_params["mem_max_bytes"] = mb * 1024.0 * 1024.0;
      break;
    }
    case PlannerInterventionType::kPause:
    case PlannerInterventionType::kTerminate:
    case PlannerInterventionType::kResume:
    case PlannerInterventionType::kUnspecified:
    default:
      break;
  }
}

std::string StrategyId(PlannerStrategy strategy) {
  switch (strategy) {
    case PlannerStrategy::kRelieveMemory:
      return "heuristic_policy_v2.relieve_memory";
    case PlannerStrategy::kRelieveCpu:
      return "heuristic_policy_v2.relieve_cpu";
    case PlannerStrategy::kEmergency:
      return "heuristic_policy_v2.emergency";
    case PlannerStrategy::kObserve:
    default:
      return "heuristic_policy_v2.observe";
  }
}

}  // namespace

HeuristicPlannerModel::HeuristicPlannerModel(int max_actions_per_tick)
    : max_actions_per_tick_(std::max(1, max_actions_per_tick)) {}

PlannerModelOutput HeuristicPlannerModel::Evaluate(const PlannerModelInput& input) {
  PlannerModelOutput out;
  out.decision_ts_ms = input.snapshot_ts_ms;
  out.strategy_id = "heuristic_policy_v2.observe";

  const double cpu_pressure_score = std::max(
      ScoreFromThresholds(std::max(input.pressure.cpu_usage_pct,
                                   input.forecast.predicted_cpu_usage_pct),
                          72.0, 88.0),
      ScoreFromThresholds(input.pressure.cpu_pressure_some, 0.4, 1.2));
  const double memory_pressure_score = std::max(
      LowValueScore(static_cast<double>(input.pressure.mem_available_mb), 1536.0, 768.0),
      std::max(ScoreFromThresholds(input.pressure.memory_pressure_some, 0.3, 1.0),
               LowValueScore(static_cast<double>(input.forecast.predicted_mem_available_mb),
                             1536.0, 768.0)));
  const double io_pressure_score =
      ScoreFromThresholds(input.pressure.io_pressure_some, 0.6, 2.0);
  const PlannerStrategy strategy = ChooseStrategy(
      input, cpu_pressure_score, memory_pressure_score, io_pressure_score);
  out.strategy_id = StrategyId(strategy);
  if (strategy == PlannerStrategy::kObserve) {
    return out;
  }

  const bool memory_dominant =
      memory_pressure_score >= (cpu_pressure_score + io_pressure_score * 0.2);

  struct ScoredTarget {
    const UnifiedTarget* target = nullptr;
    double score = 0.0;
    PlannerInterventionType planned_action = PlannerInterventionType::kUnspecified;
  };
  std::vector<ScoredTarget> ranked;
  ranked.reserve(input.targets.size());

  for (const auto& target : input.targets) {
    if (target.is_protected || !IsManagedTask(target)) {
      continue;
    }
    const auto planned_action =
        PickIntervention(target, strategy, memory_dominant);
    if (!planned_action.has_value()) {
      continue;
    }

    double score = 0.0;
    if (strategy == PlannerStrategy::kRelieveMemory ||
        strategy == PlannerStrategy::kEmergency) {
      score = ComputeMemoryTargetScore(target, input, strategy);
    } else {
      score = ComputeCpuTargetScore(target, strategy);
    }
    if (score < 0.12) {
      continue;
    }
    ScoredTarget scored;
    scored.target = &target;
    scored.score = score;
    scored.planned_action = *planned_action;
    ranked.push_back(scored);
  }

  std::sort(ranked.begin(), ranked.end(),
            [](const ScoredTarget& lhs, const ScoredTarget& rhs) {
              if (lhs.score != rhs.score) {
                return lhs.score > rhs.score;
              }
              if (lhs.target->priority != rhs.target->priority) {
                return lhs.target->priority < rhs.target->priority;
              }
              return lhs.target->target_id < rhs.target->target_id;
            });

  out.interventions.reserve(static_cast<size_t>(max_actions_per_tick_));
  for (const auto& scored : ranked) {
    if (static_cast<int>(out.interventions.size()) >= max_actions_per_tick_) {
      break;
    }

    PlannerIntervention intervention;
    intervention.target.target_id = scored.target->target_id;
    intervention.intervention_type = scored.planned_action;
    FillInterventionParams(*scored.target, strategy, &intervention);
    intervention.rationale = "heuristic policy v2 strategy=" + out.strategy_id +
                             " target_score=" + std::to_string(scored.score);
    intervention.apply_order = static_cast<int>(out.interventions.size());
    out.interventions.push_back(std::move(intervention));
  }

  (void)input.active_tasks;
  return out;
}

}  // namespace maigent
