#pragma once

#include <cstdint>

namespace maigent {

// Behavioural parameters for PlannerAgent. All timing values are in
// milliseconds; cycle counts are dimensionless integers.
//
// Key interdependency:
//   min_freeze_duration_ms should be >= cooldown_ms * sustained_high_cycles
//   so that a THAW cannot be triggered before the next HIGH-pressure cycle
//   would fire again. The defaults (15 000 ms >= 1 000 * 3) satisfy this.
struct PlannerAgentConfig {
  // Maximum number of runtime actions dispatched in a single planner cycle.
  int max_actions_per_cycle = 2;

  // Minimum wall-clock gap between successive intervention cycles (ms).
  int64_t cooldown_ms = 1000;

  // Consecutive HIGH-pressure cycles required before intervention fires.
  // Minimum effective value: 1.
  int sustained_high_cycles = 3;

  // How long to suppress an action after it fails (ms).
  int64_t action_failure_backoff_ms = 15000;

  // Post-dispatch cooldown for hard actions: FREEZE, KILL, SET_MEM_MAX,
  // SET_CPU_MAX (ms). Prevents hammering the same target.
  int64_t hard_action_cooldown_ms = 15000;

  // A frozen target stays frozen for at least this duration before THAW
  // eligibility is considered (ms). See interdependency note above.
  int64_t min_freeze_duration_ms = 15000;

  // Consecutive non-HIGH cycles (stable recovery) required to allow THAW.
  // Minimum effective value: 1.
  int thaw_stable_cycles = 2;

  // Backoff between successive THAW attempts when a THAW action fails (ms).
  int64_t thaw_retry_backoff_ms = 8000;
};

}  // namespace maigent
