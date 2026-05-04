#pragma once

#include <cstdint>

namespace maigent {

// Threshold parameters for HeuristicPlannerModel.
// Blending weights and intervention parameter values (nice, cpu_weight, quota)
// are intentionally left inline — they are internal model mechanics.
struct PlannerPolicyConfig {
  // ---- Input pressure re-scoring ------------------------------------------
  // Planner independently re-scores the SystemMonitorPressureOutput it
  // receives to derive cpu/memory/io pressure scores for ChooseStrategy.
  double cpu_usage_moderate         = 72.0;    // %
  double cpu_usage_high             = 88.0;
  // Host memory used-ratio thresholds (used in per-target memory scoring).
  double host_mem_used_ratio_moderate = 0.84;  // 1 - (available / total)
  double host_mem_used_ratio_high     = 0.92;
  double cpu_psi_moderate           =  0.4;    // PSI CPU some
  double cpu_psi_high               =  1.2;
  double mem_available_moderate_mb  = 1536.0;  // LowValueScore floor
  double mem_available_high_mb      =  768.0;
  double mem_psi_moderate           =  0.3;    // PSI mem some
  double mem_psi_high               =  1.0;
  double io_psi_moderate            =  0.6;    // PSI IO some
  double io_psi_high                =  2.0;

  // ---- Strategy selection -------------------------------------------------
  double strategy_emergency_memory_min   = 0.60;  // memory_pressure for kEmergency
  double strategy_relieve_memory_bias_gap = 0.08; // memory must exceed cpu by at least this
  double strategy_relieve_bias_min       = 0.45;  // kRelieveCpu / kRelieveMemory activation

  // ---- Per-target CPU scoring ---------------------------------------------
  double target_cpu_intensity_moderate  = 0.30;
  double target_cpu_intensity_high      = 1.20;
  double target_cpu_usage_moderate      = 0.30;   // cpu_usage / age_sec
  double target_cpu_usage_high          = 1.00;
  double target_cpu_pressure_moderate   = 0.20;
  double target_cpu_pressure_high       = 0.80;
  double target_cpu_throttle_moderate   = 0.05;
  double target_cpu_throttle_high       = 0.25;
  double target_age_moderate_sec        = 5.0;
  double target_age_high_sec            = 30.0;

  // ---- Per-target memory scoring ------------------------------------------
  double target_mem_current_moderate_mb = 256.0;
  double target_mem_current_high_mb     = 2048.0;
  double target_mem_ratio_moderate      = 0.03;   // memory_current / host_total
  double target_mem_ratio_high          = 0.12;
  double target_mem_growth_moderate_mb  = 32.0;
  double target_mem_growth_high_mb      = 256.0;
  double target_mem_pressure_moderate   = 0.2;
  double target_mem_pressure_high       = 0.8;
  double target_mem_events_moderate     = 1.0;    // memory_events_high_delta count
  double target_mem_events_high         = 4.0;

  // ---- Target candidate filtering -----------------------------------------
  double target_score_min = 0.12;  // targets below this score are excluded

  // ---- Severe memory state detection --------------------------------------
  double   severe_mem_overload_probability = 0.80;
  int64_t  severe_mem_available_mb         = 1024;
  double   severe_mem_pressure_some        = 0.80;
  double   severe_mem_target_ratio         = 0.08;
  double   severe_mem_target_delta_mb      = 128.0;

  // ---- Priority penalty ---------------------------------------------------
  int    priority_critical_bound   = 90;
  int    priority_high_bound       = 70;
  int    priority_mid_bound        = 50;
  double priority_penalty_critical = 0.50;
  double priority_penalty_high     = 0.35;
  double priority_penalty_mid      = 0.22;
  double priority_penalty_low      = 0.10;  // priority in [0, priority_mid_bound)
  double priority_penalty_none     = 0.02;  // priority < 0 (background / no deadline)
};

}  // namespace maigent
