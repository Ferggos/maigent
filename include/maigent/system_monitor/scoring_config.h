#pragma once

namespace maigent {

// Threshold parameters for HeuristicSystemMonitorModel.
// Blending weights (0.42, 0.22, …) are intentionally left inline — they are
// internal model mechanics, not deployment calibration values.
//
// IO thresholds are calibrated for a VM environment where the hypervisor's
// virtualised storage produces a persistent io_full baseline of ~0.3–2.1%
// (avg60) under idle; genuine IO pressure starts above ~3–8%.
// PSI CPU thresholds account for the slow Linux load-average EWMA: full
// 8-CPU saturation on an 8-vCPU VM only produces psi_cpu_some ~0.18–0.32.
struct SystemMonitorScoringConfig {
  // ---- Risk classification ------------------------------------------------
  double risk_high = 0.82;
  double risk_med  = 0.50;
  double saturated_primary_threshold = 0.85;
  double severe_risk_floor = 0.84;

  // ---- CPU signal thresholds ----------------------------------------------
  double cpu_usage_moderate        = 70.0;   // %
  double cpu_usage_high            = 85.0;
  double cpu_usage_severe_pct      = 90.0;
  double cpu_trend_delta_moderate  =  8.0;   // pp/sample delta
  double cpu_trend_delta_high      = 20.0;
  double cpu_trend_ema_moderate    =  4.0;   // short-minus-long EMA gap
  double cpu_trend_ema_high        = 12.0;
  double load_per_cpu_moderate     =  0.10;  // load1 / logical CPUs
  double load_per_cpu_high         =  0.50;
  double load_per_cpu_severe       =  0.75;
  double psi_cpu_moderate          =  0.05;  // PSI CPU some (avg10 + avg60)
  double psi_cpu_high              =  0.20;
  double cpu_pressure_severe       =  0.20;

  // ---- Memory signal thresholds -------------------------------------------
  double mem_used_ratio_moderate   = 0.84;   // 1 - (available / total)
  double mem_used_ratio_high       = 0.92;
  double mem_available_ratio_severe = 0.08;
  double mem_available_moderate_mb = 1536.0; // LowValueScore: concern starts here
  double mem_available_high_mb     =  768.0; // LowValueScore: hard floor
  double mem_available_severe_mb   =  768.0;
  double mem_drop_moderate_mb      =  128.0; // -delta per sample
  double mem_drop_high_mb          =  512.0;
  double swap_moderate             =  0.40;  // swap_used / swap_total
  double swap_high                 =  0.70;
  double swap_used_severe          =  0.70;
  double psi_mem_some_moderate     =  0.3;   // PSI mem some avg10
  double psi_mem_some_high         =  1.0;
  double mem_some_avg60_moderate   =  0.3;   // PSI mem some avg60
  double mem_some_avg60_high       =  0.8;
  double mem_full_moderate         =  0.05;  // PSI mem full (avg10 + avg60)
  double mem_full_high             =  0.10;
  double memory_pressure_severe    =  0.10;

  // ---- IO signal thresholds -----------------------------------------------
  double io_some_moderate          =  8.0;   // PSI IO some avg10
  double io_some_high              = 25.0;
  double io_some_avg60_moderate    =  3.0;   // PSI IO some avg60
  double io_some_avg60_high        = 10.0;
  double io_full_moderate          =  8.0;   // PSI IO full avg10
  double io_full_high              = 25.0;
  double io_full_avg60_moderate    =  2.5;   // PSI IO full avg60
  double io_full_avg60_high        =  7.0;
  double io_pressure_severe        =  7.0;   // PSI IO full avg60

  // ---- Bottleneck detection -----------------------------------------------
  double bottleneck_primary_min   = 0.20;   // score below this → kNone (primary)
  double bottleneck_secondary_min = 0.15;   // score below this → kNone (secondary)
};

}  // namespace maigent
