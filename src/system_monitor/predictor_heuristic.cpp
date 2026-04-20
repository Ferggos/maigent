#include "maigent/system_monitor/predictor.h"

#include <algorithm>

#include "maigent/common/time_utils.h"

namespace maigent {

ForecastState HeuristicPredictor::Predict(const PressureState& latest,
                                          const std::vector<PressureState>& history) {
  double cpu_sum = latest.cpu_usage_pct();
  double mem_sum = static_cast<double>(latest.mem_available_mb());
  int n = 1;

  for (auto it = history.rbegin(); it != history.rend() && n < 6; ++it) {
    cpu_sum += it->cpu_usage_pct();
    mem_sum += static_cast<double>(it->mem_available_mb());
    ++n;
  }

  const double cpu_avg = cpu_sum / static_cast<double>(n);
  const int64_t mem_avg = static_cast<int64_t>(mem_sum / static_cast<double>(n));

  ForecastState out;
  out.set_ts_ms(NowMs());
  out.set_predicted_cpu_usage_pct(std::min(100.0, cpu_avg * 1.08));
  out.set_predicted_mem_available_mb(std::max<int64_t>(0, mem_avg - 64));
  out.set_predictor("heuristic_v1");

  RiskLevel risk = RISK_LOW;
  if (out.predicted_cpu_usage_pct() >= 85.0 || out.predicted_mem_available_mb() < 768) {
    risk = RISK_HIGH;
  } else if (out.predicted_cpu_usage_pct() >= 70.0 ||
             out.predicted_mem_available_mb() < 1536) {
    risk = RISK_MED;
  }
  out.set_risk_level(risk);

  if (risk == RISK_HIGH) {
    out.set_overload_probability(0.85);
  } else if (risk == RISK_MED) {
    out.set_overload_probability(0.45);
  } else {
    out.set_overload_probability(0.15);
  }

  return out;
}

}  // namespace maigent
