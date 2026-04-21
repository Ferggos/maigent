#include "maigent/system_monitor/model_contract.h"

#include "maigent/common/target_model_proto.h"

namespace maigent {

PressureState ToProtoPressureState(const SystemMonitorPressureOutput& pressure) {
  PressureState out;
  out.set_ts_ms(pressure.ts_ms);
  out.set_cpu_usage_pct(pressure.cpu_usage_pct);
  out.set_mem_available_mb(pressure.mem_available_mb);
  out.set_load1(pressure.load1);
  out.set_cpu_pressure_some(pressure.cpu_pressure_some);
  out.set_memory_pressure_some(pressure.memory_pressure_some);
  out.set_io_pressure_some(pressure.io_pressure_some);
  out.set_risk_level(pressure.risk_level);
  return out;
}

ForecastState ToProtoForecastState(const SystemMonitorForecastOutput& forecast) {
  ForecastState out;
  out.set_ts_ms(forecast.ts_ms);
  out.set_predicted_cpu_usage_pct(forecast.predicted_cpu_usage_pct);
  out.set_predicted_mem_available_mb(forecast.predicted_mem_available_mb);
  out.set_risk_level(forecast.risk_level);
  out.set_predictor(forecast.predictor);
  out.set_overload_probability(forecast.overload_probability);
  return out;
}

CapacityState ToProtoCapacityState(const SystemMonitorCapacityOutput& capacity) {
  CapacityState out;
  out.set_ts_ms(capacity.ts_ms);
  out.set_cpu_millis_total(capacity.cpu_millis_total);
  out.set_cpu_millis_allocatable(capacity.cpu_millis_allocatable);
  out.set_mem_total_mb(capacity.mem_total_mb);
  out.set_mem_available_mb(capacity.mem_available_mb);
  out.set_mem_allocatable_mb(capacity.mem_allocatable_mb);
  out.set_max_managed_tasks(capacity.max_managed_tasks);
  return out;
}

TargetsState ToProtoTargetsState(const SystemMonitorModelOutput& output) {
  TargetsState out;
  out.set_ts_ms(output.targets_ts_ms);
  for (const auto& target : output.targets) {
    *out.add_targets() = ToProtoTargetInfo(target);
  }
  return out;
}

SystemMonitorPressureHistorySample ToPressureHistorySampleFromOutput(
    const SystemMonitorPressureOutput& pressure) {
  SystemMonitorPressureHistorySample out;
  out.ts_ms = pressure.ts_ms;
  out.cpu_usage_pct = pressure.cpu_usage_pct;
  out.mem_available_mb = pressure.mem_available_mb;
  out.risk_level = pressure.risk_level;
  return out;
}

}  // namespace maigent
