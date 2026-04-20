#include "maigent/system_monitor/system_monitor_model.h"

#include <algorithm>
#include <thread>
#include <vector>

namespace maigent {

namespace {

TargetInfo ToProtoTargetInfo(const SystemMonitorTargetInput& in) {
  TargetInfo out;
  out.set_target_id(in.target_id);
  out.set_source_type(in.source_type);
  out.set_owner_executor_id(in.owner_executor_id);
  out.set_task_id(in.task_id);
  out.set_pid(in.pid);
  out.set_cgroup_path(in.cgroup_path);
  out.set_task_class(in.task_class);
  out.set_priority(in.priority);
  out.set_cpu_usage(in.cpu_usage);
  out.set_memory_current_mb(in.memory_current_mb);
  out.set_cpu_pressure(in.cpu_pressure);
  out.set_memory_pressure(in.memory_pressure);
  out.set_io_pressure(in.io_pressure);
  return out;
}

PressureState ToProtoPressureStateFromHistorySample(
    const SystemMonitorPressureHistorySample& sample) {
  PressureState out;
  out.set_ts_ms(sample.ts_ms);
  out.set_cpu_usage_pct(sample.cpu_usage_pct);
  out.set_mem_available_mb(sample.mem_available_mb);
  out.set_risk_level(sample.risk_level);
  return out;
}

}  // namespace

SystemMonitorModelOutput HeuristicSystemMonitorModel::Evaluate(
    const SystemMonitorModelInput& input) {
  SystemMonitorModelOutput out;

  out.pressure.ts_ms = input.host.ts_ms;
  out.pressure.cpu_usage_pct = input.host.cpu_usage_pct;
  out.pressure.mem_available_mb = input.host.mem_available_mb;
  out.pressure.load1 = input.host.load1;
  out.pressure.cpu_pressure_some = input.host.psi_cpu_some;
  out.pressure.memory_pressure_some = input.host.psi_mem_some;
  out.pressure.io_pressure_some = input.host.psi_io_some;
  if (input.host.cpu_usage_pct >= 85.0 || input.host.mem_available_mb < 768 ||
      input.host.psi_mem_some > 1.0) {
    out.pressure.risk_level = RISK_HIGH;
  } else if (input.host.cpu_usage_pct >= 70.0 || input.host.mem_available_mb < 1536 ||
             input.host.psi_mem_some > 0.3) {
    out.pressure.risk_level = RISK_MED;
  } else {
    out.pressure.risk_level = RISK_LOW;
  }

  const PressureState latest_pressure = ToProtoPressureState(out.pressure);
  std::vector<PressureState> pressure_history_states;
  pressure_history_states.reserve(input.pressure_history.size() + 1);
  for (const auto& sample : input.pressure_history) {
    pressure_history_states.push_back(ToProtoPressureStateFromHistorySample(sample));
  }
  pressure_history_states.push_back(latest_pressure);

  const ForecastState forecast =
      predictor_.Predict(latest_pressure, pressure_history_states);
  out.forecast.ts_ms = forecast.ts_ms();
  out.forecast.predicted_cpu_usage_pct = forecast.predicted_cpu_usage_pct();
  out.forecast.predicted_mem_available_mb = forecast.predicted_mem_available_mb();
  out.forecast.risk_level = forecast.risk_level();
  out.forecast.predictor = forecast.predictor();
  out.forecast.overload_probability = forecast.overload_probability();

  out.capacity.ts_ms = input.host.ts_ms;
  const int cpu_total = static_cast<int>(std::thread::hardware_concurrency() * 1000);
  out.capacity.cpu_millis_total = cpu_total > 0 ? cpu_total : 4000;
  out.capacity.cpu_millis_allocatable =
      static_cast<int>(out.capacity.cpu_millis_total * 0.85);
  out.capacity.mem_total_mb = input.host.mem_total_mb;
  out.capacity.mem_available_mb = input.host.mem_available_mb;
  out.capacity.mem_allocatable_mb =
      static_cast<int64_t>(static_cast<double>(input.host.mem_total_mb) * 0.8);
  out.capacity.max_managed_tasks = std::max(4, out.capacity.cpu_millis_total / 750);

  out.targets_ts_ms = input.host.ts_ms;
  out.targets.reserve(input.targets.size());
  for (const auto& target_in : input.targets) {
    SystemMonitorTargetOutput target_out;
    target_out.target_id = target_in.target_id;
    target_out.source_type = target_in.source_type;
    target_out.owner_executor_id = target_in.owner_executor_id;
    target_out.task_id = target_in.task_id;
    target_out.pid = target_in.pid;
    target_out.cgroup_path = target_in.cgroup_path;
    target_out.task_class = target_in.task_class;
    target_out.priority = target_in.priority;
    target_out.cpu_usage = target_in.cpu_usage;
    target_out.memory_current_mb = target_in.memory_current_mb;
    target_out.cpu_pressure = target_in.cpu_pressure;
    target_out.memory_pressure = target_in.memory_pressure;
    target_out.io_pressure = target_in.io_pressure;

    TargetInfo proto_target = ToProtoTargetInfo(target_in);
    classifier_.Classify(&proto_target);
    target_out.is_protected = proto_target.is_protected();
    target_out.allowed_actions.reserve(proto_target.allowed_actions_size());
    for (int i = 0; i < proto_target.allowed_actions_size(); ++i) {
      target_out.allowed_actions.push_back(proto_target.allowed_actions(i));
    }

    out.targets.push_back(std::move(target_out));
  }

  return out;
}

}  // namespace maigent
