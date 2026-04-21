#include "feature_builder.h"

#include <utility>

namespace maigent {

SystemMonitorModelInput SystemMonitorFeatureBuilder::BuildModelInput(
    const SystemMonitorRawSnapshot& raw_snapshot,
    const std::vector<SystemMonitorPressureHistorySample>& pressure_history) const {
  SystemMonitorModelInput out;
  out.host.ts_ms = raw_snapshot.host.ts_ms;
  out.host.cpu_usage_pct = raw_snapshot.host.cpu_usage_pct;
  out.host.mem_total_mb = raw_snapshot.host.mem_total_mb;
  out.host.mem_available_mb = raw_snapshot.host.mem_available_mb;
  out.host.load1 = raw_snapshot.host.load1;
  out.host.psi_cpu_some = raw_snapshot.host.psi_cpu_some;
  out.host.psi_mem_some = raw_snapshot.host.psi_mem_some;
  out.host.psi_io_some = raw_snapshot.host.psi_io_some;

  out.targets.reserve(raw_snapshot.targets.size());
  for (const auto& target : raw_snapshot.targets) {
    UnifiedTarget model_target;
    model_target.target_id = target.target_id;
    model_target.kind = target.kind;
    model_target.source = target.source;
    model_target.owner_executor_id = target.owner_executor_id;
    model_target.task_id = target.task_id;
    model_target.pid = target.pid;
    model_target.cgroup_path = target.cgroup_path;
    model_target.task_class = target.task_class;
    model_target.priority = target.priority;
    model_target.cpu_usage = target.cpu_usage;
    model_target.memory_current_mb = target.memory_current_mb;
    model_target.cpu_pressure = target.cpu_pressure;
    model_target.memory_pressure = target.memory_pressure;
    model_target.io_pressure = target.io_pressure;
    out.targets.push_back(std::move(model_target));
  }

  out.pressure_history = pressure_history;
  return out;
}

}  // namespace maigent
