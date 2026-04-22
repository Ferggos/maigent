#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "maigent/common/target_model.h"
#include "host_raw_collector.h"

namespace maigent {

struct SystemMonitorManagedTaskRawRef {
  std::string task_id;
  std::string executor_id;
  int pid = 0;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  int64_t started_ms = 0;
};

struct SystemMonitorTargetRawState {
  std::string target_id;
  TargetKind kind = TargetKind::kUnspecified;
  TargetSource source = TargetSource::kUnspecified;
  int64_t started_ms = 0;
  std::string owner_executor_id;
  std::string task_id;
  int pid = 0;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  double cpu_usage = 0.0;
  double cpu_throttled_ratio = 0.0;
  double memory_current_mb = 0.0;
  int64_t memory_events_high = 0;
  int64_t memory_events_oom = 0;
  double cpu_pressure = 0.0;
  double memory_pressure = 0.0;
  double io_pressure = 0.0;
};

struct SystemMonitorRawSnapshot {
  HostRawState host;
  std::vector<SystemMonitorTargetRawState> targets;
};

class SystemMonitorRawCollector {
 public:
  explicit SystemMonitorRawCollector(std::string cgroup_root);

  bool CollectSnapshot(const std::vector<SystemMonitorManagedTaskRawRef>& managed_tasks,
                       SystemMonitorRawSnapshot* out);

 private:
  HostRawCollector host_raw_collector_;
  std::string cgroup_root_;
};

}  // namespace maigent
