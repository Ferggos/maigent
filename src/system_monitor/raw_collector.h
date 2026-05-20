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

struct SystemMonitorExternalProcessRawRef {
  int pid = 0;
  std::string target_id;
  std::string label;
  std::string cgroup_path;
  int priority = 0;
  bool allow_control = false;
  uint64_t expected_starttime_ticks = 0;
  int64_t registered_at_ms = 0;
};

struct SystemMonitorExternalProcessRemoval {
  std::string target_id;
  int pid = 0;
  std::string reason;
};

struct SystemMonitorExternalCgroupRawRef {
  std::string target_id;
  std::string label;
  std::string cgroup_path;
  int priority = 0;
  bool allow_control = false;
  int64_t registered_at_ms = 0;
};

struct SystemMonitorExternalCgroupRemoval {
  std::string target_id;
  std::string cgroup_path;
  std::string reason;
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
  bool allow_control = false;
  double cpu_usage = 0.0;
  int64_t cpu_nr_periods = 0;
  int64_t cpu_nr_throttled = 0;
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
  std::vector<SystemMonitorExternalProcessRemoval> external_process_removals;
  std::vector<SystemMonitorExternalCgroupRemoval> external_cgroup_removals;
};

class SystemMonitorRawCollector {
 public:
  explicit SystemMonitorRawCollector(std::string cgroup_root);

  bool CollectSnapshot(const std::vector<SystemMonitorManagedTaskRawRef>& managed_tasks,
                       const std::vector<SystemMonitorExternalProcessRawRef>& external_processes,
                       const std::vector<SystemMonitorExternalCgroupRawRef>& external_cgroups,
                       SystemMonitorRawSnapshot* out);

 private:
  HostRawCollector host_raw_collector_;
  std::string cgroup_root_;
};

std::string MakeExternalProcessTargetId(int pid, uint64_t starttime_ticks);
std::string ReadProcCgroupPath(int pid);
bool ReadProcessStarttimeTicks(int pid, uint64_t* out);

}  // namespace maigent
