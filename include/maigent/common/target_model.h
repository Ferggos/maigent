#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace maigent {

enum class TargetKind {
  kUnspecified = 0,
  kTask = 1,
  kProcess = 2,
  kCgroup = 3,
  kSystem = 4,
};

enum class TargetSource {
  kUnspecified = 0,
  kManagedTask = 1,
  kExternalGroup = 2,
  kExternalProcess = 3,
  kSystemService = 4,
};

enum class TargetAction {
  kUnspecified = 0,
  kRenice = 1,
  kSetCpuWeight = 2,
  kSetCpuMax = 3,
  kSetMemHigh = 4,
  kSetMemMax = 5,
  kFreeze = 6,
  kThaw = 7,
  kKill = 8,
};

struct UnifiedTarget {
  std::string target_id;
  TargetKind kind = TargetKind::kUnspecified;
  TargetSource source = TargetSource::kUnspecified;

  std::string owner_executor_id;
  std::string task_id;
  int pid = 0;
  std::string cgroup_path;

  std::string task_class;
  int priority = 0;
  bool is_protected = false;
  std::vector<TargetAction> allowed_actions;

  double cpu_usage = 0.0;
  double memory_current_mb = 0.0;
  double cpu_pressure = 0.0;
  double memory_pressure = 0.0;
  double io_pressure = 0.0;

  // Average CPU intensity over last sampling interval in cores (cpu-seconds/second).
  double cpu_usage_delta = 0.0;
  double memory_delta_mb = 0.0;
  double memory_ratio_of_host = 0.0;
  double age_sec = 0.0;
  double cpu_throttled_ratio = 0.0;
  int64_t memory_events_high_delta = 0;
  int64_t memory_events_oom_delta = 0;
};

TargetKind InferTargetKind(TargetSource source, const std::string& cgroup_path,
                           int pid);

}  // namespace maigent
