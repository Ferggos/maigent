#include "raw_collector.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

namespace maigent {

namespace {

double ParsePressureAvg10(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0.0;
  }
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("some", 0) != 0) {
      continue;
    }
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      if (token.rfind("avg10=", 0) == 0) {
        try {
          return std::stod(token.substr(6));
        } catch (...) {
          return 0.0;
        }
      }
    }
  }
  return 0.0;
}

int64_t ReadIntFile(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }
  int64_t value = 0;
  in >> value;
  return value;
}

int64_t ReadVmRssMb(int pid) {
  if (pid <= 0) {
    return 0;
  }

  std::ifstream in("/proc/" + std::to_string(pid) + "/status");
  if (!in.is_open()) {
    return 0;
  }

  std::string key;
  int64_t value_kb = 0;
  std::string unit;
  while (in >> key >> value_kb >> unit) {
    if (key == "VmRSS:") {
      return value_kb / 1024;
    }
  }
  return 0;
}

double ReadCpuStatUsageHint(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0.0;
  }

  std::string key;
  int64_t value = 0;
  while (in >> key >> value) {
    if (key == "usage_usec") {
      return static_cast<double>(value) / 1000000.0;
    }
    std::string rest_of_line;
    std::getline(in, rest_of_line);
  }
  return 0.0;
}

int64_t ReadMemoryEventsTotal(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }

  std::string key;
  int64_t value = 0;
  int64_t total = 0;
  while (in >> key >> value) {
    total += value;
  }
  return total;
}

int64_t ReadIoStatLines(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }
  int64_t lines = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      ++lines;
    }
  }
  return lines;
}

SystemMonitorTargetRawState BuildManagedTaskTarget(
    const SystemMonitorManagedTaskRawRef& task,
    const std::filesystem::path& cgroup_root) {
  SystemMonitorTargetRawState out;
  out.target_id = "managed:" + task.task_id;
  out.kind = TargetKind::kTask;
  out.source = TargetSource::kManagedTask;
  out.owner_executor_id = task.executor_id;
  out.task_id = task.task_id;
  out.pid = task.pid;
  out.cgroup_path = task.cgroup_path;
  out.task_class = task.task_class;
  out.priority = task.priority;
  out.memory_current_mb = static_cast<double>(ReadVmRssMb(task.pid));

  if (!task.cgroup_path.empty()) {
    const std::filesystem::path cg = cgroup_root / task.cgroup_path;
    out.cpu_usage = ReadCpuStatUsageHint(cg / "cpu.stat");
    const int64_t mem_events = ReadMemoryEventsTotal(cg / "memory.events");
    const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
    out.cpu_pressure = ParsePressureAvg10(cg / "cpu.pressure");
    out.memory_pressure = ParsePressureAvg10(cg / "memory.pressure");
    out.io_pressure = ParsePressureAvg10(cg / "io.pressure");
    if (mem_events > 0) {
      out.memory_pressure = std::max(out.memory_pressure, 0.1);
    }
    if (io_lines > 0) {
      out.io_pressure = std::max(out.io_pressure, 0.1);
    }
  }

  return out;
}

SystemMonitorTargetRawState BuildExternalGroupTarget(
    const std::filesystem::path& cgroup_root) {
  SystemMonitorTargetRawState out;
  out.target_id = "external_group:system.slice";
  out.kind = TargetKind::kCgroup;
  out.source = TargetSource::kExternalGroup;
  out.cgroup_path = "system.slice";
  out.task_class = "external";
  out.priority = 0;

  const std::filesystem::path cg = cgroup_root / "system.slice";
  out.memory_current_mb =
      static_cast<double>(ReadIntFile(cg / "memory.current")) / (1024.0 * 1024.0);
  out.cpu_usage = ReadCpuStatUsageHint(cg / "cpu.stat");
  const int64_t mem_events = ReadMemoryEventsTotal(cg / "memory.events");
  const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
  out.cpu_pressure = ParsePressureAvg10(cg / "cpu.pressure");
  out.memory_pressure = ParsePressureAvg10(cg / "memory.pressure");
  out.io_pressure = ParsePressureAvg10(cg / "io.pressure");
  if (mem_events > 0) {
    out.memory_pressure = std::max(out.memory_pressure, 0.1);
  }
  if (io_lines > 0) {
    out.io_pressure = std::max(out.io_pressure, 0.1);
  }
  return out;
}

SystemMonitorTargetRawState BuildSystemServiceTarget() {
  SystemMonitorTargetRawState out;
  out.target_id = "system_service:pid1";
  out.kind = TargetKind::kSystem;
  out.source = TargetSource::kSystemService;
  out.pid = 1;
  out.task_class = "system";
  out.priority = 100;
  out.memory_current_mb = static_cast<double>(ReadVmRssMb(1));
  return out;
}

}  // namespace

SystemMonitorRawCollector::SystemMonitorRawCollector(std::string cgroup_root)
    : cgroup_root_(std::move(cgroup_root)) {}

bool SystemMonitorRawCollector::CollectSnapshot(
    const std::vector<SystemMonitorManagedTaskRawRef>& managed_tasks,
    SystemMonitorRawSnapshot* out) {
  if (out == nullptr) {
    return false;
  }

  const bool host_ok = host_raw_collector_.Sample(&out->host);
  out->targets.clear();
  out->targets.reserve(managed_tasks.size() + 2);

  const std::filesystem::path cgroup_root(cgroup_root_);
  for (const auto& task : managed_tasks) {
    out->targets.push_back(BuildManagedTaskTarget(task, cgroup_root));
  }
  out->targets.push_back(BuildExternalGroupTarget(cgroup_root));
  out->targets.push_back(BuildSystemServiceTarget());

  return host_ok;
}

}  // namespace maigent
