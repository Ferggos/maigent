#include "raw_collector.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>

namespace maigent {

namespace {

double ParsePressureAvg(const std::filesystem::path& file,
                        const char* line_prefix,
                        const char* metric_name) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0.0;
  }

  const std::string expected_prefix(line_prefix);
  const std::string expected_metric = std::string(metric_name) + "=";
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind(expected_prefix, 0) != 0) {
      continue;
    }
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      if (token.rfind(expected_metric, 0) == 0) {
        try {
          return std::stod(token.substr(expected_metric.size()));
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

struct CpuStatSample {
  double usage_seconds = 0.0;
  int64_t nr_periods = 0;
  int64_t nr_throttled = 0;
};

CpuStatSample ReadCpuStatSample(const std::filesystem::path& file) {
  CpuStatSample out;
  std::ifstream in(file);
  if (!in.is_open()) {
    return out;
  }

  std::string key;
  int64_t value = 0;
  while (in >> key >> value) {
    if (key == "usage_usec") {
      out.usage_seconds = static_cast<double>(value) / 1000000.0;
    } else if (key == "nr_periods") {
      out.nr_periods = value;
    } else if (key == "nr_throttled") {
      out.nr_throttled = value;
    }
    std::string rest_of_line;
    std::getline(in, rest_of_line);
  }
  return out;
}

struct MemoryEventsSample {
  int64_t total = 0;
  int64_t high = 0;
  int64_t oom = 0;
};

MemoryEventsSample ReadMemoryEventsSample(const std::filesystem::path& file) {
  MemoryEventsSample out;
  std::ifstream in(file);
  if (!in.is_open()) {
    return out;
  }

  std::string key;
  int64_t value = 0;
  while (in >> key >> value) {
    out.total += value;
    if (key == "high") {
      out.high = value;
    } else if (key == "oom") {
      out.oom = value;
    }
  }
  return out;
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
  out.started_ms = task.started_ms;
  out.owner_executor_id = task.executor_id;
  out.task_id = task.task_id;
  out.pid = task.pid;
  out.cgroup_path = task.cgroup_path;
  out.task_class = task.task_class;
  out.priority = task.priority;
  out.memory_current_mb = static_cast<double>(ReadVmRssMb(task.pid));

  if (!task.cgroup_path.empty()) {
    const std::filesystem::path cg = cgroup_root / task.cgroup_path;
    const CpuStatSample cpu_stat = ReadCpuStatSample(cg / "cpu.stat");
    out.cpu_usage = cpu_stat.usage_seconds;
    out.cpu_nr_periods = cpu_stat.nr_periods;
    out.cpu_nr_throttled = cpu_stat.nr_throttled;
    if (cpu_stat.nr_periods > 0) {
      out.cpu_throttled_ratio = std::clamp(
          static_cast<double>(cpu_stat.nr_throttled) /
              static_cast<double>(cpu_stat.nr_periods),
          0.0, 1.0);
    }
    const MemoryEventsSample mem_events = ReadMemoryEventsSample(cg / "memory.events");
    out.memory_events_high = mem_events.high;
    out.memory_events_oom = mem_events.oom;
    const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
    out.cpu_pressure = ParsePressureAvg(cg / "cpu.pressure", "some", "avg10");
    out.memory_pressure =
        ParsePressureAvg(cg / "memory.pressure", "some", "avg10");
    out.io_pressure = ParsePressureAvg(cg / "io.pressure", "some", "avg10");
    if (mem_events.total > 0) {
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
  const CpuStatSample cpu_stat = ReadCpuStatSample(cg / "cpu.stat");
  out.cpu_usage = cpu_stat.usage_seconds;
  out.cpu_nr_periods = cpu_stat.nr_periods;
  out.cpu_nr_throttled = cpu_stat.nr_throttled;
  if (cpu_stat.nr_periods > 0) {
    out.cpu_throttled_ratio = std::clamp(
        static_cast<double>(cpu_stat.nr_throttled) /
            static_cast<double>(cpu_stat.nr_periods),
        0.0, 1.0);
  }
  const MemoryEventsSample mem_events = ReadMemoryEventsSample(cg / "memory.events");
  out.memory_events_high = mem_events.high;
  out.memory_events_oom = mem_events.oom;
  const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
  out.cpu_pressure = ParsePressureAvg(cg / "cpu.pressure", "some", "avg10");
  out.memory_pressure = ParsePressureAvg(cg / "memory.pressure", "some", "avg10");
  out.io_pressure = ParsePressureAvg(cg / "io.pressure", "some", "avg10");
  if (mem_events.total > 0) {
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
