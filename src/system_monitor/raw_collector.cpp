#include "raw_collector.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

namespace maigent {

namespace {

struct ProcessStatSample {
  std::string comm;
  uint64_t user_ticks = 0;
  uint64_t system_ticks = 0;
  uint64_t starttime_ticks = 0;
};

std::vector<std::string> SplitWhitespace(const std::string& value) {
  std::istringstream iss(value);
  std::vector<std::string> tokens;
  std::string token;
  while (iss >> token) {
    tokens.push_back(token);
  }
  return tokens;
}

std::optional<ProcessStatSample> ReadProcessStatSample(int pid) {
  if (pid <= 0) {
    return std::nullopt;
  }

  std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
  if (!in.is_open()) {
    return std::nullopt;
  }

  std::string line;
  std::getline(in, line);
  const auto open = line.find('(');
  const auto close = line.rfind(')');
  if (open == std::string::npos || close == std::string::npos ||
      close <= open + 1) {
    return std::nullopt;
  }

  const std::vector<std::string> fields_after_comm =
      SplitWhitespace(line.substr(close + 1));
  // fields_after_comm[0] is stat field 3 (state). starttime is field 22.
  if (fields_after_comm.size() <= 19) {
    return std::nullopt;
  }

  ProcessStatSample out;
  out.comm = line.substr(open + 1, close - open - 1);
  try {
    out.user_ticks = std::stoull(fields_after_comm[11]);
    out.system_ticks = std::stoull(fields_after_comm[12]);
    out.starttime_ticks = std::stoull(fields_after_comm[19]);
  } catch (...) {
    return std::nullopt;
  }
  return out;
}

int64_t ClockTicksPerSecond() {
  const long ticks = sysconf(_SC_CLK_TCK);
  return ticks > 0 ? static_cast<int64_t>(ticks) : 100;
}

int64_t ReadBootTimeMs() {
  std::ifstream in("/proc/stat");
  if (!in.is_open()) {
    return 0;
  }
  std::string key;
  int64_t value = 0;
  while (in >> key >> value) {
    if (key == "btime") {
      return value * 1000;
    }
    std::string rest_of_line;
    std::getline(in, rest_of_line);
  }
  return 0;
}

int64_t ProcessStartMs(uint64_t starttime_ticks) {
  const int64_t boot_ms = ReadBootTimeMs();
  if (boot_ms <= 0) {
    return 0;
  }
  return boot_ms +
         static_cast<int64_t>((starttime_ticks * 1000ULL) /
                              static_cast<uint64_t>(ClockTicksPerSecond()));
}

bool HasUnsafePathSegment(const std::filesystem::path& path) {
  for (const auto& part : path) {
    if (part == "..") {
      return true;
    }
  }
  return false;
}

std::string StripLeadingSlash(std::string value) {
  while (!value.empty() && value.front() == '/') {
    value.erase(value.begin());
  }
  return value;
}

std::string NormalizeCgroupPath(const std::string& input,
                                const std::filesystem::path& cgroup_root) {
  if (input.empty()) {
    return {};
  }

  std::filesystem::path p(input);
  if (HasUnsafePathSegment(p)) {
    return {};
  }

  try {
    if (p.is_absolute()) {
      const auto root = std::filesystem::weakly_canonical(cgroup_root);
      const auto candidate = std::filesystem::weakly_canonical(p);
      const auto root_str = root.string();
      const auto candidate_str = candidate.string();
      if (candidate_str != root_str &&
          candidate_str.rfind(root_str + "/", 0) != 0) {
        return {};
      }
      return std::filesystem::relative(candidate, root).generic_string();
    }
  } catch (...) {
    return {};
  }

  p = p.lexically_normal();
  if (p.empty() || p.is_absolute() || HasUnsafePathSegment(p)) {
    return {};
  }
  return p.generic_string();
}

std::string ReadProcCgroupPathInternal(int pid) {
  if (pid <= 0) {
    return {};
  }
  std::ifstream in("/proc/" + std::to_string(pid) + "/cgroup");
  if (!in.is_open()) {
    return {};
  }

  std::string line;
  while (std::getline(in, line)) {
    const auto second_colon = line.find(':', line.find(':') + 1);
    if (second_colon == std::string::npos) {
      continue;
    }
    std::string path = line.substr(second_colon + 1);
    path = StripLeadingSlash(path);
    if (!path.empty()) {
      return path;
    }
  }
  return {};
}

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

void PopulateCgroupMetrics(const std::filesystem::path& cg,
                           SystemMonitorTargetRawState* out) {
  if (out == nullptr) {
    return;
  }

  const CpuStatSample cpu_stat = ReadCpuStatSample(cg / "cpu.stat");
  out->cpu_usage = cpu_stat.usage_seconds;
  out->cpu_nr_periods = cpu_stat.nr_periods;
  out->cpu_nr_throttled = cpu_stat.nr_throttled;
  if (cpu_stat.nr_periods > 0) {
    out->cpu_throttled_ratio = std::clamp(
        static_cast<double>(cpu_stat.nr_throttled) /
            static_cast<double>(cpu_stat.nr_periods),
        0.0, 1.0);
  }
  const MemoryEventsSample mem_events = ReadMemoryEventsSample(cg / "memory.events");
  out->memory_events_high = mem_events.high;
  out->memory_events_oom = mem_events.oom;
  const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
  out->cpu_pressure = ParsePressureAvg(cg / "cpu.pressure", "some", "avg10");
  out->memory_pressure =
      ParsePressureAvg(cg / "memory.pressure", "some", "avg10");
  out->io_pressure = ParsePressureAvg(cg / "io.pressure", "some", "avg10");
  if (mem_events.total > 0) {
    out->memory_pressure = std::max(out->memory_pressure, 0.1);
  }
  if (io_lines > 0) {
    out->io_pressure = std::max(out->io_pressure, 0.1);
  }
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
    PopulateCgroupMetrics(cg, &out);
  }

  return out;
}

std::optional<SystemMonitorTargetRawState> BuildExternalProcessTarget(
    const SystemMonitorExternalProcessRawRef& process,
    const std::filesystem::path& cgroup_root,
    SystemMonitorExternalProcessRemoval* removal) {
  const auto stat = ReadProcessStatSample(process.pid);
  if (!stat.has_value()) {
    if (removal != nullptr) {
      removal->target_id = process.target_id;
      removal->pid = process.pid;
      removal->reason = "pid missing";
    }
    return std::nullopt;
  }

  if (process.expected_starttime_ticks > 0 &&
      stat->starttime_ticks != process.expected_starttime_ticks) {
    if (removal != nullptr) {
      removal->target_id = process.target_id;
      removal->pid = process.pid;
      removal->reason = "pid reused";
    }
    return std::nullopt;
  }

  SystemMonitorTargetRawState out;
  out.target_id = process.target_id.empty()
                      ? MakeExternalProcessTargetId(process.pid,
                                                    stat->starttime_ticks)
                      : process.target_id;
  out.kind = TargetKind::kProcess;
  out.source = TargetSource::kExternalProcess;
  out.started_ms = ProcessStartMs(stat->starttime_ticks);
  if (out.started_ms <= 0) {
    out.started_ms = process.registered_at_ms;
  }
  out.pid = process.pid;
  out.task_class = !process.label.empty() ? process.label : stat->comm;
  out.priority = process.priority;
  out.allow_control = process.allow_control;
  out.memory_current_mb = static_cast<double>(ReadVmRssMb(process.pid));
  out.cpu_usage =
      static_cast<double>(stat->user_ticks + stat->system_ticks) /
      static_cast<double>(ClockTicksPerSecond());

  std::string cgroup_path =
      NormalizeCgroupPath(process.cgroup_path, cgroup_root);
  if (cgroup_path.empty()) {
    cgroup_path = NormalizeCgroupPath(ReadProcCgroupPathInternal(process.pid),
                                      cgroup_root);
  }
  out.cgroup_path = cgroup_path;
  if (!out.cgroup_path.empty()) {
    PopulateCgroupMetrics(cgroup_root / out.cgroup_path, &out);
    // Preserve process-level CPU accounting for external process targets. The
    // cgroup counters remain useful for throttling/pressure/event features.
    out.cpu_usage =
        static_cast<double>(stat->user_ticks + stat->system_ticks) /
        static_cast<double>(ClockTicksPerSecond());
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
  PopulateCgroupMetrics(cg, &out);
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
    const std::vector<SystemMonitorExternalProcessRawRef>& external_processes,
    SystemMonitorRawSnapshot* out) {
  if (out == nullptr) {
    return false;
  }

  const bool host_ok = host_raw_collector_.Sample(&out->host);
  out->targets.clear();
  out->external_process_removals.clear();
  out->targets.reserve(managed_tasks.size() + external_processes.size() + 2);

  const std::filesystem::path cgroup_root(cgroup_root_);
  for (const auto& task : managed_tasks) {
    out->targets.push_back(BuildManagedTaskTarget(task, cgroup_root));
  }
  for (const auto& process : external_processes) {
    SystemMonitorExternalProcessRemoval removal;
    auto target = BuildExternalProcessTarget(process, cgroup_root, &removal);
    if (target.has_value()) {
      out->targets.push_back(std::move(*target));
    } else if (!removal.target_id.empty()) {
      out->external_process_removals.push_back(std::move(removal));
    }
  }
  out->targets.push_back(BuildExternalGroupTarget(cgroup_root));
  out->targets.push_back(BuildSystemServiceTarget());

  return host_ok;
}

std::string MakeExternalProcessTargetId(int pid, uint64_t starttime_ticks) {
  return "external_process:" + std::to_string(pid) + ":" +
         std::to_string(starttime_ticks);
}

std::string ReadProcCgroupPath(int pid) {
  return ReadProcCgroupPathInternal(pid);
}

bool ReadProcessStarttimeTicks(int pid, uint64_t* out) {
  const auto stat = ReadProcessStatSample(pid);
  if (!stat.has_value()) {
    return false;
  }
  if (out != nullptr) {
    *out = stat->starttime_ticks;
  }
  return true;
}

}  // namespace maigent
