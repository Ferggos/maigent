#include "host_raw_collector.h"

#include <fstream>
#include <sstream>
#include <string>

#include "maigent/common/time_utils.h"

namespace maigent {

HostRawCollector::HostRawCollector()
    : prev_idle_(0), prev_total_(0), have_prev_cpu_(false) {}

bool HostRawCollector::Sample(HostRawState* out) {
  out->ts_ms = NowMs();

  uint64_t idle = 0;
  uint64_t total = 0;
  if (ReadCpuSample(&idle, &total)) {
    if (have_prev_cpu_ && total > prev_total_) {
      const uint64_t delta_idle = idle - prev_idle_;
      const uint64_t delta_total = total - prev_total_;
      if (delta_total > 0) {
        out->cpu_usage_pct =
            100.0 * (1.0 - static_cast<double>(delta_idle) / delta_total);
      }
    }
    prev_idle_ = idle;
    prev_total_ = total;
    have_prev_cpu_ = true;
  }

  out->mem_total_mb = ReadMemInfoMb("MemTotal:");
  out->mem_available_mb = ReadMemInfoMb("MemAvailable:");
  out->load1 = ReadLoadAvg();
  out->psi_cpu_some = ReadPressureAvg10("/proc/pressure/cpu");
  out->psi_mem_some = ReadPressureAvg10("/proc/pressure/memory");
  out->psi_io_some = ReadPressureAvg10("/proc/pressure/io");
  return true;
}

bool HostRawCollector::ReadCpuSample(uint64_t* idle_out, uint64_t* total_out) const {
  std::ifstream in("/proc/stat");
  if (!in.is_open()) {
    return false;
  }

  std::string line;
  std::getline(in, line);
  std::istringstream iss(line);

  std::string cpu;
  uint64_t user = 0;
  uint64_t nice = 0;
  uint64_t system = 0;
  uint64_t idle = 0;
  uint64_t iowait = 0;
  uint64_t irq = 0;
  uint64_t softirq = 0;
  uint64_t steal = 0;
  iss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;

  if (cpu != "cpu") {
    return false;
  }

  *idle_out = idle + iowait;
  *total_out = user + nice + system + idle + iowait + irq + softirq + steal;
  return true;
}

int64_t HostRawCollector::ReadMemInfoMb(const char* key) const {
  std::ifstream in("/proc/meminfo");
  if (!in.is_open()) {
    return 0;
  }

  std::string k;
  int64_t value_kb = 0;
  std::string unit;
  while (in >> k >> value_kb >> unit) {
    if (k == key) {
      return value_kb / 1024;
    }
  }
  return 0;
}

double HostRawCollector::ReadLoadAvg() const {
  std::ifstream in("/proc/loadavg");
  if (!in.is_open()) {
    return 0.0;
  }
  double load1 = 0.0;
  in >> load1;
  return load1;
}

double HostRawCollector::ReadPressureAvg10(const char* path) const {
  std::ifstream in(path);
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

}  // namespace maigent
