#pragma once

#include <cstdint>

namespace maigent {

struct HostRawState {
  int64_t ts_ms = 0;
  double cpu_usage_pct = 0.0;
  int64_t mem_total_mb = 0;
  int64_t mem_available_mb = 0;
  double load1 = 0.0;
  double psi_cpu_some = 0.0;
  double psi_mem_some = 0.0;
  double psi_io_some = 0.0;
};

class HostRawCollector {
 public:
  HostRawCollector();

  bool Sample(HostRawState* out);

 private:
  bool ReadCpuSample(uint64_t* idle_out, uint64_t* total_out) const;
  int64_t ReadMemInfoMb(const char* key) const;
  double ReadLoadAvg() const;
  double ReadPressureAvg10(const char* path) const;

  uint64_t prev_idle_;
  uint64_t prev_total_;
  bool have_prev_cpu_;
};

}  // namespace maigent
