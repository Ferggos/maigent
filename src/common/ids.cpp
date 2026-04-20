#include "maigent/common/ids.h"

#include <atomic>
#include <iomanip>
#include <random>
#include <sstream>

namespace maigent {

std::string MakeUuid() {
  static thread_local std::mt19937_64 rng(std::random_device{}());
  static std::atomic<uint64_t> counter{0};

  const uint64_t a = rng();
  const uint64_t b = rng() ^ (counter.fetch_add(1, std::memory_order_relaxed) *
                              0x9e3779b97f4a7c15ULL);

  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(8)
      << static_cast<uint32_t>(a >> 32) << '-' << std::setw(4)
      << static_cast<uint16_t>((a >> 16) & 0xffff) << '-' << std::setw(4)
      << static_cast<uint16_t>(a & 0xffff) << '-' << std::setw(4)
      << static_cast<uint16_t>(b >> 48) << '-' << std::setw(12)
      << static_cast<uint64_t>(b & 0x0000ffffffffffffULL);
  return oss.str();
}

std::string MakeTraceId() { return "trace-" + MakeUuid(); }
std::string MakeTaskId() { return "task-" + MakeUuid(); }
std::string MakeLeaseId() { return "lease-" + MakeUuid(); }

std::string MakeRequestId(const std::string& prefix) {
  return prefix + "-" + MakeUuid();
}

}  // namespace maigent
