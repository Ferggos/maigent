#include "maigent/common/time_utils.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace maigent {

int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string FormatTs(int64_t unix_ms) {
  const std::time_t sec = static_cast<std::time_t>(unix_ms / 1000);
  const int millis = static_cast<int>(unix_ms % 1000);
  std::tm tm{};
  localtime_r(&sec, &tm);
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(3)
      << std::setfill('0') << millis;
  return oss.str();
}

}  // namespace maigent
