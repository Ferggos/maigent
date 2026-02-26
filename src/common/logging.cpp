#include "maigent/logging.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

namespace maigent {
namespace {

std::mutex g_log_mu;

std::string Timestamp() {
  using Clock = std::chrono::system_clock;
  const auto now = Clock::now();
  const auto tt = Clock::to_time_t(now);
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;
  std::tm tm{};
  localtime_r(&tt, &tm);
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(3)
      << std::setfill('0') << ms.count();
  return oss.str();
}

void Log(const char* level, const std::string& who, const std::string& message) {
  std::lock_guard<std::mutex> lock(g_log_mu);
  std::cerr << Timestamp() << " [" << level << "] [" << who << "] " << message
            << '\n';
}

}  // namespace

void LogInfo(const std::string& who, const std::string& message) {
  Log("INFO", who, message);
}

void LogWarn(const std::string& who, const std::string& message) {
  Log("WARN", who, message);
}

void LogError(const std::string& who, const std::string& message) {
  Log("ERROR", who, message);
}

}  // namespace maigent
