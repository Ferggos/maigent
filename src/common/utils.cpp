#include "maigent/utils.h"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>

namespace maigent {

int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string MakeUuid() {
  static thread_local std::mt19937_64 rng(std::random_device{}());
  static std::atomic<uint64_t> counter{0};
  const uint64_t c = counter.fetch_add(1, std::memory_order_relaxed);
  const uint64_t a = rng();
  const uint64_t b = rng() ^ (c * 0x9e3779b97f4a7c15ULL);

  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(8)
      << static_cast<uint32_t>(a >> 32) << "-" << std::setw(4)
      << static_cast<uint16_t>((a >> 16) & 0xffff) << "-" << std::setw(4)
      << static_cast<uint16_t>(a & 0xffff) << "-" << std::setw(4)
      << static_cast<uint16_t>(b >> 48) << "-" << std::setw(12)
      << static_cast<uint64_t>(b & 0x0000ffffffffffffULL);
  return oss.str();
}

std::string GetFlagValue(int argc, char** argv, const std::string& flag,
                         const std::string& default_value) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (flag == argv[i]) {
      return argv[i + 1];
    }
  }
  return default_value;
}

int GetFlagInt(int argc, char** argv, const std::string& flag, int default_value) {
  const auto value = GetFlagValue(argc, argv, flag, "");
  if (value.empty()) {
    return default_value;
  }
  try {
    return std::stoi(value);
  } catch (...) {
    return default_value;
  }
}

int64_t GetFlagInt64(int argc, char** argv, const std::string& flag,
                     int64_t default_value) {
  const auto value = GetFlagValue(argc, argv, flag, "");
  if (value.empty()) {
    return default_value;
  }
  try {
    return std::stoll(value);
  } catch (...) {
    return default_value;
  }
}

double GetFlagDouble(int argc, char** argv, const std::string& flag,
                     double default_value) {
  const auto value = GetFlagValue(argc, argv, flag, "");
  if (value.empty()) {
    return default_value;
  }
  try {
    return std::stod(value);
  } catch (...) {
    return default_value;
  }
}

bool HasFlag(int argc, char** argv, const std::string& flag) {
  for (int i = 1; i < argc; ++i) {
    if (flag == argv[i]) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> GetMultiFlagValues(int argc, char** argv,
                                            const std::string& flag) {
  std::vector<std::string> out;
  for (int i = 1; i + 1 < argc; ++i) {
    if (flag == argv[i]) {
      out.emplace_back(argv[i + 1]);
      ++i;
    }
  }
  return out;
}

std::string JoinArgs(const std::vector<std::string>& args) {
  std::ostringstream oss;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i != 0) {
      oss << ' ';
    }
    oss << args[i];
  }
  return oss.str();
}

}  // namespace maigent
