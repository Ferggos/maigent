#include "maigent/common/config.h"

namespace maigent {

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
  const std::string value = GetFlagValue(argc, argv, flag, "");
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
  const std::string value = GetFlagValue(argc, argv, flag, "");
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
  const std::string value = GetFlagValue(argc, argv, flag, "");
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
  std::vector<std::string> values;
  for (int i = 1; i + 1 < argc; ++i) {
    if (flag == argv[i]) {
      values.push_back(argv[i + 1]);
      ++i;
    }
  }
  return values;
}

}  // namespace maigent
