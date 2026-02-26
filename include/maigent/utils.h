#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace maigent {

int64_t NowMs();
std::string MakeUuid();

std::string GetFlagValue(int argc, char** argv, const std::string& flag,
                         const std::string& default_value);
int GetFlagInt(int argc, char** argv, const std::string& flag, int default_value);
int64_t GetFlagInt64(int argc, char** argv, const std::string& flag,
                     int64_t default_value);
double GetFlagDouble(int argc, char** argv, const std::string& flag,
                     double default_value);
bool HasFlag(int argc, char** argv, const std::string& flag);
std::vector<std::string> GetMultiFlagValues(int argc, char** argv,
                                            const std::string& flag);

std::string JoinArgs(const std::vector<std::string>& args);

}  // namespace maigent
