#pragma once

#include <string>

namespace maigent {

void LogInfo(const std::string& who, const std::string& message);
void LogWarn(const std::string& who, const std::string& message);
void LogError(const std::string& who, const std::string& message);

}  // namespace maigent
