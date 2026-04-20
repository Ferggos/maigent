#pragma once

#include <string>

namespace maigent {

std::string MakeUuid();
std::string MakeTraceId();
std::string MakeTaskId();
std::string MakeLeaseId();
std::string MakeRequestId(const std::string& prefix);

}  // namespace maigent
