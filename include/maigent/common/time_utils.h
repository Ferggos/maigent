#pragma once

#include <cstdint>
#include <string>

namespace maigent {

int64_t NowMs();
std::string FormatTs(int64_t unix_ms);

}  // namespace maigent
