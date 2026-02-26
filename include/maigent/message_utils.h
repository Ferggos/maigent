#pragma once

#include <string>
#include <vector>

#include "maigent.pb.h"

namespace maigent {

void FillHeader(maigent::Envelope* env, maigent::MsgType type,
                const std::string& sender_role, const std::string& sender_id,
                const std::string& request_id = "",
                const std::string& trace_id = "",
                const std::string& conversation_id = "",
                int64_t deadline_unix_ms = 0);

bool SerializeEnvelope(const maigent::Envelope& env, std::string* out);
bool ParseEnvelope(const void* data, int len, maigent::Envelope* env);

maigent::AgentHeartbeat MakeAgentHeartbeat(const std::string& agent_id,
                                           const std::string& role,
                                           const std::vector<std::string>& caps,
                                           int inflight,
                                           maigent::AgentHealth health);

}  // namespace maigent
