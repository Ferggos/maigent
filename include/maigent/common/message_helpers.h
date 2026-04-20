#pragma once

#include <string>
#include <vector>

#include "maigent.pb.h"

namespace maigent {

void FillHeader(Envelope* env, MessageCategory category, MessageKind kind,
                const std::string& sender_role,
                const std::string& sender_id,
                const std::string& request_id = "",
                const std::string& trace_id = "",
                const std::string& conversation_id = "",
                int64_t deadline_unix_ms = 0);

bool SerializeEnvelope(const Envelope& env, std::string* out);
bool ParseEnvelope(const void* data, int len, Envelope* env);

AgentHeartbeat MakeAgentHeartbeat(const std::string& agent_id,
                                  const std::string& role,
                                  const std::vector<std::string>& capabilities,
                                  int inflight_tasks, AgentHealth health,
                                  AgentSignalType signal_type);

std::string HeaderRequestId(const Envelope& env);
std::string HeaderTraceId(const Envelope& env);

}  // namespace maigent
