#include "maigent/common/message_helpers.h"

#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/time_utils.h"

namespace maigent {

void FillHeader(Envelope* env, MessageCategory category, MessageKind kind,
                const std::string& sender_role, const std::string& sender_id,
                const std::string& request_id, const std::string& trace_id,
                const std::string& conversation_id, int64_t deadline_unix_ms) {
  auto* header = env->mutable_header();
  header->set_message_category(category);
  header->set_message_kind(kind);
  header->set_request_id(request_id.empty() ? MakeRequestId("req") : request_id);
  header->set_trace_id(trace_id.empty() ? MakeTraceId() : trace_id);
  header->set_conversation_id(conversation_id);
  header->set_deadline_unix_ms(deadline_unix_ms);
  header->set_sender_role(sender_role);
  header->set_sender_id(sender_id);
  header->set_protocol_version(kProtocolVersion);
  header->set_ts_ms(NowMs());
}

bool SerializeEnvelope(const Envelope& env, std::string* out) {
  return env.SerializeToString(out);
}

bool ParseEnvelope(const void* data, int len, Envelope* env) {
  return env->ParseFromArray(data, len);
}

AgentHeartbeat MakeAgentHeartbeat(const std::string& agent_id,
                                  const std::string& role,
                                  const std::vector<std::string>& capabilities,
                                  int inflight_tasks, AgentHealth health,
                                  AgentSignalType signal_type) {
  AgentHeartbeat hb;
  hb.set_agent_id(agent_id);
  hb.set_role(role);
  for (const auto& cap : capabilities) {
    hb.add_capabilities(cap);
  }
  hb.set_inflight_tasks(inflight_tasks);
  hb.set_health(health);
  hb.set_signal_type(signal_type);
  hb.set_ts_ms(NowMs());
  hb.set_protocol_version(kProtocolVersion);
  return hb;
}

std::string HeaderRequestId(const Envelope& env) {
  return env.has_header() ? env.header().request_id() : std::string();
}

std::string HeaderTraceId(const Envelope& env) {
  return env.has_header() ? env.header().trace_id() : std::string();
}

}  // namespace maigent
