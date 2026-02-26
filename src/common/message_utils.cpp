#include "maigent/message_utils.h"

#include "maigent/constants.h"
#include "maigent/utils.h"

namespace maigent {

void FillHeader(maigent::Envelope* env, maigent::MsgType type,
                const std::string& sender_role, const std::string& sender_id,
                const std::string& request_id, const std::string& trace_id,
                const std::string& conversation_id, int64_t deadline_unix_ms) {
  auto* h = env->mutable_header();
  h->set_msg_type(type);
  h->set_request_id(request_id.empty() ? MakeUuid() : request_id);
  h->set_trace_id(trace_id.empty() ? MakeUuid() : trace_id);
  h->set_conversation_id(conversation_id);
  h->set_deadline_unix_ms(deadline_unix_ms);
  h->set_sender_role(sender_role);
  h->set_sender_id(sender_id);
  h->set_protocol_version(kProtocolVersion);
  h->set_sent_unix_ms(NowMs());
}

bool SerializeEnvelope(const maigent::Envelope& env, std::string* out) {
  return env.SerializeToString(out);
}

bool ParseEnvelope(const void* data, int len, maigent::Envelope* env) {
  return env->ParseFromArray(data, len);
}

maigent::AgentHeartbeat MakeAgentHeartbeat(const std::string& agent_id,
                                           const std::string& role,
                                           const std::vector<std::string>& caps,
                                           int inflight,
                                           maigent::AgentHealth health) {
  maigent::AgentHeartbeat hb;
  hb.set_agent_id(agent_id);
  hb.set_role(role);
  for (const auto& c : caps) {
    hb.add_capabilities(c);
  }
  hb.set_inflight_tasks(inflight);
  hb.set_health(health);
  hb.set_ts_ms(NowMs());
  hb.set_protocol_version(kProtocolVersion);
  return hb;
}

}  // namespace maigent
