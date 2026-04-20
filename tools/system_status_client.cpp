#include <iostream>
#include <string>

#include "maigent.pb.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"

int main(int argc, char** argv) {
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");

  maigent::NatsClient nats;
  const std::string sender_id = "status-client-" + maigent::MakeUuid();
  if (!nats.Connect(nats_url, sender_id)) {
    std::cerr << "failed to connect to NATS\n";
    return 1;
  }

  maigent::Envelope req;
  maigent::FillHeader(&req, maigent::COMMAND, maigent::MK_SYSTEM_STATUS_REQUEST,
                      "system_status_client",
                      sender_id, maigent::MakeRequestId("status"),
                      maigent::MakeTraceId());
  req.mutable_command()->mutable_system_status_request();

  maigent::Envelope reply;
  std::string error;
  if (!nats.RequestEnvelope(maigent::kSubjectCmdSystemStatus, req, 2000, &reply, &error) ||
      !reply.has_service() || !reply.service().has_system_status_reply() ||
      !reply.has_header() ||
      reply.header().message_category() != maigent::SERVICE ||
      reply.header().message_kind() != maigent::MK_SYSTEM_STATUS_RESPONSE) {
    std::cerr << "status request failed: " << error << '\n';
    return 1;
  }

  const auto& st = reply.service().system_status_reply();
  std::cout << "status ts_ms=" << st.ts_ms() << '\n';
  std::cout << "pressure cpu=" << st.pressure().cpu_usage_pct()
            << " mem_available_mb=" << st.pressure().mem_available_mb()
            << " risk=" << st.pressure().risk_level() << '\n';
  std::cout << "directory alive=" << st.directory().alive_agents()
            << " dead=" << st.directory().dead_agents()
            << " total=" << st.directory().agents_size() << '\n';

  for (const auto& agent : st.directory().agents()) {
    std::cout << "  - " << agent.agent_id() << " role=" << agent.role()
              << " inflight=" << agent.inflight_tasks()
              << " health=" << agent.health()
              << " last_seen=" << agent.last_seen_ms() << '\n';
  }

  return 0;
}
