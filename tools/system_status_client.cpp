#include <iostream>
#include <string>

#include "maigent.pb.h"
#include "maigent/constants.h"
#include "maigent/message_utils.h"
#include "maigent/nats_wrapper.h"
#include "maigent/utils.h"

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
  maigent::FillHeader(&req, maigent::SYSTEM_STATUS, "status_client", sender_id,
                      "status-" + maigent::MakeUuid());
  req.mutable_system_status_request();

  maigent::Envelope reply;
  std::string err;
  if (!nats.RequestEnvelope(maigent::kSubjectCmdSystemStatus, req, 2000, &reply, &err) ||
      !reply.has_system_status()) {
    std::cerr << "system status request failed: " << err << "\n";
    return 1;
  }

  const auto& st = reply.system_status();
  std::cout << "system_status ts_ms=" << st.ts_ms() << "\n";
  std::cout << "pressure cpu=" << st.pressure().cpu_usage_pct()
            << " mem_available_mb=" << st.pressure().mem_available_mb()
            << " risk=" << st.pressure().risk_level() << "\n";
  std::cout << "directory alive=" << st.directory().alive_agents()
            << " dead=" << st.directory().dead_agents()
            << " total=" << st.directory().agents_size() << "\n";

  for (const auto& agent : st.directory().agents()) {
    std::cout << "  - " << agent.agent_id() << " role=" << agent.role()
              << " inflight=" << agent.inflight_tasks() << " health=" << agent.health()
              << " last_seen=" << agent.last_seen_ms() << "\n";
  }

  return 0;
}
