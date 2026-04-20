#include <atomic>
#include <chrono>
#include <csignal>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "maigent.pb.h"
#include "maigent/common/agent_lifecycle.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/logging.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"
#include "maigent/common/time_utils.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct AgentEntry {
  maigent::AgentRecord record;
  int64_t last_seen_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "directory_agent";
  const std::string agent_id = "directory-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int dead_after_ms = maigent::GetFlagInt(argc, argv, "--dead-after-ms", 5000);

  maigent::AgentLogger log(agent_id, "logs/directory.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, AgentEntry> agents;

  auto upsert = [&](const maigent::AgentHeartbeat& hb, bool force_dead) {
    std::lock_guard<std::mutex> lock(mu);
    AgentEntry& entry = agents[hb.agent_id()];
    entry.record.set_agent_id(hb.agent_id());
    entry.record.set_role(hb.role());
    entry.record.clear_capabilities();
    for (const auto& cap : hb.capabilities()) {
      entry.record.add_capabilities(cap);
    }
    entry.record.set_inflight_tasks(hb.inflight_tasks());
    entry.record.set_health(force_dead ? maigent::AGENT_HEALTH_DEAD : hb.health());
    entry.record.set_protocol_version(hb.protocol_version());
    entry.last_seen_ms = maigent::NowMs();
    entry.record.set_last_seen_ms(entry.last_seen_ms);
  };

  auto on_agent_signal = [&](const maigent::NatsMessage& msg, bool goodbye) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_service() || !env.service().has_agent_heartbeat()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::SERVICE) {
      return;
    }
    const auto& hb = env.service().agent_heartbeat();
    upsert(hb, goodbye || hb.signal_type() == maigent::AGENT_GOODBYE);
    log.Debug("agent signal role=" + hb.role() + " id=" + hb.agent_id());
  };

  nats.Subscribe(maigent::kSubjectAgentRegister,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, false); });
  nats.Subscribe(maigent::kSubjectAgentHeartbeat,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, false); });
  nats.Subscribe(maigent::kSubjectAgentGoodbye,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, true); });

  nats.Subscribe(maigent::kSubjectSvcDirSnapshotRequest,
                 [&](const maigent::NatsMessage& msg) {
    maigent::Envelope request;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()),
                                &request) ||
        !request.has_service() ||
        !request.service().has_directory_snapshot_request()) {
      return;
    }
    if (!request.has_header() || request.header().message_category() != maigent::SERVICE ||
        request.header().message_kind() != maigent::MK_DIRECTORY_SNAPSHOT_REQUEST) {
      return;
    }

    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SERVICE,
                        maigent::MK_DIRECTORY_SNAPSHOT_RESPONSE,
                        role, agent_id,
                        request.header().request_id(), request.header().trace_id(),
                        request.header().conversation_id());

    int alive = 0;
    int dead = 0;
    const int64_t now = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto* snapshot = reply.mutable_service()->mutable_directory_snapshot();
      for (auto& [id, entry] : agents) {
        if (now - entry.last_seen_ms > dead_after_ms) {
          entry.record.set_health(maigent::AGENT_HEALTH_DEAD);
        }
        if (entry.record.health() == maigent::AGENT_HEALTH_DEAD) {
          ++dead;
        } else {
          ++alive;
        }
        *snapshot->add_agents() = entry.record;
      }
      snapshot->set_alive_agents(alive);
      snapshot->set_dead_agents(dead);
      snapshot->set_ts_ms(now);
    }

    nats.RespondEnvelope(msg, reply);
  });

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"directory.snapshot", "agent.registry"},
                                    []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    const int64_t now = maigent::NowMs();
    {
      std::lock_guard<std::mutex> lock(mu);
      for (auto& [id, entry] : agents) {
        if (now - entry.last_seen_ms > dead_after_ms) {
          entry.record.set_health(maigent::AGENT_HEALTH_DEAD);
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
