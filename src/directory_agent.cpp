#include <atomic>
#include <chrono>
#include <csignal>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "maigent.pb.h"
#include "maigent/agent_lifecycle.h"
#include "maigent/constants.h"
#include "maigent/logging.h"
#include "maigent/message_utils.h"
#include "maigent/nats_wrapper.h"
#include "maigent/utils.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct AgentRecord {
  maigent::AgentInfo info;
  int64_t last_seen_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int dead_after_ms = maigent::GetFlagInt(argc, argv, "--dead-after-ms", 5000);

  const std::string agent_id = "directory-" + maigent::MakeUuid();
  const std::string role = "directory_agent";

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, AgentRecord> agents;

  auto upsert = [&](const maigent::AgentHeartbeat& hb, maigent::AgentHealth override_health,
                    bool force_health) {
    std::lock_guard<std::mutex> lock(mu);
    AgentRecord& rec = agents[hb.agent_id()];
    rec.info.set_agent_id(hb.agent_id());
    rec.info.set_role(hb.role());
    rec.info.clear_capabilities();
    for (const auto& c : hb.capabilities()) {
      rec.info.add_capabilities(c);
    }
    rec.info.set_inflight_tasks(hb.inflight_tasks());
    rec.info.set_health(force_health ? override_health : hb.health());
    rec.info.set_protocol_version(hb.protocol_version());
    rec.last_seen_ms = maigent::NowMs();
    rec.info.set_last_seen_ms(rec.last_seen_ms);
  };

  auto on_agent_signal = [&](const maigent::NatsMessage& msg, bool goodbye) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_agent_heartbeat()) {
      return;
    }
    const auto& hb = env.agent_heartbeat();
    upsert(hb, maigent::AGENT_HEALTH_DEAD, goodbye);
  };

  nats.Subscribe(maigent::kSubjectAgentRegister,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, false); });
  nats.Subscribe(maigent::kSubjectAgentHeartbeat,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, false); });
  nats.Subscribe(maigent::kSubjectAgentGoodbye,
                 [&](const maigent::NatsMessage& msg) { on_agent_signal(msg, true); });

  nats.Subscribe(maigent::kSubjectCmdDirSnapshot, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::DIR_SNAPSHOT, role, agent_id);

    int alive = 0;
    int dead = 0;
    const int64_t now = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto* snapshot = reply.mutable_dir_snapshot();
      for (auto& [id, rec] : agents) {
        if (now - rec.last_seen_ms > dead_after_ms) {
          rec.info.set_health(maigent::AGENT_HEALTH_DEAD);
        }
        if (rec.info.health() == maigent::AGENT_HEALTH_DEAD) {
          ++dead;
        } else {
          ++alive;
        }
        *snapshot->add_agents() = rec.info;
      }
      snapshot->set_alive_agents(alive);
      snapshot->set_dead_agents(dead);
      snapshot->set_ts_ms(now);
    }

    nats.RespondEnvelope(msg, reply);
  });

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id, {"directory.snapshot", "agent.registry"}, [] { return 0; });
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");

  while (!g_stop.load()) {
    const int64_t now = maigent::NowMs();
    {
      std::lock_guard<std::mutex> lock(mu);
      for (auto& [id, rec] : agents) {
        if (now - rec.last_seen_ms > dead_after_ms) {
          rec.info.set_health(maigent::AGENT_HEALTH_DEAD);
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
