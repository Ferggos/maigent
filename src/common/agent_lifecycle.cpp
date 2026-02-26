#include "maigent/agent_lifecycle.h"

#include <chrono>

#include "maigent/constants.h"
#include "maigent/message_utils.h"

namespace maigent {

AgentLifecycle::AgentLifecycle(NatsClient* nats, std::string role,
                               std::string agent_id,
                               std::vector<std::string> capabilities,
                               std::function<int()> inflight_fn)
    : nats_(nats),
      role_(std::move(role)),
      agent_id_(std::move(agent_id)),
      capabilities_(std::move(capabilities)),
      inflight_fn_(std::move(inflight_fn)),
      running_(false) {}

AgentLifecycle::~AgentLifecycle() { Stop(true); }

void AgentLifecycle::Start(int heartbeat_ms) {
  if (running_.exchange(true)) {
    return;
  }

  Publish(kSubjectAgentRegister, maigent::AGENT_HEALTH_OK);

  thread_ = std::thread([this, heartbeat_ms]() {
    while (running_.load()) {
      Publish(kSubjectAgentHeartbeat, maigent::AGENT_HEALTH_OK);
      std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_ms));
    }
  });
}

void AgentLifecycle::Stop(bool publish_goodbye) {
  if (!running_.exchange(false)) {
    return;
  }

  if (thread_.joinable()) {
    thread_.join();
  }

  if (publish_goodbye) {
    Publish(kSubjectAgentGoodbye, maigent::AGENT_HEALTH_DEAD);
  }
}

void AgentLifecycle::Publish(const std::string& subject, maigent::AgentHealth health) {
  maigent::Envelope env;
  FillHeader(&env, maigent::AGENT_HEARTBEAT, role_, agent_id_);
  auto hb = MakeAgentHeartbeat(agent_id_, role_, capabilities_, inflight_fn_(), health);
  *env.mutable_agent_heartbeat() = std::move(hb);
  nats_->PublishEnvelope(subject, env);
}

}  // namespace maigent
