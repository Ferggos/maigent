#include "maigent/common/agent_lifecycle.h"

#include <chrono>

#include "maigent/common/constants.h"
#include "maigent/common/message_helpers.h"

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

  Publish(kSubjectAgentRegister, AGENT_REGISTER, AGENT_HEALTH_OK);
  thread_ = std::thread([this, heartbeat_ms]() {
    while (running_.load()) {
      Publish(kSubjectAgentHeartbeat, AGENT_HEARTBEAT, AGENT_HEALTH_OK);
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
    Publish(kSubjectAgentGoodbye, AGENT_GOODBYE, AGENT_HEALTH_DEAD);
  }
}

void AgentLifecycle::Publish(const std::string& subject, AgentSignalType signal_type,
                             AgentHealth health) {
  Envelope env;
  MessageKind kind = MK_UNSPECIFIED;
  if (signal_type == AGENT_REGISTER) {
    kind = MK_AGENT_REGISTER;
  } else if (signal_type == AGENT_HEARTBEAT) {
    kind = MK_AGENT_HEARTBEAT;
  } else if (signal_type == AGENT_GOODBYE) {
    kind = MK_AGENT_GOODBYE;
  }
  FillHeader(&env, SERVICE, kind, role_, agent_id_);
  *env.mutable_service()->mutable_agent_heartbeat() =
      MakeAgentHeartbeat(agent_id_, role_, capabilities_, inflight_fn_(), health,
                         signal_type);
  nats_->PublishEnvelope(subject, env);
}

}  // namespace maigent
