#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "maigent/common/nats_wrapper.h"

namespace maigent {

class AgentLifecycle {
 public:
  AgentLifecycle(NatsClient* nats, std::string role, std::string agent_id,
                 std::vector<std::string> capabilities,
                 std::function<int()> inflight_fn);
  ~AgentLifecycle();

  void Start(int heartbeat_ms = 1000);
  void Stop(bool publish_goodbye = true);

 private:
  void Publish(const std::string& subject, AgentSignalType signal_type,
               AgentHealth health);

  NatsClient* nats_;
  std::string role_;
  std::string agent_id_;
  std::vector<std::string> capabilities_;
  std::function<int()> inflight_fn_;
  std::atomic<bool> running_;
  std::thread thread_;
};

}  // namespace maigent
