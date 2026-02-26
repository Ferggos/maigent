#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

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

struct ActiveTask {
  int32_t pid = 0;
  int64_t started_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "planner_agent";
  const std::string agent_id = "planner-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int64_t mem_high_threshold_mb =
      maigent::GetFlagInt64(argc, argv, "--mem-high-threshold-mb", 65536);
  const int max_actions = maigent::GetFlagInt(argc, argv, "--max-actions", 2);
  const int64_t control_cooldown_ms =
      maigent::GetFlagInt64(argc, argv, "--control-cooldown-ms", 2000);

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, ActiveTask> active_tasks;
  std::atomic<int64_t> last_control_ms{0};

  auto on_task_event = [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }

    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    if (e.event_type() == maigent::TASK_STARTED) {
      active_tasks[e.task_id()] = ActiveTask{e.pid(), e.ts_ms()};
    } else if (e.event_type() == maigent::TASK_FINISHED ||
               e.event_type() == maigent::TASK_FAILED) {
      active_tasks.erase(e.task_id());
    }
  };

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFinished, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFailed, on_task_event);

  nats.Subscribe(maigent::kSubjectStatePressure, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_pressure_update()) {
      return;
    }

    const auto& pressure = env.pressure_update();
    const bool high = pressure.risk_level() == maigent::HIGH ||
                      pressure.mem_available_mb() < mem_high_threshold_mb;
    if (!high) {
      return;
    }

    const int64_t now = maigent::NowMs();
    if (now - last_control_ms.load() < control_cooldown_ms) {
      return;
    }

    std::vector<std::pair<std::string, ActiveTask>> candidates;
    {
      std::lock_guard<std::mutex> lock(mu);
      candidates.reserve(active_tasks.size());
      for (const auto& [task_id, task] : active_tasks) {
        candidates.push_back({task_id, task});
      }
    }

    if (candidates.empty()) {
      return;
    }

    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.second.started_ms > b.second.started_ms; });

    int sent = 0;
    for (const auto& [task_id, task] : candidates) {
      if (sent >= max_actions) {
        break;
      }
      maigent::Envelope action_env;
      maigent::FillHeader(&action_env, maigent::CONTROL_ACTION, role, agent_id);
      auto* action = action_env.mutable_control_action();
      action->set_action_type(maigent::RENICE);
      action->set_task_id(task_id);
      action->set_pid(task.pid);
      (*action->mutable_params())["nice"] = "10";
      action->set_reason("high pressure mitigation");
      action->set_ts_ms(now);
      nats.PublishEnvelope(maigent::kSubjectCmdExecControl, action_env);
      ++sent;
    }

    if (sent > 0) {
      last_control_ms.store(now);
      maigent::LogInfo(role, "sent control actions=" + std::to_string(sent));
    }
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    return static_cast<int>(active_tasks.size());
  };

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"planner.pressure", "exec.control.renice"},
                                    inflight_fn);
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
