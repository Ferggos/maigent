#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <spawn.h>
#include <string>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
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

extern char** environ;

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct RunningTask {
  pid_t pid = -1;
  std::string lease_id;
  int64_t started_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "executor_agent";
  const std::string id_suffix = maigent::GetFlagValue(argc, argv, "--id", maigent::MakeUuid());
  const std::string agent_id = "exec-" + id_suffix;
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, RunningTask> running;

  auto publish_task_event = [&](const std::string& subject, const std::string& task_id,
                                const std::string& lease_id,
                                maigent::TaskEventType type, pid_t pid,
                                int exit_code, const std::string& error,
                                int64_t started_ts = 0) {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::TASK_EVENT, role, agent_id);
    auto* te = evt.mutable_task_event();
    te->set_task_id(task_id);
    te->set_lease_id(lease_id);
    te->set_event_type(type);
    te->set_pid(static_cast<int32_t>(pid));
    te->set_exit_code(exit_code);
    te->set_ts_ms(maigent::NowMs());
    te->set_error(error);
    if (started_ts > 0) {
      te->set_started_ts_ms(started_ts);
    }
    nats.PublishEnvelope(subject, evt);
  };

  auto release_lease = [&](const std::string& task_id, const std::string& lease_id,
                           const std::string& reason) {
    maigent::Envelope req;
    maigent::FillHeader(&req, maigent::LEASE_RELEASE, role, agent_id,
                        "release-" + task_id + "-" + maigent::MakeUuid());
    req.mutable_lease_release()->set_task_id(task_id);
    req.mutable_lease_release()->set_lease_id(lease_id);
    req.mutable_lease_release()->set_reason(reason);

    maigent::Envelope reply;
    std::string err;
    if (!nats.RequestEnvelope(maigent::kSubjectCmdLeaseRelease, req, 1000, &reply, &err)) {
      maigent::LogWarn(role, "lease release failed for task=" + task_id + " err=" + err);
    }
  };

  nats.QueueSubscribe(maigent::kSubjectCmdExecLaunch, maigent::kExecQueueGroup,
                      [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_launch_task()) {
      return;
    }

    const auto& launch = env.launch_task();
    std::vector<std::string> argv_storage;
    argv_storage.reserve(static_cast<size_t>(launch.args_size() + 1));
    argv_storage.push_back(launch.cmd());
    for (const auto& arg : launch.args()) {
      argv_storage.push_back(arg);
    }

    std::vector<char*> argv_ptrs;
    argv_ptrs.reserve(argv_storage.size() + 1);
    for (auto& s : argv_storage) {
      argv_ptrs.push_back(s.data());
    }
    argv_ptrs.push_back(nullptr);

    pid_t pid = -1;
    const int rc =
        posix_spawnp(&pid, launch.cmd().c_str(), nullptr, nullptr, argv_ptrs.data(), environ);
    if (rc != 0) {
      publish_task_event(maigent::kSubjectEvtTaskFailed, launch.task_id(), launch.lease_id(),
                         maigent::TASK_FAILED, -1, -1,
                         std::string("spawn failed: ") + std::strerror(rc));
      release_lease(launch.task_id(), launch.lease_id(), "spawn_failed");
      return;
    }

    const int64_t started_ms = maigent::NowMs();
    {
      std::lock_guard<std::mutex> lock(mu);
      running[launch.task_id()] = RunningTask{pid, launch.lease_id(), started_ms};
    }

    publish_task_event(maigent::kSubjectEvtTaskStarted, launch.task_id(), launch.lease_id(),
                       maigent::TASK_STARTED, pid, 0, "", started_ms);

    std::thread([&, task_id = launch.task_id(), lease_id = launch.lease_id(), pid,
                 started_ms]() {
      int status = 0;
      const pid_t waited = waitpid(pid, &status, 0);

      if (waited < 0) {
        publish_task_event(maigent::kSubjectEvtTaskFailed, task_id, lease_id,
                           maigent::TASK_FAILED, pid, -1, "waitpid failed", started_ms);
      } else if (WIFEXITED(status)) {
        const int code = WEXITSTATUS(status);
        if (code == 0) {
          publish_task_event(maigent::kSubjectEvtTaskFinished, task_id, lease_id,
                             maigent::TASK_FINISHED, pid, code, "", started_ms);
        } else {
          publish_task_event(maigent::kSubjectEvtTaskFailed, task_id, lease_id,
                             maigent::TASK_FAILED, pid, code,
                             "process exited with non-zero code", started_ms);
        }
      } else if (WIFSIGNALED(status)) {
        publish_task_event(maigent::kSubjectEvtTaskFailed, task_id, lease_id,
                           maigent::TASK_FAILED, pid, 128 + WTERMSIG(status),
                           "process terminated by signal", started_ms);
      } else {
        publish_task_event(maigent::kSubjectEvtTaskFailed, task_id, lease_id,
                           maigent::TASK_FAILED, pid, -1,
                           "process finished with unknown status", started_ms);
      }

      release_lease(task_id, lease_id, "process_done");

      std::lock_guard<std::mutex> lock(mu);
      running.erase(task_id);
    }).detach();
  });

  nats.QueueSubscribe(maigent::kSubjectCmdExecControl, maigent::kExecQueueGroup,
                      [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_control_action()) {
      return;
    }

    const auto& action = env.control_action();
    pid_t target_pid = action.pid();

    if (target_pid <= 0 && !action.task_id().empty()) {
      std::lock_guard<std::mutex> lock(mu);
      auto it = running.find(action.task_id());
      if (it != running.end()) {
        target_pid = it->second.pid;
      }
    }

    bool success = false;
    std::string reason;
    if (action.action_type() == maigent::RENICE && target_pid > 0) {
      int nice_value = 10;
      auto it = action.params().find("nice");
      if (it != action.params().end()) {
        try {
          nice_value = std::stoi(it->second);
        } catch (...) {
          nice_value = 10;
        }
      }
      if (setpriority(PRIO_PROCESS, target_pid, nice_value) == 0) {
        success = true;
        reason = "renice applied";
      } else {
        reason = std::string("renice failed: ") + std::strerror(errno);
      }
    } else {
      reason = "control action not implemented";
    }

    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::CONTROL_APPLIED, role, agent_id);
    auto* applied = evt.mutable_control_applied();
    applied->set_task_id(action.task_id());
    applied->set_pid(target_pid > 0 ? target_pid : 0);
    applied->set_action_type(action.action_type());
    applied->set_success(success);
    applied->set_reason(reason);
    applied->set_ts_ms(maigent::NowMs());
    nats.PublishEnvelope(maigent::kSubjectEvtControlApplied, evt);
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    return static_cast<int>(running.size());
  };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id, {"exec.launch", "exec.control.renice"}, inflight_fn);
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
