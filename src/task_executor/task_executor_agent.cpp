#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
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
#include "maigent/common/agent_lifecycle.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/logging.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"
#include "maigent/common/time_utils.h"

extern char** environ;

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct RunningTask {
  std::string task_id;
  std::string trace_id;
  std::string lease_id;
  std::string executor_id;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  pid_t pid = -1;
  int64_t submitted_ms = 0;
  int64_t lease_decision_ms = 0;
  int64_t started_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "task_executor_agent";
  const std::string executor_id =
      maigent::GetFlagValue(argc, argv, "--executor-id", "exec-1");
  const std::string agent_id = "taskexec-" + executor_id + "-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");

  maigent::AgentLogger log(agent_id,
                           "logs/task_executor_" + executor_id + ".log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, RunningTask> running;

  auto publish_task_event = [&](const std::string& subject,
                                const RunningTask& task,
                                maigent::TaskEventType event_type, int exit_code,
                                const std::string& error) {
    maigent::MessageKind kind = maigent::MK_UNSPECIFIED;
    if (event_type == maigent::TASK_SUBMITTED) {
      kind = maigent::MK_TASK_SUBMITTED;
    } else if (event_type == maigent::TASK_QUEUED) {
      kind = maigent::MK_TASK_QUEUED;
    } else if (event_type == maigent::TASK_DEQUEUED) {
      kind = maigent::MK_TASK_DEQUEUED;
    } else if (event_type == maigent::TASK_STARTED) {
      kind = maigent::MK_TASK_STARTED;
    } else if (event_type == maigent::TASK_FINISHED) {
      kind = maigent::MK_TASK_FINISHED;
    } else if (event_type == maigent::TASK_FAILED) {
      kind = maigent::MK_TASK_FAILED;
    } else if (event_type == maigent::TASK_QUEUE_TIMEOUT) {
      kind = maigent::MK_TASK_QUEUE_TIMEOUT;
    }

    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::EVENT, kind, role, agent_id,
                        maigent::MakeRequestId("evt-task"), task.trace_id,
                        task.task_id);
    auto* te = evt.mutable_event()->mutable_task_event();
    te->set_task_id(task.task_id);
    te->set_trace_id(task.trace_id);
    te->set_lease_id(task.lease_id);
    te->set_executor_id(task.executor_id);
    te->set_event_type(event_type);
    te->set_pid(task.pid > 0 ? task.pid : 0);
    te->set_exit_code(exit_code);
    te->set_ts_ms(maigent::NowMs());
    te->set_error(error);
    te->set_submitted_ts_ms(task.submitted_ms);
    te->set_lease_decision_ts_ms(task.lease_decision_ms);
    te->set_started_ts_ms(task.started_ms);
    if (event_type == maigent::TASK_FINISHED || event_type == maigent::TASK_FAILED) {
      te->set_finished_ts_ms(maigent::NowMs());
    }
    te->set_task_class(task.task_class);
    nats.PublishEnvelope(subject, evt);
  };

  auto publish_control_result = [&](const maigent::ControlAction& action, bool success,
                                    const std::string& reason) {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::EVENT,
                        success ? maigent::MK_CONTROL_APPLIED
                                : maigent::MK_CONTROL_FAILED,
                        role, agent_id,
                        maigent::MakeRequestId("evt-control"), "",
                        action.task_id().empty() ? action.target_id()
                                                 : action.task_id());
    auto* result = evt.mutable_event()->mutable_actuator_result();
    result->set_target_id(action.target_id());
    result->set_task_id(action.task_id());
    result->set_executor_id(executor_id);
    result->set_pid(action.pid());
    result->set_cgroup_path(action.cgroup_path());
    result->set_action_type(action.action_type());
    result->set_success(success);
    result->set_reason(reason);
    result->set_ts_ms(maigent::NowMs());
    nats.PublishEnvelope(success ? maigent::kSubjectEvtControlApplied
                                : maigent::kSubjectEvtControlFailed,
                        evt);
  };

  auto release_lease = [&](const RunningTask& task, const std::string& reason) {
    maigent::Envelope req;
    maigent::FillHeader(&req, maigent::COMMAND, maigent::MK_LEASE_RELEASE,
                        role, agent_id,
                        maigent::MakeRequestId("lease-release"), task.trace_id,
                        task.task_id);
    auto* release = req.mutable_command()->mutable_lease_release();
    release->set_task_id(task.task_id);
    release->set_lease_id(task.lease_id);
    release->set_reason(reason);

    maigent::Envelope reply;
    std::string err;
    if (!nats.RequestEnvelope(maigent::kSubjectCmdLeaseRelease, req, 1000, &reply, &err)) {
      log.Warn("lease release failed err=" + err,
               {req.header().request_id(), task.task_id, task.trace_id});
    }
  };

  nats.QueueSubscribe(maigent::kSubjectCmdTaskExecLaunch, maigent::kTaskExecQueueGroup,
                      [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_command() || !env.command().has_launch_task()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != maigent::MK_TASK_LAUNCH) {
      return;
    }

    const auto& launch = env.command().launch_task();
    RunningTask task;
    task.task_id = launch.task_id();
    task.trace_id = launch.trace_id().empty() ? env.header().trace_id() : launch.trace_id();
    task.lease_id = launch.lease_id();
    task.executor_id = executor_id;
    task.cgroup_path = launch.cgroup_path();
    task.task_class = launch.task_class();
    task.priority = launch.priority();
    task.submitted_ms = launch.submitted_ts_ms();
    task.lease_decision_ms = launch.lease_decision_ts_ms();

    std::vector<std::string> argv_storage;
    argv_storage.reserve(static_cast<size_t>(launch.args_size() + 1));
    argv_storage.push_back(launch.cmd());
    for (const auto& arg : launch.args()) {
      argv_storage.push_back(arg);
    }

    std::vector<char*> argv_ptr;
    argv_ptr.reserve(argv_storage.size() + 1);
    for (auto& arg : argv_storage) {
      argv_ptr.push_back(arg.data());
    }
    argv_ptr.push_back(nullptr);

    pid_t pid = -1;
    const int rc =
        posix_spawnp(&pid, launch.cmd().c_str(), nullptr, nullptr, argv_ptr.data(), environ);
    if (rc != 0) {
      publish_task_event(maigent::kSubjectEvtTaskFailed, task, maigent::TASK_FAILED, -1,
                         std::string("spawn failed: ") + std::strerror(rc));
      release_lease(task, "spawn_failed");
      log.Error("spawn failed cmd=" + launch.cmd() +
                " err=" + std::string(std::strerror(rc)),
                {env.header().request_id(), task.task_id, task.trace_id});
      return;
    }

    task.pid = pid;
    task.started_ms = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      running[task.task_id] = task;
    }

    publish_task_event(maigent::kSubjectEvtTaskStarted, task, maigent::TASK_STARTED, 0, "");
    log.Info("started task pid=" + std::to_string(pid),
             {env.header().request_id(), task.task_id, task.trace_id});

    std::thread([&, task]() {
      int status = 0;
      const pid_t waited = waitpid(task.pid, &status, 0);

      RunningTask final_task = task;
      bool success = false;
      int exit_code = -1;
      std::string err;

      if (waited < 0) {
        err = "waitpid failed";
      } else if (WIFEXITED(status)) {
        exit_code = WEXITSTATUS(status);
        success = (exit_code == 0);
        if (!success) {
          err = "process exited non-zero";
        }
      } else if (WIFSIGNALED(status)) {
        exit_code = 128 + WTERMSIG(status);
        err = "process terminated by signal";
      } else {
        err = "unknown process status";
      }

      if (success) {
        publish_task_event(maigent::kSubjectEvtTaskFinished, final_task,
                           maigent::TASK_FINISHED, exit_code, "");
      } else {
        publish_task_event(maigent::kSubjectEvtTaskFailed, final_task,
                           maigent::TASK_FAILED, exit_code, err);
      }

      release_lease(final_task, "process_done");

      {
        std::lock_guard<std::mutex> lock(mu);
        running.erase(final_task.task_id);
      }

      log.Info(std::string("task done status=") + (success ? "finished" : "failed") +
                   " exit_code=" + std::to_string(exit_code),
               {"", final_task.task_id, final_task.trace_id});
    }).detach();
  });

  nats.Subscribe(maigent::TaskExecControlSubject(executor_id),
                 [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()),
                                &env) ||
        !env.has_command() || !env.command().has_control_action()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != maigent::MK_TASK_CONTROL) {
      return;
    }

    auto action = env.command().control_action();

    RunningTask task;
    bool found = false;
    {
      std::lock_guard<std::mutex> lock(mu);
      if (!action.task_id().empty()) {
        auto it = running.find(action.task_id());
        if (it != running.end()) {
          task = it->second;
          found = true;
        }
      }
      if (!found && action.pid() > 0) {
        for (const auto& [task_id, t] : running) {
          if (t.pid == action.pid()) {
            task = t;
            found = true;
            break;
          }
        }
      }
    }

    if (!found) {
      publish_control_result(action, false, "task not owned by this executor");
      return;
    }

    action.set_executor_id(executor_id);
    action.set_pid(task.pid);

    bool success = false;
    std::string reason;
    switch (action.action_type()) {
      case maigent::RENICE: {
        int nice_value = 10;
        auto it = action.numeric_params().find("nice");
        if (it != action.numeric_params().end()) {
          nice_value = static_cast<int>(it->second);
        }
        if (setpriority(PRIO_PROCESS, task.pid, nice_value) == 0) {
          success = true;
          reason = "renice applied";
        } else {
          reason = std::string("renice failed: ") + std::strerror(errno);
        }
        break;
      }
      case maigent::FREEZE: {
        if (kill(task.pid, SIGSTOP) == 0) {
          success = true;
          reason = "process stopped";
        } else {
          reason = std::string("freeze failed: ") + std::strerror(errno);
        }
        break;
      }
      case maigent::THAW: {
        if (kill(task.pid, SIGCONT) == 0) {
          success = true;
          reason = "process continued";
        } else {
          reason = std::string("thaw failed: ") + std::strerror(errno);
        }
        break;
      }
      case maigent::KILL: {
        if (kill(task.pid, SIGKILL) == 0) {
          success = true;
          reason = "process killed";
        } else {
          reason = std::string("kill failed: ") + std::strerror(errno);
        }
        break;
      }
      default:
        reason = "action is not implemented by task executor";
        break;
    }

    publish_control_result(action, success, reason);
    if (success) {
      log.Info("control applied action=" + std::to_string(action.action_type()) +
                   " task_id=" + task.task_id,
               {env.header().request_id(), task.task_id, task.trace_id});
    } else {
      log.Warn("control failed action=" + std::to_string(action.action_type()) +
                   " reason=" + reason,
               {env.header().request_id(), task.task_id, task.trace_id});
    }
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    return static_cast<int>(running.size());
  };

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"taskexec.launch", "taskexec.control"},
                                    inflight_fn);
  lifecycle.Start(1000);

  log.Info("started executor_id=" + executor_id);

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
