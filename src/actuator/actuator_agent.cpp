#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <sys/resource.h>
#include <thread>

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

bool WriteFile(const std::string& path, const std::string& value) {
  std::ofstream out(path, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    return false;
  }
  out << value;
  return out.good();
}

std::filesystem::path ResolveCgroupPath(const std::string& cgroup_path,
                                        const std::string& cgroup_root) {
  std::filesystem::path p(cgroup_path);
  if (p.empty()) {
    return {};
  }
  if (p.is_absolute()) {
    return p;
  }
  return std::filesystem::path(cgroup_root) / p;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "actuator_agent";
  const std::string agent_id = "actuator-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const std::string cgroup_root =
      maigent::GetFlagValue(argc, argv, "--cgroup-root", "/sys/fs/cgroup");

  maigent::AgentLogger log(agent_id, "logs/actuator.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  nats.Subscribe(maigent::kSubjectCmdActuatorApply, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_command() || !env.command().has_control_action()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != maigent::MK_ACTUATOR_APPLY) {
      return;
    }

    const auto& action = env.command().control_action();

    bool success = false;
    std::string reason;

    const int pid = action.pid();
    const auto cgroup_path = ResolveCgroupPath(action.cgroup_path(), cgroup_root);

    switch (action.action_type()) {
      case maigent::RENICE: {
        if (pid <= 0) {
          reason = "pid is required for RENICE";
          break;
        }
        int nice_value = 10;
        auto it = action.numeric_params().find("nice");
        if (it != action.numeric_params().end()) {
          nice_value = static_cast<int>(it->second);
        }
        if (setpriority(PRIO_PROCESS, static_cast<id_t>(pid), nice_value) == 0) {
          success = true;
          reason = "renice applied";
        } else {
          reason = std::string("renice failed: ") + std::strerror(errno);
        }
        break;
      }
      case maigent::FREEZE: {
        if (!cgroup_path.empty()) {
          success = WriteFile((cgroup_path / "cgroup.freeze").string(), "1");
          reason = success ? "cgroup frozen" : "failed to write cgroup.freeze";
        } else if (pid > 0) {
          success = (kill(pid, SIGSTOP) == 0);
          reason = success ? "process stopped"
                           : std::string("freeze failed: ") + std::strerror(errno);
        } else {
          reason = "freeze requires pid or cgroup_path";
        }
        break;
      }
      case maigent::THAW: {
        if (!cgroup_path.empty()) {
          success = WriteFile((cgroup_path / "cgroup.freeze").string(), "0");
          reason = success ? "cgroup thawed" : "failed to write cgroup.freeze";
        } else if (pid > 0) {
          success = (kill(pid, SIGCONT) == 0);
          reason = success ? "process continued"
                           : std::string("thaw failed: ") + std::strerror(errno);
        } else {
          reason = "thaw requires pid or cgroup_path";
        }
        break;
      }
      case maigent::KILL: {
        if (pid <= 0) {
          reason = "pid is required for KILL";
          break;
        }
        success = (kill(pid, SIGKILL) == 0);
        reason = success ? "process killed"
                         : std::string("kill failed: ") + std::strerror(errno);
        break;
      }
      case maigent::SET_CPU_WEIGHT: {
        if (cgroup_path.empty()) {
          reason = "cgroup_path is required for SET_CPU_WEIGHT";
          break;
        }
        auto it = action.numeric_params().find("cpu_weight");
        if (it == action.numeric_params().end()) {
          reason = "numeric param cpu_weight is required";
          break;
        }
        success = WriteFile((cgroup_path / "cpu.weight").string(),
                            std::to_string(static_cast<int>(it->second)));
        reason = success ? "cpu.weight updated" : "failed to write cpu.weight";
        break;
      }
      case maigent::SET_CPU_MAX: {
        if (cgroup_path.empty()) {
          reason = "cgroup_path is required for SET_CPU_MAX";
          break;
        }
        auto quota = action.numeric_params().find("quota");
        auto period = action.numeric_params().find("period");
        if (quota == action.numeric_params().end() ||
            period == action.numeric_params().end()) {
          reason = "numeric params quota and period are required";
          break;
        }
        success = WriteFile((cgroup_path / "cpu.max").string(),
                            std::to_string(static_cast<int>(quota->second)) + " " +
                                std::to_string(static_cast<int>(period->second)));
        reason = success ? "cpu.max updated" : "failed to write cpu.max";
        break;
      }
      case maigent::SET_MEM_HIGH: {
        if (cgroup_path.empty()) {
          reason = "cgroup_path is required for SET_MEM_HIGH";
          break;
        }
        auto it = action.numeric_params().find("mem_high_bytes");
        if (it == action.numeric_params().end()) {
          reason = "numeric param mem_high_bytes is required";
          break;
        }
        success = WriteFile((cgroup_path / "memory.high").string(),
                            std::to_string(static_cast<int64_t>(it->second)));
        reason = success ? "memory.high updated" : "failed to write memory.high";
        break;
      }
      case maigent::SET_MEM_MAX: {
        if (cgroup_path.empty()) {
          reason = "cgroup_path is required for SET_MEM_MAX";
          break;
        }
        auto it = action.numeric_params().find("mem_max_bytes");
        if (it == action.numeric_params().end()) {
          reason = "numeric param mem_max_bytes is required";
          break;
        }
        success = WriteFile((cgroup_path / "memory.max").string(),
                            std::to_string(static_cast<int64_t>(it->second)));
        reason = success ? "memory.max updated" : "failed to write memory.max";
        break;
      }
      default:
        reason = "unsupported action";
        break;
    }

    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::EVENT,
                        success ? maigent::MK_ACTUATOR_APPLIED
                                : maigent::MK_ACTUATOR_FAILED,
                        role, agent_id,
                        maigent::MakeRequestId("evt-actuator"),
                        env.header().trace_id(),
                        action.task_id().empty() ? action.target_id()
                                                 : action.task_id());
    auto* result = evt.mutable_event()->mutable_actuator_result();
    result->set_target_id(action.target_id());
    result->set_task_id(action.task_id());
    result->set_executor_id(action.executor_id());
    result->set_pid(action.pid());
    result->set_cgroup_path(action.cgroup_path());
    result->set_action_type(action.action_type());
    result->set_success(success);
    result->set_reason(reason);
    result->set_ts_ms(maigent::NowMs());

    nats.PublishEnvelope(success ? maigent::kSubjectEvtActuatorApplied
                                : maigent::kSubjectEvtActuatorFailed,
                        evt);

    if (success) {
      log.Info("applied action=" + std::to_string(action.action_type()) +
                   " target_id=" + action.target_id());
    } else {
      log.Warn("failed action=" + std::to_string(action.action_type()) +
                   " target_id=" + action.target_id() + " reason=" + reason);
    }
  });

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"actuator.apply", "external.control"},
                                    []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
