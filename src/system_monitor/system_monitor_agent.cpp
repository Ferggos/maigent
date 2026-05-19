#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>

#include "maigent.pb.h"
#include "maigent/common/agent_lifecycle.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/logging.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"
#include "maigent/common/time_utils.h"
#include "maigent/system_monitor/system_monitor_model.h"
#include "maigent/system_monitor/target_classifier.h"
#include "feature_builder.h"
#include "raw_collector.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct ExternalDiscoveryConfig {
  bool enabled = false;
  int64_t interval_ms = 2000;
  int max_processes = 32;
  int64_t idle_ttl_ms = 30000;
  uint64_t min_cpu_delta_ticks = 1;
};

struct RegisteredExternalProcess {
  int pid = 0;
  std::string target_id;
  std::string label;
  std::string external_id;
  std::string cgroup_path;
  int priority = 0;
  bool allow_control = false;
  uint64_t proc_starttime_ticks = 0;
  int64_t registered_at_ms = 0;
  bool auto_discovered = false;
};

struct ExternalDiscoveryState {
  bool initialized = false;
  uint64_t prev_cpu_ticks = 0;
  int64_t last_seen_ms = 0;
  int64_t last_relevant_ms = 0;
};

struct ExternalDiscoverySample {
  int pid = 0;
  std::string comm;
  std::string cmdline;
  uint64_t starttime_ticks = 0;
  uint64_t cpu_ticks = 0;
  int64_t rss_mb = 0;
};

struct ExternalDiscoveryCandidate {
  int pid = 0;
  std::string target_id;
  std::string label;
  std::string cgroup_path;
  uint64_t starttime_ticks = 0;
  uint64_t delta_cpu_ticks = 0;
  int64_t rss_mb = 0;
};

std::string Trim(std::string value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  return value;
}

std::vector<std::string> SplitWhitespace(const std::string& value) {
  std::istringstream iss(value);
  std::vector<std::string> tokens;
  std::string token;
  while (iss >> token) {
    tokens.push_back(token);
  }
  return tokens;
}

bool IsPidDirectoryName(const std::string& name) {
  if (name.empty()) {
    return false;
  }
  return std::all_of(name.begin(), name.end(), [](unsigned char ch) {
    return std::isdigit(ch) != 0;
  });
}

std::string ShortLabel(const ExternalDiscoverySample& sample) {
  std::string label = !sample.comm.empty() ? sample.comm : sample.cmdline;
  label = Trim(label);
  constexpr size_t kMaxLabelLen = 64;
  if (label.size() > kMaxLabelLen) {
    label.resize(kMaxLabelLen);
  }
  return label;
}

std::string ReadProcCmdline(int pid) {
  std::ifstream in("/proc/" + std::to_string(pid) + "/cmdline",
                   std::ios::in | std::ios::binary);
  if (!in.is_open()) {
    return {};
  }

  std::string value((std::istreambuf_iterator<char>(in)),
                    std::istreambuf_iterator<char>());
  for (char& ch : value) {
    if (ch == '\0') {
      ch = ' ';
    }
  }
  return Trim(value);
}

int64_t ReadProcRssMb(int pid) {
  std::ifstream in("/proc/" + std::to_string(pid) + "/status");
  if (!in.is_open()) {
    return 0;
  }

  std::string key;
  int64_t value_kb = 0;
  std::string unit;
  while (in >> key >> value_kb >> unit) {
    if (key == "VmRSS:") {
      return value_kb / 1024;
    }
  }
  return 0;
}

std::optional<ExternalDiscoverySample> ReadExternalDiscoverySample(int pid) {
  if (pid <= 0) {
    return std::nullopt;
  }

  std::ifstream in("/proc/" + std::to_string(pid) + "/stat");
  if (!in.is_open()) {
    return std::nullopt;
  }

  std::string line;
  std::getline(in, line);
  const auto open = line.find('(');
  const auto close = line.rfind(')');
  if (open == std::string::npos || close == std::string::npos ||
      close <= open + 1) {
    return std::nullopt;
  }

  const std::vector<std::string> fields_after_comm =
      SplitWhitespace(line.substr(close + 1));
  // fields_after_comm[0] is stat field 3 (state). starttime is field 22.
  if (fields_after_comm.size() <= 19) {
    return std::nullopt;
  }

  ExternalDiscoverySample out;
  out.pid = pid;
  out.comm = line.substr(open + 1, close - open - 1);
  out.cmdline = ReadProcCmdline(pid);
  out.rss_mb = ReadProcRssMb(pid);
  try {
    const uint64_t user_ticks = std::stoull(fields_after_comm[11]);
    const uint64_t system_ticks = std::stoull(fields_after_comm[12]);
    out.starttime_ticks = std::stoull(fields_after_comm[19]);
    out.cpu_ticks = user_ticks + system_ticks;
  } catch (...) {
    return std::nullopt;
  }
  return out;
}

bool ContainsBlockedProcessToken(const std::string& comm,
                                 const std::string& cmdline) {
  static const std::vector<std::string> kBlockedTokens = {
      "system_monitor_agent", "planner_agent",          "task_manager_agent",
      "task_executor_agent", "actuator_agent",          "metrics_collector_agent",
      "directory_agent",     "lease_authority_agent",   "nats-server",
      "bench_submitter",     "external_process_client",
  };
  for (const auto& token : kBlockedTokens) {
    if (cmdline.find(token) != std::string::npos || comm == token ||
        comm.rfind(token, 0) == 0 ||
        (!comm.empty() && token.rfind(comm, 0) == 0)) {
      return true;
    }
  }
  return false;
}

bool IsSafeExternalDiscoverySample(
    const ExternalDiscoverySample& sample,
    const std::unordered_set<int>& managed_task_pids,
    int self_pid) {
  if (sample.pid <= 1 || sample.pid == self_pid) {
    return false;
  }
  if (managed_task_pids.find(sample.pid) != managed_task_pids.end()) {
    return false;
  }
  // Kernel threads and many service helper processes have empty cmdline; keep
  // auto-discovery focused on explicit user-space workloads.
  if (sample.cmdline.empty()) {
    return false;
  }
  return !ContainsBlockedProcessToken(sample.comm, sample.cmdline);
}

std::vector<ExternalDiscoveryCandidate> DiscoverCpuActiveExternalProcesses(
    const ExternalDiscoveryConfig& config,
    const std::unordered_set<int>& managed_task_pids,
    std::unordered_map<std::string, ExternalDiscoveryState>* discovery_state,
    int64_t now_ms,
    int self_pid) {
  std::vector<ExternalDiscoveryCandidate> candidates;
  if (discovery_state == nullptr || !config.enabled ||
      config.max_processes <= 0) {
    return candidates;
  }

  try {
    for (const auto& entry : std::filesystem::directory_iterator(
             "/proc", std::filesystem::directory_options::skip_permission_denied)) {
      std::error_code entry_ec;
      if (!entry.is_directory(entry_ec) || entry_ec) {
        continue;
      }
      const std::string name = entry.path().filename().string();
      if (!IsPidDirectoryName(name)) {
        continue;
      }

      int pid = 0;
      try {
        pid = std::stoi(name);
      } catch (...) {
        continue;
      }

      const auto sample = ReadExternalDiscoverySample(pid);
      if (!sample.has_value() ||
          !IsSafeExternalDiscoverySample(*sample, managed_task_pids, self_pid)) {
        continue;
      }

      const std::string target_id =
          maigent::MakeExternalProcessTargetId(pid, sample->starttime_ticks);
      auto& state = (*discovery_state)[target_id];
      uint64_t delta_ticks = 0;
      if (state.initialized && sample->cpu_ticks >= state.prev_cpu_ticks) {
        delta_ticks = sample->cpu_ticks - state.prev_cpu_ticks;
      }
      state.initialized = true;
      state.prev_cpu_ticks = sample->cpu_ticks;
      state.last_seen_ms = now_ms;

      if (delta_ticks < config.min_cpu_delta_ticks) {
        continue;
      }
      state.last_relevant_ms = now_ms;

      ExternalDiscoveryCandidate candidate;
      candidate.pid = pid;
      candidate.target_id = target_id;
      candidate.label = ShortLabel(*sample);
      candidate.cgroup_path = maigent::ReadProcCgroupPath(pid);
      candidate.starttime_ticks = sample->starttime_ticks;
      candidate.delta_cpu_ticks = delta_ticks;
      candidate.rss_mb = sample->rss_mb;
      candidates.push_back(std::move(candidate));
    }
  } catch (...) {
    return candidates;
  }

  std::sort(candidates.begin(), candidates.end(),
            [](const ExternalDiscoveryCandidate& lhs,
               const ExternalDiscoveryCandidate& rhs) {
              if (lhs.delta_cpu_ticks != rhs.delta_cpu_ticks) {
                return lhs.delta_cpu_ticks > rhs.delta_cpu_ticks;
              }
              if (lhs.rss_mb != rhs.rss_mb) {
                return lhs.rss_mb > rhs.rss_mb;
              }
              return lhs.target_id < rhs.target_id;
            });
  if (candidates.size() > static_cast<size_t>(config.max_processes)) {
    candidates.resize(static_cast<size_t>(config.max_processes));
  }
  return candidates;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "system_monitor_agent";
  const std::string agent_id = "sysmon-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int interval_ms = maigent::GetFlagInt(argc, argv, "--interval-ms", 500);
  const std::string cgroup_root =
      maigent::GetFlagValue(argc, argv, "--cgroup-root", "/sys/fs/cgroup");
  ExternalDiscoveryConfig external_discovery_config;
  external_discovery_config.enabled =
      maigent::HasFlag(argc, argv, "--auto-discover-external-processes");
  external_discovery_config.interval_ms = std::max<int64_t>(
      1, maigent::GetFlagInt64(argc, argv, "--external-discovery-interval-ms",
                               2000));
  external_discovery_config.max_processes = std::max(
      0, maigent::GetFlagInt(argc, argv,
                             "--max-auto-discovered-external-processes", 32));
  external_discovery_config.idle_ttl_ms = std::max<int64_t>(
      0, maigent::GetFlagInt64(argc, argv, "--external-discovery-idle-ttl-ms",
                               30000));
  external_discovery_config.min_cpu_delta_ticks = static_cast<uint64_t>(
      std::max<int64_t>(0, maigent::GetFlagInt64(
                               argc, argv,
                               "--external-discovery-min-cpu-delta-ticks", 1)));

  maigent::AgentLogger log(agent_id, "logs/system_monitor.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  maigent::SystemMonitorRawCollector raw_collector(cgroup_root);
  maigent::HeuristicTargetClassifier target_classifier;
  maigent::SystemMonitorFeatureBuilder feature_builder(target_classifier);
  maigent::HeuristicSystemMonitorModel model;

  std::mutex mu;
  std::unordered_map<std::string, maigent::SystemMonitorManagedTaskRawRef>
      managed_tasks;
  std::unordered_map<std::string, RegisteredExternalProcess>
      external_processes;
  std::unordered_map<std::string, ExternalDiscoveryState>
      external_discovery_state;
  int64_t last_external_discovery_ms = 0;
  std::vector<maigent::SystemMonitorPressureHistorySample> pressure_history;
  pressure_history.reserve(64);

  auto on_task_event = [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()),
                                &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT) {
      return;
    }
    const auto kind = env.header().message_kind();
    if (kind != maigent::MK_TASK_STARTED && kind != maigent::MK_TASK_FINISHED &&
        kind != maigent::MK_TASK_FAILED) {
      return;
    }

    const auto& evt = env.event().task_event();
    std::lock_guard<std::mutex> lock(mu);
    if (evt.event_type() == maigent::TASK_STARTED) {
      maigent::SystemMonitorManagedTaskRawRef ref;
      ref.task_id = evt.task_id();
      ref.executor_id = evt.executor_id();
      ref.pid = evt.pid();
      ref.task_class = evt.task_class();
      ref.started_ms = evt.ts_ms();
      ref.priority = 0;
      managed_tasks[evt.task_id()] = ref;
    } else if (evt.event_type() == maigent::TASK_FINISHED ||
               evt.event_type() == maigent::TASK_FAILED) {
      managed_tasks.erase(evt.task_id());
    }
  };

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFinished, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFailed, on_task_event);

  auto respond_external_process_register =
      [&](const maigent::NatsMessage& incoming, const maigent::Envelope& request,
          bool success, const std::string& target_id, int pid,
          const std::string& reason) {
        maigent::Envelope reply;
        maigent::FillHeader(&reply, maigent::SERVICE,
                            maigent::MK_EXTERNAL_PROCESS_REGISTER_RESULT, role,
                            agent_id, request.header().request_id(),
                            request.header().trace_id(),
                            request.header().conversation_id());
        auto* result =
            reply.mutable_service()->mutable_external_process_register_result();
        result->set_success(success);
        result->set_target_id(target_id);
        result->set_pid(pid);
        result->set_reason(reason);
        result->set_ts_ms(maigent::NowMs());
        nats.RespondEnvelope(incoming, reply);
      };

  auto respond_external_process_unregister =
      [&](const maigent::NatsMessage& incoming, const maigent::Envelope& request,
          bool success, const std::string& target_id, int pid,
          const std::string& reason) {
        maigent::Envelope reply;
        maigent::FillHeader(&reply, maigent::SERVICE,
                            maigent::MK_EXTERNAL_PROCESS_UNREGISTER_RESULT, role,
                            agent_id, request.header().request_id(),
                            request.header().trace_id(),
                            request.header().conversation_id());
        auto* result =
            reply.mutable_service()->mutable_external_process_unregister_result();
        result->set_success(success);
        result->set_target_id(target_id);
        result->set_pid(pid);
        result->set_reason(reason);
        result->set_ts_ms(maigent::NowMs());
        nats.RespondEnvelope(incoming, reply);
      };

  nats.Subscribe(maigent::kSubjectCmdExternalProcessRegister,
                 [&](const maigent::NatsMessage& incoming) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(incoming.data.data(),
                                static_cast<int>(incoming.data.size()), &env) ||
        !env.has_command() || !env.command().has_external_process_register()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != maigent::MK_EXTERNAL_PROCESS_REGISTER) {
      return;
    }

    const auto& req = env.command().external_process_register();
    const int pid = req.pid();
    if (pid <= 0) {
      respond_external_process_register(incoming, env, false, "", pid,
                                        "pid must be positive");
      return;
    }

    uint64_t starttime_ticks = 0;
    if (!maigent::ReadProcessStarttimeTicks(pid, &starttime_ticks)) {
      respond_external_process_register(incoming, env, false, "", pid,
                                        "pid not found or unreadable");
      return;
    }

    RegisteredExternalProcess process;
    process.pid = pid;
    process.target_id =
        maigent::MakeExternalProcessTargetId(pid, starttime_ticks);
    process.label = !req.label().empty() ? req.label() : req.external_id();
    process.external_id = req.external_id();
    process.cgroup_path = req.cgroup_path();
    process.priority = req.priority();
    process.allow_control = req.allow_control();
    process.proc_starttime_ticks = starttime_ticks;
    process.registered_at_ms = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      external_processes[process.target_id] = process;
    }

    log.Info("external process registered pid=" + std::to_string(pid) +
             " target_id=" + process.target_id +
             " allow_control=" + std::to_string(process.allow_control));
    respond_external_process_register(incoming, env, true, process.target_id,
                                      pid, "registered");
  });

  nats.Subscribe(maigent::kSubjectCmdExternalProcessUnregister,
                 [&](const maigent::NatsMessage& incoming) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(incoming.data.data(),
                                static_cast<int>(incoming.data.size()), &env) ||
        !env.has_command() || !env.command().has_external_process_unregister()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != maigent::MK_EXTERNAL_PROCESS_UNREGISTER) {
      return;
    }

    const auto& req = env.command().external_process_unregister();
    std::string removed_target_id;
    int removed_pid = req.pid();
    bool removed = false;
    {
      std::lock_guard<std::mutex> lock(mu);
      if (!req.target_id().empty()) {
        auto it = external_processes.find(req.target_id());
        if (it != external_processes.end()) {
          removed_target_id = it->second.target_id;
          removed_pid = it->second.pid;
          external_processes.erase(it);
          removed = true;
        }
      } else if (req.pid() > 0) {
        for (auto it = external_processes.begin();
             it != external_processes.end();) {
          if (it->second.pid == req.pid()) {
            if (removed_target_id.empty()) {
              removed_target_id = it->second.target_id;
            }
            removed_pid = it->second.pid;
            it = external_processes.erase(it);
            removed = true;
          } else {
            ++it;
          }
        }
      }
    }

    if (removed) {
      log.Info("external process unregistered pid=" +
               std::to_string(removed_pid) +
               " target_id=" + removed_target_id);
      respond_external_process_unregister(incoming, env, true,
                                          removed_target_id, removed_pid,
                                          "unregistered");
    } else {
      respond_external_process_unregister(incoming, env, false,
                                          req.target_id(), req.pid(),
                                          "registered process not found");
    }
  });

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"state.pressure", "state.forecast", "state.capacity", "state.targets",
       "external_process.register"},
      []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    const int64_t now_ms = maigent::NowMs();
    if (external_discovery_config.enabled &&
        now_ms - last_external_discovery_ms >=
            external_discovery_config.interval_ms) {
      last_external_discovery_ms = now_ms;

      std::unordered_set<int> managed_task_pids;
      {
        std::lock_guard<std::mutex> lock(mu);
        managed_task_pids.reserve(managed_tasks.size());
        for (const auto& [task_id, ref] : managed_tasks) {
          (void)task_id;
          if (ref.pid > 0) {
            managed_task_pids.insert(ref.pid);
          }
        }
      }

      const auto candidates = DiscoverCpuActiveExternalProcesses(
          external_discovery_config, managed_task_pids,
          &external_discovery_state, now_ms, getpid());

      std::vector<std::string> discovery_logs;
      {
        std::lock_guard<std::mutex> lock(mu);
        for (const auto& candidate : candidates) {
          auto it = external_processes.find(candidate.target_id);
          if (it == external_processes.end()) {
            RegisteredExternalProcess process;
            process.pid = candidate.pid;
            process.target_id = candidate.target_id;
            process.label = candidate.label;
            process.cgroup_path = candidate.cgroup_path;
            process.priority = 0;
            process.allow_control = true;
            process.proc_starttime_ticks = candidate.starttime_ticks;
            process.registered_at_ms = now_ms;
            process.auto_discovered = true;
            external_processes[process.target_id] = process;

            discovery_logs.push_back(
                "auto-discovered external process pid=" +
                std::to_string(process.pid) + " target_id=" +
                process.target_id + " label=" + process.label +
                " delta_ticks=" + std::to_string(candidate.delta_cpu_ticks));
          } else if (it->second.auto_discovered) {
            it->second.label = candidate.label;
            it->second.cgroup_path = candidate.cgroup_path;
            it->second.allow_control = true;
          }
        }

        for (auto it = external_processes.begin();
             it != external_processes.end();) {
          if (!it->second.auto_discovered) {
            ++it;
            continue;
          }
          const auto state_it = external_discovery_state.find(it->second.target_id);
          const bool idle =
              state_it == external_discovery_state.end() ||
              state_it->second.last_relevant_ms <= 0 ||
              now_ms - state_it->second.last_relevant_ms >
                  external_discovery_config.idle_ttl_ms;
          if (idle) {
            discovery_logs.push_back(
                "auto-discovered external process removed: idle ttl pid=" +
                std::to_string(it->second.pid) + " target_id=" +
                it->second.target_id);
            it = external_processes.erase(it);
          } else {
            ++it;
          }
        }

        std::vector<std::pair<std::string, int64_t>> auto_entries;
        for (const auto& [target_id, process] : external_processes) {
          if (!process.auto_discovered) {
            continue;
          }
          const auto state_it = external_discovery_state.find(target_id);
          const int64_t last_relevant_ms =
              state_it == external_discovery_state.end()
                  ? 0
                  : state_it->second.last_relevant_ms;
          auto_entries.emplace_back(target_id, last_relevant_ms);
        }
        if (auto_entries.size() >
            static_cast<size_t>(external_discovery_config.max_processes)) {
          std::sort(auto_entries.begin(), auto_entries.end(),
                    [](const auto& lhs, const auto& rhs) {
                      if (lhs.second != rhs.second) {
                        return lhs.second < rhs.second;
                      }
                      return lhs.first < rhs.first;
                    });
          const size_t remove_count =
              auto_entries.size() -
              static_cast<size_t>(external_discovery_config.max_processes);
          for (size_t i = 0; i < remove_count; ++i) {
            auto it = external_processes.find(auto_entries[i].first);
            if (it == external_processes.end() || !it->second.auto_discovered) {
              continue;
            }
            discovery_logs.push_back(
                "auto-discovered external process removed: capacity limit pid=" +
                std::to_string(it->second.pid) + " target_id=" +
                it->second.target_id);
            external_processes.erase(it);
          }
        }
      }

      for (const auto& line : discovery_logs) {
        log.Info(line);
      }

      const int64_t discovery_state_ttl_ms =
          std::max<int64_t>(60000, external_discovery_config.idle_ttl_ms * 2);
      for (auto it = external_discovery_state.begin();
           it != external_discovery_state.end();) {
        if (it->second.last_seen_ms > 0 &&
            now_ms - it->second.last_seen_ms > discovery_state_ttl_ms) {
          it = external_discovery_state.erase(it);
        } else {
          ++it;
        }
      }
    }

    std::vector<maigent::SystemMonitorManagedTaskRawRef> managed_tasks_snapshot;
    std::vector<maigent::SystemMonitorExternalProcessRawRef>
        external_processes_snapshot;
    std::vector<maigent::SystemMonitorPressureHistorySample>
        pressure_history_snapshot;
    {
      std::lock_guard<std::mutex> lock(mu);
      managed_tasks_snapshot.reserve(managed_tasks.size());
      for (const auto& [task_id, ref] : managed_tasks) {
        (void)task_id;
        managed_tasks_snapshot.push_back(ref);
      }
      external_processes_snapshot.reserve(external_processes.size());
      for (const auto& [target_id, process] : external_processes) {
        (void)target_id;
        maigent::SystemMonitorExternalProcessRawRef ref;
        ref.pid = process.pid;
        ref.target_id = process.target_id;
        ref.label = process.label;
        ref.cgroup_path = process.cgroup_path;
        ref.priority = process.priority;
        ref.allow_control = process.allow_control;
        ref.expected_starttime_ticks = process.proc_starttime_ticks;
        ref.registered_at_ms = process.registered_at_ms;
        external_processes_snapshot.push_back(std::move(ref));
      }
      pressure_history_snapshot = pressure_history;
    }

    maigent::SystemMonitorRawSnapshot raw_snapshot;
    if (!raw_collector.CollectSnapshot(managed_tasks_snapshot,
                                       external_processes_snapshot,
                                       &raw_snapshot)) {
      log.Warn("failed to collect full raw snapshot");
    }
    if (!raw_snapshot.external_process_removals.empty()) {
      std::lock_guard<std::mutex> lock(mu);
      for (const auto& removal : raw_snapshot.external_process_removals) {
        const auto erased = external_processes.erase(removal.target_id);
        if (erased > 0) {
          log.Info("external process target removed: " + removal.reason +
                   " pid=" + std::to_string(removal.pid) +
                   " target_id=" + removal.target_id);
        }
      }
    }

    const maigent::SystemMonitorModelInput model_input =
        feature_builder.BuildModelInput(raw_snapshot, pressure_history_snapshot);
    const maigent::SystemMonitorModelOutput model_output =
        model.Evaluate(model_input);

    {
      std::lock_guard<std::mutex> lock(mu);
      pressure_history.push_back(
          maigent::ToPressureHistorySampleFromOutput(model_output.pressure));
      if (pressure_history.size() > 128) {
        pressure_history.erase(pressure_history.begin());
      }
    }

    const maigent::PressureState pressure =
        maigent::ToProtoPressureState(model_output.pressure);
    const maigent::ForecastState forecast =
        maigent::ToProtoForecastState(model_output.forecast);
    const maigent::CapacityState capacity =
        maigent::ToProtoCapacityState(model_output.capacity);
    const maigent::TargetsState targets =
        maigent::ToProtoTargetsState(model_output);

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_PRESSURE_STATE, role,
                          agent_id);
      *env.mutable_state()->mutable_pressure_state() = pressure;
      nats.PublishEnvelope(maigent::kSubjectStatePressure, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_FORECAST_STATE, role,
                          agent_id);
      *env.mutable_state()->mutable_forecast_state() = forecast;
      nats.PublishEnvelope(maigent::kSubjectStateForecast, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_CAPACITY_STATE, role,
                          agent_id);
      *env.mutable_state()->mutable_capacity_state() = capacity;
      nats.PublishEnvelope(maigent::kSubjectStateCapacity, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_TARGETS_STATE, role,
                          agent_id);
      *env.mutable_state()->mutable_targets_state() = targets;
      nats.PublishEnvelope(maigent::kSubjectStateTargets, env);
    }

    log.Debug("published state pressure_risk=" +
              std::to_string(pressure.risk_level()) +
              " targets=" + std::to_string(targets.targets_size()));

    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
