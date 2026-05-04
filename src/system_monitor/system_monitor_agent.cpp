#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <mutex>
#include <string>
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
#include "maigent/system_monitor/system_monitor_model.h"
#include "maigent/system_monitor/target_classifier.h"
#include "feature_builder.h"
#include "raw_collector.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

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
};

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
