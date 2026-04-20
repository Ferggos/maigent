#include <atomic>
#include <chrono>
#include <csignal>
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
#include "maigent/system_monitor/system_monitor_model.h"
#include "feature_builder.h"
#include "raw_collector.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

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
  maigent::SystemMonitorFeatureBuilder feature_builder;
  maigent::HeuristicSystemMonitorModel model;

  std::mutex mu;
  std::unordered_map<std::string, maigent::SystemMonitorManagedTaskRawRef>
      managed_tasks;
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

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"state.pressure", "state.forecast", "state.capacity", "state.targets"},
      []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    std::vector<maigent::SystemMonitorManagedTaskRawRef> managed_tasks_snapshot;
    std::vector<maigent::SystemMonitorPressureHistorySample>
        pressure_history_snapshot;
    {
      std::lock_guard<std::mutex> lock(mu);
      managed_tasks_snapshot.reserve(managed_tasks.size());
      for (const auto& [task_id, ref] : managed_tasks) {
        (void)task_id;
        managed_tasks_snapshot.push_back(ref);
      }
      pressure_history_snapshot = pressure_history;
    }

    maigent::SystemMonitorRawSnapshot raw_snapshot;
    if (!raw_collector.CollectSnapshot(managed_tasks_snapshot, &raw_snapshot)) {
      log.Warn("failed to collect full raw snapshot");
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
