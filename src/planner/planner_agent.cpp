#include <atomic>
#include <chrono>
#include <csignal>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "maigent.pb.h"
#include "maigent/common/agent_lifecycle.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/logging.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"
#include "maigent/common/time_utils.h"
#include "maigent/planner/planner_model.h"
#include "maigent/planner/runtime_command_mapper.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct ActiveTask {
  std::string task_id;
  std::string executor_id;
  int pid = 0;
  int64_t started_ms = 0;
};

struct PlannerState {
  maigent::PressureState pressure;
  maigent::ForecastState forecast;
  maigent::CapacityState capacity;
  maigent::TargetsState targets;
  std::unordered_map<std::string, ActiveTask> active_tasks;
  int64_t last_action_ms = 0;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "planner_agent";
  const std::string agent_id = "planner-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int max_actions = maigent::GetFlagInt(argc, argv, "--max-actions", 2);
  const int64_t cooldown_ms =
      maigent::GetFlagInt64(argc, argv, "--cooldown-ms", 1000);

  maigent::AgentLogger log(agent_id, "logs/planner.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  maigent::HeuristicPlannerModel planner_model(max_actions);

  std::mutex mu;
  PlannerState st;

  auto on_task_event = [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
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
    const auto& e = env.event().task_event();
    std::lock_guard<std::mutex> lock(mu);

    if (e.event_type() == maigent::TASK_STARTED) {
      ActiveTask t;
      t.task_id = e.task_id();
      t.executor_id = e.executor_id();
      t.pid = e.pid();
      t.started_ms = e.ts_ms();
      st.active_tasks[e.task_id()] = t;
    } else if (e.event_type() == maigent::TASK_FINISHED ||
               e.event_type() == maigent::TASK_FAILED) {
      st.active_tasks.erase(e.task_id());
    }
  };

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFinished, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFailed, on_task_event);

  nats.Subscribe(maigent::kSubjectStatePressure, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_state() || !env.state().has_pressure_state()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::STATE ||
        env.header().message_kind() != maigent::MK_PRESSURE_STATE) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    st.pressure = env.state().pressure_state();
  });

  nats.Subscribe(maigent::kSubjectStateForecast, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_state() || !env.state().has_forecast_state()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::STATE ||
        env.header().message_kind() != maigent::MK_FORECAST_STATE) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    st.forecast = env.state().forecast_state();
  });

  nats.Subscribe(maigent::kSubjectStateCapacity, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_state() || !env.state().has_capacity_state()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::STATE ||
        env.header().message_kind() != maigent::MK_CAPACITY_STATE) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    st.capacity = env.state().capacity_state();
  });

  nats.Subscribe(maigent::kSubjectStateTargets, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_state() || !env.state().has_targets_state()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::STATE ||
        env.header().message_kind() != maigent::MK_TARGETS_STATE) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    st.targets = env.state().targets_state();
  });

  auto dispatch_action = [&](const maigent::ControlAction& action,
                             const std::string& trace_id) {
    maigent::Envelope env;
    maigent::FillHeader(&env, maigent::COMMAND,
                        (action.target_type() == maigent::TARGET_TASK)
                            ? maigent::MK_TASK_CONTROL
                            : maigent::MK_ACTUATOR_APPLY,
                        role, agent_id,
                        maigent::MakeRequestId("planner-action"), trace_id,
                        action.task_id().empty() ? action.target_id()
                                                 : action.task_id());
    *env.mutable_command()->mutable_control_action() = action;

    if (action.target_type() == maigent::TARGET_TASK &&
        !action.executor_id().empty()) {
      nats.PublishEnvelope(maigent::TaskExecControlSubject(action.executor_id()), env);
      log.Info("sent task control action=" + std::to_string(action.action_type()) +
                   " task_id=" + action.task_id() +
                   " executor_id=" + action.executor_id());
    } else {
      nats.PublishEnvelope(maigent::kSubjectCmdActuatorApply, env);
      log.Info("sent actuator action=" + std::to_string(action.action_type()) +
                   " target_id=" + action.target_id());
    }
  };

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"policy.global", "planner.dispatch"},
                                    [&]() {
                                      std::lock_guard<std::mutex> lock(mu);
                                      return static_cast<int>(st.active_tasks.size());
                                    });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    maigent::PressureState pressure;
    maigent::ForecastState forecast;
    maigent::CapacityState capacity;
    maigent::TargetsState targets;
    int active_tasks_count = 0;
    int64_t last_action_ms = 0;

    {
      std::lock_guard<std::mutex> lock(mu);
      pressure = st.pressure;
      forecast = st.forecast;
      capacity = st.capacity;
      targets = st.targets;
      active_tasks_count = static_cast<int>(st.active_tasks.size());
      last_action_ms = st.last_action_ms;
    }

    const int64_t now = maigent::NowMs();
    if (pressure.ts_ms() > 0 && now - last_action_ms >= cooldown_ms) {
      const maigent::PlannerModelInput model_input =
          maigent::ToPlannerModelInput(pressure, forecast, capacity, targets,
                                       active_tasks_count);
      const maigent::PlannerModelOutput model_output =
          planner_model.Evaluate(model_input);
      const auto runtime_actions =
          maigent::ToRuntimeControlActions(model_output, model_input.targets);
      if (runtime_actions.size() < model_output.interventions.size()) {
        log.Warn("dropped interventions due unsupported target capability or "
                 "missing dispatch metadata dropped=" +
                 std::to_string(model_output.interventions.size() -
                                runtime_actions.size()));
      }
      if (!runtime_actions.empty()) {
        for (const auto& action : runtime_actions) {
          dispatch_action(action, maigent::MakeTraceId());
        }
        std::lock_guard<std::mutex> lock(mu);
        st.last_action_ms = now;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
