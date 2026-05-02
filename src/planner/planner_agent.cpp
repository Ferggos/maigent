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

std::string ActionFailureKey(const std::string& target_id,
                             maigent::ControlActionType action_type) {
  return target_id + "#" + std::to_string(static_cast<int>(action_type));
}

bool IsHardControlAction(maigent::ControlActionType action_type) {
  switch (action_type) {
    case maigent::FREEZE:
    case maigent::KILL:
    case maigent::SET_MEM_MAX:
    case maigent::SET_CPU_MAX:
      return true;
    case maigent::CONTROL_ACTION_UNSPECIFIED:
    case maigent::RENICE:
    case maigent::SET_CPU_WEIGHT:
    case maigent::SET_MEM_HIGH:
    case maigent::THAW:
    default:
      return false;
  }
}

bool SupportsTargetAction(const maigent::UnifiedTarget& target,
                          maigent::TargetAction action) {
  return std::find(target.allowed_actions.begin(), target.allowed_actions.end(),
                   action) != target.allowed_actions.end();
}

bool IsManagedTarget(const maigent::UnifiedTarget& target) {
  return target.source == maigent::TargetSource::kManagedTask;
}

bool IsManagedTaskRouteAction(const maigent::ControlAction& action) {
  return action.target_type() == maigent::TARGET_TASK &&
         !action.task_id().empty() && !action.executor_id().empty();
}

bool IsManagedThawCandidate(const maigent::UnifiedTarget& target) {
  return IsManagedTarget(target) && !target.target_id.empty() &&
         !target.task_id.empty() && !target.owner_executor_id.empty() &&
         SupportsTargetAction(target, maigent::TargetAction::kThaw);
}

bool BuildManagedThawAction(const maigent::UnifiedTarget& target, int64_t now,
                            maigent::ControlAction* out) {
  if (out == nullptr || !IsManagedThawCandidate(target)) {
    return false;
  }

  out->Clear();
  out->set_target_type(maigent::TARGET_TASK);
  out->set_target_id(target.target_id);
  out->set_task_id(target.task_id);
  out->set_executor_id(target.owner_executor_id);
  out->set_pid(target.pid);
  out->set_action_type(maigent::THAW);
  out->set_reason("planner recovery thaw after stabilized pressure");
  out->set_policy_id("planner_recovery_v1.thaw");
  out->set_ts_ms(now);
  return true;
}

struct FrozenManagedTarget {
  int64_t frozen_since_ms = 0;
  int64_t last_seen_ms = 0;
  int64_t thaw_eligible_after_ms = 0;
  int64_t thaw_pending_until_ms = 0;
};

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
  std::unordered_map<std::string, int64_t> failed_action_backoff_until_ms;
  std::unordered_map<std::string, int64_t> hard_action_cooldown_until_ms;
  std::unordered_map<std::string, FrozenManagedTarget> frozen_managed_targets;
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
  const int sustained_high_cycles =
      std::max(1, maigent::GetFlagInt(argc, argv, "--sustained-high-cycles", 3));
  const int64_t action_failure_backoff_ms =
      std::max<int64_t>(0, maigent::GetFlagInt64(argc, argv, "--action-failure-backoff-ms",
                                                 15000));
  const int64_t hard_action_cooldown_ms =
      std::max<int64_t>(0, maigent::GetFlagInt64(argc, argv, "--hard-action-cooldown-ms",
                                                 15000));
  const int64_t min_freeze_duration_ms =
      std::max<int64_t>(0, maigent::GetFlagInt64(argc, argv, "--min-freeze-duration-ms",
                                                 15000));
  const int thaw_stable_cycles =
      std::max(1, maigent::GetFlagInt(argc, argv, "--thaw-stable-cycles", 2));
  const int64_t thaw_retry_backoff_ms =
      std::max<int64_t>(0, maigent::GetFlagInt64(argc, argv, "--thaw-retry-backoff-ms",
                                                 8000));

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

  auto on_control_result = [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()),
                                &env) ||
        !env.has_event() || !env.event().has_actuator_result()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT) {
      return;
    }

    const auto kind = env.header().message_kind();
    const bool is_failure = kind == maigent::MK_ACTUATOR_FAILED ||
                            kind == maigent::MK_CONTROL_FAILED;
    const bool is_success = kind == maigent::MK_ACTUATOR_APPLIED ||
                            kind == maigent::MK_CONTROL_APPLIED;
    if (!is_failure && !is_success) {
      return;
    }

    const auto& result = env.event().actuator_result();
    if (result.target_id().empty() ||
        result.action_type() == maigent::CONTROL_ACTION_UNSPECIFIED) {
      return;
    }

    const int64_t event_ts_ms =
        result.ts_ms() > 0 ? result.ts_ms() : maigent::NowMs();
    const std::string key =
        ActionFailureKey(result.target_id(), result.action_type());
    std::lock_guard<std::mutex> lock(mu);
    if (is_failure) {
      const int64_t until_ms = event_ts_ms + action_failure_backoff_ms;
      auto& slot = st.failed_action_backoff_until_ms[key];
      slot = std::max(slot, until_ms);
      if (result.action_type() == maigent::FREEZE) {
        st.frozen_managed_targets.erase(result.target_id());
      } else if (result.action_type() == maigent::THAW) {
        auto it = st.frozen_managed_targets.find(result.target_id());
        if (it != st.frozen_managed_targets.end()) {
          it->second.thaw_pending_until_ms = event_ts_ms + thaw_retry_backoff_ms;
        }
      }
    } else {
      st.failed_action_backoff_until_ms.erase(key);
      if (result.action_type() == maigent::THAW) {
        st.frozen_managed_targets.erase(result.target_id());
      }
    }
  };

  nats.Subscribe(maigent::kSubjectEvtActuatorApplied, on_control_result);
  nats.Subscribe(maigent::kSubjectEvtActuatorFailed, on_control_result);
  nats.Subscribe(maigent::kSubjectEvtControlApplied, on_control_result);
  nats.Subscribe(maigent::kSubjectEvtControlFailed, on_control_result);

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

  int64_t last_pressure_ts_ms = 0;
  int consecutive_high_pressure = 0;
  int consecutive_non_high_recovery = 0;

  while (!g_stop.load()) {
    maigent::PressureState pressure;
    maigent::ForecastState forecast;
    maigent::CapacityState capacity;
    maigent::TargetsState targets;
    int active_tasks_count = 0;
    int64_t last_action_ms = 0;
    size_t frozen_managed_targets_count = 0;

    {
      std::lock_guard<std::mutex> lock(mu);
      pressure = st.pressure;
      forecast = st.forecast;
      capacity = st.capacity;
      targets = st.targets;
      active_tasks_count = static_cast<int>(st.active_tasks.size());
      last_action_ms = st.last_action_ms;
      frozen_managed_targets_count = st.frozen_managed_targets.size();
    }

    const int64_t now = maigent::NowMs();
    if (pressure.ts_ms() > 0 && pressure.ts_ms() != last_pressure_ts_ms) {
      last_pressure_ts_ms = pressure.ts_ms();
      if (pressure.risk_level() == maigent::RISK_HIGH) {
        ++consecutive_high_pressure;
      } else {
        consecutive_high_pressure = 0;
      }
      if (pressure.risk_level() != maigent::RISK_HIGH &&
          forecast.risk_level() != maigent::RISK_HIGH) {
        ++consecutive_non_high_recovery;
      } else {
        consecutive_non_high_recovery = 0;
      }
    }
    const bool sustained_high =
        consecutive_high_pressure >= sustained_high_cycles;
    const bool should_attempt_intervention =
        pressure.risk_level() == maigent::RISK_HIGH &&
        (sustained_high || forecast.risk_level() == maigent::RISK_HIGH);
    const bool stable_recovery =
        pressure.risk_level() != maigent::RISK_HIGH &&
        forecast.risk_level() != maigent::RISK_HIGH &&
        consecutive_non_high_recovery >= thaw_stable_cycles;

    maigent::PlannerModelInput model_input;
    bool model_input_ready = false;
    auto ensure_model_input = [&]() {
      if (!model_input_ready) {
        model_input = maigent::ToPlannerModelInput(pressure, forecast, capacity,
                                                   targets, active_tasks_count);
        model_input_ready = true;
      }
    };

    bool thaw_dispatched = false;
    if (frozen_managed_targets_count > 0) {
      ensure_model_input();

      std::unordered_map<std::string, const maigent::UnifiedTarget*> managed_targets;
      managed_targets.reserve(model_input.targets.size());
      for (const auto& target : model_input.targets) {
        if (IsManagedTarget(target)) {
          managed_targets[target.target_id] = &target;
        }
      }

      std::string thaw_target_id;
      {
        std::lock_guard<std::mutex> lock(mu);
        for (auto it = st.failed_action_backoff_until_ms.begin();
             it != st.failed_action_backoff_until_ms.end();) {
          if (it->second <= now) {
            it = st.failed_action_backoff_until_ms.erase(it);
          } else {
            ++it;
          }
        }
        for (auto it = st.hard_action_cooldown_until_ms.begin();
             it != st.hard_action_cooldown_until_ms.end();) {
          if (it->second <= now) {
            it = st.hard_action_cooldown_until_ms.erase(it);
          } else {
            ++it;
          }
        }

        for (auto it = st.frozen_managed_targets.begin();
             it != st.frozen_managed_targets.end();) {
          const auto target_it = managed_targets.find(it->first);
          if (target_it == managed_targets.end() ||
              !IsManagedThawCandidate(*target_it->second)) {
            it = st.frozen_managed_targets.erase(it);
            continue;
          }
          it->second.last_seen_ms = now;
          ++it;
        }

        if (stable_recovery) {
          for (const auto& [target_id, frozen_state] :
               st.frozen_managed_targets) {
            if (now < frozen_state.thaw_eligible_after_ms ||
                now - frozen_state.frozen_since_ms < min_freeze_duration_ms) {
              continue;
            }
            if (frozen_state.thaw_pending_until_ms > now) {
              continue;
            }

            const auto target_it = managed_targets.find(target_id);
            if (target_it == managed_targets.end()) {
              continue;
            }
            const std::string thaw_key = ActionFailureKey(target_id, maigent::THAW);
            const auto backoff_it =
                st.failed_action_backoff_until_ms.find(thaw_key);
            if (backoff_it != st.failed_action_backoff_until_ms.end() &&
                backoff_it->second > now) {
              continue;
            }

            if (thaw_target_id.empty()) {
              thaw_target_id = target_id;
              continue;
            }
            const auto prev_it = st.frozen_managed_targets.find(thaw_target_id);
            if (prev_it == st.frozen_managed_targets.end() ||
                frozen_state.frozen_since_ms < prev_it->second.frozen_since_ms ||
                (frozen_state.frozen_since_ms ==
                     prev_it->second.frozen_since_ms &&
                 target_id < thaw_target_id)) {
              thaw_target_id = target_id;
            }
          }
        }
      }

      if (!thaw_target_id.empty()) {
        const auto target_it = managed_targets.find(thaw_target_id);
        maigent::ControlAction thaw_action;
        if (target_it != managed_targets.end() &&
            BuildManagedThawAction(*target_it->second, now, &thaw_action)) {
          dispatch_action(thaw_action, maigent::MakeTraceId());
          {
            std::lock_guard<std::mutex> lock(mu);
            auto frozen_it = st.frozen_managed_targets.find(thaw_target_id);
            if (frozen_it != st.frozen_managed_targets.end()) {
              frozen_it->second.thaw_pending_until_ms =
                  now + thaw_retry_backoff_ms;
            }
            const std::string freeze_key =
                ActionFailureKey(thaw_target_id, maigent::FREEZE);
            auto& slot = st.hard_action_cooldown_until_ms[freeze_key];
            slot = std::max(slot, now + hard_action_cooldown_ms);
            st.last_action_ms = now;
          }
          thaw_dispatched = true;
        }
      }
    }

    if (!thaw_dispatched && should_attempt_intervention &&
        pressure.ts_ms() > last_action_ms &&
        now - last_action_ms >= cooldown_ms) {
      ensure_model_input();
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
      std::vector<maigent::ControlAction> dispatchable_actions;
      dispatchable_actions.reserve(runtime_actions.size());
      int dropped_due_backoff = 0;
      int dropped_due_hard_cooldown = 0;
      int dropped_due_hard_cycle_limit = 0;
      int dropped_due_already_frozen = 0;
      bool hard_action_selected = false;
      {
        std::lock_guard<std::mutex> lock(mu);
        for (auto it = st.failed_action_backoff_until_ms.begin();
             it != st.failed_action_backoff_until_ms.end();) {
          if (it->second <= now) {
            it = st.failed_action_backoff_until_ms.erase(it);
          } else {
            ++it;
          }
        }
        for (auto it = st.hard_action_cooldown_until_ms.begin();
             it != st.hard_action_cooldown_until_ms.end();) {
          if (it->second <= now) {
            it = st.hard_action_cooldown_until_ms.erase(it);
          } else {
            ++it;
          }
        }

        for (const auto& action : runtime_actions) {
          const std::string key =
              ActionFailureKey(action.target_id(), action.action_type());
          const auto it = st.failed_action_backoff_until_ms.find(key);
          if (it != st.failed_action_backoff_until_ms.end() && it->second > now) {
            ++dropped_due_backoff;
            continue;
          }
          if (action.action_type() == maigent::FREEZE &&
              IsManagedTaskRouteAction(action) &&
              st.frozen_managed_targets.find(action.target_id()) !=
                  st.frozen_managed_targets.end()) {
            ++dropped_due_already_frozen;
            continue;
          }
          if (IsHardControlAction(action.action_type())) {
            const auto hard_it = st.hard_action_cooldown_until_ms.find(key);
            if (hard_it != st.hard_action_cooldown_until_ms.end() &&
                hard_it->second > now) {
              ++dropped_due_hard_cooldown;
              continue;
            }
            if (hard_action_selected) {
              ++dropped_due_hard_cycle_limit;
              continue;
            }
            hard_action_selected = true;
          }
          dispatchable_actions.push_back(action);
        }
      }

      if (dropped_due_backoff > 0) {
        log.Warn("suppressed repeated failing actions by backoff dropped=" +
                 std::to_string(dropped_due_backoff));
      }
      if (dropped_due_hard_cooldown > 0) {
        log.Warn("suppressed repeated hard actions by cooldown dropped=" +
                 std::to_string(dropped_due_hard_cooldown));
      }
      if (dropped_due_hard_cycle_limit > 0) {
        log.Warn("suppressed extra hard actions in single cycle dropped=" +
                 std::to_string(dropped_due_hard_cycle_limit));
      }
      if (dropped_due_already_frozen > 0) {
        log.Debug("suppressed FREEZE for already frozen managed targets dropped=" +
                  std::to_string(dropped_due_already_frozen));
      }

      if (!dispatchable_actions.empty()) {
        std::vector<std::string> dispatched_hard_keys;
        std::vector<std::string> dispatched_managed_freeze_targets;
        std::vector<std::string> dispatched_managed_thaw_targets;
        dispatched_hard_keys.reserve(dispatchable_actions.size());
        dispatched_managed_freeze_targets.reserve(dispatchable_actions.size());
        dispatched_managed_thaw_targets.reserve(dispatchable_actions.size());
        for (const auto& action : dispatchable_actions) {
          if (IsHardControlAction(action.action_type())) {
            dispatched_hard_keys.push_back(
                ActionFailureKey(action.target_id(), action.action_type()));
          }
          if (action.action_type() == maigent::FREEZE &&
              IsManagedTaskRouteAction(action)) {
            dispatched_managed_freeze_targets.push_back(action.target_id());
          }
          if (action.action_type() == maigent::THAW &&
              IsManagedTaskRouteAction(action)) {
            dispatched_managed_thaw_targets.push_back(action.target_id());
          }
          dispatch_action(action, maigent::MakeTraceId());
        }
        std::lock_guard<std::mutex> lock(mu);
        const int64_t hard_until = now + hard_action_cooldown_ms;
        for (const auto& key : dispatched_hard_keys) {
          auto& slot = st.hard_action_cooldown_until_ms[key];
          slot = std::max(slot, hard_until);
        }
        for (const auto& target_id : dispatched_managed_freeze_targets) {
          auto& frozen = st.frozen_managed_targets[target_id];
          frozen.frozen_since_ms = now;
          frozen.last_seen_ms = now;
          frozen.thaw_eligible_after_ms = now + min_freeze_duration_ms;
        }
        for (const auto& target_id : dispatched_managed_thaw_targets) {
          auto frozen_it = st.frozen_managed_targets.find(target_id);
          if (frozen_it != st.frozen_managed_targets.end()) {
            frozen_it->second.thaw_pending_until_ms =
                now + thaw_retry_backoff_ms;
          }
          const std::string freeze_key =
              ActionFailureKey(target_id, maigent::FREEZE);
          auto& slot = st.hard_action_cooldown_until_ms[freeze_key];
          slot = std::max(slot, hard_until);
        }
        st.last_action_ms = now;
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
