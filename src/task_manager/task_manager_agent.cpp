#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cctype>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

enum class AdmissionMode {
  DENY = 0,
  QUEUE = 1,
};

enum class QueuePriority {
  HIGH = 0,
  NORMAL = 1,
  BEST_EFFORT = 2,
};

enum class TaskFsmState {
  NEW = 0,
  SUBMITTED = 1,
  RESERVING = 2,
  RESERVE_DENIED_TEMPORARY = 3,
  RESERVE_DENIED = 4,
  QUEUED = 5,
  WAITING_FOR_RESOURCES = 6,
  READY_TO_RETRY_RESERVE = 7,
  RESERVE_GRANTED = 8,
  COMMITTING = 9,
  COMMITTED = 10,
  LAUNCH_SENT = 11,
  STARTED = 12,
  FINISHED = 13,
  FAILED = 14,
  TIMEOUT = 15,
  CANCELLED = 16,
};

struct PendingWaiter {
  maigent::NatsMessage incoming;
  std::string submit_request_id;
  std::string submit_trace_id;
  std::string submit_conversation_id;
  maigent::WaitMode wait_mode = maigent::WAIT_MODE_UNSPECIFIED;
  int64_t deadline_ts_ms = 0;
};

struct TaskContext {
  std::string task_id;
  std::string trace_id;
  std::vector<std::string> submit_request_ids;

  std::string bench_id;
  std::string task_class;
  int priority = 0;
  std::string cmd;
  std::vector<std::string> args;
  maigent::ResourceRequest resources;

  std::string lease_id;
  std::string executor_id;
  int pid = 0;
  int exit_code = 0;

  TaskFsmState current_state = TaskFsmState::NEW;

  int64_t submitted_ts_ms = 0;
  int64_t reserve_requested_ts_ms = 0;
  int64_t reserve_decision_ts_ms = 0;
  int64_t reserve_granted_ts_ms = 0;
  int64_t commit_requested_ts_ms = 0;
  int64_t commit_ack_ts_ms = 0;
  int64_t launch_sent_ts_ms = 0;
  int64_t started_ts_ms = 0;
  int64_t finished_ts_ms = 0;
  int64_t completed_ts_ms = 0;

  int64_t queue_enter_ts_ms = 0;
  int64_t queue_deadline_ts_ms = 0;
  QueuePriority queue_priority = QueuePriority::NORMAL;
  int queue_attempts = 0;
  std::string last_reserve_failure_reason;
  std::string queued_reason;
  bool queued = false;
  bool reserve_in_flight = false;
  bool was_queued = false;

  maigent::SubmitStatus status_to_return = maigent::SUBMIT_STATUS_UNSPECIFIED;
  std::string error_message;
  bool lease_release_sent = false;

  std::vector<PendingWaiter> waiters;
};

struct QueueManager {
  std::deque<std::string> high;
  std::deque<std::string> normal;
  std::deque<std::string> best_effort;
  std::unordered_set<std::string> present;
  size_t max_length = 0;

  size_t Size() const { return high.size() + normal.size() + best_effort.size(); }

  bool Contains(const std::string& task_id) const {
    return present.find(task_id) != present.end();
  }

  void Enqueue(const std::string& task_id, QueuePriority p) {
    if (Contains(task_id)) {
      return;
    }
    present.insert(task_id);
    if (p == QueuePriority::HIGH) {
      high.push_back(task_id);
    } else if (p == QueuePriority::BEST_EFFORT) {
      best_effort.push_back(task_id);
    } else {
      normal.push_back(task_id);
    }
    max_length = std::max(max_length, Size());
  }

  bool DequeueNext(std::string* task_id_out) {
    auto pop_front = [&](std::deque<std::string>* q) -> bool {
      while (!q->empty()) {
        const std::string id = q->front();
        q->pop_front();
        auto it = present.find(id);
        if (it != present.end()) {
          present.erase(it);
          *task_id_out = id;
          return true;
        }
      }
      return false;
    };

    if (pop_front(&high)) {
      return true;
    }
    if (pop_front(&normal)) {
      return true;
    }
    return pop_front(&best_effort);
  }

  void Remove(const std::string& task_id) {
    present.erase(task_id);
  }

  int Position(const std::string& task_id) const {
    int pos = 1;
    auto lookup = [&](const std::deque<std::string>& q) -> int {
      for (const auto& id : q) {
        if (present.find(id) == present.end()) {
          continue;
        }
        if (id == task_id) {
          return pos;
        }
        ++pos;
      }
      return -1;
    };

    int p = lookup(high);
    if (p > 0) {
      return p;
    }

    p = lookup(normal);
    if (p > 0) {
      return p;
    }

    p = lookup(best_effort);
    if (p > 0) {
      return p;
    }
    return -1;
  }
};

struct PendingResponse {
  maigent::NatsMessage incoming;
  std::string request_id;
  std::string trace_id;
  std::string conversation_id;
  maigent::SubmitReply reply;
  maigent::WaitMode wait_mode = maigent::WAIT_MODE_UNSPECIFIED;
  std::string task_id;
};

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

bool IsTerminal(TaskFsmState s) {
  return s == TaskFsmState::RESERVE_DENIED || s == TaskFsmState::FINISHED ||
         s == TaskFsmState::FAILED || s == TaskFsmState::TIMEOUT ||
         s == TaskFsmState::CANCELLED;
}

std::string StateName(TaskFsmState s) {
  switch (s) {
    case TaskFsmState::NEW:
      return "NEW";
    case TaskFsmState::SUBMITTED:
      return "SUBMITTED";
    case TaskFsmState::RESERVING:
      return "RESERVING";
    case TaskFsmState::RESERVE_DENIED_TEMPORARY:
      return "RESERVE_DENIED_TEMPORARY";
    case TaskFsmState::RESERVE_DENIED:
      return "RESERVE_DENIED";
    case TaskFsmState::QUEUED:
      return "QUEUED";
    case TaskFsmState::WAITING_FOR_RESOURCES:
      return "WAITING_FOR_RESOURCES";
    case TaskFsmState::READY_TO_RETRY_RESERVE:
      return "READY_TO_RETRY_RESERVE";
    case TaskFsmState::RESERVE_GRANTED:
      return "RESERVE_GRANTED";
    case TaskFsmState::COMMITTING:
      return "COMMITTING";
    case TaskFsmState::COMMITTED:
      return "COMMITTED";
    case TaskFsmState::LAUNCH_SENT:
      return "LAUNCH_SENT";
    case TaskFsmState::STARTED:
      return "STARTED";
    case TaskFsmState::FINISHED:
      return "FINISHED";
    case TaskFsmState::FAILED:
      return "FAILED";
    case TaskFsmState::TIMEOUT:
      return "TIMEOUT";
    case TaskFsmState::CANCELLED:
      return "CANCELLED";
  }
  return "UNKNOWN";
}

QueuePriority PriorityFromTask(const TaskContext& ctx) {
  if (ctx.priority >= 50) {
    return QueuePriority::HIGH;
  }
  const std::string cls = ToLower(ctx.task_class);
  if (cls.find("high") != std::string::npos || cls.find("critical") != std::string::npos) {
    return QueuePriority::HIGH;
  }
  if (cls.find("best_effort") != std::string::npos || cls.find("background") != std::string::npos ||
      ctx.priority < 0) {
    return QueuePriority::BEST_EFFORT;
  }
  return QueuePriority::NORMAL;
}

bool WaitModeSatisfied(maigent::WaitMode mode, TaskFsmState state) {
  if (mode == maigent::WAIT_MODE_UNSPECIFIED || mode == maigent::WAIT_NO_WAIT) {
    return state == TaskFsmState::LAUNCH_SENT || state == TaskFsmState::STARTED ||
           state == TaskFsmState::FINISHED || state == TaskFsmState::FAILED ||
           state == TaskFsmState::TIMEOUT || state == TaskFsmState::RESERVE_DENIED ||
           state == TaskFsmState::QUEUED || state == TaskFsmState::WAITING_FOR_RESOURCES ||
           state == TaskFsmState::READY_TO_RETRY_RESERVE ||
           state == TaskFsmState::CANCELLED;
  }
  if (mode == maigent::WAIT_START) {
    return state == TaskFsmState::STARTED || state == TaskFsmState::FINISHED ||
           state == TaskFsmState::FAILED || state == TaskFsmState::TIMEOUT ||
           state == TaskFsmState::RESERVE_DENIED || state == TaskFsmState::CANCELLED;
  }
  return state == TaskFsmState::FINISHED || state == TaskFsmState::FAILED ||
         state == TaskFsmState::TIMEOUT || state == TaskFsmState::RESERVE_DENIED ||
         state == TaskFsmState::CANCELLED;
}

void Transition(TaskContext* ctx, TaskFsmState next_state, const std::string& reason,
                maigent::AgentLogger* log) {
  if (ctx == nullptr || ctx->current_state == next_state) {
    return;
  }
  const TaskFsmState prev = ctx->current_state;
  ctx->current_state = next_state;
  log->Info("task state transition " + ctx->task_id + " " + StateName(prev) + " -> " +
                StateName(next_state) + " reason=" + reason,
            {"", ctx->task_id, ctx->trace_id});
}

enum class DenyClassification {
  TEMPORARY = 0,
  HARD = 1,
};

DenyClassification ClassifyDeny(const maigent::LeaseDecision& decision) {
  switch (decision.reason_code()) {
    case maigent::LEASE_REASON_NO_SLOTS:
    case maigent::LEASE_REASON_NO_CAPACITY:
      return DenyClassification::TEMPORARY;
    case maigent::LEASE_REASON_POLICY_FORBIDDEN:
    case maigent::LEASE_REASON_INVALID_REQUEST:
    case maigent::LEASE_REASON_LIMIT_EXCEEDED:
    case maigent::LEASE_REASON_INTERNAL_ERROR:
      return DenyClassification::HARD;
    default:
      break;
  }

  const std::string reason = ToLower(decision.reason());
  if (reason.find("slots") != std::string::npos || reason.find("mem") != std::string::npos ||
      reason.find("cpu") != std::string::npos || reason.find("capacity") != std::string::npos ||
      reason.find("pressure") != std::string::npos) {
    return DenyClassification::TEMPORARY;
  }
  return DenyClassification::HARD;
}

std::string QueueReason(const maigent::LeaseDecision& decision) {
  switch (decision.reason_code()) {
    case maigent::LEASE_REASON_NO_SLOTS:
      return "no_slots";
    case maigent::LEASE_REASON_NO_CAPACITY:
      return "temporary_capacity_exhausted";
    default:
      break;
  }
  const std::string reason = ToLower(decision.reason());
  if (reason.find("slots") != std::string::npos) {
    return "no_slots";
  }
  if (reason.find("mem") != std::string::npos) {
    return "temporary_memory_pressure";
  }
  return "other_temporary_reason";
}

maigent::SubmitReply BuildSubmitReply(const TaskContext& ctx, maigent::WaitMode wait_mode,
                                      bool waiter_timed_out) {
  maigent::SubmitReply out;
  out.set_task_id(ctx.task_id);
  out.set_trace_id(ctx.trace_id);
  out.set_lease_id(ctx.lease_id);
  out.set_executor_id(ctx.executor_id);
  out.set_pid(ctx.pid);
  out.set_submitted_ts_ms(ctx.submitted_ts_ms);
  out.set_started_ts_ms(ctx.started_ts_ms);
  out.set_finished_ts_ms(ctx.finished_ts_ms);

  if (waiter_timed_out) {
    out.set_status(maigent::SUBMIT_TIMEOUT);
    out.set_message("wait timeout for mode=" + std::to_string(wait_mode));
    return out;
  }

  switch (ctx.current_state) {
    case TaskFsmState::RESERVE_DENIED:
      out.set_status(maigent::SUBMIT_DENIED);
      out.set_message(ctx.error_message.empty() ? "reserve denied" : ctx.error_message);
      return out;
    case TaskFsmState::FAILED:
      out.set_status(maigent::SUBMIT_FAILED);
      out.set_message(ctx.error_message.empty() ? "failed" : ctx.error_message);
      return out;
    case TaskFsmState::TIMEOUT:
      out.set_status(maigent::SUBMIT_TIMEOUT);
      out.set_message(ctx.error_message.empty() ? "timeout" : ctx.error_message);
      return out;
    case TaskFsmState::CANCELLED:
      out.set_status(maigent::SUBMIT_FAILED);
      out.set_message(ctx.error_message.empty() ? "cancelled" : ctx.error_message);
      return out;
    case TaskFsmState::QUEUED:
    case TaskFsmState::WAITING_FOR_RESOURCES:
    case TaskFsmState::READY_TO_RETRY_RESERVE:
    case TaskFsmState::RESERVE_DENIED_TEMPORARY:
      out.set_status(maigent::SUBMIT_ACCEPTED);
      out.set_message("queued");
      return out;
    case TaskFsmState::FINISHED:
      out.set_status(maigent::SUBMIT_ACCEPTED);
      out.set_message("finished");
      return out;
    case TaskFsmState::STARTED:
      out.set_status(maigent::SUBMIT_ACCEPTED);
      out.set_message("started");
      return out;
    case TaskFsmState::LAUNCH_SENT:
      out.set_status(maigent::SUBMIT_ACCEPTED);
      out.set_message("launch_sent");
      return out;
    case TaskFsmState::COMMITTED:
    case TaskFsmState::COMMITTING:
    case TaskFsmState::RESERVE_GRANTED:
    case TaskFsmState::RESERVING:
    case TaskFsmState::SUBMITTED:
    case TaskFsmState::NEW:
      out.set_status(maigent::SUBMIT_ACCEPTED);
      out.set_message("accepted");
      return out;
  }

  out.set_status(maigent::SUBMIT_FAILED);
  out.set_message("unknown state");
  return out;
}

maigent::TaskStatusReply BuildTaskStatusReply(const TaskContext& ctx, int queue_position) {
  maigent::TaskStatusReply out;
  out.set_task_id(ctx.task_id);
  out.set_trace_id(ctx.trace_id);
  if (!ctx.submit_request_ids.empty()) {
    out.set_submit_request_id(ctx.submit_request_ids.front());
  }
  out.set_current_state(StateName(ctx.current_state));
  out.set_status(ctx.status_to_return);
  out.set_message(ctx.error_message);
  out.set_lease_id(ctx.lease_id);
  out.set_executor_id(ctx.executor_id);
  out.set_pid(ctx.pid);
  out.set_submitted_ts_ms(ctx.submitted_ts_ms);
  out.set_reserve_requested_ts_ms(ctx.reserve_requested_ts_ms);
  out.set_reserve_decision_ts_ms(ctx.reserve_decision_ts_ms);
  out.set_commit_requested_ts_ms(ctx.commit_requested_ts_ms);
  out.set_commit_ack_ts_ms(ctx.commit_ack_ts_ms);
  out.set_launch_sent_ts_ms(ctx.launch_sent_ts_ms);
  out.set_started_ts_ms(ctx.started_ts_ms);
  out.set_finished_ts_ms(ctx.finished_ts_ms);
  out.set_exit_code(ctx.exit_code);
  out.set_error_message(ctx.error_message);
  out.set_queued(ctx.queued);
  out.set_queue_position(queue_position);
  out.set_queue_enter_ts_ms(ctx.queue_enter_ts_ms);
  out.set_queue_deadline_ts_ms(ctx.queue_deadline_ts_ms);
  out.set_queue_attempts(ctx.queue_attempts);
  out.set_queued_reason(ctx.queued_reason);
  out.set_last_reserve_failure_reason(ctx.last_reserve_failure_reason);
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "task_manager_agent";
  const std::string agent_id = "task-manager-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");

  const std::string admission_mode_str =
      ToLower(maigent::GetFlagValue(argc, argv, "--admission-mode", "queue"));
  const AdmissionMode admission_mode =
      (admission_mode_str == "deny") ? AdmissionMode::DENY : AdmissionMode::QUEUE;

  const int lease_ttl_ms = maigent::GetFlagInt(argc, argv, "--lease-ttl-ms", 5000);
  const int reserve_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--reserve-timeout-ms", 1500);
  const int commit_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--commit-timeout-ms", 1500);
  const int start_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--start-timeout-ms", 3000);
  const int finish_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--finish-timeout-ms", 10000);
  const int queue_wait_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--queue-wait-timeout-ms", 5000);
  const int queue_retry_interval_ms =
      maigent::GetFlagInt(argc, argv, "--queue-retry-interval-ms", 150);
  const int queue_pump_batch =
      maigent::GetFlagInt(argc, argv, "--queue-pump-batch", 4);
  const int retention_ms =
      maigent::GetFlagInt(argc, argv, "--task-retention-ms", 60000);

  maigent::AgentLogger log(agent_id, "logs/task_manager.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, std::shared_ptr<TaskContext>> task_registry;
  std::unordered_map<std::string, std::string> request_to_task_id;
  QueueManager queue_manager;

  maigent::PressureState latest_pressure;
  maigent::ForecastState latest_forecast;
  maigent::CapacityState latest_capacity;

  std::atomic<bool> queue_pump_requested{false};

  auto task_event_kind = [&](maigent::TaskEventType type) {
    switch (type) {
      case maigent::TASK_SUBMITTED:
        return maigent::MK_TASK_SUBMITTED;
      case maigent::TASK_QUEUED:
        return maigent::MK_TASK_QUEUED;
      case maigent::TASK_DEQUEUED:
        return maigent::MK_TASK_DEQUEUED;
      case maigent::TASK_STARTED:
        return maigent::MK_TASK_STARTED;
      case maigent::TASK_FINISHED:
        return maigent::MK_TASK_FINISHED;
      case maigent::TASK_FAILED:
        return maigent::MK_TASK_FAILED;
      case maigent::TASK_QUEUE_TIMEOUT:
        return maigent::MK_TASK_QUEUE_TIMEOUT;
      default:
        return maigent::MK_UNSPECIFIED;
    }
  };

  auto publish_queue_event = [&](const std::string& subject, maigent::TaskEventType type,
                                 const TaskContext& ctx, int queue_length,
                                 const std::string& queue_reason) {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::EVENT, task_event_kind(type), role, agent_id,
                        maigent::MakeRequestId("evt-queue"), ctx.trace_id,
                        ctx.task_id);
    auto* te = evt.mutable_event()->mutable_task_event();
    te->set_task_id(ctx.task_id);
    te->set_trace_id(ctx.trace_id);
    te->set_lease_id(ctx.lease_id);
    te->set_executor_id(ctx.executor_id);
    te->set_event_type(type);
    te->set_pid(ctx.pid);
    te->set_ts_ms(maigent::NowMs());
    te->set_submitted_ts_ms(ctx.submitted_ts_ms);
    te->set_task_class(ctx.task_class);
    te->set_queue_length(queue_length);
    te->set_queue_reason(queue_reason);
    te->set_queue_enter_ts_ms(ctx.queue_enter_ts_ms);
    nats.PublishEnvelope(subject, evt);
  };

  auto respond_submit = [&](const PendingResponse& pr) {
    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SERVICE, maigent::MK_TASK_SUBMIT_REPLY,
                        role, agent_id,
                        pr.request_id, pr.trace_id, pr.conversation_id);
    *reply.mutable_service()->mutable_submit_reply() = pr.reply;
    nats.RespondEnvelope(pr.incoming, reply);
    log.Info("submit waiter completed mode=" + std::to_string(pr.wait_mode) +
                 " status=" + std::to_string(pr.reply.status()),
             {pr.request_id, pr.task_id, pr.trace_id});
  };

  auto process_waiters_for_task = [&](const std::string& task_id) {
    std::vector<PendingResponse> pending_responses;

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = task_registry.find(task_id);
      if (it == task_registry.end()) {
        return;
      }
      auto& ctx = *it->second;
      const int64_t now = maigent::NowMs();

      std::vector<PendingWaiter> remaining;
      remaining.reserve(ctx.waiters.size());
      for (const auto& waiter : ctx.waiters) {
        const bool waiter_timed_out = waiter.deadline_ts_ms > 0 && now >= waiter.deadline_ts_ms;
        if (WaitModeSatisfied(waiter.wait_mode, ctx.current_state) || waiter_timed_out) {
          PendingResponse out;
          out.incoming = waiter.incoming;
          out.request_id = waiter.submit_request_id;
          out.trace_id = waiter.submit_trace_id.empty() ? ctx.trace_id : waiter.submit_trace_id;
          out.conversation_id = waiter.submit_conversation_id.empty()
                                    ? ctx.task_id
                                    : waiter.submit_conversation_id;
          out.reply = BuildSubmitReply(ctx, waiter.wait_mode, waiter_timed_out);
          out.wait_mode = waiter.wait_mode;
          out.task_id = ctx.task_id;
          pending_responses.push_back(std::move(out));
        } else {
          remaining.push_back(waiter);
        }
      }
      ctx.waiters.swap(remaining);
    }

    for (const auto& response : pending_responses) {
      respond_submit(response);
    }
  };

  auto publish_task_submitted = [&](const TaskContext& ctx) {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::EVENT, maigent::MK_TASK_SUBMITTED, role, agent_id,
                        maigent::MakeRequestId("evt-submit"), ctx.trace_id,
                        ctx.task_id);
    auto* te = evt.mutable_event()->mutable_task_event();
    te->set_task_id(ctx.task_id);
    te->set_trace_id(ctx.trace_id);
    te->set_event_type(maigent::TASK_SUBMITTED);
    te->set_ts_ms(ctx.submitted_ts_ms);
    te->set_submitted_ts_ms(ctx.submitted_ts_ms);
    te->set_task_class(ctx.task_class);
    nats.PublishEnvelope(maigent::kSubjectEvtTaskSubmitted, evt);
  };

  auto release_lease_if_needed = [&](const std::shared_ptr<TaskContext>& ctx,
                                     const std::string& reason) {
    if (ctx == nullptr || ctx->lease_id.empty()) {
      return;
    }

    bool should_release = false;
    {
      std::lock_guard<std::mutex> lock(mu);
      if (!ctx->lease_release_sent) {
        ctx->lease_release_sent = true;
        should_release = true;
      }
    }

    if (!should_release) {
      return;
    }

    maigent::Envelope req;
    maigent::FillHeader(&req, maigent::COMMAND, maigent::MK_LEASE_RELEASE,
                        role, agent_id,
                        maigent::MakeRequestId("lease-release-timeout"), ctx->trace_id,
                        ctx->task_id);
    auto* rel = req.mutable_command()->mutable_lease_release();
    rel->set_task_id(ctx->task_id);
    rel->set_lease_id(ctx->lease_id);
    rel->set_reason(reason);

    maigent::Envelope reply;
    std::string err;
    nats.RequestEnvelope(maigent::kSubjectCmdLeaseRelease, req, 1000, &reply, &err);
    log.Warn("lease release sent for task reason=" + reason,
             {req.header().request_id(), ctx->task_id, ctx->trace_id});
  };

  std::function<void(const std::shared_ptr<TaskContext>&, bool)> try_reserve_commit_launch;

  try_reserve_commit_launch = [&](const std::shared_ptr<TaskContext>& ctx, bool from_queue) {
    if (ctx == nullptr) {
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      if (IsTerminal(ctx->current_state) || ctx->reserve_in_flight) {
        return;
      }
      ctx->reserve_in_flight = true;
      if (from_queue) {
        Transition(ctx.get(), TaskFsmState::READY_TO_RETRY_RESERVE, "queue retry selected", &log);
      }
      Transition(ctx.get(), TaskFsmState::RESERVING, from_queue ? "retry reserve" : "initial reserve", &log);
      ctx->reserve_requested_ts_ms = maigent::NowMs();
      ctx->queue_attempts += 1;
    }

    maigent::Envelope reserve_req;
    maigent::FillHeader(&reserve_req, maigent::COMMAND, maigent::MK_LEASE_RESERVE,
                        role, agent_id,
                        maigent::MakeRequestId("lease-reserve"), ctx->trace_id, ctx->task_id);
    auto* reserve = reserve_req.mutable_command()->mutable_lease_reserve();
    reserve->set_task_id(ctx->task_id);
    reserve->set_ttl_ms(lease_ttl_ms);
    *reserve->mutable_resources() = ctx->resources;

    maigent::Envelope reserve_reply;
    std::string reserve_err;
    const bool reserve_ok = nats.RequestEnvelope(maigent::kSubjectCmdLeaseReserve, reserve_req,
                                                 reserve_timeout_ms, &reserve_reply,
                                                 &reserve_err) &&
                            reserve_reply.has_service() && reserve_reply.service().has_lease_decision() &&
                            reserve_reply.has_header() &&
                            reserve_reply.header().message_category() == maigent::SERVICE &&
                            reserve_reply.header().message_kind() == maigent::MK_LEASE_DECISION;

    if (!reserve_ok) {
      bool requeued = false;
      int queue_len = 0;
      {
        std::lock_guard<std::mutex> lock(mu);
        ctx->reserve_in_flight = false;
        ctx->last_reserve_failure_reason = "reserve request error: " + reserve_err;

        if (admission_mode == AdmissionMode::QUEUE && !IsTerminal(ctx->current_state)) {
          const int64_t now = maigent::NowMs();
          if (ctx->queue_enter_ts_ms == 0) {
            ctx->queue_enter_ts_ms = now;
            ctx->queue_deadline_ts_ms = now + queue_wait_timeout_ms;
          }
          if (ctx->queue_deadline_ts_ms <= 0 || now < ctx->queue_deadline_ts_ms) {
            Transition(ctx.get(), TaskFsmState::RESERVE_DENIED_TEMPORARY,
                       "temporary reserve transport error", &log);
            Transition(ctx.get(), TaskFsmState::QUEUED, "enqueue after reserve transport error", &log);
            Transition(ctx.get(), TaskFsmState::WAITING_FOR_RESOURCES, "waiting for resources", &log);
            ctx->queued = true;
            ctx->was_queued = true;
            ctx->queued_reason = "other_temporary_reason";
            ctx->status_to_return = maigent::SUBMIT_ACCEPTED;
            queue_manager.Enqueue(ctx->task_id, ctx->queue_priority);
            queue_len = static_cast<int>(queue_manager.Size());
            requeued = true;
          }
        }

        if (!requeued) {
          Transition(ctx.get(), TaskFsmState::TIMEOUT, "reserve request failed", &log);
          ctx->status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx->error_message = "reserve failed: " + reserve_err;
          ctx->completed_ts_ms = maigent::NowMs();
        }
      }

      if (requeued) {
        publish_queue_event(maigent::kSubjectEvtTaskQueued, maigent::TASK_QUEUED,
                            *ctx, queue_len, ctx->queued_reason);
        log.Info("task entered queue task_id=" + ctx->task_id +
                     " reason=" + ctx->queued_reason +
                     " queue_len=" + std::to_string(queue_len),
                 {"", ctx->task_id, ctx->trace_id});
        queue_pump_requested.store(true);
      } else {
        log.Warn("hard timeout on reserve task_id=" + ctx->task_id +
                     " error=" + ctx->error_message,
                 {"", ctx->task_id, ctx->trace_id});
      }
      process_waiters_for_task(ctx->task_id);
      return;
    }

    const auto reserve_decision = reserve_reply.service().lease_decision();
    {
      std::lock_guard<std::mutex> lock(mu);
      ctx->reserve_in_flight = false;
      ctx->reserve_decision_ts_ms = reserve_decision.decision_ts_ms();
      ctx->lease_id = reserve_decision.lease_id();
      ctx->last_reserve_failure_reason = reserve_decision.reason();
    }

    if (!reserve_decision.granted()) {
      const DenyClassification deny_class = ClassifyDeny(reserve_decision);
      bool queued_now = false;
      int queue_len = 0;
      {
        std::lock_guard<std::mutex> lock(mu);
        if (deny_class == DenyClassification::TEMPORARY &&
            admission_mode == AdmissionMode::QUEUE && !IsTerminal(ctx->current_state)) {
          const int64_t now = maigent::NowMs();
          if (ctx->queue_enter_ts_ms == 0) {
            ctx->queue_enter_ts_ms = now;
            ctx->queue_deadline_ts_ms = now + queue_wait_timeout_ms;
          }
          if (ctx->queue_deadline_ts_ms <= 0 || now < ctx->queue_deadline_ts_ms) {
            ctx->queued_reason = QueueReason(reserve_decision);
            Transition(ctx.get(), TaskFsmState::RESERVE_DENIED_TEMPORARY,
                       reserve_decision.reason(), &log);
            Transition(ctx.get(), TaskFsmState::QUEUED, "temporary deny -> queue", &log);
            Transition(ctx.get(), TaskFsmState::WAITING_FOR_RESOURCES, "waiting for resources", &log);
            ctx->queued = true;
            ctx->was_queued = true;
            ctx->status_to_return = maigent::SUBMIT_ACCEPTED;
            queue_manager.Enqueue(ctx->task_id, ctx->queue_priority);
            queue_len = static_cast<int>(queue_manager.Size());
            queued_now = true;
          }
        }

        if (!queued_now) {
          Transition(ctx.get(), TaskFsmState::RESERVE_DENIED, reserve_decision.reason(), &log);
          ctx->status_to_return = maigent::SUBMIT_DENIED;
          ctx->error_message = reserve_decision.reason();
          ctx->completed_ts_ms = maigent::NowMs();
        }
      }

      if (queued_now) {
        publish_queue_event(maigent::kSubjectEvtTaskQueued, maigent::TASK_QUEUED,
                            *ctx, queue_len, ctx->queued_reason);
        log.Info("task entered queue task_id=" + ctx->task_id +
                     " reason=" + ctx->queued_reason +
                     " queue_len=" + std::to_string(queue_len),
                 {"", ctx->task_id, ctx->trace_id});
        queue_pump_requested.store(true);
      } else {
        log.Warn("hard deny task_id=" + ctx->task_id +
                     " reason=" + reserve_decision.reason(),
                 {"", ctx->task_id, ctx->trace_id});
      }

      process_waiters_for_task(ctx->task_id);
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      ctx->reserve_granted_ts_ms = maigent::NowMs();
      ctx->queued = false;
      Transition(ctx.get(), TaskFsmState::RESERVE_GRANTED, "reserve granted", &log);
      Transition(ctx.get(), TaskFsmState::COMMITTING, "sending commit", &log);
      ctx->commit_requested_ts_ms = maigent::NowMs();
      ctx->status_to_return = maigent::SUBMIT_ACCEPTED;
    }
    log.Info(std::string(from_queue ? "reserve granted from queue task_id="
                                    : "accepted immediately task_id=") +
                 ctx->task_id,
             {"", ctx->task_id, ctx->trace_id});

    maigent::Envelope commit_req;
    maigent::FillHeader(&commit_req, maigent::COMMAND, maigent::MK_LEASE_COMMIT,
                        role, agent_id,
                        maigent::MakeRequestId("lease-commit"), ctx->trace_id, ctx->task_id);
    commit_req.mutable_command()->mutable_lease_commit()->set_lease_id(ctx->lease_id);
    commit_req.mutable_command()->mutable_lease_commit()->set_task_id(ctx->task_id);

    maigent::Envelope commit_reply;
    std::string commit_err;
    if (!nats.RequestEnvelope(maigent::kSubjectCmdLeaseCommit, commit_req, commit_timeout_ms,
                              &commit_reply, &commit_err) ||
        !commit_reply.has_service() ||
        !commit_reply.service().has_lease_decision() ||
        !commit_reply.has_header() ||
        commit_reply.header().message_category() != maigent::SERVICE ||
        commit_reply.header().message_kind() != maigent::MK_LEASE_DECISION ||
        !commit_reply.service().lease_decision().granted()) {
      {
        std::lock_guard<std::mutex> lock(mu);
        Transition(ctx.get(), TaskFsmState::FAILED, "commit failed", &log);
        ctx->status_to_return = maigent::SUBMIT_FAILED;
        ctx->error_message = "commit failed: " + commit_err;
        ctx->completed_ts_ms = maigent::NowMs();
      }
      process_waiters_for_task(ctx->task_id);
      release_lease_if_needed(ctx, "commit_failed");
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      Transition(ctx.get(), TaskFsmState::COMMITTED, "commit ack", &log);
      ctx->commit_ack_ts_ms = maigent::NowMs();
    }

    maigent::Envelope launch;
    maigent::FillHeader(&launch, maigent::COMMAND, maigent::MK_TASK_LAUNCH,
                        role, agent_id,
                        maigent::MakeRequestId("launch"), ctx->trace_id, ctx->task_id);
    auto* payload = launch.mutable_command()->mutable_launch_task();
    payload->set_task_id(ctx->task_id);
    payload->set_trace_id(ctx->trace_id);
    payload->set_lease_id(ctx->lease_id);
    payload->set_cmd(ctx->cmd);
    for (const auto& arg : ctx->args) {
      payload->add_args(arg);
    }
    payload->set_cgroup_path("");
    payload->set_priority(ctx->priority);
    payload->set_task_class(ctx->task_class);
    payload->set_submitted_ts_ms(ctx->submitted_ts_ms);
    payload->set_lease_decision_ts_ms(ctx->reserve_decision_ts_ms);
    nats.PublishEnvelope(maigent::kSubjectCmdTaskExecLaunch, launch);

    {
      std::lock_guard<std::mutex> lock(mu);
      Transition(ctx.get(), TaskFsmState::LAUNCH_SENT, "launch published", &log);
      ctx->launch_sent_ts_ms = maigent::NowMs();
      ctx->status_to_return = maigent::SUBMIT_ACCEPTED;
    }
    if (from_queue) {
      log.Info("task dequeued for launch task_id=" + ctx->task_id,
               {"", ctx->task_id, ctx->trace_id});
    }

    process_waiters_for_task(ctx->task_id);
  };

  auto pump_queue = [&]() {
    std::vector<std::shared_ptr<TaskContext>> to_retry;
    std::vector<std::shared_ptr<TaskContext>> timed_out;
    std::vector<int> queue_lens;

    {
      std::lock_guard<std::mutex> lock(mu);
      for (int i = 0; i < queue_pump_batch; ++i) {
        std::string task_id;
        if (!queue_manager.DequeueNext(&task_id)) {
          break;
        }

        auto it = task_registry.find(task_id);
        if (it == task_registry.end() || it->second == nullptr) {
          continue;
        }

        auto ctx = it->second;
        if (IsTerminal(ctx->current_state)) {
          continue;
        }

        ctx->queued = false;

        const int64_t now = maigent::NowMs();
        if (ctx->queue_deadline_ts_ms > 0 && now >= ctx->queue_deadline_ts_ms) {
          Transition(ctx.get(), TaskFsmState::TIMEOUT, "queue wait timeout", &log);
          ctx->status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx->error_message = "queue wait timeout";
          ctx->completed_ts_ms = now;
          timed_out.push_back(ctx);
          continue;
        }

        Transition(ctx.get(), TaskFsmState::READY_TO_RETRY_RESERVE, "dequeued for retry", &log);
        to_retry.push_back(ctx);
        queue_lens.push_back(static_cast<int>(queue_manager.Size()));
      }
    }

    for (const auto& ctx : timed_out) {
      publish_queue_event(maigent::kSubjectEvtTaskQueueTimeout, maigent::TASK_QUEUE_TIMEOUT,
                          *ctx, 0, "queue_wait_timeout");
      process_waiters_for_task(ctx->task_id);
      log.Warn("queue timeout task_id=" + ctx->task_id,
               {"", ctx->task_id, ctx->trace_id});
    }

    for (size_t i = 0; i < to_retry.size(); ++i) {
      publish_queue_event(maigent::kSubjectEvtTaskDequeued, maigent::TASK_DEQUEUED,
                          *to_retry[i], queue_lens[i], "retry_reserve");
      log.Info("task dequeued for reserve retry task_id=" + to_retry[i]->task_id +
                   " queue_len=" + std::to_string(queue_lens[i]),
               {"", to_retry[i]->task_id, to_retry[i]->trace_id});
      std::thread([&, ctx = to_retry[i]]() { try_reserve_commit_launch(ctx, true); }).detach();
    }
  };

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_STARTED) {
      return;
    }
    const auto& evt = env.event().task_event();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = task_registry.find(evt.task_id());
      if (it == task_registry.end() || it->second == nullptr) {
        return;
      }
      auto& ctx = *it->second;
      if (IsTerminal(ctx.current_state)) {
        return;
      }
      ctx.executor_id = evt.executor_id();
      ctx.pid = evt.pid();
      ctx.started_ts_ms = evt.ts_ms();
      if (evt.lease_decision_ts_ms() > 0) {
        ctx.reserve_decision_ts_ms = evt.lease_decision_ts_ms();
      }
      Transition(&ctx, TaskFsmState::STARTED, "evt.task.started", &log);
      ctx.status_to_return = maigent::SUBMIT_ACCEPTED;
    }

    process_waiters_for_task(evt.task_id());
  });

  nats.Subscribe(maigent::kSubjectEvtTaskFinished, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_FINISHED) {
      return;
    }
    const auto& evt = env.event().task_event();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = task_registry.find(evt.task_id());
      if (it == task_registry.end() || it->second == nullptr) {
        return;
      }
      auto& ctx = *it->second;
      ctx.executor_id = evt.executor_id();
      ctx.pid = evt.pid();
      ctx.exit_code = evt.exit_code();
      ctx.finished_ts_ms = evt.ts_ms();
      Transition(&ctx, TaskFsmState::FINISHED, "evt.task.finished", &log);
      ctx.status_to_return = maigent::SUBMIT_ACCEPTED;
      ctx.completed_ts_ms = maigent::NowMs();
    }

    process_waiters_for_task(evt.task_id());
    queue_pump_requested.store(true);
  });

  nats.Subscribe(maigent::kSubjectEvtTaskFailed, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_FAILED) {
      return;
    }
    const auto& evt = env.event().task_event();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = task_registry.find(evt.task_id());
      if (it == task_registry.end() || it->second == nullptr) {
        return;
      }
      auto& ctx = *it->second;
      ctx.executor_id = evt.executor_id();
      ctx.pid = evt.pid();
      ctx.exit_code = evt.exit_code();
      ctx.finished_ts_ms = evt.ts_ms();
      ctx.error_message = evt.error();
      Transition(&ctx, TaskFsmState::FAILED, "evt.task.failed", &log);
      ctx.status_to_return = maigent::SUBMIT_FAILED;
      ctx.completed_ts_ms = maigent::NowMs();
    }

    process_waiters_for_task(evt.task_id());
    queue_pump_requested.store(true);
  });

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
    latest_pressure = env.state().pressure_state();
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
    latest_forecast = env.state().forecast_state();
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
    {
      std::lock_guard<std::mutex> lock(mu);
      latest_capacity = env.state().capacity_state();
    }
    queue_pump_requested.store(true);
  });

  nats.Subscribe(maigent::kSubjectCmdTaskSubmit, [&](const maigent::NatsMessage& incoming) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(incoming.data.data(),
                                static_cast<int>(incoming.data.size()), &req) ||
        !req.has_command() || !req.command().has_submit_task()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_TASK_SUBMIT) {
      return;
    }

    const auto& submit = req.command().submit_task();
    const std::string request_id = req.header().request_id();
    const std::string trace_id =
        req.header().trace_id().empty() ? maigent::MakeTraceId() : req.header().trace_id();

    maigent::WaitMode wait_mode = submit.wait_mode();
    if (wait_mode == maigent::WAIT_MODE_UNSPECIFIED) {
      wait_mode = maigent::WAIT_START;
    }

    const int64_t now = maigent::NowMs();
    int64_t wait_timeout = 0;
    if (wait_mode == maigent::WAIT_START) {
      wait_timeout = submit.wait_timeout_ms() > 0 ? submit.wait_timeout_ms() : start_timeout_ms;
    } else if (wait_mode == maigent::WAIT_FINISH) {
      wait_timeout = submit.wait_timeout_ms() > 0 ? submit.wait_timeout_ms() : finish_timeout_ms;
    }

    std::shared_ptr<TaskContext> ctx;
    bool is_new_task = false;

    {
      std::lock_guard<std::mutex> lock(mu);
      auto existing = request_to_task_id.find(request_id);
      if (existing != request_to_task_id.end()) {
        auto it_task = task_registry.find(existing->second);
        if (it_task != task_registry.end()) {
          ctx = it_task->second;
          log.Info("idempotent submit request_id=" + request_id + " task_id=" + ctx->task_id,
                   {request_id, ctx->task_id, trace_id});
        }
      }

      if (ctx == nullptr) {
        ctx = std::make_shared<TaskContext>();
        ctx->task_id = maigent::MakeTaskId();
        ctx->trace_id = trace_id;
        ctx->submit_request_ids.push_back(request_id);
        ctx->bench_id = submit.bench_id();
        ctx->task_class = submit.task_class();
        ctx->priority = submit.priority();
        ctx->cmd = submit.cmd();
        ctx->args.assign(submit.args().begin(), submit.args().end());
        ctx->queue_priority = PriorityFromTask(*ctx);
        if (submit.has_resources()) {
          ctx->resources = submit.resources();
        } else {
          ctx->resources.set_cpu_millis(100);
          ctx->resources.set_mem_mb(64);
        }

        ctx->current_state = TaskFsmState::NEW;
        ctx->submitted_ts_ms = now;
        ctx->status_to_return = maigent::SUBMIT_ACCEPTED;
        Transition(ctx.get(), TaskFsmState::SUBMITTED, "submit received", &log);

        task_registry[ctx->task_id] = ctx;
        request_to_task_id[request_id] = ctx->task_id;
        is_new_task = true;
      } else {
        request_to_task_id[request_id] = ctx->task_id;
        ctx->submit_request_ids.push_back(request_id);
      }

      PendingWaiter waiter;
      waiter.incoming = incoming;
      waiter.submit_request_id = request_id;
      waiter.submit_trace_id = trace_id;
      waiter.submit_conversation_id = req.header().conversation_id().empty()
                                          ? ctx->task_id
                                          : req.header().conversation_id();
      waiter.wait_mode = wait_mode;
      waiter.deadline_ts_ms = wait_timeout > 0 ? now + wait_timeout : 0;
      ctx->waiters.push_back(std::move(waiter));
    }

    if (is_new_task) {
      publish_task_submitted(*ctx);
      std::thread([&, ctx]() { try_reserve_commit_launch(ctx, false); }).detach();
      log.Info("submit accepted async mode=" + std::to_string(wait_mode) +
                   " admission_mode=" +
                   (admission_mode == AdmissionMode::QUEUE ? "queue" : "deny"),
               {request_id, ctx->task_id, trace_id});
    }

    process_waiters_for_task(ctx->task_id);
  });

  nats.Subscribe(maigent::kSubjectCmdTaskStatus, [&](const maigent::NatsMessage& incoming) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(incoming.data.data(),
                                static_cast<int>(incoming.data.size()), &req) ||
        !req.has_command() || !req.command().has_task_status_request()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_TASK_STATUS_REQUEST) {
      return;
    }

    const std::string task_id = req.command().task_status_request().task_id();
    maigent::TaskStatusReply status;

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = task_registry.find(task_id);
      if (it == task_registry.end() || it->second == nullptr) {
        status.set_task_id(task_id);
        status.set_current_state("NOT_FOUND");
        status.set_status(maigent::SUBMIT_FAILED);
        status.set_message("task_id not found");
      } else {
        const int queue_position = it->second->queued ? queue_manager.Position(task_id) : -1;
        status = BuildTaskStatusReply(*it->second, queue_position);
      }
    }

    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SERVICE, maigent::MK_TASK_STATUS_RESPONSE,
                        role, agent_id,
                        req.header().request_id(), req.header().trace_id(),
                        req.header().conversation_id());
    *reply.mutable_service()->mutable_task_status_reply() = status;
    nats.RespondEnvelope(incoming, reply);
  });

  nats.Subscribe(maigent::kSubjectCmdSystemStatus, [&](const maigent::NatsMessage& incoming) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(incoming.data.data(),
                                static_cast<int>(incoming.data.size()),
                                &req) ||
        !req.has_command() ||
        !req.command().has_system_status_request()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_SYSTEM_STATUS_REQUEST) {
      return;
    }

    maigent::DirectorySnapshot dir;
    {
      maigent::Envelope snapshot_req;
      maigent::FillHeader(&snapshot_req, maigent::SERVICE,
                          maigent::MK_DIRECTORY_SNAPSHOT_REQUEST, role, agent_id,
                          maigent::MakeRequestId("dir-snapshot"));
      snapshot_req.mutable_service()->mutable_directory_snapshot_request();

      maigent::Envelope snapshot_reply;
      std::string error;
      if (nats.RequestEnvelope(maigent::kSubjectSvcDirSnapshotRequest, snapshot_req, 1000,
                               &snapshot_reply, &error) &&
          snapshot_reply.has_service() && snapshot_reply.service().has_directory_snapshot() &&
          snapshot_reply.has_header() &&
          snapshot_reply.header().message_category() == maigent::SERVICE &&
          snapshot_reply.header().message_kind() == maigent::MK_DIRECTORY_SNAPSHOT_RESPONSE) {
        dir = snapshot_reply.service().directory_snapshot();
      }
    }

    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SERVICE, maigent::MK_SYSTEM_STATUS_RESPONSE,
                        role, agent_id,
                        req.header().request_id(), req.header().trace_id(),
                        req.header().conversation_id());

    auto* status = reply.mutable_service()->mutable_system_status_reply();
    status->set_ts_ms(maigent::NowMs());
    *status->mutable_directory() = dir;

    {
      std::lock_guard<std::mutex> lock(mu);
      if (latest_pressure.ts_ms() > 0) {
        *status->mutable_pressure() = latest_pressure;
      }
      if (latest_forecast.ts_ms() > 0) {
        *status->mutable_forecast() = latest_forecast;
      }
      if (latest_capacity.ts_ms() > 0) {
        *status->mutable_capacity() = latest_capacity;
      }
    }

    nats.RespondEnvelope(incoming, reply);
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    int active = 0;
    for (const auto& [task_id, ptr] : task_registry) {
      if (ptr != nullptr && (ptr->current_state == TaskFsmState::STARTED ||
                             ptr->current_state == TaskFsmState::LAUNCH_SENT ||
                             ptr->current_state == TaskFsmState::COMMITTED ||
                             ptr->current_state == TaskFsmState::COMMITTING ||
                             ptr->current_state == TaskFsmState::RESERVING ||
                             ptr->current_state == TaskFsmState::WAITING_FOR_RESOURCES ||
                             ptr->current_state == TaskFsmState::READY_TO_RETRY_RESERVE)) {
        ++active;
      }
    }
    return active;
  };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"task.submit.async", "task.status", "task.queue", "system.status", "task.dispatch"},
      inflight_fn);
  lifecycle.Start(1000);

  log.Info("started admission_mode=" +
           std::string(admission_mode == AdmissionMode::QUEUE ? "queue" : "deny"));

  int64_t last_queue_pump_ts = 0;

  while (!g_stop.load()) {
    std::vector<std::shared_ptr<TaskContext>> needs_release;
    std::vector<std::shared_ptr<TaskContext>> queue_timeout_events;
    std::vector<std::string> to_process_waiters;
    std::vector<std::string> periodic_waiter_check;

    const int64_t now = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      std::vector<std::string> remove_task_ids;
      for (auto& [task_id, ctx_ptr] : task_registry) {
        if (ctx_ptr == nullptr) {
          continue;
        }
        auto& ctx = *ctx_ptr;

        if (ctx.queued && ctx.queue_deadline_ts_ms > 0 && now >= ctx.queue_deadline_ts_ms) {
          queue_manager.Remove(task_id);
          ctx.queued = false;
          Transition(&ctx, TaskFsmState::TIMEOUT, "queue wait timeout", &log);
          ctx.status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx.error_message = "queue wait timeout";
          ctx.completed_ts_ms = now;
          queue_timeout_events.push_back(ctx_ptr);
          to_process_waiters.push_back(task_id);
        }

        if (ctx.current_state == TaskFsmState::RESERVING &&
            ctx.reserve_requested_ts_ms > 0 &&
            now - ctx.reserve_requested_ts_ms > reserve_timeout_ms * 2) {
          Transition(&ctx, TaskFsmState::TIMEOUT, "reserve stage timeout", &log);
          ctx.status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx.error_message = "reserve stage timeout";
          ctx.completed_ts_ms = now;
          to_process_waiters.push_back(task_id);
          needs_release.push_back(ctx_ptr);
        } else if (ctx.current_state == TaskFsmState::COMMITTING &&
                   ctx.commit_requested_ts_ms > 0 &&
                   now - ctx.commit_requested_ts_ms > commit_timeout_ms * 2) {
          Transition(&ctx, TaskFsmState::TIMEOUT, "commit stage timeout", &log);
          ctx.status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx.error_message = "commit stage timeout";
          ctx.completed_ts_ms = now;
          to_process_waiters.push_back(task_id);
          needs_release.push_back(ctx_ptr);
        } else if (ctx.current_state == TaskFsmState::LAUNCH_SENT &&
                   ctx.launch_sent_ts_ms > 0 &&
                   now - ctx.launch_sent_ts_ms > start_timeout_ms) {
          Transition(&ctx, TaskFsmState::TIMEOUT, "start timeout", &log);
          ctx.status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx.error_message = "task start timeout";
          ctx.completed_ts_ms = now;
          to_process_waiters.push_back(task_id);
          needs_release.push_back(ctx_ptr);
        } else if (ctx.current_state == TaskFsmState::STARTED &&
                   ctx.started_ts_ms > 0 &&
                   now - ctx.started_ts_ms > finish_timeout_ms) {
          Transition(&ctx, TaskFsmState::TIMEOUT, "finish timeout", &log);
          ctx.status_to_return = maigent::SUBMIT_TIMEOUT;
          ctx.error_message = "task finish timeout";
          ctx.completed_ts_ms = now;
          to_process_waiters.push_back(task_id);
          needs_release.push_back(ctx_ptr);
        }

        if (IsTerminal(ctx.current_state) && ctx.completed_ts_ms > 0 &&
            now - ctx.completed_ts_ms > retention_ms && ctx.waiters.empty()) {
          remove_task_ids.push_back(task_id);
        }

        if (!ctx.waiters.empty()) {
          periodic_waiter_check.push_back(task_id);
        }
      }

      for (const auto& task_id : remove_task_ids) {
        auto it = task_registry.find(task_id);
        if (it == task_registry.end() || it->second == nullptr) {
          continue;
        }
        queue_manager.Remove(task_id);
        for (const auto& req_id : it->second->submit_request_ids) {
          request_to_task_id.erase(req_id);
        }
        task_registry.erase(it);
      }
    }

    for (const auto& ctx : queue_timeout_events) {
      publish_queue_event(maigent::kSubjectEvtTaskQueueTimeout, maigent::TASK_QUEUE_TIMEOUT,
                          *ctx, 0, "queue_wait_timeout");
    }

    for (const auto& task_id : to_process_waiters) {
      process_waiters_for_task(task_id);
    }

    for (const auto& task_id : periodic_waiter_check) {
      process_waiters_for_task(task_id);
    }

    for (const auto& ctx : needs_release) {
      release_lease_if_needed(ctx, "task_manager_timeout");
    }

    const bool should_pump = queue_pump_requested.exchange(false) ||
                             (now - last_queue_pump_ts >= queue_retry_interval_ms);
    if (should_pump) {
      pump_queue();
      last_queue_pump_ts = now;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
