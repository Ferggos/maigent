#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
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

namespace {

std::atomic<bool> g_stop{false};
constexpr const char* kSubjectCmdTaskExecControlAll = "msg.command.taskexec.control.*";

void HandleSignal(int) { g_stop.store(true); }

struct TaskSample {
  std::string task_id;
  std::string trace_id;
  std::string executor_id;
  std::string task_class;
  int64_t submit_ts = 0;
  int64_t lease_decision_ts = 0;
  int64_t start_ts = 0;
  int64_t finish_ts = 0;
  bool failed = false;
  bool denied = false;
  int exit_code = 0;
  std::string error;
  bool queued_once = false;
  bool counted_start_acceptance = false;
  bool counted_queue_timeout = false;
  int64_t queue_enter_ts = 0;
  int64_t queue_wait_ms = -1;
  int64_t queue_timeout_ts = 0;
};

struct Counters {
  int planned_requests = 0;
  int sent_requests = 0;
  int completed = 0;
  int failed = 0;
  int denied = 0;
  int timeouts = 0;
  int runtime_actions_emitted = 0;
  int runtime_actions_emitted_task_executor = 0;
  int runtime_actions_emitted_actuator = 0;
  int control_applied = 0;
  int control_failed = 0;
  int actuator_applied = 0;
  int actuator_failed = 0;
};

struct BenchSessionState {
  std::string bench_id;
  int64_t start_ts = 0;
  int64_t stop_ts = 0;
  bool running = false;
};

struct StatSummary {
  size_t n = 0;
  double min = 0.0;
  double p50 = 0.0;
  double p95 = 0.0;
  double p99 = 0.0;
  double avg = 0.0;
  double max = 0.0;
};

double Quantile(std::vector<double> values, double q) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  const double idx = q * static_cast<double>(values.size() - 1);
  const size_t lo = static_cast<size_t>(idx);
  const size_t hi = std::min(values.size() - 1, lo + 1);
  const double frac = idx - static_cast<double>(lo);
  return values[lo] * (1.0 - frac) + values[hi] * frac;
}

StatSummary BuildSummary(const std::vector<double>& values) {
  StatSummary out;
  if (values.empty()) {
    return out;
  }

  out.n = values.size();
  const auto minmax = std::minmax_element(values.begin(), values.end());
  out.min = *minmax.first;
  out.max = *minmax.second;
  out.p50 = Quantile(values, 0.50);
  out.p95 = Quantile(values, 0.95);
  out.p99 = Quantile(values, 0.99);
  out.avg = std::accumulate(values.begin(), values.end(), 0.0) /
            static_cast<double>(values.size());
  return out;
}

std::string FormatStat(const std::string& name, const StatSummary& s) {
  std::ostringstream oss;
  oss << name << ": ";
  if (s.n == 0) {
    oss << "n/a\n";
    return oss.str();
  }
  oss << std::fixed << std::setprecision(2) << "min=" << s.min
      << " p50=" << s.p50 << " p95=" << s.p95 << " p99=" << s.p99
      << " avg=" << s.avg << " max=" << s.max << " n=" << s.n << "\n";
  return oss.str();
}

std::string JsonEscape(const std::string& value) {
  std::string out;
  out.reserve(value.size());
  for (char c : value) {
    if (c == '"') {
      out += "\\\"";
    } else if (c == '\\') {
      out += "\\\\";
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out += c;
    }
  }
  return out;
}

class ReportFiles {
 public:
  bool Open(const std::string& reports_root, const std::string& bench_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (events_.is_open()) {
      events_.close();
    }
    if (latencies_.is_open()) {
      latencies_.close();
    }
    if (system_.is_open()) {
      system_.close();
    }

    bench_id_ = bench_id;
    root_dir_ = std::filesystem::path(reports_root) / bench_id;
    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);

    events_.open(root_dir_ / "events.jsonl", std::ios::out | std::ios::trunc);
    latencies_.open(root_dir_ / "latencies.csv", std::ios::out | std::ios::trunc);
    system_.open(root_dir_ / "system_state.csv", std::ios::out | std::ios::trunc);

    if (!events_.is_open() || !latencies_.is_open() || !system_.is_open()) {
      return false;
    }

    latencies_ << "task_id,task_class,executor_id,submit_to_start_ms,submit_to_finish_ms,"
              << "reserve_latency_ms,launch_latency_ms,run_ms,queue_wait_ms,status,"
              << "exit_code,error\n";
    system_ << "ts_ms,state_type,cpu_usage_pct,mem_available_mb,risk,cpu_pressure_some,"
            << "memory_pressure_some,io_pressure_some,pred_cpu_pct,pred_mem_mb,"
            << "mem_allocatable_mb,cpu_allocatable_millis,targets_count\n";
    return true;
  }

  void WriteEvent(const std::string& subject, const maigent::Envelope& env,
                  const std::string& details) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!events_.is_open()) {
      return;
    }
    events_ << "{\"ts_ms\":" << maigent::NowMs() << ",\"subject\":\""
            << JsonEscape(subject) << "\",\"sender\":\""
            << JsonEscape(env.header().sender_id()) << "\",\"message_kind\":"
            << env.header().message_kind() << ",\"message_category\":"
            << env.header().message_category() << ",\"request_id\":\""
            << JsonEscape(env.header().request_id()) << "\",\"trace_id\":\""
            << JsonEscape(env.header().trace_id()) << "\",\"details\":" << details
            << "}\n";
    events_.flush();
  }

  void WriteSystemRow(const std::string& row) {
    std::lock_guard<std::mutex> lock(mu_);
    if (system_.is_open()) {
      system_ << row << '\n';
      system_.flush();
    }
  }

  void WriteLatencyRow(const std::string& row) {
    std::lock_guard<std::mutex> lock(mu_);
    if (latencies_.is_open()) {
      latencies_ << row << '\n';
      latencies_.flush();
    }
  }

  void WriteSummary(const std::string& summary) {
    std::lock_guard<std::mutex> lock(mu_);
    std::ofstream out(root_dir_ / "summary.txt", std::ios::out | std::ios::trunc);
    if (out.is_open()) {
      out << summary;
    }
  }

  const std::filesystem::path& root_dir() const { return root_dir_; }

 private:
  std::mutex mu_;
  std::string bench_id_;
  std::filesystem::path root_dir_;
  std::ofstream events_;
  std::ofstream latencies_;
  std::ofstream system_;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "metrics_collector_agent";
  const std::string agent_id = "metrics-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const std::string reports_root =
      maigent::GetFlagValue(argc, argv, "--reports-root", "reports");

  maigent::AgentLogger log(agent_id, "logs/metrics_collector.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, TaskSample> tasks;
  std::unordered_map<std::string, int> per_agent_event_counts;
  std::unordered_map<std::string, int> tasks_started_by_executor;
  std::unordered_map<std::string, int> tasks_finished_by_executor;
  std::unordered_map<std::string, int> by_task_class;
  int command_messages_seen = 0;
  int event_messages_seen = 0;
  int state_messages_seen = 0;
  int service_messages_seen = 0;
  int queued_requests = 0;
  int accepted_immediately = 0;
  int accepted_from_queue = 0;
  int queue_timeouts = 0;
  int max_queue_length = 0;
  Counters counters;
  BenchSessionState bench;
  int current_concurrency = 0;
  int max_concurrency = 0;

  maigent::PressureState latest_pressure;
  maigent::ForecastState latest_forecast;
  maigent::CapacityState latest_capacity;
  maigent::TargetsState latest_targets;

  ReportFiles files;

  auto ensure_bench = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    if (!bench.bench_id.empty()) {
      return;
    }
    bench.bench_id = "manual-" + maigent::MakeUuid();
    bench.start_ts = maigent::NowMs();
    bench.running = true;
    files.Open(reports_root, bench.bench_id);
  };

  auto count_message = [&](const maigent::Envelope& env) {
    std::lock_guard<std::mutex> lock(mu);
    per_agent_event_counts[env.header().sender_id()]++;
    switch (env.header().message_category()) {
      case maigent::COMMAND:
        ++command_messages_seen;
        break;
      case maigent::EVENT:
        ++event_messages_seen;
        break;
      case maigent::STATE:
        ++state_messages_seen;
        break;
      case maigent::SERVICE:
        ++service_messages_seen;
        break;
      default:
        break;
    }
  };

  auto write_task_event_json = [&](const std::string& subject,
                                   const maigent::Envelope& env,
                                   const maigent::TaskEvent& evt) {
    ensure_bench();
    const std::string details =
        std::string("{\"task_id\":\"") + JsonEscape(evt.task_id()) +
        "\",\"event_type\":" + std::to_string(evt.event_type()) +
        ",\"executor_id\":\"" + JsonEscape(evt.executor_id()) + "\"}";
    files.WriteEvent(subject, env, details);
  };

  nats.Subscribe(maigent::kSubjectSvcBenchStart, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_service() || !env.service().has_bench_session()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::SERVICE ||
        env.header().message_kind() != maigent::MK_BENCH_START) {
      return;
    }

    const auto& b = env.service().bench_session();
    std::string bench_id_snapshot;
    {
      std::lock_guard<std::mutex> lock(mu);

      tasks.clear();
      per_agent_event_counts.clear();
      tasks_started_by_executor.clear();
      tasks_finished_by_executor.clear();
      by_task_class.clear();
      command_messages_seen = 0;
      event_messages_seen = 0;
      state_messages_seen = 0;
      service_messages_seen = 0;
      queued_requests = 0;
      accepted_immediately = 0;
      accepted_from_queue = 0;
      queue_timeouts = 0;
      max_queue_length = 0;
      counters = Counters{};
      current_concurrency = 0;
      max_concurrency = 0;

      bench.bench_id = b.bench_id().empty() ? ("bench-" + maigent::MakeUuid()) : b.bench_id();
      bench.start_ts = b.start_ts_ms() > 0 ? b.start_ts_ms() : maigent::NowMs();
      bench.stop_ts = 0;
      bench.running = true;

      counters.planned_requests = b.planned_requests();
      bench_id_snapshot = bench.bench_id;
      files.Open(reports_root, bench.bench_id);
    }

    count_message(env);
    files.WriteEvent(maigent::kSubjectSvcBenchStart, env,
                     std::string("{\"bench_id\":\"") + JsonEscape(bench_id_snapshot) +
                         "\",\"stage\":\"start\"}");
    log.Info("bench started bench_id=" + bench_id_snapshot);
  });

  nats.Subscribe(maigent::kSubjectSvcBenchStop, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_service() || !env.service().has_bench_session()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::SERVICE ||
        env.header().message_kind() != maigent::MK_BENCH_STOP) {
      return;
    }

    const auto& b = env.service().bench_session();
    std::string bench_id_snapshot;
    {
      std::lock_guard<std::mutex> lock(mu);
      if (bench.bench_id.empty()) {
        bench.bench_id = b.bench_id().empty() ? ("bench-" + maigent::MakeUuid()) : b.bench_id();
        files.Open(reports_root, bench.bench_id);
      }

      bench.stop_ts = b.stop_ts_ms() > 0 ? b.stop_ts_ms() : maigent::NowMs();
      bench.running = false;

      counters.planned_requests = std::max(counters.planned_requests, b.planned_requests());
      counters.sent_requests = std::max(counters.sent_requests, b.sent_requests());
      counters.completed = std::max(counters.completed, b.completed());
      counters.failed = std::max(counters.failed, b.failed());
      counters.denied = std::max(counters.denied, b.denied());
      counters.timeouts = std::max(counters.timeouts, b.timeouts());
      bench_id_snapshot = bench.bench_id;
    }

    count_message(env);
    files.WriteEvent(maigent::kSubjectSvcBenchStop, env,
                     std::string("{\"bench_id\":\"") + JsonEscape(bench_id_snapshot) +
                         "\",\"stage\":\"stop\"}");
  });

  nats.Subscribe(maigent::kSubjectEvtTaskSubmitted, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_SUBMITTED) {
      return;
    }

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      t.trace_id = e.trace_id();
      t.task_class = e.task_class();
      t.submit_ts = e.submitted_ts_ms() > 0 ? e.submitted_ts_ms() : e.ts_ms();
      by_task_class[t.task_class]++;
      counters.sent_requests++;
    }

    write_task_event_json(maigent::kSubjectEvtTaskSubmitted, env, e);
  });

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

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      t.trace_id = e.trace_id();
      t.executor_id = e.executor_id();
      t.task_class = e.task_class();
      t.start_ts = e.ts_ms();
      if (e.lease_decision_ts_ms() > 0) {
        t.lease_decision_ts = e.lease_decision_ts_ms();
      }
      if (!t.counted_start_acceptance) {
        if (t.queued_once) {
          accepted_from_queue++;
        } else {
          accepted_immediately++;
        }
        t.counted_start_acceptance = true;
      }
      if (t.queued_once && t.queue_enter_ts > 0) {
        const int64_t queue_end =
            (t.lease_decision_ts >= t.queue_enter_ts)
                ? t.lease_decision_ts
                : (t.start_ts >= t.queue_enter_ts ? t.start_ts : 0);
        if (queue_end > 0) {
          t.queue_wait_ms = queue_end - t.queue_enter_ts;
        }
      }
      const std::string exec_key =
          e.executor_id().empty() ? env.header().sender_id() : e.executor_id();
      tasks_started_by_executor[exec_key]++;
      ++current_concurrency;
      max_concurrency = std::max(max_concurrency, current_concurrency);
    }

    write_task_event_json(maigent::kSubjectEvtTaskStarted, env, e);
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

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      t.finish_ts = e.ts_ms();
      t.failed = false;
      t.exit_code = e.exit_code();
      const std::string exec_key =
          e.executor_id().empty() ? env.header().sender_id() : e.executor_id();
      tasks_finished_by_executor[exec_key]++;
      counters.completed++;
      current_concurrency = std::max(0, current_concurrency - 1);
    }

    write_task_event_json(maigent::kSubjectEvtTaskFinished, env, e);
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

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      t.finish_ts = e.ts_ms();
      t.failed = true;
      t.exit_code = e.exit_code();
      t.error = e.error();
      counters.failed++;
      current_concurrency = std::max(0, current_concurrency - 1);
    }

    write_task_event_json(maigent::kSubjectEvtTaskFailed, env, e);
  });

  nats.Subscribe(maigent::kSubjectEvtLeaseDenied, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_lease_decision()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_LEASE_DENIED) {
      return;
    }

    ensure_bench();
    count_message(env);

    std::lock_guard<std::mutex> lock(mu);
    const auto& d = env.event().lease_decision();
    const bool temporary_deny =
        d.reason_code() == maigent::LEASE_REASON_NO_SLOTS ||
        d.reason_code() == maigent::LEASE_REASON_NO_CAPACITY;
    if (!temporary_deny) {
      counters.denied++;
    }
    if (!d.task_id().empty()) {
      TaskSample& t = tasks[d.task_id()];
      t.task_id = d.task_id();
      if (!temporary_deny) {
        t.denied = true;
      }
    }

    files.WriteEvent(maigent::kSubjectEvtLeaseDenied, env,
                     std::string("{\"task_id\":\"") +
                         JsonEscape(env.event().lease_decision().task_id()) + "\"}");
  });

  nats.Subscribe(maigent::kSubjectEvtTaskQueued, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_QUEUED) {
      return;
    }

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      if (!e.trace_id().empty()) {
        t.trace_id = e.trace_id();
      }
      if (!e.task_class().empty()) {
        t.task_class = e.task_class();
      }

      const int64_t enter_ts = e.queue_enter_ts_ms() > 0 ? e.queue_enter_ts_ms() : e.ts_ms();
      if (!t.queued_once) {
        t.queued_once = true;
        t.queue_enter_ts = enter_ts;
        queued_requests++;
      } else if (t.queue_enter_ts == 0) {
        t.queue_enter_ts = enter_ts;
      }

      if (e.queue_length() > max_queue_length) {
        max_queue_length = e.queue_length();
      }
    }

    write_task_event_json(maigent::kSubjectEvtTaskQueued, env, e);
  });

  nats.Subscribe(maigent::kSubjectEvtTaskDequeued, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_DEQUEUED) {
      return;
    }

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      if (e.queue_length() > max_queue_length) {
        max_queue_length = e.queue_length();
      }
    }

    write_task_event_json(maigent::kSubjectEvtTaskDequeued, env, e);
  });

  nats.Subscribe(maigent::kSubjectEvtTaskQueueTimeout, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_task_event()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_TASK_QUEUE_TIMEOUT) {
      return;
    }

    ensure_bench();
    count_message(env);

    const auto& e = env.event().task_event();
    {
      std::lock_guard<std::mutex> lock(mu);
      TaskSample& t = tasks[e.task_id()];
      t.task_id = e.task_id();
      if (!t.queued_once) {
        t.queued_once = true;
        queued_requests++;
      }
      const int64_t enter_ts = e.queue_enter_ts_ms() > 0 ? e.queue_enter_ts_ms() : t.queue_enter_ts;
      if (t.queue_enter_ts == 0 && enter_ts > 0) {
        t.queue_enter_ts = enter_ts;
      }
      t.queue_timeout_ts = e.ts_ms();
      if (t.queue_enter_ts > 0 && t.queue_timeout_ts >= t.queue_enter_ts) {
        t.queue_wait_ms = t.queue_timeout_ts - t.queue_enter_ts;
      }
      if (!t.counted_queue_timeout) {
        t.counted_queue_timeout = true;
        queue_timeouts++;
        counters.timeouts++;
      }
      if (e.queue_length() > max_queue_length) {
        max_queue_length = e.queue_length();
      }
    }

    write_task_event_json(maigent::kSubjectEvtTaskQueueTimeout, env, e);
  });

  nats.Subscribe(maigent::kSubjectEvtControlApplied, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_actuator_result()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_CONTROL_APPLIED) {
      return;
    }
    ensure_bench();
    count_message(env);
    std::lock_guard<std::mutex> lock(mu);
    counters.control_applied++;
    files.WriteEvent(maigent::kSubjectEvtControlApplied, env,
                     "{\"type\":\"control_applied\"}");
  });

  nats.Subscribe(maigent::kSubjectEvtControlFailed, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_actuator_result()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_CONTROL_FAILED) {
      return;
    }
    ensure_bench();
    count_message(env);
    std::lock_guard<std::mutex> lock(mu);
    counters.control_failed++;
    files.WriteEvent(maigent::kSubjectEvtControlFailed, env,
                     "{\"type\":\"control_failed\"}");
  });

  nats.Subscribe(maigent::kSubjectEvtActuatorApplied, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_actuator_result()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_ACTUATOR_APPLIED) {
      return;
    }
    ensure_bench();
    count_message(env);
    std::lock_guard<std::mutex> lock(mu);
    counters.actuator_applied++;
    files.WriteEvent(maigent::kSubjectEvtActuatorApplied, env,
                     "{\"type\":\"actuator_applied\"}");
  });

  nats.Subscribe(maigent::kSubjectEvtActuatorFailed, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_event() || !env.event().has_actuator_result()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::EVENT ||
        env.header().message_kind() != maigent::MK_ACTUATOR_FAILED) {
      return;
    }
    ensure_bench();
    count_message(env);
    std::lock_guard<std::mutex> lock(mu);
    counters.actuator_failed++;
    files.WriteEvent(maigent::kSubjectEvtActuatorFailed, env,
                     "{\"type\":\"actuator_failed\"}");
  });

  auto on_runtime_control_command = [&](const maigent::NatsMessage& msg,
                                        maigent::MessageKind expected_kind,
                                        const char* route_name) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_command() || !env.command().has_control_action()) {
      return;
    }
    if (!env.has_header() || env.header().message_category() != maigent::COMMAND ||
        env.header().message_kind() != expected_kind) {
      return;
    }

    ensure_bench();
    count_message(env);

    const auto& action = env.command().control_action();
    {
      std::lock_guard<std::mutex> lock(mu);
      counters.runtime_actions_emitted++;
      if (expected_kind == maigent::MK_TASK_CONTROL) {
        counters.runtime_actions_emitted_task_executor++;
      } else {
        counters.runtime_actions_emitted_actuator++;
      }
    }

    files.WriteEvent(
        msg.subject, env,
        std::string("{\"type\":\"runtime_action_emitted\",\"route\":\"") +
            route_name + "\",\"action_type\":" +
            std::to_string(action.action_type()) + ",\"target_id\":\"" +
            JsonEscape(action.target_id()) + "\",\"task_id\":\"" +
            JsonEscape(action.task_id()) + "\",\"executor_id\":\"" +
            JsonEscape(action.executor_id()) + "\"}");
  };

  nats.Subscribe(maigent::kSubjectCmdActuatorApply,
                 [&](const maigent::NatsMessage& msg) {
                   on_runtime_control_command(msg, maigent::MK_ACTUATOR_APPLY,
                                              "actuator");
                 });

  nats.Subscribe(kSubjectCmdTaskExecControlAll,
                 [&](const maigent::NatsMessage& msg) {
                   on_runtime_control_command(msg, maigent::MK_TASK_CONTROL,
                                              "task_executor");
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

    ensure_bench();
    count_message(env);
    {
      std::lock_guard<std::mutex> lock(mu);
      latest_pressure = env.state().pressure_state();
      std::ostringstream row;
      row << latest_pressure.ts_ms() << ",pressure,"
          << latest_pressure.cpu_usage_pct() << ","
          << latest_pressure.mem_available_mb() << ","
          << latest_pressure.risk_level() << ","
          << latest_pressure.cpu_pressure_some() << ","
          << latest_pressure.memory_pressure_some() << ","
          << latest_pressure.io_pressure_some() << ",,,," << latest_targets.targets_size();
      files.WriteSystemRow(row.str());
    }
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

    ensure_bench();
    count_message(env);
    {
      std::lock_guard<std::mutex> lock(mu);
      latest_forecast = env.state().forecast_state();
      std::ostringstream row;
      row << latest_forecast.ts_ms() << ",forecast,,,,,,,,"
          << latest_forecast.predicted_cpu_usage_pct() << ","
          << latest_forecast.predicted_mem_available_mb() << ",,,"
          << latest_targets.targets_size();
      files.WriteSystemRow(row.str());
    }
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

    ensure_bench();
    count_message(env);
    {
      std::lock_guard<std::mutex> lock(mu);
      latest_capacity = env.state().capacity_state();
      std::ostringstream row;
      row << latest_capacity.ts_ms() << ",capacity,,,,,,,,,,"
          << latest_capacity.mem_allocatable_mb() << ","
          << latest_capacity.cpu_millis_allocatable() << ","
          << latest_targets.targets_size();
      files.WriteSystemRow(row.str());
    }
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

    ensure_bench();
    count_message(env);
    {
      std::lock_guard<std::mutex> lock(mu);
      latest_targets = env.state().targets_state();
      std::ostringstream row;
      row << latest_targets.ts_ms() << ",targets,,,,,,,,,,,,"
          << latest_targets.targets_size();
      files.WriteSystemRow(row.str());
    }
  });

  auto make_report = [&]() {
    std::unordered_map<std::string, TaskSample> tasks_snapshot;
    std::unordered_map<std::string, int> per_agent_events;
    std::unordered_map<std::string, int> started_by_executor;
    std::unordered_map<std::string, int> finished_by_executor;
    std::unordered_map<std::string, int> by_class;
    int command_seen = 0;
    int event_seen = 0;
    int state_seen = 0;
    int service_seen = 0;
    int queued_reqs = 0;
    int accepted_now = 0;
    int accepted_later = 0;
    int queued_timeouts = 0;
    int max_queue_len = 0;
    Counters cnt;
    BenchSessionState b;
    int max_cc = 0;

    {
      std::lock_guard<std::mutex> lock(mu);
      tasks_snapshot = tasks;
      per_agent_events = per_agent_event_counts;
      started_by_executor = tasks_started_by_executor;
      finished_by_executor = tasks_finished_by_executor;
      by_class = by_task_class;
      command_seen = command_messages_seen;
      event_seen = event_messages_seen;
      state_seen = state_messages_seen;
      service_seen = service_messages_seen;
      queued_reqs = queued_requests;
      accepted_now = accepted_immediately;
      accepted_later = accepted_from_queue;
      queued_timeouts = queue_timeouts;
      max_queue_len = max_queue_length;
      cnt = counters;
      b = bench;
      max_cc = max_concurrency;
    }

    if (b.start_ts == 0) {
      b.start_ts = maigent::NowMs();
    }
    if (b.stop_ts == 0) {
      b.stop_ts = maigent::NowMs();
    }

    std::vector<double> submit_to_start;
    std::vector<double> submit_to_finish;
    std::vector<double> reserve_latency;
    std::vector<double> launch_latency;
    std::vector<double> run_ms;
    std::vector<double> queue_wait_ms;
    std::vector<double> submit_to_start_immediate;
    std::vector<double> submit_to_start_queued;
    std::vector<double> end_to_end_immediate;
    std::vector<double> end_to_end_queued;
    std::vector<double> queue_wait_queued;

    struct SlowTask {
      std::string task_id;
      double end_to_end_ms = -1.0;
      double queue_wait_ms = -1.0;
      double reserve_latency_ms = -1.0;
      double launch_latency_ms = -1.0;
      double run_ms = -1.0;
      bool failed = false;
      bool queued = false;
    };
    std::vector<SlowTask> slowest;

    int completed_non_failed = 0;

    for (const auto& [task_id, t] : tasks_snapshot) {
      const double s2st =
          (t.submit_ts > 0 && t.start_ts >= t.submit_ts) ? (t.start_ts - t.submit_ts) : -1.0;
      const double s2f =
          (t.submit_ts > 0 && t.finish_ts >= t.submit_ts) ? (t.finish_ts - t.submit_ts) : -1.0;
      const double rsv =
          (t.submit_ts > 0 && t.lease_decision_ts >= t.submit_ts)
              ? (t.lease_decision_ts - t.submit_ts)
              : -1.0;
      const double lch =
          (t.lease_decision_ts > 0 && t.start_ts >= t.lease_decision_ts)
              ? (t.start_ts - t.lease_decision_ts)
              : -1.0;
      const double run =
          (t.start_ts > 0 && t.finish_ts >= t.start_ts) ? (t.finish_ts - t.start_ts) : -1.0;
      double qwait = (t.queue_wait_ms >= 0) ? static_cast<double>(t.queue_wait_ms) : -1.0;
      if (qwait < 0 && t.queued_once && t.queue_enter_ts > 0) {
        int64_t queue_end = 0;
        if (t.lease_decision_ts >= t.queue_enter_ts) {
          queue_end = t.lease_decision_ts;
        } else if (t.start_ts >= t.queue_enter_ts) {
          queue_end = t.start_ts;
        } else if (t.queue_timeout_ts >= t.queue_enter_ts) {
          queue_end = t.queue_timeout_ts;
        }
        if (queue_end > 0) {
          qwait = static_cast<double>(queue_end - t.queue_enter_ts);
        }
      }

      if (s2st >= 0) {
        submit_to_start.push_back(s2st);
        if (t.queued_once) {
          submit_to_start_queued.push_back(s2st);
        } else {
          submit_to_start_immediate.push_back(s2st);
        }
      }
      if (s2f >= 0) {
        submit_to_finish.push_back(s2f);
        if (t.queued_once) {
          end_to_end_queued.push_back(s2f);
        } else {
          end_to_end_immediate.push_back(s2f);
        }
        slowest.push_back({task_id, s2f, qwait, rsv, lch, run, t.failed, t.queued_once});
      }
      if (rsv >= 0) {
        reserve_latency.push_back(rsv);
      }
      if (lch >= 0) {
        launch_latency.push_back(lch);
      }
      if (run >= 0) {
        run_ms.push_back(run);
      }
      if (qwait >= 0) {
        queue_wait_ms.push_back(qwait);
        if (t.queued_once) {
          queue_wait_queued.push_back(qwait);
        }
      }
      if (!t.failed && t.finish_ts > 0) {
        completed_non_failed++;
      }

      std::ostringstream row;
      row << task_id << "," << t.task_class << "," << t.executor_id << ","
          << (s2st >= 0 ? std::to_string(s2st) : "") << ","
          << (s2f >= 0 ? std::to_string(s2f) : "") << ","
          << (rsv >= 0 ? std::to_string(rsv) : "") << ","
          << (lch >= 0 ? std::to_string(lch) : "") << ","
          << (run >= 0 ? std::to_string(run) : "") << ","
          << (qwait >= 0 ? std::to_string(qwait) : "") << ","
          << (t.failed ? "failed" : "finished") << "," << t.exit_code << ","
          << JsonEscape(t.error);
      files.WriteLatencyRow(row.str());
    }

    std::sort(slowest.begin(), slowest.end(),
              [](const SlowTask& a, const SlowTask& b) {
                return a.end_to_end_ms > b.end_to_end_ms;
              });

    const StatSummary s_submit_to_start = BuildSummary(submit_to_start);
    const StatSummary s_end_to_end = BuildSummary(submit_to_finish);
    const StatSummary s_reserve = BuildSummary(reserve_latency);
    const StatSummary s_launch = BuildSummary(launch_latency);
    const StatSummary s_run = BuildSummary(run_ms);
    const StatSummary s_queue_wait = BuildSummary(queue_wait_ms);
    const StatSummary s_submit_to_start_immediate =
        BuildSummary(submit_to_start_immediate);
    const StatSummary s_submit_to_start_queued = BuildSummary(submit_to_start_queued);
    const StatSummary s_end_to_end_immediate = BuildSummary(end_to_end_immediate);
    const StatSummary s_end_to_end_queued = BuildSummary(end_to_end_queued);
    const StatSummary s_queue_wait_queued = BuildSummary(queue_wait_queued);

    const double wall_time_sec =
        (b.stop_ts > b.start_ts) ? static_cast<double>(b.stop_ts - b.start_ts) / 1000.0 : 0.0;
    const double throughput = wall_time_sec > 0.0
                                  ? static_cast<double>(completed_non_failed) / wall_time_sec
                                  : 0.0;
    const int runtime_actions_with_outcome =
        cnt.control_applied + cnt.control_failed + cnt.actuator_applied +
        cnt.actuator_failed;
    const int runtime_actions_missing_outcome =
        std::max(0, cnt.runtime_actions_emitted - runtime_actions_with_outcome);

    std::ostringstream report;
    report << "=============== MetricsCollector Summary ===============\n";
    report << "bench_id: " << b.bench_id << "\n";
    report << "planned_requests: " << cnt.planned_requests << "\n";
    report << "sent_requests: " << cnt.sent_requests << "\n";
    report << "completed: " << cnt.completed << "\n";
    report << "failed: " << cnt.failed << "\n";
    report << "denied: " << cnt.denied << "\n";
    report << "timeouts: " << cnt.timeouts << "\n";
    report << "queued_requests: " << queued_reqs << "\n";
    report << "accepted_immediately: " << accepted_now << "\n";
    report << "accepted_from_queue: " << accepted_later << "\n";
    report << "queue_timeouts: " << queued_timeouts << "\n";
    report << "max_queue_length: " << max_queue_len << "\n";
    report << std::fixed << std::setprecision(3)
           << "wall_time_sec: " << wall_time_sec << "\n";
    report << std::fixed << std::setprecision(3)
           << "throughput_tasks_per_sec: " << throughput << "\n";
    report << "max_concurrency: " << max_cc << "\n";
    report << "runtime_actions_emitted: " << cnt.runtime_actions_emitted << "\n";
    report << "runtime_actions_emitted_task_executor: "
           << cnt.runtime_actions_emitted_task_executor << "\n";
    report << "runtime_actions_emitted_actuator: "
           << cnt.runtime_actions_emitted_actuator << "\n";
    report << "runtime_actions_with_outcome: " << runtime_actions_with_outcome
           << "\n";
    report << "runtime_actions_missing_outcome: " << runtime_actions_missing_outcome
           << "\n";
    report << "control_actions_applied: " << cnt.control_applied << "\n";
    report << "control_actions_failed: " << cnt.control_failed << "\n";
    report << "actuator_actions_applied: " << cnt.actuator_applied << "\n";
    report << "actuator_actions_failed: " << cnt.actuator_failed << "\n";
    report << "command_messages_seen: " << command_seen << "\n";
    report << "event_messages_seen: " << event_seen << "\n";
    report << "state_messages_seen: " << state_seen << "\n";
    report << "service_messages_seen: " << service_seen << "\n";

    report << "system_latency_ms:\n";
    report << FormatStat("submit_to_start_ms", s_submit_to_start);
    report << FormatStat("end_to_end_ms", s_end_to_end);
    report << FormatStat("reserve_latency_ms", s_reserve);
    report << FormatStat("queue_wait_ms", s_queue_wait);

    report << "execution_latency_ms:\n";
    report << FormatStat("launch_latency_ms", s_launch);
    report << FormatStat("run_ms", s_run);

    report << "immediate_path_latency_ms:\n";
    report << FormatStat("submit_to_start_ms", s_submit_to_start_immediate);
    report << FormatStat("end_to_end_ms", s_end_to_end_immediate);

    report << "queued_path_latency_ms:\n";
    report << FormatStat("submit_to_start_ms", s_submit_to_start_queued);
    report << FormatStat("end_to_end_ms", s_end_to_end_queued);
    report << FormatStat("queue_wait_ms", s_queue_wait_queued);

    report << "counts_by_task_class:\n";
    for (const auto& [task_class, count] : by_class) {
      report << "  " << task_class << ": " << count << "\n";
    }

    report << "per_agent_event_counts:\n";
    for (const auto& [agent, count] : per_agent_events) {
      report << "  " << agent << ": " << count << "\n";
    }

    report << "tasks_started_by_executor:\n";
    for (const auto& [executor, count] : started_by_executor) {
      report << "  " << executor << ": " << count << "\n";
    }

    report << "tasks_finished_by_executor:\n";
    for (const auto& [executor, count] : finished_by_executor) {
      report << "  " << executor << ": " << count << "\n";
    }

    report << "slowest_top_10:\n";
    auto format_ms = [](double value) -> std::string {
      if (value < 0.0) {
        return "n/a";
      }
      std::ostringstream ms;
      ms << std::fixed << std::setprecision(2) << value;
      return ms.str();
    };
    for (size_t i = 0; i < slowest.size() && i < 10; ++i) {
      report << "  " << (i + 1) << ". " << slowest[i].task_id
             << " end_to_end_ms=" << format_ms(slowest[i].end_to_end_ms)
             << " queue_wait_ms=" << format_ms(slowest[i].queue_wait_ms)
             << " reserve_latency_ms="
             << format_ms(slowest[i].reserve_latency_ms)
             << " launch_latency_ms="
             << format_ms(slowest[i].launch_latency_ms)
             << " run_ms=" << format_ms(slowest[i].run_ms)
             << " status=" << (slowest[i].failed ? "failed" : "finished")
             << " route=" << (slowest[i].queued ? "queued" : "immediate")
             << "\n";
    }
    report << "========================================================\n";

    files.WriteSummary(report.str());
    std::cout << report.str();
  };

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"metrics.collect", "metrics.report"},
                                    []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  make_report();
  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
