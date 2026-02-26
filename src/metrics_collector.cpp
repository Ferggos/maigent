#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
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
#include "maigent/agent_lifecycle.h"
#include "maigent/constants.h"
#include "maigent/logging.h"
#include "maigent/message_utils.h"
#include "maigent/nats_wrapper.h"
#include "maigent/utils.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct TaskSample {
  int64_t submit_ts = 0;
  int64_t lease_decision_ts = 0;
  int64_t start_ts = 0;
  int64_t finish_ts = 0;
  bool failed = false;
  int exit_code = 0;
  std::string error;
};

struct Counters {
  int accepted = 0;
  int denied = 0;
  int failed = 0;
  int timeout = 0;
  int completed = 0;
  int control_applied = 0;
};

struct BenchState {
  std::string bench_id;
  int planned = 0;
  int64_t start_ts = 0;
  int64_t stop_ts = 0;
  int sent = 0;
  int completed = 0;
  int timeouts = 0;
  bool running = false;
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

std::string Describe(const std::string& name, const std::vector<double>& values) {
  std::ostringstream oss;
  oss << name << ": ";
  if (values.empty()) {
    oss << "n/a\n";
    return oss.str();
  }

  const double sum = std::accumulate(values.begin(), values.end(), 0.0);
  const auto minmax = std::minmax_element(values.begin(), values.end());
  oss << std::fixed << std::setprecision(2)
      << "min=" << *minmax.first << " p50=" << Quantile(values, 0.50)
      << " p95=" << Quantile(values, 0.95) << " p99=" << Quantile(values, 0.99)
      << " avg=" << (sum / static_cast<double>(values.size()))
      << " max=" << *minmax.second << "\n";
  return oss.str();
}

std::string BuildReport(const std::unordered_map<std::string, TaskSample>& tasks,
                        const Counters& counters, const BenchState& bench) {
  std::vector<double> submit_to_start;
  std::vector<double> submit_to_finish;
  std::vector<double> run_ms;
  std::vector<double> reserve_latency;
  std::vector<double> launch_latency;

  struct SlowRow {
    std::string task_id;
    double total_ms;
  };
  std::vector<SlowRow> slowest;

  int64_t first_submit = std::numeric_limits<int64_t>::max();
  int64_t last_finish = 0;
  int completed = 0;

  for (const auto& [task_id, t] : tasks) {
    if (t.submit_ts > 0) {
      first_submit = std::min(first_submit, t.submit_ts);
    }
    if (t.finish_ts > 0) {
      last_finish = std::max(last_finish, t.finish_ts);
    }

    if (t.submit_ts > 0 && t.start_ts > 0 && t.start_ts >= t.submit_ts) {
      submit_to_start.push_back(static_cast<double>(t.start_ts - t.submit_ts));
    }
    if (t.submit_ts > 0 && t.finish_ts > 0 && t.finish_ts >= t.submit_ts) {
      const double total = static_cast<double>(t.finish_ts - t.submit_ts);
      submit_to_finish.push_back(total);
      slowest.push_back({task_id, total});
    }
    if (t.start_ts > 0 && t.finish_ts > 0 && t.finish_ts >= t.start_ts) {
      run_ms.push_back(static_cast<double>(t.finish_ts - t.start_ts));
    }
    if (t.submit_ts > 0 && t.lease_decision_ts > 0 && t.lease_decision_ts >= t.submit_ts) {
      reserve_latency.push_back(static_cast<double>(t.lease_decision_ts - t.submit_ts));
    }
    if (t.lease_decision_ts > 0 && t.start_ts > 0 && t.start_ts >= t.lease_decision_ts) {
      launch_latency.push_back(static_cast<double>(t.start_ts - t.lease_decision_ts));
    }
    if (!t.failed && t.finish_ts > 0) {
      ++completed;
    }
  }

  int64_t wall_start = bench.start_ts;
  int64_t wall_stop = bench.stop_ts;
  if (wall_start <= 0) {
    wall_start = (first_submit == std::numeric_limits<int64_t>::max()) ? 0 : first_submit;
  }
  if (wall_stop <= 0) {
    wall_stop = last_finish;
  }

  double throughput = 0.0;
  if (wall_start > 0 && wall_stop > wall_start) {
    throughput = static_cast<double>(completed) /
                 (static_cast<double>(wall_stop - wall_start) / 1000.0);
  }

  std::sort(slowest.begin(), slowest.end(),
            [](const SlowRow& a, const SlowRow& b) { return a.total_ms > b.total_ms; });

  std::ostringstream oss;
  oss << "================= MetricsCollector Report =================\n";
  oss << "bench_id: " << (bench.bench_id.empty() ? "n/a" : bench.bench_id) << "\n";
  oss << "wall_time_ms: " << ((wall_stop > wall_start) ? (wall_stop - wall_start) : 0)
      << "\n";
  oss << std::fixed << std::setprecision(2)
      << "throughput tasks/sec: " << throughput << "\n";

  oss << "counts: accepted=" << counters.accepted << " denied=" << counters.denied
      << " failed=" << counters.failed << " timeout=" << counters.timeout
      << " completed=" << counters.completed
      << " control_applied=" << counters.control_applied << "\n";
  if (bench.stop_ts > 0) {
    oss << "bench_summary: planned=" << bench.planned << " sent=" << bench.sent
        << " completed=" << bench.completed << " timeouts=" << bench.timeouts << "\n";
  }

  oss << Describe("latency_submit_to_start_ms", submit_to_start);
  oss << Describe("latency_submit_to_finish_ms", submit_to_finish);
  oss << Describe("stage.reserve_latency_ms", reserve_latency);
  oss << Describe("stage.launch_latency_ms", launch_latency);
  oss << Describe("run_ms", run_ms);

  oss << "slowest_top10:\n";
  for (size_t i = 0; i < slowest.size() && i < 10; ++i) {
    oss << "  " << (i + 1) << ". " << slowest[i].task_id << " total_ms="
        << std::fixed << std::setprecision(2) << slowest[i].total_ms << "\n";
  }
  oss << "===========================================================\n";

  return oss.str();
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "metrics_collector";
  const std::string agent_id = "metrics-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const std::string report_file =
      maigent::GetFlagValue(argc, argv, "--report-file", "derived/metrics_report.txt");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::unordered_map<std::string, TaskSample> tasks;
  Counters counters;
  BenchState bench;

  auto ensure_task = [&](const std::string& task_id) -> TaskSample& {
    return tasks[task_id];
  };

  nats.Subscribe(maigent::kSubjectEvtTaskSubmitted, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }
    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    TaskSample& t = ensure_task(e.task_id());
    t.submit_ts = e.submitted_ts_ms() > 0 ? e.submitted_ts_ms() : e.ts_ms();
  });

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }
    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    TaskSample& t = ensure_task(e.task_id());
    if (t.start_ts == 0) {
      ++counters.accepted;
    }
    t.start_ts = e.ts_ms();
    if (e.lease_decision_ts_ms() > 0) {
      t.lease_decision_ts = e.lease_decision_ts_ms();
    }
  });

  nats.Subscribe(maigent::kSubjectEvtTaskFinished, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }
    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    TaskSample& t = ensure_task(e.task_id());
    if (t.finish_ts == 0) {
      ++counters.completed;
    }
    t.finish_ts = e.ts_ms();
    t.exit_code = e.exit_code();
    t.failed = false;
  });

  nats.Subscribe(maigent::kSubjectEvtTaskFailed, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }
    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    TaskSample& t = ensure_task(e.task_id());
    if (t.finish_ts == 0) {
      ++counters.failed;
    }
    t.finish_ts = e.ts_ms();
    t.exit_code = e.exit_code();
    t.failed = true;
    t.error = e.error();
  });

  nats.Subscribe(maigent::kSubjectEvtLeaseDenied, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_lease_decision()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    ++counters.denied;
  });

  nats.Subscribe(maigent::kSubjectEvtControlApplied, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_control_applied()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    ++counters.control_applied;
  });

  nats.Subscribe(maigent::kSubjectEvtBenchStart, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_bench_start()) {
      return;
    }
    const auto& b = env.bench_start();
    std::lock_guard<std::mutex> lock(mu);
    tasks.clear();
    counters = Counters{};
    bench = BenchState{};
    bench.bench_id = b.bench_id();
    bench.planned = b.planned();
    bench.start_ts = b.start_ts_ms();
    bench.running = true;
  });

  auto print_report = [&]() {
    std::unordered_map<std::string, TaskSample> snapshot_tasks;
    Counters snapshot_counts;
    BenchState snapshot_bench;

    {
      std::lock_guard<std::mutex> lock(mu);
      snapshot_tasks = tasks;
      snapshot_counts = counters;
      snapshot_bench = bench;
      snapshot_counts.timeout = std::max(snapshot_counts.timeout, bench.timeouts);
    }

    const std::string report = BuildReport(snapshot_tasks, snapshot_counts, snapshot_bench);
    std::cout << report;

    std::ofstream out(report_file, std::ios::out | std::ios::trunc);
    if (out.is_open()) {
      out << report;
    }
  };

  nats.Subscribe(maigent::kSubjectEvtBenchStop, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_bench_stop()) {
      return;
    }
    const auto& b = env.bench_stop();
    {
      std::lock_guard<std::mutex> lock(mu);
      bench.bench_id = b.bench_id();
      bench.planned = b.planned();
      bench.start_ts = b.start_ts_ms();
      bench.stop_ts = b.stop_ts_ms();
      bench.sent = b.sent();
      bench.completed = b.completed();
      bench.timeouts = b.timeouts();
      bench.running = false;
      counters.timeout = std::max(counters.timeout, b.timeouts());
    }
    print_report();
  });

  auto inflight_fn = [&]() { return 0; };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"metrics.events", "metrics.report", "bench.boundaries"}, inflight_fn);
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  print_report();
  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
