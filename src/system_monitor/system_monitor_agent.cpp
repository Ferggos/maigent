#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <mutex>
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
#include "maigent/system_monitor/system_monitor_model.h"
#include "state_collector.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

struct ManagedTaskRef {
  std::string task_id;
  std::string executor_id;
  int pid = 0;
  std::string cgroup_path;
  std::string task_class;
  int priority = 0;
  int64_t started_ms = 0;
};

double ParsePressureAvg10(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0.0;
  }
  std::string line;
  while (std::getline(in, line)) {
    if (line.rfind("some", 0) != 0) {
      continue;
    }
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      if (token.rfind("avg10=", 0) == 0) {
        try {
          return std::stod(token.substr(6));
        } catch (...) {
          return 0.0;
        }
      }
    }
  }
  return 0.0;
}

int64_t ReadIntFile(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }
  int64_t value = 0;
  in >> value;
  return value;
}

int64_t ReadVmRssMb(int pid) {
  if (pid <= 0) {
    return 0;
  }

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

double ReadCpuStatUsageHint(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0.0;
  }

  std::string key;
  int64_t value = 0;
  while (in >> key >> value) {
    if (key == "usage_usec") {
      return static_cast<double>(value) / 1000000.0;
    }
    std::string rest_of_line;
    std::getline(in, rest_of_line);
  }
  return 0.0;
}

int64_t ReadMemoryEventsTotal(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }

  std::string key;
  int64_t value = 0;
  int64_t total = 0;
  while (in >> key >> value) {
    total += value;
  }
  return total;
}

int64_t ReadIoStatLines(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return 0;
  }
  int64_t lines = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) {
      ++lines;
    }
  }
  return lines;
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

  maigent::AgentLogger log(agent_id, "logs/system_monitor.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  maigent::StateCollector state_collector;
  maigent::HeuristicSystemMonitorModel model;

  std::mutex mu;
  std::unordered_map<std::string, ManagedTaskRef> managed_tasks;
  std::vector<maigent::SystemMonitorPressureHistorySample> pressure_history;
  pressure_history.reserve(64);

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

    const auto& evt = env.event().task_event();
    std::lock_guard<std::mutex> lock(mu);
    if (evt.event_type() == maigent::TASK_STARTED) {
      ManagedTaskRef ref;
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

  auto build_targets = [&]() {
    std::vector<maigent::SystemMonitorTargetInput> targets;
    std::vector<ManagedTaskRef> local_tasks;
    {
      std::lock_guard<std::mutex> lock(mu);
      local_tasks.reserve(managed_tasks.size());
      for (const auto& [task_id, ref] : managed_tasks) {
        local_tasks.push_back(ref);
      }
    }

    targets.reserve(local_tasks.size() + 2);
    for (const auto& task : local_tasks) {
      maigent::SystemMonitorTargetInput t;
      t.target_id = "managed:" + task.task_id;
      t.source_type = maigent::MANAGED_TASK;
      t.owner_executor_id = task.executor_id;
      t.task_id = task.task_id;
      t.pid = task.pid;
      t.cgroup_path = task.cgroup_path;
      t.task_class = task.task_class;
      t.priority = task.priority;

      const int64_t rss_mb = ReadVmRssMb(task.pid);
      t.memory_current_mb = static_cast<double>(rss_mb);

      if (!task.cgroup_path.empty()) {
        const std::filesystem::path cg = std::filesystem::path(cgroup_root) / task.cgroup_path;
        t.cpu_usage = ReadCpuStatUsageHint(cg / "cpu.stat");
        const int64_t mem_events = ReadMemoryEventsTotal(cg / "memory.events");
        const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
        t.cpu_pressure = ParsePressureAvg10(cg / "cpu.pressure");
        t.memory_pressure = ParsePressureAvg10(cg / "memory.pressure");
        t.io_pressure = ParsePressureAvg10(cg / "io.pressure");
        if (mem_events > 0) {
          t.memory_pressure = std::max(t.memory_pressure, 0.1);
        }
        if (io_lines > 0) {
          t.io_pressure = std::max(t.io_pressure, 0.1);
        }
      }
      targets.push_back(std::move(t));
    }

    {
      maigent::SystemMonitorTargetInput ext_group;
      ext_group.target_id = "external_group:system.slice";
      ext_group.source_type = maigent::EXTERNAL_GROUP;
      ext_group.cgroup_path = "system.slice";
      ext_group.task_class = "external";
      ext_group.priority = 0;
      const std::filesystem::path cg = std::filesystem::path(cgroup_root) / "system.slice";
      ext_group.memory_current_mb =
          static_cast<double>(ReadIntFile(cg / "memory.current")) / (1024.0 * 1024.0);
      ext_group.cpu_usage = ReadCpuStatUsageHint(cg / "cpu.stat");
      const int64_t mem_events = ReadMemoryEventsTotal(cg / "memory.events");
      const int64_t io_lines = ReadIoStatLines(cg / "io.stat");
      ext_group.cpu_pressure = ParsePressureAvg10(cg / "cpu.pressure");
      ext_group.memory_pressure = ParsePressureAvg10(cg / "memory.pressure");
      ext_group.io_pressure = ParsePressureAvg10(cg / "io.pressure");
      if (mem_events > 0) {
        ext_group.memory_pressure = std::max(ext_group.memory_pressure, 0.1);
      }
      if (io_lines > 0) {
        ext_group.io_pressure = std::max(ext_group.io_pressure, 0.1);
      }
      targets.push_back(std::move(ext_group));
    }

    {
      maigent::SystemMonitorTargetInput sys_target;
      sys_target.target_id = "system_service:pid1";
      sys_target.source_type = maigent::SYSTEM_SERVICE;
      sys_target.pid = 1;
      sys_target.task_class = "system";
      sys_target.priority = 100;
      sys_target.memory_current_mb = static_cast<double>(ReadVmRssMb(1));
      targets.push_back(std::move(sys_target));
    }

    return targets;
  };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"state.pressure", "state.forecast", "state.capacity", "state.targets"},
      []() { return 0; });
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    maigent::RawSystemState raw;
    state_collector.Sample(&raw);

    maigent::SystemMonitorModelInput model_input;
    model_input.host.ts_ms = raw.ts_ms;
    model_input.host.cpu_usage_pct = raw.cpu_usage_pct;
    model_input.host.mem_total_mb = raw.mem_total_mb;
    model_input.host.mem_available_mb = raw.mem_available_mb;
    model_input.host.load1 = raw.load1;
    model_input.host.psi_cpu_some = raw.psi_cpu_some;
    model_input.host.psi_mem_some = raw.psi_mem_some;
    model_input.host.psi_io_some = raw.psi_io_some;
    model_input.targets = build_targets();

    {
      std::lock_guard<std::mutex> lock(mu);
      model_input.pressure_history = pressure_history;
    }

    const maigent::SystemMonitorModelOutput model_output = model.Evaluate(model_input);

    {
      std::lock_guard<std::mutex> lock(mu);
      pressure_history.push_back(maigent::ToPressureHistorySample(model_output.pressure));
      if (pressure_history.size() > 128) {
        pressure_history.erase(pressure_history.begin());
      }
    }

    const maigent::PressureState pressure = maigent::ToProtoPressureState(model_output.pressure);
    const maigent::ForecastState forecast = maigent::ToProtoForecastState(model_output.forecast);
    const maigent::CapacityState capacity = maigent::ToProtoCapacityState(model_output.capacity);
    const maigent::TargetsState targets = maigent::ToProtoTargetsState(model_output);

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_PRESSURE_STATE,
                          role, agent_id);
      *env.mutable_state()->mutable_pressure_state() = pressure;
      nats.PublishEnvelope(maigent::kSubjectStatePressure, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_FORECAST_STATE,
                          role, agent_id);
      *env.mutable_state()->mutable_forecast_state() = forecast;
      nats.PublishEnvelope(maigent::kSubjectStateForecast, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_CAPACITY_STATE,
                          role, agent_id);
      *env.mutable_state()->mutable_capacity_state() = capacity;
      nats.PublishEnvelope(maigent::kSubjectStateCapacity, env);
    }

    {
      maigent::Envelope env;
      maigent::FillHeader(&env, maigent::STATE, maigent::MK_TARGETS_STATE,
                          role, agent_id);
      *env.mutable_state()->mutable_targets_state() = targets;
      nats.PublishEnvelope(maigent::kSubjectStateTargets, env);
    }

    log.Debug("published state pressure_risk=" + std::to_string(pressure.risk_level()) +
              " targets=" + std::to_string(targets.targets_size()));

    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
