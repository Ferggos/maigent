#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

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

bool ReadCpuSample(uint64_t* idle_out, uint64_t* total_out) {
  std::ifstream in("/proc/stat");
  if (!in.is_open()) {
    return false;
  }

  std::string line;
  std::getline(in, line);
  std::istringstream iss(line);
  std::string cpu;
  uint64_t user = 0, nice = 0, system = 0, idle = 0, iowait = 0, irq = 0, softirq = 0,
           steal = 0;
  iss >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
  if (cpu != "cpu") {
    return false;
  }

  const uint64_t idle_all = idle + iowait;
  const uint64_t total = user + nice + system + idle + iowait + irq + softirq + steal;
  *idle_out = idle_all;
  *total_out = total;
  return true;
}

int64_t ReadMemAvailableMb() {
  std::ifstream in("/proc/meminfo");
  if (!in.is_open()) {
    return 0;
  }

  std::string key;
  int64_t value_kb = 0;
  std::string unit;
  while (in >> key >> value_kb >> unit) {
    if (key == "MemAvailable:") {
      return value_kb / 1024;
    }
  }
  return 0;
}

maigent::RiskLevel ComputeRisk(double cpu_pct, int64_t mem_mb, int64_t high_mem_threshold,
                               int64_t med_mem_threshold) {
  if (cpu_pct >= 90.0 || mem_mb <= high_mem_threshold) {
    return maigent::HIGH;
  }
  if (cpu_pct >= 75.0 || mem_mb <= med_mem_threshold) {
    return maigent::MED;
  }
  return maigent::LOW;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "system_monitor";
  const std::string agent_id = "sysmon-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int interval_ms = maigent::GetFlagInt(argc, argv, "--interval-ms", 500);
  const int64_t high_mem_threshold_mb =
      maigent::GetFlagInt64(argc, argv, "--high-mem-threshold-mb", 512);
  const int64_t med_mem_threshold_mb =
      maigent::GetFlagInt64(argc, argv, "--med-mem-threshold-mb", 1024);

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id, {"state.pressure", "state.forecast"}, [] { return 0; });
  lifecycle.Start(1000);
  maigent::LogInfo(role, "started");

  uint64_t prev_idle = 0;
  uint64_t prev_total = 0;
  if (!ReadCpuSample(&prev_idle, &prev_total)) {
    maigent::LogWarn(role, "failed to read /proc/stat on startup");
  }

  while (!g_stop.load()) {
    uint64_t idle = 0;
    uint64_t total = 0;
    double cpu_usage_pct = 0.0;
    if (ReadCpuSample(&idle, &total)) {
      const uint64_t delta_idle = idle - prev_idle;
      const uint64_t delta_total = total - prev_total;
      if (delta_total > 0) {
        cpu_usage_pct = 100.0 * (1.0 - static_cast<double>(delta_idle) /
                                           static_cast<double>(delta_total));
      }
      prev_idle = idle;
      prev_total = total;
    }

    const int64_t mem_mb = ReadMemAvailableMb();
    const auto risk =
        ComputeRisk(cpu_usage_pct, mem_mb, high_mem_threshold_mb, med_mem_threshold_mb);
    const int64_t now = maigent::NowMs();

    maigent::Envelope pressure_env;
    maigent::FillHeader(&pressure_env, maigent::PRESSURE_UPDATE, role, agent_id);
    auto* p = pressure_env.mutable_pressure_update();
    p->set_ts_ms(now);
    p->set_cpu_usage_pct(cpu_usage_pct);
    p->set_mem_available_mb(mem_mb);
    p->set_risk_level(risk);
    nats.PublishEnvelope(maigent::kSubjectStatePressure, pressure_env);

    maigent::Envelope forecast_env;
    maigent::FillHeader(&forecast_env, maigent::FORECAST_UPDATE, role, agent_id);
    auto* f = forecast_env.mutable_forecast_update();
    f->set_ts_ms(now);
    f->set_predicted_cpu_usage_pct(cpu_usage_pct);
    f->set_predicted_mem_available_mb(mem_mb);
    f->set_risk_level(risk);
    nats.PublishEnvelope(maigent::kSubjectStateForecast, forecast_env);

    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
