#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

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

struct LeaseRecord {
  std::string lease_id;
  std::string task_id;
  maigent::ResourceRequest resources;
  int64_t created_ms = 0;
  int64_t expiry_ms = 0;
  maigent::LeaseState state = maigent::LEASE_STATE_UNSPECIFIED;
};

struct LeaseStateStore {
  std::mutex mu;
  std::unordered_map<std::string, LeaseRecord> leases;
  std::unordered_map<std::string, maigent::LeaseDecision> reserve_by_request;
  std::unordered_map<std::string, maigent::LeaseDecision> commit_by_request;
  std::unordered_map<std::string, maigent::LeaseDecision> release_by_request;
  int64_t latest_mem_available_mb = 4096;
};

void ExpireReserved(LeaseStateStore* st) {
  const int64_t now = maigent::NowMs();
  for (auto& [id, lease] : st->leases) {
    if (lease.state == maigent::RESERVED && lease.expiry_ms > 0 && now > lease.expiry_ms) {
      lease.state = maigent::EXPIRED;
    }
  }
}

struct CapacityUsage {
  int64_t used_mem_mb = 0;
  int64_t used_cpu_millis = 0;
  int active_tasks = 0;
};

CapacityUsage ComputeUsage(const LeaseStateStore& st) {
  CapacityUsage u;
  for (const auto& [id, lease] : st.leases) {
    if (lease.state == maigent::RESERVED || lease.state == maigent::COMMITTED) {
      u.used_mem_mb += lease.resources.mem_mb();
      u.used_cpu_millis += lease.resources.cpu_millis();
      ++u.active_tasks;
    }
  }
  return u;
}

maigent::LeaseDecision MakeDecision(const std::string& task_id, bool granted,
                                    const std::string& lease_id, int64_t expiry_ms,
                                    int64_t cpu_budget, int64_t mem_budget,
                                    const std::string& reason) {
  maigent::LeaseDecision d;
  d.set_task_id(task_id);
  d.set_granted(granted);
  d.set_lease_id(lease_id);
  d.set_expiry_ms(expiry_ms);
  d.mutable_budget()->set_cpu_millis(static_cast<int32_t>(cpu_budget));
  d.mutable_budget()->set_mem_mb(static_cast<int32_t>(mem_budget));
  d.set_reason(reason);
  d.set_decision_ts_ms(maigent::NowMs());
  return d;
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "lease_authority";
  const std::string agent_id = "lease-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const double mem_factor = maigent::GetFlagDouble(argc, argv, "--mem-factor", 0.8);
  const int64_t cores = std::thread::hardware_concurrency() > 0
                            ? static_cast<int64_t>(std::thread::hardware_concurrency())
                            : 4;
  const int64_t cpu_budget =
      maigent::GetFlagInt64(argc, argv, "--cpu-budget-millis", cores * 1000);
  const int max_tasks = maigent::GetFlagInt(argc, argv, "--max-tasks", static_cast<int>(cores * 2));

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  LeaseStateStore st;

  nats.Subscribe(maigent::kSubjectStatePressure, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_pressure_update()) {
      return;
    }
    std::lock_guard<std::mutex> lock(st.mu);
    st.latest_mem_available_mb = env.pressure_update().mem_available_mb();
  });

  auto respond_decision = [&](const maigent::NatsMessage& msg, const std::string& req_id,
                              const maigent::LeaseDecision& d) {
    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::LEASE_DECISION, role, agent_id, req_id);
    *reply.mutable_lease_decision() = d;
    nats.RespondEnvelope(msg, reply);
  };

  nats.Subscribe(maigent::kSubjectCmdLeaseReserve, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_lease_reserve()) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const auto& reserve = req.lease_reserve();

    maigent::LeaseDecision decision;
    {
      std::lock_guard<std::mutex> lock(st.mu);
      ExpireReserved(&st);

      auto it_cached = st.reserve_by_request.find(request_id);
      if (it_cached != st.reserve_by_request.end()) {
        decision = it_cached->second;
      } else {
        const int64_t mem_budget = static_cast<int64_t>(st.latest_mem_available_mb * mem_factor);
        const CapacityUsage usage = ComputeUsage(st);

        const int64_t req_mem = reserve.resources().mem_mb();
        const int64_t req_cpu = reserve.resources().cpu_millis();

        const bool mem_ok = usage.used_mem_mb + req_mem <= mem_budget;
        const bool cpu_ok = usage.used_cpu_millis + req_cpu <= cpu_budget;
        const bool slots_ok = usage.active_tasks < max_tasks;

        if (mem_ok && cpu_ok && slots_ok) {
          LeaseRecord rec;
          rec.lease_id = "lease-" + maigent::MakeUuid();
          rec.task_id = reserve.task_id();
          rec.resources = reserve.resources();
          rec.created_ms = maigent::NowMs();
          rec.expiry_ms = rec.created_ms + (reserve.ttl_ms() > 0 ? reserve.ttl_ms() : 5000);
          rec.state = maigent::RESERVED;

          decision = MakeDecision(rec.task_id, true, rec.lease_id, rec.expiry_ms, cpu_budget,
                                  mem_budget, "granted");
          st.leases[rec.lease_id] = rec;
        } else {
          std::string reason = "denied:";
          if (!mem_ok) {
            reason += " mem";
          }
          if (!cpu_ok) {
            reason += " cpu";
          }
          if (!slots_ok) {
            reason += " slots";
          }
          decision = MakeDecision(reserve.task_id(), false, "", 0, cpu_budget, mem_budget,
                                  reason);
        }

        st.reserve_by_request[request_id] = decision;
      }
    }

    if (!decision.granted()) {
      maigent::Envelope denied_evt;
      maigent::FillHeader(&denied_evt, maigent::LEASE_DECISION, role, agent_id);
      *denied_evt.mutable_lease_decision() = decision;
      nats.PublishEnvelope(maigent::kSubjectEvtLeaseDenied, denied_evt);
    }

    respond_decision(msg, request_id, decision);
  });

  nats.Subscribe(maigent::kSubjectCmdLeaseCommit, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_lease_commit()) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const auto& commit = req.lease_commit();
    maigent::LeaseDecision decision;

    {
      std::lock_guard<std::mutex> lock(st.mu);
      ExpireReserved(&st);
      auto cached = st.commit_by_request.find(request_id);
      if (cached != st.commit_by_request.end()) {
        decision = cached->second;
      } else {
        const int64_t mem_budget = static_cast<int64_t>(st.latest_mem_available_mb * mem_factor);

        auto it = st.leases.find(commit.lease_id());
        if (it == st.leases.end()) {
          decision = MakeDecision(commit.task_id(), false, commit.lease_id(), 0, cpu_budget,
                                  mem_budget, "lease not found");
        } else {
          auto& lease = it->second;
          if (lease.state == maigent::RESERVED || lease.state == maigent::COMMITTED) {
            lease.state = maigent::COMMITTED;
            decision = MakeDecision(lease.task_id, true, lease.lease_id, lease.expiry_ms,
                                    cpu_budget, mem_budget, "committed");
          } else {
            decision = MakeDecision(lease.task_id, false, lease.lease_id, lease.expiry_ms,
                                    cpu_budget, mem_budget, "lease not reservable");
          }
        }
        st.commit_by_request[request_id] = decision;
      }
    }

    respond_decision(msg, request_id, decision);
  });

  nats.Subscribe(maigent::kSubjectCmdLeaseRelease, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_lease_release()) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const auto& release = req.lease_release();
    maigent::LeaseDecision decision;

    {
      std::lock_guard<std::mutex> lock(st.mu);
      ExpireReserved(&st);
      auto cached = st.release_by_request.find(request_id);
      if (cached != st.release_by_request.end()) {
        decision = cached->second;
      } else {
        const int64_t mem_budget = static_cast<int64_t>(st.latest_mem_available_mb * mem_factor);
        auto it = st.leases.find(release.lease_id());
        if (it == st.leases.end()) {
          decision = MakeDecision(release.task_id(), true, release.lease_id(), 0, cpu_budget,
                                  mem_budget, "already released");
        } else {
          auto& lease = it->second;
          if (lease.state == maigent::RELEASED || lease.state == maigent::EXPIRED) {
            decision = MakeDecision(lease.task_id, true, lease.lease_id, lease.expiry_ms,
                                    cpu_budget, mem_budget, "already terminal");
          } else {
            lease.state = maigent::RELEASED;
            decision = MakeDecision(lease.task_id, true, lease.lease_id, lease.expiry_ms,
                                    cpu_budget, mem_budget, "released");
          }
        }
        st.release_by_request[request_id] = decision;
      }
    }

    respond_decision(msg, request_id, decision);
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(st.mu);
    ExpireReserved(&st);
    int count = 0;
    for (const auto& [id, lease] : st.leases) {
      if (lease.state == maigent::RESERVED || lease.state == maigent::COMMITTED) {
        ++count;
      }
    }
    return count;
  };

  maigent::AgentLifecycle lifecycle(&nats, role, agent_id,
                                    {"lease.reserve", "lease.commit", "lease.release"},
                                    inflight_fn);
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");

  while (!g_stop.load()) {
    {
      std::lock_guard<std::mutex> lock(st.mu);
      ExpireReserved(&st);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
