#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
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

struct State {
  std::mutex mu;
  std::unordered_map<std::string, LeaseRecord> leases;
  std::unordered_map<std::string, maigent::LeaseDecision> reserve_cache;
  std::unordered_map<std::string, maigent::LeaseDecision> commit_cache;
  std::unordered_map<std::string, maigent::LeaseDecision> release_cache;
  int64_t mem_allocatable_mb = 4096;
  int64_t cpu_allocatable_millis = 4000;
  int max_managed_tasks = 16;
};

struct Usage {
  int64_t mem_mb = 0;
  int64_t cpu_millis = 0;
  int active_leases = 0;
};

Usage ComputeUsage(const State& st) {
  Usage usage;
  for (const auto& [lease_id, lease] : st.leases) {
    if (lease.state == maigent::LEASE_RESERVED ||
        lease.state == maigent::LEASE_COMMITTED) {
      usage.mem_mb += lease.resources.mem_mb();
      usage.cpu_millis += lease.resources.cpu_millis();
      ++usage.active_leases;
    }
  }
  return usage;
}

maigent::LeaseDecision MakeDecision(const std::string& task_id, bool granted,
                                    const std::string& lease_id, int64_t expiry_ms,
                                    int64_t cpu_budget, int64_t mem_budget,
                                    const std::string& reason,
                                    maigent::LeaseState lease_state,
                                    maigent::LeaseDecisionReasonCode reason_code) {
  maigent::LeaseDecision d;
  d.set_task_id(task_id);
  d.set_granted(granted);
  d.set_lease_id(lease_id);
  d.set_expiry_ms(expiry_ms);
  d.mutable_budget()->set_cpu_millis(static_cast<int32_t>(cpu_budget));
  d.mutable_budget()->set_mem_mb(static_cast<int32_t>(mem_budget));
  d.set_reason(reason);
  d.set_decision_ts_ms(maigent::NowMs());
  d.set_lease_state(lease_state);
  d.set_reason_code(reason_code);
  return d;
}

std::string UsageString(const Usage& usage) {
  return "active=" + std::to_string(usage.active_leases) +
         " cpu_millis=" + std::to_string(usage.cpu_millis) +
         " mem_mb=" + std::to_string(usage.mem_mb);
}

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "lease_authority_agent";
  const std::string agent_id = "lease-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int64_t default_mem_alloc_mb =
      maigent::GetFlagInt64(argc, argv, "--mem-allocatable-mb", 4096);
  const int64_t default_cpu_alloc_millis =
      maigent::GetFlagInt64(argc, argv, "--cpu-allocatable-millis", 4000);
  const int default_max_tasks = maigent::GetFlagInt(argc, argv, "--max-managed-tasks", 16);

  maigent::AgentLogger log(agent_id, "logs/lease_authority.log");

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    log.Error("failed to connect to NATS");
    return 1;
  }

  State st;
  st.mem_allocatable_mb = default_mem_alloc_mb;
  st.cpu_allocatable_millis = default_cpu_alloc_millis;
  st.max_managed_tasks = default_max_tasks;

  auto publish_lease_event = [&](const std::string& subject, maigent::MessageKind kind,
                                 const maigent::LeaseDecision& decision,
                                 const std::string& trace_id) {
    maigent::Envelope env;
    maigent::FillHeader(&env, maigent::EVENT, kind, role, agent_id,
                        maigent::MakeRequestId("evt-lease"), trace_id,
                        decision.task_id());
    *env.mutable_event()->mutable_lease_decision() = decision;
    nats.PublishEnvelope(subject, env);
  };

  auto expire_leases = [&]() {
    const int64_t now = maigent::NowMs();
    std::vector<maigent::LeaseDecision> expired;
    {
      std::lock_guard<std::mutex> lock(st.mu);
      for (auto& [lease_id, lease] : st.leases) {
        if (lease.state == maigent::LEASE_RESERVED && lease.expiry_ms > 0 &&
            now > lease.expiry_ms) {
          lease.state = maigent::LEASE_EXPIRED;
          expired.push_back(MakeDecision(
              lease.task_id, false, lease.lease_id, lease.expiry_ms,
              st.cpu_allocatable_millis, st.mem_allocatable_mb, "lease expired",
              maigent::LEASE_EXPIRED, maigent::LEASE_REASON_INTERNAL_ERROR));
        }
      }
    }
    for (const auto& d : expired) {
      publish_lease_event(maigent::kSubjectEvtLeaseExpired, maigent::MK_LEASE_EXPIRED, d, "");
      log.Warn("lease expired lease_id=" + d.lease_id() + " task_id=" + d.task_id());
    }
  };

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
    const auto& cap = env.state().capacity_state();
    std::lock_guard<std::mutex> lock(st.mu);
    if (cap.mem_allocatable_mb() > 0) {
      st.mem_allocatable_mb = cap.mem_allocatable_mb();
    }
    if (cap.cpu_millis_allocatable() > 0) {
      st.cpu_allocatable_millis = cap.cpu_millis_allocatable();
    }
    if (cap.max_managed_tasks() > 0) {
      st.max_managed_tasks = cap.max_managed_tasks();
    }
  });

  auto respond = [&](const maigent::NatsMessage& incoming, const maigent::Envelope& req,
                     const maigent::LeaseDecision& decision) {
    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SERVICE, maigent::MK_LEASE_DECISION,
                        role, agent_id,
                        req.header().request_id(), req.header().trace_id(),
                        req.header().conversation_id());
    *reply.mutable_service()->mutable_lease_decision() = decision;
    nats.RespondEnvelope(incoming, reply);
  };

  nats.Subscribe(maigent::kSubjectCmdLeaseReserve, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_command() || !req.command().has_lease_reserve()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_LEASE_RESERVE) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const std::string trace_id = req.header().trace_id();
    const auto& reserve = req.command().lease_reserve();

    maigent::LeaseDecision decision;
    {
      std::lock_guard<std::mutex> lock(st.mu);
      auto cached = st.reserve_cache.find(request_id);
      if (cached != st.reserve_cache.end()) {
        decision = cached->second;
      } else {
        const Usage usage = ComputeUsage(st);
        const int64_t req_mem = reserve.resources().mem_mb();
        const int64_t req_cpu = reserve.resources().cpu_millis();

        const bool mem_ok = usage.mem_mb + req_mem <= st.mem_allocatable_mb;
        const bool cpu_ok = usage.cpu_millis + req_cpu <= st.cpu_allocatable_millis;
        const bool slots_ok = usage.active_leases < st.max_managed_tasks;

        if (mem_ok && cpu_ok && slots_ok) {
          LeaseRecord record;
          record.lease_id = maigent::MakeLeaseId();
          record.task_id = reserve.task_id();
          record.resources = reserve.resources();
          record.created_ms = maigent::NowMs();
          const int64_t ttl_ms = reserve.ttl_ms() > 0 ? reserve.ttl_ms() : 5000;
          record.expiry_ms = record.created_ms + ttl_ms;
          record.state = maigent::LEASE_RESERVED;
          st.leases[record.lease_id] = record;

          decision = MakeDecision(record.task_id, true, record.lease_id, record.expiry_ms,
                                  st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                  "granted", maigent::LEASE_RESERVED,
                                  maigent::LEASE_REASON_NONE);
        } else {
          std::string reason = "denied:";
          maigent::LeaseDecisionReasonCode reason_code = maigent::LEASE_REASON_NONE;
          if (!mem_ok) {
            reason += " mem";
            reason_code = maigent::LEASE_REASON_NO_CAPACITY;
          }
          if (!cpu_ok) {
            reason += " cpu";
            reason_code = maigent::LEASE_REASON_NO_CAPACITY;
          }
          if (!slots_ok) {
            reason += " slots";
            reason_code = maigent::LEASE_REASON_NO_SLOTS;
          }
          decision = MakeDecision(reserve.task_id(), false, "", 0,
                                  st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                  reason, maigent::LEASE_STATE_UNSPECIFIED, reason_code);
        }
        st.reserve_cache[request_id] = decision;
      }
    }

    if (!decision.granted()) {
      publish_lease_event(maigent::kSubjectEvtLeaseDenied, maigent::MK_LEASE_DENIED,
                          decision, trace_id);
    }

    respond(msg, req, decision);
    log.Info("reserve task_id=" + reserve.task_id() + " result=" +
             (decision.granted() ? "granted" : "denied"),
             {request_id, reserve.task_id(), trace_id});
  });

  nats.Subscribe(maigent::kSubjectCmdLeaseCommit, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_command() || !req.command().has_lease_commit()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_LEASE_COMMIT) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const std::string trace_id = req.header().trace_id();
    const auto& commit = req.command().lease_commit();

    maigent::LeaseDecision decision;
    {
      std::lock_guard<std::mutex> lock(st.mu);
      auto cached = st.commit_cache.find(request_id);
      if (cached != st.commit_cache.end()) {
        decision = cached->second;
      } else {
        auto it = st.leases.find(commit.lease_id());
        if (it == st.leases.end()) {
          decision = MakeDecision(commit.task_id(), false, commit.lease_id(), 0,
                                  st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                  "lease not found", maigent::LEASE_STATE_UNSPECIFIED,
                                  maigent::LEASE_REASON_INTERNAL_ERROR);
        } else {
          auto& lease = it->second;
          if (lease.state == maigent::LEASE_RESERVED ||
              lease.state == maigent::LEASE_COMMITTED) {
            lease.state = maigent::LEASE_COMMITTED;
            decision = MakeDecision(lease.task_id, true, lease.lease_id, lease.expiry_ms,
                                    st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                    "committed", maigent::LEASE_COMMITTED,
                                    maigent::LEASE_REASON_NONE);
          } else {
            decision = MakeDecision(lease.task_id, false, lease.lease_id, lease.expiry_ms,
                                    st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                    "lease not commitable", lease.state,
                                    maigent::LEASE_REASON_INTERNAL_ERROR);
          }
        }
        st.commit_cache[request_id] = decision;
      }
    }

    respond(msg, req, decision);
    log.Info("commit lease_id=" + commit.lease_id() + " result=" +
             (decision.granted() ? "ok" : "failed"),
             {request_id, commit.task_id(), trace_id});
  });

  nats.Subscribe(maigent::kSubjectCmdLeaseRelease, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_command() || !req.command().has_lease_release()) {
      return;
    }
    if (!req.has_header() || req.header().message_category() != maigent::COMMAND ||
        req.header().message_kind() != maigent::MK_LEASE_RELEASE) {
      return;
    }

    const std::string request_id = req.header().request_id();
    const std::string trace_id = req.header().trace_id();
    const auto& release = req.command().lease_release();

    maigent::LeaseDecision decision;
    {
      std::lock_guard<std::mutex> lock(st.mu);
      auto cached = st.release_cache.find(request_id);
      if (cached != st.release_cache.end()) {
        decision = cached->second;
      } else {
        auto it = st.leases.find(release.lease_id());
        if (it == st.leases.end()) {
          decision = MakeDecision(release.task_id(), true, release.lease_id(), 0,
                                  st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                  "already released", maigent::LEASE_RELEASED,
                                  maigent::LEASE_REASON_NONE);
        } else {
          auto& lease = it->second;
          lease.state = maigent::LEASE_RELEASED;
          decision = MakeDecision(lease.task_id, true, lease.lease_id, lease.expiry_ms,
                                  st.cpu_allocatable_millis, st.mem_allocatable_mb,
                                  "released", maigent::LEASE_RELEASED,
                                  maigent::LEASE_REASON_NONE);
        }
        st.release_cache[request_id] = decision;
      }
    }

    respond(msg, req, decision);
    log.Info("release lease_id=" + release.lease_id() + " reason=" + release.reason(),
             {request_id, release.task_id(), trace_id});
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(st.mu);
    return ComputeUsage(st).active_leases;
  };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id,
      {"lease.reserve", "lease.commit", "lease.release", "lease.expiry"},
      inflight_fn);
  lifecycle.Start(1000);

  log.Info("started");

  while (!g_stop.load()) {
    expire_leases();
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  lifecycle.Stop(true);
  log.Info("stopped");
  return 0;
}
