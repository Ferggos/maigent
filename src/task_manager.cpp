#include <atomic>
#include <chrono>
#include <condition_variable>
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

struct TaskState {
  std::string task_id;
  std::string lease_id;
  int64_t submitted_ts_ms = 0;
  int64_t lease_decision_ts_ms = 0;
  bool started = false;
  bool finished = false;
  bool failed = false;
  int32_t pid = 0;
  int32_t exit_code = 0;
  int64_t started_ts_ms = 0;
  int64_t finished_ts_ms = 0;
  std::string error;
};

}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  const std::string role = "task_manager";
  const std::string agent_id = "tm-" + maigent::MakeUuid();
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int lease_ttl_ms = maigent::GetFlagInt(argc, argv, "--lease-ttl-ms", 5000);
  const int start_timeout_ms = maigent::GetFlagInt(argc, argv, "--start-timeout-ms", 2000);
  const int wait_finish_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--wait-finish-timeout-ms", 10000);

  maigent::NatsClient nats;
  if (!nats.Connect(nats_url, agent_id)) {
    maigent::LogError(role, "failed to connect to NATS");
    return 1;
  }

  std::mutex mu;
  std::condition_variable cv;
  std::unordered_map<std::string, TaskState> tasks;
  std::unordered_map<std::string, maigent::SubmitReply> submit_cache;
  maigent::PressureUpdate latest_pressure;

  auto on_task_event = [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_task_event()) {
      return;
    }

    const auto& e = env.task_event();
    std::lock_guard<std::mutex> lock(mu);
    TaskState& st = tasks[e.task_id()];
    st.task_id = e.task_id();
    if (!e.lease_id().empty()) {
      st.lease_id = e.lease_id();
    }

    if (e.event_type() == maigent::TASK_STARTED) {
      st.started = true;
      st.pid = e.pid();
      st.started_ts_ms = e.ts_ms();
      if (st.lease_decision_ts_ms == 0 && e.lease_decision_ts_ms() > 0) {
        st.lease_decision_ts_ms = e.lease_decision_ts_ms();
      }
    } else if (e.event_type() == maigent::TASK_FINISHED) {
      st.finished = true;
      st.exit_code = e.exit_code();
      st.finished_ts_ms = e.ts_ms();
    } else if (e.event_type() == maigent::TASK_FAILED) {
      st.failed = true;
      st.exit_code = e.exit_code();
      st.finished_ts_ms = e.ts_ms();
      st.error = e.error();
    }
    cv.notify_all();
  };

  nats.Subscribe(maigent::kSubjectEvtTaskStarted, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFinished, on_task_event);
  nats.Subscribe(maigent::kSubjectEvtTaskFailed, on_task_event);

  nats.Subscribe(maigent::kSubjectStatePressure, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope env;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &env) ||
        !env.has_pressure_update()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu);
    latest_pressure = env.pressure_update();
  });

  auto respond_submit = [&](const maigent::NatsMessage& incoming,
                            const std::string& request_id,
                            const maigent::SubmitReply& submit_reply) {
    maigent::Envelope reply;
    maigent::FillHeader(&reply, maigent::SUBMIT_REPLY, role, agent_id, request_id);
    *reply.mutable_submit_reply() = submit_reply;
    nats.RespondEnvelope(incoming, reply);
  };

  nats.Subscribe(maigent::kSubjectCmdTaskSubmit, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req) ||
        !req.has_submit_task()) {
      return;
    }

    const std::string submit_req_id = req.header().request_id();
    const std::string trace_id = req.header().trace_id().empty() ? maigent::MakeUuid()
                                                                  : req.header().trace_id();

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it_cached = submit_cache.find(submit_req_id);
      if (it_cached != submit_cache.end()) {
        respond_submit(msg, submit_req_id, it_cached->second);
        return;
      }
    }

    const auto& submit = req.submit_task();
    const std::string task_id = "task-" + maigent::MakeUuid();
    const int64_t submitted_ts = maigent::NowMs();

    {
      std::lock_guard<std::mutex> lock(mu);
      TaskState st;
      st.task_id = task_id;
      st.submitted_ts_ms = submitted_ts;
      tasks[task_id] = st;
    }

    maigent::Envelope submitted_evt;
    maigent::FillHeader(&submitted_evt, maigent::TASK_EVENT, role, agent_id, "", trace_id,
                        task_id);
    auto* te = submitted_evt.mutable_task_event();
    te->set_task_id(task_id);
    te->set_event_type(maigent::TASK_SUBMITTED);
    te->set_ts_ms(submitted_ts);
    te->set_submitted_ts_ms(submitted_ts);
    te->set_task_class(submit.task_class());
    nats.PublishEnvelope(maigent::kSubjectEvtTaskSubmitted, submitted_evt);

    maigent::Envelope reserve_req;
    maigent::FillHeader(&reserve_req, maigent::LEASE_RESERVE, role, agent_id,
                        "reserve-" + submit_req_id, trace_id, task_id);
    auto* reserve = reserve_req.mutable_lease_reserve();
    reserve->set_task_id(task_id);
    reserve->set_ttl_ms(lease_ttl_ms);
    reserve->mutable_resources()->set_cpu_millis(
        submit.has_resources() ? submit.resources().cpu_millis() : 100);
    reserve->mutable_resources()->set_mem_mb(
        submit.has_resources() ? submit.resources().mem_mb() : 64);

    maigent::Envelope reserve_reply;
    std::string req_err;
    if (!nats.RequestEnvelope(maigent::kSubjectCmdLeaseReserve, reserve_req, 1500,
                              &reserve_reply, &req_err) ||
        !reserve_reply.has_lease_decision()) {
      maigent::SubmitReply sr;
      sr.set_task_id(task_id);
      sr.set_status(maigent::FAILED);
      sr.set_message("lease reserve request failed: " + req_err);
      sr.set_submitted_ts_ms(submitted_ts);

      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = sr;
      }
      respond_submit(msg, submit_req_id, sr);
      return;
    }

    const auto reserve_decision = reserve_reply.lease_decision();
    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = tasks.find(task_id);
      if (it != tasks.end()) {
        it->second.lease_decision_ts_ms = reserve_decision.decision_ts_ms();
      }
    }

    if (!reserve_decision.granted()) {
      maigent::SubmitReply sr;
      sr.set_task_id(task_id);
      sr.set_status(maigent::DENIED);
      sr.set_message(reserve_decision.reason());
      sr.set_submitted_ts_ms(submitted_ts);

      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = sr;
      }
      respond_submit(msg, submit_req_id, sr);
      return;
    }

    maigent::Envelope commit_req;
    maigent::FillHeader(&commit_req, maigent::LEASE_COMMIT, role, agent_id,
                        "commit-" + submit_req_id, trace_id, task_id);
    commit_req.mutable_lease_commit()->set_lease_id(reserve_decision.lease_id());
    commit_req.mutable_lease_commit()->set_task_id(task_id);

    maigent::Envelope commit_reply;
    if (!nats.RequestEnvelope(maigent::kSubjectCmdLeaseCommit, commit_req, 1000,
                              &commit_reply, &req_err) ||
        !commit_reply.has_lease_decision() || !commit_reply.lease_decision().granted()) {
      maigent::SubmitReply sr;
      sr.set_task_id(task_id);
      sr.set_status(maigent::FAILED);
      sr.set_message("lease commit failed");
      sr.set_lease_id(reserve_decision.lease_id());
      sr.set_submitted_ts_ms(submitted_ts);

      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = sr;
      }
      respond_submit(msg, submit_req_id, sr);
      return;
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      auto it = tasks.find(task_id);
      if (it != tasks.end()) {
        it->second.lease_id = reserve_decision.lease_id();
      }
    }

    maigent::Envelope launch_env;
    maigent::FillHeader(&launch_env, maigent::LAUNCH_TASK, role, agent_id,
                        "launch-" + submit_req_id, trace_id, task_id);
    auto* launch = launch_env.mutable_launch_task();
    launch->set_task_id(task_id);
    launch->set_lease_id(reserve_decision.lease_id());
    launch->set_cmd(submit.cmd());
    for (const auto& a : submit.args()) {
      launch->add_args(a);
    }
    launch->set_priority(submit.priority());
    launch->set_task_class(submit.task_class());
    nats.PublishEnvelope(maigent::kSubjectCmdExecLaunch, launch_env);

    maigent::SubmitReply out;
    out.set_task_id(task_id);
    out.set_lease_id(reserve_decision.lease_id());
    out.set_submitted_ts_ms(submitted_ts);

    const auto wait_mode = submit.wait_mode() == maigent::WAIT_MODE_UNSPECIFIED
                               ? maigent::WAIT_FOR_START
                               : submit.wait_mode();

    if (wait_mode == maigent::NO_WAIT) {
      out.set_status(maigent::ACCEPTED);
      out.set_message("submitted");
      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = out;
      }
      respond_submit(msg, submit_req_id, out);
      return;
    }

    bool started = false;
    {
      std::unique_lock<std::mutex> lock(mu);
      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(start_timeout_ms);
      cv.wait_until(lock, deadline, [&]() {
        auto it = tasks.find(task_id);
        return it != tasks.end() && (it->second.started || it->second.failed);
      });
      auto it = tasks.find(task_id);
      if (it != tasks.end() && it->second.started) {
        started = true;
        out.set_pid(it->second.pid);
        out.set_started_ts_ms(it->second.started_ts_ms);
      } else if (it != tasks.end() && it->second.failed) {
        out.set_status(maigent::FAILED);
        out.set_message(it->second.error.empty() ? "failed before start" : it->second.error);
      }
    }

    if (!started && out.status() == maigent::SUBMIT_STATUS_UNSPECIFIED) {
      out.set_status(maigent::TIMEOUT);
      out.set_message("timeout waiting for task start");
      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = out;
      }
      respond_submit(msg, submit_req_id, out);
      return;
    }

    if (wait_mode == maigent::WAIT_FOR_START) {
      out.set_status(maigent::ACCEPTED);
      out.set_message("started");
      {
        std::lock_guard<std::mutex> lock(mu);
        submit_cache[submit_req_id] = out;
      }
      respond_submit(msg, submit_req_id, out);
      return;
    }

    const int64_t finish_wait_ms =
        submit.wait_timeout_ms() > 0 ? submit.wait_timeout_ms() : wait_finish_timeout_ms;

    {
      std::unique_lock<std::mutex> lock(mu);
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(finish_wait_ms);
      cv.wait_until(lock, deadline, [&]() {
        auto it = tasks.find(task_id);
        return it != tasks.end() && (it->second.finished || it->second.failed);
      });

      auto it = tasks.find(task_id);
      if (it != tasks.end()) {
        if (it->second.finished) {
          out.set_status(maigent::ACCEPTED);
          out.set_message("finished");
          out.set_finished_ts_ms(it->second.finished_ts_ms);
        } else if (it->second.failed) {
          out.set_status(maigent::FAILED);
          out.set_message(it->second.error.empty() ? "failed" : it->second.error);
          out.set_finished_ts_ms(it->second.finished_ts_ms);
        } else {
          out.set_status(maigent::TIMEOUT);
          out.set_message("timeout waiting for finish");
        }
      } else {
        out.set_status(maigent::FAILED);
        out.set_message("task state lost");
      }
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      submit_cache[submit_req_id] = out;
    }

    respond_submit(msg, submit_req_id, out);
  });

  nats.Subscribe(maigent::kSubjectCmdSystemStatus, [&](const maigent::NatsMessage& msg) {
    maigent::Envelope req;
    if (!maigent::ParseEnvelope(msg.data.data(), static_cast<int>(msg.data.size()), &req)) {
      return;
    }

    maigent::Envelope snapshot_req;
    maigent::FillHeader(&snapshot_req, maigent::DIR_SNAPSHOT, role, agent_id,
                        "dir-snapshot-" + maigent::MakeUuid());
    snapshot_req.mutable_dir_snapshot_request();

    maigent::Envelope snapshot_reply;
    std::string err;
    bool has_snapshot =
        nats.RequestEnvelope(maigent::kSubjectCmdDirSnapshot, snapshot_req, 1000,
                            &snapshot_reply, &err) &&
        snapshot_reply.has_dir_snapshot();

    maigent::Envelope out;
    maigent::FillHeader(&out, maigent::SYSTEM_STATUS, role, agent_id, req.header().request_id());
    auto* status = out.mutable_system_status();
    if (has_snapshot) {
      *status->mutable_directory() = snapshot_reply.dir_snapshot();
    }

    {
      std::lock_guard<std::mutex> lock(mu);
      if (latest_pressure.ts_ms() > 0) {
        *status->mutable_pressure() = latest_pressure;
      }
    }
    status->set_ts_ms(maigent::NowMs());

    nats.RespondEnvelope(msg, out);
  });

  auto inflight_fn = [&]() {
    std::lock_guard<std::mutex> lock(mu);
    int count = 0;
    for (const auto& [id, t] : tasks) {
      if (t.started && !t.finished && !t.failed) {
        ++count;
      }
    }
    return count;
  };

  maigent::AgentLifecycle lifecycle(
      &nats, role, agent_id, {"task.submit", "system.status", "dir.proxy"}, inflight_fn);
  lifecycle.Start(1000);

  maigent::LogInfo(role, "started");
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  lifecycle.Stop(true);
  maigent::LogInfo(role, "stopped");
  return 0;
}
