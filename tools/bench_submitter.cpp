#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "maigent.pb.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"
#include "maigent/common/time_utils.h"

namespace {

maigent::WaitMode ParseWaitMode(const std::string& mode) {
  if (mode == "no_wait") {
    return maigent::WAIT_NO_WAIT;
  }
  if (mode == "wait_start") {
    return maigent::WAIT_START;
  }
  return maigent::WAIT_FINISH;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int n = maigent::GetFlagInt(argc, argv, "--n", 100);
  const int concurrency = maigent::GetFlagInt(argc, argv, "--concurrency", 10);
  const std::string cmd = maigent::GetFlagValue(argc, argv, "--cmd", "/bin/sleep");
  const std::string args_str = maigent::GetFlagValue(argc, argv, "--args", "");
  const std::string bench_id =
      maigent::GetFlagValue(argc, argv, "--bench-id", "bench-" + maigent::MakeUuid());
  const std::string wait_mode_str =
      maigent::GetFlagValue(argc, argv, "--wait-mode", "wait_finish");
  const int submit_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--submit-timeout-ms", 15000);
  const int wait_timeout_ms =
      maigent::GetFlagInt(argc, argv, "--wait-timeout-ms", 10000);

  std::vector<std::string> args = maigent::GetMultiFlagValues(argc, argv, "--arg");
  if (!args_str.empty()) {
    args.push_back(args_str);
  }

  maigent::NatsClient nats;
  const std::string sender_id = "bench-" + maigent::MakeUuid();
  if (!nats.Connect(nats_url, sender_id)) {
    std::cerr << "failed to connect to NATS\n";
    return 1;
  }

  const int64_t start_ts = maigent::NowMs();

  {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::SERVICE, maigent::MK_BENCH_START,
                        "bench_submitter", sender_id,
                        maigent::MakeRequestId("bench-start"));
    auto* b = evt.mutable_service()->mutable_bench_session();
    b->set_bench_id(bench_id);
    b->set_stage(maigent::BENCH_START);
    b->set_planned_requests(n);
    b->set_start_ts_ms(start_ts);
    (*b->mutable_tags())["cmd"] = cmd;
    (*b->mutable_tags())["concurrency"] = std::to_string(concurrency);
    nats.PublishEnvelope(maigent::kSubjectSvcBenchStart, evt);
  }

  std::atomic<int> next_idx{0};
  std::atomic<int> sent{0};
  std::atomic<int> completed{0};
  std::atomic<int> denied{0};
  std::atomic<int> failed{0};
  std::atomic<int> timeouts{0};

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(concurrency));

  for (int w = 0; w < concurrency; ++w) {
    workers.emplace_back([&, w]() {
      while (true) {
        const int idx = next_idx.fetch_add(1);
        if (idx >= n) {
          break;
        }

        maigent::Envelope req;
        const std::string request_id =
            bench_id + "-" + std::to_string(idx) + "-" + maigent::MakeUuid();
        maigent::FillHeader(&req, maigent::COMMAND, maigent::MK_TASK_SUBMIT,
                            "bench_submitter", sender_id,
                            request_id, maigent::MakeTraceId(), bench_id);

        auto* submit = req.mutable_command()->mutable_submit_task();
        submit->set_bench_id(bench_id);
        submit->set_cmd(cmd);
        for (const auto& arg : args) {
          submit->add_args(arg);
        }
        submit->mutable_resources()->set_cpu_millis(100);
        submit->mutable_resources()->set_mem_mb(64);
        submit->set_priority(0);
        submit->set_task_class("bench");
        submit->set_wait_mode(ParseWaitMode(wait_mode_str));
        submit->set_wait_timeout_ms(wait_timeout_ms);

        maigent::Envelope reply;
        std::string err;
        const bool ok = nats.RequestEnvelope(maigent::kSubjectCmdTaskSubmit, req,
                                             submit_timeout_ms, &reply, &err);

        ++sent;

        if (!ok || !reply.has_service() || !reply.service().has_submit_reply() ||
            !reply.has_header() ||
            reply.header().message_category() != maigent::SERVICE ||
            reply.header().message_kind() != maigent::MK_TASK_SUBMIT_REPLY) {
          ++timeouts;
          continue;
        }

        const auto& sr = reply.service().submit_reply();
        if (sr.status() == maigent::SUBMIT_ACCEPTED) {
          ++completed;
        } else if (sr.status() == maigent::SUBMIT_DENIED) {
          ++denied;
        } else if (sr.status() == maigent::SUBMIT_TIMEOUT) {
          ++timeouts;
        } else {
          ++failed;
        }
      }
    });
  }

  for (auto& worker : workers) {
    worker.join();
  }

  const int64_t stop_ts = maigent::NowMs();

  {
    maigent::Envelope evt;
    maigent::FillHeader(&evt, maigent::SERVICE, maigent::MK_BENCH_STOP,
                        "bench_submitter", sender_id,
                        maigent::MakeRequestId("bench-stop"));
    auto* b = evt.mutable_service()->mutable_bench_session();
    b->set_bench_id(bench_id);
    b->set_stage(maigent::BENCH_STOP);
    b->set_planned_requests(n);
    b->set_sent_requests(sent.load());
    b->set_completed(completed.load());
    b->set_failed(failed.load());
    b->set_denied(denied.load());
    b->set_timeouts(timeouts.load());
    b->set_start_ts_ms(start_ts);
    b->set_stop_ts_ms(stop_ts);
    (*b->mutable_tags())["cmd"] = cmd;
    nats.PublishEnvelope(maigent::kSubjectSvcBenchStop, evt);
  }

  std::cout << "bench_id=" << bench_id << " sent=" << sent.load()
            << " completed=" << completed.load() << " denied=" << denied.load()
            << " failed=" << failed.load() << " timeouts=" << timeouts.load()
            << '\n';

  return 0;
}
