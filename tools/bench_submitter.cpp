#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "maigent.pb.h"
#include "maigent/constants.h"
#include "maigent/message_utils.h"
#include "maigent/nats_wrapper.h"
#include "maigent/utils.h"

namespace {

maigent::WaitMode ParseWaitMode(const std::string& value) {
  if (value == "no_wait") {
    return maigent::NO_WAIT;
  }
  if (value == "wait_start") {
    return maigent::WAIT_FOR_START;
  }
  return maigent::WAIT_FOR_FINISH;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int n = maigent::GetFlagInt(argc, argv, "--n", 100);
  const int concurrency = maigent::GetFlagInt(argc, argv, "--concurrency", 10);
  const std::string cmd = maigent::GetFlagValue(argc, argv, "--cmd", "/bin/sleep");
  const std::string bench_id =
      maigent::GetFlagValue(argc, argv, "--bench-id", "bench-" + maigent::MakeUuid());
  const int cpu_millis = maigent::GetFlagInt(argc, argv, "--cpu-millis", 100);
  const int mem_mb = maigent::GetFlagInt(argc, argv, "--mem-mb", 64);
  const int submit_timeout_ms = maigent::GetFlagInt(argc, argv, "--submit-timeout-ms", 15000);
  const int wait_timeout_ms = maigent::GetFlagInt(argc, argv, "--wait-timeout-ms", 10000);
  const maigent::WaitMode wait_mode =
      ParseWaitMode(maigent::GetFlagValue(argc, argv, "--wait-mode", "wait_finish"));

  std::vector<std::string> args = maigent::GetMultiFlagValues(argc, argv, "--arg");
  const std::string single_args = maigent::GetFlagValue(argc, argv, "--args", "");
  if (!single_args.empty()) {
    args.push_back(single_args);
  }

  maigent::NatsClient nats;
  const std::string sender_id = "bench-" + maigent::MakeUuid();
  if (!nats.Connect(nats_url, sender_id)) {
    std::cerr << "failed to connect to NATS\n";
    return 1;
  }

  const int64_t start_ts = maigent::NowMs();
  {
    maigent::Envelope start_evt;
    maigent::FillHeader(&start_evt, maigent::BENCH_START, "bench_submitter", sender_id);
    auto* b = start_evt.mutable_bench_start();
    b->set_bench_id(bench_id);
    b->set_planned(n);
    b->set_start_ts_ms(start_ts);
    (*b->mutable_tags())["cmd"] = cmd;
    (*b->mutable_tags())["concurrency"] = std::to_string(concurrency);
    nats.PublishEnvelope(maigent::kSubjectEvtBenchStart, start_evt);
  }

  std::atomic<int> next_idx{0};
  std::atomic<int> sent{0};
  std::atomic<int> completed{0};
  std::atomic<int> timeouts{0};
  std::atomic<int> denied{0};
  std::atomic<int> failed{0};

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
            bench_id + "-submit-" + std::to_string(idx) + "-" + maigent::MakeUuid();
        maigent::FillHeader(&req, maigent::SUBMIT_TASK, "bench_submitter", sender_id,
                            request_id, bench_id, bench_id);
        auto* s = req.mutable_submit_task();
        s->set_bench_id(bench_id);
        s->set_cmd(cmd);
        for (const auto& arg : args) {
          s->add_args(arg);
        }
        s->mutable_resources()->set_cpu_millis(cpu_millis);
        s->mutable_resources()->set_mem_mb(mem_mb);
        s->set_priority(0);
        s->set_task_class("best_effort");
        s->set_wait_mode(wait_mode);
        s->set_wait_timeout_ms(wait_timeout_ms);

        maigent::Envelope reply;
        std::string err;
        if (!nats.RequestEnvelope(maigent::kSubjectCmdTaskSubmit, req, submit_timeout_ms,
                                  &reply, &err) ||
            !reply.has_submit_reply()) {
          ++timeouts;
          ++sent;
          continue;
        }

        ++sent;
        const auto& r = reply.submit_reply();
        switch (r.status()) {
          case maigent::ACCEPTED:
            ++completed;
            break;
          case maigent::DENIED:
            ++denied;
            break;
          case maigent::TIMEOUT:
            ++timeouts;
            break;
          default:
            ++failed;
            break;
        }
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }

  const int64_t stop_ts = maigent::NowMs();
  {
    maigent::Envelope stop_evt;
    maigent::FillHeader(&stop_evt, maigent::BENCH_STOP, "bench_submitter", sender_id);
    auto* b = stop_evt.mutable_bench_stop();
    b->set_bench_id(bench_id);
    b->set_planned(n);
    b->set_start_ts_ms(start_ts);
    b->set_stop_ts_ms(stop_ts);
    b->set_sent(sent.load());
    b->set_completed(completed.load());
    b->set_timeouts(timeouts.load());
    (*b->mutable_tags())["denied"] = std::to_string(denied.load());
    (*b->mutable_tags())["failed"] = std::to_string(failed.load());
    nats.PublishEnvelope(maigent::kSubjectEvtBenchStop, stop_evt);
  }

  std::cout << "bench_id=" << bench_id << " sent=" << sent.load()
            << " completed=" << completed.load() << " denied=" << denied.load()
            << " failed=" << failed.load() << " timeouts=" << timeouts.load() << "\n";

  return 0;
}
