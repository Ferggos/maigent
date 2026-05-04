#include <iostream>
#include <string>

#include "maigent.pb.h"
#include "maigent/common/config.h"
#include "maigent/common/constants.h"
#include "maigent/common/ids.h"
#include "maigent/common/message_helpers.h"
#include "maigent/common/nats_wrapper.h"

namespace {

void PrintUsage() {
  std::cerr
      << "usage:\n"
      << "  external_process_client --register-pid PID [--label NAME] "
         "[--external-id ID] [--priority N] [--cgroup-path PATH] "
         "[--allow-control] [--nats URL]\n"
      << "  external_process_client --unregister-pid PID [--nats URL]\n"
      << "  external_process_client --unregister-target-id TARGET_ID [--nats URL]\n";
}

}  // namespace

int main(int argc, char** argv) {
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const int register_pid = maigent::GetFlagInt(argc, argv, "--register-pid", 0);
  const int unregister_pid = maigent::GetFlagInt(argc, argv, "--unregister-pid", 0);
  const std::string unregister_target_id =
      maigent::GetFlagValue(argc, argv, "--unregister-target-id", "");

  const bool do_register = register_pid > 0;
  const bool do_unregister = unregister_pid > 0 || !unregister_target_id.empty();
  if (do_register == do_unregister) {
    PrintUsage();
    return 2;
  }

  maigent::NatsClient nats;
  const std::string sender_id = "external-process-client-" + maigent::MakeUuid();
  if (!nats.Connect(nats_url, sender_id)) {
    std::cerr << "failed to connect to NATS\n";
    return 1;
  }

  maigent::Envelope req;
  const std::string request_id = maigent::MakeRequestId("external-process");
  maigent::FillHeader(&req, maigent::COMMAND,
                      do_register ? maigent::MK_EXTERNAL_PROCESS_REGISTER
                                  : maigent::MK_EXTERNAL_PROCESS_UNREGISTER,
                      "external_process_client", sender_id, request_id,
                      maigent::MakeTraceId(), request_id);

  const char* subject = nullptr;
  if (do_register) {
    subject = maigent::kSubjectCmdExternalProcessRegister;
    auto* payload = req.mutable_command()->mutable_external_process_register();
    payload->set_pid(register_pid);
    payload->set_label(maigent::GetFlagValue(argc, argv, "--label", ""));
    payload->set_external_id(
        maigent::GetFlagValue(argc, argv, "--external-id", ""));
    payload->set_priority(maigent::GetFlagInt(argc, argv, "--priority", 0));
    payload->set_cgroup_path(
        maigent::GetFlagValue(argc, argv, "--cgroup-path", ""));
    payload->set_allow_control(maigent::HasFlag(argc, argv, "--allow-control"));
  } else {
    subject = maigent::kSubjectCmdExternalProcessUnregister;
    auto* payload =
        req.mutable_command()->mutable_external_process_unregister();
    payload->set_pid(unregister_pid);
    payload->set_target_id(unregister_target_id);
  }

  maigent::Envelope reply;
  std::string error;
  if (!nats.RequestEnvelope(subject, req, 2000, &reply, &error)) {
    std::cerr << "request failed: " << error << '\n';
    return 1;
  }

  if (do_register) {
    if (!reply.has_service() ||
        !reply.service().has_external_process_register_result()) {
      std::cerr << "unexpected register reply\n";
      return 1;
    }
    const auto& result = reply.service().external_process_register_result();
    std::cout << "success=" << result.success()
              << " pid=" << result.pid()
              << " target_id=" << result.target_id()
              << " reason=\"" << result.reason() << "\"\n";
    return result.success() ? 0 : 1;
  }

  if (!reply.has_service() ||
      !reply.service().has_external_process_unregister_result()) {
    std::cerr << "unexpected unregister reply\n";
    return 1;
  }
  const auto& result = reply.service().external_process_unregister_result();
  std::cout << "success=" << result.success()
            << " pid=" << result.pid()
            << " target_id=" << result.target_id()
            << " reason=\"" << result.reason() << "\"\n";
  return result.success() ? 0 : 1;
}
