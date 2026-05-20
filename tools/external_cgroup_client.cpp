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
      << "  external_cgroup_client --register-cgroup PATH [--label NAME] "
         "[--external-id ID] [--priority N] [--allow-control] [--nats URL]\n"
      << "  external_cgroup_client --unregister-cgroup PATH [--nats URL]\n"
      << "  external_cgroup_client --unregister-target-id TARGET_ID [--nats URL]\n";
}

}  // namespace

int main(int argc, char** argv) {
  const std::string nats_url =
      maigent::GetFlagValue(argc, argv, "--nats", "nats://127.0.0.1:4222");
  const std::string register_cgroup =
      maigent::GetFlagValue(argc, argv, "--register-cgroup", "");
  const std::string unregister_cgroup =
      maigent::GetFlagValue(argc, argv, "--unregister-cgroup", "");
  const std::string unregister_target_id =
      maigent::GetFlagValue(argc, argv, "--unregister-target-id", "");

  const bool do_register = !register_cgroup.empty();
  const bool do_unregister =
      !unregister_cgroup.empty() || !unregister_target_id.empty();
  if (do_register == do_unregister) {
    PrintUsage();
    return 2;
  }

  maigent::NatsClient nats;
  const std::string sender_id = "external-cgroup-client-" + maigent::MakeUuid();
  if (!nats.Connect(nats_url, sender_id)) {
    std::cerr << "failed to connect to NATS\n";
    return 1;
  }

  maigent::Envelope req;
  const std::string request_id = maigent::MakeRequestId("external-cgroup");
  maigent::FillHeader(&req, maigent::COMMAND,
                      do_register ? maigent::MK_EXTERNAL_CGROUP_REGISTER
                                  : maigent::MK_EXTERNAL_CGROUP_UNREGISTER,
                      "external_cgroup_client", sender_id, request_id,
                      maigent::MakeTraceId(), request_id);

  const char* subject = nullptr;
  if (do_register) {
    subject = maigent::kSubjectCmdExternalCgroupRegister;
    auto* payload = req.mutable_command()->mutable_external_cgroup_register();
    payload->set_cgroup_path(register_cgroup);
    payload->set_label(maigent::GetFlagValue(argc, argv, "--label", ""));
    payload->set_external_id(
        maigent::GetFlagValue(argc, argv, "--external-id", ""));
    payload->set_priority(maigent::GetFlagInt(argc, argv, "--priority", 0));
    payload->set_allow_control(maigent::HasFlag(argc, argv, "--allow-control"));
  } else {
    subject = maigent::kSubjectCmdExternalCgroupUnregister;
    auto* payload =
        req.mutable_command()->mutable_external_cgroup_unregister();
    payload->set_cgroup_path(unregister_cgroup);
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
        !reply.service().has_external_cgroup_register_result()) {
      std::cerr << "unexpected register reply\n";
      return 1;
    }
    const auto& result = reply.service().external_cgroup_register_result();
    std::cout << "success=" << result.success()
              << " cgroup_path=" << result.cgroup_path()
              << " target_id=" << result.target_id()
              << " reason=\"" << result.reason() << "\"\n";
    return result.success() ? 0 : 1;
  }

  if (!reply.has_service() ||
      !reply.service().has_external_cgroup_unregister_result()) {
    std::cerr << "unexpected unregister reply\n";
    return 1;
  }
  const auto& result = reply.service().external_cgroup_unregister_result();
  std::cout << "success=" << result.success()
            << " cgroup_path=" << result.cgroup_path()
            << " target_id=" << result.target_id()
            << " reason=\"" << result.reason() << "\"\n";
  return result.success() ? 0 : 1;
}
