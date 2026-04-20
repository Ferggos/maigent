#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <nats/nats.h>

#include "maigent.pb.h"

namespace maigent {

struct NatsMessage {
  std::string subject;
  std::string reply;
  std::string data;
};

using NatsCallback = std::function<void(const NatsMessage&)>;

class NatsClient {
 public:
  NatsClient();
  ~NatsClient();

  bool Connect(const std::string& url, const std::string& client_name = "");

  bool Publish(const std::string& subject, const std::string& data);
  bool PublishEnvelope(const std::string& subject, const Envelope& env);

  bool RequestEnvelope(const std::string& subject, const Envelope& request,
                       int timeout_ms, Envelope* reply,
                       std::string* error = nullptr);

  bool Subscribe(const std::string& subject, NatsCallback callback);
  bool QueueSubscribe(const std::string& subject, const std::string& queue,
                      NatsCallback callback);

  bool RespondEnvelope(const NatsMessage& incoming, const Envelope& reply);

  void Flush(int timeout_ms = 1000);

 private:
  struct SubscriptionContext {
    NatsCallback callback;
  };

  static void OnMessage(natsConnection* nc, natsSubscription* sub, natsMsg* msg,
                        void* closure);

  bool SubscribeImpl(const std::string& subject, const std::string& queue,
                     NatsCallback callback);
  void Close();

  natsConnection* nc_;
  natsOptions* opts_;
  std::vector<std::unique_ptr<SubscriptionContext>> sub_ctx_;
  std::vector<natsSubscription*> subscriptions_;
};

}  // namespace maigent
