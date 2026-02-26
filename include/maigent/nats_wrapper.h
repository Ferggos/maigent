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

  bool Connect(const std::string& url, const std::string& name = "");

  bool Publish(const std::string& subject, const std::string& data);
  bool PublishEnvelope(const std::string& subject, const maigent::Envelope& env);

  bool RequestEnvelope(const std::string& subject, const maigent::Envelope& req,
                       int timeout_ms, maigent::Envelope* reply,
                       std::string* error = nullptr);

  bool Subscribe(const std::string& subject, NatsCallback cb);
  bool QueueSubscribe(const std::string& subject, const std::string& queue,
                      NatsCallback cb);

  bool RespondEnvelope(const NatsMessage& incoming, const maigent::Envelope& env);

  void Flush(int timeout_ms = 1000);

 private:
  struct SubscriptionContext {
    NatsCallback callback;
  };

  static void OnMessage(natsConnection* nc, natsSubscription* sub, natsMsg* msg,
                        void* closure);

  bool SubscribeImpl(const std::string& subject, const std::string& queue,
                     NatsCallback cb);
  void Close();

  natsConnection* nc_;
  natsOptions* opts_;
  std::vector<std::unique_ptr<SubscriptionContext>> sub_ctx_;
  std::vector<natsSubscription*> subscriptions_;
};

}  // namespace maigent
