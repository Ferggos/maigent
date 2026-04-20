#include "maigent/common/nats_wrapper.h"

#include <iostream>

#include "maigent/common/message_helpers.h"

namespace maigent {

NatsClient::NatsClient() : nc_(nullptr), opts_(nullptr) { nats_Open(-1); }

NatsClient::~NatsClient() {
  Close();
  nats_Close();
}

bool NatsClient::Connect(const std::string& url, const std::string& client_name) {
  if (nc_ != nullptr) {
    return true;
  }

  natsStatus s = natsOptions_Create(&opts_);
  if (s != NATS_OK) {
    std::cerr << "natsOptions_Create failed: " << natsStatus_GetText(s) << '\n';
    return false;
  }

  s = natsOptions_SetURL(opts_, url.c_str());
  if (s != NATS_OK) {
    std::cerr << "natsOptions_SetURL failed: " << natsStatus_GetText(s) << '\n';
    return false;
  }

  if (!client_name.empty()) {
    s = natsOptions_SetName(opts_, client_name.c_str());
    if (s != NATS_OK) {
      std::cerr << "natsOptions_SetName failed: " << natsStatus_GetText(s) << '\n';
      return false;
    }
  }

  s = natsConnection_Connect(&nc_, opts_);
  if (s != NATS_OK) {
    std::cerr << "natsConnection_Connect failed: " << natsStatus_GetText(s) << '\n';
    return false;
  }

  return true;
}

bool NatsClient::Publish(const std::string& subject, const std::string& data) {
  if (nc_ == nullptr) {
    return false;
  }
  const natsStatus s =
      natsConnection_Publish(nc_, subject.c_str(), data.data(), static_cast<int>(data.size()));
  if (s != NATS_OK) {
    std::cerr << "publish failed subject=" << subject
              << " err=" << natsStatus_GetText(s) << '\n';
    return false;
  }
  return true;
}

bool NatsClient::PublishEnvelope(const std::string& subject, const Envelope& env) {
  std::string payload;
  if (!SerializeEnvelope(env, &payload)) {
    return false;
  }
  return Publish(subject, payload);
}

bool NatsClient::RequestEnvelope(const std::string& subject, const Envelope& request,
                                 int timeout_ms, Envelope* reply,
                                 std::string* error) {
  if (nc_ == nullptr) {
    if (error != nullptr) {
      *error = "nats not connected";
    }
    return false;
  }

  std::string payload;
  if (!SerializeEnvelope(request, &payload)) {
    if (error != nullptr) {
      *error = "request serialization failed";
    }
    return false;
  }

  natsMsg* msg = nullptr;
  const natsStatus s =
      natsConnection_Request(&msg, nc_, subject.c_str(), payload.data(),
                             static_cast<int>(payload.size()), timeout_ms);
  if (s != NATS_OK) {
    if (error != nullptr) {
      *error = natsStatus_GetText(s);
    }
    return false;
  }

  const bool parsed = ParseEnvelope(natsMsg_GetData(msg), natsMsg_GetDataLength(msg), reply);
  natsMsg_Destroy(msg);
  if (!parsed && error != nullptr) {
    *error = "reply parse failed";
  }
  return parsed;
}

bool NatsClient::Subscribe(const std::string& subject, NatsCallback callback) {
  return SubscribeImpl(subject, "", std::move(callback));
}

bool NatsClient::QueueSubscribe(const std::string& subject, const std::string& queue,
                                NatsCallback callback) {
  return SubscribeImpl(subject, queue, std::move(callback));
}

bool NatsClient::RespondEnvelope(const NatsMessage& incoming, const Envelope& reply) {
  if (incoming.reply.empty() || nc_ == nullptr) {
    return false;
  }

  std::string payload;
  if (!SerializeEnvelope(reply, &payload)) {
    return false;
  }

  const natsStatus s = natsConnection_Publish(nc_, incoming.reply.c_str(), payload.data(),
                                               static_cast<int>(payload.size()));
  return s == NATS_OK;
}

void NatsClient::Flush(int timeout_ms) {
  if (nc_ != nullptr) {
    natsConnection_FlushTimeout(nc_, timeout_ms);
  }
}

void NatsClient::OnMessage(natsConnection* /*nc*/, natsSubscription* /*sub*/, natsMsg* msg,
                           void* closure) {
  auto* ctx = static_cast<SubscriptionContext*>(closure);
  if (ctx != nullptr && ctx->callback) {
    NatsMessage incoming;
    const char* subject = natsMsg_GetSubject(msg);
    if (subject != nullptr) {
      incoming.subject = subject;
    }
    const char* reply = natsMsg_GetReply(msg);
    if (reply != nullptr) {
      incoming.reply = reply;
    }
    const char* data = natsMsg_GetData(msg);
    const int len = natsMsg_GetDataLength(msg);
    if (data != nullptr && len > 0) {
      incoming.data.assign(data, data + len);
    }
    ctx->callback(incoming);
  }
  natsMsg_Destroy(msg);
}

bool NatsClient::SubscribeImpl(const std::string& subject, const std::string& queue,
                               NatsCallback callback) {
  if (nc_ == nullptr) {
    return false;
  }

  auto ctx = std::make_unique<SubscriptionContext>();
  ctx->callback = std::move(callback);

  natsSubscription* sub = nullptr;
  natsStatus s;
  if (queue.empty()) {
    s = natsConnection_Subscribe(&sub, nc_, subject.c_str(), OnMessage, ctx.get());
  } else {
    s = natsConnection_QueueSubscribe(&sub, nc_, subject.c_str(), queue.c_str(), OnMessage,
                                      ctx.get());
  }

  if (s != NATS_OK) {
    std::cerr << "subscribe failed subject=" << subject
              << " err=" << natsStatus_GetText(s) << '\n';
    return false;
  }

  sub_ctx_.push_back(std::move(ctx));
  subscriptions_.push_back(sub);
  return true;
}

void NatsClient::Close() {
  for (auto* sub : subscriptions_) {
    natsSubscription_Destroy(sub);
  }
  subscriptions_.clear();
  sub_ctx_.clear();

  if (nc_ != nullptr) {
    natsConnection_Destroy(nc_);
    nc_ = nullptr;
  }
  if (opts_ != nullptr) {
    natsOptions_Destroy(opts_);
    opts_ = nullptr;
  }
}

}  // namespace maigent
