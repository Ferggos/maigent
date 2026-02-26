#include "maigent/nats_wrapper.h"

#include <cstring>

#include "maigent/logging.h"
#include "maigent/message_utils.h"

namespace maigent {

NatsClient::NatsClient() : nc_(nullptr), opts_(nullptr) {
  nats_Open(-1);
}

NatsClient::~NatsClient() {
  Close();
  nats_Close();
}

bool NatsClient::Connect(const std::string& url, const std::string& name) {
  if (nc_ != nullptr) {
    return true;
  }

  natsStatus s = natsOptions_Create(&opts_);
  if (s != NATS_OK) {
    LogError("nats", std::string("natsOptions_Create failed: ") + natsStatus_GetText(s));
    return false;
  }

  s = natsOptions_SetURL(opts_, url.c_str());
  if (s != NATS_OK) {
    LogError("nats", std::string("natsOptions_SetURL failed: ") + natsStatus_GetText(s));
    return false;
  }

  if (!name.empty()) {
    s = natsOptions_SetName(opts_, name.c_str());
    if (s != NATS_OK) {
      LogError("nats", std::string("natsOptions_SetName failed: ") + natsStatus_GetText(s));
      return false;
    }
  }

  s = natsConnection_Connect(&nc_, opts_);
  if (s != NATS_OK) {
    LogError("nats",
             std::string("natsConnection_Connect failed: ") + natsStatus_GetText(s));
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
    LogError("nats",
             std::string("publish failed for ") + subject + ": " + natsStatus_GetText(s));
    return false;
  }
  return true;
}

bool NatsClient::PublishEnvelope(const std::string& subject,
                                 const maigent::Envelope& env) {
  std::string bytes;
  if (!SerializeEnvelope(env, &bytes)) {
    LogError("nats", "protobuf serialization failed");
    return false;
  }
  return Publish(subject, bytes);
}

bool NatsClient::RequestEnvelope(const std::string& subject,
                                 const maigent::Envelope& req,
                                 int timeout_ms, maigent::Envelope* reply,
                                 std::string* error) {
  if (nc_ == nullptr) {
    if (error != nullptr) {
      *error = "not connected";
    }
    return false;
  }

  std::string payload;
  if (!SerializeEnvelope(req, &payload)) {
    if (error != nullptr) {
      *error = "serialize failed";
    }
    return false;
  }

  natsMsg* msg = nullptr;
  const natsStatus s = natsConnection_Request(
      &msg, nc_, subject.c_str(), payload.data(), static_cast<int>(payload.size()), timeout_ms);
  if (s != NATS_OK) {
    if (error != nullptr) {
      *error = natsStatus_GetText(s);
    }
    return false;
  }

  const void* data = natsMsg_GetData(msg);
  const int len = natsMsg_GetDataLength(msg);
  const bool ok = ParseEnvelope(data, len, reply);
  natsMsg_Destroy(msg);

  if (!ok && error != nullptr) {
    *error = "reply parse failed";
  }
  return ok;
}

bool NatsClient::Subscribe(const std::string& subject, NatsCallback cb) {
  return SubscribeImpl(subject, "", std::move(cb));
}

bool NatsClient::QueueSubscribe(const std::string& subject, const std::string& queue,
                                NatsCallback cb) {
  return SubscribeImpl(subject, queue, std::move(cb));
}

bool NatsClient::RespondEnvelope(const NatsMessage& incoming,
                                 const maigent::Envelope& env) {
  if (incoming.reply.empty()) {
    return false;
  }
  std::string payload;
  if (!SerializeEnvelope(env, &payload)) {
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

void NatsClient::OnMessage(natsConnection* /*nc*/, natsSubscription* /*sub*/,
                           natsMsg* msg, void* closure) {
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
                               NatsCallback cb) {
  if (nc_ == nullptr) {
    return false;
  }

  auto ctx = std::make_unique<SubscriptionContext>();
  ctx->callback = std::move(cb);

  natsSubscription* sub = nullptr;
  natsStatus s;
  if (queue.empty()) {
    s = natsConnection_Subscribe(&sub, nc_, subject.c_str(), OnMessage, ctx.get());
  } else {
    s = natsConnection_QueueSubscribe(&sub, nc_, subject.c_str(), queue.c_str(), OnMessage,
                                      ctx.get());
  }

  if (s != NATS_OK) {
    LogError("nats", std::string("subscribe failed for ") + subject + ": " +
                         natsStatus_GetText(s));
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
