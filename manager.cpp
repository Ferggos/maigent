#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <amqpcpp/libev.h>
#include <iostream>
#include <thread>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

class MyHandler : public AMQP::TcpHandler {
public:
    void onConnected(AMQP::TcpConnection *connection) override {
        std::cout << "[manager] Connected to RabbitMQ" << std::endl;
    }

    void onError(AMQP::TcpConnection *connection, const char *message) override {
        std::cerr << "[manager] Error: " << message << std::endl;
    }

    void monitor(AMQP::TcpConnection *connection, int fd, int flags) override {

        (void)connection;
        (void)fd;
        (void)flags;
    }

    void onHeartbeat(AMQP::TcpConnection *connection) override {
    }
};


int main() {
    auto *loop = EV_DEFAULT;
    AMQP::LibEvHandler handler(loop);
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://localhost/"));
    AMQP::TcpChannel channel(&connection);

    channel.declareQueue("manager.requests");
    channel.declareQueue("system_monitor.requests");
    channel.declareQueue("scheduler.requests");
    channel.declareQueue("agent.responses");

    std::cout << "[manager] Waiting for messages..." << std::endl;
    /*channel.declareQueue(AMQP::exclusive).onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount) {

        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;

        // now we can close the connection
        connection.close();
    });*/
    channel.consume("manager.requests").onReceived(
        [&](const AMQP::Message &message, uint64_t tag, bool redelivered) {
            std::string body(message.body(), message.bodySize());
            json req = json::parse(body, nullptr, false);

            if (!req.is_object()) {
                std::cerr << "[manager] Bad message" << std::endl;
                channel.ack(tag);
                return;
            }

            std::cout << "[manager] Received request: " << req.dump() << std::endl;

            json sys_req = {
                {"action", "check"},
                {"payload", req["payload"]},
                {"reply_to", "manager.responses"}};

            channel.publish("", "system_monitor.requests", sys_req.dump());

            json sched_req = {
                {"action", "schedule"},
                {"payload", req["payload"]},
                {"reply_to", "manager.responses"}};

            channel.publish("", "scheduler.requests", sched_req.dump());

            json reply = {
                {"status", "accepted"},
                {"request_id", req["request_id"]}};
            channel.publish("", "agent.responses", reply.dump());

            channel.ack(tag);
        });
    
        ev_run(loop, 0);

        return 0;
}
