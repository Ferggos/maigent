#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <unordered_map>

#define RESET   "\033[0m"
#define MAGENTA "\033[1;35m"

using json = nlohmann::json;

static ev_signal sigint_watcher;
static void sigint_cb(EV_P_ ev_signal *w, int revents) {
    std::cout << MAGENTA <<"[manager]" << RESET << " SIGINT, shutting down\n";
    ev_break(EV_A_ EVBREAK_ALL);
}

class Manager {
public:
    AMQP::TcpConnection *connection;
    AMQP::TcpChannel consume_channel;
    AMQP::TcpChannel publish_channel;
    std::unordered_map<std::string, json> pending_requests;

    Manager(AMQP::TcpConnection *conn)
        : connection(conn),
          consume_channel(conn),
          publish_channel(conn)
    {
        consume_channel.declareQueue("manager.requests");
        consume_channel.declareQueue("agent.responses");

        consume_channel.consume("manager.requests").onReceived(
            [&](const AMQP::Message &message, uint64_t tag, bool redelivered) {
                handle_request(message, tag);
            });

        consume_channel.consume("agent.responses").onReceived(
            [&](const AMQP::Message &message, uint64_t tag, bool redelivered) {
                handle_response(message, tag);
            });

        std::cout << MAGENTA <<"[manager]" << RESET << " Waiting for messages..." << std::endl;
    }

    void handle_request(const AMQP::Message &message, uint64_t tag) {
        std::string body(message.body(), message.bodySize());
        json req = json::parse(body, nullptr, false);

        if (!req.is_object() || !req.contains("request_id")) {
            std::cerr << MAGENTA <<"[manager]" << RESET << " Bad message" << std::endl;
            consume_channel.ack(tag);
            return;
        }

        std::string req_id = req["request_id"];
        pending_requests[req_id] = req;
        std::cout << MAGENTA <<"[manager]" << RESET << " Got new request " << req_id << std::endl;

        json sys_req = {
            {"action", "check"},
            {"payload", req["payload"]},
            {"reply_to", "agent.responses"},
            {"request_id", req_id}
        };

        publish_channel.publish("", "system_monitor.requests", sys_req.dump());
        consume_channel.ack(tag);
    }

    void handle_response(const AMQP::Message &message, uint64_t tag) {
        std::string body(message.body(), message.bodySize());
        json resp = json::parse(body, nullptr, false);

        if (!resp.is_object() || !resp.contains("request_id")) {
            consume_channel.ack(tag);
            return;
        }

        std::string req_id = resp["request_id"];
        if (!pending_requests.count(req_id)) {
            consume_channel.ack(tag);
            return;
        }

        auto &orig = pending_requests[req_id];
        std::string status = resp.value("status", "");
        std::string from = resp.value("from", "");

        if (from == "system_monitor") {
            if (status == "ok") {
                std::cout << MAGENTA <<"[manager]" << RESET << " System OK, sending to scheduler" << std::endl;
                json sched_req = {
                    {"action", "schedule"},
                    {"payload", orig["payload"]},
                    {"reply_to", "agent.responses"},
                    {"request_id", req_id}
                };
                publish_channel.publish("", "scheduler.requests", sched_req.dump());
            } else {
                std::cout << MAGENTA <<"[manager]" << RESET << " System check failed" << std::endl;
                json reply = {
                    {"status", "rejected"},
                    {"request_id", req_id}
                };
                publish_channel.publish("", "agent.responses", reply.dump());
                pending_requests.erase(req_id);
            }
        } else if (from == "scheduler") {
            std::cout << MAGENTA <<"[manager]" << RESET << " Scheduler done, sending final OK" << std::endl;
            json reply = {
                {"status", "accepted"},
                {"request_id", req_id}
            };
            publish_channel.publish("", "agent.responses", reply.dump());
            pending_requests.erase(req_id);
        }

        consume_channel.ack(tag);
    }
};

int main() {
    struct ev_loop *loop = EV_DEFAULT;
    AMQP::LibEvHandler handler(loop);

    AMQP::Address address("amqp://localhost/");
    AMQP::TcpConnection connection(&handler, address);

    Manager manager(&connection);

    std::cout << MAGENTA <<"[manager]" << RESET << " Event loop started" << std::endl;
    ev_signal_init(&sigint_watcher, sigint_cb, SIGINT);
    ev_signal_start(loop, &sigint_watcher);
    ev_run(loop, 0);
    std::cout << MAGENTA <<"[manager]" << RESET << " Event loop stopped" << std::endl;
    connection.close();
    return 0;
}
