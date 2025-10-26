#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>

using json = nlohmann::json;

#define BLUE   "\033[1;34m"
#define RESET  "\033[0m"

static ev_signal sigint_watcher;
static void sigint_cb(EV_P_ ev_signal *w, int revents) {
    std::cout << BLUE <<"[scheduler]" << RESET << " SIGINT, shutting down\n";
    ev_break(EV_A_ EVBREAK_ALL);
}

class Scheduler {
public:
    AMQP::TcpConnection *connection;
    AMQP::TcpChannel channel;

    Scheduler(AMQP::TcpConnection *conn)
        : connection(conn), channel(conn) {
        channel.declareQueue("scheduler.requests");

        channel.consume("scheduler.requests").onReceived(
            [&](const AMQP::Message &message, uint64_t tag, bool redelivered) {
                handle_request(message, tag);
            });

        std::cout << BLUE << "[scheduler]" << RESET
                  << " Waiting for tasks..." << std::endl;
    }

    void handle_request(const AMQP::Message &message, uint64_t tag) {
        std::string body(message.body(), message.bodySize());
        json msg;

        try {
            msg = json::parse(body);
        } catch (...) {
            std::cerr << BLUE << "[scheduler]" << RESET
                      << " Bad JSON message" << std::endl;
            channel.ack(tag);
            return;
        }

        std::cout << BLUE << "[scheduler]" << RESET
                  << " Got: " << msg.dump() << std::endl;

        std::this_thread::sleep_for(std::chrono::seconds(1));

        json result = {
            {"from", "scheduler"},
            {"status", "scheduled"},
            {"task", msg.value("payload", json::object())},
            {"reply_to", msg.value("reply_to", "agent.responses")}
        };

        channel.publish("", "agent.responses", result.dump());
        std::cout << BLUE << "[scheduler]" << RESET
                  << " Sent response: " << result.dump() << std::endl;

        channel.ack(tag);
    }
};

int main() {
    struct ev_loop *loop = EV_DEFAULT;
    AMQP::LibEvHandler handler(loop);

    AMQP::Address address("amqp://localhost/");
    AMQP::TcpConnection connection(&handler, address);

    Scheduler scheduler(&connection);

    std::cout << BLUE << "[scheduler]" << RESET << " Event loop started"
              << std::endl;

    ev_signal_init(&sigint_watcher, sigint_cb, SIGINT);
    ev_signal_start(loop, &sigint_watcher);
    ev_run(loop, 0);

    std::cout << BLUE << "[scheduler]" << RESET << " Event loop stopped"
              << std::endl;

    connection.close();
    return 0;
}
