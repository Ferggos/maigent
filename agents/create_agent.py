import pika, json, time, uuid, signal, sys

def shutdown(signum, frame):
    print("\033[1;33m[create_agent]\033[0m shutting down")
    try:
        ch.stop_consuming()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(0)


def main():
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()

    ch.queue_declare(queue="manager.requests")
    ch.queue_declare(queue="agent.responses")

    req = {
        "request_id": str(uuid.uuid4()),
        "payload": {"cpu": 10, "ram": sys.argv[1], "name": "demo_agent"}
    }

    ch.basic_publish(exchange="", routing_key="manager.requests", body=json.dumps(req))
    print(f"\033[1;33m[create_agent]\033[0m Sent request: {req}")

    def on_message(ch_, method, properties, body):
        print(f"\033[1;33m[create_agent]\033[0m Got response: {body.decode()}")
        ch_.basic_ack(method.delivery_tag)

    ch.basic_consume(queue="agent.responses", on_message_callback=on_message)
    print("\033[1;33m[create_agent]\033[0m Waiting for response...")
    ch.start_consuming()



if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    main()
