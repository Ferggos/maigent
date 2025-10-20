#!/usr/bin/env python3
import pika, json, time, uuid

def main():
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()

    ch.queue_declare(queue="manager.requests")
    ch.queue_declare(queue="agent.responses")

    req = {
        "request_id": str(uuid.uuid4()),
        "payload": {"cpu": 10, "ram": 128, "name": "demo_agent"}
    }

    ch.basic_publish(exchange="", routing_key="manager.requests", body=json.dumps(req))
    print(f"[create_agent] Sent request: {req}")

    def on_message(ch_, method, properties, body):
        print(f"[create_agent] Got response: {body.decode()}")
        ch_.basic_ack(method.delivery_tag)

    ch.basic_consume(queue="agent.responses", on_message_callback=on_message)
    print("[create_agent] Waiting for response...")
    ch.start_consuming()

if __name__ == "__main__":
    main()
