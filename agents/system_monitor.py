#!/usr/bin/env python3
import pika, json, random, time

def main():
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()

    ch.queue_declare(queue="system_monitor.requests")

    def callback(ch_, method, properties, body):
        msg = json.loads(body)
        print(f"[system_monitor] Got: {msg}")
        # Имитируем проверку
        time.sleep(1)
        result = {
            "status": "ok",
            "load": random.randint(10, 90),
            "reply_to": msg.get("reply_to", "agent.responses")
        }
        ch_.basic_publish(exchange="", routing_key=result["reply_to"], body=json.dumps(result))
        ch_.basic_ack(method.delivery_tag)

    ch.basic_consume(queue="system_monitor.requests", on_message_callback=callback)
    print("[system_monitor] Waiting for tasks...")
    ch.start_consuming()

if __name__ == "__main__":
    main()
