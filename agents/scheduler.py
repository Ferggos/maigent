#!/usr/bin/env python3
import pika, json, time, signal, sys

def shutdown(signum, frame):
    print("\033[1;34m[scheduler]\033[0m shutting down") 
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

    ch.queue_declare(queue="scheduler.requests")

    def callback(ch_, method, properties, body):
        msg = json.loads(body)
        print(f"\033[1;34m[scheduler]\033[0m Got: {msg}")
        time.sleep(1)
        result = {
            "from": "scheduler",
            "status": "scheduled",
            "task": msg["payload"],
            "reply_to": msg.get("reply_to", "agent.responses")
        }
        ch_.basic_publish(exchange="", routing_key=result["reply_to"], body=json.dumps(result))
        ch_.basic_ack(method.delivery_tag)

    ch.basic_consume(queue="scheduler.requests", on_message_callback=callback)
    print("\033[1;34m[scheduler]\033[0m Waiting for tasks...")
    ch.start_consuming()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    main()
