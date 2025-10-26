#!/usr/bin/env python3
import pika, json, time, psutil, socket, signal, sys

def shutdown(signum, frame):
    print("\033[1;32m[system_monitor]\033[0m shutting down")
    try:
        ch.stop_consuming()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(0)


def get_cpu_usage(interval=0.5):
    return psutil.cpu_percent(interval=interval)

def get_free_ram_mb():
    vm = psutil.virtual_memory()
    return vm.available / (1024 * 1024)

def main():
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()

    ch.queue_declare(queue="system_monitor.requests")

    def callback(ch_, method, properties, body):
        try:
            msg = json.loads(body)
        except Exception as e:
            print(f"\033[1;32m[system_monitor]\033[0m Bad message: {e}")
            ch_.basic_ack(method.delivery_tag)
            return

        print(f"\033[1;32m[system_monitor]\033[0m Got request: {msg}")

        req = msg.get("payload", {})
        need_cpu = float(req.get("cpu", 10))
        need_ram = float(req.get("ram", 128))

        cpu_load = get_cpu_usage(0.5)
        free_ram = get_free_ram_mb()

        print(f"\033[1;32m[system_monitor]\033[0m CPU load={cpu_load:.1f}%, free RAM={free_ram:.1f}MB")

        if cpu_load + need_cpu < 100 and free_ram > need_ram:
            status = "ok"
            msg_text = "Resources available"
        else:
            status = "denied"
            msg_text = "Not enough resources"

        result = {
            "from": "system_monitor",
            "status": status,
            "cpu_load": cpu_load,
            "free_ram": free_ram,
            "message": msg_text,
            "reply_to": msg.get("reply_to", "agent.responses"),
            "request_id": msg.get("request_id"),
            "hostname": socket.gethostname()
        }


        ch_.basic_publish(exchange="", routing_key=result["reply_to"], body=json.dumps(result))
        ch_.basic_ack(method.delivery_tag)
        print(f"\033[1;32m[system_monitor]\033[0m Sent response: {result}")

    ch.basic_consume(queue="system_monitor.requests", on_message_callback=callback)
    print("\033[1;32m[system_monitor]\033[0m Waiting for tasks...")
    ch.start_consuming()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    main()
