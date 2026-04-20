# Maigent Queue (Local Multi-Agent Resource Control)

Локальная многоагентная система управления задачами и ресурсами узла на:
- C++
- CMake
- NATS
- Protocol Buffers


## Components

- `task_manager`
- `lease_authority`
- `system_monitor_agent`
- `planner_agent`
- `task_executor`
- `actuator`
- `directory`
- `metrics_collector`

Дополнительно:
- `bench_submitter`
- `system_status_client`

## System Monitor Model Contract

Внутри `system_monitor_agent` введён стабильный internal model-layer contract:
- `SystemMonitorModelInput` (host snapshot + target snapshots + pressure history)
- `SystemMonitorModelOutput` (pressure/forecast/capacity/targets model result)

Наружные runtime публикации остаются прежними: `PressureState`, `ForecastState`, `CapacityState`, `TargetsState`.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

## Run

1. Запустить `nats-server`:
```bash
nats-server -a 127.0.0.1 -p 4222
```

2. Запустить все агенты:
```bash
./scripts/run_all.sh
```

Логи рантайма пишутся в `logs/runtime_*.log`, агентские логи в `logs/*.log`.

## Bench

Пример нагрузки:

```bash
./build/bench_submitter \
  --n 500 \
  --concurrency 50 \
  --cmd /bin/sleep \
  --args 0.01 \
  --wait-mode wait_finish \
  --nats nats://127.0.0.1:4222
```

Bench lifecycle сообщения:
- `msg.service.bench.start` (`MK_BENCH_START`)
- `msg.service.bench.stop` (`MK_BENCH_STOP`)

## Metrics / Reports

`metrics_collector_agent` пишет отчёты в `reports/<bench_id>/`:
- `events.jsonl`
- `latencies.csv`
- `system_state.csv`
- `summary.txt`

В `summary.txt` есть:
- stage counters
- latency p50/p95/p99
- queue metrics
- `per_agent_event_counts`
- `tasks_started_by_executor`
- `tasks_finished_by_executor`
- category breakdown:
  - `command_messages_seen`
  - `event_messages_seen`
  - `state_messages_seen`
  - `service_messages_seen`
