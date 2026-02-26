# MasIGent V1 (NATS + Protobuf + C++)

Рабочий локальный прототип многоагентной надстройки управления ресурсами Linux-машины.

## Что реализовано

Роли (все на C++):
- `task_manager`
- `lease_authority`
- `system_monitor`
- `planner_agent`
- `executor_agent` (queue group `exec`, можно запускать несколько экземпляров)
- `metrics_collector`
- `directory_agent`

Инструменты:
- `bench_submitter` (нагрузка submit-запросами)
- `system_status_client` (проверка `cmd.system.status` через `TaskManager`)

Транспорт/формат:
- NATS (`libnats` / nats.c)
- Protobuf (`proto/maigent.proto`, только protobuf-сообщения)

## Структура

- `proto/maigent.proto` — единая схема сообщений
- `include/maigent/*` — общие заголовки
- `src/common/*` — transport/utils/lifecycle
- `src/*.cpp` — main-файлы агентов
- `tools/*.cpp` — bench/status clients
- `scripts/run_all.sh` — запуск всех агентов
- `derived/metrics_report.txt` — отчёт `MetricsCollector`
- `legacy/` — старый код, убран из новой сборки

## Зависимости (Arch Linux)

Установите:

```bash
sudo pacman -S --needed cmake gcc make protobuf pkgconf nats-server nats.c
```

## Сборка

```bash
cmake -S . -B build
cmake --build build -j
```

Protobuf C++ код генерируется в `build/gen`.

## Запуск

### 1) Запустить NATS (если не используете `run_all.sh`)

```bash
nats-server -a 127.0.0.1 -p 4222
```

### 2) Запустить всех агентов

```bash
./scripts/run_all.sh
```

Скрипт:
- проверяет доступность `127.0.0.1:4222`
- поднимает `nats-server` при необходимости
- запускает всех агентов (включая 2 `executor_agent` по умолчанию)
- пишет логи в `logs/*.log`

Настройка количества executor-воркеров:

```bash
EXECUTOR_COUNT=4 ./scripts/run_all.sh
```

## Проверка статуса системы

`TaskManager` проксирует snapshot из `DirectoryAgent` в `cmd.system.status`:

```bash
./build/system_status_client --nats nats://127.0.0.1:4222
```

## Benchmark

Целевой прогон:

```bash
./build/bench_submitter \
  --n 500 \
  --concurrency 50 \
  --cmd /bin/sleep \
  --args 0.01 \
  --wait-mode wait_finish \
  --nats nats://127.0.0.1:4222
```

`bench_submitter`:
- отправляет `evt.bench.start` / `evt.bench.stop`
- делает request/reply в `cmd.task.submit` с ограничением inflight по `--concurrency`
- печатает `sent/completed/denied/failed/timeouts`

## MetricsCollector

`MetricsCollector` подписан на `evt.*` и печатает отчёт:
- throughput tasks/sec
- latency `submit->start` (min/p50/p95/p99/avg/max)
- latency `submit->finish` (min/p50/p95/p99/avg/max)
- stage `reserve_latency_ms` (если есть timestamps)
- stage `launch_latency_ms` (если есть timestamps)
- `run_ms`
- counts `accepted/denied/failed/timeout/completed`
- `slowest_top10`

Отчёт сохраняется в:

- `derived/metrics_report.txt`

## Subjects

Команды:
- `cmd.task.submit`
- `cmd.lease.reserve`
- `cmd.lease.commit`
- `cmd.lease.release`
- `cmd.exec.launch` (queue group `exec`)
- `cmd.exec.control` (queue group `exec`)
- `cmd.dir.snapshot`
- `cmd.system.status`

События:
- `evt.task.submitted`
- `evt.task.started`
- `evt.task.finished`
- `evt.task.failed`
- `evt.lease.denied`
- `evt.control.applied`
- `evt.bench.start`
- `evt.bench.stop`
- `agent.register`
- `agent.heartbeat`
- `agent.goodbye`

Состояние:
- `state.pressure`
- `state.forecast`

## Примечания по V1

- `TaskManager` не выбирает конкретный executor: доставка `cmd.exec.launch` идёт через NATS queue group.
- `DirectoryAgent` не участвует в горячем пути маршрутизации задач.
- `LeaseAuthority` реализует idempotency по `request_id` и lease state machine (RESERVED/COMMITTED/RELEASED/EXPIRED).
- `ExecutorAgent` выполняет процессы через `posix_spawnp` + `waitpid`, публикует `started/finished/failed` и делает `cmd.lease.release`.
