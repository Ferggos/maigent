#!/usr/bin/env python3
# Растущая CPU-нагрузка с ПЕРЕГРУЗКОЙ ядер для создания реальной конкуренции и PSI.
# Добавляет busy-loop молотилки по одной до target_workers (по умолчанию 2x ядер),
# потом держит hold_sec. При target_workers > nproc возникает конкуренция за CPU,
# load average и PSI cpu растут — это нужно, чтобы риск дошёл до HIGH.
import argparse, multiprocessing, os, time


def burn():
    x = 0.0001
    while True:
        x = x * 1.0000001 + 0.0000001  # чистый busy-loop, 100% одного ядра


def main():
    ncpu = os.cpu_count() or 8
    p = argparse.ArgumentParser()
    # по умолчанию вдвое больше ядер — гарантированная конкуренция и PSI
    p.add_argument("--target-workers", type=int, default=ncpu * 2)
    p.add_argument("--step-delay", type=float, default=3.0)
    p.add_argument("--hold-sec", type=float, default=90.0)
    a = p.parse_args()

    procs = []
    print(f"[INFO] cpus={ncpu} target_workers={a.target_workers} "
          f"(перегрузка x{a.target_workers/ncpu:.1f}) step={a.step_delay}s hold={a.hold_sec}s",
          flush=True)
    try:
        for i in range(a.target_workers):
            pr = multiprocessing.Process(target=burn, daemon=True)
            pr.start()
            procs.append(pr)
            print(f"[INFO] workers={i+1}/{a.target_workers}", flush=True)
            time.sleep(a.step_delay)
        print(f"[INFO] target reached, hold {a.hold_sec}s", flush=True)
        time.sleep(a.hold_sec)
    except KeyboardInterrupt:
        print("[INFO] interrupted", flush=True)
    finally:
        print("[INFO] stopping workers", flush=True)
        for pr in procs:
            pr.terminate()
        time.sleep(2)


if __name__ == "__main__":
    main()
