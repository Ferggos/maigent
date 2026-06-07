#!/usr/bin/env python3
import argparse
import time


def touch_block(block: bytearray, step: int = 4096) -> None:
    # Трогаем каждую страницу, чтобы память реально стала resident,
    # а не просто была лениво выделена виртуально.
    for i in range(0, len(block), step):
        block[i] = 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-mb", type=int, default=2200)
    parser.add_argument("--step-mb", type=int, default=100)
    parser.add_argument("--step-delay", type=float, default=5.0)
    parser.add_argument("--hold-sec", type=float, default=60.0)
    args = parser.parse_args()

    blocks = []
    allocated = 0

    print(f"[INFO] target={args.target_mb} MB, step={args.step_mb} MB")

    try:
        while allocated < args.target_mb:
            current_step = min(args.step_mb, args.target_mb - allocated)
            block = bytearray(current_step * 1024 * 1024)
            touch_block(block)
            blocks.append(block)

            allocated += current_step
            print(f"[INFO] allocated={allocated} MB")
            time.sleep(args.step_delay)

        print(f"[INFO] target reached, hold {args.hold_sec} sec")
        time.sleep(args.hold_sec)

    except KeyboardInterrupt:
        print("\n[INFO] interrupted")

    print("[INFO] freeing memory")
    blocks.clear()
    time.sleep(5)


if __name__ == "__main__":
    main()
