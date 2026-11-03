#!/usr/bin/env python3
"""Wall-clock gate for supercronic jobs.

supercronic uses Go's monotonic timer to wait out a schedule.  When the
wall clock is stepped backwards after a WSL host resume, a long-interval
job can fire early on the wall clock.  This gate re-anchors the trigger to
the wall-clock schedule: if the job was started well before its scheduled
minute, it waits until that minute; otherwise it runs immediately.

Usage:
  cron_gate.py --rewrite /tmp/crontab_runtime.txt
  cron_gate.py 20_17_*_*_1-5 -- /app/daily_market_sync.sh stockinfo
"""

from __future__ import annotations

import datetime as dt
import os
import re
import sys
import time

FIELD_RE = re.compile(r"^[0-9*/,\-]+$")
MIN_GATE_INTERVAL = dt.timedelta(minutes=10)
MAX_SCAN_DAYS = 400


def parse_field(field: str, lo: int, hi: int, *, dow: bool = False) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        part = part.strip()
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s)
            if step <= 0:
                raise ValueError(f"bad step in {field!r}")
        if part in ("*", "?"):
            start, end = lo, hi
        elif "-" in part:
            start_s, end_s = part.split("-", 1)
            start, end = int(start_s), int(end_s)
        else:
            start = end = int(part)
        if start > end:
            raise ValueError(f"bad range in {field!r}")
        for value in range(start, end + 1, step):
            values.add(value)
    if dow and 7 in values:
        values.discard(7)
        values.add(0)
    return values


def parse_schedule(token: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    fields = token.replace("\\", "").split("_")
    if len(fields) != 5:
        raise ValueError(f"expected 5 cron fields, got {token!r}")
    return (
        parse_field(fields[0], 0, 59),
        parse_field(fields[1], 0, 23),
        parse_field(fields[2], 1, 31),
        parse_field(fields[3], 1, 12),
        parse_field(fields[4], 0, 7, dow=True),
    )


def matches(when: dt.datetime, sched) -> bool:
    minutes, hours, days, months, weekdays = sched
    if when.minute not in minutes or when.hour not in hours or when.month not in months:
        return False

    # cron semantics: if both day-of-month and day-of-week are restricted,
    # either one matching is enough.  A field is "restricted" when it is not
    # the full range.
    dom_restricted = days != set(range(1, 32))
    dow_restricted = weekdays != set(range(0, 7))
    dom_ok = when.day in days
    dow_ok = ((when.weekday() + 1) % 7) in weekdays  # Python Mon=0 -> cron Sun=0

    if dom_restricted and dow_restricted:
        return dom_ok or dow_ok
    return dom_ok and dow_ok


def find_occurrence(start: dt.datetime, sched, step_minutes: int) -> dt.datetime | None:
    current = start.replace(second=0, microsecond=0)
    delta = dt.timedelta(minutes=step_minutes)
    for _ in range(MAX_SCAN_DAYS * 1440):
        if matches(current, sched):
            return current
        current += delta
    return None


def wait_until(target: dt.datetime) -> None:
    while True:
        remaining = (target - dt.datetime.now()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 5.0))


def run_command(command: str) -> None:
    os.execvp("/bin/sh", ["/bin/sh", "-c", command])


def gate(schedule_token: str, command: str) -> None:
    try:
        _gate(schedule_token, command)
    except Exception as exc:  # fail open: never block a job because of the gate
        print(f"[cron_gate] {exc}; running now: {command}", flush=True)
        run_command(command)


def _gate(schedule_token: str, command: str) -> None:
    sched = parse_schedule(schedule_token)

    now = dt.datetime.now()
    current_minute = now.replace(second=0, microsecond=0)

    # On time (or late within the same minute): run immediately.
    if matches(current_minute, sched):
        run_command(command)

    next_run = find_occurrence(current_minute + dt.timedelta(minutes=1), sched, +1)
    if next_run is None:
        print(f"[cron_gate] no next occurrence for {schedule_token!r}; running now", flush=True)
        run_command(command)

    previous_run = find_occurrence(current_minute - dt.timedelta(minutes=1), sched, -1)
    interval = (next_run - previous_run) if previous_run else dt.timedelta(days=1)

    # Never gate high-frequency schedules; their early offset is seconds.
    if interval <= MIN_GATE_INTERVAL:
        run_command(command)

    early_seconds = (next_run - now).total_seconds()
    if early_seconds < interval.total_seconds() / 2:
        print(
            f"[cron_gate] early by {early_seconds / 60:.1f} min; "
            f"waiting until {next_run:%Y-%m-%d %H:%M} before: {command}",
            flush=True,
        )
        wait_until(next_run)
    run_command(command)


def rewrite(path: str) -> None:
    with open(path, encoding="utf-8") as fh:
        lines = fh.readlines()

    out: list[str] = []
    wrapped = 0
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            out.append(line)
            continue

        parts = line.split(None, 5)
        if len(parts) != 6 or not all(FIELD_RE.match(p) for p in parts[:5]):
            out.append(line)  # env assignment, @reboot, etc.
            continue

        command = parts[5].rstrip("\n")
        if "/app/cron_gate.py" in command:
            out.append(line)  # idempotent
            continue

        encoded = "_".join(p.replace("*", "\\*") for p in parts[:5])
        out.append(f"{' '.join(parts[:5])} python /app/cron_gate.py {encoded} -- {command}\n")
        wrapped += 1

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.writelines(out)
    os.replace(tmp, path)
    print(f"[cron_gate] wrapped {wrapped} job line(s) in {path}", flush=True)


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "--rewrite":
        if len(argv) != 2:
            sys.exit("usage: cron_gate.py --rewrite CRONTAB_FILE")
        rewrite(argv[1])
        return

    if not argv:
        sys.exit("usage: cron_gate.py <min_hour_dom_mon_dow> -- <command...>")

    if "--" in argv:
        sep = argv.index("--")
        schedule_token = argv[0]
        command = " ".join(argv[sep + 1 :])
    else:
        schedule_token = argv[0]
        command = " ".join(argv[1:])

    if not command:
        sys.exit("cron_gate: empty command")

    gate(schedule_token, command)


if __name__ == "__main__":
    main()
