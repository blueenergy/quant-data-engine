#!/usr/bin/env python3
"""bridge 心跳监控（WSL 侧，常驻容器）。

交易日 09:40–15:10 每分钟检查 ``quant_realtime.market_spectrum_realtime_daily``
当天是否有新快照（新鲜阈值 3 分钟；快照正常每 1–2 分钟一条）。断流/恢复时告警：

  1. DingTalk webhook —— ``BRIDGE_HEARTBEAT_WEBHOOK_URL`` / ``DATA_CONTRACT_WEBHOOK_URL``，
     签名模式 ``BRIDGE_HEARTBEAT_WEBHOOK_SECRET`` / ``DATA_CONTRACT_WEBHOOK_SECRET``
     （与 quant-data-engine ContractNotifier 相同的 HMAC-SHA256 签名方案）
  2. Mongo 事件记录 —— ``quant_realtime.bridge_heartbeat_alerts``
  3. stdout 日志 —— ``docker logs bridge-heartbeat-monitor``

交易日历取 ``quant_data.trade_calendar``（is_open=1）；日历缺失时退回周一~周五判断。

部署（quant-infrastructure/apps 下）::

    docker run -d --restart unless-stopped --name bridge-heartbeat-monitor \\
      --network host -e TZ=Asia/Shanghai \\
      -v /home/shuyolin/trading/quant-data-engine/scripts/watch_bridge_heartbeat.py:/tmp/watch_bridge_heartbeat.py:ro \\
      --env-file env/common.env --env-file env/quant-data-engine.env \\
      crpi-gv3f6mfcrw75qane.cn-hangzhou.personal.cr.aliyuncs.com/wukongquant/quant-data-engine:sha-ba2d0d9 \\
      python -u /tmp/watch_bridge_heartbeat.py
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient

TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")

MONITOR_START = (9, 40)   # 开盘 09:30，留 10 分钟宽限
MONITOR_END = (15, 10)    # 收盘 15:00，留 10 分钟
# 告警窗口只覆盖连续竞价时段，收盘即止（午休 11:30-13:00 无快照是正常的）。
# 桥在非交易时段会 sleep（dayk 120s / minute 15s），最后一条快照通常比收盘早
# 1–3 分钟；窗口若延到收盘后，3 分钟 stale 阈值会在 11:31–11:33 / 15:02–15:07
# 每天误报一次（2026-10-19 起每天两条 OUTAGE 都是这个边界）。
ALERT_WINDOWS = [((9, 40), (11, 30)), ((13, 5), (15, 0))]
STALE_MINUTES = 3
ALERT_MIN_INTERVAL_SEC = 1800  # 同一事件 30 分钟内只发一次
SNAPSHOT_COLL = "market_spectrum_realtime_daily"
ALERT_COLL = "bridge_heartbeat_alerts"


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def _now() -> datetime:
    return datetime.now(TZ)


def _fmt(dt: datetime, fmt: str = "%Y%m%d%H%M") -> str:
    return dt.strftime(fmt)


def _log(msg: str) -> None:
    print(f"[{_fmt(_now(), '%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _send_dingtalk(subject: str, text_lines: list[str]) -> bool:
    url = _env("BRIDGE_HEARTBEAT_WEBHOOK_URL") or _env("DATA_CONTRACT_WEBHOOK_URL")
    if not url:
        return False
    secret = _env("BRIDGE_HEARTBEAT_WEBHOOK_SECRET") or _env("DATA_CONTRACT_WEBHOOK_SECRET")
    if secret:
        ts = int(time.time() * 1000)
        sign = base64.b64encode(
            hmac.new(secret.encode(), f"{ts}\n{secret}".encode(), hashlib.sha256).digest()
        ).decode()
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}timestamp={ts}&sign={urllib.parse.quote(sign)}"
    payload = {
        "msgtype": "markdown",
        "markdown": {"title": subject, "text": "\n".join([f"## {subject}", ""] + text_lines)},
    }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except Exception as exc:  # noqa: BLE001
        _log(f"dingtalk send failed: {exc}")
        return False


def _is_trading_day(calendar, today: str) -> bool:
    doc = calendar.find_one({"trade_date": today}, {"_id": 0, "is_open": 1})
    if doc:
        return int(doc.get("is_open") or 0) == 1
    # 日历缺失：退回周一~周五
    return datetime.strptime(today, "%Y%m%d").weekday() < 5


def main() -> None:
    mongo_uri = _env("MONGO_URI", "mongodb://admin:changeme@localhost:27017/?authSource=admin")
    data_db = _env("MONGO_DB", "quant_data")
    realtime_db = _env("REALTIME_DB_NAME", "quant_realtime")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    calendar = client[data_db]["trade_calendar"]
    snapshots = client[realtime_db][SNAPSHOT_COLL]
    alerts = client[realtime_db][ALERT_COLL]
    alerts.create_index("ts", background=True)

    _log(f"bridge heartbeat monitor started (check window {MONITOR_START[0]:02d}:{MONITOR_START[1]:02d}-"
         f"{MONITOR_END[0]:02d}:{MONITOR_END[1]:02d}, stale>{STALE_MINUTES}min, "
         f"webhook={'on' if _env('BRIDGE_HEARTBEAT_WEBHOOK_URL') or _env('DATA_CONTRACT_WEBHOOK_URL') else 'off'})")

    state = "OK"           # OK | OUTAGE
    last_alert_at = 0.0
    summary_logged = False

    def record(event: str, detail: str, last_snap: str) -> None:
        doc = {
            "ts": datetime.now(timezone.utc),
            "event": event,
            "trade_date": today,
            "last_snapshot": last_snap,
            "detail": detail,
        }
        try:
            alerts.insert_one(doc)
        except Exception as exc:  # noqa: BLE001
            _log(f"alert record insert failed: {exc}")
        _log(f"EVENT {event} last_snapshot={last_snap or '无'} {detail}")

    def alert(event: str, lines: list[str]) -> None:
        nonlocal last_alert_at
        now = time.time()
        if now - last_alert_at < ALERT_MIN_INTERVAL_SEC:
            return
        last_alert_at = now
        subject = f"bigqmt bridge 心跳{'丢失' if event == 'OUTAGE' else '恢复'}"
        _send_dingtalk(subject, lines)
        record(event, " | ".join(lines), last_snap)

    while True:
        try:
            now = _now()
            today = _fmt(now, "%Y%m%d")
            now_hm = (now.hour, now.minute)
            in_window = MONITOR_START <= now_hm <= MONITOR_END

            if not _is_trading_day(calendar, today):
                state = "OK"
                summary_logged = False
                time.sleep(60)
                continue

            # 注意：该集合的 trade_date 字段存的是完整时间戳（YYYYMMDDHHMM），
            # 且今天的最新快照按 snapshot_time 降序取即可，再校验前缀是否今天。
            latest = snapshots.find_one(
                {}, {"_id": 0, "snapshot_time": 1},
                sort=[("snapshot_time", -1)],
            )
            last_snap = latest["snapshot_time"] if latest else ""
            today_fresh = last_snap[:8] == today
            fresh_threshold = _fmt(now - timedelta(minutes=STALE_MINUTES))
            fresh = today_fresh and last_snap >= fresh_threshold

            in_alert_window = any(
                start <= now_hm <= end for start, end in ALERT_WINDOWS
            )

            if in_alert_window:
                if not fresh and state == "OK":
                    state = "OUTAGE"
                    if today_fresh:
                        gap = (now - datetime.strptime(last_snap, "%Y%m%d%H%M").replace(tzinfo=TZ)).seconds // 60
                        gap_line = f"- 断流时长: **{gap} 分钟**"
                        last_line = f"- 最后快照: **{last_snap}**"
                    else:
                        gap_line = "- 断流时长: 今天无任何快照"
                        last_line = f"- 历史最后快照: **{last_snap or '无'}**"
                    alert("OUTAGE", [
                        last_line,
                        gap_line,
                        "- 请检查 Windows 侧 QMT 终端与 quant-realtime-dayk/minute 容器是否在运行",
                    ])
                elif fresh and state == "OUTAGE":
                    state = "OK"
                    alert("RECOVERY", [f"- 最新快照恢复: **{last_snap}**"])
            elif state == "OUTAGE" and fresh:
                state = "OK"
                _log(f"recovered after window: last_snapshot={last_snap}")

            if now_hm > MONITOR_END and state == "OK" and not summary_logged:
                summary_logged = True
                _log(f"day summary {today}: last_snapshot={last_snap or '无'} — 全天正常收尾")
        except Exception as exc:  # noqa: BLE001
            _log(f"check error: {exc}")
        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
