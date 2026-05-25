"""
pose_audit.py — Go2 Pro :: Fase C
Auditoría de topics de pose/odometría del robot.

Uso:
    python3 pose_audit.py --topic ROBOTODOM
    python3 pose_audit.py --topic LIDAR_MAPPING_ODOM
    python3 pose_audit.py --topic LOW_STATE
    python3 pose_audit.py --topic SLAM_ODOMETRY

Flags opcionales:
    --duration 30         segundos de captura (default: 30)
    --dump                guarda payload del frame 1 a JSON offline
    --timeout 8           segundos sin frames antes de warning (default: 8)
"""

import asyncio
import time
import os
import json
import argparse
import math

import aioice.ice as _aioice_ice
_orig_get_host = _aioice_ice.get_host_addresses
def _filtered_get_host(use_ipv4, use_ipv6):
    return [ip for ip in _orig_get_host(use_ipv4, use_ipv6)
            if ip.startswith("192.168.12.")]
_aioice_ice.get_host_addresses = _filtered_get_host

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC

AES_KEY = os.environ.get("GO2_AES_KEY", "")
if not AES_KEY:
    raise ValueError("Exporta GO2_AES_KEY antes de correr el script")

parser = argparse.ArgumentParser(description="Auditoría de topic de pose Go2")
parser.add_argument("--topic", required=True)
parser.add_argument("--duration", type=int, default=30)
parser.add_argument("--timeout", type=int, default=8)
parser.add_argument("--dump", action="store_true")
args = parser.parse_args()

if args.topic not in RTC_TOPIC:
    print(f"\n[ERROR] '{args.topic}' no existe en RTC_TOPIC.")
    print("Keys disponibles:")
    for k in sorted(RTC_TOPIC.keys()):
        print(f"  {k} = {RTC_TOPIC[k]}")
    raise SystemExit(1)

TOPIC_KEY = args.topic
TOPIC_STR = RTC_TOPIC[TOPIC_KEY]

count = 0
last_time = None
first_time = None
intervals = []
jitter_vals = []
last_interval = None
robot_ts_vals = []
robot_ts_monotonic_failures = 0

TIMESTAMP_KEYS = {"time", "stamp", "ts", "sec", "nsec", "usec", "msec",
                  "timestamp", "header", "secs", "nanosecs", "t"}

def _is_timestamp_key(key):
    return str(key).lower() in TIMESTAMP_KEYS or \
           any(kw in str(key).lower() for kw in ["time", "stamp", "sec", "nsec"])

def inspect_structure(obj, prefix="", depth=0, max_depth=6, found_timestamps=None):
    if found_timestamps is None:
        found_timestamps = []
    indent = "  " * depth
    if depth > max_depth:
        print(f"{indent}{prefix}... (max depth)")
        return found_timestamps
    if isinstance(obj, dict):
        print(f"{indent}{prefix}dict  [{len(obj)} keys]")
        for k, v in obj.items():
            ts_mark = " <- TIMESTAMP?" if _is_timestamp_key(k) else ""
            if ts_mark:
                found_timestamps.append(f"{prefix}.{k}" if prefix else str(k))
            inspect_structure(v, prefix=f"{k}{ts_mark}", depth=depth+1,
                              max_depth=max_depth, found_timestamps=found_timestamps)
    elif isinstance(obj, (list, tuple)):
        tname = type(obj).__name__
        print(f"{indent}{prefix}{tname}  [len={len(obj)}]")
        if len(obj) > 0:
            inspect_structure(obj[0], prefix="[0]", depth=depth+1,
                              max_depth=max_depth, found_timestamps=found_timestamps)
            if len(obj) > 1:
                print(f"{'  '*(depth+1)}... ({len(obj)-1} mas)")
    elif isinstance(obj, bytes):
        print(f"{indent}{prefix}bytes  [len={len(obj)}]")
    elif isinstance(obj, bytearray):
        print(f"{indent}{prefix}bytearray  [len={len(obj)}]")
    else:
        try:
            import numpy as np
            if isinstance(obj, np.ndarray):
                print(f"{indent}{prefix}ndarray  dtype={obj.dtype}  shape={obj.shape}")
                return found_timestamps
        except ImportError:
            pass
        type_name = type(obj).__name__
        if isinstance(obj, (int, float, bool)):
            print(f"{indent}{prefix}{type_name}  = {obj}")
        elif isinstance(obj, str):
            preview = obj[:60] + "..." if len(obj) > 60 else obj
            print(f"{indent}{prefix}str  = '{preview}'")
        else:
            print(f"{indent}{prefix}{type_name}  = {repr(obj)[:60]}")
    return found_timestamps

def extract_robot_timestamp(msg):
    if not isinstance(msg, dict):
        return None
    for k in msg:
        if _is_timestamp_key(k):
            v = msg[k]
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
    for k, v in msg.items():
        if isinstance(v, dict):
            for k2 in v:
                if _is_timestamp_key(k2):
                    v2 = v[k2]
                    if isinstance(v2, (int, float)) and v2 > 0:
                        return float(v2)
                    if isinstance(v2, dict):
                        sec = v2.get("sec", v2.get("secs", None))
                        nsec = v2.get("nanosec", v2.get("nsecs", v2.get("nanosecs", 0)))
                        if sec is not None:
                            return float(sec) + float(nsec) * 1e-9
    data = msg.get("data", {})
    if isinstance(data, dict):
        header = data.get("header", {})
        if isinstance(header, dict):
            stamp = header.get("stamp", {})
            if isinstance(stamp, dict):
                sec = stamp.get("sec", stamp.get("secs", None))
                nsec = stamp.get("nanosec", stamp.get("nsecs", 0))
                if sec is not None:
                    return float(sec) + float(nsec) * 1e-9
    return None

def pose_callback(message):
    global count, last_time, first_time, last_interval
    global robot_ts_monotonic_failures
    now = time.time()
    count += 1
    if count == 1:
        first_time = now
        last_time = now
        print("\n" + "="*60)
        print(f"FRAME 1 — INSPECCION ESTRUCTURAL")
        print(f"Topic     : {TOPIC_KEY} = {TOPIC_STR}")
        print(f"host_time : {now:.6f}")
        print(f"Tipo raiz : {type(message).__name__}")
        print("="*60)
        if isinstance(message, (bytes, bytearray)):
            print(f"  Payload binario puro — {len(message)} bytes")
            if args.dump:
                fname = f"pose_audit_frame1_{TOPIC_KEY}.bin"
                with open(fname, "wb") as f:
                    f.write(bytes(message))
                print(f"  [DUMP] Guardado: {fname}")
        else:
            found_ts = inspect_structure(message, depth=0)
            if found_ts:
                print(f"\n  Timestamps internos detectados: {found_ts}")
            else:
                print(f"\n  Timestamps internos: NO detectados en primeros 6 niveles")
            if args.dump:
                fname = f"pose_audit_frame1_{TOPIC_KEY}.json"
                try:
                    with open(fname, "w") as f:
                        json.dump(message, f, indent=2, default=str)
                    print(f"  [DUMP] Guardado: {fname}")
                except Exception as e:
                    print(f"  [DUMP] Error: {e}")
        print("="*60)
        print(f"\nCapturando frames por {args.duration}s...\n")
        return
    dt = now - last_time
    last_time = now
    hz = 1.0 / dt if dt > 0 else 0.0
    if last_interval is not None:
        jitter_vals.append(abs(dt - last_interval))
    last_interval = dt
    intervals.append(dt)
    robot_ts = extract_robot_timestamp(message) if isinstance(message, dict) else None
    if robot_ts is not None:
        if robot_ts_vals and robot_ts < robot_ts_vals[-1]:
            robot_ts_monotonic_failures += 1
            mono_warn = " WARN NO MONOTÓNICO"
        else:
            mono_warn = ""
        robot_ts_vals.append(robot_ts)
        ts_str = f"  robot_ts={robot_ts:.6f}{mono_warn}"
    else:
        ts_str = "  robot_ts=N/A"
    print(f"  frame={count:4d}  dt={dt:.4f}s  hz={hz:6.2f}{ts_str}")

async def watchdog(timeout_s):
    await asyncio.sleep(timeout_s)
    if count == 0:
        print(f"\nWARNING: {timeout_s}s sin ningun frame.")
        print(f"  Topic '{TOPIC_KEY}' = '{TOPIC_STR}'")
        print("  Posibles causas:")
        print("    - Topic inactivo en este modo del robot")
        print("    - SLAM interno no corriendo")
        print("    - Robot apagado o sin conexion WebRTC")

def print_summary():
    print("\n" + "="*60)
    print(f"RESUMEN — {TOPIC_KEY} = {TOPIC_STR}")
    print("="*60)
    if count == 0:
        print("  RESULTADO: 0 frames — topic INACTIVO o sin conexion")
        print("="*60)
        return
    if count == 1:
        print("  RESULTADO: 1 frame — topic existe pero Hz no medible")
        print("="*60)
        return
    elapsed = last_time - first_time if first_time else 0
    avg_hz  = len(intervals) / sum(intervals) if intervals else 0
    avg_dt  = sum(intervals) / len(intervals) if intervals else 0
    min_dt  = min(intervals)
    max_dt  = max(intervals)
    avg_jitter = sum(jitter_vals) / len(jitter_vals) if jitter_vals else 0
    max_jitter = max(jitter_vals) if jitter_vals else 0
    print(f"  Frames recibidos     : {count}")
    print(f"  Duracion medida      : {elapsed:.1f}s")
    print(f"  Hz promedio          : {avg_hz:.2f}")
    print(f"  dt promedio          : {avg_dt:.4f}s")
    print(f"  dt minimo            : {min_dt:.4f}s  (hz pico  = {1/min_dt:.2f})")
    print(f"  dt maximo            : {max_dt:.4f}s  (hz valle = {1/max_dt:.2f})")
    print(f"  Jitter promedio      : {avg_jitter*1000:.2f}ms")
    print(f"  Jitter maximo        : {max_jitter*1000:.2f}ms")
    print()
    if robot_ts_vals:
        print(f"  Timestamps internos  : {len(robot_ts_vals)} detectados")
        print(f"  Fallos monotonicidad : {robot_ts_monotonic_failures}")
        print(f"  Monotonicidad        : {'OK' if robot_ts_monotonic_failures == 0 else 'FALLO ' + str(robot_ts_monotonic_failures) + ' saltos'}")
    else:
        print(f"  Timestamps internos  : no detectados")
    print()
    if avg_hz >= 8:
        print(f"  Hz eval   : OK suficiente para SLAM2D ({avg_hz:.1f} Hz)")
    elif avg_hz >= 4:
        print(f"  Hz eval   : MARGINAL para SLAM2D ({avg_hz:.1f} Hz)")
    else:
        print(f"  Hz eval   : INSUFICIENTE ({avg_hz:.1f} Hz)")
    if avg_jitter * 1000 < 5:
        print(f"  Jitter    : OK estable ({avg_jitter*1000:.2f}ms)")
    elif avg_jitter * 1000 < 20:
        print(f"  Jitter    : MODERADO ({avg_jitter*1000:.2f}ms)")
    else:
        print(f"  Jitter    : ALTO ({avg_jitter*1000:.2f}ms)")
    print("="*60)

async def main():
    print(f"\n[1] Conectando a robot...")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    print(f"[2] WebRTC conectado.")
    print(f"[3] Suscribiendo a {TOPIC_KEY} = '{TOPIC_STR}'...")
    conn.datachannel.pub_sub.subscribe(TOPIC_STR, pose_callback)
    print(f"[4] Subscribe llamado. Timeout warning en {args.timeout}s.\n")
    await asyncio.gather(
        watchdog(args.timeout),
        asyncio.sleep(args.duration)
    )
    print_summary()

if __name__ == "__main__":
    asyncio.run(main())
