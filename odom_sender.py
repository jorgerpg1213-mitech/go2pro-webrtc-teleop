"""
odom_sender.py — Go2 Pro :: Fase C
HOST side: WebRTC ROBOTODOM → UDP → Docker ROS2

Arquitectura (igual que lidar_sender.py):
  Thread principal asyncio → WebRTC → callback → UDP
  Docker recibe UDP → publica /odom y /tf

Formato UDP (binario, little-endian explícito):
  sec       : uint32  — header.stamp.sec del robot
  nanosec   : uint32  — header.stamp.nanosec del robot
  x         : float32
  y         : float32
  z         : float32
  qx        : float32
  qy        : float32
  qz        : float32
  qw        : float32

Total: 36 bytes fijos. Little-endian explícito (<) — sin padding nativo.
"""

import asyncio
import socket
import struct
import os

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

UDP_HOST = "127.0.0.1"
UDP_PORT = 5006

STRUCT_FMT  = "<IIfffffff"
STRUCT_SIZE = struct.calcsize(STRUCT_FMT)  # 36 bytes

_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

frame_count = 0

def odom_callback(message):
    global frame_count
    frame_count += 1

    try:
        data    = message["data"]
        stamp   = data["header"]["stamp"]
        sec     = int(stamp["sec"])
        nanosec = int(stamp["nanosec"])
        pos = data["pose"]["position"]
        ori = data["pose"]["orientation"]
        x   = float(pos["x"])
        y   = float(pos["y"])
        z   = float(pos["z"])
        qx  = float(ori["x"])
        qy  = float(ori["y"])
        qz  = float(ori["z"])
        qw  = float(ori["w"])
    except (KeyError, TypeError, ValueError) as e:
        print(f"[odom_sender] parse error frame={frame_count}: {e}")
        return

    payload = struct.pack(STRUCT_FMT, sec, nanosec, x, y, z, qx, qy, qz, qw)
    _sock.sendto(payload, (UDP_HOST, UDP_PORT))

    if frame_count % 20 == 0:
        print(f"[odom_sender] frame={frame_count}  x={x:.3f}  y={y:.3f}  z={z:.3f}  qw={qw:.4f}")

async def main():
    print("[1] Conectando a robot...")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    print("[2] WebRTC conectado.")
    print(f"[3] Suscribiendo a ROBOTODOM = '{RTC_TOPIC['ROBOTODOM']}'...")
    conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ROBOTODOM"], odom_callback)
    print(f"[4] Enviando UDP → {UDP_HOST}:{UDP_PORT}  ({STRUCT_SIZE} bytes/frame)")
    print("    Ctrl+C para salir.\n")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
