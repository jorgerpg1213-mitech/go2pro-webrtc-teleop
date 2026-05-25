"""
lidar_sender.py — Fase B
Host: recibe voxels WebRTC, extrae slice 2D, serializa LaserScan y envía por UDP.
"""
import asyncio
import os
import socket
import struct
import math
import numpy as np

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC

import aioice.ice as _aioice_ice
_orig = _aioice_ice.get_host_addresses
_aioice_ice.get_host_addresses = lambda use_ipv4, use_ipv6: [
    ip for ip in _orig(use_ipv4, use_ipv6) if ip.startswith("192.168.12.")
]

AES_KEY = os.environ.get("GO2_AES_KEY", "")
if not AES_KEY:
    raise ValueError("Exporta GO2_AES_KEY antes de correr el script")

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005
RESOLUTION = 0.05
Z_IDX_MIN = 15
Z_IDX_MAX = 23
ANGLE_MIN = -math.pi
ANGLE_MAX = math.pi
RANGE_MIN = 0.1
RANGE_MAX = 10.0
NUM_BINS = 360

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
frame_count = 0

def voxels_to_laserscan(msg):
    global frame_count
    frame_count += 1
    d = msg['data']
    origin = np.array(d['origin'], dtype=np.float32)
    pts = d['data']['positions'].reshape(-1, 3).astype(np.float32)
    mask = (pts[:, 2] >= Z_IDX_MIN) & (pts[:, 2] <= Z_IDX_MAX)
    pts_2d = pts[mask]
    if len(pts_2d) == 0:
        print(f"[frame {frame_count}] WARN: 0 puntos después de filtro Z")
        return
    xyz = origin + pts_2d * RESOLUTION
    x = xyz[:, 0]
    y = xyz[:, 1]
    angles = np.arctan2(y, x)
    ranges = np.sqrt(x**2 + y**2)
    angle_res = (ANGLE_MAX - ANGLE_MIN) / NUM_BINS
    bins = np.full(NUM_BINS, np.inf, dtype=np.float32)
    for a, r in zip(angles, ranges):
        if r < RANGE_MIN or r > RANGE_MAX:
            continue
        idx = int((a - ANGLE_MIN) / angle_res)
        if 0 <= idx < NUM_BINS:
            if r < bins[idx]:
                bins[idx] = r
    bins[bins == np.inf] = 0.0
    header = struct.pack('IIffff', frame_count, NUM_BINS, ANGLE_MIN, ANGLE_MAX, RANGE_MIN, RANGE_MAX)
    payload = header + bins.tobytes()
    sock.sendto(payload, (UDP_HOST, UDP_PORT))
    print(f"[frame {frame_count}] pts_2d={len(pts_2d)}  UDP {len(payload)}b")

async def main():
    print(f"[1] Iniciando — UDP -> {UDP_HOST}:{UDP_PORT}")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    print("[2] WebRTC conectado. Publicando LaserScan...")
    conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ULIDAR_ARRAY"], voxels_to_laserscan)
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await conn.disconnect()
        sock.close()

asyncio.run(main())
