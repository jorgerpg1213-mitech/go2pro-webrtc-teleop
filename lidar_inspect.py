import asyncio, os
import numpy as np
from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC

import aioice.ice as _aioice_ice
_orig = _aioice_ice.get_host_addresses
_aioice_ice.get_host_addresses = lambda use_ipv4, use_ipv6: [ip for ip in _orig(use_ipv4, use_ipv6) if ip.startswith("192.168.12.")]

AES_KEY = os.environ.get("GO2_AES_KEY", "")
done = asyncio.Event()

def lidar_callback(msg):
    pos = msg['data']['data']['positions']
    print(f"dtype={pos.dtype}  len={len(pos)}  len%3={len(pos)%3}")
    pts = pos.reshape(-1, 3)
    print(f"shape={pts.shape}")
    print(f"pt[0]={pts[0]}")
    print(f"pt[1]={pts[1]}")
    print(f"pt[2]={pts[2]}")
    done.set()

async def main():
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ULIDAR_ARRAY"], lidar_callback)
    print("Esperando primer frame...")
    await asyncio.wait_for(done.wait(), timeout=10)
    await conn.disconnect()

asyncio.run(main())
