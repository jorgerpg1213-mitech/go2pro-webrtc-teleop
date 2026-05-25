"""
teleop_simple.py — Go2 Pro
Teleop mínimo sin cv2/video — solo movimiento por teclado.
W/S = adelante/atrás | A/D = giros | SPACE = stop | Q = salir
"""
import asyncio
import os
import sys
import tty
import termios

import aioice.ice as _aioice_ice
_orig_get_host = _aioice_ice.get_host_addresses
def _filtered_get_host(use_ipv4, use_ipv6):
    return [ip for ip in _orig_get_host(use_ipv4, use_ipv6)
            if ip.startswith("192.168.12.")]
_aioice_ice.get_host_addresses = _filtered_get_host

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD

AES_KEY = os.environ.get("GO2_AES_KEY", "")
if not AES_KEY:
    raise ValueError("Exporta GO2_AES_KEY antes de correr el script")

def get_key():
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)

async def main():
    print("[1] Conectando...")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    print("[2] Conectado.")
    print("W/S=adelante/atras  A/D=giros  SPACE=stop  Q=salir\n")

    await conn.datachannel.pub_sub.publish_request_new(
        RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["BalanceStand"]}
    )
    await asyncio.sleep(2)

    while True:
        k = await asyncio.get_event_loop().run_in_executor(None, get_key)
        if k == 'q':
            print("Saliendo...")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
            )
            break
        elif k == ' ':
            print("STOP")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
            )
        elif k == 'w':
            print("ADELANTE")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["Move"],
                "parameter": {"x": 0.3, "y": 0.0, "z": 0.0}}
            )
        elif k == 's':
            print("ATRAS")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["Move"],
                "parameter": {"x": -0.3, "y": 0.0, "z": 0.0}}
            )
        elif k == 'a':
            print("GIRO IZQ")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["Move"],
                "parameter": {"x": 0.0, "y": 0.0, "z": 0.5}}
            )
        elif k == 'd':
            print("GIRO DER")
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["Move"],
                "parameter": {"x": 0.0, "y": 0.0, "z": -0.5}}
            )

if __name__ == "__main__":
    asyncio.run(main())
