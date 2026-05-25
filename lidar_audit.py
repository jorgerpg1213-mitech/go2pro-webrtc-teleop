import asyncio
import time
import os

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC

# Fix ICE — validado para go2_v205_env: get_host_addresses() -> list[str]
import aioice.ice as _aioice_ice
_orig_get_host = _aioice_ice.get_host_addresses
def _filtered_get_host(use_ipv4, use_ipv6):
    return [ip for ip in _orig_get_host(use_ipv4, use_ipv6)
            if ip.startswith("192.168.12.")]
_aioice_ice.get_host_addresses = _filtered_get_host

AES_KEY = os.environ.get("GO2_AES_KEY", "")
if not AES_KEY:
    raise ValueError("Exporta GO2_AES_KEY antes de correr el script")

# --- Estado del audit ---
count = 0
last_time = None
intervals = []
payload_sizes = []

def lidar_callback(message):
    global count, last_time

    now = time.time()
    count += 1

    # Primer frame: inspeccionar tipo y registrar tiempo base
    if count == 1:
        print(f"[FRAME 1] type={type(message).__name__}  repr={repr(message)[:80]}")
        last_time = now
        return

    # Frames siguientes: medir intervalo
    dt = now - last_time
    last_time = now
    hz = 1.0 / dt

    payload = len(message) if isinstance(message, (bytes, bytearray)) else -1
    intervals.append(dt)
    payload_sizes.append(payload)

    print(f"  frame={count:4d}  dt={dt:.3f}s  hz={hz:.2f}  payload={payload}b")

def print_summary():
    print("\n" + "="*50)
    print("RESUMEN LIDAR AUDIT")
    print("="*50)

    if count == 0:
        print("  PROBLEMA: 0 frames recibidos.")
        print("  Posibles causas:")
        print("    - subscribe() no funcionó")
        print("    - topic incorrecto")
        print("    - decrypt fallando silenciosamente")
        print("    - LiDAR inactivo en el robot")
        print("="*50)
        return

    if count == 1:
        print("  PROBLEMA: solo 1 frame recibido — no hay intervalos medibles.")
        print("="*50)
        return

    elapsed = last_time - intervals[0] if intervals else 0
    avg_hz  = len(intervals) / sum(intervals) if intervals else 0
    avg_dt  = sum(intervals) / len(intervals)
    min_dt  = min(intervals)
    max_dt  = max(intervals)
    valid_payloads = [p for p in payload_sizes if p >= 0]
    avg_size = sum(valid_payloads) / len(valid_payloads) if valid_payloads else -1

    print(f"  Frames recibidos : {count}")
    print(f"  Hz promedio      : {avg_hz:.2f}")
    print(f"  dt promedio      : {avg_dt:.3f}s")
    print(f"  dt mínimo        : {min_dt:.3f}s  (hz pico = {1/min_dt:.2f})")
    print(f"  dt máximo        : {max_dt:.3f}s  (hz valle = {1/max_dt:.2f})")
    if avg_size >= 0:
        print(f"  Payload promedio : {avg_size:.0f}b")
    else:
        print(f"  Payload          : no medible (tipo no bytes)")
    print("="*50)

async def main():
    print("[1] Conectando a robot...")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    await conn.connect()
    print("[2] WebRTC conectado.")

    print(f"[3] Suscribiendo a {RTC_TOPIC['ULIDAR_ARRAY']}...")
    conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ULIDAR_ARRAY"], lidar_callback)
    print("[4] Subscribe llamado. Esperando frames...")
    print("    (si no aparece FRAME 1 en 5s — subscribe o topic tienen problema)\n")

    await asyncio.sleep(30)

    print_summary()

if __name__ == "__main__":
    asyncio.run(main())
