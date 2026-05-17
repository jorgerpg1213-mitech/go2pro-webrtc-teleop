import cv2
import numpy as np
import asyncio
import logging
import threading
import time
from pynput import keyboard

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD
from aiortc import MediaStreamTrack

logging.basicConfig(level=logging.FATAL)

# Filtro ICE — CRITICO sin esto falla con ethernet conectado
import aioice.ice as _aioice_ice
_orig_get_host = _aioice_ice.get_host_addresses
def _filtered_get_host(use_ipv4, use_ipv6):
    return [ip for ip in _orig_get_host(use_ipv4, use_ipv6) if ip.startswith("192.168.12.")]
_aioice_ice.get_host_addresses = _filtered_get_host

import os
AES_KEY = os.environ.get("GO2_AES_KEY", "")
if not AES_KEY:
    raise ValueError("Debes exportar GO2_AES_KEY antes de correr el script")
FREQ_HZ = 20           # frecuencia de envio de comandos
WATCHDOG_S = 0.5       # tiempo sin input humano antes de stop automatico

# Estado compartido protegido con lock
_lock = threading.Lock()
_state = {"x": 0.0, "y": 0.0, "z": 0.0}
_last_input = [time.time()]   # ultima vez que humano toco tecla
_running = [True]
_conn_ref = [None]            # referencia al conn para stop inmediato

def get_state():
    with _lock:
        return dict(_state)

def set_state(x, y, z):
    with _lock:
        _state["x"] = x
        _state["y"] = y
        _state["z"] = z

def touch_input():
    """Registra actividad humana — resetea watchdog."""
    with _lock:
        _last_input[0] = time.time()

def get_last_input():
    with _lock:
        return _last_input[0]

def normalize_key(key):
    """Normaliza todas las teclas a strings consistentes. Sin mezcla de tipos."""
    if key == keyboard.Key.space:  return "SPACE"
    if key == keyboard.Key.up:     return "UP"
    if key == keyboard.Key.down:   return "DOWN"
    if key == keyboard.Key.left:   return "LEFT"
    if key == keyboard.Key.right:  return "RIGHT"
    if key == keyboard.Key.esc:    return "ESC"
    if hasattr(key, "char") and key.char is not None:
        return key.char.lower()
    return None

def main():
    frame_queue = []
    frame_lock = threading.Lock()
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP, aes_128_key=AES_KEY)
    _conn_ref[0] = conn

    # pressed: solo strings normalizados — sin mezcla de tipos
    # NOTA: accedido desde hilo pynput y main, sin lock explícito.
    # Python GIL + set pequeño + etapa experimental = tolerado.
    pressed = set()

    def update_motion():
        x, z = 0.0, 0.0
        if "UP" in pressed or "w" in pressed:
            x = 0.6
        elif "DOWN" in pressed or "s" in pressed:
            x = -0.5
        if "LEFT" in pressed or "a" in pressed:
            z = 0.7
        elif "RIGHT" in pressed or "d" in pressed:
            z = -0.7
        prev = get_state()
        set_state(x, 0.0, z)
        if (x, z) != (prev["x"], prev["z"]):
            if x != 0 or z != 0:
                label = []
                if x > 0: label.append("ADELANTE")
                if x < 0: label.append("ATRAS")
                if z > 0: label.append("GIRO IZQ")
                if z < 0: label.append("GIRO DER")
                print(f">> MOVE {' + '.join(label)} x={x} z={z}")
            else:
                print(">> STOP")

    def on_press(key):
        k = normalize_key(key)
        if k is None:
            return
        touch_input()  # registrar actividad humana siempre

        if k == "SPACE":
            set_state(0.0, 0.0, 0.0)
            pressed.clear()
            print(">> EMERGENCY STOP")
            # Mandar StopMove inmediato via asyncio desde hilo pynput
            if _conn_ref[0] and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    _conn_ref[0].datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
                    ), loop
                )
            return

        if k in ("q", "ESC"):
            print(">> SALIENDO...")
            set_state(0.0, 0.0, 0.0)
            # StopMove explícito antes de salir
            if _conn_ref[0] and loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    _conn_ref[0].datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
                    ), loop
                )
            time.sleep(0.3)  # dar tiempo al StopMove de llegar
            _running[0] = False
            return

        pressed.add(k)
        update_motion()

    def on_release(key):
        k = normalize_key(key)
        if k is None:
            return
        touch_input()
        pressed.discard(k)
        update_motion()

    listener = keyboard.Listener(on_press=on_press, on_release=on_release)

    async def recv_camera_stream(track: MediaStreamTrack):
        while True:
            frame = await track.recv()
            with frame_lock:
                frame_queue.clear()
                frame_queue.append(frame.to_ndarray(format="bgr24"))

    async def control_loop():
        last_state = {"x": 0.0, "y": 0.0, "z": 0.0}
        interval = 1.0 / FREQ_HZ
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            state = get_state()

            # Watchdog basado en tiempo desde ULTIMO INPUT HUMANO
            # No en tiempo del loop — detecta freeze real de input
            time_since_input = now - get_last_input()
            if time_since_input > WATCHDOG_S:
                moving = state["x"] != 0 or state["z"] != 0
                if moving:
                    await conn.datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
                    )
                    set_state(0.0, 0.0, 0.0)
                    print(f">> WATCHDOG STOP ({time_since_input:.1f}s sin input)")
                    last_state = {"x": 0.0, "y": 0.0, "z": 0.0}
                continue

            moving = state["x"] != 0 or state["z"] != 0
            was_moving = last_state["x"] != 0 or last_state["z"] != 0

            if moving:
                touch_input()  # tecla sostenida activa — resetear watchdog
                await conn.datachannel.pub_sub.publish_request_new(
                    RTC_TOPIC["SPORT_MOD"],
                    {"api_id": SPORT_CMD["Move"],
                     "parameter": {"x": state["x"], "y": 0.0, "z": state["z"]}}
                )
            elif was_moving and not moving:
                # StopMove SOLO en transicion de movimiento a cero
                await conn.datachannel.pub_sub.publish_request_new(
                    RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["StopMove"]}
                )

            last_state = dict(state)

    def run_asyncio_loop(lp):
        asyncio.set_event_loop(lp)
        async def setup():
            try:
                await conn.connect()
                print("Activando BalanceStand...")
                r = await conn.datachannel.pub_sub.publish_request_new(
                    RTC_TOPIC["SPORT_MOD"], {"api_id": SPORT_CMD["BalanceStand"]}
                )
                code = r.get("data", {}).get("header", {}).get("status", {}).get("code", "?")
                print(f"BalanceStand response code: {code}")
                await asyncio.sleep(2)
                print("="*50)
                print("Listo para control.")
                listener.start()
                print("Teclado activo")
                print("W/S = adelante/atras | A/D = giros")
                print("SPACE = EMERGENCY STOP | Q/ESC = salir")
                print("="*50)
                conn.video.switchVideoChannel(True)
                conn.video.add_track_callback(recv_camera_stream)
                asyncio.create_task(control_loop())
            except Exception as e:
                logging.error(f"Error setup: {e}")
        lp.run_until_complete(setup())
        lp.run_forever()

    loop = asyncio.new_event_loop()
    asyncio_thread = threading.Thread(target=run_asyncio_loop, args=(loop,), daemon=True)
    asyncio_thread.start()

    img = np.zeros((720, 1280, 3), dtype=np.uint8)
    cv2.imshow("Go2 Pro - Teleop", img)

    try:
        while _running[0]:
            with frame_lock:
                if frame_queue:
                    img = frame_queue[0]
            cv2.imshow("Go2 Pro - Teleop", img)
            cv2.waitKey(1)
            time.sleep(0.01)
    finally:
        print("Cerrando...")
        cv2.destroyAllWindows()
        listener.stop()
        loop.call_soon_threadsafe(loop.stop)

if __name__ == "__main__":
    main()
