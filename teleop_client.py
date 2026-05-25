"""
teleop_client.py — Teleop IPC Client
Fase 2: pynput → JSON continuo → Unix socket → go2_master.py

Usa pynput para key press/release reales — sin dependencia de key repeat del OS.
Patrón idéntico a teleop_video2.py validado.
"""

import json
import os
import socket
import threading
import time
from pynput import keyboard

# ─── Velocidades ──────────────────────────────────────────────────────────────

VX_FORWARD  =  0.5   # m/s avance
VX_BACK     = -0.4   # m/s retroceso
WZ_ANGULAR  =  1.2   # rad/s giro
VY_LINEAR   =  0.3   # m/s strafe

WATCHDOG_S    = 0.5
SEND_HZ       = 20
SEND_INTERVAL = 1.0 / SEND_HZ

IPC_SOCKET_PATH = "/tmp/go2_master.sock"

# ─── Estado compartido ────────────────────────────────────────────────────────

_lock       = threading.Lock()
_state      = {"vx": 0.0, "vy": 0.0, "wz": 0.0}
_last_input = [time.time()]
_running    = [True]

def get_state():
    with _lock:
        return dict(_state)

def set_state(vx, vy, wz):
    with _lock:
        _state["vx"] = vx
        _state["vy"]  = vy
        _state["wz"]  = wz

def touch_input():
    with _lock:
        _last_input[0] = time.time()

def get_last_input():
    with _lock:
        return _last_input[0]

# ─── Normalización de teclas ──────────────────────────────────────────────────

def normalize_key(key):
    if key == keyboard.Key.space: return "SPACE"
    if key == keyboard.Key.esc:   return "ESC"
    if key == keyboard.Key.up:    return "w"
    if key == keyboard.Key.down:  return "s"
    if key == keyboard.Key.left:  return "a"
    if key == keyboard.Key.right: return "d"
    if hasattr(key, "char") and key.char is not None:
        return key.char.lower()
    return None

# ─── Motion update ────────────────────────────────────────────────────────────

def update_motion(pressed: set):
    vx, vy, wz = 0.0, 0.0, 0.0
    if "w" in pressed: vx =  VX_FORWARD
    elif "s" in pressed: vx = VX_BACK
    if "a" in pressed: wz =  WZ_ANGULAR
    elif "d" in pressed: wz = -WZ_ANGULAR
    if "q" in pressed: vy =  VY_LINEAR
    elif "e" in pressed: vy = -VY_LINEAR
    set_state(vx, vy, wz)

# ─── IPC ──────────────────────────────────────────────────────────────────────

class IPCClient:
    def __init__(self, path: str):
        self._path = path
        self._sock: socket.socket | None = None

    def connect(self):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self._path)
        print(f"[IPC] conectado  path={self._path}", flush=True)

    def send_cmd(self, vx, vy, wz) -> bool:
        return self._send({
            "type": "teleop_cmd",
            "vx": vx, "vy": vy, "wz": wz,
            "timestamp": time.time(),
        })

    def send_stop(self) -> bool:
        return self._send({"type": "stop", "timestamp": time.time()})

    def _send(self, obj: dict) -> bool:
        if not self._sock:
            return False
        try:
            self._sock.sendall((json.dumps(obj) + "\n").encode())
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            print(f"\n[ERROR] IPC send falló: {e}", flush=True)
            self._sock = None
            return False

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        print("[IPC] desconectado", flush=True)

# ─── Main ─────────────────────────────────────────────────────────────────────

BANNER = f"""
╔════════════════════════════════════════╗
║   Go2 Teleop — Fase 2 (pynput)        ║
╠════════════════════════════════════════╣
║  W/S      → avanzar / retroceder      ║
║  A/D      → girar izq / der           ║
║  Q/E      → strafe izq / der          ║
║  ↑↓←→    → igual que WASD            ║
║  SPACE    → STOP emergencia           ║
║  ESC      → salir                     ║
╠════════════════════════════════════════╣
║  VX={VX_FORWARD:.1f}  WZ={WZ_ANGULAR:.1f}  watchdog={WATCHDOG_S:.1f}s   ║
╚════════════════════════════════════════╝
"""

def main():
    if not os.path.exists(IPC_SOCKET_PATH):
        print(f"[ERROR] Socket no encontrado: {IPC_SOCKET_PATH}")
        print("        ¿Está corriendo go2_master.py?")
        return

    ipc = IPCClient(IPC_SOCKET_PATH)
    try:
        ipc.connect()
    except ConnectionRefusedError:
        print(f"[ERROR] Conexión rechazada en {IPC_SOCKET_PATH}")
        return

    print(BANNER)

    pressed = set()

    def on_press(key):
        k = normalize_key(key)
        if k is None:
            return
        touch_input()

        if k == "SPACE":
            set_state(0.0, 0.0, 0.0)
            pressed.clear()
            ipc.send_stop()
            print("\r  [STOP] emergencia                              ", end="", flush=True)
            return

        if k == "ESC":
            set_state(0.0, 0.0, 0.0)
            pressed.clear()
            ipc.send_stop()
            _running[0] = False
            return

        pressed.add(k)
        update_motion(pressed)

    def on_release(key):
        k = normalize_key(key)
        if k is None:
            return
        touch_input()
        pressed.discard(k)
        update_motion(pressed)

    listener = keyboard.Listener(on_press=on_press, on_release=on_release)
    listener.start()

    last_state = {"vx": 0.0, "vy": 0.0, "wz": 0.0}

    print("  Listo — usa WASD para mover\n", flush=True)

    try:
        while _running[0]:
            time.sleep(SEND_INTERVAL)
            now = time.time()
            state = get_state()

            # Watchdog
            time_since_input = now - get_last_input()
            if time_since_input > WATCHDOG_S:
                moving = state["vx"] != 0 or state["vy"] != 0 or state["wz"] != 0
                if moving:
                    set_state(0.0, 0.0, 0.0)
                    pressed.clear()
                    ipc.send_stop()
                    print(f"\r  [WATCHDOG] stop ({time_since_input:.1f}s sin input)    ", end="", flush=True)
                    last_state = {"vx": 0.0, "vy": 0.0, "wz": 0.0}
                continue

            moving     = state["vx"] != 0 or state["vy"] != 0 or state["wz"] != 0
            was_moving = last_state["vx"] != 0 or last_state["vy"] != 0 or last_state["wz"] != 0

            if moving:
                ipc.send_cmd(state["vx"], state["vy"], state["wz"])
                print(f"\r  vx={state['vx']:+.2f}  vy={state['vy']:+.2f}  wz={state['wz']:+.2f}    ",
                      end="", flush=True)
            elif was_moving and not moving:
                ipc.send_stop()
                print(f"\r  [---] STOP                                    ", end="", flush=True)

            last_state = dict(state)

    except KeyboardInterrupt:
        pass
    finally:
        listener.stop()
        ipc.send_stop()
        ipc.close()
        print("\n")

if __name__ == "__main__":
    main()
