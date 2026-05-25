"""
go2_master.py — Go2 Master Bridge
Fase 2: IPC activo + SPORT_MOD teleop

SDK: unitree_webrtc_connect (patrón validado de lidar_sender.py + teleop_video2.py)
"""

import asyncio
import json
import struct
import math
import numpy as np
import os
import signal
import socket
import time
from dataclasses import dataclass, field
from enum import Enum, auto

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD

# Fix ICE — crítico si hay ethernet conectado además del robot
import aioice.ice as _aioice_ice
_orig = _aioice_ice.get_host_addresses
_aioice_ice.get_host_addresses = lambda use_ipv4, use_ipv6: [
    ip for ip in _orig(use_ipv4, use_ipv6) if ip.startswith("192.168.12.")
]

# ─── Constantes ───────────────────────────────────────────────────────────────

LIDAR_UDP_HOST = "127.0.0.1"
LIDAR_UDP_PORT = 5005
ODOM_UDP_HOST  = "127.0.0.1"
ODOM_UDP_PORT  = 5006

IPC_SOCKET_PATH = "/tmp/go2_master.sock"

AES_KEY = os.environ.get("GO2_AES_KEY", None)

LIDAR_TIMEOUT_S    = 2.0
ODOM_TIMEOUT_S     = 2.0
MONITOR_INTERVAL_S = 1.0

SPORT_MOD_PUBLISH_HZ = 20
SPORT_MOD_INTERVAL   = 1.0 / SPORT_MOD_PUBLISH_HZ

CMD_QUEUE_MAXSIZE = 20

_ZERO_CMD = {"vx": 0.0, "vy": 0.0, "wz": 0.0}
_stop_immediate = False

# ─── Lifecycle ────────────────────────────────────────────────────────────────

class LifecycleState(Enum):
    INITIALIZING = auto()
    CONNECTING   = auto()
    CONNECTED    = auto()
    DEGRADED     = auto()
    DISCONNECTED = auto()
    SHUTDOWN     = auto()

# ─── Estado ───────────────────────────────────────────────────────────────────

@dataclass
class PoseState:
    x:  float = 0.0
    y:  float = 0.0
    z:  float = 0.0
    qx: float = 0.0
    qy: float = 0.0
    qz: float = 0.0
    qw: float = 1.0

@dataclass
class RobotState:
    connection_state: LifecycleState = LifecycleState.INITIALIZING

    scan_seq:     int   = 0
    last_scan_ts: float = 0.0
    lidar_ok:     bool  = False

    odom_seq:     int   = 0
    last_odom_ts: float = 0.0
    last_pose:    PoseState = field(default_factory=PoseState)
    odom_ok:      bool  = False

    ipc_seq:      int   = 0

state = RobotState()

# ─── StructuredLogger ─────────────────────────────────────────────────────────

_last_error_log: dict[str, float] = {}

def log(tag: str, msg: str) -> None:
    print(f"[{tag}] {msg}", flush=True)

def log_error_rate_limited(category: str, msg: str) -> None:
    now = time.monotonic()
    if now - _last_error_log.get(category, 0.0) >= 1.0:
        _last_error_log[category] = now
        log("ERROR", f"[{category}] {msg}")

def set_lifecycle(new_state: LifecycleState, reason: str = "") -> None:
    if state.connection_state == new_state:
        return
    state.connection_state = new_state
    suffix = f"  reason={reason}" if reason else ""
    log("LIFECYCLE", f"state={new_state.name}{suffix}")

# ─── UDP sockets ──────────────────────────────────────────────────────────────

_udp_lidar = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_udp_odom  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def udp_send(sock: socket.socket, data: bytes, host: str, port: int, label: str) -> None:
    try:
        sock.sendto(data, (host, port))
    except Exception as e:
        log_error_rate_limited(f"udp_{label}", str(e))

# ─── Callbacks WebRTC — trabajo MÍNIMO ────────────────────────────────────────

# Constantes LiDAR — mismas que lidar_sender.py validado
_LIDAR_RESOLUTION = 0.05
_LIDAR_Z_IDX_MIN  = 15
_LIDAR_Z_IDX_MAX  = 23
_LIDAR_ANGLE_MIN  = -math.pi
_LIDAR_ANGLE_MAX  =  math.pi
_LIDAR_RANGE_MIN  = 0.1
_LIDAR_RANGE_MAX  = 10.0
_LIDAR_NUM_BINS   = 360

def lidar_callback(message: dict) -> None:
    try:
        d      = message["data"]
        origin = np.array(d["origin"], dtype=np.float32)
        pts    = d["data"]["positions"].reshape(-1, 3).astype(np.float32)
    except Exception as e:
        log_error_rate_limited("lidar_parse", str(e))
        return

    mask   = (pts[:, 2] >= _LIDAR_Z_IDX_MIN) & (pts[:, 2] <= _LIDAR_Z_IDX_MAX)
    pts_2d = pts[mask]
    if len(pts_2d) == 0:
        return

    xyz    = origin + pts_2d * _LIDAR_RESOLUTION
    x, y   = xyz[:, 0], xyz[:, 1]
    angles = np.arctan2(y, x)
    ranges = np.sqrt(x**2 + y**2)

    angle_res = (_LIDAR_ANGLE_MAX - _LIDAR_ANGLE_MIN) / _LIDAR_NUM_BINS
    bins = np.full(_LIDAR_NUM_BINS, np.inf, dtype=np.float32)
    for a, r in zip(angles, ranges):
        if r < _LIDAR_RANGE_MIN or r > _LIDAR_RANGE_MAX:
            continue
        idx = int((a - _LIDAR_ANGLE_MIN) / angle_res)
        if 0 <= idx < _LIDAR_NUM_BINS:
            if r < bins[idx]:
                bins[idx] = r
    bins[bins == np.inf] = 0.0

    state.scan_seq    += 1
    state.last_scan_ts = time.monotonic()

    header  = struct.pack("IIffff", state.scan_seq, _LIDAR_NUM_BINS,
                          _LIDAR_ANGLE_MIN, _LIDAR_ANGLE_MAX,
                          _LIDAR_RANGE_MIN, _LIDAR_RANGE_MAX)
    payload = header + bins.tobytes()
    udp_send(_udp_lidar, payload, LIDAR_UDP_HOST, LIDAR_UDP_PORT, "lidar")

def odom_callback(message: dict) -> None:
    try:
        d   = message["data"]
        hdr = d.get("header", {}).get("stamp", {})
        sec    = int(hdr.get("sec", 0))
        nanosec = int(hdr.get("nanosec", 0))
        pose = d.get("pose", {})
        pos  = pose.get("position", {})
        ori  = pose.get("orientation", {})
        x  = float(pos.get("x", 0.0))
        y  = float(pos.get("y", 0.0))
        z  = float(pos.get("z", 0.0))
        qx = float(ori.get("x", 0.0))
        qy = float(ori.get("y", 0.0))
        qz = float(ori.get("z", 0.0))
        qw = float(ori.get("w", 1.0))
    except Exception as e:
        log_error_rate_limited("odom_parse", str(e))
        return

    state.odom_seq    += 1
    state.last_odom_ts = time.monotonic()
    state.last_pose = PoseState(x=x, y=y, z=z, qx=qx, qy=qy, qz=qz, qw=qw)

    payload = struct.pack("<IIfffffff", sec, nanosec, x, y, z, qx, qy, qz, qw)
    udp_send(_udp_odom, payload, ODOM_UDP_HOST, ODOM_UDP_PORT, "odom")

# ─── ConnectionMonitor ────────────────────────────────────────────────────────

class ConnectionMonitor:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._prev_scan_seq = 0
        self._prev_odom_seq = 0

    def start(self) -> None:
        self._task = asyncio.get_event_loop().create_task(self._loop())

    async def _loop(self) -> None:
        while state.connection_state not in (LifecycleState.SHUTDOWN,):
            await asyncio.sleep(MONITOR_INTERVAL_S)
            self.tick()

    def tick(self) -> None:
        now = time.monotonic()

        scan_delta = state.scan_seq - self._prev_scan_seq
        odom_delta = state.odom_seq - self._prev_odom_seq
        self._prev_scan_seq = state.scan_seq
        self._prev_odom_seq = state.odom_seq

        lidar_elapsed = now - state.last_scan_ts if state.last_scan_ts else float("inf")
        odom_elapsed  = now - state.last_odom_ts if state.last_odom_ts else float("inf")

        lidar_timeout = lidar_elapsed > LIDAR_TIMEOUT_S
        odom_timeout  = odom_elapsed  > ODOM_TIMEOUT_S

        state.lidar_ok = not lidar_timeout
        state.odom_ok  = not odom_timeout

        if lidar_timeout:
            log("MONITOR", f"lidar_timeout  elapsed={lidar_elapsed:.1f}s")
        if odom_timeout:
            log("MONITOR", f"odom_timeout   elapsed={odom_elapsed:.1f}s")

        if scan_delta > 0:
            log("LIDAR", f"seq={state.scan_seq}  hz_est={scan_delta / MONITOR_INTERVAL_S:.1f}")
        if odom_delta > 0:
            log("ODOM",  f"seq={state.odom_seq}  hz_est={odom_delta / MONITOR_INTERVAL_S:.1f}")

        if state.connection_state == LifecycleState.CONNECTED:
            if lidar_timeout or odom_timeout:
                reason = "lidar_timeout" if lidar_timeout else "odom_timeout"
                set_lifecycle(LifecycleState.DEGRADED, reason)
        elif state.connection_state == LifecycleState.DEGRADED:
            if not lidar_timeout and not odom_timeout:
                set_lifecycle(LifecycleState.CONNECTED)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

# ─── IPC Server ───────────────────────────────────────────────────────────────

class IPCServer:
    def __init__(self, cmd_queue: asyncio.Queue) -> None:
        self._queue  = cmd_queue
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        if os.path.exists(IPC_SOCKET_PATH):
            os.unlink(IPC_SOCKET_PATH)
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=IPC_SOCKET_PATH
        )
        log("IPC", f"server listening  path={IPC_SOCKET_PATH}")

    async def _handle_client(self, reader: asyncio.StreamReader,
                              writer: asyncio.StreamWriter) -> None:
        state.ipc_seq += 1
        log("IPC", f"client_connected   seq={state.ipc_seq}")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                await self._dispatch(line.strip())
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            log_error_rate_limited("ipc_read", str(e))
        finally:
            try:
                writer.close()
            except Exception:
                pass
            log("IPC", "client_disconnected")

    async def _dispatch(self, raw: bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as e:
            log_error_rate_limited("ipc_json", str(e))
            return

        msg_type = msg.get("type")

        if msg_type == "teleop_cmd":
            cmd = {
                "vx": float(msg.get("vx", 0.0)),
                "vy": float(msg.get("vy", 0.0)),
                "wz": float(msg.get("wz", 0.0)),
            }
            if self._queue.full():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            await self._queue.put(cmd)

        elif msg_type == "stop":
            global _stop_immediate
            _stop_immediate = True
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            await self._queue.put(_ZERO_CMD.copy())

        else:
            log("IPC", f"unknown_type  type={msg_type}")

    def stop(self) -> None:
        if self._server:
            self._server.close()
        if os.path.exists(IPC_SOCKET_PATH):
            try:
                os.unlink(IPC_SOCKET_PATH)
            except Exception:
                pass

# ─── SPORT_MOD publish loop ───────────────────────────────────────────────────

async def publish_loop(conn: UnitreeWebRTCConnection, cmd_queue: asyncio.Queue) -> None:
    log("SPORT", "publish_loop started  hz=20  mode=continuous")
    last_cmd = _ZERO_CMD.copy()
    was_moving = False

    try:
        while state.connection_state not in (LifecycleState.SHUTDOWN,):
            global _stop_immediate
            if _stop_immediate:
                _stop_immediate = False
                try:
                    await conn.datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"],
                        {"api_id": SPORT_CMD["StopMove"]},
                    )
                    last_cmd = _ZERO_CMD.copy()
                    was_moving = False
                except Exception as e:
                    log_error_rate_limited("sport_stop_imm", str(e))
                await asyncio.sleep(0)
                continue
            await asyncio.sleep(SPORT_MOD_INTERVAL)

            while not cmd_queue.empty():
                try:
                    last_cmd = cmd_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

            moving = not (last_cmd["vx"] == 0.0 and last_cmd["vy"] == 0.0 and last_cmd["wz"] == 0.0)

            if moving:
                try:
                    await conn.datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"],
                        {
                            "api_id": SPORT_CMD["Move"],
                            "parameter": json.dumps({
                                "x": last_cmd["vx"],
                                "y": last_cmd["vy"],
                                "z": last_cmd["wz"],
                            }),
                        },
                    )
                except Exception as e:
                    log_error_rate_limited("sport_pub", str(e))
            elif was_moving and not moving:
                # StopMove solo en transicion movimiento -> cero
                try:
                    await conn.datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["SPORT_MOD"],
                        {"api_id": SPORT_CMD["StopMove"]},
                    )
                except Exception as e:
                    log_error_rate_limited("sport_stop", str(e))

            was_moving = moving
    except asyncio.CancelledError:
        pass
    finally:
        log("SPORT", "publish_loop terminado")
async def send_stop_move(conn: UnitreeWebRTCConnection) -> None:
    try:
        await conn.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["SPORT_MOD"],
            {"api_id": SPORT_CMD["StopMove"]},
        )
    except Exception as e:
        log_error_rate_limited("sport_stop", str(e))

# ─── Shutdown ─────────────────────────────────────────────────────────────────

_monitor: ConnectionMonitor | None = None
_ipc: IPCServer | None = None
_shutdown_event = asyncio.Event()

def handle_sigint(sig, frame) -> None:
    print("\n", flush=True)
    _shutdown_event.set()

async def shutdown(conn: UnitreeWebRTCConnection) -> None:
    set_lifecycle(LifecycleState.SHUTDOWN)

    await send_stop_move(conn)
    log("SHUTDOWN", "StopMove enviado")

    if _ipc:
        _ipc.stop()
        log("SHUTDOWN", "IPC socket cerrado")

    if _monitor:
        await _monitor.stop()
        log("SHUTDOWN", "ConnectionMonitor detenido")

    try:
        _udp_lidar.close()
        _udp_odom.close()
        log("SHUTDOWN", "UDP sockets cerrados")
    except Exception:
        pass

    log("LIFECYCLE", "state=SHUTDOWN")

# ─── Main ─────────────────────────────────────────────────────────────────────

RECONNECT_DELAY_S = 15.0  # espera entre reconexiones

async def main() -> None:
    global _monitor, _ipc

    signal.signal(signal.SIGINT, handle_sigint)

    # IPC y monitor se crean una sola vez
    cmd_queue: asyncio.Queue = asyncio.Queue(maxsize=CMD_QUEUE_MAXSIZE)
    _ipc = IPCServer(cmd_queue)
    await _ipc.start()

    _monitor = ConnectionMonitor()
    _monitor.start()

    while not _shutdown_event.is_set():
        set_lifecycle(LifecycleState.CONNECTING)
        conn = UnitreeWebRTCConnection(
            WebRTCConnectionMethod.LocalAP,
            aes_128_key=AES_KEY,
        )
        try:
            await conn.connect()
        except Exception as e:
            log_error_rate_limited("webrtc_connect", str(e))
            log("RECONNECT", f"reintentando en {RECONNECT_DELAY_S}s")
            await asyncio.sleep(RECONNECT_DELAY_S)
            continue

        set_lifecycle(LifecycleState.CONNECTED)
        conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ULIDAR_ARRAY"], lidar_callback)
        conn.datachannel.pub_sub.subscribe(RTC_TOPIC["ROBOTODOM"],    odom_callback)
        log("INIT", "subscribed  topics=[ULIDAR_ARRAY, ROBOTODOM]")
        # BalanceStand — activa gait controller antes de aceptar comandos
        await asyncio.sleep(1.0)
        try:
            await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["SPORT_MOD"],
                {"api_id": SPORT_CMD["BalanceStand"]},
            )
            log("INIT", "BalanceStand enviado")
            await asyncio.sleep(2.0)
        except Exception as e:
            log_error_rate_limited("balance_stand", str(e))

        # Vaciar queue stale antes de arrancar nuevo publish_loop
        while not cmd_queue.empty():
            try:
                cmd_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        log("RECONNECT", "cmd_queue vaciada")
        pub_task = asyncio.get_event_loop().create_task(publish_loop(conn, cmd_queue))

        # Esperar hasta que la conexión caiga o shutdown
        while not _shutdown_event.is_set():
            if state.connection_state == LifecycleState.DISCONNECTED:
                break
            # Detectar caída por peer connection closed
            try:
                pc_state = conn.pc.connectionState
            except Exception:
                pc_state = "closed"
            if pc_state == "closed" or pc_state == "failed":
                log("RECONNECT", f"conexion caida (state={pc_state}) — reconectando en {RECONNECT_DELAY_S}s")
                break
            await asyncio.sleep(1.0)

        pub_task.cancel()
        try:
            await pub_task
        except asyncio.CancelledError:
            pass
        try:
            await conn.close()
        except Exception:
            pass

        if _shutdown_event.is_set():
            break

        await asyncio.sleep(RECONNECT_DELAY_S)

    await shutdown_final()

async def shutdown_final() -> None:
    set_lifecycle(LifecycleState.SHUTDOWN)
    if _ipc:
        _ipc.stop()
        log("SHUTDOWN", "IPC socket cerrado")
    if _monitor:
        await _monitor.stop()
        log("SHUTDOWN", "ConnectionMonitor detenido")
    try:
        _udp_lidar.close()
        _udp_odom.close()
        log("SHUTDOWN", "UDP sockets cerrados")
    except Exception:
        pass
    log("LIFECYCLE", "state=SHUTDOWN")

if __name__ == "__main__":
    asyncio.run(main())
