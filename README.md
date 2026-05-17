# Go2 Pro — WebRTC Teleop + Live Camera

Python-based real-time teleoperation and live camera streaming for the **Unitree Go2 Pro** robot using WebRTC. No jailbreak, no firmware modification required.

## Features

- Live camera streaming 1280x720 H264
- Keyboard teleoperation with continuous locomotion
- Single WebRTC session — video + control over one connection
- Safety watchdog — automatic stop if input freezes
- Emergency stop via SPACE key

## Requirements

- Unitree Go2 Pro, firmware 1.1.1 – 1.1.14
- Ubuntu 22.04, Python 3.10+
- X11 display server

## Installation

```bash
git clone https://github.com/legion1581/unitree_webrtc_connect.git
cd unitree_webrtc_connect
pip install -e .
pip install pynput opencv-python
```

## Configuration

Fetch your AES key (required for firmware data2=3):

```bash
python3 examples/fetch_aes_key.py --email your@email.com --password yourpassword
export GO2_AES_KEY="your_32_char_hex_key_here"
```

## Connection

1. Power on the robot, wait 30 seconds
2. Close the Unitree mobile app
3. Connect PC to robot WiFi (Bellabot)
4. `ping -c 3 192.168.12.1` to verify

## Usage

```bash
export GO2_AES_KEY="your_key_here"
python3 teleop_video2.py
```

## Controls

| Key | Action |
|-----|--------|
| W / ↑ | Move forward |
| S / ↓ | Move backward |
| A / ← | Turn left |
| D / → | Turn right |
| SPACE | Emergency stop |
| Q | Quit |

## Known Behaviors

- Robot operates in `mcf` mode — normal for Go2 Pro
- BalanceStand activates on startup (~2s) before accepting commands
- ICE filter to 192.168.12.x required when Ethernet is connected

## Acknowledgements

Built on [unitree_webrtc_connect](https://github.com/legion1581/unitree_webrtc_connect) by legion1581.
