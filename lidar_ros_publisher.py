"""
lidar_ros_publisher.py — Fase B v4
Thread UDP: SOLO recibe y guarda ultimo scan.
Timer ROS2: publica /scan y /tf desde executor principal.
"""
import socket
import struct
import threading
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import LaserScan

UDP_HOST = "0.0.0.0"
UDP_PORT = 5005

class LaserScanPublisher(Node):
    def __init__(self):
        super().__init__('go2_laserscan')
        self.pub = self.create_publisher(LaserScan, '/scan', 10)


        # Estado compartido — thread escribe, timer lee
        self._lock = threading.Lock()
        self._latest = None

        # Thread UDP — SOLO recibe
        self._running = True
        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()

        # Timer ROS2 — SOLO publica desde executor principal
        self.create_timer(0.1, self._publish)
        self.get_logger().info("Listo.")

    def _recv_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((UDP_HOST, UDP_PORT))
        sock.settimeout(1.0)
        while self._running:
            try:
                data, _ = sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception as e:
                self.get_logger().error(f"UDP: {e}")
                continue
            parsed = self._parse(data)
            if parsed:
                with self._lock:
                    self._latest = parsed
        sock.close()

    def _parse(self, data):
        header_size = struct.calcsize('IIffff')
        if len(data) < header_size:
            return None
        frame_count, num_bins, angle_min, angle_max, range_min, range_max = \
            struct.unpack('IIffff', data[:header_size])
        ranges_bytes = data[header_size:]
        if len(ranges_bytes) != num_bins * 4:
            return None
        ranges = list(struct.unpack(f'{num_bins}f', ranges_bytes))
        return (frame_count, num_bins, angle_min, angle_max, range_min, range_max, ranges)

    def _publish(self):
        with self._lock:
            latest = self._latest
        if latest is None:
            return
        frame_count, num_bins, angle_min, angle_max, range_min, range_max, ranges = latest
        msg = LaserScan()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = 'laser'
        msg.angle_min = angle_min
        msg.angle_max = angle_max
        msg.angle_increment = (angle_max - angle_min) / num_bins
        msg.time_increment = 0.0
        msg.scan_time = 0.1
        msg.range_min = range_min
        msg.range_max = range_max
        msg.ranges = ranges
        self.pub.publish(msg)
        self.get_logger().info(f"frame={frame_count}  puntos={sum(1 for r in ranges if r > 0)}")

    def destroy_node(self):
        self._running = False
        super().destroy_node()

def main():
    rclpy.init()
    node = LaserScanPublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
