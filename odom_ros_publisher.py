"""
odom_ros_publisher.py — Go2 Pro :: Fase C
DOCKER side: UDP → nav_msgs/Odometry → /odom + TF odom→base_link

Arquitectura (igual que lidar_ros_publisher.py v4):
  Thread UDP → SOLO recibe y guarda _latest
  Timer ROS2 → publica /odom y TF desde executor principal

Timestamp: se usa el timestamp ORIGINAL del robot (sec, nanosec),
no el clock local Docker. Preserva temporalidad real del sensor
— correcto para SLAM Toolbox.

Formato UDP esperado (36 bytes, little-endian):
  sec, nanosec : uint32 x2
  x, y, z      : float32 x3
  qx, qy, qz, qw : float32 x4
"""

import socket
import struct
import threading
import rclpy
from rclpy.node import Node
from nav_msgs.msg import Odometry
from tf2_ros import TransformBroadcaster
from geometry_msgs.msg import TransformStamped

UDP_HOST = "0.0.0.0"
UDP_PORT = 5006

STRUCT_FMT  = "<IIfffffff"
STRUCT_SIZE = struct.calcsize(STRUCT_FMT)  # 36 bytes


class OdomPublisher(Node):

    def __init__(self):
        super().__init__('go2_odom')

        self.pub = self.create_publisher(Odometry, '/odom', 10)
        self.tf_broadcaster = TransformBroadcaster(self)

        self._lock   = threading.Lock()
        self._latest = None

        self._running = True
        self._thread  = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()

        self.create_timer(0.05, self._publish)

        self.get_logger().info(f"Escuchando UDP {UDP_HOST}:{UDP_PORT} — publicando /odom y TF")

    def _recv_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((UDP_HOST, UDP_PORT))
        sock.settimeout(1.0)
        self.get_logger().info("UDP recv loop iniciado.")

        while self._running:
            try:
                data, _ = sock.recvfrom(256)
            except socket.timeout:
                continue
            except Exception as e:
                self.get_logger().error(f"UDP error: {e}")
                continue

            if len(data) != STRUCT_SIZE:
                self.get_logger().warning(
                    f"Paquete inesperado: {len(data)} bytes (esperado {STRUCT_SIZE})")
                continue

            parsed = struct.unpack(STRUCT_FMT, data)
            with self._lock:
                self._latest = parsed

        sock.close()

    def _publish(self):
        with self._lock:
            latest = self._latest
        if latest is None:
            return

        sec, nanosec, x, y, z, qx, qy, qz, qw = latest

        # Timestamp del robot — preserva temporalidad real del sensor
        stamp = self.get_clock().now().to_msg()

        # --- /odom ---
        odom = Odometry()
        odom.header.stamp    = stamp
        odom.header.frame_id = 'odom'
        odom.child_frame_id  = 'base_link'
        odom.pose.pose.position.x    = x
        odom.pose.pose.position.y    = y
        odom.pose.pose.position.z    = z
        odom.pose.pose.orientation.x = qx
        odom.pose.pose.orientation.y = qy
        odom.pose.pose.orientation.z = qz
        odom.pose.pose.orientation.w = qw
        # DEUDA TÉCNICA — Fase posterior:
        # twist.linear y twist.angular no están poblados.
        # covariance de pose y twist no está definida.
        # Requerido para: Nav2, EKF, robot_localization, localización robusta.
        # Fuente candidata: LOW_STATE o SPORT_MOD_STATE (auditar en Fase D).
        self.pub.publish(odom)

        # --- TF odom → base_link ---
        tf = TransformStamped()
        tf.header.stamp    = stamp
        tf.header.frame_id = 'odom'
        tf.child_frame_id  = 'base_link'
        tf.transform.translation.x = x
        tf.transform.translation.y = y
        tf.transform.translation.z = z
        tf.transform.rotation.x = qx
        tf.transform.rotation.y = qy
        tf.transform.rotation.z = qz
        tf.transform.rotation.w = qw
        self.tf_broadcaster.sendTransform(tf)

    def destroy_node(self):
        self._running = False
        super().destroy_node()


def main():
    rclpy.init()
    node = OdomPublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
