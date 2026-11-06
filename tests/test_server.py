import socket
import subprocess
import time
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RedisServerTests(unittest.TestCase):
    def test_accepts_a_tcp_connection_on_port_6379(self):
        """Removing the listening socket should make this connection fail."""
        process = subprocess.Popen(
            ["./your_program.sh"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(30):
                try:
                    with socket.create_connection(("localhost", 6379), timeout=0.1):
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)

            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
