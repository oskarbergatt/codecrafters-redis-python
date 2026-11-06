import socket
import subprocess
import time
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RedisServerTests(unittest.TestCase):
    def test_replies_with_pong_to_a_command(self):
        """Removing the RESP response should make this assertion fail."""
        process = subprocess.Popen(
            ["./your_program.sh"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(30):
                try:
                    with socket.create_connection(("localhost", 6379), timeout=0.1) as client:
                        client.sendall(b"PING\r\n")
                        try:
                            response = client.recv(1024)
                        except ConnectionResetError:
                            response = b""
                        self.assertEqual(response, b"+PONG\r\n")
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)

            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_replies_to_multiple_commands_on_the_same_connection(self):
        """Closing a connection after the first command should fail this test."""
        process = subprocess.Popen(
            ["./your_program.sh"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(30):
                try:
                    with socket.create_connection(("localhost", 6379), timeout=0.1) as client:
                        client.sendall(b"PING\r\n")
                        self.assertEqual(client.recv(1024), b"+PONG\r\n")
                        client.sendall(b"PING\r\n")
                        self.assertEqual(client.recv(1024), b"+PONG\r\n")
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)

            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
