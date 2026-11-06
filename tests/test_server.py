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

    def test_replies_to_a_second_client_while_the_first_client_is_idle(self):
        """Serving clients serially should leave the second client without a reply."""
        process = subprocess.Popen(
            ["./your_program.sh"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        first_client = None
        try:
            for _ in range(30):
                try:
                    first_client = socket.create_connection(("localhost", 6379), timeout=0.1)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.1)
            else:
                self.fail("Redis server did not accept a connection on port 6379")

            with socket.create_connection(("localhost", 6379), timeout=0.1) as second_client:
                second_client.settimeout(0.2)
                second_client.sendall(b"PING\r\n")
                try:
                    response = second_client.recv(1024)
                except socket.timeout:
                    response = b""
                self.assertEqual(response, b"+PONG\r\n")
        finally:
            if first_client:
                first_client.close()
            process.terminate()
            process.wait(timeout=5)

    def test_echoes_a_resp_argument_as_a_bulk_string(self):
        """Returning a PONG instead of the parsed ECHO argument should fail."""
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
                        client.sendall(b"*2\r\n$4\r\neChO\r\n$5\r\nhello\r\n")
                        self.assertEqual(client.recv(1024), b"$5\r\nhello\r\n")
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)

            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_sets_and_gets_a_value(self):
        """Discarding SET values should make the later GET return the wrong response."""
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
                        client.sendall(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
                        self.assertEqual(client.recv(1024), b"+OK\r\n")
                        client.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
                        self.assertEqual(client.recv(1024), b"$5\r\nvalue\r\n")
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)

            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_expires_a_value_set_with_px(self):
        """Ignoring PX should leave the value readable after its expiry."""
        process = subprocess.Popen(["./your_program.sh"], cwd=PROJECT_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(30):
                try:
                    with socket.create_connection(("localhost", 6379), timeout=0.1) as client:
                        client.sendall(b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$2\r\n50\r\n")
                        self.assertEqual(client.recv(1024), b"+OK\r\n")
                        time.sleep(0.1)
                        client.sendall(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
                        self.assertEqual(client.recv(1024), b"$-1\r\n")
                        return
                except ConnectionRefusedError:
                    time.sleep(0.1)
            self.fail("Redis server did not accept a connection on port 6379")
        finally:
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
