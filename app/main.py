import socket  # noqa: F401
import threading
import time
from collections import defaultdict, deque

store = {}
lists = {}
list_condition = threading.Condition()
blocked_clients = defaultdict(deque)


def parse_command(buffer):
    if not buffer.startswith(b"*"):
        line_end = buffer.find(b"\r\n")
        if line_end == -1:
            return None
        return [buffer[:line_end]], line_end + 2

    header_end = buffer.find(b"\r\n")
    if header_end == -1:
        return None

    argument_count = int(buffer[1:header_end])
    position = header_end + 2
    arguments = []
    for _ in range(argument_count):
        length_end = buffer.find(b"\r\n", position)
        if length_end == -1:
            return None

        argument_length = int(buffer[position + 1 : length_end])
        argument_start = length_end + 2
        argument_end = argument_start + argument_length
        if len(buffer) < argument_end + 2:
            return None

        arguments.append(buffer[argument_start:argument_end])
        position = argument_end + 2

    return arguments, position


def handle_client(connection):
    with connection:
        buffer = b""
        while data := connection.recv(1024):
            buffer += data
            while parsed_command := parse_command(buffer):
                command, consumed_bytes = parsed_command
                buffer = buffer[consumed_bytes:]
                command_name = command[0].upper()
                if command_name == b"ECHO":
                    argument = command[1]
                    connection.sendall(
                        b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n"
                    )
                elif command_name == b"SET":
                    expiry = None
                    if len(command) >= 5 and command[3].upper() == b"PX":
                        expiry = time.monotonic() + int(command[4]) / 1000
                    store[command[1]] = (command[2], expiry)
                    connection.sendall(b"+OK\r\n")
                elif command_name == b"GET":
                    entry = store.get(command[1])
                    if entry is None or (entry[1] is not None and time.monotonic() >= entry[1]):
                        store.pop(command[1], None)
                        connection.sendall(b"$-1\r\n")
                    else:
                        value = entry[0]
                        connection.sendall(
                            b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                        )
                elif command_name == b"RPUSH":
                    with list_condition:
                        values = lists.setdefault(command[1], [])
                        values.extend(command[2:])
                        list_condition.notify_all()
                        size = len(values)
                    connection.sendall(b":" + str(size).encode() + b"\r\n")
                elif command_name == b"LPUSH":
                    values = lists.setdefault(command[1], [])
                    values[0:0] = reversed(command[2:])
                    connection.sendall(b":" + str(len(values)).encode() + b"\r\n")
                elif command_name == b"LLEN":
                    connection.sendall(b":" + str(len(lists.get(command[1], []))).encode() + b"\r\n")
                elif command_name == b"LPOP":
                    values = lists.get(command[1], [])
                    if len(command) > 2:
                        count = max(0, int(command[2]))
                        removed = values[:count]
                        del values[:count]
                        response = b"*" + str(len(removed)).encode() + b"\r\n"
                        for value in removed:
                            response += b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                        connection.sendall(response)
                    elif values:
                        value = values.pop(0)
                        connection.sendall(b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n")
                    else:
                        connection.sendall(b"$-1\r\n")
                elif command_name == b"BLPOP":
                    key = command[1]
                    with list_condition:
                        blocked_clients[key].append(threading.current_thread())
                        while not lists.get(key):
                            list_condition.wait()
                        blocked_clients[key].popleft()
                        value = lists[key].pop(0)
                    response = b"*2\r\n"
                    for item in (key, value):
                        response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                    connection.sendall(response)
                elif command_name == b"LRANGE":
                    values = lists.get(command[1], [])
                    start, stop = int(command[2]), int(command[3])
                    if start < 0:
                        start = max(0, len(values) + start)
                    if stop < 0:
                        stop = len(values) + stop
                    selected = values[start : stop + 1] if start <= stop else []
                    response = b"*" + str(len(selected)).encode() + b"\r\n"
                    for value in selected:
                        response += b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                    connection.sendall(response)
                else:
                    connection.sendall(b"+PONG\r\n")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
