import socket  # noqa: F401
import threading
import time
import sys
import secrets
import string
from collections import defaultdict, deque

store = {}
lists = {}
streams = {}
list_condition = threading.Condition()
blocked_clients = defaultdict(deque)
key_versions = defaultdict(int)
replica_connections = []
replica_lock = threading.Lock()
master_replid = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(40)).encode()


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


def stream_id_tuple(value):
    if value == b"-":
        return -1, -1
    if value == b"+":
        return float("inf"), float("inf")
    parts = value.split(b"-", 1)
    return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0


def handle_client(connection):
    with connection:
        buffer = b""
        transaction_queue = None
        watched_keys = set()
        watched_versions = {}
        while data := connection.recv(1024):
            buffer += data
            while parsed_command := parse_command(buffer):
                command, consumed_bytes = parsed_command
                buffer = buffer[consumed_bytes:]
                command_name = command[0].upper()
                if command_name in (b"SET", b"INCR", b"RPUSH", b"LPUSH", b"LPOP", b"XADD"):
                    encoded_command = b"*" + str(len(command)).encode() + b"\r\n"
                    for argument in command:
                        encoded_command += b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n"
                    with replica_lock:
                        for replica_connection in list(replica_connections):
                            try:
                                replica_connection.sendall(encoded_command)
                            except OSError:
                                replica_connections.remove(replica_connection)
                if transaction_queue is not None and command_name not in (b"EXEC", b"DISCARD", b"MULTI", b"WATCH"):
                    transaction_queue.append(command)
                    connection.sendall(b"+QUEUED\r\n")
                    continue
                if command_name == b"MULTI":
                    transaction_queue = []
                    connection.sendall(b"+OK\r\n")
                    continue
                if command_name == b"WATCH":
                    if transaction_queue is not None:
                        connection.sendall(b"-ERR WATCH inside MULTI is not allowed\r\n")
                        continue
                    watched_keys.update(command[1:])
                    watched_versions.update({key: key_versions[key] for key in command[1:]})
                    connection.sendall(b"+OK\r\n")
                    continue
                if command_name == b"UNWATCH":
                    watched_keys.clear()
                    watched_versions.clear()
                    connection.sendall(b"+OK\r\n")
                    continue
                if command_name == b"EXEC" and transaction_queue is None:
                    connection.sendall(b"-ERR EXEC without MULTI\r\n")
                    continue
                if command_name == b"EXEC" and transaction_queue == []:
                    transaction_queue = None
                    connection.sendall(b"*0\r\n")
                    continue
                if command_name == b"DISCARD":
                    if transaction_queue is None:
                        connection.sendall(b"-ERR DISCARD without MULTI\r\n")
                    else:
                        transaction_queue = None
                        watched_keys.clear()
                        watched_versions.clear()
                        connection.sendall(b"+OK\r\n")
                    continue
                if command_name == b"EXEC" and transaction_queue is not None:
                    queued = transaction_queue
                    transaction_queue = None
                    if any(key_versions[key] != watched_versions.get(key, key_versions[key]) for key in watched_keys):
                        watched_keys.clear()
                        watched_versions.clear()
                        connection.sendall(b"*-1\r\n")
                        continue
                    watched_keys.clear()
                    watched_versions.clear()
                    replies = []
                    for queued_command in queued:
                        queued_name = queued_command[0].upper()
                        if queued_name == b"SET":
                            store[queued_command[1]] = (queued_command[2], None)
                            key_versions[queued_command[1]] += 1
                            replies.append(b"+OK\r\n")
                        elif queued_name == b"INCR":
                            key = queued_command[1]
                            entry = store.get(key)
                            try:
                                number = 0 if entry is None else int(entry[0])
                                number += 1
                                store[key] = (str(number).encode(), None)
                                key_versions[key] += 1
                                replies.append(b":" + str(number).encode() + b"\r\n")
                            except ValueError:
                                replies.append(b"-ERR value is not an integer or out of range\r\n")
                        elif queued_name == b"GET":
                            entry = store.get(queued_command[1])
                            if entry is None:
                                replies.append(b"$-1\r\n")
                            else:
                                value = entry[0]
                                replies.append(b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n")
                        else:
                            replies.append(b"+PONG\r\n")
                    connection.sendall(b"*" + str(len(replies)).encode() + b"\r\n" + b"".join(replies))
                    continue
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
                    key_versions[command[1]] += 1
                    connection.sendall(b"+OK\r\n")
                elif command_name == b"INCR":
                    key = command[1]
                    entry = store.get(key)
                    try:
                        current = 0 if entry is None else int(entry[0])
                    except ValueError:
                        connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                        continue
                    current += 1
                    store[key] = (str(current).encode(), None)
                    key_versions[key] += 1
                    connection.sendall(b":" + str(current).encode() + b"\r\n")
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
                elif command_name == b"TYPE":
                    key = command[1]
                    if key in store:
                        response = b"+string\r\n"
                    elif key in streams:
                        response = b"+stream\r\n"
                    elif key in lists:
                        response = b"+list\r\n"
                    else:
                        response = b"+none\r\n"
                    connection.sendall(response)
                elif command_name == b"INFO":
                    role = globals().get("server_role", b"master")
                    info = b"role:" + role + b"\r\n"
                    if role == b"master":
                        info += b"master_replid:" + master_replid + b"\r\nmaster_repl_offset:0\r\n"
                    connection.sendall(b"$" + str(len(info)).encode() + b"\r\n" + info + b"\r\n")
                elif command_name == b"REPLCONF":
                    connection.sendall(b"+OK\r\n")
                elif command_name == b"PSYNC":
                    connection.sendall(b"+FULLRESYNC " + master_replid + b" 0\r\n")
                    empty_rdb = b"REDIS0011\xfe\x00\xff" + b"\x00" * 8
                    connection.sendall(b"$" + str(len(empty_rdb)).encode() + b"\r\n" + empty_rdb)
                    with replica_lock:
                        replica_connections.append(connection)
                elif command_name == b"XADD":
                    key, entry_id = command[1], command[2]
                    if entry_id == b"*":
                        time_part = str(int(time.time() * 1000)).encode()
                        prior = streams.get(key, [])
                        same_time = [int(item_id.split(b"-", 1)[1]) for item_id, _ in prior if item_id.split(b"-", 1)[0] == time_part]
                        sequence = max(same_time) + 1 if same_time else 0
                        entry_id = time_part + b"-" + str(sequence).encode()
                    if entry_id.endswith(b"-*"):
                        time_part = entry_id[:-2]
                        prior = streams.get(key, [])
                        same_time = [int(item_id.split(b"-", 1)[1]) for item_id, _ in prior if item_id.split(b"-", 1)[0] == time_part]
                        sequence = max(same_time) + 1 if same_time else (1 if time_part == b"0" else 0)
                        entry_id = time_part + b"-" + str(sequence).encode()
                    if entry_id == b"0-0":
                        connection.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                        continue
                    try:
                        new_id = tuple(map(int, entry_id.split(b"-", 1)))
                    except (ValueError, TypeError):
                        connection.sendall(b"-ERR Invalid stream ID specified\r\n")
                        continue
                    existing = streams.get(key, [])
                    if existing and new_id <= tuple(map(int, existing[-1][0].split(b"-", 1))):
                        connection.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        continue
                    fields = list(zip(command[3::2], command[4::2]))
                    with list_condition:
                        streams.setdefault(key, []).append((entry_id, fields))
                        list_condition.notify_all()
                    connection.sendall(b"$" + str(len(entry_id)).encode() + b"\r\n" + entry_id + b"\r\n")
                elif command_name == b"XRANGE":
                    key, start_id, end_id = command[1:4]
                    selected = [(entry_id, fields) for entry_id, fields in streams.get(key, []) if stream_id_tuple(start_id) <= stream_id_tuple(entry_id) <= stream_id_tuple(end_id)]
                    response = b"*" + str(len(selected)).encode() + b"\r\n"
                    for entry_id, fields in selected:
                        response += b"*2\r\n$" + str(len(entry_id)).encode() + b"\r\n" + entry_id
                        response += b"\r\n*" + str(len(fields) * 2).encode() + b"\r\n"
                        for field, value in fields:
                            for item in (field, value):
                                response += b"$" + str(len(item)).encode() + b"\r\n" + item + b"\r\n"
                    connection.sendall(response)
                elif command_name == b"XREAD":
                    block_index = next((index for index, part in enumerate(command) if part.upper() == b"BLOCK"), None)
                    block_timeout = int(command[block_index + 1]) / 1000 if block_index is not None else None
                    deadline = time.monotonic() + block_timeout if block_timeout else None
                    marker = next((index for index, part in enumerate(command) if part.upper() == b"STREAMS"), None)
                    if marker is None or len(command) < marker + 3:
                        connection.sendall(b"*-1\r\n")
                        continue
                    remaining = command[marker + 1 :]
                    midpoint = len(remaining) // 2
                    keys = remaining[:midpoint]
                    ids = remaining[midpoint:]
                    ids = [
                        (streams[key][-1][0] if start_id == b"$" and streams.get(key) else b"0-0")
                        if start_id == b"$" else start_id
                        for key, start_id in zip(keys, ids)
                    ]
                    streams_response = []
                    while True:
                        streams_response = []
                        for key, start_id in zip(keys, ids):
                            after = stream_id_tuple(start_id)
                            entries = [(entry_id, fields) for entry_id, fields in streams.get(key, []) if stream_id_tuple(entry_id) > after]
                            if entries:
                                entry_bytes = b"*" + str(len(entries)).encode() + b"\r\n"
                                for entry_id, fields in entries:
                                    entry_bytes += b"*2\r\n$" + str(len(entry_id)).encode() + b"\r\n" + entry_id + b"\r\n"
                                    flattened = [part for pair in fields for part in pair]
                                    entry_bytes += b"*" + str(len(flattened)).encode() + b"\r\n"
                                    for part in flattened:
                                        entry_bytes += b"$" + str(len(part)).encode() + b"\r\n" + part + b"\r\n"
                                streams_response.append((key, entry_bytes))
                        if streams_response or block_index is None:
                            break
                        remaining_time = None if deadline is None else deadline - time.monotonic()
                        if remaining_time is not None and remaining_time <= 0:
                            break
                        with list_condition:
                            list_condition.wait(remaining_time)
                    if not streams_response:
                        connection.sendall(b"*-1\r\n" if block_index is not None else b"*0\r\n")
                        continue
                    response = b"*" + str(len(streams_response)).encode() + b"\r\n"
                    for key, entries in streams_response:
                        response += b"*2\r\n$" + str(len(key)).encode() + b"\r\n" + key + b"\r\n" + entries
                    connection.sendall(response)
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
                    timeout = float(command[2]) if len(command) > 2 else 0
                    deadline = time.monotonic() + timeout if timeout else None
                    with list_condition:
                        blocked_clients[key].append(threading.current_thread())
                        while not lists.get(key):
                            remaining = None if deadline is None else deadline - time.monotonic()
                            if remaining is not None and remaining <= 0:
                                blocked_clients[key].remove(threading.current_thread())
                                connection.sendall(b"*-1\r\n")
                                break
                            list_condition.wait(remaining)
                        else:
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
    port = 6379
    role = b"master"
    replica_target = None
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    if "--replicaof" in sys.argv:
        role = b"slave"
        replica_target = sys.argv[sys.argv.index("--replicaof") + 1].split()
    global server_role
    server_role = role
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    if replica_target:
        master_host, master_port = replica_target
        with socket.create_connection((master_host, int(master_port))) as master_connection:
            master_connection.sendall(b"*1\r\n$4\r\nPING\r\n")
            master_connection.recv(1024)
            port_value = str(port).encode()
            master_connection.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + str(len(port_value)).encode() + b"\r\n" + port_value + b"\r\n")
            master_connection.recv(1024)
            master_connection.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
            master_connection.recv(1024)
            master_connection.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            master_connection.recv(1024)
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
