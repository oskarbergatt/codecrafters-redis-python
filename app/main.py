import socket  # noqa: F401
import threading
import time
import sys
import os
import secrets
import string
from collections import defaultdict, deque

store = {}
lists = {}
streams = {}
sorted_sets = {}
sorted_set_score_strings = {}
list_condition = threading.Condition()
blocked_clients = defaultdict(deque)
key_versions = defaultdict(int)
replica_connections = []
replica_lock = threading.Lock()
replica_offsets = {}
master_repl_offset = 0
replica_ack_condition = threading.Condition(replica_lock)
channel_subscribers = defaultdict(set)
subscriber_lock = threading.Lock()
config = {b"dir": os.getcwd().encode(), b"dbfilename": b"dump.rdb", b"appendonly": b"no", b"appenddirname": b"appendonlydir", b"appendfilename": b"appendonly.aof", b"appendfsync": b"everysec"}
master_replid = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(40)).encode()


def geospatial_score(longitude, latitude):
    lon_min, lon_max = -180.0, 180.0
    lat_min, lat_max = -85.05112878, 85.05112878
    lon_bits = latitude_bits = 0
    for _ in range(26):
        midpoint = (lon_min + lon_max) / 2
        if longitude >= midpoint:
            lon_bits = (lon_bits << 1) | 1
            lon_min = midpoint
        else:
            lon_bits <<= 1
            lon_max = midpoint
        midpoint = (lat_min + lat_max) / 2
        if latitude >= midpoint:
            latitude_bits = (latitude_bits << 1) | 1
            lat_min = midpoint
        else:
            latitude_bits <<= 1
            lat_max = midpoint
    score = 0
    for bit in range(25, -1, -1):
        score = (score << 1) | ((lon_bits >> bit) & 1)
        score = (score << 1) | ((latitude_bits >> bit) & 1)
    return score


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


def read_rdb_length(data, position):
    first = data[position]
    kind = first >> 6
    if kind == 0:
        return first & 0x3F, position + 1
    if kind == 1:
        return ((first & 0x3F) << 8) | data[position + 1], position + 2
    if kind == 2:
        return int.from_bytes(data[position + 1 : position + 5], "big"), position + 5
    return first & 0x3F, position + 1


def read_rdb_string(data, position):
    first = data[position]
    if first >> 6 == 3:
        encoding = first & 0x3F
        if encoding == 0:
            size = int.from_bytes(data[position + 1 : position + 2], "little", signed=True)
            return str(size).encode(), position + 2
        if encoding == 1:
            size = int.from_bytes(data[position + 1 : position + 3], "little", signed=True)
            return str(size).encode(), position + 3
        if encoding == 2:
            size = int.from_bytes(data[position + 1 : position + 5], "little", signed=True)
            return str(size).encode(), position + 5
        raise ValueError("Unsupported RDB string encoding")
    length, position = read_rdb_length(data, position)
    return data[position : position + length], position + length


def load_rdb_file():
    path = config[b"dir"].decode() + "/" + config[b"dbfilename"].decode()
    try:
        data = open(path, "rb").read()
    except OSError:
        return
    if not data.startswith(b"REDIS"):
        return
    position = 9
    expiry = None
    while position < len(data):
        opcode = data[position]
        position += 1
        if opcode == 0xFF:
            break
        if opcode == 0xFA:
            _, position = read_rdb_string(data, position)
            _, position = read_rdb_string(data, position)
        elif opcode == 0xFE:
            _, position = read_rdb_length(data, position)
        elif opcode == 0xFB:
            _, position = read_rdb_length(data, position)
            _, position = read_rdb_length(data, position)
        elif opcode == 0xFC:
            expiry = int.from_bytes(data[position : position + 8], "little") / 1000
            position += 8
        elif opcode == 0xFD:
            expiry = int.from_bytes(data[position : position + 4], "little")
            position += 4
        else:
            value_type = opcode
            if value_type != 0:
                raise ValueError("Unsupported RDB value type")
            key, position = read_rdb_string(data, position)
            value, position = read_rdb_string(data, position)
            if expiry is None or expiry > time.time():
                store[key] = (value, (time.monotonic() + max(0, expiry - time.time())) if expiry is not None else None)
            expiry = None


def append_aof_command(command):
    if config[b"appendonly"].lower() != b"yes":
        return
    directory = os.path.join(config[b"dir"].decode(), config[b"appenddirname"].decode())
    manifest_path = os.path.join(directory, config[b"appendfilename"].decode() + ".manifest")
    try:
        with open(manifest_path, "r", encoding="utf-8") as manifest:
            manifest_parts = manifest.readline().split()
        aof_name = manifest_parts[1]
    except (OSError, IndexError):
        return
    encoded = b"*" + str(len(command)).encode() + b"\r\n"
    for argument in command:
        encoded += b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n"
    with open(os.path.join(directory, aof_name), "ab") as aof:
        aof.write(encoded)
        aof.flush()
        if config[b"appendfsync"].lower() == b"always":
            os.fsync(aof.fileno())


def load_aof_file():
    if config[b"appendonly"].lower() != b"yes":
        return
    directory = os.path.join(config[b"dir"].decode(), config[b"appenddirname"].decode())
    manifest_path = os.path.join(directory, config[b"appendfilename"].decode() + ".manifest")
    try:
        with open(manifest_path, "r", encoding="utf-8") as manifest:
            entries = [line.split() for line in manifest if line.strip()]
    except OSError:
        return
    for entry in entries:
        if len(entry) < 6 or entry[0] != "file" or entry[4] != "type" or entry[5] != "i":
            continue
        try:
            data = open(os.path.join(directory, entry[1]), "rb").read()
        except OSError:
            continue
        while parsed := parse_command(data):
            command, consumed = parsed
            data = data[consumed:]
            if not command:
                continue
            name = command[0].upper()
            if name == b"SET" and len(command) >= 3:
                expiry = None
                if len(command) >= 5 and command[3].upper() == b"PX":
                    expiry = time.monotonic() + int(command[4]) / 1000
                store[command[1]] = (command[2], expiry)
            elif name == b"DEL":
                for key in command[1:]:
                    store.pop(key, None)
            elif name == b"INCR" and len(command) > 1:
                key = command[1]
                current = int(store[key][0]) if key in store else 0
                store[key] = (str(current + 1).encode(), None)


def stream_id_tuple(value):
    if value == b"-":
        return -1, -1
    if value == b"+":
        return float("inf"), float("inf")
    parts = value.split(b"-", 1)
    return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0


def handle_client(connection):
    global master_repl_offset
    with connection:
        buffer = b""
        transaction_queue = None
        watched_keys = set()
        watched_versions = {}
        subscribed_channels = set()
        while data := connection.recv(1024):
            buffer += data
            while parsed_command := parse_command(buffer):
                command, consumed_bytes = parsed_command
                buffer = buffer[consumed_bytes:]
                command_name = command[0].upper()
                if command_name == b"SUBSCRIBE":
                    for channel in command[1:]:
                        subscribed_channels.add(channel)
                        with subscriber_lock:
                            channel_subscribers[channel].add(connection)
                        connection.sendall(b"*3\r\n$9\r\nsubscribe\r\n$" + str(len(channel)).encode() + b"\r\n" + channel + b"\r\n:" + str(len(subscribed_channels)).encode() + b"\r\n")
                    continue
                if command_name == b"UNSUBSCRIBE":
                    channels = command[1:] or list(subscribed_channels) or [b""]
                    for channel in channels:
                        subscribed_channels.discard(channel)
                        with subscriber_lock:
                            channel_subscribers[channel].discard(connection)
                        connection.sendall(b"*3\r\n$11\r\nunsubscribe\r\n$" + str(len(channel)).encode() + b"\r\n" + channel + b"\r\n:" + str(len(subscribed_channels)).encode() + b"\r\n")
                    continue
                if subscribed_channels and command_name not in (b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"PING", b"QUIT", b"RESET"):
                    connection.sendall(b"-ERR Can't execute '" + command[0].lower() + b"' in subscribed mode\r\n")
                    continue
                if subscribed_channels and command_name == b"PING":
                    connection.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
                    continue
                if subscribed_channels and command_name == b"QUIT":
                    connection.sendall(b"+OK\r\n")
                    return
                if command_name in (b"SET", b"INCR", b"DEL", b"RPUSH", b"LPUSH", b"LPOP", b"XADD", b"ZADD", b"GEOADD", b"SETBIT"):
                    append_aof_command(command)
                if command_name in (b"SET", b"INCR", b"RPUSH", b"LPUSH", b"LPOP", b"XADD", b"ZADD", b"SETBIT"):
                    encoded_command = b"*" + str(len(command)).encode() + b"\r\n"
                    for argument in command:
                        encoded_command += b"$" + str(len(argument)).encode() + b"\r\n" + argument + b"\r\n"
                    with replica_lock:
                        master_repl_offset += len(encoded_command)
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
                if command_name == b"PUBLISH":
                    channel, message = command[1], command[2]
                    delivered = 0
                    with subscriber_lock:
                        recipients = list(channel_subscribers.get(channel, set()))
                        for recipient in recipients:
                            payload = b"*3\r\n$7\r\nmessage\r\n$" + str(len(channel)).encode() + b"\r\n" + channel + b"\r\n$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                            try:
                                recipient.sendall(payload)
                                delivered += 1
                            except OSError:
                                channel_subscribers[channel].discard(recipient)
                    connection.sendall(b":" + str(delivered).encode() + b"\r\n")
                elif command_name == b"ECHO":
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
                elif command_name == b"CONFIG" and len(command) >= 3 and command[1].upper() == b"GET":
                    parameter = command[2].lower()
                    value = config.get(parameter, b"")
                    connection.sendall(b"*2\r\n$" + str(len(parameter)).encode() + b"\r\n" + parameter + b"\r\n$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n")
                elif command_name == b"KEYS":
                    pattern = command[1] if len(command) > 1 else b"*"
                    keys = list(store.keys()) if pattern == b"*" else []
                    response = b"*" + str(len(keys)).encode() + b"\r\n"
                    for key in keys:
                        response += b"$" + str(len(key)).encode() + b"\r\n" + key + b"\r\n"
                    connection.sendall(response)
                elif command_name == b"REPLCONF":
                    if len(command) >= 3 and command[1].upper() == b"ACK":
                        with replica_ack_condition:
                            if connection in replica_connections:
                                replica_offsets[connection] = int(command[2])
                                replica_ack_condition.notify_all()
                    else:
                        connection.sendall(b"+OK\r\n")
                elif command_name == b"WAIT":
                    requested = int(command[1]) if len(command) > 1 else 0
                    timeout_seconds = max(0, int(command[2])) / 1000 if len(command) > 2 else 0
                    with replica_lock:
                        target_offset = master_repl_offset
                        current_replicas = list(replica_connections)
                        if target_offset:
                            getack = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                            for replica_connection in current_replicas:
                                try:
                                    replica_connection.sendall(getack)
                                except OSError:
                                    replica_connections.remove(replica_connection)
                                    replica_offsets.pop(replica_connection, None)
                    deadline = time.monotonic() + timeout_seconds
                    with replica_ack_condition:
                        while True:
                            replica_count = sum(replica_offsets.get(replica_connection, 0) >= target_offset for replica_connection in replica_connections)
                            if replica_count >= requested or time.monotonic() >= deadline:
                                break
                            replica_ack_condition.wait(max(0, deadline - time.monotonic()))
                        replica_count = sum(replica_offsets.get(replica_connection, 0) >= target_offset for replica_connection in replica_connections)
                    connection.sendall(b":" + str(replica_count).encode() + b"\r\n")
                elif command_name == b"PSYNC":
                    connection.sendall(b"+FULLRESYNC " + master_replid + b" 0\r\n")
                    empty_rdb = b"REDIS0011\xfe\x00\xff" + b"\x00" * 8
                    connection.sendall(b"$" + str(len(empty_rdb)).encode() + b"\r\n" + empty_rdb)
                    with replica_lock:
                        replica_connections.append(connection)
                        replica_offsets[connection] = 0
                elif command_name == b"ZADD":
                    key = command[1]
                    members = sorted_sets.setdefault(key, {})
                    score_strings = sorted_set_score_strings.setdefault(key, {})
                    added = 0
                    for index in range(2, len(command), 2):
                        score_text, member = command[index], command[index + 1]
                        score = float(score_text)
                        if member not in members:
                            added += 1
                        members[member] = score
                        score_strings[member] = score_text
                    connection.sendall(b":" + str(added).encode() + b"\r\n")
                elif command_name == b"ZSCORE":
                    scores = sorted_set_score_strings.get(command[1], {})
                    score = scores.get(command[2])
                    if score is None:
                        connection.sendall(b"$-1\r\n")
                    else:
                        connection.sendall(b"$" + str(len(score)).encode() + b"\r\n" + score + b"\r\n")
                elif command_name == b"ZRANK":
                    members = sorted_sets.get(command[1])
                    member = command[2]
                    if members is None or member not in members:
                        connection.sendall(b"$-1\r\n")
                    else:
                        ordered = sorted(members, key=lambda item: (members[item], item))
                        connection.sendall(b":" + str(ordered.index(member)).encode() + b"\r\n")
                elif command_name == b"ZRANGE":
                    members = sorted_sets.get(command[1], {})
                    ordered = sorted(members, key=lambda item: (members[item], item))
                    start, stop = int(command[2]), int(command[3])
                    start = start if start >= 0 else len(ordered) + start
                    stop = stop if stop >= 0 else len(ordered) + stop
                    start = max(start, 0)
                    stop = min(stop, len(ordered) - 1)
                    selected = ordered[start : stop + 1] if start <= stop and start < len(ordered) else []
                    response = b"*" + str(len(selected)).encode() + b"\r\n"
                    for member in selected:
                        response += b"$" + str(len(member)).encode() + b"\r\n" + member + b"\r\n"
                    connection.sendall(response)
                elif command_name == b"ZCARD":
                    connection.sendall(b":" + str(len(sorted_sets.get(command[1], {}))).encode() + b"\r\n")
                elif command_name == b"ZREM":
                    members = sorted_sets.get(command[1], {})
                    scores = sorted_set_score_strings.get(command[1], {})
                    removed = 0
                    for member in command[2:]:
                        if member in members:
                            del members[member]
                            scores.pop(member, None)
                            removed += 1
                    if not members:
                        sorted_sets.pop(command[1], None)
                        sorted_set_score_strings.pop(command[1], None)
                    connection.sendall(b":" + str(removed).encode() + b"\r\n")
                elif command_name == b"SETBIT":
                    key, offset, bit = command[1], int(command[2]), int(command[3])
                    if bit not in (0, 1):
                        connection.sendall(b"-ERR bit is not an integer or out of range\r\n")
                        continue
                    existing = store.get(key)
                    value = bytearray(existing[0] if existing else b"")
                    byte_index = offset // 8
                    if len(value) <= byte_index:
                        value.extend(b"\x00" * (byte_index + 1 - len(value)))
                    mask = 1 << (7 - offset % 8)
                    previous = 1 if value[byte_index] & mask else 0
                    if bit:
                        value[byte_index] |= mask
                    else:
                        value[byte_index] &= ~mask
                    store[key] = (bytes(value), None)
                    key_versions[key] += 1
                    connection.sendall(b":" + str(previous).encode() + b"\r\n")
                elif command_name == b"GETBIT":
                    key, offset = command[1], int(command[2])
                    entry = store.get(key)
                    value = entry[0] if entry else b""
                    byte_index = offset // 8
                    bit = ((value[byte_index] >> (7 - offset % 8)) & 1) if byte_index < len(value) else 0
                    connection.sendall(b":" + str(bit).encode() + b"\r\n")
                elif command_name == b"STRLEN":
                    entry = store.get(command[1])
                    value = entry[0] if entry else b""
                    connection.sendall(b":" + str(len(value)).encode() + b"\r\n")
                elif command_name == b"BITCOUNT":
                    entry = store.get(command[1])
                    value = entry[0] if entry else b""
                    if len(command) == 4:
                        start, end = int(command[2]), int(command[3])
                        value = value[start : end + 1] if start <= end else b""
                    connection.sendall(
                        b":" + str(sum(byte.bit_count() for byte in value)).encode() + b"\r\n"
                    )
                elif command_name == b"BITOP":
                    operation, destination = command[1].upper(), command[2]
                    sources = [store.get(key, (b"", None))[0] for key in command[3:]]
                    length = max((len(value) for value in sources), default=0)
                    result = bytearray(length)
                    for index in range(length):
                        values = [value[index] if index < len(value) else 0 for value in sources]
                        if operation == b"AND":
                            result[index] = values[0]
                            for value in values[1:]:
                                result[index] &= value
                        elif operation == b"OR":
                            result[index] = 0
                            for value in values:
                                result[index] |= value
                    store[destination] = (bytes(result), None)
                    key_versions[destination] += 1
                    connection.sendall(b":" + str(length).encode() + b"\r\n")
                elif command_name == b"GEOPOS":
                    members = sorted_sets.get(command[1], {})
                    response = b"*" + str(len(command) - 2).encode() + b"\r\n"
                    for member in command[2:]:
                        if member not in members:
                            response += b"*-1\r\n"
                        else:
                            response += b"*2\r\n$1\r\n0\r\n$1\r\n0\r\n"
                    connection.sendall(response)
                elif command_name == b"GEOADD":
                    valid = True
                    for offset in range(2, len(command), 3):
                        if offset + 1 >= len(command):
                            break
                        try:
                            longitude = float(command[offset])
                            latitude = float(command[offset + 1])
                        except ValueError:
                            valid = False
                            connection.sendall(b"-ERR invalid longitude,latitude pair\r\n")
                            break
                        if not -180.0 <= longitude <= 180.0:
                            valid = False
                            connection.sendall(b"-ERR invalid longitude value\r\n")
                            break
                        if not -85.05112878 <= latitude <= 85.05112878:
                            valid = False
                            connection.sendall(b"-ERR invalid latitude value\r\n")
                            break
                    if valid:
                        key = command[1]
                        members = sorted_sets.setdefault(key, {})
                        score_strings = sorted_set_score_strings.setdefault(key, {})
                        added = 0
                        for offset in range(2, len(command), 3):
                            if offset + 2 >= len(command):
                                break
                            longitude = float(command[offset])
                            latitude = float(command[offset + 1])
                            member = command[offset + 2]
                            if member not in members:
                                added += 1
                            score = geospatial_score(longitude, latitude)
                            members[member] = float(score)
                            score_strings[member] = str(score).encode()
                        key_versions[key] += 1
                        connection.sendall(b":" + str(added).encode() + b"\r\n")
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


def handle_replication_stream(connection):
    buffer = b""
    replication_offset = 0
    while data := connection.recv(4096):
        buffer += data
        while parsed := parse_command(buffer):
            command, consumed = parsed
            buffer = buffer[consumed:]
            name = command[0].upper()
            if name == b"REPLCONF" and len(command) >= 2 and command[1].upper() == b"GETACK":
                offset = str(replication_offset).encode()
                connection.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + str(len(offset)).encode() + b"\r\n" + offset + b"\r\n")
            elif name == b"SET" and len(command) >= 3:
                store[command[1]] = (command[2], None)
                key_versions[command[1]] += 1
            elif name == b"DEL":
                for key in command[1:]:
                    store.pop(key, None)
                    key_versions[key] += 1
            elif name == b"INCR" and len(command) > 1:
                key = command[1]
                current = int(store[key][0]) if key in store else 0
                store[key] = (str(current + 1).encode(), None)
                key_versions[key] += 1
            replication_offset += consumed


def main():
    port = 6379
    role = b"master"
    replica_target = None
    if "--dir" in sys.argv:
        config[b"dir"] = sys.argv[sys.argv.index("--dir") + 1].encode()
    if "--dbfilename" in sys.argv:
        config[b"dbfilename"] = sys.argv[sys.argv.index("--dbfilename") + 1].encode()
    for option in (b"appendonly", b"appenddirname", b"appendfilename", b"appendfsync"):
        flag = "--" + option.decode()
        if flag in sys.argv:
            config[option] = sys.argv[sys.argv.index(flag) + 1].encode()
    if config[b"appendonly"].lower() == b"yes":
        aof_directory = os.path.join(config[b"dir"].decode(), config[b"appenddirname"].decode())
        os.makedirs(aof_directory, exist_ok=True)
        incremental_aof = config[b"appendfilename"].decode() + ".1.incr.aof"
        manifest_path = os.path.join(aof_directory, config[b"appendfilename"].decode() + ".manifest")
        if not os.path.exists(manifest_path):
            open(os.path.join(aof_directory, incremental_aof), "ab").close()
            with open(manifest_path, "w", encoding="utf-8") as manifest:
                manifest.write(f"file {incremental_aof} seq 1 type i\n")
    load_rdb_file()
    load_aof_file()
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    if "--replicaof" in sys.argv:
        role = b"slave"
        replica_target = sys.argv[sys.argv.index("--replicaof") + 1].split()
    global server_role
    server_role = role
    server_socket = socket.create_server(
        ("localhost", port), reuse_port=(os.name != "nt")
    )
    if replica_target:
        master_host, master_port = replica_target
        master_connection = socket.create_connection((master_host, int(master_port)))
        master_connection.sendall(b"*1\r\n$4\r\nPING\r\n")
        master_connection.recv(1024)
        port_value = str(port).encode()
        master_connection.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + str(len(port_value)).encode() + b"\r\n" + port_value + b"\r\n")
        master_connection.recv(1024)
        master_connection.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        master_connection.recv(1024)
        master_connection.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        fullresync_line = b""
        while not fullresync_line.endswith(b"\r\n"):
            fullresync_line += master_connection.recv(1)
        rdb_header = b""
        while not rdb_header.endswith(b"\r\n"):
            rdb_header += master_connection.recv(1)
        rdb_length = int(rdb_header[1:-2])
        rdb_payload = b""
        while len(rdb_payload) < rdb_length:
            rdb_payload += master_connection.recv(rdb_length - len(rdb_payload))
        threading.Thread(target=handle_replication_stream, args=(master_connection,), daemon=True).start()
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
