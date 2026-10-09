import socket
import threading
import time
from collections import deque

# Storage
kv_store = {}          # string values: {key: value}
lists = {}             # list values: {key: deque([...])}
blocked_clients = {}   # {key: [(conn, deadline)]}  for BLPOP
lock = threading.Lock()

# ============================================================
# RESP PARSING
# ============================================================
def parse_resp(data):
    """Parse incoming RESP data. Returns list of commands."""
    commands = []
    try:
        lines = data.split(b'\r\n')
        i = 0
        while i < len(lines):
            line = lines[i]
            if not line:
                i += 1
                continue
            if line.startswith(b'*'):
                num_args = int(line[1:])
                args = []
                i += 1
                for _ in range(num_args):
                    if i >= len(lines): break
                    arg_line = lines[i]
                    if arg_line.startswith(b'$'):
                        length = int(arg_line[1:])
                        i += 1
                        if i < len(lines):
                            args.append(lines[i][:length].decode('utf-8', errors='replace'))
                        i += 1
                    else:
                        args.append(arg_line.decode('utf-8', errors='replace'))
                        i += 1
                if args:
                    commands.append(args)
            else:
                # Inline command (plain text, like telnet)
                parts = line.decode('utf-8', errors='replace').split()
                if parts:
                    commands.append(parts)
                i += 1
    except Exception as e:
        print(f"[Parse error] {e}")
    return commands

# ============================================================
# RESP RESPONSES
# ============================================================
def resp_simple(s):
    return f"+{s}\r\n".encode()

def resp_error(s):
    return f"-{s}\r\n".encode()

def resp_int(i):
    return f":{i}\r\n".encode()

def resp_bulk(s):
    if s is None:
        return b"$-1\r\n"
    data = str(s).encode()
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"

def resp_array(items):
    if items is None:
        return b"*-1\r\n"
    out = b"*" + str(len(items)).encode() + b"\r\n"
    for item in items:
        if item is None:
            out += b"$-1\r\n"
        else:
            data = str(item).encode()
            out += b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"
    return out

# ============================================================
# COMMAND HANDLERS
# ============================================================
def handle_command(parts):
    """Returns bytes to send back, or None if client should wait (BLPOP)."""
    if not parts:
        return None

    command = parts[0].upper()

    # --- PING ---
    if command == "PING":
        return resp_simple("PONG")

    # --- ECHO ---
    elif command == "ECHO":
        if len(parts) != 2:
            return resp_error("ERR wrong number of arguments for 'echo' command")
        return resp_bulk(parts[1])

    # --- SET key value [PX ms] ---
    elif command == "SET":
        if len(parts) < 3:
            return resp_error("ERR wrong number of arguments for 'set' command")
        key, value = parts[1], parts[2]
        expiry = None
        if len(parts) == 5 and parts[3].upper() == "PX":
            try:
                expiry = time.time() + (int(parts[4]) / 1000.0)
            except ValueError:
                return resp_error("ERR invalid expire time")
        with lock:
            kv_store[key] = (value, expiry)
        return resp_simple("OK")

    # --- GET key ---
    elif command == "GET":
        if len(parts) != 2:
            return resp_error("ERR wrong number of arguments for 'get' command")
        key = parts[1]
        with lock:
            entry = kv_store.get(key)
            if entry is None:
                return resp_bulk(None)
            value, expiry = entry
            if expiry and time.time() > expiry:
                del kv_store[key]
                return resp_bulk(None)
        return resp_bulk(value)

    # --- RPUSH key v1 [v2 ...] ---
    elif command == "RPUSH":
        if len(parts) < 3:
            return resp_error("ERR wrong number of arguments for 'rpush' command")
        key = parts[1]
        values = parts[2:]
        with lock:
            if key not in lists:
                lists[key] = deque()
            lists[key].extend(values)
            length = len(lists[key])
            # Wake up any BLPOP waiters
            if key in blocked_clients:
                waiters = blocked_clients.get(key, [])
                if waiters:
                    conn, _ = waiters.pop(0)
                    value = lists[key].popleft()
                    try:
                        conn.sendall(resp_array([key, value]))
                    except Exception:
                        pass
        return resp_int(length)

    # --- LPUSH key v1 [v2 ...] ---
    elif command == "LPUSH":
        if len(parts) < 3:
            return resp_error("ERR wrong number of arguments for 'lpush' command")
        key = parts[1]
        values = parts[2:]
        with lock:
            if key not in lists:
                lists[key] = deque()
            for v in values:
                lists[key].appendleft(v)
            length = len(lists[key])
            # Wake up any BLPOP waiters
            if key in blocked_clients:
                waiters = blocked_clients.get(key, [])
                if waiters:
                    conn, _ = waiters.pop(0)
                    value = lists[key].popleft()
                    try:
                        conn.sendall(resp_array([key, value]))
                    except Exception:
                        pass
        return resp_int(length)

    # --- LPOP key ---
    elif command == "LPOP":
        if len(parts) != 2:
            return resp_error("ERR wrong number of arguments for 'lpop' command")
        key = parts[1]
        with lock:
            if key not in lists or not lists[key]:
                return resp_bulk(None)
            value = lists[key].popleft()
            if not lists[key]:
                del lists[key]
        return resp_bulk(value)

    # --- LRANGE key start stop ---
    elif command == "LRANGE":
        if len(parts) != 4:
            return resp_error("ERR wrong number of arguments for 'lrange' command")
        key = parts[1]
        try:
            start = int(parts[2])
            stop = int(parts[3])
        except ValueError:
            return resp_error("ERR value is not an integer")
        with lock:
            if key not in lists:
                return resp_array([])
            items = list(lists[key])
        n = len(items)
        if start < 0: start = max(0, n + start)
        if stop < 0: stop = n + stop
        if stop >= n: stop = n - 1
        if start > stop or start >= n:
            return resp_array([])
        return resp_array(items[start:stop+1])

    # --- LLEN key ---
    elif command == "LLEN":
        if len(parts) != 2:
            return resp_error("ERR wrong number of arguments for 'llen' command")
        key = parts[1]
        with lock:
            length = len(lists.get(key, []))
        return resp_int(length)

    # --- BLPOP key timeout ---
    elif command == "BLPOP":
        if len(parts) != 3:
            return resp_error("ERR wrong number of arguments for 'blpop' command")
        key = parts[1]
        try:
            timeout = float(parts[2])
        except ValueError:
            return resp_error("ERR timeout is not a float")
        with lock:
            if key in lists and lists[key]:
                value = lists[key].popleft()
                if not lists[key]:
                    del lists[key]
                return resp_array([key, value])
            # Block — register the client
            if key not in blocked_clients:
                blocked_clients[key] = []
            blocked_clients[key].append((None, time.time() + timeout))
        # Signal "wait" — main loop will handle timeout
        return "BLOCKED"

    else:
        return resp_error(f"ERR unknown command '{parts[0]}'")

# ============================================================
# CLIENT HANDLER
# ============================================================
def handle_client(conn):
    with conn:
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                commands = parse_resp(data)
                for parts in commands:
                    response = handle_command(parts)
                    if response is None:
                        continue
                    if response == "BLOCKED":
                        # Wait for timeout then return nil
                        time.sleep(1)
                        conn.sendall(b"*-1\r\n")
                        continue
                    conn.sendall(response)
        except Exception as e:
            print(f"[Client error] {e}")

# ============================================================
# MAIN
# ============================================================
def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 6380))
    server_socket.listen(50)
    print("Redis server started on port 6380")
    while True:
        try:
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_client, args=(conn,), daemon=True).start()
        except KeyboardInterrupt:
            print("\nShutting down...")
            break
        except Exception as e:
            print(f"[Server error] {e}")

if __name__ == "__main__":
    main()
