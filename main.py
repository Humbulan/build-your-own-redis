import socket
import threading
import time
from collections import deque

# Storage for lists
lists = {}
# Blocked clients waiting for BLPOP: {key: deque([(conn, start_time)])}
blocked_clients = {}
# Lock for thread safety
lock = threading.Lock()

def parse_resp(data):
    """Parse RESP protocol commands"""
    if not data:
        return []
    
    lines = data.split('\r\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        if line.startswith('*'):
            # Array
            i += 1
        elif line.startswith('$'):
            # Bulk string
            length = int(line[1:])
            if i + 1 < len(lines):
                result.append(lines[i + 1])
                i += 2
            else:
                i += 1
        else:
            i += 1
    
    return result

def encode_response(response):
    """Encode response to RESP format"""
    if response is None:
        return b""
    if isinstance(response, str):
        return response.encode()
    return response

def handle_command(parts, conn=None):
    """Handle Redis commands"""
    if not parts:
        return "-ERR no command\r\n"
    
    command = parts[0].upper()
    
    # PING command
    if command == "PING":
        return "+PONG\r\n"
    
    # ECHO command
    elif command == "ECHO":
        if len(parts) > 1:
            return f"${len(parts[1])}\r\n{parts[1]}\r\n"
        return "$0\r\n\r\n"
    
    # SET command
    elif command == "SET":
        if len(parts) >= 3:
            key = parts[1]
            value = parts[2]
            lists[key] = value
            return "+OK\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # GET command
    elif command == "GET":
        if len(parts) >= 2:
            key = parts[1]
            if key in lists and not isinstance(lists[key], deque):
                value = lists[key]
                return f"${len(value)}\r\n{value}\r\n"
            return "$-1\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # RPUSH command
    elif command == "RPUSH":
    elif command == "LPUSH":
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            if key not in lists:
                lists[key] = []
            for value in reversed(values):
                lists[key].insert(0, value)
            return f":{len(lists[key])}\r\n"
        return "-ERR wrong number of arguments\r\n"
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            
            with lock:
                if key not in lists:
                    lists[key] = deque()
                elif not isinstance(lists[key], deque):
                    return "-ERR Operation against a key holding the wrong kind of value\r\n"
                
                for value in values:
                    lists[key].append(value)
                
                length = len(lists[key])
                
                # Wake up blocked BLPOP clients
                if key in blocked_clients and blocked_clients[key]:
                    # Get the first blocked client (oldest)
                    blocked_conn, _ = blocked_clients[key].popleft()
                    if lists[key]:
                        popped_value = lists[key].popleft()
                        response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(popped_value)}\r\n{popped_value}\r\n"
                        try:
                            blocked_conn.send(response.encode())
                        except:
                            pass
                
                return f":{length}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # LPUSH command
    elif command == "LPUSH":
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            
            with lock:
                if key not in lists:
                    lists[key] = deque()
                elif not isinstance(lists[key], deque):
                    return "-ERR Operation against a key holding the wrong kind of value\r\n"
                
                for value in reversed(values):
                    lists[key].appendleft(value)
                
                return f":{len(lists[key])}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # LPOP command
    elif command == "LPOP":
        if len(parts) >= 2:
            key = parts[1]
            
            with lock:
                if key not in lists or not isinstance(lists[key], deque) or not lists[key]:
                    return "$-1\r\n"
                
                value = lists[key].popleft()
                return f"${len(value)}\r\n{value}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # LRANGE command
    elif command == "LRANGE":
        if len(parts) == 4:
            key = parts[1]
            try:
                start = int(parts[2])
                end = int(parts[3])
            except ValueError:
                return "-ERR value is not an integer\r\n"
            
            with lock:
                if key not in lists or not isinstance(lists[key], deque):
                    return "*0\r\n"
                
                lst = list(lists[key])
                length = len(lst)
                
                # Handle negative indices
                if start < 0:
                    start = length + start
                if end < 0:
                    end = length + end
                
                if start < 0:
                    start = 0
                if end >= length:
                    end = length - 1
                
                if start > end or start >= length:
                    return "*0\r\n"
                
                result = lst[start:end+1]
                response = f"*{len(result)}\r\n"
                for item in result:
                    response += f"${len(item)}\r\n{item}\r\n"
                return response
        return "-ERR wrong number of arguments\r\n"
    
    # LLEN command
    elif command == "LLEN":
        if len(parts) == 2:
            key = parts[1]
            
            with lock:
                if key not in lists or not isinstance(lists[key], deque):
                    return ":0\r\n"
                return f":{len(lists[key])}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # BLPOP command - BLOCKING
    elif command == "BLPOP":
        if len(parts) >= 3:
            key = parts[1]
            try:
                timeout = int(parts[2])
            except ValueError:
                return "-ERR value is not an integer or out of range\r\n"
            
            with lock:
                # Check if list exists and has elements
                if key in lists and isinstance(lists[key], deque) and lists[key]:
                    # List has elements - pop immediately
                    value = lists[key].popleft()
                    return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                else:
                    # List is empty - block the client
                    if conn:
                        if key not in blocked_clients:
                            blocked_clients[key] = deque()
                        blocked_clients[key].append((conn, time.time()))
                        # Return None to indicate no immediate response
                        return None
                    return "*-1\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # Unknown command
    else:
        return "-ERR unknown command\r\n"

def handle_client(conn, addr):
    """Handle client connection"""
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            parts = parse_resp(data.decode())
            if not parts:
                continue
            
            response = handle_command(parts, conn)
            if response is not None:
                conn.send(response.encode())
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        # Remove client from any blocked queues
        with lock:
            for key in list(blocked_clients.keys()):
                blocked_clients[key] = deque([(c, t) for c, t in blocked_clients[key] if c != conn])
        conn.close()

def main():
    """Main server loop"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(5)
    
    print("Redis server started on port 6379")
    
    try:
        while True:
            conn, addr = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.daemon = True
            client_thread.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        server_socket.close()

if __name__ == "__main__":
    main()
