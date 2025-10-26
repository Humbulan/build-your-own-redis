import socket
import threading
import time

# Simple storage
lists = {}
blocking_queue = {}  # {key: [connection]}

def parse_resp(data):
    if not data:
        return []
    
    lines = data.split('\r\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        if line.startswith('*'):
            i += 1
        elif line.startswith('$'):
            length = int(line[1:])
            if i + 1 < len(lines) and lines[i + 1]:
                result.append(lines[i + 1])
                i += 2
            else:
                i += 1
        else:
            i += 1
    
    return result

def handle_command(parts, conn=None):
    if not parts:
        return "-ERR no command\r\n"
    
    command = parts[0].upper()
    
    if command == "PING":
        return "+PONG\r\n"
    elif command == "ECHO":
        if len(parts) > 1:
            return f"${len(parts[1])}\r\n{parts[1]}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    elif command == "BLPOP":
        if len(parts) == 3:
            key = parts[1]
            timeout = parts[2]
            
            if key in lists and lists[key]:
                value = lists[key].pop(0)
                return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            else:
                if key not in blocking_queue:
                    blocking_queue[key] = []
                blocking_queue[key].append(conn)
                
                time.sleep(0.1)
                
                if key in lists and lists[key]:
                    value = lists[key].pop(0)
                    if conn in blocking_queue[key]:
                        blocking_queue[key].remove(conn)
                    return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                else:
                    if conn in blocking_queue[key]:
                        blocking_queue[key].remove(conn)
                    return "*0\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    elif command == "RPUSH":
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            
            if key not in lists:
                lists[key] = []
            
            added_count = 0
            for value in values:
                lists[key].append(value)
                added_count += 1
            
            if key in blocking_queue and blocking_queue[key] and lists[key]:
                blocking_conn = blocking_queue[key].pop(0)
                if lists[key]:
                    value = lists[key].pop(0)
                    response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
                    try:
                        blocking_conn.send(response.encode())
                    except:
                        pass
            
            return f":{added_count}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    # ADDED BACK: LPOP with count support
    elif command == "LPOP":
        if len(parts) >= 2:
            key = parts[1]
            count = 1
            if len(parts) == 3:
                try:
                    count = int(parts[2])
                except ValueError:
                    return "-ERR value is not an integer\r\n"
            
            if key not in lists or not lists[key]:
                if count == 1:
                    return "$-1\r\n"
                else:
                    return "*0\r\n"
            
            if count == 1:
                value = lists[key].pop(0)
                return f"${len(value)}\r\n{value}\r\n"
            else:
                result = []
                for _ in range(min(count, len(lists[key]))):
                    result.append(lists[key].pop(0))
                response = f"*{len(result)}\r\n"
                for item in result:
                    response += f"${len(item)}\r\n{item}\r\n"
                return response
        return "-ERR wrong number of arguments\r\n"
    
    elif command == "LRANGE":
        if len(parts) == 4:
            key = parts[1]
            try:
                start, stop = int(parts[2]), int(parts[3])
            except ValueError:
                return "-ERR value is not an integer\r\n"
            
            if key not in lists:
                return "*0\r\n"
            
            list_len = len(lists[key])
            start = max(0, list_len + start if start < 0 else start)
            stop = min(list_len - 1, list_len + stop if stop < 0 else stop)
            
            if start > stop:
                return "*0\r\n"
            
            result = lists[key][start:stop+1]
            response = f"*{len(result)}\r\n"
            for item in result:
                response += f"${len(item)}\r\n{item}\r\n"
            return response
        return "-ERR wrong number of arguments\r\n"
    
    elif command == "LLEN":
        if len(parts) == 2:
            key = parts[1]
            if key not in lists:
                return ":0\r\n"
            return f":{len(lists[key])}\r\n"
        return "-ERR wrong number of arguments\r\n"
    
    else:
        return "-ERR unknown command\r\n"

def handle_client(conn):
    try:
        while True:
            data = conn.recv(1024).decode()
            if not data:
                break
            
            parts = parse_resp(data)
            if not parts:
                continue
            
            response = handle_command(parts, conn)
            conn.send(response.encode())
            
    except Exception as e:
        pass
    finally:
        for key in list(blocking_queue.keys()):
            if conn in blocking_queue[key]:
                blocking_queue[key].remove(conn)
        conn.close()

def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("localhost", 6379))
    server_socket.listen(10)
    
    print("Redis server started on port 6379")
    
    while True:
        conn, addr = server_socket.accept()
        client_thread = threading.Thread(target=handle_client, args=(conn,))
        client_thread.daemon = True
        client_thread.start()

if __name__ == "__main__":
    main()
