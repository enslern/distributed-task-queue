# tests/echo_server.py

import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 9999))   # claim this address
server.listen(5)                   # allow up to 5 queued connections
print("[SERVER] Listening on port 9999...")

while True:
    conn, addr = server.accept()   # blocks until a client connects
    print(f"[SERVER] Connected: {addr}")

    while True:
        data = conn.recv(1024)     # read up to 1024 bytes
        if not data:
            break                  # client disconnected
        print(f"[SERVER] Got: {data.decode()}")
        conn.send(data)            # echo it back

    conn.close()