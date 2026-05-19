# tests/echo_client.py

import socket

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(("localhost", 9999))   # connect to server

messages = ["hello", "add 2 3", "goodbye"]

for msg in messages:
    client.send(msg.encode())
    response = client.recv(1024)
    print(f"[CLIENT] Got back: {response.decode()}")

client.close()