import socket
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 6379))

# Send the start of a command
s.send(b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$10\r\n")
time.sleep(0.5) # Simulate network lag
s.send(b"half_")
time.sleep(0.5) # Simulate more lag
s.send(b"value\r\n")

print(s.recv(1024).decode()) # Should print +OK
