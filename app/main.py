import socket  # noqa: F401


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        connection.recv(1024)
        connection.sendall(b"+PONG\r\n")
        connection.close()


if __name__ == "__main__":
    main()
