import sys
import socket


def main():
    if len(sys.argv) <= 3:
        print("INVALID INPUT")
        sys.exit()

    host = sys.argv[1]
    port = sys.argv[2]

    if host == "default":
        host = '127.0.0.1'
    if port == "default":
        port = 1373
    else:
        port = int(port)

    # socket.AF_INET is an address family. sockeCLSt.SOCK_STREAM determines TCP connection
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server = (host, port)
    try:
        my_socket.connect(server)
        print("CLIENT CONNECTED")
    except:
        print("ERROR IN CONNECTING TO SERVER")
        sys.exit()

    action = sys.argv[3]
    while True:
        if action == 'subscribe':
            subscribe(my_socket, sys.argv[4:])
        elif action == 'publish':
            publish(my_socket, sys.argv[4:])
        elif action == 'ping':
            ping(my_socket)
        else:
            print("NOT VALID INPUT")
            sys.exit()

        try:
            server_response(my_socket)
        except socket.error:
            print("TIMEOUT")
            sys.exit()


def server_response(conn: socket.socket):
    conn.settimeout(10.0)
    while True:
        message = conn.recv(1024)
        if not message:
            continue
        message = message.decode("ascii")
        print(message)
        conn.settimeout(None)
        if message.split()[0] == "subAck":
            print("SUBSCRIBING ON: ")
            for topic in message.split()[1:]:
                print(topic)
        elif message == "pubAck":
            print("YOUR MESSAGE PUBLISHED SUCCESSFULLY.")
            sys.exit()
        elif message == "NOT VALID TOPIC":
            print(message)
            sys.exit()
        elif message == "pong":
            sys.exit()


def send_message(conn, message):
    message = message.encode("ascii")
    conn.send(message)


def subscribe(conn: socket.socket, topics):
    if len(topics) < 1:
        print("MUST ENTER AT LEAST ONE TOPIC\n TRY AGAIN")
        sys.exit()
    message = "subscribe"
    for topic in topics:
        message += " " + topic
    send_message(conn, message)


def publish(conn: socket.socket, data):
    message = "publish "
    if len(data) < 1:
        print("PLEASE ENTER TOPIC AND MESSAGE")
        sys.exit()
    elif len(data) < 2:
        print("PLEASE ENTER MESSAGE")
        sys.exit()
    message += data[0] + " "  # add topic to the message sent to server
    for i in data[1:]:
        message += " " + i
    send_message(conn, message)


def ping(conn: socket.socket):
    send_message(conn, "ping")


if __name__ == '__main__':
    main()
