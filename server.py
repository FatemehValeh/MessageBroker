import socket
import threading
import time

HOST_IP = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 1373  # Port to listen on (non-privileged ports are > 1023)

# a list of topics with their listeners
topics_listeners = {}


def main():
    host = (HOST_IP, PORT)
    # socket.AF_INET is an address family. socket.SOCK_STREAM determines TCP connection
    my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    my_socket.bind(host)
    print(">SERVER IS UP<")

    my_socket.listen()
    while True:
        conn, address = my_socket.accept()
        # create a thread for each connection
        thread = threading.Thread(target=client_handler, args=(conn, address))
        thread.start()


def client_handler(conn, address):
    print("NEW CONNECTION FROM {}".format(address))
    connected = True  # open and close of connection
    while connected:
        try:
            data = conn.recv(1024)
            if not data:
                continue
            data = data.decode("ascii")
            print("data received: '{}'".format(data))
            if data == "DISCONNECT":
                connected = False
            else:
                message_handler(conn, data)
        except:
            remove_client(conn)
            print("CONNECTION CLOSED SUDDENLY: ", address)
            break
    conn.close()


def message_handler(conn, data):
    action = data.split()[0]
    if action == "subscribe":
        subscribe_handler(conn, data.split()[1:])
    elif action == "publish":
        print(data)
        publish_handler(conn, data.split()[1:])
    elif action == "ping":
        pong(conn)


def pong(conn: socket.socket):
    send_message(conn, "pong")


def publish_handler(conn: socket.socket, data):
    topic = data[0]
    message = ' '.join(data[1:])
    res = topic + ":" + message
    if topic not in topics_listeners.keys():
        send_message(conn, "NOT VALID TOPIC")
    else:
        # time.sleep(20)  # for checking if pubAck wasn't received in 10 ms
        send_message(conn, "pubAck")
        for client in topics_listeners[topic]:
            try:
                send_message(client, res)
            except:
                remove_client(conn)
                print("CONNECTION CLOSED")


def subscribe_handler(conn: socket.socket, topics):
    for topic in topics:
        if topic in topics_listeners.keys():  # if the topic was already created, add this client to it's listeners
            if conn not in topics_listeners[topic]:  # if this client was not listening this topic
                topics_listeners[topic].append(conn)
        else:
            topics_listeners[topic] = [conn]
    message = "subAck"
    for topic in topics_listeners.keys():
        if conn in topics_listeners[topic]:
            message += " " + topic
    send_message(conn, message)


def send_message(conn, message):
    message = message.encode("ascii")
    conn.send(message)


def remove_client(conn):
    for i in topics_listeners:
        if conn in topics_listeners[i]:
            topics_listeners[i].remove(conn)
    conn.close()


if __name__ == '__main__':
    main()
