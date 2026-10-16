from sockettcp import SocketTCP

address = ("127.0.0.1", 5000)

client_socketTCP = SocketTCP()
client_socketTCP.set_congestion_control_on(True)
client_socketTCP.connect(address)
# test 1
message = "Mensje de len=16".encode()
client_socketTCP.send(message, mode='go_back_n')
# test 2
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message, mode='go_back_n')
# test 3
message = "Mensaje de largo 19".encode()
client_socketTCP.send(message, mode='go_back_n')