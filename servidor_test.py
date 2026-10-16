"""Experimento Go-Back-N con y sin control de congestión (lado servidor).

Acepta conexiones indefinidamente (Ctrl+C para terminar). Por cada una recibe
el archivo, verifica que llegó correcto y cuenta los ACKs enviados. Después de
recibir sigue respondiendo ACKs un rato, por si el último ACK se perdió y el
cliente sigue retransmitiendo.

Uso:
    python3 servidor_test.py [--size 100]
"""
import argparse
import socket

import sockettcp
from sockettcp import SocketTCP, TCPSegment, ACK

sockettcp.DEBUG = False


def generar_archivo(size: int) -> bytes:
    return bytes(ord('a') + i % 26 for i in range(size))


class SocketContador:
    """Envuelve el socket de la conexión para contar los segmentos enviados."""

    def __init__(self, sock):
        self._sock = sock
        self.enviados = 0

    def sendto(self, data, address):
        self.enviados += 1
        return self._sock.sendto(data, address)

    def __getattr__(self, name):
        return getattr(self._sock, name)


def atender_retransmisiones(conn: SocketTCP, silencio: float) -> None:
    """Responde con el ACK acumulado a todo lo que llegue hasta que pasen
    `silencio` segundos sin recibir nada. Si llega un SYN (nueva conexión) se
    termina sin responderlo: el cliente lo retransmitirá y lo tomará accept()."""
    ack = SocketTCP.create_segment(TCPSegment(ACK, conn.num, b''))
    conn.socket.settimeout(silencio)
    try:
        while True:
            try:
                raw, addr = conn.socket.recvfrom(20)
            except socket.timeout:
                break
            if SocketTCP.parse_segment(raw).syn:
                break
            conn.socket.sendto(ack, addr)
    finally:
        conn.socket.settimeout(None)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--size', type=int, default=100, help='tamaño esperado del archivo (igual que en el cliente)')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--silencio', type=float, default=1.5,
                        help='s sin recibir nada para dar por terminada una prueba')
    args = parser.parse_args()

    esperado = generar_archivo(args.size)
    address = (args.host, args.port)

    server = SocketTCP()
    server.bind(address)
    print(f"[Servidor] Escuchando en {address}, archivo esperado de {args.size} B (Ctrl+C para terminar)\n")

    n = 0
    try:
        while True:
            resultado = server.accept()
            if resultado is None:
                print("[Servidor] Handshake fallido, esperando otra conexión")
                continue
            conn, addr = resultado
            n += 1
            contador = SocketContador(conn.socket)
            conn.socket = contador

            datos = conn.recv(args.size, mode='go_back_n')
            atender_retransmisiones(conn, args.silencio)

            estado = "OK" if datos == esperado else f"ERROR ({len(datos)} B recibidos)"
            print(f"[Servidor] Prueba {n} desde {addr}: {estado} | ACKs enviados: {contador.enviados}")
    except KeyboardInterrupt:
        print("\n[Servidor] Terminado")


if __name__ == '__main__':
    main()
