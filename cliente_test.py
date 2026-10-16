"""Experimento Go-Back-N con y sin control de congestión (lado cliente).

Envía un archivo de prueba N veces, cada vez con una conexión nueva, y registra
el tiempo de envío y la cantidad de segmentos de datos enviados
(number_of_sent_segments, retransmisiones incluidas).

Uso:
    python3 cliente_test.py              # con control de congestión
    python3 cliente_test.py --sin-cc     # Go-Back-N sin control de congestión

Antes de correrlo, activar la pérdida en loopback:
    sudo tc qdisc add dev lo root netem loss 20%
y al terminar:
    sudo tc qdisc del dev lo root
"""
import argparse
import csv
import os
import threading
import time

import sockettcp
from sockettcp import SocketTCP
from socketUDP import SocketUDP

sockettcp.DEBUG = False


def generar_archivo(size: int) -> bytes:
    return bytes(ord('a') + i % 26 for i in range(size))


def preparar_socket(cc_on: bool, timeout: float) -> SocketTCP:
    """Crea un SocketTCP que cuenta los segmentos enviados por Go-Back-N y usa
    el timeout indicado en vez del valor fijo de send_using_go_back_n."""
    s = SocketTCP()
    s.set_congestion_control_on(cc_on)
    s.number_of_sent_segments = 0

    udp = s.socketudp
    sendto_original = udp.sendto

    def sendto_contando(data, address, timer_index=0):
        s.number_of_sent_segments += 1
        sendto_original(data, address, timer_index)

    udp.sendto = sendto_contando
    s.set_timeout(timeout)
    return s


def ejecutar_prueba(address, cc_on, timeout, archivo, limite):
    """Retorna (tiempo, segmentos). tiempo es None si el envío no terminó
    dentro de `limite` segundos (cliente colgado)."""
    s = preparar_socket(cc_on, timeout)
    s.connect(address)
    if not s.established:
        raise RuntimeError("No se pudo establecer la conexión (¿está corriendo servidor_test.py?)")

    resultado = {}

    def enviar():
        t0 = time.perf_counter()
        s.send(archivo, mode='go_back_n')
        resultado['tiempo'] = time.perf_counter() - t0

    hilo = threading.Thread(target=enviar, daemon=True)
    hilo.start()
    hilo.join(limite)
    if hilo.is_alive():
        return None, s.number_of_sent_segments
    s.socket.close()
    return resultado.get('tiempo'), s.number_of_sent_segments


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--sin-cc', action='store_true', help='Go-Back-N sin control de congestión')
    parser.add_argument('--size', type=int, default=100, help='tamaño del archivo en bytes (default 100)')
    parser.add_argument('--runs', type=int, default=5, help='cantidad de repeticiones (default 5)')
    parser.add_argument('--timeout', type=float, default=0.5, help='timeout de retransmisión en s (default 0.5)')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5000)
    parser.add_argument('--limite', type=float, default=60, help='s antes de declarar el envío colgado')
    parser.add_argument('--pausa', type=float, default=3, help='s de espera entre repeticiones')
    parser.add_argument('--csv', default='resultados.csv', help='archivo donde se agregan los resultados')
    args = parser.parse_args()

    cc_on = not args.sin_cc
    modo = 'con CC' if cc_on else 'sin CC'
    archivo = generar_archivo(args.size)
    address = (args.host, args.port)

    print(f"Go-Back-N {modo} | archivo={args.size} B | timeout={args.timeout} s | {args.runs} repeticiones\n")
    print(f"{'Prueba':>6} | {'Tiempo [s]':>10} | {'Segmentos enviados':>18}")
    print("-" * 42)

    tiempos, segmentos = [], []
    for n in range(1, args.runs + 1):
        tiempo, enviados = ejecutar_prueba(address, cc_on, args.timeout, archivo, args.limite)
        if tiempo is None:
            print(f"{n:>6} | {'COLGADO':>10} | {enviados:>18}")
            print(f"\nEl envío no terminó en {args.limite} s; se detiene el experimento.")
            os._exit(1)
        print(f"{n:>6} | {tiempo:>10.3f} | {enviados:>18}")
        tiempos.append(tiempo)
        segmentos.append(enviados)

        nuevo = not os.path.exists(args.csv)
        with open(args.csv, 'a', newline='') as f:
            w = csv.writer(f)
            if nuevo:
                w.writerow(['modo', 'size_bytes', 'timeout_s', 'prueba', 'tiempo_s', 'segmentos_enviados'])
            w.writerow([modo, args.size, args.timeout, n, f"{tiempo:.4f}", enviados])

        if n < args.runs:
            time.sleep(args.pausa)

    print("-" * 42)
    print(f"{'Prom.':>6} | {sum(tiempos) / len(tiempos):>10.3f} | {sum(segmentos) / len(segmentos):>18.1f}")
    print(f"\nResultados agregados a {args.csv}")


if __name__ == '__main__':
    main()
