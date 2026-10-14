from threading import Timer
import socket


class SocketUDP:
    """La clase SocketUDP es un wrapper de la clase socket provista por python.
    Este permite el manejo de mÃºltiples timers que pueden detenerse manualmente
    incluso si la funciÃ³n recvfrom ha recibido algo.
    """

    def __init__(self):
        """Constructor de la clase SocketUDP. Contiene los siguientes
        parÃ¡metros:
         - socket_udp: corresponde a un socket no orientado a conexiÃ³n de
         python. Este se inicializa como no bloqueante para poder simular el
         manejo de timers.
         - timeout: corresponde al tiempo en segundos que esperarÃ¡ cualquier
         timer antes de lanzar un TimeoutError. Se inicializa con un valor
         negativo (no vÃ¡lido).
         - timer_list: contiene la lista de Timers. Estos timers corresponden
         a objetos Timer provistos por threading de python. Note que estos
         objetos no pueden ser reutilizados. Inicialmente esta lista se
         encuentra vacÃ­a.
         - announce_timeout: corresponde a una lista de valores booleanos que
         indica si un timer debe ser anunciado a travÃ©s de un TimeoutError. El
         anuncio de timeout se realiza una Ãºnica vez para cada timer, salvo que
         este se resetee.
         - timed_out_timers: lista de ints que contiene los Ã­ndices de los
         timers en timer_list que ya cumplieron su timeout.

        """
        self.socket_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket_udp.setblocking(False)
        self.timeout = -1.0
        self.timer_list = [None]
        self.announce_timeout = [False]
        self.timed_out_timers = []

    def bind(self, address: tuple[str, int]) -> None:
        """Wrapper de funciÃ³n bind. Funciona igual que en sockets de python.

        :param address: tuple[str, int]
            par (IP, puerto) donde escucharÃ¡ el socket_udp.
        :return: None
        """
        self.socket_udp.bind(address)

    def settimeout(self, timeout: float) -> None:
        """Configura el tiempo en segundos que esperarÃ¡ cada timer antes de
        detenerse.

        Al configurarse aÃ±ade un timer en caso de no haberse configurado uno
        (o mÃ¡s) previamente.

        :param timeout: float
            tiempo en segundos que espera cada timer.
        :return: None
        """
        # guarda el timepo de timeout
        self.timeout = timeout

    def set_timer_list_length(self, number_of_timers: int) -> None:
        """AÃ±ade o remueve elementos de las listas asociadas a los timers.

        :param number_of_timers: int
            nÃºmero de timers que se desea manejar.
        :return: None
        """
        if number_of_timers == 0:
            return
        # si faltan timers se aÃ±aden
        while len(self.timer_list) < number_of_timers:
            self.timer_list.append(None)
            self.announce_timeout.append(False)
        # si sobran timers se sacan del final de la lista
        while len(self.timer_list) > number_of_timers:
            self.timer_list.pop()
            self.announce_timeout.pop()

    def sendto(
        self, data: bytes, address: tuple[str, int], timer_index=0
    ) -> None:
        """Wrapper de la funciÃ³n sendto de python que permite el manejo de
        timers.

        Si se desea que se inicie un timer especÃ­fico al realizar el envÃ­o de
        datos, se debe indicar a travÃ©s de la variable timer_index.

        Dentro de esta funciÃ³n se crea un nuevo objeto Timer de threading, se
        aÃ±ade a la lista en la posiciÃ³n timer_index, y se echa a correr luego
        de enviar los datos. Una vez el objeto Timer cumple su tiempo de espera
        llama a la funciÃ³n self._time_up_function con timer_index como parÃ¡metro.

        :param data: bytes
            mensaje que se desea enviar.
        :param address: tuple[str, int]
            par (IP, puerto) donde se enviarÃ¡ data.
        :param timer_index: int
            Ã­ndice del timer asociado al mensaje enviado. Su valor por defecto
            es 0.
        :return: None
        """

        # checkea que el Ã­ndice del timer sea vÃ¡lido y si lo es crea el Timer
        if timer_index not in range(len(self.timer_list)):
            raise IndexError("Indice timer_index fuera de rango.")
        elif self.timeout > 0 and self.timer_list[timer_index] is None:
            # lo creamos solo si no existe y hay un tiempo de timeot seteado
            self.timer_list[timer_index] = Timer(
                self.timeout, self._time_up_function, args=(timer_index,)
            )

        # envÃ­a el mensaje contenido en data
        self.socket_udp.sendto(data, address)
        if self.timeout > 0 and not self.timer_list[timer_index].is_alive():
            # echa a correr el timer reciÃ©n creado solo si es que no se encontraba corriendo
            self.timer_list[timer_index].start()

    def _time_up_function(self, timer_index: int) -> None:
        """FunciÃ³n que llama cada objeto Timer en caso de cumplir su timeout.

        Esta funciÃ³n indica que el timout del timer asociado a timer_index debe
        ser anunciado, y aÃ±ade el Ã­ndice timer_index a la lista de
        timed_out_timers.

        :param timer_index: int
            Ã­ndice del Timer en timer_list
        :return: None
        """
        self.announce_timeout[timer_index] = True
        self.timed_out_timers.append(timer_index)

    def recvfrom(self, buffer_size: int) -> tuple[bytes, tuple[str, int]]:
        """Wrapper de la funciÃ³n recvfrom de python que permite el manejo de
        timers.

        Esta funciÃ³n se mantiene escuchando continuamente hasta que llegue un
        mensaje o se cumpla el tiempo de espera de algÃºn Timer. En caso de que
        se cumpla el timeout de un Timer, lanza un TimeoutError indicando el
        Ã­ndice del timer correspondiente. En caso de recibir un mensaje, lo
        retorna PERO no detiene ningÃºn Timer.

        :param buffer_size: int
            tamaÃ±o del buffer de recepciÃ³n.
        :return: tuple[bytes, tuple[str, int]]
            tupla que contiene el mensaje recibido en bytes y la direcciÃ³n
            desde donde se recibiÃ³ dicho mensaje.
        """
        # para manejar la recepciÃ³n con timers es necesario mantenerse
        # escuchando con un while True.
        #
        # Esto ocurre porque se debe mantener el socket como no bloqueante
        # para poder revisar si es necesario levantar un error o no.
        while True:
            if True in self.announce_timeout:
                # si algÃºn timer cumple su tiempo, se lanza un TimeoutError
                timer_index = self.announce_timeout.index(True)
                self.announce_timeout[timer_index] = False
                raise TimeoutError(f"Timer {timer_index}")
            try:
                # si se logra recibir algo retornamos (no se cancela ningÃºn Timer)
                answer, address = self.socket_udp.recvfrom(buffer_size)
                return answer, address
            except BlockingIOError:
                # cada vez que el socket NO recibe algo, lanza BlockingIOError
                continue

    def get_stopped_timers(self) -> list[int]:
        """Retorna la lista de Ã­ndices de Timers que ya cumplieron su timeout.

        :return: list[int]
            lista de Ã­ndices de Timers detenidos
        """
        return self.timed_out_timers

    def stop_timer(self, timer_index=0) -> None:
        """Permite detener y resettear el objeto Timer asociado a timer_index.

        :param timer_index: int
            Ã­ndice del Timer a detener. Su valor default es 0.
        :return: None
        """
        # primero se cancela el Timer para que detenga su ejecuciÃ³n.
        self.timer_list[timer_index].cancel()
        # luego se borra de las listas correspondientes
        self.timer_list[timer_index] = None
        if timer_index in self.timed_out_timers:
            self.timed_out_timers.remove(timer_index)
        self.announce_timeout[timer_index] = False

    def close(self) -> None:
        """Wrapper de close.

        :return: None
        """
        self.socket_udp.close()
