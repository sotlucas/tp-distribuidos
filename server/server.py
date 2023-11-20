import hashlib
import signal
import socket
import logging
from multiprocessing import BoundedSemaphore, Process, Queue

from client_handler import ClientHandler
from results_listener import ResultsListener


class ServerConfig:
    def __init__(self, port, connection_timeout, vuelos_input, input_type, max_clients):
        self.port = port
        self.connection_timeout = connection_timeout
        self.vuelos_input = vuelos_input
        self.input_type = input_type
        self.max_clients = max_clients


class Server:
    def __init__(self, config, server_receiver_initializer, flights_sender, lat_long_sender):
        self.config = config
        self.server_receiver_initializer = server_receiver_initializer
        self.flights_sender = flights_sender
        self.lat_long_sender = lat_long_sender
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", config.port))
        self._server_socket.listen()

        self.results_listener = Process(target=self.results_listener_initializer)
        self.results_listener.start()
        self.running = True
        self.client_handlers = []
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def results_listener_initializer(self):
        """
        Initializes the results listener.
        """
        receiver = self.server_receiver_initializer.initialize_receiver(
            "vuelos_resultados",
            "QUEUE",
            1,  # REPLICAS_COUNT
        )
        sender = self.server_receiver_initializer.initialize_sender(
            "vuelos_resultados_listener",
            "EXCHANGE",
        )
        results_listener = ResultsListener(receiver, sender)
        results_listener.start()

    def run(self):
        """
        Runs the server and accepts new connections.
        """
        queue = Queue()
        client_handler_tracker = Process(target=self.client_handler_tracker, args=(queue,))
        client_handler_tracker.start()
        while self.running:
            try:
                client_sock = self.__accept_new_connection()
                client_sock.settimeout(self.config.connection_timeout)
                queue.put(client_sock)
            except OSError as e:
                logging.error(f"Error: {e}")
                return
        client_handler_tracker.join()

    def client_handler_tracker(self, queue):
        """
        Creates a new process for each client connection and keeps track of them.
        """
        sema = BoundedSemaphore(self.config.max_clients)
        procs = []
        while True:
            client_sock = queue.get()
            sema.acquire()  # wait to start until another process is finished
            procs.append(Process(target=self.worker, args=(sema, client_sock)))
            procs[-1].start()

            # cleanup completed processes TODO: check this
            while not procs[0].is_alive():
                procs.pop(0)
        for p in procs:
            p.join()

    def worker(self, sema, client_sock):
        """
        Worker that handles a client connection and releases the semaphore when it finishes
        """
        try:
            client_id = self.get_client_id(client_sock)
            vuelos_receiver = self.server_receiver_initializer.initialize_receiver(
                self.config.vuelos_input,
                self.config.input_type,
                1,  # REPLICAS_COUNT
                routing_key=str(client_id),
                replica_id=1,
            )
            client_handler = ClientHandler(
                client_id, client_sock, vuelos_receiver, self.flights_sender, self.lat_long_sender
            )
            client_handler.handle_client()
        finally:
            sema.release()  # allow a new process to be started now that this one is exiting

    def get_client_id(self, client_sock):
        """
        Creates a client id hashing the client ip and port
        """
        client_id = int(hashlib.md5(
            f"{client_sock.getpeername()[0]}:{client_sock.getpeername()[1]}".encode(),
        ).hexdigest()[:8], 16)
        logging.info(f"action: client_id | result: success | client_id: {client_id}")
        return client_id

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c

    def __stop(self, *args):
        """
        Stop server closing the server socket.
        """
        logging.info("action: server_shutdown | result: in_progress")
        self.running = False
        for client_handler in self.client_handlers:
            client_handler.stop()
        # TODO: Check if we need to close the server socket here
        self._server_socket.shutdown(socket.SHUT_RDWR)
        self._server_socket.close()
        logging.info("action: server_socket_closed | result: success")
        logging.info("action: server_shutdown | result: success")
