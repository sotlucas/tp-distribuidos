import hashlib
import signal
import socket
import logging

from client_handler import ClientHandler


class ServerConfig:
    def __init__(self, port, connection_timeout, vuelos_input, input_type):
        self.port = port
        self.connection_timeout = connection_timeout
        self.vuelos_input = vuelos_input
        self.input_type = input_type


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

        self.running = True
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)
        self.client_handlers = []

    def run(self):
        while self.running:
            try:
                client_sock = self.__accept_new_connection()
                client_sock.settimeout(self.config.connection_timeout)
                # Create client_id hashing client ip and port, and then converting it to int
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
                # TODO: create a new process for each client handler. Use a Pool of processes
                client_handler.handle_client()
            except OSError as e:
                logging.error(f"Error: {e}")
                return

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
