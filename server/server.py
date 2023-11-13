import signal
import socket
import logging
import multiprocessing as mp

from client_handler import ClientHandler
from results_listener import ResultsListener
from results_uploader import ResultsUploader


class ServerConfig:
    def __init__(self, port, connection_timeout):
        self.port = port
        self.connection_timeout = connection_timeout


class Server:
    def __init__(self, config, server_receiver, flights_sender, lat_long_sender):
        self.config = config
        self.server_receiver = server_receiver
        self.flights_sender = flights_sender
        self.lat_long_sender = lat_long_sender
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", config.port))
        self._server_socket.listen()
        self.running = True
        self.results_listener_queue = mp.Queue()  # Queue to send messages to the results listener
        self.results_listener = ResultsListener(self.server_receiver, self.results_listener_queue)
        self.results_listener_proc = mp.Process(
            target=self.results_listener.start
        )
        self.results_listener_proc.start()
        self.results_listener_proc2 = mp.Process(
            target=self.results_listener.add_client_handler
        )
        self.results_listener_proc2.start()
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        while self.running:
            try:
                client_sock = self.__accept_new_connection()
                client_sock.settimeout(self.config.connection_timeout)
                client_handler_queue = mp.Queue()  # Queue to send messages to the client handler
                results_uploader = ResultsUploader(client_handler_queue, client_sock)
                client_handler = ClientHandler(
                    client_sock, self.flights_sender, self.lat_long_sender, results_uploader
                )
                self.results_listener_queue.put((client_handler.id, results_uploader))
                # TODO: create a new process for each client handler. Use a Pool of processes
                client_handler.handle_client()
            except OSError as e:
                logging.error(f"Error: {e}")
                return

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
