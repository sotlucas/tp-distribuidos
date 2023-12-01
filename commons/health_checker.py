import logging
import signal
import socket
from multiprocessing import Process

from commons.communication_buffer import CommunicationBuffer
from commons.protocol import MessageType, HealthOkMessage

HEALTHCHECK_PORT = 5000
CONNECTION_TIMEOUT = 20


class HealthChecker:
    """
    Server that sends a message to indicate that the processor is running.
    """

    def __init__(self):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", HEALTHCHECK_PORT))

        self.running = True
        self.client_handlers = []
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        """
        Runs the server and accepts new connections.
        """
        self._server_socket.listen()
        while self.running:
            try:
                client_sock = self.__accept_new_connection()
                client_sock.settimeout(CONNECTION_TIMEOUT)
                buff = CommunicationBuffer(client_sock)
                # TODO: check pool of processes
                client_proc = Process(target=self.__handle_client, args=(buff,))
                client_proc.start()
            except OSError as e:
                logging.error(f"Error: {e}")
                return

    def __handle_client(self, buff):
        """
        Handles a client connection.
        """
        while self.running:
            try:
                logging.info("action: handle_client | result: in_progress")
                message = buff.get_message()
                if message.message_type == MessageType.HEALTH_CHECK:
                    buff.send_message(HealthOkMessage())
                logging.info("action: handle_client | result: success")
            except socket.timeout:
                logging.info("action: handle_client | result: timeout")
                break

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
        self._server_socket.close()
        logging.info("action: server_shutdown | result: success")
