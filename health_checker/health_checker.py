import signal
import socket
import logging
import time


class HealthChecker:
    def __init__(self):
        self.running = True
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        """
        Runs the server and accepts new connections.
        """
        # Call all the processors to check if they are healthy

        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(("tp1-filter_general-3", 5000))
                break
            except socket.error:
                logging.info("Connection Failed, Retrying..")
                time.sleep(1)
        logging.info("Connected to processor")
        while self.running:
            try:
                sock.sendall(b"CHECK\n")
                logging.info("Sent check message")
                # While loop to read from socket to avoid short reads
                data = b""
                while b"\n" not in data:
                    data += sock.recv(1024)
                logging.info("Received data")
                if data == b"OK\n":
                    logging.info("Processor is healthy")
                else:
                    logging.error("Processor is not healthy")
                time.sleep(5)
            except OSError as e:
                logging.exception(f"Error: {e}")
                return

    def __stop(self, *args):
        """
        Stop server closing the server socket.
        """
        logging.info("action: server_shutdown | result: in_progress")
        self.running = False
        logging.info("action: server_socket_closed | result: success")
        logging.info("action: server_shutdown | result: success")
