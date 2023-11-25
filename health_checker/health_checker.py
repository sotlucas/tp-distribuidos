import signal
import socket
import logging
import time
from multiprocessing import Process

HEALTH_CHECKER_PORT = 5000


class HealthCheckerConfig:
    def __init__(self, filter_general_replicas):
        self.filter_general_replicas = filter_general_replicas


class HealthChecker:
    def __init__(self, config):
        self.config = config
        self.running = True
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def run(self):
        """
        Starts the health checker.
        """
        # Call all the processors to check if they are healthy. Each in a new process.
        processor_checkers = []
        for i in range(1, self.config.filter_general_replicas + 1):
            processor_checker = Process(target=self.check_processor, args=(i,))
            processor_checker.start()
            processor_checkers.append(processor_checker)

        # Wait for the processes to finish
        for processor_checker in processor_checkers:
            processor_checker.join()
            logging.info("Processor checker finished")

    def check_processor(self, processor_id):
        """
        Checks if a processor is healthy.
        """
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((f"tp1-filter_general-{processor_id}", HEALTH_CHECKER_PORT))
                break
            except socket.error:
                logging.info("Connection Failed, Retrying..")
                time.sleep(2)
        logging.info(f"Connected to processor {processor_id}")
        while self.running:
            try:
                sock.sendall(b"CHECK\n")
                logging.info(f"Sent check message to processor {processor_id}")
                # While loop to read from socket to avoid short reads
                data = b""
                while b"\n" not in data:
                    data += sock.recv(1024)
                logging.info(f"Received data from processor {processor_id}")
                if data == b"OK\n":
                    logging.info(f"Processor {processor_id} is healthy")
                else:
                    logging.error(f"Processor {processor_id} is not healthy")
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
