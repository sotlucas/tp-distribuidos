import signal
import socket
import logging
import time
from multiprocessing import Process

from commons.protocol import CommunicationBuffer, Message

HEALTH_CHECKER_PORT = 5000
CONNECTION_RETRY_TIME = 2
HEALTH_CHECK_INTERVAL = 5


class HealthCheckerConfig:
    def __init__(self, filter_general_replicas, filter_multiple_replicas):
        self.filter_general_replicas = filter_general_replicas
        self.filter_multiple_replicas = filter_multiple_replicas


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
            processor_checker = Process(target=self.check_processor, args=(f"tp1-filter_general-{i}",))
            processor_checker.start()
            processor_checkers.append(processor_checker)

        for i in range(1, self.config.filter_multiple_replicas + 1):
            processor_checker = Process(target=self.check_processor, args=(f"tp1-filter_multiple-{i}",))
            processor_checker.start()
            processor_checkers.append(processor_checker)

        # Wait for the processes to finish
        for processor_checker in processor_checkers:
            processor_checker.join()
            logging.info("Processor checker finished")

    def check_processor(self, processor_name):
        """
        Checks if a processor is healthy.
        """
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((processor_name, HEALTH_CHECKER_PORT))
                buff = CommunicationBuffer(sock)
                break
            except socket.error:
                logging.info(f"Connection Failed to processor {processor_name}, Retrying...")
                time.sleep(CONNECTION_RETRY_TIME)
        logging.info(f"Connected to processor {processor_name}")
        while self.running:
            try:
                buff.send_message(Message(None, "CHECK\n"))
                if buff.get_message().content == b"OK\n":
                    logging.info(f"Processor {processor_name} is healthy")
                else:
                    logging.error(f"Processor {processor_name} is not healthy")
                time.sleep(HEALTH_CHECK_INTERVAL)
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
