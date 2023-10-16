import signal
import socket
import logging
from multiprocessing import Process
from result_handler import ResultHandler
from commons.protocol import (
    CommunicationBuffer,
    PeerDisconnected,
    END_OF_MESSAGE,
)


class ClientConfig:
    def __init__(self, server_ip, server_port, file_path, remove_file_header, batch_size):
        self.server_ip = server_ip
        self.server_port = server_port
        self.file_path = file_path
        self.remove_file_header = remove_file_header
        self.batch_size = batch_size


class Client:
    def __init__(self, config):
        self.config = config
        self.running = True
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.shutdown)

    def run(self):
        # Create a socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to the server
        self.sock.connect((self.config.server_ip, self.config.server_port))
        logging.info("Connected to server")

        # Start the process to send the file
        self.file_sender = Process(target=self.send_file)
        self.file_sender.start()
        # Start the process to receive the results
        self.results_receiver = Process(target=self.receive_results)
        self.results_receiver.start()

        # Wait for the processes to finish

        self.results_receiver.join()
        self.file_sender.join()
        if self.running:
            self.shutdown()

    def send_file(self):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """
        logging.info("Sending file")
        for batch in self.next_batch(self.config.batch_size):
            self.sock.sendall(batch.encode() + END_OF_MESSAGE)
        self.sock.sendall(b"\0" + END_OF_MESSAGE)
        logging.info("File sent")

    def next_batch(self, batch_size):
        """
        Gets a batch of flights from the file.
        """
        batch = []
        with open(self.config.file_path, "r") as f:
            if self.config.remove_file_header:
                # Skip the header
                next(f)
            for line in f:
                batch.append(line)
                if len(batch) == batch_size:
                    yield "".join(batch)
                    batch = []

    def receive_results(self):
        """
        Receive the results from the server.
        """
        logging.info("Receiving results")
        buffer = CommunicationBuffer(self.sock)
        result_handler = ResultHandler()
        while self.running:
            try:
                data = buffer.get_line()
                if not data:
                    break
                logging.debug(f"Result received: {data}")
                result_handler.save_results(data)
            except PeerDisconnected:
                logging.info("action: server_disconected")
                self.running = False
            except OSError as e:
                # When receiving SIGTERM, the socket is closed and a OSError is raised.
                # If not we want to raise the exception.
                if self.running:
                    raise e
                return

        logging.info("Results received")

    def shutdown(self, signum=None, frame=None):
        logging.info("Shutting down")
        self.sock.shutdown(socket.SHUT_WR)
        self.sock.close()
        self.running = False
        logging.info("Shut down completed")
