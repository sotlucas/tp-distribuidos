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
    def __init__(self, server_ip, server_port, flights_file_path, airports_file_path, remove_file_header, batch_size):
        self.server_ip = server_ip
        self.server_port = server_port
        self.flights_file_path = flights_file_path
        self.airports_file_path = airports_file_path
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

        # Start the process to send the airports
        self.airports_sender = Process(target=self.send_file, args=("airport", self.config.airports_file_path))
        self.airports_sender.start()
        # Start the process to send the flights
        self.flights_sender = Process(target=self.send_file, args=("flight", self.config.flights_file_path))
        self.flights_sender.start()
        # Start the process to receive the results
        self.results_receiver = Process(target=self.receive_results)
        self.results_receiver.start()

        # Wait for the processes to finish
        self.results_receiver.join()
        self.flights_sender.join()
        self.airports_sender.join()
        if self.running:
            self.shutdown()

    def send_file(self, type, file_path):
        """
        Send the csv file line by line to the server.

        Each line represents a flight with all the columns separated by commas.
        """
        # TODO: move this to the protocol serializer
        # Add the type of message to the beginning of the line
        if type == "airport":
            type_bytes = int.to_bytes(1, 1, byteorder="big")
        elif type == "flight":
            type_bytes = int.to_bytes(2, 1, byteorder="big")

        logging.info(f"Sending file: {file_path}")
        for batch in self.next_batch(file_path, self.config.batch_size):
            message = type_bytes + batch.encode() + END_OF_MESSAGE
            self.sock.sendall(message)
        # Send message to indicate that the file has ended
        message = type_bytes + b"\0" + END_OF_MESSAGE
        self.sock.sendall(message)
        logging.info(f"File sent: {file_path}")

    def next_batch(self, file_path, batch_size):
        """
        Gets a batch of rows from the file.
        """
        batch = []
        with open(file_path, "r") as f:
            if self.config.remove_file_header:
                # Skip the header
                next(f)
            for line in f:
                batch.append(line)
                if len(batch) == batch_size or not line:
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
                message = buffer.get_line()
                if not message:
                    break
                logging.debug(f"Result received: {message.type} | {message.content}")
                result_handler.save_results(message.content.decode())
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
