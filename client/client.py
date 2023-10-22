import signal
import socket
import logging
from multiprocessing import Process

from file_uploader import FileUploader
from result_handler import ResultHandler
from commons.protocol import CommunicationBuffer, PeerDisconnected


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
        self.airports_sender = Process(
            target=FileUploader("airport", self.config.airports_file_path, self.config.remove_file_header,
                                self.config.batch_size, self.sock).start)
        self.airports_sender.start()
        # We wait for the airports to be sent before sending the flights
        # TODO: fix concurrency in send to parallelize
        self.airports_sender.join()

        # Start the process to send the flights
        self.flights_sender = Process(
            target=FileUploader("flight", self.config.flights_file_path, self.config.remove_file_header,
                                self.config.batch_size, self.sock).start)
        self.flights_sender.start()

        # Start the process to receive the results
        self.results_receiver = Process(target=self.receive_results)
        self.results_receiver.start()

        # Wait for the processes to finish
        self.flights_sender.join()
        self.results_receiver.join()
        if self.running:
            self.shutdown()

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
