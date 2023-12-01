import signal
import socket
import logging
from multiprocessing import Process

from file_uploader import FileUploader
from result_handler import ResultHandler
from commons.protocol import CommunicationBuffer, MessageProtocolType, MessageType


class ClientConfig:
    def __init__(
        self,
        server_ip,
        server_port,
        flights_file_path,
        airports_file_path,
        remove_file_header,
        batch_size,
        client_id,
    ):
        self.server_ip = server_ip
        self.server_port = server_port
        self.flights_file_path = flights_file_path
        self.airports_file_path = airports_file_path
        self.remove_file_header = remove_file_header
        self.batch_size = batch_size
        self.client_id = client_id


class Client:
    def __init__(self, config):
        self.config = config
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        signal.signal(signal.SIGINT, self.__shutdown)

    def run(self):
        # Create a socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to the server
        self.sock.connect((self.config.server_ip, self.config.server_port))
        logging.info("Connected to server")
        self.buff = CommunicationBuffer(self.sock)

        # Start the process to send the airports
        self.airports_sender = Process(
            target=FileUploader(
                MessageProtocolType.AIRPORT,
                self.config.airports_file_path,
                self.config.remove_file_header,
                self.config.batch_size,
                self.buff,
                self.config.client_id,
            ).start
        )
        self.airports_sender.start()

        # Start the process to send the flights
        self.flights_sender = Process(
            target=FileUploader(
                MessageProtocolType.FLIGHT,
                self.config.flights_file_path,
                self.config.remove_file_header,
                self.config.batch_size,
                self.buff,
                self.config.client_id,
            ).start
        )
        self.flights_sender.start()

        # Start the process to receive the results
        self.results_receiver = Process(
            target=ResultHandler(self.buff, self.config.client_id).receive_results
        )
        self.results_receiver.start()

        # Wait for the processes to finish
        self.airports_sender.join()
        logging.info("Airports sender finished")
        self.flights_sender.join()
        logging.info("Waiting for results")
        self.results_receiver.join()
        logging.info("All processes finished")

    def __shutdown(self, signum=None, frame=None):
        logging.info("Shutting down")
        self.buff.stop()
        if self.airports_sender.exitcode is None:
            self.airports_sender.terminate()
        if self.flights_sender.exitcode is None:
            self.flights_sender.terminate()
        if self.results_receiver.exitcode is None:
            self.results_receiver.terminate()
        logging.info("Shut down completed")
