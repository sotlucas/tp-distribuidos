import signal
import logging
import multiprocessing as mp
import socket
import time

from commons.communication_buffer import CommunicationBuffer
from file_uploader import FileUploader
from result_handler import ResultHandler
from commons.protocol import (
    MessageProtocolType,
    AnnounceMessage,
    MessageType,
)


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
        send_queue = mp.Queue()

        airports_queue = mp.Queue()
        # Start the process to send the airports
        self.airports_sender = mp.Process(
            target=FileUploader(
                MessageProtocolType.AIRPORT,
                self.config.airports_file_path,
                self.config.remove_file_header,
                self.config.batch_size,
                self.config.client_id,
                airports_queue,
                send_queue,
            ).start
        )
        self.airports_sender.start()

        flights_queue = mp.Queue()
        # Start the process to send the flights
        self.flights_sender = mp.Process(
            target=FileUploader(
                MessageProtocolType.FLIGHT,
                self.config.flights_file_path,
                self.config.remove_file_header,
                self.config.batch_size,
                self.config.client_id,
                flights_queue,
                send_queue,
            ).start
        )
        self.flights_sender.start()

        results_queue = mp.Queue()
        # Start the process to receive the results
        self.results_receiver = mp.Process(
            target=ResultHandler(
                self.config.client_id, results_queue, send_queue
            ).receive_results
        )
        self.results_receiver.start()

        self.connect_to_server()

        # Start the sender and receiver processes
        self.sender_process = mp.Process(target=self.sender, args=(send_queue,))
        self.sender_process.start()

        self.receiver_process = mp.Process(
            target=self.receiver,
            args=(results_queue, flights_queue, airports_queue),
        )
        self.receiver_process.start()

        # Wait for the processes to finish
        self.receiver_process.join()
        logging.info("Receiver finished")
        self.sender_process.join()
        logging.info("Sender finished")
        self.airports_sender.join()
        logging.info("Airports sender finished")
        self.flights_sender.join()
        logging.info("Waiting for results")
        self.results_receiver.join()
        logging.info("All processes finished")

    def receiver(self, results_queue, flights_queue, airports_queue):
        """
        Receives messages from the server.
        """
        while True:
            try:
                message_recv = self.buff.get_message()
            except Exception as e:
                logging.info(f"Server disconnected: {e}. Retrying in 10 seconds")
                time.sleep(10)
                self.connect_to_server()
                continue
            logging.info(f"CLIENT | Received message: {message_recv}")
            if message_recv.message_type == MessageType.EOF:
                continue
            elif message_recv.message_type == MessageType.RESULT:
                results_queue.put(message_recv)
            elif message_recv.protocol_type == MessageProtocolType.FLIGHT:
                flights_queue.put(message_recv)
            elif message_recv.protocol_type == MessageProtocolType.AIRPORT:
                airports_queue.put(message_recv)

    def sender(self, send_queue):
        """
        Sends messages to the server.
        """
        while True:
            message_to_send = send_queue.get()
            try:
                logging.info(f"CLIENT | Sending message: {message_to_send}")
                if message_to_send.message_type == MessageType.EOF:
                    self.buff.send_eof(message_to_send.protocol_type)
                else:
                    self.buff.send_message(message_to_send)
            except Exception as e:
                logging.info(f"Server disconnected: {e}. Retrying in 10 seconds")
                time.sleep(10)
                self.connect_to_server()

    def connect_to_server(self):
        while True:  # TODO: check
            try:
                # Create a socket
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # Connect to the server
                self.sock.connect((self.config.server_ip, self.config.server_port))
                logging.info("CLIENT | Connected to server")
                self.buff = CommunicationBuffer(self.sock)
                self.send_announce()
                break
            except Exception as e:
                logging.info(f"Server disconnected: {e}. Retrying in 10 seconds")
                time.sleep(10)

    def send_announce(self):
        announce_message = AnnounceMessage(self.config.client_id)
        self.buff.send_message(announce_message)
        while self.buff.get_message().message_type != MessageType.ANNOUNCE_ACK:
            # TODO: retry with exponential backoff
            logging.info(f"Retrying announce message: {announce_message}")
            self.buff.send_message(announce_message)
            time.sleep(10)

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
