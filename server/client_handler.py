import socket
import signal
import logging
import commons.protocol as protocol
from commons.protocol import PeerDisconnected
from flights_uploader import FlightsUploader
from results_uploader import ResultsUploader
from multiprocessing import Queue, Process


class ClientHandler:
    def __init__(self, client_sock, communication_config):
        self.client_sock = client_sock
        self.flights_uploader_queue = Queue()
        self.flights_uploader = Process(
            target=FlightsUploader(
                communication_config, self.flights_uploader_queue
            ).start
        ).start()
        self.results_uploader = Process(
            target=ResultsUploader(communication_config, self.client_sock).start
        ).start()
        self.running = True

        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def handle_client(self):
        buff = protocol.CommunicationBuffer(self.client_sock)
        while self.running == True:
            try:
                client_message = buff.get_line()
                self.flights_uploader_queue.put(client_message)
            except OSError as e:
                logging.debug(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except ValueError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except PeerDisconnected as e:
                logging.info("action: client_disconected")
                self.running = False
        # Send EOF to queue to communicate that all the file has been sent.
        self.flights_uploader_queue.put(b"\0")
        self.client_sock.close()
        logging.info(f"action: handle_client | result: complete")

    def handle_client_message(self, client_message):
        self.communication.send_output(client_message)
        logging.info(f"action: receive_message_request | result: success")

    def __stop(self, *args):
        """
        Stop server closing the client socket.
        """
        logging.info("action: client_handler_shutdown | result: in_progress")
        self.client_sock.shutdown(socket.SHUT_RDWR)
        self.client_sock.close()
        self.running = False
        logging.info("action: client_handler_shutdown | result: success")
