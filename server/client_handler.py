import socket
import signal
import logging
import commons.protocol as protocol
from commons.protocol import PeerDisconnected
from flights_uploader import FlightsUploader
from results_uploader import ResultsUploader
from lat_long_uploader import LatLongUploader
from multiprocessing import Process


class ClientHandler:
    def __init__(self, client_sock, receiver, flights_sender, lat_long_sender):
        self.client_sock = client_sock
        self.flights_uploader = FlightsUploader(flights_sender)
        self.lat_long_uploader = LatLongUploader(lat_long_sender)
        self.results_uploader = Process(
            target=ResultsUploader(receiver, self.client_sock).start
        )
        self.results_uploader.start()
        self.running = True

        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def handle_client(self):
        """
        Handles the messages received from the client.
        """
        buff = protocol.CommunicationBuffer(self.client_sock)
        while self.running:
            try:
                client_message = buff.get_line()

                if client_message.message_type == "airport":
                    if client_message.message == b"\0":
                        # Send EOF to queue to communicate that all the file has been sent.
                        self.lat_long_uploader.finish_sending()
                    else:
                        logging.info(f"Sending airport: {client_message.message}")
                        self.lat_long_uploader.send(client_message.message)
                elif client_message.message_type == "flight":
                    if client_message.message == b"\0":
                        # Send EOF to queue to communicate that all the file has been sent.
                        self.flights_uploader.finish_sending()
                    else:
                        logging.info(f"Sending flight: {client_message.message}")
                        self.flights_uploader.send(client_message.message)
            except OSError as e:
                logging.debug(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except ValueError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except PeerDisconnected as e:
                logging.info("action: client_disconected")
                self.running = False
        # TODO: parece que no se espera aca
        self.results_uploader.join()
        self.client_sock.close()
        logging.info(f"action: handle_client | result: complete")

    def __stop(self, *args):
        """
        Stop server closing the client socket.
        """
        logging.info("action: client_handler_shutdown | result: in_progress")
        self.client_sock.shutdown(socket.SHUT_RDWR)
        self.client_sock.close()
        self.running = False
        logging.info("action: client_handler_shutdown | result: success")
