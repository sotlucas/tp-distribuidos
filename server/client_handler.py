import socket
import logging
import multiprocessing as mp

import commons.protocol as protocol
from commons.message import FlightMessage
from commons.protocol import PeerDisconnected
from message_uploader import MessageUploader
from results_uploader import ResultsUploader


class ClientHandler:
    def __init__(self, corr_id, client_sock, receiver, flights_sender, lat_long_sender):
        self.id = corr_id
        self.client_sock = client_sock
        self.flights_uploader = MessageUploader(flights_sender)
        self.lat_long_uploader = MessageUploader(lat_long_sender)
        self.results_uploader = mp.Process(
            target=ResultsUploader(receiver, self.client_sock).start
        )
        self.results_uploader.start()
        self.running = True

    def handle_client(self):
        """
        Handles the messages received from the client.
        """
        buff = protocol.CommunicationBuffer(self.client_sock)
        while self.running:
            try:
                client_message = buff.get_message()
                self.__handle_message(client_message)
            except OSError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except ValueError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self.running = False
            except PeerDisconnected as e:
                logging.info("action: client_disconected")
                self.running = False
        self.results_uploader.join()
        self.client_sock.close()
        logging.info(f"action: handle_client | result: complete")

    def __handle_message(self, message):
        """
        Handles a specific message received from the client.
        """
        uploader = self.flights_uploader if message.type == "flight" else self.lat_long_uploader
        if message.content == protocol.EOF:
            # Send EOF to queue to communicate that all the file has been sent.
            uploader.finish_sending()
            if message.type == "flight" and message.content == protocol.EOF:
                # The client will not send any more messages
                raise PeerDisconnected
        else:
            flight_message = FlightMessage(
                self.id,
                message.content
            )
            uploader.send(flight_message.to_bytes())

    def stop(self):
        """
        Stop client server closing resources.
        """
        logging.info("action: client_handler_shutdown | result: in_progress")
        self.running = False
        self.client_sock.shutdown(socket.SHUT_RDWR)
        self.client_sock.close()
        self.flights_uploader.stop()
        self.lat_long_uploader.stop()
        self.results_uploader.terminate()
        logging.info("action: client_handler_shutdown | result: success")
