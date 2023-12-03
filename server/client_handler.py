import socket
import logging
import multiprocessing as mp

from commons.communication_buffer import CommunicationBuffer, PeerDisconnected
from commons.message import ProtocolMessage
from commons.protocol import (
    Message,
    MessageProtocolType,
    MessageType,
    AnnounceACKMessage,
)
from message_uploader import MessageUploader
from results_uploader import ResultsUploader


class ClientHandler:
    def __init__(
        self,
        client_sock,
        server_receiver_initializer,
        vuelos_input,
        input_type,
        flights_sender,
        lat_long_sender,
    ):
        self.client_sock = client_sock
        self.flights_uploader = MessageUploader(flights_sender)
        self.lat_long_uploader = MessageUploader(lat_long_sender)
        self.buff = CommunicationBuffer(self.client_sock)

        self.client_id = ClientHandler.handle_announce(self.buff)
        vuelos_receiver = server_receiver_initializer.initialize_receiver(
            vuelos_input,
            input_type,
            1,  # REPLICA_ID
            1,  # REPLICAS_COUNT
            routing_key=str(self.client_id),
        )
        self.results_uploader = mp.Process(
            target=ResultsUploader(vuelos_receiver, self.buff).start
        )
        self.results_uploader.start()
        self.running = True

    def handle_announce(buff):
        """
        Handles the announce message received from the client.
        """
        announce_message = buff.get_message()
        logging.info(
            f"action: client_announce | client_id: {announce_message.client_id}"
        )
        buff.send_message(AnnounceACKMessage())
        return announce_message.client_id

    def handle_client(self):
        """
        Handles the messages received from the client.
        """
        while self.running:
            try:
                client_message = self.buff.get_message()
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

    def __handle_message(self, message: Message):
        """
        Handles a specific message received from the client.
        """
        uploader = (
            self.flights_uploader
            if message.protocol_type == MessageProtocolType.FLIGHT
            else self.lat_long_uploader
        )
        if message.message_type == MessageType.EOF:
            # Send EOF to queue to communicate that all the file has been sent.
            uploader.finish_sending(self.client_id)
            if message.protocol_type == MessageProtocolType.FLIGHT:
                # The client will not send any more messages
                # TODO: If the airpors file is large, the client will send more messages, fix this
                raise PeerDisconnected
        else:
            # TODO: Decoding here because send needs a string, find a better way
            #       And it is a list because send needs a list, find a better way

            # TODO: This is a temporal solution to send the message, remove this
            #       when the protocol is implemented
            protocol_message = ProtocolMessage(
                self.client_id, message.message_id, [message.content]
            )
            uploader.send(protocol_message)

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
