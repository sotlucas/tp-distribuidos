import logging
import socket
import multiprocessing as mp

from commons.communication_buffer import CommunicationBuffer
from commons.protocol import AnnounceMessage, MessageType


class ProtocolConnectionConfig:
    def __init__(self, server_ip, server_port, client_id):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_id = client_id


class ProtocolConnection:
    def __init__(self, config):
        self.config = config
        self.lock = mp.Lock()
        self.__reconnect()

    def __reconnect(self):
        """
        Reconnects to the server.
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.config.server_ip, self.config.server_port))
        self.buff = CommunicationBuffer(self.sock)
        self.__send_announce()

    def __send_announce(self):
        """
        Sends the announce message to the server.
        """
        with self.lock:
            announce_message = AnnounceMessage(self.config.client_id)
            self.buff.send_message(announce_message)
            while self.buff.get_message().message_type != MessageType.ANNOUNCE_ACK:
                # TODO: retry with exponential backoff
                logging.info("Waiting for announce ACK")
                announce_message = AnnounceMessage(self.config.client_id)
                self.buff.send_message(announce_message)

    def send_message(self, message):
        """
        Sends a message to the client.
        """
        logging.info(f"SEND::getting_lock")
        with self.lock:
            logging.info(f"Sending message: {message}")
            self.buff.send_message(message)
            while self.buff.get_message().message_type != MessageType.ACK:
                self.buff.send_message(message)
            logging.info(f"AAAAAAAAAAa: {message}")

    def get_message(self):
        """
        Gets a message from the client.
        """
        logging.info(f"GET::getting_lock")
        with self.lock:
            logging.info(f"Getting message")
            return self.buff.get_message()

    def send_eof(self, protocol_type):
        """
        Sends an EOF message to the client.
        """
        with self.lock:
            logging.info(f"Sending EOF: {protocol_type}")
            self.buff.send_eof(protocol_type)
            while self.get_message().message_type != MessageType.ACK:
                self.buff.send_eof(protocol_type)

    def close(self):
        """
        Closes the connection.
        """
        self.buff.stop()
