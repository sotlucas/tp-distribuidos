import socket
import signal
import logging
import protocol
from protocol import ClientDisconnected
from uploader import Uploader
from multiprocessing import Queue, Process


class ClientHandler:
    def __init__(self, client_sock, communication_config):
        self.client_sock = client_sock
        self.uploader_queue = Queue()
        self.uploader = Process(
            target=Uploader(communication_config, self.uploader_queue).start
        ).start()
        self.running = True

        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__stop)

    def handle_client(self):
        buff = protocol.CommunicationBuffer(self.client_sock)
        while self.running == True:
            try:
                client_message = buff.get_line()
                self.uploader_queue.put(client_message)
            except OSError as e:
                return
            except ValueError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                self.client_sock.close()
                self.running = False
            except ClientDisconnected as e:
                logging.info("action: client_disconected")
                self.running = False
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
