import logging
import signal
import socket

from commons.protocol import Message


class ResultsUploader:
    """
    It sends the results to the client through a socket.
    """

    def __init__(self, queue, socket):
        self.queue = queue
        self.socket = socket

    def start(self):
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        logging.info(f"action: results_listener | result: started")
        while True:  # TODO: handle shutdown
            messages = self.queue.get()
            self.output_callback(messages)

    def output_callback(self, messages):
        message_batch = "\n".join(messages)
        self.output_single(message_batch)

    def output_single(self, content):
        try:
            message = Message(None, content)
            self.socket.sendall(message.serialize())
            logging.debug(f"action: result_upload | result: success")
        except OSError as e:
            logging.error(f"action: result_upload | result: fail | error: {e}")

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("action: results_listener_shutdown | result: success")
