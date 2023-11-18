import logging
import signal
import socket

from commons.protocol import Message


class ResultsUploader:
    def __init__(self, receiver, socket):
        self.socket = socket
        self.receiver = receiver

    def start(self):
        # Register signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self.__shutdown)
        logging.info(f"action: results_uploader | result: started")
        self.receiver.bind(self.output_callback, self.handle_eof)
        self.receiver.start()

    def output_callback(self, messages):
        message_batch = "\n".join(messages.payload)
        self.output_single(message_batch)

    def output_single(self, content):
        try:
            message = Message(None, content)
            self.socket.sendall(message.serialize())
            logging.debug(f"action: result_upload | result: success")
        except OSError as e:
            logging.error(f"action: result_upload | result: fail | error: {e}")

    def handle_eof(self):
        # TODO: handle
        pass

    def __shutdown(self, *args):
        """
        Graceful shutdown. Closing all connections.
        """
        logging.info("action: results_uploader_shutdown | result: in_progress")
        self.receiver.stop()
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("action: results_uploader_shutdown | result: success")
