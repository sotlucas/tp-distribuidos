import logging
from commons.communication import Communication
from commons.protocol import END_OF_MESSAGE


class ResultsUploader:
    def __init__(self, receiver, socket):
        self.socket = socket
        self.receiver = receiver

    def start(self):
        logging.info(f"action: results_uploader | result: success ")
        self.receiver.run(
            input_callback=self.output_callback, eof_callback=self.handle_eof
        )

    def output_callback(self, message):
        # Add the END_OF_MESSAGE sequence to mark the end of the message
        message_bytes = message.encode() + END_OF_MESSAGE
        self.socket.sendall(message_bytes)
        logging.info(f"action: result_upload | result: success")

    def handle_eof(self):
        # TODO: handle
        pass
