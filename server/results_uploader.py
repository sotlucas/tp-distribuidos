import logging
from commons.protocol import END_OF_MESSAGE


class ResultsUploader:
    def __init__(self, receiver, socket):
        self.socket = socket
        self.receiver = receiver

    def start(self):
        logging.info(f"action: results_uploader | result: started")
        self.receiver.bind(self.output_callback, self.handle_eof)
        self.receiver.start()

    def output_callback(self, messages):
        message_batch = "\n".join(messages)
        self.output_single(message_batch)

    def output_single(self, message):
        # Add the END_OF_MESSAGE sequence to mark the end of the message
        message_bytes = message.encode() + END_OF_MESSAGE
        self.socket.sendall(message_bytes)
        logging.debug(f"action: result_upload | result: success")

    def handle_eof(self):
        # TODO: handle
        pass
