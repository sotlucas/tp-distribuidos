import logging
from commons.protocol import Message


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

    def output_single(self, content):
        message = Message(None, content)
        self.socket.sendall(message.serialize())
        logging.debug(f"action: result_upload | result: success")

    def handle_eof(self):
        # TODO: handle
        pass
